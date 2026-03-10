// Package grpc implements the gRPC server and service definitions for NalaDB.
package grpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/meta"
	"github.com/thatscalaguy/naladb/internal/metrics"
	"github.com/thatscalaguy/naladb/internal/query"
	nraft "github.com/thatscalaguy/naladb/internal/raft"
	"github.com/thatscalaguy/naladb/internal/segment"
	"github.com/thatscalaguy/naladb/internal/store"
	"github.com/thatscalaguy/naladb/internal/tenant"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// LogEntry represents a single gRPC request log entry.
type LogEntry struct {
	Method   string
	Duration time.Duration
	Err      error
}

// maxLogEntries is the maximum number of gRPC log entries kept in memory.
const maxLogEntries = 10_000

// Server wraps the gRPC server and NalaDB services.
type Server struct {
	grpcServer   *grpc.Server
	kvService    *KVService
	graphSvc     *GraphService
	watchSvc     *WatchService
	healthSvc    *health.Server
	logMu        sync.Mutex
	logEntries   []LogEntry
	logHead      int // index of the oldest entry in the ring buffer
	logLen       int // number of valid entries
	requestCount atomic.Int64
	metrics      *metrics.Metrics
}

// ServerOption configures a Server.
type ServerOption func(*serverOptions)

type serverOptions struct {
	metrics      *metrics.Metrics
	executor     *query.Executor
	metaRegistry *meta.Registry
	segmentMgr   *segment.Manager
}

// WithMetrics adds Prometheus metrics instrumentation to the server.
func WithMetrics(m *metrics.Metrics) ServerOption {
	return func(o *serverOptions) { o.metrics = m }
}

// WithQueryExecutor enables the NalaQL QueryService.
func WithQueryExecutor(exec *query.Executor) ServerOption {
	return func(o *serverOptions) { o.executor = exec }
}

// WithMetaRegistry sets the KeyMeta registry for the StatsService.
func WithMetaRegistry(r *meta.Registry) ServerOption {
	return func(o *serverOptions) { o.metaRegistry = r }
}

// WithSegmentManager sets the segment manager for the StatsService.
func WithSegmentManager(sm *segment.Manager) ServerOption {
	return func(o *serverOptions) { o.segmentMgr = sm }
}

// applyOptions folds functional options into a serverOptions struct.
func applyOptions(opts []ServerOption) *serverOptions {
	o := &serverOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// initGRPCServer creates the gRPC server with interceptor chains, health
// server, and reflection. extraUnary/extraStream are placed before the
// logging interceptor; metrics (if configured) are prepended to the front.
func (s *Server) initGRPCServer(opts *serverOptions, extraUnary []grpc.UnaryServerInterceptor, extraStream []grpc.StreamServerInterceptor) {
	s.healthSvc = health.NewServer()

	unaryInts := append(extraUnary, s.loggingUnaryInterceptor)
	streamInts := append(extraStream, s.loggingStreamInterceptor)
	if opts != nil && opts.metrics != nil {
		unaryInts = append([]grpc.UnaryServerInterceptor{metricsUnaryInterceptor(opts.metrics)}, unaryInts...)
		streamInts = append([]grpc.StreamServerInterceptor{metricsStreamInterceptor(opts.metrics)}, streamInts...)
	}

	s.grpcServer = grpc.NewServer(
		grpc.ChainUnaryInterceptor(unaryInts...),
		grpc.ChainStreamInterceptor(streamInts...),
	)

	healthpb.RegisterHealthServer(s.grpcServer, s.healthSvc)
	reflection.Register(s.grpcServer)
}

// markCoreServicesHealthy sets the health status for the standard services.
func (s *Server) markCoreServicesHealthy() {
	for _, svc := range []string{
		"naladb.v1.KVService",
		"naladb.v1.GraphService",
		"naladb.v1.WatchService",
		"",
	} {
		s.healthSvc.SetServingStatus(svc, healthpb.HealthCheckResponse_SERVING)
	}
}

// registerOptionalServices conditionally registers QueryService and
// StatsService based on the provided options.
func (s *Server) registerOptionalServices(opts *serverOptions, st *store.Store, g *graph.Graph) {
	if opts.executor != nil {
		pb.RegisterQueryServiceServer(s.grpcServer, NewQueryService(opts.executor))
		s.healthSvc.SetServingStatus("naladb.v1.QueryService", healthpb.HealthCheckResponse_SERVING)
	}
	if opts.metaRegistry != nil {
		pb.RegisterStatsServiceServer(s.grpcServer, NewStatsService(st, g, opts.metaRegistry, opts.segmentMgr))
		s.healthSvc.SetServingStatus("naladb.v1.StatsService", healthpb.HealthCheckResponse_SERVING)
	}
}

// NewServer creates a new gRPC server with all NalaDB services registered.
func NewServer(s *store.Store, g *graph.Graph, opts ...ServerOption) *Server {
	o := applyOptions(opts)

	srv := &Server{metrics: o.metrics}
	watchMgr := NewWatchManager()

	srv.kvService = NewKVService(s, watchMgr)
	srv.graphSvc = NewGraphService(g)
	srv.watchSvc = NewWatchService(watchMgr)

	srv.initGRPCServer(o, nil, nil)

	pb.RegisterKVServiceServer(srv.grpcServer, srv.kvService)
	pb.RegisterGraphServiceServer(srv.grpcServer, srv.graphSvc)
	pb.RegisterWatchServiceServer(srv.grpcServer, srv.watchSvc)
	srv.registerOptionalServices(o, s, g)
	srv.markCoreServicesHealthy()

	return srv
}

// NewTenantServer creates a gRPC server with multi-tenant support.
// Tenant isolation is provided via key-prefix partitioning, quota enforcement,
// and rate limiting. The tenant ID is extracted from the "x-tenant-id" gRPC metadata.
func NewTenantServer(s *store.Store, clock *hlc.Clock, mgr *tenant.Manager) *Server {
	srv := &Server{}
	watchMgr := NewWatchManager()

	srv.kvService = NewKVService(s, watchMgr)
	srv.graphSvc = NewTenantGraphService(mgr)
	srv.watchSvc = NewWatchService(watchMgr)

	srv.initGRPCServer(nil,
		[]grpc.UnaryServerInterceptor{tenant.UnaryInterceptor(mgr)},
		[]grpc.StreamServerInterceptor{tenant.StreamInterceptor(mgr)},
	)

	pb.RegisterKVServiceServer(srv.grpcServer, srv.kvService)
	pb.RegisterGraphServiceServer(srv.grpcServer, srv.graphSvc)
	pb.RegisterWatchServiceServer(srv.grpcServer, srv.watchSvc)
	srv.markCoreServicesHealthy()

	return srv
}

// NewClusterServer creates a gRPC server backed by a RAFT cluster with
// leader routing and consistency level support.
func NewClusterServer(router *nraft.Router, opts ...ServerOption) *Server {
	o := applyOptions(opts)

	srv := &Server{metrics: o.metrics}
	watchMgr := NewWatchManager()
	cluster := router.Cluster()

	srv.watchSvc = NewWatchService(watchMgr)

	srv.initGRPCServer(o,
		[]grpc.UnaryServerInterceptor{ConsistencyInterceptor(router)},
		[]grpc.StreamServerInterceptor{ConsistencyStreamInterceptor(router)},
	)

	pb.RegisterKVServiceServer(srv.grpcServer, NewClusterKVService(cluster, watchMgr))
	pb.RegisterGraphServiceServer(srv.grpcServer, NewClusterGraphService(cluster))
	pb.RegisterWatchServiceServer(srv.grpcServer, srv.watchSvc)
	srv.registerOptionalServices(o, cluster.Store(), cluster.Graph())
	srv.markCoreServicesHealthy()

	return srv
}

// Serve starts the gRPC server on the given address.
func (s *Server) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc: listen: %w", err)
	}
	log.Printf("NalaDB gRPC server listening on %s", addr)
	return s.grpcServer.Serve(lis)
}

// ServeListener starts the gRPC server on an existing listener.
func (s *Server) ServeListener(lis net.Listener) error {
	return s.grpcServer.Serve(lis)
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() {
	s.watchSvc.Close()
	s.grpcServer.GracefulStop()
}

// LogEntries returns a copy of the recorded log entries in chronological order.
// At most maxLogEntries are retained.
func (s *Server) LogEntries() []LogEntry {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	out := make([]LogEntry, s.logLen)
	cap := len(s.logEntries)
	for i := 0; i < s.logLen; i++ {
		out[i] = s.logEntries[(s.logHead+i)%cap]
	}
	return out
}

// appendLogEntry adds an entry to the ring buffer. Must be called with logMu held.
func (s *Server) appendLogEntry(e LogEntry) {
	if len(s.logEntries) < maxLogEntries {
		// Still growing the underlying slice.
		s.logEntries = append(s.logEntries, e)
		s.logLen++
		return
	}
	// Buffer is full — overwrite the oldest entry.
	idx := (s.logHead + s.logLen) % maxLogEntries
	s.logEntries[idx] = e
	if s.logLen == maxLogEntries {
		s.logHead = (s.logHead + 1) % maxLogEntries
	} else {
		s.logLen++
	}
}

// RequestCount returns the total number of requests handled.
func (s *Server) RequestCount() int64 {
	return s.requestCount.Load()
}

// loggingUnaryInterceptor logs unary RPC calls and increments the request counter.
func (s *Server) loggingUnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	duration := time.Since(start)

	s.requestCount.Add(1)
	s.logMu.Lock()
	s.appendLogEntry(LogEntry{
		Method:   info.FullMethod,
		Duration: duration,
		Err:      err,
	})
	s.logMu.Unlock()
	log.Printf("gRPC %s %v err=%v", info.FullMethod, duration, err)
	return resp, err
}

// loggingStreamInterceptor logs streaming RPC calls and increments the request counter.
func (s *Server) loggingStreamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()
	err := handler(srv, ss)
	duration := time.Since(start)

	s.requestCount.Add(1)
	s.logMu.Lock()
	s.appendLogEntry(LogEntry{
		Method:   info.FullMethod,
		Duration: duration,
		Err:      err,
	})
	s.logMu.Unlock()
	log.Printf("gRPC %s %v err=%v", info.FullMethod, duration, err)
	return err
}
