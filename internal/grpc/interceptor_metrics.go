package grpc

import (
	"context"
	"time"

	"github.com/thatscalaguy/naladb/internal/metrics"

	"google.golang.org/grpc"
)

// metricsUnaryInterceptor records gRPC request counts and classifies
// operations as reads or writes for duration histograms.
func metricsUnaryInterceptor(m *metrics.Metrics) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start).Seconds()

		m.GRPCRequestsTotal.WithLabelValues(info.FullMethod).Inc()

		if isWriteMethod(info.FullMethod) {
			m.WritesTotal.Inc()
			m.WriteDuration.Observe(duration)
		} else {
			m.ReadsTotal.Inc()
			m.ReadDuration.Observe(duration)
		}

		return resp, err
	}
}

// metricsStreamInterceptor records gRPC streaming request counts.
func metricsStreamInterceptor(m *metrics.Metrics) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		m.GRPCRequestsTotal.WithLabelValues(info.FullMethod).Inc()
		return handler(srv, ss)
	}
}

// isWriteMethod returns true for gRPC methods that perform writes.
func isWriteMethod(method string) bool {
	switch method {
	case "/naladb.v1.KVService/Set",
		"/naladb.v1.KVService/Delete",
		"/naladb.v1.KVService/BatchSet",
		"/naladb.v1.GraphService/CreateNode",
		"/naladb.v1.GraphService/UpdateNode",
		"/naladb.v1.GraphService/DeleteNode",
		"/naladb.v1.GraphService/CreateEdge",
		"/naladb.v1.GraphService/UpdateEdge",
		"/naladb.v1.GraphService/DeleteEdge":
		return true
	default:
		return false
	}
}
