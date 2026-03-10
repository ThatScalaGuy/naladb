// Package main is the entry point for the NalaDB server.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/thatscalaguy/naladb/internal/config"
	"github.com/thatscalaguy/naladb/internal/graph"
	ngrpc "github.com/thatscalaguy/naladb/internal/grpc"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/meta"
	"github.com/thatscalaguy/naladb/internal/metrics"
	"github.com/thatscalaguy/naladb/internal/query"
	nraft "github.com/thatscalaguy/naladb/internal/raft"
	"github.com/thatscalaguy/naladb/internal/segment"
	"github.com/thatscalaguy/naladb/internal/store"
	"github.com/thatscalaguy/naladb/internal/wal"
)

// version is set at build time via -ldflags.
var version = "dev"

const (
	segmentMaxBytes = 64 * 1024 * 1024 // 64 MiB per segment
	compactInterval = 30 * time.Second
)

func main() {
	// Config file flag.
	configPath := flag.String("config", "", "path to YAML config file")

	// Common flags.
	showVersion := flag.Bool("v", false, "print version and exit")
	addr := flag.String("addr", ":7301", "gRPC listen address")
	metricsAddr := flag.String("metrics-addr", ":9090", "Prometheus metrics HTTP address")
	nodeID := flag.Uint("node-id", 0, "HLC node ID (0-15)")
	maxClockSkew := flag.Duration("max-clock-skew", time.Second, "maximum tolerated clock skew between nodes (0 = disabled)")

	// Storage flags (standalone mode).
	walDir := flag.String("wal-dir", "data/wal", "WAL directory")
	segDir := flag.String("segment-dir", "data/segments", "segment storage directory")

	// RAFT flags (cluster mode).
	raftEnabled := flag.Bool("raft", false, "enable RAFT clustering")
	raftNodeID := flag.String("raft-node-id", "", "unique RAFT node ID (e.g. node-0)")
	raftAddr := flag.String("raft-addr", ":7400", "RAFT transport bind address")
	raftAdvertise := flag.String("raft-advertise", "", "RAFT advertise address (default: raft-addr)")
	raftDir := flag.String("raft-dir", "data/raft", "RAFT data directory")
	raftBootstrap := flag.Bool("raft-bootstrap", false, "bootstrap cluster (use on all initial nodes with same peers)")
	raftPeers := flag.String("raft-peers", "", "RAFT peers: id=host:port,id=host:port,...")
	grpcPeers := flag.String("grpc-peers", "", "gRPC peers for leader forwarding: id=host:port,...")

	flag.Parse()

	if *showVersion {
		fmt.Println("naladb " + version)
		return
	}

	// Load config file (if provided) and apply values for flags not
	// explicitly set on the command line.
	if *configPath != "" {
		cfg, err := config.Load(*configPath)
		if err != nil {
			log.Fatalf("Failed to load config %q: %v", *configPath, err)
		}

		explicitly := make(map[string]bool)
		flag.Visit(func(f *flag.Flag) { explicitly[f.Name] = true })

		if !explicitly["addr"] {
			*addr = cfg.Cluster.ListenAddr
		}
		if !explicitly["metrics-addr"] {
			*metricsAddr = cfg.Metrics.Addr
		}
		if !explicitly["node-id"] {
			*nodeID = cfg.HLC.NodeID
		}
		if !explicitly["max-clock-skew"] {
			d, err := cfg.HLC.ParseMaxClockSkew()
			if err != nil {
				log.Fatalf("Invalid max_clock_skew in config: %v", err)
			}
			*maxClockSkew = d
		}
		if !explicitly["wal-dir"] {
			*walDir = cfg.Storage.WALDir
		}
		if !explicitly["segment-dir"] {
			*segDir = cfg.Storage.SegmentDir
		}
		if !explicitly["raft"] {
			*raftEnabled = cfg.Raft.Enabled
		}
		if !explicitly["raft-node-id"] {
			*raftNodeID = cfg.Raft.NodeID
		}
		if !explicitly["raft-addr"] {
			*raftAddr = cfg.Raft.BindAddr
		}
		if !explicitly["raft-advertise"] {
			*raftAdvertise = cfg.Raft.AdvertiseAddr
		}
		if !explicitly["raft-dir"] {
			*raftDir = cfg.Raft.DataDir
		}
		if !explicitly["raft-bootstrap"] {
			*raftBootstrap = cfg.Raft.Bootstrap
		}
		if !explicitly["raft-peers"] {
			*raftPeers = cfg.Raft.PeersFlag()
		}
		if !explicitly["grpc-peers"] {
			*grpcPeers = cfg.Raft.GRPCPeersFlag()
		}

		log.Printf("Loaded config from %s", *configPath)
	}

	// Initialize Prometheus metrics.
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	// Initialize clock.
	clock := hlc.NewClock(uint8(*nodeID))
	clock.SetMaxSkew(*maxClockSkew)

	var (
		srv     *ngrpc.Server
		kvStore *store.Store
		gLayer  *graph.Graph
		stopBg  = make(chan struct{})
		cluster *nraft.Cluster
		router  *nraft.Router
	)

	if *raftEnabled {
		srv, kvStore, gLayer, cluster, router = startCluster(
			clock, m, *addr, *raftNodeID, *raftAddr, *raftAdvertise,
			*raftDir, *raftBootstrap, *raftPeers, *grpcPeers,
		)
	} else {
		srv, kvStore, gLayer = startStandalone(
			clock, m, *addr, *walDir, *segDir, stopBg,
		)
	}

	// Start Prometheus metrics HTTP server.
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		log.Printf("Prometheus metrics listening on %s/metrics", *metricsAddr)
		if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// Start metric collector for gauge metrics.
	collector := metrics.NewCollector(m, metrics.CollectorConfig{
		IndexLen:     func() int { return kvStore.Stats().Keys },
		SegmentCount: func() int { return 0 },
		SegmentBytes: func() int64 { return 0 },
		StoreStats: func() (int, int, int) {
			st := kvStore.Stats()
			return st.Keys, st.Versions, st.Tombstones
		},
		GraphStats: func() (int, int, int, int) {
			gs := gLayer.Stats()
			return gs.Nodes, gs.ActiveNodes, gs.Edges, gs.ActiveEdges
		},
	})
	collector.Start(5 * time.Second)
	defer collector.Stop()

	// Graceful shutdown on SIGINT/SIGTERM.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Printf("Received %v, shutting down...", sig)
		close(stopBg)
		if cluster != nil {
			_ = cluster.Shutdown()
		}
		if router != nil {
			router.Close()
		}
		srv.Stop()
	}()

	log.Printf("NalaDB %s starting on %s", version, *addr)
	if err := srv.Serve(*addr); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// startCluster initializes NalaDB in RAFT cluster mode.
func startCluster(
	clock *hlc.Clock, m *metrics.Metrics, addr string,
	raftNodeID, raftAddr, raftAdvertise, raftDir string,
	raftBootstrap bool, raftPeersFlag, grpcPeersFlag string,
) (*ngrpc.Server, *store.Store, *graph.Graph, *nraft.Cluster, *nraft.Router) {
	// In RAFT mode, the RAFT log is the WAL.
	kvStore := store.NewWithoutWAL(clock)
	graphLayer := graph.New(kvStore, clock)

	metaRegistry := meta.NewRegistry()
	kvStore.SetMeta(metaRegistry)
	executor := query.NewExecutor(kvStore, graphLayer, metaRegistry, clock)

	// Create RAFT data directory.
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		log.Fatalf("Failed to create RAFT directory: %v", err)
	}

	// Parse peer maps.
	raftPeerMap := parsePeers(raftPeersFlag)
	grpcPeerMap := parsePeers(grpcPeersFlag)

	// Build peer list for bootstrap configuration.
	peers := make([]nraft.PeerConfig, 0, len(raftPeerMap))
	for id, peerAddr := range raftPeerMap {
		peers = append(peers, nraft.PeerConfig{ID: id, Address: peerAddr})
	}

	// Create transport.
	advertise := raftAdvertise
	if advertise == "" {
		advertise = raftAddr
	}
	transport, err := nraft.NewTCPTransport(nraft.TransportConfig{
		BindAddr:      raftAddr,
		AdvertiseAddr: advertise,
		MaxPool:       5,
		Timeout:       10 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create RAFT transport: %v", err)
	}

	// Create RAFT cluster.
	cfg := nraft.DefaultClusterConfig(raftNodeID, raftDir)
	cfg.Bootstrap = raftBootstrap
	cfg.Peers = peers
	cfg.GRPCAddr = addr

	cluster, err := nraft.NewCluster(cfg, kvStore, graphLayer, clock, transport)
	if err != nil {
		log.Fatalf("Failed to create RAFT cluster: %v", err)
	}

	log.Printf("RAFT cluster node %q started (bootstrap=%v, peers=%d)", raftNodeID, raftBootstrap, len(peers))

	// Create router for leader forwarding.
	router := nraft.NewRouter(cluster, grpcPeerMap)

	// Create cluster-aware gRPC server.
	srv := ngrpc.NewClusterServer(router,
		ngrpc.WithMetrics(m),
		ngrpc.WithQueryExecutor(executor),
		ngrpc.WithMetaRegistry(metaRegistry),
	)

	return srv, kvStore, graphLayer, cluster, router
}

// startStandalone initializes NalaDB in single-node standalone mode.
func startStandalone(
	clock *hlc.Clock, m *metrics.Metrics, _, walDir, segDir string,
	stopBg chan struct{},
) (*ngrpc.Server, *store.Store, *graph.Graph) {
	// Initialize WAL.
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		log.Fatalf("Failed to create WAL directory: %v", err)
	}
	walFile, err := os.OpenFile(walDir+"/wal.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("Failed to open WAL file: %v", err)
	}
	walWriter := wal.NewWriter(walFile, wal.WriterOptions{})

	// Initialize segment manager.
	segMgr, err := segment.NewManager(segDir, segmentMaxBytes)
	if err != nil {
		log.Fatalf("Failed to initialize segment manager: %v", err)
	}

	kvStore := store.New(clock, walWriter)
	kvStore.SetSegments(segMgr)

	// Recover in-memory state from finalized segments.
	var recoveredRecords int
	for _, seg := range segMgr.Segments() {
		records, err := seg.ReadAll()
		if err != nil {
			log.Fatalf("Failed to read segment %d: %v", seg.ID, err)
		}
		for _, rec := range records {
			kvStore.SetWithHLCAndFlags(
				string(rec.Key), rec.HLC, rec.Value,
				rec.Flags.IsTombstone(), rec.Flags.IsBlobRef(),
			)
			recoveredRecords++
		}
	}

	// Replay WAL records (data written since last segment rotation).
	walReadFile, err := os.Open(walDir + "/wal.log")
	if err != nil && !os.IsNotExist(err) {
		log.Fatalf("Failed to open WAL for replay: %v", err)
	}
	if err == nil {
		walReader := wal.NewReader(walReadFile)
		walRecords, err := walReader.ReadAll()
		walReadFile.Close()
		if err != nil {
			log.Fatalf("Failed to read WAL: %v", err)
		}
		for _, rec := range walRecords {
			kvStore.SetWithHLCAndFlags(
				string(rec.Key), rec.HLC, rec.Value,
				rec.Flags.IsTombstone(), rec.Flags.IsBlobRef(),
			)
			recoveredRecords++
		}
	}

	if recoveredRecords > 0 {
		log.Printf("Recovered %d records from segments and WAL", recoveredRecords)
	}

	graphLayer := graph.New(kvStore, clock)

	metaRegistry := meta.NewRegistry()
	kvStore.SetMeta(metaRegistry)
	executor := query.NewExecutor(kvStore, graphLayer, metaRegistry, clock)

	// Truncate WAL after each segment rotation.
	segMgr.OnRotate(func() {
		if err := walWriter.Truncate(); err != nil {
			log.Printf("WAL truncation error: %v", err)
		} else {
			log.Printf("WAL truncated after segment rotation")
		}
	})

	// Start background compaction.
	compactor := segment.NewCompactor(segMgr, segment.DefaultCompactionConfig())
	go func() {
		ticker := time.NewTicker(compactInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopBg:
				return
			case <-ticker.C:
				if err := compactor.CheckAndCompact(); err != nil {
					log.Printf("Compaction error: %v", err)
				}
			}
		}
	}()

	srv := ngrpc.NewServer(kvStore, graphLayer,
		ngrpc.WithMetrics(m),
		ngrpc.WithQueryExecutor(executor),
		ngrpc.WithMetaRegistry(metaRegistry),
		ngrpc.WithSegmentManager(segMgr),
	)

	return srv, kvStore, graphLayer
}

// parsePeers parses a comma-separated list of id=host:port pairs.
func parsePeers(s string) map[string]string {
	if s == "" {
		return nil
	}
	m := make(map[string]string)
	for _, p := range strings.Split(s, ",") {
		parts := strings.SplitN(p, "=", 2)
		if len(parts) == 2 {
			m[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return m
}
