package grpc

import (
	"context"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/meta"
	"github.com/thatscalaguy/naladb/internal/segment"
	"github.com/thatscalaguy/naladb/internal/store"
)

// StatsService implements the naladb.v1.StatsService gRPC service.
type StatsService struct {
	pb.UnimplementedStatsServiceServer
	store  *store.Store
	graph  *graph.Graph
	meta   *meta.Registry
	segMgr *segment.Manager
}

// NewStatsService creates a new StatsService.
func NewStatsService(s *store.Store, g *graph.Graph, m *meta.Registry, sm *segment.Manager) *StatsService {
	return &StatsService{
		store:  s,
		graph:  g,
		meta:   m,
		segMgr: sm,
	}
}

// GetStats returns an overview of database statistics.
func (s *StatsService) GetStats(_ context.Context, _ *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {
	storeSt := s.store.Stats()
	graphSt := s.graph.Stats()

	resp := &pb.GetStatsResponse{
		TotalKeys:     int64(storeSt.Keys),
		TotalVersions: int64(storeSt.Versions),
		Tombstones:    int64(storeSt.Tombstones),

		NodesTotal:   int64(graphSt.Nodes),
		NodesActive:  int64(graphSt.ActiveNodes),
		NodesDeleted: int64(graphSt.DeletedNodes),
		EdgesTotal:   int64(graphSt.Edges),
		EdgesActive:  int64(graphSt.ActiveEdges),
		EdgesDeleted: int64(graphSt.DeletedEdges),
	}

	if s.segMgr != nil {
		segs := s.segMgr.Segments()
		resp.Segments = int64(len(segs))
		var totalBytes int64
		for _, seg := range segs {
			totalBytes += seg.Meta.SizeBytes
		}
		resp.SegmentBytes = totalBytes
	}

	if len(graphSt.NodesByType) > 0 {
		resp.NodesByType = make(map[string]int64, len(graphSt.NodesByType))
		for t, c := range graphSt.NodesByType {
			resp.NodesByType[t] = int64(c)
		}
	}

	if len(graphSt.EdgesByRel) > 0 {
		resp.EdgesByRelation = make(map[string]int64, len(graphSt.EdgesByRel))
		for r, c := range graphSt.EdgesByRel {
			resp.EdgesByRelation[r] = int64(c)
		}
	}

	return resp, nil
}

// GetKeyStats returns per-key inline statistics from the KeyMeta registry.
func (s *StatsService) GetKeyStats(_ context.Context, req *pb.GetKeyStatsRequest) (*pb.GetKeyStatsResponse, error) {
	km := s.meta.Get(req.Key)
	if km == nil {
		return &pb.GetKeyStatsResponse{Key: req.Key, Found: false}, nil
	}

	return &pb.GetKeyStatsResponse{
		Key:         km.Key,
		Found:       true,
		TotalWrites: km.TotalWrites,
		FirstSeenUs: km.FirstSeenUs,
		LastSeenUs:  km.LastSeenUs,
		WriteRateHz: km.WriteRateHz,
		MinValue:    km.MinValue,
		MaxValue:    km.MaxValue,
		AvgValue:    km.AvgValue,
		StddevValue: km.StdDevValue,
		Cardinality: km.Cardinality,
		SizeBytes:   km.SizeBytes,
	}, nil
}
