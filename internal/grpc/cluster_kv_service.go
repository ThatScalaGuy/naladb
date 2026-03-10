package grpc

import (
	"context"
	"time"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"github.com/thatscalaguy/naladb/internal/hlc"
	nraft "github.com/thatscalaguy/naladb/internal/raft"
	"github.com/thatscalaguy/naladb/internal/store"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ClusterKVService implements the naladb.v1.KVService gRPC service backed by
// a RAFT cluster. Writes go through RAFT consensus; reads respect consistency levels.
type ClusterKVService struct {
	pb.UnimplementedKVServiceServer
	cluster  *nraft.Cluster
	watchMgr *WatchManager
}

// NewClusterKVService creates a new cluster-aware KVService.
func NewClusterKVService(c *nraft.Cluster, wm *WatchManager) *ClusterKVService {
	return &ClusterKVService{cluster: c, watchMgr: wm}
}

// Set writes a key-value pair through RAFT consensus.
func (s *ClusterKVService) Set(_ context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	ts, err := s.cluster.Set(req.Key, req.Value)
	if err != nil {
		return nil, mapRaftError(err)
	}

	s.watchMgr.Notify(req.Key, req.Value, uint64(ts), false)
	return &pb.SetResponse{Timestamp: uint64(ts)}, nil
}

// Get reads a value with the specified consistency level.
func (s *ClusterKVService) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	consistency := MapConsistencyLevel(req.Consistency)
	var result store.Result
	var err error

	switch consistency {
	case nraft.Linearizable:
		result, err = s.cluster.ReadIndex(req.Key)
	case nraft.BoundedStale:
		maxStale := DefaultMaxStale
		if req.MaxStaleMs > 0 {
			maxStale = time.Duration(req.MaxStaleMs) * time.Millisecond
		}
		result, err = s.cluster.GetWithMaxStale(req.Key, maxStale)
	default:
		result, err = s.cluster.Get(req.Key, nraft.Eventual)
	}

	if err != nil {
		return nil, mapRaftError(err)
	}

	return &pb.GetResponse{
		Key:       req.Key,
		Value:     result.Value,
		Timestamp: uint64(result.HLC),
		Found:     result.Found,
	}, nil
}

// GetAt reads the value of a key at a specific point in time.
func (s *ClusterKVService) GetAt(_ context.Context, req *pb.GetAtRequest) (*pb.GetAtResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if req.At == 0 {
		return nil, status.Error(codes.InvalidArgument, "at timestamp must not be zero")
	}

	r := s.cluster.Store().GetAt(req.Key, hlc.HLC(req.At))
	return &pb.GetAtResponse{
		Key:       req.Key,
		Value:     r.Value,
		Timestamp: uint64(r.HLC),
		Found:     r.Found,
	}, nil
}

// Delete marks a key as deleted through RAFT consensus.
func (s *ClusterKVService) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	ts, err := s.cluster.Delete(req.Key)
	if err != nil {
		return nil, mapRaftError(err)
	}

	s.watchMgr.Notify(req.Key, nil, uint64(ts), true)
	return &pb.DeleteResponse{Timestamp: uint64(ts)}, nil
}

// History streams the version history of a key.
func (s *ClusterKVService) History(req *pb.HistoryRequest, stream pb.KVService_HistoryServer) error {
	if req.Key == "" {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}

	opts := store.HistoryOptions{
		From:    hlc.HLC(req.From),
		To:      hlc.HLC(req.To),
		Limit:   int(req.Limit),
		Reverse: req.Reverse,
	}

	entries := s.cluster.Store().History(req.Key, opts)
	for _, e := range entries {
		if err := stream.Send(&pb.HistoryEntry{
			Timestamp: uint64(e.HLC),
			Value:     e.Value,
			Tombstone: e.Tombstone,
		}); err != nil {
			return err
		}
	}

	return nil
}
