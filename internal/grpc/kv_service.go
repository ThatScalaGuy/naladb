package grpc

import (
	"context"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVService implements the naladb.v1.KVService gRPC service.
type KVService struct {
	pb.UnimplementedKVServiceServer
	store    *store.Store
	watchMgr *WatchManager
}

// NewKVService creates a new KVService backed by the given store.
func NewKVService(s *store.Store, wm *WatchManager) *KVService {
	return &KVService{store: s, watchMgr: wm}
}

// Set writes a key-value pair and returns the assigned HLC timestamp.
func (s *KVService) Set(_ context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	ts, err := s.store.Set(req.Key, req.Value)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "store set: %v", err)
	}

	// Notify watch subscribers.
	s.watchMgr.Notify(req.Key, req.Value, uint64(ts), false)

	return &pb.SetResponse{Timestamp: uint64(ts)}, nil
}

// Get reads the current value of a key.
func (s *KVService) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	// ConsistencyLevel is accepted but not enforced until RAFT is implemented.
	r := s.store.Get(req.Key)
	return &pb.GetResponse{
		Key:       req.Key,
		Value:     r.Value,
		Timestamp: uint64(r.HLC),
		Found:     r.Found,
	}, nil
}

// GetAt reads the value of a key at a specific point in time.
func (s *KVService) GetAt(_ context.Context, req *pb.GetAtRequest) (*pb.GetAtResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if req.At == 0 {
		return nil, status.Error(codes.InvalidArgument, "at timestamp must not be zero")
	}

	r := s.store.GetAt(req.Key, hlc.HLC(req.At))
	return &pb.GetAtResponse{
		Key:       req.Key,
		Value:     r.Value,
		Timestamp: uint64(r.HLC),
		Found:     r.Found,
	}, nil
}

// Delete marks a key as deleted (tombstone).
func (s *KVService) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	ts, err := s.store.Delete(req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "store delete: %v", err)
	}

	// Notify watch subscribers.
	s.watchMgr.Notify(req.Key, nil, uint64(ts), true)

	return &pb.DeleteResponse{Timestamp: uint64(ts)}, nil
}

// History streams the version history of a key.
func (s *KVService) History(req *pb.HistoryRequest, stream pb.KVService_HistoryServer) error {
	if req.Key == "" {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}

	opts := store.HistoryOptions{
		From:    hlc.HLC(req.From),
		To:      hlc.HLC(req.To),
		Limit:   int(req.Limit),
		Reverse: req.Reverse,
	}

	entries := s.store.History(req.Key, opts)
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
