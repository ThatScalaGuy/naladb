package grpc

import (
	"sync"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WatchManager distributes key change events to active Watch subscribers.
type WatchManager struct {
	mu   sync.RWMutex
	subs map[uint64]*subscription
	next uint64
}

type subscription struct {
	keys map[string]struct{}
	ch   chan *pb.WatchEvent
}

// NewWatchManager creates a new WatchManager.
func NewWatchManager() *WatchManager {
	return &WatchManager{
		subs: make(map[uint64]*subscription),
	}
}

// Subscribe registers a new subscriber for the given keys and returns a
// channel that receives WatchEvents. Call Unsubscribe with the returned
// ID to clean up.
func (wm *WatchManager) Subscribe(keys []string) (uint64, <-chan *pb.WatchEvent) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	id := wm.next
	wm.next++

	keySet := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}

	ch := make(chan *pb.WatchEvent, 64)
	wm.subs[id] = &subscription{keys: keySet, ch: ch}
	return id, ch
}

// Unsubscribe removes a subscriber and closes its channel.
func (wm *WatchManager) Unsubscribe(id uint64) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if sub, ok := wm.subs[id]; ok {
		close(sub.ch)
		delete(wm.subs, id)
	}
}

// Notify broadcasts a key change to all matching subscribers.
func (wm *WatchManager) Notify(key string, value []byte, timestamp uint64, deleted bool) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	event := &pb.WatchEvent{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Deleted:   deleted,
	}

	for _, sub := range wm.subs {
		if _, ok := sub.keys[key]; ok {
			select {
			case sub.ch <- event:
			default:
				// Drop event if subscriber is too slow.
			}
		}
	}
}

// Close closes all active subscriptions.
func (wm *WatchManager) Close() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for id, sub := range wm.subs {
		close(sub.ch)
		delete(wm.subs, id)
	}
}

// WatchService implements the naladb.v1.WatchService gRPC service.
type WatchService struct {
	pb.UnimplementedWatchServiceServer
	mgr *WatchManager
}

// NewWatchService creates a new WatchService.
func NewWatchService(mgr *WatchManager) *WatchService {
	return &WatchService{mgr: mgr}
}

// Watch subscribes to live updates on specified keys.
func (s *WatchService) Watch(req *pb.WatchRequest, stream pb.WatchService_WatchServer) error {
	if len(req.Keys) == 0 {
		return status.Error(codes.InvalidArgument, "keys must not be empty")
	}

	id, ch := s.mgr.Subscribe(req.Keys)
	defer s.mgr.Unsubscribe(id)

	for {
		select {
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// Close closes the watch manager, terminating all active subscriptions.
func (s *WatchService) Close() {
	s.mgr.Close()
}
