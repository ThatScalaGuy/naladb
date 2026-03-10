package grpc

import (
	"context"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	nraft "github.com/thatscalaguy/naladb/internal/raft"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ClusterGraphService implements the naladb.v1.GraphService gRPC service backed
// by a RAFT cluster. Mutations go through RAFT consensus; reads are local.
type ClusterGraphService struct {
	pb.UnimplementedGraphServiceServer
	cluster *nraft.Cluster
}

// NewClusterGraphService creates a new cluster-aware GraphService.
func NewClusterGraphService(c *nraft.Cluster) *ClusterGraphService {
	return &ClusterGraphService{cluster: c}
}

// CreateNode creates a new graph node through RAFT consensus.
func (s *ClusterGraphService) CreateNode(_ context.Context, req *pb.CreateNodeRequest) (*pb.CreateNodeResponse, error) {
	if req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "node type must not be empty")
	}

	meta, err := s.cluster.CreateNode(req.Type, req.Properties)
	if err != nil {
		return nil, mapRaftError(err)
	}

	return &pb.CreateNodeResponse{
		Id:        meta.ID,
		ValidFrom: uint64(meta.ValidFrom),
		ValidTo:   uint64(meta.ValidTo),
	}, nil
}

// GetNode retrieves the current metadata for a node (local read).
func (s *ClusterGraphService) GetNode(_ context.Context, req *pb.GetNodeRequest) (*pb.GetNodeResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "node id must not be empty")
	}

	meta, err := s.cluster.Graph().GetNode(req.Id)
	if err != nil {
		return nil, nodeError(err)
	}

	return &pb.GetNodeResponse{
		Id:        meta.ID,
		Type:      meta.Type,
		ValidFrom: uint64(meta.ValidFrom),
		ValidTo:   uint64(meta.ValidTo),
		Deleted:   meta.Deleted,
	}, nil
}

// UpdateNode updates properties of a node through RAFT consensus.
func (s *ClusterGraphService) UpdateNode(_ context.Context, req *pb.UpdateNodeRequest) (*pb.UpdateNodeResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "node id must not be empty")
	}

	// UpdateNode goes through RAFT via batch set of the property keys.
	if err := s.cluster.Graph().UpdateNode(req.Id, req.Properties); err != nil {
		return nil, nodeError(err)
	}
	return &pb.UpdateNodeResponse{}, nil
}

// DeleteNode soft-deletes a node and its connected edges through RAFT consensus.
func (s *ClusterGraphService) DeleteNode(_ context.Context, req *pb.DeleteNodeRequest) (*pb.DeleteNodeResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "node id must not be empty")
	}

	if err := s.cluster.Graph().DeleteNode(req.Id); err != nil {
		return nil, nodeError(err)
	}
	return &pb.DeleteNodeResponse{}, nil
}

// CreateEdge creates an edge between two nodes through RAFT consensus.
func (s *ClusterGraphService) CreateEdge(_ context.Context, req *pb.CreateEdgeRequest) (*pb.CreateEdgeResponse, error) {
	if req.From == "" || req.To == "" {
		return nil, status.Error(codes.InvalidArgument, "from and to must not be empty")
	}
	if req.Relation == "" {
		return nil, status.Error(codes.InvalidArgument, "relation must not be empty")
	}

	meta, err := s.cluster.CreateEdge(
		req.From, req.To, req.Relation,
		hlc.HLC(req.ValidFrom), hlc.HLC(req.ValidTo),
		req.Properties,
	)
	if err != nil {
		return nil, mapRaftError(err)
	}

	return &pb.CreateEdgeResponse{
		Id:        meta.ID,
		ValidFrom: uint64(meta.ValidFrom),
		ValidTo:   uint64(meta.ValidTo),
	}, nil
}

// GetEdge retrieves the current metadata for an edge (local read).
func (s *ClusterGraphService) GetEdge(_ context.Context, req *pb.GetEdgeRequest) (*pb.GetEdgeResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "edge id must not be empty")
	}

	meta, err := s.cluster.Graph().GetEdge(req.Id)
	if err != nil {
		return nil, edgeError(err)
	}

	return &pb.GetEdgeResponse{
		Id:        meta.ID,
		From:      meta.From,
		To:        meta.To,
		Relation:  meta.Relation,
		ValidFrom: uint64(meta.ValidFrom),
		ValidTo:   uint64(meta.ValidTo),
		Deleted:   meta.Deleted,
	}, nil
}

// UpdateEdge updates properties of an edge.
func (s *ClusterGraphService) UpdateEdge(_ context.Context, req *pb.UpdateEdgeRequest) (*pb.UpdateEdgeResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "edge id must not be empty")
	}

	if err := s.cluster.Graph().UpdateEdge(req.Id, req.Properties); err != nil {
		return nil, edgeError(err)
	}
	return &pb.UpdateEdgeResponse{}, nil
}

// DeleteEdge soft-deletes an edge.
func (s *ClusterGraphService) DeleteEdge(_ context.Context, req *pb.DeleteEdgeRequest) (*pb.DeleteEdgeResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "edge id must not be empty")
	}

	if err := s.cluster.Graph().DeleteEdge(req.Id); err != nil {
		return nil, edgeError(err)
	}
	return &pb.DeleteEdgeResponse{}, nil
}

// Traverse performs a BFS traversal with temporal filtering (local read).
func (s *ClusterGraphService) Traverse(req *pb.TraverseRequest, stream pb.GraphService_TraverseServer) error {
	if req.Start == "" {
		return status.Error(codes.InvalidArgument, "start node id must not be empty")
	}
	g := s.cluster.Graph()

	at := hlc.HLC(req.At)
	if at == 0 {
		at = g.Now()
	}

	opts := graph.TraversalOption{
		At:                    at,
		MaxDepth:              int(req.MaxDepth),
		Direction:             toGraphDirection(req.Direction),
		RelationFilter:        req.RelationFilter,
		IncludeNodeProperties: req.IncludeProperties,
	}

	results, err := g.TraverseAt(req.Start, opts)
	if err != nil {
		return status.Errorf(codes.Internal, "graph traverse: %v", err)
	}

	for _, r := range results {
		if err := stream.Send(&pb.TraverseResult{
			NodeId:      r.NodeID,
			Depth:       int32(r.Depth),
			ViaEdge:     r.ViaEdge,
			ViaRelation: r.ViaRelation,
			Properties:  r.Properties,
		}); err != nil {
			return err
		}
	}

	return nil
}

// Causal performs a causal dependency traversal (local read).
func (s *ClusterGraphService) Causal(req *pb.CausalRequest, stream pb.GraphService_CausalServer) error {
	if req.Trigger == "" {
		return status.Error(codes.InvalidArgument, "trigger node id must not be empty")
	}
	g := s.cluster.Graph()

	at := hlc.HLC(req.At)
	if at == 0 {
		at = g.Now()
	}

	query := graph.CausalQuery{
		TriggerNodeID:  req.Trigger,
		At:             at,
		MaxDepth:       int(req.MaxDepth),
		WindowMicros:   req.WindowMicros,
		MinConfidence:  req.MinConfidence,
		RelationFilter: req.RelationFilter,
	}

	if req.Direction == pb.CausalDirection_CAUSAL_DIRECTION_BACKWARD {
		query.Direction = graph.CausalBackward
	} else {
		query.Direction = graph.CausalForward
	}

	results, err := g.CausalTraverse(query)
	if err != nil {
		return status.Errorf(codes.Internal, "graph causal: %v", err)
	}

	for _, r := range results {
		if err := stream.Send(&pb.CausalResult{
			NodeId:      r.NodeID,
			Depth:       int32(r.Depth),
			DeltaMicros: r.DeltaMicros,
			Confidence:  r.Confidence,
			CausalPath:  r.Path,
			ViaEdge:     r.ViaEdge,
			ViaRelation: r.ViaRelation,
			ChangeTime:  uint64(r.ChangeTime),
		}); err != nil {
			return err
		}
	}

	return nil
}
