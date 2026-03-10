package grpc

import (
	"context"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/tenant"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GraphService implements the naladb.v1.GraphService gRPC service.
type GraphService struct {
	pb.UnimplementedGraphServiceServer
	graph     *graph.Graph    // single-tenant graph (nil in multi-tenant mode)
	tenantMgr *tenant.Manager // multi-tenant manager (nil in single-tenant mode)
}

// NewGraphService creates a new GraphService backed by the given graph layer.
func NewGraphService(g *graph.Graph) *GraphService {
	return &GraphService{graph: g}
}

// NewTenantGraphService creates a GraphService backed by a tenant manager.
func NewTenantGraphService(mgr *tenant.Manager) *GraphService {
	return &GraphService{tenantMgr: mgr}
}

// graphForRequest returns the graph instance for the current request.
func (s *GraphService) graphForRequest(ctx context.Context) *graph.Graph {
	if s.tenantMgr != nil {
		return s.tenantMgr.GraphForTenant(tenant.FromContext(ctx))
	}
	return s.graph
}

// CreateNode creates a new graph node.
func (s *GraphService) CreateNode(ctx context.Context, req *pb.CreateNodeRequest) (*pb.CreateNodeResponse, error) {
	if req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "node type must not be empty")
	}

	meta, err := s.graphForRequest(ctx).CreateNode(req.Type, req.Properties)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "graph create node: %v", err)
	}

	return &pb.CreateNodeResponse{
		Id:        meta.ID,
		ValidFrom: uint64(meta.ValidFrom),
		ValidTo:   uint64(meta.ValidTo),
	}, nil
}

// GetNode retrieves the current metadata for a node.
func (s *GraphService) GetNode(ctx context.Context, req *pb.GetNodeRequest) (*pb.GetNodeResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "node id must not be empty")
	}

	meta, err := s.graphForRequest(ctx).GetNode(req.Id)
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

// UpdateNode updates properties of a node.
func (s *GraphService) UpdateNode(ctx context.Context, req *pb.UpdateNodeRequest) (*pb.UpdateNodeResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "node id must not be empty")
	}

	if err := s.graphForRequest(ctx).UpdateNode(req.Id, req.Properties); err != nil {
		return nil, nodeError(err)
	}
	return &pb.UpdateNodeResponse{}, nil
}

// DeleteNode soft-deletes a node and its connected edges.
func (s *GraphService) DeleteNode(ctx context.Context, req *pb.DeleteNodeRequest) (*pb.DeleteNodeResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "node id must not be empty")
	}

	if err := s.graphForRequest(ctx).DeleteNode(req.Id); err != nil {
		return nil, nodeError(err)
	}
	return &pb.DeleteNodeResponse{}, nil
}

// CreateEdge creates an edge between two nodes.
func (s *GraphService) CreateEdge(ctx context.Context, req *pb.CreateEdgeRequest) (*pb.CreateEdgeResponse, error) {
	if req.From == "" || req.To == "" {
		return nil, status.Error(codes.InvalidArgument, "from and to must not be empty")
	}
	if req.Relation == "" {
		return nil, status.Error(codes.InvalidArgument, "relation must not be empty")
	}

	meta, err := s.graphForRequest(ctx).CreateEdge(
		req.From, req.To, req.Relation,
		hlc.HLC(req.ValidFrom), hlc.HLC(req.ValidTo),
		req.Properties,
	)
	if err != nil {
		return nil, edgeError(err)
	}

	return &pb.CreateEdgeResponse{
		Id:        meta.ID,
		ValidFrom: uint64(meta.ValidFrom),
		ValidTo:   uint64(meta.ValidTo),
	}, nil
}

// GetEdge retrieves the current metadata for an edge.
func (s *GraphService) GetEdge(ctx context.Context, req *pb.GetEdgeRequest) (*pb.GetEdgeResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "edge id must not be empty")
	}

	meta, err := s.graphForRequest(ctx).GetEdge(req.Id)
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
func (s *GraphService) UpdateEdge(ctx context.Context, req *pb.UpdateEdgeRequest) (*pb.UpdateEdgeResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "edge id must not be empty")
	}

	if err := s.graphForRequest(ctx).UpdateEdge(req.Id, req.Properties); err != nil {
		return nil, edgeError(err)
	}
	return &pb.UpdateEdgeResponse{}, nil
}

// DeleteEdge soft-deletes an edge.
func (s *GraphService) DeleteEdge(ctx context.Context, req *pb.DeleteEdgeRequest) (*pb.DeleteEdgeResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "edge id must not be empty")
	}

	if err := s.graphForRequest(ctx).DeleteEdge(req.Id); err != nil {
		return nil, edgeError(err)
	}
	return &pb.DeleteEdgeResponse{}, nil
}

// Traverse performs a BFS traversal with temporal filtering.
func (s *GraphService) Traverse(req *pb.TraverseRequest, stream pb.GraphService_TraverseServer) error {
	if req.Start == "" {
		return status.Error(codes.InvalidArgument, "start node id must not be empty")
	}
	g := s.graphForRequest(stream.Context())

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

// Causal performs a causal dependency traversal.
func (s *GraphService) Causal(req *pb.CausalRequest, stream pb.GraphService_CausalServer) error {
	if req.Trigger == "" {
		return status.Error(codes.InvalidArgument, "trigger node id must not be empty")
	}
	g := s.graphForRequest(stream.Context())

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

// toGraphDirection maps the proto Direction to the graph package Direction.
func toGraphDirection(d pb.Direction) graph.Direction {
	switch d {
	case pb.Direction_DIRECTION_INCOMING:
		return graph.Incoming
	case pb.Direction_DIRECTION_BOTH:
		return graph.Both
	default:
		return graph.Outgoing
	}
}

// nodeError maps graph node errors to gRPC status errors.
func nodeError(err error) error {
	switch {
	case err == nil:
		return nil
	case isNodeNotFound(err):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Errorf(codes.Internal, "graph: %v", err)
	}
}

// edgeError maps graph edge errors to gRPC status errors.
func edgeError(err error) error {
	switch {
	case err == nil:
		return nil
	case isEdgeNotFound(err):
		return status.Error(codes.NotFound, err.Error())
	case isEdgeOutsideValidity(err):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "graph: %v", err)
	}
}

func isNodeNotFound(err error) bool {
	return err.Error() == graph.ErrNodeNotFound.Error() || err.Error() == graph.ErrNodeDeleted.Error()
}

func isEdgeNotFound(err error) bool {
	return err.Error() == graph.ErrEdgeNotFound.Error()
}

func isEdgeOutsideValidity(err error) bool {
	return err.Error() == graph.ErrEdgeOutsideNodeValidity.Error()
}
