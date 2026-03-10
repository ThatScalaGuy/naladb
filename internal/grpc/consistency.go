package grpc

import (
	"context"
	"errors"
	"time"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	nraft "github.com/thatscalaguy/naladb/internal/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// DefaultMaxStale is the default maximum staleness for BOUNDED_STALE reads.
const DefaultMaxStale = 5 * time.Second

// ConsistencyInterceptor creates a gRPC unary server interceptor that:
//  1. Injects leader metadata into all responses.
//  2. Detects ErrNotLeader and transparently forwards to the leader.
func ConsistencyInterceptor(router *nraft.Router) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		router.SetLeaderMetadata(ctx)

		resp, err := handler(ctx, req)
		if err != nil && isNotLeaderError(err) {
			forwarded, fwdErr := forwardToLeader(ctx, router, info.FullMethod, req)
			if fwdErr != nil {
				return nil, fwdErr
			}
			return forwarded, nil
		}

		return resp, err
	}
}

// ConsistencyStreamInterceptor creates a gRPC stream server interceptor
// that injects leader metadata into stream responses.
func ConsistencyStreamInterceptor(router *nraft.Router) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		router.SetLeaderMetadata(ss.Context())
		return handler(srv, ss)
	}
}

// forwardToLeader transparently forwards a unary gRPC request to the
// current RAFT leader.
func forwardToLeader(
	ctx context.Context,
	router *nraft.Router,
	method string,
	req any,
) (any, error) {
	leaderAddr := router.LeaderEndpoint()
	if leaderAddr == "" {
		return nil, status.Error(codes.Unavailable, "no leader available")
	}

	conn, err := router.GetConn(leaderAddr)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "failed to connect to leader: %v", err)
	}

	resp := createResponseForMethod(method)
	if resp == nil {
		return nil, status.Errorf(codes.Internal, "unknown method for forwarding: %s", method)
	}

	if err := conn.Invoke(ctx, method, req, resp); err != nil {
		return nil, err
	}

	md := metadata.Pairs("x-naladb-leader", leaderAddr, "x-naladb-forwarded", "true")
	_ = grpc.SetHeader(ctx, md)

	return resp, nil
}

// createResponseForMethod returns a zero-value proto response for the given gRPC method.
func createResponseForMethod(method string) any {
	switch method {
	case "/naladb.v1.KVService/Set":
		return &pb.SetResponse{}
	case "/naladb.v1.KVService/Get":
		return &pb.GetResponse{}
	case "/naladb.v1.KVService/GetAt":
		return &pb.GetAtResponse{}
	case "/naladb.v1.KVService/Delete":
		return &pb.DeleteResponse{}
	case "/naladb.v1.GraphService/CreateNode":
		return &pb.CreateNodeResponse{}
	case "/naladb.v1.GraphService/GetNode":
		return &pb.GetNodeResponse{}
	case "/naladb.v1.GraphService/UpdateNode":
		return &pb.UpdateNodeResponse{}
	case "/naladb.v1.GraphService/DeleteNode":
		return &pb.DeleteNodeResponse{}
	case "/naladb.v1.GraphService/CreateEdge":
		return &pb.CreateEdgeResponse{}
	case "/naladb.v1.GraphService/GetEdge":
		return &pb.GetEdgeResponse{}
	case "/naladb.v1.GraphService/UpdateEdge":
		return &pb.UpdateEdgeResponse{}
	case "/naladb.v1.GraphService/DeleteEdge":
		return &pb.DeleteEdgeResponse{}
	default:
		return nil
	}
}

// isNotLeaderError checks whether an error indicates this node is not the RAFT leader.
func isNotLeaderError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, nraft.ErrNotLeader) ||
		status.Code(err) == codes.FailedPrecondition
}

// MapConsistencyLevel converts the protobuf ConsistencyLevel to the internal type.
func MapConsistencyLevel(pbLevel pb.ConsistencyLevel) nraft.ConsistencyLevel {
	switch pbLevel {
	case pb.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE:
		return nraft.Linearizable
	case pb.ConsistencyLevel_CONSISTENCY_LEVEL_BOUNDED_STALE:
		return nraft.BoundedStale
	default:
		return nraft.Eventual
	}
}

// mapRaftError converts RAFT-layer errors to appropriate gRPC status errors.
func mapRaftError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, nraft.ErrNotLeader) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return status.Errorf(codes.Internal, "%v", err)
}
