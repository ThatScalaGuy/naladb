package tenant

import (
	"context"
	"strings"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const metadataKeyTenantID = "x-tenant-id"

// UnaryInterceptor returns a gRPC unary server interceptor that extracts
// the tenant ID from metadata, prefixes KV keys, and enforces quotas and
// rate limits.
func UnaryInterceptor(mgr *Manager) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		tenantID := extractTenantID(ctx)
		ctx = WithTenantID(ctx, tenantID)
		prefix := KeyPrefix(tenantID)

		// Rate limiting for write operations.
		if isWriteMethod(info.FullMethod) {
			if err := mgr.LimiterForTenant(tenantID).Allow(); err != nil {
				return nil, status.Error(codes.ResourceExhausted, err.Error())
			}
		}

		// Quota check for node/edge creation.
		qt := mgr.QuotaForTenant(tenantID)
		if isCreateNodeMethod(info.FullMethod) {
			if err := qt.CheckNode(); err != nil {
				return nil, status.Error(codes.ResourceExhausted, err.Error())
			}
		}
		if isCreateEdgeMethod(info.FullMethod) {
			if err := qt.CheckEdge(); err != nil {
				return nil, status.Error(codes.ResourceExhausted, err.Error())
			}
		}

		// Prefix KV keys in request.
		prefixRequest(req, prefix)

		resp, err := handler(ctx, req)

		// Track quota on successful creation.
		if err == nil {
			if isCreateNodeMethod(info.FullMethod) {
				qt.IncrementNodes()
			}
			if isCreateEdgeMethod(info.FullMethod) {
				qt.IncrementEdges()
			}
		}

		// Strip prefix from response keys.
		stripResponse(resp, prefix)

		return resp, err
	}
}

// StreamInterceptor returns a gRPC stream server interceptor that extracts
// the tenant ID and prefixes keys in streaming requests/responses.
func StreamInterceptor(mgr *Manager) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		tenantID := extractTenantID(ss.Context())
		ctx := WithTenantID(ss.Context(), tenantID)
		prefix := KeyPrefix(tenantID)

		wrapped := &tenantStream{
			ServerStream: ss,
			ctx:          ctx,
			prefix:       prefix,
		}
		return handler(srv, wrapped)
	}
}

// tenantStream wraps a grpc.ServerStream to prefix/strip tenant keys
// in streaming messages.
type tenantStream struct {
	grpc.ServerStream
	ctx    context.Context
	prefix string
}

// Context returns the wrapped context with tenant ID.
func (s *tenantStream) Context() context.Context {
	return s.ctx
}

// RecvMsg intercepts incoming messages to prefix KV keys.
func (s *tenantStream) RecvMsg(m interface{}) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}

	switch r := m.(type) {
	case *pb.HistoryRequest:
		r.Key = s.prefix + r.Key
	case *pb.WatchRequest:
		for i, k := range r.Keys {
			r.Keys[i] = s.prefix + k
		}
	}
	// TraverseRequest and CausalRequest use node IDs,
	// not KV keys. The graph layer handles prefixing internally.
	return nil
}

// SendMsg intercepts outgoing messages to strip tenant prefix from keys.
func (s *tenantStream) SendMsg(m interface{}) error {
	switch r := m.(type) {
	case *pb.WatchEvent:
		r.Key = strings.TrimPrefix(r.Key, s.prefix)
	}
	return s.ServerStream.SendMsg(m)
}

// extractTenantID reads the tenant ID from gRPC metadata.
// Falls back to DefaultTenantID if not present.
func extractTenantID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return DefaultTenantID
	}
	values := md.Get(metadataKeyTenantID)
	if len(values) == 0 || values[0] == "" {
		return DefaultTenantID
	}
	return values[0]
}

// prefixRequest adds the tenant prefix to KV key fields in the request.
func prefixRequest(req any, prefix string) {
	switch r := req.(type) {
	case *pb.SetRequest:
		r.Key = prefix + r.Key
	case *pb.GetRequest:
		r.Key = prefix + r.Key
	case *pb.GetAtRequest:
		r.Key = prefix + r.Key
	case *pb.DeleteRequest:
		r.Key = prefix + r.Key
	}
	// Graph requests (CreateNode, GetNode, etc.) don't need key prefixing
	// here — the graph layer handles prefixing via per-tenant Graph instances.
}

// stripResponse removes the tenant prefix from KV key fields in the response.
func stripResponse(resp any, prefix string) {
	if resp == nil {
		return
	}
	switch r := resp.(type) {
	case *pb.GetResponse:
		r.Key = strings.TrimPrefix(r.Key, prefix)
	case *pb.GetAtResponse:
		r.Key = strings.TrimPrefix(r.Key, prefix)
	}
}

func isWriteMethod(method string) bool {
	return method == "/naladb.v1.KVService/Set" ||
		method == "/naladb.v1.KVService/Delete" ||
		method == "/naladb.v1.GraphService/CreateNode" ||
		method == "/naladb.v1.GraphService/UpdateNode" ||
		method == "/naladb.v1.GraphService/DeleteNode" ||
		method == "/naladb.v1.GraphService/CreateEdge" ||
		method == "/naladb.v1.GraphService/UpdateEdge" ||
		method == "/naladb.v1.GraphService/DeleteEdge"
}

func isCreateNodeMethod(method string) bool {
	return method == "/naladb.v1.GraphService/CreateNode"
}

func isCreateEdgeMethod(method string) bool {
	return method == "/naladb.v1.GraphService/CreateEdge"
}
