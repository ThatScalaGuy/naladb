package grpc

import (
	"fmt"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"github.com/thatscalaguy/naladb/internal/query"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// QueryService implements the naladb.v1.QueryService gRPC service.
type QueryService struct {
	pb.UnimplementedQueryServiceServer
	executor *query.Executor
}

// NewQueryService creates a new QueryService.
func NewQueryService(exec *query.Executor) *QueryService {
	return &QueryService{executor: exec}
}

// Query executes a NalaQL query and streams result rows to the client.
func (s *QueryService) Query(req *pb.QueryRequest, stream pb.QueryService_QueryServer) error {
	if req.Query == "" {
		return status.Error(codes.InvalidArgument, "query must not be empty")
	}

	rows, err := s.executor.Execute(req.Query)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "query: %v", err)
	}

	for _, row := range rows {
		cols := make(map[string]string, len(row))
		for k, v := range row {
			cols[k] = fmt.Sprintf("%v", v)
		}
		if err := stream.Send(&pb.QueryRow{Columns: cols}); err != nil {
			return err
		}
	}

	return nil
}
