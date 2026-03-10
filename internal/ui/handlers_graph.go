package ui

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"github.com/thatscalaguy/naladb/internal/hlc"
)

func (s *UIServer) handleGraphNodes(w http.ResponseWriter, r *http.Request) {
	q := "DESCRIBE NODES"
	if at := r.URL.Query().Get("at"); at != "" {
		q += ` AT "` + at + `"`
	}
	if typ := r.URL.Query().Get("type"); typ != "" {
		q += ` WHERE type = "` + typ + `"`
	}
	limit := r.URL.Query().Get("limit")
	if limit == "" {
		limit = "200"
	}
	q += " LIMIT " + limit

	rows, columns, err := s.executeQuery(r, q)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"columns": columns,
		"rows":    rows,
	})
}

func (s *UIServer) handleGraphEdges(w http.ResponseWriter, r *http.Request) {
	q := "DESCRIBE EDGES"
	if at := r.URL.Query().Get("at"); at != "" {
		q += ` AT "` + at + `"`
	}
	if rel := r.URL.Query().Get("relation"); rel != "" {
		q += ` WHERE relation = "` + rel + `"`
	}
	limit := r.URL.Query().Get("limit")
	if limit == "" {
		limit = "500"
	}
	q += " LIMIT " + limit

	rows, columns, err := s.executeQuery(r, q)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"columns": columns,
		"rows":    rows,
	})
}

func (s *UIServer) handleGraphTraverse(w http.ResponseWriter, r *http.Request) {
	startNode := r.URL.Query().Get("start")
	if startNode == "" {
		writeJSONError(w, "start parameter is required", http.StatusBadRequest)
		return
	}

	depth := int32(3)
	if d := r.URL.Query().Get("depth"); d != "" {
		if v, err := strconv.Atoi(d); err == nil {
			depth = int32(v)
		}
	}

	direction := pb.Direction_DIRECTION_OUTGOING
	switch r.URL.Query().Get("direction") {
	case "in":
		direction = pb.Direction_DIRECTION_INCOMING
	case "both":
		direction = pb.Direction_DIRECTION_BOTH
	}

	var at uint64
	if atStr := r.URL.Query().Get("at"); atStr != "" {
		at = parseRFC3339ToHLC(atStr)
	}

	req := &pb.TraverseRequest{
		Start:     startNode,
		At:        at,
		MaxDepth:  depth,
		Direction: direction,
	}

	stream, err := s.cfg.GraphClient.Traverse(r.Context(), req)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var results []map[string]any
	for {
		result, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeJSONError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		results = append(results, map[string]any{
			"node_id":      result.NodeId,
			"depth":        result.Depth,
			"via_edge":     result.ViaEdge,
			"via_relation": result.ViaRelation,
		})
	}

	if results == nil {
		results = []map[string]any{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
}

// executeQuery runs a NalaQL query and returns rows and columns.
func (s *UIServer) executeQuery(r *http.Request, q string) ([]map[string]string, []string, error) {
	stream, err := s.cfg.QueryClient.Query(r.Context(), &pb.QueryRequest{Query: q})
	if err != nil {
		return nil, nil, err
	}

	var rows []map[string]string
	columnSet := make(map[string]bool)
	var columns []string

	for {
		row, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, row.Columns)
		for k := range row.Columns {
			if !columnSet[k] {
				columnSet[k] = true
				columns = append(columns, k)
			}
		}
	}

	if rows == nil {
		rows = []map[string]string{}
	}
	return rows, columns, nil
}

// parseRFC3339ToHLC converts an RFC3339 timestamp string to an HLC uint64.
// HLC format: upper 48 bits = wall clock µs since NalaDB epoch, lower 16 bits = node+logical.
func parseRFC3339ToHLC(s string) uint64 {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0
	}
	micros := t.UnixMicro() - hlc.Epoch
	if micros < 0 {
		return 0
	}
	return uint64(micros) << 16
}
