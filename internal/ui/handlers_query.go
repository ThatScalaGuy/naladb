package ui

import (
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"time"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
)

type queryRequest struct {
	Query string `json:"query"`
}

type queryResponse struct {
	Columns   []string            `json:"columns"`
	Rows      []map[string]string `json:"rows"`
	ElapsedMs int64               `json:"elapsed_ms"`
}

func (s *UIServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}
	if req.Query == "" {
		http.Error(w, `{"error":"query is required"}`, http.StatusBadRequest)
		return
	}

	start := time.Now()
	stream, err := s.cfg.QueryClient.Query(r.Context(), &pb.QueryRequest{Query: req.Query})
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
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
			writeJSONError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rows = append(rows, row.Columns)
		for k := range row.Columns {
			if !columnSet[k] {
				columnSet[k] = true
				columns = append(columns, k)
			}
		}
	}
	sort.Strings(columns)

	if rows == nil {
		rows = []map[string]string{}
	}

	resp := queryResponse{
		Columns:   columns,
		Rows:      rows,
		ElapsedMs: time.Since(start).Milliseconds(),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func writeJSONError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
