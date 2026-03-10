package ui

import (
	"encoding/json"
	"net/http"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
)

func (s *UIServer) handleStats(w http.ResponseWriter, r *http.Request) {
	resp, err := s.cfg.StatsClient.GetStats(r.Context(), &pb.GetStatsRequest{})
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result := map[string]any{
		"total_keys":        resp.TotalKeys,
		"total_versions":    resp.TotalVersions,
		"tombstones":        resp.Tombstones,
		"segments":          resp.Segments,
		"segment_bytes":     resp.SegmentBytes,
		"nodes_total":       resp.NodesTotal,
		"nodes_active":      resp.NodesActive,
		"nodes_deleted":     resp.NodesDeleted,
		"edges_total":       resp.EdgesTotal,
		"edges_active":      resp.EdgesActive,
		"edges_deleted":     resp.EdgesDeleted,
		"nodes_by_type":     resp.NodesByType,
		"edges_by_relation": resp.EdgesByRelation,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
