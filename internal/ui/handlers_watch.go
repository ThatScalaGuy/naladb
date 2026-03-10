package ui

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
)

func (s *UIServer) handleWatch(w http.ResponseWriter, r *http.Request) {
	keysParam := r.URL.Query().Get("keys")
	if keysParam == "" {
		writeJSONError(w, "keys parameter is required", http.StatusBadRequest)
		return
	}

	keys := strings.Split(keysParam, ",")
	for i := range keys {
		keys[i] = strings.TrimSpace(keys[i])
	}

	// Set SSE headers.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	stream, err := s.cfg.WatchClient.Watch(r.Context(), &pb.WatchRequest{Keys: keys})
	if err != nil {
		fmt.Fprintf(w, "data: {\"error\":%q}\n\n", err.Error())
		flusher.Flush()
		return
	}

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Client likely disconnected.
			return
		}

		data, _ := json.Marshal(map[string]any{
			"key":       event.Key,
			"value":     string(event.Value),
			"timestamp": fmt.Sprintf("%d", event.Timestamp),
			"deleted":   event.Deleted,
		})

		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}
}
