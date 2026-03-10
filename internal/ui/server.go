package ui

import (
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os/exec"
	"runtime"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
)

//go:embed static
var staticFS embed.FS

// Config holds the configuration for the UI server.
type Config struct {
	QueryClient pb.QueryServiceClient
	GraphClient pb.GraphServiceClient
	WatchClient pb.WatchServiceClient
	StatsClient pb.StatsServiceClient
	Port        int
	ServerAddr  string
}

// UIServer is the embedded HTTP server for the web UI.
type UIServer struct {
	cfg  Config
	mux  *http.ServeMux
	addr string
}

// NewServer creates a new UI server with the given configuration.
func NewServer(cfg Config) *UIServer {
	s := &UIServer{
		cfg:  cfg,
		mux:  http.NewServeMux(),
		addr: fmt.Sprintf(":%d", cfg.Port),
	}
	s.routes()
	return s
}

func (s *UIServer) routes() {
	// API routes.
	s.mux.HandleFunc("POST /api/query", s.handleQuery)
	s.mux.HandleFunc("GET /api/watch", s.handleWatch)
	s.mux.HandleFunc("GET /api/graph/nodes", s.handleGraphNodes)
	s.mux.HandleFunc("GET /api/graph/edges", s.handleGraphEdges)
	s.mux.HandleFunc("GET /api/graph/traverse", s.handleGraphTraverse)
	s.mux.HandleFunc("GET /api/stats", s.handleStats)

	// Static file serving.
	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("failed to create sub filesystem: %v", err)
	}
	fileServer := http.FileServer(http.FS(staticSub))

	// Serve index.html directly for root to avoid FileServer's redirect loop
	// (FileServer redirects /index.html → / which would loop back).
	s.mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		index, err := fs.ReadFile(staticSub, "index.html")
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(index)
	})

	// All other static files.
	s.mux.Handle("GET /", fileServer)
}

// ListenAndServe starts the HTTP server and opens the browser.
func (s *UIServer) ListenAndServe() error {
	url := fmt.Sprintf("http://localhost:%d", s.cfg.Port)
	log.Printf("NalaDB Web UI starting at %s (connected to %s)", url, s.cfg.ServerAddr)

	// Open browser in background.
	go openBrowser(url)

	return http.ListenAndServe(s.addr, s.mux)
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "linux":
		cmd = exec.Command("xdg-open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		return
	}
	_ = cmd.Start()
}
