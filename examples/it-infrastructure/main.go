// Package main demonstrates NalaDB's cascading failure analysis for IT infrastructure.
//
// This example models a microservice mesh, simulates a cascading failure starting
// from a database outage, and uses CAUSAL + TRAVERSE queries to perform automated
// root cause analysis -- the core value proposition of NalaDB for SRE teams.
//
// Prerequisites: a running NalaDB server on localhost:7301.
//
// Usage:
//
//	go run ./examples/it-infrastructure/
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
)

// service describes a node in the infrastructure topology.
type service struct {
	name       string
	nodeType   string
	properties map[string][]byte
}

func main() {
	// ---------------------------------------------------------------
	// 1. Connect to NalaDB
	// ---------------------------------------------------------------
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.NewClient("localhost:7301",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	kv := pb.NewKVServiceClient(conn)
	graph := pb.NewGraphServiceClient(conn)

	fmt.Println("=== NalaDB IT Infrastructure: Cascading Failure Analysis ===")
	fmt.Println()

	// ---------------------------------------------------------------
	// 2. Create service mesh nodes
	// ---------------------------------------------------------------
	services := []service{
		{"api-gateway", "gateway", map[string][]byte{"tier": []byte("edge"), "region": []byte("us-east-1")}},
		{"auth-service", "microservice", map[string][]byte{"tier": []byte("platform"), "region": []byte("us-east-1")}},
		{"user-service", "microservice", map[string][]byte{"tier": []byte("domain"), "region": []byte("us-east-1")}},
		{"order-service", "microservice", map[string][]byte{"tier": []byte("domain"), "region": []byte("us-east-1")}},
		{"postgres-primary", "database", map[string][]byte{"tier": []byte("data"), "engine": []byte("postgresql-15")}},
		{"redis-cache", "cache", map[string][]byte{"tier": []byte("data"), "engine": []byte("redis-7")}},
	}

	nodeIDs := make(map[string]string) // name -> NalaDB node ID

	fmt.Println("[1] Creating service mesh nodes ...")
	for _, svc := range services {
		resp, err := graph.CreateNode(ctx, &pb.CreateNodeRequest{
			Type:       svc.nodeType,
			Properties: svc.properties,
		})
		if err != nil {
			log.Fatalf("  CreateNode(%s): %v", svc.name, err)
		}
		nodeIDs[svc.name] = resp.Id
		fmt.Printf("    + %-20s  id=%s\n", svc.name, resp.Id)
	}
	fmt.Println()

	// ---------------------------------------------------------------
	// 3. Create DEPENDS_ON edges (downstream -> upstream)
	// ---------------------------------------------------------------
	edges := [][2]string{
		{"api-gateway", "auth-service"},
		{"api-gateway", "order-service"},
		{"auth-service", "redis-cache"},
		{"auth-service", "postgres-primary"},
		{"user-service", "postgres-primary"},
		{"user-service", "redis-cache"},
		{"order-service", "user-service"},
		{"order-service", "postgres-primary"},
	}

	fmt.Println("[2] Creating DEPENDS_ON edges ...")
	for _, e := range edges {
		resp, err := graph.CreateEdge(ctx, &pb.CreateEdgeRequest{
			From:     nodeIDs[e[0]],
			To:       nodeIDs[e[1]],
			Relation: "DEPENDS_ON",
		})
		if err != nil {
			log.Fatalf("  CreateEdge(%s -> %s): %v", e[0], e[1], err)
		}
		fmt.Printf("    %s --DEPENDS_ON--> %s  (edge=%s)\n", e[0], e[1], resp.Id)
	}
	fmt.Println()

	// ---------------------------------------------------------------
	// 4. Simulate cascading failure: postgres-primary goes down first,
	//    then dependent services degrade and fail over ~3 seconds.
	// ---------------------------------------------------------------
	fmt.Println("[3] Simulating cascading failure ...")

	type statusChange struct {
		name   string
		status string
		delay  time.Duration
	}

	timeline := []statusChange{
		{"postgres-primary", "healthy", 0},
		{"redis-cache", "healthy", 0},
		{"auth-service", "healthy", 0},
		{"user-service", "healthy", 0},
		{"order-service", "healthy", 0},
		{"api-gateway", "healthy", 0},
		// Incident begins: postgres primary starts failing
		{"postgres-primary", "degraded", 500 * time.Millisecond},
		{"postgres-primary", "down", 300 * time.Millisecond},
		// Cascade propagates
		{"auth-service", "degraded", 200 * time.Millisecond},
		{"user-service", "degraded", 100 * time.Millisecond},
		{"order-service", "degraded", 200 * time.Millisecond},
		{"auth-service", "down", 400 * time.Millisecond},
		{"order-service", "down", 200 * time.Millisecond},
		{"api-gateway", "degraded", 300 * time.Millisecond},
		{"api-gateway", "down", 500 * time.Millisecond},
	}

	var incidentAt uint64
	for _, sc := range timeline {
		if sc.delay > 0 {
			time.Sleep(sc.delay)
		}
		key := fmt.Sprintf("infra:service:%s:status", sc.name)
		resp, err := kv.Set(ctx, &pb.SetRequest{Key: key, Value: []byte(sc.status)})
		if err != nil {
			log.Fatalf("  Set(%s=%s): %v", sc.name, sc.status, err)
		}
		fmt.Printf("    t=%d  %-20s -> %s\n", resp.Timestamp, sc.name, sc.status)
		// Record the timestamp when postgres first degrades (incident trigger).
		if sc.name == "postgres-primary" && sc.status == "degraded" {
			incidentAt = resp.Timestamp
		}
	}
	fmt.Println()

	// ---------------------------------------------------------------
	// 5. CAUSAL query: trace backward from api-gateway to find root cause
	// ---------------------------------------------------------------
	fmt.Println("[4] Causal root-cause analysis (backward from api-gateway) ...")
	causalStream, err := graph.Causal(ctx, &pb.CausalRequest{
		Trigger:       nodeIDs["api-gateway"],
		At:            incidentAt,
		MaxDepth:      5,
		WindowMicros:  10_000_000, // 10 s window
		Direction:     pb.CausalDirection_CAUSAL_DIRECTION_BACKWARD,
		MinConfidence: 0.1,
	})
	if err != nil {
		log.Fatalf("  Causal RPC: %v", err)
	}
	for {
		result, err := causalStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("  Causal recv: %v", err)
		}
		path := strings.Join(result.CausalPath, " -> ")
		fmt.Printf("    depth=%d  node=%-20s  delta=%dus  confidence=%.2f  path=[%s]  via=%s\n",
			result.Depth, result.NodeId, result.DeltaMicros, result.Confidence, path, result.ViaRelation)
	}
	fmt.Println()

	// ---------------------------------------------------------------
	// 6. TRAVERSE: map full dependency tree from api-gateway
	// ---------------------------------------------------------------
	fmt.Println("[5] Dependency tree traversal (outgoing from api-gateway) ...")
	travStream, err := graph.Traverse(ctx, &pb.TraverseRequest{
		Start:             nodeIDs["api-gateway"],
		At:                incidentAt,
		MaxDepth:          4,
		Direction:         pb.Direction_DIRECTION_OUTGOING,
		IncludeProperties: true,
	})
	if err != nil {
		log.Fatalf("  Traverse RPC: %v", err)
	}
	for {
		result, err := travStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("  Traverse recv: %v", err)
		}
		indent := strings.Repeat("  ", int(result.Depth))
		relation := result.ViaRelation
		if relation == "" {
			relation = "(root)"
		}
		fmt.Printf("    %s[%d] %s  (%s)\n", indent, result.Depth, result.NodeId, relation)
	}
	fmt.Println()

	fmt.Println("=== Analysis Complete ===")
	fmt.Println("Root cause: postgres-primary failure cascaded through auth-service,")
	fmt.Println("user-service, and order-service, ultimately taking down api-gateway.")
}
