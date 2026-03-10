// Predictive Maintenance Example -- models industrial equipment as a graph,
// ingests sensor telemetry, and uses causal + traversal queries to trace
// anomaly propagation. Requires a NalaDB server on localhost:7301.
//
// Usage: go run ./examples/predictive-maintenance
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := "localhost:7301"
	fmt.Println("=== NalaDB Predictive Maintenance Example ===")
	fmt.Printf("Connecting to NalaDB at %s ...\n", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("ERROR: failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	kv := pb.NewKVServiceClient(conn)
	graph := pb.NewGraphServiceClient(conn)

	// -- Step 1: Create equipment nodes --
	fmt.Println("\n--- Step 1: Creating equipment nodes ---")
	type node struct{ ntype, name string }
	equipment := []node{
		{"pump", "coolant-pump-01"}, {"motor", "drive-motor-01"},
		{"valve", "inlet-valve-01"}, {"sensor", "temp-sensor-01"},
		{"sensor", "vibration-sensor-01"}, {"sensor", "pressure-sensor-01"},
	}
	ids := make(map[string]string) // name -> server-assigned ID
	for _, n := range equipment {
		resp, err := graph.CreateNode(ctx, &pb.CreateNodeRequest{
			Type:       n.ntype,
			Properties: map[string][]byte{"name": []byte(n.name), "location": []byte("hall-A")},
		})
		if err != nil {
			fmt.Printf("  ERROR creating %q: %v\n", n.name, err)
			continue
		}
		ids[n.name] = resp.Id
		fmt.Printf("  Created %-8s %-25s id=%s\n", n.ntype, n.name, resp.Id)
	}

	// -- Step 2: Create edges between equipment --
	fmt.Println("\n--- Step 2: Creating equipment relationships ---")
	type edge struct{ from, to, rel string }
	edges := []edge{
		{"temp-sensor-01", "drive-motor-01", "MONITORS"},
		{"vibration-sensor-01", "drive-motor-01", "MONITORS"},
		{"pressure-sensor-01", "coolant-pump-01", "MONITORS"},
		{"drive-motor-01", "coolant-pump-01", "DRIVES"},
		{"coolant-pump-01", "inlet-valve-01", "FEEDS"},
	}
	for _, e := range edges {
		fID, tID := ids[e.from], ids[e.to]
		if fID == "" || tID == "" {
			fmt.Printf("  SKIP %s -> %s: missing node\n", e.from, e.to)
			continue
		}
		resp, err := graph.CreateEdge(ctx, &pb.CreateEdgeRequest{
			From: fID, To: tID, Relation: e.rel,
		})
		if err != nil {
			fmt.Printf("  ERROR %s -[%s]-> %s: %v\n", e.from, e.rel, e.to, err)
			continue
		}
		fmt.Printf("  %s -[%s]-> %s  edge=%s\n", e.from, e.rel, e.to, resp.Id)
	}

	// -- Step 3: Write sensor telemetry (baseline then anomaly) --
	fmt.Println("\n--- Step 3: Writing sensor telemetry ---")
	type reading struct{ key, val string }
	telemetry := []reading{
		{"sensor:temp-sensor-01:reading", "25.0"},      // baseline
		{"sensor:vibration-sensor-01:reading", "0.02"}, // baseline
		{"sensor:pressure-sensor-01:reading", "3.5"},   // baseline
		{"sensor:temp-sensor-01:reading", "85.3"},      // anomaly: temp spike
		{"sensor:vibration-sensor-01:reading", "1.47"}, // propagated: vibration
		{"sensor:pressure-sensor-01:reading", "1.1"},   // propagated: pressure drop
	}
	var lastTS uint64
	for _, t := range telemetry {
		resp, err := kv.Set(ctx, &pb.SetRequest{Key: t.key, Value: []byte(t.val)})
		if err != nil {
			fmt.Printf("  ERROR writing %s: %v\n", t.key, err)
			continue
		}
		lastTS = resp.Timestamp
		fmt.Printf("  SET %-44s = %-6s ts=%d\n", t.key, t.val, resp.Timestamp)
	}

	// -- Step 4: Read current sensor values --
	fmt.Println("\n--- Step 4: Reading current sensor values ---")
	for _, key := range []string{
		"sensor:temp-sensor-01:reading",
		"sensor:vibration-sensor-01:reading",
		"sensor:pressure-sensor-01:reading",
	} {
		resp, err := kv.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			fmt.Printf("  ERROR reading %s: %v\n", key, err)
		} else if resp.Found {
			fmt.Printf("  GET %-44s = %s\n", key, string(resp.Value))
		} else {
			fmt.Printf("  GET %-44s = (not found)\n", key)
		}
	}

	// -- Step 5: Traverse downstream dependencies from the motor --
	fmt.Println("\n--- Step 5: Traversing dependencies from drive-motor-01 ---")
	if motorID := ids["drive-motor-01"]; motorID != "" {
		stream, err := graph.Traverse(ctx, &pb.TraverseRequest{
			Start: motorID, At: lastTS, MaxDepth: 3,
			Direction: pb.Direction_DIRECTION_OUTGOING, IncludeProperties: true,
		})
		if err != nil {
			fmt.Printf("  ERROR: %v\n", err)
		} else {
			n := recvTraverse(stream)
			if n == 0 {
				fmt.Println("  (no downstream nodes found)")
			}
		}
	}

	// -- Step 6: Causal query -- trace anomaly propagation --
	fmt.Println("\n--- Step 6: Causal query from temp-sensor-01 ---")
	if triggerID := ids["temp-sensor-01"]; triggerID != "" {
		stream, err := graph.Causal(ctx, &pb.CausalRequest{
			Trigger: triggerID, At: lastTS, MaxDepth: 5,
			WindowMicros: 60_000_000, // 60-second causal window
			Direction:    pb.CausalDirection_CAUSAL_DIRECTION_FORWARD,
		})
		if err != nil {
			fmt.Printf("  ERROR: %v\n", err)
		} else {
			n := recvCausal(stream)
			if n == 0 {
				fmt.Println("  (no causal dependencies detected)")
			}
		}
	}

	fmt.Println("\n=== Done ===")
}

// recvTraverse drains a Traverse stream, printing each result. Returns count.
func recvTraverse(stream pb.GraphService_TraverseClient) int {
	count := 0
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			return count
		}
		if err != nil {
			fmt.Printf("  ERROR during traversal: %v\n", err)
			return count
		}
		count++
		fmt.Printf("  depth=%d  node=%s  via=%s  name=%s\n",
			r.Depth, r.NodeId, r.ViaRelation, string(r.Properties["name"]))
	}
}

// recvCausal drains a Causal stream, printing each result. Returns count.
func recvCausal(stream pb.GraphService_CausalClient) int {
	count := 0
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			return count
		}
		if err != nil {
			fmt.Printf("  ERROR during causal query: %v\n", err)
			return count
		}
		count++
		fmt.Printf("  depth=%d  node=%-20s  delta=%d us  confidence=%.2f  path=%v\n",
			r.Depth, r.NodeId, r.DeltaMicros, r.Confidence, r.CausalPath)
	}
}
