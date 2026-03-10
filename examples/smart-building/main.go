// Package main demonstrates the Smart Building / Digital Twin use case for NalaDB.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
)

func main() {
	conn, err := grpc.NewClient("localhost:7301",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	kv := pb.NewKVServiceClient(conn)
	graph := pb.NewGraphServiceClient(conn)
	ctx := context.Background()

	fmt.Println("=== NalaDB Smart Building / Digital Twin Example ===")
	fmt.Println()

	// Step 1: Create building topology nodes.
	fmt.Println("[1] Creating building topology nodes...")
	building := mustCreateNode(ctx, graph, "Building", map[string][]byte{
		"name": []byte("HQ-Munich"), "address": []byte("Maximilianstrasse 1, Munich"),
	})
	floor12 := mustCreateNode(ctx, graph, "Floor", map[string][]byte{
		"name": []byte("Floor-12"), "level": []byte("12"), "area": []byte("1500"),
	})
	room1204 := mustCreateNode(ctx, graph, "Room", map[string][]byte{
		"name": []byte("Room-12.04"), "type": []byte("conference"), "capacity": []byte("20"),
	})
	room1205 := mustCreateNode(ctx, graph, "Room", map[string][]byte{
		"name": []byte("Room-12.05"), "type": []byte("office"), "capacity": []byte("6"),
	})
	hvac12 := mustCreateNode(ctx, graph, "HVAC", map[string][]byte{
		"name": []byte("AHU-12A"), "model": []byte("Carrier-39HQ"), "mode": []byte("cooling"),
	})
	tempSensor := mustCreateNode(ctx, graph, "Sensor", map[string][]byte{
		"name": []byte("TEMP-12.04-01"), "unit": []byte("celsius"),
	})
	humSensor := mustCreateNode(ctx, graph, "Sensor", map[string][]byte{
		"name": []byte("HUM-12.04-01"), "unit": []byte("percent"),
	})
	fmt.Printf("  Building=%s Floor=%s Rooms=%s,%s\n", building, floor12, room1204, room1205)
	fmt.Printf("  HVAC=%s Sensors=%s,%s\n\n", hvac12, tempSensor, humSensor)

	// Step 2: Create edges (CONTAINS, SERVES, MONITORS).
	fmt.Println("[2] Creating building topology edges...")
	for _, e := range [][3]string{
		{building, floor12, "CONTAINS"}, {floor12, room1204, "CONTAINS"},
		{floor12, room1205, "CONTAINS"}, {hvac12, room1204, "SERVES"},
		{hvac12, room1205, "SERVES"}, {tempSensor, room1204, "MONITORS"},
		{humSensor, room1204, "MONITORS"},
	} {
		mustCreateEdge(ctx, graph, e[0], e[1], e[2])
		fmt.Printf("  %s --%s--> %s\n", e[0], e[2], e[1])
	}
	fmt.Println()

	// Step 3: Simulate sensor readings over time.
	fmt.Println("[3] Simulating sensor readings (5 intervals)...")
	temps := []string{"22.5", "23.1", "24.8", "26.3", "25.0"}
	humids := []string{"45.0", "46.2", "52.1", "58.7", "50.3"}
	tempKey, humKey := fmt.Sprintf("sensor:%s:temperature", tempSensor), fmt.Sprintf("sensor:%s:humidity", humSensor)
	for i := range temps {
		mustSet(ctx, kv, tempKey, []byte(temps[i]))
		mustSet(ctx, kv, humKey, []byte(humids[i]))
		fmt.Printf("  t%d: temp=%s C  humidity=%s %%\n", i+1, temps[i], humids[i])
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Println()

	// Step 4: Traverse building topology from root.
	fmt.Println("[4] Traversing building topology (3 hops outgoing)...")
	tStream, err := graph.Traverse(ctx, &pb.TraverseRequest{
		Start:             building,
		MaxDepth:          3,
		Direction:         pb.Direction_DIRECTION_OUTGOING,
		RelationFilter:    []string{"CONTAINS", "SERVES", "MONITORS"},
		IncludeProperties: false,
	})
	if err != nil {
		log.Fatalf("Traverse failed: %v", err)
	}
	for {
		r, err := tStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Traverse stream error: %v", err)
		}
		indent := ""
		for j := int32(0); j < r.Depth; j++ {
			indent += "  "
		}
		fmt.Printf("  %sdepth=%d  node=%s  via=%s\n", indent, r.Depth, r.NodeId, r.ViaRelation)
	}
	fmt.Println()

	// Step 5: Causal query -- HVAC impact analysis.
	fmt.Println("[5] Causal analysis: What did HVAC unit affect?")
	cStream, err := graph.Causal(ctx, &pb.CausalRequest{
		Trigger:       hvac12,
		MaxDepth:      3,
		WindowMicros:  int64(5 * time.Minute / time.Microsecond),
		Direction:     pb.CausalDirection_CAUSAL_DIRECTION_FORWARD,
		MinConfidence: 0.3,
	})
	if err != nil {
		log.Fatalf("Causal query failed: %v", err)
	}
	causalCount := 0
	for {
		r, err := cStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Causal stream error: %v", err)
		}
		causalCount++
		fmt.Printf("  depth=%d  node=%s  confidence=%.2f  delta=%d us  via=%s  path=%v\n",
			r.Depth, r.NodeId, r.Confidence, r.DeltaMicros, r.ViaRelation, r.CausalPath)
	}
	if causalCount == 0 {
		fmt.Println("  (no causal results -- HVAC may not have triggered changes yet)")
	}

	// Step 6: Comfort analysis summary.
	fmt.Println("[6] Comfort analysis for Room-12.04:")
	tResp, err := kv.Get(ctx, &pb.GetRequest{Key: tempKey})
	if err != nil {
		log.Fatalf("Get temperature failed: %v", err)
	}
	hResp, err := kv.Get(ctx, &pb.GetRequest{Key: humKey})
	if err != nil {
		log.Fatalf("Get humidity failed: %v", err)
	}
	fmt.Printf("  Current temperature: %s C\n", string(tResp.Value))
	fmt.Printf("  Current humidity:    %s %%\n", string(hResp.Value))
	fmt.Printf("  Comfort status:      %s\n", comfortStatus(string(tResp.Value)))
	fmt.Println("\n=== Smart Building example complete ===")
}

func mustCreateNode(ctx context.Context, c pb.GraphServiceClient, t string, p map[string][]byte) string {
	resp, err := c.CreateNode(ctx, &pb.CreateNodeRequest{Type: t, Properties: p})
	if err != nil {
		log.Fatalf("CreateNode(%s) failed: %v", t, err)
	}
	return resp.Id
}

func mustCreateEdge(ctx context.Context, c pb.GraphServiceClient, from, to, rel string) {
	if _, err := c.CreateEdge(ctx, &pb.CreateEdgeRequest{
		From: from, To: to, Relation: rel,
	}); err != nil {
		log.Fatalf("CreateEdge(%s --%s--> %s) failed: %v", from, rel, to, err)
	}
}

func mustSet(ctx context.Context, c pb.KVServiceClient, key string, val []byte) {
	if _, err := c.Set(ctx, &pb.SetRequest{Key: key, Value: val}); err != nil {
		log.Fatalf("Set(%s) failed: %v", key, err)
	}
}

func comfortStatus(temp string) string {
	v, err := strconv.ParseFloat(temp, 64)
	if err != nil {
		return "UNKNOWN (parse error)"
	}
	switch {
	case v >= 26.0:
		return "TOO HOT -- consider increasing HVAC cooling"
	case v < 20.0:
		return "TOO COLD -- consider reducing HVAC cooling"
	default:
		return "COMFORTABLE (20-26 C range)"
	}
}
