// Package main demonstrates NalaDB's Supply Chain Transparency use case.
//
// This example models a simplified automotive supply chain with raw-material
// batches, production lots, shipments, and retail deliveries. It then uses a
// graph traversal to perform an impact analysis -- answering the question:
// "Which downstream products are affected when a raw material batch has a
// quality issue?"
//
// Prerequisites: a running NalaDB server on localhost:7301.
//
//	go run ./examples/supply-chain/main.go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// ── Connect to NalaDB ────────────────────────────────────────────────
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

	fmt.Println("=== NalaDB Supply Chain Transparency Example ===")
	fmt.Println()

	// ── 1. Create supply chain nodes ─────────────────────────────────────
	fmt.Println("--- Creating supply chain nodes ---")

	// Raw material batches
	steelBatch := mustCreateNode(ctx, graph, "raw-material", map[string][]byte{
		"name":     []byte("Steel Alloy X-42"),
		"supplier": []byte("SteelWorks GmbH"),
		"origin":   []byte("Duisburg, DE"),
		"quality":  []byte("FLAGGED"),
	})
	fmt.Printf("  Raw material: Steel Alloy X-42   [id=%s]\n", steelBatch)

	aluminumBatch := mustCreateNode(ctx, graph, "raw-material", map[string][]byte{
		"name":     []byte("Aluminum 6061-T6"),
		"supplier": []byte("AluCorp AG"),
		"origin":   []byte("Zurich, CH"),
		"quality":  []byte("OK"),
	})
	fmt.Printf("  Raw material: Aluminum 6061-T6   [id=%s]\n", aluminumBatch)

	// Production lots
	gearboxLot := mustCreateNode(ctx, graph, "production-lot", map[string][]byte{
		"name":   []byte("Gearbox Assembly LOT-2024-1187"),
		"plant":  []byte("Stuttgart, DE"),
		"status": []byte("in-production"),
	})
	fmt.Printf("  Production:   Gearbox LOT-1187   [id=%s]\n", gearboxLot)

	engineBlock := mustCreateNode(ctx, graph, "production-lot", map[string][]byte{
		"name":   []byte("Engine Block LOT-2024-0833"),
		"plant":  []byte("Wroclaw, PL"),
		"status": []byte("completed"),
	})
	fmt.Printf("  Production:   Engine LOT-0833    [id=%s]\n", engineBlock)

	// Shipments
	shipment1 := mustCreateNode(ctx, graph, "shipment", map[string][]byte{
		"carrier":  []byte("DB Schenker"),
		"tracking": []byte("SCH-8827431"),
		"status":   []byte("in-transit"),
	})
	fmt.Printf("  Shipment:     DB Schenker        [id=%s]\n", shipment1)

	shipment2 := mustCreateNode(ctx, graph, "shipment", map[string][]byte{
		"carrier":  []byte("DHL Freight"),
		"tracking": []byte("DHL-5539012"),
		"status":   []byte("delivered"),
	})
	fmt.Printf("  Shipment:     DHL Freight        [id=%s]\n", shipment2)

	// Retail / OEM deliveries
	oemDelivery := mustCreateNode(ctx, graph, "delivery", map[string][]byte{
		"customer": []byte("BMW Group"),
		"plant":    []byte("Munich Assembly"),
		"po":       []byte("PO-2024-88421"),
	})
	fmt.Printf("  Delivery:     BMW Munich         [id=%s]\n", oemDelivery)

	fmt.Println()

	// ── 2. Store supplementary KV data ───────────────────────────────────
	fmt.Println("--- Storing supplementary KV data ---")

	mustSet(ctx, kv, "sc:batch:"+steelBatch+":inspection",
		[]byte(`{"date":"2024-06-15","result":"contamination_detected","ppm":42}`))
	fmt.Printf("  Stored inspection report for steel batch %s\n", steelBatch)

	mustSet(ctx, kv, "sc:batch:"+aluminumBatch+":inspection",
		[]byte(`{"date":"2024-06-14","result":"pass","ppm":0}`))
	fmt.Printf("  Stored inspection report for aluminum batch %s\n", aluminumBatch)

	fmt.Println()

	// ── 3. Create edges (supply chain relationships) ─────────────────────
	fmt.Println("--- Creating supply chain relationships ---")

	mustCreateEdge(ctx, graph, steelBatch, gearboxLot, "USED_IN", map[string][]byte{
		"quantity": []byte("1200 kg"),
	})
	fmt.Printf("  %s --[USED_IN]--> %s\n", steelBatch, gearboxLot)

	mustCreateEdge(ctx, graph, steelBatch, engineBlock, "USED_IN", map[string][]byte{
		"quantity": []byte("850 kg"),
	})
	fmt.Printf("  %s --[USED_IN]--> %s\n", steelBatch, engineBlock)

	mustCreateEdge(ctx, graph, aluminumBatch, gearboxLot, "USED_IN", map[string][]byte{
		"quantity": []byte("300 kg"),
	})
	fmt.Printf("  %s --[USED_IN]--> %s\n", aluminumBatch, gearboxLot)

	mustCreateEdge(ctx, graph, gearboxLot, shipment1, "SHIPPED_VIA", map[string][]byte{
		"pieces": []byte("480"),
	})
	fmt.Printf("  %s --[SHIPPED_VIA]--> %s\n", gearboxLot, shipment1)

	mustCreateEdge(ctx, graph, engineBlock, shipment2, "SHIPPED_VIA", map[string][]byte{
		"pieces": []byte("320"),
	})
	fmt.Printf("  %s --[SHIPPED_VIA]--> %s\n", engineBlock, shipment2)

	mustCreateEdge(ctx, graph, shipment1, oemDelivery, "DELIVERED_TO", map[string][]byte{
		"eta": []byte("2024-06-20"),
	})
	fmt.Printf("  %s --[DELIVERED_TO]--> %s\n", shipment1, oemDelivery)

	mustCreateEdge(ctx, graph, shipment2, oemDelivery, "DELIVERED_TO", map[string][]byte{
		"eta": []byte("2024-06-18"),
	})
	fmt.Printf("  %s --[DELIVERED_TO]--> %s\n", shipment2, oemDelivery)

	fmt.Println()

	// ── 4. Impact analysis via TRAVERSE ──────────────────────────────────
	fmt.Println("--- Impact Analysis: Steel Alloy X-42 contamination ---")
	fmt.Printf("Starting traversal from raw material %s (outgoing, max depth 4)\n\n", steelBatch)

	stream, err := graph.Traverse(ctx, &pb.TraverseRequest{
		Start:             steelBatch,
		MaxDepth:          4,
		Direction:         pb.Direction_DIRECTION_OUTGOING,
		IncludeProperties: true,
	})
	if err != nil {
		log.Fatalf("Traverse failed: %v", err)
	}

	affected := 0
	for {
		result, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Traverse stream error: %v", err)
		}
		affected++

		indent := ""
		for i := int32(0); i < result.Depth; i++ {
			indent += "  "
		}

		name := "(unknown)"
		if v, ok := result.Properties["name"]; ok {
			name = string(v)
		} else if v, ok := result.Properties["carrier"]; ok {
			name = "Shipment via " + string(v)
		} else if v, ok := result.Properties["customer"]; ok {
			name = "Delivery to " + string(v)
		}

		fmt.Printf("%sDepth %d | %s [%s] via %s\n",
			indent, result.Depth, result.NodeId, name, result.ViaRelation)
	}

	fmt.Println()
	fmt.Printf("=== Impact Summary ===\n")
	fmt.Printf("Contaminated batch : %s (Steel Alloy X-42)\n", steelBatch)
	fmt.Printf("Affected nodes     : %d\n", affected)
	fmt.Printf("Recommendation     : Initiate recall for all downstream shipments.\n")
	fmt.Println("=== Done ===")
}

// mustCreateNode creates a graph node or exits on error.
func mustCreateNode(ctx context.Context, c pb.GraphServiceClient, nodeType string, props map[string][]byte) string {
	resp, err := c.CreateNode(ctx, &pb.CreateNodeRequest{
		Type:       nodeType,
		Properties: props,
	})
	if err != nil {
		log.Fatalf("CreateNode(%s) failed: %v", nodeType, err)
	}
	return resp.Id
}

// mustCreateEdge creates an edge between two nodes or exits on error.
func mustCreateEdge(ctx context.Context, c pb.GraphServiceClient, from, to, relation string, props map[string][]byte) {
	_, err := c.CreateEdge(ctx, &pb.CreateEdgeRequest{
		From:       from,
		To:         to,
		Relation:   relation,
		Properties: props,
	})
	if err != nil {
		log.Fatalf("CreateEdge(%s --%s--> %s) failed: %v", from, relation, to, err)
	}
}

// mustSet writes a key-value pair or exits on error.
func mustSet(ctx context.Context, c pb.KVServiceClient, key string, value []byte) {
	_, err := c.Set(ctx, &pb.SetRequest{
		Key:   key,
		Value: value,
	})
	if err != nil {
		log.Fatalf("Set(%s) failed: %v", key, err)
	}
}
