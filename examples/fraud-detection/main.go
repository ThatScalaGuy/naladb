// Package main demonstrates NalaDB's fraud detection capabilities.
// It creates a network of bank accounts, simulates transfers and payments,
// then uses graph traversal and causal queries to trace suspicious money flows.
//
// Prerequisites: a running NalaDB server on localhost:7301.
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.NewClient("localhost:7301",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	kv := pb.NewKVServiceClient(conn)
	graph := pb.NewGraphServiceClient(conn)

	fmt.Println("=== NalaDB Fraud Detection Example ===")
	fmt.Println()

	// -- Step 1: Create account and merchant nodes ---------------------------
	fmt.Println("[1] Creating account and merchant nodes...")
	accounts := []string{"account-001", "account-002", "account-003", "account-004", "account-005"}
	nodeIDs := make(map[string]string) // friendly name -> server-assigned ID

	for _, name := range accounts {
		resp, err := graph.CreateNode(ctx, &pb.CreateNodeRequest{
			Type:       "Account",
			Properties: map[string][]byte{"name": []byte(name)},
		})
		if err != nil {
			log.Fatalf("CreateNode(%s): %v", name, err)
		}
		nodeIDs[name] = resp.Id
		fmt.Printf("    Created %-14s -> id=%s\n", name, resp.Id)
	}
	resp, err := graph.CreateNode(ctx, &pb.CreateNodeRequest{
		Type:       "Merchant",
		Properties: map[string][]byte{"name": []byte("merchant-xyz")},
	})
	if err != nil {
		log.Fatalf("CreateNode(merchant-xyz): %v", err)
	}
	nodeIDs["merchant-xyz"] = resp.Id
	fmt.Printf("    Created %-14s -> id=%s\n", "merchant-xyz", resp.Id)
	fmt.Println()

	// -- Step 2: Create edges (transfers and payments) -----------------------
	fmt.Println("[2] Creating transfer and payment edges...")
	type edge struct {
		from, to, relation string
		amount             string
	}
	edges := []edge{
		{"account-001", "account-002", "TRANSFER", "15000.00"},
		{"account-002", "account-003", "TRANSFER", "14500.00"},
		{"account-003", "account-004", "TRANSFER", "14000.00"},
		{"account-005", "merchant-xyz", "PAYMENT", "250.00"},
		{"account-004", "merchant-xyz", "PAYMENT", "13500.00"},
	}

	for _, e := range edges {
		props := map[string][]byte{"amount": []byte(e.amount), "currency": []byte("USD")}
		eResp, err := graph.CreateEdge(ctx, &pb.CreateEdgeRequest{
			From: nodeIDs[e.from], To: nodeIDs[e.to],
			Relation:   e.relation,
			Properties: props,
		})
		if err != nil {
			log.Fatalf("CreateEdge(%s->%s): %v", e.from, e.to, err)
		}
		fmt.Printf("    %s -[%s]-> %s  (edge=%s, $%s)\n",
			e.from, e.relation, e.to, eResp.Id, e.amount)
	}
	fmt.Println()

	// -- Step 3: Write transaction KV data (balances) ------------------------
	fmt.Println("[3] Writing transaction KV data...")
	kvPairs := map[string]string{
		"fraud:account-001:balance":  "50000.00",
		"fraud:account-002:balance":  "15000.00",
		"fraud:account-003:balance":  "14500.00",
		"fraud:account-004:balance":  "14000.00",
		"fraud:account-005:balance":  "3200.00",
		"fraud:merchant-xyz:revenue": "13750.00",
		"fraud:tx:001->002:amount":   "15000.00",
		"fraud:tx:002->003:amount":   "14500.00",
		"fraud:tx:003->004:amount":   "14000.00",
		"fraud:tx:004->merch:amount": "13500.00",
	}
	for key, val := range kvPairs {
		kvResp, err := kv.Set(ctx, &pb.SetRequest{Key: key, Value: []byte(val)})
		if err != nil {
			log.Fatalf("Set(%s): %v", key, err)
		}
		fmt.Printf("    SET %-34s = %-10s (hlc=%d)\n", key, val, kvResp.Timestamp)
	}
	fmt.Println()

	// -- Step 4: Graph traversal -- find transfer chains ---------------------
	fmt.Println("[4] Traversing TRANSFER edges from account-001 (max depth 4)...")
	traverseStream, err := graph.Traverse(ctx, &pb.TraverseRequest{
		Start: nodeIDs["account-001"], MaxDepth: 4,
		Direction:         pb.Direction_DIRECTION_OUTGOING,
		RelationFilter:    []string{"TRANSFER"},
		IncludeProperties: true,
	})
	if err != nil {
		log.Fatalf("Traverse: %v", err)
	}
	fmt.Println("    Hop | Node ID                              | Relation")
	fmt.Println("    ----|--------------------------------------|----------")
	for {
		result, err := traverseStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Traverse recv: %v", err)
		}
		fmt.Printf("    %3d | %-36s | %s\n", result.Depth, result.NodeId, result.ViaRelation)
	}
	fmt.Println()

	// -- Step 5: Causal query -- trace money flow forward --------------------
	fmt.Println("[5] Causal analysis from account-001 (forward, depth 5)...")
	causalStream, err := graph.Causal(ctx, &pb.CausalRequest{
		Trigger: nodeIDs["account-001"], MaxDepth: 5,
		WindowMicros:   int64(10 * time.Minute / time.Microsecond),
		Direction:      pb.CausalDirection_CAUSAL_DIRECTION_FORWARD,
		MinConfidence:  0.3,
		RelationFilter: []string{"TRANSFER", "PAYMENT"},
	})
	if err != nil {
		log.Fatalf("Causal: %v", err)
	}
	fmt.Println("    Depth | Node ID                              | Confidence | Delta (ms)")
	fmt.Println("    ------|--------------------------------------|------------|----------")
	for {
		result, err := causalStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Causal recv: %v", err)
		}
		fmt.Printf("    %5d | %-36s | %10.2f | %10.2f\n",
			result.Depth, result.NodeId, result.Confidence, float64(result.DeltaMicros)/1000.0)
	}
	fmt.Println()

	// -- Step 6: Summary -----------------------------------------------------
	fmt.Println("[6] Fraud Detection Summary")
	fmt.Println("    - Transfer chain: account-001 -> 002 -> 003 -> 004")
	fmt.Println("    - Decreasing amounts suggest layering (15000 -> 14500 -> 14000)")
	fmt.Println("    - Final payout to merchant-xyz: potential money laundering")
	fmt.Println("    - Causal analysis traces full impact of the initial transfer")
	fmt.Println()
	fmt.Println("=== Done ===")
}
