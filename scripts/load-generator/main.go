// Package main is a simple infinite load generator for NalaDB.
// It continuously writes and updates KV data and graph nodes/edges.
//
// Usage:
//
//	go run ./scripts/load-generator
//	go run ./scripts/load-generator -addr localhost:7301 -interval 500ms -keys 100
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
)

func main() {
	addr := flag.String("addr", "localhost:7301", "NalaDB server address")
	interval := flag.Duration("interval", 500*time.Millisecond, "delay between operations")
	numKeys := flag.Int("keys", 50, "number of distinct KV keys to rotate through")
	numNodes := flag.Int("nodes", 20, "number of graph nodes to create")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	kv := pb.NewKVServiceClient(conn)
	graph := pb.NewGraphServiceClient(conn)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	fmt.Printf("Load generator started (addr=%s interval=%s keys=%d nodes=%d)\n", *addr, *interval, *numKeys, *numNodes)
	fmt.Println("Press Ctrl+C to stop.")
	fmt.Println()

	var ops atomic.Uint64
	go func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		var last uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				cur := ops.Load()
				fmt.Printf("[stats] total=%d  last5s=%d  ops/s=%.1f\n", cur, cur-last, float64(cur-last)/5.0)
				last = cur
			}
		}
	}()

	// Phase 1: seed graph nodes.
	fmt.Println("[phase 1] Creating graph nodes ...")
	nodeIDs := make([]string, 0, *numNodes)
	for i := range *numNodes {
		resp, err := graph.CreateNode(ctx, &pb.CreateNodeRequest{
			Type:       randomType(),
			Properties: map[string][]byte{"name": []byte(fmt.Sprintf("node-%d", i))},
		})
		if err != nil {
			log.Fatalf("CreateNode: %v", err)
		}
		nodeIDs = append(nodeIDs, resp.Id)
		ops.Add(1)
	}
	fmt.Printf("  created %d nodes\n", len(nodeIDs))

	// Phase 2: create edges between random nodes.
	fmt.Println("[phase 2] Creating graph edges ...")
	edgeCount := 0
	for i := 0; i < len(nodeIDs); i++ {
		targets := pickN(nodeIDs, i, 2)
		for _, t := range targets {
			_, err := graph.CreateEdge(ctx, &pb.CreateEdgeRequest{
				From: nodeIDs[i], To: t, Relation: randomRelation(),
			})
			if err != nil {
				fmt.Printf("  WARN CreateEdge: %v\n", err)
				continue
			}
			edgeCount++
			ops.Add(1)
		}
	}
	fmt.Printf("  created %d edges\n", edgeCount)

	// Phase 3: infinite KV write/update loop.
	fmt.Println("[phase 3] Starting infinite KV write loop ...")
	fmt.Println()

	domains := []string{"sensor", "metric", "status", "config", "counter"}
	tick := time.NewTicker(*interval)
	defer tick.Stop()

	iteration := 0
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\nStopped after %d operations.\n", ops.Load())
			return
		case <-tick.C:
			iteration++
			keyIdx := rand.IntN(*numKeys)
			domain := domains[rand.IntN(len(domains))]
			key := fmt.Sprintf("load:%s:key-%04d", domain, keyIdx)
			value := fmt.Sprintf(`{"iter":%d,"ts":"%s","v":%.4f}`,
				iteration, time.Now().Format(time.RFC3339Nano), rand.Float64()*1000)

			resp, err := kv.Set(ctx, &pb.SetRequest{Key: key, Value: []byte(value)})
			if err != nil {
				fmt.Printf("  ERR Set(%s): %v\n", key, err)
				continue
			}
			ops.Add(1)

			if iteration%20 == 0 {
				fmt.Printf("  [%d] SET %-40s hlc=%d\n", iteration, key, resp.Timestamp)
			}

			// Every 10th iteration, also read back a random key.
			if iteration%10 == 0 {
				rKey := fmt.Sprintf("load:%s:key-%04d", domains[rand.IntN(len(domains))], rand.IntN(*numKeys))
				_, err := kv.Get(ctx, &pb.GetRequest{Key: rKey})
				if err == nil {
					ops.Add(1)
				}
			}

			// Every 50th iteration, create a new edge between random nodes.
			if iteration%50 == 0 && len(nodeIDs) > 1 {
				a, b := rand.IntN(len(nodeIDs)), rand.IntN(len(nodeIDs))
				if a != b {
					_, err := graph.CreateEdge(ctx, &pb.CreateEdgeRequest{
						From: nodeIDs[a], To: nodeIDs[b], Relation: randomRelation(),
					})
					if err == nil {
						ops.Add(1)
					}
				}
			}
		}
	}
}

func randomType() string {
	types := []string{"sensor", "gateway", "service", "database", "cache", "worker"}
	return types[rand.IntN(len(types))]
}

func randomRelation() string {
	rels := []string{"CONNECTS_TO", "DEPENDS_ON", "MONITORS", "FEEDS", "SERVES"}
	return rels[rand.IntN(len(rels))]
}

// pickN selects up to n random indices from ids, excluding skip.
func pickN(ids []string, skip, n int) []string {
	if len(ids) <= 1 {
		return nil
	}
	out := make([]string, 0, n)
	for range n * 3 {
		idx := rand.IntN(len(ids))
		if idx == skip {
			continue
		}
		out = append(out, ids[idx])
		if len(out) >= n {
			break
		}
	}
	return out
}
