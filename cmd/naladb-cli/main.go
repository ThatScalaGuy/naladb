// Package main is the entry point for the NalaDB CLI client.
// It provides an interactive REPL for executing NalaQL queries.
//
// Usage:
//
//	naladb-cli                          # connect to localhost:7301
//	naladb-cli -addr node1:7301         # connect to a specific server
//	naladb-cli -e 'MATCH (n:sensor) RETURN n.id'  # execute a single query
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"github.com/thatscalaguy/naladb/internal/ui"
)

// version is set at build time via -ldflags.
var version = "dev"

func main() {
	showVersion := flag.Bool("v", false, "print version and exit")
	addr := flag.String("addr", "localhost:7301", "NalaDB server address")
	execute := flag.String("e", "", "execute a single query and exit")
	uiMode := flag.Bool("ui", false, "launch web UI instead of REPL")
	uiPort := flag.Int("ui-port", 8080, "web UI port")
	flag.Parse()

	if *showVersion {
		fmt.Println("naladb-cli " + version)
		return
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewQueryServiceClient(conn)

	if *uiMode {
		uiSrv := ui.NewServer(ui.Config{
			QueryClient: client,
			GraphClient: pb.NewGraphServiceClient(conn),
			WatchClient: pb.NewWatchServiceClient(conn),
			StatsClient: pb.NewStatsServiceClient(conn),
			Port:        *uiPort,
			ServerAddr:  *addr,
		})
		log.Fatal(uiSrv.ListenAndServe())
	}

	if *execute != "" {
		runQuery(client, *execute)
		return
	}

	// Interactive REPL.
	fmt.Println("NalaDB CLI (connected to " + *addr + ")")
	fmt.Println("Type a NalaQL query and press Enter. Use ';' to end multi-line queries.")
	fmt.Println("Commands: \\q quit  \\h help")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	var buf strings.Builder

	for {
		if buf.Len() == 0 {
			fmt.Print("naladb> ")
		} else {
			fmt.Print("   ...> ")
		}

		if !scanner.Scan() {
			break
		}
		line := scanner.Text()

		// Handle meta commands.
		trimmed := strings.TrimSpace(line)
		if buf.Len() == 0 {
			switch trimmed {
			case "\\q", "quit", "exit":
				fmt.Println("Bye!")
				return
			case "\\h", "help":
				printHelp()
				continue
			case "":
				continue
			}
		}

		// Accumulate multi-line input.
		if buf.Len() > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(line)

		// Execute when line ends with ';' or it's a single-line query.
		input := strings.TrimSpace(buf.String())
		if strings.HasSuffix(input, ";") {
			input = strings.TrimSuffix(input, ";")
			input = strings.TrimSpace(input)
			if input != "" {
				runQuery(client, input)
			}
			buf.Reset()
		} else if buf.Len() > 0 && !strings.ContainsAny(trimmed, "()[]") && !looksMultiLine(input) {
			// Single-line query without semicolon — execute immediately.
			runQuery(client, input)
			buf.Reset()
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("input error: %v", err)
	}
	fmt.Println()
}

// looksMultiLine returns true if the input looks like the start of a multi-line query.
func looksMultiLine(s string) bool {
	upper := strings.ToUpper(s)
	// If it starts with a keyword but doesn't have RETURN yet, likely continues.
	for _, kw := range []string{"MATCH ", "CAUSAL ", "DIFF "} {
		if strings.HasPrefix(upper, kw) && !strings.Contains(upper, "RETURN") {
			return true
		}
	}
	return false
}

func runQuery(client pb.QueryServiceClient, q string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	stream, err := client.Query(ctx, &pb.QueryRequest{Query: q})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	var rows []map[string]string
	var columns []string
	columnSet := make(map[string]bool)

	for {
		row, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
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

	elapsed := time.Since(start)

	if len(rows) == 0 {
		fmt.Printf("(empty result set, %s)\n\n", elapsed.Round(time.Microsecond))
		return
	}

	// Calculate column widths.
	widths := make(map[string]int)
	for _, col := range columns {
		widths[col] = len(col)
	}
	for _, row := range rows {
		for _, col := range columns {
			if v := row[col]; len(v) > widths[col] {
				widths[col] = len(v)
			}
		}
	}

	// Print header.
	printRow(columns, widths, func(col string) string { return col })
	printSep(columns, widths)

	// Print data rows.
	for _, row := range rows {
		printRow(columns, widths, func(col string) string { return row[col] })
	}

	fmt.Printf("(%d rows, %s)\n\n", len(rows), elapsed.Round(time.Microsecond))
}

func printRow(columns []string, widths map[string]int, val func(string) string) {
	parts := make([]string, len(columns))
	for i, col := range columns {
		parts[i] = fmt.Sprintf(" %-*s ", widths[col], val(col))
	}
	fmt.Println("|" + strings.Join(parts, "|") + "|")
}

func printSep(columns []string, widths map[string]int) {
	parts := make([]string, len(columns))
	for i, col := range columns {
		parts[i] = strings.Repeat("-", widths[col]+2)
	}
	fmt.Println("+" + strings.Join(parts, "+") + "+")
}

func printHelp() {
	fmt.Print(`NalaQL Quick Reference:

  MATCH (a:type)-[r:relation]->(b)   Graph pattern query
    AT "2024-06-01T14:00:00Z"          Point-in-time
    WHERE r.weight > 0.5               Filter
    RETURN a.id, b.id                  Projection
    ORDER BY a.id ASC                  Sorting
    LIMIT 10                           Limit results

  CAUSAL FROM node_id                Causal traversal
    DEPTH 5  WINDOW 30s                Depth & time window

  DIFF (a)-[r]->(b)                  Temporal diff
    BETWEEN "t1" AND "t2"

  GET history("key")                 Key history
    LAST 100                           Last N versions
    DOWNSAMPLE LTTB(50)                Downsampling

  META "key_pattern"                 Key statistics

  DESCRIBE NODE "id"                 Inspect a node with all properties
  DESCRIBE EDGE "id"                 Inspect an edge with all properties
  DESCRIBE NODES                     List all nodes with property values
  DESCRIBE EDGES                     List all edges with property values
    AT "2024-06-01T14:00:00Z"          Point-in-time
    WHERE type = "sensor"              Optional filter
    LIMIT 10                           Optional limit

  SHOW NODES                         List all graph nodes
  SHOW EDGES                         List all graph edges
  SHOW KEYS                          List all store keys
    WHERE type = "sensor"              Optional filter
    LIMIT 10                           Optional limit

Commands:
  \q, quit, exit    Exit the CLI
  \h, help          Show this help
  ;                 End a multi-line query
`)
}
