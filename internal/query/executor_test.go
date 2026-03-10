package query

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/meta"
	"github.com/thatscalaguy/naladb/internal/store"
)

// testSetup creates a store, graph, meta registry, and clock for testing.
func testSetup(t *testing.T) (*store.Store, *graph.Graph, *meta.Registry, *hlc.Clock) {
	t.Helper()
	counter := int64(1000)
	clock := hlc.NewClockWithPhysical(0, func() int64 {
		counter++
		return counter
	})
	s := store.NewWithoutWAL(clock)
	mr := meta.NewRegistry()
	s.SetMeta(mr)
	g := graph.New(s, clock)
	return s, g, mr, clock
}

// createPredictiveMaintenanceGraph creates the test graph:
// sensor_1:sensor --monitors--> machine_1:machine (type=hydraulic_press)
// sensor_2:sensor --monitors--> machine_1:machine
// sensor_3:sensor --monitors--> machine_2:machine (type=lathe)
func createPredictiveMaintenanceGraph(t *testing.T, g *graph.Graph, clock *hlc.Clock) hlc.HLC {
	t.Helper()

	// Create nodes with explicit IDs.
	_, err := g.CreateNodeWithID("sensor_1", "sensor", map[string][]byte{
		"prop_value": []byte("42.5"),
	})
	require.NoError(t, err)

	_, err = g.CreateNodeWithID("sensor_2", "sensor", map[string][]byte{
		"prop_value": []byte("37.2"),
	})
	require.NoError(t, err)

	_, err = g.CreateNodeWithID("sensor_3", "sensor", map[string][]byte{
		"prop_value": []byte("19.8"),
	})
	require.NoError(t, err)

	m1, err := g.CreateNodeWithID("machine_1", "machine", map[string][]byte{
		"type": []byte("hydraulic_press"),
	})
	require.NoError(t, err)

	_, err = g.CreateNodeWithID("machine_2", "machine", map[string][]byte{
		"type": []byte("lathe"),
	})
	require.NoError(t, err)

	// Get the timestamp after all nodes are created for edge validity.
	at := clock.Now()

	// Create edges: sensor_1 monitors machine_1.
	_, err = g.CreateEdge("sensor_1", "machine_1", "monitors", m1.ValidFrom, hlc.MaxHLC, nil)
	require.NoError(t, err)

	// sensor_2 monitors machine_1.
	_, err = g.CreateEdge("sensor_2", "machine_1", "monitors", m1.ValidFrom, hlc.MaxHLC, nil)
	require.NoError(t, err)

	// sensor_3 monitors machine_2.
	m2, err := g.GetNode("machine_2")
	require.NoError(t, err)
	_, err = g.CreateEdge("sensor_3", "machine_2", "monitors", m2.ValidFrom, hlc.MaxHLC, nil)
	require.NoError(t, err)

	return at
}

// --- Scenario: MATCH-AT Query End-to-End ---

func TestExecutor_MatchAt(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute(`
		MATCH (s:sensor)-[r:monitors]->(m:machine)
		WHERE m.type = "hydraulic_press"
		RETURN s.id, m.id, s.prop_value
	`)
	require.NoError(t, err)

	// sensor_1 and sensor_2 monitor machine_1 (hydraulic_press).
	require.Len(t, rows, 2)

	sensorIDs := make(map[string]bool)
	for _, row := range rows {
		sID, _ := row["s.id"].(string)
		sensorIDs[sID] = true

		mID, _ := row["m.id"].(string)
		assert.Equal(t, "machine_1", mID)

		// Check that prop_value is returned.
		assert.NotNil(t, row["s.prop_value"])
	}
	assert.True(t, sensorIDs["sensor_1"])
	assert.True(t, sensorIDs["sensor_2"])
}

// --- Scenario: CAUSAL Query End-to-End ---

func TestExecutor_Causal(t *testing.T) {
	s, g, _, clock := testSetup(t)

	// Create causal chain: pump_3 -> press_7.
	_, err := g.CreateNodeWithID("pump_3", "pump", map[string][]byte{
		"pressure": []byte("5.0"),
	})
	require.NoError(t, err)

	press7, err := g.CreateNodeWithID("press_7", "press", map[string][]byte{
		"status": []byte("ok"),
	})
	require.NoError(t, err)

	// Edge from press_7 to pump_3 (backward causal: press_7's problem caused by pump_3).
	_, err = g.CreateEdge("press_7", "pump_3", "depends_on", press7.ValidFrom, hlc.MaxHLC, map[string][]byte{
		"weight": []byte("0.9"),
	})
	require.NoError(t, err)

	// Simulate a property change on pump_3 (causal evidence).
	// Write a new value slightly after the trigger time.
	triggerTime := clock.Now()

	// Update pump_3 property to simulate change.
	err = g.UpdateNode("pump_3", map[string][]byte{
		"pressure": []byte("2.0"),
	})
	require.NoError(t, err)

	cq := graph.CausalQuery{
		TriggerNodeID: "press_7",
		At:            triggerTime,
		MaxDepth:      4,
		WindowMicros:  30 * 60 * 1_000_000, // 30 minutes
		Direction:     graph.CausalForward,
	}
	scan, err := NewCausalScanOp(s, g, cq)
	require.NoError(t, err)

	var op Operator = scan
	op = NewSortOp(op, []OrderByItem{
		{Expr: &Identifier{Name: "confidence"}, Desc: true},
	})
	op = NewProjectOp(op, []Expr{
		&Identifier{Name: "path"},
		&Identifier{Name: "delta"},
		&Identifier{Name: "confidence"},
	})

	rows, err := Collect(op)
	require.NoError(t, err)

	// Should find pump_3 in the causal chain.
	for _, row := range rows {
		path, _ := row["path"].(string)
		assert.Contains(t, path, "pump_3")
		conf, _ := row["confidence"].(float64)
		assert.Greater(t, conf, 0.0)
	}
}

// --- Scenario: DIFF Query End-to-End ---

func TestExecutor_Diff(t *testing.T) {
	s, g, mr, clock := testSetup(t)

	// Create nodes.
	_, err := g.CreateNodeWithID("device_A", "device", nil)
	require.NoError(t, err)
	n2, err := g.CreateNodeWithID("device_B", "device", nil)
	require.NoError(t, err)

	// Capture timestamp before edge creation.
	t1 := clock.Now()

	// Create an edge after t1 — use later node's ValidFrom for edge validity.
	_, err = g.CreateEdge("device_A", "device_B", "connects_to", n2.ValidFrom, hlc.MaxHLC, nil)
	require.NoError(t, err)

	// Capture timestamp after edge creation.
	t2 := clock.Now()

	exec := NewExecutor(s, g, mr, clock)

	// Format timestamps as needed by the planner.
	// Use direct planner approach since timestamps need to be HLC.
	planner := NewPlanner(s, g, mr, clock)
	pattern := &GraphPattern{
		Elements: []Node{
			&NodePattern{Variable: "a", Type: "device"},
			&EdgePattern{Variable: "r", Direction: DirectionOutgoing},
			&NodePattern{Variable: "b"},
		},
	}

	diffOp := NewDiffScanOp(s, pattern, t1, t2)
	rows, err := Collect(diffOp)
	require.NoError(t, err)

	_ = exec
	_ = planner

	// Should find the added edge.
	require.GreaterOrEqual(t, len(rows), 1)
	found := false
	for _, row := range rows {
		change, _ := row["change"].(string)
		if change == "added" {
			found = true
			assert.Equal(t, "device_A", row["from"])
			assert.Equal(t, "device_B", row["to"])
		}
	}
	assert.True(t, found, "expected to find an added edge")
}

// --- Scenario: META Query End-to-End ---

func TestExecutor_Meta(t *testing.T) {
	s, _, mr, clock := testSetup(t)

	// Write 100 sensor keys with various write rates.
	for i := range 100 {
		key := fmt.Sprintf("sensor:%03d:prop:temp", i)
		for j := range 10 {
			_, err := s.Set(key, []byte(fmt.Sprintf("%d.%d", i, j)))
			require.NoError(t, err)
		}
	}

	exec := NewExecutor(s, nil, mr, clock)

	rows, err := exec.Execute(`
		META "sensor:*:prop:temp"
		WHERE write_rate > 5.0
		RETURN key, write_rate
	`)
	require.NoError(t, err)

	// All keys with write_rate > 5.0 should be returned.
	for _, row := range rows {
		wr, ok := row["write_rate"].(float64)
		require.True(t, ok)
		assert.Greater(t, wr, 5.0)
	}
}

// --- Scenario: History with Downsampling (LTTB) ---

func TestExecutor_HistoryDownsample(t *testing.T) {
	s, _, mr, clock := testSetup(t)

	key := "sensor:temp"
	// Write 10,000 data points.
	for i := range 10_000 {
		val := fmt.Sprintf("%.2f", float64(i)*0.1)
		_, err := s.Set(key, []byte(val))
		require.NoError(t, err)
	}

	exec := NewExecutor(s, nil, mr, clock)

	rows, err := exec.Execute(`
		GET history("sensor:temp")
		DOWNSAMPLE LTTB(100)
	`)
	require.NoError(t, err)

	// Should get exactly 100 representative data points.
	assert.Len(t, rows, 100)

	// Each row should have timestamp and value.
	for _, row := range rows {
		assert.Contains(t, row, "timestamp")
		assert.Contains(t, row, "value")
	}
}

// --- Scenario: ORDER BY and LIMIT ---

func TestExecutor_OrderByLimit(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute(`
		MATCH (s:sensor)-[r:monitors]->(m:machine)
		RETURN s.id, m.id
		ORDER BY s.id ASC
		LIMIT 1
	`)
	require.NoError(t, err)
	require.Len(t, rows, 1)
}

// --- Scenario: Streaming / Lazy Evaluation ---

func TestExecutor_LazyEvaluation(t *testing.T) {
	s, g, mr, clock := testSetup(t)

	// Create many nodes first.
	for i := range 200 {
		_, err := g.CreateNodeWithID(fmt.Sprintf("item_%03d", i), "item", map[string][]byte{
			"value": []byte(strconv.Itoa(i)),
		})
		require.NoError(t, err)
	}

	// Find the latest ValidFrom (last node created) for edge validity.
	lastNode, err := g.GetNode("item_199")
	require.NoError(t, err)

	// Create edges forming a chain.
	for i := 0; i < 199; i++ {
		from := fmt.Sprintf("item_%03d", i)
		to := fmt.Sprintf("item_%03d", i+1)
		_, err := g.CreateEdge(from, to, "next", lastNode.ValidFrom, hlc.MaxHLC, nil)
		require.NoError(t, err)
	}

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute(`
		MATCH (a:item)-[r:next]->(b:item)
		RETURN a.id, b.id
		LIMIT 10
	`)
	require.NoError(t, err)

	// Only 10 results materialized due to LIMIT.
	assert.Len(t, rows, 10)
}

// --- Scenario: Query Planner wählt optimalen Plan ---

func TestPlanner_FilterPushdown(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	planner := NewPlanner(s, g, mr, clock)

	stmt, err := Parse(`
		MATCH (s:sensor)-[r:monitors]->(m:machine)
		WHERE m.type = "hydraulic_press"
		RETURN s.id
	`)
	require.NoError(t, err)

	op, err := planner.Plan(stmt)
	require.NoError(t, err)
	defer op.Close()

	rows, err := Collect(op)
	require.NoError(t, err)

	// Filter is applied correctly — only hydraulic_press sensors.
	assert.Len(t, rows, 2)
}

// --- Additional test: MATCH without WHERE ---

func TestExecutor_MatchAll(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute(`
		MATCH (s:sensor)-[r:monitors]->(m:machine)
		RETURN s.id, m.id
	`)
	require.NoError(t, err)

	// 3 sensors monitoring 2 machines: sensor_1->machine_1, sensor_2->machine_1, sensor_3->machine_2.
	assert.Len(t, rows, 3)
}

// --- Test: RETURN * ---

func TestExecutor_ReturnStar(t *testing.T) {
	s, g, mr, clock := testSetup(t)

	_, err := g.CreateNodeWithID("n1", "test", map[string][]byte{
		"name": []byte("hello"),
	})
	require.NoError(t, err)

	n2, err := g.CreateNodeWithID("n2", "test", nil)
	require.NoError(t, err)

	// Use the later ValidFrom to ensure edge is within both nodes' validity.
	edgeStart := n2.ValidFrom // n2 created later, so n2.ValidFrom > n1.ValidFrom
	_, err = g.CreateEdge("n1", "n2", "link", edgeStart, hlc.MaxHLC, nil)
	require.NoError(t, err)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute(`
		MATCH (a:test)-[r:link]->(b:test)
		RETURN *
	`)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	// Should have a.id, b.id, etc. but no .__id internal fields.
	row := rows[0]
	assert.Contains(t, row, "a.id")
	assert.Contains(t, row, "b.id")
	for k := range row {
		part := k[strings.LastIndex(k, ".")+1:]
		assert.False(t, strings.HasPrefix(part, "__"),
			"internal field %q should not appear in RETURN *", k)
	}
}

// --- Test: Downsampling algorithms ---

func TestLTTB(t *testing.T) {
	// Create a simple dataset.
	data := make([]DataPoint, 1000)
	for i := range 1000 {
		data[i] = DataPoint{
			Timestamp: int64(i * 1000),
			Value:     float64(i % 100),
		}
	}

	result := LTTB(data, 50)
	assert.Len(t, result, 50)

	// First and last points should be preserved.
	assert.Equal(t, data[0].Timestamp, result[0].Timestamp)
	assert.Equal(t, data[999].Timestamp, result[49].Timestamp)
}

func TestLTTB_EdgeCases(t *testing.T) {
	assert.Nil(t, LTTB(nil, 10))
	assert.Nil(t, LTTB([]DataPoint{}, 10))
	assert.Nil(t, LTTB([]DataPoint{{1, 2.0}}, 0))

	// More points requested than available.
	data := []DataPoint{{1, 1.0}, {2, 2.0}, {3, 3.0}}
	result := LTTB(data, 10)
	assert.Len(t, result, 3)
}

func TestMinMaxDownsample(t *testing.T) {
	data := make([]DataPoint, 100)
	for i := range 100 {
		data[i] = DataPoint{
			Timestamp: int64(i),
			Value:     float64(i),
		}
	}

	result := MinMaxDownsample(data, 10)
	assert.LessOrEqual(t, len(result), 20) // at most 2 per bucket
	assert.GreaterOrEqual(t, len(result), 10)
}

func TestAvgDownsample(t *testing.T) {
	data := make([]DataPoint, 100)
	for i := range 100 {
		data[i] = DataPoint{
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}
	}

	result := AvgDownsample(data, 10)
	assert.Len(t, result, 10)
}

// --- Test: Expression evaluation ---

func TestEvalExpr(t *testing.T) {
	row := Row{
		"a.name":  "hello",
		"a.value": 42.0,
		"b.flag":  true,
	}

	// Property access.
	val := evalExpr(&PropertyAccess{
		Object:   &Identifier{Name: "a"},
		Property: "value",
	}, row)
	assert.Equal(t, 42.0, val)

	// Binary comparison.
	val = evalExpr(&BinaryExpr{
		Left:  &PropertyAccess{Object: &Identifier{Name: "a"}, Property: "value"},
		Op:    TOKEN_GT,
		Right: &FloatLiteral{Value: 40.0},
	}, row)
	assert.Equal(t, true, val)

	// String equality.
	val = evalExpr(&BinaryExpr{
		Left:  &PropertyAccess{Object: &Identifier{Name: "a"}, Property: "name"},
		Op:    TOKEN_EQ,
		Right: &StringLiteral{Value: "hello"},
	}, row)
	assert.Equal(t, true, val)

	// NOT.
	val = evalExpr(&UnaryExpr{
		Op:      TOKEN_NOT,
		Operand: &BoolLiteral{Value: false},
	}, row)
	assert.Equal(t, true, val)

	// AND.
	val = evalExpr(&BinaryExpr{
		Left:  &BoolLiteral{Value: true},
		Op:    TOKEN_AND,
		Right: &BoolLiteral{Value: false},
	}, row)
	assert.Equal(t, false, val)
}

// --- Test: HistoryScanOp without downsampling ---

func TestHistoryScanOp_NoDownsample(t *testing.T) {
	counter := int64(1000)
	clock := hlc.NewClockWithPhysical(0, func() int64 {
		counter++
		return counter
	})
	s := store.NewWithoutWAL(clock)

	for i := range 50 {
		_, err := s.Set("mykey", []byte(strconv.Itoa(i)))
		require.NoError(t, err)
	}

	op := NewHistoryScanOp(s, "mykey", store.HistoryOptions{}, nil)
	rows, err := Collect(op)
	require.NoError(t, err)
	assert.Len(t, rows, 50)
}

// --- Test: MetaScanOp filtering ---

func TestMetaScanOp_Filter(t *testing.T) {
	mr := meta.NewRegistry()
	for i := range 20 {
		key := fmt.Sprintf("sensor:%02d:prop:temp", i)
		for range 5 {
			mr.Update(key, int64(1000+i*100), []byte(strconv.Itoa(i)))
		}
	}

	scan := NewMetaScanOp(mr, "sensor:*:prop:temp")
	filter := NewFilterOp(scan, &BinaryExpr{
		Left:  &Identifier{Name: "total_writes"},
		Op:    TOKEN_GTE,
		Right: &FloatLiteral{Value: 5.0},
	})

	rows, err := Collect(filter)
	require.NoError(t, err)

	for _, row := range rows {
		tw, _ := row["total_writes"].(float64)
		assert.GreaterOrEqual(t, tw, 5.0)
	}
}

// --- Test: SortOp ---

func TestSortOp(t *testing.T) {
	input := NewSliceOp([]Row{
		{"name": "c", "value": 3.0},
		{"name": "a", "value": 1.0},
		{"name": "b", "value": 2.0},
	})

	sortOp := NewSortOp(input, []OrderByItem{
		{Expr: &Identifier{Name: "value"}, Desc: false},
	})

	rows, err := Collect(sortOp)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	assert.Equal(t, 1.0, rows[0]["value"])
	assert.Equal(t, 2.0, rows[1]["value"])
	assert.Equal(t, 3.0, rows[2]["value"])
}

func TestSortOp_Desc(t *testing.T) {
	input := NewSliceOp([]Row{
		{"name": "a", "value": 1.0},
		{"name": "c", "value": 3.0},
		{"name": "b", "value": 2.0},
	})

	sortOp := NewSortOp(input, []OrderByItem{
		{Expr: &Identifier{Name: "value"}, Desc: true},
	})

	rows, err := Collect(sortOp)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	assert.Equal(t, 3.0, rows[0]["value"])
	assert.Equal(t, 2.0, rows[1]["value"])
	assert.Equal(t, 1.0, rows[2]["value"])
}

// --- Test: OffsetOp skips rows ---

func TestOffsetOp(t *testing.T) {
	input := NewSliceOp([]Row{
		{"v": 1}, {"v": 2}, {"v": 3}, {"v": 4}, {"v": 5},
	})

	offsetOp := NewOffsetOp(input, 2)
	rows, err := Collect(offsetOp)
	require.NoError(t, err)
	assert.Len(t, rows, 3)
	assert.Equal(t, 3, rows[0]["v"])
	assert.Equal(t, 4, rows[1]["v"])
	assert.Equal(t, 5, rows[2]["v"])
}

func TestOffsetOp_ExceedsRows(t *testing.T) {
	input := NewSliceOp([]Row{
		{"v": 1}, {"v": 2},
	})

	offsetOp := NewOffsetOp(input, 5)
	rows, err := Collect(offsetOp)
	require.NoError(t, err)
	assert.Len(t, rows, 0)
}

func TestOffsetAndLimitOp(t *testing.T) {
	input := NewSliceOp([]Row{
		{"v": 1}, {"v": 2}, {"v": 3}, {"v": 4}, {"v": 5},
	})

	// OFFSET 2, LIMIT 2 → rows 3 and 4
	var op Operator = NewOffsetOp(input, 2)
	op = NewLimitOp(op, 2)
	rows, err := Collect(op)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
	assert.Equal(t, 3, rows[0]["v"])
	assert.Equal(t, 4, rows[1]["v"])
}

// --- Test: OFFSET via executor end-to-end ---

func TestExecutor_MatchWithOffset(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute(`
		MATCH (s:sensor)-[r:monitors]->(m:machine)
		RETURN s.id, m.id
		ORDER BY s.id ASC
		LIMIT 2
		OFFSET 1
	`)
	require.NoError(t, err)
	// 3 total matches, OFFSET 1 skips first, LIMIT 2 takes next 2
	assert.Len(t, rows, 2)
}

func TestExecutor_ShowNodesWithOffset(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	allRows, err := exec.Execute("SHOW NODES")
	require.NoError(t, err)
	require.Len(t, allRows, 5)

	rows, err := exec.Execute("SHOW NODES LIMIT 2 OFFSET 2")
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

// --- Test: LimitOp stops early ---

func TestLimitOp(t *testing.T) {
	input := NewSliceOp([]Row{
		{"v": 1}, {"v": 2}, {"v": 3}, {"v": 4}, {"v": 5},
	})

	limitOp := NewLimitOp(input, 3)
	rows, err := Collect(limitOp)
	require.NoError(t, err)
	assert.Len(t, rows, 3)
}

// --- Test: parseDurationMicros ---

func TestParseDurationMicros(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"30s", 30_000_000},
		{"5m", 300_000_000},
		{"1h", 3_600_000_000},
		{"500ms", 500_000},
	}

	for _, tt := range tests {
		us, err := parseDurationMicros(tt.input)
		require.NoError(t, err, "input: %s", tt.input)
		assert.Equal(t, tt.expected, us, "input: %s", tt.input)
	}
}

// --- Test: parseTimestamp ---

func TestParseTimestamp(t *testing.T) {
	ts, err := parseTimestamp("2025-06-15T08:47:22Z")
	require.NoError(t, err)
	assert.Greater(t, ts.WallMicros(), int64(0))

	ts2, err := parseTimestamp("2025-02-01")
	require.NoError(t, err)
	assert.Greater(t, ts2.WallMicros(), int64(0))
	assert.True(t, ts2.Before(ts))
}

// --- Scenario: SHOW NODES End-to-End ---

func TestExecutor_ShowNodes(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute("SHOW NODES")
	require.NoError(t, err)

	// 5 nodes: sensor_1, sensor_2, sensor_3, machine_1, machine_2
	assert.Len(t, rows, 5)

	ids := make(map[string]string)
	for _, row := range rows {
		id, _ := row["id"].(string)
		typ, _ := row["type"].(string)
		ids[id] = typ
	}

	assert.Equal(t, "sensor", ids["sensor_1"])
	assert.Equal(t, "sensor", ids["sensor_2"])
	assert.Equal(t, "machine", ids["machine_1"])
}

func TestExecutor_ShowNodesWithWhere(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute(`SHOW NODES WHERE type = "sensor"`)
	require.NoError(t, err)

	assert.Len(t, rows, 3)
	for _, row := range rows {
		assert.Equal(t, "sensor", row["type"])
	}
}

func TestExecutor_ShowNodesWithLimit(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute("SHOW NODES LIMIT 2")
	require.NoError(t, err)

	assert.Len(t, rows, 2)
}

// --- Scenario: SHOW EDGES End-to-End ---

func TestExecutor_ShowEdges(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute("SHOW EDGES")
	require.NoError(t, err)

	// 3 edges: sensor_1->machine_1, sensor_2->machine_1, sensor_3->machine_2
	assert.Len(t, rows, 3)

	for _, row := range rows {
		assert.Contains(t, row, "id")
		assert.Contains(t, row, "from")
		assert.Contains(t, row, "to")
		assert.Contains(t, row, "relation")
		assert.Equal(t, "monitors", row["relation"])
	}
}

// --- Scenario: SHOW KEYS End-to-End ---

func TestExecutor_ShowKeys(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute("SHOW KEYS")
	require.NoError(t, err)

	// Should have node:*, edge:*, graph:adj:* keys.
	assert.Greater(t, len(rows), 0)

	for _, row := range rows {
		key, _ := row["key"].(string)
		assert.NotEmpty(t, key)
	}
}

// --- Scenario: DESCRIBE NODE End-to-End ---

func TestExecutor_DescribeNodeByID(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute(`DESCRIBE NODE "sensor_1"`)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	row := rows[0]
	assert.Equal(t, "sensor_1", row["id"])
	assert.Equal(t, "sensor", row["type"])
	assert.Equal(t, 42.5, row["prop_value"])
}

func TestExecutor_DescribeNodeByID_NotFound(t *testing.T) {
	s, g, mr, clock := testSetup(t)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute(`DESCRIBE NODE "nonexistent"`)
	require.NoError(t, err)
	assert.Len(t, rows, 0)
}

func TestExecutor_DescribeAllNodes(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute("DESCRIBE NODES")
	require.NoError(t, err)
	assert.Len(t, rows, 5)

	// Verify all rows have id, type, and actual property values (not just names).
	for _, row := range rows {
		assert.Contains(t, row, "id")
		assert.Contains(t, row, "type")
	}

	// Find sensor_1 and verify its property value is returned.
	for _, row := range rows {
		if row["id"] == "sensor_1" {
			assert.Equal(t, 42.5, row["prop_value"])
			return
		}
	}
	t.Fatal("sensor_1 not found in DESCRIBE NODES results")
}

func TestExecutor_DescribeNodesWithWhere(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute(`DESCRIBE NODES WHERE type = "machine"`)
	require.NoError(t, err)
	assert.Len(t, rows, 2)

	for _, row := range rows {
		assert.Equal(t, "machine", row["type"])
	}
}

func TestExecutor_DescribeNodesWithLimit(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute("DESCRIBE NODES LIMIT 2")
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

// --- Scenario: DESCRIBE EDGE End-to-End ---

func TestExecutor_DescribeAllEdges(t *testing.T) {
	s, g, mr, clock := testSetup(t)
	createPredictiveMaintenanceGraph(t, g, clock)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute("DESCRIBE EDGES")
	require.NoError(t, err)
	assert.Len(t, rows, 3)

	for _, row := range rows {
		assert.Contains(t, row, "id")
		assert.Contains(t, row, "from")
		assert.Contains(t, row, "to")
		assert.Contains(t, row, "relation")
		assert.Equal(t, "monitors", row["relation"])
	}
}

func TestExecutor_DescribeEdgeWithProperties(t *testing.T) {
	s, g, mr, clock := testSetup(t)

	// Create nodes and an edge with properties.
	_, err := g.CreateNodeWithID("n1", "test", nil)
	require.NoError(t, err)
	n2, err := g.CreateNodeWithID("n2", "test", nil)
	require.NoError(t, err)

	edgeMeta, err := g.CreateEdge("n1", "n2", "link", n2.ValidFrom, hlc.MaxHLC, map[string][]byte{
		"weight": []byte("0.85"),
		"label":  []byte("primary"),
	})
	require.NoError(t, err)

	exec := NewExecutor(s, g, mr, clock)

	rows, err := exec.Execute(fmt.Sprintf(`DESCRIBE EDGE "%s"`, edgeMeta.ID))
	require.NoError(t, err)
	require.Len(t, rows, 1)

	row := rows[0]
	assert.Equal(t, edgeMeta.ID, row["id"])
	assert.Equal(t, "n1", row["from"])
	assert.Equal(t, "n2", row["to"])
	assert.Equal(t, "link", row["relation"])
	assert.Equal(t, 0.85, row["weight"])
	assert.Equal(t, "primary", row["label"])
}

// Silence unused import warnings.
var _ = json.Marshal
