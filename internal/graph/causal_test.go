package graph

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
)

// --- Test helpers ---

// setupNodePropertyChange writes a property value for a node at a specific HLC.
func setupNodePropertyChange(t *testing.T, s *store.Store, nodeID, propName string, value []byte, at hlc.HLC) {
	t.Helper()
	s.SetWithHLC("node:"+nodeID+":prop:"+propName, at, value, false)
}

// setupEdgeWeight writes a weight property for an edge at a specific HLC.
func setupEdgeWeight(t *testing.T, s *store.Store, edgeID string, weight float64, at hlc.HLC) {
	t.Helper()
	s.SetWithHLC("edge:"+edgeID+":prop:weight", at,
		[]byte(strconv.FormatFloat(weight, 'f', -1, 64)), false)
}

// --- Confidence unit tests ---

func TestTimeFactor_ZeroDelta(t *testing.T) {
	tf := TimeFactor(0, 1_000_000)
	assert.Equal(t, 1.0, tf)
}

func TestTimeFactor_NegativeDelta(t *testing.T) {
	tf := TimeFactor(-100, 1_000_000)
	assert.Equal(t, 1.0, tf)
}

func TestTimeFactor_HalfWindow(t *testing.T) {
	tf := TimeFactor(500_000, 1_000_000)
	expected := math.Exp(-DefaultDecayConstant * 0.5)
	assert.InDelta(t, expected, tf, 0.001)
}

func TestTimeFactor_FullWindow(t *testing.T) {
	tf := TimeFactor(1_000_000, 1_000_000)
	expected := math.Exp(-DefaultDecayConstant)
	assert.InDelta(t, expected, tf, 0.001)
}

func TestTimeFactor_PredictiveMaintenance(t *testing.T) {
	// pump_3 delta=22min/window=60min → exp(-0.25 × 22/60) ≈ 0.912
	deltaMicros := int64(22 * 60 * 1_000_000)
	windowMicros := int64(60 * 60 * 1_000_000)
	tf := TimeFactor(deltaMicros, windowMicros)
	assert.InDelta(t, 0.912, tf, 0.01)
}

func TestTimeFactor_ITInfra(t *testing.T) {
	// checkout delta=3min/window=15min → exp(-0.25 × 3/15) ≈ 0.951
	deltaMicros := int64(3 * 60 * 1_000_000)
	windowMicros := int64(15 * 60 * 1_000_000)
	tf := TimeFactor(deltaMicros, windowMicros)
	assert.InDelta(t, 0.951, tf, 0.01)
}

func TestEdgeWeight_Default(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	w := g.EdgeWeight("nonexistent", hlc.NewHLC(1000, 0, 0))
	assert.Equal(t, 1.0, w)
}

func TestEdgeWeight_Custom(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	s.SetWithHLC("edge:e1:prop:weight", hlc.NewHLC(500, 0, 0), []byte("0.75"), false)

	w := g.EdgeWeight("e1", hlc.NewHLC(1000, 0, 0))
	assert.InDelta(t, 0.75, w, 0.001)
}

func TestEdgeWeight_Invalid(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	s.SetWithHLC("edge:e1:prop:weight", hlc.NewHLC(500, 0, 0), []byte("not-a-number"), false)

	w := g.EdgeWeight("e1", hlc.NewHLC(1000, 0, 0))
	assert.Equal(t, 1.0, w)
}

func TestCausalConfidence_Composition(t *testing.T) {
	conf := CausalConfidence(0.9, 0.8, 0.7)
	assert.InDelta(t, 0.504, conf, 0.001)
}

// --- Gherkin scenario tests ---

// Scenario 1: Simple forward chain
func TestCausalTraverse_ForwardSimpleChain(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	t0 := int64(1_000_000_000) // base time in µs

	// Setup nodes valid from 0 to max.
	setupNodeWithHLC(t, s, "pump_3", "pump", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	setupNodeWithHLC(t, s, "press_7", "press", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)

	// pump_3 changes pressure at t0.
	setupNodePropertyChange(t, s, "pump_3", "pressure", []byte("61"), hlc.NewHLC(t0, 0, 0))
	// press_7 changes status at t0 + 22min.
	setupNodePropertyChange(t, s, "press_7", "status", []byte("overheat"), hlc.NewHLC(t0+22*60*1_000_000, 0, 0))

	// Edge pump_3 → press_7, valid at t0.
	setupEdgeWithHLC(t, s, "e1", "pump_3", "press_7", "supplies",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)

	results, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "pump_3",
		At:            hlc.NewHLC(t0, 0, 0),
		MaxDepth:      2,
		WindowMicros:  30 * 60 * 1_000_000, // 30 min
		Direction:     CausalForward,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "press_7", r.NodeID)
	assert.Equal(t, 1, r.Depth)
	assert.InDelta(t, 22*60*1_000_000, r.DeltaMicros, 1)
	assert.Greater(t, r.Confidence, 0.7)
	assert.Equal(t, []string{"pump_3", "press_7"}, r.Path)
}

// Scenario 2: Multi-hop chain
func TestCausalTraverse_ForwardMultiHop(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	t0 := int64(1_000_000_000)

	for _, id := range []string{"pump_3", "press_7", "conveyor_12"} {
		setupNodeWithHLC(t, s, id, "machine", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	// pump_3 changes at t0.
	setupNodePropertyChange(t, s, "pump_3", "pressure", []byte("61"), hlc.NewHLC(t0, 0, 0))
	// press_7 reacts at t0 + 22min.
	setupNodePropertyChange(t, s, "press_7", "status", []byte("overheat"), hlc.NewHLC(t0+22*60*1_000_000, 0, 0))
	// conveyor_12 reacts at t0 + 25min.
	setupNodePropertyChange(t, s, "conveyor_12", "speed", []byte("0"), hlc.NewHLC(t0+25*60*1_000_000, 0, 0))

	setupEdgeWithHLC(t, s, "e1", "pump_3", "press_7", "supplies",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "e2", "press_7", "conveyor_12", "feeds_into",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)

	results, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "pump_3",
		At:            hlc.NewHLC(t0, 0, 0),
		MaxDepth:      3,
		WindowMicros:  30 * 60 * 1_000_000,
		Direction:     CausalForward,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)

	byNode := map[string]CausalResult{}
	for _, r := range results {
		byNode[r.NodeID] = r
	}

	assert.Equal(t, 1, byNode["press_7"].Depth)
	assert.Greater(t, byNode["press_7"].Confidence, 0.7)

	assert.Equal(t, 2, byNode["conveyor_12"].Depth)
	assert.Greater(t, byNode["conveyor_12"].Confidence, 0.5)
	assert.Equal(t, []string{"pump_3", "press_7", "conveyor_12"}, byNode["conveyor_12"].Path)
}

// Scenario 3: Confidence decreases with time
func TestCausalTraverse_ConfidenceDecreases(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	t0 := int64(1_000_000_000)

	for _, id := range []string{"trigger", "b", "c"} {
		setupNodeWithHLC(t, s, id, "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	setupNodePropertyChange(t, s, "trigger", "val", []byte("changed"), hlc.NewHLC(t0, 0, 0))
	// b reacts after 1 min.
	setupNodePropertyChange(t, s, "b", "val", []byte("b_changed"), hlc.NewHLC(t0+1*60*1_000_000, 0, 0))
	// c reacts after 25 min.
	setupNodePropertyChange(t, s, "c", "val", []byte("c_changed"), hlc.NewHLC(t0+25*60*1_000_000, 0, 0))

	setupEdgeWithHLC(t, s, "e1", "trigger", "b", "affects",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "e2", "trigger", "c", "affects",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)

	results, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "trigger",
		At:            hlc.NewHLC(t0, 0, 0),
		MaxDepth:      1,
		WindowMicros:  30 * 60 * 1_000_000,
		Direction:     CausalForward,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)

	byNode := map[string]CausalResult{}
	for _, r := range results {
		byNode[r.NodeID] = r
	}

	assert.Greater(t, byNode["b"].Confidence, byNode["c"].Confidence,
		"b (1min) should have higher confidence than c (25min)")
}

// Scenario 4: Edge weight affects confidence
func TestCausalTraverse_EdgeWeight(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	t0 := int64(1_000_000_000)

	for _, id := range []string{"trigger", "x", "y"} {
		setupNodeWithHLC(t, s, id, "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	setupNodePropertyChange(t, s, "trigger", "val", []byte("changed"), hlc.NewHLC(t0, 0, 0))
	// Both react after same time (5 min).
	delta := int64(5 * 60 * 1_000_000)
	setupNodePropertyChange(t, s, "x", "val", []byte("x_changed"), hlc.NewHLC(t0+delta, 0, 0))
	setupNodePropertyChange(t, s, "y", "val", []byte("y_changed"), hlc.NewHLC(t0+delta, 0, 0))

	setupEdgeWithHLC(t, s, "ex", "trigger", "x", "affects",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "ey", "trigger", "y", "affects",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)

	// Set edge weights.
	setupEdgeWeight(t, s, "ex", 0.9, hlc.NewHLC(0, 0, 0))
	setupEdgeWeight(t, s, "ey", 0.1, hlc.NewHLC(0, 0, 0))

	results, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "trigger",
		At:            hlc.NewHLC(t0, 0, 0),
		MaxDepth:      1,
		WindowMicros:  30 * 60 * 1_000_000,
		Direction:     CausalForward,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)

	byNode := map[string]CausalResult{}
	for _, r := range results {
		byNode[r.NodeID] = r
	}

	assert.Greater(t, byNode["x"].Confidence, byNode["y"].Confidence,
		"x (weight=0.9) should have higher confidence than y (weight=0.1)")
}

// Scenario 5: Window bounds search
func TestCausalTraverse_WindowExclusion(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	t0 := int64(1_000_000_000)

	for _, id := range []string{"trigger", "late"} {
		setupNodeWithHLC(t, s, id, "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	setupNodePropertyChange(t, s, "trigger", "val", []byte("changed"), hlc.NewHLC(t0, 0, 0))
	// "late" reacts at 60min — outside the 30min window.
	setupNodePropertyChange(t, s, "late", "val", []byte("late_changed"), hlc.NewHLC(t0+60*60*1_000_000, 0, 0))

	setupEdgeWithHLC(t, s, "e1", "trigger", "late", "affects",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)

	results, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "trigger",
		At:            hlc.NewHLC(t0, 0, 0),
		MaxDepth:      1,
		WindowMicros:  30 * 60 * 1_000_000,
		Direction:     CausalForward,
	})
	require.NoError(t, err)
	assert.Empty(t, results, "node outside window should not appear")
}

// Scenario 6: min_confidence filter
func TestCausalTraverse_MinConfidence(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	t0 := int64(1_000_000_000)
	window := int64(60 * 60 * 1_000_000) // 60 min

	setupNodeWithHLC(t, s, "trigger", "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	setupNodePropertyChange(t, s, "trigger", "val", []byte("changed"), hlc.NewHLC(t0, 0, 0))

	// Create 5 nodes with varying deltas to produce varying confidences.
	// Use edge weights to produce specific confidence levels:
	// n1: delta=1min, weight=0.92 → conf ≈ 0.92 * exp(-0.25*1/60) ≈ 0.92 * 0.996 ≈ 0.916
	// n2: delta=5min, weight=0.73 → conf ≈ 0.73 * exp(-0.25*5/60) ≈ 0.73 * 0.979 ≈ 0.715
	// n3: delta=10min, weight=0.53 → conf ≈ 0.53 * exp(-0.25*10/60) ≈ 0.53 * 0.959 ≈ 0.508
	// n4: delta=20min, weight=0.33 → conf ≈ 0.33 * exp(-0.25*20/60) ≈ 0.33 * 0.920 ≈ 0.304
	// n5: delta=40min, weight=0.13 → conf ≈ 0.13 * exp(-0.25*40/60) ≈ 0.13 * 0.846 ≈ 0.110
	type nodeSetup struct {
		id     string
		delta  int64 // minutes
		weight float64
	}
	nodes := []nodeSetup{
		{"n1", 1, 0.92},
		{"n2", 5, 0.73},
		{"n3", 10, 0.53},
		{"n4", 20, 0.33},
		{"n5", 40, 0.13},
	}

	for i, n := range nodes {
		setupNodeWithHLC(t, s, n.id, "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
		setupNodePropertyChange(t, s, n.id, "val", []byte("changed"),
			hlc.NewHLC(t0+n.delta*60*1_000_000, 0, 0))

		eid := "e" + n.id
		setupEdgeWithHLC(t, s, eid, "trigger", n.id, "affects",
			hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
		setupEdgeWeight(t, s, eid, n.weight, hlc.NewHLC(0, 0, 0))
		_ = i
	}

	results, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "trigger",
		At:            hlc.NewHLC(t0, 0, 0),
		MaxDepth:      1,
		WindowMicros:  window,
		Direction:     CausalForward,
		MinConfidence: 0.5,
	})
	require.NoError(t, err)

	nodeIDs := map[string]bool{}
	for _, r := range results {
		nodeIDs[r.NodeID] = true
		assert.GreaterOrEqual(t, r.Confidence, 0.5)
	}

	assert.True(t, nodeIDs["n1"], "n1 should be included (conf >= 0.5)")
	assert.True(t, nodeIDs["n2"], "n2 should be included (conf >= 0.5)")
	assert.True(t, nodeIDs["n3"], "n3 should be included (conf >= 0.5)")
	assert.False(t, nodeIDs["n4"], "n4 should be excluded (conf < 0.5)")
	assert.False(t, nodeIDs["n5"], "n5 should be excluded (conf < 0.5)")
}

// Scenario 7: Backward root-cause
func TestCausalTraverse_BackwardRootCause(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	t0 := int64(1_000_000_000) // checkout error time

	for _, id := range []string{"checkout", "redis_main", "node_42"} {
		setupNodeWithHLC(t, s, id, "infra", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	// checkout has error at t0.
	setupNodePropertyChange(t, s, "checkout", "errors", []byte("spike"), hlc.NewHLC(t0, 0, 0))
	// redis had latency at t0 - 12min.
	setupNodePropertyChange(t, s, "redis_main", "latency", []byte("45ms"), hlc.NewHLC(t0-12*60*1_000_000, 0, 0))
	// node_42 had CPU spike at t0 - 15min.
	setupNodePropertyChange(t, s, "node_42", "cpu", []byte("99%"), hlc.NewHLC(t0-15*60*1_000_000, 0, 0))

	// Edges: node_42 → redis_main → checkout (backward from checkout).
	// For backward traversal: we follow INCOMING edges from checkout.
	// So edges are: redis_main → checkout and node_42 → redis_main.
	setupEdgeWithHLC(t, s, "e1", "redis_main", "checkout", "reads",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "e2", "node_42", "redis_main", "runs_on",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)

	results, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "checkout",
		At:            hlc.NewHLC(t0, 0, 0),
		MaxDepth:      5,
		WindowMicros:  15 * 60 * 1_000_000, // 15 min
		Direction:     CausalBackward,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)

	byNode := map[string]CausalResult{}
	for _, r := range results {
		byNode[r.NodeID] = r
	}

	assert.Contains(t, byNode, "redis_main")
	assert.InDelta(t, -12*60*1_000_000, byNode["redis_main"].DeltaMicros, 1)

	assert.Contains(t, byNode, "node_42")
	assert.InDelta(t, -15*60*1_000_000, byNode["node_42"].DeltaMicros, 1)

	// The causal chain path should include: checkout → redis_main → node_42
	assert.Equal(t, []string{"checkout", "redis_main", "node_42"}, byNode["node_42"].Path)
}

// Scenario 8: Use Case Predictive Maintenance
func TestCausalTraverse_PredictiveMaintenance(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	t0 := int64(10_000_000_000) // large enough so t0-45min stays positive

	for _, id := range []string{"cooler_1", "pump_3", "press_7"} {
		setupNodeWithHLC(t, s, id, "machine", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	// press_7 overheats at t0+22min (trigger).
	triggerTime := t0 + 22*60*1_000_000
	setupNodePropertyChange(t, s, "press_7", "temperature", []byte("89.7"), hlc.NewHLC(triggerTime, 0, 0))
	// pump_3 pressure drops at t0.
	setupNodePropertyChange(t, s, "pump_3", "pressure", []byte("61"), hlc.NewHLC(t0, 0, 0))
	// cooler_1 flow drops at t0-23min (45min before trigger).
	setupNodePropertyChange(t, s, "cooler_1", "flow_rate", []byte("low"), hlc.NewHLC(t0-23*60*1_000_000, 0, 0))

	// Edges: cooler_1 → pump_3 → press_7 (backward from press_7).
	setupEdgeWithHLC(t, s, "e1", "pump_3", "press_7", "supplies",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "e2", "cooler_1", "pump_3", "cools",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)

	results, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "press_7",
		At:            hlc.NewHLC(triggerTime, 0, 0),
		MaxDepth:      4,
		WindowMicros:  60 * 60 * 1_000_000, // 60 min
		Direction:     CausalBackward,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)

	byNode := map[string]CausalResult{}
	for _, r := range results {
		byNode[r.NodeID] = r
	}

	// pump_3: delta=-22min from trigger
	assert.Contains(t, byNode, "pump_3")
	assert.InDelta(t, -22*60*1_000_000, byNode["pump_3"].DeltaMicros, 1)
	assert.InDelta(t, 0.91, byNode["pump_3"].Confidence, 0.05)

	// cooler_1: delta=-45min from trigger
	// Per-hop confidence: 0.912 (pump_3) × exp(-0.25 × 23/60) ≈ 0.83
	assert.Contains(t, byNode, "cooler_1")
	assert.InDelta(t, -45*60*1_000_000, byNode["cooler_1"].DeltaMicros, 1)
	assert.InDelta(t, 0.83, byNode["cooler_1"].Confidence, 0.05)
}

// Scenario 9: Use Case IT Infrastructure
func TestCausalTraverse_ITInfrastructure(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	t0 := int64(1_000_000_000)
	window := int64(15 * 60 * 1_000_000) // 15 min

	for _, id := range []string{"redis_main", "checkout", "payment", "order_queue"} {
		setupNodeWithHLC(t, s, id, "service", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	// redis latency spikes at t0 (trigger).
	setupNodePropertyChange(t, s, "redis_main", "latency", []byte("45ms"), hlc.NewHLC(t0, 0, 0))
	// checkout p99 rises at t0 + 3min.
	setupNodePropertyChange(t, s, "checkout", "p99_latency", []byte("high"), hlc.NewHLC(t0+3*60*1_000_000, 0, 0))
	// payment errors at t0 + 5min.
	setupNodePropertyChange(t, s, "payment", "error_rate", []byte("high"), hlc.NewHLC(t0+5*60*1_000_000, 0, 0))
	// order_queue lag at t0 + 8min.
	setupNodePropertyChange(t, s, "order_queue", "lag", []byte("high"), hlc.NewHLC(t0+8*60*1_000_000, 0, 0))

	// Chain: redis → checkout → payment → order_queue.
	setupEdgeWithHLC(t, s, "e1", "redis_main", "checkout", "serves",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "e2", "checkout", "payment", "calls",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "e3", "payment", "order_queue", "publishes",
		hlc.NewHLC(0, 0, 0), hlc.MaxHLC)

	// Set edge weights for inner edges to match expected confidence values.
	setupEdgeWeight(t, s, "e2", 0.9, hlc.NewHLC(0, 0, 0))
	setupEdgeWeight(t, s, "e3", 0.9, hlc.NewHLC(0, 0, 0))

	results, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "redis_main",
		At:            hlc.NewHLC(t0, 0, 0),
		MaxDepth:      5,
		WindowMicros:  window,
		Direction:     CausalForward,
	})
	require.NoError(t, err)
	require.Len(t, results, 3)

	byNode := map[string]CausalResult{}
	for _, r := range results {
		byNode[r.NodeID] = r
	}

	// checkout: +3min, conf ≈ 0.95
	assert.Contains(t, byNode, "checkout")
	assert.InDelta(t, 3*60*1_000_000, byNode["checkout"].DeltaMicros, 1)
	assert.InDelta(t, 0.95, byNode["checkout"].Confidence, 0.05)

	// payment: +5min, conf ≈ 0.82
	assert.Contains(t, byNode, "payment")
	assert.InDelta(t, 5*60*1_000_000, byNode["payment"].DeltaMicros, 1)
	assert.InDelta(t, 0.82, byNode["payment"].Confidence, 0.08)

	// order_queue: +8min, conf ≈ 0.71
	assert.Contains(t, byNode, "order_queue")
	assert.InDelta(t, 8*60*1_000_000, byNode["order_queue"].DeltaMicros, 1)
	assert.InDelta(t, 0.71, byNode["order_queue"].Confidence, 0.08)

	// Verify ordering.
	assert.Greater(t, byNode["checkout"].Confidence, byNode["payment"].Confidence)
	assert.Greater(t, byNode["payment"].Confidence, byNode["order_queue"].Confidence)
}

// --- Validation tests ---

func TestCausalTraverse_InvalidQuery_EmptyTrigger(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	_, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "",
		At:            hlc.NewHLC(1000, 0, 0),
		MaxDepth:      1,
		WindowMicros:  1000,
	})
	assert.ErrorIs(t, err, ErrInvalidCausalQuery)
}

func TestCausalTraverse_InvalidQuery_ZeroAt(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	_, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "node1",
		At:            0,
		MaxDepth:      1,
		WindowMicros:  1000,
	})
	assert.ErrorIs(t, err, ErrInvalidCausalQuery)
}

func TestCausalTraverse_InvalidQuery_ZeroDepth(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	_, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "node1",
		At:            hlc.NewHLC(1000, 0, 0),
		MaxDepth:      0,
		WindowMicros:  1000,
	})
	assert.ErrorIs(t, err, ErrInvalidCausalQuery)
}

func TestCausalTraverse_InvalidQuery_ZeroWindow(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	_, err := g.CausalTraverse(CausalQuery{
		TriggerNodeID: "node1",
		At:            hlc.NewHLC(1000, 0, 0),
		MaxDepth:      1,
		WindowMicros:  0,
	})
	assert.ErrorIs(t, err, ErrInvalidCausalQuery)
}
