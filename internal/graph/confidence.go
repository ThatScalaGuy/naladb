package graph

import (
	"math"
	"strconv"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

// DefaultDecayConstant controls how quickly confidence decays with temporal
// distance. A value of 0.25 yields ~0.91 at delta/window=0.367 (22min/60min)
// and ~0.95 at delta/window=0.2 (3min/15min).
const DefaultDecayConstant = 0.25

// TimeFactor computes the exponential decay factor for a given temporal
// distance. Returns exp(-DefaultDecayConstant * deltaMicros/windowMicros).
// Returns 1.0 when deltaMicros <= 0.
func TimeFactor(deltaMicros, windowMicros int64) float64 {
	if deltaMicros <= 0 {
		return 1.0
	}
	ratio := float64(deltaMicros) / float64(windowMicros)
	return math.Exp(-DefaultDecayConstant * ratio)
}

// EdgeWeight reads the causal weight of an edge at the given timestamp.
// It checks the "weight" property first, then falls back to "confidence".
// Returns 1.0 if neither exists or cannot be parsed as float64.
func (g *Graph) EdgeWeight(edgeID string, at hlc.HLC) float64 {
	for _, prop := range []string{"weight", "confidence"} {
		r := g.store.GetAt(g.edgePropKey(edgeID, prop), at)
		if r.Found {
			if w, err := strconv.ParseFloat(string(r.Value), 64); err == nil {
				return w
			}
		}
	}
	return 1.0
}

// CausalConfidence computes the confidence of a causal link as the product
// of the parent's confidence, the time decay factor, and the edge weight.
func CausalConfidence(parentConfidence, timeFactor, edgeWeight float64) float64 {
	return parentConfidence * timeFactor * edgeWeight
}
