package query

import (
	"fmt"

	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/meta"
	"github.com/thatscalaguy/naladb/internal/store"
)

// Executor parses NalaQL queries and executes them against the store and graph.
type Executor struct {
	planner *Planner
}

// NewExecutor creates a new query executor.
func NewExecutor(s *store.Store, g *graph.Graph, m *meta.Registry, c *hlc.Clock) *Executor {
	return &Executor{
		planner: NewPlanner(s, g, m, c),
	}
}

// Execute parses and executes a NalaQL query, returning all result rows.
func (e *Executor) Execute(query string) ([]Row, error) {
	stmt, err := Parse(query)
	if err != nil {
		return nil, fmt.Errorf("executor: %w", err)
	}

	return e.ExecuteStatement(stmt)
}

// ExecuteStatement executes a pre-parsed statement.
func (e *Executor) ExecuteStatement(stmt Statement) ([]Row, error) {
	op, err := e.planner.Plan(stmt)
	if err != nil {
		return nil, fmt.Errorf("executor: %w", err)
	}
	defer op.Close()

	return Collect(op)
}

// Collect drains an operator into a slice of rows.
func Collect(op Operator) ([]Row, error) {
	var rows []Row
	for {
		row, ok, err := op.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		rows = append(rows, row)
	}
	return rows, nil
}
