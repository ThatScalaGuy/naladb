// Package tenant implements multi-tenancy with key-prefix isolation and quotas.
package tenant

import (
	"context"
	"errors"
	"sync"

	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
)

// Sentinel errors for tenant operations.
var (
	ErrTenantQuotaExceeded = errors.New("naladb: tenant quota exceeded")
	ErrRateLimitExceeded   = errors.New("naladb: rate limit exceeded")
)

// DefaultTenantID is used when no tenant is specified in the request.
const DefaultTenantID = "default"

// contextKey is an unexported type for tenant context values.
type contextKey struct{}

// WithTenantID returns a new context carrying the tenant ID.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, contextKey{}, tenantID)
}

// FromContext extracts the tenant ID from the context.
// Returns DefaultTenantID if no tenant is set.
func FromContext(ctx context.Context) string {
	if id, ok := ctx.Value(contextKey{}).(string); ok && id != "" {
		return id
	}
	return DefaultTenantID
}

// Config holds the resource limits and policies for a tenant.
type Config struct {
	MaxNodes      int64   // maximum number of graph nodes (0 = unlimited)
	MaxEdges      int64   // maximum number of graph edges (0 = unlimited)
	RateLimit     float64 // maximum writes per second (0 = unlimited)
	RetentionDays int     // data retention in days (0 = unlimited)
}

// Registry stores tenant configurations.
type Registry struct {
	mu         sync.RWMutex
	tenants    map[string]*Config
	defaultCfg *Config
}

// NewRegistry creates a new tenant registry.
func NewRegistry() *Registry {
	return &Registry{
		tenants: make(map[string]*Config),
	}
}

// SetDefault sets the default tenant configuration used when a tenant
// has no explicit configuration.
func (r *Registry) SetDefault(cfg *Config) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaultCfg = cfg
}

// Register adds or updates a tenant configuration.
func (r *Registry) Register(tenantID string, cfg *Config) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tenants[tenantID] = cfg
}

// Get returns the configuration for a tenant. Falls back to the default
// configuration if the tenant is not explicitly registered. Returns nil
// if neither is configured.
func (r *Registry) Get(tenantID string) *Config {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if cfg, ok := r.tenants[tenantID]; ok {
		return cfg
	}
	return r.defaultCfg
}

// Manager manages per-tenant resources including graph instances,
// quota trackers, and rate limiters.
type Manager struct {
	mu       sync.RWMutex
	store    *store.Store
	clock    *hlc.Clock
	registry *Registry
	graphs   map[string]*graph.Graph
	quotas   map[string]*QuotaTracker
	limiters map[string]*RateLimiter
}

// NewManager creates a new tenant manager.
func NewManager(s *store.Store, c *hlc.Clock, reg *Registry) *Manager {
	return &Manager{
		store:    s,
		clock:    c,
		registry: reg,
		graphs:   make(map[string]*graph.Graph),
		quotas:   make(map[string]*QuotaTracker),
		limiters: make(map[string]*RateLimiter),
	}
}

// GraphForTenant returns a Graph instance for the given tenant, creating
// one on demand with the appropriate key prefix.
func (m *Manager) GraphForTenant(tenantID string) *graph.Graph {
	m.mu.RLock()
	g, ok := m.graphs[tenantID]
	m.mu.RUnlock()
	if ok {
		return g
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock.
	if g, ok = m.graphs[tenantID]; ok {
		return g
	}

	g = graph.New(m.store, m.clock, graph.WithKeyPrefix(tenantID+":"))
	m.graphs[tenantID] = g
	return g
}

// QuotaForTenant returns the quota tracker for a tenant.
func (m *Manager) QuotaForTenant(tenantID string) *QuotaTracker {
	m.mu.RLock()
	qt, ok := m.quotas[tenantID]
	m.mu.RUnlock()
	if ok {
		return qt
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if qt, ok = m.quotas[tenantID]; ok {
		return qt
	}

	cfg := m.registry.Get(tenantID)
	qt = NewQuotaTracker(cfg)
	m.quotas[tenantID] = qt
	return qt
}

// LimiterForTenant returns the rate limiter for a tenant.
func (m *Manager) LimiterForTenant(tenantID string) *RateLimiter {
	m.mu.RLock()
	rl, ok := m.limiters[tenantID]
	m.mu.RUnlock()
	if ok {
		return rl
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if rl, ok = m.limiters[tenantID]; ok {
		return rl
	}

	cfg := m.registry.Get(tenantID)
	var rateLimit float64
	if cfg != nil {
		rateLimit = cfg.RateLimit
	}
	rl = NewRateLimiter(rateLimit)
	m.limiters[tenantID] = rl
	return rl
}

// Store returns the underlying store.
func (m *Manager) Store() *store.Store {
	return m.store
}

// Registry returns the tenant registry.
func (m *Manager) Registry() *Registry {
	return m.registry
}

// KeyPrefix returns the key prefix for a tenant ID.
func KeyPrefix(tenantID string) string {
	return tenantID + ":"
}
