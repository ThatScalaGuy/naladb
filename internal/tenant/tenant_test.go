package tenant_test

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	ngrpc "github.com/thatscalaguy/naladb/internal/grpc"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
	"github.com/thatscalaguy/naladb/internal/tenant"
)

// tenantEnv holds a running tenant-aware gRPC server and clients for testing.
type tenantEnv struct {
	srv      *ngrpc.Server
	conn     *grpc.ClientConn
	kv       pb.KVServiceClient
	graph    pb.GraphServiceClient
	store    *store.Store
	clock    *hlc.Clock
	mgr      *tenant.Manager
	registry *tenant.Registry
}

func newTenantEnv(t *testing.T) *tenantEnv {
	t.Helper()

	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)

	reg := tenant.NewRegistry()
	mgr := tenant.NewManager(s, clock, reg)

	srv := ngrpc.NewTenantServer(s, clock, mgr)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = srv.ServeListener(lis)
	}()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})

	return &tenantEnv{
		srv:      srv,
		conn:     conn,
		kv:       pb.NewKVServiceClient(conn),
		graph:    pb.NewGraphServiceClient(conn),
		store:    s,
		clock:    clock,
		mgr:      mgr,
		registry: reg,
	}
}

// ctxWithTenant returns a context with tenant metadata set.
func ctxWithTenant(tenantID string) context.Context {
	md := metadata.Pairs("x-tenant-id", tenantID)
	return metadata.NewOutgoingContext(context.Background(), md)
}

// ---------------------------------------------------------------------------
// Scenario: Tenant-Prefix wird automatisch injiziert
// ---------------------------------------------------------------------------

func TestTenant_PrefixAutomaticallyInjected(t *testing.T) {
	env := newTenantEnv(t)
	ctx := ctxWithTenant("acme-corp")

	// Set a key as tenant "acme-corp".
	_, err := env.kv.Set(ctx, &pb.SetRequest{
		Key:   "node:n1:prop:temp",
		Value: []byte("25.0"),
	})
	require.NoError(t, err)

	// Verify the internal key has the tenant prefix.
	r := env.store.Get("acme-corp:node:n1:prop:temp")
	assert.True(t, r.Found, "key should be stored with tenant prefix")
	assert.Equal(t, []byte("25.0"), r.Value)

	// Verify the UNPREFIXED key does NOT exist.
	r = env.store.Get("node:n1:prop:temp")
	assert.False(t, r.Found, "unprefixed key should not exist")

	// Verify Get returns the value with the prefix stripped.
	getResp, err := env.kv.Get(ctx, &pb.GetRequest{Key: "node:n1:prop:temp"})
	require.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, "node:n1:prop:temp", getResp.Key, "response key should not have prefix")
	assert.Equal(t, []byte("25.0"), getResp.Value)
}

// ---------------------------------------------------------------------------
// Scenario: Tenant kann keine fremden Daten lesen
// ---------------------------------------------------------------------------

func TestTenant_CannotReadOtherTenantsData(t *testing.T) {
	env := newTenantEnv(t)

	ctxAcme := ctxWithTenant("acme")
	ctxOther := ctxWithTenant("other")

	// Tenant "acme" sets a value.
	_, err := env.kv.Set(ctxAcme, &pb.SetRequest{
		Key:   "node:n1:prop:temp",
		Value: []byte("42.0"),
	})
	require.NoError(t, err)

	// Tenant "acme" can read it.
	getResp, err := env.kv.Get(ctxAcme, &pb.GetRequest{Key: "node:n1:prop:temp"})
	require.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, []byte("42.0"), getResp.Value)

	// Tenant "other" CANNOT read it (different prefix).
	getResp, err = env.kv.Get(ctxOther, &pb.GetRequest{Key: "node:n1:prop:temp"})
	require.NoError(t, err)
	assert.False(t, getResp.Found, "other tenant should not see acme's data")
}

// ---------------------------------------------------------------------------
// Scenario: Resource-Limits werden erzwungen
// ---------------------------------------------------------------------------

func TestTenant_QuotaEnforced(t *testing.T) {
	env := newTenantEnv(t)

	// Register tenant with MaxNodes=3.
	env.registry.Register("free-tier", &tenant.Config{
		MaxNodes: 3,
	})

	ctx := ctxWithTenant("free-tier")

	// Create 3 nodes — should succeed.
	for i := 0; i < 3; i++ {
		_, err := env.graph.CreateNode(ctx, &pb.CreateNodeRequest{Type: "sensor"})
		require.NoError(t, err, "node %d should be created", i+1)
	}

	// 4th node — should fail with quota exceeded.
	_, err := env.graph.CreateNode(ctx, &pb.CreateNodeRequest{Type: "sensor"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tenant quota exceeded")
}

// ---------------------------------------------------------------------------
// Scenario: Rate-Limiting pro Tenant
// ---------------------------------------------------------------------------

func TestTenant_RateLimiting(t *testing.T) {
	env := newTenantEnv(t)

	// Register tenant with very low rate limit.
	env.registry.Register("starter", &tenant.Config{
		RateLimit: 5, // 5 writes per second
	})

	ctx := ctxWithTenant("starter")

	// First few writes should succeed (bucket starts full).
	var successCount int
	var failCount int
	for i := 0; i < 20; i++ {
		_, err := env.kv.Set(ctx, &pb.SetRequest{
			Key:   "test-key",
			Value: []byte("v"),
		})
		if err == nil {
			successCount++
		} else {
			failCount++
			assert.Contains(t, err.Error(), "rate limit exceeded")
		}
	}

	// Some should succeed (token bucket started with capacity = rate).
	assert.Greater(t, successCount, 0, "some writes should succeed")
	// Some should be rate-limited.
	assert.Greater(t, failCount, 0, "some writes should be rate-limited")
}

// ---------------------------------------------------------------------------
// Scenario: Tenant-spezifische Retention Policy
// ---------------------------------------------------------------------------

func TestTenant_RetentionPolicy(t *testing.T) {
	env := newTenantEnv(t)

	// Register tenant with retention.
	env.registry.Register("staging", &tenant.Config{
		RetentionDays: 7,
	})

	// Verify the config is retrievable.
	cfg := env.registry.Get("staging")
	require.NotNil(t, cfg)
	assert.Equal(t, 7, cfg.RetentionDays)

	// Verify different tenants have independent configs.
	env.registry.Register("production", &tenant.Config{
		RetentionDays: 365,
	})
	prodCfg := env.registry.Get("production")
	assert.Equal(t, 365, prodCfg.RetentionDays)
	assert.Equal(t, 7, cfg.RetentionDays)
}

// ---------------------------------------------------------------------------
// Scenario: Prometheus-Metriken enthalten Tenant-Label
// (Structural test — actual metrics are AP-17)
// ---------------------------------------------------------------------------

func TestTenant_MetricsLabelSupport(t *testing.T) {
	env := newTenantEnv(t)

	ctxAcme := ctxWithTenant("acme")
	ctxBeta := ctxWithTenant("beta")

	// Each tenant writes 5 keys.
	for i := 0; i < 5; i++ {
		_, err := env.kv.Set(ctxAcme, &pb.SetRequest{
			Key:   "k",
			Value: []byte("v"),
		})
		require.NoError(t, err)

		_, err = env.kv.Set(ctxBeta, &pb.SetRequest{
			Key:   "k",
			Value: []byte("v"),
		})
		require.NoError(t, err)
	}

	// Verify data isolation: acme's data is separate from beta's.
	r := env.store.Get("acme:k")
	assert.True(t, r.Found)
	r = env.store.Get("beta:k")
	assert.True(t, r.Found)

	// Verify tenant ID extraction from server-side context works for metrics labeling.
	// (Client-side contexts use outgoing metadata; server-side interceptor sets WithTenantID.)
	serverCtxAcme := tenant.WithTenantID(context.Background(), "acme")
	serverCtxBeta := tenant.WithTenantID(context.Background(), "beta")
	assert.Equal(t, "acme", tenant.FromContext(serverCtxAcme))
	assert.Equal(t, "beta", tenant.FromContext(serverCtxBeta))
}

// ---------------------------------------------------------------------------
// Scenario: Default-Tenant für Single-Tenant-Deployments
// ---------------------------------------------------------------------------

func TestTenant_DefaultTenant(t *testing.T) {
	env := newTenantEnv(t)

	// No tenant metadata in context.
	ctx := context.Background()

	_, err := env.kv.Set(ctx, &pb.SetRequest{
		Key:   "my-key",
		Value: []byte("hello"),
	})
	require.NoError(t, err)

	// Should be stored under "default:" prefix.
	r := env.store.Get("default:my-key")
	assert.True(t, r.Found, "should be stored under default tenant prefix")
	assert.Equal(t, []byte("hello"), r.Value)

	// Should be readable without explicit tenant.
	getResp, err := env.kv.Get(ctx, &pb.GetRequest{Key: "my-key"})
	require.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, []byte("hello"), getResp.Value)
}

// ---------------------------------------------------------------------------
// Additional: Graph isolation between tenants
// ---------------------------------------------------------------------------

func TestTenant_GraphIsolation(t *testing.T) {
	env := newTenantEnv(t)

	ctxA := ctxWithTenant("tenant-a")
	ctxB := ctxWithTenant("tenant-b")

	// Tenant A creates a node.
	nodeA, err := env.graph.CreateNode(ctxA, &pb.CreateNodeRequest{
		Type:       "device",
		Properties: map[string][]byte{"name": []byte("sensor-1")},
	})
	require.NoError(t, err)

	// Tenant A can read it.
	getResp, err := env.graph.GetNode(ctxA, &pb.GetNodeRequest{Id: nodeA.Id})
	require.NoError(t, err)
	assert.Equal(t, "device", getResp.Type)

	// Tenant B CANNOT read it (separate graph with different prefix).
	_, err = env.graph.GetNode(ctxB, &pb.GetNodeRequest{Id: nodeA.Id})
	assert.Error(t, err, "tenant B should not find tenant A's node")

	// Verify internal keys have tenant prefix.
	r := env.store.Get("tenant-a:node:" + nodeA.Id + ":meta")
	assert.True(t, r.Found, "node should be stored with tenant-a prefix")

	r = env.store.Get("tenant-b:node:" + nodeA.Id + ":meta")
	assert.False(t, r.Found, "node should not exist under tenant-b prefix")
}

// ---------------------------------------------------------------------------
// Additional: Concurrent multi-tenant access
// ---------------------------------------------------------------------------

func TestTenant_ConcurrentAccess(t *testing.T) {
	env := newTenantEnv(t)

	var wg sync.WaitGroup
	tenants := []string{"tenant-1", "tenant-2", "tenant-3"}

	for _, tid := range tenants {
		wg.Add(1)
		go func(tenantID string) {
			defer wg.Done()
			ctx := ctxWithTenant(tenantID)

			for i := 0; i < 20; i++ {
				_, err := env.kv.Set(ctx, &pb.SetRequest{
					Key:   "counter",
					Value: []byte("v"),
				})
				assert.NoError(t, err)
			}

			// Each tenant should read its own data.
			resp, err := env.kv.Get(ctx, &pb.GetRequest{Key: "counter"})
			assert.NoError(t, err)
			assert.True(t, resp.Found)
		}(tid)
	}
	wg.Wait()

	// Verify isolation: each tenant has its own key.
	for _, tid := range tenants {
		r := env.store.Get(tid + ":counter")
		assert.True(t, r.Found, "%s:counter should exist", tid)
	}
}

// ---------------------------------------------------------------------------
// Unit tests for tenant context functions
// ---------------------------------------------------------------------------

func TestFromContext_Default(t *testing.T) {
	ctx := context.Background()
	assert.Equal(t, "default", tenant.FromContext(ctx))
}

func TestFromContext_WithTenant(t *testing.T) {
	ctx := tenant.WithTenantID(context.Background(), "acme")
	assert.Equal(t, "acme", tenant.FromContext(ctx))
}

func TestFromContext_EmptyString(t *testing.T) {
	ctx := tenant.WithTenantID(context.Background(), "")
	assert.Equal(t, "default", tenant.FromContext(ctx))
}

// ---------------------------------------------------------------------------
// Unit tests for quota tracker
// ---------------------------------------------------------------------------

func TestQuotaTracker_Unlimited(t *testing.T) {
	qt := tenant.NewQuotaTracker(nil)
	for i := 0; i < 1000; i++ {
		assert.NoError(t, qt.CheckNode())
		qt.IncrementNodes()
	}
}

func TestQuotaTracker_Enforced(t *testing.T) {
	qt := tenant.NewQuotaTracker(&tenant.Config{MaxNodes: 2})

	assert.NoError(t, qt.CheckNode())
	qt.IncrementNodes()
	assert.NoError(t, qt.CheckNode())
	qt.IncrementNodes()
	assert.ErrorIs(t, qt.CheckNode(), tenant.ErrTenantQuotaExceeded)
}

// ---------------------------------------------------------------------------
// Unit tests for rate limiter
// ---------------------------------------------------------------------------

func TestRateLimiter_Unlimited(t *testing.T) {
	rl := tenant.NewRateLimiter(0)
	for i := 0; i < 1000; i++ {
		assert.NoError(t, rl.Allow())
	}
}

func TestRateLimiter_Limited(t *testing.T) {
	rl := tenant.NewRateLimiter(3) // 3 per second

	// First 3 should succeed (bucket starts full).
	for i := 0; i < 3; i++ {
		assert.NoError(t, rl.Allow(), "request %d should succeed", i)
	}

	// Next should fail (bucket empty).
	assert.ErrorIs(t, rl.Allow(), tenant.ErrRateLimitExceeded)
}

// ---------------------------------------------------------------------------
// Unit tests for registry
// ---------------------------------------------------------------------------

func TestRegistry_Default(t *testing.T) {
	reg := tenant.NewRegistry()
	assert.Nil(t, reg.Get("unknown"))

	reg.SetDefault(&tenant.Config{MaxNodes: 50})
	cfg := reg.Get("unknown")
	require.NotNil(t, cfg)
	assert.Equal(t, int64(50), cfg.MaxNodes)
}

func TestRegistry_Override(t *testing.T) {
	reg := tenant.NewRegistry()
	reg.SetDefault(&tenant.Config{MaxNodes: 50})
	reg.Register("vip", &tenant.Config{MaxNodes: 500})

	assert.Equal(t, int64(50), reg.Get("regular").MaxNodes)
	assert.Equal(t, int64(500), reg.Get("vip").MaxNodes)
}
