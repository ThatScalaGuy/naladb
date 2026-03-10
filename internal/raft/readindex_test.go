package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadIndex_LeaderReturnsCurrentValue(t *testing.T) {
	c := newTestCluster(t)

	_, err := c.Set("ri-key", []byte("ri-value"))
	require.NoError(t, err)

	result, err := c.ReadIndex("ri-key")
	require.NoError(t, err)
	assert.True(t, result.Found)
	assert.Equal(t, []byte("ri-value"), result.Value)
}

func TestReadIndex_KeyNotFoundReturnsEmptyResult(t *testing.T) {
	c := newTestCluster(t)

	result, err := c.ReadIndex("nonexistent")
	require.NoError(t, err)
	assert.False(t, result.Found)
}

func TestIsStaleBeyond_LeaderNeverStale(t *testing.T) {
	c := newTestCluster(t)
	assert.False(t, c.IsStaleBeyond(0))
}

func TestGetWithMaxStale_LeaderAlwaysLocal(t *testing.T) {
	c := newTestCluster(t)

	_, err := c.Set("ms-key", []byte("ms-value"))
	require.NoError(t, err)

	result, err := c.GetWithMaxStale("ms-key", 0)
	require.NoError(t, err)
	assert.True(t, result.Found)
	assert.Equal(t, []byte("ms-value"), result.Value)
}

func TestGet_BoundedStale_OnLeader(t *testing.T) {
	c := newTestCluster(t)

	_, err := c.Set("bs-key", []byte("bs-value"))
	require.NoError(t, err)

	result, err := c.Get("bs-key", BoundedStale)
	require.NoError(t, err)
	assert.True(t, result.Found)
	assert.Equal(t, []byte("bs-value"), result.Value)
}
