package lookup_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher/internal/lookup"
)

func TestCacheHit(t *testing.T) {
	c := lookup.NewCache(10)
	rows := []lookup.Row{{"id": int64(1)}}
	c.Set("key1", rows, time.Minute)

	got, ok := c.Get("key1")
	require.True(t, ok)
	assert.Equal(t, rows, got)
}

func TestCacheExpiry(t *testing.T) {
	c := lookup.NewCache(10)
	c.Set("key1", []lookup.Row{{"x": 1}}, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	_, ok := c.Get("key1")
	assert.False(t, ok)
}

func TestCacheLRUEviction(t *testing.T) {
	c := lookup.NewCache(3)
	c.Set("a", nil, time.Minute)
	c.Set("b", nil, time.Minute)
	c.Set("c", nil, time.Minute)
	assert.Equal(t, 3, c.Len())

	// Access "a" to make it recently used
	_, _ = c.Get("a")

	// Add "d" — should evict LRU (b or c, not a)
	c.Set("d", nil, time.Minute)
	assert.Equal(t, 3, c.Len())
	_, hasA := c.Get("a")
	assert.True(t, hasA, "a should still be cached (recently used)")
}

func TestCacheJitterBounds(t *testing.T) {
	c := lookup.NewCache(10)
	ttl := time.Minute
	// Insert and immediately read back; should still be present
	c.Set("k", []lookup.Row{{}}, ttl)
	_, ok := c.Get("k")
	assert.True(t, ok, "entry with 1-min TTL should still be valid immediately")

	// An extremely short TTL should expire quickly
	c.Set("k2", []lookup.Row{{}}, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_, ok = c.Get("k2")
	assert.False(t, ok, "short TTL entry should expire")
}

func TestMakeCacheKey(t *testing.T) {
	k1 := lookup.MakeCacheKey(map[string]any{"a": 1, "b": 2})
	k2 := lookup.MakeCacheKey(map[string]any{"b": 2, "a": 1})
	assert.Equal(t, k1, k2, "key must be stable regardless of map iteration order")
}
