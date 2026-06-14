package lookup_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher/internal/lookup"
)

func TestCachedLookup_HitAfterFirstResolve(t *testing.T) {
	fl := &fakeLookup{rows: []lookup.Row{{"email": "alice@example.com"}}}
	cached := lookup.NewCachedLookup(fl, time.Minute)
	bind := map[string]any{"id": "alice"}

	// First call: cache miss → resolve
	rows, err := cached.Resolve(context.Background(), bind)
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, 1, fl.calls)

	// Second call: cache hit → no resolve
	rows2, err := cached.Resolve(context.Background(), bind)
	require.NoError(t, err)
	assert.Equal(t, rows, rows2)
	assert.Equal(t, 1, fl.calls, "second call must use cache")
}

func TestCachedLookup_NoTTL_NoCache(t *testing.T) {
	fl := &fakeLookup{rows: []lookup.Row{{"x": 1}}}
	// TTL=0 → NewCachedLookup returns inner unwrapped
	l := lookup.NewCachedLookup(fl, 0)
	bind := map[string]any{"id": "a"}

	_, _ = l.Resolve(context.Background(), bind)
	_, _ = l.Resolve(context.Background(), bind)
	assert.Equal(t, 2, fl.calls, "no caching when TTL is zero")
}

func TestCachedLookup_ZeroRows_NotCached(t *testing.T) {
	fl := &fakeLookup{rows: nil}
	cached := lookup.NewCachedLookup(fl, time.Minute)
	bind := map[string]any{"id": "missing"}

	_, _ = cached.Resolve(context.Background(), bind)
	_, _ = cached.Resolve(context.Background(), bind)
	assert.Equal(t, 2, fl.calls, "zero-row results must not be cached")
}

func TestCachedLookup_MultiRow_NotCached(t *testing.T) {
	fl := &fakeLookup{rows: []lookup.Row{{"a": 1}, {"a": 2}}}
	cached := lookup.NewCachedLookup(fl, time.Minute)
	bind := map[string]any{"id": "dup"}

	_, _ = cached.Resolve(context.Background(), bind)
	_, _ = cached.Resolve(context.Background(), bind)
	assert.Equal(t, 2, fl.calls, "multi-row results must not be cached")
}

func TestCachedLookup_TransientError_NotCached(t *testing.T) {
	fl := &fakeLookup{rows: nil, err: assert.AnError}
	cached := lookup.NewCachedLookup(fl, time.Minute)
	bind := map[string]any{"id": "x"}

	_, err := cached.Resolve(context.Background(), bind)
	assert.Error(t, err)
	_, err = cached.Resolve(context.Background(), bind)
	assert.Error(t, err)
	assert.Equal(t, 2, fl.calls, "errors must not be cached")
}
