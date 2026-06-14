package lookup_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher/internal/lookup"
)

// fakeLookup is a test-only Lookup.
type fakeLookup struct {
	rows []lookup.Row
	err  error
	calls int
}

func (f *fakeLookup) Resolve(_ context.Context, _ map[string]any) ([]lookup.Row, error) {
	f.calls++
	return f.rows, f.err
}

func makeRule(fl *fakeLookup, onMiss lookup.OnMiss) lookup.EnrichRule {
	return lookup.EnrichRule{
		LookupName: "test_lookup",
		Lookup:     fl,
		Bind:       map[string]string{"user_id": "id"},
		Into:       "user",
		OnMiss:     onMiss,
		CacheTTL:   time.Minute,
	}
}

func TestRunner_CacheHit(t *testing.T) {
	fl := &fakeLookup{rows: []lookup.Row{{"email": "alice@example.com"}}}
	cache := lookup.NewCache(10)
	runner := lookup.NewRunner([]lookup.EnrichRule{makeRule(fl, lookup.OnMissEmitUnenriched)}, cache)
	ctx := context.Background()
	data := map[string]any{"id": "alice"}

	// First call: cache miss → resolve
	result, err := runner.Run(ctx, data, nil)
	require.NoError(t, err)
	assert.Equal(t, lookup.Row{"email": "alice@example.com"}, result["user"])
	assert.Equal(t, 1, fl.calls)

	// Second call: cache hit → no resolve
	result2, err := runner.Run(ctx, data, nil)
	require.NoError(t, err)
	assert.Equal(t, result, result2)
	assert.Equal(t, 1, fl.calls, "second call should use cache")
}

func TestRunner_MissResolvePopulate(t *testing.T) {
	fl := &fakeLookup{rows: []lookup.Row{{"email": "bob@example.com"}}}
	runner := lookup.NewRunner([]lookup.EnrichRule{makeRule(fl, lookup.OnMissEmitUnenriched)}, lookup.NewCache(10))
	result, err := runner.Run(context.Background(), map[string]any{"id": "bob"}, nil)
	require.NoError(t, err)
	assert.Equal(t, lookup.Row{"email": "bob@example.com"}, result["user"])
}

func TestRunner_ZeroRows_EmitUnenriched(t *testing.T) {
	fl := &fakeLookup{rows: nil}
	runner := lookup.NewRunner([]lookup.EnrichRule{makeRule(fl, lookup.OnMissEmitUnenriched)}, lookup.NewCache(10))
	result, err := runner.Run(context.Background(), map[string]any{"id": "x"}, nil)
	require.NoError(t, err)
	val, exists := result["user"]
	assert.True(t, exists)
	assert.Nil(t, val)
}

func TestRunner_ZeroRows_Fail(t *testing.T) {
	fl := &fakeLookup{rows: nil}
	runner := lookup.NewRunner([]lookup.EnrichRule{makeRule(fl, lookup.OnMissFail)}, lookup.NewCache(10))
	_, err := runner.Run(context.Background(), map[string]any{"id": "x"}, nil)
	require.Error(t, err)
	assert.True(t, lookup.IsPoison(err))
}

func TestRunner_MultiRow_Poison(t *testing.T) {
	fl := &fakeLookup{rows: []lookup.Row{{"a": 1}, {"a": 2}}}
	runner := lookup.NewRunner([]lookup.EnrichRule{makeRule(fl, lookup.OnMissEmitUnenriched)}, lookup.NewCache(10))
	_, err := runner.Run(context.Background(), map[string]any{"id": "x"}, nil)
	require.Error(t, err)
	assert.True(t, lookup.IsPoison(err))
}

func TestRunner_NilBind_ShortCircuitsWithoutResolve(t *testing.T) {
	fl := &fakeLookup{rows: []lookup.Row{{"email": "x"}}}
	runner := lookup.NewRunner([]lookup.EnrichRule{makeRule(fl, lookup.OnMissEmitUnenriched)}, lookup.NewCache(10))
	// No "id" in data or old → nil bind
	result, err := runner.Run(context.Background(), map[string]any{}, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, fl.calls, "Resolve must NOT be called when bind is nil")
	assert.Nil(t, result["user"])
}

func TestRunner_TransientError_Propagates(t *testing.T) {
	fl := &fakeLookup{err: errors.New("db timeout")}
	runner := lookup.NewRunner([]lookup.EnrichRule{makeRule(fl, lookup.OnMissEmitUnenriched)}, lookup.NewCache(10))
	_, err := runner.Run(context.Background(), map[string]any{"id": "x"}, nil)
	require.Error(t, err)
	assert.False(t, lookup.IsPoison(err))
	assert.Contains(t, err.Error(), "db timeout")
}
