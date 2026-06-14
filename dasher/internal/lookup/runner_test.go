package lookup_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher/internal/lookup"
)

// fakeLookup is a test-only Lookup.
type fakeLookup struct {
	rows  []lookup.Row
	err   error
	calls int
}

func (f *fakeLookup) Resolve(_ context.Context, _ map[string]any) ([]lookup.Row, error) {
	f.calls++
	return f.rows, f.err
}

func makeRule(fl *fakeLookup) lookup.EnrichRule {
	return lookup.EnrichRule{
		LookupName: "test_lookup",
		Lookup:     fl,
		Bind:       map[string]string{"user_id": "id"},
		Into:       "user",
	}
}

func TestRunner_MissResolvePopulate(t *testing.T) {
	fl := &fakeLookup{rows: []lookup.Row{{"email": "bob@example.com"}}}
	runner := lookup.NewRunner([]lookup.EnrichRule{makeRule(fl)})
	result, err := runner.Run(context.Background(), map[string]any{"id": "bob"}, nil)
	require.NoError(t, err)
	assert.Equal(t, lookup.Row{"email": "bob@example.com"}, result["user"])
}

func TestRunner_ZeroRows_Poison(t *testing.T) {
	fl := &fakeLookup{rows: nil}
	runner := lookup.NewRunner([]lookup.EnrichRule{makeRule(fl)})
	_, err := runner.Run(context.Background(), map[string]any{"id": "x"}, nil)
	require.Error(t, err)
	assert.True(t, lookup.IsPoison(err))
}

func TestRunner_MultiRow_Poison(t *testing.T) {
	fl := &fakeLookup{rows: []lookup.Row{{"a": 1}, {"a": 2}}}
	runner := lookup.NewRunner([]lookup.EnrichRule{makeRule(fl)})
	_, err := runner.Run(context.Background(), map[string]any{"id": "x"}, nil)
	require.Error(t, err)
	assert.True(t, lookup.IsPoison(err))
}

func TestRunner_NilBind_Poison(t *testing.T) {
	fl := &fakeLookup{rows: []lookup.Row{{"email": "x"}}}
	runner := lookup.NewRunner([]lookup.EnrichRule{makeRule(fl)})
	// No "id" in data or old → nil bind → poison
	assert.Equal(t, 0, fl.calls, "Resolve must NOT be called when bind is nil")
	_, err := runner.Run(context.Background(), map[string]any{}, nil)
	require.Error(t, err)
	assert.True(t, lookup.IsPoison(err))
}

func TestRunner_TransientError_Propagates(t *testing.T) {
	fl := &fakeLookup{err: errors.New("db timeout")}
	runner := lookup.NewRunner([]lookup.EnrichRule{makeRule(fl)})
	_, err := runner.Run(context.Background(), map[string]any{"id": "x"}, nil)
	require.Error(t, err)
	assert.False(t, lookup.IsPoison(err))
	assert.Contains(t, err.Error(), "db timeout")
}
