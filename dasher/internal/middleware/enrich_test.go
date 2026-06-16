package middleware_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher"
	"4gclinical.com/dasher/internal/middleware"
	"4gclinical.com/dasher/internal/lookup"
)

func makeRunner(rows []lookup.Row, runErr error) *lookup.Runner {
	fl := &fakeLookup{rows: rows, err: runErr}
	rule := lookup.EnrichRule{
		LookupName: "test",
		Lookup:     fl,
		Bind:       map[string]string{"user_id": "id"},
		Into:       "user",
	}
	return lookup.NewRunner([]lookup.EnrichRule{rule})
}

type fakeLookup struct {
	rows []lookup.Row
	err  error
}

func (f *fakeLookup) Resolve(_ context.Context, _ map[string]any) ([]lookup.Row, error) {
	return f.rows, f.err
}

// ---- Enrich tests ----

func TestEnrich_MergesEnrichmentOntoEvent(t *testing.T) {
	runner := makeRunner([]lookup.Row{{"email": "alice@example.com"}}, nil)
	inner := &captureHandler{}
	h := middleware.Enrich(runner, inner)

	err := h.Handle(context.Background(), dasher.InstanceContext{}, baseEvent())
	require.NoError(t, err)
	require.True(t, inner.called)
	assert.Equal(t, lookup.Row{"email": "alice@example.com"}, inner.last.Enrichment["user"])
	// Original fields preserved
	assert.Equal(t, "insert", inner.last.Op)
	assert.Equal(t, "users", inner.last.Table)
	assert.Equal(t, "0/1", inner.last.LSN)
}

func TestEnrich_RunnerPoison_InnerNotCalled(t *testing.T) {
	// zero rows → ErrPoison from runner
	fl := &fakeLookup{rows: nil, err: nil}
	rule := lookup.EnrichRule{
		LookupName: "test",
		Lookup:     fl,
		Bind:       map[string]string{"user_id": "id"},
		Into:       "user",
	}
	runner := lookup.NewRunner([]lookup.EnrichRule{rule})
	inner := &captureHandler{}
	h := middleware.Enrich(runner, inner)

	err := h.Handle(context.Background(), dasher.InstanceContext{}, baseEvent())
	require.Error(t, err)
	assert.True(t, dasher.IsPoison(err), "ErrPoison must be wrapped as dasher.Poison")
	assert.False(t, inner.called)
}

func TestEnrich_TransientError_Returned(t *testing.T) {
	fl := &fakeLookup{err: errors.New("db down")}
	rule := lookup.EnrichRule{
		LookupName: "test",
		Lookup:     fl,
		Bind:       map[string]string{"user_id": "id"},
		Into:       "user",
	}
	runner := lookup.NewRunner([]lookup.EnrichRule{rule})
	inner := &captureHandler{}
	h := middleware.Enrich(runner, inner)

	err := h.Handle(context.Background(), dasher.InstanceContext{}, baseEvent())
	require.Error(t, err)
	assert.False(t, dasher.IsPoison(err))
	assert.False(t, inner.called)
}

// ---- Composed: Enrich(EmitAfter(Noop)) = pure transform ----

func TestComposed_PureTransform_EmitsWithEnrichment(t *testing.T) {
	runner := makeRunner([]lookup.Row{{"email": "alice@example.com"}}, nil)
	prod := &fakeProducer{}
	h := middleware.Enrich(runner, middleware.EmitAfter(prod, "enriched.users", dasher.Noop))

	err := h.Handle(context.Background(), dasher.InstanceContext{}, baseEvent())
	require.NoError(t, err)
	require.True(t, prod.called)
	assert.Equal(t, lookup.Row{"email": "alice@example.com"}, prod.last.Enrichment["user"])
}
