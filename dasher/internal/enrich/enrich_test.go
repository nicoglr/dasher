package enrich_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher"
	"4gclinical.com/dasher/internal/enrich"
	"4gclinical.com/dasher/internal/lookup"
)

// ---- Fakes ----

type captureHandler struct {
	called bool
	last   dasher.Event
	err    error
}

func (h *captureHandler) Handle(_ context.Context, _ dasher.InstanceContext, evt dasher.Event) error {
	h.called = true
	h.last = evt
	return h.err
}

type fakeProducer struct {
	called bool
	last   dasher.Event
	err    error
}

func (p *fakeProducer) Emit(_ context.Context, _ string, evt dasher.Event) error {
	p.called = true
	p.last = evt
	return p.err
}

func makeRunner(rows []lookup.Row, runErr error) *lookup.Runner {
	fl := &fakeLookup{rows: rows, err: runErr}
	rule := lookup.EnrichRule{
		LookupName: "test",
		Lookup:     fl,
		Bind:       map[string]string{"user_id": "id"},
		Into:       "user",
		OnMiss:     lookup.OnMissEmitUnenriched,
	}
	return lookup.NewRunner([]lookup.EnrichRule{rule}, lookup.NewCache(10))
}

type fakeLookup struct {
	rows []lookup.Row
	err  error
}

func (f *fakeLookup) Resolve(_ context.Context, _ map[string]any) ([]lookup.Row, error) {
	return f.rows, f.err
}

func baseEvent() dasher.Event {
	return dasher.Event{
		Op:    "insert",
		Table: "users",
		LSN:   "0/1",
		Data:  map[string]any{"id": "alice"},
	}
}

// ---- Enrich tests ----

func TestEnrich_MergesEnrichmentOntoEvent(t *testing.T) {
	runner := makeRunner([]lookup.Row{{"email": "alice@example.com"}}, nil)
	inner := &captureHandler{}
	h := enrich.Enrich(runner, inner)

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
	// on_miss=fail + no rows → ErrPoison from runner
	fl := &fakeLookup{rows: nil, err: nil}
	rule := lookup.EnrichRule{
		LookupName: "test",
		Lookup:     fl,
		Bind:       map[string]string{"user_id": "id"},
		Into:       "user",
		OnMiss:     lookup.OnMissFail,
	}
	runner := lookup.NewRunner([]lookup.EnrichRule{rule}, lookup.NewCache(10))
	inner := &captureHandler{}
	h := enrich.Enrich(runner, inner)

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
		OnMiss:     lookup.OnMissEmitUnenriched,
	}
	runner := lookup.NewRunner([]lookup.EnrichRule{rule}, lookup.NewCache(10))
	inner := &captureHandler{}
	h := enrich.Enrich(runner, inner)

	err := h.Handle(context.Background(), dasher.InstanceContext{}, baseEvent())
	require.Error(t, err)
	assert.False(t, dasher.IsPoison(err))
	assert.False(t, inner.called)
}

// ---- EmitAfter tests ----

func TestEmitAfter_InnerSuccess_Emits(t *testing.T) {
	inner := &captureHandler{}
	prod := &fakeProducer{}
	h := enrich.EmitAfter(prod, "downstream", inner)

	evt := baseEvent()
	err := h.Handle(context.Background(), dasher.InstanceContext{}, evt)
	require.NoError(t, err)
	assert.True(t, inner.called)
	assert.True(t, prod.called)
}

func TestEmitAfter_InnerError_NoEmit(t *testing.T) {
	inner := &captureHandler{err: errors.New("side effect failed")}
	prod := &fakeProducer{}
	h := enrich.EmitAfter(prod, "downstream", inner)

	err := h.Handle(context.Background(), dasher.InstanceContext{}, baseEvent())
	require.Error(t, err)
	assert.False(t, prod.called)
}

func TestEmitAfter_ProducerError_Returned(t *testing.T) {
	inner := &captureHandler{}
	prod := &fakeProducer{err: errors.New("redis down")}
	h := enrich.EmitAfter(prod, "downstream", inner)

	err := h.Handle(context.Background(), dasher.InstanceContext{}, baseEvent())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redis down")
}

// ---- Composed: Enrich(EmitAfter(Noop)) = pure transform ----

func TestComposed_PureTransform_EmitsWithEnrichment(t *testing.T) {
	runner := makeRunner([]lookup.Row{{"email": "alice@example.com"}}, nil)
	prod := &fakeProducer{}
	h := enrich.Enrich(runner, enrich.EmitAfter(prod, "enriched.users", dasher.Noop))

	err := h.Handle(context.Background(), dasher.InstanceContext{}, baseEvent())
	require.NoError(t, err)
	require.True(t, prod.called)
	assert.Equal(t, lookup.Row{"email": "alice@example.com"}, prod.last.Enrichment["user"])
}
