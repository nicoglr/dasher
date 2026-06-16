package produce_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher"
	"4gclinical.com/dasher/internal/produce"
)

func newTestProducer(t *testing.T) (*produce.Producer, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return produce.New(client, "test-instance"), mr
}

func baseEvent() dasher.Event {
	return dasher.Event{
		ID:         "1-0",
		Op:         "insert",
		Table:      "users",
		Schema:     "public",
		LSN:        "0/1234",
		StreamedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Data:       map[string]any{"id": json.Number("1"), "email": "alice@example.com"},
	}
}

func TestEmitWritesEnvelope(t *testing.T) {
	p, mr := newTestProducer(t)
	ctx := context.Background()
	evt := baseEvent()
	evt.Old = map[string]any{"id": json.Number("1")}
	evt.Enrichment = map[string]any{"user": map[string]any{"role": "admin"}}

	err := p.Emit(ctx, "events", evt)
	require.NoError(t, err)

	// Read back from miniredis
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	msgs, err := client.XRange(ctx, "events", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	v := msgs[0].Values
	assert.Equal(t, "insert", v["op"])
	assert.Equal(t, "users", v["table"])
	assert.Equal(t, "public", v["schema"])
	assert.Equal(t, "0/1234", v["lsn"])
	assert.Equal(t, "2026-01-02T03:04:05Z", v["streamed_at"])
	assert.Contains(t, v["data"], "alice@example.com")
	assert.Contains(t, v["old"], `"id"`)
	assert.Contains(t, v["enrichment"], "admin")
	assert.Equal(t, "test-instance", v["source"])
}

func TestEmitOmitsEmptyOldAndEnrichment(t *testing.T) {
	p, mr := newTestProducer(t)
	ctx := context.Background()

	err := p.Emit(ctx, "events", baseEvent())
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	msgs, err := client.XRange(ctx, "events", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	_, hasOld := msgs[0].Values["old"]
	_, hasEnrichment := msgs[0].Values["enrichment"]
	assert.False(t, hasOld)
	assert.False(t, hasEnrichment)
}

func TestEmitAfterCloseReturnsError(t *testing.T) {
	p, _ := newTestProducer(t)
	p.Close()

	err := p.Emit(context.Background(), "events", baseEvent())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "emit after close")
}

func TestEmitAfterCloseDoesNotPublish(t *testing.T) {
	p, mr := newTestProducer(t)
	p.Close()

	_ = p.Emit(context.Background(), "events", baseEvent())

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	msgs, err := client.XRange(context.Background(), "events", "-", "+").Result()
	require.NoError(t, err)
	assert.Empty(t, msgs, "no message should be published after Close")
}

func TestEmitReturnsErrorNoRetry(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	p := produce.New(client, "inst")
	mr.Close() // shut down — next call must return error immediately

	err := p.Emit(context.Background(), "stream", baseEvent())
	assert.Error(t, err)
}
