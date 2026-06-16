// Package produce implements dasher.Producer over Redis XADD.
// It serialises an Event and publishes it to the raw stream key (no instance prefix).
// Origin is recorded in the envelope "source" field.
// There is NO internal retry — the caller (consume loop) owns retry/escalation.
package produce

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"4gclinical.com/dasher"
)

// Producer implements dasher.Producer by publishing events via Redis XADD.
type Producer struct {
	client     *redis.Client
	instanceID string
	closed     atomic.Bool
}

// New returns a Producer that publishes to the raw stream key (no instance prefix)
// and stamps every envelope with the instanceID as "source".
func New(client *redis.Client, instanceID string) *Producer {
	return &Producer{client: client, instanceID: instanceID}
}

// Close marks the producer as closed. Any subsequent Emit call will return an
// error immediately. Close does NOT close the underlying Redis client — the
// client is owned by the caller (main) and shared with other components.
func (p *Producer) Close() {
	p.closed.Store(true)
}

// Emit publishes evt to the raw stream key via a single ctx-bound XADD, stamping
// "source" with the instanceID so downstream can identify the origin instance.
// Returns an error if the producer has been closed, or the raw Redis error on
// failure — no internal retry.
func (p *Producer) Emit(ctx context.Context, stream string, evt dasher.Event) error {
	if p.closed.Load() {
		return fmt.Errorf("produce: emit after close")
	}

	key := stream

	fields := map[string]any{
		"op":          evt.Op,
		"table":       evt.Table,
		"schema":      evt.Schema,
		"lsn":         evt.LSN,
		"streamed_at": evt.StreamedAt.UTC().Format(time.RFC3339),
		"source":      p.instanceID,
	}

	dataBytes, err := json.Marshal(evt.Data)
	if err != nil {
		return fmt.Errorf("produce: marshal data: %w", err)
	}
	fields["data"] = string(dataBytes)

	if len(evt.Old) > 0 {
		oldBytes, err := json.Marshal(evt.Old)
		if err != nil {
			return fmt.Errorf("produce: marshal old: %w", err)
		}
		fields["old"] = string(oldBytes)
	}

	if len(evt.Enrichment) > 0 {
		enrichBytes, err := json.Marshal(evt.Enrichment)
		if err != nil {
			return fmt.Errorf("produce: marshal enrichment: %w", err)
		}
		fields["enrichment"] = string(enrichBytes)
	}

	return p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		ID:     "*",
		Values: fields,
	}).Err()
}
