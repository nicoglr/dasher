// Package produce implements dasher.Producer over Redis XADD.
// It serialises an Event and publishes it to "<instanceID>.<stream>".
// There is NO internal retry — the caller (consume loop) owns retry/escalation.
package produce

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"4gclinical.com/dasher"
)

// Producer implements dasher.Producer by publishing events via Redis XADD.
type Producer struct {
	client     *redis.Client
	instanceID string
}

// New returns a Producer that prefixes every stream with "<instanceID>.".
func New(client *redis.Client, instanceID string) *Producer {
	return &Producer{client: client, instanceID: instanceID}
}

// Emit publishes evt to "<instanceID>.<stream>" via a single ctx-bound XADD.
// Returns the raw Redis error on failure — no internal retry.
func (p *Producer) Emit(ctx context.Context, stream string, evt dasher.Event) error {
	key := p.instanceID + "." + stream

	fields := map[string]any{
		"op":          evt.Op,
		"table":       evt.Table,
		"schema":      evt.Schema,
		"lsn":         evt.LSN,
		"streamed_at": evt.StreamedAt.UTC().Format(time.RFC3339),
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
