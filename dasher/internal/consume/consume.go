// Package consume owns the per-stream consumer-group loop: ensure the group,
// reclaim interrupted work with XAUTOCLAIM, then XREADGROUP new entries,
// dispatch to the handler, and XACK only after success. Transient handler
// errors back-pressure with exponential backoff; poison and malformed
// envelopes are routed through the StreamErrorPolicy seam.
package consume

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"4gclinical.com/dasher"
	"4gclinical.com/dasher/internal/event"
)

// defaultBlockDuration is how long XREADGROUP blocks waiting for new entries
// when the stream is empty. A non-zero value lets the loop check ctx
// cancellation regularly without busy-waiting.
const defaultBlockDuration = 2 * time.Second

// defaultFetchCount is the maximum number of entries fetched per XREADGROUP
// or XAUTOCLAIM call.
const defaultFetchCount int64 = 10

// Consumer reads one Redis stream via a consumer group.
type Consumer struct {
	rdb           *redis.Client
	stream        string // full key, e.g. "bayer-17909.cdc.orders"
	group         string // "dasher"
	name          string // consumer name = process identity (hostname/pod)
	handler       dasher.Handler
	inst          dasher.InstanceContext
	policy        dasher.StreamErrorPolicy
	escalateAfter int
	block         time.Duration
	count         int64
}

// New builds a Consumer.
func New(rdb *redis.Client, stream, group, name string, h dasher.Handler,
	inst dasher.InstanceContext, policy dasher.StreamErrorPolicy, escalateAfter int) *Consumer {
	return &Consumer{
		rdb: rdb, stream: stream, group: group, name: name, handler: h,
		inst: inst, policy: policy, escalateAfter: escalateAfter,
		block: defaultBlockDuration, count: defaultFetchCount,
	}
}

// Run ensures the group, reclaims pending work, then loops reading new entries
// until ctx is cancelled or a fatal error occurs. A returned non-nil error is
// fail-loud: the caller's errgroup cancels siblings and the process exits.
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.ensureGroup(ctx); err != nil {
		return err
	}
	if err := c.reclaim(ctx); err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.name,
			Streams:  []string{c.stream, ">"},
			Count:    c.count,
			Block:    c.block,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // BLOCK timeout with no new entries
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("XREADGROUP %s: %w", c.stream, err)
		}
		for _, s := range res {
			for _, m := range s.Messages {
				if err := c.process(ctx, m); err != nil {
					return err
				}
			}
		}
	}
}

func (c *Consumer) ensureGroup(ctx context.Context) error {
	err := c.rdb.XGroupCreateMkStream(ctx, c.stream, c.group, "$").Err()
	if err != nil {
		// BUSYGROUP means the group already exists — normal on restart.
		// Redis protocol errors begin with the error code, so HasPrefix is
		// precise enough without requiring the full message text.
		if strings.HasPrefix(err.Error(), "BUSYGROUP") {
			return nil
		}
		return fmt.Errorf("XGROUP CREATE %s: %w", c.stream, err)
	}
	return nil
}

// reclaim re-processes entries delivered to this consumer but never acked
// (a previous process crashed before XACK).
func (c *Consumer) reclaim(ctx context.Context) error {
	start := "0-0"
	for {
		msgs, next, err := c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   c.stream,
			Group:    c.group,
			Consumer: c.name,
			// MinIdle: 0 reclaims all PEL entries for this consumer from a
			// previous incarnation (crash before XACK). Safe in model-B where
			// each stream has exactly one consumer. For model-C (multiple
			// consumers per stream) change to a non-zero idle window (e.g.
			// 30s) to avoid stealing in-flight entries from running peers.
			MinIdle:  0,
			Start:    start,
			Count:    c.count,
		}).Result()
		if err != nil {
			return fmt.Errorf("XAUTOCLAIM %s: %w", c.stream, err)
		}
		for _, m := range msgs {
			if err := c.process(ctx, m); err != nil {
				return err
			}
		}
		if next == "0-0" || next == "" {
			return nil
		}
		start = next
	}
}

// process dispatches one entry: parse (malformed → fatal), invoke the handler,
// XACK on success, retry transient errors with backoff (escalating the log
// level after escalateAfter consecutive retries), and route poison to the
// StreamErrorPolicy.
func (c *Consumer) process(ctx context.Context, m redis.XMessage) error {
	evt, err := event.Parse(m.ID, m.Values)
	if err != nil {
		return c.policy.OnFatal(c.stream, fmt.Errorf("malformed envelope %s: %w", m.ID, err))
	}

	backoff := 100 * time.Millisecond
	retries := 0
	for {
		herr := c.handler.Handle(ctx, c.inst, evt)
		if herr == nil {
			return c.rdb.XAck(ctx, c.stream, c.group, m.ID).Err()
		}
		if dasher.IsPoison(herr) {
			return c.policy.OnFatal(c.stream, fmt.Errorf("poison %s: %w", m.ID, herr))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		retries++
		level := slog.LevelWarn
		if retries >= c.escalateAfter {
			level = slog.LevelError
		}
		slog.Log(ctx, level, "handler transient error, retrying",
			"stream", c.stream, "id", m.ID, "retries", retries, "backoff", backoff, "err", herr)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}
