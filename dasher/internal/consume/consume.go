// Package consume owns the per-stream consumer-group loop: ensure the group,
// reclaim interrupted work, then XREADGROUP new entries, dispatch to the
// handler, and XACK only after success. Transient handler errors back-pressure
// with exponential backoff; poison and malformed envelopes are routed through
// the StreamErrorPolicy seam.
package consume

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
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
// or XCLAIM call.
const defaultFetchCount int64 = 10

// Default lifecycle tuning parameters.
const (
	defaultReclaimMinIdle     = 30 * time.Second
	defaultReclaimInterval    = 5 * time.Second
	defaultConsumerGCInterval = 5 * time.Minute
	defaultConsumerGCTimeout  = 10 * time.Minute
)

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

	// Consumer-group lifecycle tuning.
	reclaimMinIdle     time.Duration // min idle before peer entries are reclaimed
	reclaimInterval    time.Duration // background peer-reclaim ticker interval
	consumerGCInterval time.Duration // dead-consumer GC ticker interval
	consumerGCTimeout  time.Duration // idle threshold for dead-consumer removal

	// ackFn is the function used to XACK a message. Defaults to rdb.XAck and
	// may be overridden in tests to inject transient failures.
	ackFn func(ctx context.Context, stream, group, id string) error
}

// Option is a functional option for Consumer.
type Option func(*Consumer)

// WithReclaimMinIdle sets the minimum idle time before a peer consumer's PEL
// entries are reclaimed (default 30s). Must be less than WithConsumerGCTimeout.
func WithReclaimMinIdle(d time.Duration) Option {
	return func(c *Consumer) { c.reclaimMinIdle = d }
}

// WithReclaimInterval sets how often the background peer-reclaim ticker fires
// (default 5s).
func WithReclaimInterval(d time.Duration) Option {
	return func(c *Consumer) { c.reclaimInterval = d }
}

// WithBlockDuration overrides the XREADGROUP block timeout (default 2s). Exposed
// for tests that need the main loop to cycle faster.
func WithBlockDuration(d time.Duration) Option {
	return func(c *Consumer) { c.block = d }
}

// WithConsumerGCInterval sets how often the dead-consumer GC ticker fires
// (default 5m).
func WithConsumerGCInterval(d time.Duration) Option {
	return func(c *Consumer) { c.consumerGCInterval = d }
}

// WithConsumerGCTimeout sets the idle threshold above which a consumer with
// zero pending messages is removed from the group (default 10m).
func WithConsumerGCTimeout(d time.Duration) Option {
	return func(c *Consumer) { c.consumerGCTimeout = d }
}

// New builds a Consumer.
func New(rdb *redis.Client, stream, group, name string, h dasher.Handler,
	inst dasher.InstanceContext, policy dasher.StreamErrorPolicy, escalateAfter int,
	opts ...Option) *Consumer {
	c := &Consumer{
		rdb: rdb, stream: stream, group: group, name: name, handler: h,
		inst: inst, policy: policy, escalateAfter: escalateAfter,
		block: defaultBlockDuration, count: defaultFetchCount,
		reclaimMinIdle:     defaultReclaimMinIdle,
		reclaimInterval:    defaultReclaimInterval,
		consumerGCInterval: defaultConsumerGCInterval,
		consumerGCTimeout:  defaultConsumerGCTimeout,
	}
	c.ackFn = func(ctx context.Context, stream, group, id string) error {
		return c.rdb.XAck(ctx, stream, group, id).Err()
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Run ensures the group, reclaims own pending work from a previous crash, then
// loops reading new entries until ctx is cancelled or a fatal error occurs.
//
// Processing is strictly serial: selfReclaim (drain own PEL) runs at the top
// of each iteration before XREADGROUP, so entries claimed from dead peers by
// the background goroutine are picked up on the next loop tick.
//
// A returned non-nil error is fail-loud: the caller's errgroup cancels siblings
// and the process exits.
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.ensureGroup(ctx); err != nil {
		return err
	}

	// Background: steal entries from dead peers (claim-only; processed by
	// selfReclaim at the top of the main loop).
	go c.reclaimLoop(ctx)
	go c.consumerGCLoop(ctx)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Drain own PEL first: picks up crash-recovery entries and any entries
		// claimed from dead peers by the background reclaimLoop.
		if err := c.selfReclaim(ctx); err != nil {
			return err
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
		if strings.HasPrefix(err.Error(), "BUSYGROUP") {
			return nil
		}
		return fmt.Errorf("XGROUP CREATE %s: %w", c.stream, err)
	}
	return nil
}

// selfReclaim claims and processes all entries currently in this consumer's own
// PEL (MinIdle=0 — no idle filter, own entries only). This handles both crash
// recovery (entries from a previous pod incarnation) and entries transferred
// here by the background peerReclaim.
//
// Safe to call with MinIdle=0 because the Consumer filter limits results to
// c.name — there are no live peers to protect against.
func (c *Consumer) selfReclaim(ctx context.Context) error {
	for {
		xps, err := c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream:   c.stream,
			Group:    c.group,
			Start:    "-",
			End:      "+",
			Count:    c.count,
			Consumer: c.name,
			// Idle omitted (zero value) → go-redis omits the IDLE clause →
			// all entries returned regardless of idle time.
		}).Result()
		if err != nil {
			return fmt.Errorf("XPENDING %s: %w", c.stream, err)
		}
		if len(xps) == 0 {
			return nil
		}
		ids := make([]string, len(xps))
		for i, xp := range xps {
			ids[i] = xp.ID
		}
		msgs, err := c.rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   c.stream,
			Group:    c.group,
			Consumer: c.name,
			MinIdle:  0,
			Messages: ids,
		}).Result()
		if err != nil {
			return fmt.Errorf("XCLAIM %s: %w", c.stream, err)
		}
		for _, m := range msgs {
			if err := c.process(ctx, m); err != nil {
				return err
			}
		}
		if int64(len(xps)) < c.count {
			return nil
		}
		// Full page returned — there may be more. Loop again from "-"; processed
		// entries were XACKed so they are gone from the PEL.
	}
}

// peerReclaim claims entries idle ≥ reclaimMinIdle from OTHER consumers into
// this consumer's PEL. It deliberately skips entries owned by c.name (those
// are either being processed right now or waiting for selfReclaim — touching
// them would corrupt their idle timer and inflate delivery counts).
//
// This is claim-only: no process() call. Claimed entries land in c.name's PEL
// and are processed by selfReclaim at the top of the next main loop iteration
// (one-tick lag, typically ≤ reclaimInterval + blockDuration).
func (c *Consumer) peerReclaim(ctx context.Context) error {
	start := "-"
	for {
		xps, err := c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: c.stream,
			Group:  c.group,
			Idle:   c.reclaimMinIdle,
			Start:  start,
			End:    "+",
			Count:  c.count,
		}).Result()
		if err != nil {
			return fmt.Errorf("XPENDING %s: %w", c.stream, err)
		}
		if len(xps) == 0 {
			return nil
		}
		var toClaimIDs []string
		for _, xp := range xps {
			if xp.Consumer != c.name {
				toClaimIDs = append(toClaimIDs, xp.ID)
			}
		}
		if len(toClaimIDs) > 0 {
			if err := c.rdb.XClaim(ctx, &redis.XClaimArgs{
				Stream:   c.stream,
				Group:    c.group,
				Consumer: c.name,
				MinIdle:  c.reclaimMinIdle,
				Messages: toClaimIDs,
			}).Err(); err != nil {
				return fmt.Errorf("XCLAIM peer %s: %w", c.stream, err)
			}
		}
		if int64(len(xps)) < c.count {
			return nil
		}
		// Advance cursor past the last seen entry (exclusive).
		start = "(" + xps[len(xps)-1].ID
	}
}

// reclaimLoop runs peerReclaim on a ticker until ctx is cancelled.
// Errors are logged but not propagated — a transient Redis blip should not
// affect the main processing loop.
func (c *Consumer) reclaimLoop(ctx context.Context) {
	tick := time.NewTicker(c.reclaimInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := c.peerReclaim(ctx); err != nil && ctx.Err() == nil {
				slog.Error("background peer-reclaim failed", "stream", c.stream, "err", err)
			}
		}
	}
}

// consumerGCLoop runs gcConsumers on a ticker until ctx is cancelled.
func (c *Consumer) consumerGCLoop(ctx context.Context) {
	tick := time.NewTicker(c.consumerGCInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := c.gcConsumers(ctx); err != nil && ctx.Err() == nil {
				slog.Error("consumer GC failed", "stream", c.stream, "err", err)
			}
		}
	}
}

// gcConsumers removes consumers that have been idle ≥ consumerGCTimeout and
// have no pending messages. The Pending==0 guard ensures we never delete a
// consumer that still has unprocessed work.
func (c *Consumer) gcConsumers(ctx context.Context) error {
	consumers, err := c.rdb.XInfoConsumers(ctx, c.stream, c.group).Result()
	if err != nil {
		return fmt.Errorf("XINFO CONSUMERS %s: %w", c.stream, err)
	}
	for _, xic := range consumers {
		if xic.Idle >= c.consumerGCTimeout && xic.Pending == 0 {
			if err := c.rdb.XGroupDelConsumer(ctx, c.stream, c.group, xic.Name).Err(); err != nil {
				slog.Warn("XGROUP DELCONSUMER failed",
					"stream", c.stream, "consumer", xic.Name, "err", err)
			} else {
				slog.Info("removed idle consumer",
					"stream", c.stream, "consumer", xic.Name, "idle", xic.Idle)
			}
		}
	}
	return nil
}

// process dispatches one entry: parse (malformed → fatal), invoke the handler,
// XACK on success (retried on transient error), retry transient handler errors
// with backoff, and route poison through StreamErrorPolicy.
//
// A defer/recover wraps the entire body so a panic in the handler is caught,
// logged with a full stack trace, and routed through StreamErrorPolicy.OnFatal
// rather than crashing the goroutine.
func (c *Consumer) process(ctx context.Context, m redis.XMessage) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = c.policy.OnFatal(c.stream,
				fmt.Errorf("panic in handler %s: %v\n%s", m.ID, r, debug.Stack()))
		}
	}()

	evt, err := event.Parse(m.ID, m.Values)
	if err != nil {
		return c.policy.OnFatal(c.stream, fmt.Errorf("malformed envelope %s: %w", m.ID, err))
	}

	backoff := 100 * time.Millisecond
	retries := 0
	for {
		herr := c.handler.Handle(ctx, c.inst, evt)
		if herr == nil {
			// NOTE: while retrying XACK (Redis transiently down), the handled
			// message sits idle in the PEL. If the retry window exceeds
			// reclaimMinIdle, a peer's peerReclaim could steal and re-deliver
			// it. This is a known at-least-once limitation, not a code bug.
			for {
				if err := c.ackFn(ctx, c.stream, c.group, m.ID); err == nil {
					return nil
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				slog.Warn("XACK failed, retrying", "stream", c.stream, "id", m.ID)
				select {
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
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
