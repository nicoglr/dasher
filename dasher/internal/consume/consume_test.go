package consume_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"4gclinical.com/dasher"
	"4gclinical.com/dasher/internal/consume"
)

const (
	stream = "bayer-17909.cdc.orders"
	group  = "dasher"
	name   = "pod-1"
)

func newRDB(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb, mr
}

// newRealRDB connects to a real Redis instance at REDIS_ADDR. Skips the test
// if REDIS_ADDR is not set. The client is closed on test cleanup.
func newRealRDB(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set — skipping integration test (run via make test-integration)")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis at %s unreachable: %v", addr, err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// setupGroup pre-creates the consumer group so entries added afterwards are
// visible to XREADGROUP with ">". Must be called before addEntry.
func setupGroup(t *testing.T, rdb *redis.Client) {
	t.Helper()
	if err := rdb.XGroupCreateMkStream(context.Background(), stream, group, "$").Err(); err != nil {
		t.Fatalf("XGroupCreateMkStream: %v", err)
	}
}

func addEntry(t *testing.T, rdb *redis.Client, missingOp bool) {
	t.Helper()
	vals := map[string]any{
		"op":          "insert",
		"table":       "orders",
		"schema":      "public",
		"lsn":         "0/1",
		"streamed_at": "2026-06-12T10:00:00Z",
		"data":        `{"id":1}`,
	}
	if missingOp {
		delete(vals, "op")
	}
	if err := rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: vals}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
}

func pendingCount(t *testing.T, rdb *redis.Client) int64 {
	t.Helper()
	p, err := rdb.XPending(context.Background(), stream, group).Result()
	if err != nil {
		t.Fatalf("XPending: %v", err)
	}
	return p.Count
}

// pendingCountForConsumer returns the number of pending entries owned by a specific consumer.
func pendingCountForConsumer(t *testing.T, rdb *redis.Client, consumer string) int64 {
	t.Helper()
	xps, err := rdb.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream:   stream,
		Group:    group,
		Start:    "-",
		End:      "+",
		Count:    100,
		Consumer: consumer,
	}).Result()
	if err != nil {
		t.Fatalf("XPendingExt(%s): %v", consumer, err)
	}
	return int64(len(xps))
}

type recHandler struct {
	mu     sync.Mutex
	calls  int32
	result error
}

func (h *recHandler) Handle(ctx context.Context, inst dasher.InstanceContext, evt dasher.Event) error {
	atomic.AddInt32(&h.calls, 1)
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.result
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}

// waitForStream is like waitFor but with a 5s deadline (used for integration
// tests that involve real I/O and real-time sleeps).
func waitForStream(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within 5s deadline")
}

// validVals returns a minimal valid CDC entry map.
func validVals() map[string]any {
	return map[string]any{
		"op":          "insert",
		"table":       "orders",
		"schema":      "public",
		"lsn":         "0/1",
		"streamed_at": "2026-06-12T10:00:00Z",
		"data":        `{"id":1}`,
	}
}

// isRedisBusyGroup reports whether err is a BUSYGROUP error.
func isRedisBusyGroup(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP")
}

// deliverToConsumer XREADGROUPs one entry to the named consumer without acking,
// simulating a pod that received the message but crashed before XACK.
// Uses Count=1 and no BLOCK so it never hangs if no entry is available.
func deliverToConsumer(t *testing.T, rdb *redis.Client, consumerName string) {
	t.Helper()
	_, err := rdb.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group: group, Consumer: consumerName,
		Streams: []string{stream, ">"}, Count: 1,
		Block: -1, // non-blocking
	}).Result()
	if err != nil && err != redis.Nil {
		t.Fatalf("seed XReadGroup(%s): %v", consumerName, err)
	}
}

// --- Existing tests (unchanged) ---

func TestAckAfterSuccess(t *testing.T) {
	rdb, _ := newRDB(t)
	setupGroup(t, rdb)
	addEntry(t, rdb, false)
	h := &recHandler{}
	c := consume.New(rdb, stream, group, name, h, dasher.InstanceContext{}, dasher.FailLoud{}, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	waitFor(t, func() bool { return atomic.LoadInt32(&h.calls) >= 1 })
	waitFor(t, func() bool { return pendingCount(t, rdb) == 0 })
}

func TestNoAckOnTransientError(t *testing.T) {
	rdb, _ := newRDB(t)
	setupGroup(t, rdb)
	addEntry(t, rdb, false)
	h := &recHandler{result: errors.New("downstream down")}
	c := consume.New(rdb, stream, group, name, h, dasher.InstanceContext{}, dasher.FailLoud{}, 10)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(ctx) }()
	defer func() { cancel(); <-done }()

	waitFor(t, func() bool { return atomic.LoadInt32(&h.calls) >= 1 })
	cancel()
	<-done
	if got := pendingCount(t, rdb); got != 1 {
		t.Fatalf("entry should remain pending (not acked), got %d", got)
	}
}

func TestReclaimPending(t *testing.T) {
	rdb, _ := newRDB(t)
	ctx := context.Background()
	setupGroup(t, rdb)
	addEntry(t, rdb, false)
	// Deliver to this consumer without acking, simulating a crash before XACK.
	deliverToConsumer(t, rdb, name)
	if pendingCount(t, rdb) != 1 {
		t.Fatal("precondition: entry should be pending")
	}

	h := &recHandler{}
	c := consume.New(rdb, stream, group, name, h, dasher.InstanceContext{}, dasher.FailLoud{}, 10)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = c.Run(runCtx) }()

	waitFor(t, func() bool { return atomic.LoadInt32(&h.calls) >= 1 })
	waitFor(t, func() bool { return pendingCount(t, rdb) == 0 })
}

func TestPoisonIsFatal(t *testing.T) {
	rdb, _ := newRDB(t)
	setupGroup(t, rdb)
	addEntry(t, rdb, false)
	h := &recHandler{result: dasher.Poison(errors.New("rejected"))}
	c := consume.New(rdb, stream, group, name, h, dasher.InstanceContext{}, dasher.FailLoud{}, 10)

	err := c.Run(context.Background())
	if err == nil {
		t.Fatal("expected fail-loud error for poison")
	}
}

func TestMalformedEnvelopeFatal(t *testing.T) {
	rdb, _ := newRDB(t)
	setupGroup(t, rdb)
	addEntry(t, rdb, true) // missing op
	h := &recHandler{}
	c := consume.New(rdb, stream, group, name, h, dasher.InstanceContext{}, dasher.FailLoud{}, 10)

	err := c.Run(context.Background())
	if err == nil {
		t.Fatal("expected fail-loud error for malformed envelope")
	}
}

// --- New tests ---

// TestSelfReclaimOnlyOwn verifies that selfReclaim never touches entries owned
// by other consumers — only its own PEL entries are claimed and processed.
func TestSelfReclaimOnlyOwn(t *testing.T) {
	rdb, _ := newRDB(t)
	ctx := context.Background()
	setupGroup(t, rdb)

	// Interleave addEntry+deliver so each consumer gets exactly one entry.
	// (Adding both first then delivering would give both to the first consumer
	// because XReadGroup ">" delivers all pending-delivery entries at once.)
	addEntry(t, rdb, false)
	deliverToConsumer(t, rdb, name)    // entry 1 → pod-1's PEL
	addEntry(t, rdb, false)
	deliverToConsumer(t, rdb, "pod-2") // entry 2 → pod-2's PEL

	if pendingCountForConsumer(t, rdb, "pod-2") != 1 {
		t.Fatal("precondition: pod-2 should have 1 pending entry")
	}

	h := &recHandler{}
	c := consume.New(rdb, stream, group, name, h, dasher.InstanceContext{}, dasher.FailLoud{}, 10)

	// Call selfReclaim directly — only pod-1's entry should be processed.
	if err := consume.ExposeSelfReclaim(c, ctx); err != nil {
		t.Fatalf("selfReclaim: %v", err)
	}

	// pod-1's entry was processed and XACKed.
	if got := pendingCountForConsumer(t, rdb, name); got != 0 {
		t.Fatalf("pod-1 PEL should be empty after selfReclaim, got %d", got)
	}
	// pod-2's entry must be untouched.
	if got := pendingCountForConsumer(t, rdb, "pod-2"); got != 1 {
		t.Fatalf("pod-2 PEL should still have 1 entry after selfReclaim, got %d", got)
	}
	if atomic.LoadInt32(&h.calls) != 1 {
		t.Fatalf("handler should be called exactly once (for pod-1's entry), got %d", h.calls)
	}
}

// TestBackgroundReclaimLoop verifies that an entry stranded in a dead peer's
// PEL is eventually processed by the background reclaimLoop + next selfReclaim.
// Requires real Redis (XPendingExt idle filter not supported by miniredis).
func TestBackgroundReclaimLoop(t *testing.T) {
	rdb := newRealRDB(t)
	ctx := context.Background()

	// Unique stream per run to avoid cross-test pollution on shared Redis.
	s := stream + "-bg-reclaim"
	t.Cleanup(func() { rdb.Del(ctx, s) })

	if err := rdb.XGroupCreateMkStream(ctx, s, group, "$").Err(); err != nil && !isRedisBusyGroup(err) {
		t.Fatalf("XGroupCreate: %v", err)
	}
	// Add and deliver an entry to "other-pod".
	rdb.XAdd(ctx, &redis.XAddArgs{Stream: s, Values: validVals()})
	rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "other-pod",
		Streams: []string{s, ">"}, Count: 1, Block: -1,
	})

	// Wait for the entry to be idle > reclaimMinIdle.
	const minIdle = 150 * time.Millisecond
	time.Sleep(minIdle + 50*time.Millisecond)

	h := &recHandler{}
	c := consume.New(rdb, s, group, name, h, dasher.InstanceContext{}, dasher.FailLoud{}, 10,
		consume.WithReclaimMinIdle(minIdle),
		consume.WithReclaimInterval(20*time.Millisecond),
		consume.WithBlockDuration(50*time.Millisecond),
	)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = c.Run(runCtx) }()

	waitFor(t, func() bool { return atomic.LoadInt32(&h.calls) >= 1 })
	waitForStream(t, func() bool {
		p, _ := rdb.XPending(ctx, s, group).Result()
		return p.Count == 0
	})
}

// TestPeerReclaimSkipsOwnInFlight verifies that peerReclaim never re-claims
// entries already owned by c.name (i.e., currently in this consumer's PEL).
func TestPeerReclaimSkipsOwnInFlight(t *testing.T) {
	rdb, _ := newRDB(t)
	ctx := context.Background()
	setupGroup(t, rdb)
	addEntry(t, rdb, false)

	// Deliver the entry to pod-1 (simulates it being in-flight).
	deliverToConsumer(t, rdb, name)
	if pendingCountForConsumer(t, rdb, name) != 1 {
		t.Fatal("precondition: entry should be in pod-1's PEL")
	}

	h := &recHandler{}
	c := consume.New(rdb, stream, group, name, h, dasher.InstanceContext{}, dasher.FailLoud{}, 10,
		consume.WithReclaimMinIdle(0), // reclaim anything idle ≥ 0
	)

	// peerReclaim should skip the entry because it belongs to c.name.
	if err := consume.ExposePeerReclaim(c, ctx); err != nil {
		t.Fatalf("peerReclaim: %v", err)
	}

	// Entry is still in pod-1's PEL (not re-claimed or processed).
	if got := pendingCountForConsumer(t, rdb, name); got != 1 {
		t.Fatalf("entry should still be in pod-1 PEL after peerReclaim, got %d", got)
	}
	if atomic.LoadInt32(&h.calls) != 0 {
		t.Fatal("handler should not be called by peerReclaim")
	}
}

// TestConsumerGC verifies that a consumer idle past consumerGCTimeout with no
// pending messages is removed from the group.
// Requires real Redis (XInfoConsumers.Idle always -1ms in miniredis).
func TestConsumerGC(t *testing.T) {
	rdb := newRealRDB(t)
	ctx := context.Background()

	s := stream + "-consumer-gc"
	t.Cleanup(func() { rdb.Del(ctx, s) })

	if err := rdb.XGroupCreateMkStream(ctx, s, group, "$").Err(); err != nil && !isRedisBusyGroup(err) {
		t.Fatalf("XGroupCreate: %v", err)
	}
	// Create "stale-pod": deliver + ack so Pending=0.
	rdb.XAdd(ctx, &redis.XAddArgs{Stream: s, Values: validVals()})
	res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "stale-pod",
		Streams: []string{s, ">"}, Count: 1, Block: -1,
	}).Result()
	if err != nil || len(res) == 0 || len(res[0].Messages) == 0 {
		t.Fatalf("seed XReadGroup: %v", err)
	}
	rdb.XAck(ctx, s, group, res[0].Messages[0].ID)

	// Wait for stale-pod to be idle > consumerGCTimeout.
	const gcTimeout = 150 * time.Millisecond
	time.Sleep(gcTimeout + 50*time.Millisecond)

	h := &recHandler{}
	c := consume.New(rdb, s, group, name, h, dasher.InstanceContext{}, dasher.FailLoud{}, 10,
		consume.WithConsumerGCInterval(20*time.Millisecond),
		consume.WithConsumerGCTimeout(gcTimeout),
		consume.WithBlockDuration(50*time.Millisecond),
	)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = c.Run(runCtx) }()

	waitFor(t, func() bool {
		consumers, err := rdb.XInfoConsumers(ctx, s, group).Result()
		if err != nil {
			return false
		}
		for _, xic := range consumers {
			if xic.Name == "stale-pod" {
				return false
			}
		}
		return true
	})
}

// TestXAckRetried verifies that a transient XACK failure does not propagate as
// a fatal error: the retry loop keeps trying until XACK succeeds.
func TestXAckRetried(t *testing.T) {
	rdb, _ := newRDB(t)
	setupGroup(t, rdb)
	addEntry(t, rdb, false)

	h := &recHandler{}
	c := consume.New(rdb, stream, group, name, h, dasher.InstanceContext{}, dasher.FailLoud{}, 10)

	var ackAttempts int32
	consume.SetAckFn(c, func(ctx context.Context, stream, group, id string) error {
		n := atomic.AddInt32(&ackAttempts, 1)
		if n < 3 {
			return errors.New("redis: connection refused") // transient failure
		}
		return rdb.XAck(ctx, stream, group, id).Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	waitFor(t, func() bool { return atomic.LoadInt32(&ackAttempts) >= 3 })
	waitFor(t, func() bool { return pendingCount(t, rdb) == 0 })
}

// TestPanicRecovery verifies that a panic in the handler is caught by the
// defer/recover in process(), routed through StreamErrorPolicy.OnFatal, and
// surfaces as a non-nil error from Run() (FailLoud behaviour).
func TestPanicRecovery(t *testing.T) {
	rdb, _ := newRDB(t)
	setupGroup(t, rdb)
	addEntry(t, rdb, false)

	panicHandler := dasher.HandlerFunc(func(ctx context.Context, inst dasher.InstanceContext, evt dasher.Event) error {
		panic("something went very wrong")
	})
	c := consume.New(rdb, stream, group, name, panicHandler, dasher.InstanceContext{}, dasher.FailLoud{}, 10)

	err := c.Run(context.Background())
	if err == nil {
		t.Fatal("expected non-nil error from Run after handler panic")
	}
	if !strings.Contains(err.Error(), "panic in handler") {
		t.Fatalf("error should mention 'panic in handler', got: %v", err)
	}
}

// TestHeartbeatFiresDuringSlowProcessing verifies that the heartbeat goroutine
// calls claimFn while a handler is blocked and idles once the entry is done.
func TestHeartbeatFiresDuringSlowProcessing(t *testing.T) {
	rdb, _ := newRDB(t)
	setupGroup(t, rdb)
	addEntry(t, rdb, false)

	const hbInterval = 5 * time.Millisecond

	blockCh := make(chan struct{})
	handling := make(chan struct{})
	var once sync.Once
	blockHandler := dasher.HandlerFunc(func(_ context.Context, _ dasher.InstanceContext, _ dasher.Event) error {
		once.Do(func() { close(handling) })
		<-blockCh
		return nil
	})

	var claimCount int32
	c := consume.New(rdb, stream, group, name, blockHandler, dasher.InstanceContext{}, dasher.FailLoud{}, 10,
		consume.WithHeartbeatInterval(hbInterval),
		consume.WithBlockDuration(20*time.Millisecond),
	)
	consume.SetClaimFn(c, func(_ context.Context, _ string) error {
		atomic.AddInt32(&claimCount, 1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	// Wait until handler is entered (in-flight ID published by process).
	select {
	case <-handling:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not entered within deadline")
	}

	// Heartbeat should fire at least twice while the handler is blocked.
	waitFor(t, func() bool { return atomic.LoadInt32(&claimCount) >= 2 })

	// Unblock; wait for PEL to drain (entry ACKed, in-flight cleared).
	close(blockCh)
	waitFor(t, func() bool { return pendingCount(t, rdb) == 0 })

	// After in-flight is nil, heartbeat should idle.
	// Allow ≤1 extra tick for the handler-return → nil-store race.
	countAfterDone := atomic.LoadInt32(&claimCount)
	time.Sleep(5 * hbInterval)
	finalCount := atomic.LoadInt32(&claimCount)
	if finalCount > countAfterDone+1 {
		t.Fatalf("heartbeat should idle after handler done: countAfterDone=%d final=%d",
			countAfterDone, finalCount)
	}
}

// TestHeartbeatPreventsSteal verifies the steal-prevention mechanism end-to-end:
// (1) negative control — an idle entry IS stolen; (2) positive control — a
// heartbeat claim resets the idle timer and blocks the steal.
// Requires real Redis (XPendingExt idle filter not supported by miniredis).
func TestHeartbeatPreventsSteal(t *testing.T) {
	rdb := newRealRDB(t)
	ctx := context.Background()

	s := stream + "-heartbeat-steal"
	t.Cleanup(func() { rdb.Del(ctx, s) })

	const minIdle = 150 * time.Millisecond
	if err := rdb.XGroupCreateMkStream(ctx, s, group, "$").Err(); err != nil && !isRedisBusyGroup(err) {
		t.Fatalf("XGroupCreate: %v", err)
	}

	nop := dasher.HandlerFunc(func(_ context.Context, _ dasher.InstanceContext, _ dasher.Event) error {
		return nil
	})
	// pod1: used for its claimFn (real XClaimJustID via ExposeClaimFn).
	pod1 := consume.New(rdb, s, group, "pod-1", nop, dasher.InstanceContext{}, dasher.FailLoud{}, 10,
		consume.WithReclaimMinIdle(minIdle),
	)
	// pod2: drives peerReclaim to simulate cross-pod theft attempts.
	pod2 := consume.New(rdb, s, group, "pod-2", nop, dasher.InstanceContext{}, dasher.FailLoud{}, 10,
		consume.WithReclaimMinIdle(minIdle),
	)

	pendingFor := func(consumer string) int64 {
		t.Helper()
		xps, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: s, Group: group, Start: "-", End: "+", Count: 100, Consumer: consumer,
		}).Result()
		if err != nil {
			t.Fatalf("XPendingExt(%s): %v", consumer, err)
		}
		return int64(len(xps))
	}
	deliverTo := func(consumer string) string {
		t.Helper()
		res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: consumer,
			Streams: []string{s, ">"}, Count: 1, Block: -1,
		}).Result()
		if err != nil || len(res) == 0 || len(res[0].Messages) == 0 {
			t.Fatalf("XReadGroup(%s): %v", consumer, err)
		}
		return res[0].Messages[0].ID
	}

	// ── Negative control: no heartbeat, idle > minIdle → pod-2 steals ──────────
	rdb.XAdd(ctx, &redis.XAddArgs{Stream: s, Values: validVals()})
	deliverTo("pod-1")
	time.Sleep(minIdle + 30*time.Millisecond) // entry is now idle > minIdle

	if err := consume.ExposePeerReclaim(pod2, ctx); err != nil {
		t.Fatalf("negative-ctrl peerReclaim: %v", err)
	}
	if got := pendingFor("pod-1"); got != 0 {
		t.Fatalf("negative-ctrl: expected pod-1 PEL empty after steal, got %d", got)
	}

	// ── Positive control: heartbeat resets idle → pod-2 cannot steal ────────────
	rdb.XAdd(ctx, &redis.XAddArgs{Stream: s, Values: validVals()})
	id := deliverTo("pod-1")
	time.Sleep(minIdle + 30*time.Millisecond) // entry is idle > minIdle (would be stolen)

	// Heartbeat: reset idle timer via pod1's real XClaimJustID claimFn.
	if err := consume.ExposeClaimFn(pod1, ctx, id); err != nil {
		t.Fatalf("heartbeat claimFn: %v", err)
	}

	// peerReclaim runs immediately — idle ≈ 0 < minIdle → no steal.
	if err := consume.ExposePeerReclaim(pod2, ctx); err != nil {
		t.Fatalf("positive-ctrl peerReclaim: %v", err)
	}
	if got := pendingFor("pod-1"); got != 1 {
		t.Fatalf("positive-ctrl: pod-1 should retain entry after heartbeat, got %d", got)
	}
}
