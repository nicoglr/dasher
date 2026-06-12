package consume_test

import (
	"context"
	"errors"
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
	<-done // wait for Run to exit before asserting PEL state
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
	if _, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: name, Streams: []string{stream, ">"}, Count: 10,
	}).Result(); err != nil {
		t.Fatalf("seed XReadGroup: %v", err)
	}
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
