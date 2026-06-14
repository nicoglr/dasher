// Package enrich_test — end-to-end integration test covering the full
// cdc → enrich → enriched pipeline via a real consume.Consumer and real
// produce.Producer, with miniredis as the stream backend.
package enrich_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher"
	"4gclinical.com/dasher/internal/consume"
	"4gclinical.com/dasher/internal/enrich"
	"4gclinical.com/dasher/internal/event"
	"4gclinical.com/dasher/internal/lookup"
	"4gclinical.com/dasher/internal/produce"
)

// TestEndToEnd_CDCEnrichEmit drives a complete cdc→enrich→enriched hop:
//   - seed one WALker-style entry on "instance.cdc.users"
//   - consumer reads it, enriches via a fake lookup, emits to "instance.enriched.users"
//   - assert the emitted entry preserves lsn/op/table and carries enrichment
//   - assert event.Parse can round-trip the emitted entry
func TestEndToEnd_CDCEnrichEmit(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	const (
		instanceID  = "test-instance"
		srcStream   = "cdc.users"
		dstStream   = "enriched.users"
		consumerGrp = "dasher"
		consumerID  = "test-consumer"
		lsn         = "0/ABCDEF"
	)

	// Create the consumer group BEFORE seeding entries so the group reads from
	// exactly the right position. The consumer's ensureGroup uses '$' (new-only),
	// so seeding before group creation would miss the entry.
	_, createErr := rdb.XGroupCreateMkStream(context.Background(), instanceID+"."+srcStream, consumerGrp, "0").Result()
	if createErr != nil && !contains(createErr.Error(), "BUSYGROUP") {
		t.Fatalf("create consumer group: %v", createErr)
	}

	// Seed a WALker-style entry on the source stream.
	dataJSON, _ := json.Marshal(map[string]any{"id": "42", "name": "Alice"})
	_, err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: instanceID + "." + srcStream,
		ID:     "*",
		Values: map[string]any{
			"op":          "insert",
			"table":       "users",
			"schema":      "public",
			"lsn":         lsn,
			"streamed_at": time.Now().UTC().Format(time.RFC3339),
			"data":        string(dataJSON),
		},
	}).Result()
	require.NoError(t, err)

	// Build the enrichment pipeline:
	//   Enrich(runner, EmitAfter(producer, dst, Noop))
	fl := &fakeLookupE2E{rows: []lookup.Row{{"email": "alice@example.com"}}}
	rule := lookup.EnrichRule{
		LookupName: "user_by_id",
		Lookup:     lookup.NewCachedLookup(fl, time.Minute),
		Bind:       map[string]string{"user_id": "id"},
		Into:       "user",
	}
	runner := lookup.NewRunner([]lookup.EnrichRule{rule})
	producer := produce.New(rdb, instanceID)
	handler := enrich.Enrich(runner, enrich.EmitAfter(producer, dstStream, dasher.Noop))

	inst := dasher.InstanceContext{ID: instanceID}
	policy := dasher.FailLoud{}

	consumer := consume.New(
		rdb,
		instanceID+"."+srcStream,
		consumerGrp,
		consumerID,
		handler,
		inst,
		policy,
		5,
	)

	// Run the consumer with a cancel so it processes the seeded entry and stops.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	// Poll the enriched stream until the entry appears.
	var enrichedID string
	var enrichedValues map[string]interface{}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		msgs, err := rdb.XRange(context.Background(), instanceID+"."+dstStream, "-", "+").Result()
		if err == nil && len(msgs) > 0 {
			enrichedID = msgs[0].ID
			enrichedValues = msgs[0].Values
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel() // signal consumer to stop
	<-done   // wait for it

	require.NotEmpty(t, enrichedID, "expected an entry on the enriched stream")

	// Assert preserved fields.
	assert.Equal(t, "insert", enrichedValues["op"])
	assert.Equal(t, "users", enrichedValues["table"])
	assert.Equal(t, "public", enrichedValues["schema"])
	assert.Equal(t, lsn, enrichedValues["lsn"], "lsn must be preserved (downstream dedup)")

	// Assert enrichment field is present.
	enrichmentRaw, ok := enrichedValues["enrichment"].(string)
	require.True(t, ok, "enrichment field should be a JSON string")
	var enrichment map[string]any
	require.NoError(t, json.Unmarshal([]byte(enrichmentRaw), &enrichment))
	user, ok := enrichment["user"].(map[string]any)
	require.True(t, ok, "enrichment.user should be a map")
	assert.Equal(t, "alice@example.com", user["email"])

	// Assert event.Parse can round-trip the emitted entry (field-name drift check).
	evtOut, err := event.Parse(fmt.Sprintf("%s-parsed", enrichedID), enrichedValues)
	require.NoError(t, err)
	assert.Equal(t, lsn, evtOut.LSN)
	assert.Equal(t, "insert", evtOut.Op)
	assert.Equal(t, "users", evtOut.Table)
	require.NotNil(t, evtOut.Enrichment)
	assert.Equal(t, "alice@example.com", evtOut.Enrichment["user"].(map[string]any)["email"])
}

type fakeLookupE2E struct {
	rows []lookup.Row
}

func (f *fakeLookupE2E) Resolve(_ context.Context, _ map[string]any) ([]lookup.Row, error) {
	return f.rows, nil
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
