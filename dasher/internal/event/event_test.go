package event_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher/internal/event"
)

func base() map[string]any {
	return map[string]any{
		"op":          "insert",
		"table":       "orders",
		"schema":      "public",
		"lsn":         "0/1234",
		"streamed_at": "2026-06-12T10:00:00Z",
		"data":        `{"id":42,"big":12345678901234567890}`,
	}
}

func TestParseInsert(t *testing.T) {
	evt, err := event.Parse("1-0", base())
	require.NoError(t, err)
	assert.Equal(t, "1-0", evt.ID)
	assert.Equal(t, "insert", evt.Op)
	assert.Equal(t, "orders", evt.Table)
	assert.Equal(t, "public", evt.Schema)
	assert.Equal(t, "0/1234", evt.LSN)
	assert.True(t, evt.StreamedAt.Equal(time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)))
	assert.Equal(t, json.Number("42"), evt.Data["id"])
	assert.Equal(t, json.Number("12345678901234567890"), evt.Data["big"])
	assert.Nil(t, evt.Old)
}

func TestParseUpdateWithOld(t *testing.T) {
	v := base()
	v["op"] = "update"
	v["old"] = `{"id":42}`
	evt, err := event.Parse("2-0", v)
	require.NoError(t, err)
	assert.Equal(t, json.Number("42"), evt.Old["id"])
}

func TestParseMissingFieldErrors(t *testing.T) {
	v := base()
	delete(v, "op")
	_, err := event.Parse("3-0", v)
	require.ErrorIs(t, err, event.ErrMissingField)
}

func TestParseBadDataErrors(t *testing.T) {
	v := base()
	v["data"] = "{not json"
	_, err := event.Parse("4-0", v)
	require.ErrorIs(t, err, event.ErrMalformedJSON)
}

func TestParseWithEnrichment(t *testing.T) {
	v := base()
	v["enrichment"] = `{"user":{"email":"alice@example.com"}}`
	evt, err := event.Parse("5-0", v)
	require.NoError(t, err)
	require.NotNil(t, evt.Enrichment)
	user, ok := evt.Enrichment["user"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "alice@example.com", user["email"])
}

func TestParseNoEnrichmentIsNil(t *testing.T) {
	v := base()
	evt, err := event.Parse("6-0", v)
	require.NoError(t, err)
	assert.Nil(t, evt.Enrichment)
}

func TestParseWithSource(t *testing.T) {
	v := base()
	v["source"] = "my-instance"
	evt, err := event.Parse("7-0", v)
	require.NoError(t, err)
	assert.Equal(t, "my-instance", evt.Source)
}

func TestParseSourceAbsentIsEmpty(t *testing.T) {
	// WALker-produced entries do not carry a source field — must be tolerated.
	vt, err := event.Parse("8-0", base())
	require.NoError(t, err)
	assert.Empty(t, vt.Source)
}
