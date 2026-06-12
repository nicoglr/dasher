package event_test

import (
	"encoding/json"
	"testing"
	"time"

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
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if evt.ID != "1-0" || evt.Op != "insert" || evt.Table != "orders" || evt.Schema != "public" || evt.LSN != "0/1234" {
		t.Errorf("unexpected envelope: %+v", evt)
	}
	if !evt.StreamedAt.Equal(time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("streamed_at: %v", evt.StreamedAt)
	}
	if evt.Data["id"] != json.Number("42") {
		t.Errorf("data.id: %v (%T)", evt.Data["id"], evt.Data["id"])
	}
	if evt.Data["big"] != json.Number("12345678901234567890") {
		t.Errorf("data.big lost precision: %v (%T)", evt.Data["big"], evt.Data["big"])
	}
	if evt.Old != nil {
		t.Errorf("old should be nil for insert: %v", evt.Old)
	}
}

func TestParseUpdateWithOld(t *testing.T) {
	v := base()
	v["op"] = "update"
	v["old"] = `{"id":42}`
	evt, err := event.Parse("2-0", v)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if evt.Old["id"] != json.Number("42") {
		t.Errorf("old.id: %v", evt.Old["id"])
	}
}

func TestParseMissingFieldErrors(t *testing.T) {
	v := base()
	delete(v, "op")
	if _, err := event.Parse("3-0", v); err == nil {
		t.Fatal("expected error for missing op")
	}
}

func TestParseBadDataErrors(t *testing.T) {
	v := base()
	v["data"] = "{not json"
	if _, err := event.Parse("4-0", v); err == nil {
		t.Fatal("expected error for malformed data JSON")
	}
}
