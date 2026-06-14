// Package event parses a Redis stream entry (as produced by WALker) into a
// dasher.Event. JSON payloads are decoded with UseNumber so bigint/numeric
// values keep exact decimal text and are never rounded through float64.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"4gclinical.com/dasher"
)

// Sentinel errors for named parse failures in Parse.
var (
	ErrMissingField  = errors.New("missing required field")
	ErrMalformedJSON = errors.New("malformed JSON payload")
)

// Parse turns a stream entry (id + field map) into a dasher.Event. A malformed
// envelope returns an error — a contract violation treated as fatal upstream,
// never retried.
func Parse(id string, values map[string]any) (dasher.Event, error) {
	e := dasher.Event{ID: id}
	var err error
	if e.Op, err = reqStr(values, "op"); err != nil {
		return e, err
	}
	if e.Table, err = reqStr(values, "table"); err != nil {
		return e, err
	}
	if e.Schema, err = reqStr(values, "schema"); err != nil {
		return e, err
	}
	if e.LSN, err = reqStr(values, "lsn"); err != nil {
		return e, err
	}
	sa, err := reqStr(values, "streamed_at")
	if err != nil {
		return e, err
	}
	if e.StreamedAt, err = time.Parse(time.RFC3339, sa); err != nil {
		return e, fmt.Errorf("streamed_at %q: %w", sa, err)
	}
	dataStr, err := reqStr(values, "data")
	if err != nil {
		return e, err
	}
	if e.Data, err = decodeJSON(dataStr); err != nil {
		return e, fmt.Errorf("data: %w", err)
	}
	if raw, ok := values["old"]; ok {
		if s, ok := raw.(string); ok && s != "" {
			if e.Old, err = decodeJSON(s); err != nil {
				return e, fmt.Errorf("old: %w", err)
			}
		}
	}
	if raw, ok := values["enrichment"]; ok {
		if s, ok := raw.(string); ok && s != "" {
			if e.Enrichment, err = decodeJSON(s); err != nil {
				return e, fmt.Errorf("enrichment: %w", err)
			}
		}
	}
	return e, nil
}

func reqStr(v map[string]any, k string) (string, error) {
	raw, ok := v[k]
	if !ok {
		return "", fmt.Errorf("field %q: %w", k, ErrMissingField)
	}
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("field %q is not a string: %w", k, ErrMissingField)
	}
	return s, nil
}

func decodeJSON(s string) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader([]byte(s)))
	dec.UseNumber() // keep bigint/numeric as exact decimal text, never float64
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedJSON, err)
	}
	return m, nil
}
