// Package lookup provides pluggable data-enrichment lookups for Dasher.
// All types here operate on plain maps only — no import of the dasher package
// (prevents an import cycle: dasher → config → lookup → dasher).
package lookup

import (
	"encoding/json"
	"regexp"
)

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidColumn reports whether s is a valid column identifier
// ([A-Za-z_][A-Za-z0-9_]*). Duplicated in config validation so config need
// not import lookup.
func ValidColumn(s string) bool {
	return s != "" && identRe.MatchString(s)
}

// ResolveBind resolves a bind column by checking data first, then old.
// Returns data[col] if non-nil, else old[col], else nil.
// Normalizes json.Number values: integral → int64, fractional → float64,
// raw string only if neither parses. This is required so pgx.NamedArgs can
// encode the value to integer/bigint columns (oracle B2).
func ResolveBind(col string, data, old map[string]any) any {
	var v any
	if data != nil {
		if val, ok := data[col]; ok && val != nil {
			v = val
		}
	}
	if v == nil && old != nil {
		if val, ok := old[col]; ok {
			v = val
		}
	}
	return normalizeNumber(v)
}

// normalizeNumber converts a json.Number to int64 or float64 when possible.
// Other types pass through unchanged.
func normalizeNumber(v any) any {
	n, ok := v.(json.Number)
	if !ok {
		return v
	}
	if i, err := n.Int64(); err == nil {
		return i
	}
	if f, err := n.Float64(); err == nil {
		return f
	}
	return n.String()
}
