package lookup

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrPoison is the sentinel error type returned when a lookup result is
// permanently invalid (0 rows or >1 row). The caller (internal/enrich)
// wraps this with dasher.Poison.
type ErrPoison struct{ Msg string }

func (e *ErrPoison) Error() string { return e.Msg }

// IsPoison reports whether err is (or wraps) an ErrPoison.
func IsPoison(err error) bool {
	var p *ErrPoison
	return errors.As(err, &p)
}

// EnrichRule describes one lookup step within a Runner.
type EnrichRule struct {
	// LookupName is the catalog key.
	LookupName string
	// Lookup is the resolved Lookup instance.
	Lookup Lookup
	// Bind maps query param → source column.
	Bind map[string]string
	// Into is the key set in the result map (e.g. "user" → result["user"]).
	Into string
	// CacheTTL is the TTL for cached results (from the Spec). Zero means no caching.
	CacheTTL time.Duration
}

// Runner orchestrates lookup enrichment for one binding.
// Operates on plain maps only — does NOT import the dasher package (oracle B1).
type Runner struct {
	rules []EnrichRule
	cache *Cache
}

// NewRunner creates a Runner with the given rules and cache.
func NewRunner(rules []EnrichRule, cache *Cache) *Runner {
	return &Runner{rules: rules, cache: cache}
}

// Run executes all rules in order against data and old, returning an enrichment
// map (or an error).
//
// Errors:
//   - *ErrPoison: 0 rows (no match) or >1 row returned — caller wraps with dasher.Poison.
//   - plain error: transient DB failure — retryable by the caller.
func (r *Runner) Run(ctx context.Context, data, old map[string]any) (map[string]any, error) {
	result := make(map[string]any)

	for _, rule := range r.rules {
		// Build bind map of param → resolved value.
		bind := make(map[string]any, len(rule.Bind))
		nilBind := false
		for param, col := range rule.Bind {
			v := ResolveBind(col, data, old)
			if v == nil {
				nilBind = true
				break
			}
			bind[param] = v
		}

		if nilBind {
			// Nil-bind: required column absent — treat as a permanent miss.
			return nil, &ErrPoison{Msg: fmt.Sprintf("lookup %q: bind column not found in data or old", rule.LookupName)}
		}

		// Check cache.
		cacheKey := MakeCacheKey(rule.LookupName, bind)
		if rows, ok := r.cache.Get(cacheKey); ok {
			if err := applyRows(rows, rule, result); err != nil {
				return nil, err
			}
			continue
		}

		// Cache miss: call lookup.
		rows, err := rule.Lookup.Resolve(ctx, bind)
		if err != nil {
			return nil, err // transient, retryable
		}
		if err := applyRows(rows, rule, result); err != nil {
			return nil, err
		}
		// Only cache successful single-row results.
		if len(rows) == 1 {
			r.cache.Set(cacheKey, rows, rule.CacheTTL)
		}
	}

	return result, nil
}

// applyRows stores the single-row result into result[into].
// Returns ErrPoison for 0 rows or >1 row.
func applyRows(rows []Row, rule EnrichRule, result map[string]any) error {
	switch len(rows) {
	case 0:
		return &ErrPoison{Msg: fmt.Sprintf("lookup %q: no row found", rule.LookupName)}
	case 1:
		result[rule.Into] = rows[0]
		return nil
	default:
		return &ErrPoison{Msg: fmt.Sprintf("lookup %q: expected at most 1 row, got %d", rule.LookupName, len(rows))}
	}
}
