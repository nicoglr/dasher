// Package middleware provides Handler-middleware for the CDC pipeline.
// Each function wraps an inner Handler and returns a new Handler, following
// the (...inner Handler) Handler decorator pattern.
package middleware

import (
	"context"
	"fmt"

	"4gclinical.com/dasher"
	"4gclinical.com/dasher/internal/lookup"
)

// Enrich returns a Handler that runs the lookup runner, merges the result into
// evt.Enrichment, then calls inner. If the runner returns a lookup.ErrPoison,
// it is wrapped with dasher.Poison. Transient errors are returned as-is.
func Enrich(runner *lookup.Runner, inner dasher.Handler) dasher.Handler {
	return dasher.HandlerFunc(func(ctx context.Context, inst dasher.InstanceContext, evt dasher.Event) error {
		enr, err := runner.Run(ctx, evt.Data, evt.Old)
		if err != nil {
			// TODO(#9): no DLQ yet — poison routes to FailLoud, which exits the process
			// and leaves the entry in the Redis PEL. A future DLQ policy would XADD to
			// a dead-letter stream here instead.
			if lookup.IsPoison(err) {
				return dasher.Poison(fmt.Errorf("enrich: %w", err))
			}
			return err // transient
		}
		evt.Enrichment = enr
		return inner.Handle(ctx, inst, evt)
	})
}
