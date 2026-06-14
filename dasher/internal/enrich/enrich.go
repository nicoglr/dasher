// Package enrich provides handler-middleware for CDC event enrichment.
// Enrich populates Event.Enrichment via lookup.Runner before calling the inner
// handler. EmitAfter publishes the event downstream after the inner handler
// succeeds.
package enrich

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
			if lookup.IsPoison(err) {
				return dasher.Poison(fmt.Errorf("enrich: %w", err))
			}
			return err // transient
		}
		evt.Enrichment = enr
		return inner.Handle(ctx, inst, evt)
	})
}

// EmitAfter returns a Handler that calls inner first; if inner succeeds, it
// emits the event to the downstream stream via prod. If inner fails, the emit
// is skipped and the error is returned.
func EmitAfter(prod dasher.Producer, stream string, inner dasher.Handler) dasher.Handler {
	return dasher.HandlerFunc(func(ctx context.Context, inst dasher.InstanceContext, evt dasher.Event) error {
		if err := inner.Handle(ctx, inst, evt); err != nil {
			return err
		}
		return prod.Emit(ctx, stream, evt)
	})
}
