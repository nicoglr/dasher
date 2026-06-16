package middleware

import (
	"context"

	"4gclinical.com/dasher"
)

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
