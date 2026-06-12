// Package dasher holds the core domain types shared across the CDC consumer:
// the parsed Event, the Handler contract, the per-instance context handed to
// handlers, the Poison sentinel, and the StreamErrorPolicy seam.
package dasher

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"4gclinical.com/dasher/internal/config"
	"4gclinical.com/dasher/internal/services"
)

// Event is one parsed CDC stream entry.
type Event struct {
	ID         string // Redis stream entry ID (for XACK)
	Op         string // insert | update | delete
	Table      string
	Schema     string
	LSN        string // source LSN (use for dedup)
	StreamedAt time.Time
	Data       map[string]any // full new row (INSERT/UPDATE) or PK only (DELETE)
	Old        map[string]any // PK of previous row (UPDATE/DELETE)
}

// InstanceContext is handed to every handler invocation.
type InstanceContext struct {
	ID       string
	Config   config.InstanceConfig
	Services services.Services
}

// Handler processes a single CDC event for an instance.
type Handler interface {
	Handle(ctx context.Context, inst InstanceContext, evt Event) error
}

// HandlerFunc adapts a plain function to the Handler interface.
type HandlerFunc func(context.Context, InstanceContext, Event) error

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, inst InstanceContext, evt Event) error {
	return f(ctx, inst, evt)
}

// poisonError marks an error as poison: the event will never succeed (bad
// shape, business rejection). Poison is routed through StreamErrorPolicy.
type poisonError struct{ err error }

func (e *poisonError) Error() string { return e.err.Error() }
func (e *poisonError) Unwrap() error { return e.err }

// Poison wraps err as a poison error (fail-loud, never retried). Poison(nil) is nil.
func Poison(err error) error {
	if err == nil {
		return nil
	}
	return &poisonError{err: err}
}

// IsPoison reports whether err is (or wraps) a poison error.
func IsPoison(err error) bool {
	var p *poisonError
	return errors.As(err, &p)
}

// StreamErrorPolicy decides what a fatal stream error (poison or malformed
// envelope) does. Returning a non-nil error propagates it to the errgroup
// (fail-loud → process exits). Returning nil means the policy handled it
// locally (the model-C quarantine seam). v0 ships only FailLoud.
type StreamErrorPolicy interface {
	OnFatal(stream string, err error) error
}

// FailLoud logs the fatal error and propagates it so the process exits non-zero
// and the supervisor restarts it. The offending entry remains in the PEL.
type FailLoud struct{}

// OnFatal implements StreamErrorPolicy.
func (FailLoud) OnFatal(stream string, err error) error {
	slog.Error("fatal stream error (fail-loud)", "stream", stream, "err", err)
	return err
}
