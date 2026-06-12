// Package handlers contains the concrete v0 CDC event handlers.
// Each handler is a HandlerFunc that logs the event and (if an internal service
// base URL is configured) forwards it via an authenticated POST to /events.
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"

	"4gclinical.com/dasher"
)

// Forward logs the event and POSTs it to the internal service under /events.
// Returns nil when no internal service is configured (base_url empty).
// Returns a transient error for 5xx responses and network failures.
// Returns a Poison error for 4xx responses (event rejected permanently).
func Forward(ctx context.Context, inst dasher.InstanceContext, evt dasher.Event) error {
	slog.Info("handling event",
		"instance", inst.ID, "table", evt.Table, "op", evt.Op, "lsn", evt.LSN)

	if inst.Services.Internal == nil {
		return nil
	}

	body, err := json.Marshal(evt)
	if err != nil {
		// Marshal of a map[string]any from json.Decode can only fail on
		// non-serialisable types — a contract violation, not a transient condition.
		return dasher.Poison(fmt.Errorf("marshal event: %w", err))
	}
	resp, err := inst.Services.Internal.Do(ctx, "POST", "/events", bytes.NewReader(body))
	if err != nil {
		return err // transient (network) → retry
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("internal service %d", resp.StatusCode) // transient
	}
	if resp.StatusCode >= 400 {
		return dasher.Poison(fmt.Errorf("internal service rejected event: %d", resp.StatusCode))
	}
	return nil
}
