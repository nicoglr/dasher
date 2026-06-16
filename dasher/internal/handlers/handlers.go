// Package handlers contains the concrete v0 CDC event handlers.
// Each handler is a HandlerFunc that logs the event and (if a service
// base URL is configured) forwards it via an authenticated POST to /events.
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"4gclinical.com/dasher"
)

// httpDoer is satisfied by both *services.InternalClient and *services.GatewayClient.
type httpDoer interface {
	Do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error)
}

// Service selects which configured client Forward targets.
type Service int

const (
	ServiceInternal Service = iota
	ServiceGateway
)

// Forward returns a HandlerFunc that forwards events through the named service.
// Returns nil when the selected service is not configured (no-op).
func Forward(svc Service) dasher.HandlerFunc {
	return func(ctx context.Context, inst dasher.InstanceContext, evt dasher.Event) error {
		slog.Info("handling event",
			"instance", inst.ID, "table", evt.Table, "op", evt.Op, "lsn", evt.LSN)

		var client httpDoer
		switch svc {
		case ServiceInternal:
			if inst.Services.Internal != nil {
				client = inst.Services.Internal
			}
		case ServiceGateway:
			if inst.Services.Gateway != nil {
				client = inst.Services.Gateway
			}
		}

		if client == nil {
			return nil
		}

		body, err := json.Marshal(evt)
		if err != nil {
			return dasher.Poison(fmt.Errorf("marshal event: %w", err))
		}
		resp, err := client.Do(ctx, "POST", "/events", bytes.NewReader(body))
		if err != nil {
			return err // transient (network) → retry
		}
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
		if resp.StatusCode >= 500 {
			return fmt.Errorf("service %d", resp.StatusCode) // transient
		}
		if resp.StatusCode >= 400 {
			return dasher.Poison(fmt.Errorf("service rejected event: %d", resp.StatusCode))
		}
		return nil
	}
}
