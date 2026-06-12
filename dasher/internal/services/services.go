// Package services bundles the shared cross-consumer concerns built once per
// process — notably an authenticated HTTP client to the internal service, with
// base URL and bearer token pre-wired so handlers never re-implement auth.
package services

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"4gclinical.com/dasher/internal/config"
)

// InternalClient is an authenticated HTTP client to the internal service.
type InternalClient struct {
	baseURL string
	token   string
	hc      *http.Client
}

// BaseURL returns the configured base URL.
func (c *InternalClient) BaseURL() string { return c.baseURL }

// Do issues an authenticated request to baseURL+path with a Bearer token.
func (c *InternalClient) Do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	url := strings.TrimRight(c.baseURL, "/") + "/" + strings.TrimLeft(path, "/")
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	return c.hc.Do(req)
}

// Services is the shared capability bundle handed to handlers via InstanceContext.
type Services struct {
	Internal *InternalClient
}

// New builds the per-instance Services from config and the secret token (env).
func New(cfg config.InstanceConfig, token string) Services {
	return Services{Internal: &InternalClient{
		baseURL: cfg.Services.Internal.BaseURL,
		token:   token,
		hc:      &http.Client{Timeout: 30 * time.Second},
	}}
}
