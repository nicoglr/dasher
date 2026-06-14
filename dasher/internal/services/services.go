// Package services bundles the shared cross-consumer concerns built once per
// process — notably an authenticated HTTP client to the internal service and
// an optional pgxpool for sql lookups.
package services

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"4gclinical.com/dasher/internal/config"
)

// InternalClient is an authenticated HTTP client to the internal service.
type InternalClient struct {
	baseURL string
	token   string
	hc      *http.Client
}

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
	// DB is the pgxpool for sql lookups. Nil when no DB is configured.
	DB *pgxpool.Pool
}

// New builds the per-instance Services. It takes a context (used by pgxpool
// config parsing — pool connect is lazy, so a down DB does not fail startup).
// Returns an error if DB config is present but invalid.
// Internal is nil when base_url is empty.
// DB is nil when dsn_env is not configured.
func New(ctx context.Context, cfg config.InstanceConfig, token string) (*Services, error) {
	svc := &Services{}

	if cfg.Services.Internal.BaseURL != "" {
		svc.Internal = &InternalClient{
			baseURL: cfg.Services.Internal.BaseURL,
			token:   token,
			hc:      &http.Client{Timeout: 30 * time.Second},
		}
	}

	if cfg.Services.DB.DSNEnv != "" {
		dsn := os.Getenv(cfg.Services.DB.DSNEnv)
		if dsn != "" {
			poolCfg, err := pgxpool.ParseConfig(dsn)
			if err != nil {
				return nil, fmt.Errorf("services: parse db dsn: %w", err)
			}
			maxConns := int32(cfg.Services.DB.MaxConns)
			if maxConns <= 0 {
				maxConns = 4
			}
			poolCfg.MaxConns = maxConns
			pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
			if err != nil {
				return nil, fmt.Errorf("services: create db pool: %w", err)
			}
			svc.DB = pool
		}
	}

	return svc, nil
}

// Close releases all resources (DB pool).
func (s *Services) Close() {
	if s.DB != nil {
		s.DB.Close()
	}
}
