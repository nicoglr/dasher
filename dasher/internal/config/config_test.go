package config_test

import (
	"errors"
	"testing"

	"4gclinical.com/dasher/internal/config"
)

func setEnv(t *testing.T, inst string) {
	t.Helper()
	t.Setenv("DASHER_INSTANCE_ID", inst)
	t.Setenv("DASHER_CONFIG", "testdata/config.yaml")
	t.Setenv("DASHER_REDIS_ADDR", "localhost:6379")
	t.Setenv("DASHER_AUTH_TOKEN", "t")
	t.Setenv("DASHER_ESCALATE_AFTER", "")
}

func TestLoadSelectsInstance(t *testing.T) {
	setEnv(t, "bayer-17909")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Instance.ID != "bayer-17909" {
		t.Errorf("instance id: %q", cfg.Instance.ID)
	}
	if len(cfg.Instance.Streams) != 2 {
		t.Fatalf("expected 2 streams, got %d", len(cfg.Instance.Streams))
	}
	if cfg.Instance.Streams[0].Handler != "order-sync@v1" {
		t.Errorf("handler: %q", cfg.Instance.Streams[0].Handler)
	}
	if cfg.Instance.Streams[0].Stream != "cdc.orders" {
		t.Errorf("stream: %q", cfg.Instance.Streams[0].Stream)
	}
	if cfg.Instance.Services.Internal.BaseURL != "https://bayer.internal.svc" {
		t.Errorf("base_url: %q", cfg.Instance.Services.Internal.BaseURL)
	}
	if cfg.Group != "dasher" {
		t.Errorf("group: %q", cfg.Group)
	}
	if cfg.Consumer == "" {
		t.Error("consumer must not be empty")
	}
	if cfg.EscalateAfter != 10 {
		t.Errorf("escalate after: %d", cfg.EscalateAfter)
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("redis addr: %q", cfg.RedisAddr)
	}
	if cfg.AuthToken != "t" {
		t.Errorf("auth token: %q", cfg.AuthToken)
	}
}

func TestLoadMissingInstance(t *testing.T) {
	setEnv(t, "nope")
	_, err := config.Load()
	if !errors.Is(err, config.ErrInstanceNotFound) {
		t.Fatalf("expected ErrInstanceNotFound, got %v", err)
	}
}

func TestLoadRequiresInstanceID(t *testing.T) {
	setEnv(t, "")
	_, err := config.Load()
	if !errors.Is(err, config.ErrMissingInstanceID) {
		t.Fatalf("expected ErrMissingInstanceID, got %v", err)
	}
}

func TestLoadInstanceWithoutStreams(t *testing.T) {
	setEnv(t, "empty-99999")
	_, err := config.Load()
	if !errors.Is(err, config.ErrNoStreams) {
		t.Fatalf("expected ErrNoStreams, got %v", err)
	}
}

func TestLoadStreamMissingHandlerName(t *testing.T) {
	setEnv(t, "no-handler-99998")
	_, err := config.Load()
	if !errors.Is(err, config.ErrMissingHandlerName) {
		t.Fatalf("expected ErrMissingHandlerName, got %v", err)
	}
}

func TestLoadStreamMissingStreamName(t *testing.T) {
	setEnv(t, "no-stream-name-99997")
	_, err := config.Load()
	if !errors.Is(err, config.ErrMissingStreamName) {
		t.Fatalf("expected ErrMissingStreamName, got %v", err)
	}
}
