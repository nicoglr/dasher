package services_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"4gclinical.com/dasher/internal/config"
	"4gclinical.com/dasher/internal/services"
)

func TestNewReturnsNilInternalWhenBaseURLEmpty(t *testing.T) {
	cfg := config.InstanceConfig{}
	svc := services.New(cfg, "secret")
	if svc.Internal != nil {
		t.Error("expected Internal to be nil when base_url is empty")
	}
}

func TestInternalClientWiring(t *testing.T) {
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config.InstanceConfig{
		Services: config.ServicesConfig{
			Internal: config.InternalServiceConfig{BaseURL: srv.URL},
		},
	}
	svc := services.New(cfg, "secret")

	resp, err := svc.Internal.Do(context.Background(), http.MethodGet, "/ping", nil)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: %d", resp.StatusCode)
	}
	if gotAuth != "Bearer secret" {
		t.Errorf("auth header: %q", gotAuth)
	}
	if gotPath != "/ping" {
		t.Errorf("path: %q", gotPath)
	}
}
