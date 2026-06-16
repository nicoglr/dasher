package handlers_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher"
	"4gclinical.com/dasher/internal/config"
	"4gclinical.com/dasher/internal/handlers"
	"4gclinical.com/dasher/internal/services"
)

// minJWT builds an unsigned JWT with the given expiry for test auth servers.
func minJWT(exp time.Time) string {
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	cls, _ := json.Marshal(map[string]any{"exp": exp.Unix()})
	pay := base64.RawURLEncoding.EncodeToString(cls)
	sig := base64.RawURLEncoding.EncodeToString([]byte("sig"))
	return hdr + "." + pay + "." + sig
}

// gatewayAuthServer returns an httptest.Server that issues tokens for
// /api/auth/api_login/ and delegates all other paths to handler h.
func gatewayAuthServer(h http.Handler) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/api_login/" {
			_ = json.NewEncoder(w).Encode(map[string]string{
				"access": minJWT(time.Now().Add(10 * time.Minute)),
			})
			return
		}
		h.ServeHTTP(w, r)
	}))
}

func makeInternalCfg() config.InstanceConfig {
	return config.InstanceConfig{
		Services: config.ServicesConfig{
			Internal: config.InternalServiceConfig{
				URLEnv:   "DASHER_TEST_INT_URL",
				TokenEnv: "DASHER_TEST_INT_TOKEN",
			},
		},
	}
}

func makeGatewayCfg() config.InstanceConfig {
	return config.InstanceConfig{
		Services: config.ServicesConfig{
			Gateway: config.GatewayServiceConfig{
				URLEnv:             "DASHER_TEST_GW_URL",
				AppInstanceCodeEnv: "DASHER_TEST_GW_CODE",
				APIKeyEnv:          "DASHER_TEST_GW_KEY",
			},
		},
	}
}

var testEvt = dasher.Event{ID: "1-0", Op: "insert", Table: "orders"}

func newInst(svc services.Services) dasher.InstanceContext {
	return dasher.InstanceContext{ID: "test", Services: svc}
}

// TestForwardInternalNoopWhenNil — no internal client configured → no error.
func TestForwardInternalNoopWhenNil(t *testing.T) {
	err := handlers.Forward(handlers.ServiceInternal)(context.Background(), newInst(services.Services{}), testEvt)
	assert.NoError(t, err)
}

// TestForwardGatewayNoopWhenNil — no gateway client configured → no error.
func TestForwardGatewayNoopWhenNil(t *testing.T) {
	err := handlers.Forward(handlers.ServiceGateway)(context.Background(), newInst(services.Services{}), testEvt)
	assert.NoError(t, err)
}

// TestForwardInternalPostsEvent — event is marshalled and POSTed to /events.
func TestForwardInternalPostsEvent(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("DASHER_TEST_INT_URL", srv.URL)
	t.Setenv("DASHER_TEST_INT_TOKEN", "tok")
	svc, err := services.New(context.Background(), makeInternalCfg())
	require.NoError(t, err)

	err = handlers.Forward(handlers.ServiceInternal)(context.Background(), newInst(*svc), testEvt)
	require.NoError(t, err)

	var decoded dasher.Event
	require.NoError(t, json.Unmarshal(gotBody, &decoded))
	assert.Equal(t, "orders", decoded.Table)
}

// TestForwardInternalTransientOn5xx — 5xx → retryable error (not poison).
func TestForwardInternalTransientOn5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	t.Setenv("DASHER_TEST_INT_URL", srv.URL)
	t.Setenv("DASHER_TEST_INT_TOKEN", "tok")
	svc, err := services.New(context.Background(), makeInternalCfg())
	require.NoError(t, err)

	err = handlers.Forward(handlers.ServiceInternal)(context.Background(), newInst(*svc), testEvt)
	require.Error(t, err)
	assert.False(t, dasher.IsPoison(err))
}

// TestForwardInternalPoisonOn4xx — 4xx → poison (never retried).
func TestForwardInternalPoisonOn4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	t.Setenv("DASHER_TEST_INT_URL", srv.URL)
	t.Setenv("DASHER_TEST_INT_TOKEN", "tok")
	svc, err := services.New(context.Background(), makeInternalCfg())
	require.NoError(t, err)

	err = handlers.Forward(handlers.ServiceInternal)(context.Background(), newInst(*svc), testEvt)
	require.Error(t, err)
	assert.True(t, dasher.IsPoison(err))
}

// TestForwardGatewayTransientOn5xx — gateway 5xx → retryable error (not poison).
func TestForwardGatewayTransientOn5xx(t *testing.T) {
	srv := gatewayAuthServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	t.Setenv("DASHER_TEST_GW_URL", srv.URL)
	t.Setenv("DASHER_TEST_GW_CODE", "app")
	t.Setenv("DASHER_TEST_GW_KEY", "key")
	svc, err := services.New(context.Background(), makeGatewayCfg())
	require.NoError(t, err)

	err = handlers.Forward(handlers.ServiceGateway)(context.Background(), newInst(*svc), testEvt)
	require.Error(t, err)
	assert.False(t, dasher.IsPoison(err))
}

// TestForwardGatewayPoisonOn4xx — gateway 4xx → poison (never retried).
func TestForwardGatewayPoisonOn4xx(t *testing.T) {
	srv := gatewayAuthServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))
	defer srv.Close()

	t.Setenv("DASHER_TEST_GW_URL", srv.URL)
	t.Setenv("DASHER_TEST_GW_CODE", "app")
	t.Setenv("DASHER_TEST_GW_KEY", "key")
	svc, err := services.New(context.Background(), makeGatewayCfg())
	require.NoError(t, err)

	err = handlers.Forward(handlers.ServiceGateway)(context.Background(), newInst(*svc), testEvt)
	require.Error(t, err)
	assert.True(t, dasher.IsPoison(err))
}
