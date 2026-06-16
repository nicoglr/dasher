package services_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher/internal/config"
	"4gclinical.com/dasher/internal/services"
)

// makeJWT builds a minimal unsigned JWT with the given exp claim.
func makeJWT(exp int64) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]any{"exp": exp, "sub": "test"})
	payload := base64.RawURLEncoding.EncodeToString(claims)
	sig := base64.RawURLEncoding.EncodeToString([]byte("fakesig"))
	return header + "." + payload + "." + sig
}

func setupGatewayServer(t *testing.T, loginCalls *atomic.Int32, expOffset time.Duration) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/api_login/" {
			loginCalls.Add(1)
			exp := time.Now().Add(expOffset).Unix()
			tok := makeJWT(exp)
			_ = json.NewEncoder(w).Encode(map[string]string{"access": tok})
			return
		}
		// echo handler
		w.Header().Set("X-Auth", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	return srv
}

func buildGatewayClient(t *testing.T, srvURL string) *services.GatewayClient {
	t.Helper()
	t.Setenv("DASHER_GW_URL", srvURL)
	t.Setenv("DASHER_GW_CODE", "myapp")
	t.Setenv("DASHER_GW_KEY", "secret")
	cfg := config.InstanceConfig{
		Services: config.ServicesConfig{
			Gateway: config.GatewayServiceConfig{
				URLEnv:             "DASHER_GW_URL",
				AppInstanceCodeEnv: "DASHER_GW_CODE",
				APIKeyEnv:          "DASHER_GW_KEY",
			},
		},
	}
	svc, err := services.New(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, svc.Gateway)
	return svc.Gateway
}

func TestGatewayFirstDoTriggersLogin(t *testing.T) {
	var loginCalls atomic.Int32
	srv := setupGatewayServer(t, &loginCalls, 10*time.Minute)
	defer srv.Close()

	client := buildGatewayClient(t, srv.URL)
	resp, err := client.Do(context.Background(), http.MethodGet, "/ping", nil)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, int32(1), loginCalls.Load())
}

func TestGatewayTokenCachedOnSecondDo(t *testing.T) {
	var loginCalls atomic.Int32
	srv := setupGatewayServer(t, &loginCalls, 10*time.Minute)
	defer srv.Close()

	client := buildGatewayClient(t, srv.URL)
	for i := 0; i < 3; i++ {
		resp, err := client.Do(context.Background(), http.MethodGet, "/ping", nil)
		require.NoError(t, err)
		resp.Body.Close()
	}
	assert.Equal(t, int32(1), loginCalls.Load(), "token should be cached")
}

func TestGatewayTokenRefreshedOnNearExpiry(t *testing.T) {
	var loginCalls atomic.Int32
	// Token expires in 30s (< 60s buffer → always stale)
	srv := setupGatewayServer(t, &loginCalls, 30*time.Second)
	defer srv.Close()

	client := buildGatewayClient(t, srv.URL)
	// Two calls should each trigger a new login because the token is always near-expired.
	resp1, err := client.Do(context.Background(), http.MethodGet, "/ping", nil)
	require.NoError(t, err)
	resp1.Body.Close()
	resp2, err := client.Do(context.Background(), http.MethodGet, "/ping", nil)
	require.NoError(t, err)
	resp2.Body.Close()
	assert.Equal(t, int32(2), loginCalls.Load(), "near-expiry token should re-auth on each Do")
}

func TestGatewayLoginNon2xxReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/api_login/" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := buildGatewayClient(t, srv.URL)
	_, err := client.Do(context.Background(), http.MethodGet, "/ping", nil)
	require.Error(t, err)
}

func TestGatewayConcurrentDoNoDataRace(t *testing.T) {
	var loginCalls atomic.Int32
	// Token always near-expiry (30s < 60s buffer) so every goroutine that acquires
	// the lock will attempt a re-auth. The test verifies no data race occurs and
	// all requests succeed; it does not assert a specific login count.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/api_login/" {
			loginCalls.Add(1)
			time.Sleep(5 * time.Millisecond) // simulate latency
			exp := time.Now().Add(30 * time.Second).Unix()
			tok := makeJWT(exp)
			_ = json.NewEncoder(w).Encode(map[string]string{"access": tok})
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := buildGatewayClient(t, srv.URL)

	// Trigger initial auth so subsequent calls find a near-expired token.
	resp, err := client.Do(context.Background(), http.MethodGet, "/ping", nil)
	require.NoError(t, err)
	resp.Body.Close()

	// Now all concurrent goroutines will find the token near-expired.
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := client.Do(context.Background(), http.MethodGet, "/ping", nil)
			if e == nil {
				r.Body.Close()
			}
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		assert.NoError(t, e)
	}
	// With mutex double-check: all concurrent goroutines serialize, so we get
	// exactly n+1 logins (one per call since each finds a near-expiry token).
	// The important thing is no data race.
	assert.Positive(t, loginCalls.Load())
}

func TestNewGatewayNilWhenURLEnvEmpty(t *testing.T) {
	cfg := config.InstanceConfig{}
	svc, err := services.New(context.Background(), cfg)
	require.NoError(t, err)
	assert.Nil(t, svc.Gateway)
}

func TestNewGatewayConstructedWhenConfigured(t *testing.T) {
	var loginCalls atomic.Int32
	srv := setupGatewayServer(t, &loginCalls, 10*time.Minute)
	defer srv.Close()
	client := buildGatewayClient(t, srv.URL)
	assert.NotNil(t, client)
}

func TestGatewayBearerHeaderSet(t *testing.T) {
	var gotAuth string
	var loginCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/api_login/" {
			loginCalls.Add(1)
			exp := time.Now().Add(10 * time.Minute).Unix()
			tok := makeJWT(exp)
			_ = json.NewEncoder(w).Encode(map[string]string{"access": tok})
			return
		}
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := buildGatewayClient(t, srv.URL)
	resp, err := client.Do(context.Background(), http.MethodGet, "/ping", nil)
	require.NoError(t, err)
	resp.Body.Close()
	assert.True(t, len(gotAuth) > 7 && gotAuth[:7] == "Bearer ", fmt.Sprintf("expected Bearer token, got %q", gotAuth))
}
