package services_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher/internal/config"
	"4gclinical.com/dasher/internal/services"
)

func TestNewReturnsNilInternalWhenBaseURLEmpty(t *testing.T) {
	cfg := config.InstanceConfig{}
	svc, err := services.New(context.Background(), cfg, "secret")
	require.NoError(t, err)
	assert.Nil(t, svc.Internal)
}

func TestNewReturnsNilDBWhenNotConfigured(t *testing.T) {
	cfg := config.InstanceConfig{}
	svc, err := services.New(context.Background(), cfg, "secret")
	require.NoError(t, err)
	assert.Nil(t, svc.DB)
}

func TestNewBadDSNReturnsError(t *testing.T) {
	t.Setenv("DASHER_SVC_TEST_DSN", "not-a-valid-dsn://!@#")
	cfg := config.InstanceConfig{
		Services: config.ServicesConfig{
			DB: config.DBConfig{DSNEnv: "DASHER_SVC_TEST_DSN"},
		},
	}
	_, err := services.New(context.Background(), cfg, "")
	require.Error(t, err)
}

func TestNewValidDSNParsesPool(t *testing.T) {
	// A syntactically valid DSN — pool creation is lazy so no live DB needed.
	t.Setenv("DASHER_SVC_TEST_DSN", "postgres://user:pass@localhost:5432/db")
	cfg := config.InstanceConfig{
		Services: config.ServicesConfig{
			DB: config.DBConfig{DSNEnv: "DASHER_SVC_TEST_DSN", MaxConns: 2},
		},
	}
	svc, err := services.New(context.Background(), cfg, "")
	require.NoError(t, err)
	require.NotNil(t, svc.DB)
	svc.Close() // lazy pool, no live connection
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
	svc, err := services.New(context.Background(), cfg, "secret")
	require.NoError(t, err)

	resp, err := svc.Internal.Do(context.Background(), http.MethodGet, "/ping", nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Bearer secret", gotAuth)
	assert.Equal(t, "/ping", gotPath)
}
