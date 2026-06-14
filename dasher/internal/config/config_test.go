package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err)
	assert.Equal(t, "bayer-17909", cfg.Instance.ID)
	require.Len(t, cfg.Instance.Streams, 2)
	assert.Equal(t, "order-sync@v1", cfg.Instance.Streams[0].Handler)
	assert.Equal(t, "cdc.orders", cfg.Instance.Streams[0].Stream)
	assert.Equal(t, "https://bayer.internal.svc", cfg.Instance.Services.Internal.BaseURL)
	assert.Equal(t, "dasher", cfg.Group)
	assert.NotEmpty(t, cfg.Consumer)
	assert.Equal(t, 10, cfg.EscalateAfter)
	assert.Equal(t, "localhost:6379", cfg.RedisAddr)
	assert.Equal(t, "t", cfg.AuthToken)
}

func TestLoadMissingInstance(t *testing.T) {
	setEnv(t, "nope")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrInstanceNotFound)
}

func TestLoadRequiresInstanceID(t *testing.T) {
	setEnv(t, "")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrMissingInstanceID)
}

func TestLoadInstanceWithoutStreams(t *testing.T) {
	setEnv(t, "empty-99999")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrNoStreams)
}

func TestLoadStreamMissingHandlerName(t *testing.T) {
	setEnv(t, "no-handler-99998")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrEmptyBinding)
}

func TestLoadStreamMissingStreamName(t *testing.T) {
	setEnv(t, "no-stream-name-99997")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrMissingStreamName)
}

func TestLoadPureTransform(t *testing.T) {
	t.Setenv("DASHER_TEST_DSN", "postgres://localhost/test")
	setEnv(t, "pure-transform-99990")
	cfg, err := config.Load()
	require.NoError(t, err)
	s := cfg.Instance.Streams[0]
	assert.Equal(t, "cdc.user_role_grant", s.Stream)
	assert.Equal(t, "enriched.user_role_grant", s.Emit)
	assert.Equal(t, "", s.Handler)
	require.Len(t, s.Enrich, 1)
	assert.Equal(t, "user_email", s.Enrich[0].Lookup)
}

func TestLoadEnrichedTerminal(t *testing.T) {
	t.Setenv("DASHER_TEST_DSN", "postgres://localhost/test")
	setEnv(t, "enriched-terminal-99989")
	_, err := config.Load()
	require.NoError(t, err)
}

func TestLoadHandlerAndEmit(t *testing.T) {
	setEnv(t, "handler-and-emit-99988")
	_, err := config.Load()
	require.NoError(t, err)
}

func TestLoadEmitSelfLoop(t *testing.T) {
	setEnv(t, "emit-self-loop-99987")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrSelfEmit)
}

func TestLoadEmitCycle(t *testing.T) {
	setEnv(t, "emit-cycle-99986")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrEmitCycle)
}

func TestLoadUnknownLookupRef(t *testing.T) {
	t.Setenv("DASHER_TEST_DSN", "postgres://localhost/test")
	setEnv(t, "unknown-lookup-ref-99985")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrUnknownLookup)
}

func TestLoadUnknownLookupType(t *testing.T) {
	setEnv(t, "unknown-lookup-type-99984")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrUnknownLookupType)
}

func TestLoadEnrichNoDB(t *testing.T) {
	// Ensure no DSN env var is set
	t.Setenv("DASHER_TEST_DSN", "")
	setEnv(t, "enrich-no-db-99983")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrMissingDBConfig)
}

func TestLoadBadBindKey(t *testing.T) {
	t.Setenv("DASHER_TEST_DSN", "postgres://localhost/test")
	setEnv(t, "bad-bind-key-99982")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrBadBindKey)
}

func TestLoadBadOnMiss(t *testing.T) {
	t.Setenv("DASHER_TEST_DSN", "postgres://localhost/test")
	setEnv(t, "bad-on-miss-99981")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrBadOnMiss)
}
