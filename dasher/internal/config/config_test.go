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
	require.ErrorIs(t, err, config.ErrMissingHandlerName)
}

func TestLoadStreamMissingStreamName(t *testing.T) {
	setEnv(t, "no-stream-name-99997")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrMissingStreamName)
}
