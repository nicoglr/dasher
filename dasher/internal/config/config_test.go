package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher/internal/config"
)

func setEnv(t *testing.T, inst string) {
	t.Helper()
	t.Setenv("DASHER_INSTANCE_ID", inst)
	t.Setenv("DASHER_CONFIG", "testdata/config.yaml")
	t.Setenv("DASHER_REDIS_ADDR", "localhost:6379")
	t.Setenv("DASHER_ESCALATE_AFTER", "")
	t.Setenv("DASHER_TEST_INTERNAL_URL", "https://test.internal.svc")
}

func TestLoadSelectsInstance(t *testing.T) {
	setEnv(t, "bayer-17909")
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "bayer-17909", cfg.Instance.ID)
	require.Len(t, cfg.Instance.Streams, 2)
	assert.Equal(t, "order-sync@v1", cfg.Instance.Streams[0].Handler)
	assert.Equal(t, "cdc.orders", cfg.Instance.Streams[0].Stream)
	assert.Equal(t, "DASHER_TEST_INTERNAL_URL", cfg.Instance.Services.Internal.URLEnv)
	assert.Equal(t, "dasher", cfg.Group)
	assert.NotEmpty(t, cfg.Consumer)
	assert.Equal(t, 10, cfg.EscalateAfter)
	assert.Equal(t, "localhost:6379", cfg.RedisAddr)
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
	// emit-cycle-99986 has a genuine 2-hop cycle: cdc.orders→enriched.orders→cdc.orders
	setEnv(t, "emit-cycle-99986")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrEmitCycle)
}

func TestLoadPureTransformChain(t *testing.T) {
	// valid-transform-chain-99979: cdc.events→enriched.events (terminal) — not a cycle
	setEnv(t, "valid-transform-chain-99979")
	_, err := config.Load()
	require.NoError(t, err)
}

func TestLoadExampleConfig(t *testing.T) {
	// Verify config.example.yaml loads without error for the enrichment-example instance.
	t.Setenv("DASHER_DB_DSN", "postgres://localhost/test")
	t.Setenv("DASHER_INTERNAL_URL", "https://enrichment.internal.svc")
	t.Setenv("DASHER_INSTANCE_ID", "enrichment-example")
	t.Setenv("DASHER_CONFIG", "../../config.example.yaml")
	t.Setenv("DASHER_REDIS_ADDR", "localhost:6379")
	t.Setenv("DASHER_ESCALATE_AFTER", "")
	_, err := config.Load()
	require.NoError(t, err)
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

func TestLoadBadLookupTTL(t *testing.T) {
	setEnv(t, "bad-lookup-ttl-99980")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrBadLookupTTL)
}

func TestLoadMissingURLEnv(t *testing.T) {
	// url_env is set to DASHER_MISSING_INTERNAL_URL but that env var is not set.
	t.Setenv("DASHER_MISSING_INTERNAL_URL", "")
	setEnv(t, "missing-url-env-99978")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrMissingURLEnv)
}

// --- Consumer-group lifecycle env var tests ---

func TestLoadLifecycleDefaults(t *testing.T) {
	setEnv(t, "bayer-17909")
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, cfg.ReclaimMinIdle)
	assert.Equal(t, 5*time.Second, cfg.ReclaimInterval)
	assert.Equal(t, 5*time.Minute, cfg.ConsumerGCInterval)
	assert.Equal(t, 10*time.Minute, cfg.ConsumerGCTimeout)
}

func TestLoadLifecycleEnvVars(t *testing.T) {
	setEnv(t, "bayer-17909")
	t.Setenv("DASHER_RECLAIM_MIN_IDLE", "45s")
	t.Setenv("DASHER_RECLAIM_INTERVAL", "10s")
	t.Setenv("DASHER_CONSUMER_GC_INTERVAL", "2m")
	t.Setenv("DASHER_CONSUMER_GC_TIMEOUT", "15m")
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 45*time.Second, cfg.ReclaimMinIdle)
	assert.Equal(t, 10*time.Second, cfg.ReclaimInterval)
	assert.Equal(t, 2*time.Minute, cfg.ConsumerGCInterval)
	assert.Equal(t, 15*time.Minute, cfg.ConsumerGCTimeout)
}

func TestLoadBadReclaimMinIdle(t *testing.T) {
	setEnv(t, "bayer-17909")
	t.Setenv("DASHER_RECLAIM_MIN_IDLE", "not-a-duration")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrBadReclaimMinIdle)
}

func TestLoadBadReclaimInterval(t *testing.T) {
	setEnv(t, "bayer-17909")
	t.Setenv("DASHER_RECLAIM_INTERVAL", "-1s")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrBadReclaimInterval)
}

func TestLoadBadConsumerGCInterval(t *testing.T) {
	setEnv(t, "bayer-17909")
	t.Setenv("DASHER_CONSUMER_GC_INTERVAL", "0s")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrBadConsumerGCInterval)
}

func TestLoadBadConsumerGCTimeout(t *testing.T) {
	setEnv(t, "bayer-17909")
	t.Setenv("DASHER_CONSUMER_GC_TIMEOUT", "banana")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrBadConsumerGCTimeout)
}

func TestLoadConsumerGCTimeoutTooSmall(t *testing.T) {
	setEnv(t, "bayer-17909")
	// GCTimeout (20s) <= ReclaimMinIdle (30s default) — must be rejected.
	t.Setenv("DASHER_CONSUMER_GC_TIMEOUT", "20s")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrConsumerGCTimeoutTooSmall)
}

// --- Heartbeat interval tests ---

func TestHeartbeatIntervalDefault(t *testing.T) {
	setEnv(t, "bayer-17909")
	// Default: HeartbeatInterval = ReclaimMinIdle/2 = 30s/2 = 15s.
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 15*time.Second, cfg.HeartbeatInterval)
}

func TestHeartbeatIntervalDefaultFollowsReclaimMinIdle(t *testing.T) {
	setEnv(t, "bayer-17909")
	// Custom ReclaimMinIdle: default HeartbeatInterval should track it.
	t.Setenv("DASHER_RECLAIM_MIN_IDLE", "60s")
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, cfg.HeartbeatInterval)
}

func TestHeartbeatIntervalExplicit(t *testing.T) {
	setEnv(t, "bayer-17909")
	t.Setenv("DASHER_HEARTBEAT_INTERVAL", "10s")
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, cfg.HeartbeatInterval)
}

func TestHeartbeatIntervalBadValue(t *testing.T) {
	setEnv(t, "bayer-17909")
	t.Setenv("DASHER_HEARTBEAT_INTERVAL", "not-a-duration")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrBadHeartbeatInterval)
}

func TestHeartbeatIntervalZero(t *testing.T) {
	setEnv(t, "bayer-17909")
	t.Setenv("DASHER_HEARTBEAT_INTERVAL", "0s")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrBadHeartbeatInterval)
}

func TestHeartbeatIntervalNegative(t *testing.T) {
	setEnv(t, "bayer-17909")
	t.Setenv("DASHER_HEARTBEAT_INTERVAL", "-5s")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrBadHeartbeatInterval)
}

func TestHeartbeatIntervalTooLarge(t *testing.T) {
	setEnv(t, "bayer-17909")
	// HeartbeatInterval (20s) > ReclaimMinIdle/2 (15s) — must be rejected.
	t.Setenv("DASHER_HEARTBEAT_INTERVAL", "20s")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrHeartbeatIntervalTooLarge)
}


func TestGatewayValidConfig(t *testing.T) {
	setEnv(t, "gateway-valid-99970")
	t.Setenv("DASHER_TEST_GW_URL", "https://gateway.example.com")
	t.Setenv("DASHER_TEST_GW_CODE", "myapp")
	t.Setenv("DASHER_TEST_GW_KEY", "secret")
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "DASHER_TEST_GW_URL", cfg.Instance.Services.Gateway.URLEnv)
}

func TestGatewayMissingURLEnvVar(t *testing.T) {
	setEnv(t, "gateway-missing-url-99969")
	// DASHER_MISSING_GW_URL not set
	t.Setenv("DASHER_TEST_GW_CODE", "myapp")
	t.Setenv("DASHER_TEST_GW_KEY", "secret")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrMissingGatewayURLEnv)
}

func TestGatewayMissingCredentials(t *testing.T) {
	setEnv(t, "gateway-missing-creds-99968")
	t.Setenv("DASHER_TEST_GW_URL", "https://gateway.example.com")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrMissingGatewayCredentials)
}
