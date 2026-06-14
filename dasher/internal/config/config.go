package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// validIdentRe matches a valid column/bind identifier.
var validIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// allowedLookupTypes is the static allowlist of lookup type names.
var allowedLookupTypes = map[string]bool{
	"sql": true,
}

// DBConfig configures the optional database pool for sql lookups.
type DBConfig struct {
	// DSNEnv is the name of the environment variable holding the DSN.
	DSNEnv string `yaml:"dsn_env"`
	// MaxConns is the maximum pool connections; defaults to 4 if zero.
	MaxConns int `yaml:"max_conns"`
}

// LookupSpecRaw is a raw catalog entry (type-specific fields preserved as-is).
type LookupSpecRaw struct {
	Type string `yaml:"type"`
	TTL  string `yaml:"ttl"` // parsed later as time.Duration
	SQL  string `yaml:"sql"` // used by type=sql
}

// EnrichRuleConfig is one lookup step in a binding's enrich list.
type EnrichRuleConfig struct {
	Lookup string            `yaml:"lookup"`
	Bind   map[string]string `yaml:"bind"`
	Into   string            `yaml:"into"`
}

// StreamBinding binds a logical stream name to an optional handler, optional
// enrich list, and optional emit target. At least one of handler/emit must be
// set. Enrich alone is invalid.
type StreamBinding struct {
	Stream  string             `yaml:"stream"`
	Handler string             `yaml:"handler"`
	Emit    string             `yaml:"emit"`
	Enrich  []EnrichRuleConfig `yaml:"enrich"`
}

// InternalServiceConfig is the per-instance config for the internal service client.
type InternalServiceConfig struct {
	// URLEnv is the name of the environment variable holding the base URL.
	URLEnv string `yaml:"url_env"`
	// TokenEnv is the name of the environment variable holding the bearer token.
	// Optional — omit for unauthenticated services.
	TokenEnv string `yaml:"token_env"`
}

// ServicesConfig groups the shared per-instance service config.
type ServicesConfig struct {
	Internal InternalServiceConfig `yaml:"internal"`
	DB       DBConfig              `yaml:"db"`
}

// InstanceConfig is one application instance's fully-expanded config block.
type InstanceConfig struct {
	ID       string                   `yaml:"-"`
	Services ServicesConfig           `yaml:"services"`
	Lookups  map[string]LookupSpecRaw `yaml:"lookups"`
	Streams  []StreamBinding          `yaml:"streams"`
}

// Sentinel errors for named validation failures in Load.
var (
	ErrMissingInstanceID = errors.New("DASHER_INSTANCE_ID is required")
	ErrInstanceNotFound  = errors.New("instance not found in config")
	ErrNoStreams          = errors.New("instance has no streams")
	ErrMissingStreamName = errors.New("stream binding missing stream name")
	ErrBadEscalateAfter  = errors.New("DASHER_ESCALATE_AFTER must be a positive integer")
	ErrEmptyBinding      = errors.New("stream binding must have at least one of handler or emit")
	ErrSelfEmit          = errors.New("emit must not point to the same stream")
	// ErrEmitCycle is returned when a DFS back-edge walk detects a genuine
	// cycle in the emit graph (A → B → … → A). A valid two-stage enrichment
	// chain (A → B, where B is terminal) is not a cycle and is accepted.
	ErrEmitCycle         = errors.New("emit graph contains a cycle")
	ErrUnknownLookup     = errors.New("enrich references unknown lookup name")
	ErrUnknownLookupType = errors.New("lookup catalog entry has unknown type")
	// ErrMissingDBConfig is checked at config-load time using the current
	// process environment. If the DSN env var is set after process start
	// (e.g. in test harnesses), validation passes but services.New will
	// leave DB nil, causing a nil-dereference at lookup time.
	ErrMissingDBConfig  = errors.New("enrich requires db config (services.db.dsn_env)")
	// ErrMissingURLEnv is returned when url_env is set but the named env var is empty.
	ErrMissingURLEnv = errors.New("services.internal.url_env is set but the env var is empty")
	ErrBadBindKey      = errors.New("bind value must be a valid column identifier")
	ErrBadLookupTTL    = errors.New("lookup ttl is invalid")

	// Consumer-group lifecycle tuning.
	ErrBadReclaimMinIdle         = errors.New("DASHER_RECLAIM_MIN_IDLE must be a positive duration")
	ErrBadReclaimInterval        = errors.New("DASHER_RECLAIM_INTERVAL must be a positive duration")
	ErrBadConsumerGCInterval     = errors.New("DASHER_CONSUMER_GC_INTERVAL must be a positive duration")
	ErrBadConsumerGCTimeout      = errors.New("DASHER_CONSUMER_GC_TIMEOUT must be a positive duration")
	ErrConsumerGCTimeoutTooSmall = errors.New("DASHER_CONSUMER_GC_TIMEOUT must be greater than DASHER_RECLAIM_MIN_IDLE")
)

type file struct {
	Instances map[string]InstanceConfig `yaml:"instances"`
}

// Config is the resolved runtime configuration for this Dasher process.
type Config struct {
	InstanceID    string
	RedisAddr     string
	Group         string // consumer group name, always "dasher" in v0
	Consumer      string // consumer name = process identity (hostname)
	EscalateAfter int    // consecutive transient retries before WARN→ERROR escalation
	Instance      InstanceConfig

	// Consumer-group lifecycle tuning (see DASHER_* env vars).
	ReclaimMinIdle     time.Duration // min idle before peer entries are reclaimed
	ReclaimInterval    time.Duration // how often the background peer-reclaim ticker fires
	ConsumerGCInterval time.Duration // how often the dead-consumer GC ticker fires
	ConsumerGCTimeout  time.Duration // idle threshold for dead-consumer removal
}

// Load reads env vars + the YAML config file, selects this process's instance
// block (DASHER_INSTANCE_ID), and validates it. Dasher does no merging — the
// file is the fully-expanded compiled config.
func Load() (Config, error) {
	instanceID := os.Getenv("DASHER_INSTANCE_ID")
	if instanceID == "" {
		return Config{}, fmt.Errorf("load config: %w", ErrMissingInstanceID)
	}

	path := getenv("DASHER_CONFIG", "config.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	var f file
	if err := yaml.Unmarshal(raw, &f); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}

	inst, ok := f.Instances[instanceID]
	if !ok {
		return Config{}, fmt.Errorf("instance %q: %w", instanceID, ErrInstanceNotFound)
	}
	inst.ID = instanceID

	if err := validateInstance(inst); err != nil {
		return Config{}, fmt.Errorf("instance %q: %w", instanceID, err)
	}

	escalate := 10
	if v := os.Getenv("DASHER_ESCALATE_AFTER"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("DASHER_ESCALATE_AFTER %q: %w", v, ErrBadEscalateAfter)
		}
		escalate = n
	}

	consumer, err := os.Hostname()
	if err != nil || consumer == "" {
		consumer = "dasher"
	}

	reclaimMinIdle, err := parseDurationEnv("DASHER_RECLAIM_MIN_IDLE", 30*time.Second, ErrBadReclaimMinIdle)
	if err != nil {
		return Config{}, err
	}
	reclaimInterval, err := parseDurationEnv("DASHER_RECLAIM_INTERVAL", 5*time.Second, ErrBadReclaimInterval)
	if err != nil {
		return Config{}, err
	}
	consumerGCInterval, err := parseDurationEnv("DASHER_CONSUMER_GC_INTERVAL", 5*time.Minute, ErrBadConsumerGCInterval)
	if err != nil {
		return Config{}, err
	}
	consumerGCTimeout, err := parseDurationEnv("DASHER_CONSUMER_GC_TIMEOUT", 10*time.Minute, ErrBadConsumerGCTimeout)
	if err != nil {
		return Config{}, err
	}
	if consumerGCTimeout <= reclaimMinIdle {
		return Config{}, fmt.Errorf(
			"DASHER_CONSUMER_GC_TIMEOUT (%s) must be > DASHER_RECLAIM_MIN_IDLE (%s): %w",
			consumerGCTimeout, reclaimMinIdle, ErrConsumerGCTimeoutTooSmall)
	}

	return Config{
		InstanceID:         instanceID,
		RedisAddr:          getenv("DASHER_REDIS_ADDR", "localhost:6379"),
		Group:              "dasher",
		Consumer:           consumer,
		EscalateAfter:      escalate,
		Instance:           inst,
		ReclaimMinIdle:     reclaimMinIdle,
		ReclaimInterval:    reclaimInterval,
		ConsumerGCInterval: consumerGCInterval,
		ConsumerGCTimeout:  consumerGCTimeout,
	}, nil
}

// validateInstance validates an InstanceConfig.
func validateInstance(inst InstanceConfig) error {
	if len(inst.Streams) == 0 {
		return ErrNoStreams
	}

	// Validate lookup catalog.
	for name, spec := range inst.Lookups {
		if !allowedLookupTypes[spec.Type] {
			return fmt.Errorf("lookup %q: %w %q", name, ErrUnknownLookupType, spec.Type)
		}
		if spec.TTL != "" {
			if _, err := time.ParseDuration(spec.TTL); err != nil {
				return fmt.Errorf("lookup %q: invalid ttl %q: %w", name, spec.TTL, ErrBadLookupTTL)
			}
		}
	}

	// Validate internal service URL env var.
	if inst.Services.Internal.URLEnv != "" && os.Getenv(inst.Services.Internal.URLEnv) == "" {
		return ErrMissingURLEnv
	}

	// Check whether any binding has enrich (need DB config once).
	anyEnrich := false
	for _, s := range inst.Streams {
		if len(s.Enrich) > 0 {
			anyEnrich = true
			break
		}
	}
	if anyEnrich {
		if inst.Services.DB.DSNEnv == "" || os.Getenv(inst.Services.DB.DSNEnv) == "" {
			return ErrMissingDBConfig
		}
	}

	for _, s := range inst.Streams {
		if s.Stream == "" {
			return ErrMissingStreamName
		}
		// At least one of handler or emit must be set.
		if s.Handler == "" && s.Emit == "" {
			return fmt.Errorf("stream %q: %w", s.Stream, ErrEmptyBinding)
		}
		// Emit must not equal stream (self-loop).
		if s.Emit != "" && s.Emit == s.Stream {
			return fmt.Errorf("stream %q: %w", s.Stream, ErrSelfEmit)
		}
		// Validate enrich rules.
		for _, rule := range s.Enrich {
			if _, ok := inst.Lookups[rule.Lookup]; !ok {
				return fmt.Errorf("stream %q enrich lookup %q: %w", s.Stream, rule.Lookup, ErrUnknownLookup)
			}
			for _, col := range rule.Bind {
				if !validIdentRe.MatchString(col) {
					return fmt.Errorf("stream %q: %w %q", s.Stream, ErrBadBindKey, col)
				}
			}
		}
	}

	// Detect genuine cycles in the emit graph via DFS back-edge walk.
	if err := detectEmitCycles(inst.Streams); err != nil {
		return err
	}

	return nil
}

// detectEmitCycles performs a DFS back-edge walk on the emit graph.
// A → B where B is terminal (no further emit) is valid.
// A → B → … → A is a genuine cycle and returns ErrEmitCycle.
func detectEmitCycles(streams []StreamBinding) error {
	// Build adjacency: stream → emit target (only if non-empty).
	emitEdge := make(map[string]string, len(streams))
	for _, s := range streams {
		if s.Emit != "" {
			emitEdge[s.Stream] = s.Emit
		}
	}

	const (
		unvisited = 0
		visiting  = 1
		visited   = 2
	)
	state := make(map[string]int, len(streams))

	var dfs func(node string) error
	dfs = func(node string) error {
		switch state[node] {
		case visiting:
			return fmt.Errorf("stream %q: %w", node, ErrEmitCycle)
		case visited:
			return nil
		}
		state[node] = visiting
		if next, ok := emitEdge[node]; ok {
			if err := dfs(next); err != nil {
				return err
			}
		}
		state[node] = visited
		return nil
	}

	for _, s := range streams {
		if state[s.Stream] == unvisited {
			if err := dfs(s.Stream); err != nil {
				return err
			}
		}
	}
	return nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parseDurationEnv reads an env var as a positive time.Duration, returning
// def if unset. Returns a wrapped sentinel error on bad/non-positive values.
func parseDurationEnv(key string, def time.Duration, sentinel error) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s %q: %w", key, v, sentinel)
	}
	return d, nil
}
