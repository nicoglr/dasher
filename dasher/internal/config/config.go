package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"

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
	Type string        `yaml:"type"`
	TTL  string        `yaml:"ttl"`  // parsed later as time.Duration
	SQL  string        `yaml:"sql"`  // used by type=sql
}

// EnrichRuleConfig is one lookup step in a binding's enrich list.
type EnrichRuleConfig struct {
	Lookup string            `yaml:"lookup"`
	Bind   map[string]string `yaml:"bind"`
	Into   string            `yaml:"into"`
	OnMiss string            `yaml:"on_miss"`
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
	BaseURL string `yaml:"base_url"`
}

// ServicesConfig groups the shared per-instance service config.
type ServicesConfig struct {
	Internal InternalServiceConfig `yaml:"internal"`
	DB       DBConfig              `yaml:"db"`
}

// InstanceConfig is one application instance's fully-expanded config block.
type InstanceConfig struct {
	ID       string                    `yaml:"-"`
	Services ServicesConfig            `yaml:"services"`
	Lookups  map[string]LookupSpecRaw  `yaml:"lookups"`
	Streams  []StreamBinding           `yaml:"streams"`
}

// Sentinel errors for named validation failures in Load.
var (
	ErrMissingInstanceID  = errors.New("DASHER_INSTANCE_ID is required")
	ErrInstanceNotFound   = errors.New("instance not found in config")
	ErrNoStreams           = errors.New("instance has no streams")
	ErrMissingStreamName  = errors.New("stream binding missing stream name")
	ErrMissingHandlerName = errors.New("stream binding missing handler name")
	ErrBadEscalateAfter   = errors.New("DASHER_ESCALATE_AFTER must be a positive integer")
	ErrEmptyBinding       = errors.New("stream binding must have at least one of handler or emit")
	ErrSelfEmit           = errors.New("emit must not point to the same stream")
	ErrEmitCycle          = errors.New("emit must not point to another binding's stream (cycle)")
	ErrUnknownLookup      = errors.New("enrich references unknown lookup name")
	ErrUnknownLookupType  = errors.New("lookup catalog entry has unknown type")
	ErrMissingDBConfig    = errors.New("enrich requires db config (services.db.dsn_env)")
	ErrBadOnMiss          = errors.New("on_miss must be emit_unenriched or fail")
	ErrBadBindKey         = errors.New("bind value must be a valid column identifier")
)

type file struct {
	Instances map[string]InstanceConfig `yaml:"instances"`
}

// Config is the resolved runtime configuration for this Dasher process.
type Config struct {
	InstanceID    string
	RedisAddr     string
	AuthToken     string
	Group         string // consumer group name, always "dasher" in v0
	Consumer      string // consumer name = process identity (hostname)
	EscalateAfter int    // consecutive transient retries before WARN→ERROR escalation
	Instance      InstanceConfig
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

	return Config{
		InstanceID:    instanceID,
		RedisAddr:     getenv("DASHER_REDIS_ADDR", "localhost:6379"),
		AuthToken:     os.Getenv("DASHER_AUTH_TOKEN"),
		Group:         "dasher",
		Consumer:      consumer,
		EscalateAfter: escalate,
		Instance:      inst,
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
	}

	// Collect all stream names for cycle detection.
	streamNames := make(map[string]bool, len(inst.Streams))
	for _, s := range inst.Streams {
		if s.Stream != "" {
			streamNames[s.Stream] = true
		}
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
		// Emit must not point at another binding's stream (one-hop cycle).
		if s.Emit != "" && streamNames[s.Emit] {
			return fmt.Errorf("stream %q emit %q: %w", s.Stream, s.Emit, ErrEmitCycle)
		}
		// Validate enrich rules.
		for _, rule := range s.Enrich {
			if _, ok := inst.Lookups[rule.Lookup]; !ok {
				return fmt.Errorf("stream %q enrich lookup %q: %w", s.Stream, rule.Lookup, ErrUnknownLookup)
			}
			if rule.OnMiss != "" && rule.OnMiss != "emit_unenriched" && rule.OnMiss != "fail" {
				return fmt.Errorf("stream %q: %w %q", s.Stream, ErrBadOnMiss, rule.OnMiss)
			}
			for _, col := range rule.Bind {
				if !validIdentRe.MatchString(col) {
					return fmt.Errorf("stream %q: %w %q", s.Stream, ErrBadBindKey, col)
				}
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
