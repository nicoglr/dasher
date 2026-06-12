package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// StreamBinding binds a logical stream name to a handler name.
type StreamBinding struct {
	Stream  string `yaml:"stream"`
	Handler string `yaml:"handler"`
}

// InternalServiceConfig is the per-instance config for the internal service client.
type InternalServiceConfig struct {
	BaseURL string `yaml:"base_url"`
}

// ServicesConfig groups the shared per-instance service config.
type ServicesConfig struct {
	Internal InternalServiceConfig `yaml:"internal"`
}

// InstanceConfig is one application instance's fully-expanded config block.
type InstanceConfig struct {
	ID       string          `yaml:"-"`
	Services ServicesConfig  `yaml:"services"`
	Streams  []StreamBinding `yaml:"streams"`
}

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
		return Config{}, fmt.Errorf("DASHER_INSTANCE_ID is required")
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
		return Config{}, fmt.Errorf("instance %q not found in config %q", instanceID, path)
	}
	inst.ID = instanceID
	if len(inst.Streams) == 0 {
		return Config{}, fmt.Errorf("instance %q has no streams", instanceID)
	}
	for _, s := range inst.Streams {
		if s.Stream == "" {
			return Config{}, fmt.Errorf("instance %q: a stream binding is missing its stream name", instanceID)
		}
		if s.Handler == "" {
			return Config{}, fmt.Errorf("instance %q: stream %q has no handler name", instanceID, s.Stream)
		}
	}

	escalate := 10
	if v := os.Getenv("DASHER_ESCALATE_AFTER"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("DASHER_ESCALATE_AFTER %q must be a positive integer", v)
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

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
