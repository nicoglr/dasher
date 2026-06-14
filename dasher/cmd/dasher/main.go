package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"4gclinical.com/dasher"
	"4gclinical.com/dasher/internal/config"
	"4gclinical.com/dasher/internal/consume"
	"4gclinical.com/dasher/internal/enrich"
	"4gclinical.com/dasher/internal/lookup"
	"4gclinical.com/dasher/internal/produce"
	"4gclinical.com/dasher/internal/registry"
	"4gclinical.com/dasher/internal/services"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	slog.Info("Dasher starting",
		"instance", cfg.InstanceID,
		"consumer", cfg.Consumer,
		"redis", cfg.RedisAddr,
		"streams", len(cfg.Instance.Streams),
	)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	// NOTE: rdb.Close() is intentionally deferred BEFORE the errgroup block.
	// redis client close is idempotent and doesn't affect running consumers.
	defer rdb.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	svc, err := services.New(ctx, cfg.Instance)
	if err != nil {
		slog.Error("init services", "err", err)
		os.Exit(1)
	}
	// svc.Close() is called after g.Wait() below — after all consumers stop —
	// so the DB pool stays available while consumers are running.

	inst := dasher.InstanceContext{
		ID:       cfg.Instance.ID,
		Config:   cfg.Instance,
		Services: *svc,
	}

	reg := registry.Default()
	policy := dasher.FailLoud{}

	// Build the lookup catalog once (each Lookup is shared across bindings).
	resolvedLookups, err := buildLookupCatalog(cfg.Instance, svc)
	if err != nil {
		slog.Error("build lookup catalog", "err", err)
		os.Exit(1)
	}

	// Per-instance shared producer.
	producer := produce.New(rdb, cfg.InstanceID)

	g, gctx := errgroup.WithContext(ctx)
	for _, b := range cfg.Instance.Streams {
		h, err := buildHandler(b, reg, producer, resolvedLookups)
		if err != nil {
			slog.Error("build handler", "stream", b.Stream, "err", err)
			os.Exit(1)
		}
		key := cfg.InstanceID + "." + b.Stream
		c := consume.New(rdb, key, cfg.Group, cfg.Consumer, h, inst, policy, cfg.EscalateAfter,
			consume.WithReclaimMinIdle(cfg.ReclaimMinIdle),
			consume.WithReclaimInterval(cfg.ReclaimInterval),
			consume.WithConsumerGCInterval(cfg.ConsumerGCInterval),
			consume.WithConsumerGCTimeout(cfg.ConsumerGCTimeout),
		)
		g.Go(func() error { return c.Run(gctx) })
	}

	werr := g.Wait()
	if werr != nil {
		slog.Error("dasher exited", "err", werr)
	}

	// Shutdown ordering: close the DB pool strictly AFTER all consumers stop.
	// Placing svc.Close() here (not as a defer before g.Wait()) ensures no
	// consumer can attempt a lookup after the pool is closed.
	svc.Close()

	if werr != nil {
		os.Exit(1)
	}
}

// buildLookupCatalog instantiates all catalog entries once, returning a map
// of name → Lookup and a map of name → resolved TTL duration.
//
// EXTENSION POINTS — adding a new lookup type should NOT require editing this
// function. The intended path is:
//   1. Register a Factory under a new type name in lookup.DefaultRegistry
//      (see lookup/sql.go init() for the pattern). The factory reads its own
//      type-specific fields out of Spec.Raw.
//   2. Allow the new type name in config.allowedLookupTypes.
// The two pieces of hardwiring below are the current obstacles to that:
//   - config.LookupSpecRaw has a typed SQL field, so the YAML schema only
//     understands SQL. To support arbitrary params, give LookupSpecRaw a
//     map[string]any of unknown keys (custom UnmarshalYAML) and pass it
//     straight through as Spec.Raw.
//   - The Raw map below is built with a hardcoded "sql" key. Once the config
//     carries a generic map, replace this with `Raw: raw.Raw` and this
//     function becomes fully type-agnostic.
func buildLookupCatalog(inst config.InstanceConfig, svc *services.Services) (map[string]lookup.Lookup, error) {
	out := make(map[string]lookup.Lookup, len(inst.Lookups))
	for name, raw := range inst.Lookups {
		factory, ok := lookup.DefaultRegistry[raw.Type]
		if !ok {
			return nil, fmt.Errorf("lookup %q: unknown type %q", name, raw.Type)
		}
		var ttl time.Duration
		if raw.TTL != "" {
			var err error
			ttl, err = time.ParseDuration(raw.TTL)
			if err != nil {
				return nil, fmt.Errorf("lookup %q: invalid ttl %q: %w", name, raw.TTL, err)
			}
		}
		spec := lookup.Spec{
			Type: raw.Type,
			TTL:  ttl,
			// HARDWIRED: only the "sql" field is forwarded. A generic
			// map[string]any from config would let factories accept arbitrary
			// parameters without changing this call site. See the doc comment.
			Raw: map[string]any{"sql": raw.SQL},
		}
		deps := lookup.Deps{Pool: svc.DB}
		l, err := factory(spec, deps)
		if err != nil {
			return nil, fmt.Errorf("lookup %q: %w", name, err)
		}
		out[name] = l
	}
	return out, nil
}

// buildHandler composes the handler chain for a binding in fixed order:
// Enrich → handler-or-Noop → EmitAfter.
func buildHandler(
	b config.StreamBinding,
	reg registry.Registry,
	producer *produce.Producer,
	resolvedLookups map[string]lookup.Lookup,
) (dasher.Handler, error) {
	// Base handler: named handler or Noop for pure transforms.
	var base dasher.Handler = dasher.Noop
	if b.Handler != "" {
		h, ok := reg.Lookup(b.Handler)
		if !ok {
			return nil, fmt.Errorf("unknown handler %q", b.Handler)
		}
		base = h
	}

	h := base

	// EmitAfter wraps the base handler (inner-to-outer: emit happens after base).
	if b.Emit != "" {
		h = enrich.EmitAfter(producer, b.Emit, h)
	}

	// Enrich wraps everything (outermost: enrichment happens first).
	if len(b.Enrich) > 0 {
		rules := make([]lookup.EnrichRule, 0, len(b.Enrich))
		for _, rule := range b.Enrich {
			l, ok := resolvedLookups[rule.Lookup]
			if !ok {
				return nil, fmt.Errorf("enrich references unknown lookup %q", rule.Lookup)
			}
			rules = append(rules, lookup.EnrichRule{
				LookupName: rule.Lookup,
				Lookup:     l,
				Bind:       rule.Bind,
				Into:       rule.Into,
			})
		}
		runner := lookup.NewRunner(rules)
		h = enrich.Enrich(runner, h)
	}

	return h, nil
}
