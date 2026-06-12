package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"4gclinical.com/dasher"
	"4gclinical.com/dasher/internal/config"
	"4gclinical.com/dasher/internal/consume"
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
	defer rdb.Close()

	svc := services.New(cfg.Instance, cfg.AuthToken)
	inst := dasher.InstanceContext{
		ID:       cfg.Instance.ID,
		Config:   cfg.Instance,
		Services: svc,
	}
	reg := registry.Default()
	policy := dasher.FailLoud{}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)
	for _, b := range cfg.Instance.Streams {
		h, ok := reg.Lookup(b.Handler)
		if !ok {
			slog.Error("unknown handler", "handler", b.Handler, "stream", b.Stream)
			os.Exit(1)
		}
		key := cfg.InstanceID + "." + b.Stream
		c := consume.New(rdb, key, cfg.Group, cfg.Consumer, h, inst, policy, cfg.EscalateAfter)
		g.Go(func() error { return c.Run(gctx) })
	}

	if err := g.Wait(); err != nil {
		slog.Error("dasher exited", "err", err)
		os.Exit(1)
	}
}
