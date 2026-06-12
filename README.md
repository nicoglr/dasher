# Dasher

Dasher is a CDC event consumer. It reads events from Redis Streams (written by **WALker**) and dispatches them to registered handlers.

## How it works

- One Dasher process per application instance (`DASHER_INSTANCE_ID`)
- One goroutine per watched stream, using Redis consumer groups (`XREADGROUP` / `XACK` / `XAUTOCLAIM`)
- Acks only after the handler succeeds — at-least-once delivery
- Crashed/interrupted entries are reclaimed on startup via `XAUTOCLAIM`

Stream keys follow the convention `<instance-id>.<stream>`, e.g. `bayer-17909.cdc.orders`. WALker must emit the same format.

## Failure semantics

| Handler return | Behavior |
|---|---|
| `nil` | `XACK` — done |
| `dasher.Poison(err)` | Fatal — process exits (entry stays in PEL for inspection) |
| Any other error | Transient — retry with backoff; log escalates `WARN→ERROR` after N retries |

Malformed envelope (unparseable stream entry) is also fatal.

## Configuration

```
DASHER_INSTANCE_ID   which instance this process serves
DASHER_CONFIG        path to YAML config file
DASHER_AUTH_TOKEN    secret token for internal service calls
DASHER_REDIS_ADDR    Redis address
```

Example `config.yaml`:

```yaml
instances:
  bayer-17909:
    services:
      internal:
        base_url: https://bayer.internal.svc
    streams:
      - stream: cdc.orders      # full key: bayer-17909.cdc.orders
        handler: order-sync@v1
      - stream: cdc.products
        handler: product-sync
```

Config is fully expanded per instance — Dasher does no merging. Each process reads only its own instance block.

## Package layout

```
cmd/dasher/main.go      wiring: load config, build Services, start consume loops
internal/config         YAML + env load, instance selection, validation
internal/consume        XAUTOCLAIM reclaim, XREADGROUP loop, XACK, retry/backoff
internal/event          parse stream entry → Event (UseNumber for numeric precision)
internal/registry       name → Handler map (wazero seam for future hot-swap)
internal/services       authenticated HTTP client to the internal service
internal/handlers       concrete handlers (order-sync, product-sync, …)
```

## Handler interface

```go
type Handler interface {
    Handle(ctx context.Context, inst InstanceContext, evt Event) error
}
```

Handlers are registered by name and bound to streams in config. This indirection is the wazero seam for future hot-swappable modules.

**Handlers must be idempotent** — delivery is at-least-once; use `lsn` for dedup.

## Relationship to WALker

WALker produces the streams; Dasher consumes them. Both share the same stream-key convention and the same fail-loud error model. See WALker's README for the producer side.

## Out of scope (v0)

- wazero dynamic handler loading
- One process serving multiple instances (model C)
- Dead-letter queue
- Metrics, TLS
