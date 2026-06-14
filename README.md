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
| Lookup miss or multi-row result | Also fatal (poison) — same PEL consequence; see task #9 for DLQ |
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
- Dead-letter queue — lookup misses and multi-row results are permanent
  failures (poison). Today the process exits via `FailLoud` and the offending
  entry stays in the Redis PEL until an operator manually ACKs or deletes it.
  A DLQ would route these events to an inspectable stream instead. See task #9.
- Metrics, TLS

## Enrichment

Dasher can enrich CDC events with related data (e.g. resolve a `user_role_grant`
change to the affected user's email) **before** dispatching downstream. This
keeps enrichment queries off the Postgres replication slot and off the WALker
critical path.

### Pipeline

A binding has three orthogonal optional fields, composed in a fixed order:

```
event ─► [enrich: populate Event.Enrichment] ─► [handler: side effect] ─► [emit: XADD downstream] ─► XACK
```

| Field | Meaning | Required? |
|---|---|---|
| `enrich` | List of lookup rules; populates `Event.Enrichment` | Optional |
| `handler` | Named handler for side effects | At least one of handler/emit |
| `emit` | Downstream stream name; published after handler success | At least one of handler/emit |

**Combinations:**
- **Terminal**: `handler` only — standard pattern, unchanged from v0.
- **Pure transform**: `enrich` + `emit`, no handler — consume, enrich, re-publish.
- **Enriched terminal**: `enrich` + `handler` — enrich then side-effect.
- **handler + emit** (dual-write): both `handler` and `emit` are set — the
  handler runs first; only on success is the event emitted downstream. The
  stream is XACKed only after both succeed. Prefer pure transforms over
  dual-write to avoid partial-write foot-guns.

### Lookup catalog

Each instance declares named lookups under `lookups:`. A lookup is resolved
once at startup and shared across all bindings that reference it. In v1, only
`type: sql` is supported.

```yaml
lookups:
  user_email_by_role:
    type: sql
    ttl: 24h     # in-process LRU+TTL cache TTL
    sql: |
      SELECT u.email FROM users u
      JOIN user_roles ur ON ur.user_id = u.id
      WHERE ur.id = @role_id
```

### Bind keys

A bind maps a query parameter (`@name`) to a source column. Dasher automatically
tries `data[col]` first, then `old[col]` — the universal CDC rule for
insert/update/delete:

```yaml
bind:
  role_id: user_id    # @role_id ← data.user_id, falling back to old.user_id
```

### Caching

Results are cached in-process with bounded LRU + TTL (±15% jitter to prevent
synchronized expiry stampedes). The cache is shared across all bindings in an
instance.

### Type notes

Enrichment column values are decoded from JSON via `json.Number`. Integer
columns are represented as `int64` and fractional columns as `float64` after
normalisation, but very large floats may lose precision. Do not rely on exact
floating-point representation for enrichment columns used as identifiers.

### Operational notes

- **MAXLEN**: `enriched.*` streams produced by `emit` have no automatic
  MAXLEN trimming. Set `MAXLEN` on those streams (e.g. via `XADD … MAXLEN ~
  <N>` or a separate trim job) or accept unbounded growth.

### Prerequisites

- A `services.db` block with `dsn_env` pointing at the **primary** Postgres DSN.
- For `DELETE` enrichment: `ALTER TABLE … REPLICA IDENTITY FULL` — otherwise
  `old` will only contain the PK, not the enrichment column.

See `config.example.yaml` for a complete example.
