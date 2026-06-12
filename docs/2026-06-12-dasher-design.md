# Dasher — Design Spec (v0)

Status: draft (for hand refinement)
Date: 2026-06-12

## 1. Purpose & relationship to WALker

Dasher is the consumer counterpart to **WALker**. WALker (N processes, one per
Postgres DB) reads committed Postgres changes via logical decoding and publishes
one CDC event per change to a Redis Stream, with **at-least-once** delivery and
a source `lsn` on every event.

**Stream-key convention (shared producer/consumer contract):** every stream key
is **app-instance-first**, `<app-instance>.<prefix>.<rest>` — e.g.
`bayer-17909.cdc.orders`. The leading segment is the application instance id;
the remainder is the logical stream name (`cdc.orders`, or later something like
`messages.billing`). This is a change from WALker's earlier `cdc.<db>.<table>`
scheme: **WALker must be updated to emit app-instance-first keys**, where its
leading segment equals the same instance id Dasher is configured with. See §8.

Dasher reads those streams via Redis **consumer groups**, dispatches each event
to a registered **handler**, and acks only after the handler succeeds.

Two motivations, mirroring WALker:
1. **Learn** the Redis Streams consumer-group mechanics by speaking the raw
   primitives directly (`XREADGROUP` / `XACK` / `XAUTOCLAIM`) — the same
   mechanism a library like `gtrs` wraps. The redis client is the only real
   dependency in v0.
2. Have a lean, in-house CDC consumer that turns stream events into actions
   (e.g. authenticated calls to internal services).

Priorities, in order: **reliable**, **simple**. Latency is not important.

## 2. Topology & process model

**v0 runs model B: one Dasher process per application instance**, mirroring
WALker's one-process-per-DB model: full isolation (one instance's crash-loop
never stalls another), independent consumer-group/offset state, and the same
"fail loud, supervisor restarts" supervision model. This is chosen for v0
because it keeps the fail-loud error model (§5) trivial and gives true
per-tenant crash/deploy isolation while the instance count is small.

- Each process is told which instance(s) it serves via `DASHER_INSTANCE_ID`. In
  v0 this selector is bounded to exactly **one** instance; the process loads
  that instance's fully-expanded config block and watches its streams.
- **One goroutine per watched stream**, each owning its `XREADGROUP` loop. An
  `errgroup` ties them together: a fatal error invokes the **stream error
  policy** (§5) which, in v0, cancels the shared context so the process exits
  non-zero (supervisor restarts). This extends WALker's "one fatal error →
  exit" model to the N-streams-per-instance case.

Axis summary:
- **Application instance** = deployment unit in v0 (one Dasher process, selected
  by `DASHER_INSTANCE_ID`).
- **Stream** = unit of work within a process (one goroutine + one handler each).

### Forward-compat: cheap migration to model C (one process, many instances)

As instance count grows, running N containers (and the matching k8s workloads)
becomes the cost driver, and **model C** — one Dasher process serving many
instances' streams — becomes attractive (also the natural shape for the eventual
wazero hot-swap: one host process, per-instance modules). The design keeps the
B→C migration to *config + a localized error-policy change*, never a redesign,
by planting these seams now:

1. **Keying is already C-ready.** `<instance>.cdc.orders` is identical under B
   and C — no contract change ever needed.
2. **The process serves a *set* of instances; v0 set = exactly one.** The run
   layer iterates `instances × streams` uniformly, so "add more instances" is a
   longer list into the same errgroup loop, not new machinery. (A process
   already runs multiple stream-loops per instance.)
3. **Consumer name = process identity (hostname/pod), not the instance id** — so
   B→C changes no consumer-group semantics (groups are per-stream regardless).
4. **Poison handling sits behind a one-line `StreamErrorPolicy` seam.** v0
   policy = cancel-group/exit (fail-loud). C swaps it for *quarantine this one
   stream, keep siblings running* — a localized edit (see §5).
5. **Config already lists all instances of the stack; the selector just narrows
   it.** `DASHER_INSTANCE_ID` later accepts many / a glob / "all"; config shape
   is unchanged.

Migration B→C is then: (1) let the selector take multiple instances, (2) flip
the error policy to per-stream quarantine, (3) deploy one pod instead of N.
The C-quarantine error path (and/or a DLQ) is consciously **deferred** — v0 stays
fail-loud; only its trigger is isolated behind the policy seam.

## 3. Consumption mechanism (consumer groups)

For each watched stream, on startup:

1. `XGROUP CREATE <stream> <group> $ MKSTREAM` — ignore `BUSYGROUP` (already
   exists). `MKSTREAM` so the group can be created before WALker has written
   anything.
2. **Reclaim** this consumer's interrupted work with `XAUTOCLAIM` (entries
   delivered but never acked because a previous process crashed), process them,
   then…
3. Read new entries with `XREADGROUP GROUP <group> <consumer> COUNT n BLOCK m
   STREAMS <stream> >`.

Identifiers:
- Group name: `dasher`.
- Consumer name: the **process identity** (hostname / k8s pod name), *not* the
  instance id — so a future model-C process reading many instances' streams
  needs no consumer-group renaming. **One consumer per process** in v0 (no
  horizontal scale within a group yet).

**Ack-after-success:** call `XACK` only after the handler returns success. This
gives at-least-once end-to-end. The residual redelivery window is "handler
succeeded but the process crashed before `XACK`," so **handlers must be
idempotent** (see §5 dedup).

## 4. Event shape & decode

WALker writes each stream entry with these fields:

| Field | Meaning |
|---|---|
| `op` | `insert` / `update` / `delete` |
| `table` | e.g. `orders` |
| `schema` | e.g. `public` |
| `lsn` | source LSN (use for dedup) |
| `streamed_at` | RFC3339 timestamp |
| `data` | full new row (INSERT/UPDATE) or PK only (DELETE), JSON |
| `old` | PK of previous row (UPDATE/DELETE), JSON |

Dasher parses each entry into an `Event`:

```go
type Event struct {
    ID         string          // Redis stream entry ID (for XACK)
    Op         string
    Table      string
    Schema     string
    LSN        string
    StreamedAt time.Time
    Data       map[string]any
    Old        map[string]any
}
```

`Data`/`Old` JSON is parsed with `json.Decoder` + `UseNumber()` so `bigint` /
`numeric` values keep exact decimal text and are never rounded through
`float64` (honoring WALker's consumer contract). A **malformed envelope** (an
entry that cannot be parsed into `Event`) is treated as **fatal / fail-loud** —
it indicates a contract violation, not a transient condition — and is not
retried.

## 5. Handler contract & failure semantics

```go
type Handler interface {
    Handle(ctx context.Context, inst InstanceContext, evt Event) error
}
```

Failure handling mirrors WALker's sink (transient → back-pressure; bad data →
fail loud):

- returns `nil` → `XACK` the entry.
- returns `dasher.Poison(err)` (a sentinel-wrapped error) → **fail loud +
  crash**: the event will never succeed (bad shape, business rejection). No DLQ
  in v0; surfacing poison loudly is wanted during learning.
- returns any other non-nil error → **transient**: retry with exponential
  backoff, blocking this stream's goroutine, never acking. Back-pressure, same
  as WALker's XADD retry. **Default for unclassified errors is transient.**
  Dasher keeps a per-entry **retry counter** and **escalates the log level from
  `WARN` to `ERROR` after N consecutive retries** (N configurable, default
  e.g. 10), so a persistently-down downstream surfaces loudly without the
  process ever giving up. The counter resets on success.

A poison error (and a malformed envelope) is routed through a single
**`StreamErrorPolicy`** seam rather than crashing inline:

```go
// Decides what a fatal stream error does. v0 ships only the fail-loud policy.
type StreamErrorPolicy interface {
    OnFatal(stream string, err error) // v0: cancel the errgroup → process exits
}
```

In v0 the policy cancels the `errgroup`, so the process exits non-zero
(supervisor restarts; the entry remains in the PEL for inspection) — plain
fail-loud. Keeping this behind one seam is what makes the future **model-C**
swap (§2) localized: C replaces it with a *quarantine* policy that stops only
the offending stream's goroutine and leaves sibling streams running (optionally
feeding a DLQ). No consume-loop changes required.

**Dedup is the handler's responsibility.** Because delivery is at-least-once,
handlers must be idempotent — e.g. an upsert that ignores `lsn <= last_seen`
for the row's primary key. A shared dedup helper may live in `internal/services`
later; it is not core to v0.

## 6. Instance identity & shared concerns

Instance identity is **opaque and decoupled from "database."** Handlers receive
an `InstanceContext`:

```go
type InstanceContext struct {
    ID       string          // opaque instance id, e.g. "bayer-17909"
    Config   InstanceConfig  // this instance's resolved config
    Services Services        // shared capabilities, built once per process
}
```

`Services` bundles the shared cross-consumer concerns — notably an
**authenticated HTTP client** to the internal service, with base URL and token
already wired in, so handlers never re-implement auth:

```go
type Services struct {
    Internal *InternalClient // authenticated client to the internal service
    // future: dedup store, metrics, etc.
}
```

`Services` lives in `internal/services` and is **extractable to its own Go
module later** when more than one consumer binary needs it. v0 keeps it as an
internal package (no premature module ceremony).

## 7. Handler registration & per-instance code selection

- Handlers are Go functions compiled into the binary, registered by **name** in
  a registry:

  ```go
  type Registry map[string]Handler
  ```

- The config's per-instance `streams` list binds each stream to a handler
  **name**. Different instances may bind the same stream to different handler
  names (e.g. `order-sync@v1` vs `order-sync@v2`), giving **per-instance code
  selection** in v0 with no special machinery.
- This name-based indirection is the **wazero seam**: later, registry lookup can
  resolve a name to a dynamically-loaded wasm module instead of a compiled-in
  Go func — hot-swappable, with no change to the consume loop or config shape.

## 8. Configuration (fully-expanded YAML + env secrets)

The YAML file is the v0 stand-in for ConfigHub's *compiled output*. It is
**fully expanded per instance — Dasher does no merging.** Dasher is purely a
consumer of compiled config; the future ConfigHub swap is "where the file comes
from," not "remove Dasher's resolver."

Env vars:

| Var | Meaning |
|---|---|
| `DASHER_INSTANCE_ID` | which instance block this process serves (selector) **and** the leading segment of every stream key |
| `DASHER_CONFIG` | path to the YAML config file |
| `DASHER_AUTH_TOKEN` | secret token for the internal-service client (never in file) |
| `DASHER_REDIS_ADDR` | Redis server address — strictly an environment concern (natural for a k8s pod spec); never in the file |

Example config:

```yaml
instances:
  bayer-17909:
    services:
      internal:
        base_url: https://bayer.internal.svc
        # token comes from DASHER_AUTH_TOKEN (env), never stored in the file
    streams:
      - stream: cdc.orders            # → key: bayer-17909.cdc.orders
        handler: order-sync@v1
      - stream: cdc.products          # → key: bayer-17909.cdc.products
        handler: product-sync
      - stream: messages.billing      # → key: bayer-17909.messages.billing
        handler: billing-sync
  beigene-22001:
    services:
      internal:
        base_url: https://beigene.internal.svc
    streams:
      - stream: cdc.orders            # → key: beigene-22001.cdc.orders
        handler: order-sync@v2        # different consumer code for this instance
      - stream: cdc.products
        handler: product-sync
```

A process reads **only** `instances.<DASHER_INSTANCE_ID>`. The full file lists
all instances of the stack (the "resolved config for all application instances
of the current stack" idea); each process picks out its own block.

**Stream-key resolution:** the full Redis key is `DASHER_INSTANCE_ID + "." +
stream`, where `stream` is the value listed verbatim in config (it already
carries its own prefix, e.g. `cdc.orders` or `messages.billing`). Dasher treats
`stream` as an opaque suffix and only prepends the app-instance segment. There
is **no separate `stream_prefix` field** — the prefix is just the leading part
of the stream name you write — and **no per-instance namespace mapping**: the
key segment *is* the instance id. (If a future deployment ever needs the key
segment to differ from the logical id, add an optional per-instance `namespace`
override then — additive, no v0 cost.)

WALker is configured so its leading stream-key segment equals this same
instance id (e.g. WALker for `bayer-17909` writes `bayer-17909.cdc.orders`).

## 9. Go package layout

Flat and small, matching WALker's style. Each package has one job and a small
interface so it can be tested in isolation.

```
cmd/dasher/main.go      — wiring: load config, select served instance(s),
                          build Services, register handlers, start one consume
                          loop (goroutine) per (instance × stream) via errgroup
internal/config         — YAML + env load; select instance block(s); validate
internal/consume        — group ensure, XAUTOCLAIM reclaim, XREADGROUP loop,
                          XACK, retry/backoff, StreamErrorPolicy on fatal
internal/event          — parse a stream entry → Event (UseNumber for precision)
internal/registry       — name → Handler map
internal/services       — authenticated internal-service client (+ future helpers)
internal/handlers       — concrete v0 handlers (order-sync, product-sync, …)
```

Key interfaces:

- `Handler.Handle(ctx, inst InstanceContext, evt Event) error`
- `event.Parse(values map[string]any) (Event, error)`
- `services.New(cfg InstanceConfig, token string) Services`

## 10. Error handling (summary)

- **Handler transient error:** retry with backoff, block the stream's goroutine,
  do not ack. Back-pressure. Retry forever, but escalate log `WARN`→`ERROR`
  after N consecutive retries (counter resets on success).
- **Handler poison (`dasher.Poison`):** routed through `StreamErrorPolicy`. v0
  policy = fail loud, crash the process (errgroup cancels siblings; entry stays
  in the PEL). Model-C later swaps in a per-stream quarantine policy.
- **Envelope decode error:** fatal/fail-loud (contract violation), do not retry.
- **Redis connection drop:** exit; supervisor restarts; group state + PEL on the
  Redis server let the new process resume (`XAUTOCLAIM` reclaims pending).

## 11. Testing

- `internal/event`: table-driven decode of sample stream entries → `Event`,
  including `bigint`/`numeric` precision. No Redis.
- `internal/consume`: against `miniredis` — assert ack-after-success,
  no-ack-on-transient-error, `XAUTOCLAIM` reclaim of pending, and
  fatal-on-poison.
- `internal/services`: against `httptest` — assert the auth header and base URL
  wiring.
- End-to-end smoke: bring up WALker + Dasher; `INSERT`/`UPDATE`/`DELETE` in
  Postgres; assert the bound handler is invoked and the entry is acked (the
  group's PEL empties).

## 12. Out of scope for v0

- wazero / dynamic hot-swappable handler loading (the registry is the seam).
- Model C (one process serving many instances) + per-stream quarantine policy
  and/or dead-letter queue — the seams (§2) are planted; the error path is not
  built. v0 is model B, fail-loud.
- Multiple consumers per group (horizontal scale within a stream).
- ConfigHub resolver / base+overlay merge — the v0 file is pre-expanded.
- Dasher-side dedup store (handlers are idempotent on `lsn`).
- Metrics, TLS.

## Open questions (for hand refinement)

1. ~~Consumer/group naming for horizontal scale.~~ RESOLVED for now: consumer
   name = process identity (hostname/pod); revisit if we add multiple consumers
   per group within one stream.
2. ~~Should `redis_addr` / `stream_prefix` be per-instance or global?~~
   RESOLVED: `redis_addr` is environment-only (`DASHER_REDIS_ADDR`, not in the
   file). `stream_prefix` is removed entirely — the prefix is now part of the
   stream name listed in config (`cdc.orders`), and the stream key is
   app-instance-first (`<instance-id>.<stream>`). See §8.
3. ~~Backoff cap / escalate-to-crash on long stalls.~~ RESOLVED: retry forever
   (like WALker), but keep a per-entry retry counter and escalate the log level
   `WARN`→`ERROR` after N consecutive retries (counter resets on success). No
   crash on stall.
4. Handler versioning convention (`@v1`) — free-form string vs structured.
5. Do any handlers need ordered, cross-stream transaction grouping? (Assumed no,
   same as WALker's per-change model.)

## Decisions (rationale)

- **Model B (process per instance) for v0, with seams for a cheap migration to
  model C (one process, many instances).** B keeps the fail-loud error model
  trivial and gives true per-tenant crash/deploy isolation at low instance
  counts; it stays symmetric with WALker. C becomes attractive as instance
  count grows (fewer pods, shared resources) and is the natural shape for the
  wazero hot-swap. The five seams (§2) — C-ready keying, set-of-instances run
  layer, process-identity consumer name, `StreamErrorPolicy`, narrowing
  selector — keep B→C to config + a localized error-policy change. Rejected:
  shared single stream with instance encoded in the payload (loses per-tenant
  data separation, and one poison event stalls every tenant).
- **Consumer groups over simple XREAD+stored-ID** — the real production
  mechanism with server-side pending tracking and crash recovery; best learning
  value; `gtrs` wraps exactly this.
- **Transient retry + fail-loud poison (behind a `StreamErrorPolicy` seam), no
  DLQ** — mirrors WALker's sink and decode-error behavior; simplest; surfaces
  bad data loudly while learning. The seam lets model C later swap crash for
  per-stream quarantine without touching the consume loop.
- **Opaque instance identity + InstanceContext** — generic, decoupled from
  "database"; aligns with ConfigHub's per-instance resolved config.
- **Fully-expanded config, no in-Dasher merge** — keeps Dasher's job narrow
  ("consume compiled config"); the future ConfigHub swap changes only the file
  source.
- **Per-instance stream→handler bindings + name registry** — per-instance code
  selection now; clean wazero seam later.
- **Services on InstanceContext, internal package** — single handler arg; shared
  auth in one place; extract to a module only when a second binary needs it.
