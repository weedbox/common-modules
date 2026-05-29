# NATS Connector Module

A NATS messaging connector module built on [NATS Go Client](https://github.com/nats-io/nats.go), integrated with [Uber Fx](https://github.com/uber-go/fx) for dependency injection. Includes JetStream support and work queue consumer functionality.

## Features

- Uber Fx dependency injection integration
- NATS core messaging support
- JetStream support for persistent messaging
- Work queue consumer with concurrency control
- Batch message processing for bulk-friendly handlers
- Batch publishing (cross-stream async fan-out) and single-stream atomic publishing
- Automatic panic recovery and consumer restart with exponential backoff
- Authentication support (credentials, NKey)
- TLS support
- Automatic reconnection

## Installation

```bash
go get github.com/weedbox/common-modules/nats_connector
```

## Quick Start

### Basic Usage

```go
package main

import (
    "github.com/weedbox/common-modules/logger"
    "github.com/weedbox/common-modules/nats_connector"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        logger.Module(),
        nats_connector.Module("nats"),
    ).Run()
}
```

### Publishing Messages

```go
package publisher

import (
    "github.com/weedbox/common-modules/nats_connector"
    "go.uber.org/fx"
)

type Params struct {
    fx.In

    NATS *nats_connector.NATSConnector
}

func (p *Publisher) Publish(subject string, data []byte) error {
    conn := p.params.NATS.GetConnection()
    return conn.Publish(subject, data)
}
```

### Subscribing to Messages

```go
func (s *Subscriber) Subscribe(subject string) error {
    conn := s.params.NATS.GetConnection()

    _, err := conn.Subscribe(subject, func(msg *nats.Msg) {
        // Handle message
        fmt.Printf("Received: %s\n", string(msg.Data))
    })

    return err
}
```

### Using JetStream

```go
func (p *Publisher) PublishToStream(subject string, data []byte) error {
    js := p.params.NATS.GetJetStreamContext()

    _, err := js.Publish(subject, data)
    return err
}
```

### Work Queue Consumer

The module provides a work queue consumer for processing messages with concurrency control:

```go
package worker

import (
    "context"

    "github.com/nats-io/nats.go/jetstream"
    "github.com/weedbox/common-modules/nats_connector"
    "go.uber.org/fx"
)

type Params struct {
    fx.In

    NATS *nats_connector.NATSConnector
}

func (w *Worker) StartConsumer() error {
    config := nats_connector.NewWorkQueueConsumerConfig()
    config.ConsumerName = "my-worker"
    config.Subjects = []string{"tasks.>"}
    config.MaxConcurrent = 10
    config.AckWait = 30 * time.Second
    config.OnError = func(err error) {
        log.Printf("Error: %v", err)
    }

    consumer, err := w.params.NATS.NewWorkQueueConsumer("my-stream", config)
    if err != nil {
        return err
    }

    // Start consuming (blocking)
    return consumer.Start(func(ctx context.Context, msg jetstream.Msg) error {
        // Process message
        fmt.Printf("Processing: %s\n", string(msg.Data()))
        return nil
    })
}

func (w *Worker) StartAsyncConsumer() error {
    // ... create consumer as above ...

    // Start consuming (non-blocking)
    _, err := consumer.StartAsync(func(ctx context.Context, msg jetstream.Msg) error {
        // Process message
        return nil
    })

    return err
}

func (w *Worker) StartConsumerWithRestart() error {
    config := nats_connector.NewWorkQueueConsumerConfig()
    config.ConsumerName = "my-worker"
    config.Subjects = []string{"tasks.>"}
    config.MaxConcurrent = 10
    config.MaxRestarts = -1               // -1 = unlimited, 0 = no restart, N = max N restarts
    config.RestartBaseDelay = time.Second  // base delay for exponential backoff
    config.RestartMaxDelay = 30 * time.Second
    config.OnError = func(err error) {
        log.Printf("Error: %v", err)
    }

    consumer, err := w.params.NATS.NewWorkQueueConsumer("my-stream", config)
    if err != nil {
        return err
    }

    // Start consuming with automatic restart (blocking)
    // Panics in handlers are recovered and treated as errors.
    // If the consumer crashes, it restarts with exponential backoff.
    return consumer.StartWithRestart(func(ctx context.Context, msg jetstream.Msg) error {
        // Process message
        return nil
    })
}
```

### Batch Message Processing

When a handler can process many messages more efficiently together — bulk DB inserts, bulk HTTP forwarding, batched analytics — use the `StartBatch*` family. The consumer pulls up to `BatchSize` messages per iteration (waiting up to `BatchMaxWait` for the batch to fill) and invokes the handler once per batch.

```go
func (w *Worker) StartBatchConsumer() error {
    config := nats_connector.NewWorkQueueConsumerConfig()
    config.ConsumerName = "my-bulk-worker"
    config.Subjects = []string{"events.>"}
    config.MaxConcurrent = 4                // up to 4 batches in flight at once
    config.BatchSize = 200                  // up to 200 messages per batch
    config.BatchMaxWait = 500 * time.Millisecond
    config.AckWait = 60 * time.Second       // give the bulk handler time to finish
    config.MaxAckPending = 1000             // must be ≥ MaxConcurrent * BatchSize
    config.OnError = func(err error) { log.Printf("Error: %v", err) }

    consumer, err := w.params.NATS.NewWorkQueueConsumer("events-stream", config)
    if err != nil {
        return err
    }

    // Start batch consumer (blocking).
    return consumer.StartBatch(func(ctx context.Context, msgs []jetstream.Msg) error {
        rows := make([]Row, 0, len(msgs))
        for _, m := range msgs {
            rows = append(rows, parse(m.Data()))
        }
        // One round-trip for the whole batch.
        if err := w.db.BulkInsert(ctx, rows); err != nil {
            return err  // entire batch is nacked with backoff and redelivered
        }
        return nil  // entire batch is acked
    })
}
```

**Semantics:**

- **All-or-nothing acknowledgement** — handler returns `nil` → every message in the batch is acked; handler returns an `error` (or panics) → every message is `NakWithDelay`'d using the same exponential backoff as the single-message path.
- **Do not call `msg.Ack()` / `msg.Nak()` / `msg.InProgress()` inside the batch handler.** The consumer manages acknowledgement collectively. If you need per-message control, use the single-message `Start()` API instead — its `MaxConcurrent` already gives you parallelism.
- **WIP / ack deadline extension** — while the batch handler is running, the consumer periodically (every `AckWait/3`, min 1s) sends `InProgress()` for every message in the batch, so long-running handlers do not get redelivered. Note that for very large batches (e.g. `BatchSize=1000`) this is `BatchSize` publishes per tick — tune `AckWait` and `BatchSize` together if this becomes noticeable.
- **Shutdown** — `Shutdown()` cancels the consumer context. Any in-flight batches are `Nak()`'d immediately for redelivery. The graceful drain may wait up to `BatchMaxWait` for an in-progress `Fetch` to return.
- **Backoff calculation** — when a batch is nacked, the delay is computed from the first message's `NumDelivered`; all messages in the batch get the same delay.
- **`MaxConcurrent` × `BatchSize` ≤ `MaxAckPending`** — the total in-flight cap is `MaxConcurrent * BatchSize`. Keep `MaxAckPending` at least this large or the consumer will throttle on the ack-pending limit. The connector logs a warning at start-up if it detects this misconfiguration.
- **`StartBatchAsync(handler)`** — non-blocking variant that runs the loop in a background goroutine; use `Done()` / `Shutdown()` for lifecycle.
- **`StartBatchWithRestart(handler)`** — same restart wrapper as `StartWithRestart`; mainly catches handler panics, since the inner batch loop already retries on transient `Fetch` errors with `BatchMaxWait` backoff.

### Batch Publishing

The connector exposes two distinct entry points for publishing many
messages at once. Pick based on whether you need atomic semantics:

| API | Stream scope | Guarantee | Use when |
|-----|--------------|-----------|----------|
| `BatchPublish` | Items may target subjects across **multiple streams** | None — async fan-out, per-item ack | Bulk publishing for throughput; no transactional requirement |
| `AtomicPublish` | All items must target a **single stream** | All-or-nothing when stream supports atomic batch; transparent fallback to async otherwise | You want one-shot commit semantics where possible |

#### BatchPublish (cross-stream, async)

```go
items := []nats_connector.BatchPublishItem{
    {Subject: "events.created", Data: payload1},
    {Subject: "audit.created",  Data: payload2}, // different stream is fine
    {Subject: "events.created", Data: payload3},
}

res, err := nc.BatchPublish(ctx, items)
if err != nil {
    return err
}
for i, ack := range res.Acks {
    log.Printf("item %d → stream=%s seq=%d", i, ack.Stream, ack.Sequence)
}
```

`BatchPublish` calls `PublishMsgAsync` for every item and waits for all
acks. There is no atomic semantic: each item is independently confirmed.
If any item's ack returns an error the call returns the first error and
abandons the rest.

**Options:**

- `WithBatchTimeout(d)` — overall wait cap; default 30s. Only consulted
  when the caller's context has no deadline.

#### AtomicPublish (single stream, atomic-or-fallback)

```go
res, err := nc.AtomicPublish(ctx, items)
if err != nil {
    return err
}
log.Printf("published %d msgs via %s (stream=%s, seq=%d..%d, batch=%s)",
    res.Count, res.Mode, res.Stream, res.FirstSeq, res.LastSeq, res.BatchID)
```

`AtomicPublish` resolves the target stream from the first item's subject
(or `WithAtomicStream`) and uses JetStream Atomic Batch Publish when the
stream advertises `AllowAtomicPublish` (NATS 2.12+). When the stream
does not support atomic batches, it transparently falls back to async
fan-out and returns `Mode == AtomicPublishModeAsyncFallback`. Use
`WithStrictAtomic` to disable the fallback.

**Options:**

- `WithAtomicStream(name)` — explicit stream name; skips the
  `StreamNameBySubject` lookup. Required when subjects are bound to more
  than one stream.
- `WithAtomicTimeout(d)` — overrides the commit/wait timeout (default
  30s). Only consulted when the caller's context has no deadline.
- `WithAtomicID(id)` — supplies an explicit `Nats-Batch-Id` (otherwise
  auto-generated).
- `WithStrictAtomic()` — disables the async fallback. Returns an error
  if the target stream does not support atomic batch publish.

**Semantics:**

- **Atomic path** — `len(items)-1` non-commit messages are published via
  `conn.PublishMsg`, then the last item is sent via `RequestMsg` with
  `Nats-Batch-Commit: 1`. The aggregate ack carries the batch id, the
  count, and the last assigned stream sequence; `FirstSeq` is derived
  as `LastSeq - Count + 1`. `Acks` is empty (the server returns one
  aggregate ack, not per-item acks).
- **Fallback path** — same as `BatchPublish` for the same items.
  `BatchID` is empty and `Acks` is populated in input order.
- **Header preservation** — `BatchPublishItem.Header` is cloned per
  message; in atomic mode the connector injects the batch headers on
  top of the caller's headers.
- **Partial-failure recovery** — if an intermediate publish errors or
  the caller cancels `ctx` before commit, the partial batch staged on
  the server is discarded automatically when its atomic-batch timeout
  elapses; you can safely retry with a fresh call (the connector
  generates a new `Nats-Batch-Id` on each retry unless you pin one
  with `WithAtomicID`).
- **Commit ambiguity** — if the commit `RequestMsg` times out or the
  connection drops after the commit reaches the server, the server may
  still apply the batch while the caller observes an error. The
  outcome is indeterminable without correlating via your own
  application-level dedup key.

#### Enabling atomic batch on a stream

```go
js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
    Name:               "EVENTS",
    Subjects:           []string{"events.>"},
    AllowAtomicPublish: true, // required for AtomicPublish to use the atomic path
})
```

Atomic batch publish is not propagated through mirrors or source
streams: the server strips `Nats-Batch-*` headers when replicating, so
downstream readers cannot reconstruct the batch boundary. If your
consumers need that, carry your own correlation header (e.g.
`X-Tx-Id`).

### Multi-Instance-Safe Resource Provisioning

When several replicas boot at the same time and each tries to create the same JetStream stream / KV bucket / durable consumer, naive `CreateStream` races can produce `"no responders"` errors, leadership-transferred errors, or "already in use" errors. The connector exposes a family of **convergent ensure** helpers that wrap JetStream's `CreateOrUpdate*` API with classification of transient vs terminal errors, bounded exponential backoff, and single-node replica fallback. They rely on JetStream meta-leader to serialize `CreateOrUpdate*` by name across the cluster — no distributed lock required.

Use these whenever you need to declaratively provision a stream / bucket / consumer at start-up and don't want each replica to fight over it.

```go
// Method form: most common — uses the connector's JetStream handle + logger.
stream, err := nc.EnsureStream(ctx, jetstream.StreamConfig{
    Name:     "EVENTS",
    Subjects: []string{"events.>"},
    Replicas: 3, // auto-falls back to 1 on single-node servers
})
if err != nil { return err }

kv, err := nc.EnsureKV(ctx, jetstream.KeyValueConfig{
    Bucket:   "user-sessions",
    History:  5,
    Replicas: 3,
})
if err != nil { return err }

consumer, err := nc.EnsureConsumer(ctx, stream, jetstream.ConsumerConfig{
    Durable:       "worker-A",
    FilterSubject: "events.>",
    AckPolicy:     jetstream.AckExplicitPolicy,
})
if err != nil { return err }

// Opportunistic scale-up: best-effort, never returns an error.
// Useful when an older deployment created the stream as replicas=1 and
// a new cluster topology now supports replicas=3.
nc.EnsureReplicaScale(ctx, "EVENTS", 3)
```

```go
// Stand-alone form: caller supplies the JetStream handle. Useful when
// you need to provision against a different JetStream account, or in
// code paths that already have a jetstream.JetStream value.
js := nc.GetJetStream()
stream, err := nats_connector.EnsureStream(ctx, js, cfg,
    nats_connector.WithEnsureLogger(logger),
    nats_connector.WithEnsureBackoff(200*time.Millisecond, 2*time.Second),
)
```

**Semantics:**

- **Idempotency via `CreateOrUpdate`** — every helper calls `js.CreateOrUpdateKeyValue` / `js.CreateOrUpdateStream` / `stream.CreateOrUpdateConsumer`. The JetStream meta-leader serializes these by resource name, so concurrent callers across all replicas converge on the same final config without races.
- **Transient error retry** — `"no responders"`, leadership-transferred, "stream in use", and timeout errors are classified as transient and retried with exponential backoff (default 200ms → 2s, capped per attempt). Non-transient errors (e.g. subjects overlap, conflicting config) are surfaced immediately.
- **Replica fallback** — when the server replies `err_code 10074` (insufficient peers), the helper retries once with `Replicas=1`, logs a warning, and returns the single-node resource. This keeps single-node dev environments working with the same `Replicas=3` config used in production. Disable via `WithoutReplicaFallback()` to get a hard error instead.
- **Stuck-resource recovery (escape hatch)** — `EnsureStream` / `EnsureKV` use a lookup-first fast path: if the resource already exists with an elected leader and current replicas, the call returns without touching the server-side definition. When the resource exists but is not publishable (no leader / replicas offline-or-not-current), the loop sleeps and re-checks readiness, trusting per-stream Raft to settle on its own. That is correct for transient cold-start and leader-flap, but is a blind spot when a prior process / peer crash left the resource in a state Raft cannot recover from without external nudging — the loop will then spin until `ctx` expires, and every subsequent process restart re-observes the same stuck state. With `WithStuckResourceRecovery(after)`, once a continuous stuck-existing observation has persisted for `after` duration the helper re-issues `CreateOrUpdateStream` / `CreateOrUpdateKeyValue` with the requested config (idempotent for matching cfg, serialized by the meta-leader). The timer resets after each attempt so the server is not spammed. The package-level helpers default to `0` (disabled, opt-in); the method-form helpers `(*NATSConnector).EnsureStream` / `EnsureKV` default to `DefaultStuckResourceRecoveryThreshold` (30s).
- **Subjects-drift reconciliation (`EnsureStream` only)** — the lookup-first fast path compares an existing stream's bound `Subjects` against `cfg.Subjects` as a set (order-independent). On mismatch the helper re-issues `CreateOrUpdateStream` to bring the server-side subjects in line with the request, then loops back to verify readiness. This closes the failure mode where an upgrade changes a stream's subject filter but the lookup-first path silently returns the stream with its stale subjects, leaving subsequent publishes to surface `ErrNoStreamResponse` because no stream owns the new subject on the server. Reconciliation is always-on (no option flag): the cost is one extra `StreamInfo` round-trip per ensure call and a single `CreateOrUpdate` when drift is detected. **The reconcile `CreateOrUpdate` overlays only `Subjects` on top of the existing server-side config (preserving `Replicas`, `MaxBytes`, `MaxAge`, etc.)** — this avoids a single-node deployment whose existing R=1 stream drifts under a cfg requesting R=3 from death-looping on insufficient peers (the fast path skips the create-side replica fallback machinery). Replica promotion is still attempted on the next iteration via `EnsureReplicaScale` once the cluster can satisfy it. Empty `cfg.Subjects` (mirror / source-only streams whose ingress is defined by `Mirror` / `Sources`) is treated as "no drift" and never triggers reconciliation. Other config fields (`MaxBytes`, `MaxAge`, `Retention`, `Storage`) are NOT reconciled by the fast path; callers needing those should issue an explicit `js.UpdateStream`.
- **Per-attempt RPC timeout** — every individual JetStream RPC inside the Ensure* loop (`Stream` / `KeyValue` lookup, `CreateOrUpdate*`, `Stream.Info`, `UpdateStream`) is wrapped with a sub-context bounded by `DefaultPerAttemptTimeout` (15s). When the meta-leader drops a reply during a multi-pod cold-start CreateOrUpdate race for the same stream name — a known NATS server fragility where N concurrent CreateOrUpdate requests for the same name can leave all callers hanging — the wrapped ctx surfaces `context.DeadlineExceeded`, the transient classifier picks it up, and the loop retries with a fresh RPC instead of blocking on the lost reply for the full caller-ctx budget. Without this bound, a single stalled RPC parks the goroutine inside the JetStream client's synchronous request and the loop's retry / backoff path never runs. Override via `WithPerAttemptTimeout(d)`; `0` disables per-attempt timeouts (restores pre-fix behaviour where the caller's ctx is the only deadline).
- **`EnsureReplicaScale`** — best-effort. Reads current stream info, returns early if `desired ≤ 1` or current replicas already meet `desired`. Calls `UpdateStream` to promote replicas; logs and swallows any error. Use this when a topology change should make an existing stream more durable.
- **Logger** — pass a `*zap.Logger` via `WithEnsureLogger(l)`. The method form auto-supplies the connector's logger. Nil logger is safe (logs are skipped).
- **No state at this layer** — these helpers do not touch viper or any shared state. Backoff and replica behaviour are controlled per call via options.

**When to use `Ensure*` vs `Once`:**

| Need | Use |
|------|-----|
| Provision the same stream / KV / consumer from every replica | `Ensure*` |
| Run an arbitrary `func(ctx) error` body exactly once across all replicas (e.g. schema migration, seed data) | `Once` |
| Acquire mutual exclusion around a critical section | `NewLock` |

For new code that only needs to declare JetStream resources, prefer `Ensure*` — it requires no distributed lock, no bucket bootstrapping for the lock itself, and tolerates replicas racing to start up.

### Distributed Lock & Once

Across multiple instances of the same service, the connector exposes a NATS JetStream-backed distributed lock and a `Once` helper for cross-instance "init exactly once" patterns. Typical use cases: schema migrations, seed data, registering one-time external resources.

> For declaratively provisioning streams, KV buckets, or durable consumers, prefer the `Ensure*` helpers above — they're cheaper (no lock bucket bootstrap), simpler (one call per resource), and rely on JetStream's built-in name-level serialization instead of a CAS lock.

```go
// Low-level mutex: blocks until acquired or ctx is cancelled.
lock, err := nc.NewLock(nats_connector.LockConfig{
    Key: "user-module.migration",
    TTL: 30 * time.Second, // lease, auto-released if the holder crashes
})
if err != nil { return err }

if err := lock.Lock(ctx); err != nil { return err }
defer lock.Unlock(ctx)

// ... critical section ...
```

```go
// High-level "init once across all instances": the first caller runs fn,
// subsequent callers (in this run and future runs) skip it and return nil.
err := nc.Once(ctx, "user-module.bootstrap", func(ctx context.Context) error {
    return migrateSchema(ctx)
})
```

**Semantics:**

- The lock is implemented on a shared JetStream KV bucket (default `{scope}_locks`). The bucket is created lazily on first use. The lock key is `lock.<your-key>`, the `Once` done sentinel is `done.<your-key>`.
- **Replication & single-mode** — the bucket defaults to `replicas=3` for HA on clustered JetStream. If the connected server is *not* part of a cluster (`ConnectedClusterName()` is empty), the connector silently caps the replica count to `1`. If the cluster is smaller than the requested replica count, the bucket creation is retried with `replicas=1` as a safety net.
- **Atomic acquire** — `Create` on the KV bucket is atomic; only one caller wins, the rest see `ErrKeyExists` and either return `(false, nil)` from `TryLock` or block in `Lock` until the holder releases.
- **Lease + heartbeat** — while held, the lock holder renews its lease (default every `TTL/3`). If the holder crashes, the per-key TTL on the bucket expires the key and another caller can take over. NATS server requires `TTL ≥ 1s`.
- **CAS release** — `Unlock` uses `Delete` with `LastRevision`, so a stale holder cannot delete a lock that was already taken over by someone else.
- **`Done()` channel** — closes when the lock is released, either by `Unlock` or because the heartbeat detected a loss (e.g. external purge, network split that ate the lease). Useful to abort the critical section.
- **`Once` retries on failure** — if `fn` returns an error, the done sentinel is NOT written, so the next caller will run `fn` again. The done sentinel is permanent on success, making future runs near-zero-cost (single KV `Get`).
- Lock blocking uses a KV `Watch`, so callers wake promptly when the lock is released (no busy polling).

**`LockConfig`:**

```go
type LockConfig struct {
    Key               string        // required: lock identity (mutually exclusive across instances)
    Bucket            string        // optional: override default bucket name
    TTL               time.Duration // lease duration (default: lock.defaultTTL; min: 1s)
    HeartbeatInterval time.Duration // renewal interval (default: TTL/3)
    OwnerID           string        // optional: identity stamped on the lock key (default: "<hostname>-<random>")
    OnLost            func(error)   // optional: callback when heartbeat detects loss
}
```

## Configuration

Configuration is managed via Viper. All config keys are prefixed with the module's scope:

| Key | Default | Description |
|-----|---------|-------------|
| `{scope}.host` | `0.0.0.0:32803` | NATS server address |
| `{scope}.pingInterval` | `10` | Ping interval in seconds |
| `{scope}.maxPingsOutstanding` | `3` | Max outstanding pings before disconnect |
| `{scope}.maxReconnects` | `-1` | Max reconnection attempts (-1 = unlimited) |
| `{scope}.auth.creds` | `""` | Path to credentials file |
| `{scope}.auth.nkey` | `""` | Path to NKey seed file |
| `{scope}.tls.cert` | `""` | Path to TLS certificate |
| `{scope}.tls.key` | `""` | Path to TLS key |
| `{scope}.tls.ca` | `""` | Path to TLS CA certificate |
| `{scope}.lock.bucket` | `{scope}_locks` | KV bucket name backing the distributed lock |
| `{scope}.lock.replicas` | `3` | KV bucket replica count; automatically falls back to `1` when the server isn't clustered (or the cluster is too small to honour the requested count). |
| `{scope}.lock.defaultTTL` | `30s` | Default lock lease when `LockConfig.TTL` is zero |

### TOML Configuration Example

```toml
[nats]
host = "nats://localhost:4222"
pingInterval = 10
maxPingsOutstanding = 3
maxReconnects = -1

[nats.auth]
creds = "/path/to/user.creds"
# or
nkey = "/path/to/user.nkey"

[nats.tls]
cert = "/path/to/client.crt"
key = "/path/to/client.key"
ca = "/path/to/ca.crt"
```

### Environment Variables Example

```bash
export NATS_HOST=nats://localhost:4222
export NATS_PINGINTERVAL=10
export NATS_MAXPINGSOUTSTANDING=3
export NATS_MAXRECONNECTS=-1
export NATS_AUTH_CREDS=/path/to/user.creds
```

## API Reference

### NATSConnector

#### `Module(scope string) fx.Option`

Creates a NATS Connector module and returns an Fx Option.

- `scope`: Module namespace, used for config key prefix and logger naming

#### `GetConnection() *nats.Conn`

Returns the NATS connection instance.

#### `GetJetStreamContext() nats.JetStreamContext`

Returns the legacy JetStream context for stream operations. Kept for backward compatibility; prefer `GetJetStream()` for new code.

#### `GetJetStream() jetstream.JetStream`

Returns the modern `jetstream.JetStream` handle (new nats.go API). Use this when working with `jetstream.KeyValueConfig` / `jetstream.StreamConfig` / `jetstream.ConsumerConfig`, or when calling the package-level `Ensure*` functions directly.

#### `EnsureKV(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error)`

Provisions a JetStream KV bucket safely across multiple instances. Method form of the package-level `EnsureKV`; supplies the connector's JetStream handle and logger. See **Multi-Instance-Safe Resource Provisioning** above.

#### `EnsureStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error)`

Provisions a JetStream stream safely across multiple instances. Method form of the package-level `EnsureStream`.

#### `EnsureConsumer(ctx context.Context, stream jetstream.Stream, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error)`

Provisions a durable consumer on the given stream safely across multiple instances. Method form of the package-level `EnsureConsumer`.

#### `EnsureReplicaScale(ctx context.Context, streamName string, desired int)`

Best-effort opportunistic promotion of an existing stream toward `desired` replicas. Never returns an error; logs and swallows failures. No-op when `desired ≤ 1` or current replicas already meet `desired`.

#### `NewWorkQueueConsumer(streamName string, cfg WorkQueueConfig) (*WorkQueueConsumer, error)`

Creates a new work queue consumer for the specified stream.

#### `BatchPublish(ctx context.Context, items []BatchPublishItem, opts ...BatchPublishOption) (*BatchPublishResult, error)`

Publishes a slice of messages via async fan-out. Items may target
subjects bound to different streams. See **Batch Publishing** above.

#### `AtomicPublish(ctx context.Context, items []BatchPublishItem, opts ...AtomicPublishOption) (*AtomicPublishResult, error)`

Publishes a slice of messages targeting a single stream, preferring
JetStream Atomic Batch Publish and falling back to async fan-out when
the stream does not support it. See **Batch Publishing** above.

#### `NewLock(cfg LockConfig) (*Lock, error)`

Creates a distributed lock backed by JetStream KV. See **Distributed Lock & Once** above.

#### `Once(ctx context.Context, key string, fn func(context.Context) error) error`

Runs `fn` exactly once across all instances. See **Distributed Lock & Once** above.

### Package-level Ensure Functions

For callers that already have a `jetstream.JetStream` handle (e.g. wiring a different account, embedding in another module), the convergent helpers are also exposed as package-level functions. Each has the same name as its `*NATSConnector` method form; the package-level form takes an explicit `jetstream.JetStream` handle and `...EnsureOption`.

#### `EnsureKV(ctx context.Context, js jetstream.JetStream, cfg jetstream.KeyValueConfig, opts ...EnsureOption) (jetstream.KeyValue, error)`

Stand-alone form of `(*NATSConnector).EnsureKV`.

#### `EnsureStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig, opts ...EnsureOption) (jetstream.Stream, error)`

Stand-alone form of `(*NATSConnector).EnsureStream`.

#### `EnsureConsumer(ctx context.Context, stream jetstream.Stream, cfg jetstream.ConsumerConfig, opts ...EnsureOption) (jetstream.Consumer, error)`

Stand-alone form of `(*NATSConnector).EnsureConsumer`.

#### `EnsureReplicaScale(ctx context.Context, js jetstream.JetStream, streamName string, desired int, opts ...EnsureOption)`

Stand-alone form of `(*NATSConnector).EnsureReplicaScale`. Best-effort; never returns an error.

#### `WithEnsureLogger(l *zap.Logger) EnsureOption`

Installs a zap logger that receives structured-log lines on non-trivial transitions (insufficient-peers fallback, replica scale-up result, final scale-up failure). `nil` is allowed and silences the logger.

#### `WithEnsureBackoff(base, max time.Duration) EnsureOption`

Overrides the retry backoff window. `base` is the initial sleep between retries; `max` caps the doubled value. Defaults are 200ms and 2s.

#### `WithoutReplicaFallback() EnsureOption`

Disables the silent demotion to `Replicas=1` when the cluster reports insufficient peers; surfaces the placement error instead.

#### `WithInsufficientPeersBudget(d time.Duration) EnsureOption`

Overrides how long `EnsureStream` / `EnsureKV` treat "insufficient peers" as a cluster-bootstrap transient (retrying at the requested replica count) before falling back to a single replica. `0` falls back immediately on the first occurrence. Default `DefaultInsufficientPeersBudget` (30s). No effect when combined with `WithoutReplicaFallback`.

#### `WithStuckResourceRecovery(after time.Duration) EnsureOption`

Enables the lookup-first **stuck-resource escape hatch**. After the helper has continuously observed an existing stream / KV bucket as not-publishable (no leader elected, or replicas offline / not current) for `after` duration, it re-issues `CreateOrUpdateStream` / `CreateOrUpdateKeyValue` with the requested config to nudge the server. CreateOrUpdate is idempotent for matching cfg and serialized by the meta-leader, so the call is safe to repeat under load. The recovery timer resets after each attempt so the server is not spammed. `0` disables the escape hatch (package-level default). Recommended value: `DefaultStuckResourceRecoveryThreshold` (30s). The method-form helpers `(*NATSConnector).EnsureStream` / `EnsureKV` enable this by default.

#### `WithPerAttemptTimeout(d time.Duration) EnsureOption`

Bounds each individual JetStream RPC made from inside the Ensure* loop (`Stream` / `KeyValue` lookup, `CreateOrUpdate*`, `Stream.Info`, `UpdateStream`). When the underlying request stalls — most commonly the meta-leader **dropping a reply during a multi-pod cold-start CreateOrUpdate race for the same stream name** — the wrapped sub-context surfaces `context.DeadlineExceeded`, the transient classifier picks it up, and the loop retries on the next backoff tick. Without this bound, a single stalled RPC blocks the convergent loop for the full caller-ctx budget (typically the 5-minute OnStart provisioning window) because the goroutine is parked inside the JetStream client's synchronous request and never observes the loop's retry path. `0` disables per-attempt timeouts (caller ctx is the only deadline; restores pre-fix behaviour). Default `DefaultPerAttemptTimeout` (15s).

### WorkQueueConsumer

#### `Start(handler MessageHandler) error`

Starts consuming messages synchronously (blocking).

#### `StartAsync(handler MessageHandler) (jetstream.ConsumeContext, error)`

Starts consuming messages asynchronously (non-blocking).

#### `StartWithRestart(handler MessageHandler) error`

Starts consuming with automatic restart on failure (blocking). Panics in handlers are recovered and converted to errors. If the consumer crashes, it restarts with exponential backoff. Respects `MaxRestarts`, `RestartBaseDelay`, and `RestartMaxDelay` from config.

#### `StartBatch(handler BatchMessageHandler) error`

Starts consuming messages in batches synchronously (blocking). The handler receives up to `BatchSize` messages at a time. Returns when `Shutdown()` is called. See the **Batch Message Processing** section above for full semantics.

#### `StartBatchAsync(handler BatchMessageHandler) error`

Starts the batch consumer in a background goroutine and returns immediately. Use `Done()` / `Shutdown()` for lifecycle.

#### `StartBatchWithRestart(handler BatchMessageHandler) error`

Batch equivalent of `StartWithRestart`. Mainly recovers handler panics; the inner batch loop already handles transient `Fetch` errors internally.

#### `Shutdown()`

Gracefully shuts down the consumer. Also interrupts any pending restart backoff wait.

#### `Done() <-chan struct{}`

Returns a channel that is closed when the consumer context is cancelled.

### WorkQueueConfig

```go
type WorkQueueConfig struct {
    Conn          *nats.Conn       // NATS connection (optional, uses connector's connection if nil)
    Stream        *nats.StreamInfo // Stream info (optional, fetched by stream name if nil)
    ConsumerName  string           // Consumer name
    Subjects      []string         // Subjects to consume
    MaxConcurrent int              // Max concurrent message processing (default: 10)
    AckWait       time.Duration    // Ack wait time (default: 30s)
    MaxRetries    int              // Max retries (-1 = unlimited)
    MaxAckPending    int              // Max pending acks (default: MaxConcurrent)
    OnError          ErrorHandler     // Error handler callback
    MaxRestarts      int              // Max restart attempts; -1 unlimited, 0 no restart (default: -1)
    RestartBaseDelay time.Duration    // Restart backoff base delay (default: 1s)
    RestartMaxDelay  time.Duration    // Restart backoff max delay (default: 30s)

    BatchSize    int           // Max messages per batch (default: 100). Used only by StartBatch* APIs.
    BatchMaxWait time.Duration // Max time Fetch waits to fill a batch (default: 1s). Used only by StartBatch* APIs.
}
```

### MessageHandler / BatchMessageHandler

```go
type MessageHandler      func(ctx context.Context, msg jetstream.Msg) error
type BatchMessageHandler func(ctx context.Context, msgs []jetstream.Msg) error
```

`MessageHandler` is for the single-message `Start*` family; the consumer acks on `nil`, nacks-with-backoff on `error`. `BatchMessageHandler` is for the `StartBatch*` family; the consumer acks the entire batch on `nil`, nacks the entire batch with backoff on `error`. Batch handlers MUST NOT call `msg.Ack()` / `msg.Nak()` / `msg.InProgress()` themselves.

## License

Apache License 2.0
