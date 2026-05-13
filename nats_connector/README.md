# NATS Connector Module

A NATS messaging connector module built on [NATS Go Client](https://github.com/nats-io/nats.go), integrated with [Uber Fx](https://github.com/uber-go/fx) for dependency injection. Includes JetStream support and work queue consumer functionality.

## Features

- Uber Fx dependency injection integration
- NATS core messaging support
- JetStream support for persistent messaging
- Work queue consumer with concurrency control
- Batch message processing for bulk-friendly handlers
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

### Distributed Lock & Once

Across multiple instances of the same service, the connector exposes a NATS JetStream-backed distributed lock and a `Once` helper for cross-instance "init exactly once" patterns. Typical use cases: schema migrations, stream/bucket bootstrapping, seed data, registering one-time external resources.

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

Returns the JetStream context for stream operations.

#### `NewWorkQueueConsumer(streamName string, cfg WorkQueueConfig) (*WorkQueueConsumer, error)`

Creates a new work queue consumer for the specified stream.

#### `NewLock(cfg LockConfig) (*Lock, error)`

Creates a distributed lock backed by JetStream KV. See **Distributed Lock & Once** above.

#### `Once(ctx context.Context, key string, fn func(context.Context) error) error`

Runs `fn` exactly once across all instances. See **Distributed Lock & Once** above.

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
