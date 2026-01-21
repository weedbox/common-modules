# NATS Connector Module

A NATS messaging connector module built on [NATS Go Client](https://github.com/nats-io/nats.go), integrated with [Uber Fx](https://github.com/uber-go/fx) for dependency injection. Includes JetStream support and work queue consumer functionality.

## Features

- Uber Fx dependency injection integration
- NATS core messaging support
- JetStream support for persistent messaging
- Work queue consumer with concurrency control
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

### WorkQueueConsumer

#### `Start(handler MessageHandler) error`

Starts consuming messages synchronously (blocking).

#### `StartAsync(handler MessageHandler) (jetstream.ConsumeContext, error)`

Starts consuming messages asynchronously (non-blocking).

#### `Shutdown()`

Gracefully shuts down the consumer.

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
    MaxAckPending int              // Max pending acks (default: MaxConcurrent)
    OnError       ErrorHandler     // Error handler callback
}
```

## License

Apache License 2.0
