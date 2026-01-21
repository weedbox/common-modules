# NATS JetStream Server Module

An embedded NATS JetStream server module built on [NATS Server](https://github.com/nats-io/nats-server), integrated with [Uber Fx](https://github.com/uber-go/fx) for dependency injection.

## Features

- Uber Fx dependency injection integration
- Embedded NATS server with JetStream
- Cluster support
- Authentication (username/password, token)
- TLS support
- HTTP monitoring endpoint
- Configurable memory and storage limits

## Installation

```bash
go get github.com/weedbox/common-modules/nats_jetstream_server
```

## Quick Start

### Basic Usage

```go
package main

import (
    "github.com/weedbox/common-modules/logger"
    "github.com/weedbox/common-modules/nats_jetstream_server"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        logger.Module(),
        nats_jetstream_server.Module("nats_server"),
    ).Run()
}
```

### With NATS Connector

```go
package main

import (
    "github.com/weedbox/common-modules/logger"
    "github.com/weedbox/common-modules/nats_connector"
    "github.com/weedbox/common-modules/nats_jetstream_server"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        logger.Module(),
        nats_jetstream_server.Module("nats_server"),
        nats_connector.Module("nats"),
    ).Run()
}
```

### Accessing Server Information

```go
package myservice

import (
    "github.com/weedbox/common-modules/nats_jetstream_server"
    "go.uber.org/fx"
)

type Params struct {
    fx.In

    NATSServer *nats_jetstream_server.NATSJetStreamServer
}

func (s *Service) GetServerInfo() {
    // Get client connection URL
    clientURL := s.params.NATSServer.GetClientURL()
    // e.g., "nats://0.0.0.0:4222"

    // Get HTTP monitoring URL
    httpURL := s.params.NATSServer.GetHTTPURL()
    // e.g., "http://0.0.0.0:8222"

    // Check if server is running
    if s.params.NATSServer.IsRunning() {
        // Server is ready
    }

    // Get connection count
    count := s.params.NATSServer.GetConnectionCount()
}
```

## Configuration

Configuration is managed via Viper. All config keys are prefixed with the module's scope:

### Server Basic Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `{scope}.host` | `0.0.0.0` | Server host |
| `{scope}.port` | `4222` | Client port |
| `{scope}.http_port` | `8222` | HTTP monitoring port |
| `{scope}.cluster_port` | `6222` | Cluster port |
| `{scope}.max_connections` | `65536` | Max client connections |
| `{scope}.max_payload` | `1048576` | Max message payload (1MB) |
| `{scope}.write_deadline` | `2s` | Write deadline |
| `{scope}.log_level` | `INFO` | Log level (DEBUG, TRACE, INFO) |

### JetStream Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `{scope}.jetstream.enabled` | `true` | Enable JetStream |
| `{scope}.jetstream.max_memory` | `1073741824` | Max memory (1GB) |
| `{scope}.jetstream.max_storage` | `10737418240` | Max storage (10GB) |
| `{scope}.jetstream.store_dir` | `./data/jetstream` | Storage directory |

### Cluster Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `{scope}.cluster.enabled` | `false` | Enable clustering |
| `{scope}.cluster.name` | `""` | Cluster name |
| `{scope}.cluster.routes` | `[]` | Cluster route URLs |

### Authentication Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `{scope}.auth.enabled` | `false` | Enable authentication |
| `{scope}.auth.username` | `""` | Username |
| `{scope}.auth.password` | `""` | Password |
| `{scope}.auth.token` | `""` | Auth token |

### TLS Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `{scope}.tls.enabled` | `false` | Enable TLS |
| `{scope}.tls.cert_file` | `""` | TLS certificate file |
| `{scope}.tls.key_file` | `""` | TLS key file |
| `{scope}.tls.ca_file` | `""` | TLS CA file |

### TOML Configuration Example

```toml
[nats_server]
host = "0.0.0.0"
port = 4222
http_port = 8222
max_connections = 65536
max_payload = 1048576
log_level = "INFO"

[nats_server.jetstream]
enabled = true
max_memory = 1073741824
max_storage = 10737418240
store_dir = "./data/jetstream"

[nats_server.cluster]
enabled = true
name = "my-cluster"
routes = ["nats://node2:6222", "nats://node3:6222"]

[nats_server.auth]
enabled = true
username = "admin"
password = "secret"

[nats_server.tls]
enabled = true
cert_file = "/path/to/server.crt"
key_file = "/path/to/server.key"
ca_file = "/path/to/ca.crt"
```

### Environment Variables Example

```bash
export NATS_SERVER_HOST=0.0.0.0
export NATS_SERVER_PORT=4222
export NATS_SERVER_HTTP_PORT=8222
export NATS_SERVER_JETSTREAM_ENABLED=true
export NATS_SERVER_JETSTREAM_STORE_DIR=./data/jetstream
export NATS_SERVER_AUTH_ENABLED=true
export NATS_SERVER_AUTH_USERNAME=admin
export NATS_SERVER_AUTH_PASSWORD=secret
```

## API Reference

### NATSJetStreamServer

#### `Module(scope string) fx.Option`

Creates a NATS JetStream Server module and returns an Fx Option.

- `scope`: Module namespace, used for config key prefix and logger naming

#### `GetServer() *server.Server`

Returns the underlying NATS server instance.

#### `GetClientURL() string`

Returns the client connection URL (e.g., `nats://0.0.0.0:4222`).

#### `GetHTTPURL() string`

Returns the HTTP monitoring URL (e.g., `http://0.0.0.0:8222`).

#### `IsRunning() bool`

Returns `true` if the server is running and ready for connections.

#### `GetConnectionCount() int`

Returns the current number of client connections.

## HTTP Monitoring

When the server is running, you can access monitoring endpoints:

- `http://localhost:8222/varz` - General server statistics
- `http://localhost:8222/connz` - Connection information
- `http://localhost:8222/routez` - Route information
- `http://localhost:8222/subsz` - Subscription information
- `http://localhost:8222/jsz` - JetStream information

## License

Apache License 2.0
