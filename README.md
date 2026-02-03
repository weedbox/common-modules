# Common Modules

A collection of reusable Go modules for building microservices, integrated with [Uber Fx](https://github.com/uber-go/fx) dependency injection framework.

## Installation

```bash
go get github.com/weedbox/common-modules
```

## Modules

### Core

| Module | Description |
|--------|-------------|
| [configs](./configs) | Configuration management with Viper (TOML, environment variables) |
| [logger](./logger) | Zap-based logging with debug mode support |
| [daemon](./daemon) | Service lifecycle management with ready/health status |

### HTTP

| Module | Description |
|--------|-------------|
| [http_server](./http_server) | Gin-based HTTP server with CORS support |
| [healthcheck_apis](./healthcheck_apis) | Kubernetes-compatible health check endpoints (`/healthz`, `/ready`) |
| [swagger](./swagger) | Swagger/OpenAPI documentation with Scalar UI |

### Database

| Module | Description |
|--------|-------------|
| [database](./database) | Database abstraction interface (`DatabaseConnector`) |
| [postgres_connector](./postgres_connector) | PostgreSQL connector with GORM |
| [sqlite_connector](./sqlite_connector) | SQLite connector with GORM |

### Messaging

| Module | Description |
|--------|-------------|
| [nats_connector](./nats_connector) | NATS client with JetStream and work queue consumer |
| [nats_jetstream_server](./nats_jetstream_server) | Embedded NATS JetStream server |

### Cache

| Module | Description |
|--------|-------------|
| [redis_connector](./redis_connector) | Redis client connector |

### Utilities

| Module | Description |
|--------|-------------|
| [mailer](./mailer) | SMTP email sending with Gomail |

## Quick Start

```go
package main

import (
    "github.com/weedbox/common-modules/configs"
    "github.com/weedbox/common-modules/daemon"
    "github.com/weedbox/common-modules/healthcheck_apis"
    "github.com/weedbox/common-modules/http_server"
    "github.com/weedbox/common-modules/logger"
    "github.com/weedbox/common-modules/postgres_connector"
    "go.uber.org/fx"
)

func main() {
    config := configs.NewConfig("MYAPP")

    fx.New(
        fx.Supply(config),

        // Core
        logger.Module(),

        // Infrastructure
        postgres_connector.Module("database"),
        http_server.Module("http_server"),
        healthcheck_apis.Module("healthcheck_apis"),

        // Application modules
        // your_module.Module("your_module"),

        // Daemon (must be last)
        daemon.Module("daemon"),
    ).Run()
}
```

## Configuration

All modules support configuration via:

1. **TOML file** - `config.toml` in `./` or `./configs/`
2. **Environment variables** - Prefixed with your app name

Example `config.toml`:

```toml
[http_server]
host = "0.0.0.0"
port = 8080

[database]
host = "localhost"
port = 5432
dbname = "myapp"
user = "postgres"
password = "secret"
```

## License

Apache License 2.0
