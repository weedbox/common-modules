# Daemon Module

A service lifecycle management module integrated with [Uber Fx](https://github.com/uber-go/fx), providing ready state and health status tracking for your application.

## Features

- Uber Fx dependency injection integration
- Ready state tracking
- Health status management
- Automatic lifecycle management

## Installation

```bash
go get github.com/weedbox/common-modules/daemon
```

## Quick Start

### Basic Usage

**Important:** The daemon module should be placed at the end of all other modules. When `OnStart` is called, it sets `isReady` to `true`. Placing it last ensures that all other modules have completed their initialization before the service is marked as ready.

```go
package main

import (
    "github.com/weedbox/common-modules/daemon"
    "github.com/weedbox/common-modules/http_server"
    "github.com/weedbox/common-modules/logger"
    "github.com/weedbox/common-modules/postgres_connector"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        // Infrastructure modules
        logger.Module(),
        postgres_connector.Module("database"),
        http_server.Module("http_server"),

        // Application modules
        // ...

        // Daemon should be last - marks service as ready after all modules are initialized
        daemon.Module("daemon"),
    ).Run()
}
```

### Checking Service Status

```go
package myservice

import (
    "github.com/weedbox/common-modules/daemon"
    "go.uber.org/fx"
)

type Params struct {
    fx.In

    Daemon *daemon.Daemon
}

func (s *Service) CheckStatus(p Params) {
    // Check if service is ready
    if p.Daemon.Ready() {
        // Service is ready to accept requests
    }

    // Check health status
    status := p.Daemon.GetHealthStatus()
    if status == daemon.HealthStatus_Healthy {
        // Service is healthy
    }
}
```

## API Reference

### Daemon

#### `Module(scope string) fx.Option`

Creates a Daemon module and returns an Fx Option.

- `scope`: Module namespace for logger naming

#### `Ready() bool`

Returns `true` if the daemon has started and is ready to serve requests.

#### `GetHealthStatus() HealthStatus`

Returns the current health status of the daemon.

### HealthStatus

```go
const (
    HealthStatus_Healthy   HealthStatus = iota  // Service is healthy
    HealthStatus_Unhealthy                       // Service is unhealthy
)
```

## Integration with Health Check APIs

The daemon module is designed to work with the `healthcheck_apis` module:

```go
package main

import (
    "github.com/weedbox/common-modules/daemon"
    "github.com/weedbox/common-modules/healthcheck_apis"
    "github.com/weedbox/common-modules/http_server"
    "github.com/weedbox/common-modules/logger"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        logger.Module(),
        http_server.Module("http_server"),
        healthcheck_apis.Module("healthcheck_apis"),

        // Other application modules...

        // Daemon must be last
        daemon.Module("daemon"),
    ).Run()
}
```

This exposes:
- `GET /healthz` - Returns health status
- `GET /ready` - Returns ready state (only `true` after all modules are initialized)

## License

Apache License 2.0
