# Daemon Module

A service lifecycle management module integrated with [Uber Fx](https://github.com/uber-go/fx), providing ready state and health status tracking for your application.

## Features

- Uber Fx dependency injection integration
- Ready state tracking
- Composite readiness via pluggable ready checkers
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

### Composite Readiness with Ready Checkers

`Ready()` returns `true` only when the daemon has started **and** every
registered ready checker also returns `true`. Modules that have their own
runtime "am I ready?" notion (cluster membership, leader election, warmed
cache, established upstream connection, etc.) can plug themselves in so that
`/ready` reflects the true composite state of the service.

```go
package cluster

import (
    "github.com/weedbox/common-modules/daemon"
    "go.uber.org/fx"
)

type Params struct {
    fx.In

    Lifecycle fx.Lifecycle
    Daemon    *daemon.Daemon
    Cluster   *Cluster
}

func Register(p Params) {
    p.Lifecycle.Append(fx.Hook{
        OnStart: func(ctx context.Context) error {
            // Plug the cluster's own readiness into the daemon. /ready will
            // only flip to true once the cluster reports ready as well.
            p.Daemon.RegisterReadyChecker("cluster", func() bool {
                return p.Cluster.Ready()
            })
            return nil
        },
        OnStop: func(ctx context.Context) error {
            p.Daemon.UnregisterReadyChecker("cluster")
            return nil
        },
    })
}
```

Checkers are invoked on every call to `Ready()`, so they should be cheap and
non-blocking — typically a single atomic / mutex-guarded field read.

## API Reference

### Daemon

#### `Module(scope string) fx.Option`

Creates a Daemon module and returns an Fx Option.

- `scope`: Module namespace for logger naming

#### `Ready() bool`

Returns `true` if the daemon has started **and** every registered ready
checker currently returns `true`. Safe for concurrent use.

#### `RegisterReadyChecker(name string, fn func() bool)`

Registers a named readiness predicate that participates in `Ready()`.

- `name`: stable identifier for the checker. Registering the same name again
  replaces the previous checker.
- `fn`: predicate evaluated on every `Ready()` call. A `nil` fn is ignored;
  use `UnregisterReadyChecker` to remove a checker.

Checkers should be cheap and non-blocking, since they are evaluated on every
readiness probe.

#### `UnregisterReadyChecker(name string)`

Removes the ready checker registered under `name`. No-op if no checker with
that name is registered. Typically called from a module's `OnStop` hook to
mirror its `OnStart` registration.

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
- `GET /ready` - Returns ready state (only `true` after all modules are initialized **and** every registered ready checker reports ready)

## License

Apache License 2.0
