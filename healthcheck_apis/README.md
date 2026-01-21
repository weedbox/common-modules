# Health Check APIs Module

A health check HTTP endpoints module integrated with [Uber Fx](https://github.com/uber-go/fx), providing Kubernetes-compatible health and readiness endpoints.

## Features

- Uber Fx dependency injection integration
- Kubernetes-compatible health check endpoints
- Integration with daemon module for status tracking
- `/healthz` and `/ready` endpoints

## Installation

```bash
go get github.com/weedbox/common-modules/healthcheck_apis
```

## Quick Start

### Basic Usage

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
        daemon.Module("daemon"),
        healthcheck_apis.Module("healthcheck_apis"),
    ).Run()
}
```

## Endpoints

### GET /healthz

Returns the health status of the service.

**Healthy Response (200 OK):**
```json
{
    "status": "ok"
}
```

**Unhealthy Response (500 Internal Server Error):**
```json
{
    "status": "unhealthy"
}
```

### GET /ready

Returns whether the service is ready to accept requests.

**Ready Response (200 OK):**
```json
{
    "ready": true
}
```

**Not Ready Response (500 Internal Server Error):**
```json
{
    "ready": false
}
```

## Dependencies

This module requires:
- `http_server.HTTPServer` - For registering HTTP endpoints
- `daemon.Daemon` - For checking health and ready status

## Kubernetes Integration

These endpoints are designed for Kubernetes liveness and readiness probes:

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: myapp
    livenessProbe:
      httpGet:
        path: /healthz
        port: 8080
      initialDelaySeconds: 3
      periodSeconds: 10
    readinessProbe:
      httpGet:
        path: /ready
        port: 8080
      initialDelaySeconds: 3
      periodSeconds: 5
```

## API Reference

### Module

#### `Module(scope string) fx.Option`

Creates a Health Check APIs module and returns an Fx Option.

- `scope`: Module namespace for logger naming

## License

Apache License 2.0
