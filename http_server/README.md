# HTTP Server Module

An HTTP server module built on [Gin](https://github.com/gin-gonic/gin) framework, integrated with [Uber Fx](https://github.com/uber-go/fx) dependency injection for clean lifecycle management.

## Features

- Uber Fx dependency injection integration
- Built-in CORS support
- Multiple running modes (test/prod)
- Static file serving (SPA support)
- Configuration management via Viper

## Installation

```bash
go get github.com/weedbox/common-modules/http_server
```

## Quick Start

### Basic Usage

```go
package main

import (
    "github.com/weedbox/common-modules/http_server"
    "go.uber.org/fx"
    "go.uber.org/zap"
)

func main() {
    fx.New(
        fx.Provide(zap.NewDevelopment),
        http_server.Module("http_server"),
    ).Run()
}
```

### Creating an API Module

The following example demonstrates how to create an API module and register routes:

```go
package apis

import (
    "context"
    "net/http"

    "github.com/gin-gonic/gin"
    "github.com/weedbox/common-modules/http_server"
    "go.uber.org/fx"
    "go.uber.org/zap"
)

type APIs struct {
    params Params
    logger *zap.Logger
    router *gin.RouterGroup
    scope  string
}

type Params struct {
    fx.In

    Lifecycle  fx.Lifecycle
    Logger     *zap.Logger
    HTTPServer *http_server.HTTPServer
}

func Module(scope string) fx.Option {

    var a *APIs

    return fx.Module(
        scope,
        fx.Provide(func(p Params) *APIs {
            a = &APIs{
                params: p,
                logger: p.Logger.Named(scope),
                scope:  scope,
            }
            return a
        }),
        fx.Populate(&a),
        fx.Invoke(func(p Params) {
            p.Lifecycle.Append(
                fx.Hook{
                    OnStart: a.onStart,
                    OnStop:  a.onStop,
                },
            )
        }),
    )
}

func (a *APIs) onStart(ctx context.Context) error {
    a.logger.Info("Starting APIs")

    // Get the router and create a route group
    a.router = a.params.HTTPServer.GetRouter().Group("apis")

    // Register routes
    a.router.GET("/v1/health", a.healthCheck)
    a.router.POST("/v1/users", a.createUser)
    a.router.GET("/v1/users/:id", a.getUser)

    return nil
}

func (a *APIs) onStop(ctx context.Context) error {
    a.logger.Info("Stopped APIs")
    return nil
}

func (a *APIs) healthCheck(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{
        "status": "ok",
    })
}

func (a *APIs) createUser(c *gin.Context) {
    // Implement create user logic
}

func (a *APIs) getUser(c *gin.Context) {
    id := c.Param("id")
    // Implement get user logic
    c.JSON(http.StatusOK, gin.H{
        "id": id,
    })
}
```

### Integrating Multiple Modules

```go
package main

import (
    "github.com/weedbox/common-modules/http_server"
    "your-project/pkg/apis"
    "go.uber.org/fx"
    "go.uber.org/zap"
)

func main() {
    fx.New(
        fx.Provide(zap.NewDevelopment),
        http_server.Module("http_server"),
        apis.Module("apis"),
    ).Run()
}
```

## Configuration

Configuration is managed via Viper. All config keys are prefixed with the module's scope:

| Key | Default | Description |
|-----|---------|-------------|
| `{scope}.host` | `0.0.0.0` | Host address to listen on |
| `{scope}.port` | `80` | Port number to listen on |
| `{scope}.mode` | `test` | Running mode (`test` / `prod`) |
| `{scope}.loglevel` | - | Log level (`test` / `release` / `prod`) |
| `{scope}.allow_origins` | `""` (allow all) | CORS allowed origins, comma-separated |
| `{scope}.allow_methods` | `""` | CORS allowed HTTP methods, comma-separated |
| `{scope}.allow_headers` | `Authorization,Accept` | CORS allowed headers, comma-separated |

### TOML Configuration Example

```toml
[http_server]
host = "0.0.0.0"
port = 8080
mode = "prod"
loglevel = "release"
allow_origins = "https://example.com,https://api.example.com"
allow_methods = "GET,POST,PUT,DELETE"
allow_headers = "Authorization,Accept,Content-Type"
```

### Environment Variables Example

```bash
export HTTP_SERVER_HOST=0.0.0.0
export HTTP_SERVER_PORT=8080
export HTTP_SERVER_MODE=prod
export HTTP_SERVER_LOGLEVEL=release
export HTTP_SERVER_ALLOW_ORIGINS=https://example.com
```

## API Reference

### HTTPServer

#### `Module(scope string) fx.Option`

Creates an HTTP Server module and returns an Fx Option for application use.

- `scope`: Module namespace, used for config key prefix and logger naming

#### `GetRouter() *gin.Engine`

Returns the Gin router engine for registering routes and middleware.

```go
router := httpServer.GetRouter()
router.GET("/ping", func(c *gin.Context) {
    c.String(200, "pong")
})
```

#### `ServeFS(routePath string, fst fs.FS)`

Serves static files from an `fs.FS` filesystem. Ideal for SPA (Single Page Application) deployments.

- `routePath`: Route prefix path
- `fst`: Filesystem implementing the `fs.FS` interface

```go
//go:embed dist/*
var staticFiles embed.FS

func (a *App) onStart(ctx context.Context) error {
    // Get the dist subdirectory
    distFS, _ := fs.Sub(staticFiles, "dist")

    // Serve static files under /app path
    a.httpServer.ServeFS("/app", distFS)

    return nil
}
```

Static file serving features:
- Automatic `index.html` handling as default page
- SPA routing support (unmatched paths fallback to `index.html`)
- Automatic MIME type detection

## Running Modes

| Mode | Gin Mode | Description |
|------|----------|-------------|
| `test` | TestMode | For development, outputs detailed logs |
| `release` | ReleaseMode | For production, reduced log output |
| `prod` | ReleaseMode | For production, uses minimal Gin instance (no default middleware) |

## License

Apache License 2.0
