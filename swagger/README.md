# Swagger Module

Swagger module provides API documentation endpoints, including Swagger JSON spec and Scalar API Reference UI.

## Features

- **Swagger JSON Spec**: Serves OpenAPI/Swagger specification using `gin-swagger`
- **Scalar API Reference UI**: Provides a beautiful API documentation browsing interface
- **Configurable Toggle**: Can be enabled or disabled via configuration

## Dependencies

This module depends on the following packages:

```go
import (
    swaggerFiles "github.com/swaggo/files"
    ginSwagger "github.com/swaggo/gin-swagger"
)
```

Make sure your project has generated Swagger documentation. You can use the [swag](https://github.com/swaggo/swag) tool:

```bash
go install github.com/swaggo/swag/cmd/swag@latest
swag init
```

## Usage

```go
package main

import (
    "github.com/weedbox/common-modules/configs"
    "github.com/weedbox/common-modules/http_server"
    "github.com/weedbox/common-modules/logger"
    "github.com/weedbox/common-modules/swagger"
    "github.com/weedbox/common-modules/daemon"
    "go.uber.org/fx"

    _ "your-project/docs" // Import generated swagger docs
)

func main() {
    config := configs.NewConfig("MYAPP")

    fx.New(
        fx.Supply(config),
        logger.Module(),
        http_server.Module("http_server"),
        swagger.Module("swagger"),
        daemon.Module("daemon"),
    ).Run()
}
```

## Configuration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `{scope}.enabled` | bool | `true` | Enable or disable Swagger |
| `{scope}.base_path` | string | `/swagger` | Base path for endpoints |
| `{scope}.title` | string | `API Reference` | UI page title |
| `{scope}.docs_path` | string | `/docs` | Swagger JSON spec path |
| `{scope}.ui_path` | string | `/ui` | Scalar UI path |
| `{scope}.spec_path` | string | `/doc.json` | JSON spec file path |

### Configuration File Example (config.toml)

```toml
[swagger]
enabled = true
base_path = "/swagger"
title = "My API Reference"
docs_path = "/docs"
ui_path = "/ui"
spec_path = "/doc.json"
```

### Environment Variables Example

```bash
export MYAPP_SWAGGER_ENABLED=true
export MYAPP_SWAGGER_BASE_PATH=/api-docs
export MYAPP_SWAGGER_TITLE="My API"
```

## Endpoints

With default configuration, the following endpoints are available:

- `GET /swagger/docs/*` - Swagger JSON spec (gin-swagger)
- `GET /swagger/docs/doc.json` - OpenAPI JSON specification
- `GET /swagger/ui/*` - Scalar API Reference UI

## Disabling Swagger

In production environments, you may want to disable Swagger:

**config.toml:**
```toml
[swagger]
enabled = false
```

**Environment Variable:**
```bash
export MYAPP_SWAGGER_ENABLED=false
```
