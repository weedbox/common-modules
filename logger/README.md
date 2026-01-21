# Logger Module

A logging module built on [Zap](https://github.com/uber-go/zap), integrated with [Uber Fx](https://github.com/uber-go/fx) for dependency injection.

## Features

- Uber Fx dependency injection integration
- Zap high-performance logging
- Debug mode with configurable log levels
- Colorful console output
- Custom timestamp format

## Installation

```bash
go get github.com/weedbox/common-modules/logger
```

## Quick Start

### Basic Usage

```go
package main

import (
    "github.com/weedbox/common-modules/logger"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        logger.Module(),
        // ... other modules
    ).Run()
}
```

### Using Logger in Your Module

```go
package myservice

import (
    "go.uber.org/fx"
    "go.uber.org/zap"
)

type Params struct {
    fx.In

    Logger *zap.Logger
}

type Service struct {
    logger *zap.Logger
}

func NewService(p Params) *Service {
    return &Service{
        logger: p.Logger.Named("myservice"),
    }
}

func (s *Service) DoSomething() {
    s.logger.Info("Doing something",
        zap.String("key", "value"),
        zap.Int("count", 42),
    )
}
```

## Configuration

Configuration is done via environment variables:

| Environment Variable | Values | Default | Description |
|---------------------|--------|---------|-------------|
| `DEBUG_MODE` | `debug`, `true` | - | Enable debug mode |
| `DEBUG_LEVEL` | `debug`, `info`, `warn`, `error`, `dpanic`, `panic`, `fatal` | `debug` | Log level when debug mode is enabled |

### Enable Debug Mode

```bash
export DEBUG_MODE=true
export DEBUG_LEVEL=debug
```

## Log Output Format

The logger outputs in console format with colorful level encoding:

```
2024-01-15 10:30:45  INFO    myservice   Doing something    {"key": "value", "count": 42}
```

## API Reference

### Module

#### `Module() fx.Option`

Creates a Logger module and returns an Fx Option. This module provides a `*zap.Logger` instance.

#### `SetupLogger() *zap.Logger`

Manually setup and return a logger instance. Called automatically by the module.

#### `GetLogger() *zap.Logger`

Returns the global logger instance after initialization.

## Debug Mode Behavior

| Mode | Caller Info | Log Level |
|------|-------------|-----------|
| Normal | No | Info |
| Debug | Yes | Configurable via `DEBUG_LEVEL` |

## License

Apache License 2.0
