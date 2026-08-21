# Logger Module

A logging module built on [Zap](https://github.com/uber-go/zap), integrated with [Uber Fx](https://github.com/uber-go/fx) for dependency injection.

## Features

- Uber Fx dependency injection integration
- Zap high-performance logging
- Debug mode with configurable log levels
- Colorful console output
- Custom timestamp format
- Quiet-by-default Fx event logging that still surfaces startup failures

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

Creates a Logger module and returns an Fx Option. This module provides a `*zap.Logger` instance
and installs an Fx event logger (see [Fx Event Logging](#fx-event-logging)).

#### `SetupLogger() *zap.Logger`

Manually setup and return a logger instance. Called automatically by the module.

#### `GetLogger() *zap.Logger`

Returns the global logger instance after initialization.

## Debug Mode Behavior

| Mode | Caller Info | Log Level |
|------|-------------|-----------|
| Normal | No | Info |
| Debug | Yes | Configurable via `DEBUG_LEVEL` |

## Fx Event Logging

`Module()` installs an `fxevent.ZapLogger` with its **non-error** events demoted to
`Debug`. Fx emits one event per constructor and per lifecycle hook, which floods
startup in any non-trivial application — historically applications worked around
this by adding `fx.NopLogger`, which silences everything.

Silencing everything is dangerous. Fx discards the error returned by `app.Start`:

```go
// go.uber.org/fx App.run()
if err := app.Start(startCtx); err != nil {
    return 1        // err is never returned, wrapped, or printed
}
```

The `fxevent.Logger` is therefore the **only** channel through which a startup
failure is ever reported. Under `fx.NopLogger`, a failed start is an `exit 1`
with a completely empty log — the process dies without saying why.

Demoting only the non-error path gives both properties at once:

| | Routine events | Startup failures |
|---|---|---|
| `fx.NopLogger` | silent | **silent** |
| `ZapLogger` (fx default) | one line per constructor/hook | logged |
| `logger.Module()` | silent at Info | **logged** |

Observed output from a three-line app whose `OnStart` returns an error:

```
$ ./app                        # normal start, DEBUG_MODE unset
INFO   Logger initialized  {"level": "info"}

$ FAIL=1 ./app                 # OnStart returns an error
INFO   Logger initialized  {"level": "info"}
ERROR  OnStart hook failed  {"callee": "main.main.func1.1()", "error": "simulated migration failure"}
ERROR  start failed, rolling back  {"error": "simulated migration failure"}
ERROR  start failed  {"error": "simulated migration failure"}
```

Setting `DEBUG_MODE=true` lowers the zap level to `Debug` and the full Fx event
stream reappears, so no separate verbose flag is needed.

> **Do not add `fx.NopLogger`.** `fx.WithLogger` is last-one-wins, so a
> `NopLogger` listed after `logger.Module()` overrides this behavior and restores
> the silent-failure mode.

## License

Apache License 2.0
