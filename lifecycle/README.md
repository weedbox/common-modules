# Lifecycle Module

An application-level lifecycle phase module integrated with [Uber Fx](https://github.com/uber-go/fx). It adds two phases that fx does not provide out of the box:

- **PostStart** — hooks that run after **every** module's `OnStart` has completed
- **PreStop** — hooks that run when shutdown begins, before **any** module's `OnStop`

Any module, anywhere in the module list, can inject the `Manager` and register hooks — without needing to be placed at the end of the module list itself.

## How It Works

fx executes `OnStart` hooks in append order and `OnStop` hooks in reverse append order, and module invokes run in module registration order. The lifecycle module concentrates the "must be registered last" constraint into this single module: because its own fx hook is appended after every other module's, its `OnStart` (which runs the PostStart hooks) fires last, and its `OnStop` (which runs the PreStop hooks) fires first.

## Features

- Uber Fx dependency injection integration
- PostStart hooks: run after all modules have started, in registration order
- PreStop hooks: run before all modules stop, in reverse registration order
- Fail-fast startup: a failing PostStart hook aborts application startup (fx rolls back already-started modules)
- Resilient shutdown: a failing PreStop hook is logged and joined into the returned error, but every hook still runs
- Panics in hooks are recovered and converted into errors
- Structured logging with per-hook duration

## Installation

```bash
go get github.com/weedbox/common-modules/lifecycle
```

## Quick Start

### Module Placement

**Important:** `lifecycle.Module` must be registered after all business modules, and before `daemon.Module`. By convention it goes into `afterModules()`:

```go
func afterModules() ([]fx.Option, error) {
    modules := []fx.Option{
        // lifecycle must come after all business modules so its PostStart
        // phase runs after every module's OnStart.
        lifecycle.Module("lifecycle"),

        // daemon stays last: readiness flips to true only after the
        // PostStart phase has completed successfully.
        daemon.Module("daemon"),
    }

    return modules, nil
}
```

With this ordering:

- **Startup:** business modules `OnStart` → PostStart hooks → daemon marks ready (traffic starts)
- **Shutdown:** daemon un-marks ready (traffic drains) → PreStop hooks → business modules `OnStop`

### Registering Hooks from a Module

Inject `*lifecycle.Manager` (a common module — no `name` tag) and register hooks from a constructor, an `fx.Invoke`, or `OnStart`:

```go
package mymodule

import (
    "context"

    "github.com/weedbox/common-modules/lifecycle"
    "github.com/weedbox/weedbox"
)

type Params struct {
    weedbox.Params

    AppLifecycle *lifecycle.Manager
}

type MyModule struct {
    weedbox.Module[*Params]
}

func (m *MyModule) OnStart(ctx context.Context) error {

    // Runs only after ALL modules have completed OnStart
    m.Params().AppLifecycle.PostStart("mymodule.warmup", func(ctx context.Context) error {
        return m.warmupCache(ctx)
    })

    // Runs at shutdown, before ANY module's OnStop
    m.Params().AppLifecycle.PreStop("mymodule.drain", func(ctx context.Context) error {
        return m.drain(ctx)
    })

    return nil
}
```

> **Naming note:** the injected field is named `AppLifecycle` (not `Lifecycle`) to avoid shadowing the `fx.Lifecycle` field promoted from the embedded `weedbox.Params`.

## Semantics

| Phase | When | Order | Failure behavior |
|-------|------|-------|------------------|
| `PostStart` | After all modules' `OnStart` completed | Registration order, sequential | First error aborts startup; fx rolls back already-started modules |
| `PreStop` | At shutdown, before all modules' `OnStop` | Reverse registration order | Logged; remaining hooks still run; errors joined into the returned error |

Additional rules:

- Hook names are for logging only; duplicates are allowed.
- A `nil` hook function is ignored with a warning.
- Registering after the phase has executed returns `ErrPhaseCompleted`.
- A panic inside a hook is recovered and converted into an error.
- If startup fails (any `OnStart` or PostStart hook errors), fx skips the lifecycle module's `OnStop`, so **PreStop hooks do not run on failed startup** — standard fx rollback semantics.

## Caveats

- **Start timeout:** PostStart hooks run synchronously inside `app.Start`, so they count against `fx.StartTimeout` (default 15s). Long-running work should spawn its own goroutine or be dispatched to a work queue; use PostStart only for the coordination point.
- **Multi-instance deployments:** every replica runs the PostStart hooks. One-time jobs (migrations, seeding) must be idempotent or guarded by leader election / a distributed lock.
- **Root-level `fx.Invoke`:** fx runs module invokes before root-level (non-module) invokes. An `fx.Hook` appended by a bare `fx.Invoke(...)` in your `modules.go` is appended after the lifecycle module's hook, so its `OnStart` runs *after* the PostStart phase. Keep glue code that appends hooks inside an `fx.Module`, or register it as a PostStart hook instead.

## API Reference

### Manager

| Method | Description |
|--------|-------------|
| `PostStart(name string, fn HookFunc) error` | Register a hook to run after all modules have started. Returns `ErrPhaseCompleted` if the phase already ran. |
| `PreStop(name string, fn HookFunc) error` | Register a hook to run before all modules stop. Returns `ErrPhaseCompleted` if the phase already ran. |

### Types

```go
type HookFunc func(ctx context.Context) error
```

### Errors

| Error | Description |
|-------|-------------|
| `ErrPhaseCompleted` | The hook was registered after its phase had already executed. |

## License

Licensed under the Apache License, Version 2.0. See the [LICENSE](../LICENSE) file for details.
