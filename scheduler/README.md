# Scheduler Module

A common weedbox job scheduler module built on top of
[`github.com/Weedbox/scheduler`](https://github.com/Weedbox/scheduler) and
wired into [Uber Fx](https://github.com/uber-go/fx). Supports two backends:

| Mode       | Backend                                             | Deployment                     |
|------------|-----------------------------------------------------|--------------------------------|
| `gorm`     | `*gorm.DB` + in-memory timer                        | **Single node only**           |
| `nats`     | NATS 2.12+ JetStream Scheduled Message Delivery     | Single node + KV-backed restart |

The public API is identical in both modes — only `scheduler.mode` in config
changes.

> ⚠️ **Important:** `gorm` mode uses an in-memory timer inside the process.
> Running multiple replicas against the same database will cause **every
> job to fire on every replica**. Do not deploy `gorm` mode with more than
> one instance. Use `nats` mode for multi-node deployments.

## Installation

```bash
go get github.com/weedbox/common-modules/scheduler
```

## Configuration

```toml
[scheduler]
mode = "gorm"      # or "nats"

# Only relevant in nats mode
[scheduler.nats]
streamName    = "SCHEDULER"
subjectPrefix = "scheduler"
consumerName  = "scheduler-worker"
jobBucket     = "SCHEDULER_JOBS"
execBucket    = "SCHEDULER_EXECUTIONS"
```

In `gorm` mode the module also needs a
[`database.DatabaseConnector`](../database/) to be provided (e.g.
`sqlite_connector` or `postgres_connector`). In `nats` mode it needs
[`nats_connector`](../nats_connector/).

## Quick Start

### GORM Mode (single node)

```go
package main

import (
    "context"
    "time"

    libsched "github.com/Weedbox/scheduler"
    "github.com/weedbox/common-modules/logger"
    "github.com/weedbox/common-modules/scheduler"
    "github.com/weedbox/common-modules/sqlite_connector"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        logger.Module(),
        sqlite_connector.Module("sqlite"),
        scheduler.Module("scheduler"),

        // Set the global job handler. Users dispatch on their own, typically
        // by switching on event.ID() or a metadata field.
        fx.Invoke(func(s *scheduler.SchedulerModule) {
            s.SetHandler(func(ctx context.Context, e libsched.JobEvent) error {
                switch e.ID() {
                case "daily_cleanup":
                    return doCleanup(ctx)
                case "hourly_metrics":
                    return flushMetrics(ctx)
                }
                return nil
            })
        }),

        // Idempotent registration — safe to call on every startup.
        fx.Invoke(func(s *scheduler.SchedulerModule) error {
            sch, _ := libsched.NewCronSchedule("0 3 * * *")
            return s.EnsureJob("daily_cleanup", sch, nil)
        }),
        fx.Invoke(func(s *scheduler.SchedulerModule) error {
            sch, _ := libsched.NewIntervalSchedule(time.Hour)
            return s.EnsureJob("hourly_metrics", sch, nil)
        }),
    ).Run()
}

func doCleanup(ctx context.Context) error  { /* ... */ return nil }
func flushMetrics(ctx context.Context) error { /* ... */ return nil }
```

### NATS / JetStream Mode

```go
fx.New(
    logger.Module(),
    nats_connector.Module("nats"),
    scheduler.Module("scheduler"),
    // ... same SetHandler / EnsureJob invokes as above
)
```

Job definitions are persisted in a JetStream KV bucket and scheduled
deliveries are published to a JetStream stream. On startup the scheduler
purges stale in-flight messages and reloads jobs from KV, so a crashed
process can be replaced by a fresh instance pointing at the same stream
without losing or duplicating registered jobs.

> ⚠️ **Important:** Do not run multiple scheduler instances concurrently
> against the same stream. Each `Start` purges the stream and recreates the
> durable consumer, which disrupts any peer that is currently subscribed.
> Run a single active instance; use NATS mode primarily for KV-backed
> persistence and clean restart/failover semantics, not for concurrent
> multi-node load splitting.

## API

```go
// Set the single global job handler. Typically called once at startup.
// Calling SetHandler after the scheduler has started is allowed; subsequent
// events will be routed to the new handler.
func (m *SchedulerModule) SetHandler(h libsched.JobHandler)

// EnsureJob registers a job idempotently.
// - If the ID does not exist, it is added.
// - If the ID exists, its schedule is updated (no-op if unchanged).
// Safe to call from fx.Invoke — the operation is queued until the scheduler
// has started.
func (m *SchedulerModule) EnsureJob(id string, schedule libsched.Schedule, metadata map[string]string) error

// SubmitJob adds a one-shot or dynamically created job. Returns
// libsched.ErrJobAlreadyExists if the ID collides. Also supports the
// pending-queue pattern used by EnsureJob.
func (m *SchedulerModule) SubmitJob(id string, schedule libsched.Schedule, metadata map[string]string) error

// Lookup / removal / listing — valid after the module has started.
func (m *SchedulerModule) RemoveJob(id string) error
func (m *SchedulerModule) GetJob(id string) (libsched.Job, error)
func (m *SchedulerModule) ListJobs() []libsched.Job

// Escape hatch — direct access to the underlying library scheduler.
func (m *SchedulerModule) GetScheduler() libsched.Scheduler
```

### Static vs. Dynamic Jobs

Two patterns that cover most use cases:

**Static jobs** — registered at startup with compile-time constant IDs.
Use `EnsureJob`; repeat calls are harmless, so a service can always register
its cron-like jobs on boot without worrying about duplicates.

```go
const JobDailyCleanup = "daily_cleanup"

sch, _ := libsched.NewCronSchedule("0 3 * * *")
_ = sm.EnsureJob(JobDailyCleanup, sch, nil)
```

**Dynamic jobs** — created at runtime with unique IDs, e.g. reminders,
delayed tasks. Use `SubmitJob`:

```go
func scheduleReminder(sm *scheduler.SchedulerModule, userID string, at time.Time) error {
    sch, _ := libsched.NewOnceSchedule(at)
    return sm.SubmitJob(
        fmt.Sprintf("reminder-%s-%d", userID, at.Unix()),
        sch,
        map[string]string{"kind": "reminder", "user_id": userID},
    )
}
```

In the global handler you dispatch dynamic jobs by metadata or ID prefix:

```go
sm.SetHandler(func(ctx context.Context, e libsched.JobEvent) error {
    if kind := e.Metadata()["kind"]; kind == "reminder" {
        return sendReminder(ctx, e.Metadata()["user_id"])
    }
    switch e.ID() {
    case JobDailyCleanup:
        return doCleanup(ctx)
    }
    return nil
})
```

The module itself is intentionally unopinionated about how you classify
jobs — you choose the convention (ID prefix, metadata key, etc.).

## Schedule Types

Schedule constructors live in the underlying library; import
`libsched "github.com/Weedbox/scheduler"` and use:

- `libsched.NewIntervalSchedule(d)` — every `d`
- `libsched.NewCronSchedule(expr)` — cron expression
- `libsched.NewOnceSchedule(t)` — one-shot at time `t`
- `libsched.NewStartAtIntervalSchedule(start, d)` — begin at `start`, every `d` after

## Deployment Notes

- **`gorm` mode** is for single-node deployments (a worker, a single-process
  service, a CLI). Do not scale horizontally — timers fire independently in
  every process.
- **`nats` mode** requires NATS Server **2.12 or later** with JetStream
  enabled. Startup will fail with `ErrNATSServerTooOld` on older servers.
- On startup in NATS mode the library purges pending scheduled messages
  from the previous run and reloads jobs from the KV bucket, so in-flight
  triggers from a crashed run are not duplicated.
- **NATS mode is single-active.** Each `Start` purges the stream and
  recreates the durable consumer; running two instances against the same
  stream will disrupt the existing one. The intended deployment is one
  active scheduler with KV-backed restart/failover, not concurrent
  multi-node fan-out.
