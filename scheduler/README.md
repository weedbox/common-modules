# Scheduler Module

A common weedbox job scheduler module built on top of
[`github.com/Weedbox/scheduler`](https://github.com/Weedbox/scheduler) and
wired into [Uber Fx](https://github.com/uber-go/fx). Supports three backends:

| Mode       | Backend                                             | Deployment                                  |
|------------|-----------------------------------------------------|---------------------------------------------|
| `gorm`     | `*gorm.DB` + in-memory timer                        | **Single node only**                        |
| `postgres` | PostgreSQL 9.5+ claim-based scheduler (`FOR UPDATE SKIP LOCKED`) | Multi-instance coordinated through the shared database |
| `nats`     | NATS 2.12+ JetStream Scheduled Message Delivery     | Multi-instance with KV-backed persistence   |

The public API is identical in all modes — only `scheduler.mode` in config
changes.

> ⚠️ **Important:** `gorm` mode uses an in-memory timer inside the process.
> Running multiple replicas against the same database will cause **every
> job to fire on every replica**. Do not deploy `gorm` mode with more than
> one instance. Use `postgres` or `nats` mode for multi-node deployments.

> 💡 `postgres` mode shares `gorm` mode's tables (two claim-coordination
> columns are added on start), so a single-node `gorm` deployment on
> PostgreSQL upgrades to multi-instance `postgres` mode in place — no data
> migration. A typical setup runs `gorm` mode on SQLite in development and
> `postgres` mode in production.

## Installation

```bash
go get github.com/weedbox/common-modules/scheduler
```

## Configuration

```toml
[scheduler]
mode = "gorm"      # or "postgres" / "nats"

# Only relevant in postgres mode — all optional (omit to use upstream defaults)
# [scheduler.postgres]
# pollInterval            = "1s"   # idle poll; bounds cross-instance pickup latency
# leaseDuration           = "5m"   # claim lease; crashed instance's jobs recover after this
# maxConcurrentExecutions = 32     # per-instance cap on in-flight handlers
# execRecordTTL           = "720h" # prune execution records older than this (0 = keep forever)

# Only relevant in nats mode
[scheduler.nats]
streamName    = "SCHEDULER"
subjectPrefix = "scheduler"
consumerName  = "scheduler-worker"
jobBucket     = "SCHEDULER_JOBS"
execBucket    = "SCHEDULER_EXECUTIONS"

# Optional tuning knobs (omit to use upstream defaults)
# duplicatesWindow         = "24h"   # stream-level dedup window
# reconcilerInterval       = "30s"   # background republish reconciler
# reconcilerGracePeriod    = "30s"   # lag tolerated before a job is treated as stuck
# addJobRetryBudget        = "5s"    # AddJob/Update retry budget across raft hiccups
# jetStreamReadyTimeout    = "30s"   # wait for JetStream metaleader
# startPhaseTimeout        = "30s"   # per-phase cap inside Start()
# loadJobsConcurrency      = 32      # worker pool size for parallel KV reads at startup
# loadJobsAsyncPublishTimeout = "30s" # cap for draining startup async publishes
# [scheduler.nats.publishRetry]
# attempts        = 3
# initialBackoff  = "1s"
```

In `gorm` mode the module also needs a
[`database.DatabaseConnector`](../database/) to be provided (e.g.
`sqlite_connector` or `postgres_connector`). In `postgres` mode the same
dependency applies, but the connector must be `postgres_connector` — the
claim queries use PostgreSQL-specific SQL. In `nats` mode it needs
[`nats_connector`](../nats_connector/); first-deploy provisioning of the
stream, KV buckets, and durable consumer is handled internally by the
upstream library's convergent-ensure primitives.

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

### PostgreSQL Mode (multi-instance)

```go
fx.New(
    logger.Module(),
    postgres_connector.Module("postgres"),
    scheduler.Module("scheduler"),
    // ... same SetHandler / EnsureJob invokes as above
)
```

With `mode = "postgres"` any number of replicas coordinate through the
shared database: each due job is claimed by exactly one instance via
`FOR UPDATE SKIP LOCKED`, claims are lease-protected with automatic
heartbeats, and a crashed replica's jobs are re-claimed by a peer once the
lease expires. Delivery is at-least-once — keep handlers idempotent.

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
reloads jobs from KV and lets the shared durable consumer fan triggers out
to whichever instance is free; stale messages left by a previous run are
filtered against KV state in the message handler rather than purged from
the stream. This keeps single-instance restarts and multi-instance load
splitting on the same code path.

**Multi-instance deployments.** Multiple scheduler replicas can run against
the same stream concurrently. First-deploy provisioning of the stream, KV
buckets, and durable consumer converges through the upstream library's
ensure helpers, and the durable consumer's work-queue semantics ensure
each trigger is delivered to exactly one replica. Recurring chains survive
replica crashes, leader re-elections, and rolling restarts; a background
reconciler republishes any job whose next-run has slipped past its grace
window.

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
- **`postgres` mode supports multi-instance deployments** without NATS. Due
  jobs are claimed with a single `FOR UPDATE SKIP LOCKED` round-trip —
  exactly one winner per tick, no leader election. Claims are leased and
  heartbeated; a crashed replica's jobs are taken over once the lease
  expires (`postgres.leaseDuration`, default 5m). The database clock is the
  single time authority, so host clock skew cannot shift or duplicate
  ticks. Cross-instance pickup latency for new/updated jobs is bounded by
  `postgres.pollInterval` (default 1s).
- **Delivery is at-least-once in `postgres` and `nats` modes — write
  idempotent handlers.** Failover, lease expiry after a crash, and
  reconciler repairs can all re-fire a tick in rare windows.
- **`nats` mode** requires NATS Server **2.12 or later** with JetStream
  enabled. Startup will fail with `ErrNATSServerTooOld` on older servers.
- On startup in NATS mode the library reloads jobs from the KV bucket and
  filters stale in-flight scheduled messages against the persisted state
  (rather than purging the stream), so a crashed run does not duplicate
  triggers and a healthy peer is not disrupted.
- **`nats` mode supports multi-instance deployments.** Replicas share the
  same durable consumer; each scheduled message is delivered to exactly
  one replica. First-deploy provisioning converges through the upstream
  library's ensure helpers. The scheduler's background reconciler
  republishes any recurring job whose next-run has slipped past
  `nats.reconcilerGracePeriod` to recover from publish failures or
  unclean shutdowns.
