# SQLite Connector Module

A SQLite database connector module built on [GORM](https://gorm.io/), integrated with [Uber Fx](https://github.com/uber-go/fx) dependency injection for clean lifecycle management.

## Features

- Uber Fx dependency injection integration
- Implements `database.DatabaseConnector` interface for abstraction
- Automatic directory creation for database file
- GORM logger integration with debug mode support
- Lightweight embedded database
- Concurrent read/write support via WAL mode and connection pool
- Optional read/write splitting via [`gorm.io/plugin/dbresolver`](https://gorm.io/docs/dbresolver.html)

## Installation

```bash
go get github.com/weedbox/common-modules/sqlite_connector
```

## Quick Start

### Basic Usage

```go
package main

import (
    "github.com/weedbox/common-modules/logger"
    "github.com/weedbox/common-modules/sqlite_connector"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        logger.Module(),
        sqlite_connector.Module("database"),
    ).Run()
}
```

### Creating a Repository Module

```go
package repository

import (
    "context"
    "time"

    "github.com/weedbox/common-modules/database"
    "go.uber.org/fx"
    "go.uber.org/zap"
)

// Define your models
type User struct {
    ID        string    `gorm:"type:text;primary_key"`
    Name      string
    Email     string    `gorm:"uniqueIndex"`
    CreatedAt time.Time
}

type Repository struct {
    params Params
    logger *zap.Logger
    scope  string
}

type Params struct {
    fx.In

    Lifecycle fx.Lifecycle
    Logger    *zap.Logger
    Database  database.DatabaseConnector
}

func Module(scope string) fx.Option {

    var r *Repository

    return fx.Module(
        scope,
        fx.Provide(func(p Params) *Repository {
            r = &Repository{
                params: p,
                logger: p.Logger.Named(scope),
                scope:  scope,
            }
            return r
        }),
        fx.Populate(&r),
        fx.Invoke(func(p Params) {
            p.Lifecycle.Append(
                fx.Hook{
                    OnStart: r.onStart,
                    OnStop:  r.onStop,
                },
            )
        }),
    )
}

func (r *Repository) onStart(ctx context.Context) error {
    r.logger.Info("Starting Repository")

    // Auto migration
    db := r.params.Database.GetDB()
    db.AutoMigrate(&User{})

    return nil
}

func (r *Repository) onStop(ctx context.Context) error {
    r.logger.Info("Stopped Repository")
    return nil
}

// CreateUser creates a new user
func (r *Repository) CreateUser(user *User) error {
    return r.params.Database.GetDB().Create(user).Error
}

// GetUserByID retrieves a user by ID
func (r *Repository) GetUserByID(id string) (*User, error) {
    var user User
    err := r.params.Database.GetDB().First(&user, "id = ?", id).Error
    if err != nil {
        return nil, err
    }
    return &user, nil
}

// GetAllUsers retrieves all users
func (r *Repository) GetAllUsers() ([]User, error) {
    var users []User
    err := r.params.Database.GetDB().Find(&users).Error
    return users, err
}

// UpdateUser updates an existing user
func (r *Repository) UpdateUser(user *User) error {
    return r.params.Database.GetDB().Save(user).Error
}

// DeleteUser deletes a user by ID
func (r *Repository) DeleteUser(id string) error {
    return r.params.Database.GetDB().Delete(&User{}, "id = ?", id).Error
}
```

### Integrating with Other Modules

```go
package main

import (
    "github.com/weedbox/common-modules/logger"
    "github.com/weedbox/common-modules/sqlite_connector"
    "your-project/pkg/repository"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        logger.Module(),
        sqlite_connector.Module("database"),
        repository.Module("repository"),
    ).Run()
}
```

## Loading Multiple Connectors

`sqlite_connector` and `postgres_connector` can be loaded into the same `fx.App` simultaneously — for example to run SQLite for a local cache alongside a PostgreSQL primary. Each `Module(scope)` call always registers a named `database.DatabaseConnector` keyed by its `scope`, so consumers disambiguate using fx's `name` tag:

```go
type Params struct {
    fx.In

    Cache database.DatabaseConnector `name:"cache"`   // sqlite
    Main  database.DatabaseConnector `name:"main"`    // postgres
}

func main() {
    fx.New(
        logger.Module(),
        sqlite_connector.Module("cache"),
        postgres_connector.Module("main"),
        fx.Invoke(func(p Params) { /* use p.Cache, p.Main */ }),
    ).Run()
}
```

The **first** connector module loaded into the process (across all `database.DatabaseConnector` implementations) also exposes itself as the **unnamed default**, so existing single-load code that injects `database.DatabaseConnector` without a tag keeps working with zero changes. Load order controls which connector becomes the default. If load order is brittle, always inject by named tag in multi-load setups.

### Test caveat: ResetClaim between fx.Apps

The "first call wins" claim on the unnamed default uses process-level state. Tests that build more than one `fx.App` in the same process must reset that claim between apps, otherwise later apps cannot register an unnamed default:

```go
import "github.com/weedbox/weedbox/fxmodule"

func TestSomething(t *testing.T) {
    fxmodule.ResetClaim[database.DatabaseConnector]()
    t.Cleanup(func() { fxmodule.ResetClaim[database.DatabaseConnector]() })

    app := fx.New(
        sqlite_connector.Module("database"),
        // ...
    )
    // ...
}
```

## Configuration

Configuration is managed via Viper. All config keys are prefixed with the module's scope:

| Key | Default | Description |
|-----|---------|-------------|
| `{scope}.path` | `./data.db` | Path to SQLite database file |
| `{scope}.loglevel` | `4` (Error) | GORM log level (1=Silent, 2=Error, 3=Warn, 4=Info) |
| `{scope}.debug_mode` | `false` | Enable debug mode with detailed SQL logging |
| `{scope}.enable_wal` | `true` | Enable WAL (Write-Ahead Logging) journal mode for concurrent read/write |
| `{scope}.busy_timeout` | `5000` | Milliseconds to wait when the database is locked before returning an error |
| `{scope}.max_open_conns` | `10` | Maximum number of open connections in the pool. When `enable_read_write_split` is `true`, this applies to the read replica pool only; the primary (write) pool uses `write_max_open_conns`. |
| `{scope}.max_idle_conns` | `1` | Maximum number of idle connections in the pool. When `enable_read_write_split` is `true`, this applies to the read replica pool only; the primary (write) pool uses `write_max_idle_conns`. Kept low so idle reader connections do not pin WAL frames and block checkpointing. |
| `{scope}.conn_max_lifetime` | `3600` | Maximum lifetime of a connection in seconds |
| `{scope}.conn_max_idle_time` | `30` | Seconds an idle connection can stay in the pool before being closed. Important for SQLite + WAL: an idle connection holds the snapshot of its last `SELECT`, preventing `wal_checkpoint` from advancing past it. Closing idle connections promptly releases that snapshot. |
| `{scope}.enable_read_write_split` | `true` | Enable read/write splitting via dbresolver. Writes go to the primary connection; reads go to a separate pool. Set to `false` to fall back to the legacy single-pool behavior. |
| `{scope}.write_max_open_conns` | `10` | Maximum open connections in the primary (write) pool when read/write split is enabled. |
| `{scope}.write_max_idle_conns` | `1` | Maximum idle connections in the primary (write) pool when read/write split is enabled. Defaults to `1` because SQLite serializes writes — keeping many idle writer connections holding the file gains nothing. |
| `{scope}.synchronous` | `""` (use SQLite default) | Sets `PRAGMA synchronous`. Accepts `OFF`, `NORMAL`, `FULL`, `EXTRA`. With WAL, `NORMAL` is safe and significantly cheaper than `FULL` (one fsync per checkpoint instead of one per commit). Recommended on slow-fsync storage (e.g. NFS). |
| `{scope}.cache_size` | `0` (use SQLite default) | Sets `PRAGMA cache_size`. Negative values are KB (e.g. `-65536` = 64 MiB), positive values are pages. A larger cache reduces page reads from disk and is one of the cheapest wins on slow storage. |
| `{scope}.locking_mode` | `""` (use SQLite default `NORMAL`) | Sets `PRAGMA locking_mode`. `EXCLUSIVE` keeps the database lock held across the entire connection lifetime, avoiding repeated lock acquisition syscalls — useful on filesystems with slow/unreliable advisory locks (e.g. NFS). **Only safe with a single connection**: set `enable_read_write_split=false`, `max_open_conns=1`, and `write_max_open_conns=1`. The connector logs a warning if these are misconfigured. |

### TOML Configuration Example

```toml
[database]
path = "./data/myapp.db"
loglevel = 4
debug_mode = false
enable_wal = true
busy_timeout = 5000
max_open_conns = 10
max_idle_conns = 1
conn_max_lifetime = 3600
conn_max_idle_time = 30
enable_read_write_split = true
write_max_open_conns = 10
write_max_idle_conns = 1
synchronous = ""
cache_size = 0
locking_mode = ""
```

### Environment Variables Example

```bash
export DATABASE_PATH=./data/myapp.db
export DATABASE_LOGLEVEL=4
export DATABASE_DEBUG_MODE=true
export DATABASE_ENABLE_WAL=true
export DATABASE_BUSY_TIMEOUT=5000
export DATABASE_MAX_OPEN_CONNS=10
export DATABASE_MAX_IDLE_CONNS=1
export DATABASE_CONN_MAX_LIFETIME=3600
export DATABASE_CONN_MAX_IDLE_TIME=30
export DATABASE_ENABLE_READ_WRITE_SPLIT=true
export DATABASE_WRITE_MAX_OPEN_CONNS=10
export DATABASE_WRITE_MAX_IDLE_CONNS=1
export DATABASE_SYNCHRONOUS=NORMAL
export DATABASE_CACHE_SIZE=-65536
export DATABASE_LOCKING_MODE=
```

## API Reference

### SQLiteConnector

#### `Module(scope string) fx.Option`

Creates a SQLite Connector module and returns an Fx Option. The module provides a `database.DatabaseConnector` interface implementation.

- `scope`: Module namespace, used for config key prefix and logger naming

#### `GetDB() *gorm.DB`

Returns the GORM database instance for performing database operations.

```go
db := connector.GetDB()

// Auto migration
db.AutoMigrate(&User{}, &Product{})

// Query
var users []User
db.Find(&users)

// Create
db.Create(&User{Name: "John"})

// Raw SQL
db.Raw("SELECT * FROM users WHERE age > ?", 18).Scan(&users)
```

## Database Interface

The module implements the `database.DatabaseConnector` interface:

```go
type DatabaseConnector interface {
    GetDB() *gorm.DB
}
```

This abstraction allows you to swap database implementations (e.g., SQLite, PostgreSQL) without changing your application code.

## Debug Mode

When `debug_mode` is enabled:
- Log level is set to Info (shows all SQL queries)
- Parameterized queries are logged with actual values
- Colorful output is enabled
- RecordNotFound errors are shown

This is useful for development and debugging database queries.

## Concurrent Read/Write

By default, the connector enables **WAL (Write-Ahead Logging)** mode and configures a connection pool to support concurrent access. WAL mode allows multiple readers to operate simultaneously without being blocked by a writer.

Key behaviors:
- **WAL mode** (`enable_wal`): Readers do not block writers and writers do not block readers. Only one writer can operate at a time; other writes will wait up to `busy_timeout` milliseconds.
- **Busy timeout** (`busy_timeout`): When a connection encounters a database lock, it waits for the specified duration instead of immediately returning a `database is locked` error.
- **Connection pool** (`max_open_conns`, `max_idle_conns`, `conn_max_lifetime`, `conn_max_idle_time`): Manages multiple connections to allow concurrent database operations. `conn_max_idle_time` is particularly important under WAL: an idle connection holds the read snapshot of its last query and prevents `wal_checkpoint` from advancing past it. Closing idle connections promptly lets WAL frames recycle into the main file.
- **Foreign keys**: Automatically enabled via `_foreign_keys=on` PRAGMA.

> **Note**: WAL mode is recommended for most use cases. Set `enable_wal` to `false` only if you need rollback journal mode for specific compatibility reasons.

## Read/Write Splitting

Read/write splitting is enabled by default (`enable_read_write_split=true`). The connector wires a single `*gorm.DB` to two separate `*sql.DB` pools using GORM's official [`dbresolver`](https://gorm.io/docs/dbresolver.html) plugin:

- **Primary (write)** — pool size is controlled by `write_max_open_conns` / `write_max_idle_conns` (defaults `10` / `1`). The idle default is `1` because SQLite serializes writes; lower `write_max_open_conns` to `1` as well to fully serialize writes at the Go layer if you are seeing `SQLITE_BUSY` errors under contention.
- **Replicas (read)** — pool size follows the configured `max_open_conns` / `max_idle_conns`, allowing many concurrent readers.

Both pools open the database with the same DSN (including `_journal_mode=WAL`). Routing is handled at the SQL operation layer by `dbresolver`: `SELECT` / `First` / `Find` / `Count` / `Pluck` / `Take` / `Scan` go to the replica pool, while `Create` / `Save` / `Update` / `Delete` / `Transaction` / `Raw` / `Exec` go to the primary. Application code does not need to change. Set `enable_read_write_split` to `false` to fall back to the legacy single-pool behavior.

> **Why no `mode=ro` on the replica DSN**: a `mode=ro` connection cannot create or write the `-shm` file, register read marks in the WAL, or otherwise participate in the WAL protocol — so it would silently miss frames produced by the primary. Routing safety comes from `dbresolver` at the SQL layer, not from the file open mode.

> **Note**: Read/write splitting only makes sense when `enable_wal` is `true`. WAL is what allows readers and writers to operate concurrently against the same SQLite file.

## Running on Slow / Networked Storage (e.g. NFS-backed PV)

SQLite expects a local filesystem with working POSIX advisory locks and fast `fsync`. Networked or shared storage (NFS, some CSI drivers, certain k8s `PersistentVolume` classes) provides neither reliably:

- `fsync` over the network turns every COMMIT into a round-trip-bound operation (often 10–100× slower than local disk).
- Byte-range advisory locks (used by WAL) can break under contention.
- The `-shm` shared-memory file used by WAL behaves poorly when the filesystem is not truly local.

**The first thing to check if you see slow SQL on SQLite is the underlying StorageClass** — `kubectl get pvc -n <ns>` then `kubectl get storageclass`. If it routes through NFS / `nfs-csi-driver` / Longhorn-over-network / similar, that is almost certainly the bottleneck and no amount of connector tuning will eliminate it. The proper fix is a local-disk-backed StorageClass (`local-path`, `topolvm`, hostPath with locality), or switching to PostgreSQL.

If you genuinely cannot move off networked storage, the following PRAGMAs trade a small amount of durability/concurrency for substantially less fsync pressure:

```toml
[database]
# WAL + NORMAL: one fsync per checkpoint, not one per commit. Safe under WAL —
# you can lose the *very last* committed transaction on a crash, but the database
# remains consistent.
synchronous = "NORMAL"

# 64 MiB page cache. Cuts how often SQLite has to read pages back from the
# slow filesystem.
cache_size = -65536

# Optional, single-connection only: keep the file lock held for the lifetime of
# the connection so SQLite stops doing per-statement lock dances against NFS.
# REQUIRES enable_read_write_split=false, max_open_conns=1, write_max_open_conns=1.
locking_mode = ""
```

> **Warning**: `locking_mode=EXCLUSIVE` only works with a single connection holding the database. If `enable_read_write_split=true` or any pool size is greater than `1`, the second connection will block forever or fail with `SQLITE_BUSY`. The connector logs a warning at startup if it detects a misconfiguration.

## SQLite vs PostgreSQL

Use SQLite when:
- Building prototypes or small applications
- Need an embedded database without external dependencies
- Running tests that require a real database
- Building desktop or mobile applications

Use PostgreSQL when:
- Building production web services
- Need advanced features (JSON, full-text search, etc.)
- Need horizontal scaling

## License

Apache License 2.0
