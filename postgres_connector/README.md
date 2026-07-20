# PostgreSQL Connector Module

A PostgreSQL database connector module built on [GORM](https://gorm.io/), integrated with [Uber Fx](https://github.com/uber-go/fx) dependency injection for clean lifecycle management.

## Features

- Uber Fx dependency injection integration
- Implements `database.DatabaseConnector` interface for abstraction
- Configurable connection parameters via Viper
- GORM logger integration with debug mode support
- Automatic connection lifecycle management

## Installation

```bash
go get github.com/weedbox/common-modules/postgres_connector
```

## Quick Start

### Basic Usage

```go
package main

import (
    "github.com/weedbox/common-modules/postgres_connector"
    "go.uber.org/fx"
    "go.uber.org/zap"
)

func main() {
    fx.New(
        fx.Provide(zap.NewDevelopment),
        postgres_connector.Module("database"),
    ).Run()
}
```

### Creating a Module with Database Access

The following example demonstrates how to create a module that uses the database connector:

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
    ID        string    `gorm:"type:uuid;primary_key"`
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
    Database  database.DatabaseConnector  // Inject the database connector
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
    "github.com/weedbox/common-modules/postgres_connector"
    "your-project/pkg/repository"
    "go.uber.org/fx"
    "go.uber.org/zap"
)

func main() {
    fx.New(
        fx.Provide(zap.NewDevelopment),
        postgres_connector.Module("database"),
        repository.Module("repository"),
    ).Run()
}
```

## Loading Multiple Connectors

`postgres_connector` and `sqlite_connector` can be loaded into the same `fx.App` simultaneously — for example to run PostgreSQL as the primary store while keeping a local SQLite cache. Each `Module(scope)` call always registers a named `database.DatabaseConnector` keyed by its `scope`, so consumers disambiguate using fx's `name` tag:

```go
type Params struct {
    fx.In

    Main  database.DatabaseConnector `name:"main"`    // postgres
    Cache database.DatabaseConnector `name:"cache"`   // sqlite
}

func main() {
    fx.New(
        fx.Provide(zap.NewDevelopment),
        postgres_connector.Module("main"),
        sqlite_connector.Module("cache"),
        fx.Invoke(func(p Params) { /* use p.Main, p.Cache */ }),
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
        postgres_connector.Module("database"),
        // ...
    )
    // ...
}
```

## Configuration

Configuration is managed via Viper. All config keys are prefixed with the module's scope:

| Key | Default | Description |
|-----|---------|-------------|
| `{scope}.host` | `0.0.0.0` | PostgreSQL server host |
| `{scope}.port` | `5432` | PostgreSQL server port |
| `{scope}.dbname` | `default` | Database name |
| `{scope}.user` | `postgres` | Database user |
| `{scope}.password` | `""` | Database password |
| `{scope}.sslmode` | `false` | SSL mode. Accepts a boolean (`true` → `require`, `false` → `disable`) or any libpq mode string: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full`. Unknown values fail at startup. |
| `{scope}.loglevel` | `4` (Error) | GORM log level (1=Silent, 2=Error, 3=Warn, 4=Info) |
| `{scope}.debug_mode` | `false` | Enable debug mode with detailed SQL logging |
| `{scope}.max_open_conns` | `0` | Maximum open connections in the pool. `0` means unlimited (Go `database/sql` default). Set a finite value (e.g. `50`) in production to avoid exhausting PostgreSQL's `max_connections`. |
| `{scope}.max_idle_conns` | `2` | Maximum idle connections kept in the pool (Go `database/sql` default). Raise this (e.g. `25`) under bursty load to reduce reconnection churn. |
| `{scope}.conn_max_lifetime` | `0` | Maximum lifetime of a connection in seconds. `0` means no expiration. Set to e.g. `1800` (30 min) so connections rotate through PostgreSQL failovers and don't accumulate stale state. |
| `{scope}.conn_max_idle_time` | `0` | Maximum time a connection can stay idle in the pool, in seconds. `0` means no expiration. Set to e.g. `600` (10 min) to release idle connections back to the server under low load. |

### TOML Configuration Example

```toml
[database]
host = "localhost"
port = 5432
dbname = "myapp"
user = "postgres"
password = "secret"
sslmode = false            # or a libpq mode string: "require", "verify-full", ...
loglevel = 4
debug_mode = false

# Connection pool tuning (defaults are 0 / 2 / 0 / 0 — i.e. Go database/sql
# native defaults). The values below are typical production settings.
max_open_conns = 50
max_idle_conns = 25
conn_max_lifetime = 1800   # 30 minutes
conn_max_idle_time = 600   # 10 minutes
```

### Environment Variables Example

```bash
export DATABASE_HOST=localhost
export DATABASE_PORT=5432
export DATABASE_DBNAME=myapp
export DATABASE_USER=postgres
export DATABASE_PASSWORD=secret
export DATABASE_SSLMODE=false
export DATABASE_DEBUG_MODE=true
export DATABASE_MAX_OPEN_CONNS=50
export DATABASE_MAX_IDLE_CONNS=25
export DATABASE_CONN_MAX_LIFETIME=1800
export DATABASE_CONN_MAX_IDLE_TIME=600
```

## API Reference

### PostgresConnector

#### `Module(scope string) fx.Option`

Creates a PostgreSQL Connector module and returns an Fx Option. The module provides a `database.DatabaseConnector` interface implementation.

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

This abstraction allows you to swap database implementations (e.g., PostgreSQL, SQLite) without changing your application code.

## Debug Mode

When `debug_mode` is enabled:
- Log level is set to Info (shows all SQL queries)
- Parameterized queries are logged with actual values
- Colorful output is enabled
- RecordNotFound errors are shown

This is useful for development and debugging database queries.

## License

Apache License 2.0
