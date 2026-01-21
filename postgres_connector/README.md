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

## Configuration

Configuration is managed via Viper. All config keys are prefixed with the module's scope:

| Key | Default | Description |
|-----|---------|-------------|
| `{scope}.host` | `0.0.0.0` | PostgreSQL server host |
| `{scope}.port` | `5432` | PostgreSQL server port |
| `{scope}.dbname` | `default` | Database name |
| `{scope}.user` | `postgres` | Database user |
| `{scope}.password` | `""` | Database password |
| `{scope}.sslmode` | `false` | Enable SSL connection |
| `{scope}.loglevel` | `4` (Error) | GORM log level (1=Silent, 2=Error, 3=Warn, 4=Info) |
| `{scope}.debug_mode` | `false` | Enable debug mode with detailed SQL logging |

### TOML Configuration Example

```toml
[database]
host = "localhost"
port = 5432
dbname = "myapp"
user = "postgres"
password = "secret"
sslmode = false
loglevel = 4
debug_mode = false
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
