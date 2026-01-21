# Database Module

A database abstraction interface for [GORM](https://gorm.io/), providing a common interface for different database connectors.

## Features

- Common interface for database operations
- GORM integration
- Swappable database backends (PostgreSQL, SQLite, etc.)

## Installation

```bash
go get github.com/weedbox/common-modules/database
```

## Interface

```go
type DatabaseConnector interface {
    GetDB() *gorm.DB
}
```

## Available Implementations

- [postgres_connector](../postgres_connector) - PostgreSQL database connector
- [sqlite_connector](../sqlite_connector) - SQLite database connector

## Usage

### Injecting Database Connector

```go
package repository

import (
    "github.com/weedbox/common-modules/database"
    "go.uber.org/fx"
)

type Params struct {
    fx.In

    Database database.DatabaseConnector
}

func (r *Repository) GetUsers() ([]User, error) {
    var users []User
    err := r.params.Database.GetDB().Find(&users).Error
    return users, err
}
```

### Switching Database Backends

The `DatabaseConnector` interface allows you to switch between different database implementations without changing your application code:

```go
package main

import (
    "github.com/weedbox/common-modules/postgres_connector"
    // or
    // "github.com/weedbox/common-modules/sqlite_connector"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        // Use PostgreSQL
        postgres_connector.Module("database"),

        // Or use SQLite
        // sqlite_connector.Module("database"),

        // Your application modules that depend on database.DatabaseConnector
        // will work with either implementation
    ).Run()
}
```

## License

Apache License 2.0
