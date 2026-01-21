# Configs Module

A configuration management module built on [Viper](https://github.com/spf13/viper), providing unified configuration loading from files and environment variables.

## Features

- Load configuration from TOML files
- Environment variable support with automatic prefix
- Programmatic configuration override
- Print all settings for debugging

## Installation

```bash
go get github.com/weedbox/common-modules/configs
```

## Quick Start

### Basic Usage

```go
package main

import (
    "github.com/weedbox/common-modules/configs"
    "go.uber.org/fx"
)

func main() {
    // Create config with environment variable prefix
    config := configs.NewConfig("MYAPP")

    fx.New(
        fx.Supply(config),
        // ... other modules
    ).Run()
}
```

### Setting Default Configurations

```go
config := configs.NewConfig("MYAPP")

// Set default values (won't override existing values)
config.SetConfigs(map[string]interface{}{
    "http_server.host": "0.0.0.0",
    "http_server.port": 8080,
    "database.host":    "localhost",
    "database.port":    5432,
})
```

### Print All Settings

```go
config := configs.NewConfig("MYAPP")

// Print all current configurations
config.PrintAllSettings()
```

Output:
```
[List of current configs]
http_server.host=0.0.0.0
http_server.port=8080
database.host=localhost
database.port=5432
```

## Configuration Sources

The module loads configuration from multiple sources in the following priority order (highest to lowest):

1. **Environment Variables** - Prefixed with the specified prefix (e.g., `MYAPP_HTTP_SERVER_PORT`)
2. **Configuration File** - `config.toml` in `./` or `./configs/` directory
3. **Programmatic Defaults** - Set via `SetConfigs()`

### Environment Variables

Environment variables are automatically mapped using the prefix:

```bash
# With prefix "MYAPP", these environment variables:
export MYAPP_HTTP_SERVER_HOST=0.0.0.0
export MYAPP_HTTP_SERVER_PORT=8080
export MYAPP_DATABASE_HOST=localhost

# Map to these config keys:
# http_server.host
# http_server.port
# database.host
```

Key transformation rules:
- Prefix is prepended (e.g., `MYAPP_`)
- Dots (`.`) become underscores (`_`)
- Hyphens (`-`) become underscores (`_`)

### TOML Configuration File

Create a `config.toml` file in the project root or `./configs/` directory:

```toml
[http_server]
host = "0.0.0.0"
port = 8080

[database]
host = "localhost"
port = 5432
dbname = "myapp"
user = "postgres"
password = "secret"
```

## API Reference

### Config

#### `NewConfig(prefix string) *Config`

Creates a new Config instance and initializes Viper with the specified environment variable prefix.

- `prefix`: Prefix for environment variables (e.g., "MYAPP" results in "MYAPP_*" env vars)

#### `SetConfigs(configs map[string]interface{})`

Sets configuration values only if they are not already set. Useful for providing default values.

```go
config.SetConfigs(map[string]interface{}{
    "server.port": 8080,
    "server.host": "localhost",
})
```

#### `GetAllSettings() map[string]interface{}`

Returns all current configuration settings as a nested map.

#### `PrintAllSettings()`

Prints all configuration settings to stdout in a flat key=value format.

## License

Apache License 2.0
