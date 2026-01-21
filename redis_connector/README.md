# Redis Connector Module

A Redis connector module built on [go-redis](https://github.com/go-redis/redis), integrated with [Uber Fx](https://github.com/uber-go/fx) for dependency injection.

## Features

- Uber Fx dependency injection integration
- Connection pooling
- Configuration via Viper
- Automatic connection verification on startup

## Installation

```bash
go get github.com/weedbox/common-modules/redis_connector
```

## Quick Start

### Basic Usage

```go
package main

import (
    "github.com/weedbox/common-modules/logger"
    "github.com/weedbox/common-modules/redis_connector"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        logger.Module(),
        redis_connector.Module("redis"),
    ).Run()
}
```

### Using Redis Client

```go
package cache

import (
    "context"
    "time"

    "github.com/weedbox/common-modules/redis_connector"
    "go.uber.org/fx"
)

type Params struct {
    fx.In

    Redis *redis_connector.RedisConnector
}

type CacheService struct {
    params Params
}

func (c *CacheService) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
    client := c.params.Redis.GetClient()
    return client.Set(ctx, key, value, expiration).Err()
}

func (c *CacheService) Get(ctx context.Context, key string) (string, error) {
    client := c.params.Redis.GetClient()
    return client.Get(ctx, key).Result()
}

func (c *CacheService) Delete(ctx context.Context, key string) error {
    client := c.params.Redis.GetClient()
    return client.Del(ctx, key).Err()
}

func (c *CacheService) Exists(ctx context.Context, key string) (bool, error) {
    client := c.params.Redis.GetClient()
    result, err := client.Exists(ctx, key).Result()
    return result > 0, err
}
```

### Common Operations

```go
func (c *CacheService) Examples(ctx context.Context) {
    client := c.params.Redis.GetClient()

    // String operations
    client.Set(ctx, "key", "value", time.Hour)
    client.Get(ctx, "key")
    client.SetNX(ctx, "key", "value", time.Hour) // Set if not exists

    // Hash operations
    client.HSet(ctx, "hash", "field", "value")
    client.HGet(ctx, "hash", "field")
    client.HGetAll(ctx, "hash")

    // List operations
    client.LPush(ctx, "list", "value1", "value2")
    client.RPop(ctx, "list")
    client.LRange(ctx, "list", 0, -1)

    // Set operations
    client.SAdd(ctx, "set", "member1", "member2")
    client.SMembers(ctx, "set")
    client.SIsMember(ctx, "set", "member1")

    // Sorted set operations
    client.ZAdd(ctx, "zset", &redis.Z{Score: 1, Member: "one"})
    client.ZRange(ctx, "zset", 0, -1)
    client.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "100"})

    // Key operations
    client.Expire(ctx, "key", time.Hour)
    client.TTL(ctx, "key")
    client.Del(ctx, "key")

    // Pipeline
    pipe := client.Pipeline()
    pipe.Set(ctx, "key1", "value1", 0)
    pipe.Set(ctx, "key2", "value2", 0)
    pipe.Exec(ctx)

    // Transaction
    client.Watch(ctx, func(tx *redis.Tx) error {
        _, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
            pipe.Set(ctx, "key1", "value1", 0)
            pipe.Set(ctx, "key2", "value2", 0)
            return nil
        })
        return err
    }, "key1", "key2")
}
```

## Configuration

Configuration is managed via Viper. All config keys are prefixed with the module's scope:

| Key | Default | Description |
|-----|---------|-------------|
| `{scope}.host` | `0.0.0.0` | Redis server host |
| `{scope}.port` | `6379` | Redis server port |
| `{scope}.password` | `""` | Redis password |
| `{scope}.db` | `0` | Redis database number |

### TOML Configuration Example

```toml
[redis]
host = "localhost"
port = 6379
password = "secret"
db = 0
```

### Environment Variables Example

```bash
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_PASSWORD=secret
export REDIS_DB=0
```

## API Reference

### RedisConnector

#### `Module(scope string) fx.Option`

Creates a Redis Connector module and returns an Fx Option.

- `scope`: Module namespace, used for config key prefix and logger naming

#### `GetClient() *redis.Client`

Returns the Redis client instance for performing operations.

```go
client := connector.GetClient()

// Use client for Redis operations
err := client.Set(ctx, "key", "value", time.Hour).Err()
val, err := client.Get(ctx, "key").Result()
```

## License

Apache License 2.0
