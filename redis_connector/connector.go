package redis_connector

import (
	"context"
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"github.com/go-redis/redis/v8"
)

var logger *zap.Logger

const (
	DefaultHost     = "0.0.0.0"
	DefaultPort     = 6379
	DefaultDB       = 0
	DefaultPassword = ""
)

type RedisConnector struct {
	params Params
	logger *zap.Logger
	client *redis.Client
	scope  string
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var c *RedisConnector

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *RedisConnector {

			logger = p.Logger.Named(scope)

			rc := &RedisConnector{
				params: p,
				logger: logger,
				scope:  scope,
			}

			rc.initDefaultConfigs()

			return rc
		}),
		fx.Populate(&c),
		fx.Invoke(func(p Params) *RedisConnector {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: c.onStart,
					OnStop:  c.onStop,
				},
			)

			return c
		}),
	)
}

func (c *RedisConnector) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", c.scope, key)
}

func (c *RedisConnector) initDefaultConfigs() {
	viper.SetDefault(c.getConfigPath("host"), DefaultHost)
	viper.SetDefault(c.getConfigPath("port"), DefaultPort)
	viper.SetDefault(c.getConfigPath("password"), DefaultPassword)
	viper.SetDefault(c.getConfigPath("db"), DefaultDB)
}

func (c *RedisConnector) onStart(ctx context.Context) error {

	// Prparing configurations
	host := viper.GetString(c.getConfigPath("host"))
	port := viper.GetInt(c.getConfigPath("port"))
	password := viper.GetString(c.getConfigPath("password"))
	db := viper.GetInt(c.getConfigPath("db"))

	logger.Info("Starting RedisConnector",
		zap.String("host", host),
		zap.Int("port", port),
		zap.Int("db", db),
	)

	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%v:%v", host, port), // use default Addr
		Password: password, // no password set
		DB:       db, // use default DB
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return err
	}

	c.client = rdb

	return nil
}

func (c *RedisConnector) onStop(ctx context.Context) error {

	logger.Info("Stopped RedisConnector")

	return c.client.Close()
}

func (c *RedisConnector) GetClient() *redis.Client {
	return c.client
}
