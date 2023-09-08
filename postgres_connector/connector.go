package postgres_connector

import (
	"context"
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	DefaultHost     = "0.0.0.0"
	DefaultPort     = 5432
	DefaultDbName   = "default"
	DefaultUser     = "postgres"
	DefaultPassword = ""
	DefaultSSLMode  = false
)

type PostgresConnector struct {
	params Params
	logger *zap.Logger
	db     *gorm.DB
	scope  string
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var c *PostgresConnector

	return fx.Options(
		fx.Provide(func(p Params) *PostgresConnector {

			c := &PostgresConnector{
				params: p,
				logger: p.Logger.Named(scope),
				scope:  scope,
			}

			return c
		}),
		fx.Populate(&c),
		fx.Invoke(func(p Params) {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: c.onStart,
					OnStop:  c.onStop,
				},
			)
		}),
	)

}

func (c *PostgresConnector) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", c.scope, key)
}

func (c *PostgresConnector) onStart(ctx context.Context) error {

	// default settings
	viper.SetDefault(c.getConfigPath("host"), DefaultHost)
	viper.SetDefault(c.getConfigPath("port"), DefaultPort)
	viper.SetDefault(c.getConfigPath("dbname"), DefaultDbName)
	viper.SetDefault(c.getConfigPath("user"), DefaultUser)
	viper.SetDefault(c.getConfigPath("password"), DefaultPassword)
	viper.SetDefault(c.getConfigPath("sslmode"), DefaultSSLMode)

	sslmode := "disable"
	if viper.GetBool(c.getConfigPath("sslmode")) {
		sslmode = "enable"
	}

	dsn := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d sslmode=%s",
		viper.GetString(c.getConfigPath("user")),
		viper.GetString(c.getConfigPath("password")),
		viper.GetString(c.getConfigPath("dbname")),
		viper.GetString(c.getConfigPath("host")),
		viper.GetInt(c.getConfigPath("port")),
		sslmode,
	)

	c.logger.Info("Starting PostgresConnector",
		zap.String("host", viper.GetString(c.getConfigPath("host"))),
		zap.Int("port", viper.GetInt(c.getConfigPath("port"))),
		zap.String("dbname", viper.GetString(c.getConfigPath("dbname"))),
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}

	c.db = db

	return nil
}

func (c *PostgresConnector) onStop(ctx context.Context) error {

	c.logger.Info("Stopped PostgresConnector")

	db, err := c.db.DB()
	if err != nil {
		return err
	}

	return db.Close()
}

func (c *PostgresConnector) GetDB() *gorm.DB {
	return c.db
}
