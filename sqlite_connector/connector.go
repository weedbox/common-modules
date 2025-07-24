package sqlite_connector

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	"github.com/weedbox/common-modules/database"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gorm_logger "gorm.io/gorm/logger"
)

const (
	DefaultPath     = "./data.db"
	DefaultLogLevel = gorm_logger.Error
)

type SQLiteConnector struct {
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
	var dc database.DatabaseConnector
	return fx.Module(
		scope,
		fx.Provide(func(p Params) database.DatabaseConnector {
			c := &SQLiteConnector{
				params: p,
				logger: p.Logger.Named(scope),
				scope:  scope,
			}
			c.initDefaultConfigs()
			return c
		}),
		fx.Populate(&dc),
		fx.Invoke(func(p Params) {
			c := dc.(*SQLiteConnector)
			p.Lifecycle.Append(
				fx.Hook{
					OnStart: c.onStart,
					OnStop:  c.onStop,
				},
			)
		}),
	)
}

func (c *SQLiteConnector) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", c.scope, key)
}

func (c *SQLiteConnector) initDefaultConfigs() {
	viper.SetDefault(c.getConfigPath("path"), DefaultPath)
	viper.SetDefault(c.getConfigPath("loglevel"), DefaultLogLevel)
}

func (c *SQLiteConnector) onStart(ctx context.Context) error {
	dbPath := viper.GetString(c.getConfigPath("path"))

	// Ensure the directory exists
	if dir := filepath.Dir(dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			c.logger.Error("Failed to create database directory",
				zap.String("dir", dir),
				zap.Error(err))
			return fmt.Errorf("failed to create database directory %s: %w", dir, err)
		}
	}

	c.logger.Info("Starting SQLiteConnector",
		zap.String("path", dbPath),
		zap.Int("loglevel", viper.GetInt(c.getConfigPath("loglevel"))),
	)

	opts := &gorm.Config{
		Logger:         gorm_logger.Default.LogMode(gorm_logger.LogLevel(viper.GetInt(c.getConfigPath("loglevel")))),
		TranslateError: true,
	}

	db, err := gorm.Open(sqlite.Open(dbPath), opts)
	if err != nil {
		return err
	}

	c.db = db
	return nil
}

func (c *SQLiteConnector) onStop(ctx context.Context) error {
	c.logger.Info("Stopped SQLiteConnector")
	db, err := c.db.DB()
	if err != nil {
		return err
	}
	return db.Close()
}

func (c *SQLiteConnector) GetDB() *gorm.DB {
	return c.db
}
