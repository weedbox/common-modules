package sqlite_connector

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"
	"github.com/weedbox/common-modules/database"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gorm_logger "gorm.io/gorm/logger"
)

const (
	DefaultPath            = "./data.db"
	DefaultLogLevel        = gorm_logger.Error
	DefaultDebugMode       = false
	DefaultEnableWAL       = true
	DefaultBusyTimeout     = 5000
	DefaultMaxOpenConns    = 10
	DefaultMaxIdleConns    = 5
	DefaultConnMaxLifetime = 3600
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
	viper.SetDefault(c.getConfigPath("debug_mode"), DefaultDebugMode)
	viper.SetDefault(c.getConfigPath("enable_wal"), DefaultEnableWAL)
	viper.SetDefault(c.getConfigPath("busy_timeout"), DefaultBusyTimeout)
	viper.SetDefault(c.getConfigPath("max_open_conns"), DefaultMaxOpenConns)
	viper.SetDefault(c.getConfigPath("max_idle_conns"), DefaultMaxIdleConns)
	viper.SetDefault(c.getConfigPath("conn_max_lifetime"), DefaultConnMaxLifetime)
}

func (c *SQLiteConnector) buildDSN(dbPath string) string {
	enableWAL := viper.GetBool(c.getConfigPath("enable_wal"))
	busyTimeout := viper.GetInt(c.getConfigPath("busy_timeout"))

	dsn := fmt.Sprintf("file:%s?_busy_timeout=%d&_foreign_keys=on", dbPath, busyTimeout)

	if enableWAL {
		dsn += "&_journal_mode=WAL"
	}

	return dsn
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

	enableWAL := viper.GetBool(c.getConfigPath("enable_wal"))
	maxOpenConns := viper.GetInt(c.getConfigPath("max_open_conns"))
	maxIdleConns := viper.GetInt(c.getConfigPath("max_idle_conns"))
	connMaxLifetime := viper.GetInt(c.getConfigPath("conn_max_lifetime"))

	c.logger.Info("Starting SQLiteConnector",
		zap.String("path", dbPath),
		zap.Int("loglevel", viper.GetInt(c.getConfigPath("loglevel"))),
		zap.Bool("wal_mode", enableWAL),
		zap.Int("max_open_conns", maxOpenConns),
		zap.Int("max_idle_conns", maxIdleConns),
	)

	// Default logger configuration
	loggerCfg := gorm_logger.Config{
		SlowThreshold:             200 * time.Millisecond,
		LogLevel:                  gorm_logger.LogLevel(viper.GetInt(c.getConfigPath("loglevel"))),
		IgnoreRecordNotFoundError: true,
		ParameterizedQueries:      false,
		Colorful:                  false,
	}

	if viper.GetBool(c.getConfigPath("debug_mode")) {
		loggerCfg.LogLevel = gorm_logger.Info
		loggerCfg.ParameterizedQueries = true
		loggerCfg.Colorful = true
		loggerCfg.IgnoreRecordNotFoundError = false // Show RecordNotFound in debug mode
	}

	// Create logger based on Default config but with IgnoreRecordNotFoundError enabled
	gormLogger := gorm_logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // Same as Default
		loggerCfg,
	)

	opts := &gorm.Config{
		Logger:         gormLogger,
		TranslateError: true,
	}

	dsn := c.buildDSN(dbPath)
	db, err := gorm.Open(sqlite.Open(dsn), opts)
	if err != nil {
		return err
	}

	// Configure connection pool for concurrent access
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying sql.DB: %w", err)
	}

	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetMaxIdleConns(maxIdleConns)
	sqlDB.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Second)

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
