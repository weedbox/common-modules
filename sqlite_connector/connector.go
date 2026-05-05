package sqlite_connector

import (
	"context"
	"database/sql"
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
	"gorm.io/plugin/dbresolver"
)

const (
	DefaultPath                 = "./data.db"
	DefaultLogLevel             = gorm_logger.Error
	DefaultDebugMode            = false
	DefaultEnableWAL            = true
	DefaultBusyTimeout          = 5000
	DefaultMaxOpenConns         = 10
	DefaultMaxIdleConns         = 5
	DefaultConnMaxLifetime      = 3600
	DefaultEnableReadWriteSplit = true

	// When read/write split is enabled, the primary (write) pool is forced
	// to a single connection so SQLite writes are serialized at the Go layer.
	primaryWriteMaxOpenConns = 1
	primaryWriteMaxIdleConns = 1
)

type SQLiteConnector struct {
	params     Params
	logger     *zap.Logger
	db         *gorm.DB
	scope      string
	resolver   *dbresolver.DBResolver
	splitMode  bool
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
	viper.SetDefault(c.getConfigPath("enable_read_write_split"), DefaultEnableReadWriteSplit)
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

// buildReadOnlyDSN returns a DSN suitable for replica (read-only) connections.
// We deliberately drop `_journal_mode=WAL` and `_foreign_keys=on` because RO
// connections cannot mutate PRAGMAs and FK enforcement only applies to writes.
func (c *SQLiteConnector) buildReadOnlyDSN(dbPath string) string {
	busyTimeout := viper.GetInt(c.getConfigPath("busy_timeout"))
	return fmt.Sprintf("file:%s?mode=ro&_busy_timeout=%d", dbPath, busyTimeout)
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
	enableSplit := viper.GetBool(c.getConfigPath("enable_read_write_split"))

	c.logger.Info("Starting SQLiteConnector",
		zap.String("path", dbPath),
		zap.Int("loglevel", viper.GetInt(c.getConfigPath("loglevel"))),
		zap.Bool("wal_mode", enableWAL),
		zap.Int("max_open_conns", maxOpenConns),
		zap.Int("max_idle_conns", maxIdleConns),
		zap.Bool("read_write_split", enableSplit),
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

	gormLogger := gorm_logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
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

	primaryDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying sql.DB: %w", err)
	}

	c.splitMode = enableSplit
	c.db = db

	if !enableSplit {
		primaryDB.SetMaxOpenConns(maxOpenConns)
		primaryDB.SetMaxIdleConns(maxIdleConns)
		primaryDB.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Second)
		return nil
	}

	// Read/write split: register replica via dbresolver first, applying the
	// configured pool settings to both source and replicas, then override the
	// primary (source) pool down to a single serialized writer.
	readDSN := c.buildReadOnlyDSN(dbPath)
	resolver := dbresolver.Register(dbresolver.Config{
		Replicas: []gorm.Dialector{sqlite.Open(readDSN)},
		Policy:   dbresolver.RandomPolicy{},
	}).
		SetMaxOpenConns(maxOpenConns).
		SetMaxIdleConns(maxIdleConns).
		SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Second)

	if err := db.Use(resolver); err != nil {
		_ = primaryDB.Close()
		return fmt.Errorf("failed to register dbresolver: %w", err)
	}

	c.resolver = resolver

	// Override the primary pool: SQLite writes must be serialized.
	primaryDB.SetMaxOpenConns(primaryWriteMaxOpenConns)
	primaryDB.SetMaxIdleConns(primaryWriteMaxIdleConns)
	primaryDB.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Second)

	c.logger.Info("Read/write split enabled",
		zap.String("read_dsn", readDSN),
		zap.Int("primary_max_open_conns", primaryWriteMaxOpenConns),
		zap.Int("replica_max_open_conns", maxOpenConns),
	)

	return nil
}

func (c *SQLiteConnector) onStop(ctx context.Context) error {
	c.logger.Info("Stopped SQLiteConnector")

	if c.resolver != nil {
		// Close every connPool registered with the resolver. The resolver
		// iterates sources (primary) and replicas, so this also closes the
		// primary's underlying sql.DB.
		return c.resolver.Call(func(cp gorm.ConnPool) error {
			if closer, ok := cp.(*sql.DB); ok {
				return closer.Close()
			}
			return nil
		})
	}

	db, err := c.db.DB()
	if err != nil {
		return err
	}
	return db.Close()
}

func (c *SQLiteConnector) GetDB() *gorm.DB {
	return c.db
}
