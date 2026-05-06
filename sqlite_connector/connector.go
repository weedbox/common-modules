package sqlite_connector

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
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
	DefaultMaxIdleConns         = 1
	DefaultConnMaxLifetime      = 3600
	DefaultConnMaxIdleTime      = 30
	DefaultEnableReadWriteSplit = true

	DefaultWriteMaxOpenConns = 10
	DefaultWriteMaxIdleConns = 1

	DefaultSynchronous = ""
	DefaultCacheSize   = 0
	DefaultLockingMode = ""
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
	viper.SetDefault(c.getConfigPath("conn_max_idle_time"), DefaultConnMaxIdleTime)
	viper.SetDefault(c.getConfigPath("enable_read_write_split"), DefaultEnableReadWriteSplit)
	viper.SetDefault(c.getConfigPath("write_max_open_conns"), DefaultWriteMaxOpenConns)
	viper.SetDefault(c.getConfigPath("write_max_idle_conns"), DefaultWriteMaxIdleConns)
	viper.SetDefault(c.getConfigPath("synchronous"), DefaultSynchronous)
	viper.SetDefault(c.getConfigPath("cache_size"), DefaultCacheSize)
	viper.SetDefault(c.getConfigPath("locking_mode"), DefaultLockingMode)
}

func (c *SQLiteConnector) buildDSN(dbPath string) string {
	enableWAL := viper.GetBool(c.getConfigPath("enable_wal"))
	busyTimeout := viper.GetInt(c.getConfigPath("busy_timeout"))
	synchronous := viper.GetString(c.getConfigPath("synchronous"))
	cacheSize := viper.GetInt(c.getConfigPath("cache_size"))
	lockingMode := viper.GetString(c.getConfigPath("locking_mode"))

	dsn := fmt.Sprintf("file:%s?_busy_timeout=%d&_foreign_keys=on", dbPath, busyTimeout)

	if enableWAL {
		dsn += "&_journal_mode=WAL"
	}

	if synchronous != "" {
		dsn += "&_synchronous=" + synchronous
	}

	if cacheSize != 0 {
		dsn += fmt.Sprintf("&_cache_size=%d", cacheSize)
	}

	if lockingMode != "" {
		dsn += "&_locking_mode=" + lockingMode
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
	connMaxIdleTime := viper.GetInt(c.getConfigPath("conn_max_idle_time"))
	enableSplit := viper.GetBool(c.getConfigPath("enable_read_write_split"))
	writeMaxOpenConns := viper.GetInt(c.getConfigPath("write_max_open_conns"))
	writeMaxIdleConns := viper.GetInt(c.getConfigPath("write_max_idle_conns"))
	synchronous := viper.GetString(c.getConfigPath("synchronous"))
	cacheSize := viper.GetInt(c.getConfigPath("cache_size"))
	lockingMode := viper.GetString(c.getConfigPath("locking_mode"))

	if strings.EqualFold(lockingMode, "EXCLUSIVE") {
		if enableSplit {
			c.logger.Warn("locking_mode=EXCLUSIVE is incompatible with read/write split; the second pool will fail to acquire the database lock — set enable_read_write_split=false")
		}
		if maxOpenConns > 1 || writeMaxOpenConns > 1 {
			c.logger.Warn("locking_mode=EXCLUSIVE only works with a single connection; set max_open_conns=1 and write_max_open_conns=1 to avoid SQLITE_BUSY",
				zap.Int("max_open_conns", maxOpenConns),
				zap.Int("write_max_open_conns", writeMaxOpenConns),
			)
		}
	}

	c.logger.Info("Starting SQLiteConnector",
		zap.String("path", dbPath),
		zap.Int("loglevel", viper.GetInt(c.getConfigPath("loglevel"))),
		zap.Bool("wal_mode", enableWAL),
		zap.Int("max_open_conns", maxOpenConns),
		zap.Int("max_idle_conns", maxIdleConns),
		zap.Int("conn_max_idle_time", connMaxIdleTime),
		zap.Bool("read_write_split", enableSplit),
		zap.String("synchronous", synchronous),
		zap.Int("cache_size", cacheSize),
		zap.String("locking_mode", lockingMode),
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
		primaryDB.SetConnMaxIdleTime(time.Duration(connMaxIdleTime) * time.Second)
		return nil
	}

	// Read/write split: register replica via dbresolver first, applying the
	// configured pool settings to both source and replicas, then override the
	// primary (source) pool with the writer-specific settings.
	//
	// The replica DSN intentionally matches the primary DSN (no mode=ro). With
	// mode=ro the connection cannot create or write the *-shm file or register
	// itself in the WAL read marks, so it would fail to see frames written by
	// the primary. WAL is also passed explicitly so the first connection (which
	// might be the replica on a fresh DB) sets the journal mode.
	resolver := dbresolver.Register(dbresolver.Config{
		Replicas: []gorm.Dialector{sqlite.Open(dsn)},
		Policy:   dbresolver.RandomPolicy{},
	}).
		SetMaxOpenConns(maxOpenConns).
		SetMaxIdleConns(maxIdleConns).
		SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Second).
		SetConnMaxIdleTime(time.Duration(connMaxIdleTime) * time.Second)

	if err := db.Use(resolver); err != nil {
		_ = primaryDB.Close()
		return fmt.Errorf("failed to register dbresolver: %w", err)
	}

	c.resolver = resolver

	// Override the primary pool with writer-specific settings. ConnMaxIdleTime
	// was already applied to both source and replicas via the dbresolver chain
	// above; it is reapplied here for symmetry so this block fully describes
	// the primary pool and won't silently regress if the chain order changes.
	primaryDB.SetMaxOpenConns(writeMaxOpenConns)
	primaryDB.SetMaxIdleConns(writeMaxIdleConns)
	primaryDB.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Second)
	primaryDB.SetConnMaxIdleTime(time.Duration(connMaxIdleTime) * time.Second)

	c.logger.Info("Read/write split enabled",
		zap.Int("primary_max_open_conns", writeMaxOpenConns),
		zap.Int("primary_max_idle_conns", writeMaxIdleConns),
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
