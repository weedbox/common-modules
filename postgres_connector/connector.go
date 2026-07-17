package postgres_connector

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/viper"
	"github.com/weedbox/common-modules/database"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gorm_logger "gorm.io/gorm/logger"
)

const (
	DefaultHost      = "0.0.0.0"
	DefaultPort      = 5432
	DefaultDbName    = "default"
	DefaultUser      = "postgres"
	DefaultPassword  = ""
	DefaultSSLMode   = false
	DefaultLogLevel  = gorm_logger.Error
	DefaultDebugMode = false

	// Connection pool defaults aligned with Go database/sql native defaults.
	// Override via viper in production (typical: 50 / 25 / 1800 / 600).
	DefaultMaxOpenConns    = 0 // 0 = unlimited
	DefaultMaxIdleConns    = 2 // database/sql defaultMaxIdleConns
	DefaultConnMaxLifetime = 0 // seconds, 0 = no expiration
	DefaultConnMaxIdleTime = 0 // seconds, 0 = no expiration

	// Server-side per-session timeouts, sent as startup runtime parameters.
	// 0 = not set (server/database defaults apply).
	DefaultStatementTimeout = 0 // milliseconds
	DefaultLockTimeout      = 0 // milliseconds
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

// Module registers a Postgres-backed database.DatabaseConnector.
//
// Wiring (multi-load semantics, ResetClaim test caveat, etc.) is handled
// by database.Module — see that helper's doc for details.
func Module(scope string) fx.Option {
	return database.Module(scope, func(p Params) database.DatabaseConnector {
		c := &PostgresConnector{
			params: p,
			logger: p.Logger.Named(scope),
			scope:  scope,
		}
		c.initDefaultConfigs()
		p.Lifecycle.Append(fx.Hook{
			OnStart: c.onStart,
			OnStop:  c.onStop,
		})
		return c
	})
}

func (c *PostgresConnector) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", c.scope, key)
}

func (c *PostgresConnector) initDefaultConfigs() {
	viper.SetDefault(c.getConfigPath("host"), DefaultHost)
	viper.SetDefault(c.getConfigPath("port"), DefaultPort)
	viper.SetDefault(c.getConfigPath("dbname"), DefaultDbName)
	viper.SetDefault(c.getConfigPath("user"), DefaultUser)
	viper.SetDefault(c.getConfigPath("password"), DefaultPassword)
	viper.SetDefault(c.getConfigPath("sslmode"), DefaultSSLMode)
	viper.SetDefault(c.getConfigPath("loglevel"), DefaultLogLevel)
	viper.SetDefault(c.getConfigPath("debug_mode"), DefaultDebugMode)
	viper.SetDefault(c.getConfigPath("max_open_conns"), DefaultMaxOpenConns)
	viper.SetDefault(c.getConfigPath("max_idle_conns"), DefaultMaxIdleConns)
	viper.SetDefault(c.getConfigPath("conn_max_lifetime"), DefaultConnMaxLifetime)
	viper.SetDefault(c.getConfigPath("conn_max_idle_time"), DefaultConnMaxIdleTime)
	viper.SetDefault(c.getConfigPath("statement_timeout"), DefaultStatementTimeout)
	viper.SetDefault(c.getConfigPath("lock_timeout"), DefaultLockTimeout)
}

func (c *PostgresConnector) onStart(ctx context.Context) error {

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

	// pgx passes unrecognized DSN keys to the server as session runtime
	// parameters, so these become per-connection GUCs (milliseconds).
	statementTimeout := viper.GetInt(c.getConfigPath("statement_timeout"))
	if statementTimeout > 0 {
		dsn += fmt.Sprintf(" statement_timeout=%d", statementTimeout)
	}
	lockTimeout := viper.GetInt(c.getConfigPath("lock_timeout"))
	if lockTimeout > 0 {
		dsn += fmt.Sprintf(" lock_timeout=%d", lockTimeout)
	}

	maxOpenConns := viper.GetInt(c.getConfigPath("max_open_conns"))
	maxIdleConns := viper.GetInt(c.getConfigPath("max_idle_conns"))
	connMaxLifetime := viper.GetInt(c.getConfigPath("conn_max_lifetime"))
	connMaxIdleTime := viper.GetInt(c.getConfigPath("conn_max_idle_time"))

	c.logger.Info("Starting PostgresConnector",
		zap.String("host", viper.GetString(c.getConfigPath("host"))),
		zap.Int("port", viper.GetInt(c.getConfigPath("port"))),
		zap.String("dbname", viper.GetString(c.getConfigPath("dbname"))),
		zap.Int("loglevel", viper.GetInt(c.getConfigPath("loglevel"))),
		zap.Int("max_open_conns", maxOpenConns),
		zap.Int("max_idle_conns", maxIdleConns),
		zap.Int("conn_max_lifetime", connMaxLifetime),
		zap.Int("conn_max_idle_time", connMaxIdleTime),
		zap.Int("statement_timeout", statementTimeout),
		zap.Int("lock_timeout", lockTimeout),
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

	db, err := gorm.Open(postgres.Open(dsn), opts)
	if err != nil {
		return err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying sql.DB: %w", err)
	}
	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetMaxIdleConns(maxIdleConns)
	sqlDB.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Second)
	sqlDB.SetConnMaxIdleTime(time.Duration(connMaxIdleTime) * time.Second)

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
