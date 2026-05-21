package nats_connector

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var logger *zap.Logger

const (
	DefaultHost                = "0.0.0.0:32803"
	DefaultPingInterval        = 10
	DefaultMaxPingsOutstanding = 3
	DefaultMaxReconnects       = -1
	DefaultAccessKey           = ""

	DefaultLockReplicas   = 3
	DefaultLockTTL        = 30 * time.Second
	DefaultLockBucketHint = "_locks" // suffix appended to scope
)

type NATSConnector struct {
	logger *zap.Logger
	conn   *nats.Conn
	js     nats.JetStreamContext
	jsv2   jetstream.JetStream
	scope  string
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var c *NATSConnector

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *NATSConnector {

			logger = p.Logger.Named(scope)

			hs := &NATSConnector{
				logger: logger,
				scope:  scope,
			}

			hs.initDefaultConfigs()

			return hs
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

func (c *NATSConnector) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", c.scope, key)
}

func (c *NATSConnector) initDefaultConfigs() {
	viper.SetDefault(c.getConfigPath("host"), DefaultHost)
	viper.SetDefault(c.getConfigPath("pingInterval"), DefaultPingInterval)
	viper.SetDefault(c.getConfigPath("maxPingsOutstanding"), DefaultMaxPingsOutstanding)
	viper.SetDefault(c.getConfigPath("maxReconnects"), DefaultMaxReconnects)

	viper.SetDefault(c.getConfigPath("lock.bucket"), c.scope+DefaultLockBucketHint)
	viper.SetDefault(c.getConfigPath("lock.replicas"), DefaultLockReplicas)
	viper.SetDefault(c.getConfigPath("lock.defaultTTL"), DefaultLockTTL)
}

func (c *NATSConnector) onStart(ctx context.Context) error {

	// Prparing configurations
	host := viper.GetString(c.getConfigPath("host"))
	pingInterval := viper.GetInt64(c.getConfigPath("pingInterval"))
	maxPingsOutstanding := viper.GetInt(c.getConfigPath("maxPingsOutstanding"))
	maxReconnects := viper.GetInt(c.getConfigPath("maxReconnects"))

	// Authentication and TLS configurations
	creds := viper.GetString(c.getConfigPath("auth.creds"))
	nkey := viper.GetString(c.getConfigPath("auth.nkey"))
	tlscert := viper.GetString(c.getConfigPath("tls.cert"))
	tlskey := viper.GetString(c.getConfigPath("tls.key"))
	tlsca := viper.GetString(c.getConfigPath("tls.ca"))

	logger.Info("Starting NATSConnector",
		zap.String("host", host),
	)

	opts := []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.PingInterval(time.Duration(pingInterval) * time.Second),
		nats.MaxPingsOutstanding(maxPingsOutstanding),
		nats.MaxReconnects(maxReconnects),
		//		nats.ReconnectHandler(eb.ReconnectHandler),
		//		nats.DisconnectHandler(eb.handler.Disconnect),
	}

	if len(creds) > 0 {
		opts = append(opts, nats.UserCredentials(creds))
	} else if len(nkey) > 0 {
		opt, err := nats.NkeyOptionFromSeed(nkey)
		if err != nil {
			return err
		}

		opts = append(opts, opt)
	}

	if len(tlscert) > 0 && len(tlskey) > 0 && len(tlsca) > 0 {
		opts = append(opts, nats.ClientCert(tlscert, tlskey))
		opts = append(opts, nats.RootCAs(tlsca))
	}

	nc, err := nats.Connect(host, opts...)
	if err != nil {
		return err
	}

	c.conn = nc

	// JetStream
	c.js, err = nc.JetStream()
	if err != nil {
		return err
	}

	// New-style JetStream client, used by the distributed lock / Once
	// helpers (the legacy nats.JetStreamContext above is kept for backward
	// compatibility with existing callers).
	c.jsv2, err = jetstream.New(nc)
	if err != nil {
		return err
	}

	return nil
}

func (c *NATSConnector) onStop(ctx context.Context) error {
	c.conn.Close()
	logger.Info("Stopped NATSConnector")
	return nil
}

func (c *NATSConnector) NewWorkQueueConsumer(streamName string, cfg WorkQueueConfig) (*WorkQueueConsumer, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("nats connection not initialized")
	}

	if cfg.Conn == nil {
		cfg.Conn = c.conn
	}

	if len(cfg.Subjects) == 0 {
		cfg.Subjects = []string{"work_queue"}
	}

	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = DefaultMaxConcurrent
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = DefaultAckWait
	}
	if cfg.MaxAckPending <= 0 {
		cfg.MaxAckPending = DefaultMaxAckPending
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.BatchMaxWait <= 0 {
		cfg.BatchMaxWait = DefaultBatchMaxWait
	}

	if cfg.MaxRestarts != 0 || cfg.RestartBaseDelay != 0 || cfg.RestartMaxDelay != 0 {
		if cfg.RestartBaseDelay <= 0 {
			cfg.RestartBaseDelay = DefaultRestartBaseDelay
		}
		if cfg.RestartMaxDelay <= 0 {
			cfg.RestartMaxDelay = DefaultRestartMaxDelay
		}
	}

	if cfg.Stream == nil {
		if streamName == "" {
			return nil, fmt.Errorf("stream name is required when stream info is not provided")
		}
		if c.js == nil {
			return nil, fmt.Errorf("jetstream context not initialized")
		}
		info, err := c.js.StreamInfo(streamName)
		if err != nil {
			return nil, fmt.Errorf("failed to load stream info for %s: %w", streamName, err)
		}
		cfg.Stream = info
	}

	return NewWorkQueueConsumer(cfg)
}

func (c *NATSConnector) GetConnection() *nats.Conn {
	return c.conn
}

func (c *NATSConnector) GetJetStreamContext() nats.JetStreamContext {
	return c.js
}

// GetJetStream returns the modern jetstream.JetStream handle (nats.go new
// API). Prefer this for new code that uses jetstream.KeyValueConfig /
// jetstream.StreamConfig / jetstream.ConsumerConfig. The legacy
// GetJetStreamContext() above is kept for backward compatibility.
func (c *NATSConnector) GetJetStream() jetstream.JetStream {
	return c.jsv2
}

// EnsureKV provisions a JetStream KV bucket safely across multiple
// instances. Method form of the package-level EnsureKV; supplies the
// connector's JetStream handle and logger.
func (c *NATSConnector) EnsureKV(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	if c.jsv2 == nil {
		return nil, fmt.Errorf("nats jetstream not initialized")
	}
	return EnsureKV(ctx, c.jsv2, cfg, WithEnsureLogger(c.logger))
}

// EnsureStream provisions a JetStream stream safely across multiple
// instances. Method form of the package-level EnsureStream.
func (c *NATSConnector) EnsureStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if c.jsv2 == nil {
		return nil, fmt.Errorf("nats jetstream not initialized")
	}
	return EnsureStream(ctx, c.jsv2, cfg, WithEnsureLogger(c.logger))
}

// EnsureConsumer provisions a durable consumer on the given stream safely
// across multiple instances. Method form of the package-level EnsureConsumer.
func (c *NATSConnector) EnsureConsumer(ctx context.Context, stream jetstream.Stream, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	if c.jsv2 == nil {
		return nil, fmt.Errorf("nats jetstream not initialized")
	}
	return EnsureConsumer(ctx, stream, cfg, WithEnsureLogger(c.logger))
}

// EnsureReplicaScale opportunistically promotes an existing stream toward
// `desired` replicas. Best-effort; never returns an error. See
// EnsureReplicaScale (package-level) for details.
func (c *NATSConnector) EnsureReplicaScale(ctx context.Context, streamName string, desired int) {
	if c.jsv2 == nil {
		return
	}
	EnsureReplicaScale(ctx, c.jsv2, streamName, desired, WithEnsureLogger(c.logger))
}

// NewLock creates a distributed lock backed by the NATS JetStream KV bucket
// configured via {scope}.lock.bucket. Multiple Lock instances with the same
// Key are mutually exclusive across all NATS clients connected to the same
// JetStream domain.
func (c *NATSConnector) NewLock(cfg LockConfig) (*Lock, error) {
	if c.jsv2 == nil {
		return nil, fmt.Errorf("nats jetstream not initialized")
	}
	return newLock(c.jsv2, c.lockBucketName(), c.defaultLockTTL(), c.lockReplicas(), c.logger, cfg)
}

// Once runs fn exactly once across all instances sharing the same NATS
// cluster, identified by key. The first caller to acquire the underlying
// lock runs fn; subsequent callers (current and future) skip fn and return
// nil once the first run completes successfully. If fn returns an error,
// the next caller will retry.
func (c *NATSConnector) Once(ctx context.Context, key string, fn func(context.Context) error) error {
	if c.jsv2 == nil {
		return fmt.Errorf("nats jetstream not initialized")
	}
	return runOnce(ctx, c.jsv2, c.lockBucketName(), c.defaultLockTTL(), c.lockReplicas(), c.logger, key, fn)
}

func (c *NATSConnector) lockBucketName() string {
	return viper.GetString(c.getConfigPath("lock.bucket"))
}

func (c *NATSConnector) lockReplicas() int {
	r := viper.GetInt(c.getConfigPath("lock.replicas"))
	if r <= 0 {
		r = DefaultLockReplicas
	}
	// If the connected NATS server isn't part of a cluster, JetStream
	// rejects replicas > 1. Cap to 1 in that case so single-mode
	// deployments keep working with the default config.
	if r > 1 && c.conn != nil && c.conn.ConnectedClusterName() == "" {
		return 1
	}
	return r
}

func (c *NATSConnector) defaultLockTTL() time.Duration {
	d := viper.GetDuration(c.getConfigPath("lock.defaultTTL"))
	if d <= 0 {
		return DefaultLockTTL
	}
	return d
}
