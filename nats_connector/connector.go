package nats_connector

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
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
)

type NATSConnector struct {
	logger *zap.Logger
	conn   *nats.Conn
	js     nats.JetStreamContext
	scope  string
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	return fx.Options(

		fx.Invoke(func(p Params) *NATSConnector {

			logger = p.Logger.Named(scope)

			c := &NATSConnector{
				logger: logger,
				scope:  scope,
			}

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

func (c *NATSConnector) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", c.scope, key)
}

func (c *NATSConnector) onStart(ctx context.Context) error {

	// default settings
	viper.SetDefault(c.getConfigPath("host"), DefaultHost)
	viper.SetDefault(c.getConfigPath("pingInterval"), DefaultPingInterval)
	viper.SetDefault(c.getConfigPath("maxPingsOutstanding"), DefaultMaxPingsOutstanding)
	viper.SetDefault(c.getConfigPath("maxReconnects"), DefaultMaxReconnects)

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

	return nil
}

func (c *NATSConnector) onStop(ctx context.Context) error {
	c.conn.Close()
	logger.Info("Stopped NATSConnector")
	return nil
}

func (c *NATSConnector) GetConnection() *nats.Conn {
	return c.conn
}

func (c *NATSConnector) GetJetStreamContext() nats.JetStreamContext {
	return c.js
}
