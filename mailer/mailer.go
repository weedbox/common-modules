package mailer

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"gopkg.in/gomail.v2"
)

const (
	DefaultHost     = "0.0.0.0"
	DefaultPort     = 25
	DefaultUsername = ""
	DefaultPassword = ""
	DefaultTLS      = false
)

var logger *zap.Logger

type Mailer struct {
	logger *zap.Logger
	dialer *gomail.Dialer
	scope  string
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var m *Mailer

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *Mailer {

			logger = p.Logger.Named(scope)

			m := &Mailer{
				logger: logger,
				scope:  scope,
			}

			m.initDefaultConfigs()

			return m
		}),
		fx.Populate(&m),
		fx.Invoke(func(p Params) *Mailer {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: m.onStart,
					OnStop:  m.onStop,
				},
			)

			return m
		}),
	)

}

func (m *Mailer) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", m.scope, key)
}

func (m *Mailer) initDefaultConfigs() {
	viper.SetDefault(m.getConfigPath("host"), DefaultHost)
	viper.SetDefault(m.getConfigPath("port"), DefaultPort)
	viper.SetDefault(m.getConfigPath("tls"), DefaultTLS)
	viper.SetDefault(m.getConfigPath("username"), DefaultUsername)
	viper.SetDefault(m.getConfigPath("password"), DefaultPassword)
}

func (m *Mailer) onStart(ctx context.Context) error {

	logger.Info("Starting mailer")

	host := viper.GetString(m.getConfigPath("host"))
	port := viper.GetInt(m.getConfigPath("port"))
	enabledTLS := viper.GetBool(m.getConfigPath("tls"))
	username := viper.GetString(m.getConfigPath("username"))
	password := viper.GetString(m.getConfigPath("password"))

	m.dialer = gomail.NewDialer(host, port, username, password)

	if enabledTLS {
		m.dialer.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	return nil
}

func (m *Mailer) onStop(ctx context.Context) error {

	logger.Info("Stopped mailer")

	return nil
}

func (m *Mailer) NewMessage() *gomail.Message {
	return gomail.NewMessage()
}

func (m *Mailer) Send(msg *gomail.Message) error {
	return m.dialer.DialAndSend(msg)
}
