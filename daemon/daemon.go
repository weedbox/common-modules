package daemon

import (
	"context"
	"fmt"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

var logger *zap.Logger

type Daemon struct {
	logger *zap.Logger
	scope  string
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var d *Daemon

	return fx.Options(
		fx.Provide(func(p Params) *Daemon {

			logger = p.Logger.Named(scope)

			d := &Daemon{
				logger: logger,
				scope:  scope,
			}

			return d
		}),
		fx.Populate(&d),
		fx.Invoke(func(p Params) *Daemon {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: d.onStart,
					OnStop:  d.onStop,
				},
			)

			return d
		}),
	)

}

func (d *Daemon) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", d.scope, key)
}

func (d *Daemon) onStart(ctx context.Context) error {

	logger.Info("Starting daemon")

	return nil
}

func (d *Daemon) onStop(ctx context.Context) error {

	logger.Info("Stopped daemon")

	return nil
}
