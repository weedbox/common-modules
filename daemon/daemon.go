package daemon

import (
	"context"
	"fmt"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

type HealthStatus int32

const (
	HealthStatus_Healthy HealthStatus = iota
	HealthStatus_Unhealthy
)

var logger *zap.Logger

type Daemon struct {
	logger       *zap.Logger
	scope        string
	isReady      bool
	healthStatus HealthStatus
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var d *Daemon

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *Daemon {

			logger = p.Logger.Named(scope)

			d := &Daemon{
				logger:       logger,
				scope:        scope,
				isReady:      false,
				healthStatus: HealthStatus_Healthy,
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
	d.isReady = true

	return nil
}

func (d *Daemon) onStop(ctx context.Context) error {

	logger.Info("Stopped daemon")
	d.isReady = false

	return nil
}

func (d *Daemon) Ready() bool {
	return d.isReady
}

func (d *Daemon) GetHealthStatus() HealthStatus {
	return d.healthStatus
}
