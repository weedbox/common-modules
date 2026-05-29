package daemon

import (
	"context"
	"fmt"
	"sync"

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
	mu           sync.RWMutex
	isReady      bool
	healthStatus HealthStatus
	readyCheckers map[string]func() bool
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
				logger:        logger,
				scope:         scope,
				isReady:       false,
				healthStatus:  HealthStatus_Healthy,
				readyCheckers: make(map[string]func() bool),
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

	d.mu.Lock()
	d.isReady = true
	d.mu.Unlock()

	return nil
}

func (d *Daemon) onStop(ctx context.Context) error {

	logger.Info("Stopped daemon")

	d.mu.Lock()
	d.isReady = false
	d.mu.Unlock()

	return nil
}

// RegisterReadyChecker registers a named readiness predicate that participates
// in Ready(). The daemon is considered ready only when isReady is true AND
// every registered checker returns true.
//
// Passing the same name twice replaces the previous checker. A nil fn is
// ignored; use UnregisterReadyChecker to remove a checker.
func (d *Daemon) RegisterReadyChecker(name string, fn func() bool) {

	if fn == nil {
		if d.logger != nil {
			d.logger.Warn("Ignoring nil ready checker", zap.String("name", name))
		}
		return
	}

	d.mu.Lock()
	d.readyCheckers[name] = fn
	d.mu.Unlock()

	if d.logger != nil {
		d.logger.Info("Registered ready checker", zap.String("name", name))
	}
}

// UnregisterReadyChecker removes the ready checker registered under name.
// It is a no-op if no checker with that name is registered.
func (d *Daemon) UnregisterReadyChecker(name string) {

	d.mu.Lock()
	_, existed := d.readyCheckers[name]
	delete(d.readyCheckers, name)
	d.mu.Unlock()

	if existed && d.logger != nil {
		d.logger.Info("Unregistered ready checker", zap.String("name", name))
	}
}

// Ready reports whether the daemon has started and every registered ready
// checker currently reports true.
func (d *Daemon) Ready() bool {

	d.mu.RLock()
	if !d.isReady {
		d.mu.RUnlock()
		return false
	}

	checkers := make([]struct {
		name string
		fn   func() bool
	}, 0, len(d.readyCheckers))
	for name, fn := range d.readyCheckers {
		checkers = append(checkers, struct {
			name string
			fn   func() bool
		}{name, fn})
	}
	d.mu.RUnlock()

	for _, c := range checkers {
		if !c.fn() {
			return false
		}
	}
	return true
}

func (d *Daemon) GetHealthStatus() HealthStatus {
	return d.healthStatus
}
