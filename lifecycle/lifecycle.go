package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

// HookFunc is a callback executed by the Manager during the post-start or
// pre-stop phase.
type HookFunc func(ctx context.Context) error

// ErrPhaseCompleted is returned when a hook is registered after its phase has
// already executed.
var ErrPhaseCompleted = errors.New("lifecycle: phase already executed")

type namedHook struct {
	name string
	fn   HookFunc
}

// Manager collects hooks from other modules and runs them around the fx
// lifecycle:
//
//   - PostStart hooks run after every module's OnStart has completed, in
//     registration order. The first error aborts application startup.
//   - PreStop hooks run before every module's OnStop, in reverse
//     registration order. Errors are logged and joined, but every hook runs.
//
// This works because fx executes OnStart hooks in append order and OnStop
// hooks in reverse append order. Module() must therefore be registered after
// all business modules (by convention in afterModules(), before
// daemon.Module) so the Manager's own hook is appended last.
type Manager struct {
	logger *zap.Logger
	scope  string

	mu             sync.Mutex
	postStartHooks []namedHook
	preStopHooks   []namedHook
	postStartDone  bool
	preStopDone    bool
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var m *Manager

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *Manager {

			m := &Manager{
				logger: p.Logger.Named(scope),
				scope:  scope,
			}

			return m
		}),
		fx.Populate(&m),
		fx.Invoke(func(p Params) {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: m.onStart,
					OnStop:  m.onStop,
				},
			)
		}),
	)
}

// PostStart registers a hook to run after all modules have started. It may be
// called from a constructor, an fx.Invoke, or another module's OnStart. The
// name is used for logging only; duplicate names are allowed and hooks run in
// registration order.
//
// A nil fn is ignored. Returns ErrPhaseCompleted if the post-start phase has
// already executed.
func (m *Manager) PostStart(name string, fn HookFunc) error {

	if fn == nil {
		m.logger.Warn("Ignoring nil post-start hook", zap.String("name", name))
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.postStartDone {
		m.logger.Warn("Post-start phase already executed, hook rejected", zap.String("name", name))
		return ErrPhaseCompleted
	}

	m.postStartHooks = append(m.postStartHooks, namedHook{name: name, fn: fn})
	m.logger.Info("Registered post-start hook", zap.String("name", name))

	return nil
}

// PreStop registers a hook to run when the application begins shutting down,
// before any module's OnStop. Hooks run in reverse registration order,
// mirroring fx's OnStop semantics. The name is used for logging only;
// duplicate names are allowed.
//
// A nil fn is ignored. Returns ErrPhaseCompleted if the pre-stop phase has
// already executed.
func (m *Manager) PreStop(name string, fn HookFunc) error {

	if fn == nil {
		m.logger.Warn("Ignoring nil pre-stop hook", zap.String("name", name))
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.preStopDone {
		m.logger.Warn("Pre-stop phase already executed, hook rejected", zap.String("name", name))
		return ErrPhaseCompleted
	}

	m.preStopHooks = append(m.preStopHooks, namedHook{name: name, fn: fn})
	m.logger.Info("Registered pre-stop hook", zap.String("name", name))

	return nil
}

func (m *Manager) onStart(ctx context.Context) error {

	m.mu.Lock()
	hooks := make([]namedHook, len(m.postStartHooks))
	copy(hooks, m.postStartHooks)
	m.postStartDone = true
	m.mu.Unlock()

	m.logger.Info("Running post-start hooks", zap.Int("count", len(hooks)))

	for _, h := range hooks {

		start := time.Now()

		if err := runHook(ctx, h.fn); err != nil {
			m.logger.Error("Post-start hook failed",
				zap.String("name", h.name),
				zap.Error(err),
			)
			return fmt.Errorf("lifecycle: post-start hook %q: %w", h.name, err)
		}

		m.logger.Info("Post-start hook completed",
			zap.String("name", h.name),
			zap.Duration("duration", time.Since(start)),
		)
	}

	return nil
}

func (m *Manager) onStop(ctx context.Context) error {

	m.mu.Lock()
	hooks := make([]namedHook, len(m.preStopHooks))
	copy(hooks, m.preStopHooks)
	m.preStopDone = true
	m.mu.Unlock()

	m.logger.Info("Running pre-stop hooks", zap.Int("count", len(hooks)))

	var errs []error

	// Reverse registration order, mirroring fx's OnStop semantics. Unlike the
	// post-start phase, a failing hook does not stop the others: shutdown
	// should always make as much progress as possible.
	for i := len(hooks) - 1; i >= 0; i-- {

		h := hooks[i]
		start := time.Now()

		if err := runHook(ctx, h.fn); err != nil {
			m.logger.Error("Pre-stop hook failed",
				zap.String("name", h.name),
				zap.Error(err),
			)
			errs = append(errs, fmt.Errorf("lifecycle: pre-stop hook %q: %w", h.name, err))
			continue
		}

		m.logger.Info("Pre-stop hook completed",
			zap.String("name", h.name),
			zap.Duration("duration", time.Since(start)),
		)
	}

	return errors.Join(errs...)
}

// runHook invokes fn, converting a panic into an error so a misbehaving hook
// fails its phase cleanly instead of crashing the process mid-lifecycle.
func runHook(ctx context.Context, fn HookFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn(ctx)
}
