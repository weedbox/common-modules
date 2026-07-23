package lifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

// newTestManager constructs a Manager without going through fx, for testing
// registration semantics in isolation.
func newTestManager() *Manager {
	return &Manager{
		logger: zap.NewNop(),
		scope:  "lifecycle",
	}
}

// bizModule simulates a business module registered before Module(): it
// appends its own fx hooks and records lifecycle events into order. It must
// be an fx.Module (like every weedbox module) — fx runs module invokes before
// root-level invokes, so a bare fx.Invoke would not reflect real hook order.
func bizModule(name string, order *[]string, register func(m *Manager)) fx.Option {
	return fx.Module(name, fx.Invoke(func(lc fx.Lifecycle, m *Manager) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				*order = append(*order, name+".OnStart")
				if register != nil {
					register(m)
				}
				return nil
			},
			OnStop: func(ctx context.Context) error {
				*order = append(*order, name+".OnStop")
				return nil
			},
		})
	}))
}

func startStop(t *testing.T, app *fx.App) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatalf("fx start: %v", err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatalf("fx stop: %v", err)
	}
}

func TestPostStart_RunsAfterAllModulesStarted(t *testing.T) {

	var order []string

	app := fx.New(
		fx.NopLogger,
		fx.Provide(zap.NewNop),
		bizModule("a", &order, func(m *Manager) {
			if err := m.PostStart("a.task", func(ctx context.Context) error {
				order = append(order, "postStart.a.task")
				return nil
			}); err != nil {
				t.Errorf("PostStart: %v", err)
			}
		}),
		bizModule("b", &order, nil),
		Module("lifecycle"),
	)

	startStop(t, app)

	want := []string{"a.OnStart", "b.OnStart", "postStart.a.task", "b.OnStop", "a.OnStop"}
	if len(order) != len(want) {
		t.Fatalf("unexpected events: %v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("event %d = %q, want %q (all: %v)", i, order[i], want[i], order)
		}
	}
}

func TestPostStart_RunsInRegistrationOrder(t *testing.T) {

	var order []string
	m := newTestManager()

	for _, name := range []string{"first", "second", "third"} {
		if err := m.PostStart(name, func(ctx context.Context) error {
			order = append(order, name)
			return nil
		}); err != nil {
			t.Fatalf("PostStart(%s): %v", name, err)
		}
	}

	if err := m.onStart(context.Background()); err != nil {
		t.Fatalf("onStart: %v", err)
	}

	if strings.Join(order, ",") != "first,second,third" {
		t.Fatalf("unexpected order: %v", order)
	}
}

func TestPostStart_ErrorAbortsStartupAndRollsBack(t *testing.T) {

	var order []string
	boom := errors.New("boom")

	app := fx.New(
		fx.NopLogger,
		fx.Provide(zap.NewNop),
		bizModule("a", &order, func(m *Manager) {
			m.PostStart("a.fail", func(ctx context.Context) error {
				return boom
			})
		}),
		Module("lifecycle"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := app.Start(ctx)
	if err == nil {
		t.Fatal("expected start to fail")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped boom error, got: %v", err)
	}

	// fx rolls back already-started hooks, so the business module's OnStop
	// must have run.
	found := false
	for _, e := range order {
		if e == "a.OnStop" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a.OnStop during rollback, got: %v", order)
	}
}

func TestPreStop_RunsBeforeModulesStopInReverseOrder(t *testing.T) {

	var order []string

	app := fx.New(
		fx.NopLogger,
		fx.Provide(zap.NewNop),
		bizModule("a", &order, func(m *Manager) {
			m.PreStop("drain-1", func(ctx context.Context) error {
				order = append(order, "preStop.drain-1")
				return nil
			})
			m.PreStop("drain-2", func(ctx context.Context) error {
				order = append(order, "preStop.drain-2")
				return nil
			})
		}),
		Module("lifecycle"),
	)

	startStop(t, app)

	want := []string{"a.OnStart", "preStop.drain-2", "preStop.drain-1", "a.OnStop"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected events: %v, want %v", order, want)
	}
}

func TestPreStop_ErrorDoesNotStopOtherHooks(t *testing.T) {

	var order []string
	m := newTestManager()

	m.PreStop("ok", func(ctx context.Context) error {
		order = append(order, "ok")
		return nil
	})
	m.PreStop("fail", func(ctx context.Context) error {
		order = append(order, "fail")
		return errors.New("boom")
	})

	err := m.onStop(context.Background())
	if err == nil {
		t.Fatal("expected joined error from onStop")
	}

	// Reverse order: "fail" runs first and errors, "ok" must still run.
	if strings.Join(order, ",") != "fail,ok" {
		t.Fatalf("unexpected order: %v", order)
	}
}

func TestRegistrationAfterPhaseRejected(t *testing.T) {

	m := newTestManager()

	if err := m.onStart(context.Background()); err != nil {
		t.Fatalf("onStart: %v", err)
	}
	if err := m.PostStart("late", func(ctx context.Context) error { return nil }); !errors.Is(err, ErrPhaseCompleted) {
		t.Fatalf("expected ErrPhaseCompleted, got: %v", err)
	}

	if err := m.onStop(context.Background()); err != nil {
		t.Fatalf("onStop: %v", err)
	}
	if err := m.PreStop("late", func(ctx context.Context) error { return nil }); !errors.Is(err, ErrPhaseCompleted) {
		t.Fatalf("expected ErrPhaseCompleted, got: %v", err)
	}
}

func TestNilHookIgnored(t *testing.T) {

	m := newTestManager()

	if err := m.PostStart("nil", nil); err != nil {
		t.Fatalf("PostStart(nil): %v", err)
	}
	if err := m.PreStop("nil", nil); err != nil {
		t.Fatalf("PreStop(nil): %v", err)
	}
	if len(m.postStartHooks) != 0 || len(m.preStopHooks) != 0 {
		t.Fatal("nil hooks must not be registered")
	}
}

func TestPostStart_PanicConvertedToError(t *testing.T) {

	m := newTestManager()

	m.PostStart("panics", func(ctx context.Context) error {
		panic("kaboom")
	})

	err := m.onStart(context.Background())
	if err == nil || !strings.Contains(err.Error(), "kaboom") {
		t.Fatalf("expected panic converted to error, got: %v", err)
	}
}
