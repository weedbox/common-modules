package database_test

import (
	"context"
	"testing"

	"github.com/weedbox/common-modules/database"
	"github.com/weedbox/common-modules/postgres_connector"
	"github.com/weedbox/common-modules/sqlite_connector"
	"github.com/weedbox/weedbox/fxmodule"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// nopLifecycle satisfies fx.Lifecycle but never runs the hooks. Used so
// fx.New can complete without OnStart attempting to open a real DB.
type nopLifecycle struct{}

func (nopLifecycle) Append(fx.Hook)                    {}
func (nopLifecycle) Start(ctx context.Context) error   { return nil }
func (nopLifecycle) Stop(ctx context.Context) error    { return nil }

// supplyZapLogger provides a no-op zap.Logger so connectors can name their
// loggers off the root.
func supplyZapLogger() fx.Option {
	return fx.Provide(zap.NewNop)
}

// buildApp constructs an fx.App with both connectors loaded and a
// no-op lifecycle so OnStart never runs.
func buildApp(t *testing.T, populate any) *fx.App {
	t.Helper()
	fxmodule.ResetClaim[database.DatabaseConnector]()
	t.Cleanup(func() { fxmodule.ResetClaim[database.DatabaseConnector]() })
	return fx.New(
		supplyZapLogger(),
		// Replace fx's real Lifecycle with a no-op so OnStart hooks never fire.
		fx.Decorate(func(fx.Lifecycle) fx.Lifecycle { return nopLifecycle{} }),
		sqlite_connector.Module("sqlite"),
		postgres_connector.Module("postgres"),
		fx.Invoke(populate),
		fx.NopLogger,
	)
}

func TestMultiLoad_NamedInstancesResolve(t *testing.T) {
	type params struct {
		fx.In
		Sqlite database.DatabaseConnector `name:"sqlite"`
		PG     database.DatabaseConnector `name:"postgres"`
	}

	var got params
	app := buildApp(t, func(p params) { got = p })
	if err := app.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	if got.Sqlite == nil {
		t.Error("named sqlite not resolved")
	}
	if got.PG == nil {
		t.Error("named postgres not resolved")
	}
	if got.Sqlite == got.PG {
		t.Error("named instances should be distinct objects")
	}
}

func TestMultiLoad_UnnamedDefaultIsFirstRegistered(t *testing.T) {
	type params struct {
		fx.In
		Default database.DatabaseConnector
		Sqlite  database.DatabaseConnector `name:"sqlite"`
	}

	var got params
	app := buildApp(t, func(p params) { got = p })
	if err := app.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	if got.Default == nil {
		t.Fatal("unnamed default not resolved")
	}
	if got.Default != got.Sqlite {
		t.Errorf("unnamed default should be the first-loaded (sqlite) instance")
	}
}

func TestMultiLoad_OrderControlsDefault(t *testing.T) {
	type params struct {
		fx.In
		Default database.DatabaseConnector
		PG      database.DatabaseConnector `name:"postgres"`
	}

	fxmodule.ResetClaim[database.DatabaseConnector]()
	t.Cleanup(func() { fxmodule.ResetClaim[database.DatabaseConnector]() })

	var got params
	app := fx.New(
		supplyZapLogger(),
		fx.Decorate(func(fx.Lifecycle) fx.Lifecycle { return nopLifecycle{} }),
		// Loading postgres first should make it the unnamed default.
		postgres_connector.Module("postgres"),
		sqlite_connector.Module("sqlite"),
		fx.Invoke(func(p params) { got = p }),
		fx.NopLogger,
	)
	if err := app.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	if got.Default == nil {
		t.Fatal("unnamed default not resolved")
	}
	if got.Default != got.PG {
		t.Error("unnamed default should be postgres when loaded first")
	}
}

func TestSingleLoad_UnnamedDefaultBackwardsCompat(t *testing.T) {
	type params struct {
		fx.In
		Default database.DatabaseConnector
	}

	fxmodule.ResetClaim[database.DatabaseConnector]()
	t.Cleanup(func() { fxmodule.ResetClaim[database.DatabaseConnector]() })

	var got params
	app := fx.New(
		supplyZapLogger(),
		fx.Decorate(func(fx.Lifecycle) fx.Lifecycle { return nopLifecycle{} }),
		sqlite_connector.Module("sqlite"),
		fx.Invoke(func(p params) { got = p }),
		fx.NopLogger,
	)
	if err := app.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	if got.Default == nil {
		t.Fatal("single-load with no tag must resolve unnamed default")
	}
}
