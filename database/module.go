package database

import (
	"github.com/weedbox/weedbox/fxmodule"
	"go.uber.org/fx"
)

// Module wires a DatabaseConnector constructor into fx with the standard
// connector pattern: always register as `name:"<scope>"`, and let the
// first caller in the process claim the unnamed default slot. Existing
// single-load consumers that inject DatabaseConnector without a tag
// continue to work with zero changes.
//
// Intended for use from a connector package's Module(scope) factory —
// the application composition root still writes `<pkg>.Module("<scope>")`.
//
// In tests that construct multiple fx.Apps in the same process, call
// fxmodule.ResetClaim[DatabaseConnector]() between apps.
func Module(scope string, ctor any) fx.Option {
	return fxmodule.InterfaceModule[DatabaseConnector](scope, ctor)
}
