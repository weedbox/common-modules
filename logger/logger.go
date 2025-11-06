package logger

import (
	"os"
	"strings"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

type Params struct {
	fx.In
}

func Module() fx.Option {
	return fx.Options(
		fx.Provide(SetupLogger),
		fx.WithLogger(func(l *zap.Logger) fxevent.Logger {
			return &fxevent.ZapLogger{Logger: l}
		}),
	)
}

func NewCustomEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05"),
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

func SetupLogger() *zap.Logger {
	debugMode := isDebugMode()
	debugLevel := setupLevel(debugMode)

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(NewCustomEncoderConfig()),
		zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout)),
		debugLevel,
	)

	if debugMode {
		logger = zap.New(core, zap.AddCaller(), zap.Development())
	} else {
		logger = zap.New(core)
	}

	zap.ReplaceGlobals(logger)

	logger.Info("Logger initialized", zap.String("level", debugLevel.String()))

	return logger
}

func GetLogger() *zap.Logger {
	return logger
}

func setupLevel(debugMode bool) zap.AtomicLevel {
	if !debugMode {
		return zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	debugLevel := zap.DebugLevel
	switch strings.ToLower(os.Getenv("DEBUG_LEVEL")) {
	case zap.InfoLevel.String():
		debugLevel = zap.InfoLevel
	case zap.WarnLevel.String():
		debugLevel = zap.WarnLevel
	case zap.ErrorLevel.String():
		debugLevel = zap.ErrorLevel
	case zap.DPanicLevel.String():
		debugLevel = zap.DPanicLevel
	case zap.PanicLevel.String():
		debugLevel = zap.PanicLevel
	case zap.FatalLevel.String():
		debugLevel = zap.FatalLevel
	}

	return zap.NewAtomicLevelAt(debugLevel)
}

func isDebugMode() bool {
	switch strings.ToLower(os.Getenv("DEBUG_MODE")) {
	case "debug", "true":
		return true
	default:
		return false
	}
}
