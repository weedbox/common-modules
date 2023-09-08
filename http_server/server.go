package http_server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var logger *zap.Logger

type HTTPServer struct {
	logger *zap.Logger
	server *http.Server
	router *gin.Engine
	scope  string
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var hs *HTTPServer

	return fx.Options(
		fx.Provide(func(p Params) *HTTPServer {

			logger = p.Logger.Named(scope)

			hs := &HTTPServer{
				logger: logger,
				scope:  scope,
			}

			return hs
		}),
		fx.Populate(&hs),
		fx.Invoke(func(p Params) {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: hs.onStart,
					OnStop:  hs.onStop,
				},
			)
		}),
	)
}

func (hs *HTTPServer) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", hs.scope, key)
}

func (hs *HTTPServer) onStart(ctx context.Context) error {

	port := viper.GetInt(hs.getConfigPath("port"))
	host := viper.GetString(hs.getConfigPath("host"))
	addr := fmt.Sprintf("%s:%d", host, port)

	logger.Info("Starting HTTPServer",
		zap.String("address", addr),
	)

	hs.router = gin.Default()

	hs.server = &http.Server{
		Addr:    addr,
		Handler: hs.router,
	}

	go func() {
		if err := hs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal(err.Error())
		}
	}()

	return nil
}

func (hs *HTTPServer) onStop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hs.server.Shutdown(ctx)

	logger.Info("Stopped HTTPServer")

	return nil
}

func (hs *HTTPServer) GetRouter() *gin.Engine {
	return hs.router
}
