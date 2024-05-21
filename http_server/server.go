package http_server

import (
	"context"
	"fmt"
	"net/http"
	"time"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	DefaultHost = "0.0.0.0"
	DefaultPort = 80
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

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *HTTPServer {

			logger = p.Logger.Named(scope)

			hs := &HTTPServer{
				logger: logger,
				scope:  scope,
			}

			hs.initDefaultConfigs()

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

func (hs *HTTPServer) initDefaultConfigs() {
	viper.SetDefault(hs.getConfigPath("host"), DefaultHost)
	viper.SetDefault(hs.getConfigPath("port"), DefaultPort)
}

func (hs *HTTPServer) onStart(ctx context.Context) error {

	port := viper.GetInt(hs.getConfigPath("port"))
	host := viper.GetString(hs.getConfigPath("host"))
	addr := fmt.Sprintf("%s:%d", host, port)

	logLevel := viper.GetString(hs.getConfigPath("loglevel"))

	allowOrigins := viper.GetString(hs.getConfigPath("allow_origins"))
	allowMethods := viper.GetString(hs.getConfigPath("allow_methods"))
	allowHeaders := viper.GetString(hs.getConfigPath("allow_headers"))

	logger.Info("Starting HTTPServer",
		zap.String("address", addr),
	)

	if logLevel == "test" {
		gin.SetMode(gin.TestMode)
	}

	if logLevel == "release" || logLevel = "prod" {
		gin.SetMode(gin.ReleaseMode)
	}

	hs.router = gin.Default()

	if logLevel = "prod" {
		hs.router = gin.New()
	}

	// Setup Cors
	corsConfig := cors.DefaultConfig()

	if allowOrigins != "" {
		allows := strings.Split(allowOrigins, ",")
		for _, a := range allows {
			corsConfig.AllowOrigins = append(corsConfig.AllowOrigins, a)
		}
	} else {
		corsConfig.AllowAllOrigins = true
	}
	if allowMethods != "" {
		allows := strings.Split(allowMethods, ",")
		for _, a := range allows {
			corsConfig.AllowMethods = append(corsConfig.AllowMethods, a)
		}
	}
	if allowHeaders != "" {
		allows := strings.Split(allowHeaders, ",")
		for _, a := range allows {
			corsConfig.AllowHeaders = append(corsConfig.AllowHeaders, a)
		}
	}
	hs.router.Use(cors.New(corsConfig))

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
