package http_server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	DefaultHost    = "0.0.0.0"
	DefaultPort    = 80
	DefaultHeaders = "Authorization,Accept"
	DefaultMethods = ""
	DefaultOrigins = ""
	DefaultMode    = "test" // e.g. test, prod

	// DefaultReadHeaderTimeout bounds how long a connection may take to
	// deliver its request headers. With no bound, a client that opens a
	// socket and never finishes the header block pins a server goroutine and
	// a file descriptor for the life of the process — the Slowloris shape,
	// but just as easily a half-dead load balancer or a NAT that dropped the
	// connection without a FIN. It covers the header phase only, so slow
	// bodies (large multipart uploads) are unaffected.
	DefaultReadHeaderTimeout = 20 * time.Second

	// DefaultIdleTimeout bounds how long a keep-alive connection may sit
	// between requests. net/http falls back to ReadTimeout when IdleTimeout
	// is zero, and ReadTimeout is deliberately left unset here (see
	// onStart), so leaving both unset means idle connections are never
	// reaped.
	DefaultIdleTimeout = 120 * time.Second
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
	viper.SetDefault(hs.getConfigPath("mode"), DefaultMode)

	viper.SetDefault(hs.getConfigPath("allow_origins"), DefaultOrigins)
	viper.SetDefault(hs.getConfigPath("allow_methods"), DefaultMethods)
	viper.SetDefault(hs.getConfigPath("allow_headers"), DefaultHeaders)

	viper.SetDefault(hs.getConfigPath("read_header_timeout"), DefaultReadHeaderTimeout)
	viper.SetDefault(hs.getConfigPath("idle_timeout"), DefaultIdleTimeout)

}

func (hs *HTTPServer) onStart(ctx context.Context) error {

	port := viper.GetInt(hs.getConfigPath("port"))
	host := viper.GetString(hs.getConfigPath("host"))
	addr := fmt.Sprintf("%s:%d", host, port)

	mode := viper.GetString(hs.getConfigPath("mode"))

	logLevel := viper.GetString(hs.getConfigPath("loglevel"))

	allowOrigins := viper.GetString(hs.getConfigPath("allow_origins"))
	allowMethods := viper.GetString(hs.getConfigPath("allow_methods"))
	allowHeaders := viper.GetString(hs.getConfigPath("allow_headers"))

	readHeaderTimeout := viper.GetDuration(hs.getConfigPath("read_header_timeout"))
	idleTimeout := viper.GetDuration(hs.getConfigPath("idle_timeout"))

	logger.Info("Starting HTTPServer",
		zap.String("address", addr),
		zap.Duration("read_header_timeout", readHeaderTimeout),
		zap.Duration("idle_timeout", idleTimeout),
	)

	if logLevel == "test" {
		gin.SetMode(gin.TestMode)
	}

	if logLevel == "release" || logLevel == "prod" {
		gin.SetMode(gin.ReleaseMode)
	}

	hs.router = gin.Default()

	if logLevel == "prod" {
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

	// Add default or custom headers if in testing mode
	if mode == "test" {
		allows := strings.Split(allowHeaders, ",")
		for _, a := range allows {
			corsConfig.AllowHeaders = append(corsConfig.AllowHeaders, a)
		}
	}

	hs.router.Use(cors.New(corsConfig))

	// ReadTimeout and WriteTimeout stay unset deliberately. Both are
	// whole-request deadlines measured from the moment the connection is
	// accepted, and consumers of this module serve responses that
	// legitimately outlive any value we could pick here (SSE / streaming
	// endpoints) as well as request bodies that do the same (large multipart
	// uploads). ReadHeaderTimeout and IdleTimeout bound the two phases where
	// no legitimate client is ever slow, which is what stops an abandoned or
	// deliberately-stalled connection from being free to hold.
	//
	// Either can be set to 0 in config to restore the unbounded behaviour.
	hs.server = &http.Server{
		Addr:              addr,
		Handler:           hs.router,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
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
