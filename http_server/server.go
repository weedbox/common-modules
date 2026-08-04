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

	// DefaultExposeHeaders is empty: a response header is invisible to
	// browser JavaScript unless it is named here, and which ones a consumer
	// needs is entirely application-specific. Anything serving ranged reads
	// to a browser (an S3-compatible surface, a media endpoint) has to
	// expose at least "Content-Range,Content-Length,ETag,Accept-Ranges" —
	// without them the client can see the 206 but not where in the object
	// it landed, so range-based readers fail in ways that look like a
	// server bug rather than a CORS one.
	DefaultExposeHeaders = ""

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

// appendCSV appends the comma-separated entries of csv to dst, trimming
// surrounding whitespace and dropping empty entries — so a natural
// "GET, POST" or a trailing comma cannot turn into a " POST" or "" header
// name that silently never matches.
func appendCSV(dst []string, csv string) []string {
	for _, v := range strings.Split(csv, ",") {
		if v = strings.TrimSpace(v); v != "" {
			dst = append(dst, v)
		}
	}
	return dst
}

func (hs *HTTPServer) initDefaultConfigs() {
	viper.SetDefault(hs.getConfigPath("host"), DefaultHost)
	viper.SetDefault(hs.getConfigPath("port"), DefaultPort)
	viper.SetDefault(hs.getConfigPath("mode"), DefaultMode)

	viper.SetDefault(hs.getConfigPath("allow_origins"), DefaultOrigins)
	viper.SetDefault(hs.getConfigPath("allow_methods"), DefaultMethods)
	viper.SetDefault(hs.getConfigPath("allow_headers"), DefaultHeaders)
	viper.SetDefault(hs.getConfigPath("expose_headers"), DefaultExposeHeaders)

	viper.SetDefault(hs.getConfigPath("read_header_timeout"), DefaultReadHeaderTimeout)
	viper.SetDefault(hs.getConfigPath("idle_timeout"), DefaultIdleTimeout)

}

func (hs *HTTPServer) onStart(ctx context.Context) error {

	port := viper.GetInt(hs.getConfigPath("port"))
	host := viper.GetString(hs.getConfigPath("host"))
	addr := fmt.Sprintf("%s:%d", host, port)

	// NOTE: the "mode" key is no longer read. It used to gate allow_headers
	// (see the Cors block below); the default is still registered so that
	// existing config files carrying it keep loading without complaint.

	logLevel := viper.GetString(hs.getConfigPath("loglevel"))

	allowOrigins := viper.GetString(hs.getConfigPath("allow_origins"))
	allowMethods := viper.GetString(hs.getConfigPath("allow_methods"))
	allowHeaders := viper.GetString(hs.getConfigPath("allow_headers"))
	exposeHeaders := viper.GetString(hs.getConfigPath("expose_headers"))

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
		// "prod" drops gin's per-request access log, which is the only reason
		// to pick it over "release" — services at production traffic already
		// log requests through their own middleware. It must NOT also drop
		// Recovery: gin.New() installs neither, so a panic in any handler
		// unwinds past gin into net/http, which aborts the connection without
		// a response. The client sees a broken connection rather than a 500,
		// and the panic lands on the stdlib error log instead of the
		// structured one. Keep the access log off; keep the safety net on.
		hs.router = gin.New()
		hs.router.Use(gin.Recovery())
	}

	// Setup Cors. Each setting ADDS to what cors.DefaultConfig already
	// permits (Origin/Content-Length/Content-Type, and the usual methods)
	// rather than replacing it.
	corsConfig := cors.DefaultConfig()

	if allowOrigins != "" {
		corsConfig.AllowOrigins = appendCSV(corsConfig.AllowOrigins, allowOrigins)
	} else {
		corsConfig.AllowAllOrigins = true
	}
	corsConfig.AllowMethods = appendCSV(corsConfig.AllowMethods, allowMethods)

	// allow_headers applies in every mode. It used to be gated on
	// mode == "test", which silently reduced production to the three
	// headers cors.DefaultConfig ships with — so a configured
	// "Authorization" (the default!) never reached the preflight response
	// and any authenticated cross-origin request failed. Widening the
	// preflight allow-list cannot break an existing caller: it only ever
	// permits request headers that were previously rejected.
	corsConfig.AllowHeaders = appendCSV(corsConfig.AllowHeaders, allowHeaders)
	corsConfig.ExposeHeaders = appendCSV(corsConfig.ExposeHeaders, exposeHeaders)

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
