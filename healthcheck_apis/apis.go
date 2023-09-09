package healthcheck_apis

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/weedbox/common-modules/daemon"
	"github.com/weedbox/common-modules/http_server"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type APIs struct {
	params Params
	logger *zap.Logger
	scope  string
}

type Params struct {
	fx.In

	Lifecycle  fx.Lifecycle
	Logger     *zap.Logger
	HTTPServer *http_server.HTTPServer
	Daemon     *daemon.Daemon
}

func Module(scope string) fx.Option {

	var a *APIs

	return fx.Options(
		fx.Provide(func(p Params) *APIs {

			a := &APIs{
				params: p,
				logger: p.Logger.Named(scope),
				scope:  scope,
			}

			return a
		}),
		fx.Populate(&a),
		fx.Invoke(func(p Params) {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: a.onStart,
					OnStop:  a.onStop,
				},
			)
		}),
	)

}

func (a *APIs) onStart(ctx context.Context) error {

	a.logger.Info("Starting healthcheck APIs")

	router := a.params.HTTPServer.GetRouter()

	router.GET("/healthz", a.healthz)
	router.GET("/ready", a.healthz)

	return nil
}

func (a *APIs) onStop(ctx context.Context) error {
	a.logger.Info("Stopped healthcheck APIs")

	return nil
}

func (a *APIs) healthz(c *gin.Context) {

	if a.params.Daemon.GetHealthStatus() != daemon.HealthStatus_Healthy {

		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "unhealthy",
		})

		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
	})
}

func (a *APIs) ready(c *gin.Context) {

	if !a.params.Daemon.Ready() {

		c.JSON(http.StatusInternalServerError, gin.H{
			"ready": false,
		})

		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ready": true,
	})
}
