package swagger

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/weedbox/common-modules/http_server"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	DefaultEnabled   = true
	DefaultBasePath  = "/swagger"
	DefaultTitle     = "API Reference"
	DefaultDocsPath  = "/docs"
	DefaultUIPath    = "/ui"
	DefaultSpecPath  = "/doc.json"
)

type Swagger struct {
	params Params
	logger *zap.Logger
	scope  string
}

type Params struct {
	fx.In

	Lifecycle  fx.Lifecycle
	Logger     *zap.Logger
	HTTPServer *http_server.HTTPServer
}

func Module(scope string) fx.Option {

	var s *Swagger

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *Swagger {

			s := &Swagger{
				params: p,
				logger: p.Logger.Named(scope),
				scope:  scope,
			}

			s.initDefaultConfigs()

			return s
		}),
		fx.Populate(&s),
		fx.Invoke(func(p Params) {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: s.onStart,
					OnStop:  s.onStop,
				},
			)
		}),
	)
}

func (s *Swagger) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", s.scope, key)
}

func (s *Swagger) initDefaultConfigs() {
	viper.SetDefault(s.getConfigPath("enabled"), DefaultEnabled)
	viper.SetDefault(s.getConfigPath("base_path"), DefaultBasePath)
	viper.SetDefault(s.getConfigPath("title"), DefaultTitle)
	viper.SetDefault(s.getConfigPath("docs_path"), DefaultDocsPath)
	viper.SetDefault(s.getConfigPath("ui_path"), DefaultUIPath)
	viper.SetDefault(s.getConfigPath("spec_path"), DefaultSpecPath)
}

func (s *Swagger) onStart(ctx context.Context) error {

	enabled := viper.GetBool(s.getConfigPath("enabled"))

	if !enabled {
		s.logger.Info("Swagger is disabled")
		return nil
	}

	s.logger.Info("Starting Swagger")

	basePath := viper.GetString(s.getConfigPath("base_path"))
	title := viper.GetString(s.getConfigPath("title"))
	docsPath := viper.GetString(s.getConfigPath("docs_path"))
	uiPath := viper.GetString(s.getConfigPath("ui_path"))
	specPath := viper.GetString(s.getConfigPath("spec_path"))

	router := s.params.HTTPServer.GetRouter()
	swaggerGroup := router.Group(basePath)

	// Swagger JSON spec endpoint (gin-swagger)
	swaggerGroup.GET(docsPath+"/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// Scalar API Reference UI
	swaggerGroup.GET(uiPath+"/*any", s.scalarUIHandler(title, basePath+docsPath+specPath))

	s.logger.Info("Swagger endpoints registered",
		zap.String("docs", basePath+docsPath),
		zap.String("ui", basePath+uiPath),
	)

	return nil
}

func (s *Swagger) onStop(ctx context.Context) error {
	s.logger.Info("Stopped Swagger")
	return nil
}

func (s *Swagger) scalarUIHandler(title string, specURL string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Content-Type", "text/html")
		c.String(200, fmt.Sprintf(`<!doctype html>
<html>
  <head>
    <title>%s</title>
    <meta charset="utf-8" />
    <meta
      name="viewport"
      content="width=device-width, initial-scale=1" />
  </head>

  <body>
    <div id="app"></div>

    <!-- Load the Script -->
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>

    <!-- Initialize the Scalar API Reference -->
    <script>
      Scalar.createApiReference('#app', {
        url: '%s',
        hideClientButton: true,
        showDeveloperTools: "never"
      })
    </script>
  </body>
</html>`, title, specURL))
	}
}
