package webui

import (
	"errors"
	"io/fs"
	"net/http"

	"github.com/bitmagnet-io/bitmagnet/internal/httpserver"
	"github.com/bitmagnet-io/bitmagnet/webui"
	webuireact "github.com/bitmagnet-io/bitmagnet/webui-react"
	"github.com/gin-gonic/gin"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Params struct {
	fx.In
	Config Config
	Logger *zap.SugaredLogger
}

type Result struct {
	fx.Out
	Option httpserver.Option `group:"http_server_options"`
}

func New(p Params) Result {
	return Result{
		Option: &builder{
			config: p.Config,
			logger: p.Logger.Named("webui"),
		},
	}
}

type builder struct {
	config Config
	logger *zap.SugaredLogger
}

func (*builder) Key() string {
	return "webui"
}

func (b *builder) Apply(e *gin.Engine) error {
	defaultFrontend := resolveFrontend("", "", b.config, webuireact.Enabled)

	if defaultFrontend.warnReactDisabled {
		b.logger.Warn(
			"WEBUI_DEFAULT_FRONTEND=react but the binary was built without -tags webuireact; " +
				"serving angular",
		)
	}

	webuiFS := webui.StaticFS()

	appRoot, appRootErr := fs.Sub(webuiFS, "dist/bitmagnet/browser")
	if appRootErr != nil {
		b.logger.Errorf(
			"the webui app root directory is missing; run `npm run build` within the `webui` folder: %v",
			appRootErr)

		return nil
	}

	e.StaticFS("/webui", wrappedFs{http.FS(appRoot)})
	e.GET("/", func(c *gin.Context) {
		cookieFrontend, _ := c.Cookie(frontendCookieName)
		selection := resolveFrontend(c.Query("frontend"), cookieFrontend, b.config, webuireact.Enabled)

		if selection.warnReactDisabled && selection.source != frontendSourceConfig {
			b.logger.Warnf(
				"requested frontend %q but the binary was built without -tags webuireact; serving angular",
				selection.requestedFrontend,
			)
		}

		if selection.setCookie {
			http.SetCookie(c.Writer, &http.Cookie{
				Name:     frontendCookieName,
				Value:    selection.cookieValue,
				Path:     "/",
				MaxAge:   frontendCookieMaxAge,
				HttpOnly: false,
				SameSite: http.SameSiteLaxMode,
			})
		}

		c.Redirect(http.StatusMovedPermanently, selection.redirectPath)
	})

	if webuireact.Enabled {
		appHandler := gin.WrapH(webuireact.Handler(webuireact.FS))
		e.GET("/app", appHandler)
		e.HEAD("/app", appHandler)
		e.GET("/app/*filepath", appHandler)
		e.HEAD("/app/*filepath", appHandler)
	}

	return nil
}

type defaultFrontendSelection struct {
	frontend          string
	redirectPath      string
	setCookie         bool
	cookieValue       string
	warnReactDisabled bool
	requestedFrontend string
	source            frontendSource
}

type frontendSource string

const (
	frontendSourceConfig frontendSource = "config"
	frontendSourceCookie frontendSource = "cookie"
	frontendSourceQuery  frontendSource = "query"
)

func resolveFrontend(
	queryFrontend string,
	cookieFrontend string,
	cfg Config,
	reactEnabled bool,
) defaultFrontendSelection {
	selectedFrontend := validFrontendOrDefault(cfg.DefaultFrontend)
	source := frontendSourceConfig
	setCookie := false
	cookieValue := ""

	if isValidFrontend(queryFrontend) {
		selectedFrontend = queryFrontend
		source = frontendSourceQuery
		setCookie = true
		cookieValue = queryFrontend
	} else if isValidFrontend(cookieFrontend) {
		selectedFrontend = cookieFrontend
		source = frontendSourceCookie
	}

	resolvedFrontend := selectedFrontend
	warnReactDisabled := selectedFrontend == defaultFrontendReact && !reactEnabled

	if warnReactDisabled {
		resolvedFrontend = defaultFrontendAngular
	}

	return defaultFrontendSelection{
		frontend:          resolvedFrontend,
		redirectPath:      redirectPathForFrontend(resolvedFrontend),
		setCookie:         setCookie,
		cookieValue:       cookieValue,
		warnReactDisabled: warnReactDisabled,
		requestedFrontend: selectedFrontend,
		source:            source,
	}
}

const (
	defaultFrontendAngular = "angular"
	defaultFrontendReact   = "react"
	frontendCookieMaxAge   = 365 * 24 * 60 * 60
	frontendCookieName     = "bitmagnet-frontend"
)

func validFrontendOrDefault(frontend string) string {
	if isValidFrontend(frontend) {
		return frontend
	}

	return defaultFrontendAngular
}

func isValidFrontend(frontend string) bool {
	return frontend == defaultFrontendAngular || frontend == defaultFrontendReact
}

func redirectPathForFrontend(frontend string) string {
	if frontend == defaultFrontendReact {
		return "/app/"
	}

	return "/webui"
}

type wrappedFs struct {
	http.FileSystem
}

func (w wrappedFs) Open(name string) (http.File, error) {
	f, err := w.FileSystem.Open(name)
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		return w.FileSystem.Open("/index.html")
	}

	return f, err
}
