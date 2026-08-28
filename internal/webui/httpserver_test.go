package webui

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestResolveFrontendPrecedenceAndFallbacks(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                  string
		queryFrontend         string
		cookieFrontend        string
		cfg                   Config
		reactEnabled          bool
		wantFrontend          string
		wantRedirectPath      string
		wantSetCookie         bool
		wantCookieValue       string
		wantWarnReactDisabled bool
	}{
		{
			name:             "query beats cookie and env",
			queryFrontend:    defaultFrontendAngular,
			cookieFrontend:   defaultFrontendReact,
			cfg:              Config{DefaultFrontend: defaultFrontendReact},
			reactEnabled:     true,
			wantFrontend:     defaultFrontendAngular,
			wantRedirectPath: "/webui",
			wantSetCookie:    true,
			wantCookieValue:  defaultFrontendAngular,
		},
		{
			name:             "cookie beats env",
			cookieFrontend:   defaultFrontendReact,
			cfg:              Config{DefaultFrontend: defaultFrontendAngular},
			reactEnabled:     true,
			wantFrontend:     defaultFrontendReact,
			wantRedirectPath: "/app/",
		},
		{
			name:             "invalid query falls through to cookie",
			queryFrontend:    "vue",
			cookieFrontend:   defaultFrontendReact,
			cfg:              Config{DefaultFrontend: defaultFrontendAngular},
			reactEnabled:     true,
			wantFrontend:     defaultFrontendReact,
			wantRedirectPath: "/app/",
		},
		{
			name:             "invalid query and cookie fall through to env",
			queryFrontend:    "vue",
			cookieFrontend:   "svelte",
			cfg:              Config{DefaultFrontend: defaultFrontendReact},
			reactEnabled:     true,
			wantFrontend:     defaultFrontendReact,
			wantRedirectPath: "/app/",
		},
		{
			name:                  "react query falls back to angular when disabled",
			queryFrontend:         defaultFrontendReact,
			cfg:                   Config{DefaultFrontend: defaultFrontendAngular},
			reactEnabled:          false,
			wantFrontend:          defaultFrontendAngular,
			wantRedirectPath:      "/webui",
			wantSetCookie:         true,
			wantCookieValue:       defaultFrontendReact,
			wantWarnReactDisabled: true,
		},
		{
			name:                  "react cookie falls back to angular when disabled",
			cookieFrontend:        defaultFrontendReact,
			cfg:                   Config{DefaultFrontend: defaultFrontendAngular},
			reactEnabled:          false,
			wantFrontend:          defaultFrontendAngular,
			wantRedirectPath:      "/webui",
			wantWarnReactDisabled: true,
		},
		{
			name:                  "react env falls back to angular when disabled",
			cfg:                   Config{DefaultFrontend: defaultFrontendReact},
			reactEnabled:          false,
			wantFrontend:          defaultFrontendAngular,
			wantRedirectPath:      "/webui",
			wantWarnReactDisabled: true,
		},
		{
			name:             "default config resolves to angular",
			cfg:              NewDefaultConfig(),
			reactEnabled:     true,
			wantFrontend:     defaultFrontendAngular,
			wantRedirectPath: "/webui",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			selection := resolveFrontend(tc.queryFrontend, tc.cookieFrontend, tc.cfg, tc.reactEnabled)

			assert.Equal(t, tc.wantFrontend, selection.frontend)
			assert.Equal(t, tc.wantRedirectPath, selection.redirectPath)
			assert.Equal(t, tc.wantSetCookie, selection.setCookie)
			assert.Equal(t, tc.wantCookieValue, selection.cookieValue)
			assert.Equal(t, tc.wantWarnReactDisabled, selection.warnReactDisabled)
		})
	}
}

func TestRootRedirectSetsFrontendCookie(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	option := builder{
		config: NewDefaultConfig(),
		logger: zap.NewNop().Sugar(),
	}
	require.NoError(t, option.Apply(engine))

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/?frontend=angular", nil)

	engine.ServeHTTP(response, request)

	assert.Equal(t, http.StatusMovedPermanently, response.Code)
	assert.Equal(t, "/webui", response.Header().Get("Location"))

	result := response.Result()
	defer result.Body.Close()

	cookie := findCookie(result.Cookies(), frontendCookieName)
	require.NotNil(t, cookie)
	assert.Equal(t, defaultFrontendAngular, cookie.Value)
	assert.Equal(t, "/", cookie.Path)
	assert.Equal(t, frontendCookieMaxAge, cookie.MaxAge)
	assert.False(t, cookie.HttpOnly)
	assert.False(t, cookie.Secure)
	assert.Equal(t, http.SameSiteLaxMode, cookie.SameSite)
}

func findCookie(cookies []*http.Cookie, name string) *http.Cookie {
	for _, cookie := range cookies {
		if cookie.Name == name {
			return cookie
		}
	}

	return nil
}
