package health

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestHandlerBuilderRegistersLivenessWithoutDependencyChecks(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)

	ckr := checkerMock{}
	lChecker := lazy.New[Checker](func() (Checker, error) {
		return &ckr, nil
	})
	router := gin.New()

	require.NoError(t, handlerBuilder{Checker: lChecker}.Apply(router))

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/livez", nil)
	router.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{"status":"up"}`, response.Body.String())
	ckr.AssertNotCalled(t, "Check", mock.Anything)
}
