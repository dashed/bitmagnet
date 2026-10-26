//go:build webuireact

package webuireact

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func TestHandlerServesReactApp(t *testing.T) {
	fsys := fstest.MapFS{
		"index.html": {
			Data: []byte(`<!doctype html><html><body><div id="root">app shell</div></body></html>`),
		},
		"assets/index.js": {
			Data: []byte(`console.log("asset loaded");`),
		},
	}
	handler := Handler(fsys)

	t.Run("redirects mount path to trailing slash", func(t *testing.T) {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/app", nil)

		handler.ServeHTTP(response, request)

		if response.Code != http.StatusMovedPermanently {
			t.Fatalf("expected status %d, got %d", http.StatusMovedPermanently, response.Code)
		}
		if location := response.Header().Get("Location"); location != "/app/" {
			t.Fatalf("expected Location /app/, got %q", location)
		}
	})

	t.Run("serves index at app root", func(t *testing.T) {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/app/", nil)

		handler.ServeHTTP(response, request)

		assertIndexResponse(t, response)
	})

	t.Run("falls back to index for deep links", func(t *testing.T) {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/app/some/deep/route", nil)

		handler.ServeHTTP(response, request)

		assertIndexResponse(t, response)
	})

	t.Run("serves javascript assets with javascript content type", func(t *testing.T) {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/app/assets/index.js", nil)

		handler.ServeHTTP(response, request)

		if response.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
		}
		if contentType := response.Header().Get("Content-Type"); !strings.Contains(contentType, "javascript") {
			t.Fatalf("expected javascript content type, got %q", contentType)
		}
		if body := response.Body.String(); !strings.Contains(body, "asset loaded") {
			t.Fatalf("expected asset body, got %q", body)
		}
	})
}

func assertIndexResponse(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}
	if contentType := response.Header().Get("Content-Type"); !strings.Contains(contentType, "text/html") {
		t.Fatalf("expected HTML content type, got %q", contentType)
	}
	if body := response.Body.String(); !strings.Contains(body, "app shell") {
		t.Fatalf("expected index.html body, got %q", body)
	}
}

// Review-mandated pins (p03-review): asset-miss 404, traversal sandbox,
// query-string-preserving redirect.
func TestHandlerReviewPins(t *testing.T) {
	fsys := fstest.MapFS{
		"index.html":      {Data: []byte(`<!doctype html><html><body>shell</body></html>`)},
		"assets/index.js": {Data: []byte(`console.log("ok");`)},
	}
	handler := Handler(fsys)

	t.Run("missing hashed asset 404s instead of serving the HTML shell", func(t *testing.T) {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/app/assets/missing-xyz.js", nil)

		handler.ServeHTTP(response, request)

		if response.Code != http.StatusNotFound {
			t.Fatalf("expected 404 for a missing asset (deploy skew must fail loudly), got %d", response.Code)
		}
	})

	t.Run("path traversal stays inside the SPA sandbox", func(t *testing.T) {
		for _, target := range []string{"/app/../graphql", "/app/%2e%2e/graphql", "/app/../../etc/passwd"} {
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, target, nil)

			handler.ServeHTTP(response, request)

			if response.Code == http.StatusOK && !strings.Contains(response.Body.String(), "shell") &&
				response.Body.Len() > 0 {
				t.Fatalf("traversal %q escaped the sandbox (body %q)", target, response.Body.String()[:min(64, response.Body.Len())])
			}
		}
	})

	t.Run("mount redirect preserves the query string", func(t *testing.T) {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/app?q=1&x=2", nil)

		handler.ServeHTTP(response, request)

		if location := response.Header().Get("Location"); location != "/app/?q=1&x=2" {
			t.Fatalf("expected query-preserving redirect, got %q", location)
		}
	})
}
