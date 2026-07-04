package webuireact

import (
	"errors"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

const mountPath = "/app"

func Handler(fsys fs.FS) http.Handler {
	fileServer := http.FileServer(fallbackFileSystem{FileSystem: http.FS(fsys)})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == mountPath {
			target := mountPath + "/"
			if r.URL.RawQuery != "" {
				target += "?" + r.URL.RawQuery
			}

			http.Redirect(w, r, target, http.StatusMovedPermanently)

			return
		}

		if r.URL.Path != mountPath+"/" && !strings.HasPrefix(r.URL.Path, mountPath+"/") {
			http.NotFound(w, r)
			return
		}

		r2 := new(http.Request)
		*r2 = *r

		u := *r.URL
		u.Path = strings.TrimPrefix(r.URL.Path, mountPath)
		u.RawPath = ""

		if u.Path == "" {
			u.Path = "/"
		}

		r2.URL = &u

		fileServer.ServeHTTP(w, r2)
	})
}

type fallbackFileSystem struct {
	http.FileSystem
}

// Open serves the SPA shell for missing NON-FILE paths (deep-link refresh) but
// 404s missing paths that look like files (an extension in the last segment):
// a stale hashed asset after deploy skew must fail loudly with 404, not return
// 200 text/html that the browser rejects with a confusing MIME error.
func (w fallbackFileSystem) Open(name string) (http.File, error) {
	f, err := w.FileSystem.Open(name)
	if err != nil && errors.Is(err, fs.ErrNotExist) && path.Ext(name) == "" {
		return w.FileSystem.Open("/index.html")
	}

	return f, err
}
