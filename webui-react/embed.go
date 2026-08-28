//go:build webuireact

package webuireact

import (
	"embed"
	"io/fs"
)

const Enabled = true

//go:embed all:dist
var embeddedFS embed.FS

var FS = mustSub(embeddedFS, "dist")

func mustSub(fsys fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic(err)
	}

	return sub
}
