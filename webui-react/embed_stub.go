//go:build !webuireact

package webuireact

import "io/fs"

const Enabled = false

var FS fs.FS = emptyFS{}

type emptyFS struct{}

func (emptyFS) Open(string) (fs.File, error) {
	return nil, fs.ErrNotExist
}
