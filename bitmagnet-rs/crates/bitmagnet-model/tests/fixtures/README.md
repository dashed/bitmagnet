# Blob fixtures

These `*.blob` files are **ground truth produced by the real Go serializer**
`internal/blobmigration.SerializeFiles` (MessagePack via
`vmihailenco/msgpack/v5` → ZSTD via `klauspost/compress`). `tests/blob_fixture.rs`
asserts the Rust `deserialize_files` reads them identically, and that the Rust
`serialize_files` produces byte-identical inner MessagePack — proving Go ⇄ Rust
wire compatibility for the `torrents.files_data` column.

The inputs are reconstructed in `tests/blob_fixture.rs` (`basic`/`edge`/`single`/
`empty`). To regenerate after an intentional format change, run a throwaway
generator from the Go module root (`github.com/bitmagnet-io/bitmagnet`):

```go
// tmp_blobfixture/main.go — `go run ./tmp_blobfixture`, then delete it.
package main

import (
	"os"
	"path/filepath"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
)

const outDir = "bitmagnet-rs/crates/bitmagnet-model/tests/fixtures"

func ext(s string) model.NullString {
	if s == "" {
		return model.NullString{}
	}
	return model.NewNullString(s)
}

func write(name string, files []model.TorrentFile) {
	b, err := blobmigration.SerializeFiles(files)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(outDir, name+".blob"), b, 0o644); err != nil {
		panic(err)
	}
}

func main() {
	write("basic", []model.TorrentFile{
		{Index: 0, Path: "Season 1/Episode 1.mkv", Extension: ext("mkv"), Size: 1500000000},
		{Index: 1, Path: "Season 1/Episode 2.mkv", Extension: ext("mkv"), Size: 1600000123},
		{Index: 2, Path: "Season 1/subs/ep1.srt", Extension: ext("srt"), Size: 40000},
	})
	write("edge", []model.TorrentFile{
		{Index: 0, Path: "RÉADME", Extension: ext(""), Size: 0},
		{Index: 1234567, Path: "音楽/曲.flac", Extension: ext("flac"), Size: 9999999999},
	})
	write("single", []model.TorrentFile{
		{Index: 0, Path: "ubuntu-24.04.iso", Extension: ext("iso"), Size: 6203484160},
	})
	write("empty", []model.TorrentFile{})
}
```

Keep the inputs above in sync with the expectations in `tests/blob_fixture.rs`.
