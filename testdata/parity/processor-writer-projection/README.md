# Processor writer-projection oracle

`fixtures.json` is a checked-in Go oracle for the pure Rust unattached writer
projection. The Go test constructs real `model.Torrent` and
`classification.Result` values, calls package-private `newTorrentContent`, and
checks the exact nullable counts, microsecond publication time, and
`Tsvector.String()` output. The Rust integration test consumes the same inputs
and expected values through `project_unattached_persistence`.

The oracle is deliberately compact. The bounded-path record uses an overlong
path lexeme followed by normal terms; the near-900 KB exhaustion case remains an
in-module Rust test rather than inflating this cross-language fixture.

To print a candidate regenerated fixture for review without modifying files:

```sh
BITMAGNET_PRINT_WRITER_PROJECTION_ORACLE=1 \
  go test ./internal/processor -run '^TestWriterProjectionOracle$' -v
```

Copy the printed JSON only after reviewing the Go source change that caused the
delta. A normal test run always verifies the checked-in expected records.
