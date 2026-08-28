package tantivy

// The Go gRPC bindings in ./pb are generated from the Rust-owned protobuf
// contract in bitmagnet-rs/proto. The .proto files are shared with the sidecar,
// so their package / go_package are left untouched and the Go output location is
// overridden at generation time (protoc M-flags + module= prefix stripping,
// writing to internal/search/tantivy/pb).
//
// Regenerate with `task gen-search-proto` (wired into `task gen`); it needs
// protoc plus protoc-gen-go (v1.35.1, matching internal/protobuf) and
// protoc-gen-go-grpc on PATH (go install ...; ensure $(go env GOPATH)/bin is on
// PATH).
//
// There is intentionally no //go:generate directive: the protoc invocation's
// repeated M-flag paths make a single line that exceeds the repo's revive
// line-length-limit (120) and gen.go is not a "generated" file, so the
// .golangci.yml `generated: lax` exclusion does not cover it. Taskfile-driven
// codegen also matches the existing protobuf gen flow (Taskfile's gen-protoc).
