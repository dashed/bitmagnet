# bitmagnet-rs

The Rust workspace for the [bitmagnet](https://github.com/bitmagnet-io/bitmagnet)
rewrite. This lives alongside the Go application in a polyglot monorepo — the Go
app stays at the repository root; everything Rust lives under `bitmagnet-rs/`.

The first deliverable is a **Tantivy search sidecar** exposed over gRPC, which
also becomes the foundation for the incremental Rust port. See the full plan in
[`docs/rust-rewrite-plan.md`](../docs/rust-rewrite-plan.md).

## Layout

```
bitmagnet-rs/
├── Cargo.toml              # workspace root (members + pinned dependencies)
├── rust-toolchain.toml     # stable channel + rustfmt/clippy
├── proto/bitmagnet/        # protobuf schema (shared with the Go side)
│   ├── common.proto        # ContentType / FileType enums (values match Go)
│   └── search.proto        # TorrentDocument + SearchService
└── crates/
    ├── bitmagnet-proto/    # generated tonic/prost bindings (build.rs)
    ├── bitmagnet-common/   # shared error, config, tracing helpers
    ├── bitmagnet-model/    # domain models                 (stub — later task)
    ├── bitmagnet-db/       # SQLx PostgreSQL access         (stub — later task)
    └── bitmagnet-search/   # Tantivy index + gRPC server    (stub — later task)
```

## Prerequisites

- Rust toolchain (pinned to `stable` via `rust-toolchain.toml`)
- `protoc` (Protocol Buffers compiler) — required to build `bitmagnet-proto`

## Build & test

```sh
cd bitmagnet-rs
cargo build --workspace          # compile everything
cargo test --workspace           # run unit tests
cargo fmt --all --check          # formatting
cargo clippy --workspace --all-targets   # lints
```

## Wire compatibility

`proto/bitmagnet/common.proto` re-declares the `ContentType` and `FileType`
enums from the Go service (`internal/protobuf/bitmagnet.proto`). The **integer
values are kept identical** so the Go and Rust components agree on the wire;
`bitmagnet-proto` has unit tests that assert the generated discriminants match.
