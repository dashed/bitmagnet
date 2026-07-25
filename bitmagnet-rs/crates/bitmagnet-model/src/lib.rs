//! Domain models for bitmagnet, ported from the Go `internal/model/` package,
//! together with the compressed torrent-file blob format from
//! `internal/blobmigration/`.
//!
//! Highlights:
//! * [`InfoHash`] — the 20-byte BitTorrent v1 info hash (Go `protocol.ID`).
//! * [`ContentType`], [`FileType`], [`FilesStatus`] — the string enums whose
//!   values match both the PostgreSQL columns and (for content/file type) the
//!   proto integer enums used on the gRPC wire.
//! * [`BlobFile`] + [`serialize_files`] / [`deserialize_files`] — the
//!   MessagePack→ZSTD file blob, verified byte-for-byte against the Go
//!   serializer (see `tests/blob_fixture.rs`).
//! * [`Torrent`], [`Content`], [`TorrentContent`], [`TorrentFileSummary`] —
//!   the core domain structs.
//!
//! See `docs/rust-rewrite-plan.md`.

mod blob;
mod content;
mod enums;
mod info_hash;
mod torrent;

pub use blob::{
    deserialize_files, deserialize_files_bounded, serialize_files, BlobError, BlobFile,
    DecodedFiles,
};
pub use content::{Content, ContentAttribute, ContentCollection, ContentRef, Date, TorrentContent};
pub use enums::{file_extension_from_path, ContentType, FileType, FilesStatus, ParseEnumError};
pub use info_hash::{InfoHash, InfoHashError, INFO_HASH_LEN};
pub use torrent::{Torrent, TorrentFileSummary};
