//! The serde-bound CEL `torrent` / `result` objects and the `NewTorrent`
//! transformer that builds `torrent` from a `ClassifierInput`.
//!
//! These mirror the proto `bitmagnet.Torrent` / `bitmagnet.Classification`
//! *shape* (proto camelCase field names, int enum discriminants) but are plain
//! serde structs living inside Lane C, per frozen decision #5. cel-rust has no
//! separate type-check phase, so — unlike cel-go — proto3 optional presence is
//! irrelevant here: every field is emitted with its proto3 default when unset
//! (`""` / `0` / `false`), which is exactly what cel-go field selection returns
//! for an unset optional scalar (and `classifier.core.yml` never calls `has()`).
//!
//! 🚨 The proto `year` (field 4) and `video3d` (field 10) are deliberately
//! ABSENT from [`CelClassification`] — the Go transformer never sets them
//! (contract §0.2). Reproducing the omission is a parity requirement.

use serde::Serialize;

use crate::model::{
    file_extension_from_path, file_type_from_extension, ClassifierInput, ContentType, FileType,
    FilesStatus,
};
use crate::result::Classification;

/// One file in the CEL `torrent.files` list.
#[derive(Serialize)]
pub(crate) struct CelFile {
    index: i32,
    path: String,
    #[serde(rename = "basePath")]
    base_path: String,
    #[serde(rename = "baseName")]
    base_name: String,
    size: i64,
    /// Proto3 optional string; `""` when the file has no extension.
    extension: String,
    #[serde(rename = "fileType")]
    file_type: i32,
}

/// The CEL `torrent` object.
#[derive(Serialize)]
pub(crate) struct CelTorrent {
    #[serde(rename = "infoHash")]
    info_hash: String,
    name: String,
    #[serde(rename = "baseName")]
    base_name: String,
    size: i64,
    extension: String,
    files: Vec<CelFile>,
    #[serde(rename = "filesCount")]
    files_count: i32,
    #[serde(rename = "filesSize")]
    files_size: i64,
    #[serde(rename = "fileExtensions")]
    file_extensions: Vec<String>,
    seeders: i32,
    leechers: i32,
    #[serde(rename = "hasHint")]
    has_hint: bool,
    #[serde(rename = "hasHintedContentId")]
    has_hinted_content_id: bool,
}

/// The CEL `result` object — the proto `bitmagnet.Classification` minus `year`
/// and `video3d` (see module note).
#[derive(Serialize)]
pub(crate) struct CelClassification {
    #[serde(rename = "contentType")]
    content_type: i32,
    #[serde(rename = "hasAttachedContent")]
    has_attached_content: bool,
    #[serde(rename = "hasBaseTitle")]
    has_base_title: bool,
    languages: Vec<String>,
    episodes: Vec<String>,
    #[serde(rename = "videoResolution")]
    video_resolution: String,
    #[serde(rename = "videoSource")]
    video_source: String,
    #[serde(rename = "videoCodec")]
    video_codec: String,
    #[serde(rename = "releaseGroup")]
    release_group: String,
    #[serde(rename = "contentId")]
    content_id: String,
    #[serde(rename = "contentSource")]
    content_source: String,
}

/// Whether the (already-lowercased-name) input carries a hinted content id
/// whose content-source is set — `HasHintedContentId` in the transformer.
fn hint_has_source(input: &ClassifierInput) -> bool {
    input
        .hint
        .as_ref()
        .is_some_and(|h| !h.content_type.is_empty() && !h.content_source.is_empty())
}

fn hint_present(input: &ClassifierInput) -> bool {
    input
        .hint
        .as_ref()
        .is_some_and(|h| !h.content_type.is_empty())
}

/// Torrent `BaseName()` — the name with a trailing `.<extension>` stripped when
/// the single-file extension is set.
fn torrent_base_name(name: &str, extension: Option<&str>) -> String {
    match extension {
        Some(ext) => name[..name.len().saturating_sub(ext.len() + 1)].to_string(),
        None => name.to_string(),
    }
}

/// File `BasePath()` — the path with a trailing `.<extension>` stripped when the
/// file extension is set.
fn file_base_path(path: &str, extension: Option<&str>) -> String {
    match extension {
        Some(ext) => path[..path.len().saturating_sub(ext.len() + 1)].to_string(),
        None => path.to_string(),
    }
}

/// File `BaseName()` — the last `/`-separated segment of `BasePath()`.
fn file_base_name(base_path: &str) -> String {
    base_path
        .rsplit('/')
        .next()
        .unwrap_or(base_path)
        .to_string()
}

/// Port of `protobuf.NewTorrent` (composed with `corpus_test.go toTorrent`):
/// builds the CEL `torrent` object directly from a `ClassifierInput`.
pub(crate) fn build_cel_torrent(input: &ClassifierInput) -> CelTorrent {
    let status = FilesStatus::parse(&input.files_status);
    let mut files: Vec<CelFile> = Vec::new();
    let mut files_size: i64 = 0;
    let size = input.size as i64;

    match status {
        Some(FilesStatus::NoInfo) => {
            if let Some(ext) = file_extension_from_path(&input.name) {
                let ft = file_type_from_extension(&ext).map_or(0, FileType::proto_i32);
                files.push(CelFile {
                    index: 0,
                    path: input.name.clone(),
                    base_path: input.name.clone(),
                    base_name: input.name.clone(),
                    size: 0,
                    extension: ext,
                    file_type: ft,
                });
                files_size = size;
            }
        }
        Some(FilesStatus::Single) => {
            let ext = input.extension.clone();
            let base = torrent_base_name(&input.name, ext.as_deref());
            let ft = ext
                .as_deref()
                .and_then(file_type_from_extension)
                .map_or(0, FileType::proto_i32);
            files.push(CelFile {
                index: 0,
                path: input.name.clone(),
                base_path: base.clone(),
                base_name: base,
                size,
                extension: ext.unwrap_or_default(),
                file_type: ft,
            });
            files_size = size;
        }
        Some(FilesStatus::Multi | FilesStatus::OverThreshold) => {
            let mut fs: i64 = 0;
            for f in &input.files {
                fs += f.size as i64;
                let ext = if f.extension.is_empty() {
                    None
                } else {
                    Some(f.extension.clone())
                };
                let base_path = file_base_path(&f.path, ext.as_deref());
                // f.FileType() derives from f.Path (fileTypeFromPath), NOT the
                // pre-set extension field.
                let file_type = file_extension_from_path(&f.path)
                    .and_then(|e| file_type_from_extension(&e))
                    .map_or(0, FileType::proto_i32);
                files.push(CelFile {
                    index: f.index as i32,
                    base_name: file_base_name(&base_path),
                    path: f.path.clone(),
                    base_path,
                    size: f.size as i64,
                    extension: ext.unwrap_or_default(),
                    file_type,
                });
            }
            if files.is_empty() && matches!(status, Some(FilesStatus::OverThreshold)) {
                files_size = size;
            } else {
                files_size = fs;
            }
        }
        None => {}
    }

    let file_extensions = torrent_file_extensions(input, status);

    CelTorrent {
        info_hash: String::new(),
        base_name: torrent_base_name(&input.name, input.extension.as_deref()),
        name: input.name.clone(),
        size,
        extension: input.extension.clone().unwrap_or_default(),
        files,
        files_count: input.files_count.map_or(0, |c| c as i32),
        files_size,
        file_extensions,
        seeders: 0,
        leechers: 0,
        has_hint: hint_present(input),
        has_hinted_content_id: hint_has_source(input),
    }
}

/// Port of `Torrent.FileExtensions()`.
fn torrent_file_extensions(input: &ClassifierInput, status: Option<FilesStatus>) -> Vec<String> {
    match status {
        Some(FilesStatus::Single) => file_extension_from_path(&input.name).into_iter().collect(),
        _ => {
            let mut seen = std::collections::HashSet::new();
            let mut out = Vec::new();
            for f in &input.files {
                if let Some(ext) = file_extension_from_path(&f.path) {
                    if seen.insert(ext.clone()) {
                        out.push(ext);
                    }
                }
            }
            out
        }
    }
}

/// Port of `protobuf.NewClassification` — builds the CEL `result` object from
/// the current classification (minus `year`/`video3d`).
pub(crate) fn build_cel_classification(c: &Classification) -> CelClassification {
    CelClassification {
        content_type: c.content_type.map_or(0, ContentType::proto_i32),
        has_attached_content: c.content_attached(),
        has_base_title: c.base_title.is_some(),
        // core.yml reads none of the fields below; emitted for shape parity.
        languages: c.languages.clone(),
        episodes: Vec::new(),
        video_resolution: c
            .video_resolution
            .map(|r| r.as_str().to_string())
            .unwrap_or_default(),
        video_source: c
            .video_source
            .map(|s| s.as_str().to_string())
            .unwrap_or_default(),
        video_codec: c
            .video_codec
            .map(|c| c.as_str().to_string())
            .unwrap_or_default(),
        release_group: c.release_group.clone().unwrap_or_default(),
        // `protobuf.NewClassification` reads these off the attached content, so
        // they are the attached row's primary-key parts, not blanks. Before the
        // B′-0 seam there was no way to attach content and they were hardcoded
        // to `""`; that stays the emitted value on the flags-off path (proto3's
        // default for an unset optional string, which is what cel-go field
        // selection returns), but it is now a *consequence* of nothing being
        // attached rather than a stub.
        content_id: c.content.as_ref().map(|x| x.id.clone()).unwrap_or_default(),
        content_source: c
            .content
            .as_ref()
            .map(|x| x.source.clone())
            .unwrap_or_default(),
    }
}
