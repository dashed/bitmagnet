//! The CEL `torrent` / `result` objects and the `NewTorrent` transformer that
//! builds `torrent` from a `ClassifierInput`.
//!
//! These mirror the proto `bitmagnet.Torrent` / `bitmagnet.Classification`
//! *shape* (proto camelCase field names, int enum discriminants) but live inside
//! Lane C, per frozen decision #5: `torrent` is a plain serde struct, and
//! `result` a table of field conversions ([`RESULT_FIELDS`]). cel-rust has no
//! separate type-check phase, so — unlike cel-go — proto3 optional presence is
//! irrelevant here: every field is emitted with its proto3 default when unset
//! (`""` / `0` / `false`), which is exactly what cel-go field selection returns
//! for an unset optional scalar (and `classifier.core.yml` never calls `has()`).
//!
//! 🚨 The proto `year` (field 4) and `video3d` (field 10) are deliberately
//! ABSENT from [`RESULT_FIELDS`] — the Go transformer never sets them
//! (contract §0.2). Reproducing the omission is a parity requirement.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use bitmagnet_release::{VideoCodec, VideoResolution, VideoSource};
use cel::common::traits::{Container, Indexer, Iterable, Sizer, Zeroer};
use cel::common::types::{CelString, Type, MAP_TYPE};
use cel::common::value::Val;
use cel::{ExecutionError, Value};
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

/// How one `result` field is read off the current classification.
type ResultField = fn(&Classification) -> Value;

/// Port of `protobuf.NewClassification`: the fields of the CEL `result` object —
/// the proto `bitmagnet.Classification` minus `year` and `video3d` (see module
/// note) — each with how it is read off the current classification.
///
/// The one place a field is named and converted. [`cel_classification`]
/// builds the whole object from it and [`LazyResult`] only the fields an
/// expression selects, so the two cannot disagree.
const RESULT_FIELDS: [(&str, ResultField); 11] = [
    ("contentType", |c| {
        Value::Int(c.content_type.map_or(0, ContentType::proto_i32).into())
    }),
    ("hasAttachedContent", |c| Value::Bool(c.content_attached())),
    ("hasBaseTitle", |c| Value::Bool(c.base_title.is_some())),
    // core.yml reads none of the fields below; emitted for shape parity.
    ("languages", |c| c.languages.clone().into()),
    ("episodes", |_| Value::List(Arc::default())),
    ("videoResolution", |c| {
        c.video_resolution
            .map_or("", VideoResolution::as_str)
            .into()
    }),
    ("videoSource", |c| {
        c.video_source.map_or("", VideoSource::as_str).into()
    }),
    ("videoCodec", |c| {
        c.video_codec.map_or("", VideoCodec::as_str).into()
    }),
    ("releaseGroup", |c| {
        c.release_group.as_deref().unwrap_or_default().into()
    }),
    // `protobuf.NewClassification` reads these off the attached content, so
    // they are the attached row's primary-key parts, not blanks. Before the
    // B′-0 seam there was no way to attach content and they were hardcoded
    // to `""`; that stays the emitted value on the flags-off path (proto3's
    // default for an unset optional string, which is what cel-go field
    // selection returns), but it is now a *consequence* of nothing being
    // attached rather than a stub.
    ("contentId", |c| {
        c.content.as_ref().map_or("", |x| x.id.as_str()).into()
    }),
    ("contentSource", |c| {
        c.content.as_ref().map_or("", |x| x.source.as_str()).into()
    }),
];

/// The conversion for the `result` field named `name`, if there is one.
fn result_field(name: &str) -> Option<ResultField> {
    RESULT_FIELDS
        .iter()
        .find(|(field, _)| *field == name)
        .map(|(_, value)| *value)
}

/// The CEL `result` object for the current classification, every field
/// converted.
pub(crate) fn cel_classification(c: &Classification) -> Value {
    RESULT_FIELDS
        .iter()
        .map(|(field, value)| (*field, value(c)))
        .collect::<HashMap<_, _>>()
        .into()
}

/// The CEL `result` object, converting only the fields an expression selects.
///
/// Binding [`cel_classification`] converts and boxes every field for each
/// condition, while `classifier.core.yml`'s conditions read one or two. This
/// holds the classification instead, and answers `get` and `contains` for a
/// field name from [`RESULT_FIELDS`]. Everything else — any other key, and
/// every operation on the object as a whole — goes to the map
/// [`cel_classification`] would have bound, built on first use, so it gives
/// the same answer or error that map does.
///
/// 🚨 Bind it only for an expression that reads `result` through field
/// selection alone (`crate::env` works that out when it compiles the
/// expression). cel-rust converts a map-typed value to [`Value`] by
/// downcasting it to its own map type and panics on any other, and its map's
/// equality only recognises its own map, so a `result` that is returned,
/// passed to a function, compared, or put into a list or map must be the real
/// thing.
#[derive(Debug)]
pub(crate) struct LazyResult {
    classification: Arc<Classification>,
    eager: OnceLock<Box<dyn Val>>,
}

impl LazyResult {
    pub(crate) fn new(classification: Arc<Classification>) -> Self {
        Self {
            classification,
            eager: OnceLock::new(),
        }
    }

    /// The map [`cel_classification`] binds.
    fn eager(&self) -> &dyn Val {
        self.eager
            .get_or_init(|| {
                // What `Context::add_variable_from_value` does with it.
                cel_classification(&self.classification)
                    .try_into()
                    .expect("a map of ints, bools, strings and lists converts")
            })
            .as_ref()
    }

    /// The field `key` names, when it is a string naming one.
    fn field(&self, key: &dyn Val) -> Option<Result<Box<dyn Val>, ExecutionError>> {
        let value = result_field(key.downcast_ref::<CelString>()?.inner())?;
        Some(value(&self.classification).try_into())
    }
}

impl Val for LazyResult {
    fn get_type(&self) -> &Type {
        &MAP_TYPE
    }

    fn as_container(&self) -> Option<&dyn Container> {
        Some(self)
    }

    fn as_indexer(&self) -> Option<&dyn Indexer> {
        Some(self)
    }

    fn into_indexer(self: Box<Self>) -> Option<Box<dyn Indexer>> {
        Some(self)
    }

    fn as_iterable(&self) -> Option<&dyn Iterable> {
        self.eager().as_iterable()
    }

    fn as_sizer(&self) -> Option<&dyn Sizer> {
        self.eager().as_sizer()
    }

    fn as_zeroer(&self) -> Option<&dyn Zeroer> {
        self.eager().as_zeroer()
    }

    fn equals(&self, other: &dyn Val) -> bool {
        match other.downcast_ref::<Self>() {
            Some(other) => self.eager().equals(other.eager()),
            None => self.eager().equals(other),
        }
    }

    fn clone_as_boxed(&self) -> Box<dyn Val> {
        Box::new(Self::new(Arc::clone(&self.classification)))
    }
}

impl Container for LazyResult {
    fn contains(&self, key: &dyn Val) -> Result<bool, ExecutionError> {
        match key.downcast_ref::<CelString>() {
            Some(key) => Ok(result_field(key.inner()).is_some()),
            None => self
                .eager()
                .as_container()
                .ok_or(ExecutionError::NoSuchOverload)?
                .contains(key),
        }
    }
}

impl Indexer for LazyResult {
    fn get<'a>(&'a self, key: &dyn Val) -> Result<Cow<'a, dyn Val>, ExecutionError> {
        match self.field(key) {
            Some(value) => value.map(Cow::Owned),
            None => self
                .eager()
                .as_indexer()
                .ok_or(ExecutionError::NoSuchOverload)?
                .get(key),
        }
    }

    fn steal(self: Box<Self>, key: &dyn Val) -> Result<Box<dyn Val>, ExecutionError> {
        match self.field(key) {
            Some(value) => value,
            None => self
                .eager()
                .clone_as_boxed()
                .into_indexer()
                .ok_or(ExecutionError::NoSuchOverload)?
                .steal(key),
        }
    }
}
