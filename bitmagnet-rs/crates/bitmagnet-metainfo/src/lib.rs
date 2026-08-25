//! Owned BitTorrent v1/v2 info-dictionary parsing.
//!
//! [`parse_info_bytes`] hashes the exact received bytes before decoding them.
//! It deliberately does not parse a whole `.torrent` file, perform BEP-9
//! transport, decide whether content should be banned, or persist anything.

use std::borrow::Cow;
use std::collections::BTreeMap;

use bendy::decoding::{Decoder, FromBencode};
use bendy::value::Value;
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use thiserror::Error;

/// Maximum nested list/dictionary depth accepted from an untrusted peer.
pub const MAX_INFO_NESTING_DEPTH: usize = 64;

type OwnedValue = Value<'static>;
type OwnedDict = BTreeMap<Cow<'static, [u8]>, OwnedValue>;

/// The normalized BitTorrent metadata generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetaVersion {
    V1,
    V2,
}

impl MetaVersion {
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
        }
    }
}

/// A decoded v1 file entry. Byte strings remain bytes so a later banning
/// policy can distinguish invalid UTF-8 from a parser failure.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InfoFile {
    length: i64,
    path: Vec<Vec<u8>>,
    path_utf8: Vec<Vec<u8>>,
}

impl InfoFile {
    #[must_use]
    pub const fn length(&self) -> i64 {
        self.length
    }

    #[must_use]
    pub fn path(&self) -> &[Vec<u8>] {
        &self.path
    }

    #[must_use]
    pub fn path_utf8(&self) -> &[Vec<u8>] {
        &self.path_utf8
    }

    #[must_use]
    pub fn best_path(&self) -> &[Vec<u8>] {
        if self.path_utf8.is_empty() {
            &self.path
        } else {
            &self.path_utf8
        }
    }
}

/// File properties stored under the empty key in a BEP-52 file tree.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileTreeFile {
    length: i64,
    pieces_root: Vec<u8>,
}

impl FileTreeFile {
    #[must_use]
    pub const fn length(&self) -> i64 {
        self.length
    }

    #[must_use]
    pub fn pieces_root(&self) -> &[u8] {
        &self.pieces_root
    }
}

/// An owned recursive BEP-52 file tree.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileTree {
    file: FileTreeFile,
    properties_present: bool,
    children: BTreeMap<Vec<u8>, FileTree>,
}

impl FileTree {
    #[must_use]
    pub const fn file(&self) -> &FileTreeFile {
        &self.file
    }

    #[must_use]
    pub const fn properties_present(&self) -> bool {
        self.properties_present
    }

    #[must_use]
    pub fn children(&self) -> &BTreeMap<Vec<u8>, Self> {
        &self.children
    }

    #[must_use]
    pub fn is_dir(&self) -> bool {
        !self.children.is_empty()
    }
}

/// The fields consumed by Bitmagnet from a v1, v2, or hybrid info dictionary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Info {
    piece_length: i64,
    pieces: Vec<u8>,
    name: Vec<u8>,
    name_utf8: Vec<u8>,
    length: i64,
    private: Option<bool>,
    source: Vec<u8>,
    files: Option<Vec<InfoFile>>,
    raw_meta_version: i64,
    file_tree: FileTree,
}

impl Info {
    #[must_use]
    pub const fn piece_length(&self) -> i64 {
        self.piece_length
    }

    #[must_use]
    pub fn pieces(&self) -> &[u8] {
        &self.pieces
    }

    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    #[must_use]
    pub fn name_utf8(&self) -> &[u8] {
        &self.name_utf8
    }

    #[must_use]
    pub fn best_name(&self) -> &[u8] {
        if self.name_utf8.is_empty() {
            &self.name
        } else {
            &self.name_utf8
        }
    }

    #[must_use]
    pub const fn length(&self) -> i64 {
        self.length
    }

    #[must_use]
    pub const fn private(&self) -> Option<bool> {
        self.private
    }

    #[must_use]
    pub fn source(&self) -> &[u8] {
        &self.source
    }

    /// `None` means the v1 `files` key was omitted; `Some([])` means it was
    /// present with an empty list. That distinction participates in Go's
    /// hybrid detection.
    #[must_use]
    pub fn files(&self) -> Option<&[InfoFile]> {
        self.files.as_deref()
    }

    #[must_use]
    pub const fn raw_meta_version(&self) -> i64 {
        self.raw_meta_version
    }

    #[must_use]
    pub const fn file_tree(&self) -> &FileTree {
        &self.file_tree
    }

    /// Mirrors `anacrolix/metainfo.Info.HasV1`, including presence of an empty
    /// `files` list.
    #[must_use]
    pub fn has_v1(&self) -> bool {
        self.raw_meta_version == 0
            || self.raw_meta_version == 1
            || self.files.is_some()
            || self.length != 0
            || !self.pieces.is_empty()
    }

    /// Mirrors `anacrolix/metainfo.Info.HasV2`.
    #[must_use]
    pub const fn has_v2(&self) -> bool {
        self.raw_meta_version == 2
    }
}

/// A verified info dictionary plus every identity it legitimately carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedInfo {
    info: Info,
    meta_version: MetaVersion,
    info_hash_v1: Option<[u8; 20]>,
    info_hash_v2: Option<[u8; 32]>,
}

impl ParsedInfo {
    #[must_use]
    pub const fn info(&self) -> &Info {
        &self.info
    }

    #[must_use]
    pub const fn meta_version(&self) -> MetaVersion {
        self.meta_version
    }

    #[must_use]
    pub const fn info_hash_v1(&self) -> Option<[u8; 20]> {
        self.info_hash_v1
    }

    #[must_use]
    pub const fn info_hash_v2(&self) -> Option<[u8; 32]> {
        self.info_hash_v2
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseInfoError {
    #[error("info bytes have wrong hash")]
    WrongHash,
    #[error("invalid bencode: {0}")]
    Bencode(String),
    #[error("empty info bytes")]
    Empty,
    #[error("trailing bencode object after info dictionary")]
    TrailingObject,
    #[error("{field} must be {expected}")]
    InvalidField {
        field: String,
        expected: &'static str,
    },
}

/// Verify and decode the raw info dictionary returned for a 20-byte DHT hash.
///
/// The requested hash may match either the SHA-1 digest or the first 20 bytes
/// of the SHA-256 digest. Hashing always precedes decoding, which preserves the
/// Go crawl path's anti-poisoning error precedence.
pub fn parse_info_bytes(
    requested: [u8; 20],
    raw_info: &[u8],
) -> Result<ParsedInfo, ParseInfoError> {
    let v1: [u8; 20] = Sha1::digest(raw_info).into();
    let v2: [u8; 32] = Sha256::digest(raw_info).into();
    if requested != v1 && requested.as_slice() != &v2[..20] {
        return Err(ParseInfoError::WrongHash);
    }

    let mut decoder = Decoder::new(raw_info).with_max_depth(MAX_INFO_NESTING_DEPTH);
    let object = decoder
        .next_object()
        .map_err(bencode_error)?
        .ok_or(ParseInfoError::Empty)?;
    let value = Value::decode_bencode_object(object)
        .map_err(bencode_error)?
        .into_owned();
    if decoder.next_object().map_err(bencode_error)?.is_some() {
        return Err(ParseInfoError::TrailingObject);
    }

    let info = decode_info(value)?;
    let info_hash_v1 = info.has_v1().then_some(v1);
    let info_hash_v2 = info.has_v2().then_some(v2);
    let meta_version = if info.has_v2() {
        MetaVersion::V2
    } else {
        MetaVersion::V1
    };

    Ok(ParsedInfo {
        info,
        meta_version,
        info_hash_v1,
        info_hash_v2,
    })
}

fn bencode_error(error: bendy::decoding::Error) -> ParseInfoError {
    ParseInfoError::Bencode(error.to_string())
}

fn decode_info(value: OwnedValue) -> Result<Info, ParseInfoError> {
    let mut dict = into_dict(value, "info")?;
    Ok(Info {
        piece_length: take_integer(&mut dict, b"piece length", "info.piece length")?
            .unwrap_or_default(),
        pieces: take_bytes(&mut dict, b"pieces", "info.pieces")?.unwrap_or_default(),
        name: take_bytes(&mut dict, b"name", "info.name")?.unwrap_or_default(),
        name_utf8: take_bytes(&mut dict, b"name.utf-8", "info.name.utf-8")?.unwrap_or_default(),
        length: take_integer(&mut dict, b"length", "info.length")?.unwrap_or_default(),
        private: take_integer(&mut dict, b"private", "info.private")?.map(|value| value != 0),
        source: take_bytes(&mut dict, b"source", "info.source")?.unwrap_or_default(),
        files: take_value(&mut dict, b"files")
            .map(|value| decode_files(value, "info.files"))
            .transpose()?,
        raw_meta_version: take_integer(&mut dict, b"meta version", "info.meta version")?
            .unwrap_or_default(),
        file_tree: take_value(&mut dict, b"file tree")
            .map(|value| decode_file_tree(value, "info.file tree"))
            .transpose()?
            .unwrap_or_default(),
    })
}

fn decode_files(value: OwnedValue, field: &str) -> Result<Vec<InfoFile>, ParseInfoError> {
    let list = into_list(value, field)?;
    list.into_iter()
        .enumerate()
        .map(|(index, value)| {
            let prefix = format!("{field}[{index}]");
            let mut dict = into_dict(value, &prefix)?;
            Ok(InfoFile {
                length: take_integer(&mut dict, b"length", &format!("{prefix}.length"))?
                    .unwrap_or_default(),
                path: take_value(&mut dict, b"path")
                    .map(|value| decode_byte_list(value, &format!("{prefix}.path")))
                    .transpose()?
                    .unwrap_or_default(),
                path_utf8: take_value(&mut dict, b"path.utf-8")
                    .map(|value| decode_byte_list(value, &format!("{prefix}.path.utf-8")))
                    .transpose()?
                    .unwrap_or_default(),
            })
        })
        .collect()
}

fn decode_file_tree(value: OwnedValue, field: &str) -> Result<FileTree, ParseInfoError> {
    let mut dict = into_dict(value, field)?;
    let properties = take_value(&mut dict, b"");
    let properties_present = properties.is_some();
    let file = properties
        .map(|value| decode_file_tree_file(value, &format!("{field}.<properties>")))
        .transpose()?
        .unwrap_or_default();
    let children = dict
        .into_iter()
        .map(|(name, value)| {
            let name = name.into_owned();
            let child_field = format!("{field}.{}", String::from_utf8_lossy(&name));
            decode_file_tree(value, &child_field).map(|tree| (name, tree))
        })
        .collect::<Result<_, _>>()?;
    Ok(FileTree {
        file,
        properties_present,
        children,
    })
}

fn decode_file_tree_file(value: OwnedValue, field: &str) -> Result<FileTreeFile, ParseInfoError> {
    let mut dict = into_dict(value, field)?;
    Ok(FileTreeFile {
        length: take_integer(&mut dict, b"length", &format!("{field}.length"))?.unwrap_or_default(),
        pieces_root: take_bytes(&mut dict, b"pieces root", &format!("{field}.pieces root"))?
            .unwrap_or_default(),
    })
}

fn decode_byte_list(value: OwnedValue, field: &str) -> Result<Vec<Vec<u8>>, ParseInfoError> {
    into_list(value, field)?
        .into_iter()
        .enumerate()
        .map(|(index, value)| into_bytes(value, &format!("{field}[{index}]")))
        .collect()
}

fn take_value(dict: &mut OwnedDict, key: &[u8]) -> Option<OwnedValue> {
    dict.remove(key)
}

fn take_bytes(
    dict: &mut OwnedDict,
    key: &[u8],
    field: &str,
) -> Result<Option<Vec<u8>>, ParseInfoError> {
    take_value(dict, key)
        .map(|value| into_bytes(value, field))
        .transpose()
}

fn take_integer(
    dict: &mut OwnedDict,
    key: &[u8],
    field: &str,
) -> Result<Option<i64>, ParseInfoError> {
    take_value(dict, key)
        .map(|value| into_integer(value, field))
        .transpose()
}

fn into_dict(value: OwnedValue, field: &str) -> Result<OwnedDict, ParseInfoError> {
    match value {
        Value::Dict(value) => Ok(value),
        _ => Err(invalid_field(field, "a dictionary")),
    }
}

fn into_list(value: OwnedValue, field: &str) -> Result<Vec<OwnedValue>, ParseInfoError> {
    match value {
        Value::List(value) => Ok(value),
        _ => Err(invalid_field(field, "a list")),
    }
}

fn into_bytes(value: OwnedValue, field: &str) -> Result<Vec<u8>, ParseInfoError> {
    match value {
        Value::Bytes(value) => Ok(value.into_owned()),
        _ => Err(invalid_field(field, "a byte string")),
    }
}

fn into_integer(value: OwnedValue, field: &str) -> Result<i64, ParseInfoError> {
    match value {
        Value::Integer(value) => Ok(value),
        _ => Err(invalid_field(field, "an integer")),
    }
}

fn invalid_field(field: &str, expected: &'static str) -> ParseInfoError {
    ParseInfoError::InvalidField {
        field: field.to_owned(),
        expected,
    }
}
