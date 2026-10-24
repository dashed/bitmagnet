//! [`Content`] and [`TorrentContent`], mirroring Go's `model.Content`
//! (`content.gen.go`) and `model.TorrentContent` (`torrent_contents.gen.go`).
//!
//! As with [`crate::Torrent`], the typed video enums (`NullVideoResolution`, …)
//! are represented as `Option<String>` for now, since only [`ContentType`] /
//! [`FileType`] are needed as first-class enums.
//!
//! The B′-0 classifier-dependency seam completed [`Content`] to the full Go
//! column set, because the classifier's `attach_*` actions attach a whole
//! `model.Content` and the ingest path then persists it: the `tsv` column and
//! the `Collections` / `Attributes` associations are load-bearing inputs to
//! [`Content::update_tsv`], not decoration.

use serde::{Deserialize, Serialize};

use bitmagnet_fts::{Tsvector, TsvectorWeight};

use crate::enums::ContentType;
use crate::info_hash::InfoHash;

/// A calendar date, mirroring Go `model.Date` (`date.go`). The zero value is
/// Go's nil date.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Date {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

impl Date {
    /// Go `Date.IsNil()` — the zero value.
    #[must_use]
    pub fn is_nil(self) -> bool {
        self == Date::default()
    }
}

/// The `(type, source, id)` primary key of a [`Content`] row — Go
/// `model.ContentRef` (`content.go:7`). This is what the classifier's
/// `attach_*_content_by_id` actions look a row up by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContentRef {
    #[serde(rename = "type")]
    pub content_type: ContentType,
    pub source: String,
    pub id: String,
}

/// A row from `content_collections` joined onto a [`Content`] — Go
/// `model.ContentCollection` (`content_collections.gen.go`).
///
/// Only rows whose [`Self::collection_type`] is `"genre"` contribute to the
/// content tsvector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContentCollection {
    /// Collection kind, e.g. `"genre"`; serialised as `"type"` to match Go.
    #[serde(rename = "type")]
    pub collection_type: String,
    pub source: String,
    pub id: String,
    pub name: String,
}

/// A row from `content_attributes` joined onto a [`Content`] — Go
/// `model.ContentAttribute` (`content_attributes.gen.go`).
///
/// Only rows whose [`Self::key`] is `"id"` (external identifiers such as the
/// IMDb id) contribute to the content tsvector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContentAttribute {
    pub content_type: ContentType,
    pub content_source: String,
    pub content_id: String,
    pub source: String,
    pub key: String,
    pub value: String,
}

/// A row from the `content` table — externally-sourced metadata (TMDB, etc.)
/// keyed by `(type, source, id)`.
///
/// Mirrors Go's `model.Content` (`content.gen.go:16-38`). The GORM
/// `MetadataSource` association is omitted: it is a foreign-key expansion of
/// [`Self::source`] into a `(key, name)` lookup row, carries no data the
/// classifier or the ingest write-set reads, and is never populated by the TMDB
/// transformers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Content {
    /// Content type (part of the primary key); serialised as `"type"` to match
    /// the Go JSON tag.
    #[serde(rename = "type")]
    pub content_type: ContentType,
    /// Metadata source identifier, e.g. `"tmdb"` (part of the primary key).
    pub source: String,
    /// Source-specific id (part of the primary key).
    pub id: String,
    /// Display title.
    pub title: String,
    /// Full release date, when known.
    #[serde(default)]
    pub release_date: Option<Date>,
    /// Release year, when known.
    ///
    /// Go's `model.Year` is a `uint16` whose zero value means nil; the `Option`
    /// carries that presence bit explicitly, and the width is widened to `u32`
    /// to match the existing PostgreSQL decode path (no year is affected).
    #[serde(default)]
    pub release_year: Option<u32>,
    /// Adult / XXX flag from the metadata source, when known.
    #[serde(default)]
    pub adult: Option<bool>,
    /// Original-language ISO code, when known.
    #[serde(default)]
    pub original_language: Option<String>,
    /// Original-language title, when known.
    #[serde(default)]
    pub original_title: Option<String>,
    /// Plot overview / synopsis.
    #[serde(default)]
    pub overview: Option<String>,
    /// Runtime in minutes, when known.
    #[serde(default)]
    pub runtime: Option<u32>,
    /// Popularity score from the metadata source.
    #[serde(default)]
    pub popularity: Option<f32>,
    /// Average vote / rating.
    #[serde(default)]
    pub vote_average: Option<f32>,
    /// Number of votes backing [`Self::vote_average`].
    #[serde(default)]
    pub vote_count: Option<u32>,
    /// Row creation time as a Unix timestamp in seconds. DB-managed
    /// (`<-:create`): never set by the classifier, populated only when a row is
    /// read back.
    #[serde(default)]
    pub created_at: Option<i64>,
    /// Row update time as a Unix timestamp in seconds. DB-managed.
    #[serde(default)]
    pub updated_at: Option<i64>,
    /// The full-text search vector, maintained by [`Self::update_tsv`].
    #[serde(default)]
    pub tsv: Tsvector,
    /// The `content_collections_content` many-to-many expansion.
    #[serde(default)]
    pub collections: Vec<ContentCollection>,
    /// The `content_attributes` rows for this content.
    #[serde(default)]
    pub attributes: Vec<ContentAttribute>,
}

impl Content {
    /// The `(type, source, id)` primary key — Go `Content.Ref()`.
    #[must_use]
    pub fn content_ref(&self) -> ContentRef {
        ContentRef {
            content_type: self.content_type,
            source: self.source.clone(),
            id: self.id.clone(),
        }
    }

    /// Port of Go `Content.UpdateTsv` (`internal/model/content.go:80-105`).
    ///
    /// Rebuilds [`Self::tsv`] from scratch, appending in exactly Go's order so
    /// the lexeme positions match byte-for-byte:
    ///
    /// 1. [`Self::title`] at weight `A`;
    /// 2. [`Self::original_title`] at weight `A`, **only** when present *and*
    ///    different from the title (a same-title duplicate would otherwise
    ///    inflate positions);
    /// 3. [`Self::release_year`] at weight `B`, when non-nil;
    /// 4. every `"genre"` collection's name at weight `D`, in slice order;
    /// 5. every attribute whose key is `"id"`, by value, at weight `D`, in slice
    ///    order.
    ///
    /// Steps 4 and 5 iterate the slices in order, which is where Rust is
    /// *stricter* than Go: Go ranges over slices too, so the orders agree as
    /// long as the caller preserves the DB's ordering of the preloaded
    /// associations.
    pub fn update_tsv(&mut self) {
        let mut tsv = Tsvector::new();
        tsv.add_text(&self.title, TsvectorWeight::A);

        if let Some(original_title) = &self.original_title {
            if original_title != &self.title {
                tsv.add_text(original_title, TsvectorWeight::A);
            }
        }

        // Go's `Year` is a `uint16` whose zero value *is* nil, so a `Some(0)`
        // must be treated as absent, not rendered as the lexeme "0".
        if let Some(year) = self.release_year.filter(|y| *y != 0) {
            tsv.add_text(&year.to_string(), TsvectorWeight::B);
        }

        for collection in &self.collections {
            if collection.collection_type == "genre" {
                tsv.add_text(&collection.name, TsvectorWeight::D);
            }
        }

        for attribute in &self.attributes {
            if attribute.key == "id" {
                tsv.add_text(&attribute.value, TsvectorWeight::D);
            }
        }

        self.tsv = tsv;
    }
}

/// A row from the `torrent_contents` table — the join between a torrent and its
/// (optional) classified content, carrying the fields the search index needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentContent {
    /// Surrogate primary key.
    pub id: String,
    /// Info hash of the associated torrent.
    pub info_hash: InfoHash,
    /// Classified content type, when known.
    #[serde(default)]
    pub content_type: Option<ContentType>,
    /// Metadata source of the linked [`Content`], when classified.
    #[serde(default)]
    pub content_source: Option<String>,
    /// Source-specific id of the linked [`Content`], when classified.
    #[serde(default)]
    pub content_id: Option<String>,
    /// Detected languages (the `languages` JSON column).
    #[serde(default)]
    pub languages: Vec<String>,
    /// Video resolution, e.g. `"1080p"`.
    #[serde(default)]
    pub video_resolution: Option<String>,
    /// Video source, e.g. `"BluRay"`.
    #[serde(default)]
    pub video_source: Option<String>,
    /// Video codec, e.g. `"x265"`.
    #[serde(default)]
    pub video_codec: Option<String>,
    /// Scene release group, when detected.
    #[serde(default)]
    pub release_group: Option<String>,
    /// Swarm seeders, when known.
    #[serde(default)]
    pub seeders: Option<u32>,
    /// Swarm leechers, when known.
    #[serde(default)]
    pub leechers: Option<u32>,
    /// Publication time as a Unix timestamp in seconds (maps to the proto
    /// `published_at` / Tantivy Date field).
    pub published_at: i64,
    /// Total size in bytes (denormalised from the torrent).
    pub size: u64,
    /// Number of files, when known.
    #[serde(default)]
    pub files_count: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_serde_round_trips() {
        let mut c = matrix();
        c.update_tsv();
        let bytes = rmp_serde::to_vec_named(&c).unwrap();
        let back: Content = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(c, back);
    }

    fn matrix() -> Content {
        Content {
            content_type: ContentType::Movie,
            source: "tmdb".to_owned(),
            id: "603".to_owned(),
            title: "The Matrix".to_owned(),
            release_date: Some(Date {
                year: 1999,
                month: 3,
                day: 30,
            }),
            release_year: Some(1999),
            adult: Some(false),
            original_language: Some("en".to_owned()),
            original_title: Some("The Matrix".to_owned()),
            overview: None,
            runtime: Some(136),
            popularity: Some(42.5),
            vote_average: Some(8.2),
            vote_count: Some(24000),
            created_at: None,
            updated_at: None,
            tsv: Tsvector::new(),
            collections: Vec::new(),
            attributes: Vec::new(),
        }
    }

    /// The original title equals the title, so Go's `UpdateTsv` skips it — the
    /// year therefore lands at position 4 (2 title lexemes, then the one-slot
    /// gap), not 6.
    #[test]
    fn update_tsv_skips_an_original_title_equal_to_the_title() {
        let mut c = matrix();
        c.update_tsv();
        assert_eq!(c.tsv.to_string(), "'1999':4B 'matrix':2A 'the':1A");
    }

    #[test]
    fn update_tsv_includes_a_distinct_original_title() {
        let mut c = matrix();
        c.original_title = Some("Matrix".to_owned());
        c.update_tsv();
        assert_eq!(c.tsv.to_string(), "'1999':6B 'matrix':2A,4A 'the':1A");
    }

    /// Only `"genre"` collections and only `"id"`-keyed attributes contribute,
    /// both at weight D (rendered bare).
    #[test]
    fn update_tsv_filters_collections_and_attributes() {
        let mut c = matrix();
        c.collections = vec![
            ContentCollection {
                collection_type: "genre".to_owned(),
                source: "tmdb".to_owned(),
                id: "28".to_owned(),
                name: "Action".to_owned(),
            },
            ContentCollection {
                collection_type: "network".to_owned(),
                source: "tmdb".to_owned(),
                id: "1".to_owned(),
                name: "Excluded".to_owned(),
            },
        ];
        c.attributes = vec![
            ContentAttribute {
                content_type: ContentType::Movie,
                content_source: "tmdb".to_owned(),
                content_id: "603".to_owned(),
                source: "imdb".to_owned(),
                key: "id".to_owned(),
                value: "tt0133093".to_owned(),
            },
            ContentAttribute {
                content_type: ContentType::Movie,
                content_source: "tmdb".to_owned(),
                content_id: "603".to_owned(),
                source: "tmdb".to_owned(),
                key: "poster".to_owned(),
                value: "excluded".to_owned(),
            },
        ];
        c.update_tsv();
        assert_eq!(
            c.tsv.to_string(),
            "'1999':4B 'action':6 'matrix':2A 'the':1A 'tt0133093':8"
        );
    }

    /// `update_tsv` rebuilds from scratch, so running it twice is idempotent.
    #[test]
    fn update_tsv_is_idempotent() {
        let mut c = matrix();
        c.update_tsv();
        let once = c.tsv.clone();
        c.update_tsv();
        assert_eq!(once, c.tsv);
    }

    /// Go's `Year` zero value is nil, so a `Some(0)` must not emit a "0" lexeme.
    #[test]
    fn update_tsv_treats_year_zero_as_nil() {
        let mut c = matrix();
        c.release_year = Some(0);
        c.update_tsv();
        assert_eq!(c.tsv.to_string(), "'matrix':2A 'the':1A");
    }

    #[test]
    fn content_ref_is_the_primary_key() {
        let c = matrix();
        assert_eq!(
            c.content_ref(),
            ContentRef {
                content_type: ContentType::Movie,
                source: "tmdb".to_owned(),
                id: "603".to_owned(),
            }
        );
    }

    #[test]
    fn torrent_content_serde_round_trips() {
        let tc = TorrentContent {
            id: "abc".to_owned(),
            info_hash: "0123456789abcdef0123456789abcdef01234567".parse().unwrap(),
            content_type: Some(ContentType::TvShow),
            content_source: Some("tmdb".to_owned()),
            content_id: Some("1399".to_owned()),
            languages: vec!["en".to_owned(), "de".to_owned()],
            video_resolution: Some("1080p".to_owned()),
            video_source: Some("BluRay".to_owned()),
            video_codec: Some("x265".to_owned()),
            release_group: None,
            seeders: Some(123),
            leechers: Some(4),
            published_at: 1_700_000_000,
            size: 9_000_000_000,
            files_count: Some(10),
        };
        let bytes = rmp_serde::to_vec_named(&tc).unwrap();
        let back: TorrentContent = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(tc, back);
    }
}
