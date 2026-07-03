//! Tantivy schema for torrent documents.
//!
//! This is the source-of-truth mapping from bitmagnet's Postgres `tsvector`
//! model to Tantivy fields. It reconciles three things that must agree:
//!
//! 1. The "Field Mapping (PG → Tantivy)" table in `docs/rust-rewrite-plan.md`.
//! 2. The proto `TorrentDocument` contract (`proto/bitmagnet/search.proto`),
//!    which is what the Go side actually sends on the wire.
//! 3. The *real* tsvector construction in Go (`internal/model/content.go`
//!    `Content.UpdateTsv` and `internal/model/torrent_contents.go`
//!    `TorrentContent.UpdateTsv`), which decides what text lands at which
//!    weight.
//!
//! ## Relevance model: weight tiers, not per-source fields
//!
//! Postgres stores ONE `tsvector` whose lexemes are labelled A/B/C/D, and
//! `ts_rank` scores them with the weights `{D:0.1, C:0.2, B:0.4, A:1.0}`. We
//! reproduce that with four tokenized TEXT fields — one per weight tier — and
//! let the query layer apply a per-tier boost (the proto documents the boosts
//! as A=4.0, B=2.0, C=1.5, D=0.5, i.e. the PG weights renormalised to A=4.0).
//! What Go puts where (verified against the two `UpdateTsv` functions):
//!
//! | Tier | Field    | Boost | Sources |
//! |------|----------|-------|---------|
//! | A    | `text_a` | 4.0   | info_hash (hex), torrent name, content title, original title |
//! | B    | `text_b` | 2.0   | release year |
//! | C    | `text_c` | 1.5   | video resolution, source, codec |
//! | D    | `text_d` | 0.5   | genres, file paths |
//!
//! These tier fields are indexed-only (relevance); they are never stored.
//!
//! ## Retrieval / facets / filters / sort
//!
//! Separate fields carry the *exact* values needed to rebuild a hit's
//! `TorrentDocument` (STORED) and to facet/filter/sort (FAST + INDEXED). The
//! big one — `file_paths` — is intentionally NOT a retrieval field: it only
//! feeds `text_d` for relevance, because storing every file path is exactly
//! the 273 GB problem the blob migration removed.
//!
//! Flag conventions:
//! - `STRING` — indexed, untokenized (exact match), for enum/keyword values.
//! - `TEXT` — built here by hand so the tier fields use the bitmagnet
//!   tokenizer, with positions only on the title/name tier that phrase-class
//!   queries can target.
//! - `STORED` — value is retrievable from the doc store.
//! - `FAST` — value lives in the columnar store (sorting / faceting / filter).

use tantivy::schema::{
    BytesOptions, Field, IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
    FAST, INDEXED, STORED, STRING,
};
use tantivy::Score;

use crate::tokenizer::TOKENIZER_NAME;

/// Query boost applied to each weight tier. Mirrors the proto `TorrentDocument`
/// field comments (PG `ts_rank` weights renormalised so A = 4.0).
pub const BOOST_A: Score = 4.0;
pub const BOOST_B: Score = 2.0;
pub const BOOST_C: Score = 1.5;
pub const BOOST_D: Score = 0.5;

/// Field names, kept as consts so [`build_schema`] and [`Fields::from_schema`]
/// can never drift apart.
mod name {
    pub(super) const DOC_ID: &str = "doc_id";
    pub(super) const INFO_HASH: &str = "info_hash";
    pub(super) const TORRENT_NAME: &str = "torrent_name";
    pub(super) const CONTENT_TITLE: &str = "content_title";
    pub(super) const ORIGINAL_TITLE: &str = "original_title";

    pub(super) const TEXT_A: &str = "text_a";
    pub(super) const TEXT_B: &str = "text_b";
    pub(super) const TEXT_C: &str = "text_c";
    pub(super) const TEXT_D: &str = "text_d";

    pub(super) const CONTENT_TYPE: &str = "content_type";
    pub(super) const VIDEO_RESOLUTION: &str = "video_resolution";
    pub(super) const VIDEO_SOURCE: &str = "video_source";
    pub(super) const VIDEO_CODEC: &str = "video_codec";
    pub(super) const VIDEO_3D: &str = "video_3d";
    pub(super) const VIDEO_MODIFIER: &str = "video_modifier";
    pub(super) const RELEASE_GROUP: &str = "release_group";
    pub(super) const GENRES: &str = "genres";
    pub(super) const LANGUAGES: &str = "languages";
    pub(super) const AUDIO_LANGUAGES: &str = "audio_languages";
    pub(super) const FILE_EXTENSIONS: &str = "file_extensions";
    pub(super) const CONTENT_SOURCE: &str = "content_source";
    pub(super) const CONTENT_ID: &str = "content_id";

    pub(super) const RELEASE_YEAR: &str = "release_year";
    pub(super) const SIZE: &str = "size";
    pub(super) const SEEDERS: &str = "seeders";
    pub(super) const LEECHERS: &str = "leechers";
    pub(super) const FILES_COUNT: &str = "files_count";
    pub(super) const PUBLISHED_AT: &str = "published_at";
}

/// Every field name in the schema, in declaration order. Drives the schema
/// completeness test.
pub const FIELD_NAMES: [&str; 28] = [
    name::DOC_ID,
    name::INFO_HASH,
    name::TORRENT_NAME,
    name::CONTENT_TITLE,
    name::ORIGINAL_TITLE,
    name::TEXT_A,
    name::TEXT_B,
    name::TEXT_C,
    name::TEXT_D,
    name::CONTENT_TYPE,
    name::VIDEO_RESOLUTION,
    name::VIDEO_SOURCE,
    name::VIDEO_CODEC,
    name::VIDEO_3D,
    name::VIDEO_MODIFIER,
    name::RELEASE_GROUP,
    name::GENRES,
    name::LANGUAGES,
    name::AUDIO_LANGUAGES,
    name::FILE_EXTENSIONS,
    name::CONTENT_SOURCE,
    name::CONTENT_ID,
    name::RELEASE_YEAR,
    name::SIZE,
    name::SEEDERS,
    name::LEECHERS,
    name::FILES_COUNT,
    name::PUBLISHED_AT,
];

/// Build the Tantivy [`Schema`] for torrent documents.
///
/// Multi-valued fields (`genres`, `languages`, `file_extensions`) are plain
/// fields added more than once per document; Tantivy needs no special flag.
#[must_use]
pub fn build_schema() -> Schema {
    let mut builder = Schema::builder();

    // Tokenized relevance tiers: bitmagnet tokenizer, indexed only — never
    // stored or fast. Positions are retained only where phrase/prefix queries
    // can target; PS-MB1 found lower-tier positions are ~83% dead weight.
    let tier_with_positions = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(TOKENIZER_NAME)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    let tier_without_positions = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(TOKENIZER_NAME)
            .set_index_option(IndexRecordOption::WithFreqs),
    );

    // Exact keyword fields that are also faceted/filtered: indexed (exact),
    // stored (hit reconstruction), and fast (columnar facet/filter).
    let keyword_facet = (STRING | STORED).set_fast(None);

    // Numerics used for sort and/or range filter, plus stored for retrieval.
    let numeric: NumericOptions = (STORED | INDEXED | FAST).into();

    // --- Identity ---------------------------------------------------------
    // Composite row id (`hex:type:source:id`) = the PG `torrent_contents.id`
    // generated column / Go `TorrentContent.InferID()`. This is the UPSERT key:
    // one info_hash maps to many torrent_content rows, so documents are replaced
    // per composite id, not per info_hash. Indexed (delete-by-term) + stored
    // (stable SearchHit id for the read path).
    builder.add_text_field(name::DOC_ID, STRING | STORED);
    // 20-byte v1 info hash: exact-match term + stored. Non-unique — it is the
    // DeleteDocument key (a torrent's removal cascade-deletes all its content
    // rows, so deleting by info_hash removes all of a torrent's documents).
    builder.add_bytes_field(
        name::INFO_HASH,
        BytesOptions::default().set_indexed().set_stored(),
    );

    // --- Stored-only display text (searchability comes from the tiers) ----
    builder.add_text_field(name::TORRENT_NAME, STORED);
    builder.add_text_field(name::CONTENT_TITLE, STORED);
    builder.add_text_field(name::ORIGINAL_TITLE, STORED);

    // --- Relevance tiers --------------------------------------------------
    // text_a keeps positions: phrase/prefix queries are constrained to this
    // title/name tier, so Tantivy can safely run phrase-class queries here.
    builder.add_text_field(name::TEXT_A, tier_with_positions);
    // text_b drops positions: phrase/prefix cannot target it, and PS-MB1 found
    // lower-tier positions are ~83% dead weight.
    builder.add_text_field(name::TEXT_B, tier_without_positions.clone());
    // text_c drops positions: phrase/prefix cannot target it, and PS-MB1 found
    // lower-tier positions are ~83% dead weight.
    builder.add_text_field(name::TEXT_C, tier_without_positions.clone());
    // text_d drops positions: phrase/prefix cannot target it, and PS-MB1 found
    // lower-tier positions are ~83% dead weight.
    builder.add_text_field(name::TEXT_D, tier_without_positions);

    // --- Keyword facets / filters (exact value, stored + fast) ------------
    builder.add_text_field(name::CONTENT_TYPE, keyword_facet.clone());
    builder.add_text_field(name::VIDEO_RESOLUTION, keyword_facet.clone());
    builder.add_text_field(name::VIDEO_SOURCE, keyword_facet.clone());
    builder.add_text_field(name::VIDEO_CODEC, keyword_facet.clone());
    builder.add_text_field(name::VIDEO_3D, keyword_facet.clone());
    builder.add_text_field(name::VIDEO_MODIFIER, keyword_facet.clone());
    builder.add_text_field(name::RELEASE_GROUP, keyword_facet.clone());
    builder.add_text_field(name::GENRES, keyword_facet.clone()); // multi-valued
    builder.add_text_field(name::LANGUAGES, keyword_facet.clone()); // multi-valued
    builder.add_text_field(name::AUDIO_LANGUAGES, keyword_facet.clone()); // multi-valued
    builder.add_text_field(name::FILE_EXTENSIONS, keyword_facet.clone()); // multi-valued
    builder.add_text_field(name::CONTENT_SOURCE, keyword_facet.clone());
    builder.add_text_field(name::CONTENT_ID, keyword_facet);

    // --- Numerics (sort / range filter / facet bucketing) ----------------
    builder.add_u64_field(name::RELEASE_YEAR, numeric.clone());
    builder.add_u64_field(name::SIZE, numeric.clone());
    builder.add_u64_field(name::SEEDERS, numeric.clone());
    builder.add_u64_field(name::LEECHERS, numeric.clone());
    builder.add_u64_field(name::FILES_COUNT, numeric.clone());
    // `published_at` is i64 Unix seconds, matching the proto (an as-built
    // delta from the plan's "Date" — see docs/rust-rewrite-plan.md).
    builder.add_i64_field(name::PUBLISHED_AT, numeric);

    builder.build()
}

/// Resolved [`Field`] handles for the torrent schema.
///
/// All handles are [`Copy`], so this whole struct is cheap to clone and hold in
/// the [`crate::server::SearchServer`]. The index writer ([`crate::indexer`])
/// and the read path (`query.rs` / `facets.rs`) both build against it.
#[derive(Debug, Clone, Copy)]
pub struct Fields {
    // Identity + stored display.
    pub doc_id: Field,
    pub info_hash: Field,
    pub torrent_name: Field,
    pub content_title: Field,
    pub original_title: Field,
    // Relevance tiers (boosts via [`Fields::weighted_text_fields`]).
    pub text_a: Field,
    pub text_b: Field,
    pub text_c: Field,
    pub text_d: Field,
    // Keyword facets / filters.
    pub content_type: Field,
    pub video_resolution: Field,
    pub video_source: Field,
    pub video_codec: Field,
    pub video_3d: Field,
    pub video_modifier: Field,
    pub release_group: Field,
    pub genres: Field,
    pub languages: Field,
    pub audio_languages: Field,
    pub file_extensions: Field,
    pub content_source: Field,
    pub content_id: Field,
    // Numerics.
    pub release_year: Field,
    pub size: Field,
    pub seeders: Field,
    pub leechers: Field,
    pub files_count: Field,
    pub published_at: Field,
}

impl Fields {
    /// Resolve every handle against `schema`.
    ///
    /// # Errors
    /// Returns the underlying [`tantivy::TantivyError`] if `schema` is missing
    /// any expected field — i.e. it was not produced by [`build_schema`].
    pub fn from_schema(schema: &Schema) -> tantivy::Result<Self> {
        Ok(Self {
            doc_id: schema.get_field(name::DOC_ID)?,
            info_hash: schema.get_field(name::INFO_HASH)?,
            torrent_name: schema.get_field(name::TORRENT_NAME)?,
            content_title: schema.get_field(name::CONTENT_TITLE)?,
            original_title: schema.get_field(name::ORIGINAL_TITLE)?,
            text_a: schema.get_field(name::TEXT_A)?,
            text_b: schema.get_field(name::TEXT_B)?,
            text_c: schema.get_field(name::TEXT_C)?,
            text_d: schema.get_field(name::TEXT_D)?,
            content_type: schema.get_field(name::CONTENT_TYPE)?,
            video_resolution: schema.get_field(name::VIDEO_RESOLUTION)?,
            video_source: schema.get_field(name::VIDEO_SOURCE)?,
            video_codec: schema.get_field(name::VIDEO_CODEC)?,
            video_3d: schema.get_field(name::VIDEO_3D)?,
            video_modifier: schema.get_field(name::VIDEO_MODIFIER)?,
            release_group: schema.get_field(name::RELEASE_GROUP)?,
            genres: schema.get_field(name::GENRES)?,
            languages: schema.get_field(name::LANGUAGES)?,
            audio_languages: schema.get_field(name::AUDIO_LANGUAGES)?,
            file_extensions: schema.get_field(name::FILE_EXTENSIONS)?,
            content_source: schema.get_field(name::CONTENT_SOURCE)?,
            content_id: schema.get_field(name::CONTENT_ID)?,
            release_year: schema.get_field(name::RELEASE_YEAR)?,
            size: schema.get_field(name::SIZE)?,
            seeders: schema.get_field(name::SEEDERS)?,
            leechers: schema.get_field(name::LEECHERS)?,
            files_count: schema.get_field(name::FILES_COUNT)?,
            published_at: schema.get_field(name::PUBLISHED_AT)?,
        })
    }

    /// The four weight-tier text fields paired with their query boost, for the
    /// read path to OR a term across with the right relevance weighting.
    #[must_use]
    pub fn weighted_text_fields(&self) -> [(Field, Score); 4] {
        [
            (self.text_a, BOOST_A),
            (self.text_b, BOOST_B),
            (self.text_c, BOOST_C),
            (self.text_d, BOOST_D),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::{build_schema, Fields, FIELD_NAMES};
    use tantivy::schema::IndexRecordOption;

    #[test]
    fn schema_contains_all_mapped_fields() {
        let schema = build_schema();
        for name in FIELD_NAMES {
            assert!(
                schema.get_field(name).is_ok(),
                "schema is missing field `{name}`"
            );
        }
    }

    #[test]
    fn fields_resolve_against_built_schema() {
        let schema = build_schema();
        Fields::from_schema(&schema).expect("all field handles resolve");
    }

    #[test]
    fn weighted_text_fields_are_descending_by_tier() {
        let fields = Fields::from_schema(&build_schema()).unwrap();
        let boosts: Vec<_> = fields
            .weighted_text_fields()
            .iter()
            .map(|(_, b)| *b)
            .collect();
        assert_eq!(boosts, vec![4.0, 2.0, 1.5, 0.5]);
    }

    #[test]
    fn text_tier_index_options_keep_positions_only_on_text_a() {
        let schema = build_schema();
        let fields = Fields::from_schema(&schema).unwrap();
        let index_option = |field| {
            schema
                .get_field_entry(field)
                .field_type()
                .get_index_record_option()
        };

        assert_eq!(
            index_option(fields.text_a),
            Some(IndexRecordOption::WithFreqsAndPositions)
        );
        assert_eq!(
            index_option(fields.text_b),
            Some(IndexRecordOption::WithFreqs)
        );
        assert_eq!(
            index_option(fields.text_c),
            Some(IndexRecordOption::WithFreqs)
        );
        assert_eq!(
            index_option(fields.text_d),
            Some(IndexRecordOption::WithFreqs)
        );
    }
}
