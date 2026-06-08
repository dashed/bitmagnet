//! File-grained Tantivy schema, parameterized by `Variant`.
//!
//! Mirrors the §4.1 table of `file-grained-search-spec.md`, with one knob per
//! costing question the smoke pass must answer:
//!   * `size` / `published_at`: INDEXED|FAST (spec-as-written) vs FAST-only
//!     (recommended — range comes from FAST, `range_query.rs:102-117`).
//!   * identity: STORED `doc_id` (spec D3) vs FAST `info_hash`+`file_index`.
//!
//! Field-flag idioms match the shipped `bitmagnet-search/src/schema.rs`
//! (`keyword_facet = (STRING|STORED).set_fast(None)` :149; `numeric =
//! (STORED|INDEXED|FAST)` :152) so the measured bytes transfer to production.

use anyhow::Result;
use clap::ValueEnum;
use tantivy::schema::{
    BytesOptions, Field, IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
    FAST, STORED, STRING, TEXT,
};
use tantivy::tokenizer::{LowerCaser, NgramTokenizer, TextAnalyzer};
use tantivy::Index;

/// How a numeric field (`size`, `published_at`) is configured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumMode {
    /// FAST only — range via `FastFieldRangeWeight`, no term dict. (recommended)
    Fast,
    /// INDEXED|FAST — spec-as-written; adds a term dict + postings we measure.
    IndexedFast,
}

impl NumMode {
    fn u64_opts(self) -> NumericOptions {
        match self {
            NumMode::Fast => NumericOptions::default().set_fast(),
            NumMode::IndexedFast => NumericOptions::default().set_fast().set_indexed(),
        }
    }
    fn i64_opts(self) -> NumericOptions {
        self.u64_opts()
    }
}

/// A schema configuration. The variant matrix (V1-V11) is just a set of these.
#[derive(Clone, Debug)]
pub struct Variant {
    pub name: &'static str,
    /// STORED stored-only `doc_id` (`hex:file_index`). Spec identity (D3).
    pub doc_id_stored: bool,
    /// FAST `info_hash` (hex) + `file_index` — the cheaper identity alternative.
    pub identity_fast: bool,
    /// `size` field, when present.
    pub size: Option<NumMode>,
    /// `extension` STRING|FAST.
    pub extension: bool,
    /// `content_type` STRING|FAST (synth denorm).
    pub content_type: bool,
    /// `published_at` i64, when present.
    pub published_at: Option<NumMode>,
    /// `path` tokenized TEXT (v1.1 — default tokenizer, CJK-broken; size only).
    pub path: bool,
}

impl Variant {
    /// Resolve the named variant from the spec's matrix, or `None`.
    pub fn from_name(name: &str) -> Option<Variant> {
        let base = Variant {
            name: "",
            doc_id_stored: true,
            identity_fast: false,
            size: None,
            extension: false,
            content_type: false,
            published_at: None,
            path: false,
        };
        Some(match name.to_ascii_uppercase().as_str() {
            // V1: baseline = stored doc_id + (mandatory) info_hash delete key.
            "V1" => Variant { name: "V1", ..base },
            // V2: identity-as-FAST instead of stored doc_id.
            "V2" => Variant {
                name: "V2",
                doc_id_stored: false,
                identity_fast: true,
                ..base
            },
            // V3 / V4: size FAST vs INDEXED|FAST (the key two-variant diff).
            "V3" => Variant {
                name: "V3",
                size: Some(NumMode::Fast),
                ..base
            },
            "V4" => Variant {
                name: "V4",
                size: Some(NumMode::IndexedFast),
                ..base
            },
            // V5 / V6: extension; content_type.
            "V5" => Variant {
                name: "V5",
                extension: true,
                ..base
            },
            "V6" => Variant {
                name: "V6",
                content_type: true,
                ..base
            },
            // V7 / V8: published_at FAST vs INDEXED|FAST.
            "V7" => Variant {
                name: "V7",
                published_at: Some(NumMode::Fast),
                ..base
            },
            "V8" => Variant {
                name: "V8",
                published_at: Some(NumMode::IndexedFast),
                ..base
            },
            // V9: full v1, spec-as-written (numerics INDEXED|FAST).
            "V9" => Variant {
                name: "V9",
                size: Some(NumMode::IndexedFast),
                extension: true,
                content_type: true,
                published_at: Some(NumMode::IndexedFast),
                ..base
            },
            // V10: full v1, FAST-only (recommended) = the GO/NO-GO size number.
            "V10" => Variant {
                name: "V10",
                size: Some(NumMode::Fast),
                extension: true,
                content_type: true,
                published_at: Some(NumMode::Fast),
                ..base
            },
            // V11: V10 + tokenized path (v1.1 smoke).
            "V11" => Variant {
                name: "V11",
                size: Some(NumMode::Fast),
                extension: true,
                content_type: true,
                published_at: Some(NumMode::Fast),
                path: true,
                ..base
            },
            _ => return None,
        })
    }

    pub fn all() -> Vec<&'static str> {
        vec![
            "V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10", "V11",
        ]
    }
}

/// Resolved field handles for whichever variant built the schema.
#[derive(Clone, Copy, Debug)]
pub struct FileFields {
    pub doc_id: Option<Field>,
    /// Always present: the 20-byte info_hash delete key (INDEXED bytes).
    pub info_hash: Field,
    pub info_hash_fast: Option<Field>,
    pub file_index_fast: Option<Field>,
    pub extension: Option<Field>,
    pub size: Option<Field>,
    pub content_type: Option<Field>,
    pub published_at: Option<Field>,
    pub path: Option<Field>,
}

/// Build the Tantivy schema + field handles for `variant`.
pub fn build_file_schema(variant: &Variant) -> (Schema, FileFields) {
    let mut b = Schema::builder();

    let doc_id = variant
        .doc_id_stored
        .then(|| b.add_text_field("doc_id", STORED));

    // Mandatory delete/upsert key: indexed bytes, not stored, not fast.
    let info_hash = b.add_bytes_field("info_hash", BytesOptions::default().set_indexed());

    // FAST-only identity alternative (no postings): hex info_hash + file_index.
    let info_hash_fast = variant
        .identity_fast
        .then(|| b.add_text_field("info_hash_fast", TextOptions::default().set_fast(None)));
    let file_index_fast = variant
        .identity_fast
        .then(|| b.add_u64_field("file_index_fast", NumericOptions::default().set_fast()));

    let extension = variant
        .extension
        .then(|| b.add_text_field("extension", STRING.set_fast(None)));

    let size = variant
        .size
        .map(|m| b.add_u64_field("size", m.u64_opts()));

    // Multivalued in production; one synth value/doc here (size is identical).
    let content_type = variant
        .content_type
        .then(|| b.add_text_field("content_type", STRING.set_fast(None)));

    let published_at = variant
        .published_at
        .map(|m| b.add_i64_field("published_at", m.i64_opts()));

    // Default tokenizer (WithFreqsAndPositions). Production uses the bitmagnet
    // tokenizer; for SIZE smoke the token count is comparable. CJK caveat: H2.
    let path = variant.path.then(|| b.add_text_field("path", TEXT));

    let _ = FAST; // (kept in scope for clarity of the flag vocabulary)

    let schema = b.build();
    let fields = FileFields {
        doc_id,
        info_hash,
        info_hash_fast,
        file_index_fast,
        extension,
        size,
        content_type,
        published_at,
        path,
    };
    (schema, fields)
}

// ===========================================================================
// EXP-D: path tokenizer knob + path-only recall schema
// ===========================================================================

/// Which tokenizer drives the `path` TEXT field (EXP-D). All three index the
/// *identical* doc population; only the term/postings shape differs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum PathTokenizer {
    /// Tantivy's built-in `default` analyzer (`SimpleTokenizer` + `LowerCaser` +
    /// `RemoveLongFilter`). A CJK run is ONE whole token → mid-run substrings
    /// miss. The known-broken V11 baseline.
    Default,
    /// Built-in `NgramTokenizer` at the CHARACTER level (min=2, max=3,
    /// `prefix_only=false`) + `LowerCaser`. Language-agnostic CJK substring;
    /// zero external dependency. NOTE: ngram tokens all carry position 0
    /// (`ngram_tokenizer.rs:168`), so substring queries are a conjunction of
    /// the query's ngram terms, never a `PhraseQuery` (see `main.rs`).
    Ngram,
    /// `lindera-tantivy` CJK morphological segmentation. OPTIONAL — only built
    /// when the `lindera` cargo feature is enabled; otherwise selecting it errors
    /// rather than silently degrading. Keeps a heavy/failed lindera build from
    /// blocking the dict-free `default`-vs-`ngram` core result.
    Lindera,
}

impl PathTokenizer {
    /// Name the chosen tokenizer is registered under / the `path` field is bound
    /// to. `default` is pre-registered by Tantivy's `TokenizerManager`.
    pub fn tantivy_name(self) -> &'static str {
        match self {
            PathTokenizer::Default => "default",
            PathTokenizer::Ngram => "ngram",
            PathTokenizer::Lindera => "lindera",
        }
    }
}

/// Register the chosen path tokenizer on `index` (mirrors the shipped sidecar's
/// `index::register_tokenizer` idiom — tokenizers are runtime state, not
/// persisted, so this must run on every freshly opened/created index before it
/// is read or written). No-op for `Default` (built-in). Errors for `Lindera`
/// when the `lindera` feature is not compiled in.
pub fn register_path_tokenizer(
    index: &Index,
    tok: PathTokenizer,
    ngram: (usize, usize),
) -> Result<()> {
    match tok {
        PathTokenizer::Default => {} // built-in default analyzer
        PathTokenizer::Ngram => {
            // LowerCaser → case-insensitive parity with the lowercased exact
            // substring truth computed in-process. NgramTokenizer::new returns a
            // tantivy::Result (min/max validation); `?` lifts it into anyhow.
            // (min,max) is caller-tunable: (2,3) = spec bi/tri-gram (higher
            // precision on ≥3-char queries); (2,2) = bigram-only (smaller index,
            // lower precision on ≥3-char queries — still 100% recall since we
            // query as a CONJUNCTION of ngram terms).
            let (min, max) = ngram;
            let analyzer = TextAnalyzer::builder(NgramTokenizer::new(min, max, false)?)
                .filter(LowerCaser)
                .build();
            index.tokenizers().register("ngram", analyzer);
        }
        PathTokenizer::Lindera => {
            #[cfg(feature = "lindera")]
            {
                register_lindera(index)?;
            }
            #[cfg(not(feature = "lindera"))]
            {
                anyhow::bail!(
                    "lindera tokenizer requires building with --features lindera (not compiled in)"
                );
            }
        }
    }
    Ok(())
}

/// Build the bitmagnet `lindera` analyzer. Behind the `lindera` feature so the
/// dict-free core never depends on it. Implementation is intentionally minimal
/// and may need a version bump to match tantivy 0.26's `Tokenizer` trait.
#[cfg(feature = "lindera")]
fn register_lindera(index: &Index) -> Result<()> {
    use lindera_tantivy::tokenizer::LinderaTokenizer;
    // IPADIC, normal mode — standard Japanese/CJK segmentation. (Dictionary is
    // embedded by the lindera-tantivy feature flags in Cargo.toml.)
    let tokenizer = LinderaTokenizer::new()
        .map_err(|e| anyhow::anyhow!("lindera tokenizer init: {e}"))?;
    index
        .tokenizers()
        .register("lindera", TextAnalyzer::from(tokenizer));
    Ok(())
}

/// Resolved handles for the path-only recall index (EXP-D).
#[derive(Clone, Copy, Debug)]
pub struct RecallFields {
    /// Tokenized `path` (chosen tokenizer, `WithFreqsAndPositions`). The ONLY
    /// indexed text field, so `report_segment_bytes` attributes term/postings/
    /// positions bytes to the path field alone.
    pub path: Field,
    /// Per-doc identity hash of `(info_hash, file_index)`, FAST-only. Lets the
    /// evaluator map a Tantivy hit back to a doc identity to intersect with the
    /// in-process truth set. FAST → reported under the separate "FAST columns"
    /// component, so it never contaminates the path-field size attribution.
    pub ident: Field,
}

/// Build the path-only schema for the `recall` subcommand: one tokenized `path`
/// field (per `tok`) + a FAST identity hash. Everything else is deliberately
/// omitted to isolate path-field cost (spec §"skip the other fields").
pub fn build_recall_schema(tok: PathTokenizer) -> (Schema, RecallFields) {
    let mut b = Schema::builder();
    // Positions kept for ALL tokenizers so phrase/substring queries work and the
    // size comparison is apples-to-apples (ngram positions are all 0 → cheap).
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(tok.tantivy_name())
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let path = b.add_text_field("path", TextOptions::default().set_indexing_options(indexing));
    let ident = b.add_u64_field("ident", NumericOptions::default().set_fast());
    let schema = b.build();
    (schema, RecallFields { path, ident })
}
