//! The path typeahead read path: tokenize → guard → gram-conjunction query →
//! seeders-ranked top-k → [`PathHit`]s.
//!
//! ## Why a conjunction, and why no early-out
//!
//! A substring query is the **intersection** of its char-grams (PS-T3 §0): a
//! `BooleanQuery` of `(Must, TermQuery(gram))`. PS-T3 verified against the
//! tantivy 0.26 source that block-max WAND fires only for score-sorted
//! *disjunctions* and `SegmentCollector` has no abort, so top-k tricks do **not**
//! bound the scan for our shape. Latency is governed by the match-set size,
//! which the design shrinks structurally: per-torrent path-bag granularity
//! (~17 M docs) + the server gram-count guard (≥ 2 grams = ≥ 3 ASCII / 2 CJK
//! chars) + a seeders-sorted index so the capped top-k returns *desirable* hits.
//!
//! ## No `Count` on the hot path
//!
//! `total_hits` is the page size, not a full match count — a `Count` is an
//! O(match-set) scan the broad case cannot afford (PS-T3 §2.3). We skip it.

use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Value};
use tantivy::{IndexReader, Order, TantivyDocument, Term};
use tonic::Status;

use super::schema::PathFields;
use super::tokenizer::path_grams;
use crate::proto::{PathHit, PathTypeaheadRequest, PathTypeaheadResponse};

/// Default typeahead page size when the request carries no pagination.
const DEFAULT_LIMIT: usize = 20;
/// Hard cap on the typeahead page size (a typeahead never needs more).
const MAX_LIMIT: usize = 50;
/// Minimum tokenized gram count for an **ASCII** query. A query producing fewer
/// grams (a 1-char query → 0 grams; a 2-char ASCII query → 1 bigram, the measured
/// 100–320 ms worst case) is rejected so a hand-crafted short request can't hammer
/// the broadest bigram. A single **CJK** bigram is exempt (see [`gram_guard_ok`]):
/// 2 CJK chars is the minimum meaningful CJK query and is highly selective (the
/// CJK gram vocabulary is huge), so "min-3-chars ASCII / 2-char CJK" (PS-T3 §1).
const MIN_GRAMS: usize = 2;

/// The server-side query-length guard: accept iff the tokenized query has
/// `>= MIN_GRAMS` grams, OR is exactly one CJK (non-ASCII) gram (a 2-char CJK
/// query). Everything shorter (1-char → 0 grams; 2-char ASCII → 1 ASCII bigram)
/// is rejected.
fn gram_guard_ok(grams: &[String]) -> bool {
    grams.len() >= MIN_GRAMS || (grams.len() == 1 && !grams[0].is_ascii())
}

/// Build the gram-conjunction [`Query`] for `grams`: a `BooleanQuery` of
/// `(Must, TermQuery(path_grams, gram, WithFreqs))`. Caller must ensure
/// `grams.len() >= MIN_GRAMS` (see [`path_typeahead`]'s guard).
fn build_path_query(fields: &PathFields, grams: &[String]) -> Box<dyn Query> {
    let clauses: Vec<(Occur, Box<dyn Query>)> = grams
        .iter()
        .map(|g| {
            let q: Box<dyn Query> = Box::new(TermQuery::new(
                Term::from_field_text(fields.path_grams, g),
                IndexRecordOption::WithFreqs,
            ));
            (Occur::Must, q)
        })
        .collect();
    Box::new(BooleanQuery::new(clauses))
}

/// Resolve the effective top-k from the request pagination (default 20, cap 50).
fn limit_offset(request: &PathTypeaheadRequest) -> (usize, usize) {
    match request.pagination.as_ref() {
        None => (DEFAULT_LIMIT, 0),
        Some(p) => {
            let limit = if p.limit == 0 {
                DEFAULT_LIMIT
            } else {
                (p.limit as usize).min(MAX_LIMIT)
            };
            (limit, p.offset as usize)
        }
    }
}

/// Run a path typeahead: tokenize `request.query` with the shared ngram
/// analyzer, reject if it yields `< MIN_GRAMS` grams, build the gram-conjunction,
/// and collect the top-k torrents by seeders (DESC). Each hit's `info_hash` comes
/// from the stored field; `seeders`/`size`/`files_count` from the FAST columns.
///
/// Returns a gRPC [`Status`] directly so the server impl is a thin delegate:
/// `INVALID_ARGUMENT` for a too-short query, `INTERNAL` for a Tantivy error.
pub fn path_typeahead(
    reader: &IndexReader,
    fields: &PathFields,
    request: &PathTypeaheadRequest,
) -> Result<PathTypeaheadResponse, Status> {
    let grams = path_grams(&request.query);
    if !gram_guard_ok(&grams) {
        return Err(Status::invalid_argument(format!(
            "query too short: needs ≥ 3 ASCII chars or ≥ 2 CJK chars (got {} gram(s))",
            grams.len()
        )));
    }

    let (limit, offset) = limit_offset(request);
    let query = build_path_query(fields, &grams);
    let searcher = reader.searcher();

    // Sort by seeders DESC. order_by_fast_field hands back the seeders value as
    // the sort key, so we get it for free (no extra column read for seeders).
    let collector = TopDocs::with_limit(limit)
        .and_offset(offset)
        .order_by_fast_field::<u64>(PathFields::seeders_fast_name(), Order::Desc);

    let top = searcher
        .search(&query, &collector)
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut hits = Vec::with_capacity(top.len());
    for (seeders, addr) in top {
        // order_by_fast_field yields Option (None when the doc has no value).
        let seeders = seeders.unwrap_or(0);
        let segment = searcher.segment_reader(addr.segment_ord);
        let ff = segment.fast_fields();
        let (_, size_name, files_name) = PathFields::fast_names();
        let size = ff
            .u64(size_name)
            .ok()
            .and_then(|c| c.first(addr.doc_id))
            .unwrap_or(0);
        let files_count = ff
            .u64(files_name)
            .ok()
            .and_then(|c| c.first(addr.doc_id))
            .unwrap_or(0);

        let stored: TantivyDocument = searcher
            .doc(addr)
            .map_err(|e| Status::internal(e.to_string()))?;
        let info_hash = stored
            .get_first(fields.info_hash)
            .and_then(|v| v.as_bytes())
            .map(<[u8]>::to_vec)
            .unwrap_or_default();

        hits.push(PathHit {
            info_hash,
            seeders: u32::try_from(seeders).unwrap_or(u32::MAX),
            files_count: u32::try_from(files_count).unwrap_or(u32::MAX),
            size,
            // Relevance is not the rank key here (seeders is); expose 0.0 so
            // callers don't read a meaningless score. (PS-T3: ranking = seeders.)
            score: 0.0,
        });
    }

    let total_hits = hits.len() as u64;
    Ok(PathTypeaheadResponse { hits, total_hits })
}

#[cfg(test)]
mod tests {
    use super::path_typeahead;
    use crate::pathsearch::index::{path_reader, path_writer, register_path_index};
    use crate::pathsearch::indexer::{upsert, PathDoc};
    use crate::pathsearch::schema::{build_path_schema, PathFields};
    use crate::proto::{Pagination, PathTypeaheadRequest};
    use tantivy::Index;

    fn build() -> (tantivy::IndexReader, PathFields) {
        let index = Index::create_in_ram(build_path_schema());
        register_path_index(&index);
        let fields = PathFields::from_schema(&index.schema()).unwrap();
        let mut w = path_writer(&index).unwrap();

        let docs = [
            (&[0x01u8; 20], vec!["Movies/The.Matrix.1999.1080p.mkv".to_owned()], 500u64),
            (&[0x02u8; 20], vec!["Shows/The.Office.S01E01.mkv".to_owned()], 50u64),
            (&[0x03u8; 20], vec!["北京/旅游纪录片.mkv".to_owned()], 10u64),
        ];
        for (hash, paths, seeders) in &docs {
            upsert(
                &w,
                &fields,
                &PathDoc {
                    info_hash: *hash,
                    file_paths: paths,
                    seeders: *seeders,
                    size: 1_000,
                    files_count: 1,
                    name_fallback: "",
                },
            )
            .unwrap();
        }
        w.commit().unwrap();
        let reader = path_reader(&index).unwrap();
        reader.reload().unwrap();
        (reader, fields)
    }

    fn req(q: &str) -> PathTypeaheadRequest {
        PathTypeaheadRequest {
            query: q.to_owned(),
            pagination: Some(Pagination { limit: 20, offset: 0 }),
        }
    }

    #[test]
    fn rejects_short_ascii_but_allows_two_char_cjk() {
        let (reader, fields) = build();
        // 1-char → 0 grams; 2-char ASCII → 1 ASCII bigram → both rejected.
        for q in ["m", "mk"] {
            let status = path_typeahead(&reader, &fields, &req(q)).expect_err("must reject");
            assert_eq!(status.code(), tonic::Code::InvalidArgument, "q={q:?}");
        }
        // 2-char CJK → 1 CJK bigram → ALLOWED (highly selective; PS-T3 guard).
        assert!(
            super::gram_guard_ok(&super::path_grams("北京")),
            "a 2-char CJK query must pass the guard"
        );
        assert!(!super::gram_guard_ok(&super::path_grams("mk")));
    }

    #[test]
    fn ascii_substring_matches_and_ranks_by_seeders() {
        let (reader, fields) = build();
        let resp = path_typeahead(&reader, &fields, &req("matrix")).unwrap();
        assert_eq!(resp.hits.len(), 1);
        assert_eq!(resp.hits[0].info_hash, vec![0x01; 20]);
        assert_eq!(resp.hits[0].seeders, 500);

        // A gram shared by two torrents → both, highest seeders first.
        let resp = path_typeahead(&reader, &fields, &req("mkv")).unwrap();
        assert!(resp.hits.len() >= 2);
        assert!(
            resp.hits[0].seeders >= resp.hits[1].seeders,
            "hits must be ordered by seeders DESC"
        );
    }

    #[test]
    fn cjk_substring_is_findable() {
        // The recall-1.0 property: a mid-run CJK substring matches via char-grams.
        let (reader, fields) = build();
        let resp = path_typeahead(&reader, &fields, &req("北京")).unwrap();
        assert_eq!(resp.hits.len(), 1);
        assert_eq!(resp.hits[0].info_hash, vec![0x03; 20]);
    }

    #[test]
    fn limit_is_capped() {
        let (reader, fields) = build();
        let r = PathTypeaheadRequest {
            query: "mkv".to_owned(),
            pagination: Some(Pagination { limit: 9999, offset: 0 }),
        };
        // Must not panic / over-allocate: the cap keeps the heap bounded.
        let _ = path_typeahead(&reader, &fields, &r).unwrap();
    }
}
