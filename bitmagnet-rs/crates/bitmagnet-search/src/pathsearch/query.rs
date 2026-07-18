//! Query path for L3 pathsearch candidates.

use std::collections::HashSet;

use anyhow::Context;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, BoostQuery, EmptyQuery, Occur, Query, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Value};
use tantivy::tokenizer::TokenStream;
use tantivy::{DocAddress, Index, IndexReader, Order, Score, Searcher, TantivyDocument, Term};

use crate::pathsearch::schema::{Fields, PATH_TOKENIZER};
use crate::proto::{PathCandidate, PathCandidatesRequest, PathCandidatesResponse, SortBy};

const DEFAULT_LIMIT: usize = 50;
const DEFAULT_OVERSAMPLE: usize = 200;
const MAX_CANDIDATES: usize = 5_000;

/// Environment override for the relevance boost applied to torrent-name matches
/// relative to file-path matches. See [`name_boost`].
const NAME_BOOST_ENV: &str = "BITMAGNET_PATHSEARCH_NAME_BOOST";

/// Default multiplier by which a name-field gram match outscores a path-field
/// match. >1 so a torrent whose display name contains the term ranks above one
/// where the term only appears deep in a file path, while both remain candidates.
const DEFAULT_NAME_BOOST: f32 = 1.75;

/// Resolve the name-match relevance boost from [`NAME_BOOST_ENV`], falling back
/// to [`DEFAULT_NAME_BOOST`]. Non-finite or non-positive overrides are ignored.
fn name_boost() -> f32 {
    std::env::var(NAME_BOOST_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<f32>().ok())
        .filter(|w| w.is_finite() && *w > 0.0)
        .unwrap_or(DEFAULT_NAME_BOOST)
}

/// Run a path candidate search.
///
/// The returned `candidate_total` is a torrent-doc count, not an exact matching
/// file count. The backend must exact-refine via L1/L2.
///
/// # Errors
/// Returns Tantivy retrieval/search errors.
pub fn run_path_candidates(
    index: &Index,
    reader: &IndexReader,
    fields: &Fields,
    request: PathCandidatesRequest,
) -> anyhow::Result<PathCandidatesResponse> {
    let query = build_path_query(index, fields, &request.query);
    let searcher = reader.searcher();
    let candidate_total = searcher.search(&*query, &Count)? as u64;
    let limit = candidate_limit(request.limit, request.oversample);
    let candidates = if limit == 0 {
        Vec::new()
    } else {
        collect_candidates(&searcher, fields, &*query, limit, &request.sort)?
    };
    Ok(PathCandidatesResponse {
        candidates,
        candidate_total,
        estimated: true,
    })
}

/// Build the ngram query that recalls a torrent when the term appears in EITHER
/// a file path OR the torrent display name (F1 name visibility).
///
/// The same gram-token set is built once, then required (`Must`) as a
/// conjunction over the `path` field and, separately, over the `name` field. The
/// two conjunctions are combined as top-level `Should` clauses so a doc is a
/// candidate if it matches either — and the name conjunction is wrapped in a
/// [`BoostQuery`] (weight [`name_boost`]) so name matches rank above path-only
/// matches under `order_by_score`.
///
/// Empty/too-short queries intentionally match nothing; a blank query must not
/// become a full-index scan.
pub fn build_path_query(index: &Index, fields: &Fields, raw: &str) -> Box<dyn Query> {
    let Some(tokens) = gram_tokens(index, raw) else {
        return Box::new(EmptyQuery);
    };

    let path_clause = gram_conjunction(fields.path, &tokens);
    let name_clause = gram_conjunction(fields.name, &tokens);
    let boosted_name = BoostQuery::new(Box::new(name_clause), name_boost());

    Box::new(BooleanQuery::new(vec![
        (Occur::Should, Box::new(path_clause) as Box<dyn Query>),
        (Occur::Should, Box::new(boosted_name) as Box<dyn Query>),
    ]))
}

/// Tokenize `raw` into the de-duplicated gram set used for both field clauses.
///
/// Returns `None` (⇒ [`EmptyQuery`]) when the query is too short, the tokenizer
/// is unregistered, or it produces no grams.
fn gram_tokens(index: &Index, raw: &str) -> Option<Vec<String>> {
    let raw = raw.trim();
    if raw.chars().count() < 2 {
        return None;
    }

    let mut analyzer = index.tokenizers().get(PATH_TOKENIZER)?;
    let mut tokens = Vec::new();
    let mut seen = HashSet::new();
    let mut stream = analyzer.token_stream(raw);
    while stream.advance() {
        let token = stream.token().text.clone();
        if seen.insert(token.clone()) {
            tokens.push(token);
        }
    }

    (!tokens.is_empty()).then_some(tokens)
}

/// Build a `Must` conjunction of gram term-queries over a single `field`.
fn gram_conjunction(field: Field, tokens: &[String]) -> BooleanQuery {
    let clauses = tokens
        .iter()
        .map(|token| {
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(field, token),
                    IndexRecordOption::WithFreqs,
                )) as Box<dyn Query>,
            )
        })
        .collect();
    BooleanQuery::new(clauses)
}

fn candidate_limit(limit: u32, oversample: u32) -> usize {
    let limit = if limit == 0 {
        DEFAULT_LIMIT
    } else {
        limit as usize
    };
    let oversample = if oversample == 0 {
        DEFAULT_OVERSAMPLE
    } else {
        oversample as usize
    };
    limit.saturating_add(oversample).min(MAX_CANDIDATES)
}

#[derive(Clone, Copy)]
enum FastType {
    U64,
    I64,
}

fn sort_key(sort: &[SortBy]) -> Option<(&'static str, FastType, Order)> {
    let first = sort.first()?;
    let order = if first.descending {
        Order::Desc
    } else {
        Order::Asc
    };
    match first.field.as_str() {
        "seeders" => Some(("seeders", FastType::U64, order)),
        "size" => Some(("size", FastType::U64, order)),
        "files_count" => Some(("files_count", FastType::U64, order)),
        "published_at" => Some(("published_at", FastType::I64, order)),
        _ => None,
    }
}

fn collect_candidates(
    searcher: &Searcher,
    fields: &Fields,
    query: &dyn Query,
    limit: usize,
    sort: &[SortBy],
) -> anyhow::Result<Vec<PathCandidate>> {
    match sort_key(sort) {
        None => searcher
            .search(query, &TopDocs::with_limit(limit).order_by_score())?
            .into_iter()
            .map(|(score, addr)| candidate(searcher, fields, addr, score, 0))
            .collect(),
        Some((name, FastType::U64, order)) => searcher
            .search(
                query,
                &TopDocs::with_limit(limit).order_by_fast_field::<u64>(name, order),
            )?
            .into_iter()
            .map(|(sort_value, addr)| {
                candidate(searcher, fields, addr, 0.0, sort_value.unwrap_or_default())
            })
            .collect(),
        Some((name, FastType::I64, order)) => searcher
            .search(
                query,
                &TopDocs::with_limit(limit).order_by_fast_field::<i64>(name, order),
            )?
            .into_iter()
            .map(|(sort_value, addr)| {
                let value = sort_value
                    .and_then(|v| u64::try_from(v).ok())
                    .unwrap_or_default();
                candidate(searcher, fields, addr, 0.0, value)
            })
            .collect(),
    }
}

fn candidate(
    searcher: &Searcher,
    fields: &Fields,
    addr: DocAddress,
    score: Score,
    sort_value: u64,
) -> anyhow::Result<PathCandidate> {
    let doc: TantivyDocument = searcher.doc(addr)?;
    let info_hash = doc
        .get_first(fields.info_hash)
        .and_then(|v| v.as_bytes())
        .map(<[u8]>::to_vec)
        .context("pathsearch hit missing stored info_hash")?;
    Ok(PathCandidate {
        info_hash,
        score,
        sort_value,
    })
}

#[cfg(test)]
mod tests {
    use super::run_path_candidates;
    use crate::pathsearch::document::PathDocument;
    use crate::pathsearch::index::{reader, writer};
    use crate::pathsearch::indexer::upsert;
    use crate::pathsearch::schema::{build_schema, register_tokenizer, Fields};
    use crate::proto::PathCandidatesRequest;
    use tantivy::Index;

    fn doc(byte: u8, paths: &[&str], seeders: u64) -> PathDocument {
        doc_named(byte, "", paths, seeders)
    }

    fn doc_named(byte: u8, name: &str, paths: &[&str], seeders: u64) -> PathDocument {
        PathDocument {
            info_hash: vec![byte; 20],
            name: name.to_owned(),
            paths: paths.iter().map(|p| (*p).to_owned()).collect(),
            size: 1_000,
            files_count: paths.len() as u64,
            seeders,
            published_at: 1_600_000_000,
        }
    }

    fn index_docs(docs: &[PathDocument]) -> (Index, tantivy::IndexReader, Fields) {
        let index = Index::create_in_ram(build_schema());
        register_tokenizer(&index).unwrap();
        let fields = Fields::from_schema(&index.schema()).unwrap();
        let mut w = writer(&index, 256 * 1024 * 1024, 1).unwrap();
        for d in docs {
            upsert(&w, &fields, d).unwrap();
        }
        w.commit().unwrap();
        let r = reader(&index).unwrap();
        r.reload().unwrap();
        (index, r, fields)
    }

    #[test]
    fn ascii_substring_returns_candidate() {
        let (index, reader, fields) = index_docs(&[doc(1, &["Show.S01E01.1080p.mkv"], 5)]);
        let out = run_path_candidates(
            &index,
            &reader,
            &fields,
            PathCandidatesRequest {
                query: "s01e01".to_owned(),
                limit: 10,
                oversample: 0,
                sort: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(out.candidate_total, 1);
        assert_eq!(out.candidates[0].info_hash, vec![1; 20]);
        assert!(out.estimated);
    }

    #[test]
    fn name_only_match_returns_candidate() {
        // OmegaPACK-shaped: a multi-file torrent whose files never contain the
        // term, but whose display name does. Before F1 this doc was un-indexed
        // for the term and invisible on the relevance route.
        let (index, reader, fields) = index_docs(&[doc_named(
            1,
            "OmegaPACK.SoreForDays.Complete",
            &["disc1/track01.flac", "disc1/track02.flac"],
            5,
        )]);
        let out = run_path_candidates(
            &index,
            &reader,
            &fields,
            PathCandidatesRequest {
                query: "sorefordays".to_owned(),
                limit: 10,
                oversample: 0,
                sort: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(out.candidate_total, 1);
        assert_eq!(out.candidates[0].info_hash, vec![1; 20]);
    }

    #[test]
    fn no_info_name_only_doc_is_candidate() {
        // no_info-shaped: EMPTY paths, name only. Proves an empty-path doc is
        // still recallable purely by its name field (the ~21.9M no_info case).
        let (index, reader, fields) =
            index_docs(&[doc_named(7, "OmegaPACK.SoreForDays.Complete", &[], 5)]);
        let out = run_path_candidates(
            &index,
            &reader,
            &fields,
            PathCandidatesRequest {
                query: "sorefordays".to_owned(),
                limit: 10,
                oversample: 0,
                sort: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(out.candidate_total, 1);
        assert_eq!(out.candidates[0].info_hash, vec![7; 20]);
    }

    #[test]
    fn name_match_outranks_path_only_match() {
        // Two docs match "aurora": one only in a file path, one in its name. The
        // name-boosted doc must sort first under relevance (order_by_score).
        let (index, reader, fields) = index_docs(&[
            doc_named(1, "unrelated release", &["films/aurora.borealis.mkv"], 5),
            doc_named(2, "Aurora.2021.1080p", &["films/movie.mkv"], 5),
        ]);
        let out = run_path_candidates(
            &index,
            &reader,
            &fields,
            PathCandidatesRequest {
                query: "aurora".to_owned(),
                limit: 10,
                oversample: 0,
                sort: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(out.candidate_total, 2);
        assert_eq!(
            out.candidates[0].info_hash,
            vec![2; 20],
            "name match must outrank the path-only match"
        );
    }

    #[test]
    fn short_query_is_not_full_scan() {
        let (index, reader, fields) = index_docs(&[doc(1, &["a.mkv"], 5)]);
        let out = run_path_candidates(
            &index,
            &reader,
            &fields,
            PathCandidatesRequest {
                query: "a".to_owned(),
                limit: 10,
                oversample: 0,
                sort: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(out.candidate_total, 0);
        assert!(out.candidates.is_empty());
    }
}
