//! Query path for L3 pathsearch candidates.

use std::collections::HashSet;

use anyhow::Context;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, EmptyQuery, Occur, Query, TermQuery};
use tantivy::schema::Value;
use tantivy::tokenizer::TokenStream;
use tantivy::{DocAddress, Index, IndexReader, Order, Score, Searcher, TantivyDocument, Term};

use crate::pathsearch::schema::{Fields, PATH_TOKENIZER};
use crate::proto::{PathCandidate, PathCandidatesRequest, PathCandidatesResponse, SortBy};

const DEFAULT_LIMIT: usize = 50;
const DEFAULT_OVERSAMPLE: usize = 200;
const MAX_CANDIDATES: usize = 5_000;

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

/// Build a path ngram conjunction query.
///
/// Empty/too-short queries intentionally match nothing; a blank path query must
/// not become a full-index scan.
pub fn build_path_query(index: &Index, fields: &Fields, raw: &str) -> Box<dyn Query> {
    let raw = raw.trim();
    if raw.chars().count() < 2 {
        return Box::new(EmptyQuery);
    }

    let Some(mut analyzer) = index.tokenizers().get(PATH_TOKENIZER) else {
        return Box::new(EmptyQuery);
    };
    let mut tokens = Vec::new();
    let mut seen = HashSet::new();
    let mut stream = analyzer.token_stream(raw);
    while stream.advance() {
        let token = stream.token().text.clone();
        if seen.insert(token.clone()) {
            tokens.push(token);
        }
    }

    if tokens.is_empty() {
        return Box::new(EmptyQuery);
    }

    let clauses = tokens
        .into_iter()
        .map(|token| {
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(fields.path, &token),
                    tantivy::schema::IndexRecordOption::WithFreqs,
                )) as Box<dyn Query>,
            )
        })
        .collect();
    Box::new(BooleanQuery::new(clauses))
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
        PathDocument {
            info_hash: vec![byte; 20],
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
