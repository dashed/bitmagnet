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

/// Maximum number of per-word recall clauses built from a multi-word query.
///
/// Each per-word clause is an independent gram conjunction, and the sidecar
/// search is synchronous and uncancellable, so an unbounded word count is a DoS
/// amplifier (≈50 dense 2-char words project to ~12.7s on the 48.8M-doc prod
/// index; abandoned requests keep burning workers). Capping at 12 bounds the
/// worst case to ≈ K × dense-scan ≈ 2.4s, under the Go side's 5s RPC-abandon
/// deadline. Dropping the least-selective words only WIDENS recall — F11 refine
/// re-enforces the FULL token set downstream — so precision is unaffected.
const MAX_RECALL_WORDS: usize = 12;

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

/// Build the ngram query that recalls a torrent when EVERY whitespace-separated
/// query word appears in EITHER a file path OR the torrent display name (F1 name
/// visibility, F12 per-word recall).
///
/// The raw query is first split on whitespace into words. For each word its
/// de-duplicated gram set is built once and required (`Must`) as a conjunction
/// over the `path` field and, separately, over the `name` field; those two
/// conjunctions are combined as `Should` clauses so the word matches when it
/// appears in either field — with the name conjunction wrapped in a
/// [`BoostQuery`] (weight [`name_boost`]) so name matches rank above path-only
/// matches under `order_by_score`. The per-word clauses are then ANDed
/// (`Must`) so a doc is a candidate only when all words are present, though each
/// word may land in a different field and the words need not be adjacent.
///
/// A single-word query yields exactly one per-word clause, returned directly so
/// its structure — `Should(path) OR Should(boosted name)` — is identical to the
/// pre-F12 whole-string form.
///
/// Words too short to produce grams (fewer than 2 chars) are skipped for recall;
/// they are still enforced by the downstream refine (F11). When EVERY word is
/// too short (e.g. `"a b"`) the per-word loop yields no clauses, so we fall back
/// to the pre-F12 whole-string gram set — those space-crossing grams recall the
/// same docs the old query did, keeping recall an unconditional superset of
/// refine. Empty/whitespace-only queries still match nothing; a blank query must
/// not become a full-index scan.
///
/// At most [`MAX_RECALL_WORDS`] per-word clauses are built; excess words are
/// dropped from recall (keeping the most-selective, i.e. gram-richest, words).
/// This only widens recall, so refine downstream keeps precision intact.
pub fn build_path_query(index: &Index, fields: &Fields, raw: &str) -> Box<dyn Query> {
    // Gram set of every recallable word, in first-occurrence order.
    let mut words: Vec<Vec<String>> = raw
        .split_whitespace()
        .filter_map(|word| gram_tokens(index, word))
        .collect();

    // DoS guard: cap the per-word clause count. When more words than the cap
    // yield grams, keep the K with the MOST grams (longest ⇒ most selective ⇒
    // cheapest to scan and most useful), tie-broken by first-occurrence order
    // for determinism, then restore query order for stable clause layout.
    if words.len() > MAX_RECALL_WORDS {
        let mut ranked: Vec<(usize, Vec<String>)> = words.into_iter().enumerate().collect();
        ranked.sort_by(|(ai, a), (bi, b)| b.len().cmp(&a.len()).then_with(|| ai.cmp(bi)));
        ranked.truncate(MAX_RECALL_WORDS);
        ranked.sort_by_key(|(i, _)| *i);
        words = ranked.into_iter().map(|(_, tokens)| tokens).collect();
    }

    let mut word_clauses: Vec<(Occur, Box<dyn Query>)> = words
        .iter()
        .map(|tokens| (Occur::Must, word_clause(fields, tokens)))
        .collect();

    match word_clauses.len() {
        // No word produced grams (all sub-2-char, or empty). Recall via the
        // whole trimmed string's grams so short spaced queries stay a superset
        // of refine; a truly empty/whitespace-only query yields None ⇒ nothing.
        0 => match gram_tokens(index, raw) {
            Some(tokens) => word_clause(fields, &tokens),
            None => Box::new(EmptyQuery),
        },
        // A lone word IS the whole query: return its clause unwrapped so the
        // top-level structure matches the pre-F12 single-string query exactly.
        1 => word_clauses.pop().expect("len checked == 1").1,
        _ => Box::new(BooleanQuery::new(word_clauses)),
    }
}

/// Build the per-word recall clause: the word's gram conjunction over `path`
/// combined (`Should`) with its boosted conjunction over `name`, so the word
/// matches in either field.
fn word_clause(fields: &Fields, tokens: &[String]) -> Box<dyn Query> {
    let path_clause = gram_conjunction(fields.path, tokens);
    let name_clause = gram_conjunction(fields.name, tokens);
    let boosted_name = BoostQuery::new(Box::new(name_clause), name_boost());

    Box::new(BooleanQuery::new(vec![
        (Occur::Should, Box::new(path_clause) as Box<dyn Query>),
        (Occur::Should, Box::new(boosted_name) as Box<dyn Query>),
    ]))
}

/// Tokenize a single query `word` into the de-duplicated gram set used for both
/// field clauses.
///
/// Returns `None` when the word is too short (⇒ skipped for recall), the
/// tokenizer is unregistered, or it produces no grams.
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

    /// Convenience: run a query and return the matching info_hash byte-tags.
    fn recalled_tags(
        index: &Index,
        reader: &tantivy::IndexReader,
        fields: &Fields,
        query: &str,
    ) -> Vec<u8> {
        let out = run_path_candidates(
            index,
            reader,
            fields,
            PathCandidatesRequest {
                query: query.to_owned(),
                limit: 50,
                oversample: 0,
                sort: Vec::new(),
            },
        )
        .unwrap();
        let mut tags: Vec<u8> = out.candidates.iter().map(|c| c.info_hash[0]).collect();
        tags.sort_unstable();
        assert_eq!(tags.len() as u64, out.candidate_total);
        tags
    }

    #[test]
    fn single_word_regression_recall_unchanged() {
        // A single-word query must recall exactly the doc containing the word and
        // exclude one that does not — the F12 single-clause path is structurally
        // the pre-F12 query.
        let (index, reader, fields) = index_docs(&[
            doc_named(1, "Aurora.2021.1080p", &["films/movie.mkv"], 5),
            doc_named(2, "Unrelated.Release", &["films/other.mkv"], 5),
        ]);
        assert_eq!(recalled_tags(&index, &reader, &fields, "aurora"), vec![1]);
    }

    #[test]
    fn multiword_non_adjacent_recalled() {
        // Name reads "OmegaPACK by SoreForDays": the two query words appear in the
        // name but NOT space-adjacent and in reversed order. Pre-F12 the
        // whole-string gram AND (with the "by" and inter-word grams) dropped this
        // doc; F12 recalls it because each word matches independently.
        let (index, reader, fields) = index_docs(&[doc_named(
            1,
            "OmegaPACK by SoreForDays",
            &["disc1/track01.flac"],
            5,
        )]);
        assert_eq!(
            recalled_tags(&index, &reader, &fields, "sorefordays omegapack"),
            vec![1],
            "reordered non-adjacent multi-word query must recall the doc"
        );
        // A word genuinely absent still excludes the doc — recall is an AND.
        assert_eq!(
            recalled_tags(&index, &reader, &fields, "omegapack missingword"),
            Vec::<u8>::new(),
            "an absent word must drop the candidate"
        );
    }

    #[test]
    fn word_split_across_name_and_path_recalled() {
        // "aurora" appears only in the name, "borealis" only in a file path.
        // Recall must union across fields per word and AND across words.
        let (index, reader, fields) =
            index_docs(&[doc_named(3, "Aurora Complete", &["disc/borealis.flac"], 5)]);
        assert_eq!(
            recalled_tags(&index, &reader, &fields, "aurora borealis"),
            vec![3]
        );
    }

    #[test]
    fn sub_two_char_word_skipped_for_recall() {
        // "Grab" contains the grams for "ab" but no "c". The 1-char word "c"
        // yields no grams and is skipped, so "ab c" recalls exactly as "ab".
        let (index, reader, fields) = index_docs(&[doc_named(4, "Grab", &["misc/file.mkv"], 5)]);
        let just_ab = recalled_tags(&index, &reader, &fields, "ab");
        assert_eq!(just_ab, vec![4]);
        assert_eq!(
            recalled_tags(&index, &reader, &fields, "ab c"),
            just_ab,
            "a sub-2-char word must not affect recall"
        );
    }

    #[test]
    fn all_short_words_fall_back_to_whole_string() {
        // Every whitespace token is a single char, so the per-word loop yields no
        // clauses. Recall must fall back to the pre-F12 whole-string grams
        // ("a ", " b", "a b") rather than collapsing to EmptyQuery — F11 refine
        // would keep such docs, so recall must too.
        let (index, reader, fields) = index_docs(&[
            doc_named(1, "a b canonical", &["disc/track.flac"], 5),
            doc_named(2, "zztop unrelated", &["disc/other.flac"], 5),
        ]);
        assert_eq!(recalled_tags(&index, &reader, &fields, "a b"), vec![1]);
    }

    #[test]
    fn whitespace_only_query_is_empty() {
        let (index, reader, fields) = index_docs(&[doc(1, &["Show.mkv"], 5)]);
        for q in ["", "   ", "\t \n"] {
            let out = run_path_candidates(
                &index,
                &reader,
                &fields,
                PathCandidatesRequest {
                    query: q.to_owned(),
                    limit: 10,
                    oversample: 0,
                    sort: Vec::new(),
                },
            )
            .unwrap();
            assert_eq!(out.candidate_total, 0, "query {q:?} must recall nothing");
            assert!(out.candidates.is_empty());
        }
    }

    #[test]
    fn over_cap_drops_least_selective_words_and_widens_recall() {
        // K+2 gram-yielding words: 12 gram-rich words that ARE in the doc, plus
        // two 2-char words (fewest grams) that are NOT. With MAX_RECALL_WORDS=12
        // the two shortest are dropped from recall, so the doc is still recalled
        // — proving the cap WIDENS recall. An un-capped 14-way AND would exclude
        // the doc on the two absent words.
        let present: [&str; 12] = [
            "wolfgang",
            "tangerine",
            "mango",
            "papaya",
            "kiwifruit",
            "blueberry",
            "raspberry",
            "cranberry",
            "pineapple",
            "coconut",
            "apricot",
            "nectarine",
        ];
        let name = present.join(" ");
        let (index, reader, fields) =
            index_docs(&[doc_named(1, &name, &["disc/track.flac"], 5)]);

        // Two absent 2-char words bracket the gram-rich ones; neither substring
        // occurs in the doc, so an un-capped query would drop it.
        let query = format!("zz {} qq", present.join(" "));
        assert_eq!(recalled_tags(&index, &reader, &fields, &query), vec![1]);
    }

    #[test]
    fn recall_is_deterministic() {
        // The clause-selection ordering is deterministic, so the same query must
        // yield the same candidate set on repeated runs.
        let (index, reader, fields) = index_docs(&[
            doc_named(1, "Aurora Complete", &["disc/borealis.flac"], 5),
            doc_named(2, "zztop unrelated", &["disc/other.flac"], 5),
        ]);
        let first = recalled_tags(&index, &reader, &fields, "aurora borealis");
        let second = recalled_tags(&index, &reader, &fields, "aurora borealis");
        assert_eq!(first, second);
        assert_eq!(first, vec![1]);
    }
}
