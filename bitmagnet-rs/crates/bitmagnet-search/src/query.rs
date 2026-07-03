//! Translation from bitmagnet's search syntax into Tantivy queries, plus the
//! `Search` RPC entry point the server delegates to.
//!
//! ## Two-stage translation (mirrors the Go pipeline)
//!
//! bitmagnet's Postgres search does *not* feed the user's query straight to the
//! engine. It runs `fts.AppQueryToTsquery` to turn the user-facing **app query**
//! (the `+`/`|`/`.`/`!`/`*`/quote/paren syntax) into a Postgres `tsquery`
//! *string*, then Postgres matches `tsv @@ tsquery` and ranks with `ts_rank_cd`
//! (see `internal/database/query/query.go`). We reproduce both halves:
//!
//! 1. [`app_query_to_tsquery`] is a faithful port of Go `AppQueryToTsquery`
//!    (`internal/database/fts/tsquery.go`): it lexes the app query and emits the
//!    exact same `tsquery` string Postgres would see. The operators are the ones
//!    the Go lexer recognises — `&` (and; also the default between bare words),
//!    `|` (or), `.` (followed-by → `<->`), `!` (negation), `*` (prefix → `:*`),
//!    plus quotes and parens. Each word is tokenised through the *shared*
//!    [`crate::tokenizer`] (the identical routine the index writer registers as
//!    its analyzer), so query lexemes equal indexed terms byte-for-byte.
//! 2. [`tsquery_to_tantivy`] parses that `tsquery` string with Postgres operator
//!    precedence (`!` > `<->` > `&` > `|`) into a Tantivy [`Query`]. Plain
//!    lexemes are searched across the four weight tiers `text_a..d` with the
//!    A/B/C/D boosts (a [`DisjunctionMaxQuery`] of per-tier [`BoostQuery`]s).
//!    Phrase-class queries (`<->` chains and trailing-prefix `:*`) are
//!    constrained to `text_a`, the only tier that stores positions.
//!
//! [`run_search`] glues the two together, applies the structured
//! [`SearchFilters`], paginates, sorts, and reconstructs each hit's
//! [`TorrentDocument`] from the STORED fields (`file_paths` is intentionally not
//! retrievable — it only feeds relevance).

use std::ops::Bound;

use tantivy::collector::{Count, TopDocs};
use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, DisjunctionMaxQuery, Occur, PhrasePrefixQuery, PhraseQuery,
    Query, RangeQuery, TermQuery,
};
use tantivy::schema::{Field, IndexRecordOption, Schema, Value};
use tantivy::{DocAddress, Index, IndexReader, Order, Score, Searcher, TantivyDocument, Term};

use bitmagnet_model::ContentType as ModelContentType;

use crate::proto::{
    Pagination, SearchFilters, SearchHit, SearchRequest, SearchResponse, SortBy, TorrentDocument,
};
use crate::schema::{Fields, BOOST_A};
use crate::tokenizer::tokenize_flat;

/// Page size used when a request carries no [`Pagination`] message at all.
const DEFAULT_LIMIT: usize = 20;
/// Upper bound on the requested page size; `TopDocs` allocates a heap of this
/// order, so an unbounded client value must not be honoured verbatim.
const MAX_LIMIT: usize = 10_000;
/// `DisjunctionMaxQuery` tie-breaker: the best-scoring tier dominates, with a
/// small bonus for the term also matching in other tiers. Approximates the
/// additive nature of PG `ts_rank` without fully summing the tiers.
const TIE_BREAKER: Score = 0.3;
/// Cap on prefix (`:*`) term expansions, bounding each `PhrasePrefixQuery`.
/// Postgres prefix matches are unbounded; Tantivy requires a cap, so prefixes
/// with more than this many distinct indexed completions may silently miss the
/// tail during shadow-mode PG parity comparisons. Tantivy does not appear to
/// expose whether the cap was hit.
const PREFIX_MAX_EXPANSIONS: u32 = 2_048;
/// Bound phrase-over-group distribution so `(a|...) <-> (b|...)` cannot explode.
const PHRASE_GROUP_MAX_COMBINATIONS: usize = 64;

// ===========================================================================
// Public entry points
// ===========================================================================

/// Run a full search: translate `request.query`, AND-in `request.filters`,
/// paginate, sort, and collect ranked hits into a [`SearchResponse`]. This is
/// the entry point [`crate::server::SearchServer`] delegates the `Search` RPC to.
///
/// Ordering is by relevance (the weight-tier boosted score) unless
/// `request.sort` names a fast field (`size`, `seeders`, `leechers`,
/// `files_count`, `release_year`, `published_at`); only the first sort key is
/// honoured. `total_hits` is the full match count, independent of pagination.
///
/// # Errors
/// Returns an error if the search or document retrieval fails.
pub fn run_search(
    _index: &Index,
    reader: &IndexReader,
    fields: &Fields,
    request: SearchRequest,
) -> anyhow::Result<SearchResponse> {
    let searcher = reader.searcher();
    let query = build_search_query(fields, &request.query, request.filters.as_ref());

    // Total matches, ignoring pagination (Count skips scoring/enumeration).
    let total_hits = searcher.search(&*query, &Count)? as u64;

    let (limit, offset) = pagination(request.pagination.as_ref());
    let hits = if limit == 0 {
        Vec::new()
    } else {
        collect_hits(&searcher, fields, &*query, limit, offset, &request.sort)?
    };

    Ok(SearchResponse { hits, total_hits })
}

/// Parse a Postgres-style `tsquery` string (as produced by
/// [`app_query_to_tsquery`]) into a Tantivy [`Query`] over the weight tiers.
///
/// Supports lexemes (bare or `'single-quoted'`), the prefix marker `:*`, the
/// operators `!` `<->` `&` `|`, and parentheses, parsed with Postgres precedence
/// (`!` > `<->` > `&` > `|`). An empty/blank query becomes an [`AllQuery`].
///
/// # Errors
/// Returns a [`tantivy::TantivyError`] if `schema` is missing a tier field — i.e.
/// it was not produced by [`crate::schema::build_schema`].
pub fn tsquery_to_tantivy(schema: &Schema, tsquery: &str) -> tantivy::Result<Box<dyn Query>> {
    let fields = Fields::from_schema(schema)?;
    Ok(lower(&parse_tsquery(tsquery), &fields))
}

/// Port of Go `fts.AppQueryToTsquery`: turn a user-facing app query into the
/// Postgres `tsquery` string Postgres would match against. Pure and
/// deterministic, so it is asserted byte-for-byte against the Go test vectors.
#[must_use]
pub fn app_query_to_tsquery(raw: &str) -> String {
    tokens_to_tsquery(&lex_app_query(raw))
}

// ===========================================================================
// Shared query builders (used by both `run_search` and `crate::facets`)
// ===========================================================================

/// Translate the user's app query into a boosted multi-tier Tantivy query
/// (relevance only — no structured filters). An empty query yields [`AllQuery`].
pub(crate) fn build_text_query(fields: &Fields, raw_query: &str) -> Box<dyn Query> {
    lower(&parse_tsquery(&app_query_to_tsquery(raw_query)), fields)
}

/// Build the full read query: the text relevance query AND-ed with the
/// structured [`SearchFilters`]. Shared by the `Search` and `GetFacets` paths so
/// facet counts reflect the same active filters.
pub(crate) fn build_search_query(
    fields: &Fields,
    raw_query: &str,
    filters: Option<&SearchFilters>,
) -> Box<dyn Query> {
    let text = build_text_query(fields, raw_query);

    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
    if let Some(f) = filters {
        push_keyword_filter(&mut clauses, fields.content_type, &content_type_strings(f));
        push_keyword_filter(&mut clauses, fields.languages, &f.languages);
        push_keyword_filter(&mut clauses, fields.file_extensions, &f.file_extensions);
        push_keyword_filter(&mut clauses, fields.video_resolution, &f.video_resolutions);
        push_u64_range(
            &mut clauses,
            fields.release_year,
            f.release_year_min.map(u64::from),
            f.release_year_max.map(u64::from),
        );
        push_u64_range(&mut clauses, fields.size, f.size_min, f.size_max);
        push_u64_range(
            &mut clauses,
            fields.seeders,
            f.seeders_min.map(u64::from),
            None,
        );
    }

    if clauses.is_empty() {
        text
    } else {
        clauses.insert(0, (Occur::Must, text));
        Box::new(BooleanQuery::new(clauses))
    }
}

/// Canonical content-type strings (e.g. `"movie"`) for the proto enum filter
/// values, dropping the `UNKNOWN`/invalid sentinel.
fn content_type_strings(filters: &SearchFilters) -> Vec<String> {
    filters
        .content_types
        .iter()
        .filter_map(|&v| ModelContentType::from_proto_value(v))
        .map(|ct| ct.as_str().to_owned())
        .collect()
}

/// Append an exact-match keyword filter: a document matches if the field equals
/// *any* of `values` (OR within the facet), and the whole set is required (AND
/// across facets). No-op for an empty value set.
fn push_keyword_filter(
    clauses: &mut Vec<(Occur, Box<dyn Query>)>,
    field: Field,
    values: &[String],
) {
    let mut subs: Vec<Box<dyn Query>> = values
        .iter()
        .filter(|v| !v.is_empty())
        .map(|v| {
            Box::new(TermQuery::new(
                Term::from_field_text(field, v),
                IndexRecordOption::Basic,
            )) as Box<dyn Query>
        })
        .collect();

    match subs.len() {
        0 => {}
        1 => clauses.push((Occur::Must, subs.pop().expect("len == 1"))),
        _ => clauses.push((Occur::Must, Box::new(BooleanQuery::union(subs)))),
    }
}

/// Append an inclusive `[min, max]` range filter over a `u64` fast field. No-op
/// when both bounds are absent.
fn push_u64_range(
    clauses: &mut Vec<(Occur, Box<dyn Query>)>,
    field: Field,
    min: Option<u64>,
    max: Option<u64>,
) {
    if min.is_none() && max.is_none() {
        return;
    }
    let lower = min.map_or(Bound::Unbounded, |v| {
        Bound::Included(Term::from_field_u64(field, v))
    });
    let upper = max.map_or(Bound::Unbounded, |v| {
        Bound::Included(Term::from_field_u64(field, v))
    });
    clauses.push((Occur::Must, Box::new(RangeQuery::new(lower, upper))));
}

// ===========================================================================
// Stage 1: app query  ->  Postgres tsquery string  (port of Go tsquery.go)
// ===========================================================================

/// Lexed app-query tokens, mirroring Go `queryLexer.readQueryToken`.
#[derive(Clone, Debug, PartialEq, Eq)]
enum QToken {
    OpenParens,
    CloseParens,
    /// `&` — and (also the default operator between two operands).
    And,
    /// `|` — or.
    Or,
    /// `.` — followed-by (`<->`).
    FollowedBy,
    /// `!` — negation.
    Negation,
    /// `*` — prefix wildcard.
    Wildcard,
    /// A `"…"` quoted run (value is the raw, un-tokenised content).
    Quoted(String),
    /// A maximal run of word characters (value is the raw run).
    Phrase(String),
}

/// Pending connective between two emitted operands.
#[derive(Clone, Copy)]
enum PendingOp {
    And,
    Or,
    FollowedBy,
}

/// Word-character test for the app-query lexer, approximating Go
/// `lexer.IsWordChar` (`unicode.IsLetter || unicode.IsDigit`). `is_alphanumeric`
/// agrees on every letter and decimal digit (Latin, CJK, Cyrillic, Arabic, and
/// e.g. Arabic-Indic digits); it is marginally broader on a few exotic numeric
/// categories (`Nl`/`No`, e.g. `Ⅷ`, `½`), which would only ever flip a `&` to a
/// `<->` for such a character wedged between letters with no separator — the
/// final lexemes are re-derived by [`tokenize_flat`] regardless.
fn is_query_word_char(c: char) -> bool {
    c.is_alphanumeric()
}

/// Lex `raw` into [`QToken`]s. Faithful to Go's lexer: operator chars first,
/// then a `"`-quoted string, then a word run, else skip one character.
fn lex_app_query(raw: &str) -> Vec<QToken> {
    let chars: Vec<char> = raw.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        match c {
            '(' => {
                out.push(QToken::OpenParens);
                i += 1;
            }
            ')' => {
                out.push(QToken::CloseParens);
                i += 1;
            }
            '&' => {
                out.push(QToken::And);
                i += 1;
            }
            '|' => {
                out.push(QToken::Or);
                i += 1;
            }
            '.' => {
                out.push(QToken::FollowedBy);
                i += 1;
            }
            '!' => {
                out.push(QToken::Negation);
                i += 1;
            }
            '*' => {
                out.push(QToken::Wildcard);
                i += 1;
            }
            '"' => {
                let (value, next) = read_quoted(&chars, i);
                i = next;
                // Go only emits a TokenQuoted when the content is non-empty;
                // an empty `""` is consumed and skipped.
                if !value.is_empty() {
                    out.push(QToken::Quoted(value));
                }
            }
            _ if is_query_word_char(c) => {
                let start = i;
                while i < chars.len() && is_query_word_char(chars[i]) {
                    i += 1;
                }
                out.push(QToken::Phrase(chars[start..i].iter().collect()));
            }
            _ => i += 1, // unrecognised separator — skip (Go `l.Read()`).
        }
    }

    out
}

/// Read a quoted run starting at the opening quote `chars[start]`, returning the
/// unescaped content and the index just past the closing quote (or EOF). A
/// doubled quote (`""`) is an escaped literal quote. Mirrors Go
/// `ftsLexer.readQuotedString`, including its lenient handling of an unterminated
/// quote (returns the content read so far).
fn read_quoted(chars: &[char], start: usize) -> (String, usize) {
    let quote = chars[start];
    let mut i = start + 1;
    let mut value = String::new();

    while i < chars.len() {
        let ch = chars[i];
        i += 1;
        if ch == quote {
            if i < chars.len() && chars[i] == quote {
                i += 1; // escaped quote: consume the second, keep one.
                value.push(ch);
            } else {
                return (value, i); // closing quote.
            }
        } else {
            value.push(ch);
        }
    }

    (value, i) // EOF before a closing quote.
}

/// Quote a lexeme for the `tsquery` string the way Go `quoteLexeme(_, false)`
/// does: bare if it is all `[0-9A-Za-z_]`, else single-quoted with `'` doubled.
fn quote_lexeme(word: &str) -> String {
    if word.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        word.to_owned()
    } else {
        format!("'{}'", word.replace('\'', "''"))
    }
}

/// Port of Go `appQueryTokensToTsquery`: fold the lexed tokens into a `tsquery`
/// string, inserting the pending connective (default `&`) before each operand,
/// expanding quoted/phrase runs through [`tokenize_flat`] joined by `<->`, and
/// recursing into parentheses.
fn tokens_to_tsquery(tokens: &[QToken]) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut operator: Option<PendingOp> = None;
    let mut negated = false;
    let mut i = 0;

    while i < tokens.len() {
        match &tokens[i] {
            QToken::And => operator = Some(PendingOp::And),
            QToken::Or => operator = Some(PendingOp::Or),
            QToken::FollowedBy => operator = Some(PendingOp::FollowedBy),
            QToken::Negation => negated = true,
            QToken::Quoted(value) | QToken::Phrase(value) => {
                // A word run is a single phrase, so all its lexemes are adjacent
                // (`<->`); separate operands are joined by the pending operator.
                let lexemes = tokenize_flat(value);
                if !lexemes.is_empty() {
                    let expr = lexemes
                        .iter()
                        .map(|w| quote_lexeme(w))
                        .collect::<Vec<_>>()
                        .join(" <-> ");
                    add_expr(
                        &mut parts,
                        &mut operator,
                        &mut negated,
                        expr,
                        tokens,
                        &mut i,
                    );
                }
            }
            QToken::OpenParens => {
                let inner = collect_parens(tokens, i);
                i += inner.len(); // advance past the inner tokens (Go: `i += len`).
                let parens_expr = tokens_to_tsquery(&inner);
                if !parens_expr.is_empty() {
                    let expr = format!("({parens_expr})");
                    add_expr(
                        &mut parts,
                        &mut operator,
                        &mut negated,
                        expr,
                        tokens,
                        &mut i,
                    );
                }
            }
            // A wildcard not consumed by a preceding operand, and a stray close
            // paren, are ignored — exactly as Go's switch leaves them unhandled.
            QToken::Wildcard | QToken::CloseParens => {}
        }
        i += 1;
    }

    parts.join(" ")
}

/// Emit one operand `expr` into `parts`, prefixing the pending connective (only
/// when something precedes it) and a negation, and absorbing a following
/// wildcard as a `:*` suffix (advancing `i` past it). Resets the pending
/// operator/negation. Mirrors Go's `addExpr` closure.
fn add_expr(
    parts: &mut Vec<String>,
    operator: &mut Option<PendingOp>,
    negated: &mut bool,
    mut expr: String,
    tokens: &[QToken],
    i: &mut usize,
) {
    if !parts.is_empty() {
        parts.push(
            match operator {
                Some(PendingOp::Or) => "|",
                Some(PendingOp::FollowedBy) => "<->",
                _ => "&", // explicit `&` or the default between operands.
            }
            .to_owned(),
        );
    }
    if *negated {
        parts.push("!".to_owned());
    }
    if matches!(tokens.get(*i + 1), Some(QToken::Wildcard)) {
        expr.push_str(":*");
        *i += 1;
    }
    parts.push(expr);

    *operator = None;
    *negated = false;
}

/// Collect the tokens inside the parentheses opened at `open`, excluding the
/// matching close paren (an unmatched open runs to the end). Mirrors Go's
/// depth-tracking loop.
fn collect_parens(tokens: &[QToken], open: usize) -> Vec<QToken> {
    let mut depth = 1usize;
    let mut inner = Vec::new();
    for tok in &tokens[open + 1..] {
        match tok {
            QToken::OpenParens => depth += 1,
            QToken::CloseParens => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            _ => {}
        }
        inner.push(tok.clone());
    }
    inner
}

// ===========================================================================
// Stage 2: tsquery string  ->  AST  ->  Tantivy query
// ===========================================================================

/// Parsed `tsquery` expression. Pure data, so the parser is unit-testable
/// without a Tantivy index.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Ast {
    /// A single lexeme; `prefix` marks a trailing `:*`.
    Term {
        text: String,
        prefix: bool,
    },
    /// An adjacency (`<->`) chain of plain lexemes; only the last `prefix` flag
    /// is meaningful (Postgres can only prefix the final term of a phrase).
    Phrase {
        terms: Vec<(String, bool)>,
    },
    And(Vec<Ast>),
    Or(Vec<Ast>),
    Not(Box<Ast>),
    /// Empty query — matches everything (filters still apply).
    MatchAll,
}

/// `tsquery` string token.
#[derive(Clone, Debug, PartialEq, Eq)]
enum TsTok {
    LParen,
    RParen,
    Not,
    And,
    Or,
    Followed,
    Lexeme { text: String, prefix: bool },
}

/// Tokenise a `tsquery` string. Whitespace separates tokens; `<…>` (only `<->`
/// is ever produced) is a followed-by; `'…'` is a quoted lexeme with `''`
/// escaping; a bare lexeme is a run of `[alnum_]`; a trailing `:*` marks a prefix.
fn lex_tsquery(s: &str) -> Vec<TsTok> {
    let chars: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '(' => {
                out.push(TsTok::LParen);
                i += 1;
            }
            ')' => {
                out.push(TsTok::RParen);
                i += 1;
            }
            '!' => {
                out.push(TsTok::Not);
                i += 1;
            }
            '&' => {
                out.push(TsTok::And);
                i += 1;
            }
            '|' => {
                out.push(TsTok::Or);
                i += 1;
            }
            '<' => {
                i += 1;
                while i < chars.len() && chars[i] != '>' {
                    i += 1;
                }
                if i < chars.len() {
                    i += 1; // consume '>'.
                }
                out.push(TsTok::Followed);
            }
            '\'' => {
                let (text, next) = read_quoted(&chars, i);
                i = next;
                let prefix = eat_prefix(&chars, &mut i);
                out.push(TsTok::Lexeme { text, prefix });
            }
            c if c.is_alphanumeric() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let text: String = chars[start..i].iter().collect();
                let prefix = eat_prefix(&chars, &mut i);
                out.push(TsTok::Lexeme { text, prefix });
            }
            _ => i += 1, // unknown — skip.
        }
    }

    out
}

/// Consume a `:*` prefix marker at `*i`, returning whether one was present.
fn eat_prefix(chars: &[char], i: &mut usize) -> bool {
    if chars.get(*i) == Some(&':') && chars.get(*i + 1) == Some(&'*') {
        *i += 2;
        true
    } else {
        false
    }
}

/// Recursive-descent parser over [`TsTok`]s, honouring Postgres precedence
/// (`!` > `<->` > `&` > `|`).
struct TsParser<'a> {
    toks: &'a [TsTok],
    source: &'a str,
    pos: usize,
}

impl TsParser<'_> {
    fn peek(&self) -> Option<&TsTok> {
        self.toks.get(self.pos)
    }

    fn parse_or(&mut self) -> Ast {
        let mut nodes = vec![self.parse_and()];
        while matches!(self.peek(), Some(TsTok::Or)) {
            self.pos += 1;
            nodes.push(self.parse_and());
        }
        if nodes.len() == 1 {
            nodes.pop().expect("len == 1")
        } else {
            Ast::Or(nodes)
        }
    }

    fn parse_and(&mut self) -> Ast {
        let mut nodes = vec![self.parse_phrase()];
        while matches!(self.peek(), Some(TsTok::And)) {
            self.pos += 1;
            nodes.push(self.parse_phrase());
        }
        if nodes.len() == 1 {
            nodes.pop().expect("len == 1")
        } else {
            Ast::And(nodes)
        }
    }

    fn parse_phrase(&mut self) -> Ast {
        let mut nodes = vec![self.parse_unary()];
        while matches!(self.peek(), Some(TsTok::Followed)) {
            self.pos += 1;
            nodes.push(self.parse_unary());
        }
        if nodes.len() == 1 {
            nodes.pop().expect("len == 1")
        } else {
            build_phrase(nodes, self.source)
        }
    }

    fn parse_unary(&mut self) -> Ast {
        let mut negated = false;
        while matches!(self.peek(), Some(TsTok::Not)) {
            self.pos += 1;
            negated = !negated;
        }
        let atom = self.parse_atom();
        if negated {
            Ast::Not(Box::new(atom))
        } else {
            atom
        }
    }

    fn parse_atom(&mut self) -> Ast {
        match self.peek() {
            Some(TsTok::LParen) => {
                self.pos += 1;
                let inner = self.parse_or();
                if matches!(self.peek(), Some(TsTok::RParen)) {
                    self.pos += 1;
                }
                inner
            }
            Some(TsTok::Lexeme { text, prefix }) => {
                let node = Ast::Term {
                    text: text.clone(),
                    prefix: *prefix,
                };
                self.pos += 1;
                node
            }
            // An unexpected token (or end of input) is not consumed; the callers
            // only recurse after eating an operator, so this cannot loop.
            _ => Ast::MatchAll,
        }
    }
}

/// Build a `<->` chain: a [`Ast::Phrase`] when every operand is a plain lexeme,
/// or an [`Ast::Or`] of concrete phrase combinations when an operand is a
/// disjunction. Unsupported operands keep the old conjunction fallback.
fn build_phrase(nodes: Vec<Ast>, source: &str) -> Ast {
    if nodes.iter().all(|n| matches!(n, Ast::Term { .. })) {
        let terms = nodes
            .into_iter()
            .map(|n| match n {
                Ast::Term { text, prefix } => (text, prefix),
                _ => unreachable!("guarded by all(matches Term)"),
            })
            .collect();
        Ast::Phrase { terms }
    } else {
        match distribute_phrase_terms(&nodes, PHRASE_GROUP_MAX_COMBINATIONS) {
            Ok(mut alternatives) => match alternatives.len() {
                0 => Ast::And(nodes),
                1 => Ast::Phrase {
                    terms: alternatives.pop().expect("len == 1"),
                },
                _ => Ast::Or(
                    alternatives
                        .into_iter()
                        .map(|terms| Ast::Phrase { terms })
                        .collect(),
                ),
            },
            Err(PhraseExpansionError::TooMany) => {
                tracing::warn!(
                    query = %source,
                    max_combinations = PHRASE_GROUP_MAX_COMBINATIONS,
                    "phrase-over-group expansion exceeded bound; falling back to conjunction"
                );
                Ast::And(nodes)
            }
            Err(PhraseExpansionError::NonExpandable) => Ast::And(nodes),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PhraseExpansionError {
    NonExpandable,
    TooMany,
}

/// Expand `<->` operands into concrete term sequences. Nested `Or` groups are
/// distributed recursively, with the same global combination bound enforced at
/// each recursive fan-out and again when operand alternatives are multiplied.
fn distribute_phrase_terms(
    nodes: &[Ast],
    limit: usize,
) -> Result<Vec<Vec<(String, bool)>>, PhraseExpansionError> {
    let mut combinations: Vec<Vec<(String, bool)>> = vec![Vec::new()];

    for node in nodes {
        let alternatives = phrase_term_alternatives(node, limit)?;
        let next_len = combinations
            .len()
            .checked_mul(alternatives.len())
            .ok_or(PhraseExpansionError::TooMany)?;
        if next_len > limit {
            return Err(PhraseExpansionError::TooMany);
        }

        let mut next = Vec::with_capacity(next_len);
        for existing in &combinations {
            for alternative in &alternatives {
                let mut terms = existing.clone();
                terms.extend(alternative.clone());
                next.push(terms);
            }
        }
        combinations = next;
    }

    Ok(combinations)
}

fn phrase_term_alternatives(
    node: &Ast,
    limit: usize,
) -> Result<Vec<Vec<(String, bool)>>, PhraseExpansionError> {
    match node {
        Ast::Term { text, prefix } => Ok(vec![vec![(text.clone(), *prefix)]]),
        Ast::Phrase { terms } => Ok(vec![terms.clone()]),
        Ast::Or(children) => {
            let mut alternatives = Vec::new();
            for child in children {
                let child_alternatives = phrase_term_alternatives(child, limit)?;
                let next_len = alternatives
                    .len()
                    .checked_add(child_alternatives.len())
                    .ok_or(PhraseExpansionError::TooMany)?;
                if next_len > limit {
                    return Err(PhraseExpansionError::TooMany);
                }
                alternatives.extend(child_alternatives);
            }
            Ok(alternatives)
        }
        Ast::And(_) | Ast::Not(_) | Ast::MatchAll => Err(PhraseExpansionError::NonExpandable),
    }
}

/// Parse a `tsquery` string into an [`Ast`].
fn parse_tsquery(s: &str) -> Ast {
    let toks = lex_tsquery(s);
    if toks.is_empty() {
        return Ast::MatchAll;
    }
    TsParser {
        toks: &toks,
        source: s,
        pos: 0,
    }
    .parse_or()
}

/// Lower an [`Ast`] into a Tantivy [`Query`] over the weight tiers.
fn lower(ast: &Ast, fields: &Fields) -> Box<dyn Query> {
    match ast {
        Ast::MatchAll => Box::new(AllQuery),
        Ast::Term { text, prefix } => term_across_tiers(fields, text, *prefix),
        Ast::Phrase { terms } => phrase_on_text_a(fields, terms),
        Ast::Not(inner) => Box::new(BooleanQuery::new(vec![
            (Occur::Must, Box::new(AllQuery) as Box<dyn Query>),
            (Occur::MustNot, lower(inner, fields)),
        ])),
        Ast::And(children) => {
            let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(children.len());
            let mut has_positive = false;
            for child in children {
                match child {
                    Ast::Not(inner) => clauses.push((Occur::MustNot, lower(inner, fields))),
                    other => {
                        clauses.push((Occur::Must, lower(other, fields)));
                        has_positive = true;
                    }
                }
            }
            // A pure-negation conjunction needs a positive anchor, else Tantivy
            // matches nothing (Postgres `!a` matches every doc without `a`).
            if !has_positive {
                clauses.push((Occur::Must, Box::new(AllQuery)));
            }
            Box::new(BooleanQuery::new(clauses))
        }
        Ast::Or(children) => {
            let clauses = children
                .iter()
                .map(|child| (Occur::Should, lower(child, fields)))
                .collect();
            Box::new(BooleanQuery::new(clauses))
        }
    }
}

/// Search a single non-prefix lexeme across the four weight tiers, boosted per
/// tier and combined with a [`DisjunctionMaxQuery`]. A prefix lexeme uses a
/// single-term [`PhrasePrefixQuery`] on `text_a`, because lower tiers do not
/// store positions.
fn term_across_tiers(fields: &Fields, text: &str, prefix: bool) -> Box<dyn Query> {
    if prefix {
        return prefix_on_text_a(fields, text);
    }

    let disjuncts = fields
        .weighted_text_fields()
        .iter()
        .map(|&(field, boost)| {
            let inner = Box::new(TermQuery::new(
                Term::from_field_text(field, text),
                IndexRecordOption::WithFreqs,
            ));
            Box::new(BoostQuery::new(inner, boost)) as Box<dyn Query>
        })
        .collect();
    Box::new(DisjunctionMaxQuery::with_tie_breaker(
        disjuncts,
        TIE_BREAKER,
    ))
}

/// Search a single-term prefix on `text_a`, preserving the weight-A boost while
/// avoiding positionless lower-tier fields.
fn prefix_on_text_a(fields: &Fields, text: &str) -> Box<dyn Query> {
    let mut q = PhrasePrefixQuery::new(vec![Term::from_field_text(fields.text_a, text)]);
    q.set_max_expansions(PREFIX_MAX_EXPANSIONS);
    Box::new(BoostQuery::new(Box::new(q), BOOST_A))
}

/// Search an adjacency phrase on `text_a`, preserving the weight-A boost while
/// avoiding positionless lower-tier fields. A trailing-prefix phrase uses a
/// [`PhrasePrefixQuery`]; otherwise a [`PhraseQuery`]. `terms` always has ≥ 2
/// elements (the parser only builds a phrase from a multi-operand `<->` chain),
/// so [`PhraseQuery::new`] never hits its single-term assertion.
fn phrase_on_text_a(fields: &Fields, terms: &[(String, bool)]) -> Box<dyn Query> {
    let last_is_prefix = terms.last().is_some_and(|(_, p)| *p);
    let term_objs: Vec<Term> = terms
        .iter()
        .map(|(t, _)| Term::from_field_text(fields.text_a, t))
        .collect();
    let inner: Box<dyn Query> = if last_is_prefix {
        let mut q = PhrasePrefixQuery::new(term_objs);
        q.set_max_expansions(PREFIX_MAX_EXPANSIONS);
        Box::new(q)
    } else {
        Box::new(PhraseQuery::new(term_objs))
    };
    Box::new(BoostQuery::new(inner, BOOST_A))
}

// ===========================================================================
// Pagination, sorting, and hit reconstruction
// ===========================================================================

/// Resolve the effective `(limit, offset)`. An absent [`Pagination`] message
/// defaults the page size; an explicit `limit == 0` is honoured (no hits, count
/// only); a present limit is capped at [`MAX_LIMIT`].
fn pagination(p: Option<&Pagination>) -> (usize, usize) {
    match p {
        None => (DEFAULT_LIMIT, 0),
        Some(p) => ((p.limit as usize).min(MAX_LIMIT), p.offset as usize),
    }
}

/// A `u64` or `i64` fast field a sort key resolves to.
enum FastType {
    U64,
    I64,
}

/// Resolve the first [`SortBy`] to a fast field and direction, or `None` to fall
/// back to relevance ordering (empty sort, or an unknown/`relevance` field).
fn sort_key(sort: &[SortBy]) -> Option<(&'static str, FastType, Order)> {
    let first = sort.first()?;
    let order = if first.descending {
        Order::Desc
    } else {
        Order::Asc
    };
    let (name, ty) = match first.field.as_str() {
        "size" => ("size", FastType::U64),
        "seeders" => ("seeders", FastType::U64),
        "leechers" => ("leechers", FastType::U64),
        "files_count" => ("files_count", FastType::U64),
        "release_year" => ("release_year", FastType::U64),
        "published_at" => ("published_at", FastType::I64),
        _ => return None,
    };
    Some((name, ty, order))
}

/// Collect one page of hits, ordered by relevance or by a fast field. Hits
/// ordered by a fast field carry a `score` of `0.0` (relevance is not computed).
fn collect_hits(
    searcher: &Searcher,
    fields: &Fields,
    query: &dyn Query,
    limit: usize,
    offset: usize,
    sort: &[SortBy],
) -> anyhow::Result<Vec<SearchHit>> {
    match sort_key(sort) {
        None => {
            let collector = TopDocs::with_limit(limit)
                .and_offset(offset)
                .order_by_score();
            searcher
                .search(query, &collector)?
                .into_iter()
                .map(|(score, addr)| make_hit(searcher, fields, addr, score))
                .collect()
        }
        Some((name, FastType::U64, order)) => {
            let collector = TopDocs::with_limit(limit)
                .and_offset(offset)
                .order_by_fast_field::<u64>(name, order);
            searcher
                .search(query, &collector)?
                .into_iter()
                .map(|(_, addr)| make_hit(searcher, fields, addr, 0.0))
                .collect()
        }
        Some((name, FastType::I64, order)) => {
            let collector = TopDocs::with_limit(limit)
                .and_offset(offset)
                .order_by_fast_field::<i64>(name, order);
            searcher
                .search(query, &collector)?
                .into_iter()
                .map(|(_, addr)| make_hit(searcher, fields, addr, 0.0))
                .collect()
        }
    }
}

/// Reconstruct a [`SearchHit`] from a result document.
fn make_hit(
    searcher: &Searcher,
    fields: &Fields,
    addr: DocAddress,
    score: Score,
) -> anyhow::Result<SearchHit> {
    Ok(SearchHit {
        document: Some(reconstruct_document(searcher, fields, addr)?),
        score,
    })
}

/// Rebuild the proto [`TorrentDocument`] from the STORED fields. `file_paths` is
/// always empty: it feeds relevance only and is never stored (the cost the blob
/// migration removed).
fn reconstruct_document(
    searcher: &Searcher,
    fields: &Fields,
    addr: DocAddress,
) -> anyhow::Result<TorrentDocument> {
    let doc: TantivyDocument = searcher.doc(addr)?;

    let info_hash = doc
        .get_first(fields.info_hash)
        .and_then(|v| v.as_bytes())
        .map(<[u8]>::to_vec)
        .unwrap_or_default();

    // content_type is stored as its canonical string; map back to the proto int.
    let content_type = doc
        .get_first(fields.content_type)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<ModelContentType>().ok())
        .map_or(0, ModelContentType::to_proto_value);

    Ok(TorrentDocument {
        info_hash,
        torrent_name: str_field(&doc, fields.torrent_name),
        content_title: str_field(&doc, fields.content_title),
        original_title: str_field(&doc, fields.original_title),
        release_year: u64_field(&doc, fields.release_year) as u32,
        video_resolution: str_field(&doc, fields.video_resolution),
        video_source: str_field(&doc, fields.video_source),
        video_codec: str_field(&doc, fields.video_codec),
        genres: multi_str(&doc, fields.genres),
        file_paths: Vec::new(),
        content_type,
        seeders: u64_field(&doc, fields.seeders) as u32,
        leechers: u64_field(&doc, fields.leechers) as u32,
        files_count: u64_field(&doc, fields.files_count) as u32,
        size: u64_field(&doc, fields.size),
        published_at: doc
            .get_first(fields.published_at)
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        languages: multi_str(&doc, fields.languages),
        file_extensions: multi_str(&doc, fields.file_extensions),
        video_3d: str_field(&doc, fields.video_3d),
        video_modifier: str_field(&doc, fields.video_modifier),
        release_group: str_field(&doc, fields.release_group),
        audio_languages: multi_str(&doc, fields.audio_languages),
        content_source: str_field(&doc, fields.content_source),
        content_id: str_field(&doc, fields.content_id),
    })
}

/// First stored string value of `field`, or empty.
fn str_field(doc: &TantivyDocument, field: Field) -> String {
    doc.get_first(field)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned()
}

/// All stored string values of a multi-valued `field`.
fn multi_str(doc: &TantivyDocument, field: Field) -> Vec<String> {
    doc.get_all(field)
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect()
}

/// First stored `u64` value of `field`, or `0`.
fn u64_field(doc: &TantivyDocument, field: Field) -> u64 {
    doc.get_first(field).and_then(|v| v.as_u64()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{reader, register_tokenizer, writer};
    use crate::proto::{ContentType, TorrentDocument};
    use crate::schema::build_schema;
    use tantivy::Index;

    // ---- Stage 1: app query -> tsquery parity with Go (tsquery_test.go) ----

    #[test]
    fn app_query_to_tsquery_matches_go_vectors() {
        // The exact table from internal/database/fts/tsquery_test.go.
        let cases = [
            ("", ""),
            ("foo", "foo"),
            ("foo Bar", "foo & bar"),
            ("foo bar baz", "foo & bar & baz"),
            (
                "\"make me a \" . (sandwich | panini) !cheese mayo*",
                "make <-> me <-> a <-> (sandwich | panini) & ! cheese & mayo:*",
            ),
            ("\"make me a sandwich", "make <-> me <-> a <-> sandwich"),
            (
                "\"make me a \" . (sandwich | panini",
                "make <-> me <-> a <-> (sandwich | panini)",
            ),
            ("зроби мені бутерброд", "zrobi & meni & buterbrod"),
            (
                "给我做一个三明治",
                "Gei <-> Wo <-> Zuo <-> Yi <-> Ge <-> San <-> Ming <-> Zhi",
            ),
            ("اصنع لي شطيرة", "'Sn`' & ly & 'shTyr@'"),
            ("\"اصنع لي شطيرة\"", "'Sn`' <-> ly <-> 'shTyr@'"),
            ("&eacute;", "eacute"),
        ];
        for (input, want) in cases {
            assert_eq!(app_query_to_tsquery(input), want, "input = {input:?}");
        }
    }

    // ---- Stage 2: tsquery string -> AST ----

    fn term(text: &str) -> Ast {
        Ast::Term {
            text: text.to_owned(),
            prefix: false,
        }
    }

    fn or_group_terms(count: usize) -> String {
        (0..count)
            .map(|i| format!("t{i}"))
            .collect::<Vec<_>>()
            .join(" | ")
    }

    #[test]
    fn parses_operators_with_postgres_precedence() {
        assert_eq!(parse_tsquery("foo"), term("foo"));
        assert_eq!(
            parse_tsquery("foo & bar"),
            Ast::And(vec![term("foo"), term("bar")])
        );
        assert_eq!(
            parse_tsquery("foo | bar"),
            Ast::Or(vec![term("foo"), term("bar")])
        );
        // `<->` binds tighter than `&`: a <-> b & c == (a<->b) & c.
        assert_eq!(
            parse_tsquery("a <-> b & c"),
            Ast::And(vec![
                Ast::Phrase {
                    terms: vec![("a".to_owned(), false), ("b".to_owned(), false)]
                },
                term("c"),
            ])
        );
        // `!` is unary on the next atom.
        assert_eq!(
            parse_tsquery("foo & ! bar"),
            Ast::And(vec![term("foo"), Ast::Not(Box::new(term("bar")))])
        );
        // Prefix marker.
        assert_eq!(
            parse_tsquery("mayo:*"),
            Ast::Term {
                text: "mayo".to_owned(),
                prefix: true
            }
        );
        // Quoted lexeme with non-word chars round-trips to a single term.
        assert_eq!(parse_tsquery("'Sn`'"), term("Sn`"));
        // Empty -> match all.
        assert_eq!(parse_tsquery(""), Ast::MatchAll);
    }

    #[test]
    fn parens_override_precedence() {
        // a & (b | c)
        assert_eq!(
            parse_tsquery("a & (b | c)"),
            Ast::And(vec![term("a"), Ast::Or(vec![term("b"), term("c")]),])
        );
    }

    #[test]
    fn phrase_over_or_group_distributes_to_concrete_phrases() {
        assert_eq!(
            parse_tsquery("a <-> (b | c)"),
            Ast::Or(vec![
                Ast::Phrase {
                    terms: vec![("a".to_owned(), false), ("b".to_owned(), false)]
                },
                Ast::Phrase {
                    terms: vec![("a".to_owned(), false), ("c".to_owned(), false)]
                },
            ])
        );
    }

    #[test]
    fn phrase_over_large_or_group_uses_bounded_conjunction_fallback() {
        let at_bound = format!("a <-> ({})", or_group_terms(PHRASE_GROUP_MAX_COMBINATIONS));
        match parse_tsquery(&at_bound) {
            Ast::Or(children) => assert_eq!(children.len(), PHRASE_GROUP_MAX_COMBINATIONS),
            other => panic!("expected distributed OR at bound, got {other:?}"),
        }

        let oversized = format!(
            "a <-> ({})",
            or_group_terms(PHRASE_GROUP_MAX_COMBINATIONS + 1)
        );
        match parse_tsquery(&oversized) {
            Ast::And(children) => {
                assert_eq!(children.len(), 2);
                assert!(matches!(
                    &children[0],
                    Ast::Term { text, prefix } if text == "a" && !*prefix
                ));
                match &children[1] {
                    Ast::Or(group) => assert_eq!(group.len(), PHRASE_GROUP_MAX_COMBINATIONS + 1),
                    other => panic!("expected original OR group in fallback, got {other:?}"),
                }
            }
            other => panic!("expected bounded conjunction fallback, got {other:?}"),
        }
    }

    #[test]
    fn prefix_max_expansions_is_pg_parity_budget() {
        assert_eq!(PREFIX_MAX_EXPANSIONS, 2_048);
    }

    // ---- End-to-end search over an in-RAM index ----

    fn base_doc(info_hash: Vec<u8>, name: &str) -> TorrentDocument {
        TorrentDocument {
            info_hash,
            torrent_name: name.to_owned(),
            content_title: name.to_owned(),
            original_title: String::new(),
            release_year: 2020,
            video_resolution: "1080p".to_owned(),
            video_source: "BluRay".to_owned(),
            video_codec: "x264".to_owned(),
            genres: vec!["action".to_owned()],
            file_paths: vec![format!("{name}.mkv")],
            content_type: ContentType::Movie as i32,
            seeders: 10,
            leechers: 1,
            files_count: 1,
            size: 1_000_000,
            published_at: 1_600_000_000,
            languages: vec!["en".to_owned()],
            file_extensions: vec!["mkv".to_owned()],
            video_3d: String::new(),
            video_modifier: String::new(),
            release_group: "GRP".to_owned(),
            audio_languages: vec!["en".to_owned()],
            content_source: "tmdb".to_owned(),
            content_id: "1".to_owned(),
        }
    }

    /// Build an in-RAM index, register the tokenizer, index `docs`, and return an
    /// `(index, reader, fields)` triple ready for `run_search`.
    fn index_docs(docs: &[TorrentDocument]) -> (Index, tantivy::IndexReader, Fields) {
        let index = Index::create_in_ram(build_schema());
        register_tokenizer(&index);
        let fields = Fields::from_schema(&index.schema()).unwrap();
        let mut w = writer(&index).unwrap();
        for d in docs {
            crate::indexer::upsert(&w, &fields, d).unwrap();
        }
        w.commit().unwrap();
        let r = reader(&index).unwrap();
        r.reload().unwrap();
        (index, r, fields)
    }

    fn search(
        index: &Index,
        reader: &tantivy::IndexReader,
        fields: &Fields,
        q: &str,
    ) -> SearchResponse {
        run_search(
            index,
            reader,
            fields,
            SearchRequest {
                query: q.to_owned(),
                filters: None,
                pagination: None,
                sort: Vec::new(),
            },
        )
        .unwrap()
    }

    fn names(resp: &SearchResponse) -> Vec<String> {
        resp.hits
            .iter()
            .map(|h| h.document.as_ref().unwrap().torrent_name.clone())
            .collect()
    }

    fn sorted_names(resp: &SearchResponse) -> Vec<String> {
        let mut out = names(resp);
        out.sort();
        out
    }

    #[test]
    fn search_and_or_not_phrase_prefix() {
        let docs = [
            base_doc(vec![0x01; 20], "The Matrix"),
            base_doc(vec![0x02; 20], "Matrix Reloaded"),
            base_doc(vec![0x03; 20], "Spider Man"),
            base_doc(vec![0x04; 20], "Batman Begins"),
        ];
        let (index, reader, fields) = index_docs(&docs);

        // Single term.
        let r = search(&index, &reader, &fields, "matrix");
        assert_eq!(r.total_hits, 2);

        // AND (default operator between words): both terms must be present.
        let r = search(&index, &reader, &fields, "matrix reloaded");
        assert_eq!(names(&r), vec!["Matrix Reloaded"]);

        // OR.
        let r = search(&index, &reader, &fields, "spider | batman");
        assert_eq!(r.total_hits, 2);

        // NOT: matrix but not reloaded.
        let r = search(&index, &reader, &fields, "matrix !reloaded");
        assert_eq!(names(&r), vec!["The Matrix"]);

        // Phrase (followed-by): "spider man" adjacency.
        let r = search(&index, &reader, &fields, "spider.man");
        assert_eq!(names(&r), vec!["Spider Man"]);

        // Prefix.
        let r = search(&index, &reader, &fields, "spid*");
        assert_eq!(names(&r), vec!["Spider Man"]);

        // Empty query matches everything.
        let r = search(&index, &reader, &fields, "");
        assert_eq!(r.total_hits, 4);
    }

    #[test]
    fn phrase_over_or_group_matches_only_adjacent_alternatives() {
        let docs = [
            base_doc(vec![0x01; 20], "Alpha Beta"),
            base_doc(vec![0x02; 20], "Alpha Gamma"),
            base_doc(vec![0x03; 20], "Alpha Far Beta"),
        ];
        let (index, reader, fields) = index_docs(&docs);

        let r = search(&index, &reader, &fields, "alpha . (beta|gamma)");
        assert_eq!(r.total_hits, 2);
        assert_eq!(sorted_names(&r), vec!["Alpha Beta", "Alpha Gamma"]);
    }

    #[test]
    fn multi_field_boost_ranks_title_over_genre() {
        // "action" is doc B's title (weight A) and every doc's genre (weight D).
        // The title match must outrank the genre-only matches.
        let mut a = base_doc(vec![0x01; 20], "Generic Movie");
        a.genres = vec!["action".to_owned()];
        let mut b = base_doc(vec![0x02; 20], "Action Heroes");
        b.genres = vec!["drama".to_owned()];
        let (index, reader, fields) = index_docs(&[a, b]);

        let r = search(&index, &reader, &fields, "action");
        assert_eq!(r.total_hits, 2);
        assert_eq!(
            r.hits[0].document.as_ref().unwrap().torrent_name,
            "Action Heroes",
            "weight-A title match must rank first"
        );
        assert!(r.hits[0].score > r.hits[1].score);
    }

    #[test]
    fn filters_pagination_and_sort() {
        let mut a = base_doc(vec![0x01; 20], "Alpha");
        a.content_type = ContentType::Movie as i32;
        a.size = 100;
        a.seeders = 5;
        let mut b = base_doc(vec![0x02; 20], "Beta");
        b.content_type = ContentType::TvShow as i32;
        b.size = 300;
        b.seeders = 50;
        let mut c = base_doc(vec![0x03; 20], "Gamma");
        c.content_type = ContentType::Movie as i32;
        c.size = 200;
        c.seeders = 20;
        let (index, reader, fields) = index_docs(&[a, b, c]);

        // content_type filter (Movie) -> 2 hits.
        let resp = run_search(
            &index,
            &reader,
            &fields,
            SearchRequest {
                query: String::new(),
                filters: Some(SearchFilters {
                    content_types: vec![ContentType::Movie as i32],
                    ..Default::default()
                }),
                pagination: None,
                sort: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(resp.total_hits, 2);

        // size range filter [150, 1000] -> Gamma (200) only among Movies... but
        // here with no content filter -> Beta(300) and Gamma(200).
        let resp = run_search(
            &index,
            &reader,
            &fields,
            SearchRequest {
                query: String::new(),
                filters: Some(SearchFilters {
                    size_min: Some(150),
                    size_max: Some(1000),
                    ..Default::default()
                }),
                pagination: None,
                sort: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(resp.total_hits, 2);

        // Sort by seeders descending.
        let resp = run_search(
            &index,
            &reader,
            &fields,
            SearchRequest {
                query: String::new(),
                filters: None,
                pagination: None,
                sort: vec![SortBy {
                    field: "seeders".to_owned(),
                    descending: true,
                }],
            },
        )
        .unwrap();
        assert_eq!(names(&resp), vec!["Beta", "Gamma", "Alpha"]);

        // Pagination: limit 1, offset 1 over the seeders-desc order.
        let resp = run_search(
            &index,
            &reader,
            &fields,
            SearchRequest {
                query: String::new(),
                filters: None,
                pagination: Some(Pagination {
                    limit: 1,
                    offset: 1,
                }),
                sort: vec![SortBy {
                    field: "seeders".to_owned(),
                    descending: true,
                }],
            },
        )
        .unwrap();
        assert_eq!(resp.total_hits, 3);
        assert_eq!(names(&resp), vec!["Gamma"]);
    }

    #[test]
    fn reconstructs_document_without_file_paths() {
        let (index, reader, fields) = index_docs(&[base_doc(vec![0xAB; 20], "Sample")]);
        let resp = search(&index, &reader, &fields, "sample");
        let doc = resp.hits[0].document.as_ref().unwrap();
        assert_eq!(doc.info_hash, vec![0xAB; 20]);
        assert_eq!(doc.content_type, ContentType::Movie as i32);
        assert_eq!(doc.release_year, 2020);
        assert_eq!(doc.size, 1_000_000);
        assert_eq!(doc.genres, vec!["action".to_owned()]);
        assert_eq!(doc.audio_languages, vec!["en".to_owned()]);
        assert_eq!(doc.content_source, "tmdb");
        // file_paths is relevance-only and never stored.
        assert!(doc.file_paths.is_empty());
    }
}
