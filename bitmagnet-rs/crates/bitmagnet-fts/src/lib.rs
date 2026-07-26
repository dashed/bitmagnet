//! Shared FTS port for the bitmagnet Rust rewrite: the Go
//! `internal/database/fts` tokenizer (`TokenizeFlat`) plus the
//! `AppQueryToTsquery` builder, ported verbatim (Go `src/query.rs`
//! `app_query_to_tsquery` + `src/tokenizer.rs` + `src/tokenizer/tables.rs`,
//! go-unidecode v0.2.0). Kept Tantivy-free so every consumer
//! (`bitmagnet-search-query`'s full builder, a future ingest-side indexer)
//! shares ONE tokenizer instead of copying it.

mod tokenizer;
mod tsvector;

pub use self::tokenizer::tokenize_flat;
pub use self::tsvector::{Tsvector, TsvectorWeight, MAX_LEXEME_BYTES};

/// Port of Go `fts.AppQueryToTsquery`: turn a user-facing app query into the
/// Postgres `tsquery` string Postgres would match against. Pure and
/// deterministic, so it is asserted byte-for-byte against the Go test vectors.
#[must_use]
pub fn app_query_to_tsquery(raw: &str) -> String {
    tokens_to_tsquery(&lex_app_query(raw))
}
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

/// Word-character test for the app-query lexer: Go `lexer.IsWordChar`
/// (`unicode.IsLetter || unicode.IsDigit`), read from the Go-generated
/// [`tokenizer::is_go_word_char`] table.
///
/// 🚨 Do NOT "simplify" this to `char::is_alphanumeric()`. That predicate is
/// 12,322 code points wider than Go's — it accepts `² ³ ¹ ¼ ½ ¾ ①②③` and the
/// rest of `No`/`Nl`/`Other_Alphabetic`, which Go treats as separators. The
/// consequence is not cosmetic: a wider word-char class merges what Go sees as
/// two operands into one run, which changes the tsquery **operator** from `&`
/// to `<->`. `<->` (adjacency) is strictly narrower than `&` (conjunction), so
/// Rust silently returns a SUBSET of Go's results. Re-deriving the lexemes in
/// [`tokenize_flat`] does not undo an operator change — that is the reasoning
/// the comment this replaced got wrong, and why the divergence shipped.
fn is_query_word_char(c: char) -> bool {
    tokenizer::is_go_word_char(c)
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

#[cfg(test)]
mod tests {
    use super::app_query_to_tsquery;

    #[test]
    fn app_query_to_tsquery_matches_full_go_vector_table() {
        // The exact table from internal/database/fts/tsquery_test.go.
        let tests = [
            ("empty", "", ""),
            ("1 word", "foo", "foo"),
            ("2 words", "foo Bar", "foo & bar"),
            ("3 words", "foo bar baz", "foo & bar & baz"),
            (
                "quotes, operators, parens, wildcards",
                "\"make me a \" . (sandwich | panini) !cheese mayo*",
                "make <-> me <-> a <-> (sandwich | panini) & ! cheese & mayo:*",
            ),
            (
                "unmatched quotes",
                "\"make me a sandwich",
                "make <-> me <-> a <-> sandwich",
            ),
            (
                "unmatched parens",
                "\"make me a \" . (sandwich | panini",
                "make <-> me <-> a <-> (sandwich | panini)",
            ),
            (
                "Ukrainian",
                "зроби мені бутерброд",
                "zrobi & meni & buterbrod",
            ),
            (
                "Chinese",
                "给我做一个三明治",
                "Gei <-> Wo <-> Zuo <-> Yi <-> Ge <-> San <-> Ming <-> Zhi",
            ),
            ("Arabic", "اصنع لي شطيرة", "'Sn`' & ly & 'shTyr@'"),
            (
                "Arabic (quoted)",
                "\"اصنع لي شطيرة\"",
                "'Sn`' <-> ly <-> 'shTyr@'",
            ),
            ("ampersand prefix", "&eacute;", "eacute"),
        ];

        for (name, input, want) in tests {
            assert_eq!(
                app_query_to_tsquery(input),
                want,
                "Go vector {name:?} diverged for input {input:?}"
            );
        }
    }
}
