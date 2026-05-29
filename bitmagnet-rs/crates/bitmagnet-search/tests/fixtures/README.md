# Tokenizer parity fixtures

`tokenizer_fixtures.json` is the **ground truth** for the Rust tokenizer
([`src/tokenizer.rs`](../../src/tokenizer.rs)). Each entry is

```json
{ "input": "<arbitrary string>", "tokens": ["<token>", ...] }
```

where `tokens` is the output of the **real Go** `TokenizeFlat` for that input:

```go
github.com/bitmagnet-io/bitmagnet/internal/database/fts.TokenizeFlat(input)
```

The integration test [`tests/tokenizer_parity.rs`](../tokenizer_parity.rs)
asserts `bitmagnet_search::tokenizer::tokenize_flat(input) == tokens` for every
entry, so any divergence from production tokenization fails the build.

## Provenance

| | |
|---|---|
| Generated on | 2026-05-28 |
| Go toolchain | `go1.23.6` |
| `unicode.Version` | `15.0.0` |
| `go-unidecode` | `v0.2.0` (`github.com/mozillazg/go-unidecode`) |
| Fixture count | 4223 |

The same generator also emitted [`src/tokenizer/tables.rs`](../../src/tokenizer/tables.rs) —
the embedded word-char ranges, `ToLower` map, and verbatim go-unidecode
transliteration tables. Because those tables are read directly from the Go
`unicode` package and `go-unidecode/table.Tables`, transliteration/classification
parity is guaranteed *by construction*; these fixtures verify the surrounding
Rust algorithm (lexing, word boundaries, the non-breaking-language rule, offset
tracking) end-to-end.

> **Unicode version note:** the embedded tables pin Unicode 15.0.0 (Go 1.23.6).
> If bitmagnet is ever rebuilt on a Go release with a newer Unicode version,
> regenerate both this fixture and `tables.rs` from that toolchain so the
> sidecar continues to match production.

## Corpus

Curated adversarial strings (ASCII, accents, ß/Ł/Ø/æ/ñ, symbols ½/№/€/™/©,
Cyrillic, Greek, Arabic/Hebrew, CJK, emoji + ZWJ/skin-tone sequences, literal
`'`/`\`, ligatures, Roman numerals, mathematical alphanumerics, Turkish İ/ı,
fullwidth/halfwidth, combining marks, multi-space and leading/trailing junk),
plus single-code-point sweeps over `U+0080..U+0700`, `U+0900..U+0980`,
`U+1E00..U+2200` (the `U+2000` non-breaking boundary), `U+2460..U+24FF`,
`U+3040..U+30FF`, sampled CJK / mathematical alphanumerics, `U+FF00..U+FFEF`,
and a few astral-plane runes with no table entry.

## Regenerating

These files are produced by a **throwaway** generator (not committed, to avoid a
stray `main` package in the module). To recreate it, add
`tmp_tokgen/main.go` under the repo root and `go run ./tmp_tokgen` from there.
The generator:

1. builds `WORD_CHAR_RANGES` by scanning `0..=0x10FFFF` for
   `unicode.IsLetter(r) || unicode.IsDigit(r)` (== `lexer.IsWordChar`);
2. builds `LOWER_MAP` from every `r` where `unicode.ToLower(r) != r`;
3. copies `go-unidecode/table.Tables` verbatim, one Rust slice per section;
4. emits this JSON by calling `fts.TokenizeFlat` over the corpus above.

It imports only existing module dependencies plus the standard library, so
`go.mod` is never modified.

## Attribution

The transliteration data embedded in `src/tokenizer/tables.rs` is derived from
[`go-unidecode`](https://github.com/mozillazg/go-unidecode) (© 2016 mozillazg),
distributed under the MIT License, itself derived from Python `unidecode`.
