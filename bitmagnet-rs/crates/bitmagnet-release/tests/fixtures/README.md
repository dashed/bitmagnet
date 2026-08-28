# Go-pinned Unicode class tables + behavioural oracle

Two artifacts keep the Rust ports on Go's exact Unicode behaviour:

1. [`src/goclass/tables.rs`](../../src/goclass/tables.rs) — **generated** char
   classes (`WORD_CHAR_RANGES`, `LETTER_RANGES`, `LETTER_CLASS_BODY`), swept
   from the real Go toolchain. `tests/goclass_all_scalars.rs` re-proves the
   pinning over all 1,112,064 Unicode scalar values, so a `regex`/rustc bump
   fails loudly instead of drifting.
2. `testdata/parity/unicode/go-oracle.jsonl` (repo-root `testdata/`, shared by
   the `unicode_class_parity` tests in `bitmagnet-fts`, `bitmagnet-search`,
   `bitmagnet-release`, and the date-parser test in `bitmagnet-classifier`) —
   the **production Go** outputs for 728 probe strings: `AppQueryToTsquery`,
   `TokenizeFlat`, `ParseDate` (+ `IsValid`), and
   `ParseTitleYearEpisodes(NullContentType, …)`.

## Why this exists

Rust's Unicode predicates are STRICTLY WIDER than Go's (measured over every
scalar, rustc 1.97.1 / regex 1.13 vs Go go1.23.6, `unicode.Version` 15.0.0 in
both Go 1.23.6 and 1.24.1):

| Rust                    | Go                        | differing code points |
| ----------------------- | ------------------------- | --------------------- |
| `char::is_alphanumeric` | `IsLetter \| IsDigit`     | 12,322                |
| `char::is_alphabetic`   | `unicode.IsLetter`        | 11,317                |
| `char::is_numeric`      | `unicode.IsDigit`         | 1,244                 |
| `regex` `\p{L}`         | RE2 `\p{L}`               | 4,924                 |
| `regex` `\w`            | RE2 `\w` (ASCII)          | 144,604               |
| `regex` `\d`            | RE2 `\d` (ASCII)          | 750                   |

Go is never the wider of the two. The probes are built from the divergent code
points (`² ³ ¹ ¼ ½ ¾ ①②③ ⅓ ⅔ Ⅷ …`) wedged between word-char runs, where a wider
class merges two operands into one and flips the tsquery operator `&` → `<->`
(strictly narrower — Rust silently returned a SUBSET of Go's results), shifts a
`tokenize_flat` lexeme boundary, drops a date, or splits a title differently.

The 728 probes = 348 unquoted shapes (app-query path — the OPERATOR FLIP) +
the same 348 wrapped in `"` (the classifier path — `ContentBySearch` issues
`fmt.Sprintf("\"%s\"", baseTitle)`; the phrase lexes as ONE quoted token, so
what must match is the LEXEME BOUNDARIES inside it) + 8 compound/degenerate
quote shapes + 24 date shapes (divergent chars gluing a title to its date —
the defect that made dates vanish).

## Regenerating

The generator is a **throwaway** Go program — `tmp-unicode-oracle/` at the repo
root, deliberately left untracked so no stray `main` package lands in the
module. It has two files:

- `main.go` reads one JSON-encoded string per line on stdin and emits one JSON
  object per probe with the Go results (`fts.AppQueryToTsquery`,
  `fts.TokenizeFlat`, `parsers.ParseDate`,
  `parsers.ParseTitleYearEpisodes(model.NullContentType{}, in)`).
- `bytes.go` (`byteProbe`, prints to stderr) measures the single-BYTE word-char
  set `lexer.IsWordChar(rune(b))` accepts — the parity target for a future
  `fileSearchStrings` port (`internal/model/torrents.go` classifies a path
  byte-by-byte, splitting multi-byte UTF-8 chars at boundaries). Measured:
  **127 of 256 bytes** — ASCII `[0-9A-Za-z]` plus `{AA, B5, BA}`,
  `C0..F4` except `D7`, and `F5..FF` except `F7`. See the warning on
  `goclass::is_word_char`.

To regenerate:

```sh
# table sweep: extend the program to re-emit goclass/tables.rs exactly as
# documented in that file's header (WORD_CHAR_RANGES from
# unicode.IsLetter||IsDigit; LETTER_RANGES read off the COMPILED
# regexp.MustCompile("^\\p{L}$") so it is the class RE2 actually applies).
go run ./tmp-unicode-oracle < probe-inputs.jsonl > testdata/parity/unicode/go-oracle.jsonl
```

The probe inputs are recoverable from the committed oracle itself:
`jq -c '.input' testdata/parity/unicode/go-oracle.jsonl`.

## Provenance

|                    |                                                   |
| ------------------ | ------------------------------------------------- |
| Generated on       | 2026-07-24 (extended 2026-07-28 with the quoted   |
|                    | classifier-path + date shapes)                    |
| Go toolchain       | `go1.24.1` (deployed prod is go1.23.6 — both pin  |
|                    | `unicode.Version` 15.0.0, so the classes agree)    |
| Verified           | 2026-07-28 — re-run against the committed inputs   |
|                    | reproduces the committed oracle byte-for-byte      |
| Fixture count      | 728 probes                                         |
| `regex` crate      | 1.13                                              |

> If bitmagnet is ever rebuilt on a Go release with a newer Unicode version,
> regenerate BOTH `goclass/tables.rs` and this oracle from that toolchain, and
> expect the divergence counts asserted in `tests/goclass_all_scalars.rs` to
> move — update them deliberately, never to make a failure go away.
