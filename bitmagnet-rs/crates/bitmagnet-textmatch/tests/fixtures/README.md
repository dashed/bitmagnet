# textmatch parity fixtures

`textmatch_fixtures.json` is the **ground truth** for `bitmagnet-textmatch`.
Every expected value in it was produced by calling the **real Go** functions —
none of it is hand-authored — so `tests/parity.rs` asserts byte-equality
against production behaviour rather than against someone's reading of the Go
source.

| Go function                                    | Rust port                             | Section               |
| ---------------------------------------------- | ------------------------------------- | --------------------- |
| `unidecode.Unidecode`                           | `unidecode`                           | `unidecode`           |
| `regex.NormalizeString`                         | `normalize_string`                    | `normalize`           |
| `classifier.levenshteinNormalizeString`         | `levenshtein_normalize_string`        | `lev_normalize`       |
| `levenshtein.ComputeDistance`                   | `compute_distance`                    | `distance`            |
| `classifier.levenshteinFindMinDistance`         | `find_min_distance`                   | `min_distance`        |
| `classifier.levenshteinFindBestMatch`           | `find_best_match_index`               | `best_match`          |
| `regex.WordTokenRegex().String()`               | `GO_WORD_TOKEN_PATTERN`               | `word_token_pattern`  |
| `regexp.MustCompile("^[\p{L}\d]$")`             | `WORD_CHAR_CLASS`                     | `word_char_class`, `letter_digit_ranges` |
| `classifier.levenshteinThreshold`               | `LEVENSHTEIN_THRESHOLD`               | `threshold`           |

`-1` in the `min_distance.d` and `best_match.index` fields is Go's sentinel
(no candidates / `ok == false`) and decodes to `None`.

## Provenance

|                   |                                                |
| ----------------- | ---------------------------------------------- |
| Generated on      | 2026-07-25                                     |
| Go toolchain      | `go1.23.6`                                     |
| `unicode.Version` | `15.0.0`                                       |
| `go-unidecode`    | `v0.2.0` (`github.com/mozillazg/go-unidecode`) |
| `agnivade/levenshtein` | `v1.2.1`                                  |
| `hedhyw/rex`      | `v1.0.0`                                       |

Fixture counts: 4,073 `unidecode` · 8,059 `normalize` · 8,059 `lev_normalize` ·
2,772 `distance` · 410 `min_distance` · 918 `best_match` · 660
`letter_digit_ranges`.

## Corpus

* **Curated adversarial strings** — punctuation-heavy release titles
  (`S.W.A.T.`, `Spider-Man: No Way Home`, `Mission.Impossible '…'`), mid-word
  `'`/`-`, quote/paren wrapping, accents, `ß`/`ẞ`, Turkish `İ`/`ı`, ligatures,
  Cyrillic/Greek/Arabic/Hebrew/Hangul/CJK, emoji + ZWJ sequences, Roman
  numerals (`Ⅷ`, category `Nl`), vulgar fractions (`½`, `No`), Thai /
  Arabic-Indic / Devanagari digits (category `Nd` but **not** ASCII `\d`),
  fullwidth + halfwidth forms, combining marks and the NFD/NFC pairs, the
  ANGSTROM SIGN, the `U+007F` boundary of go-unidecode's `r < MaxASCII` test,
  astral-plane runes above the table, `U+10FFFF`, and empty/whitespace-only
  inputs.
* **Single-code-point sweeps** over `U+0000..U+0800`, `U+1E00..U+2200`,
  `U+2460..U+24FF`, `U+3000..U+30FF` and `U+FE00..U+FFEF`, each probed bare and
  wedged between ASCII letters (`a<cp>b`) to exercise word boundaries.
* **Randomized differential cases** (seed `20260725`): 2,750 distance pairs
  built by applying 0–8 single-rune edits to a random base string over an ASCII
  and a Unicode alphabet, plus 500 unrelated pairs; 400 `min_distance` and 900
  `best_match` scenarios with 0–5 items of 0–2 candidate strings each.
* **Named semantic cases**, asserted by name in `parity.rs` so a regression
  identifies itself: `tie-first-wins`, `tie-first-wins-distance-5`,
  `threshold-5-accepted`, `threshold-6-rejected`, `threshold-6-then-5`,
  `exact-first`, `exact-later-wins-over-near`, `empty-candidate-list-skipped`,
  `multi-string-per-item-second-wins`, `multi-string-min-taken`,
  `duplicate-normalized-candidates`, `all-above-threshold`, `unidecode-path`,
  `empty-target`, `whitespace-target`, `punctuation-only-candidates`,
  `empty-items`, `nil-items`.

## Regenerating

The generator is **throwaway** (not committed — it would leave a stray test in
`internal/classifier`, and it has to live there to reach the unexported
`levenshteinFindBestMatch`). To recreate it, add
`internal/classifier/zz_textmatch_gen_test.go` with a
`TestZZGenTextmatchFixtures` that:

1. reads `regex.WordTokenRegex().String()` and `levenshteinThreshold`;
2. scans `0..=0x10FFFF` against a compiled `^[\p{L}\d]$` to derive
   `letter_digit_ranges`, and renders them as the explicit character class
   written to [`src/word_char_class.rs`](../../src/word_char_class.rs);
3. calls the six Go functions above over the corpus and writes this JSON.

Then `go test ./internal/classifier/ -run TestZZGenTextmatchFixtures` and delete
the file. It imports only existing module dependencies, so `go.mod` is never
touched.

## Why a pinned word-char class

`src/word_char_class.rs` exists because a literal `[\p{L}0-9]` in Rust is **not**
Go's `[\p{L}\d]`. Two independent reasons, both measured rather than assumed:

* Go's RE2 `\d` is ASCII `[0-9]`; Rust's `\d` with Unicode enabled is all of
  `Nd` (Arabic-Indic `٣`, Devanagari `१`, …).
* Rust's `regex` crate ships **newer** Unicode tables than the Go toolchain
  bitmagnet is built with, so even `\p{L}` disagrees — at 4,924 code points
  (first: `U+1C89`, then `U+A7CB…`, `U+10600…`), all of which Rust counts as
  letters and Go 15.0.0 does not.

`tests/parity.rs::word_char_class_matches_go_over_all_scalar_values` re-proves
the pinned class equals Go's over all 1,112,064 Unicode scalar values, so a
future `regex` upgrade cannot silently reintroduce the drift.

## Table reuse

The go-unidecode transliteration tables and the `unicode.ToLower` map are **not**
duplicated in this crate. `src/lib.rs` includes
`crates/bitmagnet-fts/src/tables.rs` verbatim via `#[path]`, so the workspace
holds exactly one transcription of `go-unidecode`'s `table.Tables`; see that
crate's `tests/fixtures/README.md` for its own provenance and attribution.
