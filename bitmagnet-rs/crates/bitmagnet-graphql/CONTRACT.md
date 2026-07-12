# bitmagnet-graphql — Phase-2 SDL 0-diff gate contract (G0/P1 spike output)

This is the contract Lane G (G1 schema) and Lane P (P2/P3) build on. It is proven
by `tests/sdl_parity.rs` in this crate. Produced by the G0/P1 de-risk spike
(branch `p2gp-spike`), which resolves Risk P2-1.

## 1. The gate

The SDL 0-diff gate asserts:

```
normalize_sdl(bitmagnet_graphql::schema().sdl()) == testdata/parity/schema.graphql
```

`testdata/parity/schema.graphql` is the Phase-0 B1 golden — a *normalized
concatenation of the Go source `graphql/schema/*.graphqls`*, produced by the Go
reference normalizer `internal/gql/schema_sdl_parity_test.go::normalizeSchemaSDL`.

**Answer to Risk P2-1's open question:** the P0 golden IS the correct canonical
target. No fresh introspection-based golden is needed. Because BOTH sides are run
through a *parser-based* canonicalizer (parse SDL → AST → sort → re-emit),
async-graphql's printed-SDL differences from the Go source (type/field ordering,
scalar/directive syntax, description formatting) are normalized away. The gate
therefore reduces to exactly: **presence, kind, type reference (incl. nullability),
and exact enum/scalar name strings.** Ordering is NOT part of the contract.

## 2. Normalization rules (P1 — `src/normalize.rs::normalize_sdl`)

A faithful Rust reimplementation of the Go `normalizeSchemaSDL`, using
`async-graphql-parser`'s `parse_schema`. The rules, verbatim:

1. **Parse** the SDL string to a type-system document.
2. **Drop** non-app definitions: the root `schema { … }` block; the 5 built-in
   scalars `String Int Float Boolean ID`; any type whose name starts with `__`
   (introspection); the built-in directive *definitions*
   `skip include deprecated specifiedBy oneOf`. Any non-built-in directive
   definition is kept (there are none today) and emitted sorted.
3. **Drop all descriptions / doc-strings.**
4. **Sort everything** (order-independence): type definitions by name; within each
   type — enum values, object/interface/input fields, field arguments, implemented
   interfaces, and union members — each sorted by name.
5. **Emit** canonically: 2-space indent, one member per line.
   - `scalar Name`
   - `enum Name {\n  VALUE\n  …\n}`
   - `union Name = A | B`
   - `type|interface|input Name[ implements A & B] {\n  field…\n}`
   - Object/interface field: `  name(arg: T, …): T`. **Argument** defaults render
     as ` = <default>`; **input-object field** defaults do NOT render (Go-parity:
     `writeFields` emits only the type for input fields — see the comment in
     `render_input_fields`). Type references use GraphQL canonical form `[Hash20!]!`.
   - Definitions separated by exactly one blank line; output right-trimmed of
     newlines then a single trailing `\n`.

**Headline proof (normalizer fidelity, independent of async-graphql):**
`normalizer_is_idempotent_on_full_golden` asserts `normalize_sdl(full_golden) ==
full_golden` byte-for-byte over the entire 854-line real schema. If any layout /
type-ref / sort rule diverged from Go, this fails.

## 3. The nullable-input wrapper decision (PINNED)

gqlgen runs with `nullable_input_omittable: true` + `omit_slice_element_pointers:
true`. The question was which async-graphql wrapper reproduces gqlgen's nullability.

**Finding: the SDL 0-diff gate is AGNOSTIC to the wrapper.** `Option<T>`,
`MaybeUndefined<T>`, and `Option<Option<T>>` all render a nullable input field
identically as `T` (no `!`). Proven by `nullable_wrapper_is_sdl_agnostic`
(`WrapperPinInput` → all three fields print `Boolean`).

So the choice is a **runtime-semantics** decision, not a gate constraint. gqlgen's
`Omittable[T]` adds exactly one capability: distinguishing "field absent" from
"field present = null". The faithful mirror is the 3-state
`async_graphql::MaybeUndefined<T>` (`Undefined | Null | Value`).

**Binding rule for Lane G:**
- Nullable **input** fields (everything gqlgen wrapped in `Omittable`): use
  `MaybeUndefined<T>`. It is free at the SDL layer and preserves absent≠null.
- Nullable **output** fields: use plain `Option<T>` (no absent/null distinction on
  output).
- Plain `Option<T>` on an input is acceptable only where the resolver treats
  absent == null (true for nearly all bitmagnet filters) — but `MaybeUndefined` is
  the correct default and costs nothing.

## 4. async-graphql mechanism list (for G1 — use verbatim)

async-graphql = "7" (resolved 7.2.1), async-graphql-parser = "7" (same major).
(8.0.0 is only rc — do NOT use.)

- **Custom scalars — pin the SDL name with the `scalar!` macro** (NOT the default
  derived name). Define a newtype with serde, then:
  ```rust
  #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
  struct Hash20(String);            // inner repr is free (String / i64 / unit)
  async_graphql::scalar!(Hash20, "Hash20");   // 2nd arg = exact SDL name
  ```
  `Void` wraps `()` the same way. This reproduces all 7 scalars
  (`Hash20 Hash32 Date DateTime Duration Year Void`) byte-for-byte.
  (Equivalent alternative: a manual `#[Scalar(name = "Hash20")] impl ScalarType`.)
- **Enums — pin every wire string per-variant** (async-graphql's default renames
  variants to SCREAMING_SNAKE, which would corrupt e.g. `tv_show`, `x264`,
  `WEBRip`, `V1080p`):
  ```rust
  #[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
  enum ContentType {
      #[graphql(name = "movie")]     Movie,
      #[graphql(name = "tv_show")]   TvShow,
      // …one #[graphql(name = "…")] per variant, exact Go enum string…
  }
  ```
  The Go strings are generated by `enums/gen/genenums.go` and ARE the contract.
- **Input objects — default field rename is camelCase**, which already matches the
  golden (`apis_disabled` → `apisDisabled`). Use snake_case Rust fields; no
  per-field override needed. Derive `#[derive(async_graphql::InputObject)]`.
- **Nullability / list mapping** (verified byte-identical to the golden):
  | GraphQL SDL        | async-graphql type                 | meaning                          |
  |--------------------|------------------------------------|----------------------------------|
  | `T!`               | `T`                                | required                         |
  | `T`                | `Option<T>` / `MaybeUndefined<T>`  | nullable (§3)                    |
  | `[T!]!`            | `Vec<T>`                           | required list, required elem     |
  | `[T!]`             | `Option<Vec<T>>`                   | nullable list, required elem     |
  | `[T]`              | `Option<Vec<Option<T>>>`           | nullable list, nullable elem     |
  All three list shapes occur in the real schema (`[Hash20!]!` in
  `TorrentReprocessInput`, `[Hash20!]` in `TorrentContentSearchQueryInput`,
  `[ContentType]` in `ContentTypeFacetInput`) and are covered by the tests.
- **Import path:** `async_graphql::MaybeUndefined`.
- The schema needs a root `Query` (async-graphql requirement). Mutations: declared
  in the SDL but routed to Go per Risk P2-3 (out of this spike's scope).

## 5. What is proven vs. what remains

Proven here (all green on Coder, rustc 1.97, async-graphql 7.2.1):
- The parser-based normalizer matches the Go canonical form over the whole real
  schema (idempotence).
- async-graphql reproduces the 7 scalars + 2 enums (Go-string values) + full
  nullability matrix byte-for-byte against a real slice of the golden
  (`testdata/parity/graphql/schema_subset.graphql`).
- The wrapper choice is SDL-agnostic; MaybeUndefined pinned for inputs.

Remains for Lane P (inherits `normalize.rs`): async-graphql's `.sdl()` may emit
introspection/`schema{}`/built-in-directive noise on the FULL schema — the
normalizer already drops the known set, but the full-schema G1 diff may surface a
stray built-in async-graphql emits (e.g. a `@oneOf` on input objects in newer
spec) that needs adding to the drop-list; handle at G1 golden regen. G1 builds the
full 869-line schema; P2 (shadow comparator + mutation-double-execute gate) and P3
(numeric gate) are separate.
