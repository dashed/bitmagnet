# G1e.4 — Adversarial review of the v2-identity GraphQL implementation

Verdict: **GO.** Reviewed read-only, refute-by-default, against the committed code
(`2f4e273 feat(gql): expose BitTorrent v2 identity ...`). Every claim below has
file:line evidence and was checked by reading the generated output and by empirically
re-running the generators. No blocking issues found. One subtle nil-path detail is
documented (it is correct, not a bug). No required changes; two optional follow-ups.

---

## 1. Autobind correctness — PASS (no hand-written resolver)

Both fields are generated as plain autobound field reads, **no resolver**:

- `_Torrent_infoHashV2` (`gql.gen.go:8740-8766`): `IsResolver:false, IsMethod:false`
  (`:8772-8773`), reads `obj.InfoHashV2` directly (`:8754`), marshals via
  `marshalOHash322ᚖ…InfoHashV2` (`:8765`).
- `_Torrent_metaVersion` (`gql.gen.go:8781-8807`): `IsResolver:false, IsMethod:false`
  (`:8813-8814`), reads `obj.MetaVersion` directly (`:8795`), marshals via
  `marshalOInt2…NullUint16` (`:8806`).
- No new entries in `internal/gql/resolvers/*.go`; commit touches no resolver file.

**nil → null (infoHashV2):** the real guard is in the marshaller, not the resolver.
`marshalOHash322ᚖ…InfoHashV2` (`gql.gen.go:22669-22674`): `if v == nil { return graphql.Null }; return v`.
Note (verified, not a bug): the resolver's `if resTmp == nil` (`:8760`) does **not** catch a
typed-nil `*protocol.InfoHashV2` boxed into `any` (that is a non-nil interface), so execution
falls through to `res := resTmp.(*protocol.InfoHashV2)` (= nil pointer) and the **marshaller**
renders `null`. End-to-end behaviour is correct because the marshaller dereferences/guards on the
concrete pointer. This is the identical pattern already in production for `ReleaseYearAgg.value`
(`*model.Year`, `marshalOYear2ᚖ…Year` at `:23793-23798`).

**null-when-!Valid (metaVersion):** `marshalOInt2…NullUint16` (`gql.gen.go:22692-22694`) is
`return v`; `NullUint16` is a value type whose `MarshalGQL` (`internal/model/null.go:462-469`)
writes `"null"` when `!Valid`, else `%d`. Same machinery as the existing `filesCount` (`NullUint`).

**Non-null marshalling guard:** the executor block (`gql.gen.go:18871-18874`) emits the two new
fields **without** the `if out.Values[i] == graphql.Null { atomic.AddUint32(&out.Invalids, 1) }`
wrapper that non-null fields like `infoHash`/`name` get (`:18868-18870`, `:18877-18879`). So a null
result is accepted, not promoted to a field error. Correct for nullable fields.

## 2. Receiver correctness — PASS

- `MarshalGQL` is a **value receiver** (`internal/protocol/infohash_v2.go:136-138`), so both
  `InfoHashV2` and `*InfoHashV2` satisfy `graphql.Marshaler`. The generated pointer marshaller does
  `return v` with `v *protocol.InfoHashV2` (`gql.gen.go:22673`) — this only compiles because the
  value-receiver method is promoted to the pointer's method set. The green build confirms it.
- `UnmarshalGQL` is a **pointer receiver** (`infohash_v2.go:116-130`); the generated
  `unmarshalOHash322ᚖ…` calls `res.UnmarshalGQL(v)` on `new(protocol.InfoHashV2)` (`:22664-22665`),
  i.e. on a pointer. Correct.
- No generated call site invokes the wrong variant. Hash32 appears only as a nullable output field
  bound to a pointer, so only the `marshalO…ᚖ` / `unmarshalO…ᚖ` variants are generated; both are
  receiver-consistent. `"io"` is imported (`infohash_v2.go:8`).

## 3. Generated determinism for CI — PASS (verified empirically)

Re-ran the full generation chain on this machine and diffed against the committed files:

- `task gen-gql-enums` + `task gen-gql` → `internal/gql/gql.gen.go` sha **unchanged**
  (`e194761…` before and after); `git diff` empty.
- `npm run graphql:codegen` (webui) → `webui/src/app/graphql/generated/index.ts` sha **unchanged**
  (`83e8e6c…`); `git diff` empty.
- After both regenerations, `git status` is **completely clean** — no stray file touched.

Determinism rationale: gqlgen is version-pinned (`go.mod` `github.com/99designs/gqlgen v0.17.64`,
invoked via `go run`), webui generator versions are locked by `webui/package-lock.json` (CI uses
`npm ci`, `Taskfile.yml:130`), and the generated webui dir is in `.prettierignore`
(`webui/.prettierignore: webui/src/app/graphql/generated/**/*.*`) so `task lint` will not reformat
it. No map-ordering or version-drift nondeterminism observed. CI's `task gen` + `git diff --exit-code`
will be green.

## 4. strictScalars / webui — PASS

`webui/src/app/graphql/codegen.ts` maps `Hash32: "string"` (alphabetically placed after `Hash20`).
Generated TS reflects it: `Hash32: { input: string; output: string; }` (`generated/index.ts:24`),
`infoHashV2?: Maybe<Scalars['Hash32']['output']>` (`:473`),
`metaVersion?: Maybe<Scalars['Int']['output']>` (`:476`). codegen ran without a strictScalars error.
Both fields are optional (`?`) → no breakage for existing typed fragments/queries that don't select
them. The `Torrent.graphql` fragment was (correctly) left unchanged — additive, no consumer forced.

## 5. Nullability — PASS

Schema: `infoHashV2: Hash32` and `metaVersion: Int`, both **nullable** (no `!`)
(`graphql/schema/models.graphqls`, Torrent block). Matches DB reality:
`InfoHashV2 *protocol.InfoHashV2` is nil for v1-only and set for v2/hybrid; `MetaVersion NullUint16`
is `1`/`2` for crawled rows (`internal/protocol/metainfo/parse.go:55,66` →
`internal/dhtcrawler/persist.go:245`) and NULL only for legacy/pre-G1a rows. Marking either as
non-null would raise a "must not be null" field error on legitimate rows. Correct as nullable.

## 6. Missed generated files / other schema consumers — PASS

Commit `2f4e273` changes exactly: `graphql/schema/{models,scalars}.graphqls`, `internal/gql/gqlgen.yml`,
`internal/gql/gql.gen.go`, `internal/protocol/infohash_v2.go(+_test)`, `webui/.../codegen.ts`,
`webui/.../generated/index.ts`, plus the spec doc. Notably **no** `internal/gql/gqlmodel/gen/model.gen.go`
— correct, because `Torrent` autobinds to `model.Torrent` and is not regenerated as a gqlmodel struct
(my full `task gen` rerun confirmed this file is untouched). `gen-gql-enums` produced no change
(no new enum). No second webui codegen output exists. Other schema references
(`internal/gql/enums/gen`, `internal/gql/httpserver`, resolvers) are unaffected by adding a scalar +
two scalar fields. Nothing missed.

## 7. Torznab — PASS (correctly untouched)

No code change in `internal/torznab/**` (commit only mentions it in the message). `AttrInfoHash`/`GUID`
keep the canonical 20-byte id and `AttrMagnetURL` already carries the `urn:btmh` magnet via
`Torrent.MagnetURI()` (G1b). No standard Torznab/Newznab v2-infohash attribute exists; adding a
non-standard one would risk confusing Prowlarr/Sonarr/Radarr. Correct to leave it.

## 8. Test adequacy — PASS (resolver test OPTIONAL, not required)

Present: `internal/protocol/infohash_v2_test.go` covers `MarshalGQL` (quoted 64-hex) and
`UnmarshalGQL` (round-trip, `0x` prefix, reject non-string, reject malformed), plus pre-existing
Parse/Scan/Value/ToShort/JSON coverage.

A gqlgen resolver/integration test is **OPTIONAL**, justification:

- The repo has **no** existing gql resolver-test harness; building one for this slice is
  disproportionate to the risk.
- The generated resolvers are thin direct field reads (`IsResolver:false`); the nil→null and
  !Valid→null logic is gqlgen's own stdlib marshaller machinery, byte-identical in shape to fields
  already in production (`filesCount`/`NullUint`, `ReleaseYearAgg.value`/`*model.Year`).
- The custom seam — `InfoHashV2.MarshalGQL`/`UnmarshalGQL` — is unit-tested; `NullUint16` GQL
  behaviour is shared code already exercised by `filesCount`.
- The remaining risk is caught by the compile (green build) + the CI generated-diff guard.

Optional follow-ups (nice-to-have, not blockers):

1. A table-test asserting `marshalOHash322ᚖ(nil) == graphql.Null` and a non-nil value renders the
   64-hex string — but this is testing gqlgen-generated code.
2. If/when a resolver-test harness is introduced for any reason, add a Torrent end-to-end case:
   pure-v2 → infoHashV2 set / metaVersion 2; v1-only → infoHashV2 null / metaVersion 1; legacy →
   both null.

---

## Summary table

| #   | Point                                                              | Verdict |
| --- | ------------------------------------------------------------------ | ------- |
| 1   | Autobind, no resolver, nil→null, !Valid→null                       | PASS    |
| 2   | Receiver correctness (value Marshal / ptr Unmarshal)               | PASS    |
| 3   | Generated determinism (empirically reproduced, clean tree)         | PASS    |
| 4   | strictScalars / `Hash32:"string"` / no webui breakage              | PASS    |
| 5   | Nullability (both nullable, matches DB)                            | PASS    |
| 6   | No missed generated file / other schema consumer                   | PASS    |
| 7   | Torznab untouched & correct                                        | PASS    |
| 8   | Test adequacy (unit + CI guard sufficient; resolver test OPTIONAL) | PASS    |

**Overall: GO.** No required changes.
