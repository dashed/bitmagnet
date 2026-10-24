# G1e.1 — Expose BitTorrent v2 identity via GraphQL — Spec

Status: **GO** (validated against code). Branch `feat/bittorrent-v2-graphql`.

Goal: expose the BitTorrent v2 (BEP 52) identity already stored on `model.Torrent`
(`InfoHashV2 *protocol.InfoHashV2`, `MetaVersion NullUint16`) through the GraphQL API,
with **zero hand-written resolvers** (pure gqlgen autobind), and keep generated code
(gqlgen + webui codegen) reproducible under CI's `task gen` + `git diff --exit-code`.

This was validated adversarially against the actual generated code; every claim below
has file:line evidence. The design as proposed is sound with one mandatory receiver
constraint (§2) and the webui scalar-map change (§4) being non-optional.

---

## 1. Verified facts (evidence)

- `model.Torrent` already carries the v2 fields:
  - `InfoHashV2 *protocol.InfoHashV2 \`gorm:"column:info_hash_v2"\``—`internal/model/torrents.gen.go:29`
  - `MetaVersion NullUint16 \`gorm:"column:meta_version"\``—`internal/model/torrents.gen.go:30`
- `protocol.InfoHashV2 [32]byte` has `String`/`Bytes`/`ToShort`/`Scan`/`Value`/`MarshalJSON`/`UnmarshalJSON`
  but **no** `MarshalGQL`/`UnmarshalGQL` — `internal/protocol/infohash_v2.go:23-112`.
- `protocol.ID` has the GQL pair we mirror: `func (id *ID) UnmarshalGQL(input interface{}) error`
  (**pointer receiver**, `id.go:191`) and `func (id ID) MarshalGQL(w io.Writer)`
  (**value receiver**, `id.go:207`).
- gqlgen config: `autobind` includes `internal/model` (`gqlgen.yml:68-70`); `Hash20 -> protocol.ID`
  (`gqlgen.yml:116-118`); `Int` is bound to `model.NullUint` **and** `model.NullUint16`
  (`gqlgen.yml:88-97`). `struct_tag: gqlgen` (`gqlgen.yml:30`) — note model.Torrent fields carry
  only `gorm`/`json` tags, so binding is by **case-insensitive field-name match**, which works
  (`infoHash`→`InfoHash`, `filesCount`→`FilesCount` already prove it).
- `type Torrent` binds to `*model.Torrent` — `gql.gen.go:18752` (`_Torrent(... obj *model.Torrent)`).
- The GraphQL `Torrent` type: `graphql/schema/models.graphqls` (`type Torrent { infoHash: Hash20! ... magnetUri: String! ... }`).
- Existing scalars: `graphql/schema/scalars.graphqls` (`Hash20`, `Date`, `DateTime`, `Duration`, `Void`, `Year`). No `Hash32`.
- `MetaVersion` is always set by the crawler: `internal/protocol/metainfo/parse.go:55,66` sets `MetaVersion`
  to `1` (v1) / `2` (v2/hybrid); persisted at `internal/dhtcrawler/persist.go:245`
  (`MetaVersion: model.NewNullUint16(uint16(parsed.MetaVersion))`). It is NULL only for
  pre-G1a/legacy rows and any importer that doesn't set it → nullable in GQL is correct.
- Torznab already emits the btmh-aware magnet: `internal/torznab/adapter/search_result.go:69-70`
  (`AttrMagnetURL = item.Torrent.MagnetURI()`); `AttrInfoHash`/`GUID` use the canonical 20-byte
  `InfoHash.String()` (lines 65-66, 178).

---

## 2. Design — Go / gqlgen side

### 2.1 Scalar declaration

Add to `graphql/schema/scalars.graphqls`:

```graphql
scalar Hash32
```

Bind it in `internal/gql/gqlgen.yml` (alongside `Hash20`):

```yaml
Hash32:
  model:
    - github.com/bitmagnet-io/bitmagnet/internal/protocol.InfoHashV2
```

### 2.2 GQL marshaller methods on `protocol.InfoHashV2`

Add to `internal/protocol/infohash_v2.go`, mirroring `id.go:191-209` **exactly** (receivers matter):

```go
func (h *InfoHashV2) UnmarshalGQL(input interface{}) error {
    switch input := input.(type) {
    case string:
        parsed, err := ParseInfoHashV2(input)
        if err != nil {
            return err
        }
        *h = parsed
        return nil
    default:
        return errors.New("invalid hash type")
    }
}

func (h InfoHashV2) MarshalGQL(w io.Writer) {
    _, _ = w.Write([]byte(`"` + h.String() + `"`))
}
```

**Receiver constraint (mandatory, not stylistic):**

- `MarshalGQL` MUST have a **value receiver** so that _both_ `InfoHashV2` and `*InfoHashV2`
  satisfy `graphql.Marshaler` (value-receiver methods are in the pointer's method set). The field
  is a pointer (`*protocol.InfoHashV2`), so gqlgen emits a pointer marshaller that returns the
  value directly as a `graphql.Marshaler` — see the exact precedent for `*model.Year` at
  `gql.gen.go:23793-23798` (`marshalOYear2ᚖ…Year(v *model.Year)` → `if v == nil { return graphql.Null }; return v`).
  A pointer receiver here would break compilation if gqlgen ever needs the value variant.
- `UnmarshalGQL` MUST have a **pointer receiver** (it mutates). Matches `id.go:191`.
- Add `"io"` to the import block (already imports `encoding/hex`, `errors`, `strings`).

### 2.3 Schema fields on `type Torrent`

Add two nullable fields to `type Torrent` in `graphql/schema/models.graphqls`:

```graphql
type Torrent {
  infoHash: Hash20!
  ...
  magnetUri: String!
  infoHashV2: Hash32      # null for v1-only; set for pure-v2 / hybrid (BEP 52)
  metaVersion: Int        # 1 = v1, 2 = v2/hybrid; null for legacy/un-parsed rows
  createdAt: DateTime!
  updatedAt: DateTime!
}
```

Both are **nullable** (no `!`). No resolver code is required — autobind maps:

- `infoHashV2` → `obj.InfoHashV2` (`*protocol.InfoHashV2`), marshalled by the generated
  `marshalOHash32…ᚖ…InfoHashV2` (nil → `null`). Resolver-free; precedent `ReleaseYearAgg.value`
  at `gql.gen.go:8331-8357` (`IsResolver:false, IsMethod:false`, direct `obj.Value`, nil-safe pointer marshalO).
- `metaVersion` → `obj.MetaVersion` (`NullUint16`, value type carrying its own `Valid` flag),
  marshalled by `marshalOInt…NullUint16`. Exact precedent: `filesCount` (`NullUint`) at
  `gql.gen.go:8979-9005` and `NullUint16.MarshalGQL` at `internal/model/null.go:462-469`
  (writes `"null"` when `!Valid`, else `%d`). `NullUint16` already round-trips Int via its
  `Marshal/UnmarshalGQL` (`null.go:425-469`).

### 2.4 Regenerate & commit (Go)

`task gen-gql` → `go run github.com/99designs/gqlgen generate --config ./internal/gql/gqlgen.yml`
(`Taskfile.yml:28-30`). Commit:

- `internal/gql/gql.gen.go` (new `_Torrent_infoHashV2`/`_Torrent_metaVersion` + `marshalOHash32…` funcs)
- `internal/gql/gqlmodel/gen/model.gen.go` (only if it changes — `Torrent` is autobound to
  `model.Torrent`, not regenerated as a gqlmodel struct, so this file likely shows no diff;
  commit whatever `task gen` produces).

---

## 3. webui / graphql-codegen side

### 3.1 `Hash32: "string"` in codegen.ts is MANDATORY

`webui/src/app/graphql/codegen.ts` has `strictScalars: true` (line 26) and a `scalars` map
(lines 28-35) that currently includes `Hash20: "string"`. The `typescript` plugin processes the
**whole schema**, so once `Hash32` is declared and used by `Torrent.infoHashV2`, codegen will fail
with an unknown-scalar error unless mapped. Add:

```ts
scalars: {
  Date: "string",
  DateTime: "string",
  Duration: "string",
  Hash20: "string",
  Hash32: "string",   // <-- required
  Void: "void",
  Year: "number",
},
```

### 3.2 generated/index.ts WILL change even without touching any operation document

The `typescript` plugin emits a base TS type for every schema type (incl. `Torrent`), so the new
`infoHashV2?: Maybe<Scalars['Hash32']['output']>` and `metaVersion?: Maybe<...>` appear regardless
of whether any `.graphql` document selects them. Therefore `webui/src/app/graphql/generated/index.ts`
**must be regenerated and committed**.

### 3.3 Fragment update is OPTIONAL (product decision)

`graphql/fragments/Torrent.graphql` does **not** need the new fields for codegen to succeed.
Adding `infoHashV2` / `metaVersion` there is only needed if the webui should actually consume them
(changes the `TorrentFragment` TS type + every query embedding it). Recommendation: keep the API
change (this slice) decoupled from UI consumption — leave the fragment unchanged unless a UI task
asks for it. No other webui breakage: GraphQL is additive; existing typed fragments need not select
the new optional fields.

### 3.4 Regenerate & commit (webui)

`task gen-webui-graphql` → `npm run graphql:codegen` in `./webui` (`Taskfile.yml:62-65`,
`webui/package.json:17`). Runs fully **offline** (schema = local `graphql/schema/**/*.graphqls`,
documents = local `graphql/{fragments,mutations,queries}/*.graphql`). Commit
`webui/src/app/graphql/generated/index.ts`.

---

## 4. Determinism / CI

CI's `generated` job runs full `task gen` then `git diff --exit-code`. Reproducibility holds because:

- gqlgen is version-pinned: `github.com/99designs/gqlgen v0.17.64` (`go.mod:6`); `go run` uses that
  pinned module → byte-identical output across machines on the same Go toolchain. gqlgen output is
  deterministic (stable field/func ordering).
- webui codegen versions are locked by `webui/package-lock.json`; CI installs via `npm ci`
  (`Taskfile.yml:130`). graphql-codegen output is deterministic for fixed plugin versions + fixed
  schema/document inputs.
- **Action item to avoid a CI red:** run the _full_ `task gen` (gqlgen **and** webui) locally with
  `npm ci`-installed deps and commit the complete result — do not hand-edit generated files. This is
  the known CI gotcha class for this repo (generated-diff + prettier-masks-lint). Run `task lint`
  locally too; prettier reformats `generated/index.ts`.

---

## 5. Scalar vs String decision (recommendation: Hash32)

Use a dedicated `Hash32` scalar, not `infoHashV2: String`:

- **Consistency:** mirrors `Hash20` for the v1 hash; the schema self-documents that this is a 32-byte
  hex hash, not arbitrary text.
- **Validation:** `UnmarshalGQL` enforces 64-hex / 32-byte parsing (`ParseInfoHashV2`), giving input
  validation for free if a `torrentByInfoHashV2` query is ever added.
- **Cost:** one line in `codegen.ts` (§3.1). Webui still sees it as `string` — no ergonomic loss.

---

## 6. Nullability semantics (confirmed correct)

| field                | null when                                          | non-null when                            |
| -------------------- | -------------------------------------------------- | ---------------------------------------- |
| `infoHashV2: Hash32` | v1-only torrent (`InfoHashV2 == nil`)              | pure-v2 or hybrid (BEP 52)               |
| `metaVersion: Int`   | legacy/pre-G1a rows or importers that don't set it | crawled rows: `1` (v1) / `2` (v2/hybrid) |

`metaVersion` should stay **nullable**: the DB column is nullable and historical rows predating G1a
have no value. Making it non-null (`Int!`) would force a non-null marshal on rows where
`NullUint16.Valid == false`, producing a `"must not be null"` GraphQL error at query time. Keep it `Int`.

---

## 7. Torznab — N/A (confirmed)

No change. There is no standard Torznab/Newznab attribute for a v2 infohash; `AttrInfoHash`/`GUID`
carry the canonical 20-byte identity (`search_result.go:65-66,178`) and `AttrMagnetURL` already
carries the v2 `urn:btmh` via `Torrent.MagnetURI()` (line 69-70, btmh-aware since G1b). Adding a
non-standard attribute would risk confusing existing Torznab clients (Prowlarr/Sonarr/Radarr) for no
interoperability gain. Recommendation: do not expose a v2 attr in Torznab.

---

## 8. Required test matrix

Go (unit):

1. `protocol.InfoHashV2.MarshalGQL` writes a quoted 64-hex string (round-trips `String()`).
2. `protocol.InfoHashV2.UnmarshalGQL`:
   - accepts a 64-hex string (and `0x`-prefixed, since `ParseInfoHashV2` strips `0x`);
   - rejects wrong length / non-hex (error, not panic);
   - rejects non-string input type (`"invalid hash type"`).
3. Marshal/unmarshal round-trip equality (`Unmarshal(Marshal(h)) == h`).

Go (gqlgen integration / resolver-level, mirroring existing Torrent field tests if present): 4. Pure-v2 torrent → `infoHashV2` is the 64-hex string, `metaVersion == 2`. 5. Hybrid torrent → `infoHashV2` non-null (full v2 hash), `metaVersion == 2`. 6. v1-only torrent → `infoHashV2 == null`, `metaVersion == 1`. 7. Legacy row (`MetaVersion.Valid == false`, `InfoHashV2 == nil`) → both fields `null` (no error).

Generated-code / CI guards: 8. After `task gen` (gqlgen + webui), `git diff --exit-code` is clean against the committed files
(`gql.gen.go`, `gqlmodel/gen/model.gen.go`, `webui/.../generated/index.ts`). This is the
authoritative GO check — if regen produces a diff in CI, the commit is incomplete. 9. webui `npm run graphql:codegen` succeeds with `strictScalars` (i.e. `Hash32` mapping present). 10. (Optional, if fragment updated) snapshot of `TorrentFragment` TS type includes `infoHashV2`/`metaVersion`.

---

## 9. File change list

| File                                       | Change                                                               |
| ------------------------------------------ | -------------------------------------------------------------------- |
| `graphql/schema/scalars.graphqls`          | add `scalar Hash32`                                                  |
| `graphql/schema/models.graphqls`           | add `infoHashV2: Hash32` + `metaVersion: Int` to `type Torrent`      |
| `internal/gql/gqlgen.yml`                  | add `Hash32 -> protocol.InfoHashV2` binding                          |
| `internal/protocol/infohash_v2.go`         | add `MarshalGQL` (value rcv) + `UnmarshalGQL` (ptr rcv); import `io` |
| `webui/src/app/graphql/codegen.ts`         | add `Hash32: "string"` to scalars map                                |
| `internal/gql/gql.gen.go`                  | **generated** (`task gen-gql`)                                       |
| `internal/gql/gqlmodel/gen/model.gen.go`   | **generated** (commit if changed)                                    |
| `webui/src/app/graphql/generated/index.ts` | **generated** (`task gen-webui-graphql`)                             |
| `internal/protocol/infohash_v2_test.go`    | tests §8.1-3 (new/extend)                                            |
| `graphql/fragments/Torrent.graphql`        | OPTIONAL — only if UI consumes the fields                            |
