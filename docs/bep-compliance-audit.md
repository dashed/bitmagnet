# bitmagnet — BitTorrent Enhancement Proposal (BEP) Compliance Audit

**Date:** 2026-06-04
**Scope:** Read-only audit of bitmagnet's compliance with the full BEP corpus
(`bittorrent.org/beps/`, 56 BEPs + process docs).
**Method:** Multi-agent investigation (DHT, metadata/wire, peripheral, catalog),
with the lead independently verifying the highest-severity findings against source.
**Status:** Analysis only — **no code changes made**. Actionable gaps are tracked as
follow-up tasks (see [§7](#7-prioritized-gap-register--follow-up-tasks)).

---

## 1. Executive summary

bitmagnet is **not a BitTorrent client**. It is a _passive DHT crawler + metadata
fetcher + indexer/search engine_. It:

1. crawls the mainline DHT to **discover infohashes** (primarily via `sample_infohashes`, BEP 51);
2. fetches each torrent's **info dict** from peers via the **ut_metadata** extension (BEP 9 over BEP 10);
3. parses, classifies, indexes and serves the metadata (GraphQL / Torznab / web UI);
4. derives **seeders/leechers counts exclusively from DHT scrape** (BEP 33) — it is **trackerless by design**.

It never downloads piece data, never seeds, and never contacts a tracker. BEP
applicability is judged through that lens.

**Overall verdict:** The core crawl/index path is **solidly compliant** with the BEPs
that matter for an indexer (BEP 3, 5, 9, 10, 23, 51 are correct; BEP 33 client-side is
correct). The notable gaps fall into three buckets:

| Theme                        | Gap                                                                                                                                                                                                              | Severity |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| **Modern-format coverage**   | **BitTorrent v2 / hybrid (BEP 52)** is non-compliant and _not representable_ — the infohash type is a hard 20-byte SHA-1 primary key. Pure-v2 torrents are **silently dropped**; hybrids are **degraded to v1**. | **High** |
|                              | **Padding files (BEP 47)** are not recognized — they pollute file listings and inflate file counts on modern/hybrid torrents.                                                                                    | Medium   |
| **DHT hardening / security** | DHT response handling matches replies by **transaction ID only** (predictable monotonic counter) with **no source-address check** → off-path response-injection / routing-table & peer poisoning (BEP 5).        | Medium   |
|                              | **Node ID is not BEP 42-derived** (random + client suffix), reducing routing-table retention and inbound query yield.                                                                                            | Medium   |
| **IPv6 coverage**            | **IPv6 DHT (BEP 32) and dual-stack (BEP 45) are disabled** — the wire structs exist but are dead scaffolding; sockets are AF_INET only. Metadata dial is **IPv4-only** too.                                      | Medium   |

None of these break the current v1/IPv4 crawl; they bound _coverage_, _yield_, and
_robustness_. The v2 gap is the most strategically important as v2/hybrid torrents grow.

> **Implementation status (updated 2026-06-05):** Implemented on the `dashed/bitmagnet`
> fork — **G9** ✅ (single-file extension in the Tantivy doc, PR #5), **G2** ✅ (DHT
> response source-address verification + `crypto/rand` transaction IDs, PR #6), and the
> **G1 foundation (G1a)** ✅ (BitTorrent v2 / hybrid torrents are now ingested instead of
> dropped/degraded — see the G1 note below). All verified (golangci-lint v2.1.6 clean,
> `go test ./...` + `-race`, v2/hybrid integration tests against Postgres, generated code
> regenerated). The remaining v2 slices (G1b–G1e) and gaps G3–G8, G10 are open. The
> findings below are preserved as the original pre-implementation audit; the "Recommended
> fix" text for completed gaps describes what was actually implemented.

---

## 2. Methodology & applicability framing

- **Corpus:** 56 BEPs present as `.rst` under `/…/bittorrent.org/beps/`, cross-checked
  against the official index (`bep_0000`). BEP 13 (Protocol Encryption / MSE) has no
  `.rst` but is listed in `bep_1000` as implemented-but-unwritten.
- **Applicability classes:** **Core** (essential to bitmagnet's function), **High**
  (directly relevant / partially present), **Peripheral** (edge/optional), **N/A**
  (download / seed / tracker / LAN-client concerns outside a trackerless non-downloading crawler).
- **Compliance status values:** `Compliant` · `Partial` · `Non-compliant` ·
  `Not-applicable` · `Delegated` (handled correctly by a dependency).
- **Dependency note:** bitmagnet implements the **entire DHT/KRPC stack itself**
  (`internal/protocol/dht/**`). `anacrolix/dht/v2` is used **only** for the default
  bootstrap host list (`dhtcrawlerfx/module.go`); `anacrolix/torrent/bencode` +
  `anacrolix/torrent/metainfo` do bencode/info-dict work. So "delegated" applies to
  bencode/metainfo parsing, not to the DHT protocol.

---

## 3. Compliance matrix (full corpus)

Legend: ✅ Compliant · 🟡 Partial · ❌ Non-compliant · ⚪ N/A · 🔵 Delegated

| BEP                                | Title                                                                                                       | Applicability   | Status    | One-line finding                                                                                                                                                                                                                                           |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------- | --------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 3                                  | BitTorrent Protocol (bencode/info-dict)                                                                     | Core            | 🔵✅      | Protocol-compliant on active path (raw-byte infohash `parse.go:13`, correct handshake). Minor functional gaps: single-file torrents get no file rows/ext-indexing (G9); non-UTF-8 names dropped (G10); 10 MiB cap; dead `.torrent` parser. v1 fields only. |
| 5                                  | DHT Protocol                                                                                                | Core            | ✅ (gaps) | Full ping/find_node/get_peers/announce_peer responder; IP-bound tokens. Gaps: response source-addr not checked; token secret never rotated.                                                                                                                |
| 9                                  | ut_metadata (metadata exchange)                                                                             | Core            | ✅        | Hand-rolled, BEP 9-correct: 16 KiB pieces, last-piece rule, reject handling, hash verification.                                                                                                                                                            |
| 10                                 | Extension Protocol                                                                                          | Core            | ✅        | LTEP bit 20 set + checked; `m`/`ut_metadata` handshake; extended msg id 20.                                                                                                                                                                                |
| 51                                 | DHT Infohash Indexing (sample_infohashes)                                                                   | Core            | ✅        | Well-implemented both as client (discovery) and responder; smart interval/candidate handling.                                                                                                                                                              |
| 52                                 | **BitTorrent v2 / hybrid**                                                                                  | Core            | ❌        | **Not representable** — 20-byte SHA-1 infohash PK; pure-v2 dropped, hybrid degraded to v1.                                                                                                                                                                 |
| 23                                 | Compact peer lists                                                                                          | High            | ✅        | `values` parsed as compact 4+2 / 16+2 by length (`dht/nodeaddr.go`).                                                                                                                                                                                       |
| 27                                 | Private torrents                                                                                            | High            | ✅        | `info.Private` stored on the model (`persist.go:161-164`). Correct scope for a crawler.                                                                                                                                                                    |
| 32                                 | IPv6 DHT                                                                                                    | High            | ❌        | want/nodes6 structs exist but unused; AF_INET-only sockets.                                                                                                                                                                                                |
| 33                                 | DHT scrape                                                                                                  | High            | 🟡        | Client-side BLS bloom filter fully correct; responder does not _serve_ scrape (fine for an indexer).                                                                                                                                                       |
| 42                                 | DHT Security Extension                                                                                      | High            | ❌        | Node ID is random + `-BM0001-` suffix, **not** CRC32C-from-IP; `ip` echo never set.                                                                                                                                                                        |
| 43                                 | Read-only DHT nodes                                                                                         | High            | 🟡        | Never sets `ro` (correct). Ignores incoming `ro` (adds such nodes to routing table).                                                                                                                                                                       |
| 45                                 | Multiple-address / dual-stack DHT                                                                           | High            | ❌        | Single AF_INET socket; moot until BEP 32 enabled.                                                                                                                                                                                                          |
| 47                                 | Padding files / extended file attrs                                                                         | High            | ❌        | `attr`/padding ignored; padding files counted & listed (`persist.go:176-188`).                                                                                                                                                                             |
| 4                                  | Assigned Numbers (reserved bits)                                                                            | Peripheral      | 🟡        | Uses the reserved-bit registry; sets DHT(0)+LTEP(20); Fast(2)/V2(7) defined but unset.                                                                                                                                                                     |
| 11                                 | Peer Exchange (ut_pex)                                                                                      | Peripheral      | ❌        | Not implemented; peers come from DHT get_peers. Defensible.                                                                                                                                                                                                |
| 12                                 | Multitracker metadata                                                                                       | High→Peripheral | 🟡        | `announce-list` parsed but **unused** (trackerless).                                                                                                                                                                                                       |
| 13                                 | Protocol Encryption (MSE/PE)                                                                                | Peripheral      | ❌        | Metadata fetch is plain TCP; no MSE.                                                                                                                                                                                                                       |
| 19                                 | WebSeed url-list                                                                                            | Peripheral      | 🟡        | `url-list` parsed but **unused**.                                                                                                                                                                                                                          |
| 20                                 | Peer ID conventions                                                                                         | Peripheral      | ✅        | Azureus-style `-BM0001-` peer id.                                                                                                                                                                                                                          |
| 44 / 46                            | DHT arbitrary / mutable items                                                                               | Peripheral      | ⚪        | Structs/error-codes present but unimplemented (dead scaffolding).                                                                                                                                                                                          |
| 53                                 | Magnet select-only                                                                                          | Peripheral      | ⚪        | Selective-download concern; no download path.                                                                                                                                                                                                              |
| 6, 16, 17, 21, 38, 54              | Fast ext / superseed / HTTP-seed / partial-seed / data-hints / lt_donthave                                  | N/A             | ⚪        | Download/seed-time behaviors; bitmagnet never downloads or seeds.                                                                                                                                                                                          |
| 7, 15, 24, 31, 41, 48              | Tracker (IPv6 / UDP / ext-IP / retry / UDP-ext / scrape)                                                    | N/A             | ⚪        | **Trackerless by design** — seeders via DHT scrape (BEP 33).                                                                                                                                                                                               |
| 8, 28, 34                          | Tracker obfuscation / exchange / DNS prefs                                                                  | N/A             | ⚪        | Tracker-related.                                                                                                                                                                                                                                           |
| 14, 22, 26                         | LSD / local tracker / Zeroconf                                                                              | N/A             | ⚪        | LAN discovery; uses global DHT.                                                                                                                                                                                                                            |
| 29                                 | uTP transport                                                                                               | N/A             | ⚪        | Uses TCP (metadata) + KRPC/UDP (DHT); no uTP.                                                                                                                                                                                                              |
| 30                                 | Merkle hash torrent                                                                                         | N/A             | ⚪        | No merkle code (v2 merkle = BEP 52).                                                                                                                                                                                                                       |
| 18, 25, 35, 36, 37, 39, 40, 49, 50 | Search-engine spec / cache / signing / RSS / proxy / feed-url / peer-priority / distributed-feeds / pub-sub | Peripheral→N/A  | ⚪        | Not implemented; out of scope.                                                                                                                                                                                                                             |
| 55                                 | Holepunch                                                                                                   | Peripheral      | ⚪        | NAT traversal for peer conns; direct fetch doesn't need it.                                                                                                                                                                                                |

(Process docs BEP 0/1/2 and index BEP 1000 omitted as non-technical.)

---

## 4. Core BEPs — detailed findings

### BEP 3 — BitTorrent protocol / bencode / info-dict — 🔵✅ Compliant (v1), with minor functional gaps

**Protocol-level: compliant on the active path.** There are no BEP 3 _protocol violations_ in the live DHT/ut_metadata path:

- bencode + info-dict parsing delegated to `anacrolix/torrent` (`internal/protocol/metainfo/metainfo.go` aliases `mi.Info`).
- Infohash integrity: `ParseMetaInfoBytes` recomputes the SHA-1 over the **raw received** info-dict bytes (`mi.HashBytes` = `infohash.HashBytes`, raw `sha1.Sum`) and rejects on mismatch — `internal/protocol/metainfo/parse.go:13`. This is the _correct_ way to compute the infohash (no lossy re-encode) and doubles as anti-poisoning (see BEP 9).
- Peer-wire **handshake** is BEP 3-correct: 68-byte frame `<19>"BitTorrent protocol"<reserved><info_hash><peer_id>`, prefix + infohash-echo checked (`metainforequester/requester.go:165-212`).
- Single-file **size** and **name** are captured even with no `files[]`: `Size = info.TotalLength()` (which sums `UpvertedFiles()`) and `name = info.BestName()` (honors the `name.utf-8` convention) — `persist.go:159,213`.

**Functional gaps / deviations (not protocol breaks, but real):**

- **[Low — G9] Single-file torrents are missing their extension in the Tantivy search index.** The original framing ("single-file torrents get no file rows / no extension indexing") is **partly imprecise**: the Postgres path already filters single-file torrents by extension via the generated `torrents.extension` column (`criteria_torrent_file_extension.go:17-19`), and `transformer.go`'s `Single` case + `model.FileExtensions()` already derive the extension from the name for display. **The actual, observable gap** is symmetric in the new Tantivy search document: both Go `BuildDocument` (`document.go:71`) and Rust `transform.rs` build `file_extensions` solely from per-file blob entries — empty for single-file torrents → invisible to Tantivy extension/file-type faceting. **Recommended fix:** synthesize `file_extensions = [name-derived ext]` (only — _not_ `file_paths`, to preserve the weight-A/weight-D split and Postgres ranking parity) for `FilesStatusSingle` in both builders, plumbing `files_status` into the Rust indexing SQL; keep multi-file output byte-identical, with no schema/proto/DocID/persist changes. **Avoid** the naive prescription of building file rows from `info.UpvertedFiles()` in `persist.go` — it would corrupt the load-bearing `FilesStatusSingle` semantics (`SingleFile()`/`HasFilesInfo()` `torrents.go:99-105`, `transformer.go` Single case, `processor.go:213`, the re-crawl gate `infohash_triage.go:86-88`), and the persisted `FileExts` column is write-only (no search reader). `NoInfo` is out of scope (PG only generates `extension` for single-file). The broader "single-file has no per-file _listing_" point is a deliberate design choice (file == torrent), left as-is.
- **[Low/Med — G10] Non-UTF-8 names are dropped.** BEP 3 strings are _byte strings_ and need not be valid UTF-8 (hence the `name.utf-8`/`path.utf-8` convention). `banning/utf8.go:13-27` **rejects** any torrent whose `BestName()`/file paths aren't valid UTF-8 (or contain NUL). Torrents with legitimate non-UTF-8 names (Shift-JIS/GBK/Latin-1, etc.) that lack a `name.utf-8` are discarded entirely → lost coverage of legacy/non-Latin content. Defensible (Postgres `text` can't store invalid UTF-8) but a strictness deviation from the byte-string model; a transliterate/sanitize-and-keep approach would be more faithful.
- **[Low] 10 MiB info-dict cap.** `maxMetadataSize = 10 MiB` (`requester.go:228`) rejects very large info dicts; BEP 3 sets no upper bound, so torrents with hundreds of thousands of files can be dropped. Rare.
- **[Low — code quality] Dead, hash-unsafe `.torrent` parser.** `metainfo.TorrentFile{ Info Info }` / `ReadTorrentFileBytes` (`read_torrent_file.go`) decodes `info` into a struct **without retaining the raw `info` bytes** and computes no infohash; it has **no non-test callers** (the importer ingests pre-computed `Item`s, it does not parse `.torrent` files). If ever wired up and used to derive an infohash by re-marshaling `Info`, the result would be wrong for any dict with unmodeled keys. Recommend removing it, or — if a `.torrent` import path is added — using `anacrolix/torrent/metainfo.MetaInfo` (retains `InfoBytes bencode.Bytes`) + `HashInfoBytes()`.
- Limitation: only v1 info-dict fields are consumed (see BEP 52 / G1).
- Negligible: `piece length`/`pieces` consistency (len multiple of 20, count vs total length) is not validated — harmless since bitmagnet never verifies piece data.

### BEP 5 — DHT protocol — ✅ Compliant, with hardening gaps

- Responder handles `ping`, `find_node`, `get_peers` (values | closest+token), `announce_peer` (token-verified) — `internal/protocol/dht/responder/responder.go:54-115`.
- Client issues `ping`/`find_node`/`get_peers`; **no `announce_peer`** by design (passive indexer) — `internal/protocol/dht/client/interface.go`.
- Token derivation `md5(secret‖nodeID‖infohash‖queryingID‖addr)` is **IP-bound** per spec — `responder.go:128-137`.
- Routing table: custom Kademlia binary tree, bucket cap k=80 (`ktable/btree`, `factory.go`), larger than spec's 8 — beneficial for a crawler.
- **Gap [Med] — response injection:** `server.handleResponse` correlates replies by transaction ID **only**, never checking `msg.From` against the queried address (`internal/protocol/dht/server/server.go:146-156`), and transaction IDs are a **predictable monotonic uvarint counter from 0** (`server/id_issuer.go:18-26`) — letting an off-path attacker who guesses the TID and spoofs the source address inject forged nodes/peers → routing-table / peer-list poisoning. _Verified independently._ **Recommended fix:** store the queried address per in-flight transaction (`pendingQuery{ch, addr}`) and drop any response whose `msg.From` doesn't match — comparing with `netip.Addr.Unmap()` on **both** sides (mandatory: bootstrap addresses arrive 4-in-6 from `net.ResolveUDPAddr`, while the AF_INET socket yields 4-byte response addresses, so a naïve compare would false-reject every bootstrap reply and break crawl startup). Replace the monotonic issuer with `crypto/rand` 2-byte transaction IDs, kept unique among in-flight queries via collision-retry under the mutex; deliver via a non-blocking send on the cap-1 channel (also closes a pre-existing duplicate-response hang); add a `dht_server_response_dropped_total{reason}` counter for observability.
- **Gap [Low] — token secret never rotated:** set once at construction (`responder/factory.go`); BEP 5 mandates ~5-min rotation / 10-min window. Mitigated by IP-binding.
- **Gap [Low] — no routing-table persistence:** in-memory only; full re-bootstrap on restart.

### BEP 9 — ut_metadata — ✅ Compliant

- Implemented in `internal/protocol/metainfo/metainforequester/requester.go`: extension handshake reads `metadata_size` + `ut_metadata` id (`exHandshake`); requests `ceil(size/2^14)` pieces; enforces the **16 KiB piece rule** including the "last piece may be shorter" clause (`readAllPieces:326-339`); handles `reject` (msg_type 2); assembled metadata is **hash-verified** against the infohash (`parse.go:13`).
- Minor: `maxMetadataSize = 10 MiB` cap could truncate very large multi-file torrents (`requester.go:228`).

### BEP 10 — Extension protocol — ✅ Compliant

- Reserved LTEP bit 20 set (`requester.go:163`) and required of the peer (`btHandshake:192-194`); `m` dict advertises `ut_metadata`; extended message id 20 processed (`readExMessage`).

### BEP 51 — DHT infohash indexing — ✅ Compliant (well-implemented)

- Client `SampleInfoHashes` parses `samples`/`nodes`/`num`/`interval` (`dht/client/server_adapter.go:83-119`).
- Crawl loop (`internal/dhtcrawler/sample_infohashes.go`): candidate gating (`node.IsSampleInfoHashesCandidate`), in-memory bloom dedup, respects `interval` but overrides hostile >300 s backoffs to 60 s for productive nodes, deprioritizes empty responders, harvests returned nodes; `soughtNodeID` rotates every 10 s for keyspace coverage.
- Also **serves** `sample_infohashes` (cooperative; `responder.go:99-111`).

### BEP 52 — BitTorrent v2 / hybrid — ❌ Non-compliant (strategic gap)

- **Root cause is the schema, not the parser.** `protocol.ID` is a hard `[20]byte` and is the **primary key** of every torrent table (`internal/protocol/id.go:52`; `ParseID`/`UnmarshalBinary` reject ≠20 bytes; `internal/model/torrents.gen.go`). There is **no 32-byte / SHA-256 infohash anywhere** in the model. A v2 infohash cannot be stored even if parsed.
- Where v2 dies:
  1. `ParseMetaInfoBytes` verifies with SHA-1 `mi.HashBytes` (`parse.go:13`); a pure-v2 info dict hashes under SHA-256, so the check **always fails → silently dropped**.
  2. Handshake never advertises the v2 upgrade bit (`ExtensionBitV2 = 7` defined but omitted from `myExBits`, `requester.go:41,163`); `btHandshake` does an exact 20-byte infohash compare.
  3. `createTorrentModel` reads `info.Files` directly, not `info.UpvertedFiles()` (`persist.go:169-176`) — a v2-only dict (empty `Files`, populated `FileTree`) would mis-classify and lose files (though #1 prevents reaching here).
  4. Magnets emitted as v1 `urn:btih:` only (`internal/model/torrents.go:93`); no `urn:btmh:`.
- **Hybrid (v1+v2)** torrents are ingested but **degraded to v1**: the v1 SHA-1 infohash passes, v1 `Files`/`Pieces` stored, but `MetaVersion`/`FileTree`/piece-layers ignored — v2 identity lost.
- The dependency is _not_ the blocker: `anacrolix/torrent v1.58.0` fully supports v2 (`metainfo.Info.MetaVersion`/`FileTree`/`ExtendedFileAttrs`, `types/infohash-v2`, `magnet-v2`). bitmagnet uses none of it.
- **Severity: High** — a growing share of the swarm is invisible (pure-v2) or partially indexed (hybrid). A real fix is a migration (dual/32-byte infohash columns, `btmh` magnets, `UpvertedFiles`, advertise bit 7, SHA-256 verification, piece layers), not a one-line change.

> **Status — G1a foundation implemented (2026-06-05).** The foundation slice is done on
> branch `feat/bittorrent-v2-foundation`: pure-v2 and hybrid torrents are now **ingested
> and representable** instead of silently dropped/degraded. Specifically — (1) `parse.go`
> verifies the received info dict under **both** SHA-1 and (truncated) SHA-256 and returns
> a `ParsedInfo{MetaVersion, InfoHashV1, InfoHashV2}` descriptor (fixes #1 above);
> (2) `createTorrentModel` now classifies by `info.IsDir()` and enumerates files via
> `info.UpvertedFiles()`, so v2 `FileTree` torrents get correct file rows (fixes #3);
> (3) a new `protocol.InfoHashV2 [32]byte` type + migration `00023` add
> `info_hash_v1` / `info_hash_v2` (plain index) / `meta_version` columns recording the full
> v2 identity. (The `info_hash_v2` index is deliberately **non-unique**: a hybrid is
> announced under both its v1 and truncated-v2 hashes, so it can be ingested as two rows
> with the same full v2 hash; a UNIQUE index would abort the batched persist upsert. Exact
> v2 dedup is G1b.) **Design decision (synthesis):** the canonical `info_hash` **primary key stays
> 20 bytes** — the v1 SHA-1 for v1/hybrid and the BEP-52 **truncated** SHA-256 for pure-v2
> (the value the DHT crawl already keys on) — so no `protocol.ID`/FK/GraphQL/Rust-reader
> churn was needed. The full 32-byte hash lives in `info_hash_v2`. Deferred to follow-on
> stacked branches: **G1b** `btmh` magnets + DHT truncated-hash lookups (fixes #4 + the
> hybrid-vs-v2 dedup gap), **G1c** advertise handshake bit 7 (fixes #2), **G1d** Rust
> parity (read `info_hash_v2`/`meta_version`), **G1e** GraphQL/API surface. v2 **piece
> layers** live outside the info dict (in the `.torrent` `piece layers` key) and are not
> recoverable on the ut_metadata crawl path, so they remain out of scope. Full design +
> decision record: `docs/dev/g1a-v2-foundation-spec.md`.

> **G1d ↔ file-grained search convergence.** The proposed **file-grained Tantivy index**
> (one doc per _file_; `docs/dev/file-grained-search-spec.md`) is the natural home for BEP-52
> **per-file identity** — v2 makes files first-class with per-file merkle roots, which map
> directly onto one-doc-per-file granularity. When G1d (Rust reads `info_hash_v2`/`meta_version`)
> lands, the file index can carry per-file v2 attributes without further schema churn (the file
> index is a disposable, rebuildable cache — a schema bump just triggers a re-backfill).
>
> ⚠️ **Name collision (disambiguation):** this audit's **G1d** (BEP-52 v2 Rust parity) is unrelated to
> the file-grained-search work's **"G1"** (the empty-blob-`extension` correctness fix in
> `docs/dev/perfile-search-complete-parity.md`). The latter is the same _path-derivation_ family as
> this audit's **G9** (single-file extension), not a v2 concern. See the complete-parity analysis for
> the full per-file parity composition and its two prerequisites (G1, G2).

---

## 5. High-relevance BEPs — detailed findings

### BEP 23 — Compact peer lists — ✅

`values` decoded as `[]NodeAddr`; `NodeAddr.UnmarshalBinary` distinguishes 4+2 (v4) and 16+2 (v6) by length (`internal/protocol/dht/nodeaddr.go:36-42`).

### BEP 27 — Private torrents — ✅ (storage)

`info.Private` read into `model.Torrent.Private` (`persist.go:161-164`). For a crawler, _storing_ the flag (rather than honoring DHT/PEX restrictions as a client would) is the correct scope.

### BEP 32 / 45 — IPv6 & dual-stack DHT — ❌

Wire structs exist — `Want` n4/n6, `Nodes6`/`CompactIPv6NodeInfo` (`dht/msg.go`) — but the client never sends `want`, the responder never reads it or returns `Nodes6`, and sockets are **AF_INET only** (`server/socket_unix.go`, `socket_windows.go`; binds `IPv4Unspecified`). IPv6-only nodes/peers are invisible. **[Med]**

### BEP 33 — DHT scrape — 🟡 (client compliant; not served)

`ScrapeBloomFilter` is spec-correct: m=2048, k=2, `sha1(ip)`, dual index, `EstimateCount` matches the BEP-33 formula (`internal/protocol/dht/scrape.go`). Client sends `Scrape:1`, decodes `BFsd`/`BFpe` (`client/server_adapter.go:60-81`); crawler turns them into seeders/leechers (`dhtcrawler/scrape.go`, persisted `Source:"dht"` in `persist.go:287-298`). The **responder does not serve** scrape — acceptable for an indexer. **[Low]**

### BEP 42 — DHT security extension — ❌

Node ID = `RandomNodeIDWithClientSuffix` (random + `-BM0001-`), **not** CRC32C-derived from external IP (`internal/protocol/id.go:42-50`; `dhtfx/module.go`). The `ip` key is never echoed in responses. Non-conforming IDs are deprioritized/enforced by many modern nodes → **reduced routing-table retention → fewer inbound `get_peers`/`sample_infohashes` queries**, which is itself a node-discovery source. Affects **yield, not correctness**. **[Med]**

### BEP 43 — Read-only DHT nodes — 🟡

bitmagnet correctly never sets `ro` (it does respond). But incoming `ro` is ignored: `responderNodeDiscovery` adds every successful querier to discovery regardless of `ro` (`responder/node_discovery.go:17-31`); BEP 43 says don't add read-only senders. Cost: wasted pings (pruned anyway). **[Low]**

### BEP 47 — Padding files / extended attrs — ❌

anacrolix exposes `ExtendedFileAttrs.Attr` (`p` = padding, plus symlink/sha1), but bitmagnet ignores `Attr`: `createTorrentModel` adds **all** `info.Files` — including `.pad/…` padding files — to the file list and `FilesCount` (`persist.go:176-188`), and the UTF-8 banning checker iterates them too (`banning/utf8.go`). Padding files **pollute listings and inflate counts** on modern/hybrid torrents. **[Med]**

---

## 6. Peripheral / N-A highlights

- **Trackerless by design (confirmed):** No UDP/HTTP tracker client exists anywhere. `announce`/`announce-list`/`url-list` are parsed off `.torrent` files (`read_torrent_file.go:12-17`) but **never dialed or persisted** — so **BEP 7/12/15/19/24/31/41/48 are all N/A**. Seeders/leechers come solely from DHT scrape (BEP 33). `internal/metrics/torrentmetrics` only SQL-aggregates existing rows for Prometheus, it is not a peer-count source.
- **BEP 11 (PEX):** not implemented. Could marginally expand peer discovery but DHT `get_peers` is the intended source — low priority.
- **BEP 13 (MSE/PE):** metadata fetch is plain TCP; some peers require encryption, so a minority of metadata fetches may fail. Low priority.
- **BEP 44/46 (DHT items):** message structs and error codes exist (inherited from anacrolix-style msg types) but are unimplemented dead scaffolding. Correctly omitted for an indexer.
- **BEP 4 / 20:** reserved-bit registry and Azureus-style `-BM0001-` peer/node id are used appropriately.

---

## 7. Prioritized gap register → follow-up tasks

> These are **recommendations**; no code has been changed. Severity reflects impact on a
> trackerless non-downloading crawler/indexer (coverage, yield, robustness, correctness).
>
> **Status (2026-06-05):** ✅ **G9** (PR #5), ✅ **G2** (PR #6), and the ✅ **G1
> foundation (G1a)** (`feat/bittorrent-v2-foundation`) are implemented and verified; the
> follow-on v2 slices **G1b–G1e** and gaps G3–G8, G10 remain open. The rows below are the
> original findings.

| #   | Gap                                                                                                         | BEP         | Severity          | Nature of fix                                                                                                                                                                                                                                                                                                                                                                            |
| --- | ----------------------------------------------------------------------------------------------------------- | ----------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| G1  | **BitTorrent v2 / hybrid support** — pure-v2 dropped, hybrid degraded                                       | 52 (+47)    | **High**          | Large, multi-PR. **G1a foundation ✅ done** (`feat/bittorrent-v2-foundation`): dual-hash columns + 20-byte canonical PK (truncated SHA-256 for pure-v2), SHA-256 verification, `UpvertedFiles` — v2/hybrid now ingested. **Open:** G1b `btmh` magnets + DHT truncated-hash lookups, G1c advertise ext bit 7, G1d Rust parity, G1e GraphQL surface. (Piece layers N/A on the crawl path.) |
| G2  | **DHT response source-address verification** + non-predictable transaction IDs                              | 5           | Medium (security) | Store the queried addr per in-flight TID and drop source-mismatched responses (`netip.Addr.Unmap()`-normalized on both sides — avoids false-rejecting 4-in-6 bootstrap replies); replace monotonic TIDs with `crypto/rand` + collision-retry; add a drop counter. Confined to `internal/protocol/dht/server`.                                                                            |
| G3  | **Padding-file (BEP 47) awareness** — exclude `attr:p` files from listings/counts                           | 47          | Medium            | Read `ExtendedFileAttrs.Attr`; skip padding files in `createTorrentModel` + banning.                                                                                                                                                                                                                                                                                                     |
| G4  | **IPv6 / dual-stack DHT** — activate existing want/nodes6 scaffolding + v6 socket; allow IPv6 metadata dial | 32, 45, (9) | Medium            | Bind v6 socket, send/honor `want`, return `Nodes6`; relax `tcp4`-only dial in `requester.go`.                                                                                                                                                                                                                                                                                            |
| G5  | **BEP 42-compliant node ID** (CRC32C-from-IP) + echo `ip` in responses                                      | 42          | Medium            | Derive node ID from observed external IP; learn external IP (e.g. via responses) and set `Msg.IP`.                                                                                                                                                                                                                                                                                       |
| G6  | **Honor incoming `ro` flag** — don't add read-only nodes to routing/discovery                               | 43          | Low               | Check `args.ReadOnly` in `responderNodeDiscovery`.                                                                                                                                                                                                                                                                                                                                       |
| G7  | **Rotate DHT token secret** on the BEP 5 schedule (~5 min / 10 min window)                                  | 5           | Low               | Periodic secret rotation with a short grace window.                                                                                                                                                                                                                                                                                                                                      |
| G8  | **Evaluate PEX (ut_pex) and MSE (BEP 13)** for peer-discovery / fetch-success uplift                        | 11, 13      | Low               | Investigation/spike before committing.                                                                                                                                                                                                                                                                                                                                                   |
| G9  | **Single-file torrents: index the name-derived extension in the Tantivy search doc**                        | 3           | Low               | Synthesize `file_extensions=[name-derived ext]` for `FilesStatusSingle` in Go `BuildDocument` + Rust `transform.rs` (byte-parity); plumb `files_status` into the indexing SQL. NOT `persist.go`/`UpvertedFiles()` (would break `FilesStatusSingle` semantics).                                                                                                                           |
| G10 | **Non-UTF-8 name handling** — transliterate/sanitize instead of dropping the torrent                        | 3           | Low/Med           | Revisit `banning/utf8.go` reject; lossy-decode + keep raw, rather than discard legacy/non-Latin torrents.                                                                                                                                                                                                                                                                                |

These map 1:1 to the follow-up tasks created in the team task list.

---

## 8. Appendix — evidence index (file:line)

- Infohash type / PK: `internal/protocol/id.go:52,60-62,145-146`; `internal/model/torrents.gen.go`
- Node ID generation: `internal/protocol/id.go:42-50`; `internal/protocol/dht/dhtfx/module.go`
- ut_metadata fetch: `internal/protocol/metainfo/metainforequester/requester.go` (handshake 163-212; exHandshake 230-264; pieces 266-349)
- Metadata hash verification: `internal/protocol/metainfo/parse.go:13`
- DHT responder: `internal/protocol/dht/responder/responder.go:54-137`
- DHT response correlation / TID: `internal/protocol/dht/server/server.go:146-179`; `internal/protocol/dht/server/id_issuer.go:18-26`
- DHT scrape (BEP 33): `internal/protocol/dht/scrape.go`; `internal/protocol/dht/client/server_adapter.go:60-81`; `internal/dhtcrawler/scrape.go`; `internal/dhtcrawler/persist.go:287-298`
- sample_infohashes (BEP 51): `internal/dhtcrawler/sample_infohashes.go`; `internal/protocol/dht/client/server_adapter.go:83-119`
- IPv6 scaffolding: `internal/protocol/dht/msg.go` (Want/Nodes6); `internal/protocol/dht/nodeaddr.go:36-42`; `internal/protocol/dht/server/socket_unix.go`
- Files / private / padding: `internal/dhtcrawler/persist.go:161-198`; `internal/protocol/metainfo/banning/utf8.go`
- Trackerless: `internal/protocol/metainfo/read_torrent_file.go:12-17` (parsed-but-unused tracker keys)

---

_Generated by a read-only multi-agent BEP compliance audit. No source files were modified._
