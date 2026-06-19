# PSX‑D1 — End‑to‑end blob → Parquet on REAL blobs (the L2 measurement gap)

**Team:** `bitmagnet-bench` · **Task:** psx‑d1‑gap (#59) · **Status:** DESIGN‑ONLY spec (nothing executed)
**Source of truth:** `/Users/me/aaa/github/bitmagnet` · **Date:** 2026‑06‑09

---

## 0. The gap, stated precisely

Every prior L2 / DuckDB / file‑index number was sourced from **`torrent_files`**, never from the
production blob. Reason (documented in `bench/export_parquet_pg.py:5‑15`): the HEL1 bench restore is the
**pre‑backfill** `pg_dump` (`bitmagnet-pg-20260605-235749.dump`, 2026‑06‑05), taken _before_ the
2026‑06‑06 blob backfill — so `torrents.files_data` and `torrent_file_summary` are **EMPTY**, while
`torrent_files` (~879.5M rows) is fully present.

That proxy is _content‑valid_ for query latency, because `torrent_files.extension` is the stored
generated column `substring(lower(path) from '[^/.]\.([a-z0-9]+)$')` — path‑derived, i.e. **G1‑correct**
and byte‑identical to what decoding a blob then path‑deriving would produce (`blob_export` ignores the
blob's `e` field and re‑derives from the path, `bench/blob_export/src/main.rs:169‑173`).

**What was therefore never measured end‑to‑end:**

1. The real **decode→ext→Parquet** pipeline running over _actual blob bytes_ (`run_from_db` →
   `stream_torrents_with_files` → `TorrentWithBlob::files()` → `zstd → msgpack` → Parquet), at the full
   **16.97M‑torrent / 856.79M‑file** scale.
2. The **0.6–0.94 µs/file** decode figure — grounded only on a smoke sample, never on the full corpus
   through the production read path.
3. **Format fidelity & parity** of a blob‑sourced Parquet vs the torrent_files‑sourced Parquet that all
   conclusions rest on.

This spec closes that gap **safely** (no prod blob reads, ever).

---

## 1. RESOLVED data source (the gating question)

| Option                                                                                              | Verdict                                                                              |
| --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| Read 16.97M blobs from **live prod FSN1 PG**                                                        | ❌ **FORBIDDEN** — heavy load + server‑safety. Never.                                |
| Bench restore's `files_data`                                                                        | ❌ empty (pre‑backfill dump).                                                        |
| **Re‑create `files_data` ON THE BENCH** from `torrent_files` using the **exact production encoder** | ✅ **DEFAULT — chosen.**                                                             |
| A **post‑backfill** dump with populated blobs, if one exists                                        | ✅ **Preferred IF it exists** — zero‑encode, true prod bytes. Probe first (Stage 0). |

### 1.1 Why bench re‑encode is faithful (verified in code)

Production blob format (`internal/blobmigration/serializer.go` + `bitmagnet-rs/.../blob.rs:1‑18`):

```
files_data = zstd_L3( msgpack_named_array[ {"i":uint,"p":str,"e":str,"s":uint}, … ] )
```

- `vmihailenco/msgpack/v5` encodes the Go `compactFile` struct as a **map keyed `i`/`p`/`e`/`s`**
  (serializer.go:21‑45). The Rust `BlobFile` mirrors this with `#[serde(rename)]` + `to_vec_named`.
- zstd at klauspost `SpeedDefault` ≈ level 3.

Round‑trip is **already proven**, so the bench‑encoded blob _is_ the production wire format:

- Go ⇄ Go round‑trip: `internal/blobmigration/serializer_test.go` (incl. CJK/cyrillic paths, 1500‑file
  lists, empty, single).
- Rust ⇄ Rust + zstd magic: `blob.rs` unit tests.
- **Cross‑language, byte‑for‑byte** (`bitmagnet-rs/crates/bitmagnet-model/tests/blob_fixture.rs`):
  Rust deserializes **real Go‑produced `.blob` fixtures**, AND Rust's **inner msgpack is byte‑identical**
  to Go's. (Only the outer zstd frame differs between libzstd and klauspost — _mutually decodable_, so
  immaterial to any decoder.)

⟹ A blob written on the bench by `blobmigration.SerializeFiles` is indistinguishable from a prod blob
for every downstream consumer (decode yields identical `{i,p,e,s}`).

### 1.2 Encoder choice

- **Primary = the Go encoder `blobmigration.SerializeFiles`** — it is _literally the production code_
  that wrote the prod blobs (live importer `internal/dhtcrawler/persist.go:226` and backfill
  `internal/blobmigration/queue/handler.go:167`), and running it lets us **time the importer's encode
  path** as the task requires.
- **Fallback = Rust `serialize_files`** if the Go toolchain is unavailable on HEL1. Inner msgpack is
  byte‑identical (proven); only the outer zstd frame differs, which no decoder cares about. Flag the
  caveat if used.

### 1.3 Known semantic nuance (flag, not a blocker)

Two prod write paths differ on a file cap:

- **Backfill** (`handler.go` `processBatch`) serializes **ALL** `torrent_files` rows for a hash — **no cap**.
  This wrote the bulk of prod blobs.
- **Live importer** (`persist.go:206`) caps at `saveFilesThreshold` (=100) → `files_status = over_threshold`.

`torrent_files` itself was already cap‑bounded at crawl time, so re‑encoding **all** `torrent_files` rows
(mirroring the backfill) gives a blob whose decoded file set === `torrent_files` for that hash → exact
parity in Stage 3. The live @100 cap is a separate, pre‑existing concern (see EXP‑A FIND‑1) and is **not
material to this gap's measurement**. Do NOT re‑apply the cap during bench encode.

---

## 2. Pipeline & exact commands

All work on the **idle HEL1 throwaway restore** (ns `bitmagnet-bench`, bench‑pg NodePort). Zero prod impact.

### Stage 0 — probe for a post‑backfill dump (read‑only; preferred fast path)

```bash
# One short, read-only ls. If a dump dated >= 2026-06-06 exists, it has populated
# blobs -> use it directly (restore to a 2nd throwaway DB) and SKIP Stage 1.
ssh -o IdentityAgent=none -i ~/.ssh/id_ed25519 ansible@<HEL1_TAILSCALE_IP> \
  'ls -la /var/lib/bitmagnet-backups/ 2>/dev/null; \
   sudo k3s kubectl -n bitmagnet get pvc 2>/dev/null | grep -i backup'
```

- **GATE G0** — report findings to lead. If a post‑backfill dump exists → restore it, jump to Stage 2,
  and Stage 1 becomes a _cross‑check only_ (still worth timing the encoder). The only dump known today is
  the **pre‑backfill** one (empty blobs), so the DEFAULT remains Stage‑1 re‑encode.
- **Never** read blobs from prod to fill this gap.

### Stage 1 — encode `torrent_files` → `files_data` on the bench (+ time the importer encode path)

Throwaway Go tool `bench/blob_encode/main.go` (NEW; path‑deps the fork like `blob_export` does).
Logic = a standalone, bench‑PG copy of `handler.processBatch` (no queue, no consistency sampling):

```
keyset over DISTINCT info_hash (ORDER BY info_hash):
  for each hash:
    files := SELECT * FROM torrent_files WHERE info_hash=$1 ORDER BY "index"
    blob  := blobmigration.SerializeFiles(files)        # TIME THIS (encode-only ns/file accumulator)
    UPDATE torrents SET files_data=$blob WHERE info_hash=$1
  batch the UPDATEs (e.g. 5–10k/tx) for write throughput
```

Run (after lead GO; single connection; under the run guard in §4):

```bash
# build on HEL1 (Go) or scp a prebuilt static binary
go build -o /home/ansible/bench-scratch/blob_encode ./bench/blob_encode

# SMOKE first (gate G3): 100k torrents, report encode µs/file + write t/s
DSN='postgresql://postgres:<BENCH_PW>@127.0.0.1:30654/bitmagnet'
/home/ansible/bench-scratch/blob_encode --dsn "$DSN" --limit 100000 --batch 5000

# FULL run after smoke passes:
/home/ansible/bench-scratch/blob_encode --dsn "$DSN" --batch 5000
```

- Emits: `torrents encoded, files encoded, encode µs/file (pure SerializeFiles), wall t/s, files_data bytes written`.
- The **encode µs/file** is the importer encode‑path number the task asks for; compare to the live
  `persist.go` hot path (~1–1.5 ms/torrent @ ≤100 files).
- **Rust fallback**: `bench/blob_encode_rs` calling `serialize_files` if no Go on HEL1 (§1.2 caveat).

### Stage 2 — REAL blob → Parquet (the actual gap measurement)

Use the **unmodified** `bench/blob_export` against the now‑populated bench `files_data`:

```bash
source ~/.cargo/env
cd /home/ansible/bench-scratch/blob_export    # or wherever the crate is synced
DSN='postgresql://postgres:<BENCH_PW>@127.0.0.1:30654/bitmagnet'

# full (with path) + slim (no path) — these run `run_from_db` -> stream_torrents_with_files
#   -> TorrentWithBlob::files() (zstd->msgpack decode) -> path-derive ext -> Parquet.
cargo run --release -- --dsn "$DSN" --out /home/ansible/bench-scratch/files_full_blob.parquet
cargo run --release -- --dsn "$DSN" --out /home/ansible/bench-scratch/files_slim_blob.parquet --slim
```

- The `DONE:` line already prints **torrents, file‑rows, blob errors, wall s, torrents/s, M files/s** —
  the end‑to‑end throughput at full scale.
- **`blob errors` MUST be 0** — a non‑zero count means a bench‑encoded blob failed to decode (encoder/format bug).

### Stage 3 — PARITY: Parquet‑from‑blobs == Parquet‑from‑torrent_files

The torrent_files‑sourced Parquet is produced by the existing `bench/export_parquet_pg.py` (already the
RUN‑2 artifact, or regenerate). Compare with DuckDB (`uv run`, throwaway script `bench/d1_parity.py`):

```sql
-- counts
SELECT (SELECT count(*) FROM read_parquet('files_slim_blob.parquet')) AS blob_rows,
       (SELECT count(*) FROM read_parquet('files_slim.parquet'))      AS tf_rows;

-- content hash: ordered, full tuple. Must be identical.
SELECT md5(string_agg(info_hash||'|'||file_index||'|'||coalesce(extension,'∅')||'|'||size, '\n'
            ORDER BY info_hash, file_index)) FROM read_parquet('files_slim_blob.parquet');
-- vs the same over files_slim.parquet

-- if hashes differ, localize with an ANTI JOIN on (info_hash,file_index):
SELECT * FROM read_parquet('files_slim_blob.parquet') b
ANTI JOIN read_parquet('files_slim.parquet') t USING (info_hash,file_index,extension,size)
LIMIT 50;

-- full file: also diff `path` (FTS column).
```

- Expected: **exact** equality (counts + content hash), since both path‑derive `extension` and the bench
  encode is uncapped. Any diff must be explainable solely by a documented over‑threshold cap row‑set; flag
  anything else as an encoder/decoder bug.

### Stage 4 — throughput verdict at full scale

- Report Stage‑2 `M files/s` and derive **ns/file end‑to‑end**; confirm/refute **0.6–0.94 µs/file**.
- Decompose: also run the pure‑decode `--from-hex` smoke path on a sampled PSV
  (`info_hash|count|hex` from the bench, e.g. 1M torrents) to separate **decode‑only** cost from
  **PG‑read + Parquet‑write** overhead. This isolates whether 0.6–0.94 µs/file was decode‑only or end‑to‑end.

---

## 3. Success criteria

1. **Decode integrity:** Stage 2 reports **0 blob errors** across all ~16.97M torrents.
2. **Parity:** `blob_rows == tf_rows` (≈856.79M) AND content‑hash identical for slim _and_ full
   (incl. `path`). Any delta fully attributable to documented cap semantics.
3. **Throughput (full scale):** end‑to‑end µs/file reported; **PASS** if it lands at/near 0.6–0.94 µs/file
   (decode‑only) — otherwise record the real number and flag the discrepancy.
4. **Format fidelity:** bench‑encoded blob is a valid zstd frame (magic `28 b5 2f fd`) and decodes to the
   exact `{i,p,e,s}` set of its `torrent_files` rows (implied by #1 + #2).
5. **Encode path timed:** importer encode µs/file (pure `SerializeFiles`) reported, contextualized vs the
   live `persist.go` ~1–1.5 ms/torrent hot path.

---

## 4. Safety protocol (HEL1, server‑safety)

- **ONE ssh connection at a time.** No `ControlMaster`, no tight pollers (trips HEL1 sshd).
- **Connection string:** `ssh -o IdentityAgent=none -i ~/.ssh/id_ed25519 ansible@<HEL1_TAILSCALE_IP>`
  (tailscale/WireGuard — public IP `<HEL1_PUBLIC_IP>` is flaky; maple‑bastion ProxyJump FAILS).
- **Long runs:** `flock` a lockfile + `setsid` detached + write a `.exit` sentinel on completion; poll the
  sentinel **gently** (≥30–60s apart). `setsid` jobs **survive client‑side ssh timeouts** → an rc=124 can
  still have _launched_ → **guard every launcher with `flock` + `pgrep`** to prevent duplicate concurrent
  writers (this previously caused colliding writers).
- `source ~/.cargo/env` for Rust; `uv` is userspace‑installed for the DuckDB/python steps.
- Bench DSN: `postgresql://postgres:<BENCH_PW>@127.0.0.1:30654/bitmagnet` (NodePort, read paths
  `READ_ONLY` where possible; Stage 1 is the only writer and writes ONLY the throwaway bench DB).
- **Prod is never touched.** No FSN1 connection, no prod blob reads.

---

## 5. Disk / time budget

**Disk (on top of the existing 353GB restore):**
| Artifact | Est. |
|---|---|
| `files_data` written into bench `torrents` | ~16 GB |
| `files_full_blob.parquet` (with path) | ~11.7 GB |
| `files_slim_blob.parquet` | ~3.9 GB |
| (torrent_files‑sourced Parquet for parity, if not kept) | ~15.6 GB |
| **Headroom needed** | **~50 GB free** — **GATE G2: `df -h` before starting.** |

**Time (rough, single‑threaded sinks):**
| Stage | Est. |
|---|---|
| 0 probe | seconds |
| 1 encode + write 16.97M `files_data` | ~20–60 min (PG write‑bound; measure on smoke) |
| 2 blob→Parquet full + slim | ~10–20 min each (decode ~8.5–13.4 min for 856.79M @0.6–0.94µs + PG read + write) |
| 3 parity DuckDB | ~3–5 min |
| 4 decode‑only sample | ~2–5 min |
| **Total** | **~1–2 h** |

---

## 6. Gates to flag to the lead

- **G0** — post‑backfill dump? (Stage 0 ls). If yes → preferred zero‑encode source.
- **G1** — Go toolchain available on HEL1? If not → Rust‑encode fallback (zstd‑frame caveat, §1.2).
- **G2** — ≥~50 GB free disk (`df -h`) before Stage 1.
- **G3** — encode smoke (`--limit 100000`) throughput acceptable before the full encode commit.
- **G4** — lead GO before _each_ HEL1 connection; bench env (`bitmagnet-bench` ns + bench‑pg + scratch)
  is **still up pending RUN‑6 teardown** — coordinate so this runs before teardown.

**Not a gate but note:** this is bench‑only and does **not** touch the production `torrent_files` DROP
sequencing constraint (DROP stays deferred until every replacement layer is proven in prod).

---

## 7. New throwaway artifacts (uncommitted, bench‑only)

- `bench/blob_encode/main.go` (or `bench/blob_encode_rs/`) — torrent_files → files_data encoder + encode timer.
- `bench/d1_parity.py` — DuckDB parity check (counts + content hash + anti‑join localizer).
- Reuses **unmodified**: `bench/blob_export` (Stage 2), `bench/export_parquet_pg.py` (parity baseline).
