# cb-D1-l3 — Concurrency bench spec (E1 readers, E2 readers + live writer)

**Status:** DESIGN + DRAFTED HARNESS (cargo-green, locally smoke-validated). NOT yet
run on HEL1. Closes the one optional gap in the torrent_files-replacement suite:
every L3 latency number to date (ascii3 p50 24.7 ms, broad-gram production
`TopDocs` p95 ~77–94 ms, realistic multi-word < 50 ms) is **single-client**.
Production = N concurrent typeahead users hitting the per-torrent ngram index
while ONE live writer commits supersessions on the same mmap'd index.

**Subject under test:** the built WithFreqs per-torrent ngram artifact
`idx_pt_ngram_wf` (13.32 GiB, ~17 M path-bag docs, force-merged to 1 segment).
HEL1 has **24 cores**.

**Harness:** the new `loadtest` subcommand of `bench-file-index`
(`/Users/me/aaa/github/bitmagnet/bench-file-index`). Sync, self-contained, **no
DB** — the E2 writer synthesizes realistic path-bag docs. Models:
`pathquery`/`recall` (query construction, tokenizer registration, `load_queries`,
`build_path_query` are reused verbatim).

---

## 0. TL;DR — what runs

```
# (one-time, on FleetView host with the crate; release build)
cargo build --release            # in bench-file-index/

# E1 — read-only, on the REAL artifact (safe; opens no writer):
bench-file-index loadtest --mode e1 \
  --index-path /home/ansible/bench-scratch/idx_pt_ngram_wf \
  --tokenizer ngram --ngram-min 2 --ngram-max 3 \
  --queries-file queries/loadtest_mix.tsv \
  --levels 1,2,4,8,16,24 --duration-secs 75 --warmup-secs 5

# E2a — readers (real read scale) + APPEND writer, on a COPY:
cp -r idx_pt_ngram_wf idx_pt_ngram_wf_e2          # ~13 GB; cheap on 900 GB free
bench-file-index loadtest --mode e2 --write-op append \
  --index-path /home/ansible/bench-scratch/idx_pt_ngram_wf_e2 \
  --tokenizer ngram --queries-file queries/loadtest_mix.tsv \
  --e2-readers 24 --write-rates 5,20,50 --duration-secs 75 --writer-heap-mb 2000

# E2b — full delete_term SUPERSESSION under read load, on a KEYED ngram index:
bench-file-index recall --source torrent-files --granularity per-torrent \
  --tokenizer ngram --no-positions --with-delete-key --skip-truth \
  --limit-docs 17000000 --index-path .../idx_pt_ngram_wf_keyed --queries-file queries/ps_prefix_sweep.tsv
bench-file-index loadtest --mode both --write-op supersede \
  --index-path .../idx_pt_ngram_wf_keyed --tokenizer ngram \
  --queries-file queries/loadtest_mix.tsv \
  --levels 1,2,4,8,16,24 --e2-readers 24 --write-rates 5,20,50 --duration-secs 75
```

The full HEL1 orchestration (single ssh, flock+setsid+.exit, teardown) is §6.

---

## 1. Why this is correct — tantivy 0.26.1 concurrency semantics (VERIFIED)

All citations are from the vendored crate
`~/.cargo/registry/src/index.crates.io-*/tantivy-0.26.1/src/`.

| Claim | Evidence |
|---|---|
| `IndexReader` is cheap to clone & share across threads | `#[derive(Clone)] pub struct IndexReader { inner: Arc<InnerIndexReader>, … }` — `reader/mod.rs:266-269` |
| The live searcher lives behind a lock-free swap cell | `searcher: arc_swap::ArcSwap<SearcherInner>` — `reader/mod.rs:156` |
| `searcher()` is an atomic load+clone; **call it per query** | `fn searcher(&self){ self.searcher.load().clone().into() }` — `reader/mod.rs:255-257,298`; doc: *"This method should be called every single time a search query is performed. The same searcher must be used for a given query…"* — `reader/mod.rs:290-296` |
| `reload()` = atomic store; in-flight queries keep their generation (MVCC) | `fn reload(){ … self.searcher.store(searcher); }` — `reader/mod.rs:243-253`. Old `Arc<SearcherInner>` stays alive for threads that already `load()`ed it. |
| GC won't delete segment files a live searcher still references | `SearcherGeneration` tracked in an `Inventory<…>` keyed by generation id; `open_segment_readers` takes `META_LOCK` while opening — `reader/mod.rs:189-213,228`. We never call `garbage_collect_files` during E2. |
| `Searcher` is Clone + Send+Sync (Arc wrapper) | `#[derive(Clone)] pub struct Searcher { inner: Arc<SearcherInner> }` — `core/searcher.rs:68-70` |
| N reader threads = N **independent single-threaded** searches | `search()` → `self.inner.index.search_executor()` — `core/searcher.rs:205`; default executor is `Executor::single_thread()` — `index/index.rs:392`; `SingleThread.map` runs the closure **inline on the calling thread** — `core/executor.rs:51-58`. So one query never fans out across threads; concurrency comes purely from running many queries at once. |
| Exactly one writer (directory lock) | `IndexWriter { _directory_lock: Option<DirectoryLock>, … }` — `indexer/index_writer.rs:74`; `commit(&mut self)` — `index_writer.rs:664`. The single E2 writer holds the lock; readers are independent. |
| Index is Clone+Send+Sync | `#[derive(Clone)] pub struct Index { directory: ManagedDirectory, …, executor: Executor, … }` — `index/index.rs:266-274` |

**Conclusion.** The faithful production model — and what `loadtest` implements — is:
**one `Index`, one `IndexReader`** (Manual reload so E2 controls visibility),
**N reader threads** each calling `reader.searcher()` *per query*, optional **one
writer** that `commit()`s then `reader.reload()`s. This is lock-free on the read
path, MVCC-safe under concurrent writes, and uses true OS parallelism up to the
core count. `std::thread::scope` lets reader threads borrow `&Index`/`&[QuerySpec]`
and build their own `Box<dyn Query>` (so `Query` need never be `Send`).

---

## 2. E1 — reader-concurrency sweep (read-only)

**Goal:** does the index degrade gracefully as concurrent typeahead users scale to
the core count, and do the per-client numbers hold?

* `N ∈ {1, 2, 4, 8, 16, 24}` reader threads sharing one `Index`+`IndexReader`.
* Each thread loops the query mix for a fixed wall-clock (`--duration-secs`,
  default 75 s after a `--warmup-secs` page-cache warm-up), doing **both**
  collectors per query:
  * `Count` (cheap match count), and
  * the production page collector
    `TopDocs::with_limit(30).order_by_fast_field::<u64>("ident", Desc)` — a full
    match-set scan + top-K heap with **no early-term** (tantivy 0.26.1), so
    ordering by `ident` is a value-independent proxy for ordering by `seeders`
    (same justification `pathquery --topdocs` uses).
* **Query mix** = `queries_realistic.tsv` (a1_broad/a2_2word/a3_dotted/a4_long/
  cjk2word) **+** the `ascii3`/`cjk3` GATE rows of `ps_prefix_sweep.tsv` (the
  broadest per-keystroke firing queries). Build the merged file once:

  ```bash
  cd bench-file-index/queries
  { cat queries_realistic.tsv; grep -E '^(ascii3|cjk3)\b' ps_prefix_sweep.tsv; } > loadtest_mix.tsv
  ```
  (or pass both files with `--groups ascii3,cjk3,a1_broad,a2_2word,a3_dotted,a4_long,cjk2word`.)

**Output** (per N): aggregate QPS (iters/s and searches/s) + per-group
`iters / avgHits / C-p50/p95/p99 / TD-p50/p95/p99`.

**Read-only & safe on the real artifact:** E1 opens NO `IndexWriter`, never
mutates segments, and never calls GC — so it may run directly against
`idx_pt_ngram_wf`. (Opening a reader transiently takes `META_LOCK` only to open
segment readers; it writes nothing.)

---

## 3. E2 — readers + ONE live writer

**Goal:** reader latency while a writer commits on the same mmap; commit→searchable
fresh-lag *under read load*; segment growth under default `LogMergePolicy`.

Fixed `--e2-readers` (default **24** = worst contention, compared to the E1 N=24
row) while sweeping writer rate `--write-rates 5,20,50` torrents/s. The full
N×rate matrix is possible but pinning N=24 keeps runtime bounded; we note that.

Per write-rate, in ONE `std::thread::scope`: spawn `e2_readers` reader threads
(same hot loop as E1, no warm-up — the writer must see live readers from t0) **+ 1
writer thread**. The writer, paced to the target rate:

1. (supersede only) `delete_term(info_hash)` for the rotating key,
2. `add_document` a synthesized realistic per-torrent path bag
   (`--paths-per-doc`, ~12% CJK so ngram tokenization cost mirrors production),
3. `commit()`, then `reader.reload()`.

**Two write modes** (auto-selected by whether the index has an indexed `info_hash`
key — see §4):

* **E2a `append`** — runs on a `cp -r` COPY of the real `idx_pt_ngram_wf`
  (keyless). Readers hit the **real 13 GiB read scale**; the writer appends. This
  is the load-relevant reader-under-write + add-fresh-lag + seg-growth result.
  (Appends are bounded: 50/s × 75 s = 3,750 docs vs 17 M.)
* **E2b `supersede`** — runs on a **keyed** ngram index (built with
  `recall --with-delete-key`, §4). True per-torrent `delete_term`+re-add under
  read load. EXP-E already pinned single-writer supersession at ~11 ms; E2b shows
  it holds under concurrent reads. Reader latencies here are on a smaller index,
  so trust E2a for read-scale numbers and E2b for the delete/merge mechanics.

**Measured per rate:**
* reader `Count`/`TopDocs` p50/p95/p99 (same table as E1) + the **ratio vs the E1
  N=`e2_readers` baseline** (printed automatically when `--mode both`);
* writer **commit p50/p95**, **achieved vs target rate** (if commit cost exceeds
  the inter-arrival interval the writer can't keep up — a real finding, surfaced
  not masked), **fresh-lag p50/p95/p99** under read load, **segment count
  min/max/first/last** (~1 Hz samples — watch for LogMergePolicy fan-out);
* (supersede) a one-off **supersession-correctness verify**: a key written with 5
  files → `delete_term`+re-add 3 → reload → resolves to exactly 3 (old gen gone),
  `OK`/`MISMATCH`. This **retroactively covers the skipped per-torrent freshness
  sanity** from the EXP-D2 build.

**Fresh-lag probe** (works in both modes): keyed → a per-tick **unique** sentinel
`info_hash` (0xBB marker + tick) committed in the same batch, searched by exact
term = visibility of *this* commit; keyless append → `searcher.num_docs() ≥`
running committed target (append never deletes ⇒ num_docs is monotonic).

**🚨 Artifact protection:** E2 mutates (commits append segments + merges). ALWAYS
`cp -r idx_pt_ngram_wf idx_pt_ngram_wf_e2` first and point E2a at the COPY; E2b
uses its own keyed index. NEVER run E2 against the canonical `idx_pt_ngram_wf`.
Do not call `garbage_collect_files` during E2 (the harness doesn't).

---

## 4. Why E2b needs `--with-delete-key` (the one schema change)

`idx_pt_ngram_wf` was built by `recall` → `build_recall_schema` = `{path TEXT,
ident u64 FAST}`. There is **no indexed term usable as a delete key**: `ident` is
FAST-only (not indexed → no `delete_term`), and an ngram `path` gram is never
unique. So `delete_term`-based supersession **cannot** run on the recall artifact.

Minimal, contained fix (committed in this change, default OFF → zero behavior
change to `recall`/`pathquery`/`freshness`, size numbers stay apples-to-apples):

* `schema.rs::build_recall_schema(tok, with_positions, **with_delete_key**)` adds an
  indexed `info_hash` bytes field (same flags as `build_file_schema:182`);
  `RecallFields` gains `info_hash: Option<Field>`.
* `main.rs::recall` gains `--with-delete-key`; `index_unit` adds the key when present.

Build a keyed ngram per-torrent index exactly like the production shape:

```bash
bench-file-index recall --source torrent-files --granularity per-torrent \
  --tokenizer ngram --no-positions --with-delete-key --skip-truth \
  --limit-docs 17000000 --writer-threads 1 --writer-heap-mb 2000 \
  --index-path .../idx_pt_ngram_wf_keyed --queries-file queries/ps_prefix_sweep.tsv
```

`loadtest --write-op auto` picks supersede iff the opened index has the key, else
append; explicit `--write-op supersede` on a keyless index errors cleanly.

---

## 5. Success criteria & gate flags

**E1 — graceful degradation (no collapse):**
* per-group p95 grows **≲ linearly** to the core count; aggregate QPS rises then
  plateaus near 24 cores. **FAIL** if any group's p95 blows up super-linearly
  before N=24 (lock/allocator contention).
* **Cross-check** the N=1 row reproduces the known single-client figures (ascii3
  `Count` p50 ~24.7 ms WithFreqs; broad-gram `TopDocs` p95 ~77–94 ms). A large
  drift means the artifact/tokenizer/ngram width doesn't match the build.
* The GATE groups (`ascii3`, `cjk3`) p95 at N=24 should stay within ~2–3× the N=1
  p95 — i.e. the index still clears interactive latency under full concurrency.

**E2 — reader-under-write & freshness:**
* reader `Count` p95 at the swept rates ≤ **~2×** the E1 N=`e2_readers` baseline
  (printed as the `→ X.XX× (gate ≲2×)` line);
* **fresh-lag** stays **ms-class** (single/low-double-digit ms) under read load
  (matches EXP-E's ~2 ms FLAT and ~11 ms supersession);
* **segment count BOUNDED** under default `LogMergePolicy` (no monotone fan-out);
* writer **achieved ≈ target** rate, or the achieved-vs-target line documents the
  per-commit cost ceiling (a real result, e.g. local smoke hit ~12/s against a
  20/s target because each tiny-index commit fsync’d in ~80 ms);
* **supersession verify = OK** (superseded key resolves to exactly the new fileset).

**Gate flags / knobs:** `--mode {e1,e2,both}`, `--levels`, `--e2-readers`,
`--write-rates`, `--write-op {auto,append,supersede}`, `--duration-secs`,
`--warmup-secs`, `--writer-heap-mb` (warns < 2000 — EXP-D's ngram single-writer
arena rule), `--writer-threads` (default 1), `--supersede-window`, `--paths-per-doc`,
`--groups` (group allowlist), `--ngram-min/--ngram-max` (must match the build).
Verdicts are printed (advisory); thresholds above are the human gate.

---

## 6. HEL1 orchestration protocol

Per MEMORY ops notes: ONE ssh connection; the parallelism is INSIDE the box
(that's the whole point and is allowed); gentle pollers; `flock` + `setsid` +
`.exit` sentinel so a client-side ssh timeout can't spawn a duplicate run; use the
**tailscale IP** `ansible@<HEL1_TAILSCALE_IP>` (public `<HEL1_PUBLIC_IP>` SSH is flaky;
maple-bastion ProxyJump FAILS).

```bash
ssh -o IdentityAgent=none -i ~/.ssh/id_ed25519 ansible@<HEL1_TAILSCALE_IP> 'bash -s' <<'REMOTE'
set -euo pipefail
cd /home/ansible/bench-scratch
LOCK=/home/ansible/bench-scratch/.loadtest.lock
exec 9>"$LOCK"; flock -n 9 || { echo "another loadtest holds the lock"; exit 1; }
RUN=/home/ansible/bench-scratch/loadtest.$(date +%s)        # date stamp from the shell, not the bench
mkdir -p "$RUN"; : > "$RUN/.exit"

# crate already synced to ~/bench-file-index; build once
cd ~/bench-file-index && cargo build --release 2>&1 | tail -2

# merged query mix
{ cat queries/queries_realistic.tsv; grep -E '^(ascii3|cjk3)\b' queries/ps_prefix_sweep.tsv; } > queries/loadtest_mix.tsv

BIN=./target/release/bench-file-index
IDX=/home/ansible/bench-scratch/idx_pt_ngram_wf

run() {  # detached, survives ssh drop; .exit flips on completion
  setsid bash -c "$1; echo \$? > '$RUN/.exit'" >"$2" 2>&1 &
}

# E1 on the REAL artifact (read-only)
$BIN loadtest --mode e1 --index-path "$IDX" --tokenizer ngram \
  --queries-file queries/loadtest_mix.tsv \
  --levels 1,2,4,8,16,24 --duration-secs 75 --warmup-secs 5 2>&1 | tee "$RUN/e1.log"

# E2a APPEND on a COPY
cp -r "$IDX" "$IDX"_e2
$BIN loadtest --mode e2 --write-op append --index-path "$IDX"_e2 --tokenizer ngram \
  --queries-file queries/loadtest_mix.tsv \
  --e2-readers 24 --write-rates 5,20,50 --duration-secs 75 --writer-heap-mb 2000 2>&1 | tee "$RUN/e2a.log"
rm -rf "$IDX"_e2

# E2b SUPERSEDE on a keyed index (build if absent)
KEYED=/home/ansible/bench-scratch/idx_pt_ngram_wf_keyed
[ -d "$KEYED" ] || $BIN recall --source torrent-files --granularity per-torrent \
  --tokenizer ngram --no-positions --with-delete-key --skip-truth \
  --limit-docs 17000000 --writer-threads 1 --writer-heap-mb 2000 \
  --index-path "$KEYED" --queries-file queries/ps_prefix_sweep.tsv 2>&1 | tee "$RUN/keyed-build.log"
$BIN loadtest --mode both --write-op supersede --index-path "$KEYED" --tokenizer ngram \
  --queries-file queries/loadtest_mix.tsv \
  --levels 1,2,4,8,16,24 --e2-readers 24 --write-rates 5,20,50 --duration-secs 75 2>&1 | tee "$RUN/e2b.log"

echo "DONE → $RUN"
REMOTE
```

Poll gently (one connection, infrequent): `ssh … "cat <RUN>/.exit 2>/dev/null"` —
empty = running, `0` = clean. **Teardown:** `rm -rf "$IDX"_e2` (done above) and,
when finished with E2b, `rm -rf "$KEYED"`; both live under the RUN-6 bench-scratch
that the suite teardown removes.

---

## 7. Harness reference (`loadtest` subcommand)

`bench-file-index/src/main.rs`: `run_loadtest` + `reader_worker` (the shared E1/E2
hot loop) + `writer_worker` + `LevelAgg`/`ReaderResult`/`WriterResult` +
`synth_pathbag`/`rot_ih`/`probe_ih`/`resolve_write_op`. `src/schema.rs`:
`build_recall_schema(…, with_delete_key)` + `RecallFields.info_hash`. cargo check
& clippy green (the harness adds no warnings); locally smoke-validated on a
20k-doc synthetic keyed index (E1 QPS scaled 34k→68k→126k iters/s with flat p95;
E2 reader-p95 1.07–1.14× the baseline; fresh-lag 0.2–0.6 ms; segments 2→8 bounded;
supersession verify OK; achieved-rate correctly capped below target by commit
cost). All real numbers come from the HEL1 run above.
