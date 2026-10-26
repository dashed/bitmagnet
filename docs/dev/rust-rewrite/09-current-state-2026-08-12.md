# Rust rewrite — current state, 2026-08-12

Supersedes [`08-current-state-2026-08-09.md`](08-current-state-2026-08-09.md).
That document remains useful history, but its corpus sizes and its claim that a
both-rerun harness does not exist are stale.

## One-line status

The flags-on classifier port and its Go/Rust dependency replay are implemented.
The same-input gate now compares the complete normalized classifier result,
ordered action trace, deterministic terminal outcome, and classification-derived
processor write set byte-for-byte. The machinery is ready; the final 25,000
record production artifact has not yet passed it.

## Evidence already accepted

- The immutable `prod-20260811` tape has 2,000 authoritative records, 715
  observations, embedded classifier-time input, deterministic outcomes, and no
  incomplete records.
- Rust replays all 2,000 subjects and all 715 observations without desync,
  misses, unconsumed observations, errors, or non-authoritative records.
- The live PostgreSQL local-search gate compared all recorded local requests:
  17/17 by-ID responses were identical; 418 searches had 414 identical results
  and four adjudicated membership additions caused by rows written immediately
  after the original recorded lookup. There were no reorderings or failures.
- A compact nine-record fixture covers the three private evidence workflows,
  by-ID resolution, non-empty identifiers, the acquisition-plan digest, and
  deterministic terminal outcomes. Current Go and Rust rerunners produce the
  same canonical report bytes on it.

## Gate added in this state

`bitmagnet classifier tape-parity` is the single operational gate for an
arbitrary qualifying tape. It:

1. loads the same immutable tape into Go and Rust;
2. emits each implementation's canonical report;
3. requires exact report-byte equality; and
4. publishes `receipt.json` last, only on success.

The receipt binds the source commit and tree, effective classifier digest,
optional acquisition-plan digest, record count, all three tape-file hashes,
both executable hashes, and both report hashes. Tape inputs and executables are
hashed before and after the run, so changing evidence cannot receive a passing
receipt.

The report schema is `bitmagnet.classifier-tape-rerun/v2`. Every record carries:

- the full frozen classifier projection, including `baseTitle`, `date`, and
  `languageMulti`, even when those fields do not affect persistence;
- the exact ordered attach-action sequence;
- the deterministic tape outcome; and
- canonical `contents`, `torrentContents`, delete, tag, and failure write sets.

The dedicated replay image contains the current Go executable and Rust tape
rerunner built from one source archive. It requires exact commit/tree build
arguments and exposes them through OCI labels and runtime environment. CI builds
that image and runs the fixture through the real image-backed parity command.

## Evidence in progress, not yet accepted

A bounded T1 production capture is running with an exact 25,000-record cap and
a reviewed 3,000-record acquisition seed. The seed supplies the otherwise-rare
attach-entry and deterministic terminal strata; organic production traffic
supplies the remaining volume. Completion still requires the final quiescent
artifact, all manifest/action/outcome integrity checks, the exact Go/Rust parity
receipt, and a full live local-search replay. Do not call T1 or Phase 3 complete
until those artifacts are landed and green.

## Remaining port work after T1

1. Land the final corpus create-only and rebuild the replay image from the exact
   artifact commit.
2. Run the image-backed Go/Rust parity gate over all qualifying records.
3. Run and adjudicate the full local-search answer gate.
4. Gate queue batch grouping, transaction, retry, and republish semantics; the
   classifier tape is per classification and does not encode queue-job batches.
5. Wire the Rust resolver/processor into a reversible production shadow and
   prove the roadmap's throughput and zero-double-processing criteria before
   any writer cutover.

## Commands

Fixture parity from a provenance-labelled replay image:

```sh
make classifier-tape-parity \
  PARITY_IMAGE=<exact-local-image-ref> \
  TAPE_DIR="$PWD/testdata/parity/classifier-tape-rerun/example/tape" \
  OUTPUT_DIR="$(mktemp -d)/evidence"
```

Focused source gates:

```sh
go test ./internal/classifier ./internal/processor ./internal/app/cmd/classifiercmd
cargo test --manifest-path bitmagnet-rs/Cargo.toml \
  -p bitmagnet-processor --test tape_rerun --test tape_rerun_cross_language
```
