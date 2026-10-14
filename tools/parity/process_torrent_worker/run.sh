#!/usr/bin/env bash
# Go-vs-Rust process_torrent worker differential on disposable PostgreSQL.
#
#   A  = seed, drained by the Go queue server alone (the oracle)
#   B  = seed, drained by the Rust worker, then by Go for whatever Rust
#        abstained from (the canary hand-off)
#   C  = seed, drained by the Rust worker and the Go queue server concurrently
#
# A second round (REPROCESS_ROUND=0 skips it) requeues every surviving torrent
# on the drained copies and repeats A/B/C, covering the existing-content paths.
#
# The script dumps every copy's processor/queue image and diffs B and C
# against A. It never touches anything except databases it creates itself.
#
# Required on PATH: psql, goose, go, cargo. Inputs (env):
#   PG_ADMIN_DSN   libpq DSN of a disposable server (default: local socket)
#   GOOSE_VERSION  schema head to migrate the seed to (default 29 = production)
#   REPLAY_LIMIT   real-name replay inputs to seed (default 20000)
#   OUT            evidence directory (default: mktemp)
#   BITMAGNET_GO   Go binary (default: go build into $OUT)
#   RUST_WORKER    Rust worker binary (default: cargo build --release)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
PG_HOST="${PG_HOST:-/tmp}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
GOOSE_VERSION="${GOOSE_VERSION:-29}"
REPLAY_LIMIT="${REPLAY_LIMIT:-20000}"
OUT="${OUT:-$(mktemp -d)}"
mkdir -p "$OUT"
PREFIX=bm_ptw_parity

dsn() { echo "postgres://${PG_USER}@localhost:${PG_PORT}/$1?host=${PG_HOST}&sslmode=disable"; }
sql() { psql -X -q -v ON_ERROR_STOP=1 -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" "$@"; }
active() { sql -d "$1" -tAc "SELECT count(*) FROM queue_jobs WHERE status IN ('pending','retry')"; }
# Go's backoff is 16s..~1min; collapse it so the retry lifecycle finishes
# quickly. Both copies get the identical treatment, and run_after is excluded
# from the dump because Go's jitter is random.
hasten() { sql -d "$1" -c "UPDATE queue_jobs SET run_after = clock_timestamp() WHERE status = 'retry' AND run_after > clock_timestamp()"; }

if [[ -z "${BITMAGNET_GO:-}" ]]; then
  (cd "$ROOT" && go build -o "$OUT/bitmagnet" .)
  BITMAGNET_GO="$OUT/bitmagnet"
fi
if [[ -z "${RUST_WORKER:-}" ]]; then
  (cd "$ROOT/bitmagnet-rs" && cargo build --release -p bitmagnet-processor --bin bitmagnet-process-torrent-worker)
  RUST_WORKER="$ROOT/bitmagnet-rs/target/release/bitmagnet-process-torrent-worker"
fi

for db in seed a b c; do
  sql -d postgres -c "DROP DATABASE IF EXISTS ${PREFIX}_$db"
done
sql -d postgres -c "CREATE DATABASE ${PREFIX}_seed"
goose -dir "$ROOT/migrations" postgres "$(dsn ${PREFIX}_seed)" up-to "$GOOSE_VERSION" >"$OUT/goose.log" 2>&1
(cd "$ROOT" && go run ./tools/parity/process_torrent_worker seed -dsn "$(dsn ${PREFIX}_seed)" \
  -root "$ROOT" -replay-limit "$REPLAY_LIMIT") 2>&1 | tee "$OUT/seed.log"
for db in a b c; do
  sql -d postgres -c "CREATE DATABASE ${PREFIX}_$db TEMPLATE ${PREFIX}_seed"
done
JOBS="$(active ${PREFIX}_seed)"

# The Go processor logs its effective classifier digest; the Rust worker
# refuses to start unless it matches its own embedded configuration.
GO_DIGEST="$(POSTGRES_DSN="$(dsn ${PREFIX}_seed)" "$BITMAGNET_GO" classifier digest 2>/dev/null | grep -o 'sha256:[0-9a-f]*' | head -1)"
echo "go classifier digest: $GO_DIGEST" | tee "$OUT/digest.txt"

start_go() {
  POSTGRES_DSN="$(dsn "$1")" LOG_LEVEL=info \
    "$BITMAGNET_GO" worker run --keys=queue_server >>"$OUT/go-$1.log" 2>&1 &
  echo $!
}
start_rust() {
  BITMAGNET_POSTGRES_DSN="$(dsn "$1")" RUST_LOG=info \
    "$RUST_WORKER" --expected-classifier-config-digest "$GO_DIGEST" \
    --check-interval-seconds 1 >>"$OUT/rust-$1.log" 2>&1 &
  echo $!
}
stop() { kill -INT "$@" 2>/dev/null || true; wait "$@" 2>/dev/null || true; }

# Wait until the active count is at most $2, has dropped below the seeded job
# count, and has not moved for 5 consecutive 2s polls (retries are collapsed
# every poll).
drain() {
  local db=$1 want=$2 last=-1 same=0 now
  while :; do
    hasten "$db"
    now="$(active "$db")"
    if [[ "$now" == "$last" ]]; then same=$((same + 1)); else same=0; fi
    last=$now
    if [[ "$now" -le "$want" && "$now" -lt "$JOBS" && "$same" -ge 5 ]]; then break; fi
    sleep 2
  done
  echo "$db settled with $now active jobs"
}

# One round: drain A with Go alone, B with Rust then Go, C with both at once,
# then dump and diff. $1 suffixes the evidence files (round 2 = "2").
status=0
run_round() {
  local r=$1 T0 pid rpid gpid db
  T0=$SECONDS
  pid="$(start_go ${PREFIX}_a)"; drain ${PREFIX}_a 0; stop "$pid"
  echo "round ${r:-1} A (Go alone): $((SECONDS - T0))s" | tee -a "$OUT/timing.txt"

  T0=$SECONDS
  pid="$(start_rust ${PREFIX}_b)"; drain ${PREFIX}_b "$JOBS"; stop "$pid"
  ABSTAINED="$(active ${PREFIX}_b)"
  sql -d ${PREFIX}_b -tAc "SELECT id FROM queue_jobs WHERE status IN ('pending','retry') ORDER BY id" >"$OUT/b$r-left-for-go.txt"
  echo "round ${r:-1} B Rust phase: $((SECONDS - T0))s, left for Go: $ABSTAINED" | tee -a "$OUT/timing.txt"
  pid="$(start_go ${PREFIX}_b)"; drain ${PREFIX}_b 0; stop "$pid"

  T0=$SECONDS
  rpid="$(start_rust ${PREFIX}_c)"; gpid="$(start_go ${PREFIX}_c)"
  drain ${PREFIX}_c 0; stop "$rpid" "$gpid"
  echo "round ${r:-1} C (concurrent): $((SECONDS - T0))s" | tee -a "$OUT/timing.txt"

  for db in a b c; do
    (cd "$ROOT" && go run ./tools/parity/process_torrent_worker dump -dsn "$(dsn ${PREFIX}_$db)") \
      >"$OUT/$db$r.dump" 2>"$OUT/$db$r.counts"
  done

  for db in b c; do
    if diff -u "$OUT/a$r.dump" "$OUT/$db$r.dump" >"$OUT/a-vs-$db$r.diff"; then
      echo "round ${r:-1} A vs ${db^^}: IDENTICAL ($(wc -l <"$OUT/a$r.dump") lines)"
    else
      echo "round ${r:-1} A vs ${db^^}: DIFFERENT ($(grep -c '^[-+][^-+]' "$OUT/a-vs-$db$r.diff") changed lines, see $OUT/a-vs-$db$r.diff)"
      status=1
    fi
  done
}

run_round ""

# Which worker settled each job in B and C (from the worker logs).
grep -c '"process_torrent job processed"\|process_torrent job processed' "$OUT/rust-${PREFIX}_b.log" >"$OUT/b-rust-processed.txt" || true
grep -c 'process_torrent job processed' "$OUT/rust-${PREFIX}_c.log" >"$OUT/c-rust-processed.txt" || true

# Round 2 reprocesses the drained, already-classified copies (see `requeue`).
# A, B and C are identical here when round 1 passed, so each starts from the
# same image.
if [[ "${REPROCESS_ROUND:-1}" == 1 ]]; then
  for db in a b c; do
    (cd "$ROOT" && go run ./tools/parity/process_torrent_worker requeue -dsn "$(dsn ${PREFIX}_$db)") \
      2>&1 | tee -a "$OUT/requeue.log"
  done
  JOBS="$(active ${PREFIX}_a)"
  run_round 2
fi
cat "$OUT/a.counts"
echo "evidence: $OUT"
exit $status
