#!/usr/bin/env bash
# Flags-on Go-vs-Rust process_torrent differential on disposable PostgreSQL,
# with a stub TMDB (see flagson*.go and tmdb_stub.go). `MODE=flags-on run.sh`
# runs this script; run.sh's own flags-off A/B/C mode is unchanged.
#
# Every corpus is seeded once into ${PREFIX}_<corpus>_seed and cloned per run.
# Go drains its queue one job at a time (the process_torrent handler's
# concurrency is 1) in run_after order, and so does the Rust worker, so a
# Go-alone and a Rust-alone run see the same content at every job even though
# jobs read what earlier jobs wrote.
#
#   main   G-1: Go alone, twice (run 1 also records Go's classifier tape for
#          the coverage report); extended dumps and stub request multisets
#          must be identical.
#          G2 (with RUST_WORKER): Rust alone vs Go run 1; identical dumps and
#          request multisets, no job left behind, no abstention logged.
#   hint   Explicit content-ID hints (by-ID actions, /find), in a database of
#          their own, so they cannot change the main corpus's results. G-1 as
#          for main. With RUST_WORKER (G2's hint slice): Rust then Go; Rust
#          must abstain from every hint job before classification, with the
#          attach-hint reason, zero stub requests and zero writes, and the
#          result (dump and request multiset) must equal Go alone.
#   fault  TMDB failures Go handles per torrent, and transport faults its
#          client's retry recovers from, by class (flagson_faults.go), in
#          fault jobs of pseudo-titled torrents no other job can see.
#          G-1: Go alone three times, then flagson-faultcheck: every fault job
#          ends as its class says (the per-class floors). G2-fault (with
#          RUST_WORKER): Rust alone vs Go run 1, identical dumps and request
#          multisets, no job left behind, no abstention logged.
#   abstain  Failures Rust leaves to Go: a 401, timeouts, resets, a failed
#          key validation. G-1 and flagson-faultcheck as for fault. G3 (with
#          RUST_WORKER): Rust then Go vs Go alone. Rust abstains from exactly
#          the fault jobs and writes nothing for them; the dumps must be
#          identical; and flagson-attribution checks that the extra requests
#          are Rust's, on the abstained jobs' keys, and that by the worker's
#          guard events no call started after its job's failure was
#          recorded. Held jobs run after the others, in fresh processes: the
#          401 jobs one at a time, since a 401 latches both clients for the
#          life of the process; the key-validation jobs together, and in
#          Rust's process the stub fails /authentication.
#   The fault and abstain stubs run without keep-alive (tmdb-stub -keepalive).
#   rematch  A reprocess round over Go's drained main round 1 (main go1,
#          from MAIN_RUN): flagson-requeue (flagson_rematch.go) queues its
#          attached, unmatched and rule-derived torrents again, half in
#          ClassifyMode rematch and half in the default mode (content reuse),
#          drops some file lists and inserts better candidates. G-1 as for
#          main, then flagson-rematch-coverage counts the phase's floors. G2
#          (with RUST_WORKER): Rust alone vs Go run 1, as for main.
#   organic  process_torrent jobs as Go's own producers write them
#          (flagson_organic.go), in slices of one producer shape each: the
#          GraphQL and CLI slices' jobs come from Go's own batch handler, in a
#          copy of the seed per slice (flagson-organic-produce), and every job
#          is queued verbatim (flagson-organic-enqueue). G-1 as for main, then
#          flagson-organic-coverage (per-slice floors from Go's tape) and
#          flagson-organic-check (G7: the fault slices' republish rows). With
#          RUST_WORKER (an M4b worker, organic admission on): Rust then Go at
#          --organic-max-priority 20 (G2, G3, G5) and at 0 (G4), each equal to
#          Go alone; flagson-abstained -organic-bound checks that Rust left
#          exactly the negative slices' jobs, the hinted slice's (one
#          abstention each) and those above the bound, untouched.
#   organic2  Round 2 over Go's drained organic round 1 (organic go1, from
#          ORGANIC_RUN): flagson-requeue -organic queues the round-1 replay
#          slices' torrents again in organic round-2 slices, half in
#          ClassifyMode rematch, half in the default mode, made and queued as
#          for organic. G-1, flagson-rematch-coverage (floors scaled to the
#          smaller corpus) and flagson-organic-coverage; with RUST_WORKER,
#          Rust then Go at bound 20 vs Go alone (G6).
#
# Dumps are canonical (flagson_dump.go); dump.raw beside each is not, and its
# comparison is reported for information. Then flagson-coverage counts every
# floor over main + hint.
#
# Inputs (env):
#   PG_HOST, PG_PORT, PG_USER  disposable server (as run.sh)
#   GOOSE_VERSION  schema head (default: the highest migration in the tree)
#   REPLAY_LIMIT   real-name replay inputs in the main corpus (default 120000)
#   PHASES         corpora to run (default "main hint fault abstain rematch")
#   MAIN_RUN       the main phase's evidence the rematch phase starts from
#                  (default $OUT/main); ${PREFIX}_main_go1 must still hold it
#   ORGANIC_RUN    the same for organic2 (default $OUT/organic)
#   ORGANIC        1: every Rust phase admits organic jobs up to priority 20
#                  (G0: main, hint, fault and abstain must pass unchanged)
#   OUT            evidence directory (default: mktemp)
#   BITMAGNET_GO   Go binary (default: go build into $OUT)
#   RUST_WORKER    Rust worker binary; without it only the Go-side gates run
#   TMDB_KEY       the dummy API key both sides send (never Go's default key,
#                  which downgrades Go to 1 request/s)
#   STUB_PORT      the stub's port (default 38573)
#   SEED_ARGS      extra flagson-seed flags, e.g. "-hints 300" for a smoke run
#   FLAGSON_PREFIX database name prefix (default bm_ptw_flagson), to keep
#                  another run's databases on the same server
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
PG_HOST="${PG_HOST:-/tmp}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
GOOSE_VERSION="${GOOSE_VERSION:-$(ls "$ROOT/migrations" | sed -n 's/^0*\([0-9][0-9]*\)_.*\.sql$/\1/p' | sort -n | tail -1)}"
REPLAY_LIMIT="${REPLAY_LIMIT:-120000}"
PHASES="${PHASES:-main hint fault abstain rematch}"
OUT="${OUT:-$(mktemp -d)}"
TMDB_KEY="${TMDB_KEY:-flagson-harness-dummy-key-0001}"
# Fixed, because Go's job error text quotes the request URL (host and port
# included) and the dump compares it.
STUB_PORT="${STUB_PORT:-38573}"
PREFIX="${FLAGSON_PREFIX:-bm_ptw_flagson}"
HELD=2099-01-01
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
MAIN_RUN="${MAIN_RUN:-$OUT/main}"
ORGANIC_RUN="${ORGANIC_RUN:-$OUT/organic}"
ORGANIC="${ORGANIC:-}"
# The organic phases' bound, set around their Rust phases.
ORGANIC_BOUND=

if [[ "$TMDB_KEY" == 9c6689fa83ae6814fbfb200d70bba3a8 ]]; then
  echo "TMDB_KEY must not be Go's default key" >&2; exit 2
fi

dsn() { echo "postgres://${PG_USER}@localhost:${PG_PORT}/$1?host=${PG_HOST}&sslmode=disable"; }
sql() { psql -X -q -v ON_ERROR_STOP=1 -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" "$@"; }
# Jobs a worker may claim now: held jobs (run_after in 2100) don't count.
active() { sql -d "$1" -tAc "SELECT count(*) FROM queue_jobs WHERE status IN ('pending','retry') AND run_after < '$HELD'"; }
# Open transactions other than ours: a claimed job is in flight.
inflight() { sql -d "$1" -tAc "SELECT count(*) FROM pg_stat_activity WHERE datname = '$1' AND xact_start IS NOT NULL AND pid <> pg_backend_pid()"; }
# As in run.sh: collapse Go's 16s..1min retry backoff; run_after is not dumped.
hasten() { sql -d "$1" -c "UPDATE queue_jobs SET run_after = clock_timestamp() WHERE status = 'retry' AND run_after > clock_timestamp() AND run_after < '$HELD'"; }

if [[ -z "${BITMAGNET_GO:-}" ]]; then
  (cd "$ROOT" && go build -o "$OUT/bitmagnet" .)
  BITMAGNET_GO="$OUT/bitmagnet"
fi
(cd "$ROOT" && go build -o "$OUT/ptw" ./tools/parity/process_torrent_worker)
PTW="$OUT/ptw"
RUST_WORKER="${RUST_WORKER:-}"

PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do [[ -n "$p" ]] && kill -INT "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

start_stub() { # corpus rundir [stub flags] -> sets STUB_PID, STUB_URL
  local corpus=$1 run=$2 keepalive=true
  shift 2
  [[ $corpus == fault || $corpus == abstain ]] && keepalive=false
  rm -f "$run/stub.ready"
  "$PTW" tmdb-stub -listen "127.0.0.1:$STUB_PORT" -api-key "$TMDB_KEY" -keepalive="$keepalive" \
    -responses "$OUT/$corpus/seed/responses.jsonl" -faults "$OUT/$corpus/seed/faults.jsonl" \
    -log "$run/stub.jsonl" -ready "$run/stub.ready" "$@" 2>>"$run/stub.log" &
  STUB_PID=$!
  PIDS+=("$STUB_PID")
  for _ in $(seq 100); do [[ -s "$run/stub.ready" ]] && break; sleep 0.1; done
  STUB_URL="http://$(head -1 "$run/stub.ready")/3"
}
start_go() { # db rundir [tape] -> pid
  local env=(POSTGRES_DSN="$(dsn "$1")" LOG_LEVEL=info TMDB_BASE_URL="$STUB_URL" TMDB_API_KEY="$TMDB_KEY")
  if [[ "${3:-}" == tape ]]; then
    mkdir -p "$2/tape"
    env+=(CLASSIFIER_TAPE_DIR="$2/tape" CLASSIFIER_TAPE_MAX_RECORDS=10000000)
  fi
  (cd "$2" && exec env "${env[@]}" "$BITMAGNET_GO" worker run --keys=queue_server >>"$2/go.log" 2>&1) &
  echo $!
}
# One log per Rust phase ($3), for flagson-abstained and flagson-attribution,
# with the resolver guard's call events (debug). The cool-off outlasts any
# phase, so each abstention is logged (and its requests made) once.
start_rust() { # db rundir log -> pid
  local organic=()
  if [[ -n "$ORGANIC_BOUND" ]]; then
    organic=(--admit-organic --organic-max-priority "$ORGANIC_BOUND")
  elif [[ "$ORGANIC" == 1 ]]; then
    organic=(--admit-organic --organic-max-priority 20)
  fi
  (cd "$2" && BITMAGNET_POSTGRES_DSN="$(dsn "$1")" RUST_LOG=info,bitmagnet_processor::live=debug exec "$RUST_WORKER" \
    --expected-classifier-config-digest "$GO_DIGEST" --check-interval-seconds 1 --abstain-cooloff-seconds 86400 \
    --admit-flags-on "${organic[@]}" --tmdb-api-key "$TMDB_KEY" --tmdb-base-url "$STUB_URL" >>"$2/$3" 2>&1) &
  echo $!
}
# The workers are not this shell's children (they start inside a command
# substitution), so wait by polling: Go writes its tape on the way out.
stop() {
  local p
  kill -INT "$@" 2>/dev/null || true
  for p in "$@"; do
    for _ in $(seq 600); do kill -0 "$p" 2>/dev/null || break; sleep 0.2; done
  done
}

# Settled: at most $2 claimable jobs, nothing in flight, and the count
# unchanged for 5 consecutive 2s polls after at least $3 polls (default 5).
drain() {
  local db=$1 want=$2 min=${3:-5} last=-1 same=0 polls=0 now
  while :; do
    hasten "$db" >/dev/null
    now="$(active "$db")"
    if [[ "$now" == "$last" && "$(inflight "$db")" == 0 ]]; then same=$((same + 1)); else same=0; fi
    last=$now
    polls=$((polls + 1))
    if [[ "$now" -le "$want" && "$same" -ge 5 && "$polls" -ge "$min" ]]; then break; fi
    sleep 2
  done
  echo "$db settled with $now claimable jobs"
}

# An abstention leaves the queue unchanged, so drain alone can settle before
# Rust has reached every job it must abstain from: first wait (up to 30 min)
# for that many abstention lines in the phase's log.
wait_abstained() { # log n
  local _
  for _ in $(seq 900); do
    [[ "$(grep -c 'abstaining from' "$1" 2>/dev/null || true)" -ge "$2" ]] && return 0
    sleep 2
  done
  echo "timed out waiting for $2 Rust abstentions in $1" >&2
}

clone() { sql -d postgres -c "DROP DATABASE IF EXISTS $2" -c "CREATE DATABASE $2 TEMPLATE $1"; }

# A round-2 seed's round1-jobs.jsonl lists round 1's seeded jobs, which stay
# in its database (organic round 1's negative payloads do not even decode).
dump_run() { # db rundir
  local seeded=(-seed-jobs "$2/seed/jobs.jsonl")
  [[ -e "$2/seed/round1-jobs.jsonl" ]] && seeded+=(-seed-jobs "$2/seed/round1-jobs.jsonl")
  "$PTW" flagson-dump -dsn "$(dsn "$1")" "${seeded[@]}" -raw-out "$2/dump.raw" \
    >"$2/dump" 2>"$2/dump.counts"
}

# Request multisets leave /authentication out: Go validates its key on first
# use and again from a 5-minute periodic health check (tmdbhealthcheck), so the
# count follows wall-clock run time and process count, not classification.
# A 4th argument "info" reports the request diff without failing on it: in a
# Rust-then-Go run Rust's requests before it abstains come on top of Go's.
compare() { # label rundirA rundirB [info]
  local label=$1 a=$2 b=$3 requests_gate=${4:-gate}
  if diff -u "$a/dump" "$b/dump" >"$b/dump.diff"; then
    echo "$label dumps: IDENTICAL ($(wc -l <"$a/dump") lines)" | tee -a "$OUT/gates.txt"
  else
    echo "$label dumps: DIFFERENT ($(grep -c '^[-+][^-+]' "$b/dump.diff") changed lines, see $b/dump.diff)" | tee -a "$OUT/gates.txt"
    status=1
  fi
  if diff -u "$a/dump.raw" "$b/dump.raw" >"$b/dump.raw.diff"; then
    echo "$label raw dumps (info): IDENTICAL" | tee -a "$OUT/gates.txt"
  else
    echo "$label raw dumps (info): $(grep -c '^[-+][^-+]' "$b/dump.raw.diff") changed lines, see $b/dump.raw.diff" | tee -a "$OUT/gates.txt"
    "$PTW" flagson-rawdiff "$a/dump.raw" "$b/dump.raw" >"$b/dump.raw.fields" 2>&1 || true
    echo "$label raw dumps by field (info): $(head -1 "$b/dump.raw.fields")" | tee -a "$OUT/gates.txt"
  fi
  if "$PTW" request-diff -ignore-endpoint /authentication "$a/stub.jsonl" "$b/stub.jsonl" \
      >"$b/requests.diff" 2>"$b/requests.summary"; then
    echo "$label requests: $(tail -1 "$b/requests.summary")" | tee -a "$OUT/gates.txt"
  else
    echo "$label requests: $(tail -1 "$b/requests.summary") (see $b/requests.diff)" | tee -a "$OUT/gates.txt"
    [[ "$requests_gate" == info ]] || status=1
  fi
  echo "$label /authentication (excluded, info): $(auth_count "$a") vs $(auth_count "$b")" | tee -a "$OUT/gates.txt"
}
auth_count() { grep -c '"endpoint":"/authentication"' "$1/stub.jsonl" || true; }

# Held jobs of one hold kind (flagson_faults.go: alone, auth), in queue order.
held_jobs() { # corpus kind
  python3 -I -c 'import json,sys
for l in open(sys.argv[1]):
    j = json.loads(l)
    if j.get("hold") == sys.argv[2]: print(j["fingerprint"])' "$OUT/$1/seed/jobs.jsonl" "$2"
}
release() { sql -d "$1" -c "UPDATE queue_jobs SET run_after = clock_timestamp() WHERE fingerprint = '$2'" >/dev/null; }

seed_corpus() {
  local corpus=$1 db=${PREFIX}_$1_seed
  mkdir -p "$OUT/$corpus/seed"
  sql -d postgres -c "DROP DATABASE IF EXISTS $db" -c "CREATE DATABASE $db"
  goose -dir "$ROOT/migrations" postgres "$(dsn "$db")" up-to "$GOOSE_VERSION" >"$OUT/$corpus/goose.log" 2>&1
  "$PTW" flagson-seed -dsn "$(dsn "$db")" -root "$ROOT" -out "$OUT/$corpus/seed" -corpus "$corpus" \
    -replay-limit "$REPLAY_LIMIT" ${SEED_ARGS:-} 2>&1 | tee "$OUT/$corpus/seed.log"
  local planned
  planned="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["effectiveConfigDigest"])' "$OUT/$corpus/seed/summary.json")"
  if [[ "$planned" != "$GO_DIGEST" ]]; then
    echo "$corpus: the plan ran under $planned but the Go binary reports $GO_DIGEST" >&2; exit 1
  fi
}

# The rematch phase's seed: a copy of Go's drained main round 1 that
# flagson-requeue queues again. The copy must be MAIN_RUN's round 1: the
# database's dump must still equal MAIN_RUN/go1/dump. The phase answers TMDB
# from the main seed's fixtures.
seed_rematch() {
  local db=${PREFIX}_rematch_seed seed="$OUT/rematch/seed" f
  mkdir -p "$seed"
  "$PTW" flagson-dump -dsn "$(dsn ${PREFIX}_main_go1)" -seed-jobs "$MAIN_RUN/seed/jobs.jsonl" \
    >"$OUT/rematch/main-go1.dump" 2>/dev/null
  if ! cmp -s "$OUT/rematch/main-go1.dump" "$MAIN_RUN/go1/dump"; then
    echo "rematch: ${PREFIX}_main_go1 no longer holds $MAIN_RUN/go1's round 1" >&2; exit 1
  fi
  clone ${PREFIX}_main_go1 "$db" >/dev/null
  for f in responses.jsonl faults.jsonl plan.jsonl notes.jsonl; do ln -sfn "$MAIN_RUN/seed/$f" "$seed/$f"; done
  "$PTW" flagson-requeue -dsn "$(dsn "$db")" -out "$seed" 2>&1 | tee "$OUT/rematch/seed.log"
}

# An organic phase's jobs: the batch slices' made by Go's own batch handler,
# each in a copy of the phase's seed holding only its torrents, then every
# slice's queued verbatim in the seed.
organic_jobs() { # corpus
  local corpus=$1 db=${PREFIX}_$1_produce run="$OUT/$1/produce" slice
  mkdir -p "$run"
  start_stub "$corpus" "$run"
  for slice in $("$PTW" flagson-organic-info -seed "$OUT/$corpus/seed" -batch); do
    clone "${PREFIX}_${corpus}_seed" "$db" >/dev/null
    "$PTW" flagson-organic-produce -dsn "$(dsn "$db")" -seed "$OUT/$corpus/seed" -slice "$slice" \
      -bitmagnet "$BITMAGNET_GO" -run "$run" -tmdb-base-url "$STUB_URL" -tmdb-api-key "$TMDB_KEY" \
      2>&1 | tee -a "$OUT/$corpus/seed.log"
  done
  stop "$STUB_PID"
  sql -d postgres -c "DROP DATABASE IF EXISTS $db" >/dev/null
  "$PTW" flagson-organic-enqueue -dsn "$(dsn "${PREFIX}_${corpus}_seed")" -seed "$OUT/$corpus/seed" \
    2>&1 | tee -a "$OUT/$corpus/seed.log"
}

# organic2's seed: a copy of Go's drained organic round 1 (checked as for
# rematch) that flagson-requeue -organic queues again, answering TMDB from
# the organic seed's fixtures.
seed_organic2() {
  local db=${PREFIX}_organic2_seed seed="$OUT/organic2/seed" f
  mkdir -p "$seed"
  "$PTW" flagson-dump -dsn "$(dsn ${PREFIX}_organic_go1)" -seed-jobs "$ORGANIC_RUN/seed/jobs.jsonl" \
    >"$OUT/organic2/organic-go1.dump" 2>/dev/null
  if ! cmp -s "$OUT/organic2/organic-go1.dump" "$ORGANIC_RUN/go1/dump"; then
    echo "organic2: ${PREFIX}_organic_go1 no longer holds $ORGANIC_RUN/go1's round 1" >&2; exit 1
  fi
  clone ${PREFIX}_organic_go1 "$db" >/dev/null
  for f in responses.jsonl faults.jsonl plan.jsonl notes.jsonl; do ln -sfn "$ORGANIC_RUN/seed/$f" "$seed/$f"; done
  ln -sfn "$ORGANIC_RUN/seed/jobs.jsonl" "$seed/round1-jobs.jsonl"
  "$PTW" flagson-requeue -organic "$ORGANIC_RUN/seed" -dsn "$(dsn "$db")" -out "$seed" 2>&1 | tee "$OUT/organic2/seed.log"
}

# Go alone over a clone of the corpus seed; $3 = tape to record the tape.
go_alone() { # corpus name [tape]
  local corpus=$1 name=$2 db=${PREFIX}_$1_$2 run="$OUT/$1/$2" pid fp auth T0=$SECONDS
  mkdir -p "$run"; ln -sfn ../seed "$run/seed"
  clone "${PREFIX}_${corpus}_seed" "$db" >/dev/null
  start_stub "$corpus" "$run"
  pid="$(start_go "$db" "$run" "${3:-}")"; drain "$db" 0; stop "$pid"
  for fp in $(held_jobs "$corpus" alone); do
    release "$db" "$fp"
    pid="$(start_go "$db" "$run")"; drain "$db" 0; stop "$pid"
  done
  auth="$(held_jobs "$corpus" auth)"
  if [[ -n "$auth" ]]; then
    for fp in $auth; do release "$db" "$fp"; done
    pid="$(start_go "$db" "$run")"; drain "$db" 0; stop "$pid"
  fi
  stop "$STUB_PID"
  dump_run "$db" "$run"
  echo "$corpus $name (Go alone): $((SECONDS - T0))s" | tee -a "$OUT/timing.txt"
}

# Rust, then Go for whatever Rust left; $3 = how many jobs Rust must leave in
# its first phase (each held job is one more: a phase per holdAlone job, one
# for the holdAuth jobs, whose stub fails /authentication), checked by
# flagson-abstained against the phase's log with reason $5. The stub restarts around every Rust
# phase, which flushes its log and restarts its fault sequences, so the
# requests Rust made are the lines that phase added (rust-requests-<phase>.jsonl,
# all of them in rust-requests.jsonl); with $6 = none Rust must make none.
rust_then_go() { # corpus name expected slice reason [none]
  local corpus=$1 name=$2 expect=$3 slice=$4 reason=$5 requests=${6:-any} db=${PREFIX}_$1_$2 run="$OUT/$1/$2"
  local pid fp T0=$SECONDS left n=0 auth=()
  mkdir -p "$run"; ln -sfn ../seed "$run/seed"
  clone "${PREFIX}_${corpus}_seed" "$db" >/dev/null
  : >"$run/stub.jsonl"; : >"$run/rust-requests.jsonl"
  rust_phase() { # expect log [stub flags]
    local expect=$1 log=$2 before after
    shift 2
    before="$(wc -l <"$run/stub.jsonl")"
    start_stub "$corpus" "$run" "$@"
    pid="$(start_rust "$db" "$run" "$log")"; wait_abstained "$run/$log" "$expect"; drain "$db" "$expect" 15; stop "$pid"
    stop "$STUB_PID"
    after="$(wc -l <"$run/stub.jsonl")"
    tail -n "+$((before + 1))" "$run/stub.jsonl" >"$run/rust-requests-${log%.log}.jsonl"
    cat "$run/rust-requests-${log%.log}.jsonl" >>"$run/rust-requests.jsonl"
    echo "$corpus $name Rust phase $log: $((after - before)) stub requests" | tee -a "$OUT/timing.txt"
    if ! "$PTW" flagson-abstained -dsn "$(dsn "$db")" -seed "$OUT/$corpus/seed" -slice "$slice" -expect "$expect" \
        -rust-log "$run/$log" -reason "$reason" 2>&1 | tee -a "$run/abstained.txt" "$OUT/gates.txt"; then status=1; fi
  }
  go_phase() {
    start_stub "$corpus" "$run"
    pid="$(start_go "$db" "$run")"; drain "$db" 0; stop "$pid"
    stop "$STUB_PID"
  }
  rust_phase "$expect" rust.log
  left="$(active "$db")"
  echo "$corpus $name Rust phase: $((SECONDS - T0))s, left for Go: $left (expected $expect)" | tee -a "$OUT/timing.txt" "$OUT/gates.txt"
  if [[ "$left" -gt 0 ]]; then go_phase; fi
  for fp in $(held_jobs "$corpus" alone); do
    release "$db" "$fp"
    n=$((n + 1))
    rust_phase 1 "rust-held-$n.log"
    go_phase
  done
  mapfile -t auth < <(held_jobs "$corpus" auth)
  if [[ ${#auth[@]} -gt 0 ]]; then
    for fp in "${auth[@]}"; do release "$db" "$fp"; done
    rust_phase "${#auth[@]}" rust-auth.log -authentication-fault 500
    go_phase
  fi
  n="$(grep -c . "$run/rust-requests.jsonl" || true)"
  if [[ "$requests" == none ]]; then
    if [[ "$n" == 0 ]]; then
      echo "$corpus $name: Rust made 0 stub requests" | tee -a "$OUT/gates.txt"
    else
      echo "$corpus $name: Rust made $n stub requests, want 0 (see $run/rust-requests.jsonl)" | tee -a "$OUT/gates.txt"
      status=1
    fi
  else
    echo "$corpus $name: Rust made $n stub requests ($(grep -c '"source":"fault"' "$run/rust-requests.jsonl" || true) answered by a fault)" | tee -a "$OUT/gates.txt"
  fi
  dump_run "$db" "$run"
  echo "$corpus $name (Rust then Go): $((SECONDS - T0))s" | tee -a "$OUT/timing.txt"
}

# Rust alone: nothing may be left behind.
rust_alone() { # corpus name
  local corpus=$1 name=$2 db=${PREFIX}_$1_$2 run="$OUT/$1/$2" pid T0=$SECONDS left jobs
  mkdir -p "$run"; ln -sfn ../seed "$run/seed"
  clone "${PREFIX}_${corpus}_seed" "$db" >/dev/null
  jobs="$(active "$db")"
  start_stub "$corpus" "$run"
  pid="$(start_rust "$db" "$run" rust.log)"; drain "$db" "$jobs" 15; stop "$pid"
  stop "$STUB_PID"
  left="$(active "$db")"
  sql -d "$db" -tAc "SELECT fingerprint FROM queue_jobs WHERE status IN ('pending','retry') ORDER BY fingerprint" >"$run/left.txt"
  echo "$corpus $name (Rust alone): $((SECONDS - T0))s, jobs left behind (abstentions): $left" | tee -a "$OUT/timing.txt" "$OUT/gates.txt"
  [[ "$left" == 0 ]] || status=1
  if ! "$PTW" flagson-abstained -dsn "$(dsn "$db")" -seed "$OUT/$corpus/seed" -slice "$corpus" -expect 0 \
      -rust-log "$run/rust.log" -reason none 2>&1 | tee -a "$run/abstained.txt" "$OUT/gates.txt"; then status=1; fi
  dump_run "$db" "$run"
}

# Rust with organic admission up to priority $3, then Go for what it left.
# flagson-abstained -organic-bound checks what Rust left: the negative
# slices' jobs and those above the bound never claimed, the hinted slice's
# abstained from once each (the cool-off outlasts the phase), all untouched.
rust_then_go_organic() { # corpus name bound
  local corpus=$1 name=$2 bound=$3 db=${PREFIX}_$1_$2 run="$OUT/$1/$2" pid T0=$SECONDS info expect hinted left
  mkdir -p "$run"; ln -sfn ../seed "$run/seed"
  clone "${PREFIX}_${corpus}_seed" "$db" >/dev/null
  info="$("$PTW" flagson-organic-info -seed "$OUT/$corpus/seed" -left "$bound")"
  read -r expect hinted <<<"$info"
  : >"$run/stub.jsonl"
  start_stub "$corpus" "$run"
  ORGANIC_BOUND=$bound
  pid="$(start_rust "$db" "$run" rust.log)"; wait_abstained "$run/rust.log" "$hinted"; drain "$db" "$expect" 15; stop "$pid"
  ORGANIC_BOUND=
  stop "$STUB_PID"
  cp "$run/stub.jsonl" "$run/rust-requests.jsonl"
  left="$(active "$db")"
  echo "$corpus $name Rust phase (bound $bound): $((SECONDS - T0))s, $(grep -c . "$run/rust-requests.jsonl" || true) stub requests, left for Go: $left (expected $expect, $hinted hinted)" \
    | tee -a "$OUT/timing.txt" "$OUT/gates.txt"
  if ! "$PTW" flagson-abstained -dsn "$(dsn "$db")" -seed "$OUT/$corpus/seed" -organic-bound "$bound" \
      -rust-log "$run/rust.log" 2>&1 | tee -a "$run/abstained.txt" "$OUT/gates.txt"; then status=1; fi
  if [[ "$left" -gt 0 ]]; then
    start_stub "$corpus" "$run"
    pid="$(start_go "$db" "$run")"; drain "$db" 0; stop "$pid"
    stop "$STUB_PID"
  fi
  dump_run "$db" "$run"
  echo "$corpus $name (Rust then Go, bound $bound): $((SECONDS - T0))s" | tee -a "$OUT/timing.txt"
}

# G7 in a drained organic run ($2).
organic_check() { # name
  if "$PTW" flagson-organic-check -dsn "$(dsn "${PREFIX}_organic_$1")" -seed "$OUT/organic/seed" \
      >"$OUT/organic/$1/organic-check.txt" 2>&1; then
    echo "organic G7 $1: $(tail -1 "$OUT/organic/$1/organic-check.txt")" | tee -a "$OUT/gates.txt"
  else
    echo "organic G7 $1: FAIL (see $OUT/organic/$1/organic-check.txt)" | tee -a "$OUT/gates.txt"
    status=1
  fi
}

# Go alone ($2) against its corpus's fault classes (flagson-faultcheck).
faultcheck() { # corpus name
  if "$PTW" flagson-faultcheck -dsn "$(dsn "${PREFIX}_$1_$2")" -seed "$OUT/$1/seed" \
      -requests "$OUT/$1/$2/stub.jsonl" 2>&1 | tee "$OUT/$1/$2/faultcheck.txt" | tee -a "$OUT/gates.txt"; then
    echo "$1 faultcheck $2: every fault job as modelled" | tee -a "$OUT/gates.txt"
  else
    echo "$1 faultcheck $2: FAIL (see $OUT/$1/$2/faultcheck.txt)" | tee -a "$OUT/gates.txt"
    status=1
  fi
}

status=0

# The Go processor logs its effective classifier digest; the plan and the Rust
# worker must run under the same configuration.
sql -d postgres -c "DROP DATABASE IF EXISTS ${PREFIX}_digest" -c "CREATE DATABASE ${PREFIX}_digest" >/dev/null
goose -dir "$ROOT/migrations" postgres "$(dsn ${PREFIX}_digest)" up-to "$GOOSE_VERSION" >"$OUT/goose-digest.log" 2>&1
GO_DIGEST="$(cd "$OUT" && POSTGRES_DSN="$(dsn ${PREFIX}_digest)" "$BITMAGNET_GO" classifier digest 2>/dev/null | grep -o 'sha256:[0-9a-f]*' | head -1)"
echo "go classifier digest: $GO_DIGEST (goose $GOOSE_VERSION)" | tee "$OUT/digest.txt"

for corpus in $PHASES; do
  case $corpus in
    rematch) seed_rematch ;;
    organic2) seed_organic2; organic_jobs organic2 ;;
    organic) seed_corpus organic; organic_jobs organic ;;
    *) seed_corpus "$corpus" ;;
  esac
  case $corpus in
    main|hint)
      go_alone "$corpus" go1 tape
      go_alone "$corpus" go2
      compare "$corpus G-1 go1 vs go2" "$OUT/$corpus/go1" "$OUT/$corpus/go2"
      if [[ -n "$RUST_WORKER" ]]; then
        if [[ $corpus == main ]]; then
          rust_alone main rust
          compare "main G2 go1 vs rust" "$OUT/main/go1" "$OUT/main/rust"
        else
          rust_then_go hint rust "$(grep -c . "$OUT/hint/seed/jobs.jsonl")" hint hint none
          compare "hint G2 go1 vs rust-then-go" "$OUT/hint/go1" "$OUT/hint/rust"
        fi
      fi
      ;;
    fault)
      go_alone fault go1
      go_alone fault go2
      go_alone fault go3
      compare "fault G-1 go1 vs go2" "$OUT/fault/go1" "$OUT/fault/go2"
      compare "fault G-1 go1 vs go3" "$OUT/fault/go1" "$OUT/fault/go3"
      faultcheck fault go1
      if [[ -n "$RUST_WORKER" ]]; then
        rust_alone fault rust
        compare "fault G2-fault go1 vs rust" "$OUT/fault/go1" "$OUT/fault/rust"
      fi
      ;;
    abstain)
      go_alone abstain go1
      go_alone abstain go2
      compare "abstain G-1 go1 vs go2" "$OUT/abstain/go1" "$OUT/abstain/go2"
      faultcheck abstain go1
      if [[ -n "$RUST_WORKER" ]]; then
        unheld="$(python3 -I -c 'import json,sys
print(sum(1 for l in open(sys.argv[1]) if not json.loads(l).get("hold")))' "$OUT/abstain/seed/faultplan.jsonl")"
        rust_then_go abstain rust "$unheld" fault resolver
        compare "abstain G3 go1 vs rust-then-go" "$OUT/abstain/go1" "$OUT/abstain/rust" info
        logs=()
        for f in "$OUT"/abstain/rust/rust*.log; do logs+=(-rust-log "$f"); done
        if "$PTW" flagson-attribution -go "$OUT/abstain/go1/stub.jsonl" -rtg "$OUT/abstain/rust/stub.jsonl" \
            -seed "$OUT/abstain/seed" "${logs[@]}" 2>&1 \
            | tee "$OUT/abstain/rust/attribution.txt" | tee -a "$OUT/gates.txt"; then
          echo "abstain G3 attribution: PASS" | tee -a "$OUT/gates.txt"
        else
          echo "abstain G3 attribution: FAIL (see $OUT/abstain/rust/attribution.txt)" | tee -a "$OUT/gates.txt"
          status=1
        fi
      fi
      ;;
    rematch)
      go_alone rematch go1
      go_alone rematch go2
      compare "rematch G-1 go1 vs go2" "$OUT/rematch/go1" "$OUT/rematch/go2"
      if "$PTW" flagson-rematch-coverage -dsn "$(dsn ${PREFIX}_rematch_go1)" -seed "$OUT/rematch/seed" \
          | tee "$OUT/rematch/coverage.txt"; then
        echo "rematch coverage: every floor met" | tee -a "$OUT/gates.txt"
      else
        echo "rematch coverage: floors NOT met (see $OUT/rematch/coverage.txt)" | tee -a "$OUT/gates.txt"
        status=1
      fi
      if [[ -n "$RUST_WORKER" ]]; then
        rust_alone rematch rust
        compare "rematch G2 go1 vs rust" "$OUT/rematch/go1" "$OUT/rematch/rust"
      fi
      ;;
    organic|organic2)
      go_alone "$corpus" go1 tape
      go_alone "$corpus" go2
      compare "$corpus G-1 go1 vs go2" "$OUT/$corpus/go1" "$OUT/$corpus/go2"
      if "$PTW" flagson-organic-coverage -dir "$OUT/$corpus/go1" | tee "$OUT/$corpus/coverage.txt"; then
        echo "$corpus coverage: every floor met" | tee -a "$OUT/gates.txt"
      else
        echo "$corpus coverage: floors NOT met (see $OUT/$corpus/coverage.txt)" | tee -a "$OUT/gates.txt"
        status=1
      fi
      if [[ $corpus == organic ]]; then
        organic_check go1
      elif "$PTW" flagson-rematch-coverage -dsn "$(dsn ${PREFIX}_organic2_go1)" -seed "$OUT/organic2/seed" \
          -floor-scale 0.15 | tee "$OUT/organic2/rematch-coverage.txt"; then
        echo "organic2 rematch coverage: every floor met" | tee -a "$OUT/gates.txt"
      else
        echo "organic2 rematch coverage: floors NOT met (see $OUT/organic2/rematch-coverage.txt)" | tee -a "$OUT/gates.txt"
        status=1
      fi
      if [[ -n "$RUST_WORKER" ]]; then
        rust_then_go_organic "$corpus" rust 20
        compare "$corpus G2 go1 vs rust-then-go (bound 20)" "$OUT/$corpus/go1" "$OUT/$corpus/rust"
        if [[ $corpus == organic ]]; then
          organic_check rust
          rust_then_go_organic organic rust0 0
          compare "organic G4 go1 vs rust-then-go (bound 0)" "$OUT/organic/go1" "$OUT/organic/rust0"
        fi
      fi
      ;;
  esac
done

if [[ " $PHASES " == *" main "* && " $PHASES " == *" hint "* ]]; then
  if "$PTW" flagson-coverage -out "$OUT/coverage.json" \
      -name main -dir "$OUT/main/go1" -dsn "$(dsn ${PREFIX}_main_go1)" \
      -name hint -dir "$OUT/hint/go1" -dsn "$(dsn ${PREFIX}_hint_go1)" | tee "$OUT/coverage.txt"; then
    echo "coverage: every floor met" | tee -a "$OUT/gates.txt"
  else
    echo "coverage: floors NOT met (see $OUT/coverage.txt)" | tee -a "$OUT/gates.txt"
    status=1
  fi
fi

echo "evidence: $OUT"
exit $status
