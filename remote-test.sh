#!/usr/bin/env bash
# remote-test.sh — build/test gate for the Phase-2 G0/P1 spike lane (p2gp).
#
# Local machine is CPU/RAM constrained: never run cargo locally. Rsyncs the
# worktree's bitmagnet-rs/ AND testdata/parity/ (the tests read the goldens via
# ../../../testdata/parity/) to the bm-p1q Coder workspace under a distinct
# REMOTE_DIR (p2gp) so it does not clobber other lanes, then runs the
# bitmagnet-graphql fmt + clippy(-D warnings) + test gate there.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WS="coder.bm-p1q"
REMOTE_DIR="p2gp"

echo "==> [1/3] rsync bitmagnet-rs + testdata/parity -> ${WS}:${REMOTE_DIR}/"
ssh "$WS" "mkdir -p ~/${REMOTE_DIR}/bitmagnet-rs ~/${REMOTE_DIR}/testdata/parity"
rsync -az --delete \
  --exclude '.git' --exclude '.jj' --exclude 'target' --exclude 'node_modules' \
  -e ssh "$REPO_ROOT/bitmagnet-rs/" "${WS}:${REMOTE_DIR}/bitmagnet-rs/"
rsync -az --delete \
  -e ssh "$REPO_ROOT/testdata/parity/" "${WS}:${REMOTE_DIR}/testdata/parity/"

echo "==> [2/3] self-heal Rust toolchain under \$HOME"
ssh "$WS" 'bash -s' <<'REMOTE'
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
if ! command -v rustc >/dev/null 2>&1; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal >/dev/null
  export PATH="$HOME/.cargo/bin:$PATH"
  rustup component add rustfmt clippy >/dev/null 2>&1 || true
fi
echo "    rustc: $(rustc --version)"
echo "    cargo: $(cargo --version)"
REMOTE

echo "==> [3/3] cargo gate on ${WS} (CARGO_BUILD_JOBS=4)"
ssh "$WS" 'bash -s' <<REMOTE
set -uo pipefail
export PATH="\$HOME/.cargo/bin:\$PATH"
export CARGO_BUILD_JOBS=4
export CARGO_TERM_COLOR=never
cd ~/${REMOTE_DIR}/bitmagnet-rs
: > ~/${REMOTE_DIR}/run.log
run() { echo "--- \$* ---" | tee -a ~/${REMOTE_DIR}/run.log; "\$@" >>~/${REMOTE_DIR}/run.log 2>&1; echo "@@RC=\$? (\$*)" | tee -a ~/${REMOTE_DIR}/run.log; }

run cargo fmt -p bitmagnet-graphql --check
run cargo clippy -p bitmagnet-graphql --all-targets --no-deps -- -D warnings
run cargo test -p bitmagnet-graphql -- --nocapture
echo "@@ALLDONE"
REMOTE

echo "==> fetch log tail"
ssh "$WS" "tail -n 120 ~/${REMOTE_DIR}/run.log"
