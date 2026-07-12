#!/usr/bin/env bash
# remote-test.sh — the ONLY build/test command for Lane C (p2c-searchserve).
#
# The local machine is CPU/RAM constrained: never run cargo locally. This rsyncs
# the worktree to the bm-p2c Coder workspace and builds/tests the
# bitmagnet-search-serve crate + its path deps there. Self-heals the Rust
# toolchain under $HOME (the workspace rootfs is ephemeral; only $HOME persists
# across auto-stop).
#
# Usage: ./remote-test.sh [cargo-subcommand-args...]
#   default: fmt-check + clippy + test for -p bitmagnet-search-serve
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WS="coder.bm-fu"
REMOTE_DIR="p2c"

echo "==> [1/3] rsync bitmagnet-rs (+ testdata/parity) -> ${WS}:${REMOTE_DIR}/"
ssh "$WS" "mkdir -p ~/${REMOTE_DIR}/bitmagnet-rs ~/${REMOTE_DIR}/testdata/parity"
rsync -az --delete \
  --exclude '.git' --exclude '.jj' --exclude 'target' --exclude 'node_modules' \
  -e ssh "$REPO_ROOT/bitmagnet-rs/" "${WS}:${REMOTE_DIR}/bitmagnet-rs/"
if [ -d "$REPO_ROOT/testdata/parity" ]; then
  rsync -az --delete -e ssh "$REPO_ROOT/testdata/parity/" "${WS}:${REMOTE_DIR}/testdata/parity/"
fi

echo "==> [2/3] self-heal Rust toolchain under \$HOME"
ssh "$WS" 'bash -s' <<'REMOTE'
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
if ! command -v rustc >/dev/null 2>&1; then
  echo "    installing rustup (stable, minimal)"
  curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal >/dev/null
  export PATH="$HOME/.cargo/bin:$PATH"
  rustup component add rustfmt clippy >/dev/null 2>&1 || true
fi
echo "    rustc: $(rustc --version)"
echo "    cargo: $(cargo --version)"
REMOTE

echo "==> [3/3] cargo fmt/clippy/test on ${WS} (CARGO_BUILD_JOBS=4)"
CARGO_ARGS="${*:-}"
ssh "$WS" 'bash -s' <<REMOTE
set -uo pipefail
export PATH="\$HOME/.cargo/bin:\$PATH"
export CARGO_BUILD_JOBS=4
export CARGO_TERM_COLOR=never
cd ~/${REMOTE_DIR}/bitmagnet-rs
: > ~/${REMOTE_DIR}/run.log
run() { echo "--- \$* ---" | tee -a ~/${REMOTE_DIR}/run.log; "\$@" >>~/${REMOTE_DIR}/run.log 2>&1; echo "@@RC=\$?" | tee -a ~/${REMOTE_DIR}/run.log; }
if [ -n "${CARGO_ARGS}" ]; then
  run cargo ${CARGO_ARGS}
else
  run cargo fmt -p bitmagnet-search-serve --check
  run cargo clippy -p bitmagnet-search-serve --all-targets --all-features -- -D warnings
  run cargo test -p bitmagnet-search-serve --all-features
fi
echo "@@ALLDONE"
REMOTE
echo "==> remote run.log tail:"
ssh "$WS" "tail -40 ~/${REMOTE_DIR}/run.log" 2>&1 || true