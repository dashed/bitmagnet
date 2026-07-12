#!/usr/bin/env bash
# remote-test.sh — the ONLY build/test command for Lane S (p2s-searchquery).
#
# The local machine is CPU/RAM constrained: never run cargo locally. This
# transfers the worktree to the bm-p2s Coder workspace (tar-over-ssh — rsync
# silently no-ops through the coder ssh proxy) and builds/tests there. Self-heals
# the Rust toolchain under $HOME (workspace rootfs is ephemeral; $HOME persists).
#
# Usage: ./remote-test.sh [cargo-subcommand-args...]
#   default: fmt-check + clippy + test for -p bitmagnet-search-query
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WS="coder.bm-p2s"
REMOTE_DIR="p2s"

echo "==> [1/3] tar bitmagnet-rs src -> ${WS}:${REMOTE_DIR}/ (PRESERVES target/ cache)"
# Clear only the source crates, never target/ (the warm dependency cache lives in
# bitmagnet-rs/target and must survive re-transfers for fast incremental builds).
ssh "$WS" "mkdir -p ~/${REMOTE_DIR}/bitmagnet-rs ~/${REMOTE_DIR}/testdata/parity && rm -rf ~/${REMOTE_DIR}/bitmagnet-rs/crates ~/${REMOTE_DIR}/bitmagnet-rs/Cargo.toml ~/${REMOTE_DIR}/bitmagnet-rs/Cargo.lock"
tar czf - -C "$REPO_ROOT" \
  --exclude='.git' --exclude='.jj' --exclude='target' --exclude='node_modules' \
  bitmagnet-rs \
  | ssh "$WS" "tar xzf - -C ~/${REMOTE_DIR}/"
if [ -d "$REPO_ROOT/testdata/parity" ]; then
  tar czf - -C "$REPO_ROOT" testdata/parity | ssh "$WS" "tar xzf - -C ~/${REMOTE_DIR}/"
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
  run cargo fmt -p bitmagnet-search-query --check
  run cargo clippy -p bitmagnet-search-query --all-targets -- -D warnings
  run cargo test -p bitmagnet-search-query
fi
echo "@@ALLDONE"
REMOTE
