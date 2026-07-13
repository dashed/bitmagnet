#!/usr/bin/env bash
# Remote Rust gate for the Phase-2 integration branch.
#
# The local machine is CPU/RAM constrained. This transfers the Rust workspace
# and its complete test fixture tree to a Coder workspace while preserving its
# target cache.
#
# Usage: ./remote-test.sh [cargo-subcommand-args...]
# Environment overrides: REMOTE_TEST_WS, REMOTE_TEST_DIR, CARGO_BUILD_JOBS.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WS="${REMOTE_TEST_WS:-coder.bm-fu}"
REMOTE_DIR="${REMOTE_TEST_DIR:-p2int}"
JOBS="${CARGO_BUILD_JOBS:-4}"

echo "==> [1/3] transfer bitmagnet-rs + test fixtures -> ${WS}:${REMOTE_DIR}/"
ssh "$WS" "mkdir -p ~/${REMOTE_DIR}/bitmagnet-rs && rm -rf ~/${REMOTE_DIR}/bitmagnet-rs/crates ~/${REMOTE_DIR}/bitmagnet-rs/Cargo.toml ~/${REMOTE_DIR}/bitmagnet-rs/Cargo.lock ~/${REMOTE_DIR}/testdata"
tar czf - -C "$REPO_ROOT" \
  --exclude='.git' --exclude='.jj' --exclude='target' --exclude='node_modules' \
  bitmagnet-rs \
  | ssh "$WS" "tar xzf - -C ~/${REMOTE_DIR}/"
if [ -d "$REPO_ROOT/testdata" ]; then
  tar czf - -C "$REPO_ROOT" testdata \
    | ssh "$WS" "tar xzf - -C ~/${REMOTE_DIR}/"
fi

echo "==> [2/3] ensure Rust toolchain + protoc"
ssh "$WS" 'bash -s' <<'REMOTE'
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
if ! command -v rustc >/dev/null 2>&1; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal >/dev/null
  export PATH="$HOME/.cargo/bin:$PATH"
fi
rustup component add rustfmt clippy >/dev/null 2>&1 || true
if ! command -v protoc >/dev/null 2>&1; then
  if command -v sudo >/dev/null 2>&1 && sudo -n apt-get --version >/dev/null 2>&1; then
    sudo apt-get update -qq
    sudo apt-get install -y -qq protobuf-compiler || true
  fi
fi
if ! command -v protoc >/dev/null 2>&1; then
  mkdir -p "$HOME/.local/bin" /tmp/protoc-dl
  cd /tmp/protoc-dl
  case "$(uname -m)" in
    aarch64|arm64) protoc_arch=aarch_64 ;;
    *) protoc_arch=x86_64 ;;
  esac
  curl -sSL -o protoc.zip "https://github.com/protocolbuffers/protobuf/releases/download/v27.3/protoc-27.3-linux-${protoc_arch}.zip"
  unzip -oq protoc.zip -d "$HOME/.local"
  chmod +x "$HOME/.local/bin/protoc"
fi
echo "    rustc: $(rustc --version)"
echo "    cargo: $(cargo --version)"
echo "    protoc: $(protoc --version 2>&1)"
REMOTE

echo "==> [3/3] run cargo gates on ${WS} (CARGO_BUILD_JOBS=${JOBS})"
CARGO_ARGS="${*:-}"
ssh "$WS" 'bash -s' <<REMOTE
set -uo pipefail
export PATH="\$HOME/.cargo/bin:\$HOME/.local/bin:\$PATH"
export CARGO_BUILD_JOBS="${JOBS}"
export CARGO_TERM_COLOR=never
cd ~/${REMOTE_DIR}/bitmagnet-rs
: > ~/${REMOTE_DIR}/run.log
failed=0
run() {
  echo "--- \$* ---" | tee -a ~/${REMOTE_DIR}/run.log
  "\$@" >>~/${REMOTE_DIR}/run.log 2>&1
  rc=\$?
  echo "@@RC=\${rc}" | tee -a ~/${REMOTE_DIR}/run.log
  if [ "\${rc}" -ne 0 ]; then failed=1; fi
}
if [ -n "${CARGO_ARGS}" ]; then
  run cargo ${CARGO_ARGS}
else
  run cargo fmt --all --check
  run cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings
  run cargo test --workspace --all-features
fi
echo "@@ALLDONE"
exit "\${failed}"
REMOTE
