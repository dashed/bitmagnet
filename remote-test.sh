#!/usr/bin/env bash
# remote-test.sh — the check command for the react-result-info lane (bm-rinfo).
#
# The local machine is CPU/RAM constrained and the webui-react vitest suite
# flakes under load (5s per-test timeouts trip on unrelated dashboard / command
# palette / URL-params tests). So the full JS suite runs on the bm-rinfo Coder
# workspace, which has node + pnpm and a quiet CPU. The Go gql checks are fast
# and deterministic, so they run locally (the workspace has no Go toolchain).
#
# SSH reaches the workspace through the coder ProxyCommand in ~/.ssh/config,
# authenticated by the already-logged-in local coder CLI — no token plumbing.
#
# Usage: ./remote-test.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WS="coder.bm-rinfo"
REMOTE_DIR="rinfo"

echo "==> [1/3] Go: gqlgen build + gql tests (local; workspace has no Go)"
( cd "$REPO_ROOT" && go build ./internal/gql/... && go test ./internal/gql/... )

echo "==> [2/3] rsync worktree -> ${WS}:${REMOTE_DIR}/"
ssh "$WS" "mkdir -p ~/${REMOTE_DIR}"
rsync -az --delete \
  --exclude '.git' --exclude '.jj' --exclude 'target' --exclude 'node_modules' \
  --exclude 'webui-react/dist' \
  -e ssh "$REPO_ROOT/" "${WS}:${REMOTE_DIR}/"

echo "==> [3/3] webui-react: install + codegen + typecheck + lint + test + build on ${WS}"
ssh "$WS" 'bash -lc "set -euo pipefail
cd ~/'"${REMOTE_DIR}"'/webui-react
pnpm install --frozen-lockfile
pnpm run codegen
pnpm run typecheck
pnpm run lint
pnpm run test
pnpm run build"'

echo "==> all checks passed"
