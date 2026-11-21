#!/usr/bin/env bash
# Check out the orphan `data` branch into $DATA_DIR. Creates the branch on first use.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
DATA_DIR="${DATA_DIR:-$ROOT/data-wt}"

if [[ -d "$DATA_DIR" ]] && git -C "$DATA_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "data worktree already at $DATA_DIR"
  exit 0
fi

mkdir -p "$(dirname "$DATA_DIR")"
# ls-remote exits 0 with empty output when the branch is absent, and non-zero on network errors.
# A failed fetch must not be treated as "create a new empty branch" or we would orphan real data.
if ! remote_data="$(git ls-remote --heads origin refs/heads/data)"; then
  echo "cannot list origin refs; not creating a data branch" >&2
  exit 1
fi
if [[ -n "$remote_data" ]]; then
  git fetch --depth=1 origin data
  git worktree add --detach "$DATA_DIR" FETCH_HEAD
else
  echo "no data branch yet; creating an orphan branch"
  git worktree add --orphan -b data "$DATA_DIR"
fi
echo "DATA_DIR=$DATA_DIR"
