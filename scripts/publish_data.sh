#!/usr/bin/env bash
# Commit the data directory and push it to origin's data branch.
# Safe to run repeatedly. No-op when the directory is not a git checkout.
set -euo pipefail
DATA_DIR="${1:-${DATA_DIR:-}}"
if [[ -z "${DATA_DIR}" || ! -d "${DATA_DIR}" ]]; then
  echo "publish: no data dir"
  exit 0
fi
if ! git -C "$DATA_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "publish: $DATA_DIR is not a git checkout; leaving files on disk"
  exit 0
fi

cd "$DATA_DIR"
# A STOP file pushed by a person should survive our next commit.
if git fetch --depth=20 origin data >/dev/null 2>&1; then
  if git cat-file -e FETCH_HEAD:STOP >/dev/null 2>&1; then
    git checkout FETCH_HEAD -- STOP 2>/dev/null || true
    echo "publish: STOP is on the data branch"
  fi
fi

mkdir -p .run
# Same lock the collector holds while it appends shards, so git add never reads a half-written line.
flock .run/export.lock git add -A
if git diff --cached --quiet; then
  echo "publish: nothing to commit"
  exit 0
fi

msg="data: checkpoint $(date -u +%Y-%m-%dT%H:%M:%SZ)"
if [[ -f mutable/last_checkpoint.json ]]; then
  extra="$(python3 -c 'import json; i=json.load(open("mutable/last_checkpoint.json")); print("new=%s pools=%s candidates=%s pending_followups=%s" % (i.get("counts"), i.get("pools_total"), i.get("candidates_total"), i.get("followups_pending")))' 2>/dev/null || true)"
  if [[ -n "$extra" ]]; then msg="$msg $extra"; fi
fi

git -c user.name="github-actions[bot]" \
    -c user.email="41898282+github-actions[bot]@users.noreply.github.com" \
    commit -m "$msg"

push_data() {
  git push origin HEAD:data
}
if ! push_data; then
  echo "publish: push rejected; rebasing onto origin/data"
  git fetch --depth=50 origin data
  git rebase FETCH_HEAD
  push_data
fi
echo "publish: pushed data"
