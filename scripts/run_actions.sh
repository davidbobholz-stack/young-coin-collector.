#!/usr/bin/env bash
# Run one bounded collector pass, publish shards every few minutes, and queue the next run.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export DATA_DIR="${DATA_DIR:-$ROOT/data-wt}"
export PYTHONUNBUFFERED=1

if ! git -C "$DATA_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  bash "$ROOT/scripts/prepare_data_worktree.sh"
fi
mkdir -p "$DATA_DIR/.run"

RUN_SECONDS="${RUN_SECONDS:-14400}"
CHECKPOINT_SECONDS="${CHECKPOINT_SECONDS:-180}"
if [[ -z "$RUN_SECONDS" ]]; then RUN_SECONDS=14400; fi
if [[ -z "$CHECKPOINT_SECONDS" ]]; then CHECKPOINT_SECONDS=180; fi
# workflow_dispatch is the only event with inputs. A push or schedule must
# not depend on github.event.inputs; a missing or non-numeric value is 4 hours.
if ! [[ "$RUN_SECONDS" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  RUN_SECONDS=14400
fi
read -r RUN_SECONDS CHECKPOINT_SECONDS < <(python3 -c "
rs=float('''${RUN_SECONDS}''' or 14400)
cs=float('''${CHECKPOINT_SECONDS}''' or 180)
rs=min(max(rs, 60), 16200)
cs=min(max(cs, 30), 600)
print(int(rs), int(cs))
")
export RUN_SECONDS CHECKPOINT_SECONDS

stopped() {
  [[ -f "$DATA_DIR/STOP" || "${COLLECTOR_STOP:-}" == "1" ]]
}

if stopped; then
  echo "STOP set; not starting and not chaining"
  exit 0
fi

dispatch_successor() {
  if stopped; then
    echo "not chaining; STOP set"
    return 0
  fi
  if [[ -z "${GITHUB_REPOSITORY:-}" || -z "${GITHUB_TOKEN:-}${GH_TOKEN:-}" ]]; then
    echo "no GitHub token; skip successor dispatch"
    return 0
  fi
  # Push and schedule have no inputs; the ref is the branch the event ran on
  # (main for both of those). A manual run continues on the ref it was started from.
  local ref="${GITHUB_REF_NAME:-main}"
  echo "queue next collect run (event=${GITHUB_EVENT_NAME:-local} ref=$ref)"
  # workflow_dispatch is allowed from GITHUB_TOKEN; a queued run waits on the concurrency group.
  gh workflow run collect.yml --repo "$GITHUB_REPOSITORY" --ref "$ref" \
    || echo "dispatch failed (the workflow must exist on the default branch)"
}

echo "collector run_seconds=$RUN_SECONDS checkpoint_seconds=$CHECKPOINT_SECONDS data=$DATA_DIR"
python3 "$ROOT/collector.py" &
CPID=$!

dispatch_successor || true
next_dispatch=$(( $(date +%s) + 900 ))
rc=0

publish() {
  bash "$ROOT/scripts/publish_data.sh" "$DATA_DIR" || echo "publish failed"
}

cleanup() {
  set +e
  if kill -0 "$CPID" 2>/dev/null; then
    kill -TERM "$CPID" 2>/dev/null || true
    for _ in $(seq 1 45); do
      kill -0 "$CPID" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$CPID" 2>/dev/null; then
      kill -KILL "$CPID" 2>/dev/null || true
    fi
  fi
  wait "$CPID"
  rc=$?
  publish
  exit "$rc"
}
trap cleanup EXIT

last_ready=0
while kill -0 "$CPID" 2>/dev/null; do
  sleep 5
  if stopped; then
    echo "STOP observed; shutting collector down"
    kill -TERM "$CPID" 2>/dev/null || true
  fi
  ready="$DATA_DIR/checkpoint.ready"
  if [[ -f "$ready" ]]; then
    m="$(stat -c %Y "$ready" 2>/dev/null || echo 0)"
    if [[ "$m" -gt "$last_ready" ]]; then
      publish
      last_ready="$m"
    fi
  fi
  hb="$DATA_DIR/.run/heartbeat.json"
  if [[ -f "$hb" ]]; then
    hb_mtime="$(stat -c %Y "$hb")"
    age=$(( $(date +%s) - hb_mtime ))
    if [[ "$age" -gt 300 ]]; then
      echo "watchdog: heartbeat ${age}s stale; killing collector"
      kill -TERM "$CPID" 2>/dev/null || true
    fi
  fi
  now_s="$(date +%s)"
  if [[ "$now_s" -ge "$next_dispatch" ]]; then
    dispatch_successor || true
    next_dispatch=$((now_s + 900))
  fi
done
