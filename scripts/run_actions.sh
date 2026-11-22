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

# Queue the next run only after this one has published. No run_seconds input:
# the successor uses the workflow default (4 hours), even if this run was short.
# A missing token or a non-204 from the API fails the job. Cancelling the job
# does not chain (that is how a run is stopped).
dispatch_successor() {
  if stopped; then
    echo "not chaining; STOP set"
    return 0
  fi
  local ref="${GITHUB_REF_NAME:-main}"
  local token_present=no
  if [[ -n "${GH_TOKEN:-}${GITHUB_TOKEN:-}" ]]; then token_present=yes; fi
  echo "dispatch successor event=${GITHUB_EVENT_NAME:-local} ref=$ref repo=${GITHUB_REPOSITORY:-} token_present=$token_present"
  if [[ -z "${GITHUB_REPOSITORY:-}" ]]; then
    echo "::error::GITHUB_REPOSITORY is empty; cannot queue the next run"
    return 1
  fi
  if [[ "$token_present" != yes ]]; then
    echo "::error::GH_TOKEN is empty. The workflow must pass github.token (the runner does not export it)."
    return 1
  fi
  export GH_TOKEN="${GH_TOKEN:-$GITHUB_TOKEN}"
  local out rc
  set +e
  out=$(gh api --method POST \
    -H "Accept: application/vnd.github+json" \
    "repos/${GITHUB_REPOSITORY}/actions/workflows/collect.yml/dispatches" \
    -f "ref=${ref}" 2>&1)
  rc=$?
  set -e
  if [[ "$rc" -ne 0 ]]; then
    echo "::error::workflow_dispatch failed (exit ${rc}): ${out}"
    return "$rc"
  fi
  echo "workflow_dispatch accepted for ref=${ref} (no inputs; successor collects for 4h)"
  return 0
}

echo "collector run_seconds=$RUN_SECONDS checkpoint_seconds=$CHECKPOINT_SECONDS data=$DATA_DIR event=${GITHUB_EVENT_NAME:-local}"
python3 "$ROOT/collector.py" &
CPID=$!
rc=0
CANCELLED=0

publish() {
  bash "$ROOT/scripts/publish_data.sh" "$DATA_DIR" || echo "publish failed"
}

on_term() {
  CANCELLED=1
  echo "SIGTERM; will not queue a successor"
  kill -TERM "$CPID" 2>/dev/null || true
}
cleanup() {
  set +e
  trap - TERM
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
  if [[ "$CANCELLED" -eq 1 ]]; then
    echo "job cancelled; not chaining"
    exit "$rc"
  fi
  if stopped; then
    echo "not chaining; STOP set"
    exit 0
  fi
  if ! dispatch_successor; then
    echo "::error::collect finished but the next run was not queued"
    exit 1
  fi
  exit "$rc"
}
trap on_term TERM
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
done
