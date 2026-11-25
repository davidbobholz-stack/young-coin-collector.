# Young-coin forward collector (DATA ONLY)

Collection is stopped. Do NOT restart Collect until the OHLCV persistence bug in COLLECTOR_BUG.md is fixed and its regression test passes. Do NOT squash, force-push or rewrite the `data` branch: its history is the only in-repo copy of candles missing from the tip.

Forward research data for young Solana memecoin pools. The collector polls the free public [GeckoTerminal](https://api.geckoterminal.com) API, records pools that pass a fixed filter, and schedules follow-up snapshots. It does not trade, it does not hold a wallet, and it does not use API keys, secrets, or paid services.

The filter, the discovery cadence, and the follow-up schedule are the ones this collector was written with. GitHub Actions is only the process that keeps it running while the machine it was written on sleeps.

## Run shape

GitHub will not run one job forever (the job cap is 6 hours) and cron is allowed to fire late or not at all. Coverage is a chain, with cron as the spare:

1. **Each job collects for 4 hours** (`RUN_SECONDS=14400`), inside a job timeout of 6 hours. A manual run can pass a shorter `run_seconds` (60–16200).
2. **When a job finishes it queues the next one** with `workflow_dispatch`, using the job token passed in as `GH_TOKEN` (`actions: write`). The call is at the end, after the last data push, and a failure fails the job instead of being skipped. The concurrency group `young-coin-collector` allows one running job and one waiting job, and it never cancels the running job. When this job exits, the waiting job starts. The gap is runner startup, usually one to three minutes, not the cron interval. A cancelled job does not queue another. The successor always collects for 4 hours; a short manual `run_seconds` applies only to that one run.
3. **Cron at :08 and :38** is only the spare. If the chain breaks (dispatch failed, queued run dropped), the next cron starts a new one. GitHub often delays cron, worst at the top of the hour, which is why these minutes are offset. A cron fire while a job is running just replaces the waiting successor; it does not start a second collector.
4. **A push to `main` that changes `.github/workflows/collect.yml` or `collector.py`** also starts a run. That is how the chain is kicked off when cron is late and nobody can press the button: merging that workflow change is itself the push. The same concurrency group applies, so the push waits behind a job that is already running instead of starting a second collector. Pushes to other branches, and pushes that do not touch those two paths (including the `data` branch checkpoints), do not start a run.
5. **Every 3 minutes** the job appends new rows to the `data` branch and pushes. A kill loses at most that interval, plus a push that had not finished. On SIGTERM (cancel, timeout, watchdog) the process checkpoints once more before it exits.

Overlapping runners are rejected twice: the concurrency group, and a lock file so a second local process exits.

The in-job watchdog matches the old one: if the heartbeat file is not touched for 5 minutes, the collector is killed and the successor takes over. Follow-ups that came due during a gap run on the next start and are stored with `late=1` and `late_min`. Prices at the exact due time can still be read from the stored 1-minute and 5-minute candles.

### Actions minutes

A public repository's `ubuntu-latest` minutes are free. This uses about **1,400–1,500 minutes a day** (the runner is busy almost all day). Windows and macOS runners are billed even on public repos; the workflow does not use them.

If the repository is made private, the free quota is 2,000 minutes a month and this schedule would spend that in about a day and a half. Leave the repository public.

GitHub can still delay or refuse scheduled work. A collector that runs all day is heavier than a normal CI job. If runs start sitting in the queue for a long time, the downtime table shows it; there is no hidden backfill.

## What is durable

Code lives on `main`. Data lives on an orphan branch named `data` (do not merge it into `main`). Each Actions run shallow-fetches that branch, collects, and pushes.

```
data branch
  heartbeat.json                 last successful checkpoint
  mutable/meta.json              np_newest, coverage origin, clean-shutdown flag
  mutable/followups.jsonl        every follow-up, including done / rugged / delisted
  mutable/watch_state.jsonl      pools still being watched, plus candidates
  mutable/api_calls.jsonl        daily call counts
  shards/YYYY-MM-DD/
    pools_seen.csv               every pool ever observed (the denominator)
    candidates.jsonl             first time a pool passed the filter
    followup_results.jsonl       snapshot and OHLCV outcomes, append-only
    watch_snapshots.jsonl        near-misses (MC >= $50k) and misses
    watch_state_closed.jsonl     terminal watch rows (missing / aged out) after 48h
    ohlcv.csv                    1m and 5m candles fetched at +24h and +48h
    downtime.jsonl               every gap
    run_log.jsonl
```

Shards older than yesterday are gzipped (`.csv.gz` / `.jsonl.gz`) so the branch tip stays smaller. Today and yesterday stay plain text so a checkpoint can append. Files rotate at 40 MB, under GitHub's 100 MB file limit.

The live SQLite file (`.run/collector.db`) is throwaway. On startup it is rebuilt from `mutable/` plus the recent `pools_seen` window and the candidate history. Pool ids that have already been archived are kept in an on-disk index for that run, so a later trending hit cannot overwrite `first_seen`.

Rows that must never disappear — candidates, follow-ups, `rugged_suspect`, `illiquid`, `zero_liquidity`, `delisted`, `missing`, near-miss snapshots, candles — are append-only in the shards. Closing a watch row copies it into `watch_state_closed.jsonl` before it leaves the live snapshot.

`pools_seen` is the bulk of the growth: Solana creates on the order of tens of thousands of pools a day, and each is stored once. Expect on the order of **10–30 MB/day** at the branch tip after old days are gzipped, and a git pack of about that size because the files are append-only. Clone the data branch shallow:

```bash
git clone --depth 1 --branch data https://github.com/davidbobholz-stack/young-coin-collector. data
```

(The repository name ends with a period.) If the tip passes a couple of GB, squash the branch history with an orphan commit of the current tree. That keeps every shard at the tip and drops the checkpoint-by-checkpoint history. Do it when nobody is mid-run, or the running job's push will be rejected and retried.

## Gaps and the born-during-coverage cohort

Every interruption is a row in `downtime`:

| kind | when |
|---|---|
| `between_runs` | the previous job exited cleanly; this is the runner-startup gap |
| `killed` | the previous process did not mark a clean stop (timeout, cancel, crash). The row starts at the last checkpoint, so the uncheckpointed tail is included |
| `pause_in_process` | more than 5 minutes with no API call inside a running process |

A gap is logged whenever it is longer than 15 seconds. Short gaps count. `status.py` also treats "last checkpoint older than 5 minutes, and no live heartbeat" as an open gap, so a checkout made while the job is dead does not show 100% coverage. The next start writes the authoritative row.

`status.py` prints coverage since the first start and coverage over the last 24 hours.

**Cohort.** A pool is in the born-during-coverage cohort only if all of these hold:

- `first_source` is `new_pools` (it was seen on the new-pool feed, not discovered later on trending)
- `created_at` is after the collector's first start
- `created_at` and `first_seen` fall outside every downtime gap
- `first_seen` is within 20 minutes of `created_at` (it was still on the new-pool horizon, not found long after birth)

Pools born inside a gap, and pools that had already survived until a later poll, stay in `pools_seen` but are not in the cohort. Counting only the cohort is what avoids survivorship bias. `status.py` prints the cohort size and how many of those pools later became candidates.

A separate counter, `np_gaps`, increments when ten `new_pools` pages do not reach the previous poll's newest pool (the feed moved faster than 10 pages). That is not a downtime row: the missed pools were never observed, so they are simply absent. The cohort does not include them.

## Filter

Approximation of the GMGN screen this collector was built to match. It is not a trading rule.

| GMGN | Here |
|---|---|
| pair age 0.5–20h | GeckoTerminal `pool_created_at` of that pool, 0.5–20h at observation. A graduated pump.fun coin gets a new PumpSwap pool, so age is time since that pool, not since the mint |
| market cap ≥ $200k | `market_cap_usd`, falling back to `fdv_usd` |
| ATH MC ≥ $400k | max of market caps observed for the pool, and (only when MC is $200–400k, at most every 30 min) max 5-minute high since creation × MC/price |
| total fees ≥ 1 SOL | not on the free API. Proxy: 24h buys+sells ≥ 1000. Raw volume and txn counts are stored so the cut can be redone |
| (implicit) | reserve > 0; base token is not SOL, USDC, or USDT |

A **candidate** is the first time a pool passes all of the above. One row per pool. `first_for_token` marks the first pool of that token, so events and unique coins can be counted separately.

Discovery:

- `new_pools` pages 1–10 every 150s, stopping early once a page reaches the previous poll's newest pool
- `trending_pools` 5m/1h/6h/24h, pages 1–3 every 15 min and pages 1–10 hourly
- `pools_seen`: every pool observed, any age
- `watch_state`: pools under 20h with FDV ≥ $20k, or any trending pool under 20h. Bonding-curve pools below $20k are not watched; they cannot reach a $200k market cap without migrating to a new pool, and that new pool is collected on its own. Refresh via `/pools/multi` (30 per call): FDV ≥ $100k every 5 min, $30–100k every 20 min, otherwise every 2h. Stops at 20h (`aged_out`) or after 3 consecutive misses (`missing`)

`watch_snapshots` stores every observation of a watched pool with MC ≥ $50k, or a passing observation, with `fail_reasons`. Misses are stored as status `missing`.

## Follow-ups

For each candidate, snapshots at **+15m, +1h, +4h, +24h, +48h** (price, FDV/MC, reserve, volume, txns) go to `followup_results`. A pool missing from `/multi` is retried 3 times, 5 minutes apart, then fetched alone: HTTP 404 is `delisted`, anything else is `missing`.

Statuses: `ok`, `illiquid` (reserve < $5k), `rugged_suspect` (reserve < $1k, or price < 10% of the event price), `zero_liquidity`, `delisted`, `missing`, plus the late flag.

Candles: at +24h, 5-minute (1000 bars) and two 1-minute pulls; at +48h, 5-minute out to +48h. Results are `ok`, `missing` (empty), `candles_stopped` (last bar more than 3h before the window end), `delisted`, or `partial_error`. GeckoTerminal omits minutes with no trades; that is not a gap.

## Rate limit

At least 5 seconds between calls (at most 12 per minute). HTTP 429 waits 30s, then 60s, 120s, 240s. Other errors wait 10s, 20s, 30s, 40s. Daily counts are in `api_calls`. A busy watchlist is on the order of 8–12 calls per minute.

## 60 days of silence

GitHub disables a scheduled workflow after 60 days without repository activity. Activity includes commits on any branch. The checkpoint pushes to `data` are that activity, so a healthy collector keeps its own schedule enabled. The Actions bot's commits count.

If a schedule is ever disabled anyway (GitHub has changed this rule before), open the Actions tab, select **Collect**, and click **Enable workflow**. One commit on `main` also clears the inactivity clock.

## How to read the data

```bash
git clone --depth 1 https://github.com/davidbobholz-stack/young-coin-collector. code
git clone --depth 1 --branch data https://github.com/davidbobholz-stack/young-coin-collector. data
DATA_DIR="$PWD/data" python3 code/status.py
```

`status.py` prints coverage, the cohort, candidate and follow-up counts, result statuses (including rugged / delisted / illiquid / missing), today's API usage, and the latest warnings.

The shard files are the raw record. `pools_seen.csv` columns are `pool,token,quote,name,dex,created_at,first_seen,first_source,first_fdv,first_reserve,first_age_h` (unix seconds, UTC). JSONL files are one object per line with the same fields the SQLite tables use. OHLCV analysis should ignore a duplicate `(pool, tf, ts)` if a crash ever wrote a candle twice; the collector deletes candles from the live DB after a successful append, so a duplicate is rare.

Cohort membership is `persist.is_cohort_pool` in `persist.py`, which is what `status.py` calls.

## How to run it, and how to stop it

The workflow is `.github/workflows/collect.yml`. Cron runs only after the file is on the default branch, and on a new repository it is often late. A push to `main` of this workflow file or of `collector.py` starts a run immediately, using the same 4-hour length as the schedule (manual `run_seconds` exists only on **Run workflow**). Merging such a change is the push.

1. Merge a commit that touches `.github/workflows/collect.yml` or `collector.py`, or use Actions → **Collect** → **Run workflow**.
2. Confirm a commit appears on the `data` branch within a few minutes.

To stop:

1. Actions → **Collect** → **Disable workflow**, and cancel the in-progress run. That is the reliable stop. Disabling also drops the queued successor.
2. Or push an empty file named `STOP` to the `data` branch (and cancel the current run). The next run sees `STOP`, exits, and does not queue another. Delete `STOP` and enable the workflow to resume; the gap is logged.
3. Or set the repository variable `COLLECTOR_STOP` to `1`. Same effect, read at the start of the job.

A run that is already going will keep going until it is cancelled or it reaches its time limit. Cancel it if you want it to stop now; it will checkpoint on the way out.

Locally, with no token, the same process is a single bounded or endless loop and does not try to chain:

```bash
DATA_DIR=/path/to/data-checkout python3 collector.py
# or a finite pass:
RUN_SECONDS=600 CHECKPOINT_SECONDS=60 DATA_DIR=/path/to/data-checkout python3 collector.py
```

`RUN_SECONDS` unset means run until SIGTERM. Tests: `python3 tests/test_persist.py`.

## Known limits

- Pools that are created and gone between polls, or during a gap, or past the ten-page `new_pools` horizon, are not in the data.
- The fee cut is a proxy. ATH between candle checks is a lower bound. GMGN's pair age can differ from GeckoTerminal's pool age.
- The first poll after a cold start includes pools that already qualified; `event_ts` is the time we first saw them pass. The cohort cut (seen within 20 minutes of creation, and not during a gap) is what keeps those out of a survivorship-free count.
- Checkpoint history and the `pools_seen` shards grow with the chain. Shallow-clone `data`. Squash that branch's history if the repository gets large; do not delete shard files at the tip if you still want the full record.
- GeckoTerminal can 429 or go away. Backoff is automatic and the calls are counted. There is no second data source.
