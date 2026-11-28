# OHLCV persistence bug

Code fix status: the never-overwrite gzip change, the regression test, the $20k candidate floor, venue tags, the pump.fun feed, and the wallet logger are in the campaign PR. The `data` tip has not been restored. `STOP` is still on `data`. Do not restart from this note.

Collection is stopped. Do not restart it, and do not squash, force-push, or rewrite the `data` branch. The candles missing from the tip still exist in earlier commits on that branch. Those commits are the only in-repo copy.

Counts below were checked against commit `442694be` (2026-11-25 22:23 UTC). The branch tip has since moved, by normal appends only, to `600a87f1` (the STOP checkpoint). The destroyed `ohlcv.csv.gz` files are the same blobs at both commits. `main` still has the bug; this note does not change code.

## Symptom

At `442694be`, 529 pools had a finished `ohlcv24` follow-up (`mutable/followups.jsonl`, `label=ohlcv24`, `state=done`). The tip shards `shards/*/ohlcv.csv*` contain candles for 249 pools and 68,437 unique `(pool, timeframe, timestamp)` bars (72,160 raw rows).

Of those 529 pools:

- 281 have no candles left at the tip
- 248 still have at least one bar. A strict recount (every historical bar for that pool is still present) is 107 complete and 141 missing at least one bar. An earlier pass of the same 248 called 111 complete and 137 incomplete; the two cuts differ by four near-complete pools. One extra pool has candles at the tip and no finished `ohlcv24` row, which is why the file count is 249 rather than 248.

The union of every historical `ohlcv` blob reachable from `442694be` is 182,911 unique `(pool, timeframe, timestamp)` rows across 530 pools. 114,474 of those bars are absent from the tip. Candidate rows do reconcile: 657 unique pools in the tip shards and the same 657 across every historical `candidates.jsonl` blob.

## Evidence

`shards/2026-11-21/ohlcv.csv.gz` is 267,710 bytes (6,621 data rows) at `58a16631` and 776 bytes (14 data rows) at `442694be`. The largest committed gzip of the other destroyed days, and the tip size of the same path:

| path | largest gzip in history | tip at `442694be` |
|---|---:|---:|
| `shards/2026-11-21/ohlcv.csv.gz` | 267,710 B (6,621 rows), including `58a16631` | 776 B (14 rows) |
| `shards/2026-11-22/ohlcv.csv.gz` | 1,536,233 B (39,807 rows) | 520 B (10 rows) |
| `shards/2026-11-23/ohlcv.csv.gz` | 2,793,245 B (72,372 rows) | 33,316 B (714 rows) |

Nov 24 and Nov 25 were still inside the two-day plain-text window at the time of the count, so those `ohlcv.csv` files were not rewritten this way (9,356,706 B and 2,163,387 B at their largest).

## Root cause

Candle rows are sharded by the bar's own UTC date, not by the day of the export. `_export_unexported` calls `day_of(ts)` on the candle timestamp (`persist.py` lines 511–523) and appends with stem `ohlcv.csv` (`persist.py` line 603). A +24h or +48h pull therefore writes into shard directories that are already two or more days old.

`gzip_old_shards` (`persist.py` lines 239–268) gzips any plain file in a shard directory older than `GZIP_AFTER_DAYS` (2, line 20). The next run's +24h/+48h candles then have to land in a directory whose only `ohlcv` file is `ohlcv.csv.gz`.

The late-append sidecar never runs. `_current_part` (`persist.py` lines 170–189) skips every name ending in `.gz` (line 177), so it returns no path when the day has already been gzipped. `_append_bytes` then takes the `path is None` branch (lines 203–205) and creates a new small `ohlcv.csv`. The comment at lines 206–209 that would write `ohlcv-late.csv` requires `path.endswith(".gz")`. `_current_part` never returns such a path, so that branch is dead.

On the following run, `checkpoint` calls `gzip_old_shards` before exporting (`persist.py` lines 589–590). For the new small `ohlcv.csv` it sets `dst = src + ".gz"` (line 258) and `os.replace(tmp, dst)` (line 265). `os.replace` overwrites the existing complete `ohlcv.csv.gz`. `scripts/publish_data.sh` line 26 runs `git add -A` and commits the replacement.

The live database cannot repair this. After a successful export, `_export_unexported` deletes the exported `ohlcv` rows (`persist.py` lines 528–531, called with `delete_after=True` at line 603). The next process starts from shards plus `mutable/`, not from those deleted rows.

## Proposed fix

Not applied in this change.

- `gzip_old_shards` must not overwrite an existing `.gz`. A later plain file should become a new numbered part, for example `ohlcv-2.csv.gz`, never a replacement of `ohlcv.csv.gz`.
- Late appends into an already-gzipped day should also be new numbered plain files (`ohlcv-2.csv`, `ohlcv-3.csv`), not a fresh `ohlcv.csv` that the next gzip will promote onto the original name.
- Add a regression test that fails on current `main` and passes after the fix: gzip a day that already has `ohlcv.csv.gz`, append a late bar into that day, gzip again, and assert the original bars are still readable.
- Only after that test passes, restore the missing candles onto the `data` tip as one normal commit (parent = current tip). Do not squash, force-push, or rebase published `data` history. The recovered bars are the union of the historical blobs described above.

## Before any restart

Two gates, both still open:

1. The persistence fix and its regression test, then a normal commit that puts the recovered candles on the tip and checks that the tip matches that recovery.
2. A separate diff that adds a liquidity floor in `evaluate()` (`collector.py` lines 186–197). The only liquidity check today is line 195: `reserve <= 0` is `no_liquidity`. At `442694be`, 219 of 657 candidates had `reserve < $1`, and only 288 had `reserve >= $20,000`. The floor should be `reserve >= 20_000`. Pools that fail it must still be logged (the same `fail_reasons` path), not dropped from `pools_seen`.

The self-chaining bug (the runner does not export `GITHUB_TOKEN`, so every dispatch logged `no GitHub token` and returned success) was fixed in PR #3. That fix is not a reason to restart. `STOP` is on the `data` tip, and Collect must stay stopped until both gates above are done.

## Off-repo copy

A full recovered copy is outside this repository:

- `ohlcv_recovered.csv`
- archive `young-coin-dataset-2026-11-25.tar.gz`
- sha256 `a40d5ddd4eed9ef17a60c02189558453ddd2a169f6071fa5d8910c7b72d6f067`
