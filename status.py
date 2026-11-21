#!/usr/bin/env python3
"""One-screen summary of the young-coin collector.

Reads the data directory (shards + mutable snapshots). If a live SQLite file is
present it includes rows not yet checkpointed. Coverage and the born-during-coverage
cohort are computed from the downtime log plus every pools_seen row.
"""
import argparse, json, os, sqlite3, time, datetime as dt
import persist

STALE_S = 300  # checkpoint older than this counts as an open gap on the screen


def resolve_data(arg):
    if arg: return arg
    env = os.environ.get("DATA_DIR")
    if env: return env
    here = os.path.dirname(os.path.abspath(__file__))
    cand = os.path.join(here, "data")
    if os.path.isdir(cand): return cand
    return here


def iso(t):
    if not t: return "-"
    return dt.datetime.fromtimestamp(float(t), dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def load_json(path, default):
    try:
        with open(path) as fh: return json.load(fh)
    except Exception: return default


def open_live_db(data_dir):
    path = os.path.join(data_dir, ".run", "collector.db")
    if not os.path.exists(path): return None
    try:
        return sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except Exception:
        return None


def count_csv(data_dir, stem):
    n = 0
    for part in persist.iter_part_files(data_dir, stem):
        for _ in persist.iter_csv(part):
            n += 1
    return n


def count_jsonl(data_dir, stem):
    n = 0
    for part in persist.iter_part_files(data_dir, stem):
        for _ in persist.iter_jsonl(part):
            n += 1
    return n


def collector_running(data_dir):
    """True when this machine holds the collector lock. A data-branch checkout has no lock."""
    path = os.path.join(data_dir, ".run", "collector.lock")
    if not os.path.exists(path):
        return False
    try:
        import fcntl
        fh = open(path, "a+")
    except OSError:
        return False
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    else:
        fcntl.flock(fh, fcntl.LOCK_UN)
        return False
    finally:
        fh.close()


def heartbeat_info(data_dir):
    run_hb = load_json(os.path.join(data_dir, ".run", "heartbeat.json"), {})
    pub = load_json(os.path.join(data_dir, "heartbeat.json"), {})
    now = time.time()
    run_age = (now - float(run_hb["ts"])) if run_hb.get("ts") else None
    pub_age = (now - float(pub["ts"])) if pub.get("ts") else None
    alive = collector_running(data_dir)
    return run_hb, pub, run_age, pub_age, alive


def downtime_intervals(data_dir, db, pub, pub_age, alive):
    rows = persist.load_downtime(data_dir, db)
    intervals = []
    seen = set()
    logged = 0
    for obj in rows:
        try:
            a, b = float(obj["start_ts"]), float(obj["end_ts"])
        except Exception:
            continue
        key = (round(a, 3), round(b, 3), obj.get("kind"))
        if key in seen: continue
        seen.add(key)
        logged += 1
        intervals.append((a, b))
    last_alive = None
    if pub.get("last_alive_ts"):
        try: last_alive = float(pub["last_alive_ts"])
        except Exception: last_alive = None
    if last_alive is None and pub.get("ts"):
        try: last_alive = float(pub["ts"])
        except Exception: last_alive = None
    # Lock present but not held: this machine's collector has stopped, so the
    # tail after the last checkpoint is already a gap. A data-branch checkout has
    # no lock; there, only a checkpoint older than STALE_S is treated as down,
    # because a live Actions job pushes about every 3 minutes.
    lock_path = os.path.join(data_dir, ".run", "collector.lock")
    local_stopped = os.path.exists(lock_path) and not alive
    open_tail = bool(last_alive) and not alive and (local_stopped or pub_age is None or pub_age > STALE_S)
    if open_tail:
        intervals.append((last_alive, time.time()))
    return persist.merge_intervals(intervals), logged, last_alive, open_tail


def scan_pools(data_dir, db, ckpt_ts, intervals, origin):
    """Return pools_seen, cohort, cohort_candidates. Shard order keeps the earliest row."""
    candidates = set()
    for part in persist.iter_part_files(data_dir, "candidates.jsonl"):
        for obj in persist.iter_jsonl(part):
            if obj.get("pool"): candidates.add(obj["pool"])
    if db is not None and ckpt_ts:
        try:
            for (pool,) in db.execute("SELECT pool FROM candidates WHERE event_ts > ?", (ckpt_ts,)):
                candidates.add(pool)
        except Exception:
            pass
    elif db is not None:
        try:
            for (pool,) in db.execute("SELECT pool FROM candidates"):
                candidates.add(pool)
        except Exception:
            pass
    seen = set()
    pools_n = cohort_n = cohort_c = 0
    newest = None

    def consider(pool, created, first, source):
        nonlocal pools_n, cohort_n, cohort_c, newest
        if not pool or pool in seen: return
        seen.add(pool)
        pools_n += 1
        if first and (newest is None or first > newest): newest = first
        if persist.is_cohort_pool(created, first, source, intervals, origin):
            cohort_n += 1
            if pool in candidates: cohort_c += 1

    for part in persist.iter_part_files(data_dir, "pools_seen.csv"):
        for row in persist.iter_csv(part):
            consider(row.get("pool"), persist._blank_float(row.get("created_at")),
                     persist._blank_float(row.get("first_seen")), row.get("first_source"))
    if db is not None:
        try:
            q = "SELECT pool, created_at, first_seen, first_source FROM pools_seen"
            args = ()
            if ckpt_ts:
                q += " WHERE first_seen > ?"
                args = (ckpt_ts,)
            for pool, created, first, source in db.execute(q, args):
                consider(pool, created, first, source)
        except Exception:
            pass
    return pools_n, cohort_n, cohort_c, len(candidates), newest


def result_counts(data_dir, db):
    counts = {}
    late = 0
    def add(status, n=1, is_late=0):
        nonlocal late
        if status: counts[status] = counts.get(status, 0) + n
        if is_late: late += 1
    for part in persist.iter_part_files(data_dir, "followup_results.jsonl"):
        for obj in persist.iter_jsonl(part):
            add(obj.get("status"), 1, int(obj.get("late") or 0))
    if db is not None:
        try:
            for status, n in db.execute("SELECT status, COUNT(*) FROM followup_results GROUP BY status"):
                add(status, n, 0)
            late += db.execute("SELECT COUNT(*) FROM followup_results WHERE late=1").fetchone()[0]
        except Exception:
            pass
    return counts, late


def followup_counts(data_dir, db):
    if db is not None:
        try:
            rows = db.execute("SELECT label||':'||state, COUNT(*) FROM followups GROUP BY 1").fetchall()
            if rows: return dict(rows)
        except Exception:
            pass
    counts = {}
    path = os.path.join(data_dir, "mutable", "followups.jsonl")
    if os.path.exists(path):
        for obj in persist.iter_jsonl(path):
            k = f"{obj.get('label')}:{obj.get('state')}"
            counts[k] = counts.get(k, 0) + 1
    return counts


def watch_counts(data_dir, db):
    if db is not None:
        try:
            rows = db.execute("SELECT status, COUNT(*) FROM watch_state GROUP BY status").fetchall()
            if rows: return dict(rows)
        except Exception:
            pass
    counts = {}
    path = os.path.join(data_dir, "mutable", "watch_state.jsonl")
    if os.path.exists(path):
        for obj in persist.iter_jsonl(path):
            k = obj.get("status") or "?"
            counts[k] = counts.get(k, 0) + 1
    return counts


def api_today(data_dir, db):
    day = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    rows = []
    if db is not None:
        try:
            rows = db.execute("SELECT endpoint, calls, n429, nerr FROM api_calls WHERE day=?", (day,)).fetchall()
        except Exception:
            rows = []
    if not rows:
        path = os.path.join(data_dir, "mutable", "api_calls.jsonl")
        if os.path.exists(path):
            for obj in persist.iter_jsonl(path):
                if obj.get("day") == day:
                    rows.append((obj.get("endpoint"), obj.get("calls") or 0, obj.get("n429") or 0, obj.get("nerr") or 0))
    calls = sum(r[1] for r in rows); n429 = sum(r[2] for r in rows); nerr = sum(r[3] for r in rows)
    return day, calls, n429, nerr, {r[0]: r[1] for r in rows}


def recent_warnings(data_dir, db, limit=4):
    items = []
    if db is not None:
        try:
            items.extend(db.execute("SELECT ts, level, msg FROM run_log WHERE level!='INFO' ORDER BY ts").fetchall())
        except Exception:
            pass
    for part in persist.iter_part_files(data_dir, "run_log.jsonl"):
        for obj in persist.iter_jsonl(part):
            if obj.get("level") and obj.get("level") != "INFO":
                items.append((obj.get("ts"), obj.get("level"), obj.get("msg")))
    items.sort(key=lambda r: (r[0] or 0), reverse=True)
    out, seen = [], set()
    for ts, level, msg in items:
        key = (round(float(ts or 0), 3), msg)
        if key in seen: continue
        seen.add(key)
        out.append((ts, level, msg))
        if len(out) >= limit: break
    return out


def dir_bytes(data_dir):
    total = 0
    for root, _dirs, files in os.walk(data_dir):
        if ".run" in root.split(os.sep): continue
        for fn in files:
            try: total += os.path.getsize(os.path.join(root, fn))
            except OSError: pass
    return total


def summarize(data_dir):
    run_hb, pub, run_age, pub_age, alive = heartbeat_info(data_dir)
    meta = load_json(os.path.join(data_dir, "mutable", "meta.json"), {})
    ckpt = load_json(os.path.join(data_dir, "mutable", "last_checkpoint.json"), {})
    origin = persist._blank_float(meta.get("coverage_origin_ts")) or persist._blank_float(pub.get("coverage_origin_ts"))
    ckpt_ts = persist._blank_float(ckpt.get("ts"))
    db = open_live_db(data_dir)
    try:
        intervals, gap_n, last_alive, open_tail = downtime_intervals(data_dir, db, pub, pub_age, alive)
        now = time.time()
        frac, span, down = persist.coverage(origin, now, intervals)
        day_origin = max(origin, now - 86400) if origin else now - 86400
        frac24, span24, down24 = persist.coverage(day_origin, now, intervals) if origin else (0.0, 0.0, 0.0)
        pools_n, cohort_n, cohort_c, cand_n, newest = scan_pools(data_dir, db, ckpt_ts, intervals, origin)
        statuses, late = result_counts(data_dir, db)
        snaps = count_jsonl(data_dir, "watch_snapshots.jsonl")
        if db is not None:
            try: snaps += db.execute("SELECT COUNT(*) FROM watch_snapshots").fetchone()[0]
            except Exception: pass
        ohlcv_n = count_csv(data_dir, "ohlcv.csv")
        if db is not None:
            try: ohlcv_n += db.execute("SELECT COUNT(*) FROM ohlcv").fetchone()[0]
            except Exception: pass
        return {
            "alive": alive, "run_age": run_age, "pub_age": pub_age, "pub": pub, "run_hb": run_hb,
            "origin": origin, "frac": frac, "span": span, "down": down, "gaps": gap_n,
            "open_tail": open_tail,
            "frac24": frac24, "down24": down24, "pools": pools_n, "cohort": cohort_n,
            "cohort_candidates": cohort_c, "candidates": cand_n, "newest_pool": newest,
            "followups": followup_counts(data_dir, db), "statuses": statuses, "late": late,
            "snapshots": snaps, "ohlcv": ohlcv_n, "watch": watch_counts(data_dir, db),
            "api": api_today(data_dir, db), "warnings": recent_warnings(data_dir, db),
            "np_gaps": meta.get("np_gaps") or "0", "bytes": dir_bytes(data_dir),
            "runs": meta.get("runs") or "0", "last_alive": last_alive, "intervals": intervals,
        }
    finally:
        if db is not None:
            db.close()


def fmt_pct(frac):
    return f"{frac * 100:.1f}%"


def main():
    ap = argparse.ArgumentParser(description="Summarize young-coin collector data")
    ap.add_argument("--data", help="data directory (default DATA_DIR or ./data)")
    args = ap.parse_args()
    data = resolve_data(args.data)
    s = summarize(data)
    print(f"data={data}")
    run_age = "-" if s["run_age"] is None else f"{s['run_age']:.0f}s"
    pub_age = "-" if s["pub_age"] is None else f"{s['pub_age']:.0f}s"
    print(f"collector_alive={s['alive']}  run_heartbeat_age={run_age}  checkpoint_age={pub_age}  runs={s['runs']}")
    if s["pub"].get("iso"):
        print(f"last_checkpoint={s['pub'].get('iso')}  pools_total_at_checkpoint={s['pub'].get('pools_total')}  "
              f"pending_followups={s['pub'].get('followups_pending')}")
    if s["origin"]:
        tail = ", plus the open tail since the last checkpoint" if s["open_tail"] else ""
        glabel = "gap" if s["gaps"] == 1 else "gaps"
        print(f"coverage since {iso(s['origin'])}: {fmt_pct(s['frac'])}  "
              f"({(s['span']-s['down'])/3600:.2f}h covered / {s['span']/3600:.2f}h span, "
              f"downtime {s['down']/3600:.2f}h in {s['gaps']} logged {glabel}{tail})")
        print(f"coverage last 24h: {fmt_pct(s['frac24'])}  (downtime {s['down24']/3600:.2f}h)")
    else:
        print("coverage: no origin yet (collector has not checkpointed)")
    hrs = (time.time() - s["origin"]) / 3600 if s["origin"] else 0
    rate = (s["candidates"] / hrs * 24) if hrs > 0 else 0
    print(f"born-during-coverage cohort: {s['cohort']} pools ({s['cohort_candidates']} became candidates)  "
          f"rule=first_source new_pools, born outside downtime, first_seen within {persist.COHORT_SEEN_WITHIN_S//60}m")
    print(f"pools_seen={s['pools']}  candidates={s['candidates']}  rate={rate:.2f}/day  "
          f"near-miss/snapshots={s['snapshots']}  ohlcv_rows={s['ohlcv']}")
    print(f"watchlist={s['watch'] or '-'}  followups={s['followups'] or '-'}")
    print(f"result statuses={s['statuses'] or '-'}  late={s['late']}")
    day, calls, n429, nerr, by_ep = s["api"]
    print(f"API calls today(UTC {day})={calls} 429s={n429} errors={nerr}  by endpoint={by_ep or '-'}")
    print(f"new_pools page-gap events={s['np_gaps']}  data_tree={s['bytes']/1e6:.1f}MB (excludes .run)")
    for ts, level, msg in s["warnings"]:
        print(f"  {iso(ts)} {level} {str(msg)[:140]}")


if __name__ == "__main__":
    main()
