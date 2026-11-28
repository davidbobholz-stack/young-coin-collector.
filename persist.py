#!/usr/bin/env python3
"""Durable storage for the young-coin collector.

The live SQLite file is ephemeral (it dies with the runner). What survives is
the git tree under DATA_DIR, which is published to the `data` branch:

- shards/YYYY-MM-DD/   append-only CSV/JSONL (the full record)
- mutable/             small rewritten snapshots needed to resume
- heartbeat.json       last successful checkpoint

A killed job loses only rows not yet checkpointed.
"""
import csv, fcntl, gzip, json, os, sqlite3, time, datetime as dt

SHARD_MAX_BYTES = 40 * 1024 * 1024
HYDRATE_POOLS_S = 36 * 3600
GAP_MIN_S = 15
# Pools first seen in new_pools this long after creation were not caught at birth.
COHORT_SEEN_WITHIN_S = 20 * 60
GZIP_AFTER_DAYS = 2

POOLS_COLUMNS = ["pool", "token", "quote", "name", "dex", "created_at", "first_seen",
                 "first_source", "first_fdv", "first_reserve", "first_age_h"]
OHLCV_COLUMNS = ["pool", "tf", "ts", "o", "h", "l", "c", "v"]
WALLET_COLUMNS = ["signature", "wallet", "kind", "mint", "token_amount", "sol_amount", "sender", "receiver",
                  "slot", "block_time", "observed_ts", "ts", "pool", "venue", "fee_bps", "reserve_usd",
                  "reading_age_s", "distinct_tokens_24h", "note"]
PUMP_EVENT_COLUMNS = ["ts", "observed_ts", "block_time", "kind", "mint", "symbol", "pool", "venue", "fee_bps",
                      "reserve_usd", "reading_age_s", "swap_signature", "swap_is_first", "slot", "note"]
POOL_SWAP_COLUMNS = ["signature", "pool", "mint", "venue", "block_time", "observed_ts", "ts", "slot", "price",
                     "sol_amount", "token_amount", "reserve_quote", "reserve_usd", "note"]
WATCH_COLUMNS = ["pool", "token", "created_at", "next_check", "last_check", "last_fdv",
                 "ath_mc_obs", "ath_mc_ohlcv", "ath_checked", "status", "misses", "is_candidate"]
FOLLOWUP_COLUMNS = ["pool", "label", "due_ts", "kind", "state", "attempts", "next_try", "done_ts"]
CANDIDATE_COLUMNS = ["pool", "token", "name", "dex", "first_for_token", "event_ts", "created_at",
                     "age_h", "price", "fdv", "mc", "reserve", "vol_m5", "vol_h1", "vol_h6",
                     "vol_h24", "tx_h1", "tx_h24", "ath_proxy", "ath_source", "source", "raw"]
SNAPSHOT_COLUMNS = ["ts", "pool", "source", "age_h", "price", "fdv", "mc", "reserve", "vol_m5",
                    "vol_h1", "vol_h6", "vol_h24", "tx_h1", "tx_h24", "ath_proxy", "passes",
                    "fail_reasons", "status", "venue"]
RESULT_COLUMNS = ["pool", "label", "due_ts", "ts", "late", "late_min", "attempts", "status",
                  "price", "fdv", "mc", "reserve", "vol_m5", "vol_h1", "vol_h24", "tx_h1",
                  "tx_h24", "note", "venue"]

_index = None
_known_pools = set()
_known_candidates = set()
_gzip_done = False


def reset():
    """Drop in-memory indexes. The next hydrate() rebuilds them."""
    global _index, _known_pools, _known_candidates, _gzip_done
    if _index is not None:
        try: _index.close()
        except Exception: pass
    _index = None
    _known_pools = set()
    _known_candidates = set()
    _gzip_done = False


def _clog(msg, level="INFO"):
    try:
        import collector
        if getattr(collector, "db", None) is not None and not getattr(collector, "_in_ckpt", [False])[0]:
            collector.log(msg, level)
            return
    except Exception:
        pass
    print(f"{dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} {level} {msg}", flush=True)


def meta_get(db, k):
    r = db.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return r[0] if r else None


def meta_set(db, k, v):
    db.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


def day_of(ts):
    return dt.datetime.fromtimestamp(float(ts), dt.timezone.utc).strftime("%Y-%m-%d")


def _num(x):
    if x is None or x == "": return ""
    try: return repr(float(x))
    except Exception: return ""


def _int(x):
    if x is None or x == "": return ""
    try: return str(int(x))
    except Exception: return ""


def _blank_float(x):
    if x is None or x == "": return None
    try: return float(x)
    except Exception: return None


def _blank_int(x):
    if x is None or x == "": return None
    try: return int(float(x))
    except Exception: return None


def atomic_write(path, text):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def atomic_write_if_changed(path, text):
    if os.path.exists(path):
        try:
            with open(path) as fh:
                if fh.read() == text: return False
        except Exception:
            pass
    atomic_write(path, text)
    return True


def _open_text(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", newline="")
    return open(path, "r", newline="")


def iter_part_files(data_dir, stem):
    """Yield shard parts for stem like 'pools_seen.csv' or 'downtime.jsonl', oldest first."""
    root = os.path.join(data_dir, "shards")
    if not os.path.isdir(root):
        return
    root_name, ext = os.path.splitext(stem)
    days = sorted(d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d)))
    for day in days:
        d = os.path.join(root, day)
        matched = []
        for fn in os.listdir(d):
            name = fn[:-3] if fn.endswith(".gz") else fn
            if name == stem:
                matched.append(fn); continue
            if name.endswith(ext) and name.startswith(root_name + "-"):
                mid = name[len(root_name) + 1:-len(ext)]
                if mid.isdigit():
                    matched.append(fn)
        for fn in sorted(matched):
            yield os.path.join(d, fn)


def iter_csv(path):
    with _open_text(path) as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if not row or not any(row.values()): continue
            yield row


def iter_jsonl(path):
    with _open_text(path) as fh:
        for line in fh:
            line = line.strip()
            if not line: continue
            try: yield json.loads(line)
            except Exception: continue


def _part_map(day_dir, stem):
    """Index of shard parts, including gzipped ones. n=0 is the unsuffixed file."""
    root_name, ext = os.path.splitext(stem)
    found = {}
    if not os.path.isdir(day_dir):
        return found
    for fn in os.listdir(day_dir):
        gz = fn.endswith(".gz")
        name = fn[:-3] if gz else fn
        if name == stem:
            n = 0
        elif name.endswith(ext) and name.startswith(root_name + "-"):
            mid = name[len(root_name) + 1:-len(ext)]
            if not mid.isdigit():
                continue
            n = int(mid)
        else:
            continue
        slot = found.setdefault(n, {"plain": None, "gz": None})
        slot["gz" if gz else "plain"] = os.path.join(day_dir, fn)
    return found


def _plain_name(day_dir, stem, n):
    root_name, ext = os.path.splitext(stem)
    fn = stem if n == 0 else f"{root_name}-{n}{ext}"
    return os.path.join(day_dir, fn)


def _append_bytes(data_dir, day, stem, header, payload):
    """Append payload to a plain part. Never open or replace a .gz.

    If the highest existing part is gzip-only, start the next numbered plain
    file (ohlcv-1.csv, ohlcv-2.csv, ...) instead of recreating ohlcv.csv.
    """
    day_dir = os.path.join(data_dir, "shards", day)
    os.makedirs(day_dir, exist_ok=True)
    parts = _part_map(day_dir, stem)
    new_file = True
    if not parts:
        path = _plain_name(day_dir, stem, 0)
    else:
        n = max(parts)
        plain = parts[n]["plain"]
        if plain and os.path.exists(plain) and os.path.getsize(plain) < SHARD_MAX_BYTES:
            path = plain
            new_file = os.path.getsize(plain) == 0
        else:
            n2 = n + 1
            while n2 in parts:
                n2 += 1
            path = _plain_name(day_dir, stem, n2)
            new_file = True
    with open(path, "a", newline="") as fh:
        if new_file or os.path.getsize(path) == 0:
            if header: fh.write(header)
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    return path


def append_csv(data_dir, day, stem, columns, values):
    buf = []
    class _W:
        def write(self, s): buf.append(s)
    w = csv.writer(_W(), lineterminator="\n")
    w.writerow(values)
    header = ""
    hbuf = []
    class _H:
        def write(self, s): hbuf.append(s)
    csv.writer(_H(), lineterminator="\n").writerow(columns)
    header = "".join(hbuf)
    _append_bytes(data_dir, day, stem, header, "".join(buf))


def append_jsonl(data_dir, day, stem, obj):
    line = json.dumps(obj, separators=(",", ":"), sort_keys=True, default=str) + "\n"
    _append_bytes(data_dir, day, stem, "", line)


def gzip_old_shards(data_dir, keep_days=GZIP_AFTER_DAYS):
    """Gzip shard files older than keep_days so the data-branch tip stays smaller.

    Today and yesterday stay plain so a late append never rewrites a gzip member.
    """
    root = os.path.join(data_dir, "shards")
    if not os.path.isdir(root): return 0
    today = utcnow().date()
    n = 0
    for day in list(os.listdir(root)):
        dpath = os.path.join(root, day)
        if not os.path.isdir(dpath): continue
        try: d = dt.date.fromisoformat(day)
        except ValueError: continue
        if (today - d).days < keep_days: continue
        for fn in list(os.listdir(dpath)):
            if fn.endswith(".gz") or fn.endswith(".tmp"): continue
            src = os.path.join(dpath, fn)
            if not os.path.isfile(src): continue
            dst = _gzip_dest(dpath, src)
            tmp = dst + ".tmp"
            with open(src, "rb") as inp, gzip.open(tmp, "wb", compresslevel=9) as out:
                while True:
                    chunk = inp.read(1024 * 1024)
                    if not chunk: break
                    out.write(chunk)
            # Never replace a gzip that already holds history. Pick another name if it appeared.
            if os.path.exists(dst):
                dst = _gzip_dest(dpath, src)
            os.replace(tmp, dst)
            if os.path.exists(src):
                os.remove(src)
            n += 1
    return n


def _gzip_dest(day_dir, src):
    """Destination .gz path that does not already exist."""
    direct = src + ".gz"
    if not os.path.exists(direct):
        return direct
    root, ext = os.path.splitext(os.path.basename(src))
    # src may be ohlcv.csv or ohlcv-3.csv. Numbered dest keeps the .csv.gz suffix.
    n = 1
    while True:
        cand = os.path.join(day_dir, f"{root}-{n}{ext}.gz")
        if not os.path.exists(cand):
            return cand
        n += 1


def _ensure_files(data_dir):
    os.makedirs(os.path.join(data_dir, "shards"), exist_ok=True)
    os.makedirs(os.path.join(data_dir, "mutable"), exist_ok=True)
    os.makedirs(os.path.join(data_dir, ".run"), exist_ok=True)
    gi = os.path.join(data_dir, ".gitignore")
    if not os.path.exists(gi):
        atomic_write(gi, ".run/\ncheckpoint.ready\n*.tmp\n")
    readme = os.path.join(data_dir, "README.md")
    if not os.path.exists(readme):
        atomic_write(readme,
            "# young-coin-collector data\n\n"
            "Append-only research data for the collector on `main`.\n"
            "Do not merge this branch into `main` (unrelated history).\n\n"
            "Read it with `DATA_DIR=<this directory> python3 status.py` from a checkout of `main`.\n"
            "See the main branch README for the cohort definition, the filter, and how to stop the workflow.\n\n"
            "Layout: `shards/YYYY-MM-DD/` (full record), `mutable/` (resume snapshots), `heartbeat.json`.\n"
            "A `STOP` file here makes the next Actions run exit without collecting or chaining.\n")


def _build_index(data_dir):
    """Ephemeral pool-id index so a pruned SQLite cannot mint a new first_seen for an old pool."""
    global _index, _known_pools, _known_candidates
    run = os.path.join(data_dir, ".run")
    os.makedirs(run, exist_ok=True)
    path = os.path.join(run, "seen_index.sqlite")
    try:
        if os.path.exists(path): os.remove(path)
    except Exception:
        pass
    ix = sqlite3.connect(path)
    ix.execute("PRAGMA journal_mode=OFF")
    ix.execute("PRAGMA synchronous=OFF")
    ix.execute("CREATE TABLE seen(pool TEXT PRIMARY KEY, created_at REAL, first_seen REAL, first_source TEXT)")
    ix.execute("CREATE TABLE cands(pool TEXT PRIMARY KEY)")
    n = 0
    t0 = time.time()
    batch = []
    cutoff = time.time() - HYDRATE_POOLS_S
    recent = []
    for part in iter_part_files(data_dir, "pools_seen.csv"):
        for row in iter_csv(part):
            pool = row.get("pool") or ""
            if not pool: continue
            created = _blank_float(row.get("created_at"))
            first = _blank_float(row.get("first_seen"))
            src = row.get("first_source") or ""
            batch.append((pool, created, first, src))
            if first is not None and first >= cutoff:
                recent.append(row)
            if len(batch) >= 5000:
                ix.executemany("INSERT OR IGNORE INTO seen VALUES(?,?,?,?)", batch)
                n += len(batch); batch = []
    if batch:
        ix.executemany("INSERT OR IGNORE INTO seen VALUES(?,?,?,?)", batch)
        n += len(batch)
    for part in iter_part_files(data_dir, "candidates.jsonl"):
        pools = [(obj.get("pool"),) for obj in iter_jsonl(part) if obj.get("pool")]
        if pools:
            ix.executemany("INSERT OR IGNORE INTO cands VALUES(?)", pools)
    ix.commit()
    _index = ix
    _known_pools = set()
    # The known set is "already durable". Hydrate fills it from the index.
    for (pool,) in ix.execute("SELECT pool FROM seen"):
        _known_pools.add(pool)
    _known_candidates = set(r[0] for r in ix.execute("SELECT pool FROM cands"))
    _clog(f"seen index {n} shard rows, {len(_known_pools)} pools, {len(_known_candidates)} candidates in {time.time()-t0:.1f}s")
    return recent


def _insert_pool_row(db, row):
    db.execute("INSERT OR IGNORE INTO pools_seen(pool,token,quote,name,dex,created_at,first_seen,first_source,first_fdv,first_reserve,first_age_h) "
               "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
               (row.get("pool"), row.get("token"), row.get("quote"), row.get("name"), row.get("dex"),
                _blank_float(row.get("created_at")), _blank_float(row.get("first_seen")), row.get("first_source"),
                _blank_float(row.get("first_fdv")), _blank_float(row.get("first_reserve")), _blank_float(row.get("first_age_h"))))


def _insert_candidate(db, obj):
    db.execute("INSERT OR IGNORE INTO candidates(pool,token,name,dex,first_for_token,event_ts,created_at,age_h,price,fdv,mc,reserve,"
               "vol_m5,vol_h1,vol_h6,vol_h24,tx_h1,tx_h24,ath_proxy,ath_source,source,raw) "
               "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
               tuple(obj.get(c) if c in ("pool", "token", "name", "dex", "ath_source", "source", "raw")
                     else (_blank_int(obj.get(c)) if c in ("first_for_token", "tx_h1", "tx_h24") else _blank_float(obj.get(c)))
                     for c in CANDIDATE_COLUMNS))


def _row_vals(columns, obj):
    num = {"token_amount", "sol_amount", "block_time", "observed_ts", "ts", "fee_bps", "reserve_usd",
           "reading_age_s", "price", "reserve_quote", "created_at", "first_seen", "graduated_at",
           "trigger_observed_ts", "until_ts", "covered_until"}
    integ = {"slot", "swap_is_first", "distinct_tokens_24h", "complete"}
    vals = []
    for c in columns:
        v = obj.get(c)
        if c in integ:
            vals.append(_blank_int(v))
        elif c in num:
            vals.append(_blank_float(v))
        else:
            vals.append(v)
    return vals


def _hydrate_marked(db, data_dir, table, columns, stem, max_age_s):
    try:
        db.execute(f"SELECT 1 FROM {table} LIMIT 1")
    except sqlite3.OperationalError:
        return
    cutoff = time.time() - max_age_s if max_age_s else None
    colsql = ",".join(columns)
    ph = ",".join("?" * len(columns))
    for part in iter_part_files(data_dir, stem):
        for obj in iter_jsonl(part):
            if cutoff is not None:
                try:
                    if float(obj.get("ts") or 0) < cutoff:
                        continue
                except Exception:
                    continue
            cur = db.execute(f"INSERT OR IGNORE INTO {table}({colsql}) VALUES({ph})", _row_vals(columns, obj))
            if cur.rowcount and cur.lastrowid:
                db.execute("INSERT OR IGNORE INTO _exported(tbl,rid) VALUES(?,?)", (table, cur.lastrowid))


def _hydrate_pump_coins(db, data_dir):
    path = os.path.join(data_dir, "mutable", "pump_coins.jsonl")
    if not os.path.exists(path):
        return
    try:
        db.execute("SELECT 1 FROM pump_coins LIMIT 1")
    except sqlite3.OperationalError:
        return
    cols = ["mint", "name", "symbol", "created_at", "bonding_curve", "complete", "pool", "venue", "first_seen", "graduated_at", "fee_bps", "reserve_usd"]
    for obj in iter_jsonl(path):
        if not obj.get("mint"):
            continue
        db.execute("INSERT OR REPLACE INTO pump_coins(" + ",".join(cols) + ") VALUES(" + ",".join("?" * len(cols)) + ")",
                   _row_vals(cols, obj))


def _rewrite_pump_coins(db, data_dir):
    try:
        rows = db.execute("SELECT mint,name,symbol,created_at,bonding_curve,complete,pool,venue,first_seen,graduated_at,fee_bps,reserve_usd FROM pump_coins ORDER BY mint").fetchall()
    except sqlite3.OperationalError:
        return
    cols = ["mint", "name", "symbol", "created_at", "bonding_curve", "complete", "pool", "venue", "first_seen", "graduated_at", "fee_bps", "reserve_usd"]
    lines = [json.dumps({c: row[i] for i, c in enumerate(cols)}, separators=(",", ":"), sort_keys=True, default=str) for row in rows]
    atomic_write_if_changed(os.path.join(data_dir, "mutable", "pump_coins.jsonl"), ("\n".join(lines) + ("\n" if lines else "")))


def _hydrate_pool_follows(db, data_dir):
    path = os.path.join(data_dir, "mutable", "pool_follows.jsonl")
    if not os.path.exists(path):
        return
    try:
        db.execute("SELECT 1 FROM pool_follows LIMIT 1")
    except sqlite3.OperationalError:
        return
    cols = ["pool", "mint", "venue", "trigger_observed_ts", "until_ts", "last_sig", "covered_until"]
    for obj in iter_jsonl(path):
        if not obj.get("pool"):
            continue
        db.execute("INSERT OR REPLACE INTO pool_follows(" + ",".join(cols) + ") VALUES(" + ",".join("?" * len(cols)) + ")",
                   _row_vals(cols, obj))


def _rewrite_pool_follows(db, data_dir):
    try:
        rows = db.execute("SELECT pool,mint,venue,trigger_observed_ts,until_ts,last_sig,covered_until FROM pool_follows ORDER BY pool").fetchall()
    except sqlite3.OperationalError:
        return
    cols = ["pool", "mint", "venue", "trigger_observed_ts", "until_ts", "last_sig", "covered_until"]
    lines = [json.dumps({c: row[i] for i, c in enumerate(cols)}, separators=(",", ":"), sort_keys=True, default=str) for row in rows]
    atomic_write_if_changed(os.path.join(data_dir, "mutable", "pool_follows.jsonl"), ("\n".join(lines) + ("\n" if lines else "")))


def hydrate(db, data_dir):
    """Load resume state into an empty SQLite. Append-only history stays in shards."""
    global _gzip_done
    reset()
    _ensure_files(data_dir)
    db.execute("CREATE TABLE IF NOT EXISTS _exported(tbl TEXT, rid INTEGER, PRIMARY KEY(tbl, rid))")
    gz = gzip_old_shards(data_dir)
    _gzip_done = True
    if gz: _clog(f"gzipped {gz} old shard files")
    meta_path = os.path.join(data_dir, "mutable", "meta.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path) as fh:
                blob = json.load(fh)
            for k, v in blob.items():
                meta_set(db, k, v)
        except Exception as e:
            _clog(f"meta hydrate failed: {e}", "ERROR")
    recent = _build_index(data_dir)
    for row in recent:
        _insert_pool_row(db, row)
    for part in iter_part_files(data_dir, "candidates.jsonl"):
        for obj in iter_jsonl(part):
            if obj.get("pool"): _insert_candidate(db, obj)
    fu_path = os.path.join(data_dir, "mutable", "followups.jsonl")
    if os.path.exists(fu_path):
        for obj in iter_jsonl(fu_path):
            if not obj.get("pool") or not obj.get("label"): continue
            db.execute("INSERT OR REPLACE INTO followups(pool,label,due_ts,kind,state,attempts,next_try,done_ts) VALUES(?,?,?,?,?,?,?,?)",
                       (obj.get("pool"), obj.get("label"), _blank_float(obj.get("due_ts")), obj.get("kind"),
                        obj.get("state") or "pending", _blank_int(obj.get("attempts")) or 0,
                        _blank_float(obj.get("next_try")), _blank_float(obj.get("done_ts"))))
    ws_path = os.path.join(data_dir, "mutable", "watch_state.jsonl")
    if os.path.exists(ws_path):
        for obj in iter_jsonl(ws_path):
            if not obj.get("pool"): continue
            vals = []
            for c in WATCH_COLUMNS:
                if c in ("pool", "token", "status"): vals.append(obj.get(c))
                elif c in ("misses", "is_candidate"): vals.append(_blank_int(obj.get(c)) or 0)
                else: vals.append(_blank_float(obj.get(c)))
            db.execute("INSERT OR REPLACE INTO watch_state(" + ",".join(WATCH_COLUMNS) + ") VALUES(" + ",".join("?" * len(WATCH_COLUMNS)) + ")", vals)
    # Candidate pools must stay terminal even if their watch row was pruned.
    db.execute("""INSERT INTO watch_state(pool,token,created_at,last_check,last_fdv,ath_mc_obs,status,misses,is_candidate)
                  SELECT pool, token, created_at, event_ts, fdv, mc, 'candidate', 0, 1 FROM candidates
                  WHERE pool NOT IN (SELECT pool FROM watch_state)""")
    api_path = os.path.join(data_dir, "mutable", "api_calls.jsonl")
    if os.path.exists(api_path):
        for obj in iter_jsonl(api_path):
            if not obj.get("day") or not obj.get("endpoint"): continue
            db.execute("INSERT OR REPLACE INTO api_calls(day,endpoint,calls,n429,nerr) VALUES(?,?,?,?,?)",
                       (obj["day"], obj["endpoint"], _blank_int(obj.get("calls")) or 0,
                        _blank_int(obj.get("n429")) or 0, _blank_int(obj.get("nerr")) or 0))
    _hydrate_pump_coins(db, data_dir)
    _hydrate_marked(db, data_dir, "wallet_events", WALLET_COLUMNS, "wallet_events.jsonl", 48 * 3600)
    _hydrate_marked(db, data_dir, "pump_events", PUMP_EVENT_COLUMNS, "pump_events.jsonl", None)
    _hydrate_marked(db, data_dir, "pool_swaps", POOL_SWAP_COLUMNS, "pool_swaps.jsonl", 48 * 3600)
    _hydrate_pool_follows(db, data_dir)
    db.commit()
    _clog(f"hydrate pools_in_db={db.execute('SELECT COUNT(*) FROM pools_seen').fetchone()[0]} "
          f"candidates={db.execute('SELECT COUNT(*) FROM candidates').fetchone()[0]} "
          f"followups={db.execute('SELECT COUNT(*) FROM followups').fetchone()[0]} "
          f"watch={db.execute('SELECT COUNT(*) FROM watch_state').fetchone()[0]}")


def is_archived(pool):
    """True if this pool already has a durable pools_seen row (do not rewrite first_seen)."""
    if not pool or _index is None: return False
    return _index.execute("SELECT 1 FROM seen WHERE pool=?", (pool,)).fetchone() is not None


def record_start_gap(db):
    """Log the uncovered interval since the previous checkpoint. Every gap, including short ones."""
    last = meta_get(db, "last_alive_ts")
    clean = meta_get(db, "clean_shutdown") == "1"
    t = time.time()
    if last:
        try: last_f = float(last)
        except Exception: last_f = None
        if last_f and t - last_f > GAP_MIN_S:
            kind = "between_runs" if clean else "killed"
            db.execute("INSERT INTO downtime(start_ts,end_ts,minutes,kind) VALUES(?,?,?,?)",
                       (last_f, t, (t - last_f) / 60.0, kind))
            _clog(f"GAP {kind} {(t - last_f) / 60.0:.2f} min ({day_of(last_f)} -> now)", "WARN")
    if not meta_get(db, "coverage_origin_ts"):
        meta_set(db, "coverage_origin_ts", repr(t))
    runs = int(float(meta_get(db, "runs") or 0))
    meta_set(db, "runs", str(runs + 1))
    meta_set(db, "last_alive_ts", repr(t))
    meta_set(db, "clean_shutdown", "0")
    meta_set(db, "run_started_ts", repr(t))
    db.commit()


def mark_clean(db):
    meta_set(db, "clean_shutdown", "1")
    meta_set(db, "last_alive_ts", repr(time.time()))
    db.commit()


def _export_new_pools(db, data_dir):
    rows = db.execute("SELECT " + ",".join(POOLS_COLUMNS) + " FROM pools_seen").fetchall()
    n = 0
    for row in rows:
        pool = row[0]
        if not pool or pool in _known_pools: continue
        created, first = row[5], row[6]
        day = day_of(first or time.time())
        out = []
        for i, c in enumerate(POOLS_COLUMNS):
            v = row[i]
            if c in ("created_at", "first_seen", "first_fdv", "first_reserve", "first_age_h"):
                out.append(_num(v))
            else:
                out.append("" if v is None else str(v))
        append_csv(data_dir, day, "pools_seen.csv", POOLS_COLUMNS, out)
        _known_pools.add(pool)
        if _index is not None:
            _index.execute("INSERT OR IGNORE INTO seen VALUES(?,?,?,?)", (pool, created, first, row[7]))
        n += 1
    if n and _index is not None: _index.commit()
    return n


def _export_new_candidates(db, data_dir):
    cols = ",".join(CANDIDATE_COLUMNS)
    rows = db.execute(f"SELECT {cols} FROM candidates").fetchall()
    n = 0
    for row in rows:
        pool = row[0]
        if not pool or pool in _known_candidates: continue
        obj = {}
        for i, c in enumerate(CANDIDATE_COLUMNS):
            v = row[i]
            if isinstance(v, float): obj[c] = v
            else: obj[c] = v
        day = day_of(obj.get("event_ts") or time.time())
        append_jsonl(data_dir, day, "candidates.jsonl", obj)
        _known_candidates.add(pool)
        if _index is not None:
            _index.execute("INSERT OR IGNORE INTO cands VALUES(?)", (pool,))
        n += 1
    if n and _index is not None: _index.commit()
    return n


def _export_unexported(db, data_dir, table, columns, stem, ts_col, delete_after):
    colsql = ",".join(columns)
    rows = db.execute(
        f"SELECT rowid, {colsql} FROM {table} WHERE rowid NOT IN (SELECT rid FROM _exported WHERE tbl=?) ORDER BY rowid",
        (table,)).fetchall()
    n = 0
    for row in rows:
        rid = row[0]
        obj = {}
        for i, c in enumerate(columns):
            obj[c] = row[i + 1]
        ts = obj.get(ts_col) or time.time()
        try: day = day_of(ts)
        except Exception: day = day_of(time.time())
        if stem.endswith(".csv"):
            vals = []
            for c in columns:
                v = obj[c]
                if c == "ts" or c in ("tx_h1", "tx_h24"):
                    vals.append(_int(v) if v is not None else "")
                elif isinstance(v, float) or c in ("o", "h", "l", "c", "v"):
                    vals.append(_num(v))
                else:
                    vals.append("" if v is None else v)
            append_csv(data_dir, day, stem, columns, vals)
        else:
            append_jsonl(data_dir, day, stem, obj)
        db.execute("INSERT OR IGNORE INTO _exported(tbl,rid) VALUES(?,?)", (table, rid))
        n += 1
    if delete_after and n:
        db.execute(f"DELETE FROM {table} WHERE rowid IN (SELECT rid FROM _exported WHERE tbl=?)", (table,))
        # Drop the marks with the rows so a reused rowid cannot hide a future insert.
        db.execute("DELETE FROM _exported WHERE tbl=?", (table,))
    return n


def _rewrite_followups(db, data_dir):
    rows = db.execute("SELECT " + ",".join(FOLLOWUP_COLUMNS) + " FROM followups ORDER BY pool, label").fetchall()
    lines = []
    for row in rows:
        obj = {c: row[i] for i, c in enumerate(FOLLOWUP_COLUMNS)}
        lines.append(json.dumps(obj, separators=(",", ":"), sort_keys=True, default=str))
    text = ("\n".join(lines) + ("\n" if lines else ""))
    return atomic_write_if_changed(os.path.join(data_dir, "mutable", "followups.jsonl"), text)


def _rewrite_watch(db, data_dir):
    # Archive terminal rows for pools that can no longer be re-watched, then drop them
    # from the live snapshot so the mutable file stays small. The shard keeps them.
    cutoff = time.time() - 48 * 3600
    old = db.execute("SELECT " + ",".join(WATCH_COLUMNS) +
                     " FROM watch_state WHERE created_at IS NOT NULL AND created_at < ? AND status!='watch' AND is_candidate=0",
                     (cutoff,)).fetchall()
    for row in old:
        obj = {c: row[i] for i, c in enumerate(WATCH_COLUMNS)}
        append_jsonl(data_dir, day_of(obj.get("created_at") or time.time()), "watch_state_closed.jsonl", obj)
    if old:
        db.execute("DELETE FROM watch_state WHERE created_at IS NOT NULL AND created_at < ? AND status!='watch' AND is_candidate=0",
                   (cutoff,))
    rows = db.execute("SELECT " + ",".join(WATCH_COLUMNS) + " FROM watch_state ORDER BY pool").fetchall()
    lines = []
    for row in rows:
        obj = {c: row[i] for i, c in enumerate(WATCH_COLUMNS)}
        lines.append(json.dumps(obj, separators=(",", ":"), sort_keys=True, default=str))
    text = ("\n".join(lines) + ("\n" if lines else ""))
    atomic_write_if_changed(os.path.join(data_dir, "mutable", "watch_state.jsonl"), text)
    return len(old)


def _rewrite_api(db, data_dir):
    rows = db.execute("SELECT day,endpoint,calls,n429,nerr FROM api_calls ORDER BY day, endpoint").fetchall()
    lines = [json.dumps({"day": r[0], "endpoint": r[1], "calls": r[2], "n429": r[3], "nerr": r[4]},
                        separators=(",", ":"), sort_keys=True) for r in rows]
    text = ("\n".join(lines) + ("\n" if lines else ""))
    atomic_write_if_changed(os.path.join(data_dir, "mutable", "api_calls.jsonl"), text)


def _dump_meta(db, data_dir):
    rows = db.execute("SELECT k, v FROM meta ORDER BY k").fetchall()
    blob = {k: v for k, v in rows}
    atomic_write_if_changed(os.path.join(data_dir, "mutable", "meta.json"),
                            json.dumps(blob, indent=2, sort_keys=True) + "\n")
    return blob


def checkpoint(db, data_dir):
    """Flush every durable row and rewrite resume snapshots. Single-threaded; no API calls."""
    global _gzip_done
    _ensure_files(data_dir)
    db.execute("CREATE TABLE IF NOT EXISTS _exported(tbl TEXT, rid INTEGER, PRIMARY KEY(tbl, rid))")
    if not _gzip_done:
        gzip_old_shards(data_dir)
        _gzip_done = True
    os.makedirs(os.path.join(data_dir, ".run"), exist_ok=True)
    lock_path = os.path.join(data_dir, ".run", "export.lock")
    with open(lock_path, "a") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            db.commit()
            counts = {}
            counts["pools_seen"] = _export_new_pools(db, data_dir)
            counts["candidates"] = _export_new_candidates(db, data_dir)
            counts["watch_snapshots"] = _export_unexported(db, data_dir, "watch_snapshots", SNAPSHOT_COLUMNS, "watch_snapshots.jsonl", "ts", True)
            counts["followup_results"] = _export_unexported(db, data_dir, "followup_results", RESULT_COLUMNS, "followup_results.jsonl", "ts", True)
            counts["ohlcv"] = _export_unexported(db, data_dir, "ohlcv", OHLCV_COLUMNS, "ohlcv.csv", "ts", True)
            counts["wallet_events"] = _export_unexported(db, data_dir, "wallet_events", WALLET_COLUMNS, "wallet_events.jsonl", "ts", False)
            counts["pump_events"] = _export_unexported(db, data_dir, "pump_events", PUMP_EVENT_COLUMNS, "pump_events.jsonl", "ts", False)
            counts["pool_swaps"] = _export_unexported(db, data_dir, "pool_swaps", POOL_SWAP_COLUMNS, "pool_swaps.jsonl", "ts", False)
            _rewrite_pump_coins(db, data_dir)
            _rewrite_pool_follows(db, data_dir)
            counts["downtime"] = _export_unexported(db, data_dir, "downtime", ["start_ts", "end_ts", "minutes", "kind"], "downtime.jsonl", "end_ts", False)
            counts["run_log"] = _export_unexported(db, data_dir, "run_log", ["ts", "level", "msg"], "run_log.jsonl", "ts", False)
            counts["watch_closed"] = _rewrite_watch(db, data_dir)
            _rewrite_followups(db, data_dir)
            _rewrite_api(db, data_dir)
            now_ts = time.time()
            meta_set(db, "last_alive_ts", repr(now_ts))
            blob = _dump_meta(db, data_dir)
            db.commit()
            total_pools = len(_known_pools)
            info = {"ts": now_ts, "iso": dt.datetime.fromtimestamp(now_ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "counts": counts, "pools_total": total_pools,
                    "candidates_total": db.execute("SELECT COUNT(*) FROM candidates").fetchone()[0],
                    "followups_pending": db.execute("SELECT COUNT(*) FROM followups WHERE state='pending'").fetchone()[0]}
            atomic_write(os.path.join(data_dir, "mutable", "last_checkpoint.json"),
                         json.dumps(info, indent=2, sort_keys=True) + "\n")
            hb = {"ts": now_ts, "iso": info["iso"], "checkpoint": True,
                  "clean_shutdown": blob.get("clean_shutdown") == "1",
                  "last_alive_ts": now_ts,
                  "coverage_origin_ts": _blank_float(blob.get("coverage_origin_ts")),
                  "runs": int(float(blob.get("runs") or 0)),
                  "pools_total": total_pools,
                  "candidates_total": info["candidates_total"],
                  "followups_pending": info["followups_pending"],
                  "state": _safe_json(blob.get("state"))}
            atomic_write(os.path.join(data_dir, "heartbeat.json"), json.dumps(hb, indent=2, sort_keys=True) + "\n")
            # Publisher watches this mtime. It is gitignored.
            atomic_write(os.path.join(data_dir, "checkpoint.ready"), info["iso"] + "\n")
            return info
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def _safe_json(v):
    if not v: return None
    try: return json.loads(v)
    except Exception: return None


def merge_intervals(intervals):
    xs = [(float(a), float(b)) for a, b in intervals if a is not None and b is not None and float(b) > float(a)]
    xs.sort()
    out = []
    for a, b in xs:
        if not out or a > out[-1][1]:
            out.append([a, b])
        else:
            out[-1][1] = max(out[-1][1], b)
    return [(a, b) for a, b in out]


def load_downtime(data_dir, db=None):
    rows = []
    for part in iter_part_files(data_dir, "downtime.jsonl"):
        for obj in iter_jsonl(part):
            rows.append(obj)
    if db is not None:
        try:
            for start, end, minutes, kind in db.execute("SELECT start_ts,end_ts,minutes,kind FROM downtime"):
                rows.append({"start_ts": start, "end_ts": end, "minutes": minutes, "kind": kind})
        except Exception:
            pass
    return rows


def coverage(origin, end, intervals):
    """Return (fraction, span_s, down_s) of [origin, end] that is not inside a gap."""
    if origin is None or end is None or end <= origin:
        return 1.0, 0.0, 0.0
    clipped = []
    for a, b in intervals:
        lo, hi = max(a, origin), min(b, end)
        if hi > lo: clipped.append((lo, hi))
    merged = merge_intervals(clipped)
    down = sum(b - a for a, b in merged)
    span = end - origin
    return (span - down) / span, span, down


def is_cohort_pool(created_at, first_seen, first_source, intervals, origin):
    """Born-during-coverage: first observed on new_pools while we were up, near birth.

    Excludes pools born inside a logged gap (or before the collector's first start) and
    pools first seen long after creation (discovered late; survivorship).
    """
    if first_source != "new_pools": return False
    if created_at is None or first_seen is None or origin is None: return False
    if created_at < origin: return False
    if first_seen + 1 < created_at: return False
    if first_seen - created_at > COHORT_SEEN_WITHIN_S: return False
    for a, b in intervals:
        if a <= created_at <= b: return False
        if a <= first_seen <= b: return False
    return True
