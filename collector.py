#!/usr/bin/env python3
"""Young Solana pool forward collector. DATA ONLY - no trading. See README.md.

Local:  python3 collector.py
Actions: RUN_SECONDS and DATA_DIR are set by scripts/run_actions.sh.
The research filter, discovery cadence, and follow-up schedule are unchanged.
"""
import fcntl, json, os, sqlite3, signal, sys, time, traceback, urllib.request, urllib.error, datetime as dt
import persist

BASE = os.path.dirname(os.path.abspath(__file__))
GT = "https://api.geckoterminal.com/api/v2/networks/solana"
UA = {"Accept": "application/json", "User-Agent": "young-coin-collector/1.0 (research, data only)"}

# ---- filter (approximation of David's GMGN filter; see README) ----
AGE_MIN_H, AGE_MAX_H = 0.5, 20.0
MC_MIN, ATH_MIN = 200_000, 400_000
TX24_MIN = 1000            # proxy for "total fees >= 1 SOL"
NEAR_MC = 50_000           # near-miss threshold stored in watchlist snapshots
WATCH_MIN_FDV = 20_000     # pools below this (and not trending) are only recorded in pools_seen
BONDING_DEXES = {"pump-fun", "meteora-dbc", "raydium-launchlab", "boop-fun", "moonshot"}
QUOTES = {"So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
          "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
FOLLOWUPS = [("15m", 900), ("1h", 3600), ("4h", 14400), ("24h", 86400), ("48h", 172800)]
MIN_SPACING = 5.0         # hard cap 12 calls/min
NEW_POOLS_EVERY, TREND_EVERY, TREND_DEEP_EVERY = 150, 900, 3600
LATE_S = 600

db = None
_in_ckpt = [False]
_ready = [False]
_last_ckpt = [0.0]
STOP = False
DEADLINE = None


def env_float(name, default):
    raw = os.environ.get(name, "")
    if raw is None or raw == "": return default
    try: return float(raw)
    except ValueError: return default


def run_paths():
    data = os.environ.get("DATA_DIR", os.path.join(BASE, "data"))
    run = os.path.join(data, ".run")
    return data, run, os.path.join(run, "collector.db"), os.path.join(run, "collector.lock"), os.path.join(run, "heartbeat.json"), os.path.join(run, "logs", "collector.log")


def now(): return time.time()
def iso(t): return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
def ptime(s):
    try: return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp()
    except Exception: return None
def f(x):
    try: return float(x) if x is not None else None
    except Exception: return None

def should_stop():
    if STOP: return True
    if DEADLINE is not None and now() >= DEADLINE: return True
    return False

def sleep_q(sec):
    """Sleep up to sec seconds, waking early on shutdown or the run deadline."""
    if sec <= 0: return not should_stop()
    end = time.time() + sec
    while True:
        if should_stop(): return False
        left = end - time.time()
        if left <= 0: return True
        time.sleep(min(1.0, left))

def log(msg, level="INFO"):
    line = f"{iso(now())} {level} {msg}"
    print(line, flush=True)
    try:
        logf = run_paths()[5]
        os.makedirs(os.path.dirname(logf), exist_ok=True)
        with open(logf, "a") as fh: fh.write(line + "\n")
    except Exception: pass
    try:
        if db is not None:
            db.execute("INSERT INTO run_log(ts,level,msg) VALUES(?,?,?)", (now(), level, msg[:2000])); db.commit()
    except Exception: pass

def maybe_checkpoint(force=False):
    if not _ready[0] or db is None or _in_ckpt[0]: return None
    every = env_float("CHECKPOINT_SECONDS", 180)
    if every < 15: every = 15
    if not force and now() - _last_ckpt[0] < every: return None
    _in_ckpt[0] = True
    try:
        info = persist.checkpoint(db, run_paths()[0])
        _last_ckpt[0] = now()
        _in_ckpt[0] = False
        log("checkpoint " + json.dumps(info["counts"], sort_keys=True) + f" pools_total={info['pools_total']}")
        return info
    except Exception as e:
        _in_ckpt[0] = False
        log("checkpoint failed: " + "".join(traceback.format_exception_only(type(e), e)).strip(), "ERROR")
        return None

SCHEMA = """
CREATE TABLE IF NOT EXISTS pools_seen(pool TEXT PRIMARY KEY, token TEXT, quote TEXT, name TEXT, dex TEXT,
  created_at REAL, first_seen REAL, first_source TEXT, first_fdv REAL, first_reserve REAL, first_age_h REAL);
CREATE TABLE IF NOT EXISTS watch_state(pool TEXT PRIMARY KEY, token TEXT, created_at REAL, next_check REAL,
  last_check REAL, last_fdv REAL, ath_mc_obs REAL, ath_mc_ohlcv REAL, ath_checked REAL, status TEXT, misses INTEGER DEFAULT 0,
  is_candidate INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS watch_snapshots(id INTEGER PRIMARY KEY, ts REAL, pool TEXT, source TEXT, age_h REAL,
  price REAL, fdv REAL, mc REAL, reserve REAL, vol_m5 REAL, vol_h1 REAL, vol_h6 REAL, vol_h24 REAL,
  tx_h1 INTEGER, tx_h24 INTEGER, ath_proxy REAL, passes INTEGER, fail_reasons TEXT, status TEXT);
CREATE TABLE IF NOT EXISTS candidates(id INTEGER PRIMARY KEY, pool TEXT UNIQUE, token TEXT, name TEXT, dex TEXT,
  first_for_token INTEGER, event_ts REAL, created_at REAL, age_h REAL, price REAL, fdv REAL, mc REAL, reserve REAL,
  vol_m5 REAL, vol_h1 REAL, vol_h6 REAL, vol_h24 REAL, tx_h1 INTEGER, tx_h24 INTEGER, ath_proxy REAL, ath_source TEXT,
  source TEXT, raw TEXT);
CREATE TABLE IF NOT EXISTS followups(id INTEGER PRIMARY KEY, pool TEXT, label TEXT, due_ts REAL, kind TEXT,
  state TEXT DEFAULT 'pending', attempts INTEGER DEFAULT 0, next_try REAL, done_ts REAL, UNIQUE(pool,label));
CREATE TABLE IF NOT EXISTS followup_results(id INTEGER PRIMARY KEY, followup_id INTEGER, pool TEXT, label TEXT,
  due_ts REAL, ts REAL, late INTEGER, late_min REAL, attempts INTEGER, status TEXT, price REAL, fdv REAL, mc REAL,
  reserve REAL, vol_m5 REAL, vol_h1 REAL, vol_h24 REAL, tx_h1 INTEGER, tx_h24 INTEGER, note TEXT);
CREATE TABLE IF NOT EXISTS ohlcv(pool TEXT, tf TEXT, ts INTEGER, o REAL, h REAL, l REAL, c REAL, v REAL, PRIMARY KEY(pool,tf,ts));
CREATE TABLE IF NOT EXISTS api_calls(day TEXT, endpoint TEXT, calls INTEGER, n429 INTEGER, nerr INTEGER, PRIMARY KEY(day,endpoint));
CREATE TABLE IF NOT EXISTS run_log(id INTEGER PRIMARY KEY, ts REAL, level TEXT, msg TEXT);
CREATE TABLE IF NOT EXISTS downtime(id INTEGER PRIMARY KEY, start_ts REAL, end_ts REAL, minutes REAL, kind TEXT);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
"""

_last_call = [0.0]
def api(path, endpoint):
    """GET with global spacing, 429 backoff. Returns (status_code, json_or_None)."""
    day = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    data, run, _dbp, _lock, heart, _logf = run_paths()
    if _last_call[0] and now() - _last_call[0] > 300:  # stalled (no calls for >5 min)
        g = now() - _last_call[0]
        db.execute("INSERT INTO downtime(start_ts,end_ts,minutes,kind) VALUES(?,?,?,'pause_in_process')", (_last_call[0], now(), g / 60))
        log(f"pause detected: {g/60:.1f} min without activity", "WARN")
    for attempt in range(4):
        if should_stop(): return -2, None
        maybe_checkpoint()
        wait = _last_call[0] + MIN_SPACING - now()
        if wait > 0 and not sleep_q(wait): return -2, None
        if should_stop(): return -2, None
        _last_call[0] = now()
        try: os.utime(heart)  # liveness for the Actions watchdog during long loops
        except Exception: pass
        code, js = None, None
        try:
            req = urllib.request.Request(GT + path, headers=UA)
            with urllib.request.urlopen(req, timeout=25) as r:
                code, js = r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            code = e.code
        except Exception:
            code = -1
        db.execute("INSERT INTO api_calls VALUES(?,?,1,?,?) ON CONFLICT(day,endpoint) DO UPDATE SET calls=calls+1,"
                   "n429=n429+excluded.n429, nerr=nerr+excluded.nerr",
                   (day, endpoint, 1 if code == 429 else 0, 1 if (code not in (200, 404, 429)) else 0))
        db.commit()
        if code == 429:
            back = 30 * (2 ** attempt); log(f"429 on {endpoint}; backoff {back}s", "WARN")
            maybe_checkpoint()
            if not sleep_q(back): return code, None
            continue
        if code == -1 or (code and code >= 500):
            maybe_checkpoint()
            if not sleep_q(10 * (attempt + 1)): return code, None
            continue
        return code, js
    return code, None

def parse_pool(d):
    a, r = d["attributes"], d.get("relationships", {})
    tok = (r.get("base_token", {}).get("data") or {}).get("id", "").replace("solana_", "")
    quo = (r.get("quote_token", {}).get("data") or {}).get("id", "").replace("solana_", "")
    dex = (r.get("dex", {}).get("data") or {}).get("id")
    tx = a.get("transactions") or {}; vol = a.get("volume_usd") or {}
    def txn(k): t = tx.get(k) or {}; return (t.get("buys") or 0) + (t.get("sells") or 0)
    fdv, mc = f(a.get("fdv_usd")), f(a.get("market_cap_usd"))
    return dict(pool=a["address"], token=tok, quote=quo, dex=dex, name=a.get("name"),
                created_at=ptime(a.get("pool_created_at") or ""), price=f(a.get("base_token_price_usd")),
                fdv=fdv, mc=mc if mc else fdv, reserve=f(a.get("reserve_in_usd")),
                vol_m5=f(vol.get("m5")), vol_h1=f(vol.get("h1")), vol_h6=f(vol.get("h6")), vol_h24=f(vol.get("h24")),
                tx_h1=txn("h1"), tx_h24=txn("h24"))

def evaluate(p, ath, t):
    age = (t - p["created_at"]) / 3600 if p["created_at"] else None
    fails = []
    if age is None: fails.append("age_unknown")
    elif age < AGE_MIN_H: fails.append("too_young")
    elif age > AGE_MAX_H: fails.append("too_old")
    if (p["mc"] or 0) < MC_MIN: fails.append("mc")
    if (ath or 0) < ATH_MIN: fails.append("ath")
    if (p["tx_h24"] or 0) < TX24_MIN: fails.append("tx24_fee_proxy")
    if not p["reserve"] or p["reserve"] <= 0: fails.append("no_liquidity")
    if p["token"] in QUOTES: fails.append("base_is_quote")
    return age, fails

def next_interval(fdv):
    if (fdv or 0) >= 100_000: return 300
    if (fdv or 0) >= 30_000: return 1200
    return 7200

def ath_from_ohlcv(p):
    code, js = api(f"/pools/{p['pool']}/ohlcv/minute?aggregate=5&limit=1000&currency=usd", "ohlcv_ath")
    try:
        lst = js["data"]["attributes"]["ohlcv_list"]
        if not lst or not p["price"]: return None
        return max(c[2] for c in lst) * (p["mc"] / p["price"])
    except Exception: return None

def observe(p, source, t):
    """Record a pool observation; maybe create candidate."""
    if not p["created_at"] or p["token"] in QUOTES: return
    age = (t - p["created_at"]) / 3600
    seen = db.execute("SELECT 1 FROM pools_seen WHERE pool=?", (p["pool"],)).fetchone()
    # A pool already durable in a shard must keep its original first_seen. The live table
    # only holds a recent window, so a trending reprint of an old pool is not a new birth.
    if not seen and persist.is_archived(p["pool"]):
        seen = True
    if not seen:
        db.execute("INSERT OR IGNORE INTO pools_seen(pool,token,quote,name,dex,created_at,first_seen,first_source,first_fdv,first_reserve,first_age_h) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                   (p["pool"], p["token"], p["quote"], p["name"], p["dex"], p["created_at"], t, source, p["fdv"], p["reserve"], age))
    if age > AGE_MAX_H + 0.01:  # too old: nothing to watch (unless already a candidate - handled by followups)
        db.execute("UPDATE watch_state SET status='aged_out', next_check=NULL WHERE pool=? AND status='watch'", (p["pool"],))
        return
    ws = db.execute("SELECT ath_mc_obs, ath_mc_ohlcv, ath_checked, is_candidate FROM watch_state WHERE pool=?", (p["pool"],)).fetchone()
    trending = source.startswith("trending")
    if ws is None:
        if db.execute("SELECT 1 FROM candidates WHERE pool=?", (p["pool"],)).fetchone():
            return
        if (p["fdv"] or 0) < WATCH_MIN_FDV and not trending and p["dex"] in BONDING_DEXES: return
        if (p["fdv"] or 0) < WATCH_MIN_FDV and not trending and (p["reserve"] or 0) < 5000: return
        db.execute("INSERT INTO watch_state(pool,token,created_at,status,ath_mc_obs) VALUES(?,?,?,'watch',?)",
                   (p["pool"], p["token"], p["created_at"], p["mc"] or 0))
        ws = (p["mc"] or 0, None, None, 0)
    ath_obs = max(ws[0] or 0, p["mc"] or 0); ath_o = ws[1]; ath_chk = ws[2]
    if ws[3]:
        db.execute("UPDATE watch_state SET ath_mc_obs=?, last_check=?, last_fdv=? WHERE pool=?", (ath_obs, t, p["fdv"], p["pool"])); return
    ath = max(ath_obs, ath_o or 0); ath_src = "observed"
    if (MC_MIN <= (p["mc"] or 0) < ATH_MIN and ath < ATH_MIN and age >= AGE_MIN_H and (p["tx_h24"] or 0) >= TX24_MIN
            and (not ath_chk or t - ath_chk > 1800)):
        a2 = ath_from_ohlcv(p); ath_chk = t
        if a2: ath_o = max(ath_o or 0, a2)
        ath = max(ath_obs, ath_o or 0)
    if ath_o and ath_o >= ath_obs: ath_src = "ohlcv5m_high"
    age, fails = evaluate(p, ath, t)
    passes = 0 if fails else 1
    status = "ok" if (p["reserve"] or 0) > 0 else "zero_liquidity"
    if (p["mc"] or 0) >= NEAR_MC or passes:
        db.execute("INSERT INTO watch_snapshots(ts,pool,source,age_h,price,fdv,mc,reserve,vol_m5,vol_h1,vol_h6,vol_h24,tx_h1,tx_h24,"
                   "ath_proxy,passes,fail_reasons,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                   (t, p["pool"], source, age, p["price"], p["fdv"], p["mc"], p["reserve"], p["vol_m5"], p["vol_h1"], p["vol_h6"],
                    p["vol_h24"], p["tx_h1"], p["tx_h24"], ath, passes, ",".join(fails), status))
    db.execute("UPDATE watch_state SET ath_mc_obs=?, ath_mc_ohlcv=?, ath_checked=?, last_check=?, last_fdv=?, misses=0,"
               "next_check=? WHERE pool=?", (ath_obs, ath_o, ath_chk, t, p["fdv"], t + next_interval(p["fdv"]), p["pool"]))
    if passes:
        first_tok = 0 if db.execute("SELECT 1 FROM candidates WHERE token=?", (p["token"],)).fetchone() else 1
        cur = db.execute("INSERT OR IGNORE INTO candidates(pool,token,name,dex,first_for_token,event_ts,created_at,age_h,price,fdv,mc,"
                   "reserve,vol_m5,vol_h1,vol_h6,vol_h24,tx_h1,tx_h24,ath_proxy,ath_source,source,raw) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                   (p["pool"], p["token"], p["name"], p["dex"], first_tok, t, p["created_at"], age, p["price"], p["fdv"], p["mc"],
                    p["reserve"], p["vol_m5"], p["vol_h1"], p["vol_h6"], p["vol_h24"], p["tx_h1"], p["tx_h24"], ath, ath_src, source, json.dumps(p)))
        if cur.rowcount:
            db.execute("UPDATE watch_state SET is_candidate=1, status='candidate', next_check=NULL WHERE pool=?", (p["pool"],))
            for lab, off in FOLLOWUPS:
                db.execute("INSERT OR IGNORE INTO followups(pool,label,due_ts,kind,next_try) VALUES(?,?,?,'snapshot',?)", (p["pool"], lab, t + off, t + off))
            db.execute("INSERT OR IGNORE INTO followups(pool,label,due_ts,kind,next_try) VALUES(?,?,?,'ohlcv',?)", (p["pool"], "ohlcv24", t + 86400 + 300, t + 86400 + 300))
            db.execute("INSERT OR IGNORE INTO followups(pool,label,due_ts,kind,next_try) VALUES(?,?,?,'ohlcv',?)", (p["pool"], "ohlcv48", t + 172800 + 300, t + 172800 + 300))
            log(f"CANDIDATE {p['name']} {p['pool']} age={age:.2f}h mc={p['mc']:.0f} ath={ath:.0f} tx24={p['tx_h24']} src={source}")

def poll_list(path, source, endpoint):
    code, js = api(path, endpoint)
    if code != 200 or not js:
        if code != -2: log(f"{endpoint} {path} -> {code}", "WARN")
        return []
    t = now(); out = []
    for d in js.get("data", []):
        try: p = parse_pool(d); observe(p, source, t); out.append(p)
        except Exception as e: log(f"parse/observe error {e}", "ERROR")
    db.commit(); return out

def do_new_pools(state):
    last_newest = state.get("np_newest", 0); newest = last_newest; reached = False; pages = 0
    for page in range(1, 11):
        if should_stop(): break
        ps = poll_list(f"/new_pools?page={page}", "new_pools", "new_pools"); pages += 1
        if not ps: break
        cts = [p["created_at"] for p in ps if p["created_at"]]
        if cts: newest = max(newest, max(cts))
        if cts and min(cts) <= last_newest: reached = True; break
    if last_newest and not reached and not should_stop():
        log(f"new_pools coverage gap: 10 pages did not reach last newest ({iso(last_newest)})", "WARN")
        db.execute("INSERT INTO meta VALUES('np_gaps', '1') ON CONFLICT(k) DO UPDATE SET v=CAST(v AS INTEGER)+1")
    state["np_newest"] = newest
    log(f"new_pools pages={pages} reached_prev={int(reached)} newest={iso(newest) if newest else '-'}")

def do_trending(pages):
    for dur in ("5m", "1h", "6h", "24h"):
        for page in pages:
            if should_stop(): return
            poll_list(f"/trending_pools?duration={dur}&page={page}", f"trending_{dur}", "trending")

def snapshot_status(p, ev):
    if not p: return "missing"
    if (p["reserve"] or 0) <= 0: return "zero_liquidity"
    if ev and ev[0] and p["price"] and p["price"] < 0.1 * ev[0]: return "rugged_suspect"
    if (p["reserve"] or 0) < 1000: return "rugged_suspect"
    if (p["reserve"] or 0) < 5000: return "illiquid"
    return "ok"

def do_snapshots(rows, t):
    pools = sorted({r[1] for r in rows})[:30]
    code, js = api("/pools/multi/" + ",".join(pools), "multi_followup")
    if code != 200 or js is None:
        for r in rows:
            if r[1] in pools: db.execute("UPDATE followups SET attempts=attempts+1, next_try=? WHERE id=?", (t + 300, r[0]))
        db.commit(); log(f"multi_followup -> {code}", "WARN"); return
    got = {}
    for d in js.get("data", []):
        try: p = parse_pool(d); got[p["pool"]] = p
        except Exception: pass
    for fid, pool, label, due, attempts in rows:
        if pool not in pools: continue
        p = got.get(pool); attempts += 1
        if not p and attempts < 3:
            db.execute("UPDATE followups SET attempts=?, next_try=? WHERE id=?", (attempts, t + 300, fid)); continue
        note = None; st = None
        if not p:
            c2, _ = api(f"/pools/{pool}", "pool_single")
            st = "delisted" if c2 == 404 else "missing"; note = f"single_endpoint_http={c2}"
        ev = db.execute("SELECT price FROM candidates WHERE pool=?", (pool,)).fetchone()
        st = st or snapshot_status(p, ev)
        late_min = max(0.0, (t - due) / 60); late = 1 if t - due > LATE_S else 0
        if late: note = (note + ";" if note else "") + "late: exact-time price derivable from stored 1m/5m OHLCV"
        g = p or {}
        db.execute("INSERT INTO followup_results(followup_id,pool,label,due_ts,ts,late,late_min,attempts,status,price,fdv,mc,reserve,"
                   "vol_m5,vol_h1,vol_h24,tx_h1,tx_h24,note) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                   (fid, pool, label, due, t, late, late_min, attempts, st, g.get("price"), g.get("fdv"), g.get("mc"), g.get("reserve"),
                    g.get("vol_m5"), g.get("vol_h1"), g.get("vol_h24"), g.get("tx_h1"), g.get("tx_h24"), note))
        db.execute("UPDATE followups SET state='done', attempts=?, done_ts=? WHERE id=?", (attempts, t, fid))
    db.commit()

def fetch_ohlcv(pool, tf, agg, before, limit):
    code, js = api(f"/pools/{pool}/ohlcv/minute?aggregate={agg}&limit={limit}&before_timestamp={int(before)}&currency=usd", f"ohlcv_{tf}")
    if code == 404: return "delisted", 0, None
    if code != 200 or js is None: return "error", 0, None
    lst = (js.get("data") or {}).get("attributes", {}).get("ohlcv_list") or []
    for c in lst:
        db.execute("INSERT OR IGNORE INTO ohlcv VALUES(?,?,?,?,?,?,?,?)", (pool, tf, int(c[0]), c[1], c[2], c[3], c[4], c[5]))
    return ("ok" if lst else "empty"), len(lst), (max(c[0] for c in lst) if lst else None)

def do_ohlcv(row, t):
    fid, pool, label, due, attempts = row
    ev = db.execute("SELECT event_ts FROM candidates WHERE pool=?", (pool,)).fetchone()
    ev_ts = ev[0] if ev else due - 86400
    end = ev_ts + (86400 if label == "ohlcv24" else 172800)
    notes = []; statuses = []
    s, n, last = fetch_ohlcv(pool, "5m", 5, end, 1000); statuses.append(s); notes.append(f"5m:{s}:{n}")
    if label == "ohlcv24" and s not in ("delisted",):
        s1, n1, last1 = fetch_ohlcv(pool, "1m", 1, end, 1000); statuses.append(s1); notes.append(f"1m_a:{s1}:{n1}")
        s2, n2, _ = fetch_ohlcv(pool, "1m", 1, end - 60000, 1000); statuses.append(s2); notes.append(f"1m_b:{s2}:{n2}")
    if "error" in statuses and attempts < 3:
        db.execute("UPDATE followups SET attempts=attempts+1, next_try=? WHERE id=?", (t + 900, fid)); db.commit(); return
    if "delisted" in statuses: st = "delisted"
    elif all(x == "empty" for x in statuses): st = "missing"
    elif last and last < end - 3 * 3600: st = "candles_stopped"; notes.append(f"last_candle={iso(last)}")
    elif "error" in statuses: st = "partial_error"
    else: st = "ok"
    late = 1 if t - due > LATE_S else 0
    db.execute("INSERT INTO followup_results(followup_id,pool,label,due_ts,ts,late,late_min,attempts,status,note) VALUES(?,?,?,?,?,?,?,?,?,?)",
               (fid, pool, label, due, t, late, max(0, (t - due) / 60), attempts + 1, st, ";".join(notes)))
    db.execute("UPDATE followups SET state='done', attempts=attempts+1, done_ts=? WHERE id=?", (t, fid)); db.commit()

def do_watch(rows, t):
    pools = [r[0] for r in rows]
    code, js = api("/pools/multi/" + ",".join(pools), "multi_watch")
    if code != 200 or js is None:
        db.execute(f"UPDATE watch_state SET next_check=? WHERE pool IN ({','.join('?'*len(pools))})", [t + 300] + pools); db.commit(); return
    seen = set()
    for d in js.get("data", []):
        try: p = parse_pool(d); seen.add(p["pool"]); observe(p, "watch_refresh", t)
        except Exception as e: log(f"watch parse {e}", "ERROR")
    for pool in pools:
        if pool in seen: continue
        m = db.execute("UPDATE watch_state SET misses=misses+1, next_check=? WHERE pool=? RETURNING misses", (t + 900, pool)).fetchone()
        db.execute("INSERT INTO watch_snapshots(ts,pool,source,status,passes,fail_reasons) VALUES(?,?,'watch_refresh','missing',0,?)",
                   (t, pool, f"miss#{m[0] if m else '?'}"))
        if m and m[0] >= 3: db.execute("UPDATE watch_state SET status='missing', next_check=NULL WHERE pool=?", (pool,))
    db.commit()

def heartbeat(state, started):
    _data, _run, _dbp, _lock, heart, _logf = run_paths()
    tmp = heart + ".tmp"
    os.makedirs(os.path.dirname(heart), exist_ok=True)
    with open(tmp, "w") as fh:
        json.dump({"ts": now(), "iso": iso(now()), "pid": os.getpid(), "started": started,
                   **{k: v for k, v in state.items()}}, fh)
    os.replace(tmp, heart)

def _request_stop(signum, _frame):
    global STOP
    STOP = True
    try: log(f"signal {signum}, stopping after this iteration")
    except Exception: pass

def main():
    global db, DEADLINE, STOP
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    data, run, db_path, lock_path, _heart, _logf = run_paths()
    os.makedirs(os.path.join(run, "logs"), exist_ok=True)
    os.makedirs(data, exist_ok=True)
    run_seconds = env_float("RUN_SECONDS", 0)
    if run_seconds < 0: run_seconds = 0
    # Stay under the 6h Actions job cap even if a caller passes a huge value.
    if run_seconds > 16200: run_seconds = 16200
    lockfh = open(lock_path, "a+")
    try: fcntl.flock(lockfh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("another collector holds the lock; exiting"); return 0
    lockfh.seek(0); lockfh.truncate(); lockfh.write(str(os.getpid())); lockfh.flush()
    db = sqlite3.connect(db_path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.executescript(SCHEMA)
    clean_exit = False
    try:
        persist.hydrate(db, data)
        persist.record_start_gap(db)
        started = now()
        DEADLINE = (started + run_seconds) if run_seconds else None
        pend = db.execute("SELECT COUNT(*), SUM(next_try < ?) FROM followups WHERE state='pending'", (started,)).fetchone()
        log(f"START pid={os.getpid()} pending_followups={pend[0]} overdue={pend[1] or 0} "
            f"run_seconds={run_seconds or 'forever'} data={data} (overdue ones run now, flagged late)")
        st = db.execute("SELECT v FROM meta WHERE k='state'").fetchone()
        state = json.loads(st[0]) if st and st[0] else {}
        state.setdefault("np_newest", 0)
        heartbeat(state, started)
        _ready[0] = True
        maybe_checkpoint(force=True)
        last = {"np": 0, "tr": 0, "trd": 0}
        while not should_stop():
            try:
                if DEADLINE and now() >= DEADLINE - 10: break
                t = now()
                due = db.execute("SELECT id,pool,label,due_ts,attempts FROM followups WHERE state='pending' AND kind='snapshot' AND next_try<=? ORDER BY next_try LIMIT 60", (t,)).fetchall()
                if due: do_snapshots(due, t)  # follow-ups take priority over discovery
                t = now()
                if should_stop(): break
                if t - last["np"] >= NEW_POOLS_EVERY: do_new_pools(state); last["np"] = t
                elif t - last["trd"] >= TREND_DEEP_EVERY: do_trending(range(1, 11)); last["trd"] = last["tr"] = t
                elif t - last["tr"] >= TREND_EVERY: do_trending(range(1, 4)); last["tr"] = t
                else:
                    t = now()
                    fu = db.execute("SELECT id,pool,label,due_ts,attempts,kind FROM followups WHERE state='pending' AND next_try<=? ORDER BY next_try LIMIT 60", (t,)).fetchall()
                    snaps = [r[:5] for r in fu if r[5] == "snapshot"]; oh = [r[:5] for r in fu if r[5] == "ohlcv"]
                    if snaps: do_snapshots(snaps, t)
                    elif oh: do_ohlcv(oh[0], t)
                    else:
                        db.execute("UPDATE watch_state SET status='aged_out', next_check=NULL WHERE status='watch' AND created_at < ?", (t - AGE_MAX_H * 3600,))
                        w = db.execute("SELECT pool FROM watch_state WHERE status='watch' AND next_check IS NOT NULL AND next_check<=? ORDER BY next_check LIMIT 30", (t,)).fetchall()
                        if w: do_watch(w, t)
                        elif not sleep_q(3): break
                if should_stop(): break
                state["backlog_watch"] = db.execute("SELECT COUNT(*) FROM watch_state WHERE status='watch' AND next_check<=?", (now(),)).fetchone()[0]
                db.execute("INSERT INTO meta VALUES('state',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (json.dumps(state),)); db.commit()
                heartbeat(state, started)
                maybe_checkpoint()
            except Exception as e:
                log("loop error: " + "".join(traceback.format_exception_only(type(e), e)).strip(), "ERROR")
                if not sleep_q(20): break
        clean_exit = not STOP
        if clean_exit: log("STOP clean")
        else: log("STOP interrupted", "WARN")
    finally:
        _ready[0] = True
        try:
            if clean_exit: persist.mark_clean(db)
            maybe_checkpoint(force=True)
        except Exception as e:
            log("final checkpoint failed: " + "".join(traceback.format_exception_only(type(e), e)).strip(), "ERROR")
        try: db.commit(); db.close()
        except Exception: pass
        db = None
        try: fcntl.flock(lockfh, fcntl.LOCK_UN); lockfh.close()
        except Exception: pass
    return 0 if clean_exit else 1

if __name__ == "__main__":
    sys.exit(main())
