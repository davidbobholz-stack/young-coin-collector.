#!/usr/bin/env python3
"""Tracked-wallet buys, sells, and SOL transfers. Free public RPC only.

TRACKED_WALLETS is a JSON array in the environment (a GitHub Actions secret).
When it is unset or invalid, poll() does nothing. The list is never written
to the repo. Venue is recorded and never used to drop an event.
"""
import json, os, time, urllib.request
import collector

RPC = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
MIN_GAP = 1.0 / 3.0          # about 3 requests/second
MAX_CALLS = 4                # per collector iteration, so discovery is not stalled
FOLLOW_S = 3600
SIG_PAGE = 20
WSOL = "So11111111111111111111111111111111111111112"
_last = [0.0]
_idx = [0]
_skipped_logged = [False]


def load_wallets():
    raw = os.environ.get("TRACKED_WALLETS", "").strip()
    if not raw:
        if not _skipped_logged[0]:
            collector.log("TRACKED_WALLETS unset; wallet logger skipped")
            _skipped_logged[0] = True
        return []
    try:
        data = json.loads(raw)
    except Exception:
        collector.log("TRACKED_WALLETS is not a JSON array; wallet logger skipped", "WARN")
        return []
    if not isinstance(data, list):
        collector.log("TRACKED_WALLETS is not a JSON array; wallet logger skipped", "WARN")
        return []
    return [str(x) for x in data if isinstance(x, str) and x]


def rpc(method, params):
    """One JSON-RPC call, spaced to about 3/s. Returns the result or None."""
    wait = _last[0] + MIN_GAP - time.time()
    if wait > 0:
        time.sleep(wait)
    if collector.should_stop():
        return None
    _last[0] = time.time()
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(RPC, data=body, headers={"Content-Type": "application/json", "User-Agent": collector.UA["User-Agent"]})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            js = json.loads(r.read().decode())
    except Exception as e:
        collector.log(f"solana rpc {method} error {e}", "WARN")
        time.sleep(2)
        return None
    if js.get("error"):
        collector.log(f"solana rpc {method} {js['error']}", "WARN")
        if (js["error"] or {}).get("code") == 429:
            time.sleep(2)
        return None
    return js.get("result")


def _pubkey(key):
    if isinstance(key, str):
        return key
    if isinstance(key, dict):
        return key.get("pubkey")
    return None


def _ui(balance):
    ui = (balance or {}).get("uiTokenAmount") or {}
    if ui.get("uiAmount") is not None:
        return float(ui["uiAmount"])
    amount, dec = ui.get("amount"), int(ui.get("decimals") or 0)
    if amount is None:
        return 0.0
    return int(amount) / (10 ** dec)


def parse_transaction(tx, tracked, signature=None):
    """Pull buys, sells, and tracked-to-tracked SOL transfers out of one transaction.

    Venue is not a filter. Price and vault balances are taken from the transaction
    itself when they are present; this does not do an extra account read.
    """
    if not tx or (tx.get("meta") or {}).get("err"):
        return []
    meta = tx.get("meta") or {}
    msg = ((tx.get("transaction") or {}).get("message") or {})
    sig = signature or ((tx.get("transaction") or {}).get("signatures") or [None])[0]
    slot = tx.get("slot")
    block_time = tx.get("blockTime")
    keys = [_pubkey(k) for k in (msg.get("accountKeys") or [])]
    tracked = set(tracked)
    events = []
    seen_xfer = set()

    def walk(instrs):
        for ins in instrs or []:
            if not isinstance(ins, dict):
                continue
            parsed = ins.get("parsed")
            if isinstance(parsed, dict) and parsed.get("type") == "transfer" and ins.get("program") == "system":
                info = parsed.get("info") or {}
                src, dst, lam = info.get("source"), info.get("destination"), info.get("lamports")
                if src in tracked and dst in tracked and src != dst and lam:
                    key = (sig, src, dst)
                    if key not in seen_xfer:
                        seen_xfer.add(key)
                        events.append({
                            "signature": sig, "wallet": src, "kind": "sol_transfer", "mint": "",
                            "token_amount": None, "sol_amount": int(lam) / 1e9,
                            "sender": src, "receiver": dst, "slot": slot, "block_time": block_time,
                        })
            walk(ins.get("instructions"))

    walk(msg.get("instructions"))
    for inner in meta.get("innerInstructions") or []:
        walk(inner.get("instructions") if isinstance(inner, dict) else None)

    pre_b = meta.get("preBalances") or []
    post_b = meta.get("postBalances") or []
    sol_of = {}
    for i, pub in enumerate(keys):
        if pub in tracked and i < len(pre_b) and i < len(post_b):
            sol_of[pub] = (post_b[i] - pre_b[i]) / 1e9

    def by_owner(rows):
        out = {}
        for b in rows or []:
            owner, mint = b.get("owner"), b.get("mint")
            if owner in tracked and mint:
                out.setdefault(owner, {})
                out[owner][mint] = out[owner].get(mint, 0.0) + _ui(b)
        return out

    pre, post = by_owner(meta.get("preTokenBalances")), by_owner(meta.get("postTokenBalances"))
    # Vault-looking balances: token accounts whose owner is not a tracked wallet.
    vault_quote = vault_base = None
    for b in meta.get("postTokenBalances") or []:
        if b.get("owner") in tracked:
            continue
        amt = _ui(b)
        if b.get("mint") == WSOL:
            vault_quote = amt if vault_quote is None else max(vault_quote, amt)
        else:
            vault_base = amt if vault_base is None else max(vault_base, amt)
    for owner in set(pre) | set(post):
        mints = set(pre.get(owner, {})) | set(post.get(owner, {}))
        sol_delta = sol_of.get(owner)
        for mint in mints:
            delta = post.get(owner, {}).get(mint, 0.0) - pre.get(owner, {}).get(mint, 0.0)
            if abs(delta) < 1e-12:
                continue
            kind = "buy" if delta > 0 else "sell"
            events.append({
                "signature": sig, "wallet": owner, "kind": kind, "mint": mint,
                "token_amount": abs(delta),
                "sol_amount": abs(sol_delta) if sol_delta is not None else None,
                "sender": None, "receiver": None, "slot": slot, "block_time": block_time,
                "price": (abs(sol_delta) / abs(delta)) if sol_delta and delta else None,
                "reserve_quote": vault_quote, "reserve_base": vault_base,
            })
    return events


def _distinct_24h(db, wallet, mint, now_ts):
    n = db.execute(
        "SELECT COUNT(DISTINCT mint) FROM wallet_events WHERE wallet=? AND kind='buy' AND COALESCE(block_time, observed_ts)>=?",
        (wallet, now_ts - 86400)).fetchone()[0]
    have = db.execute(
        "SELECT 1 FROM wallet_events WHERE wallet=? AND kind='buy' AND mint=? AND COALESCE(block_time, observed_ts)>=?",
        (wallet, mint, now_ts - 86400)).fetchone()
    return n + (0 if have else 1)


def _insert_event(db, ev, observed):
    note = ev.get("note")
    cur = db.execute(
        "INSERT OR IGNORE INTO wallet_events(signature,wallet,kind,mint,token_amount,sol_amount,sender,receiver,slot,"
        "block_time,observed_ts,ts,pool,venue,fee_bps,reserve_usd,reading_age_s,distinct_tokens_24h,note) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (ev.get("signature"), ev.get("wallet"), ev.get("kind"), ev.get("mint") or "",
         ev.get("token_amount"), ev.get("sol_amount"), ev.get("sender"), ev.get("receiver"),
         ev.get("slot"), ev.get("block_time"), observed, observed, ev.get("pool"), ev.get("venue"),
         ev.get("fee_bps"), ev.get("reserve_usd"), ev.get("reading_age_s"),
         _distinct_24h(db, ev.get("wallet"), ev.get("mint") or "", observed) if ev.get("kind") == "buy" else None,
         note))
    if cur.rowcount and ev.get("kind") in ("buy", "sell") and ev.get("mint"):
        _open_follow(db, ev, observed)
    return cur.rowcount


def _open_follow(db, ev, observed):
    """Schedule 60 minutes of pool swaps so a fill >=60s after observed time can be found."""
    pool = ev.get("pool")
    if not pool:
        return
    db.execute(
        "INSERT INTO pool_follows(pool,mint,venue,trigger_observed_ts,until_ts,last_sig,covered_until) "
        "VALUES(?,?,?,?,?,?,NULL) ON CONFLICT(pool) DO UPDATE SET "
        "until_ts=MAX(pool_follows.until_ts, excluded.until_ts), "
        "trigger_observed_ts=MIN(pool_follows.trigger_observed_ts, excluded.trigger_observed_ts)",
        (pool, ev.get("mint"), ev.get("venue"), observed, observed + FOLLOW_S, None))


def _fee_from_pump(db, mint):
    try:
        row = db.execute("SELECT fee_bps, pool, venue, reserve_usd FROM pump_coins WHERE mint=?", (mint,)).fetchone()
    except Exception:
        return None
    return row


def _enrich_one(db, observed_now):
    """One GeckoTerminal read for the freshest buy/sell still inside 60s. No AMM fee is available there."""
    row = db.execute(
        "SELECT id, mint, observed_ts FROM wallet_events WHERE kind IN ('buy','sell') AND reserve_usd IS NULL "
        "AND observed_ts>=? ORDER BY observed_ts LIMIT 1",
        (observed_now - 60,)).fetchone()
    if not row:
        return
    eid, mint, seen = row
    code, js = collector.api(f"/tokens/{mint}/pools?page=1", "wallet_pool")
    age = observed_now - (seen or observed_now)
    pump = _fee_from_pump(db, mint)
    fee = pump[0] if pump else None
    note = "amm_fee_not_in_free_gecko"
    if fee is not None:
        note = "fee_bps_is_pump_transfer_fee_not_amm"
    if code != 200 or not js or not js.get("data"):
        db.execute("UPDATE wallet_events SET reading_age_s=?, fee_bps=?, note=? WHERE id=?",
                   (age, fee, note + ";no_gecko_pool", eid))
        return
    top = js["data"][0]
    attrs = top.get("attributes") or {}
    dex = ((top.get("relationships") or {}).get("dex") or {}).get("data") or {}
    venue = dex.get("id")
    pool = attrs.get("address")
    db.execute(
        "UPDATE wallet_events SET pool=COALESCE(pool,?), venue=COALESCE(venue,?), reserve_usd=?, fee_bps=?, "
        "reading_age_s=?, note=? WHERE id=?",
        (pool, venue, collector.f(attrs.get("reserve_in_usd")), fee, age, note, eid))
    if pool:
        ev = db.execute("SELECT wallet, kind, mint, signature FROM wallet_events WHERE id=?", (eid,)).fetchone()
        _open_follow(db, {"pool": pool, "mint": mint, "venue": venue, "kind": ev[1], "signature": ev[3], "wallet": ev[0]}, seen)


def _record_swap(db, pool, mint, venue, tx, signature):
    meta = tx.get("meta") or {}
    if meta.get("err"):
        return
    observed = time.time()
    block_time = tx.get("blockTime")
    slot = tx.get("slot")
    # Execution price from the largest non-tracked token delta vs SOL delta on any account.
    keys = [_pubkey(k) for k in (((tx.get("transaction") or {}).get("message") or {}).get("accountKeys") or [])]
    pre_b, post_b = meta.get("preBalances") or [], meta.get("postBalances") or []
    sol_delta = None
    if keys and pre_b and post_b:
        # fee payer is index 0; use the biggest absolute SOL change that isn't only the fee
        deltas = [(post_b[i] - pre_b[i]) / 1e9 for i in range(min(len(keys), len(pre_b), len(post_b)))]
        if deltas:
            sol_delta = max(deltas, key=lambda x: abs(x))
    token_delta = None
    reserve_quote = reserve_base = None
    for b in meta.get("postTokenBalances") or []:
        amt = _ui(b)
        if b.get("mint") == WSOL:
            reserve_quote = amt if reserve_quote is None else max(reserve_quote, amt)
        elif mint and b.get("mint") == mint:
            reserve_base = amt if reserve_base is None else max(reserve_base, amt)
    for side in (meta.get("preTokenBalances") or [], meta.get("postTokenBalances") or []):
        pass
    pre_m = {}
    post_m = {}
    for b in meta.get("preTokenBalances") or []:
        if not mint or b.get("mint") == mint:
            pre_m[b.get("owner")] = pre_m.get(b.get("owner"), 0) + _ui(b)
    for b in meta.get("postTokenBalances") or []:
        if not mint or b.get("mint") == mint:
            post_m[b.get("owner")] = post_m.get(b.get("owner"), 0) + _ui(b)
    best = 0.0
    for owner in set(pre_m) | set(post_m):
        d = abs(post_m.get(owner, 0) - pre_m.get(owner, 0))
        if d > best:
            best = d
    token_delta = best or None
    price = (abs(sol_delta) / token_delta) if sol_delta and token_delta else None
    db.execute(
        "INSERT OR IGNORE INTO pool_swaps(signature,pool,mint,venue,block_time,observed_ts,ts,slot,price,sol_amount,"
        "token_amount,reserve_quote,reserve_usd,note) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (signature, pool, mint, venue, block_time, observed, observed, slot, price,
         abs(sol_delta) if sol_delta is not None else None, token_delta, reserve_quote, None,
         "reserves_are_max_vault_balances_inside_this_tx; quote reserve is WSOL when the tx touched it"))


def _follow_one(db, calls):
    """Walk one pool's signatures backward toward the trigger. At most one page plus a few transactions."""
    now = time.time()
    row = db.execute(
        "SELECT pool, mint, venue, trigger_observed_ts, until_ts, last_sig, covered_until FROM pool_follows "
        "WHERE until_ts>=? ORDER BY until_ts LIMIT 1", (now - 120,)).fetchone()
    if not row or calls[0] >= MAX_CALLS:
        return
    pool, mint, venue, trigger, until, last_sig, covered = row
    opts = {"limit": SIG_PAGE}
    if last_sig:
        opts["before"] = last_sig
    sigs = rpc("getSignaturesForAddress", [pool, opts])
    calls[0] += 1
    if not sigs:
        db.execute("UPDATE pool_follows SET covered_until=? WHERE pool=?", (covered or now, pool))
        return
    oldest = sigs[-1].get("signature")
    oldest_bt = None
    for s in reversed(sigs):  # oldest first
        if calls[0] >= MAX_CALLS:
            break
        sig = s.get("signature")
        bt = s.get("blockTime")
        if bt is not None:
            oldest_bt = bt if oldest_bt is None else min(oldest_bt, bt)
        if bt is not None and trigger and bt + 5 < trigger:
            oldest = sig
            break
        if bt is not None and until and bt > until + 180:
            continue
        tx = rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
        calls[0] += 1
        if tx:
            _record_swap(db, pool, mint, venue, tx, sig)
        oldest = sig
    db.execute("UPDATE pool_follows SET last_sig=?, covered_until=? WHERE pool=?",
               (oldest, oldest_bt or time.time(), pool))


def _wallet_one(db, wallets, calls):
    if not wallets or calls[0] >= MAX_CALLS:
        return
    w = wallets[_idx[0] % len(wallets)]
    _idx[0] += 1
    prev = db.execute("SELECT last_sig FROM wallet_cursor WHERE wallet=?", (w,)).fetchone()
    prev_sig = prev[0] if prev else None
    sigs = rpc("getSignaturesForAddress", [w, {"limit": SIG_PAGE}])
    calls[0] += 1
    if not sigs:
        return
    fresh = []
    saw_prev = prev_sig is None
    for s in sigs:  # newest first
        if prev_sig and s.get("signature") == prev_sig:
            saw_prev = True
            break
        fresh.append(s)
    if prev_sig and not saw_prev:
        collector.log(f"wallet signature page gap for {w[:6]}…; older signatures in this burst were not fetched", "WARN")
    for s in reversed(fresh):
        if calls[0] >= MAX_CALLS:
            break
        sig = s.get("signature")
        tx = rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
        calls[0] += 1
        observed = time.time()
        if tx:
            for ev in parse_transaction(tx, wallets, sig):
                pump = _fee_from_pump(db, ev.get("mint")) if ev.get("mint") else None
                if pump:
                    ev["fee_bps"] = pump[0]
                    ev["pool"] = ev.get("pool") or pump[1]
                    ev["venue"] = ev.get("venue") or pump[2]
                    ev["note"] = "fee_bps_is_pump_transfer_fee_not_amm; quote reserve in token units is not in this payload"
                else:
                    ev["note"] = "amm_fee_not_in_free_gecko; quote reserve in token units is not in the gecko payload"
                _insert_event(db, ev, observed)
    newest = sigs[0].get("signature")
    db.execute("INSERT INTO wallet_cursor(wallet,last_sig) VALUES(?,?) ON CONFLICT(wallet) DO UPDATE SET last_sig=excluded.last_sig",
               (w, newest))


def poll(db):
    wallets = load_wallets()
    if not wallets or collector.should_stop():
        return
    calls = [0]
    _follow_one(db, calls)
    _wallet_one(db, wallets, calls)
    if calls[0] < MAX_CALLS:
        _enrich_one(db, time.time())
    db.commit()
