#!/usr/bin/env python3
"""Free public pump.fun launch and graduation feed. No API key.

Launches come from the newest coins. A graduation is the first time we see
complete=true with a pool address. Destination venue is pumpswap when
pump_swap_pool is set, otherwise raydium when raydium_pool is set.
transfer_fee_bps and canonical_pool_liquidity_usd are taken from that same
response (age 0). The AMM fee schedule is not in this payload.
"""
import json, time, urllib.request
import collector
import wallet_log

PUMP = "https://frontend-api-v3.pump.fun/coins"
_next_launch = [0.0]
_next_grad = [0.0]


def _get(url):
    req = urllib.request.Request(url, headers=collector.UA)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        collector.log(f"pump.fun {url.split('?')[0]} {e}", "WARN")
        return None


def _venue(coin):
    if coin.get("pump_swap_pool"):
        return "pumpswap", coin.get("pump_swap_pool")
    if coin.get("raydium_pool"):
        return "raydium", coin.get("raydium_pool")
    if coin.get("pool_address"):
        return coin.get("protocol") or "pump", coin.get("pool_address")
    return "pump-fun", coin.get("bonding_curve")


def _upsert(db, coin, now_ts):
    mint = coin.get("mint")
    if not mint:
        return None
    venue, pool = _venue(coin)
    created = coin.get("created_timestamp")
    created_s = (created / 1000.0) if created and created > 10_000_000_000 else created
    prev = db.execute("SELECT complete, pool FROM pump_coins WHERE mint=?", (mint,)).fetchone()
    db.execute(
        "INSERT INTO pump_coins(mint,name,symbol,created_at,bonding_curve,complete,pool,venue,first_seen,graduated_at,fee_bps,reserve_usd) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(mint) DO UPDATE SET "
        "complete=excluded.complete, pool=COALESCE(excluded.pool, pump_coins.pool), venue=excluded.venue, "
        "fee_bps=excluded.fee_bps, reserve_usd=excluded.reserve_usd, "
        "graduated_at=COALESCE(pump_coins.graduated_at, excluded.graduated_at)",
        (mint, coin.get("name"), coin.get("symbol"), created_s, coin.get("bonding_curve"),
         1 if coin.get("complete") else 0, pool, venue, now_ts,
         now_ts if coin.get("complete") else None, coin.get("transfer_fee_bps"),
         coin.get("canonical_pool_liquidity_usd")))
    return prev


def _first_swap(pool):
    """Oldest signature among at most 3 pages. swap_is_first only if the last page was short."""
    before = None
    oldest = None
    pages = 0
    for _ in range(3):
        opts = {"limit": wallet_log.SIG_PAGE}
        if before:
            opts["before"] = before
        sigs = wallet_log.rpc("getSignaturesForAddress", [pool, opts])
        pages += 1
        if not sigs:
            return oldest, 1 if pages else 0, None
        oldest = sigs[-1]
        before = oldest.get("signature")
        if len(sigs) < wallet_log.SIG_PAGE:
            return oldest, 1, oldest.get("blockTime")
    return oldest, 0, oldest.get("blockTime") if oldest else None


def _graduation(db, coin, now_ts):
    venue, pool = _venue(coin)
    if not pool or venue == "pump-fun":
        return
    mint = coin["mint"]
    cur = db.execute(
        "INSERT OR IGNORE INTO pump_events(ts,observed_ts,block_time,kind,mint,symbol,pool,venue,fee_bps,reserve_usd,"
        "reading_age_s,swap_signature,swap_is_first,slot,note) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (now_ts, now_ts, None, "graduation", mint, coin.get("symbol"), pool, venue,
         coin.get("transfer_fee_bps"), coin.get("canonical_pool_liquidity_usd"), 0.0,
         None, None, None,
         "fee_bps is pump transfer_fee_bps, not the AMM fee; reserve_usd is canonical_pool_liquidity_usd from the same response"))
    if not cur.rowcount:
        return
    collector.log(f"GRADUATION {coin.get('symbol')} {mint} venue={venue} pool={pool}")
    sig, is_first, bt = _first_swap(pool)
    if not sig:
        return
    db.execute("UPDATE pump_events SET swap_signature=?, swap_is_first=?, block_time=?, slot=? WHERE kind='graduation' AND mint=? AND pool=?",
               (sig.get("signature"), is_first, bt, sig.get("slot"), mint, pool))
    tx = wallet_log.rpc("getTransaction", [sig.get("signature"), {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
    if tx:
        wallet_log._record_swap(db, pool, mint, venue, tx, sig.get("signature"))


def poll(db):
    if collector.should_stop():
        return
    now_ts = time.time()
    if now_ts >= _next_launch[0]:
        _next_launch[0] = now_ts + 45
        coins = _get(PUMP + "?offset=0&limit=50&sort=created_timestamp&order=DESC&includeNsfw=false") or []
        for coin in coins:
            prev = _upsert(db, coin, now_ts)
            if prev is None:
                venue, pool = _venue(coin)
                db.execute(
                    "INSERT OR IGNORE INTO pump_events(ts,observed_ts,block_time,kind,mint,symbol,pool,venue,fee_bps,reserve_usd,"
                    "reading_age_s,note) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (now_ts, now_ts, (coin.get("created_timestamp") or 0) / 1000.0, "launch", coin.get("mint"),
                     coin.get("symbol"), pool, "pump-fun", coin.get("transfer_fee_bps"),
                     coin.get("canonical_pool_liquidity_usd"), 0.0,
                     "bonding-curve launch; venue tag pump-fun until graduation"))
            elif coin.get("complete") and not prev[0]:
                _graduation(db, coin, now_ts)
    if now_ts >= _next_grad[0]:
        _next_grad[0] = now_ts + 60
        coins = _get(PUMP + "?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false") or []
        for coin in coins:
            if not coin.get("complete"):
                _upsert(db, coin, now_ts)
                continue
            prev = _upsert(db, coin, now_ts)
            if prev is None or not prev[0]:
                _graduation(db, coin, now_ts)
    db.commit()
