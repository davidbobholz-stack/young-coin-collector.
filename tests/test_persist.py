#!/usr/bin/env python3
"""Resume, export, coverage, and filter tests. No network."""
import json, os, sqlite3, sys, tempfile, time, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import collector
import persist

class PersistTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data = self.tmp.name
        os.environ["DATA_DIR"] = self.data
        persist.reset()
        self.db = sqlite3.connect(os.path.join(self.data, "live.db"))
        self.db.executescript(collector.SCHEMA)
        collector.db = self.db

    def tearDown(self):
        collector.db = None
        try: self.db.close()
        except Exception: pass
        persist.reset()
        self.tmp.cleanup()

    def _pool_row(self, pool, created, first, source="new_pools", fdv=1000.0):
        age = (first - created) / 3600
        self.db.execute("INSERT INTO pools_seen VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (pool, "Mint" + pool[:6], "So11111111111111111111111111111111111111112",
                         "Name", "pumpswap", created, first, source, fdv, 100.0, age))

    def test_roundtrip_and_no_duplicate_export(self):
        t = time.time()
        self._pool_row("PoolRecent111", t - 3 * 3600, t - 100)
        self.db.execute("INSERT INTO candidates(pool,token,name,dex,first_for_token,event_ts,created_at,age_h,price,fdv,mc,reserve,tx_h24,ath_proxy,ath_source,source) "
                        "VALUES('PoolRecent111','MintPool','Name','pumpswap',1,?,?,2,1,500000,500000,9000,1500,500000,'observed','new_pools')",
                        (t - 50, t - 3 * 3600))
        self.db.execute("INSERT INTO followups(pool,label,due_ts,kind,state,attempts,next_try) VALUES('PoolRecent111','15m',?,'snapshot','pending',0,?)",
                        (t + 900, t + 900))
        self.db.execute("INSERT INTO followup_results(pool,label,due_ts,ts,late,late_min,attempts,status,price,reserve,note) "
                        "VALUES('PoolRecent111','15m',?,?,0,0,1,'rugged_suspect',0.01,100,'kept')",
                        (t, t))
        self.db.execute("INSERT INTO watch_snapshots(ts,pool,source,mc,passes,fail_reasons,status) VALUES(?, 'PoolRecent111','new_pools',80000,0,'ath','ok')", (t,))
        self.db.execute("INSERT INTO ohlcv VALUES('PoolRecent111','5m',?,1,2,0.5,1.5,10)", (int(t),))
        self.db.execute("INSERT INTO downtime(start_ts,end_ts,minutes,kind) VALUES(?,?,?,'between_runs')", (t - 120, t - 60, 1))
        self.db.execute("INSERT INTO api_calls VALUES('2026-11-21','new_pools',3,0,0)")
        self.db.execute("INSERT INTO watch_state(pool,token,created_at,status,is_candidate,ath_mc_obs) VALUES('PoolRecent111','MintPool',?,'candidate',1,500000)", (t - 3 * 3600,))
        self.db.execute("INSERT INTO meta VALUES('state',?)", (json.dumps({"np_newest": 12345}),))
        persist.meta_set(self.db, "coverage_origin_ts", repr(t - 5000))
        self.db.commit()
        info = persist.checkpoint(self.db, self.data)
        self.assertEqual(info["counts"]["pools_seen"], 1)
        self.assertEqual(info["counts"]["candidates"], 1)
        self.assertEqual(info["counts"]["followup_results"], 1)
        self.assertEqual(info["counts"]["ohlcv"], 1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM followup_results").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM ohlcv").fetchone()[0], 0)
        info2 = persist.checkpoint(self.db, self.data)
        self.assertEqual(info2["counts"]["pools_seen"], 0)
        self.assertEqual(info2["counts"]["candidates"], 0)
        self.assertEqual(sum(1 for _ in persist.iter_part_files(self.data, "pools_seen.csv")
                             for _r in persist.iter_csv(_)), 1)
        # resume into a fresh database
        db2 = sqlite3.connect(os.path.join(self.data, "live2.db"))
        db2.executescript(collector.SCHEMA)
        persist.hydrate(db2, self.data)
        self.assertEqual(db2.execute("SELECT first_source FROM pools_seen WHERE pool='PoolRecent111'").fetchone()[0], "new_pools")
        self.assertEqual(db2.execute("SELECT COUNT(*) FROM candidates").fetchone()[0], 1)
        self.assertEqual(db2.execute("SELECT state FROM followups WHERE pool='PoolRecent111' AND label='15m'").fetchone()[0], "pending")
        self.assertEqual(db2.execute("SELECT is_candidate FROM watch_state WHERE pool='PoolRecent111'").fetchone()[0], 1)
        self.assertEqual(json.loads(db2.execute("SELECT v FROM meta WHERE k='state'").fetchone()[0])["np_newest"], 12345)
        self.assertEqual(db2.execute("SELECT calls FROM api_calls WHERE endpoint='new_pools'").fetchone()[0], 3)
        # rugged result is not reloaded into sqlite; it stays in the shard forever
        results = []
        for part in persist.iter_part_files(self.data, "followup_results.jsonl"):
            results.extend(persist.iter_jsonl(part))
        self.assertEqual(results[0]["status"], "rugged_suspect")
        db2.close()

    def test_old_pool_is_archived_not_reborn(self):
        old_created = 1577836800.0  # 2020-01-01
        old_seen = old_created + 120
        day = "2020-01-01"
        os.makedirs(os.path.join(self.data, "shards", day), exist_ok=True)
        path = os.path.join(self.data, "shards", day, "pools_seen.csv")
        with open(path, "w") as fh:
            fh.write("pool,token,quote,name,dex,created_at,first_seen,first_source,first_fdv,first_reserve,first_age_h\n")
            fh.write(f"OldPool,TokenOld,SOL,Old,pump-fun,{old_created!r},{old_seen!r},new_pools,10,1,1\n")
        persist.hydrate(self.db, self.data)
        self.assertIsNone(self.db.execute("SELECT 1 FROM pools_seen WHERE pool='OldPool'").fetchone())
        self.assertTrue(persist.is_archived("OldPool"))
        self.assertFalse(persist.is_archived("BrandNew"))
        # gzipped history still resolves
        self.assertTrue(any(p.endswith(".gz") for p in persist.iter_part_files(self.data, "pools_seen.csv")))
        db3 = sqlite3.connect(":memory:")
        db3.executescript(collector.SCHEMA)
        persist.hydrate(db3, self.data)
        self.assertTrue(persist.is_archived("OldPool"))
        db3.close()

    def test_gap_kinds_and_cohort(self):
        t = time.time()
        persist.meta_set(self.db, "last_alive_ts", repr(t - 600))
        persist.meta_set(self.db, "clean_shutdown", "1")
        persist.meta_set(self.db, "coverage_origin_ts", repr(t - 86400))
        persist.record_start_gap(self.db)
        row = self.db.execute("SELECT kind, minutes FROM downtime").fetchone()
        self.assertEqual(row[0], "between_runs")
        self.assertGreater(row[1], 9)
        self.db.execute("DELETE FROM downtime")
        persist.meta_set(self.db, "last_alive_ts", repr(t - 600))
        persist.meta_set(self.db, "clean_shutdown", "0")
        persist.record_start_gap(self.db)
        self.assertEqual(self.db.execute("SELECT kind FROM downtime").fetchone()[0], "killed")
        origin = t - 86400
        intervals = persist.merge_intervals([(t - 600, t - 60)])
        born = t - 300  # inside the gap
        self.assertFalse(persist.is_cohort_pool(born, born + 30, "new_pools", intervals, origin))
        ok_born = t - 5000
        self.assertTrue(persist.is_cohort_pool(ok_born, ok_born + 40, "new_pools", intervals, origin))
        self.assertFalse(persist.is_cohort_pool(ok_born, ok_born + 40, "trending_5m", intervals, origin))
        self.assertFalse(persist.is_cohort_pool(ok_born, ok_born + 3600, "new_pools", intervals, origin))
        self.assertFalse(persist.is_cohort_pool(origin - 10, origin + 10, "new_pools", intervals, origin))
        frac, span, down = persist.coverage(origin, origin + 1000, [(origin + 100, origin + 400)])
        self.assertAlmostEqual(down, 300)
        self.assertAlmostEqual(frac, 0.7)
        # overlapping gaps are not double-counted
        frac2, _, down2 = persist.coverage(0, 100, [(0, 40), (30, 50), (90, 120)])
        self.assertAlmostEqual(down2, 60)
        self.assertAlmostEqual(frac2, 0.4)

    def test_terminal_watch_is_archived_not_dropped(self):
        t = time.time()
        old = t - 49 * 3600
        self.db.execute("INSERT INTO watch_state(pool,token,created_at,status,misses,is_candidate) VALUES('Gone','tok',?,'missing',3,0)", (old,))
        self.db.execute("INSERT INTO watch_state(pool,token,created_at,status,misses,is_candidate) VALUES('Keep','tok',?,'candidate',0,1)", (old,))
        persist.checkpoint(self.db, self.data)
        self.assertIsNone(self.db.execute("SELECT 1 FROM watch_state WHERE pool='Gone'").fetchone())
        self.assertIsNotNone(self.db.execute("SELECT 1 FROM watch_state WHERE pool='Keep'").fetchone())
        closed = []
        for part in persist.iter_part_files(self.data, "watch_state_closed.jsonl"):
            closed.extend(persist.iter_jsonl(part))
        self.assertEqual([c["pool"] for c in closed], ["Gone"])
        self.assertEqual(closed[0]["status"], "missing")

    def test_filter_still_schedules_followups(self):
        t = time.time()
        base = dict(pool="CandPool", token="MintCand11111111111111111111111111111111",
                    quote="So11111111111111111111111111111111111111112", dex="pumpswap", name="CAND",
                    created_at=t - 2 * 3600, price=1.0, fdv=500_000, mc=500_000, reserve=20_000,
                    vol_m5=1, vol_h1=1, vol_h6=1, vol_h24=1, tx_h1=10, tx_h24=1500)
        collector.observe(dict(base), "new_pools", t)
        row = self.db.execute("SELECT mc, ath_source, first_for_token FROM candidates WHERE pool='CandPool'").fetchone()
        self.assertEqual(row[0], 500_000)
        self.assertEqual(row[1], "observed")
        self.assertEqual(row[2], 1)
        labels = sorted(r[0] for r in self.db.execute("SELECT label FROM followups WHERE pool='CandPool'"))
        self.assertEqual(labels, ["15m", "1h", "24h", "48h", "4h", "ohlcv24", "ohlcv48"])
        # too young: recorded in pools_seen, not a candidate
        young = dict(base, pool="YoungPool", token="MintYoung1111111111111111111111111111111", created_at=t - 60)
        collector.observe(young, "new_pools", t)
        self.assertIsNone(self.db.execute("SELECT 1 FROM candidates WHERE pool='YoungPool'").fetchone())
        self.assertIsNotNone(self.db.execute("SELECT 1 FROM pools_seen WHERE pool='YoungPool'").fetchone())
        # second pool for the same token is not first_for_token, and a repeat observe does not double-schedule
        again = dict(base, pool="CandPool2")
        collector.observe(again, "trending_5m", t)
        self.assertEqual(self.db.execute("SELECT first_for_token FROM candidates WHERE pool='CandPool2'").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM followups WHERE pool='CandPool'").fetchone()[0], 7)
        # archived old pool must not be rewritten as a new birth
        persist.hydrate(self.db, self.data)  # builds an empty index over this data dir; no shards yet
        # manually archive
        persist._known_pools.add("OldSeen")
        if persist._index is not None:
            persist._index.execute("INSERT OR IGNORE INTO seen VALUES('OldSeen',?,?,?)", (t - 50 * 3600, t - 49 * 3600, "new_pools"))
            persist._index.commit()
        oldp = dict(base, pool="OldSeen", token="MintOld111111111111111111111111111111111", created_at=t - 50 * 3600)
        collector.observe(oldp, "trending_5m", t)
        self.assertIsNone(self.db.execute("SELECT 1 FROM pools_seen WHERE pool='OldSeen'").fetchone())


if __name__ == "__main__":
    unittest.main()
