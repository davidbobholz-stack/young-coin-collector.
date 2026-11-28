#!/usr/bin/env python3
"""Put recovered OHLCV bars back on the data tip as new shard parts.

Does not delete or replace any existing file, including STOP and any .gz.
Run this only when told to restore, then commit the result as a normal
append on `data` (no squash, no force-push).

Usage:
  git fetch origin data && git worktree add --detach /tmp/data-tip origin/data
  python3 scripts/restore_ohlcv_tip.py --data /tmp/data-tip --csv /path/to/ohlcv_recovered.csv
  # inspect the printed counts, then commit and push HEAD:data
"""
import argparse, csv, datetime as dt, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import persist

HEADER = ["pool", "tf", "ts", "o", "h", "l", "c", "v"]


def _existing_keys(data_dir):
    keys = set()
    for part in persist.iter_part_files(data_dir, "ohlcv.csv"):
        for row in persist.iter_csv(part):
            if row.get("pool") and row.get("tf") and row.get("ts"):
                keys.add((row["pool"], row["tf"], str(row["ts"]).split(".")[0]))
    return keys


def _dest(day_dir):
    os.makedirs(day_dir, exist_ok=True)
    n = 1
    while True:
        path = os.path.join(day_dir, f"ohlcv-restored-{n}.csv")
        if not os.path.exists(path) and not os.path.exists(path + ".gz"):
            return path
        n += 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="checkout of the data branch")
    ap.add_argument("--csv", required=True, help="ohlcv_recovered.csv (pool,tf,ts,o,h,l,c,v)")
    args = ap.parse_args()
    if not os.path.isdir(args.data):
        raise SystemExit(f"not a directory: {args.data}")
    have = _existing_keys(args.data)
    added = 0
    days = {}
    with open(args.csv, newline="") as fh:
        for row in csv.DictReader(fh):
            ts = str(row["ts"]).split(".")[0]
            key = (row["pool"], row["tf"], ts)
            if key in have:
                continue
            have.add(key)
            day = dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).strftime("%Y-%m-%d")
            days.setdefault(day, []).append([row["pool"], row["tf"], ts, row.get("o"), row.get("h"), row.get("l"), row.get("c"), row.get("v")])
            added += 1
    for day, rows in sorted(days.items()):
        path = _dest(os.path.join(args.data, "shards", day))
        with open(path, "w", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(HEADER)
            w.writerows(rows)
        print(f"wrote {len(rows)} bars -> {path}")
    stop = os.path.join(args.data, "STOP")
    print(f"added_keys={added} readable_keys={len(have)} stop_present={os.path.exists(stop)}")
    print("STOP was not modified. Commit this as a normal append. Do not squash or force-push.")


if __name__ == "__main__":
    main()
