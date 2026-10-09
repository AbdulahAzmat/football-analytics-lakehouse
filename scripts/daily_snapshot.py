"""Daily incremental pull from football-data.org.

Phase 1 pulled only date-scoped match updates, which came to 15-30 KB on a
match day and nothing at all on a rest day. That is too thin to be a
meaningful incremental load, and it threw away the thing that actually changes
day to day.

This script instead captures a full daily SNAPSHOT of mutable state for every
tracked competition:

  * standings  - the table as it stood today
  * scorers    - the scoring charts as they stood today
  * teams      - squad reference data
  * matches    - fixtures and results in a date window

Snapshotting standings and scorers daily is what makes change-over-time
analysis possible: diffing consecutive snapshots gives you a team's league
position by day, and goals scored per player per day, neither of which the
API exposes directly as a time series.

Output: raw/football-data/daily/<date>/<feed>_<COMP>.json
Typical size: ~1.4 MB/day across 12 competitions.

Usage:
    export FOOTBALL_DATA_TOKEN=...
    python scripts/daily_snapshot.py
    python scripts/daily_snapshot.py --date 2026-10-09 --out raw/football-data/daily
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

BASE = "https://api.football-data.org/v4"

COMPETITIONS = ["PL", "ELC", "PD", "BL1", "SA", "FL1",
                "DED", "PPL", "BSA", "CL", "EC", "WC"]

# Free tier allows 10 requests/minute. 7s spacing keeps us comfortably under.
SLEEP_SECONDS = 7.0


def fetch(url: str, token: str, retries: int = 3):
    req = urllib.request.Request(url, headers={"X-Auth-Token": token})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read(), 200
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                return None, e.code          # expected for cups without tables
            if e.code == 429:                 # rate limited: back off and retry
                time.sleep(20 * (attempt + 1))
                continue
            return None, e.code
        except Exception:                      # noqa: BLE001 transient network
            time.sleep(3 * (attempt + 1))
    return None, 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--out", default="raw/football-data/daily")
    ap.add_argument("--window-days", type=int, default=3,
                    help="match window size ending on --date")
    args = ap.parse_args(argv)

    token = os.environ.get("FOOTBALL_DATA_TOKEN")
    if not token:
        print("FOOTBALL_DATA_TOKEN is not set", file=sys.stderr)
        return 2

    snap = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    outdir = os.path.join(args.out, snap)
    os.makedirs(outdir, exist_ok=True)

    date_to = date.fromisoformat(snap)
    date_from = date_to - timedelta(days=args.window_days)

    total = 0
    skipped = []

    # 1. per-competition snapshots: standings, scorers, teams
    for comp in COMPETITIONS:
        for feed, url in (
            ("standings", f"{BASE}/competitions/{comp}/standings"),
            ("scorers",   f"{BASE}/competitions/{comp}/scorers?limit=100"),
            ("teams",     f"{BASE}/competitions/{comp}/teams"),
        ):
            body, code = fetch(url, token)
            if body is None:
                skipped.append(f"{feed}/{comp} (HTTP {code})")
            else:
                path = os.path.join(outdir, f"{feed}_{comp}.json")
                with open(path, "wb") as f:
                    f.write(body)
                total += len(body)
            time.sleep(SLEEP_SECONDS)

    # 2. one date-windowed matches pull covering all competitions
    comps = ",".join(COMPETITIONS)
    body, code = fetch(
        f"{BASE}/matches?competitions={comps}&dateFrom={date_from}&dateTo={date_to}", token
    )
    if body is not None:
        with open(os.path.join(outdir, "matches_window.json"), "wb") as f:
            f.write(body)
        total += len(body)
    else:
        skipped.append(f"matches_window (HTTP {code})")

    manifest = {
        "snapshot_date": snap,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "competitions": COMPETITIONS,
        "match_window": {"from": str(date_from), "to": str(date_to)},
        "bytes": total,
        "files": len(os.listdir(outdir)),
        "skipped": skipped,
    }
    with open(os.path.join(outdir, "_manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"snapshot {snap}: {manifest['files']} files, {total/1e6:.2f} MB -> {outdir}")
    if skipped:
        print(f"  skipped (expected for cups / out-of-cycle tournaments): {len(skipped)}")
        for s in skipped:
            print(f"    - {s}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
