"""
Ingest StatsBomb open data (second source) into the raw landing zone.

Why a second source: football-data.org's free tier gives broad coverage
(12 competitions, 4 seasons) but only match-level aggregates - a scoreline,
no detail about what happened inside the match. StatsBomb open data gives the
opposite shape: a small number of matches, but every on-ball event (pass,
shot, carry, pressure) with coordinates and outcomes. Together they give the
lakehouse both breadth and depth, and the Bundesliga 2023/24 season exists in
BOTH sources, so the two can genuinely be joined on competition + season +
teams + date.

Data licence: StatsBomb open data is free for public use under the StatsBomb
user agreement, which requires attribution. See data/statsbomb/LICENCE_NOTE.md.

Usage:
    python scripts/ingest_statsbomb.py --out data/statsbomb
    python scripts/ingest_statsbomb.py --out data/statsbomb --competitions 9/281
"""

import argparse
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"

# competition_id/season_id pairs.
#   9/281  = 1. Bundesliga 2023/2024  -> overlaps football-data.org BL1 season 2023
#   7/235  = Ligue 1 2022/2023        -> same competition as football-data.org FL1
DEFAULT_COMPETITIONS = ["9/281", "7/235"]


def fetch(url: str, retries: int = 3) -> bytes:
    last = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"failed after {retries} attempts: {url}: {last}")


def write(path: str, payload: bytes) -> int:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(payload)
    return len(payload)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/statsbomb")
    ap.add_argument("--competitions", nargs="*", default=DEFAULT_COMPETITIONS,
                    help="competition_id/season_id pairs, e.g. 9/281")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--skip-existing", action="store_true", default=True)
    args = ap.parse_args()

    total_bytes = 0
    manifest = {"competitions": [], "files": 0, "bytes": 0}

    # 1. competitions index
    total_bytes += write(f"{args.out}/competitions.json", fetch(f"{BASE}/competitions.json"))

    # 2. match indexes, then collect match ids
    match_ids = []
    for cs in args.competitions:
        comp_id, season_id = cs.split("/")
        raw = fetch(f"{BASE}/matches/{comp_id}/{season_id}.json")
        total_bytes += write(f"{args.out}/matches/{comp_id}/{season_id}.json", raw)
        matches = json.loads(raw)
        ids = [m["match_id"] for m in matches]
        match_ids.extend(ids)
        manifest["competitions"].append({
            "competition_id": int(comp_id),
            "season_id": int(season_id),
            "competition_name": matches[0]["competition"]["competition_name"],
            "season_name": matches[0]["season"]["season_name"],
            "matches": len(ids),
        })
        print(f"  {cs}: {len(ids)} matches", flush=True)

    # 3. events + lineups per match, in parallel
    def grab(mid: int):
        got = 0
        for kind in ("events", "lineups"):
            dest = f"{args.out}/{kind}/{mid}.json"
            if args.skip_existing and os.path.exists(dest) and os.path.getsize(dest) > 0:
                got += os.path.getsize(dest)
                continue
            got += write(dest, fetch(f"{BASE}/{kind}/{mid}.json"))
        return got

    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(grab, mid): mid for mid in match_ids}
        for fut in as_completed(futures):
            mid = futures[fut]
            try:
                total_bytes += fut.result()
            except Exception as e:  # noqa: BLE001
                print(f"  FAILED match {mid}: {e}", file=sys.stderr, flush=True)
                continue
            done += 1
            if done % 10 == 0:
                print(f"  {done}/{len(match_ids)} matches  ({total_bytes/1e6:.1f} MB)", flush=True)

    file_count = sum(len(files) for _, _, files in os.walk(args.out))
    manifest["files"] = file_count
    manifest["bytes"] = total_bytes
    with open(f"{args.out}/_manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nDone. {file_count} files, {total_bytes/1e6:.1f} MB -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
