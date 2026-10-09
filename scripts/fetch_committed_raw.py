"""Download the committed football-data.org raw files from GitHub into a target dir.

Why this exists: Databricks Repos does not bring large files down with the
clone, so `data/full_load/` is absent in the Databricks workspace even though it
is committed here. Re-pulling from the football-data.org API is possible but
needs the API key and ~9 minutes of rate-limited calls, and would produce
*different* data (the API moves on), which breaks reproducibility.

So instead we fetch the exact committed files over raw.githubusercontent.com.
Same bytes, no API key, no rate limit.

Usage:
    python scripts/fetch_committed_raw.py --out /dbfs/FileStore/football_lakehouse/raw/football-data/full_load
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

OWNER = "AbdulahAzmat"
REPO = "football-analytics-lakehouse"
BRANCH = "main"
SRC_DIR = "data/full_load"

API = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{SRC_DIR}?ref={BRANCH}"
RAW = f"https://raw.githubusercontent.com/{OWNER}/{REPO}/{BRANCH}/{SRC_DIR}"


def normalise_out(path: str) -> str:
    """dbfs:/x -> /dbfs/x, because python file IO needs the FUSE mount."""
    if path.startswith("dbfs:/"):
        fixed = "/dbfs/" + path[len("dbfs:/"):].lstrip("/")
        print(f"note: rewriting {path} -> {fixed}")
        return fixed
    return path


def get(url: str, retries: int = 3) -> bytes:
    last = None
    for i in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"failed: {url}: {last}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args(argv)

    out = normalise_out(args.out)
    os.makedirs(out, exist_ok=True)

    listing = json.loads(get(API))
    names = [e["name"] for e in listing if e.get("type") == "file" and e["name"].endswith(".json")]
    if not names:
        print("no files listed - is the repo public and the branch correct?", file=sys.stderr)
        return 1
    print(f"{len(names)} files to fetch")

    total = 0

    def grab(name: str) -> int:
        dest = os.path.join(out, name)
        if os.path.exists(dest) and os.path.getsize(dest) > 0:
            return os.path.getsize(dest)
        body = get(f"{RAW}/{name}")
        with open(dest, "wb") as f:
            f.write(body)
        return len(body)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(grab, n): n for n in names}
        done = 0
        for fut in as_completed(futs):
            try:
                total += fut.result()
            except Exception as e:  # noqa: BLE001
                print(f"  FAILED {futs[fut]}: {e}", file=sys.stderr)
                continue
            done += 1
            if done % 20 == 0:
                print(f"  {done}/{len(names)}", flush=True)

    print(f"\nDone. {done}/{len(names)} files, {total/1e6:.2f} MB -> {out}")
    return 0 if done == len(names) else 1


if __name__ == "__main__":
    raise SystemExit(main())
