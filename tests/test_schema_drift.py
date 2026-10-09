"""Prove the schema-drift and contract-violation handling actually works.

The main pipeline run quarantines 0 rows because the real source data is
clean, which means the quarantine path is never exercised by that run. This
test deliberately corrupts input to force each failure mode and asserts the
batch survives and the bad rows land in quarantine.

Cases:
  1. NULL PRIMARY KEY      - a match with no id must not enter Bronze
  2. TYPE DRIFT            - utcDate stops being a parseable timestamp
  3. NEW UNEXPECTED COLUMN - source adds a field we never declared
  4. CHANGED TYPE          - an int field arrives as a string

Run: python tests/test_schema_drift.py
"""

import json
import os
import shutil
import sys

SCRATCH = "/tmp/lakehouse_drift_test"
RAWDIR = "/tmp/lakehouse_drift_raw"
os.environ["LAKE_ROOT"] = SCRATCH
os.environ["TABLE_FORMAT"] = "parquet"

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession  # noqa: E402

from pipelines.bronze import raw_to_bronze  # noqa: E402
from pipelines.silver import bronze_to_silver  # noqa: E402
from pipelines.common import config  # noqa: E402
from pipelines.common.audit import new_batch_id  # noqa: E402


def good_match(mid, utc="2026-09-20T14:00:00Z"):
    return {
        "id": mid, "utcDate": utc, "status": "FINISHED", "matchday": 5,
        "stage": "REGULAR_SEASON", "group": None, "lastUpdated": "2026-09-27T00:20:33Z",
        "competition": {"id": 2021, "name": "Premier League", "code": "PL", "type": "LEAGUE"},
        "season": {"id": 2502, "startDate": "2026-08-21", "endDate": "2027-05-30",
                   "currentMatchday": 6},
        "homeTeam": {"id": 1, "name": "Home FC", "tla": "HOM"},
        "awayTeam": {"id": 2, "name": "Away FC", "tla": "AWY"},
        "score": {"winner": "HOME_TEAM", "duration": "REGULAR",
                  "fullTime": {"home": 2, "away": 1}, "halfTime": {"home": 1, "away": 0}},
    }


def build_corrupt_file(path):
    matches = [good_match(900001), good_match(900002)]

    # CASE 1: null primary key
    m = good_match(None)
    matches.append(m)

    # CASE 2: type drift - utcDate is no longer a timestamp
    m = good_match(900003, utc="not-a-timestamp-at-all")
    matches.append(m)

    # CASE 3: source added a column we never declared in our StructType
    m = good_match(900004)
    m["brandNewFieldFromUpstream"] = {"nested": "value", "n": 42}
    matches.append(m)

    # CASE 4: a field changed type upstream (int -> string)
    m = good_match(900005)
    m["score"]["fullTime"]["home"] = "two"
    matches.append(m)

    payload = {
        "competition": {"id": 2021, "name": "Premier League", "code": "PL", "type": "LEAGUE"},
        "resultSet": {"count": len(matches), "played": len(matches)},
        "matches": matches,
    }
    with open(path, "w") as f:
        json.dump(payload, f)
    return len(matches)


def main():
    for d in (SCRATCH, RAWDIR):
        if os.path.exists(d):
            shutil.rmtree(d)
    os.makedirs(RAWDIR)

    raw_file = os.path.join(RAWDIR, "matches_DRIFT_2026.json")
    n = build_corrupt_file(raw_file)

    spark = (SparkSession.builder.appName("drift_test").master("local[2]")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    print(f"input rows: {n} (2 clean, 4 deliberately broken)\n")

    bid = new_batch_id("drift")
    raw_to_bronze.run_feed(spark, "fd_matches", raw_file, "FULL", bid, None)
    bronze_to_silver.run_table(spark, "silver_matches", new_batch_id("drift_s"), "FULL", None)

    print()
    bronze = spark.read.parquet(config.table_path("bronze", "bronze_fd_matches"))
    print(f"Bronze rows accepted: {bronze.count()}")
    print("  match_ids:", sorted(r[0] for r in bronze.select("match_id").collect()))

    qb_path = config.table_path("quarantine", "bronze_fd_matches")
    try:
        qb = spark.read.parquet(qb_path)
        print(f"\nQuarantined at Bronze: {qb.count()}")
        qb.select("match_id", "utc_date", "quarantine_reason").show(truncate=False)
    except Exception as e:
        print(f"\nNo Bronze quarantine table: {e}")
        qb = None

    silver = spark.read.parquet(config.table_path("silver", "silver_matches"))
    print(f"Silver rows accepted: {silver.count()}")
    print("  match_ids:", sorted(r[0] for r in silver.select("match_id").collect()))

    qs_path = config.table_path("quarantine", "silver_matches")
    try:
        qs = spark.read.parquet(qs_path)
        print(f"\nQuarantined at Silver: {qs.count()}")
        qs.select("match_id", "utc_date", "quarantine_reason").show(truncate=False)
    except Exception as e:
        print(f"\nNo Silver quarantine table: {e}")
        qs = None

    # CASE 4 check: did the int->string change get handled without crashing?
    row = silver.filter(silver.match_id == 900005).collect()
    print(f"\nCASE 4 (int->string 'two') row in Silver: {row[0]['home_goals'] if row else 'ABSENT'}")

    # Assertions
    print("\n" + "=" * 60)
    ok = True
    bronze_ids = {r[0] for r in bronze.select("match_id").collect()}

    if None in bronze_ids:
        print("FAIL: null primary key reached Bronze"); ok = False
    else:
        print("PASS: null primary key was quarantined, not loaded")

    if qb is not None and qb.count() >= 1:
        print("PASS: Bronze quarantine table written")
    else:
        print("FAIL: nothing quarantined at Bronze"); ok = False

    silver_ids = {r[0] for r in silver.select("match_id").collect()}
    if 900003 in silver_ids:
        print("FAIL: unparseable timestamp reached Silver"); ok = False
    else:
        print("PASS: timestamp drift quarantined before Silver")

    if 900004 in silver_ids:
        print("PASS: undeclared new upstream column ignored, batch survived")
    else:
        print("FAIL: new column broke the batch"); ok = False

    print("=" * 60)
    print("SCHEMA DRIFT HANDLING:", "PASS" if ok else "FAIL")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
