# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — run the pipeline on Databricks
# MAGIC
# MAGIC Import this into Databricks and run top to bottom. It does steps 3–5:
# MAGIC ingest raw data into DBFS, build Bronze and Silver with **Delta**, then
# MAGIC run everything a second time to prove idempotency.
# MAGIC
# MAGIC Attach to serverless compute. Roughly 10-20 minutes: the slow part is
# MAGIC Bronze over the 267k StatsBomb events, not the ingest, since the raw data
# MAGIC is copied from the Repo when it is already there rather than downloaded.

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

import os, sys, subprocess

REPO = "/Workspace/Repos/abdullahazmat.w@gmail.com/football-analytics-lakehouse"
LAKE = "dbfs:/FileStore/football_lakehouse"

os.environ["LAKE_ROOT"] = LAKE
os.environ["TABLE_FORMAT"] = "delta"          # the path that has never been run
sys.path.insert(0, REPO)

assert os.path.exists(REPO), f"repo not found at {REPO} - fix REPO above"
print("repo :", REPO)
print("lake :", LAKE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Get raw data into DBFS
# MAGIC
# MAGIC Whether the committed data survives the Repos clone varies by workspace
# MAGIC and file size, so this detects rather than assumes:
# MAGIC
# MAGIC * **data present in the Repo** - copy it across (seconds, no network)
# MAGIC * **data absent** - download it (~233 MB, several minutes)
# MAGIC
# MAGIC Either way the pipeline reads from DBFS, so everything downstream is
# MAGIC identical.

# COMMAND ----------

RAW = "/dbfs/FileStore/football_lakehouse/raw"     # FUSE path, for python file IO
RAW_URI = f"{LAKE}/raw"                            # dbfs: URI, for Spark


def dir_bytes(p: str) -> int:
    out = subprocess.run(["du", "-sb", p], capture_output=True, text=True).stdout
    try:
        return int(out.split()[0])
    except Exception:
        return 0


sb_local = f"{REPO}/data/statsbomb"
fd_local = f"{REPO}/data/full_load"
sb_have = dir_bytes(sb_local) > 100_000_000      # expect ~233 MB
fd_have = dir_bytes(fd_local) > 10_000_000       # expect ~16 MB

print(f"StatsBomb in repo    : {dir_bytes(sb_local)/1e6:7.1f} MB  -> {'COPY' if sb_have else 'DOWNLOAD'}")
print(f"football-data in repo: {dir_bytes(fd_local)/1e6:7.1f} MB  -> {'COPY' if fd_have else 'DOWNLOAD'}")

# COMMAND ----------

if sb_have:
    dbutils.fs.cp(f"file:{sb_local}", f"{RAW_URI}/statsbomb", recurse=True)  # noqa: F821
    print("copied StatsBomb from the repo")
else:
    r = subprocess.run([sys.executable, f"{REPO}/scripts/ingest_statsbomb.py",
                        "--out", f"{RAW}/statsbomb"],
                       capture_output=True, text=True, cwd=REPO)
    print(r.stdout[-3000:]); print(r.stderr[-2000:] if r.returncode else "")
    assert r.returncode == 0, "StatsBomb ingest failed"

# COMMAND ----------

if fd_have:
    dbutils.fs.cp(f"file:{fd_local}", f"{RAW_URI}/football-data/full_load", recurse=True)  # noqa: F821
    print("copied football-data full load from the repo")
else:
    # Same bytes as the committed files (checksum-verified), fetched over
    # raw.githubusercontent so no API key and no rate limit are involved.
    r = subprocess.run([sys.executable, f"{REPO}/scripts/fetch_committed_raw.py",
                        "--out", f"{RAW}/football-data/full_load"],
                       capture_output=True, text=True, cwd=REPO)
    print(r.stdout[-2000:]); print(r.stderr[-1000:] if r.returncode else "")
    assert r.returncode == 0, "football-data fetch failed"

# COMMAND ----------

# confirm what actually landed in DBFS before building anything on it
for sub, expect in [("statsbomb/events", 66), ("football-data/full_load", 76)]:
    n = len(dbutils.fs.ls(f"{RAW_URI}/{sub}"))  # noqa: F821
    print(f"{sub:28} {n:>4} files   {'OK' if n >= expect else f'EXPECTED >= {expect}  <-- PROBLEM'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: a live daily snapshot
# MAGIC
# MAGIC Only needed to demonstrate the incremental load against the live API.
# MAGIC Store the key as a secret first - do not paste it into the notebook:
# MAGIC ```
# MAGIC databricks secrets create-scope football
# MAGIC databricks secrets put-secret football api_token
# MAGIC ```
# MAGIC Takes ~5 minutes (rate limited to 10 req/min). Skip it if you only need
# MAGIC the full load.

# COMMAND ----------

try:
    os.environ["FOOTBALL_DATA_TOKEN"] = dbutils.secrets.get("football", "api_token")  # noqa: F821
    r = subprocess.run([sys.executable, f"{REPO}/scripts/daily_snapshot.py",
                        "--out", f"{RAW}/football-data/daily"],
                       capture_output=True, text=True, cwd=REPO, timeout=1800)
    print(r.stdout[-3000:])
except Exception as e:  # noqa: BLE001
    print(f"skipped daily snapshot ({e}) - the full load above is enough to run the pipeline")

# COMMAND ----------

# MAGIC %md ## 2. Build Bronze and Silver (Delta)

# COMMAND ----------

from pipelines.bronze import raw_to_bronze
from pipelines.silver import bronze_to_silver
from pipelines.common.audit import new_batch_id
from pipelines.common import config

SNAPSHOT_DATE = "2026-09-27"   # the date the committed full load was pulled

BRONZE_JOBS = [
    ("fd_matches",   f"{RAW_URI}/football-data/full_load/matches_*.json",   "FULL", None),
    ("fd_standings", f"{RAW_URI}/football-data/full_load/standings_*.json", "FULL", SNAPSHOT_DATE),
    ("fd_scorers",   f"{RAW_URI}/football-data/full_load/scorers_*.json",   "FULL", SNAPSHOT_DATE),
    ("fd_teams",     f"{RAW_URI}/football-data/full_load/teams_*.json",     "FULL", None),
    ("sb_matches",   f"{RAW_URI}/statsbomb/matches/*/*.json",               "FULL", None),
    ("sb_lineups",   f"{RAW_URI}/statsbomb/lineups/*.json",                 "FULL", None),
    ("sb_events",    f"{RAW_URI}/statsbomb/events/*.json",                  "FULL", None),
]
SILVER_JOBS = [
    ("silver_matches", None), ("silver_standings", SNAPSHOT_DATE),
    ("silver_scorers", SNAPSHOT_DATE), ("silver_teams", None),
    ("silver_sb_matches", None), ("silver_lineups", None), ("silver_events", None),
]


def run_everything(tag: str):
    b = new_batch_id(f"bronze_{tag}")
    for feed, path, lt, snap in BRONZE_JOBS:
        raw_to_bronze.run_feed(spark, feed, path, lt, b, snap)  # noqa: F821
    s = new_batch_id(f"silver_{tag}")
    for tbl, snap in SILVER_JOBS:
        bronze_to_silver.run_table(spark, tbl, s, "FULL", snap)  # noqa: F821


def snapshot_counts():
    out = {}
    for layer, tables in (("bronze", [t for _, t in
                                      sorted(raw_to_bronze.TARGET_TABLE.items())]),
                          ("silver", [t for t, _ in SILVER_JOBS])):
        for t in tables:
            try:
                out[t] = spark.read.format("delta").load(  # noqa: F821
                    config.table_path(layer, t)).count()
            except Exception:
                out[t] = None
    return out

# COMMAND ----------

print("=" * 60); print("PASS 1"); print("=" * 60)
run_everything("p1")
counts_1 = snapshot_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Idempotency — run it all again
# MAGIC
# MAGIC Identical input, fresh batch ids. Row counts must not move, and the
# MAGIC second pass must report `inserted=0, updated=N`. This is what exercises
# MAGIC Delta `MERGE INTO`.

# COMMAND ----------

print("=" * 60); print("PASS 2"); print("=" * 60)
run_everything("p2")
counts_2 = snapshot_counts()

# COMMAND ----------

ok = True
print(f"{'table':26}{'pass1':>10}{'pass2':>10}   idempotent")
for t in counts_1:
    a, b = counts_1[t], counts_2.get(t)
    good = a is not None and a == b
    ok = ok and good
    print(f"{t:26}{str(a):>10}{str(b):>10}   {'YES' if good else 'NO  <-- FAIL'}")
print()
print("IDEMPOTENCY (Delta MERGE):", "PASS" if ok else "FAIL")

# COMMAND ----------

# MAGIC %md ## 4. Audit log — evidence for submission

# COMMAND ----------

log = spark.read.format("delta").load(config.EXECUTION_LOG_PATH)  # noqa: F821
print("total audit rows:", log.count())
log.groupBy("layer", "load_type", "status").count().orderBy("layer").display()

# COMMAND ----------

(log.select("layer", "table_name", "parameter", "load_type", "status",
            "rows_read", "rows_inserted", "rows_updated", "rows_quarantined",
            "start_time", "end_time", "duration_seconds")
    .orderBy("start_time")
    .display())

# COMMAND ----------

# MAGIC %md
# MAGIC Screenshot the two tables above for the submission: they show every run,
# MAGIC its parameter, timings, status and row counts, and the second pass
# MAGIC updating rather than inserting.

# COMMAND ----------

failed = log.filter("status != 'SUCCESS'")
if failed.count():
    failed.select("table_name", "parameter", "error_message").display()
else:
    print("no failed runs")

# COMMAND ----------

# MAGIC %md ## 5. Quarantine (should be empty on clean data)

# COMMAND ----------

for t in ["bronze_fd_matches", "silver_matches", "silver_events"]:
    try:
        q = spark.read.format("delta").load(config.table_path("quarantine", t))  # noqa: F821
        print(f"{t}: {q.count()} quarantined")
    except Exception:
        print(f"{t}: no quarantine table (nothing rejected)")
