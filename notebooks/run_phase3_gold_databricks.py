# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 (parts 1-2) - Gold layer on Databricks
# MAGIC
# MAGIC Run top to bottom **after** `run_phase2_databricks.py`, because Gold reads
# MAGIC the Silver tables that notebook builds. It:
# MAGIC
# MAGIC 1. builds the Gold star schema from Silver
# MAGIC 2. runs it a second time to prove the refresh is idempotent
# MAGIC 3. shows the three Phase 1 business questions answered from Gold
# MAGIC 4. shows the Silver-to-Gold rows in the same audit log as earlier layers
# MAGIC 5. shows the read optimisation (liquid clustering / OPTIMIZE)
# MAGIC 6. pushes a *new* snapshot through Bronze -> Silver -> Gold in one call
# MAGIC
# MAGIC Screenshot sections 2, 3, 4, 5 and 6 for the report.

# COMMAND ----------

import os, sys, time

REPO = os.path.dirname(os.getcwd())          # this notebook lives in <repo>/notebooks
assert os.path.exists(f"{REPO}/pipelines"), f"pipelines/ not found under {REPO}"
if REPO not in sys.path:
    sys.path.insert(0, REPO)

VOLUME_NAME = "football_lakehouse"


def _first_writable_catalog_schema():
    prefer = ["workspace", "main"]
    cats = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]  # noqa: F821
    ordered = [c for c in prefer if c in cats] + [c for c in cats if c not in prefer
                                                  and c not in ("samples", "system", "hive_metastore")]
    for cat in ordered:
        try:
            schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{cat}`").collect()]  # noqa: F821
        except Exception:
            continue
        for sch in (["default"] + [s for s in schemas if s != "default"]):
            if sch in schemas and sch != "information_schema":
                return cat, sch
    return None, None


cat, sch = _first_writable_catalog_schema()
LAKE = f"/Volumes/{cat}/{sch}/{VOLUME_NAME}"
os.environ["LAKE_ROOT"] = LAKE
os.environ["TABLE_FORMAT"] = "delta"

for m in [m for m in sys.modules if m == "pipelines" or m.startswith("pipelines.")]:
    del sys.modules[m]

from pipelines.common import config                      # noqa: E402
from pipelines.common.audit import new_batch_id          # noqa: E402
from pipelines.gold import silver_to_gold                # noqa: E402
from pipelines import run_pipeline                       # noqa: E402

RAW_URI = f"{LAKE}/raw"
print("lake:", LAKE)

# COMMAND ----------

# MAGIC %md ## 1. Build Gold from Silver

# COMMAND ----------

t0 = time.time()
silver_to_gold.run_all(spark, new_batch_id("gold_p1"), "FULL")  # noqa: F821
print(f"\nGold built in {time.time() - t0:.0f}s")

# COMMAND ----------

def gold_counts():
    out = {}
    for t in silver_to_gold.GOLD_TABLES:
        out[t] = spark.read.format("delta").load(config.table_path("gold", t)).count()  # noqa: F821
    return out


counts_1 = gold_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Idempotency: refresh Gold again, nothing may move
# MAGIC
# MAGIC Same Silver, new batch id. Facts and dimensions go through `MERGE`
# MAGIC (second pass reports `inserted=0`), aggregates are overwritten. Row counts
# MAGIC must be identical.

# COMMAND ----------

silver_to_gold.run_all(spark, new_batch_id("gold_p2"), "FULL", optimize=False)  # noqa: F821
counts_2 = gold_counts()

ok = True
print(f"{'table':32}{'pass1':>10}{'pass2':>10}   idempotent")
for t, a in counts_1.items():
    b = counts_2[t]
    ok &= (a == b)
    print(f"{t:32}{a:>10,}{b:>10,}   {'YES' if a == b else 'NO  <-- FAIL'}")
print("\nGOLD IDEMPOTENCY:", "PASS" if ok else "FAIL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The business questions, answered from Gold
# MAGIC
# MAGIC Plain joins from a fact/aggregate to its dimensions on integer keys: this
# MAGIC is the shape Power BI's relationships will mirror.

# COMMAND ----------

for t in silver_to_gold.GOLD_TABLES:
    spark.read.format("delta").load(config.table_path("gold", t)).createOrReplaceTempView(t)  # noqa: F821

# COMMAND ----------

# MAGIC %md ### Q1. Which teams performed best, Premier League 2025/26?

# COMMAND ----------

spark.sql("""
SELECT a.season_rank AS pos, t.team_name, a.matches_played AS p, a.wins AS w, a.draws AS d,
       a.losses AS l, a.goals_for AS gf, a.goals_against AS ga, a.goal_difference AS gd, a.points AS pts
FROM agg_team_season_performance a
JOIN dim_team t USING (team_key)
JOIN dim_competition c USING (competition_key)
JOIN dim_season s USING (season_key)
WHERE c.competition_code = 'PL' AND s.season_label = '2025/26'
ORDER BY a.season_rank
LIMIT 10
""").display()  # noqa: F821

# COMMAND ----------

# MAGIC %md ### Q2. Do teams do better at home than away? (all league competitions)

# COMMAND ----------

spark.sql("""
SELECT a.venue,
       SUM(a.matches_played)                                       AS matches,
       ROUND(SUM(a.goals_for) / SUM(a.matches_played), 2)          AS avg_goals_for,
       ROUND(SUM(a.wins)   * 1.0 / SUM(a.matches_played), 3)       AS win_rate,
       ROUND(SUM(a.draws)  * 1.0 / SUM(a.matches_played), 3)       AS draw_rate,
       ROUND(SUM(a.points) * 1.0 / SUM(a.matches_played), 2)       AS points_per_game
FROM agg_home_away_split a
JOIN dim_competition c USING (competition_key)
WHERE c.competition_type = 'LEAGUE'
GROUP BY a.venue
""").display()  # noqa: F821

# COMMAND ----------

# MAGIC %md ### Q3. Top scorers (latest snapshot) and the teams they concentrate in

# COMMAND ----------

spark.sql("""
SELECT c.competition_code, p.player_name, t.team_name, f.goals, f.assists, f.played_matches, f.goals_per_match
FROM fact_top_scorers f
JOIN dim_player p USING (player_key)
JOIN dim_team t USING (team_key)
JOIN dim_competition c USING (competition_key)
WHERE f.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_top_scorers)
ORDER BY f.goals DESC
LIMIT 10
""").display()  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution log: Silver-to-Gold is in the same table as the other layers
# MAGIC
# MAGIC For aggregates `rows_read` is the number of rows aggregated and
# MAGIC `rows_inserted` the number of groups produced; `duration_seconds` is the
# MAGIC execution time.

# COMMAND ----------

log = spark.read.format("delta").load(config.EXECUTION_LOG_PATH)  # noqa: F821
(log.filter("layer = 'Silver-to-Gold'")
    .select("batch_id", "table_name", "parameter", "load_type", "status",
            "rows_read", "rows_inserted", "rows_updated", "duration_seconds", "start_time")
    .orderBy("start_time", "table_name").display())

# COMMAND ----------

log.groupBy("layer", "status").count().orderBy("layer", "status").display()

# COMMAND ----------

# the validation checks write their own rows: rows_read = rows tested,
# rows_quarantined = violations (grain duplicates or orphan foreign keys)
(log.filter("layer = 'Gold-Validation'")
    .select("table_name", "parameter", "status", "rows_read", "rows_quarantined")
    .orderBy("table_name", "parameter").display())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Read optimisation
# MAGIC
# MAGIC `optimize_gold()` enables liquid clustering on the columns the dashboard
# MAGIC filters and joins on, then runs `OPTIMIZE`. If a workspace rejects
# MAGIC clustering it falls back to Z-ORDER, then plain `OPTIMIZE`; the
# MAGIC `parameter` column below shows which one actually ran.

# COMMAND ----------

(log.filter("layer = 'Gold-Optimize'")
    .select("table_name", "parameter", "status", "duration_seconds", "error_message")
    .orderBy("start_time").display())

# COMMAND ----------

for t in silver_to_gold.CLUSTER_PLAN:
    p = config.table_path("gold", t)
    print(f"--- {t}")
    spark.sql(f"DESCRIBE DETAIL delta.`{p}`").select(  # noqa: F821
        "numFiles", "sizeInBytes", "clusteringColumns", "partitionColumns").show(truncate=False)

# COMMAND ----------

p = config.table_path("gold", "fact_team_match")
spark.sql(f"DESCRIBE HISTORY delta.`{p}`").select(  # noqa: F821
    "version", "timestamp", "operation", "operationParameters").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. A new incremental file reaches Gold with no extra step
# MAGIC
# MAGIC Replays the scorer and standings files as if they were a new day's
# MAGIC snapshot, plus one match file, and calls the end-to-end runner once.
# MAGIC Nobody calls `silver_to_gold` here: it is the third stage of the run.

# COMMAND ----------

INC_DATE = "2026-09-29"
snaps_before = sorted(str(r[0]) for r in spark.read.format("delta").load(  # noqa: F821
    config.table_path("gold", "fact_top_scorers")).select("snapshot_date").distinct().collect())
matches_before = spark.read.format("delta").load(config.table_path("gold", "fact_matches")).count()  # noqa: F821
print("fact_top_scorers snapshots before:", snaps_before)

batch = run_pipeline.run_end_to_end(
    spark, "INCREMENTAL", RAW_URI, INC_DATE,  # noqa: F821
    base_dir=f"{RAW_URI}/football-data/full_load", matches_glob="matches_PL_2026.json")

snaps_after = sorted(str(r[0]) for r in spark.read.format("delta").load(  # noqa: F821
    config.table_path("gold", "fact_top_scorers")).select("snapshot_date").distinct().collect())
matches_after = spark.read.format("delta").load(config.table_path("gold", "fact_matches")).count()  # noqa: F821
print("fact_top_scorers snapshots after :", snaps_after)
print(f"fact_matches rows: {matches_before:,} -> {matches_after:,} (same matches re-sent: updated, not duplicated)")
print("NEW SNAPSHOT REACHED GOLD:", "PASS" if INC_DATE in snaps_after else "FAIL")
print("NO DUPLICATE MATCHES     :", "PASS" if matches_before == matches_after else "FAIL")

# COMMAND ----------

# one batch id, three layers
(spark.read.format("delta").load(config.EXECUTION_LOG_PATH)  # noqa: F821
      .filter(f"batch_id = '{batch}' AND layer IN ('Raw-to-Bronze','Bronze-to-Silver','Silver-to-Gold')")
      .select("layer", "table_name", "load_type", "status", "rows_read", "rows_inserted",
              "rows_updated", "duration_seconds")
      .orderBy("layer", "table_name").display())
