# Databricks notebook source
# MAGIC %md
# MAGIC # Daily pipeline task
# MAGIC
# MAGIC One notebook, three stages. The Workflow in
# MAGIC `workflows/football_lakehouse_daily_job.json` runs it as three dependent
# MAGIC tasks (`bronze` -> `silver` -> `gold`). Run it by hand with `stage = all`
# MAGIC to execute the whole chain in one go.
# MAGIC
# MAGIC Gold is not a separate script someone has to remember to run: it is the
# MAGIC last task of the same job, under the same batch id.

# COMMAND ----------

import os, sys

dbutils.widgets.dropdown("stage", "all", ["bronze", "silver", "gold", "all"])  # noqa: F821
dbutils.widgets.dropdown("load_type", "INCREMENTAL", ["INCREMENTAL", "FULL"])  # noqa: F821
dbutils.widgets.text("snapshot_date", "")   # empty = today (UTC)  # noqa: F821
dbutils.widgets.text("batch_id", "")        # empty = generate one  # noqa: F821
dbutils.widgets.text("lake_root", "")       # empty = auto-detect the UC volume  # noqa: F821

STAGE = dbutils.widgets.get("stage")  # noqa: F821
LOAD_TYPE = dbutils.widgets.get("load_type")  # noqa: F821
SNAPSHOT_DATE = dbutils.widgets.get("snapshot_date") or None  # noqa: F821
BATCH_ID = dbutils.widgets.get("batch_id") or None  # noqa: F821
LAKE = dbutils.widgets.get("lake_root") or None  # noqa: F821

# the repo is the parent of the notebooks/ folder this notebook lives in
REPO = os.path.dirname(os.getcwd())
assert os.path.exists(f"{REPO}/pipelines"), f"pipelines/ not found under {REPO}"
if REPO not in sys.path:
    sys.path.insert(0, REPO)

# COMMAND ----------

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


if LAKE is None:
    cat, sch = _first_writable_catalog_schema()
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{cat}`.`{sch}`.`{VOLUME_NAME}`")  # noqa: F821
    LAKE = f"/Volumes/{cat}/{sch}/{VOLUME_NAME}"

os.environ["LAKE_ROOT"] = LAKE
os.environ["TABLE_FORMAT"] = "delta"

# Databricks keeps the interpreter alive between runs: drop cached modules so
# the code that was just pulled from git is the code that runs.
for m in [m for m in sys.modules if m == "pipelines" or m.startswith("pipelines.")]:
    del sys.modules[m]

from pipelines import run_pipeline  # noqa: E402
from pipelines.common.audit import new_batch_id  # noqa: E402

BATCH_ID = BATCH_ID or new_batch_id(f"{STAGE}_{LOAD_TYPE.lower()}")
RAW_ROOT = f"{LAKE}/raw"
print(f"stage={STAGE} load_type={LOAD_TYPE} snapshot_date={SNAPSHOT_DATE or 'today'} batch={BATCH_ID}\nlake={LAKE}")

# COMMAND ----------

if STAGE == "all":
    run_pipeline.run_end_to_end(spark, LOAD_TYPE, RAW_ROOT, SNAPSHOT_DATE, BATCH_ID)  # noqa: F821
else:
    from datetime import datetime, timezone
    snap = SNAPSHOT_DATE or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_pipeline.run_stage(spark, STAGE, LOAD_TYPE, RAW_ROOT, snap, BATCH_ID)  # noqa: F821

# COMMAND ----------

# a failed stage raises above, so reaching here means this stage succeeded
dbutils.notebook.exit(f"OK stage={STAGE} batch={BATCH_ID}")  # noqa: F821
