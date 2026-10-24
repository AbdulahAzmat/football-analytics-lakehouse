# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 - publish Gold and audit tables for Power BI
# MAGIC
# MAGIC Power BI connects through a SQL warehouse and browses
# MAGIC `catalog.schema.table`. The pipeline's own tables are path-based Delta
# MAGIC files in a volume, which it cannot see, so this copies Gold plus the audit
# MAGIC tables into `<catalog>.football_gold`. Run it after
# MAGIC `run_phase3_gold_databricks.py`. From now on `run_pipeline` does this
# MAGIC automatically as the last step of the Gold stage.

# COMMAND ----------

import os, sys

REPO = os.path.dirname(os.getcwd())
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
os.environ["LAKE_ROOT"] = f"/Volumes/{cat}/{sch}/{VOLUME_NAME}"
os.environ["TABLE_FORMAT"] = "delta"

for m in [m for m in sys.modules if m == "pipelines" or m.startswith("pipelines.")]:
    del sys.modules[m]

from pipelines.common import config               # noqa: E402
from pipelines.common.audit import new_batch_id   # noqa: E402
from pipelines.gold import publish                # noqa: E402

print("publishing to:", f"{config.PUBLISH_CATALOG}.{config.PUBLISH_SCHEMA}")

# COMMAND ----------

publish.publish_to_catalog(spark, new_batch_id("publish"), "FULL")  # noqa: F821

# COMMAND ----------

# MAGIC %md ## Check what Power BI will see

# COMMAND ----------

spark.sql(f"SHOW TABLES IN `{config.PUBLISH_CATALOG}`.`{config.PUBLISH_SCHEMA}`").display()  # noqa: F821

# COMMAND ----------

for t in ["dim_team", "fact_team_match", "agg_team_season_performance",
          "audit_execution_log", "audit_quarantine_summary"]:
    n = spark.table(f"`{config.PUBLISH_CATALOG}`.`{config.PUBLISH_SCHEMA}`.`{t}`").count()  # noqa: F821
    print(f"{t:32}{n:>10,}")
