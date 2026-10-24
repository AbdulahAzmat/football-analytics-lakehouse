"""Publish Gold and the audit tables to Unity Catalog for BI.

The pipeline stores every table as Delta files under LAKE_ROOT and addresses
them by path. A BI tool connects through a SQL warehouse and browses
catalog.schema.table, so it cannot see path-based tables. This step copies the
finished Gold tables, the execution log and a quarantine summary into managed
catalog tables (default: <volume catalog>.football_gold), overwriting them on
every run so the dashboard always reflects the latest pipeline state.

It is the last step of the Gold stage in run_pipeline, and it is logged like
every other step (layer Gold-Publish).
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from ..common import config
from ..common.audit import RunLogger
from . import silver_to_gold

LAYER = "Gold-Publish"

# every table name a quarantine() call can write under (Bronze and Silver)
QUARANTINE_TABLES = [
    "bronze_fd_matches", "bronze_fd_standings", "bronze_fd_scorers", "bronze_fd_teams",
    "bronze_sb_matches", "bronze_sb_events", "bronze_sb_lineups",
    "silver_matches", "silver_standings", "silver_scorers", "silver_teams",
    "silver_events", "silver_lineups", "silver_sb_matches",
]

_QUARANTINE_SUMMARY_SCHEMA = StructType([
    StructField("source_table", StringType(), True),
    StructField("quarantine_reason", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("quarantine_date", DateType(), True),
    StructField("rows_quarantined", LongType(), True),
])


def _quarantine_summary(spark: SparkSession):
    frames = []
    for t in QUARANTINE_TABLES:
        try:
            q = spark.read.format(config.TABLE_FORMAT).load(config.table_path("quarantine", t))
            frames.append(q.select(F.lit(t).alias("source_table"), "quarantine_reason", "batch_id",
                                   F.to_date("quarantined_at").alias("quarantine_date")))
        except Exception:  # noqa: BLE001 - no quarantine table means nothing was rejected
            continue
    if not frames:
        return spark.createDataFrame([], _QUARANTINE_SUMMARY_SCHEMA)
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return (out.groupBy("source_table", "quarantine_reason", "batch_id", "quarantine_date")
               .agg(F.count("*").alias("rows_quarantined")))


def publish_to_catalog(spark: SparkSession, batch_id: str, load_type: str,
                       catalog: str | None = None, schema: str | None = None) -> None:
    catalog = catalog or config.PUBLISH_CATALOG
    schema = schema or config.PUBLISH_SCHEMA
    if config.TABLE_FORMAT != "delta" or not catalog:
        print("[publish] skipped (needs Delta and a catalog; set PUBLISH_CATALOG)")
        return

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    def _save(df, name: str, source: str) -> None:
        target = f"`{catalog}`.`{schema}`.`{name}`"
        with RunLogger(spark, LAYER, name, f"{source} -> {catalog}.{schema}.{name}",
                       batch_id, load_type) as run:
            n = df.count()
            (df.write.format("delta").mode("overwrite")
               .option("overwriteSchema", "true").saveAsTable(target))
            run.record(read=n, inserted=n)
        print(f"[publish] {catalog}.{schema}.{name}: {n} rows")

    for table in silver_to_gold.GOLD_TABLES:
        df = spark.read.format("delta").load(config.table_path("gold", table))
        _save(df, table, f"gold/{table}")

    # audit tables go last so they include this run's earlier steps
    log = spark.read.format("delta").load(config.EXECUTION_LOG_PATH)
    _save(log, "audit_execution_log", "ops/pipeline_execution_logs")
    _save(_quarantine_summary(spark), "audit_quarantine_summary", "quarantine/*")
