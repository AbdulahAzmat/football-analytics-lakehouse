"""Write helpers: idempotent upsert, quarantine, and schema-drift handling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config


@dataclass
class UpsertResult:
    inserted: int
    updated: int


def add_metadata(df: DataFrame, batch_id: str, load_type: str, source_file_col: str | None = None) -> DataFrame:
    """Attach the metadata every table carries.

    load_timestamp is the moment THIS record was processed, which is what the
    requirement asks for - not the source's own lastUpdated, which is a
    different thing and is kept separately as a business column.

    source_file is expected to already be on the frame: Bronze materialises it
    in read_raw from _metadata.file_path, and Silver inherits it from Bronze.
    It deliberately does NOT fall back to input_file_name(), which Unity
    Catalog rejects with UC_COMMAND_NOT_SUPPORTED. If the column is genuinely
    absent we write an explicit marker rather than silently inventing a path.
    """
    out = (
        df.withColumn("load_timestamp", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
          .withColumn("batch_id", F.lit(batch_id))
          .withColumn("load_type", F.lit(load_type))
    )
    if "source_file" not in out.columns:
        out = out.withColumn("source_file", F.lit("UNKNOWN"))
    return out


class TableProbeError(RuntimeError):
    """Raised when we cannot tell whether a table exists."""


def table_exists(spark: SparkSession, path: str, fmt: str | None = None) -> bool:
    """Does the target table already exist?

    This must NOT guess. upsert() calls it to decide between MERGE and a
    first-time overwrite write, so a wrong "no" overwrites a populated table
    and destroys data. An earlier version caught every exception and returned
    False, which meant a permissions error, an expired credential or a
    transient storage blip all looked exactly like "table not created yet" and
    would have silently wiped the table on the next run.

    Now: a genuine "path not found" returns False. Anything else is re-raised
    as TableProbeError so the run fails loudly and the audit log records it.
    """
    fmt = fmt or config.TABLE_FORMAT

    if fmt == "delta":
        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(spark, path):
            return True
        # Not a Delta table: either nothing is there, or something non-Delta is.
        try:
            jvm = spark.sparkContext._jvm
            jsc = spark.sparkContext._jsc.hadoopConfiguration()
            p = jvm.org.apache.hadoop.fs.Path(path)
            fs = p.getFileSystem(jsc)
            if fs.exists(p):
                raise TableProbeError(
                    f"{path} exists but is not a Delta table. Refusing to overwrite it."
                )
            return False
        except TableProbeError:
            raise
        except Exception as e:  # noqa: BLE001
            raise TableProbeError(f"could not probe {path}: {e}") from e

    try:
        spark.read.format(fmt).load(path).limit(1).collect()
        return True
    except Exception as e:  # noqa: BLE001
        msg = str(e).lower()
        not_found = ("path does not exist" in msg or "path_not_found" in msg
                     or "no such file" in msg or "is not a parquet file" in msg
                     or "unable to infer schema" in msg)
        if not_found:
            return False
        raise TableProbeError(f"could not probe {path}: {e}") from e


def upsert(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: list[str],
    fmt: str | None = None,
    partition_by: list[str] | None = None,
) -> UpsertResult:
    """Idempotent write.

    Re-running the same batch over the same input must not create duplicate
    rows, and must leave the table in the same state. Matching on `keys`:
      - a key already present is UPDATED in place
      - a key not present is INSERTED

    Production path (Databricks / Azure) is Delta MERGE INTO.

    The parquet path exists only so the pipeline can be exercised in an
    environment without Delta's JARs (our offline test harness). It achieves
    the same idempotency by anti-joining the existing table on the merge keys
    and rewriting - semantically equivalent, but it rewrites the whole table,
    so it is NOT suitable for production volumes.
    """
    fmt = fmt or config.TABLE_FORMAT

    # Defend against a batch containing the same key twice: MERGE raises if the
    # source has duplicate matches, so collapse to the latest row per key first.
    if "load_timestamp" in df.columns:
        from pyspark.sql.window import Window
        w = Window.partitionBy(*keys).orderBy(F.col("load_timestamp").desc())
        df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    else:
        df = df.dropDuplicates(keys)

    incoming = df.count()

    if not table_exists(spark, path, fmt):
        writer = df.write.format(fmt).mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        return UpsertResult(inserted=incoming, updated=0)

    # How many of the incoming keys already exist? That split is what we report
    # as updated vs inserted. Join on key NAMES (both sides share them), which
    # avoids ambiguous-column headaches with composite keys.
    existing = spark.read.format(fmt).load(path)
    incoming_keys = df.select(*keys).distinct()
    updated = existing.select(*keys).join(incoming_keys, keys, "inner").count()
    inserted = incoming - updated

    if fmt == "delta":
        from delta.tables import DeltaTable
        tgt = DeltaTable.forPath(spark, path)
        on = " AND ".join([f"t.{k} = s.{k}" for k in keys])
        builder = (tgt.alias("t")
                   .merge(df.alias("s"), on)
                   .whenMatchedUpdateAll()
                   .whenNotMatchedInsertAll())
        # updateAll/insertAll fail outright if the source carries a column the
        # target does not have, which is precisely what happens when the source
        # system adds a field. Schema evolution lets the table grow instead of
        # the batch dying. withSchemaEvolution() is Delta 3.x+; on older
        # versions the session conf below is the equivalent.
        if hasattr(builder, "withSchemaEvolution"):
            builder = builder.withSchemaEvolution()
        else:
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        builder.execute()
    else:
        keep = existing.join(df.select(*keys).distinct(), keys, "left_anti")
        combined = keep.unionByName(df, allowMissingColumns=True)
        tmp = f"{path}__staging"
        w = combined.write.format(fmt).mode("overwrite")
        if partition_by:
            w = w.partitionBy(*partition_by)
        w.save(tmp)
        spark.read.format(fmt).load(tmp).write.format(fmt).mode("overwrite").save(path)

    return UpsertResult(inserted=inserted, updated=updated)


def quarantine(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    reason: str,
    batch_id: str,
    fmt: str | None = None,
) -> int:
    """Park non-conforming records instead of failing the whole batch.

    Schema drift strategy: records that violate the contract (null primary key,
    unparseable timestamp, a type that changed upstream) are written here with
    the reason, and the batch continues with the records that are valid. The
    quarantine table is written with mergeSchema so a drifted column can land
    without a migration.
    """
    fmt = fmt or config.TABLE_FORMAT
    n = df.count()
    if n == 0:
        return 0
    path = config.table_path("quarantine", table)
    out = (
        df.withColumn("quarantine_reason", F.lit(reason))
          .withColumn("quarantined_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
          .withColumn("batch_id", F.lit(batch_id))
    )
    writer = out.write.format(fmt).mode("append")
    if fmt == "delta":
        writer = writer.option("mergeSchema", "true")
    writer.save(path)
    return n


def split_valid_invalid(df: DataFrame, required: list[str]) -> tuple[DataFrame, DataFrame]:
    """Split a frame into (valid, invalid) on required-column nullness.

    This is the contract check that feeds quarantine(): a row missing its
    primary key or another non-negotiable field cannot go into Silver.
    """
    cond = F.lit(True)
    for c in required:
        cond = cond & F.col(c).isNotNull()
    return df.filter(cond), df.filter(~cond)
