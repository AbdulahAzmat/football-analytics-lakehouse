"""Execution audit framework.

Requirement: every file/table processed, in every layer, for both full and
incremental loads, writes a log row recording what ran, with what parameter,
when it started and finished, whether it succeeded, and how many rows it
inserted and updated.

Usage:

    with RunLogger(spark, layer="Raw-to-Bronze", table="bronze_fd_matches",
                   parameter="matches_PL_2023.json", batch_id=bid,
                   load_type="FULL") as run:
        result = do_the_work()
        run.record(inserted=result.inserted, updated=result.updated)

The context manager guarantees a row is written on the way out whether the
body succeeded or raised, so a crashed batch is visible in the log rather than
silently absent. Exceptions are re-raised after logging.
"""

from __future__ import annotations

import traceback
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from . import config

EXECUTION_LOG_SCHEMA = StructType([
    StructField("log_id", StringType(), False),           # PK, uuid4
    StructField("batch_id", StringType(), False),
    StructField("layer", StringType(), False),            # Raw-to-Bronze | Bronze-to-Silver
    StructField("table_name", StringType(), False),
    StructField("parameter", StringType(), True),         # file/date/path processed
    StructField("load_type", StringType(), False),        # FULL | INCREMENTAL
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("status", StringType(), False),           # SUCCESS | FAILED
    StructField("rows_read", LongType(), True),
    StructField("rows_inserted", LongType(), True),
    StructField("rows_updated", LongType(), True),
    StructField("rows_quarantined", LongType(), True),
    StructField("error_message", StringType(), True),
])


def new_batch_id(prefix: str = "batch") -> str:
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}"


class RunLogger:
    def __init__(
        self,
        spark: SparkSession,
        layer: str,
        table: str,
        parameter: str | None,
        batch_id: str,
        load_type: str,
        log_path: str | None = None,
        table_format: str | None = None,
    ):
        self.spark = spark
        self.layer = layer
        self.table = table
        self.parameter = parameter
        self.batch_id = batch_id
        self.load_type = load_type
        self.log_path = log_path or config.EXECUTION_LOG_PATH
        self.table_format = table_format or config.TABLE_FORMAT

        self.log_id = str(uuid.uuid4())
        self.start = datetime.now(timezone.utc)
        self.rows_read = 0
        self.rows_inserted = 0
        self.rows_updated = 0
        self.rows_quarantined = 0
        self._status = "SUCCESS"
        self._error: str | None = None

    def record(self, read=0, inserted=0, updated=0, quarantined=0) -> None:
        self.rows_read = read
        self.rows_inserted = inserted
        self.rows_updated = updated
        self.rows_quarantined = quarantined

    def __enter__(self) -> "RunLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is not None:
            self._status = "FAILED"
            self._error = f"{exc_type.__name__}: {exc}\n{''.join(traceback.format_tb(tb))[:2000]}"
        self._write()
        return False  # never swallow the exception

    def _write(self) -> None:
        end = datetime.now(timezone.utc)
        row = [(
            self.log_id,
            self.batch_id,
            self.layer,
            self.table,
            self.parameter,
            self.load_type,
            self.start,
            end,
            int((end - self.start).total_seconds()),
            self._status,
            int(self.rows_read),
            int(self.rows_inserted),
            int(self.rows_updated),
            int(self.rows_quarantined),
            self._error,
        )]
        df = self.spark.createDataFrame(row, EXECUTION_LOG_SCHEMA)
        # Append-only: the log is an immutable record of what happened.
        df.write.format(self.table_format).mode("append").save(self.log_path)
