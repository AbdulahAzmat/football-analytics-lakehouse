"""End-to-end orchestration: Raw -> Bronze -> Silver -> Gold.

Gold is the last stage of the same run, not a separate manual script. When new
data is processed into Silver, the next stage rebuilds Gold from it, under the
same batch id, so one logical run can be traced through all three layers in
the execution log.

Three ways to run it, all the same code:

  * Databricks Workflow  - workflows/football_lakehouse_daily_job.json runs the
                           stages as dependent tasks (Gold only starts if Silver
                           succeeded) on a daily schedule.
  * Notebook             - notebooks/job_daily_pipeline.py with stage=all.
  * Python               - run_end_to_end(spark, "INCREMENTAL", raw_root, date)

Stages fail fast. If Bronze or Silver raises, Gold is not refreshed, so the
dashboard keeps showing the last good state instead of a half-updated one.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession

from .bronze import raw_to_bronze
from .common.audit import new_batch_id
from .gold import silver_to_gold
from .silver import bronze_to_silver

STAGES = ("bronze", "silver", "gold")


def bronze_jobs(load_type: str, raw_root: str, snapshot_date: str,
                base_dir: str | None = None, matches_glob: str | None = None):
    """(feed, path, snapshot_date) for the Bronze stage.

    FULL reads the committed historical files. INCREMENTAL reads one day's
    snapshot folder written by scripts/daily_snapshot.py. `base_dir` and
    `matches_glob` let a test replay an existing file as if it were a new day.
    """
    if load_type == "FULL":
        fd = base_dir or f"{raw_root}/football-data/full_load"
        return [
            ("fd_matches",   f"{fd}/{matches_glob or 'matches_*.json'}", None),
            ("fd_standings", f"{fd}/standings_*.json", snapshot_date),
            ("fd_scorers",   f"{fd}/scorers_*.json",   snapshot_date),
            ("fd_teams",     f"{fd}/teams_*.json",     None),
            ("sb_matches",   f"{raw_root}/statsbomb/matches/*/*.json", None),
            ("sb_lineups",   f"{raw_root}/statsbomb/lineups/*.json",   None),
            ("sb_events",    f"{raw_root}/statsbomb/events/*.json",    None),
        ]
    day = base_dir or f"{raw_root}/football-data/daily/{snapshot_date}"
    return [
        ("fd_matches",   f"{day}/{matches_glob or 'matches_window.json'}", None),
        ("fd_standings", f"{day}/standings_*.json", snapshot_date),
        ("fd_scorers",   f"{day}/scorers_*.json",   snapshot_date),
        ("fd_teams",     f"{day}/teams_*.json",     None),
    ]


def silver_jobs(load_type: str, snapshot_date: str):
    """(table, snapshot_date) for the Silver stage."""
    core = [("silver_matches", None), ("silver_standings", snapshot_date),
            ("silver_scorers", snapshot_date), ("silver_teams", None)]
    if load_type == "FULL":
        core += [("silver_sb_matches", None), ("silver_lineups", None), ("silver_events", None)]
    return core


def run_stage(spark: SparkSession, stage: str, load_type: str, raw_root: str,
              snapshot_date: str, batch_id: str, base_dir: str | None = None,
              matches_glob: str | None = None, optimize: bool = True) -> None:
    if stage not in STAGES:
        raise ValueError(f"unknown stage {stage!r}; expected one of {STAGES}")

    if stage == "bronze":
        for feed, path, snap in bronze_jobs(load_type, raw_root, snapshot_date, base_dir, matches_glob):
            raw_to_bronze.run_feed(spark, feed, path, load_type, batch_id, snap)
    elif stage == "silver":
        for table, snap in silver_jobs(load_type, snapshot_date):
            bronze_to_silver.run_table(spark, table, batch_id, load_type, snap)
    else:
        silver_to_gold.run_all(spark, batch_id, load_type, optimize=optimize)


def run_end_to_end(spark: SparkSession, load_type: str, raw_root: str,
                   snapshot_date: str | None = None, batch_id: str | None = None,
                   base_dir: str | None = None, matches_glob: str | None = None,
                   optimize: bool = True) -> str:
    """Bronze -> Silver -> Gold under one batch id. Returns the batch id."""
    snapshot_date = snapshot_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    batch_id = batch_id or new_batch_id(f"e2e_{load_type.lower()}")
    for stage in STAGES:
        run_stage(spark, stage, load_type, raw_root, snapshot_date, batch_id,
                  base_dir, matches_glob, optimize)
    return batch_id
