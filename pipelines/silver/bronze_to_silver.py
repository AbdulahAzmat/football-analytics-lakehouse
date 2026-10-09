"""Bronze -> Silver.

Silver is where the contract gets enforced. Bronze keeps values as the source
sent them (dates as strings, scores possibly null); Silver casts them to real
types, applies business rules, deduplicates, and rejects anything that cannot
be made to conform.

Casting is deliberately non-destructive: we cast into a new column and compare
against the original. A value that was present but failed to parse is a schema
drift signal (the source changed a format), so that row is quarantined with a
reason rather than silently becoming null.

Parameterised the same way as Bronze:

    python -m pipelines.silver.bronze_to_silver --table silver_matches
    python -m pipelines.silver.bronze_to_silver --table silver_standings \
        --snapshot-date 2026-10-09
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..common import config, io_utils
from ..common.audit import RunLogger, new_batch_id

LAYER = "Bronze-to-Silver"


def _read_bronze(spark: SparkSession, table: str, snapshot_date: str | None = None) -> DataFrame:
    df = spark.read.format(config.TABLE_FORMAT).load(config.table_path("bronze", table))
    if snapshot_date and "snapshot_date" in df.columns:
        df = df.filter(F.col("snapshot_date") == F.lit(snapshot_date).cast("date"))
    return df


def _cast_checked(df: DataFrame, src: str, dst: str, dtype: str, fmt: str | None = None) -> DataFrame:
    """Cast src -> dst, returning NULL (not raising) when the value will not parse.

    try_cast, not cast. Spark 4 and current Databricks runtimes run with ANSI
    mode ON by default, where a plain cast of a malformed value THROWS and kills
    the whole batch. That is the exact opposite of the behaviour we want: the
    requirement is to quarantine non-conforming records and keep going. try_cast
    yields NULL instead, which _drift_split() then detects as drift.

    (This was caught by tests/test_schema_drift.py - with a plain cast, a single
    malformed utcDate aborted the entire Silver build.)
    """
    if fmt:
        casted = (F.try_to_timestamp(F.col(src), F.lit(fmt)) if dtype == "timestamp"
                  else F.try_to_timestamp(F.col(src), F.lit(fmt)).try_cast("date"))
    else:
        casted = F.col(src).try_cast(dtype)
    return df.withColumn(dst, casted)


def _drift_split(df: DataFrame, pairs: list[tuple[str, str]]) -> tuple[DataFrame, DataFrame]:
    """Split on cast failures: (clean, drifted). `pairs` is [(source, casted)]."""
    bad = F.lit(False)
    for src, dst in pairs:
        bad = bad | (F.col(src).isNotNull() & F.col(dst).isNull())
    return df.filter(~bad), df.filter(bad)


# --------------------------------------------------------------------------
# Silver builders
# --------------------------------------------------------------------------

def build_silver_matches(spark, snapshot_date=None):
    b = _read_bronze(spark, "bronze_fd_matches")
    df = _cast_checked(b, "utc_date", "kickoff_utc", "timestamp")
    df = _cast_checked(df, "source_last_updated", "source_last_updated_ts", "timestamp")
    df = _cast_checked(df, "season_start_date", "season_start_dt", "date")
    df = _cast_checked(df, "season_end_date", "season_end_dt", "date")

    clean, drifted = _drift_split(df, [
        ("utc_date", "kickoff_utc"),
        ("source_last_updated", "source_last_updated_ts"),
    ])

    out = clean.select(
        F.col("match_id").cast("long").alias("match_id"),
        F.col("competition_code"),
        F.col("competition_id").cast("long").alias("competition_id"),
        F.col("competition_name"),
        F.col("season_id").cast("long").alias("season_id"),
        F.col("season_start_dt").alias("season_start_date"),
        F.col("season_end_dt").alias("season_end_date"),
        F.col("kickoff_utc"),
        F.to_date("kickoff_utc").alias("match_date"),
        F.col("status").alias("match_status"),
        F.col("matchday").cast("int").alias("matchday"),
        F.col("stage"),
        F.coalesce(F.col("group_name"), F.lit("NONE")).alias("group_name"),
        F.col("home_team_id").cast("long").alias("home_team_id"),
        F.col("home_team_name"),
        F.col("home_team_tla"),
        F.col("away_team_id").cast("long").alias("away_team_id"),
        F.col("away_team_name"),
        F.col("away_team_tla"),
        F.col("full_time_home").cast("int").alias("home_goals"),
        F.col("full_time_away").cast("int").alias("away_goals"),
        F.col("half_time_home").cast("int").alias("home_goals_ht"),
        F.col("half_time_away").cast("int").alias("away_goals_ht"),
        F.col("score_winner").alias("winner"),
        # business rules
        (F.col("status") == "FINISHED").alias("is_finished"),
        F.when(F.col("status") != "FINISHED", None)
         .when(F.col("full_time_home") > F.col("full_time_away"), "HOME_WIN")
         .when(F.col("full_time_home") < F.col("full_time_away"), "AWAY_WIN")
         .otherwise("DRAW").alias("result"),
        F.when(F.col("status") == "FINISHED",
               F.col("full_time_home") + F.col("full_time_away")).alias("total_goals"),
        F.col("source_last_updated_ts").alias("source_last_updated"),
        F.col("source_file"), F.col("batch_id"), F.col("load_type"),
    )
    return out, drifted, ["match_id"]


def build_silver_standings(spark, snapshot_date=None):
    b = _read_bronze(spark, "bronze_fd_standings", snapshot_date)
    out = b.select(
        F.col("competition_code"),
        F.col("competition_id").cast("long").alias("competition_id"),
        F.col("season_id").cast("long").alias("season_id"),
        F.col("snapshot_date").cast("date").alias("snapshot_date"),
        F.col("stage"),
        F.coalesce(F.col("group_name"), F.lit("NONE")).alias("group_name"),
        F.col("standing_type"),
        F.col("position").cast("int").alias("position"),
        F.col("team_id").cast("long").alias("team_id"),
        F.col("team_name"), F.col("team_tla"),
        F.col("played_games").cast("int").alias("played_games"),
        F.col("won").cast("int").alias("won"),
        F.col("draw").cast("int").alias("draw"),
        F.col("lost").cast("int").alias("lost"),
        F.col("points").cast("int").alias("points"),
        F.col("goals_for").cast("int").alias("goals_for"),
        F.col("goals_against").cast("int").alias("goals_against"),
        F.col("goal_difference").cast("int").alias("goal_difference"),
        F.col("form"),
        F.col("source_file"), F.col("batch_id"), F.col("load_type"),
    )
    empty = out.limit(0)
    return out, empty, ["competition_code", "snapshot_date", "team_id", "stage", "group_name"]


def build_silver_scorers(spark, snapshot_date=None):
    b = _read_bronze(spark, "bronze_fd_scorers", snapshot_date)
    df = _cast_checked(b, "player_date_of_birth", "player_dob", "date")
    clean, drifted = _drift_split(df, [("player_date_of_birth", "player_dob")])
    out = clean.select(
        F.col("competition_code"),
        F.col("season_id").cast("long").alias("season_id"),
        F.col("snapshot_date").cast("date").alias("snapshot_date"),
        F.col("player_id").cast("long").alias("player_id"),
        F.col("player_name"),
        F.col("player_dob").alias("player_date_of_birth"),
        F.col("player_nationality"), F.col("player_position"),
        F.col("team_id").cast("long").alias("team_id"),
        F.col("team_name"),
        F.col("played_matches").cast("int").alias("played_matches"),
        F.coalesce(F.col("goals"), F.lit(0)).cast("int").alias("goals"),
        F.coalesce(F.col("assists"), F.lit(0)).cast("int").alias("assists"),
        F.coalesce(F.col("penalties"), F.lit(0)).cast("int").alias("penalties"),
        F.col("source_file"), F.col("batch_id"), F.col("load_type"),
    )
    return out, drifted, ["competition_code", "snapshot_date", "player_id"]


def build_silver_teams(spark, snapshot_date=None):
    b = _read_bronze(spark, "bronze_fd_teams")
    out = b.select(
        F.col("competition_code"),
        F.col("team_id").cast("long").alias("team_id"),
        F.col("team_name"), F.col("team_short_name"), F.col("team_tla"),
        F.col("founded").cast("int").alias("founded"),
        F.col("club_colors"), F.col("venue"), F.col("area_name"),
        F.col("coach_id").cast("long").alias("coach_id"),
        F.col("coach_name"), F.col("coach_nationality"),
        F.col("squad_size").cast("int").alias("squad_size"),
        F.col("source_file"), F.col("batch_id"), F.col("load_type"),
    )
    empty = out.limit(0)
    return out, empty, ["competition_code", "team_id"]


def build_silver_events(spark, snapshot_date=None):
    b = _read_bronze(spark, "bronze_sb_events")
    # StatsBomb timestamps are "HH:MM:SS.mmm" within a period, not clock times.
    # Convert to elapsed seconds, which is what analysis actually needs.
    # try_cast for the same ANSI-mode reason as _cast_checked: a malformed
    # timestamp must null out and be quarantined, not abort the batch.
    parts = F.split(F.col("event_timestamp_raw"), ":")
    elapsed = (parts.getItem(0).try_cast("double") * 3600
               + parts.getItem(1).try_cast("double") * 60
               + parts.getItem(2).try_cast("double"))
    df = b.withColumn("event_seconds", elapsed)
    clean, drifted = _drift_split(df, [("event_timestamp_raw", "event_seconds")])

    out = clean.select(
        F.col("event_id"),
        F.col("match_id").cast("long").alias("match_id"),
        F.col("event_index").cast("int").alias("event_index"),
        F.col("period").cast("int").alias("period"),
        F.col("event_seconds").cast("double").alias("event_seconds"),
        F.col("minute").cast("int").alias("minute"),
        F.col("second").cast("int").alias("second"),
        F.col("duration").cast("double").alias("duration"),
        F.col("type_name").alias("event_type"),
        F.col("play_pattern"),
        F.col("possession").cast("int").alias("possession"),
        F.col("possession_team_name").alias("possession_team"),
        F.col("team_id").cast("long").alias("team_id"),
        F.col("team_name"),
        F.col("player_id").cast("long").alias("player_id"),
        F.col("player_name"),
        F.col("position_name").alias("position"),
        F.col("location_x").cast("double").alias("location_x"),
        F.col("location_y").cast("double").alias("location_y"),
        F.coalesce(F.col("under_pressure"), F.lit(False)).alias("under_pressure"),
        F.col("pass_length").cast("double").alias("pass_length"),
        F.col("pass_angle").cast("double").alias("pass_angle"),
        F.col("pass_height"), F.col("pass_outcome"), F.col("pass_body_part"),
        F.col("pass_recipient_name"),
        F.col("pass_end_x").cast("double").alias("pass_end_x"),
        F.col("pass_end_y").cast("double").alias("pass_end_y"),
        F.coalesce(F.col("pass_is_cross"), F.lit(False)).alias("pass_is_cross"),
        F.coalesce(F.col("pass_is_goal_assist"), F.lit(False)).alias("pass_is_goal_assist"),
        F.col("shot_xg").cast("double").alias("shot_xg"),
        F.col("shot_outcome"), F.col("shot_technique"), F.col("shot_body_part"), F.col("shot_type"),
        F.col("shot_end_x").cast("double").alias("shot_end_x"),
        F.col("shot_end_y").cast("double").alias("shot_end_y"),
        F.col("carry_end_x").cast("double").alias("carry_end_x"),
        F.col("carry_end_y").cast("double").alias("carry_end_y"),
        F.col("dribble_outcome"), F.col("duel_type"), F.col("duel_outcome"),
        F.col("goalkeeper_action"), F.col("foul_card"),
        # business rules
        (F.col("type_name") == "Shot").alias("is_shot"),
        ((F.col("type_name") == "Shot") & (F.col("shot_outcome") == "Goal")).alias("is_goal"),
        ((F.col("type_name") == "Pass") & F.col("pass_outcome").isNull()).alias("is_completed_pass"),
        F.col("source_file"), F.col("batch_id"), F.col("load_type"),
    )
    return out, drifted, ["event_id"]


def build_silver_lineups(spark, snapshot_date=None):
    b = _read_bronze(spark, "bronze_sb_lineups")
    out = b.select(
        F.col("match_id").cast("long").alias("match_id"),
        F.col("team_id").cast("long").alias("team_id"),
        F.col("team_name"),
        F.col("player_id").cast("long").alias("player_id"),
        F.col("player_name"),
        F.col("player_nickname"),
        F.col("jersey_number").cast("int").alias("jersey_number"),
        F.col("country_name"),
        F.col("card_count").cast("int").alias("card_count"),
        F.col("source_file"), F.col("batch_id"), F.col("load_type"),
    )
    empty = out.limit(0)
    return out, empty, ["match_id", "team_id", "player_id"]


def build_silver_sb_matches(spark, snapshot_date=None):
    b = _read_bronze(spark, "bronze_sb_matches")
    df = _cast_checked(b, "match_date", "match_dt", "date")
    clean, drifted = _drift_split(df, [("match_date", "match_dt")])
    out = clean.select(
        F.col("match_id").cast("long").alias("match_id"),
        F.col("match_dt").alias("match_date"),
        F.col("kick_off"),
        F.col("competition_id").cast("long").alias("competition_id"),
        F.col("competition_name"), F.col("country_name"),
        F.col("season_id").cast("long").alias("season_id"),
        F.col("season_name"),
        F.col("home_team_id").cast("long").alias("home_team_id"),
        F.col("home_team_name"),
        F.col("away_team_id").cast("long").alias("away_team_id"),
        F.col("away_team_name"),
        F.col("home_score").cast("int").alias("home_goals"),
        F.col("away_score").cast("int").alias("away_goals"),
        F.col("match_status"),
        F.col("match_week").cast("int").alias("match_week"),
        F.col("competition_stage"), F.col("stadium_name"), F.col("referee_name"),
        F.col("source_file"), F.col("batch_id"), F.col("load_type"),
    )
    return out, drifted, ["match_id"]


BUILDERS = {
    "silver_matches": build_silver_matches,
    "silver_standings": build_silver_standings,
    "silver_scorers": build_silver_scorers,
    "silver_teams": build_silver_teams,
    "silver_events": build_silver_events,
    "silver_lineups": build_silver_lineups,
    "silver_sb_matches": build_silver_sb_matches,
}


def run_table(spark: SparkSession, table: str, batch_id: str, load_type: str,
              snapshot_date: str | None = None) -> None:
    target = config.table_path("silver", table)
    param = snapshot_date or "ALL"

    with RunLogger(spark, LAYER, table, param, batch_id, load_type) as run:
        built, drifted, keys = BUILDERS[table](spark, snapshot_date)
        built = io_utils.add_metadata(built.drop("load_timestamp", "batch_id", "load_type"),
                                      batch_id, load_type, source_file_col="source_file")
        read_n = built.count()
        q = io_utils.quarantine(spark, drifted, table, "cast_failed_schema_drift", batch_id)
        res = io_utils.upsert(spark, built, target, keys)
        run.record(read=read_n, inserted=res.inserted, updated=res.updated, quarantined=q)
        print(f"[{table}] read={read_n} inserted={res.inserted} updated={res.updated} quarantined={q}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Bronze -> Silver loader")
    ap.add_argument("--table", required=True, choices=sorted(BUILDERS))
    ap.add_argument("--load-type", default="FULL", choices=["FULL", "INCREMENTAL"])
    ap.add_argument("--batch-id", default=None)
    ap.add_argument("--snapshot-date", default=None)
    args = ap.parse_args(argv)

    spark = SparkSession.builder.appName(f"bronze_to_silver_{args.table}").getOrCreate()
    run_table(spark, args.table, args.batch_id or new_batch_id("silver"),
              args.load_type, args.snapshot_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
