"""Raw -> Bronze.

Bronze is the landing layer. It flattens each feed to one row per business
entity, attaches ingestion metadata, and keeps the values as the source sent
them. Type casting and business rules belong in Silver, not here.

Everything is parameterised: nothing in this module knows what "today" is.
Pass --feed and --path (a glob) and it processes exactly that, which is what
makes backfills re-runnable for any historical window.

    # full backfill of every football-data.org match file
    python -m pipelines.bronze.raw_to_bronze --feed fd_matches \
        --path 'raw/football-data/full_load/matches_*.json' --load-type FULL

    # one day's standings snapshot
    python -m pipelines.bronze.raw_to_bronze --feed fd_standings \
        --path 'raw/football-data/daily/2026-10-09/standings_*.json' \
        --load-type INCREMENTAL
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..common import config, io_utils
from ..common.audit import RunLogger, new_batch_id
from ..common.schemas import ARRAY_ROOTED, RAW_SCHEMAS

LAYER = "Raw-to-Bronze"


def read_raw(spark: SparkSession, feed: str, path: str) -> DataFrame:
    """Read with an explicit schema. inferSchema is never used.

    `source_file` is materialised here, at read time, from the hidden
    `_metadata` column. Two reasons it has to happen here and not later:

      * Unity Catalog rejects input_file_name() outright
        (UC_COMMAND_NOT_SUPPORTED), so _metadata.file_path is the only way to
        know which file a row came from on a UC workspace.
      * _metadata is only resolvable against the file scan itself. Once the
        frame has been exploded or aggregated it is gone, so it is captured as
        an ordinary column immediately and carried through the flatteners.
    """
    if feed not in RAW_SCHEMAS:
        raise ValueError(f"unknown feed {feed!r}; known: {sorted(RAW_SCHEMAS)}")
    reader = (spark.read.schema(RAW_SCHEMAS[feed])
              .option("mode", "PERMISSIVE")
              .option("multiLine", "true"))   # both root shapes need this
    return reader.json(path).withColumn("source_file", F.col("_metadata.file_path"))


# --------------------------------------------------------------------------
# Per-feed flattening. Each returns one row per business entity.
# --------------------------------------------------------------------------

def _fd_matches(df: DataFrame) -> DataFrame:
    m = df.select(F.explode("matches").alias("m"),
                  F.col("competition.code").alias("file_competition_code"),
                  F.col("source_file"))
    return m.select(
        F.col("m.id").alias("match_id"),
        F.coalesce(F.col("m.competition.code"), F.col("file_competition_code")).alias("competition_code"),
        F.col("m.competition.id").alias("competition_id"),
        F.col("m.competition.name").alias("competition_name"),
        F.col("m.season.id").alias("season_id"),
        F.col("m.season.startDate").alias("season_start_date"),
        F.col("m.season.endDate").alias("season_end_date"),
        F.col("m.season.currentMatchday").alias("season_current_matchday"),
        F.col("m.utcDate").alias("utc_date"),
        F.col("m.status").alias("status"),
        F.col("m.matchday").alias("matchday"),
        F.col("m.stage").alias("stage"),
        F.col("m.group").alias("group_name"),
        F.col("m.lastUpdated").alias("source_last_updated"),
        F.col("m.homeTeam.id").alias("home_team_id"),
        F.col("m.homeTeam.name").alias("home_team_name"),
        F.col("m.homeTeam.tla").alias("home_team_tla"),
        F.col("m.awayTeam.id").alias("away_team_id"),
        F.col("m.awayTeam.name").alias("away_team_name"),
        F.col("m.awayTeam.tla").alias("away_team_tla"),
        F.col("m.score.winner").alias("score_winner"),
        F.col("m.score.duration").alias("score_duration"),
        F.col("m.score.fullTime.home").alias("full_time_home"),
        F.col("m.score.fullTime.away").alias("full_time_away"),
        F.col("m.score.halfTime.home").alias("half_time_home"),
        F.col("m.score.halfTime.away").alias("half_time_away"),
        F.col("m.referees").alias("referees"),
        F.col("source_file"),
    )


def _fd_standings(df: DataFrame) -> DataFrame:
    s = df.select(
        F.col("competition.code").alias("competition_code"),
        F.col("competition.id").alias("competition_id"),
        F.col("season.id").alias("season_id"),
        F.col("source_file"),
        F.explode("standings").alias("st"),
    )
    r = s.select("competition_code", "competition_id", "season_id", "source_file",
                 F.col("st.stage").alias("stage"),
                 F.col("st.type").alias("standing_type"),
                 F.coalesce(F.col("st.group"), F.lit("NONE")).alias("group_name"),
                 F.explode("st.table").alias("row"))
    return r.select(
        "competition_code", "competition_id", "season_id", "stage", "standing_type", "group_name",
        "source_file",
        F.col("row.position").alias("position"),
        F.col("row.team.id").alias("team_id"),
        F.col("row.team.name").alias("team_name"),
        F.col("row.team.tla").alias("team_tla"),
        F.col("row.playedGames").alias("played_games"),
        F.col("row.form").alias("form"),
        F.col("row.won").alias("won"),
        F.col("row.draw").alias("draw"),
        F.col("row.lost").alias("lost"),
        F.col("row.points").alias("points"),
        F.col("row.goalsFor").alias("goals_for"),
        F.col("row.goalsAgainst").alias("goals_against"),
        F.col("row.goalDifference").alias("goal_difference"),
    )


def _fd_scorers(df: DataFrame) -> DataFrame:
    s = df.select(
        F.col("competition.code").alias("competition_code"),
        F.col("season.id").alias("season_id"),
        F.col("source_file"),
        F.explode("scorers").alias("sc"),
    )
    return s.select(
        "competition_code", "season_id", "source_file",
        F.col("sc.player.id").alias("player_id"),
        F.col("sc.player.name").alias("player_name"),
        F.col("sc.player.dateOfBirth").alias("player_date_of_birth"),
        F.col("sc.player.nationality").alias("player_nationality"),
        F.col("sc.player.position").alias("player_position"),
        F.col("sc.team.id").alias("team_id"),
        F.col("sc.team.name").alias("team_name"),
        F.col("sc.playedMatches").alias("played_matches"),
        F.col("sc.goals").alias("goals"),
        F.col("sc.assists").alias("assists"),
        F.col("sc.penalties").alias("penalties"),
    )


def _fd_teams(df: DataFrame) -> DataFrame:
    t = df.select(
        F.col("competition.code").alias("competition_code"),
        F.col("season.id").alias("season_id"),
        F.col("source_file"),
        F.explode("teams").alias("t"),
    )
    return t.select(
        "competition_code", "season_id", "source_file",
        F.col("t.id").alias("team_id"),
        F.col("t.name").alias("team_name"),
        F.col("t.shortName").alias("team_short_name"),
        F.col("t.tla").alias("team_tla"),
        F.col("t.founded").alias("founded"),
        F.col("t.clubColors").alias("club_colors"),
        F.col("t.venue").alias("venue"),
        F.col("t.address").alias("address"),
        F.col("t.website").alias("website"),
        F.col("t.area.name").alias("area_name"),
        F.col("t.coach.id").alias("coach_id"),
        F.col("t.coach.name").alias("coach_name"),
        F.col("t.coach.nationality").alias("coach_nationality"),
        F.size(F.col("t.squad")).alias("squad_size"),
        F.col("t.squad").alias("squad"),
        F.col("t.lastUpdated").alias("source_last_updated"),
    )


def _sb_matches(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("match_id"),
        F.col("match_date"),
        F.col("kick_off"),
        F.col("competition.competition_id").alias("competition_id"),
        F.col("competition.competition_name").alias("competition_name"),
        F.col("competition.country_name").alias("country_name"),
        F.col("season.season_id").alias("season_id"),
        F.col("season.season_name").alias("season_name"),
        F.col("home_team.home_team_id").alias("home_team_id"),
        F.col("home_team.home_team_name").alias("home_team_name"),
        F.col("away_team.away_team_id").alias("away_team_id"),
        F.col("away_team.away_team_name").alias("away_team_name"),
        F.col("home_score"),
        F.col("away_score"),
        F.col("match_status"),
        F.col("match_week"),
        F.col("competition_stage.name").alias("competition_stage"),
        F.col("stadium.name").alias("stadium_name"),
        F.col("referee.name").alias("referee_name"),
        F.col("last_updated").alias("source_last_updated"),
        F.col("source_file"),
    )


def _match_id_from_filename():
    """StatsBomb events/lineups files do not carry match_id inside the records;
    it is only in the filename (events/3895292.json). Recover it from the path.

    Uses the source_file column materialised in read_raw rather than
    input_file_name(), which Unity Catalog refuses to run."""
    return F.regexp_extract(F.col("source_file"), r"/(\d+)\.json", 1).cast("long")


def _sb_events(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("id").alias("event_id"),
        _match_id_from_filename().alias("match_id"),
        F.col("index").alias("event_index"),
        F.col("period"),
        F.col("timestamp").alias("event_timestamp_raw"),
        F.col("minute"),
        F.col("second"),
        F.col("duration"),
        F.col("possession"),
        F.col("type.id").alias("type_id"),
        F.col("type.name").alias("type_name"),
        F.col("play_pattern.name").alias("play_pattern"),
        F.col("possession_team.id").alias("possession_team_id"),
        F.col("possession_team.name").alias("possession_team_name"),
        F.col("team.id").alias("team_id"),
        F.col("team.name").alias("team_name"),
        F.col("player.id").alias("player_id"),
        F.col("player.name").alias("player_name"),
        F.col("position.name").alias("position_name"),
        F.col("location").getItem(0).alias("location_x"),
        F.col("location").getItem(1).alias("location_y"),
        F.col("under_pressure"),
        F.col("counterpress"),
        F.col("out"),
        # pass detail
        F.col("pass.length").alias("pass_length"),
        F.col("pass.angle").alias("pass_angle"),
        F.col("pass.height.name").alias("pass_height"),
        F.col("pass.recipient.id").alias("pass_recipient_id"),
        F.col("pass.recipient.name").alias("pass_recipient_name"),
        F.col("pass.outcome.name").alias("pass_outcome"),
        F.col("pass.body_part.name").alias("pass_body_part"),
        F.col("pass.cross").alias("pass_is_cross"),
        F.col("pass.switch").alias("pass_is_switch"),
        F.col("pass.through_ball").alias("pass_is_through_ball"),
        F.col("pass.goal_assist").alias("pass_is_goal_assist"),
        F.col("pass.end_location").getItem(0).alias("pass_end_x"),
        F.col("pass.end_location").getItem(1).alias("pass_end_y"),
        # shot detail
        F.col("shot.statsbomb_xg").alias("shot_xg"),
        F.col("shot.outcome.name").alias("shot_outcome"),
        F.col("shot.technique.name").alias("shot_technique"),
        F.col("shot.body_part.name").alias("shot_body_part"),
        F.col("shot.type.name").alias("shot_type"),
        F.col("shot.first_time").alias("shot_first_time"),
        F.col("shot.end_location").getItem(0).alias("shot_end_x"),
        F.col("shot.end_location").getItem(1).alias("shot_end_y"),
        # carry / dribble / duel
        F.col("carry.end_location").getItem(0).alias("carry_end_x"),
        F.col("carry.end_location").getItem(1).alias("carry_end_y"),
        F.col("dribble.outcome.name").alias("dribble_outcome"),
        F.col("duel.type.name").alias("duel_type"),
        F.col("duel.outcome.name").alias("duel_outcome"),
        F.col("goalkeeper.type.name").alias("goalkeeper_action"),
        F.col("foul_committed.card.name").alias("foul_card"),
        F.col("substitution.replacement.name").alias("substitution_replacement"),
        F.col("source_file"),
    )


def _sb_lineups(df: DataFrame) -> DataFrame:
    base = df.select(
        _match_id_from_filename().alias("match_id"),
        F.col("team_id"),
        F.col("team_name"),
        F.col("source_file"),
        F.explode("lineup").alias("p"),
    )
    return base.select(
        "match_id", "team_id", "team_name", "source_file",
        F.col("p.player_id").alias("player_id"),
        F.col("p.player_name").alias("player_name"),
        F.col("p.player_nickname").alias("player_nickname"),
        F.col("p.jersey_number").alias("jersey_number"),
        F.col("p.country.name").alias("country_name"),
        F.size(F.coalesce(F.col("p.cards"), F.array())).alias("card_count"),
        F.col("p.positions").alias("positions"),
    )


FLATTENERS = {
    "fd_matches": _fd_matches,
    "fd_standings": _fd_standings,
    "fd_scorers": _fd_scorers,
    "fd_teams": _fd_teams,
    "sb_matches": _sb_matches,
    "sb_events": _sb_events,
    "sb_lineups": _sb_lineups,
}

TARGET_TABLE = {f: f"bronze_{f}" for f in FLATTENERS}

# Columns that must be present for a row to be allowed into Bronze.
REQUIRED = {
    "fd_matches": ["match_id"],
    "fd_standings": ["competition_code", "team_id"],
    "fd_scorers": ["competition_code", "player_id"],
    "fd_teams": ["competition_code", "team_id"],
    "sb_matches": ["match_id"],
    "sb_events": ["event_id", "match_id"],
    "sb_lineups": ["match_id", "team_id", "player_id"],
}


def run_feed(
    spark: SparkSession,
    feed: str,
    path: str,
    load_type: str,
    batch_id: str,
    snapshot_date: str | None = None,
) -> None:
    table = TARGET_TABLE[feed]
    target = config.table_path("bronze", table)

    with RunLogger(spark, LAYER, table, path, batch_id, load_type) as run:
        raw = read_raw(spark, feed, path)
        flat = FLATTENERS[feed](raw)

        # Daily snapshot feeds are keyed by the date they were captured, so the
        # same competition can be stored once per day and tracked over time.
        if feed in ("fd_standings", "fd_scorers"):
            if snapshot_date is None:
                raise ValueError(f"{feed} requires --snapshot-date")
            flat = flat.withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))

        flat = io_utils.add_metadata(flat, batch_id, load_type)
        read_n = flat.count()

        valid, invalid = io_utils.split_valid_invalid(flat, REQUIRED[feed])
        q = io_utils.quarantine(spark, invalid, table, "null_required_field", batch_id)

        res = io_utils.upsert(spark, valid, target, config.MERGE_KEYS[table])
        run.record(read=read_n, inserted=res.inserted, updated=res.updated, quarantined=q)
        print(f"[{table}] read={read_n} inserted={res.inserted} updated={res.updated} quarantined={q}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Raw -> Bronze loader")
    ap.add_argument("--feed", required=True, choices=sorted(FLATTENERS))
    ap.add_argument("--path", required=True, help="file or glob to process")
    ap.add_argument("--load-type", default="FULL", choices=["FULL", "INCREMENTAL"])
    ap.add_argument("--batch-id", default=None)
    ap.add_argument("--snapshot-date", default=None,
                    help="YYYY-MM-DD; required for fd_standings / fd_scorers")
    args = ap.parse_args(argv)

    spark = SparkSession.builder.appName(f"raw_to_bronze_{args.feed}").getOrCreate()
    batch_id = args.batch_id or new_batch_id("bronze")
    run_feed(spark, args.feed, args.path, args.load_type, batch_id, args.snapshot_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
