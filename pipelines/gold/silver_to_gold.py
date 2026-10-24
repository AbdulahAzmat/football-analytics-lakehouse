"""Silver -> Gold: the dimensional (star schema) model for BI.

Gold is not a copy of Silver. Silver is shaped for correctness (typed, deduped,
one table per source feed); Gold is shaped for questions: a few narrow fact
tables at a declared grain, surrounded by small descriptive dimensions, plus
pre-aggregated tables for the visuals that would otherwise re-aggregate 28k
rows on every slicer click.

Only the football-data.org feed is modelled here, because that is what the
Phase 1 business questions are about (team form, home/away, top scorers).
StatsBomb stays in Silver for now.

    dimensions   dim_date  dim_competition  dim_season  dim_team  dim_player
    facts        fact_matches        grain: one row per match
                 fact_team_match     grain: one row per team per match
                 fact_top_scorers    grain: one row per player per competition
                                            per snapshot date
    aggregates   agg_team_season_performance
                 agg_home_away_split
                 agg_team_matchday_progress

Refresh strategy (the rubric asks for "MERGE or overwrite, depending on the
dimensional strategy", so the choice is explicit per table):

  * dim_competition/season/team/player  MERGE on the surrogate key (SCD Type 1:
                                        an attribute change updates in place).
  * dim_date                            OVERWRITE. It is a deterministic spine.
  * facts                               MERGE on the grain key, so a corrected
                                        score updates the row rather than
                                        duplicating it.
  * aggregates                          OVERWRITE. One corrected result changes
                                        every cumulative figure after it, so
                                        there is no row-level key to merge on.

Every table is wrapped in RunLogger, so Silver-to-Gold shows up in the same
execution log as the earlier layers, with rows read (rows aggregated), rows
written and duration.

    python -m pipelines.gold.silver_to_gold --load-type INCREMENTAL
    python -m pipelines.gold.silver_to_gold --table fact_matches
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from ..common import config, io_utils
from ..common.audit import RunLogger, new_batch_id

LAYER = "Silver-to-Gold"
VALIDATION_LAYER = "Gold-Validation"
OPTIMIZE_LAYER = "Gold-Optimize"

# Surrogate key of the "Unknown" member every dimension carries, so a fact row
# whose lookup fails still joins instead of silently dropping out of a report.
UNKNOWN_KEY = -1

# Hand-maintained reference data: football-data.org does not return these on
# the matches payload. The 12 codes are the free-tier set in config.
COMPETITION_REFERENCE = {
    "PL": ("LEAGUE", "England"), "ELC": ("LEAGUE", "England"),
    "PD": ("LEAGUE", "Spain"), "BL1": ("LEAGUE", "Germany"),
    "SA": ("LEAGUE", "Italy"), "FL1": ("LEAGUE", "France"),
    "DED": ("LEAGUE", "Netherlands"), "PPL": ("LEAGUE", "Portugal"),
    "BSA": ("LEAGUE", "Brazil"),
    "CL": ("CUP", "Europe"), "EC": ("CUP", "Europe"), "WC": ("CUP", "World"),
}

# Stages where a points table and a matchday progression are meaningful.
TABLE_STAGES = ["REGULAR_SEASON", "LEAGUE_STAGE", "GROUP_STAGE"]


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _silver(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.format(config.TABLE_FORMAT).load(config.table_path("silver", table))


def _gold(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.format(config.TABLE_FORMAT).load(config.table_path("gold", table))


def _col(x):
    return F.col(x) if isinstance(x, str) else x


def _sk(x) -> "F.Column":
    """Deterministic surrogate key: a positive 31-bit hash of the natural key.

    Deterministic matters. An identity column or row_number would hand out a
    different number for the same team after a rebuild, which breaks MERGE and
    orphans every fact row pointing at it. A hash gives the same key for the
    same input on every run, on any cluster, with no sequence state to keep.

    31 bits rather than 64 so the key survives Power BI and Tableau, which
    lose precision on integers above 2^53. The price is a (tiny) collision
    chance, which validate_gold() checks for rather than assumes away.
    A NULL natural key maps to UNKNOWN_KEY, not to hash(NULL).
    """
    c = _col(x)
    return (F.when(c.isNull(), F.lit(UNKNOWN_KEY))
             .otherwise(F.pmod(F.xxhash64(c), F.lit(2147483647)))
             .cast("int"))


def _date_key(x) -> "F.Column":
    return F.coalesce(F.date_format(_col(x), "yyyyMMdd").cast("int"), F.lit(UNKNOWN_KEY))


def _season_label(start="season_start_date", end="season_end_date") -> "F.Column":
    """'2025/26' for a season that crosses the new year, '2026' for one that
    does not (Brazil, World Cup, Euros). Unknown when the start is missing."""
    s, e = _col(start), _col(end)
    sy, ey = F.year(s), F.year(e)
    return (F.when(s.isNull(), F.lit("Unknown"))
             .when(F.coalesce(sy == ey, F.lit(True)), sy.cast("string"))
             .otherwise(F.concat(sy.cast("string"), F.lit("/"),
                                 F.lpad((ey % 100).cast("string"), 2, "0"))))


def _season_key(label) -> "F.Column":
    lab = _col(label)
    return _sk(F.when(lab == "Unknown", F.lit(None).cast("string")).otherwise(lab))


def _latest(df: DataFrame, keys: list[str], order: list) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def _with_unknown(spark: SparkSession, df: DataFrame, **values) -> DataFrame:
    """Append the 'Unknown' member. Columns not named in `values` are NULL."""
    schema = StructType([StructField(f.name, f.dataType, True) for f in df.schema.fields])
    row = tuple(values.get(f.name) for f in df.schema.fields)
    return df.unionByName(spark.createDataFrame([row], schema))


def _stamp(df: DataFrame, batch_id: str, load_type: str) -> DataFrame:
    """Lineage columns. Gold deliberately has no source_file: a Gold row is
    derived from many Silver rows, so there is no single file to point at."""
    return (df.withColumn("load_timestamp", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
              .withColumn("batch_id", F.lit(batch_id))
              .withColumn("load_type", F.lit(load_type)))


def _season_lookup(spark: SparkSession) -> DataFrame:
    """(competition_code, season_id) -> season_label, from the matches feed."""
    m = _silver(spark, "silver_matches")
    return (m.select("competition_code", "season_id", "season_start_date", "season_end_date")
             .dropDuplicates(["competition_code", "season_id"])
             .select("competition_code", "season_id", _season_label().alias("season_label")))


# --------------------------------------------------------------------------
# dimensions
# --------------------------------------------------------------------------

def build_dim_date(spark):
    m = _silver(spark, "silver_matches")
    sc = _silver(spark, "silver_scorers")
    mb = m.agg(F.min("match_date").alias("a"), F.max("match_date").alias("b")).first()
    sb = sc.agg(F.min("snapshot_date").alias("a"), F.max("snapshot_date").alias("b")).first()
    lows = [d for d in (mb["a"], sb["a"]) if d is not None]
    highs = [d for d in (mb["b"], sb["b"]) if d is not None]
    if not lows:
        raise ValueError("silver_matches has no dates: cannot build dim_date")
    lo, hi = min(lows), max(highs)

    d = F.col("full_date")
    spine = spark.sql(f"SELECT explode(sequence(DATE'{lo}', DATE'{hi}', INTERVAL 1 DAY)) AS full_date")
    out = spine.select(
        F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"),
        d.alias("full_date"),
        F.year(d).alias("year"),
        F.quarter(d).alias("quarter"),
        F.month(d).alias("month"),
        F.date_format(d, "MMMM").alias("month_name"),
        F.dayofmonth(d).alias("day_of_month"),
        F.dayofweek(d).alias("day_of_week"),          # 1 = Sunday
        F.date_format(d, "EEEE").alias("day_name"),
        F.weekofyear(d).alias("iso_week"),
        F.dayofweek(d).isin(1, 7).alias("is_weekend"),
    )
    out = _with_unknown(spark, out, date_key=UNKNOWN_KEY, month_name="Unknown",
                        day_name="Unknown", is_weekend=False)
    return out, m


def build_dim_competition(spark):
    m = _silver(spark, "silver_matches")
    base = _latest(m.select("competition_code", "competition_id", "competition_name", "load_timestamp"),
                   ["competition_code"], [F.col("load_timestamp").desc()])
    ref = spark.createDataFrame(
        [(c, t, k) for c, (t, k) in COMPETITION_REFERENCE.items()],
        "competition_code string, competition_type string, country string")
    out = (base.join(ref, "competition_code", "left")
               .select(_sk("competition_code").alias("competition_key"),
                       "competition_code", "competition_id", "competition_name",
                       F.coalesce("competition_type", F.lit("UNKNOWN")).alias("competition_type"),
                       F.coalesce("country", F.lit("UNKNOWN")).alias("country")))
    out = _with_unknown(spark, out, competition_key=UNKNOWN_KEY, competition_code="UNKNOWN",
                        competition_name="Unknown", competition_type="UNKNOWN", country="UNKNOWN")
    return out, m


def build_dim_season(spark):
    m = _silver(spark, "silver_matches")
    x = (m.select("season_start_date", "season_end_date").dropDuplicates()
          .withColumn("season_label", _season_label())
          .filter(F.col("season_label") != "Unknown"))
    cur = ((F.current_date() >= F.col("season_start_date"))
           & (F.current_date() <= F.col("season_end_date")))
    out = (x.groupBy("season_label")
            .agg(F.min(F.year("season_start_date")).alias("season_start_year"),
                 F.coalesce(F.max(cur.cast("int")) > 0, F.lit(False)).alias("is_current"))
            .select(_season_key("season_label").alias("season_key"),
                    "season_label", "season_start_year", "is_current"))
    out = _with_unknown(spark, out, season_key=UNKNOWN_KEY, season_label="Unknown", is_current=False)
    return out, m


def build_dim_team(spark):
    teams = _silver(spark, "silver_teams")
    m = _silver(spark, "silver_matches")
    sc = _silver(spark, "silver_scorers")

    # One row per team_id: the same club appears once per competition it is in.
    known = (_latest(teams, ["team_id"], [F.col("load_timestamp").desc(), F.col("competition_code")])
             .select("team_id", "team_name", "team_short_name", "team_tla", "founded", "club_colors",
                     F.col("venue").alias("home_venue"), F.col("area_name").alias("country"),
                     "coach_name", F.lit(False).alias("is_inferred")))

    # Late-arriving dimension members. The teams endpoint does not cover every
    # club that plays a match (69 of 315 team ids in the full load are missing,
    # mostly cup opponents), so any team seen in a fact but absent from
    # silver_teams gets a stub row rather than an orphaned foreign key.
    seen = (m.select(F.col("home_team_id").alias("team_id"), F.col("home_team_name").alias("team_name"),
                     F.col("home_team_tla").alias("team_tla"))
             .unionByName(m.select(F.col("away_team_id").alias("team_id"),
                                   F.col("away_team_name").alias("team_name"),
                                   F.col("away_team_tla").alias("team_tla")))
             .unionByName(sc.select("team_id", "team_name", F.lit(None).cast("string").alias("team_tla")))
             .filter(F.col("team_id").isNotNull())
             .groupBy("team_id")
             .agg(F.max("team_name").alias("team_name"), F.max("team_tla").alias("team_tla")))
    inferred = (seen.join(known.select("team_id"), "team_id", "left_anti")
                    .select("team_id", "team_name", F.col("team_name").alias("team_short_name"), "team_tla",
                            F.lit(None).cast("int").alias("founded"),
                            F.lit(None).cast("string").alias("club_colors"),
                            F.lit(None).cast("string").alias("home_venue"),
                            F.lit(None).cast("string").alias("country"),
                            F.lit(None).cast("string").alias("coach_name"),
                            F.lit(True).alias("is_inferred")))

    out = (known.unionByName(inferred)
                .select(_sk("team_id").alias("team_key"), "team_id", "team_name", "team_short_name",
                        "team_tla", "founded", "club_colors", "home_venue", "country", "coach_name",
                        "is_inferred"))
    out = _with_unknown(spark, out, team_key=UNKNOWN_KEY, team_name="Unknown",
                        team_short_name="Unknown", team_tla="UNK", is_inferred=False)
    return out, teams


def build_dim_player(spark):
    sc = _silver(spark, "silver_scorers")
    # PII decision (Phase 1, section 3.2): date of birth and nationality are
    # quasi-identifiers. They stay in Silver and are deliberately NOT selected
    # here, because this is the table Power BI reads.
    latest = _latest(sc, ["player_id"], [F.col("snapshot_date").desc(), F.col("load_timestamp").desc()])
    out = latest.select(_sk("player_id").alias("player_key"), "player_id", "player_name", "player_position")
    out = _with_unknown(spark, out, player_key=UNKNOWN_KEY, player_name="Unknown")
    return out, sc


# --------------------------------------------------------------------------
# facts
# --------------------------------------------------------------------------

def _fact_matches_frame(spark) -> tuple[DataFrame, DataFrame]:
    m = _silver(spark, "silver_matches")
    label = _season_label()
    ok = (F.col("is_finished") & F.col("home_goals").isNotNull() & F.col("away_goals").isNotNull())
    out = m.select(
        F.col("match_id"),
        _date_key("match_date").alias("date_key"),
        F.col("kickoff_utc"),
        _sk("competition_code").alias("competition_key"),
        _season_key(label).alias("season_key"),
        _sk("home_team_id").alias("home_team_key"),
        _sk("away_team_id").alias("away_team_key"),
        F.col("matchday"), F.col("stage"), F.col("group_name"),
        F.col("match_status"), F.col("is_finished"),
        F.col("home_goals"), F.col("away_goals"),
        F.col("home_goals_ht"), F.col("away_goals_ht"),
        F.when(ok, F.col("home_goals") + F.col("away_goals")).alias("total_goals"),
        F.when(ok, F.col("home_goals") - F.col("away_goals")).alias("goal_difference"),
        F.col("result"),
        # AWARDED and POSTPONED matches are not FINISHED, so they carry no
        # points. A walkover has no played score to trust.
        F.when(ok, F.when(F.col("home_goals") > F.col("away_goals"), 3)
                    .when(F.col("home_goals") == F.col("away_goals"), 1).otherwise(0)).alias("home_points"),
        F.when(ok, F.when(F.col("away_goals") > F.col("home_goals"), 3)
                    .when(F.col("home_goals") == F.col("away_goals"), 1).otherwise(0)).alias("away_points"),
        F.col("source_last_updated"),
    )
    return out, m


def build_fact_matches(spark):
    return _fact_matches_frame(spark)


def build_fact_team_match(spark):
    fm, m = _fact_matches_frame(spark)
    common = ["match_id", "date_key", "kickoff_utc", "competition_key", "season_key",
              "matchday", "stage", "group_name", "match_status", "is_finished"]
    home = fm.select(*common, F.lit("HOME").alias("venue"),
                     F.col("home_team_key").alias("team_key"), F.col("away_team_key").alias("opponent_key"),
                     F.col("home_goals").alias("goals_for"), F.col("away_goals").alias("goals_against"))
    away = fm.select(*common, F.lit("AWAY").alias("venue"),
                     F.col("away_team_key").alias("team_key"), F.col("home_team_key").alias("opponent_key"),
                     F.col("away_goals").alias("goals_for"), F.col("home_goals").alias("goals_against"))
    tm = home.unionByName(away)
    ok = (F.col("is_finished") & F.col("goals_for").isNotNull() & F.col("goals_against").isNotNull())
    out = (tm.withColumn("goal_difference", F.when(ok, F.col("goals_for") - F.col("goals_against")))
             .withColumn("result", F.when(ok, F.when(F.col("goals_for") > F.col("goals_against"), "WIN")
                                               .when(F.col("goals_for") < F.col("goals_against"), "LOSS")
                                               .otherwise("DRAW")))
             .withColumn("points", F.when(F.col("result") == "WIN", 3)
                                    .when(F.col("result") == "DRAW", 1)
                                    .when(F.col("result") == "LOSS", 0))
             .withColumn("is_clean_sheet", F.when(ok, F.col("goals_against") == 0)))
    return out, m


def build_fact_top_scorers(spark):
    sc = _silver(spark, "silver_scorers")
    j = sc.join(_season_lookup(spark), ["competition_code", "season_id"], "left")
    label = F.coalesce(F.col("season_label"), F.lit("Unknown"))
    out = j.select(
        _sk("competition_code").alias("competition_key"),
        _season_key(label).alias("season_key"),
        _date_key("snapshot_date").alias("snapshot_date_key"),
        F.col("snapshot_date"),
        _sk("player_id").alias("player_key"),
        _sk("team_id").alias("team_key"),
        F.col("played_matches"), F.col("goals"), F.col("assists"), F.col("penalties"),
        (F.col("goals") - F.col("penalties")).alias("non_penalty_goals"),
        F.when(F.col("played_matches") > 0,
               F.round(F.col("goals") / F.col("played_matches"), 2)).alias("goals_per_match"),
    )
    return out, sc


# --------------------------------------------------------------------------
# aggregates (read the Gold fact, not Silver: Gold is built on Gold)
# --------------------------------------------------------------------------

def _wdl(col: str = "result"):
    return (F.sum((F.col(col) == "WIN").cast("int")).alias("wins"),
            F.sum((F.col(col) == "DRAW").cast("int")).alias("draws"),
            F.sum((F.col(col) == "LOSS").cast("int")).alias("losses"))


def build_agg_team_season_performance(spark):
    f = _gold(spark, "fact_team_match")
    fin = f.filter(F.col("is_finished") & F.col("result").isNotNull())
    g = (fin.groupBy("team_key", "competition_key", "season_key")
            .agg(F.count("*").alias("matches_played"), *_wdl(),
                 F.sum("goals_for").alias("goals_for"),
                 F.sum("goals_against").alias("goals_against"),
                 F.sum("points").alias("points")))
    w = Window.partitionBy("competition_key", "season_key").orderBy(
        F.col("points").desc(), F.col("goal_difference").desc(), F.col("goals_for").desc())
    out = (g.withColumn("goal_difference", F.col("goals_for") - F.col("goals_against"))
            .withColumn("points_per_game", F.round(F.col("points") / F.col("matches_played"), 2))
            .withColumn("win_rate", F.round(F.col("wins") / F.col("matches_played"), 3))
            # Only meaningful where the competition is a league; filter on
            # dim_competition.competition_type = 'LEAGUE' in the report.
            .withColumn("season_rank", F.rank().over(w)))
    return out, f


def build_agg_home_away_split(spark):
    f = _gold(spark, "fact_team_match")
    fin = f.filter(F.col("is_finished") & F.col("result").isNotNull())
    g = (fin.groupBy("team_key", "competition_key", "season_key", "venue")
            .agg(F.count("*").alias("matches_played"), *_wdl(),
                 F.sum("goals_for").alias("goals_for"),
                 F.sum("goals_against").alias("goals_against"),
                 F.sum("points").alias("points")))
    n = F.col("matches_played")
    out = (g.withColumn("avg_goals_for", F.round(F.col("goals_for") / n, 2))
            .withColumn("avg_goals_against", F.round(F.col("goals_against") / n, 2))
            .withColumn("win_rate", F.round(F.col("wins") / n, 3))
            .withColumn("draw_rate", F.round(F.col("draws") / n, 3))
            .withColumn("loss_rate", F.round(F.col("losses") / n, 3))
            .withColumn("points_per_game", F.round(F.col("points") / n, 2)))
    return out, f


def build_agg_team_matchday_progress(spark):
    f = _gold(spark, "fact_team_match")
    fin = f.filter(F.col("is_finished") & F.col("result").isNotNull()
                   & F.col("matchday").isNotNull() & F.col("stage").isin(*TABLE_STAGES))
    grain = ["team_key", "competition_key", "season_key", "stage", "group_name", "matchday"]
    per_md = fin.groupBy(*grain).agg(F.sum("points").alias("points"),
                                     F.sum("goals_for").alias("goals_for"),
                                     F.sum("goals_against").alias("goals_against"))
    run_w = (Window.partitionBy("team_key", "competition_key", "season_key", "stage", "group_name")
                   .orderBy("matchday").rowsBetween(Window.unboundedPreceding, Window.currentRow))
    cum = (per_md.withColumn("cumulative_points", F.sum("points").over(run_w))
                 .withColumn("cumulative_goals_for", F.sum("goals_for").over(run_w))
                 .withColumn("cumulative_goals_against", F.sum("goals_against").over(run_w))
                 .withColumn("cumulative_goal_difference",
                             F.col("cumulative_goals_for") - F.col("cumulative_goals_against")))
    # Position among the teams that have a result for that matchday.
    pos_w = Window.partitionBy("competition_key", "season_key", "stage", "group_name", "matchday").orderBy(
        F.col("cumulative_points").desc(), F.col("cumulative_goal_difference").desc(),
        F.col("cumulative_goals_for").desc())
    out = cum.withColumn("league_position", F.rank().over(pos_w))
    return out, f


# --------------------------------------------------------------------------
# registry and execution
# --------------------------------------------------------------------------

DIMENSIONS = ["dim_date", "dim_competition", "dim_season", "dim_team", "dim_player"]
FACTS = ["fact_matches", "fact_team_match", "fact_top_scorers"]
AGGREGATES = ["agg_team_season_performance", "agg_home_away_split", "agg_team_matchday_progress"]
GOLD_TABLES = DIMENSIONS + FACTS + AGGREGATES          # build order: dims, facts, then aggregates

BUILDERS = {
    "dim_date": (build_dim_date, "overwrite"),
    "dim_competition": (build_dim_competition, "merge"),
    "dim_season": (build_dim_season, "merge"),
    "dim_team": (build_dim_team, "merge"),
    "dim_player": (build_dim_player, "merge"),
    "fact_matches": (build_fact_matches, "merge"),
    "fact_team_match": (build_fact_team_match, "merge"),
    "fact_top_scorers": (build_fact_top_scorers, "merge"),
    "agg_team_season_performance": (build_agg_team_season_performance, "overwrite"),
    "agg_home_away_split": (build_agg_home_away_split, "overwrite"),
    "agg_team_matchday_progress": (build_agg_team_matchday_progress, "overwrite"),
}


def _overwrite(spark: SparkSession, df: DataFrame, path: str) -> int:
    w = df.write.format(config.TABLE_FORMAT).mode("overwrite")
    if config.TABLE_FORMAT == "delta":
        w = w.option("overwriteSchema", "true")     # lets a new column land without a migration
    w.save(path)
    return spark.read.format(config.TABLE_FORMAT).load(path).count()


def run_table(spark: SparkSession, table: str, batch_id: str, load_type: str) -> None:
    builder, mode = BUILDERS[table]
    target = config.table_path("gold", table)

    with RunLogger(spark, LAYER, table, mode.upper(), batch_id, load_type) as run:
        built, source = builder(spark)
        read_n = source.count()                 # rows read (for aggregates: rows aggregated)
        stamped = _stamp(built, batch_id, load_type)

        if mode == "merge":
            res = io_utils.upsert(spark, stamped, target, config.MERGE_KEYS[table])
            run.record(read=read_n, inserted=res.inserted, updated=res.updated)
            print(f"[{table}] read={read_n} inserted={res.inserted} updated={res.updated}")
        else:
            n = _overwrite(spark, stamped, target)
            run.record(read=read_n, inserted=n, updated=0)
            print(f"[{table}] read={read_n} written={n} (overwrite)")


# --------------------------------------------------------------------------
# validation: the BI contract
# --------------------------------------------------------------------------

# (fact or aggregate column, dimension table, dimension key column)
FK_CHECKS = {
    "fact_matches": [("date_key", "dim_date", "date_key"),
                     ("competition_key", "dim_competition", "competition_key"),
                     ("season_key", "dim_season", "season_key"),
                     ("home_team_key", "dim_team", "team_key"),
                     ("away_team_key", "dim_team", "team_key")],
    "fact_team_match": [("date_key", "dim_date", "date_key"),
                        ("competition_key", "dim_competition", "competition_key"),
                        ("season_key", "dim_season", "season_key"),
                        ("team_key", "dim_team", "team_key"),
                        ("opponent_key", "dim_team", "team_key")],
    "fact_top_scorers": [("snapshot_date_key", "dim_date", "date_key"),
                         ("competition_key", "dim_competition", "competition_key"),
                         ("season_key", "dim_season", "season_key"),
                         ("player_key", "dim_player", "player_key"),
                         ("team_key", "dim_team", "team_key")],
    "agg_team_season_performance": [("team_key", "dim_team", "team_key"),
                                    ("competition_key", "dim_competition", "competition_key"),
                                    ("season_key", "dim_season", "season_key")],
    "agg_home_away_split": [("team_key", "dim_team", "team_key"),
                            ("competition_key", "dim_competition", "competition_key"),
                            ("season_key", "dim_season", "season_key")],
    "agg_team_matchday_progress": [("team_key", "dim_team", "team_key"),
                                   ("competition_key", "dim_competition", "competition_key"),
                                   ("season_key", "dim_season", "season_key")],
}

# The grain each table promises. Key uniqueness on dimensions also catches a
# hash collision in _sk().
GRAIN = {
    "dim_date": ["date_key"], "dim_competition": ["competition_key"], "dim_season": ["season_key"],
    "dim_team": ["team_key"], "dim_player": ["player_key"],
    "fact_matches": ["match_id"], "fact_team_match": ["match_id", "venue"],
    "fact_top_scorers": ["competition_key", "snapshot_date_key", "player_key"],
    "agg_team_season_performance": ["team_key", "competition_key", "season_key"],
    "agg_home_away_split": ["team_key", "competition_key", "season_key", "venue"],
    "agg_team_matchday_progress": ["team_key", "competition_key", "season_key", "stage", "group_name", "matchday"],
}


class GoldValidationError(RuntimeError):
    pass


def validate_gold(spark: SparkSession, batch_id: str, load_type: str) -> None:
    """Check grain uniqueness and foreign-key integrity; log each check.

    Every check writes its own audit row (layer Gold-Validation, rows_read =
    rows tested, rows_quarantined = violations). All checks run before anything
    raises, so one failure does not hide another.
    """
    failures: list[str] = []

    for table, keys in GRAIN.items():
        df = _gold(spark, table)
        try:
            with RunLogger(spark, VALIDATION_LAYER, table, f"grain:{'+'.join(keys)}",
                           batch_id, load_type) as run:
                total = df.count()
                distinct = df.select(*keys).distinct().count()
                run.record(read=total, quarantined=total - distinct)
                if total != distinct:
                    raise GoldValidationError(f"{table}: {total - distinct} duplicate grain rows")
        except GoldValidationError as e:
            failures.append(str(e))

    for table, checks in FK_CHECKS.items():
        df = _gold(spark, table)
        total = df.count()
        for fk, dim, dim_key in checks:
            try:
                with RunLogger(spark, VALIDATION_LAYER, table, f"fk:{fk}->{dim}.{dim_key}",
                               batch_id, load_type) as run:
                    dk = _gold(spark, dim).select(F.col(dim_key).alias("_dk")).distinct()
                    orphans = (df.select(fk).distinct()
                                 .join(dk, F.col(fk) == F.col("_dk"), "left_anti").count())
                    run.record(read=total, quarantined=orphans)
                    if orphans:
                        raise GoldValidationError(f"{table}.{fk}: {orphans} key(s) missing from {dim}")
            except GoldValidationError as e:
                failures.append(str(e))

    if failures:
        raise GoldValidationError("; ".join(failures))
    print(f"[validation] {sum(len(v) for v in FK_CHECKS.values()) + len(GRAIN)} checks passed")


# --------------------------------------------------------------------------
# read optimisation (Delta only)
# --------------------------------------------------------------------------

# Cluster on the columns the dashboard filters and joins on.
CLUSTER_PLAN = {
    "fact_matches": ["competition_key", "season_key"],
    "fact_team_match": ["team_key", "season_key"],
    "fact_top_scorers": ["competition_key", "snapshot_date_key"],
}


def optimize_gold(spark: SparkSession, batch_id: str, load_type: str) -> None:
    """Liquid clustering + OPTIMIZE, falling back to Z-ORDER, falling back to
    plain OPTIMIZE. An optimisation failure is logged but never fails the
    pipeline: the data is already correct, just not yet compacted."""
    if config.TABLE_FORMAT != "delta":
        print("[optimize] skipped (not a Delta run)")
        return
    for table, cols in CLUSTER_PLAN.items():
        path = config.table_path("gold", table)
        ident = f"delta.`{path}`"
        col_list = ", ".join(cols)
        try:
            with RunLogger(spark, OPTIMIZE_LAYER, table, "pending", batch_id, load_type) as run:
                try:
                    spark.sql(f"ALTER TABLE {ident} CLUSTER BY ({col_list})")
                    spark.sql(f"OPTIMIZE {ident}")
                    run.parameter = f"LIQUID_CLUSTER_BY({col_list})"
                except Exception:  # noqa: BLE001
                    try:
                        spark.sql(f"OPTIMIZE {ident} ZORDER BY ({col_list})")
                        run.parameter = f"ZORDER_BY({col_list})"
                    except Exception:  # noqa: BLE001
                        spark.sql(f"OPTIMIZE {ident}")
                        run.parameter = "OPTIMIZE_ONLY"
            print(f"[optimize] {table}: {run.parameter}")
        except Exception as e:  # noqa: BLE001
            print(f"[optimize] {table}: could not optimise ({e}); continuing")


def run_all(spark: SparkSession, batch_id: str, load_type: str,
            validate: bool = True, optimize: bool = True) -> None:
    """Build every Gold table in dependency order, then validate, then optimise."""
    for table in GOLD_TABLES:
        run_table(spark, table, batch_id, load_type)
    if validate:
        validate_gold(spark, batch_id, load_type)
    if optimize:
        optimize_gold(spark, batch_id, load_type)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Silver -> Gold loader")
    ap.add_argument("--table", choices=GOLD_TABLES, default=None,
                    help="build one table (default: all, in dependency order)")
    ap.add_argument("--load-type", default="INCREMENTAL", choices=["FULL", "INCREMENTAL"])
    ap.add_argument("--batch-id", default=None)
    ap.add_argument("--skip-validate", action="store_true")
    ap.add_argument("--skip-optimize", action="store_true")
    args = ap.parse_args(argv)

    spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()
    batch = args.batch_id or new_batch_id("gold")
    if args.table:
        run_table(spark, args.table, batch, args.load_type)
    else:
        run_all(spark, batch, args.load_type,
                validate=not args.skip_validate, optimize=not args.skip_optimize)
    return 0


if __name__ == "__main__":
    sys.exit(main())
