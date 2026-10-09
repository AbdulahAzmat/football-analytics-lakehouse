"""Explicit schemas for every raw feed.

Requirement: strict schema-on-read. Spark's inferSchema is never used anywhere
in this pipeline. Every read declares its StructType up front, which means:

  * a source that silently changes a type does not silently change our table
  * reads are faster (no inference pass over the data)
  * the schema is reviewable in version control

Fields the source sends that are NOT declared here are dropped by Spark on
read. Bronze also keeps the raw JSON text in `_raw_payload`, so nothing is
lost permanently and a schema can be widened later without re-fetching.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# --------------------------------------------------------------------------
# Shared building blocks
# --------------------------------------------------------------------------

_ID_NAME = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
])

_FD_TEAM = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("shortName", StringType(), True),
    StructField("tla", StringType(), True),
    StructField("crest", StringType(), True),
])

_FD_COMPETITION = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("code", StringType(), True),
    StructField("type", StringType(), True),
    StructField("emblem", StringType(), True),
])

_FD_SEASON = StructType([
    StructField("id", LongType(), True),
    StructField("startDate", StringType(), True),   # cast to DateType in Silver
    StructField("endDate", StringType(), True),
    StructField("currentMatchday", IntegerType(), True),
    StructField("winner", StringType(), True),
])

_FD_SCORE_PAIR = StructType([
    StructField("home", IntegerType(), True),
    StructField("away", IntegerType(), True),
])

# --------------------------------------------------------------------------
# football-data.org : /v4/competitions/{id}/matches
# --------------------------------------------------------------------------

FD_MATCH = StructType([
    StructField("id", LongType(), True),
    StructField("utcDate", StringType(), True),      # ISO8601 -> TimestampType in Silver
    StructField("status", StringType(), True),
    StructField("matchday", IntegerType(), True),
    StructField("stage", StringType(), True),
    StructField("group", StringType(), True),
    StructField("lastUpdated", StringType(), True),
    StructField("area", _ID_NAME, True),
    StructField("competition", _FD_COMPETITION, True),
    StructField("season", _FD_SEASON, True),
    StructField("homeTeam", _FD_TEAM, True),
    StructField("awayTeam", _FD_TEAM, True),
    StructField("score", StructType([
        StructField("winner", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("fullTime", _FD_SCORE_PAIR, True),
        StructField("halfTime", _FD_SCORE_PAIR, True),
    ]), True),
    StructField("referees", ArrayType(StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("nationality", StringType(), True),
    ])), True),
])

FD_MATCHES_FILE = StructType([
    StructField("competition", _FD_COMPETITION, True),
    StructField("resultSet", StructType([
        StructField("count", IntegerType(), True),
        StructField("first", StringType(), True),
        StructField("last", StringType(), True),
        StructField("played", IntegerType(), True),
    ]), True),
    StructField("matches", ArrayType(FD_MATCH), True),
])

# --------------------------------------------------------------------------
# football-data.org : /v4/competitions/{id}/standings
# --------------------------------------------------------------------------

FD_STANDINGS_ROW = StructType([
    StructField("position", IntegerType(), True),
    StructField("team", _FD_TEAM, True),
    StructField("playedGames", IntegerType(), True),
    StructField("form", StringType(), True),
    StructField("won", IntegerType(), True),
    StructField("draw", IntegerType(), True),
    StructField("lost", IntegerType(), True),
    StructField("points", IntegerType(), True),
    StructField("goalsFor", IntegerType(), True),
    StructField("goalsAgainst", IntegerType(), True),
    StructField("goalDifference", IntegerType(), True),
])

FD_STANDINGS_FILE = StructType([
    StructField("area", _ID_NAME, True),
    StructField("competition", _FD_COMPETITION, True),
    StructField("season", _FD_SEASON, True),
    StructField("standings", ArrayType(StructType([
        StructField("stage", StringType(), True),
        StructField("type", StringType(), True),
        StructField("group", StringType(), True),
        StructField("table", ArrayType(FD_STANDINGS_ROW), True),
    ])), True),
])

# --------------------------------------------------------------------------
# football-data.org : /v4/competitions/{id}/scorers
# --------------------------------------------------------------------------

FD_SCORERS_FILE = StructType([
    StructField("count", IntegerType(), True),
    StructField("competition", _FD_COMPETITION, True),
    StructField("season", _FD_SEASON, True),
    StructField("scorers", ArrayType(StructType([
        StructField("player", StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("dateOfBirth", StringType(), True),
            StructField("nationality", StringType(), True),
            StructField("section", StringType(), True),
            StructField("position", StringType(), True),
            StructField("shirtNumber", IntegerType(), True),
        ]), True),
        StructField("team", _FD_TEAM, True),
        StructField("playedMatches", IntegerType(), True),
        StructField("goals", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("penalties", IntegerType(), True),
    ])), True),
])

# --------------------------------------------------------------------------
# football-data.org : /v4/competitions/{id}/teams
# --------------------------------------------------------------------------

FD_TEAMS_FILE = StructType([
    StructField("count", IntegerType(), True),
    StructField("competition", _FD_COMPETITION, True),
    StructField("season", _FD_SEASON, True),
    StructField("teams", ArrayType(StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("shortName", StringType(), True),
        StructField("tla", StringType(), True),
        StructField("crest", StringType(), True),
        StructField("address", StringType(), True),
        StructField("website", StringType(), True),
        StructField("founded", IntegerType(), True),
        StructField("clubColors", StringType(), True),
        StructField("venue", StringType(), True),
        StructField("lastUpdated", StringType(), True),
        StructField("area", _ID_NAME, True),
        StructField("coach", StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("nationality", StringType(), True),
        ]), True),
        StructField("squad", ArrayType(StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("position", StringType(), True),
            StructField("dateOfBirth", StringType(), True),
            StructField("nationality", StringType(), True),
        ])), True),
    ])), True),
])

# --------------------------------------------------------------------------
# StatsBomb : data/matches/{competition_id}/{season_id}.json
# --------------------------------------------------------------------------

SB_MATCH = StructType([
    StructField("match_id", LongType(), True),
    StructField("match_date", StringType(), True),   # -> DateType in Silver
    StructField("kick_off", StringType(), True),
    StructField("home_score", IntegerType(), True),
    StructField("away_score", IntegerType(), True),
    StructField("match_status", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("match_week", IntegerType(), True),
    StructField("competition", StructType([
        StructField("competition_id", LongType(), True),
        StructField("country_name", StringType(), True),
        StructField("competition_name", StringType(), True),
    ]), True),
    StructField("season", StructType([
        StructField("season_id", LongType(), True),
        StructField("season_name", StringType(), True),
    ]), True),
    StructField("home_team", StructType([
        StructField("home_team_id", LongType(), True),
        StructField("home_team_name", StringType(), True),
        StructField("home_team_gender", StringType(), True),
        StructField("country", _ID_NAME, True),
    ]), True),
    StructField("away_team", StructType([
        StructField("away_team_id", LongType(), True),
        StructField("away_team_name", StringType(), True),
        StructField("away_team_gender", StringType(), True),
        StructField("country", _ID_NAME, True),
    ]), True),
    StructField("competition_stage", _ID_NAME, True),
    StructField("stadium", _ID_NAME, True),
    StructField("referee", _ID_NAME, True),
])

SB_MATCHES_FILE = ArrayType(SB_MATCH)

# --------------------------------------------------------------------------
# StatsBomb : data/events/{match_id}.json
#
# Events are polymorphic: a Pass event carries a `pass` struct, a Shot carries
# `shot`, and so on. We declare the stable core plus the event-type structs we
# actually model. Anything else is preserved in Bronze's _raw_payload column.
# --------------------------------------------------------------------------

SB_EVENT = StructType([
    StructField("id", StringType(), True),          # UUID
    StructField("index", IntegerType(), True),
    StructField("period", IntegerType(), True),
    StructField("timestamp", StringType(), True),   # "00:00:01.077"
    StructField("minute", IntegerType(), True),
    StructField("second", IntegerType(), True),
    StructField("duration", DoubleType(), True),
    StructField("possession", IntegerType(), True),
    StructField("under_pressure", BooleanType(), True),
    StructField("counterpress", BooleanType(), True),
    StructField("off_camera", BooleanType(), True),
    StructField("out", BooleanType(), True),
    StructField("location", ArrayType(DoubleType()), True),
    StructField("related_events", ArrayType(StringType()), True),
    StructField("type", _ID_NAME, True),
    StructField("possession_team", _ID_NAME, True),
    StructField("play_pattern", _ID_NAME, True),
    StructField("team", _ID_NAME, True),
    StructField("player", _ID_NAME, True),
    StructField("position", _ID_NAME, True),
    StructField("pass", StructType([
        StructField("recipient", _ID_NAME, True),
        StructField("length", DoubleType(), True),
        StructField("angle", DoubleType(), True),
        StructField("height", _ID_NAME, True),
        StructField("end_location", ArrayType(DoubleType()), True),
        StructField("body_part", _ID_NAME, True),
        StructField("type", _ID_NAME, True),
        StructField("outcome", _ID_NAME, True),
        StructField("switch", BooleanType(), True),
        StructField("cross", BooleanType(), True),
        StructField("through_ball", BooleanType(), True),
        StructField("shot_assist", BooleanType(), True),
        StructField("goal_assist", BooleanType(), True),
    ]), True),
    StructField("shot", StructType([
        StructField("statsbomb_xg", DoubleType(), True),
        StructField("end_location", ArrayType(DoubleType()), True),
        StructField("outcome", _ID_NAME, True),
        StructField("technique", _ID_NAME, True),
        StructField("body_part", _ID_NAME, True),
        StructField("type", _ID_NAME, True),
        StructField("first_time", BooleanType(), True),
        StructField("one_on_one", BooleanType(), True),
        StructField("aerial_won", BooleanType(), True),
    ]), True),
    StructField("carry", StructType([
        StructField("end_location", ArrayType(DoubleType()), True),
    ]), True),
    StructField("dribble", StructType([
        StructField("outcome", _ID_NAME, True),
        StructField("nutmeg", BooleanType(), True),
        StructField("overrun", BooleanType(), True),
    ]), True),
    StructField("duel", StructType([
        StructField("type", _ID_NAME, True),
        StructField("outcome", _ID_NAME, True),
    ]), True),
    StructField("goalkeeper", StructType([
        StructField("type", _ID_NAME, True),
        StructField("outcome", _ID_NAME, True),
        StructField("position", _ID_NAME, True),
        StructField("technique", _ID_NAME, True),
        StructField("body_part", _ID_NAME, True),
    ]), True),
    StructField("ball_receipt", StructType([
        StructField("outcome", _ID_NAME, True),
    ]), True),
    StructField("foul_committed", StructType([
        StructField("card", _ID_NAME, True),
        StructField("type", _ID_NAME, True),
        StructField("advantage", BooleanType(), True),
    ]), True),
    StructField("substitution", StructType([
        StructField("outcome", _ID_NAME, True),
        StructField("replacement", _ID_NAME, True),
    ]), True),
])

SB_EVENTS_FILE = ArrayType(SB_EVENT)

# --------------------------------------------------------------------------
# StatsBomb : data/lineups/{match_id}.json
# --------------------------------------------------------------------------

SB_LINEUP_FILE = ArrayType(StructType([
    StructField("team_id", LongType(), True),
    StructField("team_name", StringType(), True),
    StructField("lineup", ArrayType(StructType([
        StructField("player_id", LongType(), True),
        StructField("player_name", StringType(), True),
        StructField("player_nickname", StringType(), True),
        StructField("jersey_number", IntegerType(), True),
        StructField("country", _ID_NAME, True),
        StructField("cards", ArrayType(StructType([
            StructField("time", StringType(), True),
            StructField("card_type", StringType(), True),
            StructField("reason", StringType(), True),
            StructField("period", IntegerType(), True),
        ])), True),
        StructField("positions", ArrayType(StructType([
            StructField("position_id", IntegerType(), True),
            StructField("position", StringType(), True),
            StructField("from", StringType(), True),
            StructField("to", StringType(), True),
            StructField("from_period", IntegerType(), True),
            StructField("to_period", IntegerType(), True),
            StructField("start_reason", StringType(), True),
            StructField("end_reason", StringType(), True),
        ])), True),
    ])), True),
]))


# Registry the Bronze loader looks a schema up by feed name.
#
# Note on shape: football-data.org returns a JSON OBJECT at the root, so the
# schema is the whole-file StructType and we explode the inner array. StatsBomb
# returns a JSON ARRAY at the root; with multiLine=true Spark parses each array
# element as its own row, so those feeds declare the ELEMENT struct, not an
# ArrayType wrapper.
RAW_SCHEMAS = {
    # root = object
    "fd_matches": FD_MATCHES_FILE,
    "fd_standings": FD_STANDINGS_FILE,
    "fd_scorers": FD_SCORERS_FILE,
    "fd_teams": FD_TEAMS_FILE,
    # root = array -> element schema
    "sb_matches": SB_MATCH,
    "sb_events": SB_EVENT,
    "sb_lineups": SB_LINEUP_FILE.elementType,
}

# Which feeds have an array at the root (read with multiLine=true).
ARRAY_ROOTED = {"sb_matches", "sb_events", "sb_lineups"}
