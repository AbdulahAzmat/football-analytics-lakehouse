"""Central configuration for the Football Analytics Lakehouse pipeline.

Nothing in the pipeline hardcodes a path. Every layer root is derived from
LAKE_ROOT so the same code runs against DBFS on Databricks, a mounted ADLS
container on Azure, or a local folder during development.
"""

import os

# Root of the lakehouse. Override with the LAKE_ROOT env var.
#   Databricks : dbfs:/FileStore/football_lakehouse
#   Local test : /tmp/lakehouse
LAKE_ROOT = os.environ.get("LAKE_ROOT", "dbfs:/FileStore/football_lakehouse")

# Root of the raw landing zone (the files the ingestion scripts write).
RAW_ROOT = os.environ.get("RAW_ROOT", f"{LAKE_ROOT}/raw")

BRONZE_ROOT = f"{LAKE_ROOT}/bronze"
SILVER_ROOT = f"{LAKE_ROOT}/silver"
GOLD_ROOT = f"{LAKE_ROOT}/gold"
QUARANTINE_ROOT = f"{LAKE_ROOT}/quarantine"
OPS_ROOT = f"{LAKE_ROOT}/ops"

# Unity Catalog location the BI tool reads from. The pipeline keeps its tables as
# Delta files under LAKE_ROOT (path-based), which a BI tool cannot browse by name,
# so Gold and the audit tables are also published as catalog tables. The catalog
# is taken from the volume path (/Volumes/<catalog>/<schema>/...) unless overridden.
def _volume_catalog():
    parts = LAKE_ROOT.split("/")
    return parts[2] if LAKE_ROOT.startswith("/Volumes/") and len(parts) > 2 else None


PUBLISH_CATALOG = os.environ.get("PUBLISH_CATALOG", _volume_catalog())
PUBLISH_SCHEMA = os.environ.get("PUBLISH_SCHEMA", "football_gold")

# Audit table (requirement: dedicated logging tables)
EXECUTION_LOG_PATH = f"{OPS_ROOT}/pipeline_execution_logs"

# Storage format. Delta on Databricks/Azure. The local test harness overrides
# this to "parquet" because Delta's JARs are not available offline.
TABLE_FORMAT = os.environ.get("TABLE_FORMAT", "delta")

# football-data.org free tier (TIER_ONE) competitions, verified live.
FD_COMPETITIONS = [
    "PL", "ELC", "PD", "BL1", "SA", "FL1",
    "DED", "PPL", "BSA", "CL", "EC", "WC",
]

# Free tier returns HTTP 403 for season 2022 and earlier.
FD_SEASONS = [2023, 2024, 2025, 2026]

# StatsBomb open data: competition_id/season_id pairs in the raw zone.
#   9/281 = 1. Bundesliga 2023/2024 (overlaps football-data.org BL1 season 2023)
#   7/235 = Ligue 1 2022/2023
SB_COMPETITIONS = ["9/281", "7/235"]


# Natural keys used for MERGE (upsert) on each table. These are what make the
# pipeline idempotent: re-running a batch matches on these and updates in place
# rather than appending duplicates.
MERGE_KEYS = {
    "bronze_fd_matches":    ["match_id"],
    "bronze_fd_standings":  ["competition_code", "snapshot_date", "team_id", "stage", "group_name"],
    "bronze_fd_scorers":    ["competition_code", "snapshot_date", "player_id"],
    "bronze_fd_teams":      ["competition_code", "team_id"],
    "bronze_sb_matches":    ["match_id"],
    "bronze_sb_events":     ["event_id"],
    "bronze_sb_lineups":    ["match_id", "team_id", "player_id"],

    "silver_matches":       ["match_id"],
    "silver_standings":     ["competition_code", "snapshot_date", "team_id", "stage", "group_name"],
    "silver_scorers":       ["competition_code", "snapshot_date", "player_id"],
    "silver_teams":         ["competition_code", "team_id"],
    "silver_events":        ["event_id"],
    "silver_lineups":       ["match_id", "team_id", "player_id"],
    "silver_sb_matches":    ["match_id"],

    # Gold. Dimensions are keyed on their deterministic surrogate key, facts on
    # their grain. Aggregates are not listed: they are fully recomputed
    # (overwrite), because a late score correction changes every cumulative
    # figure after it, so there is no row-level key to merge on.
    "dim_competition":      ["competition_key"],
    "dim_season":           ["season_key"],
    "dim_team":             ["team_key"],
    "dim_player":           ["player_key"],
    "fact_matches":         ["match_id"],
    "fact_team_match":      ["match_id", "venue"],
    "fact_top_scorers":     ["competition_key", "snapshot_date_key", "player_key"],
}

# Columns every table carries (requirement: metadata integration).
METADATA_COLUMNS = ["load_timestamp", "batch_id", "load_type", "source_file"]


def table_path(layer: str, table: str) -> str:
    roots = {
        "bronze": BRONZE_ROOT,
        "silver": SILVER_ROOT,
        "gold": GOLD_ROOT,
        "quarantine": QUARANTINE_ROOT,
        "ops": OPS_ROOT,
    }
    if layer not in roots:
        raise ValueError(f"unknown layer {layer!r}; expected one of {sorted(roots)}")
    return f"{roots[layer]}/{table}"
