# Databricks notebook source
# Bronze layer — raw ingestion from football-data.org
#
# Lands raw API responses as-is, partitioned by ingestion date and load_type,
# with ingestion metadata added (ingestion_timestamp, source_endpoint, load_type, batch_id).
# No transformation beyond that — see notebooks/silver for cleaning/conformance.

import requests
import json
from datetime import datetime, timezone

API_TOKEN = dbutils.secrets.get(scope="football-lakehouse", key="football_data_token")  # noqa: F821
BASE_URL = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_TOKEN}

COMPETITIONS = ["PL", "CL", "PD", "BL1", "SA", "FL1"]
SEASONS = [2023, 2024, 2025]  # verified accessible on free tier — see docs/api-verification.md


def fetch_matches(competition_code: str, season: int) -> dict:
    resp = requests.get(
        f"{BASE_URL}/competitions/{competition_code}/matches",
        headers=HEADERS,
        params={"season": season},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def land_bronze(payload: dict, competition_code: str, season: int, load_type: str, batch_id: str):
    record = {
        "raw": payload,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "source_endpoint": f"/competitions/{competition_code}/matches",
        "load_type": load_type,
        "batch_id": batch_id,
        "competition_code": competition_code,
        "season": season,
    }
    df = spark.createDataFrame([json.dumps(record)], "string").toDF("value")  # noqa: F821
    (
        df.write.mode("append")
        .partitionBy("load_type")
        .format("delta")
        .save(f"/mnt/bronze/matches/date={datetime.now(timezone.utc).date()}")
    )


# Full load — run once to establish the historical baseline
if __name__ == "__main__":
    batch_id = datetime.now(timezone.utc).strftime("full_%Y%m%d%H%M%S")
    for comp in COMPETITIONS:
        for season in SEASONS:
            data = fetch_matches(comp, season)
            land_bronze(data, comp, season, load_type="FULL", batch_id=batch_id)
