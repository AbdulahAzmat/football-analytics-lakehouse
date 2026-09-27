# Databricks notebook source
# Bronze layer — daily incremental ingestion from football-data.org
#
# Scheduled daily job. Pulls only matches newly scheduled/finished/updated
# since the last run, using the date-scoped /v4/matches endpoint (mirrors a
# CDC-style pattern even though the source is a REST API, not a database).

import requests
import json
from datetime import datetime, timedelta, timezone

API_TOKEN = dbutils.secrets.get(scope="football-lakehouse", key="football_data_token")  # noqa: F821
BASE_URL = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_TOKEN}

TRACKED_COMPETITIONS = ["PL", "CL", "PD", "BL1", "SA", "FL1"]


def fetch_incremental(date_from: str, date_to: str) -> dict:
    resp = requests.get(
        f"{BASE_URL}/matches",
        headers=HEADERS,
        params={
            "competitions": ",".join(TRACKED_COMPETITIONS),
            "dateFrom": date_from,
            "dateTo": date_to,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    payload = fetch_incremental(str(yesterday), str(today))

    record = {
        "raw": payload,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "source_endpoint": "/matches",
        "load_type": "INCREMENTAL",
        "batch_id": f"incr_{today.strftime('%Y%m%d')}",
    }
    df = spark.createDataFrame([json.dumps(record)], "string").toDF("value")  # noqa: F821
    (
        df.write.mode("append")
        .format("delta")
        .save(f"/mnt/bronze/matches/date={today}")
    )
