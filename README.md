# Football Analytics Lakehouse

An end-to-end Medallion Architecture (Bronze/Silver/Gold) data pipeline for football match, standings, and performance analytics — built with Spark on Databricks Community Edition, sourced from [football-data.org](https://www.football-data.org/), visualized in Power BI.

Data Engineering — Semester Project, Phase 1.

## Data source

**football-data.org REST API (v4)**, free tier. Verified live against the API on 2026-09-27 with a real registered key — see `docs/api-verification.md` for the exact endpoints tested and their results, including where the free tier's real limits differ from what's documented on the football-data.org site.

Tracked competitions: Premier League (PL), UEFA Champions League (CL), La Liga (PD), Bundesliga (BL1), Serie A (SA), Ligue 1 (FL1).

## Repository structure

```
data/samples/       Real sample payloads pulled from the live API (full load + incremental)
notebooks/bronze/   Raw ingestion notebooks (PySpark)
notebooks/silver/   Cleaning & conformance notebooks
notebooks/gold/     Dimensional modeling notebooks
docs/                Verification notes, volume estimates, data model
```

## Sample data

| File | Contents | Size |
|---|---|---|
| `full_load_sample_pl_2023-24.json` | Premier League, full 2023/24 season (380 matches) | ~383 KB |
| `full_load_sample_pl_2024-25.json` | Premier League, full 2024/25 season (380 matches) | ~384 KB |
| `full_load_sample_pl_2025-26.json` | Premier League, full 2025/26 season (380 matches) | ~383 KB |
| `full_load_sample_pl_teams.json` | Premier League team/squad metadata | ~88 KB |
| `full_load_sample_pl_standings.json` | Premier League current standings | ~5.5 KB |
| `incremental_load_sample_2026-09-20.json` | One matchday's fixtures across 5 tracked leagues (date-scoped pull) | ~20.2 KB |

See `docs/volume-estimate.md` for how these scale to the full multi-competition, multi-season pull.

## Setup

1. Register a free API key at https://www.football-data.org/client/register
2. Set it as an environment variable — never commit it:
   ```
   export FOOTBALL_DATA_TOKEN=your_token_here
   ```
3. Requests must include the header `X-Auth-Token: $FOOTBALL_DATA_TOKEN`.
4. Free tier is rate-limited to 10 requests/minute — pace calls accordingly (see `docs/api-verification.md`).
