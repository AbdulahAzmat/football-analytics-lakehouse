# Volume & Frequency (Section 2.2)

These are measured figures from a real full pull, not projections. The complete historical extraction was run on 2026-09-27 against the live API.

## Full load, as actually pulled

All 12 free-tier competitions, seasons 2023/24 through 2026/27.

| Metric | Measured |
|---|---|
| Total size | **15,871,684 bytes (15.87 MB)** |
| Files | 76 JSON |
| Match records | **14,001** |
| Team records | 284 |
| Squad player records | 7,149 |
| Standings rows | 212 |
| Scorer rows | 600 |
| Wall-clock time | ~9 minutes (paced at 10 req/min) |

### Matches per competition (4 seasons combined)

| Competition | Matches | Note |
|---|---|---|
| ELC (Championship) | 2,223 | Largest: 24 teams, 552 matches/season |
| BSA (Brasileirão) | 1,520 | |
| PD (La Liga) | 1,520 | |
| PL (Premier League) | 1,520 | |
| SA (Serie A) | 1,520 | |
| BL1 (Bundesliga) | 1,224 | 18 teams |
| DED (Eredivisie) | 1,224 | |
| FL1 (Ligue 1) | 1,224 | |
| PPL (Primeira Liga) | 1,224 | |
| CL (Champions League) | 647 | Multi-stage format |
| WC (World Cup) | 104 | Quadrennial |
| EC (European Championship) | 51 | Quadrennial |

### Expected failures

8 of the 84 planned calls returned HTTP 404, all for EC and WC in years those tournaments were not held (EC 2023/2025/2026, WC 2023/2024/2025), plus standings for EC and WC, which are knockout cups and have no league table. These are correct API behaviour, not access problems.

## Incremental load

Measured: one active matchday across 5 competitions = 20,697 bytes for 20 matches (~1,030 bytes/match).

- Daily run on match days: **~15 to 30 KB**
- Daily run on rest days: near zero (empty `matches` array, ~157 bytes)
- Weekly: **~100 to 150 KB**
- Monthly: **~400 to 600 KB**

## Projected Bronze size at end of semester

Full load (15.87 MB) + ~10 weeks of incrementals (~1.5 MB) ≈ **~17 MB**.

Comfortably inside Databricks Free Edition's default storage. (Community Edition was retired on 1 January 2026 and replaced by Free Edition, which is serverless and quota-limited.)

## Rate limiting

Free tier allows 10 requests/minute. The full backfill is 84 calls, which completed in about 9 minutes at 7-second spacing. Not a practical constraint.

## Honest note on scale

At ~14,000 match rows and ~16 MB, this dataset is small by Spark standards. It is large enough to demonstrate every Medallion technique the project requires (partitioning, dedup, SCD, star schema, incremental merge), but it would also fit in memory on a single machine. Using all 12 free competitions rather than a subset roughly doubled the row count; going further would require a paid tier, which is out of scope for this project. Modelling matches at team-match grain (one row per team per match) doubles the fact table to ~28,000 rows, and per-matchday standings snapshots would add substantially more if finer granularity is wanted in Phase 2.
