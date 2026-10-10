# Football Analytics Lakehouse

End-to-end Medallion Architecture (Bronze / Silver / Gold) data pipeline for football match, event and performance analytics. Built with PySpark on Databricks Free Edition, sourced from two independent providers, visualised in Power BI.

Data Engineering, semester project. **Phase 2: Bronze and Silver layers.**

---

## 1. Data sources

| | football-data.org | StatsBomb open data |
|---|---|---|
| Shape | Match-level aggregates | Ball-by-ball event detail |
| Coverage | 12 competitions, 4 seasons | 2 competition-seasons, 66 matches |
| Grain | one row per match | ~4,000 events per match |
| Access | REST API, free key, 10 req/min | Public GitHub repo |
| Rows delivered | 14,001 matches | 267,255 events |

The two were chosen to be complementary rather than redundant. football-data.org gives **breadth** (every match in twelve competitions) but tells you only the scoreline. StatsBomb gives **depth** (every pass, shot, carry, pressure, with pitch coordinates and expected-goals values) for a much smaller set of matches.

They overlap deliberately: **1. Bundesliga 2023/24 exists in both sources**, so the two can be joined on competition + season + teams + date, letting a single match be analysed at both levels.

### Why a second source was added

Phase 1 review flagged that a 15.87 MB full load and a 15–30 KB daily load were too small to justify Spark, and that football-data.org's free tier stops at season 2023. Both points were correct. The fix:

| | Phase 1 | Phase 2 | Target |
|---|---|---|---|
| Full load | 15.87 MB | **248.7 MB** | ~200 MB |
| Daily load | 15–30 KB | **~1.85 MB** | ≥ 1 MB |
| Largest table | 14,001 rows | **267,255 rows** | — |

The daily load grew because it no longer pulls only date-scoped match updates. It now captures a full daily **snapshot** of standings, scorers and squads for all 12 competitions, which is what makes change-over-time analysis possible: diffing consecutive snapshots yields each team's league position by day and each player's goals by day, neither of which the API exposes as a time series.

---

## 2. Architecture

```
raw/                                  Immutable landing zone, source format
  football-data/full_load/            matches_{COMP}_{SEASON}.json, teams_, standings_, scorers_
  football-data/daily/{date}/         daily snapshots (standings, scorers, teams, match window)
  statsbomb/                          competitions, matches/, events/, lineups/

bronze/        One row per business entity. Explicit schema, flattened,
               values as the source sent them. Ingestion metadata attached.

silver/        Typed, cast, deduplicated, business rules applied.
               Non-conforming rows diverted to quarantine.

quarantine/    Rows that failed the contract, with the reason and batch id.

ops/
  pipeline_execution_logs/            One row per table per run: layer, parameter,
                                      start/end, status, rows in/updated/quarantined.
```

Storage format is **Delta** on Databricks (set by `TABLE_FORMAT`). The offline test harness overrides it to Parquet because Delta's JARs need Maven, which the sandbox cannot reach.

### Repository layout

```
pipelines/
  common/config.py        paths, competition lists, merge keys
  common/schemas.py       explicit StructType for every feed (no inferSchema anywhere)
  common/audit.py         RunLogger + pipeline_execution_logs schema
  common/io_utils.py      idempotent upsert (MERGE INTO), quarantine, drift split
  bronze/raw_to_bronze.py Raw -> Bronze, one flattener per feed
  silver/bronze_to_silver.py  Bronze -> Silver, casting + business rules
scripts/
  ingest_statsbomb.py     downloads the StatsBomb source
  daily_snapshot.py       daily incremental snapshot from football-data.org
tests/
  run_pipeline_local.py   end-to-end run + idempotency proof on real data
  test_schema_drift.py    forces every failure mode, asserts quarantine works
data/
  full_load/              football-data.org full extraction (15.87 MB)
  statsbomb/              StatsBomb open data (232.8 MB)
  samples/                curated Phase 1 samples
```

---

## 3. Requirements and where they are implemented

| Requirement | Where | Verified |
|---|---|---|
| Explicit `StructType`, no `inferSchema` | `pipelines/common/schemas.py` | all 7 feeds load |
| Strict casting into Silver | `bronze_to_silver._cast_checked` | timestamps, dates, ints |
| `load_timestamp` on every record | `io_utils.add_metadata` | present on all 14 tables |
| Idempotency via `MERGE INTO` | `io_utils.upsert` | run twice, counts identical |
| Parameterised backfills | `--path`, `--snapshot-date`, `--batch-id` | see §5 |
| Schema drift handling | `quarantine`, `split_valid_invalid`, `_drift_split` | 4 failure modes tested |
| Dedicated logging table | `pipeline_execution_logs` | cumulative, see note below |
| Audit metrics | `RunLogger.record` | layer, parameter, times, status, row counts |

### Where to see each feature running

Grading is on what the pipeline does, not what this file claims, so section 6
of `notebooks/run_phase2_databricks.py` demonstrates each item live in the
workspace rather than describing it:

| Notebook section | Shows |
|---|---|
| 2 | Bronze and Silver built from raw, into Delta |
| 3 | the same run repeated - row counts unchanged, `inserted=0, updated=N` |
| 4 | the audit log, one row per table per run |
| 6.1 | every declared `StructType`, and a grep proving no `inferSchema` anywhere |
| 6.2 | a backfill of one competition and season by `--path`, with a chosen `--batch-id`, leaving the table total unmoved |
| 6.3 | an `INCREMENTAL` snapshot load, so the log holds both load types and the table holds more than one day |
| 6.4 | a deliberately corrupt file loaded live: null key and failed cast quarantined, undeclared column dropped, wrong type nulled, batch still `SUCCESS` |
| 6.5 | the final log, grouped by load type |

That run is saved in [`proof/`](proof/) so it can be read without a Databricks
account:

* **[`proof/phase2_run_with_outputs.ipynb`](proof/phase2_run_with_outputs.ipynb)**
  - the executed notebook. GitHub renders it in the browser, including the
  execution log table (all 64 rows, both load types) and the quarantine output.
* **[`proof/phase2_run_output.html`](proof/phase2_run_output.html)** - the same
  run as a standalone page. Download and open in a browser.

Both are from the run of 2026-10-10 against Delta tables in a Unity Catalog
volume on Databricks Free Edition, serverless compute.

---

## 4. Test results

Both suites run the real production modules against the real downloaded data.

### End-to-end + idempotency — `tests/run_pipeline_local.py`

Every table was loaded, then loaded again with a fresh batch id over identical input. Row counts after pass 2 are identical to pass 1, and the second pass reports `inserted=0, updated=N`, which is the MERGE behaving correctly rather than appending.

| table | rows | idempotent |
|---|---|---|
| `bronze_fd_matches` | 14,001 | yes |
| `bronze_fd_standings` | 212 | yes |
| `bronze_fd_scorers` | 600 | yes |
| `bronze_fd_teams` | 284 | yes |
| `bronze_sb_matches` | 66 | yes |
| `bronze_sb_lineups` | 2,620 | yes |
| `bronze_sb_events` | **267,255** | yes |
| `silver_matches` | 14,001 | yes |
| `silver_standings` | 212 | yes |
| `silver_scorers` | 600 | yes |
| `silver_teams` | 284 | yes |
| `silver_sb_matches` | 66 | yes |
| `silver_lineups` | 2,620 | yes |
| `silver_events` | **267,255** | yes |

28 audit rows written, 0 failures.

### Schema drift — `tests/test_schema_drift.py`

The real source data is clean, so the main run quarantines nothing and the error path never executes. This suite deliberately corrupts input to force each failure mode:

| case | injected | result |
|---|---|---|
| Null primary key | match with `id: null` | quarantined at Bronze, batch continued |
| Type drift | `utcDate: "not-a-timestamp-at-all"` | quarantined at Silver, batch continued |
| New upstream column | undeclared `brandNewFieldFromUpstream` | ignored by the explicit schema, row loaded |
| Changed type | `fullTime.home: "two"` (int → string) | nulled by PERMISSIVE read, row loaded |

**This test found a real bug.** The cast originally used `Column.cast()`, which under Spark 4 / current Databricks runtimes (ANSI mode on by default) **raises** on a malformed value rather than returning null — so a single bad timestamp aborted the entire Silver build, the exact opposite of the requirement. Fixed by switching to `try_cast` / `try_to_timestamp`. See the comment in `_cast_checked`.

### Config consistency — `tests/test_config_consistency.py`

Guards that every table the pipeline can build has merge keys registered. Added after `silver_sb_matches` was found missing from `MERGE_KEYS`: it still ran (each Silver builder returns its own keys) but the generated data dictionary showed that table with no primary key, which the spec requires. The test fails loudly rather than letting the docs quietly go wrong.

### Databricks run — Delta `MERGE INTO` verified

The pipeline was run end to end on **Databricks Free Edition, serverless compute, Unity Catalog, `TABLE_FORMAT=delta`**, against the full dataset. Two passes over identical input with fresh batch ids:

| table | rows | pass 2 | idempotent |
|---|---|---|---|
| `bronze_fd_matches` | 14,001 | `inserted=0 updated=14001` | yes |
| `bronze_fd_standings` | 212 | `inserted=0 updated=212` | yes |
| `bronze_fd_scorers` | 600 | `inserted=0 updated=600` | yes |
| `bronze_fd_teams` | 284 | `inserted=0 updated=284` | yes |
| `bronze_sb_matches` | 66 | `inserted=0 updated=66` | yes |
| `bronze_sb_lineups` | 2,620 | `inserted=0 updated=2620` | yes |
| `bronze_sb_events` | **267,255** | `inserted=0 updated=267255` | yes |
| `silver_matches` | 14,001 | `inserted=0 updated=14001` | yes |
| `silver_standings` | 212 | `inserted=0 updated=212` | yes |
| `silver_scorers` | 600 | `inserted=0 updated=600` | yes |
| `silver_teams` | 284 | `inserted=0 updated=284` | yes |
| `silver_sb_matches` | 66 | `inserted=0 updated=66` | yes |
| `silver_lineups` | 2,620 | `inserted=0 updated=2620` | yes |
| `silver_events` | **267,255** | `inserted=0 updated=267255` | yes |

`IDEMPOTENCY (Delta MERGE): PASS`

`inserted=0, updated=N` on the second pass is the point: the merge matched every existing key and updated in place rather than appending. Row counts are unchanged across both passes.

### What the Databricks run caught that local testing could not

The offline harness runs Parquet on open-source Spark. That is structurally blind to Unity Catalog and serverless restrictions, and three real defects only surfaced on the first real run:

| failure | cause | fix |
|---|---|---|
| `DBFS_DISABLED` | newer workspaces disable the public DBFS root, so `dbfs:/FileStore` was never writable | root the lakehouse at a Unity Catalog volume, discovered at runtime |
| `UC_COMMAND_NOT_SUPPORTED: input_file_name` | UC bans `input_file_name()`. It backed the `source_file` column **and** recovered `match_id` for StatsBomb events, which carry it only in the filename — all 267k event rows would have failed their required-field check and been quarantined | capture `_metadata.file_path` at the file scan and carry it through the flatteners |
| `JVM_ATTRIBUTE_NOT_SUPPORTED: sparkContext` | the "don't overwrite a non-Delta path" guard reached into `spark.sparkContext._jvm`; serverless blocks driver JVM access | re-express the probe in pure DataFrame API, classifying the read failure |

The second is the one worth noting: it would not have thrown a visible error in a less strict setup, it would have silently emptied the largest table.

Everything else — explicit schemas, casting, metadata, parameterisation, drift quarantine, audit logging — behaved on Databricks exactly as it did locally.

### A note on the `FAILED` rows in the execution log

`pipeline_execution_logs` is cumulative: it lives in the lakehouse volume and is
appended to by every run, so it spans development as well as the final run. It
therefore contains rows with `status=FAILED`, each carrying the exception that
caused it. Those are the three defects in the table above, recorded as they
happened.

They are left in deliberately. `RunLogger` writes its row on exit whether the
body succeeded or raised, and a log that has only ever recorded SUCCESS does not
demonstrate that it captures anything else. The failures are dated, attributed to
a table and a parameter, and carry their error text, which is what the logging
requirement is actually for.

The final run itself is clean: the audit log grew by exactly the number of runs
executed, with no new failures.

---

## 5. Execution guide

All entry points are parameterised. Nothing processes "today" implicitly.

### Setup

```bash
export LAKE_ROOT=dbfs:/FileStore/football_lakehouse   # or a local path
export TABLE_FORMAT=delta                             # parquet for offline runs
export FOOTBALL_DATA_TOKEN=<your key>                 # ingestion only
```

### Ingesting raw data

```bash
# StatsBomb (one-off, ~233 MB)
python scripts/ingest_statsbomb.py --out data/statsbomb

# today's football-data.org snapshot (~1.85 MB)
python scripts/daily_snapshot.py

# a specific historical date
python scripts/daily_snapshot.py --date 2026-10-01
```

### Standard incremental load

Processes one day. `--snapshot-date` is what keys the snapshot, so the same competition can be stored once per day and tracked over time.

```bash
DATE=2026-10-09

python -m pipelines.bronze.raw_to_bronze \
    --feed fd_standings \
    --path "raw/football-data/daily/$DATE/standings_*.json" \
    --load-type INCREMENTAL --snapshot-date $DATE

python -m pipelines.silver.bronze_to_silver \
    --table silver_standings \
    --load-type INCREMENTAL --snapshot-date $DATE
```

### Backfill

Same code, different parameters. Because the writes are idempotent upserts, a backfill can be re-run over a window that was already processed without creating duplicates.

```bash
# re-process every historical match file
python -m pipelines.bronze.raw_to_bronze \
    --feed fd_matches \
    --path "raw/football-data/full_load/matches_*.json" \
    --load-type FULL

# one competition, one season
python -m pipelines.bronze.raw_to_bronze \
    --feed fd_matches \
    --path "raw/football-data/full_load/matches_BL1_2023.json" \
    --load-type FULL

# a range of snapshot dates
for d in 2026-10-01 2026-10-02 2026-10-03; do
  python -m pipelines.bronze.raw_to_bronze --feed fd_scorers \
      --path "raw/football-data/daily/$d/scorers_*.json" \
      --load-type INCREMENTAL --snapshot-date $d
  python -m pipelines.silver.bronze_to_silver --table silver_scorers \
      --load-type INCREMENTAL --snapshot-date $d
done

# rebuild a whole Silver table from Bronze
python -m pipelines.silver.bronze_to_silver --table silver_events --load-type FULL
```

| flag | meaning |
|---|---|
| `--feed` / `--table` | which feed or target table |
| `--path` | file or glob to process; the backfill control |
| `--load-type` | `FULL` or `INCREMENTAL`, recorded in the audit log |
| `--snapshot-date` | date key for snapshot feeds; required for standings and scorers |
| `--batch-id` | override the generated id to group several jobs into one logical run |

### Running the tests

```bash
python tests/run_pipeline_local.py     # end-to-end + idempotency
python tests/test_schema_drift.py      # forced failure modes
```

### Inspecting the audit log

```python
log = spark.read.format("delta").load(f"{LAKE_ROOT}/ops/pipeline_execution_logs")
log.orderBy("start_time", ascending=False).show()
log.filter("status = 'FAILED'").show()
```

---

## 6. Data dictionary

Generated from the built tables, not hand-written, so it cannot drift from what the pipeline actually produces.

Every table carries these four metadata columns:

| column | type | meaning |
|---|---|---|
| `load_timestamp` | timestamp | when this record was processed by this layer |
| `batch_id` | string | groups all tables written by one pipeline run |
| `load_type` | string | `FULL` or `INCREMENTAL` |
| `source_file` | string | the raw file this record came from |

Note `load_timestamp` is deliberately distinct from `source_last_updated`, which is the provider's own modification time and is kept as a business column.

### Bronze layer

#### `bronze_fd_matches`  (14,001 rows)

**Primary key:** `match_id`

| column | type | PK |
|---|---|---|
| `match_id` | bigint | Y |
| `competition_code` | string |  |
| `competition_id` | bigint |  |
| `competition_name` | string |  |
| `season_id` | bigint |  |
| `season_start_date` | string |  |
| `season_end_date` | string |  |
| `season_current_matchday` | int |  |
| `utc_date` | string |  |
| `status` | string |  |
| `matchday` | int |  |
| `stage` | string |  |
| `group_name` | string |  |
| `source_last_updated` | string |  |
| `home_team_id` | bigint |  |
| `home_team_name` | string |  |
| `home_team_tla` | string |  |
| `away_team_id` | bigint |  |
| `away_team_name` | string |  |
| `away_team_tla` | string |  |
| `score_winner` | string |  |
| `score_duration` | string |  |
| `full_time_home` | int |  |
| `full_time_away` | int |  |
| `half_time_home` | int |  |
| `half_time_away` | int |  |
| `referees` | array<struct<id:bigint,name:string,type:string,nationality:string>> |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |
| `source_file` | string |  |

#### `bronze_fd_standings`  (212 rows)

**Primary key:** `competition_code`, `snapshot_date`, `team_id`, `stage`, `group_name`

| column | type | PK |
|---|---|---|
| `competition_code` | string | Y |
| `snapshot_date` | date | Y |
| `team_id` | bigint | Y |
| `stage` | string | Y |
| `group_name` | string | Y |
| `competition_id` | bigint |  |
| `season_id` | bigint |  |
| `standing_type` | string |  |
| `position` | int |  |
| `team_name` | string |  |
| `team_tla` | string |  |
| `played_games` | int |  |
| `form` | string |  |
| `won` | int |  |
| `draw` | int |  |
| `lost` | int |  |
| `points` | int |  |
| `goals_for` | int |  |
| `goals_against` | int |  |
| `goal_difference` | int |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |
| `source_file` | string |  |

#### `bronze_fd_scorers`  (600 rows)

**Primary key:** `competition_code`, `snapshot_date`, `player_id`

| column | type | PK |
|---|---|---|
| `competition_code` | string | Y |
| `snapshot_date` | date | Y |
| `player_id` | bigint | Y |
| `season_id` | bigint |  |
| `player_name` | string |  |
| `player_date_of_birth` | string |  |
| `player_nationality` | string |  |
| `player_position` | string |  |
| `team_id` | bigint |  |
| `team_name` | string |  |
| `played_matches` | int |  |
| `goals` | int |  |
| `assists` | int |  |
| `penalties` | int |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |
| `source_file` | string |  |

#### `bronze_fd_teams`  (284 rows)

**Primary key:** `competition_code`, `team_id`

| column | type | PK |
|---|---|---|
| `competition_code` | string | Y |
| `team_id` | bigint | Y |
| `season_id` | bigint |  |
| `team_name` | string |  |
| `team_short_name` | string |  |
| `team_tla` | string |  |
| `founded` | int |  |
| `club_colors` | string |  |
| `venue` | string |  |
| `address` | string |  |
| `website` | string |  |
| `area_name` | string |  |
| `coach_id` | bigint |  |
| `coach_name` | string |  |
| `coach_nationality` | string |  |
| `squad_size` | int |  |
| `squad` | array<struct<id:bigint,name:string,position:string,dateOfBirth:string,nationality:string>> |  |
| `source_last_updated` | string |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |
| `source_file` | string |  |

#### `bronze_sb_matches`  (66 rows)

**Primary key:** `match_id`

| column | type | PK |
|---|---|---|
| `match_id` | bigint | Y |
| `match_date` | string |  |
| `kick_off` | string |  |
| `competition_id` | bigint |  |
| `competition_name` | string |  |
| `country_name` | string |  |
| `season_id` | bigint |  |
| `season_name` | string |  |
| `home_team_id` | bigint |  |
| `home_team_name` | string |  |
| `away_team_id` | bigint |  |
| `away_team_name` | string |  |
| `home_score` | int |  |
| `away_score` | int |  |
| `match_status` | string |  |
| `match_week` | int |  |
| `competition_stage` | string |  |
| `stadium_name` | string |  |
| `referee_name` | string |  |
| `source_last_updated` | string |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |
| `source_file` | string |  |

#### `bronze_sb_events`  (267,255 rows)

**Primary key:** `event_id`

| column | type | PK |
|---|---|---|
| `event_id` | string | Y |
| `match_id` | bigint |  |
| `event_index` | int |  |
| `period` | int |  |
| `event_timestamp_raw` | string |  |
| `minute` | int |  |
| `second` | int |  |
| `duration` | double |  |
| `possession` | int |  |
| `type_id` | bigint |  |
| `type_name` | string |  |
| `play_pattern` | string |  |
| `possession_team_id` | bigint |  |
| `possession_team_name` | string |  |
| `team_id` | bigint |  |
| `team_name` | string |  |
| `player_id` | bigint |  |
| `player_name` | string |  |
| `position_name` | string |  |
| `location_x` | double |  |
| `location_y` | double |  |
| `under_pressure` | boolean |  |
| `counterpress` | boolean |  |
| `out` | boolean |  |
| `pass_length` | double |  |
| `pass_angle` | double |  |
| `pass_height` | string |  |
| `pass_recipient_id` | bigint |  |
| `pass_recipient_name` | string |  |
| `pass_outcome` | string |  |
| `pass_body_part` | string |  |
| `pass_is_cross` | boolean |  |
| `pass_is_switch` | boolean |  |
| `pass_is_through_ball` | boolean |  |
| `pass_is_goal_assist` | boolean |  |
| `pass_end_x` | double |  |
| `pass_end_y` | double |  |
| `shot_xg` | double |  |
| `shot_outcome` | string |  |
| `shot_technique` | string |  |
| `shot_body_part` | string |  |
| `shot_type` | string |  |
| `shot_first_time` | boolean |  |
| `shot_end_x` | double |  |
| `shot_end_y` | double |  |
| `carry_end_x` | double |  |
| `carry_end_y` | double |  |
| `dribble_outcome` | string |  |
| `duel_type` | string |  |
| `duel_outcome` | string |  |
| `goalkeeper_action` | string |  |
| `foul_card` | string |  |
| `substitution_replacement` | string |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |
| `source_file` | string |  |

#### `bronze_sb_lineups`  (2,620 rows)

**Primary key:** `match_id`, `team_id`, `player_id`

| column | type | PK |
|---|---|---|
| `match_id` | bigint | Y |
| `team_id` | bigint | Y |
| `player_id` | bigint | Y |
| `team_name` | string |  |
| `player_name` | string |  |
| `player_nickname` | string |  |
| `jersey_number` | int |  |
| `country_name` | string |  |
| `card_count` | int |  |
| `positions` | array<struct<position_id:int,position:string,from:string,to:string,from_period:int,to_period:int,start_reason:string,end_reason:string>> |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |
| `source_file` | string |  |


### Silver layer
#### `silver_matches`  (14,001 rows)

**Primary key:** `match_id`

| column | type | PK |
|---|---|---|
| `match_id` | bigint | Y |
| `competition_code` | string |  |
| `competition_id` | bigint |  |
| `competition_name` | string |  |
| `season_id` | bigint |  |
| `season_start_date` | date |  |
| `season_end_date` | date |  |
| `kickoff_utc` | timestamp |  |
| `match_date` | date |  |
| `match_status` | string |  |
| `matchday` | int |  |
| `stage` | string |  |
| `group_name` | string |  |
| `home_team_id` | bigint |  |
| `home_team_name` | string |  |
| `home_team_tla` | string |  |
| `away_team_id` | bigint |  |
| `away_team_name` | string |  |
| `away_team_tla` | string |  |
| `home_goals` | int |  |
| `away_goals` | int |  |
| `home_goals_ht` | int |  |
| `away_goals_ht` | int |  |
| `winner` | string |  |
| `is_finished` | boolean |  |
| `result` | string |  |
| `total_goals` | int |  |
| `source_last_updated` | timestamp |  |
| `source_file` | string |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |

#### `silver_standings`  (212 rows)

**Primary key:** `competition_code`, `snapshot_date`, `team_id`, `stage`, `group_name`

| column | type | PK |
|---|---|---|
| `competition_code` | string | Y |
| `snapshot_date` | date | Y |
| `team_id` | bigint | Y |
| `stage` | string | Y |
| `group_name` | string | Y |
| `competition_id` | bigint |  |
| `season_id` | bigint |  |
| `standing_type` | string |  |
| `position` | int |  |
| `team_name` | string |  |
| `team_tla` | string |  |
| `played_games` | int |  |
| `won` | int |  |
| `draw` | int |  |
| `lost` | int |  |
| `points` | int |  |
| `goals_for` | int |  |
| `goals_against` | int |  |
| `goal_difference` | int |  |
| `form` | string |  |
| `source_file` | string |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |

#### `silver_scorers`  (600 rows)

**Primary key:** `competition_code`, `snapshot_date`, `player_id`

| column | type | PK |
|---|---|---|
| `competition_code` | string | Y |
| `snapshot_date` | date | Y |
| `player_id` | bigint | Y |
| `season_id` | bigint |  |
| `player_name` | string |  |
| `player_date_of_birth` | date |  |
| `player_nationality` | string |  |
| `player_position` | string |  |
| `team_id` | bigint |  |
| `team_name` | string |  |
| `played_matches` | int |  |
| `goals` | int |  |
| `assists` | int |  |
| `penalties` | int |  |
| `source_file` | string |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |

#### `silver_teams`  (284 rows)

**Primary key:** `competition_code`, `team_id`

| column | type | PK |
|---|---|---|
| `competition_code` | string | Y |
| `team_id` | bigint | Y |
| `team_name` | string |  |
| `team_short_name` | string |  |
| `team_tla` | string |  |
| `founded` | int |  |
| `club_colors` | string |  |
| `venue` | string |  |
| `area_name` | string |  |
| `coach_id` | bigint |  |
| `coach_name` | string |  |
| `coach_nationality` | string |  |
| `squad_size` | int |  |
| `source_file` | string |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |

#### `silver_sb_matches`  (66 rows)

**Primary key:** `match_id`

| column | type | PK |
|---|---|---|
| `match_id` | bigint | Y |
| `match_date` | date |  |
| `kick_off` | string |  |
| `competition_id` | bigint |  |
| `competition_name` | string |  |
| `country_name` | string |  |
| `season_id` | bigint |  |
| `season_name` | string |  |
| `home_team_id` | bigint |  |
| `home_team_name` | string |  |
| `away_team_id` | bigint |  |
| `away_team_name` | string |  |
| `home_goals` | int |  |
| `away_goals` | int |  |
| `match_status` | string |  |
| `match_week` | int |  |
| `competition_stage` | string |  |
| `stadium_name` | string |  |
| `referee_name` | string |  |
| `source_file` | string |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |

#### `silver_events`  (267,255 rows)

**Primary key:** `event_id`

| column | type | PK |
|---|---|---|
| `event_id` | string | Y |
| `match_id` | bigint |  |
| `event_index` | int |  |
| `period` | int |  |
| `event_seconds` | double |  |
| `minute` | int |  |
| `second` | int |  |
| `duration` | double |  |
| `event_type` | string |  |
| `play_pattern` | string |  |
| `possession` | int |  |
| `possession_team` | string |  |
| `team_id` | bigint |  |
| `team_name` | string |  |
| `player_id` | bigint |  |
| `player_name` | string |  |
| `position` | string |  |
| `location_x` | double |  |
| `location_y` | double |  |
| `under_pressure` | boolean |  |
| `pass_length` | double |  |
| `pass_angle` | double |  |
| `pass_height` | string |  |
| `pass_outcome` | string |  |
| `pass_body_part` | string |  |
| `pass_recipient_name` | string |  |
| `pass_end_x` | double |  |
| `pass_end_y` | double |  |
| `pass_is_cross` | boolean |  |
| `pass_is_goal_assist` | boolean |  |
| `shot_xg` | double |  |
| `shot_outcome` | string |  |
| `shot_technique` | string |  |
| `shot_body_part` | string |  |
| `shot_type` | string |  |
| `shot_end_x` | double |  |
| `shot_end_y` | double |  |
| `carry_end_x` | double |  |
| `carry_end_y` | double |  |
| `dribble_outcome` | string |  |
| `duel_type` | string |  |
| `duel_outcome` | string |  |
| `goalkeeper_action` | string |  |
| `foul_card` | string |  |
| `is_shot` | boolean |  |
| `is_goal` | boolean |  |
| `is_completed_pass` | boolean |  |
| `source_file` | string |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |

#### `silver_lineups`  (2,620 rows)

**Primary key:** `match_id`, `team_id`, `player_id`

| column | type | PK |
|---|---|---|
| `match_id` | bigint | Y |
| `team_id` | bigint | Y |
| `player_id` | bigint | Y |
| `team_name` | string |  |
| `player_name` | string |  |
| `player_nickname` | string |  |
| `jersey_number` | int |  |
| `country_name` | string |  |
| `card_count` | int |  |
| `source_file` | string |  |
| `load_timestamp` | timestamp |  |
| `batch_id` | string |  |
| `load_type` | string |  |
---

## 7. Attribution and licensing

- **football-data.org** — free tier, personal API key. Key is read from the `FOOTBALL_DATA_TOKEN` environment variable and is never committed.
- **StatsBomb open data** — free for public use under the StatsBomb user agreement, which requires attribution. Data provided by StatsBomb.

## 8. Team

| | |
|---|---|
| Abdullah Azmat | 23L-2611 |
| Muhammad Usaid Chaudhry | 23L-2538 |
| Program | BS Data Science, FAST-NUCES Lahore |
