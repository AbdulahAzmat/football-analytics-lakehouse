# Silver layer (Phase 2)

Cleaning/conformance notebooks land here in Phase 2 per the course schedule (due 2026-10-10). Planned tables, per the Phase 1 proposal:

- `silver_matches` — one row per match, flattened, typed, deduplicated on `(match_id, last_updated)`.
- `silver_teams` — team & squad reference data, SCD-lite with an `as_of` date.
- `silver_standings` — one row per team per matchday snapshot.
- `silver_scorers` — flattened player scoring records (added after verifying the `scorers` endpoint works on the free tier — see `../../docs/api-verification.md`).
