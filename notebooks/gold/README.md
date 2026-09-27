# Gold layer (Phase 2)

Dimensional modeling notebooks land here in Phase 2. Planned star schema, per the Phase 1 proposal:

- `fact_matches` — one row per match; FKs to team, competition, venue, date; measures: goals_home, goals_away, result, matchday.
- `fact_top_scorers` — one row per player per competition per season; goals, assists, team. Sourced from the `scorers` endpoint, confirmed free-tier accessible (`../../docs/api-verification.md`). Added specifically to support the dashboard's "which players are driving scoring trends" question, which the original Phase 1 draft promised without a supporting table.
  - Note: this is a season-total leaderboard, not per-match detail (the free API doesn't expose goal-by-goal events). A real trend-over-time can be derived later by snapshotting `scorers` on each incremental run and diffing consecutive snapshots per player — only worth building if it's actually asked for.
- `dim_team`, `dim_competition`, `dim_date`, `dim_venue`, `dim_player` — conformed dimensions.
- `agg_team_season_performance` — wins/draws/losses, goals for/against, points by team per season.
- `agg_home_away_split` — home vs. away performance metrics per team.
