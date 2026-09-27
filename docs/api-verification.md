# API Verification Log — football-data.org Free Tier

Verified live on **2026-09-27** using a real registered free-tier API key. This exists because published third-party comparisons of football-data.org's free tier disagreed with each other and with the vendor's own pricing page, so every claim below was tested directly rather than assumed.

## What we tested

| Endpoint | Query | Result |
|---|---|---|
| `GET /v4/competitions` | — | 200. All 190 competitions listed (unauthenticated access also works for this endpoint). |
| `GET /v4/competitions/PL/matches` | `season=2022` | **403** — `"The resource you are looking for is restricted and apparently not within your permissions."` |
| `GET /v4/competitions/PL/matches` | `season=2023` | 200 — 380 matches returned in full. |
| `GET /v4/competitions/PL/matches` | `season=2024` | 200 — 380 matches returned in full. |
| `GET /v4/competitions/PL/matches` | `season=2025` | 200 — 380 matches returned in full. |
| `GET /v4/competitions/PL/scorers` | `limit=5` | 200 — full player-level scorer data returned (name, team, goals). |
| `GET /v4/competitions/PL/standings` | — | 200 — full current standings table. |
| `GET /v4/competitions/{CL,PD,BL1,SA,FL1,WC}` | — | 200 for all six — all accessible on free tier. |
| `GET /v4/matches` | date-scoped, multi-competition | 200 — works as the daily incremental source. |
| Rate limit | rapid sequential calls | **429** hit after ~10 calls within a minute — confirms the documented 10 req/min cap is real and enforced. |

## Findings that changed the proposal

1. **Historical depth is 3 completed seasons + the current one, not 4+.** Free tier blocks `season=2022` (403) but allows 2023, 2024, 2025 (all completed), and the in-progress current season. The original proposal claimed "2022/23 through 2025/26" — that boundary is one season too far back. **Fix applied:** full load rescoped to 2023/24–2025/26 plus the current in-progress season.

2. **Player-level scorer data IS available free**, contrary to football-data.org's own pricing page (which lists "player/scorer data" under paid-tier exclusions) and contrary to at least one third-party comparison blog. The `scorers` endpoint returns real name/team/goals data on the free key with no error. **Fix applied:** this resolves the original Gold-layer gap where the dashboard promised a "which players are driving scoring trends" insight but no Gold table carried player-level data — a `fact_top_scorers` table sourced from this endpoint is now included in the data model (see main proposal, Section 4.3).

3. **Rate limiting is not a real constraint for this project's volume.** A full backfill across 6 competitions × 4 seasons (matches + teams + standings) is roughly 30–40 calls total. At 10 req/min that's under 5 minutes of paced calls — not the bottleneck the original proposal implied.

## Caveats

- Only Premier League was pulled at full depth for cost/time reasons during proposal verification; the other five competitions were confirmed *accessible* (200 on their base endpoint) but not fully pulled. Volume estimates for the other five leagues in `docs/volume-estimate.md` are extrapolated from Premier League's per-season size, not independently measured — cup competitions like the Champions League have a different match count/structure and may vary from this estimate.
- "Current season" behaves as expected from the API's own season-numbering: querying `season=2025` returns the just-completed 2025/26 season; the true in-progress season is queried without a `season` parameter (defaults to it) or explicitly as the current year.
