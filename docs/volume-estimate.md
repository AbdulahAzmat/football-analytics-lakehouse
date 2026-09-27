# Volume & Frequency Estimate (Section 2.2)

Based on real measured pulls (see `docs/api-verification.md`), not assumption.

## Measured baseline (Premier League, real data)

| Item | Measured size | Basis |
|---|---|---|
| One season of matches (380 matches) | ~383 KB | `full_load_sample_pl_2025-26.json` |
| Per match | ~1,032 bytes | 392,073 bytes / 380 matches |
| Team + squad metadata (20 teams) | ~88 KB | `full_load_sample_pl_teams.json` |
| Standings snapshot | ~5.5 KB | `full_load_sample_pl_standings.json` |
| One matchday, 5 competitions (20 matches) | ~20.2 KB | `incremental_load_sample_2026-09-20.json` |

## Full load estimate (all 6 tracked competitions, 4 seasons: 2023/24–2025/26 + current)

Extrapolated from the Premier League baseline, assuming similar per-match payload size across leagues (PD/BL1/SA are structurally similar to PL; FL1 has slightly fewer matches per season at 18 teams; CL has a different format and is harder to estimate precisely without pulling it — flagged as a caveat below):

- Matches: 6 competitions × ~350 avg matches/season × 4 seasons × ~1,032 bytes ≈ **~8.7 MB**
- Team/squad metadata: 6 competitions × ~88 KB (one current snapshot each) ≈ **~530 KB**
- Standings snapshots: 6 competitions × ~5.5 KB ≈ **~33 KB**
- Scorers: 6 competitions × ~5 KB ≈ **~30 KB**

**Estimated total full load: ~9.3 MB.**

This is comfortably inside Databricks Community Edition's free-tier DBFS storage limits, with wide headroom for growth (adding more seasons or competitions would need to 50–100x before it became a real constraint).

## Incremental load estimate

Measured: one active matchday across 5 competitions ≈ 20.2 KB for 20 matches.

- Match days occur roughly 3–4 times per week per competition during an active season, with a full "matchday" (10 PL matches, etc.) landing on the same 1–2 days per week per league — so a daily incremental pull frequently returns 0 matches (rest days) and occasionally returns the ~20 KB batch above.
- **Estimated daily incremental average (across an active season): ~15–30 KB on match days, near 0 on rest days.**
- **Estimated weekly incremental total: ~100–150 KB** across all 6 tracked competitions.
- **Estimated monthly incremental total: ~400–600 KB.**

## Rate-limit math for the full backfill

Free tier: 10 requests/minute.

- Full load: 6 competitions × 4 seasons (matches) + 6 competitions (teams) + 6 competitions (standings) + 6 competitions (scorers) = **~48 requests**.
- At 10 req/min with safe pacing (~6–7 sec between calls), the full historical backfill completes in **under 6 minutes**. Rate limiting is not a practical bottleneck for this project's scope.

## Caveat

Only Premier League was pulled at full depth to produce these measurements (see `docs/api-verification.md`). The other five competitions were confirmed accessible but not fully measured — actual sizes for Champions League in particular may differ meaningfully from this estimate given its multi-stage format (league phase + knockout rounds) versus a flat round-robin league season.
