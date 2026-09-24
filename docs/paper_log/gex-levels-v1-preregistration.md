# Pre-registration: SPY GEX structural levels, forward paper log v1

Status: registered 2026-09-24 (the git commit that adds this file is the registration record).
Amended once, 2026-09-24, before the first logged session (see Amendment 1 at the end).
Owner: Anik. Author: Claude (Opus 5.5), from the 2026-09-24 review of Gemini's dealer-gamma plan.
Nothing below may change after the first logged session. Any change is a new version (v2) with a
new start date; the v1 log is kept as-is.

## Why

Gemini's plan treats GRID's dealer-gamma regime and levels (gamma flip, put wall, call wall) as a
map of where SPY will stall, bounce or accelerate. The review found the levels roughly right, the
regime label inverted by a sign bug, and the spot input faked. Both are fixed on
`fix/dealer-gamma-sign-spot-20260924`. None of GRID's gamma outputs has ever been scored against
what the market did next. This log does that, forward only, with the rules fixed in advance.

This is research. GRID cannot trade SPY (its Robinhood connector is crypto-only), no orders are
placed, and the database is read-only for this job.

## Schedule

- Pre-open run: 08:45 America/New_York, Monday to Friday.
- Post-close run: 16:30 America/New_York, Monday to Friday.
- A session counts only if its pre-open record was written before 09:30 America/New_York that day.
- First session: the first NYSE regular session after the job is installed on grid-svr.
- Evaluation: once, after 60 valid sessions. Until then the job reports only activity and data
  quality (sessions logged, sessions excluded and why, touches, trades), never returns or hit
  rates. Any earlier look at outcomes is labeled interim and cannot be used to stop, extend or
  change the test.

## Inputs, recorded at the pre-open run

- Chain: GRID `options_snapshots` for SPY, the latest `snap_date` whose rows were created before
  the run. Record `snap_date` and `created_at`.
- Levels, from `physics.dealer_gamma.DealerGammaEngine` at the pinned code commit (recorded in
  every record): gamma flip, the engine's spot and its source, aggregate GEX, normalized GEX, and
  regime (LONG_GAMMA / SHORT_GAMMA / NEUTRAL, the engine's own thresholds). Sign convention:
  dealers modeled long calls and short puts; GEX > 0 means dealers long gamma.
- Walls (Amendment 1). The engine's own put wall and call wall are recorded as
  `engine_put_wall` / `engine_call_wall` but are not tested. The tested walls come from the
  engine's per-strike output:
  - Put wall: among strikes at or below 0.995 * P0, the strike with the largest put gamma exposure
    by magnitude (|put_gex|).
  - Call wall: among strikes at or above 1.005 * P0, the strike with the largest call gamma exposure
    (call_gex).
  - Ties go to the strike closer to P0. If no strike qualifies on a side, that wall is missing for
    the session (recorded, not an exclusion); H2 and H3 use the levels that exist.
- Reference price P0: SPY's previous regular-session close (yfinance daily bar, unadjusted),
  with fetch time. Cross-check against the engine's spot; if they differ by more than 0.25%, the
  session is excluded (reason `ref_mismatch`).
- VIX: previous close of ^VIX (yfinance), with fetch time.
- Placebo levels: each real level L is mirrored around P0 (L' = 2*P0 - L). A placebo within 0.10%
  of any real level is dropped.

## Outcomes, recorded at the post-close run

SPY regular-session 5-minute bars (09:30-16:00 America/New_York; early closes use the bars that
exist) and the session open, high, low and close, from yfinance, with fetch time.

## Definitions

- Side: a level above P0 is an "above" level; a level below P0 is a "below" level.
- Reached: an above level is reached at the first bar with high >= L; a below level at the first
  bar with low <= L. If the session opens at or beyond the level (open >= L for an above level,
  open <= L for a below level), it is a gap-through, logged separately and not counted in H2.
- Held: after being reached, the session closes on the same side of L as the open. Otherwise broke.
- Range: ln(session high / session low).
- Costs: 1 basis point adverse slippage per side, no commission. Notional $1,000 per trade.

## Hypotheses and tests (evaluated once, at 60 valid sessions)

Three tests, Bonferroni-corrected: a hypothesis passes only with one-sided p < 0.0167.

**H1 (primary). Regime predicts move size.** On SHORT_GAMMA mornings the session range is larger
than on LONG_GAMMA mornings, after controlling for how nervous the market already was.
OLS on valid SHORT and LONG sessions: ln(range) = a + b * SHORT + c * ln(VIX previous close) + e,
Newey-West standard errors with 5 lags. Pass if b > 0 with one-sided p < 0.0167. NEUTRAL sessions
are reported but not used in the test. If either group has fewer than 10 sessions, H1 is
inconclusive.

**H2. Levels hold more than chance.** Across all first reaches of the flip, put wall and call wall,
the held rate is higher than for their mirror placebos reached in the same sessions.
Permutation test on the difference (real minus placebo held rate), shuffling real/placebo labels
within each session, 10,000 draws with fixed seed 20260924. Pass if one-sided p < 0.0167. Fewer
than 20 real-level reaches means inconclusive.

**H3. A simple level rule makes money after costs.**
- LONG_GAMMA morning (fade): first reach of the call wall, pretend short at the wall; first reach of
  the put wall, pretend long at the wall. Exit at the session close.
- SHORT_GAMMA morning (follow the break): the first 5-minute bar that closes below the put wall,
  pretend short at that bar's close; the first bar that closes above the call wall, pretend long at
  that bar's close. Exit at the session close.
- NEUTRAL morning: no trade. At most one trade per session, the first trigger in time.
- The same rule runs on the mirror placebo walls as a control and is reported alongside.
- Pass if the mean return per real-level trade is above 0 with one-sided p < 0.0167 (t-test), with
  at least 20 trades. Fewer trades means inconclusive.

## Exclusions (logged with a reason code, never silently)

- `no_chain`: no chain for the run, or the latest chain was created after the pre-open run.
- `stale_chain`: the latest chain is more than one trading day old.
- `engine_unavailable`: the engine returns its unavailable result (no measured spot) or no regime.
  A missing flip or wall is recorded, not excluded.
- `ref_mismatch`: see P0 above.
- `late_preopen`: the pre-open record was written at or after 09:30 America/New_York.
- `data_unavailable` (Amendment 1): the market-data source for P0 or VIX was unreachable or
  returned nothing at the pre-open run, after one retry.
- `no_preopen` (Amendment 1): the post-close run found no pre-open record for the session.
- `bars_missing`: more than 10% of the expected 5-minute bars are missing.
- `market_closed`: no regular session that day.

If more than 10% of sessions are excluded (not counting `market_closed`) by the 30th session, v1
stops and the problem is fixed in a v2 with a new start date.

## Integrity

- Append-only JSONL on grid-svr under `/data/grid/paper_log/gex_levels_v1/`. Every record carries
  the SHA-256 of the previous record, the pinned code commit, and the job run time. The first record
  carries the SHA-256 of this file.
- The job opens database sessions read-only and places no orders.
- The log is mirrored into the vault for review; the grid-svr copy is the record.

## What a result means

- H1 pass: GRID's regime label carries real information about move size and belongs in the
  briefing as a volatility call, not a direction call.
- H2 or H3 pass: the levels carry information about where price stalls, and the rule is worth a
  second, independent forward test before any real money.
- No pass: the levels and regime stay out of trading decisions.

## Amendment 1 (2026-09-24, before the first logged session)

Made before any session was logged. The only record written so far was a smoke test in a temporary
directory on grid-svr, which was deleted; it is not part of the log. The first real record carries
the SHA-256 of this amended file.

1. Tested walls are defined here instead of taken from the engine. In the 2026-09-24 smoke check
   the engine's put wall and call wall were both 765.0 with spot 764.9: short-dated at-the-money
   gamma dominates both, so "reached" and "held" at a level sitting on the open price would be a
   coin flip, not a test of structure. The tested walls must be at least 0.5% from P0, on the
   correct side. The engine's own walls are still recorded, untested.
2. `engine_unavailable` now means no measured spot or no regime; a missing flip or wall is
   recorded, not excluded.
3. Two exclusion codes added so no session can go missing silently: `data_unavailable` and
   `no_preopen`.
4. Wording only: "side" is set by P0, the permutation seed is fixed at 20260924.
