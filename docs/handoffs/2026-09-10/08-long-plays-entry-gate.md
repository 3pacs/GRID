# 08 — Long Plays board: make the entry gate reachable for trial gems

Branch: `claude/handoff-08-long-plays-gate`. Lane: code + ops-exec rebuild.

## Evidence

`intelligence/long_plays.py` marks a candidate `entry_candidate` only if
p50 3-year multiple > 1.5 (or catalyst override), max drawdown > −75 %, and a
coverage gate: latest 90-day sweep verdict in {high, moderate} when the ticker
has sweep coverage, else options payoff multiple ≥ 20. On 2026-09-10 the board
(row 4) had 223 in the universe, 25 candidates, all "watch", **0 entry
candidates**: the weekly sweep "exists but produced no rankable verdicts"
(`SWEEP_NOTE_EMPTY`) and small caps rarely have options depth, so the gate is
unreachable for exactly the names the operator wants (sub-$2B trial gems such as
OLMA BUY $906 M, Phase 3 readout 2026-10-31, runway 3.2 months; TECX, OCGN,
MLTX on WATCHLIST).

## Design

1. Sweep coverage: find why `universe_ranking_history` has no rankable verdicts
   (`intelligence/scheduler.py` weekly 90 d sweep; `api/routers/conviction.py`
   sweeps endpoints). Fix the producer if it is a bug (likely the sweep universe
   never includes the enriched small caps, or the verdict column is NULL when
   the conviction stack has < N signals). Do not fake verdicts.
2. Trial path: add a third coverage route in `long_plays.py`:
   `has_trial_signal` (a `trial_signals` row for the ticker in the last 30 days
   with `signal in (BUY, WATCHLIST)`), with `cash_runway_score >= 0.4` and a
   catalyst date inside 18 months → coverage gate passes. Keep the return and
   drawdown gates. Expose the route used (`sweep` / `options` / `trial`) in the
   board row so the digest can say why a name qualified.
3. Board: `entry_candidate` rows must carry cap, runway months, catalyst date and
   the p50/p90 multiples (they already exist for the enriched names). Digest
   (`scripts/daily_digest.py`, Long Plays section) lists entry candidates first.
4. Tests in `tests/test_long_plays*.py`: gate matrix (sweep / options / trial /
   none), stance labels, board serialization.
5. After merge: rebuild the board on grid-svr (`intelligence/long_plays.py`
   entry point the weekly job uses) and report the entry candidates with their
   gate route. Persona surfaces are frozen — change data, not the UI copy.

## Done when

The board has ≥ 1 entry candidate with an explained route or you can show the
data honestly disqualifies every name; the sweep produces verdicts again; tests pass.
