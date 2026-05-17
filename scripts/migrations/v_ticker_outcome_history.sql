-- v_ticker_outcome_history.sql
--
-- Per-ticker rollup of gem_outcomes calls. Surfaces
--   "we called BULL on AAPL 12 times this quarter — 8 HIT, 2 MISS, 2 WRONG_DIRECTION"
-- as a single SELECT. Powers per-ticker dashboards + the trade-postmortem
-- ticker-history sidebar.
--
-- Background
-- ----------
-- ``scripts/evaluate_gem_outcomes.py`` (codex 2026-05-17) writes one
-- ``gem_outcomes`` row per ``(gem_id, ticker, evaluation_window)``.
-- ``hit_or_miss`` is one of ``HIT / MISS / WRONG_DIRECTION / PARTIAL / INCONCLUSIVE``.
-- The view aggregates by ``(ticker, predicted_direction, evaluation_window)``
-- and exposes hit_rate computed only over decisive verdicts so it stays
-- meaningful for thin-data tickers.
--
-- Cheap (regular VIEW, no materialization) — re-runs on every query but
-- the underlying ``gem_outcomes`` table is small (~455 rows as of
-- 2026-05-17) and indexed on ``(gem_id, ticker, evaluation_window)``.
--
-- Companion to codex's 2026-05-17 gem-postmortem-loop report follow-up.

CREATE OR REPLACE VIEW v_ticker_outcome_history AS
SELECT
    ticker,
    predicted_direction,
    evaluation_window,
    COUNT(*)                                                AS n_calls,
    COUNT(*) FILTER (WHERE hit_or_miss = 'HIT')             AS n_hit,
    COUNT(*) FILTER (WHERE hit_or_miss = 'MISS')            AS n_miss,
    COUNT(*) FILTER (WHERE hit_or_miss = 'WRONG_DIRECTION') AS n_wrong_direction,
    COUNT(*) FILTER (WHERE hit_or_miss = 'PARTIAL')         AS n_partial,
    COUNT(*) FILTER (WHERE hit_or_miss = 'INCONCLUSIVE')    AS n_inconclusive,
    -- Hit rate computed only over decisive verdicts (HIT/MISS/WRONG_DIRECTION
    -- + PARTIAL counted as half-hit). INCONCLUSIVE rows excluded from the
    -- denominator to keep the rate meaningful for thin-data tickers.
    ROUND(
      (COUNT(*) FILTER (WHERE hit_or_miss = 'HIT')
       + 0.5 * COUNT(*) FILTER (WHERE hit_or_miss = 'PARTIAL'))::numeric
      / NULLIF(COUNT(*) FILTER (WHERE hit_or_miss IN ('HIT', 'MISS', 'WRONG_DIRECTION', 'PARTIAL')), 0)::numeric,
      4
    )                                                       AS hit_rate,
    ROUND(AVG(pct_move) FILTER (WHERE pct_move IS NOT NULL)::numeric, 3) AS mean_pct_move,
    MIN(detection_date)                                     AS first_call,
    MAX(detection_date)                                     AS last_call
FROM gem_outcomes
GROUP BY ticker, predicted_direction, evaluation_window;

GRANT SELECT ON v_ticker_outcome_history TO PUBLIC;
