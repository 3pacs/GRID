-- Migration: 0053_signal_subtype.sql
-- Author: opus (task #137 — signal_data direction pollution fix)
-- Applies via: sudo -u postgres psql griddb -f migrations/0053_signal_subtype.sql
--
-- Root cause: multiple emitters were stuffing categorical signal-type strings
-- (price_surge, off_exchange, gov_contracts, unusual_options, heat_spike,
-- new_pool, lobbying, insider_sell, ...) into signal_data.direction.
-- The direction column is supposed to be a small directional vocabulary
-- (BULL / BEAR / NEUTRAL / NULL).
--
-- Fix:
--   1) Add signal_subtype column for categorical features.
--   2) Backfill: move polluted values out of direction into signal_subtype.
--   3) Canonicalize remaining direction values to BULL/BEAR/NEUTRAL/NULL.
--
-- Emitter code is patched separately so new writes are clean.

BEGIN;

-- ====== SCHEMA CHANGES ======

ALTER TABLE signal_data
    ADD COLUMN IF NOT EXISTS signal_subtype TEXT;

CREATE INDEX IF NOT EXISTS idx_signal_data_subtype
    ON signal_data(signal_subtype)
    WHERE signal_subtype IS NOT NULL;

-- ====== BACKFILL ======
-- Categorical / signal-type leaks → signal_subtype, direction → NULL.
WITH polluted AS (
    SELECT unnest(ARRAY[
        'price_surge', 'off_exchange', 'gov_contracts', 'unusual_options',
        'heat_spike', 'new_pool', 'lobbying', 'insider_sell',
        'house_trading', 'senate_trading', 'legislation_new', 'net_position_delta',
        'unusual_sell', 'unusual_buy', 'unusual_volume',
        'contract_award', 'political_beta',
        'trade_idea_long', 'trade_idea_watch', 'trade_idea_hedge',
        'cluster_buy', 'sale (full)', 'sale (partial)',
        'flights', 'new_rule', 'exchange', 'twitter',
        'risk', 'influence', 'donation', 'spike_volume',
        'wsb_bullish', 'wsb_bearish', 'wsb_neutral',
        'unknown'
    ]) AS leak
)
UPDATE signal_data SET
    signal_subtype = direction,
    direction = CASE
        -- Preserve embedded directional hint for some leaks
        WHEN direction IN ('unusual_buy', 'cluster_buy', 'wsb_bullish', 'trade_idea_long') THEN 'BULL'
        WHEN direction IN ('unusual_sell', 'wsb_bearish', 'insider_sell') THEN 'BEAR'
        WHEN direction IN ('wsb_neutral', 'trade_idea_watch') THEN 'NEUTRAL'
        ELSE NULL
    END
WHERE direction IN (SELECT leak FROM polluted);

-- Canonicalize the remaining 'real' directional labels to BULL/BEAR/NEUTRAL.
UPDATE signal_data SET direction = 'BULL'
    WHERE direction IN ('bullish', 'buy', 'long', 'call', 'up', 'increase', 'increases', 'rising');
UPDATE signal_data SET direction = 'BEAR'
    WHERE direction IN ('bearish', 'sell', 'short', 'put', 'down', 'decrease', 'decreases', 'falling');
UPDATE signal_data SET direction = 'NEUTRAL'
    WHERE direction IN ('neutral', 'flat', 'sideways');

COMMIT;

-- ====== GRANT FOOTER ======
-- signal_data already exists; no new GRANT needed for the new column.
-- (Column grants are inherited from the table grant.)
