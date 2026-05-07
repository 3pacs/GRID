-- 0051_options_rec_signals.sql
-- Persist the scanner signal payload behind every options recommendation so
-- post-mortems can attribute failures to specific feature contributions
-- instead of guessing from a reconstructed snapshot.
--
-- Audit item #34 (Missing Feature Importance Tracking) noted there was no
-- per-prediction trail from features → outcome on the trade path. The
-- oracle_predictions table already had signals JSONB; options_recommendations
-- did not. This closes the gap.

ALTER TABLE options_recommendations
    ADD COLUMN IF NOT EXISTS signals          JSONB,
    ADD COLUMN IF NOT EXISTS opposing_signals JSONB;

COMMENT ON COLUMN options_recommendations.signals IS
    'Scanner signals dict (MispricingOpportunity.signals) at decision time. '
    'Used by intelligence/postmortem.py to attribute failures to specific '
    'feature contributions.';

COMMENT ON COLUMN options_recommendations.opposing_signals IS
    'Signals that contradicted the trade direction at decision time, kept '
    'separate so the postmortem LLM can identify what was ignored.';
