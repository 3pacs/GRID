# Cboe clock provenance, writer-only slice

Retain provider raw clock strings on each newly captured Cboe contract as `options_snapshots_all.clock_metadata` JSONB. This is not a quote-freshness certification or a source artifact archive. Version 1 retains document timestamp (timezone unknown), underlying last trade (existing adapter America/New_York assumption), contract last trade (timezone unknown), and explicit null bid/ask update time plus unknown freshness/synchronization. Raw invalid or absent clock types remain null.

`provider_regular_market_at` retains its existing meaning: underlying trade time, NOT contract bid/ask time. Capture clocks retain collection meaning. No historical backfill; older rows and non-Cboe rows have NULL metadata. Unknown is not fresh and not stale. Neither repeated quotes nor old last-trade times establish bid/ask staleness.

The nullable additive migration follows options_spot_clock_20261110. Apply schema before writer code; normal release integration must reconcile migration heads. Downgrade refuses destructive provenance erasure. Unit tests use mock DB; real PostgreSQL migration, grants, triggers and performance are NOT verified here.

The latest-snapshot view and research readers are intentionally unchanged. Consumers that need this metadata must read the append-only table with capture_batch_id, or obtain a separately reviewed view/reader extension. Existing records and derived analytics are not retroactively repaired. This slice prevents future raw clock loss; it does not certify synchronized spot/chain, filter existing analytics or enable execution claims. A weekly thesis need not wait for intraday precision, but any executable option-economics claim needs source quote timing and its declared tolerance.

No provider calls, production changes, subscriptions, releases, empirical runs or holdout reads authorized by this kit. Claude verifies; Anik applies.
