# Cboe clock provenance, writer-only slice v2

Shared provider/document clock semantics live once per capture in `options_capture_batches.clock_metadata`. Each `options_snapshots_all.clock_metadata` contains only `contract_last_trade_raw`; join through capture_batch_id. Both columns are nullable JSONB. Batch schema_version=2 retains document timestamp (timezone unknown), underlying last trade (existing adapter America/New_York assumption), contract clock timezone unknown, null bid/ask update time and unknown freshness/synchronization. Invalid or missing raw clock types remain null.

`provider_regular_market_at` retains underlying-trade semantics, NOT contract quote time. Capture clocks retain collection meaning. No historical backfill; old rows and non-Cboe rows have NULL metadata. Unknown is not fresh or stale. Neither repeated quotes nor old last-trade times establish bid/ask staleness.

Migration follows current local origin/main tip finra_dix_feature_20261113. Single-head test is mandatory and must be rechecked when rebasing. schema.sql mirrors both columns. Schema MUST precede writer code: do not use skip_migrations. Downgrade refuses destructive provenance erasure.

## Storage choice
Previous v1 repeated ~421 serialized bytes per contract. KO fixture v2 is 371 serialized UTF-8 bytes per batch + 50 per timestamped contract. A disposable PostgreSQL 15 test measured 55 bytes pg_column_size for the row JSONB. At one million timestamped contracts this is about 55 MB of row JSONB payload, not total disk cost. Batch metadata, alignment, tuple/WAL/index/backup overhead are additional; NULL raw clocks are smaller. Production row counts and disk budget were not queried. This avoids repeating shared labels/clocks while retaining semantics.

## Verified locally
152 options and Alembic-head tests pass. Disposable PG15 container, network none/no ports/no host mounts, existing image only: exact new migration over pre-column options-store schema, existing-row NULL, existing table grants, JSONB cast INSERT, append-only UPDATE guard and fresh-install options-store block all pass. The initial harness caught PG's temporary initialization server; corrected readiness to TCP final server and reran successfully. This is NOT full historical migration replay nor production privilege/performance verification.

Latest-snapshot view and research readers are intentionally unchanged. Consumers must join append-only rows to their exact batch for these fields or receive a separately reviewed view extension. This slice stops clock loss; it does not certify synchronized spot/chain, filter existing analytics or enable execution claims. Weekly thesis work need not wait for intraday precision; executable option economics still need the declared source-timing tolerance.

No provider calls, live actions, subscriptions, empirical runs or holdout reads. Claude verifies; Anik applies.
