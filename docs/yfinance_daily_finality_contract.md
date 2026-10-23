# Yahoo raw captures and daily qualification, V2

This is an **AUTHOR_UNEXECUTED** source-only compatibility revision against `c4662193a1b526e4ccc149b0394bfc52754e3eec`. V1 and its independent review remain frozen. V1's offline append/PIT mechanism was supported within its declared policy, but its original regression gate had 25 passes and three failures and its coverage restrictions removed ingestion behavior. No V2 source, test, harness or mutant has been executed or imported. AST, patch-replay and hash checks are metadata checks, not runtime validation.

## Raw acceptance and qualification

A SUCCESS row is an accepted raw provider observation; it is not a finality certificate. Generic valid captures continue to ingest for all 70 defaults, valid arbitrary caller symbols, non-session labels, post-2026 dates and non-daily requests. The pre-existing provider request configuration, literal `auto_adjust=False`, instrument, duplicate-column, ratio, SQL-parameter and locking guards remain. This is a prospective source change, not authorization to run a provider request or repair history.

Each new generic payload declares `raw_capture_contract=yf_raw_capture_v2`, `price_basis`, `interval`, `obs_date`, `capture_state`, `capture_reason`, and the exact boolean `provider_certified_final=false`.

| State | Meaning | Selected daily reader |
| --- | --- | --- |
| INCOMPLETE | A supported daily label has not reached its declared boundary | Excluded |
| UNKNOWN | The symbol, calendar/year, non-session label or interval has no supported qualification policy | Excluded |
| POLICY_ELIGIBLE | The exact daily capture-policy marker and actual capture timestamp qualify | Eligible after marker validation; still not provider-certified final |

Explicit UNKNOWN or INCOMPLETE state rejects selection even if a contradictory payload includes otherwise valid marker fields. Genuine existing V1/SPY markers without a capture-state field remain compatible. Legacy unmarked data stays raw UNKNOWN; it is never retroactively certified from SUCCESS, date, value or a late database timestamp alone.

The 52 explicit US-listed default instruments keep V1's conservative post-America/New_York-label-day policy, standard holiday/weekend checks and **2026 qualification bound**. Their observation dates after 2026 continue to ingest as raw UNKNOWN. BTC-USD and ETH-USD retain their separate UTC 24-hour daily convention. The 16 other defaults remain UNKNOWN for qualification: ^VIX, DX-Y.NYB, ^VIX9D, ^VIX3M, ^VIX6M; HG=F, GC=F, SI=F, CL=F; EURUSD=X, GBPUSD=X, USDJPY=X, AUDUSD=X, USDCHF=X, USDCAD=X, NZDUSD=X. Arbitrary tickers such as AAPL also ingest as raw UNKNOWN until a separately supported policy exists. No New York calendar is inferred from ticker spelling. All six fields follow these rules.

The standard calendar module does not prove exceptional-closure coverage or implement half-day close times. Waiting until local midnight avoids assuming regular versus early close times and handles DST through ZoneInfo, but remains an elapsed-label-day capture policy. Broader qualified-calendar coverage is a separate owner policy choice. No such extension is made here.

SPY **daily close only** retains the exact original `spy_close_v1` payload, post-UTC-day-end threshold, four-day capture lookback and scoring contract. It does not acquire generic provisional/raw fallback. Its incomplete/expired refusal remains PARTIAL/no_data. Other SPY fields use the generic rules; non-daily SPY close writes use the isolated non-daily identifier.

## Interval identity and result mapping

Canonical daily IDs stay `YF:{ticker}:{field}`. New non-daily captures use `YF:{ticker}:{field}:{interval}`. For example, `YF:^GSPC:close:1wk` and `YF:^GSPC:close:1mo` cannot overwrite, suppress or qualify the canonical daily close. A returned weekly/monthly value is accepted as raw UNKNOWN. The supplied interval still reaches the existing provider request; V2 does not replace or alter that request or introduce a new pre-provider interval refusal.

`pull_ticker` returns `interval` and `series_id_map` for available selected fields, e.g. `{'close': 'YF:^GSPC:close:1wk'}`. Callers requesting non-daily data must use this explicit new-ID mapping to read new captures. This is the authorized compatibility contract; there is no silent alias or migration of old shared-ID interval rows. Existing raw reader APIs, historical IDs/rows and interfaces are unchanged. Legacy unmarked shared-ID intervals remain ambiguous raw facts, excluded from qualified daily reads.

`rows_inserted` and `raw_rows_inserted` both count all accepted inserted rows, including eligible captures. `qualified_rows_inserted` counts their policy-qualified subset. `capture_states_seen` and `qualification_withheld` expose candidate classification even on idempotent raw reruns. UNKNOWN/INCOMPLETE alone does not turn a valid raw ingestion into no_data or an error. Existing download/validation failures still report their errors. Numeric guard refusals remain explicit PARTIAL/no_data when nothing is inserted. None of these result fields claims source-wide freshness.

## Append-only vintages and PIT

The generic path uses the existing stable transaction advisory lock for each source/series/date. It checks for an existing valid eligible capture before raw deduplication. Once eligible, the same capture remains idempotent even if a later provider response changes its value; this is not a general final-price revision ingester.

Otherwise, the latest SUCCESS vintage for the exact source/series/date and explicit interval determines raw identity. Both payload/state and value must match to skip. The lookup does not filter to an earlier state or search all history for a matching value. Therefore UNKNOWN A→B→A appends three observations, followed by an identical rerun that inserts nothing. A raw same-value capture cannot suppress a later policy-qualified promotion. Unmarked old data cannot suppress the first explicitly typed capture. No prior row is updated, deleted or retimestamped.

`read_daily_policy_n` retains its strict daily-marker, series/date/boundary/state validation and SQL PIT timestamp cutoff before vintage collapse/count. UNKNOWN, INCOMPLETE, non-daily and unmarked history remain available through unchanged raw readers but cannot become completed-close evidence. A cutoff before an appended capture retains its earlier raw value and cannot see the later qualified value.

## Diary and compatibility boundaries

V2 leaves the V1 diary selection, current timestamp cutoff, exact target-date check, lookback/age gates, scoring withholding and default-off operator flag unchanged. It introduces no raw/UNKNOWN substitution, historical promotion or new display feature. Futures/VIX and old unmarked history can therefore remain unavailable as qualified diary closes even while raw ingestion continues. Two suitable qualified observation dates may be needed for a pair. No diary was run.

## Tests and original fixture amendment

The new test copy changes only V1 expectations that raw ingestion must be refused: current-session values now persist as INCOMPLETE, unsupported/default/arbitrary/session/year values as UNKNOWN, and non-daily captures under explicit interval-suffixed IDs. Each adapted case also asserts that the same observations do not qualify as daily closes. Exact SPY behavior, calendar-boundary qualification negatives, raw preservation, append/idempotency, PIT, instrument/adjustment/bounds and diary withholding assertions remain. New cases cover all default symbols and six fields, future years, arbitrary tickers, A→B→A, same-value promotion, interval isolation, contradictory capture states and exact reruns. The test-change receipt enumerates these adaptations.

The only original test amendment is fixture setup in `test_outcome_is_duplicate_only_when_all_dates_already_exist`: a fixed clock and mocked existing eligible SPY-open payload/timestamp now represent an actual qualified duplicate. Its zero-insert, SUCCESS and duplicate_only assertions remain unchanged; every other original test AST is unchanged. The original V1 capsule and its three failures remain immutable. The dynamic non-session ratio and AAA/BBB budget tests are not modified; restoring raw UNKNOWN ingestion is intended to restore their behavior, but this has not been executed.

A proposed future author child consolidates baseline reproduction, candidate controls, original regressions and eight assertion-based source mutants, with fresh module namespaces and no nested process. A separate independent reviewer child would bring the explicitly proposed global ceiling from the spent 100 to 102. Each requires 45 seconds plus two seconds to stop/reap, 512 MiB address space and 1 MiB stdout. No allocation is presumed. The gated unrun harness avoids pytest startup/plugin/logging/capture configuration and keeps future outputs, temporary files and mutant copies in an explicit owned run root. Native Python 3.11, provider labels, PostgreSQL semantics/concurrency and full CI remain unqualified.

No provider, network, SQL database, backfill, historical repair, diary rerun, install, credential, service, timer, security, publication, CI, deploy or adoption action is authorized or performed by this revision.
