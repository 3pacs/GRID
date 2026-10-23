# Selected stored-source evidence

This is a finite source interface, not activation of a materializer, resolver job,
provider request, database migration or screen. Missing live bindings do not stop
offline implementation: fixtures declare synthetic IDs and supply independent
registry/catalog rows. Those IDs are never defaults for a live caller.

The production types stay in the existing modules:

1. `EntityMap.for_selected_bindings(selections, registry_rows)` verifies exact
   caller-expected feature names and IDs against separately supplied registry rows.
   It neither runs the normal mapper constructor nor changes static mappings.
2. `Resolver.resolve_selected_rows(selections, raw_rows, registry_rows, source_rows,
   *, as_of, start_date, vintage_policy="LATEST_AS_OF")` constructs resolved-schema
   rows from successful selected raw observations. It does no I/O or persistence;
   its receipt says `persisted: false`. A separately scoped caller must provide
   input custody and any future transaction. The existing bulk resolver uses the
   same extracted priority/conflict calculation and retains its defaults.
3. `PITStore.get_source_snapshot(selections, *, as_of, start_date,
   vintage_policy="LATEST_AS_OF")` reads existing resolved rows only. It never
   invokes resolution, registration, ingestion or a provider fallback.

`validate_source_snapshot_request` in `store.pit` validates before engine creation.
It returns normalized `selections`, UTC-aware `as_of`, `start_date` and
`vintage_policy`. Invalid types, duplicate/unknown keys, extra fields, bad identity
bindings, naive clocks and excessive ranges raise `ValueError` before any read.
Missing selections are completed to the fixed seven source keys. An absent series
or source is `selection_missing`; an absent feature name/ID pair is
`mapping_missing`. `PITStore(None)` handles these cases without a connection.

## Explicit selections and limits

A selector contains `source_key`, `scope`, `ticker_or_cusip`, `series_id`,
`expected_source_name`, `feature_name`, `feature_id`, and optional
`freshness_policy`. Feature name and ID must be supplied together. An expected
binding is not proof of registration or raw-row lineage. The snapshot independently
checks the existing registry name/ID and the chosen resolved row's source.

| Source key | Catalog name | Series and scope |
| --- | --- | --- |
| `fx_rates` | `yfinance` | `FX:{CCY}:close`, currency; CCY→USD except DXY index |
| `ag_commodity_futures` | `YFINANCE_COMMODITY_FUTURES` | `YF:{contract}:close`, contract |
| `finra_short_volume` | `FINRA_SHORT_VOLUME` | `finra:short_volume:{SYMBOL}`, ticker |
| `sec_ftd` | `SEC_FTD` | `sec:ftd_balance:{CUSIP}`, CUSIP; no inferred ticker join |
| `nyfed_gscpi` | `NYFED_GSCPI` | `NYFED:gscpi`, global monthly observation |
| `baltic_exchange` | `Baltic_Exchange` | `baltic.bdi` or supported subindex, index |
| `quiverquant` | `quiverquant` | Stored JSON events; `resolver_unsupported` here |

At most seven unique selectors and seven independent registry/catalog rows are
accepted. The raw batch is a list capped at 4096 rows; overflow is refused, never
silently truncated. Raw rows outside the selected namespaces are refused. The
observation interval contains at most 252 inclusive UTC dates. A supplied raw pull
timestamp must be aware and no later than `as_of`. Raw resolution checks a finite
five-second computation deadline and uses no threads. Missing/failed/invalid raw
values never become zero. Contradictory same-day values refuse that selector;
there is no invented intraday revision order for a day-precision storage key.

Each bound snapshot selection requests a read-only transaction and a statement
timeout within one shared five-second deadline. The original deadline is never reset for another
source or phase. Expiry is checked again after connection acquisition, before
each costly query, after each query and after transaction exit. Each costly
query's statement timeout is refreshed from the remaining budget, rather than
inheriting an earlier phase's timeout. Expired acquisition issues no setup SQL;
expired setup/configuration issues no later data query. A sub-millisecond remainder
is refused rather than configuring zero, which would disable PostgreSQL's timeout.
These source checks cannot cancel pool/OS acquisition, enforce a scheduler hard
wall-clock bound or qualify real driver/PostgreSQL cancellation. Configuration,
query and transaction operations themselves still need native qualification.
Registry lookup returns at most
two rows to detect ambiguity. The resolved query filters observation range,
release date, vintage date and retractions before vintage selection, then returns
at most one latest observation. The vintage policy is `FIRST_RELEASE` or
`LATEST_AS_OF`. Conflict detail is capped at 2048 characters with a truncation
indicator. There is no all-history read, registry scan or arbitrary SQL input.
Actual planner/timeout/cancellation behavior still requires PostgreSQL qualification.

## Result and clock meanings

The outer schema is `stored-source-snapshot-v1`, with `as_of`, `start_date`,
`vintage_policy`, and seven `selections`. Each row preserves declared identity,
feature binding, original observation/release/vintage dates, actual source
`{id, name}`, conflict metadata and an optional numeric value. Available rows use
`status: available`, `reason: resolved_observation`. Unavailable rows keep a null
value and an explicit reason such as missing mapping, absent row, invalid scalar,
source mismatch, unsupported event storage or read failure. Zero is a valid value.
Choosing a wrong-source latest row produces `source_mismatch`; the reader does
not substitute an older preferred-source value or a raw observation.

Freshness is unknown unless the caller declares exactly
`freshness_policy: {max_age_days: <integer 0..252>}`. That policy compares the
observation's age with the UTC cutoff date. It is a declared display-age rule,
not a provider cadence, publication or availability claim. Missing observations
cannot become fresh. The Card must preserve the original dates and show its
unavailable/unknown reason rather than substitute a fetch or render time.

Storage release/vintage dates are derived from pull dates. They do **not** prove
provider publication instants or exact intraday point-in-time availability. The
snapshot applies retraction timestamps at the exact supplied aware cutoff, but
the other storage clocks remain date precision. Every row declares
`storage_clock: pull_date_not_provider_publication` and
`lineage: resolved_source_not_raw_row`: source priority metadata does not identify
the exact raw occurrence. Caller-supplied series/feature bindings remain explicit
expectations rather than independently certified semantic lineage.

## Offline evidence and remaining integration

The two new test files execute the actual EntityMap, Resolver and PITStore source
bodies with inert external dependency names and a finite fake relational boundary.
`actual_pipeline()` in `tests/test_selected_source_resolution.py` supplies six
synthetic raw scalar observations, independent registry/catalog fixtures, actual
Resolver-produced resolved rows and an actual PIT snapshot. Quiver remains an
explicit seventh unsupported row. The fixture includes zero and negative values.
The original display owner can pass this real pipeline output through the actual
stored-only API serialization and React Card; a pre-seeded snapshot alone is not
evidence of that chain. No native DB/provider/browser calls are made by these tests.

The source fixtures cannot prove current data presence, transaction durability,
provider rights, live registry bindings, resolver-job invocation, API process/PWA
release binding or an actual rendered screen. Generic bulk normalization remains
unchanged apart from factoring its existing priority/conflict arithmetic into a
shared helper. The selected mapper does not auto-register missing mappings.
Source and test acceptance must precede any separately authorized runtime wiring.
SEC-XBRL/Card activation and intraday OR/VWAP/quality/temporal qualification are
outside this seven-source scalar interface.
