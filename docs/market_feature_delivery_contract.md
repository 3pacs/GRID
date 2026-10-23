# Existing Watchlist price evidence delivery

The existing authenticated `GET /api/v1/watchlist/{ticker}/analysis` response gains
`market_features` (`watchlist-existing-evidence-v1`). Its daily scalar evidence is
copied from the existing price_history rows, or the existing live_price scalar
when no history was returned. No SQL, provider, polling, backfill, model call or
new endpoint is added. No quote is transformed into a qualified intrabar result.
The existing client request/cache remains in use.

`daily.rows` retain each original value and date (renamed `as_of`) in returned
order. Zero stays zero; null stays null. Existing Python/JSON scalar precision is
unchanged; these fields are not newly promised exact Decimal/Fraction results.
The UI renders the existing scalar directly, without Number/parseFloat/toFixed
conversion or feature math. Exact string values, if supplied, remain strings.
`daily.as_of` is the last returned row's date, not a generated timestamp. Missing
source/date remains null and is visibly Unknown. `returned_row_count` describes
only returned response rows, not whole-market/session coverage.

`daily.status=observed` means rows were returned, never that they are current or
eligible. An existing section status of unavailable/failed/stale/unknown stays
negative with `price_section_<status>` reason. Missing rows produce unknown /
no_returned_price_rows. No freshness threshold/policy is invented: freshness is
unknown / freshness_policy_not_supplied, except an explicitly supplied stale
section stays declared stale. Source names remain the endpoint's existing grid,
yfinance, live or null labels; the card labels them existing scalar evidence,
not authenticated feeds. Source/adaptor qualification and evidence hashes are
not inferred from these names.

Normal and cached returns pass through the same decoration. Cached delivery
retains the original dates/values and shows Cached response; receipt of a cache
hit never refreshes as-of or freshness. The envelope is generated without
mutating the cache object and is not added to the pre-existing caching decision.
Partial preloader payloads with missing section availability remain explicit.

`intraday` always states unavailable / qualified_intraday_result_absent with
null opening_range, trade_vwap and hlc3_volume_proxy. This useful data card is not
a permanently fake feature producer: an actual normalized bar/session/grid,
source-receipt, quality/temporal policy binding is a separate future slice.
The accepted pure TEST:SPY/TEST:QQQ contracts are not relabeled live SPY/QQQ.

No latest briefing is queried. When the original supplied analysis already has a
briefing receipt, only its status/reason/as_of is copied. Otherwise it stays null;
the current endpoint normally has none. Whenever receipt is null/missing, the
card says No briefing receipt supplied by this response, even when the separately
fetched overview supplies a reason. That reason appears only under Overview
reason, never as a Briefing receipt reason. A supplied briefing receipt keeps its
own status/reason/as-of. Synthetic source/transport reason controls do not claim
an observed production incident. No latest-briefing label or invented reason.

The card is mounted immediately above the existing AI overview. It uses the
existing page fetch: no request is added. Existing ticker-switch/unmount active
checks remain. Initial and period analysis requests now share a generation,
ticker and requested-period guard for price fields and asynchronous error/finally
updates. Cleanup invalidates both roles. A same-ticker initial success can still
merge nonperiod sections while keeping the current price history/live scalar/
feature envelope; when period data has not arrived, it supplies no old prices.
The current period response provides a complete fallback if initial data is
absent and completes loading. Its full declared availability map is copied first,
then any existing nonprice declarations are preserved field by field, then the
latest price declaration overrides price. Missing/partial previous maps cannot
silently drop unavailable/failed section statuses or other fresh declarations. Ticker-only overview/options requests keep their
existing lifecycle guards. The card also rejects a missing/unknown-schema/
mismatched-ticker envelope. Period refresh replaces market_features alongside
price_history; obsolete initial prices cannot overwrite the selected period.

## Verification and remaining checks

Focused backend tests compile the actual added helper and actual cached-return
prefix under an external-effect/import fence, without importing backend/db or
executing SQL. Node tests execute actual utilities and callback source against
mocked promises. These are source tests, not live, DOM or backend integration.
The supplied Vitest component tests require the project's existing frontend
dependencies; if absent locally, they remain UNRUN without an install. Publisher
must run normal frontend build/Vitest and exact-head CI after source review.

## Minimum manual end-user acceptance, after reviewed integration is deployed

1. Open an existing ticker's Watchlist analysis. Verify the Price evidence card
   shows a returned daily scalar (including zero), source, original as-of date,
   row count and unknown freshness reason. Expand Daily values and compare to
   the unchanged price-history response. Historical dates must remain visible.
2. Confirm Opening range / trade VWAP / HLC3-volume proxy explicitly says
   unavailable / qualified_intraday_result_absent, without invented values,
   currentness, eligible or trading badges.
3. Inspect a cache hit: old as-of is unchanged and Cached response is visible.
   With unavailable/empty response, visible status/reason is negative or unknown,
   never silently replaced by a made-up zero/current date.
4. If a briefing receipt is supplied, verify its original status/reason/as-of.
   Otherwise No briefing receipt supplied remains visible, including when an
   Overview reason is present under its separate label. Do not create a
   briefing/model request just to satisfy this acceptance check.
5. Switch SPY to QQQ while the first response is delayed; the card must not show
   SPY values/reasons under QQQ. Change period with responses arriving out of
   order; only the matching latest request updates the card.

No manual check, UI action, deployment or runtime qualification was executed by
the source author. Old qualified feature, materialization, scoring and trading
holds remain unchanged.


## Optional bounded stored-source context

The same authenticated analysis route additionally accepts `stored_only=true`
and `stored_request`, a JSON query string limited to 16 KiB of UTF-8. This mode
returns before the normal analysis cache, saved-watchlist read, legacy price
work, yfinance/live-price fallback and all other analysis sections. It never
invokes resolver persistence or a provider. Ordinary requests without
`stored_only=true` keep their previous behavior and response, including their
existing cold-navigation provider/overview side effects. This source change
adds no client request, view mode, route or automatic selection/fetch.

The closed request has only `selections`, `as_of`, `start_date` and
`vintage_policy`. `as_of` must be an explicit aware ISO datetime; `start_date`
a date; observation range is inclusive and at most 252 days; vintage policy is
FIRST_RELEASE or LATEST_AS_OF. Duplicate JSON object keys, extra fields, invalid
types, bounds or repeated/unknown source selectors fail with HTTP 400 before
store/engine access. Shared `store.pit.validate_source_snapshot_request` owns
selection and policy validation; API parsing does not implement another resolver
or freshness engine. `scope=ticker` must match the normalized route ticker.
Global/currency/contract/index/CUSIP context is explicitly labeled and does not
claim a ticker association.

At most seven unique source keys are supported: fx_rates,
ag_commodity_futures, finra_short_volume, sec_ftd, nyfed_gscpi, baltic_exchange
and quiverquant. Explicit selectors carry source_key, scope, ticker_or_cusip,
series_id, expected_source_name, feature_name and feature_id, with optional
freshness_policy `{max_age_days: integer 0..252}`. Actual IDs, registry mappings
and SEC CUSIP are caller inputs; no IDs, aliases or ticker-to-CUSIP linkage are
guessed. Quiver is a stored event source, not a raw/resolved scalar, and remains
resolver_unsupported under this scalar read contract. Missing optional bindings
are unavailable/selection_missing or mapping_missing without a DB read when no
bound scalar selector exists. An absent stored_request returns
unavailable/stored_request_missing with null cutoff and no invented clock.

The existing PIT store's bounded read supplies `stored-source-snapshot-v1`,
original stored value, observation/release/vintage dates, selected actual source,
conflict metadata, source selection and explicit freshness. Missing policy stays
unknown/freshness_policy_absent. A source mismatch is not repaired by choosing
an older expected-source row or promoting raw data. Storage release/vintage
fields derived from pull time are not provider publication/availability proof.
PIT selection/retraction rules and explicit range belong to the shared store;
this display does not recalculate them. Zero stays zero and null is unavailable.

The result is nested at `market_features.stored_source_evidence`, alongside
rather than inside daily price data. The original daily and intraday schema
remains intact; stored-only mode has unknown price evidence and explicitly
unavailable OR/VWAP/proxy. The new context echoes the normalized requested
ticker/cutoff/range/vintage policy and selectors as `request_binding`. A returned
snapshot with a different schema/cutoff/range/policy becomes read_failed with
no value substitution. Store exceptions likewise produce read_failed; no
cache/provider fallback or exception details are exposed.

The existing Card renders the separate stored-source section only for matching
envelope, ticker, cutoff/range/policy and selector bindings. Invalid response
context is visibly unavailable. Source-mismatched or unavailable rows never
render their supplied scalar, even if a direct constructed DTO populates one.
It shows original source/series, scope/selection, observation/storage-release/
vintage dates, requested cutoff, status/reason, declared freshness, conflict and
lineage limitations. It never labels the data qualified intraday/current/eligible.
The section lives in the existing whole market_features envelope so actual
request-generation/ticker/period callback guards also mask pending old context
and prevent stale cutoff/value reappearance. The underlying view/client stays
unchanged. No current production screen is claimed to send the new optional mode.

The frontend source baseline includes the separately accepted 15-test postimage
550fc736 (unmerged f387/2bb dependency), rather than claiming that regression was
already on main. All 15 tests remain unchanged; new source-context cases append.
One old backend test's cache-prefix AST locator is shifted past the new early
branch; all its cache/as-of/nonmutation assertions remain. The ordinary default
cached response is separately checked for exact equality to its prior decorator.

Offline acceptance must bind actual selected synthetic raw rows through the
existing finite EntityMap/Resolver and PIT snapshot, execute the actual analysis
endpoint with inert storage, serialize the result, and render that exact JSON
through the real Card and production callbacks in both response orders. This
proves source/consumer coupling under fixtures, not real SQL selection execution,
DB commit/durability, HTTP/auth transport, provider truth, production route
activation, browser behavior or live freshness. No live DB/query/cold navigation,
service, materializer job, provider or publication is authorized by these tests.


### Consumer stored-date proof (C1 correction)

A primary stored scalar additionally requires present, strictly typed real
calendar dates for observation, storage release and vintage. Observation must
fall inclusively between the declared start date and the cutoff's UTC calendar
day. Storage release and vintage must not be after that UTC day. Missing,
invalid/nonexistent or contradictory row dates mask the value with
unavailable/invalid_date_context while retaining the original string date
diagnostics and unknown freshness. Invalid typed date diagnostics are displayed
safely without treating them as dates. Request dates/cutoff must themselves be
valid; equal offsets representing the same instant use the same UTC day.

These are consistency checks on supplied stored date columns, not an additional
resolver or source-publication rule. The columns retain day precision: inclusive
cutoff-day equality is permitted even for a midnight cutoff. No ordering among
observation/release/vintage is invented beyond their separately stated bounds,
and no provider publication instant or freshness is inferred. Valid zero and
null, source/context masking, actual callback ordering and all previous price
semantics remain unchanged. The C1 finding and original four false-positive
renderings remain immutable; it is a consumer defect, not evidence of a live
incident or a PIT source defect.
