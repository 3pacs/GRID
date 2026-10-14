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
