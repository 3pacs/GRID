# Native SPY volume activation plan — offline, not authorized to execute

Source GRID201498c7, existing native retention/calendar code and GEX projection.
Scope: private SPY native1m OHLCV, regular-session cumulative volume, same-time
RVOL over exact10 preceding exchange sessions, labelled HLC3 bar VWAP estimate.
Do not enable rollover gate, replace quote/chain feeds or unlock old volume signals.

## One writer, bounded requests, no work in HTTP request handlers
Choose one GRID-owned invocation, separate from gex-watch startup. Use existing
SchwabMarketClient(max_retries=0), shared credential lock; do not copy credentials.
Explicit date ranges; no ambiguous default period10, which excludes current day.
Initial bootstrap fetch from oldest expected prior session open through current
clock (at most31calendar days, bounded parser100k bars/12MB receipt). Retain actual
arrival timestamps; never call these historical point-in-time observations.
After bootstrap, fetch only a small overlap of recent minutes (proposed3minutes)
at a proposed60-second cadence, at least5seconds after closed boundary. Never
refetch the entire10day history every minute. One dedicated writer/lease; existing
request timeout and no retries; expose errors without fallback to Yahoo.
Revisions append immutably; overlapping requests are not duplicate trades and
must be selected as-of by latest receipt, never summed. Record receipt hashes,
row counts, last successful event/receipt and refusal reason. Set disk budget and
retention policy before enabling; no automatic deletion in initial activation.

## API and GEX binding
Initialize owner-private dedicated research store and receipt folder under trusted
ancestors, not production trading/scoring tables. Mount existing build_router
with sqlite_loader and pinned XNYSCalendar. Default GRID admin auth remains.
Expose one private latest descriptive view (as_of omitted => server UTC), plus
explicit historical as_of. Every response private/no-store, source/calendar,
window end, receipt age and quality. Native endpoint is read-only; no fetch/refresh.
GEX consumes that exact API contract using explicitly provisioned private read
access. Do not embed GRID admin token in browser or public feed. Do not silently
weaken auth to obtain dashboard access. Need agree scoped service access before
runtime wiring; current prototype intentionally has none. Cache fail-closed in
GEX; no calculations or baseline substitutions in dashboard. Add separate private
Native volume panel, not reuse legacy unsafe VWAP/RVOL signals.

## Acceptance before enabling
- Offline integration existing92tests+11subtests,8calendar and consumer4 pass.
- Run private staging from actual retained receipts; prove10expected full sessions,
  exact current completed bars, no malformed/duplicate/gapped/early receipt rows.
- Show stale provider/event/receipt, request failure, missing baseline, early close,
  DST, revised bars, unavailable calendar, token lock and disk-bound failure cases.
- Prove API401/403 without authorized access and no values in public projections.
- Shadow live output against provider charts and quantify residual coverage gap.
  Provider bar volume is not quote total; exact trade VWAP remains unavailable.
- One bounded activation with backup, no GEX feed changes. Rollback removes only
  newly enabled consumer/writer, leaving current GEX/volume counters intact.

## Separate approval required for runtime scope
No new timer/daemon, production store, API mount, credential configuration or GEX
restart is implied by isolated code approval or the five-file gamma release.
These runtime items need a single explicit activation proposal after scoped read
access and existing scheduling ownership are resolved. Nothing in this document
has been executed on production.
