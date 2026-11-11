# Native SPY volume activation plan — offline, not authorized to execute

Source GRID201498c7, existing native retention/calendar code and GEX projection.
Scope: private SPY native1m OHLCV, regular-session cumulative volume, same-time
RVOL over exact10 preceding exchange sessions, labelled HLC3 bar VWAP estimate.
Do not enable rollover gate, replace quote/chain feeds or unlock old volume signals.

## One writer, bounded requests, no work in HTTP request handlers
Choose one GRID-owned invocation, separate from gex-watch startup. Use existing
SchwabMarketClient(max_retries=0,timeout=12,token_lock_timeout=3,reauth_on_401=False),
shared credential lock; do not copy credentials.
Explicit date ranges; no ambiguous default period10, which excludes current day.
Initial bootstrap fetch from oldest expected prior session open through current
clock (at most31calendar days, bounded parser100k bars/12MB receipt). Retain actual
arrival timestamps; never call these historical point-in-time observations.
After bootstrap, fetch only a small overlap of recent minutes (proposed3minutes)
at a proposed60-second cadence, at least5seconds after closed boundary. Never
refetch the entire10day history every minute. One dedicated writer/lease; existing
12-second socket timeout,3-second token-lock wait and45-second whole CLI wall
deadline. At most one history GET, no401 replay or transient retry. Before that
GET, normal expiry refresh may make one OAuth POST and atomically update the
shared token under its original lock. This is not a one-network-call contract.
The wall deadline encompasses client construction, lock/refresh, GET, retention
and snapshot; expiry preserves uncertain custody and requires reconciliation.
Expose errors without fallback to Yahoo.
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

## Implemented activation candidate update

One-shot `python -m scripts.native_volume_once --fetch --root <private0700-root>
--calendar-manifest <reviewed-json> --calendar-sha256 <pin> [--initialize]`
now composes bounded acquisition, immutable retention and private snapshot.
Use the existing GRID service environment without printing/copying credentials.
No scheduler is installed. Invocation lease covers fetch through snapshot publish.
A >3minute interruption or failed/uncertain checkpoint stops for explicit receipt
reconciliation; there is no blind recovery/reset flag. Disk budget also stops
before further acquisition. These failures must surface in operations, not be
silently treated as clean data. Initial bootstrap must prove all10 prior sessions
and current settled minutes before incremental mode.

Actual runtime inventory: grid-api and gex-watch both User=grid. Therefore the
private0600 native-volume.json can be consumed directly by GEX from a trusted0700
root, using the SAME derived GRID contract; no admin-token provisioning or auth
weakening is necessary. Private API remains available as a separate read view.
GEX candidate uses `GEX_NATIVE_VOLUME_SNAPSHOT` defaultempty and a separate private
panel; no snapshot read when disabled, public frame omits the field entirely.
No legacy signals are unlocked. GEX needs one separately approved restart to
load consumer code and operator-configured path; quote/chain/rollover unchanged.

Actual GRID interpreter has pandas3.1.1 and lacks exchange_calendars/tzdata.
Do NOT downgrade/install into its live environment. Runtime now uses stdlib-only
ManifestCalendar with reviewed canonical session manifest, exported offline from
the pinned/tested adapter. Coverage2026-10-01 through2027-12-31,315sessions,
SHA256 efa8897e2fd94afd4f9f9b29951f142b97aece83812aca7403026df1ec4eb8ad.
This is a calendar snapshot; future emergency closures need reviewed revision,
not a guessed runtime fallback. Dates outside coverage fail closed.

Verification:157 GRID tests+25subtests pass, one optional calendar-export test
skipped in general environment; pinned calendar environment separately passed10
manifest tests. GEX38suites pass. Synthetic GRIDsnapshot→GEXreader roundtrip and
real retained Schwab historical-cutoff replay both pass. Historical replay is
not current-live certification or evidence of predictive efficacy.

Before enabling: freeze source hashes, verify existing producer ownership, stage
candidate code separately from the running release, provision private store/root,
run one explicit bootstrap with normal GRID environment, inspect actual coverage
and private snapshot, then authorize cadence and private GEX consumer activation.
No timer/service/environment/file installation on production has been performed.

## Bounded transport and explicit recovery correction
The opt-in worker now refuses401 without auth replay; existing unrelated clients
retain their original defaults. Token contention is bounded at3seconds and the
CLI has a45-second POSIX main-thread wall deadline. No provider exception can
swallow that deadline. A timeout during an OAuth refresh can leave its remote
outcome uncertain even though token-file replacement remains atomic; inspect
existing token state through the established owner before retry, never re-login
or reset a checkpoint automatically.

`python -m scripts.native_volume_reconcile --root ROOT --receipt-sha256 SHA
--calendar-manifest MANIFEST --calendar-sha256 DIGEST` is a local-only operator
reconciliation. It accepts only an existing immutable captured receipt matching
the exact checkpoint request, preserves the old checkpoint, replays idempotently,
and verifies every expected prior/current minute before completing the checkpoint.
`--republish-only` regenerates a snapshot from a completed checkpoint without any
provider call. An old receipt produces stale/degraded output, not fresh data.
No matching receipt or incomplete coverage remains blocked. This is not an
automatic reset or authorization for another fetch. More-than3minute catch-up
still requires an explicit bounded recovery acquisition plan.

Before unattended activation, wire the existing operations alert route to nonzero
exit, checkpoint not complete, missing/stale snapshot (>180seconds), and disk
budget refusal. Notification configuration and a recovery drill are required;
they have not been installed by this offline change. Do not call this unattended
ready merely because the normal cadence succeeds.

## 2026-11-11 offline health and recovery qualification

A disabled-by-default read-only health CLI and two synthetic recovery drills now
cover the existing private checkpoint/snapshot contracts. The health report uses
fixed reason codes, bounded private reads, exact calendar identity, 180-second
freshness checks, and a maximum 45-second pending publication interval. Shared
envelope validation prevents malformed provenance from appearing healthy.

See `docs/NATIVE-VOLUME-HEALTH-20261111.md` for command semantics, final focused
verification, and limitations. No alert route or cadence was installed, and no
provider acquisition or production mutation was performed. The separately
scheduled one-shot bootstrap remains unchanged. Unattended readiness still needs
verified notification delivery and current live coverage acceptance.
