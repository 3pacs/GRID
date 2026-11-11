# Native volume diagnostics: source-only review

Base: c59f0291302ee4ea881a502302141c734abd0880. This extends the existing
fetch/retention/worker/CLI. No provider, credential, server, GEX, timer or
consumer changes were performed. Release apply remains user-only.

## Custody and diagnostics

A bounded, JSON-serializable response is fsynced privately as
receipts/unvalidated-<sha256>.json before semantic validation. This is NOT an
accepted receipt. Invalid payloads cannot append to the store. Only after the
unchanged validator passes does link/unlink grant the accepted <sha256>.json
name; only then does append run. Success retains one payload copy. Temporary
hardlink windows are under the writer lock; interruption leaves non-ready
custody, never permission to resume automatically. Reconciliation still requires
an accepted digest filename AND strict semantic/request/coverage validation.
No diagnostic filename promotion command or reset/retry flag was added.

The existing 12 MB file cap, owner-private root/receipts/file modes, bounded
inventory, 128 MB total budget and 24 MB write allowance remain. Serialization
failure (including nonfinite/nonserializable/oversized data) yields capture
failure with no durable payload. Existing encoding allocates a serialized value
before checking size; this change does not claim bounded transport memory.
Capture is the decoded return from the client, not raw HTTP bytes, headers or
secrets. Failures inside client construction, auth, HTTP or JSON decoding are
all request-stage uncertainty: no conclusion that a provider request did or did
not reach the service. The transport remains unchanged and zero-retry.

CLI/checkpoint fixed categories identify request, capture, validation, append,
coverage, or unknown; CLI also identifies preflight, worker, snapshot and
deadline_uncertain. No exception messages/types, raw values, paths, response
bodies or headers are emitted. Private captures must not be published or copied
into the review kit. A deadline remains BaseException and leaves in_progress
uncertainty held; failure does not enable retry. Existing snapshots and receipt
replay rules remain strict. Source stages do not reconstruct the old failure.

## Original failed run: preserve, do not reset

The staged runtime remains 7ff8a6640f727c4e386df3a368d41931ecc62c6a. The scheduled
2026-11-11T14:42:50.190194Z invocation exited 1; XNYS:2026-11-11:RTH bootstrap
window_end 14:42Z is failed. No accepted receipt/snapshot exists; SQLite has zero
records and quick_check passed. Whether the provider returned is unresolved.
Reconcile cannot repair this root without a matching accepted complete receipt.

Read-only evidence at 14:46:51Z supplied by the operations owner:
- checkpoint SHA256 22943daf33e1e01621930f6e31cd0fa814e0dd9702592826f5b40947e317d932,
  227 bytes.
- minutes.sqlite SHA256 f34c8b80835e9b3c634c246cbc5953320a34a2ccef0953894642153890c665d3,
  20480 bytes.
- Both grid-owned 0600/nlink1; private root and empty receipts 0700.

## Proposed recovery boundary, NOT authorized execution

1. Claude verifies pinned source and this kit offline. Full integration/required
   CI and operator review still precede any runtime release. User alone applies.
2. Preserve original private root and sanitized RESULT.json/RESULT.md. Recheck
   original fingerprints, process/locks and source pins before any operator
   action; if drift exists, stop and review, never force reset.
3. A separately approved plan may provision a FRESH isolated diagnostic root,
   reviewed calendar/pins and one bounded same-pipeline SPY acquisition. It must
   name time/session, operator, maximum one history GET and possible normal
   preflight OAuth refresh, private evidence custody and allowed disk budget.
   Review potential duplication/cost from the uncertain first request. A new
   root does not authorize retry and must not silently replace the original.
4. Any new provider acquisition needs explicit approval after that plan is
   reviewed. This kit intentionally supplies no runnable apply/fetch command.
   Retain all old failed evidence. No second scheduler, reset, automatic retry,
   timer, consumer activation or live-volume/trading claim follows from tests.
5. After an authorized diagnostic acquisition, inspect only sanitized categories
   initially; private payload review remains local. Validate actual coverage and
   provider scope before considering a separately reviewed consumer/cadence plan.

Operations owner recovery proposal (local review artifact):
/home/anik/Documents/Codex/native-volume-bootstrap-20261110/scheduled-run-20261111/RECOVERY-PROPOSAL.md.
It governs the proposed user-run staging and separately approved new acquisition;
there is no canonical-root switch or consumer/cadence authorization. Capture
JSON cannot reconstruct original HTTP encoding or duplicate keys already parsed
by the client. Neither capture nor accepted receipt is provider attestation.

## Offline verification

268 passed, 1 optional export skipped, 47 subtests passed. The separate pinned
calendar interpreter passed all 19 calendar/manifest tests. Ruff and diff check
passed. Both Python suites block socket connections. This is focused offline
qualification, not full repository CI, runtime qualification or live coverage.
Review kit includes the narrow c59f029 delta and cumulative staged 7ff8a664 to
candidate patch, explicitly source-only with no installer. Exact verification:

    bash /home/anik/Documents/Codex/native-volume-bootstrap-20261110/diagnostics-20261111/check.sh
