# V5 narrow receipt-consistency correction

This derivative changes only validate_selected_child and validate_selected_result. Producer, default route, transport, SQL, Caps/Call, orchestration, retry behavior and closed schemas/signatures remain V4-byte/AST-identical. V4 source/review/spent artifacts remain immutable.

Child consistency is now bidirectional. Positive write attempts/ACK/count require DB-start and write-bearing evidence; ACK (even zero rows) requires commit-attempt evidence and a typed known count. Positive committed rows require ACK. UNKNOWN commit keeps count null, while an unknown SQL affected count can validly have write attempts and no commit attempt. No-write flags require no write attempts/ACK; no DB-start requires unknown before/after readback. These checks reject contradictions; supplied JSON facts alone do not authenticate a real durable commit. Existing native/live/durability qualifications remain false/UNKNOWN.

Result aggregation is derived from the final available attempt independent of outer workflow state. A missing/untrusted attempt has a fully unknown effect/closure/accounting record and cannot provide aggregate ACK, rows or known readback. Three safe pre-DB retries preserve NOT_ATTEMPTED/0/UNKNOWN. A known closed attempt whose commit outcome is UNKNOWN can preserve observed after-readback and DB-bearing1. Likewise, a later outer deadline/callback failure may set overall state UNKNOWN while retaining already observed ACK/count/readback. No blanket UNKNOWN-state erasure occurs. Zero committed rows never imply that no DB/commit happened.

FIRST own byte-identical V4 copy reproduced the exact three reviewer methods: six assertion false accepts, zero errors/effects. Final focused check is29methods PASS:3literal reviewer methods +6paired methods +17unchanged selected methods +3selected retained financial/transport methods. All54 V4 test method bodies remain AST-identical; the full closed37-method suite was not replayed. Two actual-source guard omissions produce4assertion failures/0errors. An attempted nonessential mutation-selection edit stopped at its ambiguous-target assertion before writing; that note is preserved, no credit/no extra rerun. All test children are bounded45+2total47s/512MiBAS/1MiB streamed output and reaped; the aggregate eight-child allowance and ROOT global100 ceiling remain in force, not100 per lane.

Run the focused memory-only check from this author-work with a fresh label:

```bash
python3 -I -S -B evidence/run_bounded.py user-FR-check source/tests/test_finra_occurrence_driver.py FR-focused
```

Mode FR-mutants selects the two functional omissions. No target/native/provider/DB/SQL/Git/service effects or live admission are authorized. Python3.14.4 memory/fake evidence is distinct from target3.10/TLS/native behavior. The source-only same retained reviewer must adjudicate this exact freeze before dependency adoption.

---

# V4 selected start-ready workflow — offline source only

This version adds a new, separately closed start-ready route. The V3 narrative below is historical background; its old test commands/current-main timing are superseded by this section. Producer76579 and its default parser/math/SQL/series/default route are byte-identical. The canonical publication-base snapshot is now c466219 (see source pins for full commit/tree), distinct from unchanged historical Call.release a94/sourcebase. No release adoption is claimed.

Public source API:

- `validate_selected_admission(admission: bytes, expected_sha: str)` returns a private parsed value or refuses.
- `run_selected_workflow(admission, expected_sha, *, entry_origin: float, runtime=None)` executes the concrete bounded source path; no caller retry.
- `validate_selected_result(result, admission, expected_sha, entry_origin)` is a pure closed/type/identity/proof validator.

The selected CLI shape is `PINNED_PYTHON -I -S -B PINNED_DRIVER --selected-workflow --admission EXACT_PATH --admission-sha256 EXACT_SHA`. It is a prospective shape only, not an admitted command. CLI captures actual CLOCK_MONOTONIC before argument parsing/config read. Hermes must capture the same original entry before selected source reads/imports, then forward it; a supplied float cannot itself prove that provenance. No target NativeRuntime is invoked here.

Static admission `finra-selected-workflow-admission-v2` has exactly schema,status,workflow_id,issued_utc,expires_utc,call,caps,workflow_caps,runtime,db_plan. Status must be ADMITTED at a future separately approved invocation; no real admission is created by this packet. Issued/expiry are aware UTC with positive span<=7days; at actual entry the consent must already be issued and unexpired. The static Call retains the exact11 FINRA fields; call_id/day/cutoff_utc/t0 are null and all finite source/file args remain exact. Runtime fields and DBplan retain their strict original shapes. Caps remain120work/30stop (TERM120/KILL135/end150),8MiB body,20000rows and original SQL/read/network limits. Workflow caps are attempts3,gets3,body25165824,http_wall135,total450; boolean/numeric substitutes and extra fields refuse.

At earliest actual entry a finite immutable frame binds original origin, end=origin+450, original admissionSHA, workflowID, expiry, execution UTCday, CLOCK_MONOTONIC and the actual observed monotonic/UTC pair. Parsing/preflight/consumption time counts against this origin; it never restarts450. Actual day and cutoff=min(staticexpiry,entry-day end) are derived once, with enough reserve for the full original450 at entry. No day rollover, scheduled not_before window or five-second externally built t0 packet is required. Every attempt derives a fresh actual t0 only within that same outer frame and requires its full150s reserve. Original clock/UTC reader identity and the private binding are rechecked around callbacks; altered, backward, late or expired clocks refuse before further effects. Module/interpreter startup before the first trusted CLI sample is outside the userspace measurement.

NativeRuntime's concrete source first performs existing preflight (hash-bound interpreter/driver/producer/package, executing paths/API exports, delegated leaf and receipt FD identity/mode, named DSN presence). It then creates one exclusive nofollow `<workflow_id>.workflow.spent` marker bound to workflow/admissionSHA, fsyncs and independently closes its original context. A failed post-consumption bootstrap remains spent; no retry of that workflow. Each attempt gets its own deterministic ID from SHA256(FINRA_SELECTED_V2|workflowID|index), its own exclusive original attempt marker and the existing owned worker. No fresh live identities or marker are supplied here. At most3attempts occur; no new actor/process family/framework is introduced.

Selected worker HTTP is deliberately BEFORE the first DB session (legacy v1 keeps read-before-HTTP ordering). This is necessary for genuine pre-DB transient retry. Internally tracked effects are set before session allocation/write-context/commit calls, never from a caller safe flag. A retry requires an exact source/Call/attempt/outer/admission-bound child and parent, both actual observed stdout/stderr EOFs, zero exit, reap, no TERM/KILL and final owned closure within the original bound. Only exact built-in TimeoutError/ConnectionError or429/502/503/504, with successful transport cleanup, no sticky deadline failure, no DB-start/write-bearing/commit and no later readback/finalizer error, may retry. Unknown/custom exceptions, unavailable403/404, parser/framing/authority/schema/ownership/output/receipt/stop failures and any DB-start failure are terminal. Every commit attempt is terminal even when rows were stored then the ACK was lost. A trustworthy UNKNOWNcommit receipt returns workflow state UNKNOWN while preserving observed DB-bearing1/readback where available; it never launches another attempt. Missing or untrustworthy receipt/closure makes effect counts, actual usage and after UNKNOWN/null rather than zero. No parent DB recovery is attempted.

Reservations are conservative and irrevocable: before each launch charge1GET+8MiB+45HTTPs; missing receipts never refund them. Thus the source permits at most3GET invocations/24MiB consumed-body allowance/135s aggregate HTTP allowance under original450. `actual_usage.exchange_calls` counts observed transport entries (which may fail before a wire GET); body_bytes and http_seconds are observed bounded child values. `wire_gets` always remains UNKNOWN. The code does not claim packet-level traffic accounting or prove a connect failure sent no request. The same complete file/date/header/parser and dedicated read/write role/table/catalog/trigger/unique/partition checks remain mandatory before writes. At most1DB-write-bearing attempt exists. Acknowledged affected rows are distinct from new rows; new_rows/durability remain UNKNOWN.

Result schema `finra-selected-workflow-result-v2` is closed; exact keys and signatures are pinned in API-PIN.json. Attempt summaries retain actual outcome/phase/code, closure and effect/accounting proof. COMPLETE requires a terminal observed closed positive outcome, never merely a label. EXHAUSTED requires3safe retryable pre-DB outcomes. Missing/unknown effects cannot be relabelled COMPLETE or zero. Saved PENDING_AT_PERSIST is not final closure; final parent receipt is separate. Native/live acceptance remain false for every source result; that flag is not a qualifying interface.

Lifecycle fixes independently reproduce FINRA's original primary-masking and concrete NativeRuntime no-EOF promotion (2assertion failures/0errors) before editing. Original verified handles are independently finalized even after clock/deadline errors; secondary failure cannot mask primary. Both original pipe empty-read EOFs are required before receipt/ACK/count promotion. Safe resource closure retains original identities; foreign/rebound handles are not closed and failed/late closure does not pass. This is not native target qualification or a deterministic realtime guarantee.

From author-work the runnable memory-only check is:

```bash
python3 -I -S -B evidence/run_bounded.py user-selected-check source/tests/test_finra_occurrence_driver.py selected
```

Use a fresh label. Mode `retained` runs37 unchanged original test methods; mode `mutants` runs6selected actual-source omissions. The final17new +37retained methods pass separately;6mutants yield6assertion failures/0errors. Serial child bound45work+2TOTALstop/reap/47total,512MiBAS/1MiB streamed output; actualPython3.14.4/in-process adapters only. Earlier missing stdlib preloads triggered blocked source-library open attempts and are uncredited; an initial commit-retry mutant raised Refused at the final independent reducer and is not credited until an effect-count assertion detects the forbidden second launch with0errors. No native3.10/TLS/ABI/provider/DB/cgroup/target qualification, public upload, live permission, staging or Hermes activation occurs. Synchronous parent filesystem/loader/native scheduling behavior is not a proven hard-interruption guarantee: outer pre/post deadline checks and worker stop code are implemented; target whole-entry containment remains unqualified.

---

# One-file FINRA occurrence contract

This source implements one direct FINRAShortVolumePuller occurrence for CNMSshvol20261013.txt. It adds a keyword-only occurrence_io constructor argument and an instance-bound pull dispatch. Without that argument the original constructor, public/default route, parser, series IDs, SQL and arithmetic remain unchanged. No scheduler, BasePuller, classifier, D2, SEC or service configuration changes are included.

The dedicated CLI implements parent supervision and a fresh-process worker. It is not a live admission. No actual FINRA request, database transaction, native worker, cgroup, PIDFD or service operation has been exercised in this delivery. All native_accepted, Card_PIT_accepted and live_qualified receipt flags remain false. Runtime authority and execution window are absent.

## Immutable occurrence and finite inputs

Call binds a UUID, actual monotonic T0, UTC observation day and same-day cutoff, source ID, exact reviewed producer SHA, release a94db4efaba6b8d1cfe0a242627540aa93012f31, trade date 2026-10-13, CNMS, dry_run=false and the one HTTPS URL. A byte-pinned admission is copied into a canonical private packet before callbacks. It is declared controller input, not independently authenticated authorization. Missing bindings refuse. There is no dynamic date selection, backfill, alternate market, retry or fallback.

Caps are exact: 120 seconds work plus 30 seconds owned stop/finalization; one HTTPS request; 8 MiB body; 20,000 parsed rows; complete bounded fetchmany plus EOF proof; 20,032 SQL calls; 45 seconds aggregate HTTP wall time; connect/read limits 5/15 seconds; statement/lock limits 5/2 seconds; 1 MiB receipt. UTC day/cutoff and monotonic regressions are sticky failures. Cooperative boundary checks do not guarantee kernel scheduling or interrupt a blocked callback; the owned parent stop route bounds the worker separately.

The HTTPS adapter uses the pinned cdn.finra.org host/path, identity encoding, no redirects/proxy/retry, declared and streamed body caps. The exact existing parser runs only after a complete numeric trailer count check, all six ordered header names (Date, Symbol, ShortVolume, ShortExemptVolume, TotalVolume, Market), and exactly eight ASCII date digits equal to the bound trade date on every raw body row. Header case and surrounding field whitespace retain the original normalization; fractional volumes and comma-joined market fields retain original arithmetic/meaning. Reordered, renamed, truncated or extra header columns and trailing date text refuse before parsing or writes. Every parsed row must match the fixed date, cardinality, nonempty symbol/market and finite nonnegative volume fields. Duplicate same-file keys preserve first-row semantics. HTTP 403/404 yields SKIPPED, a valid empty file yields NO_NEW_DATA, malformed/partial/overlimit input fails before writes. Publication vintage stays UNKNOWN. The fixed file trade date is distinct from retrieval/write observation time; cutoff_utc is operational expiry, not a claimed publication or PIT input cutoff.

## Executed source and SQL effects

The worker verifies and executes an AST projection of the exact pinned producer: literal constants, original parser, error helper and class. Default imports, GitSink, config/engine factories and BasePuller catalog constructor are excluded. Only called retry decorators are excluded from the projection; the dedicated transport has no retry. The constructor receives occurrence_io and never takes the default catalog path. This is not a claim that a normal producer module import is inert.

A direct psycopg2 adapter uses named read/write DSNs from environment only, never argv or receipts. Dedicated repeatable-read connections have explicit read-only mode, connect/statement/lock/idle-transaction limits. The finite SQL allowlist comprises actual-role/database checks, existing source_catalog lookup, raw_series/source_catalog table properties, trigger/unique-plan fingerprints, exact source/date/prefix reads, the original FINRA file/date advisory-lock key, and parameterized raw_series INSERTs. No DDL, catalog INSERT, PullContext, logger write, schema migration or auxiliary-table write is in this route. Fingerprinted triggers/constraints are compared with supplied exact plans; they are not proof of benign trigger behavior or a general database security audit.

Independent before and after read-only connections stay in the owned worker. Complete partition reads are bounded; overflow, duplicate persisted keys, wrong source/date/status, invalid/nonfinite/negative value, or missing/naive/future-to-observed-read timestamp refuses rather than truncates positive evidence. Decimal/date/datetime exports preserve exact typed values. Receipts expose count/digest metadata, not row bodies. SQL counts/affected rows are distinct from newly inserted logical keys. Only a nonboolean integer affected count 0 or 1 is accepted per INSERT. A successful driver commit return is ACKNOWLEDGED, not a durable-storage proof. Lost/failed/missing child acknowledgement preserves UNKNOWN and null committed count; there is no parent DB recovery or retry. A failed after-read or finalization cannot erase an acknowledged commit.

## Ownership and native limits

The concrete parent queues packet delivery only after owned supervision, uses nonblocking pipes and a PIDFD identity, and carries the same work/stop end through TERM, KILL, reap, EOF, persistence and close. Original receipt-directory FD identity is checked before exclusive spent-marker creation, relative writes and close. Failed partial allocation closes only retained owned resources. A persisted pre-close receipt reports PENDING_AT_PERSIST; only the returned terminal result can report actual final closure/deadline observations. It does not rewrite the exclusive earlier receipt.

Native preflight requires exact executing interpreter/driver/producer/package pins, existing owner-bound private receipt root, and an already delegated empty cgroup leaf with checked controls. No cgroup is installed or discovered by this artifact. memory.max is a charged-memory cap (at most 256 MiB here), with possible temporary overshoot; it is not exact aggregate RSS. pids.max=1 counts TIDs and is not a barrier against unrelated migration. Joining before target imports does not relocate every preexisting memory charge. Child address-space 512 MiB and CPU 30 seconds are separately named limits. These kernel/runtime mechanisms are implemented but native-UNRUN; fake checks do not qualify their real-time behavior, database grants, effective settings or OS containment.

## Local offline execution

From the delivery root, run `bash RUN-OFFLINE-LOCALLY.sh`. It verifies the sealed packet, copies it to a fresh ordinary temporary directory, and runs the rejecting-effect-fenced synthetic test child with 30 seconds plus 2 seconds stop reserve, 512 MiB child address space and 1 MiB output cap. It never invokes the live CLI or native worker. The temporary packet and output are retained for inspection. The interpreter used for author checks was CPython 3.14.4; Python 3.11 grammar parsing is separate from actual 3.11 execution.

The implemented future CLI shape is `python -I -S -B scripts/finra_occurrence_driver.py --admission FILE --admission-sha256 SHA256`. This is syntax only: no admitted FILE, hash, DSNs, role/catalog plan, native dependency binding, actual T0 or runtime window is supplied. exact_live_exec is null. Native execution, persistence/durability acceptance and a real FINRA Card/PIT consumer binding require separately scoped evidence; offline success supplies none of those claims.


## Response-owned HTTP derivative V3 (source only)

The exact accepted FINRA V2 transport incorrectly requires connection.sock after getresponse. Real CPython HTTPConnection detaches that field on Connection:close while HTTPResponse still owns readable fp/raw/_sock. The focused witness uses the exact old29f734 module/method bytes and actual HTTPConnection.getresponse/HTTPResponse with in-memory RawIOBase/BufferedReader. No socket/server/SocketIO/TLS or target3.10 is exercised; actual interpreter is3.14.

The dedicated adapter now captures that actual response-owned handle, requires callable settimeout, validates the original response/socket identity and positive finite original deadline before each blocking read, sets timeout<=15s and remaining budget, rechecks after the timeout callback and read, enforces bytes/count/declared-length and EOF. It never falls back to connection.sock or omits the blocking timeout. Known-length completed bodies return EOF without another blocking read. Missing/rebound handles, late callbacks/read, malformed lengths and invalid counts refuse. Existing aggregate body/wall and parent owned-stop limits remain unchanged and additionally required; userspace clock checks alone are not hard native-interruption proof.

HTTP cleanup is independent for response, connection and budget finalization. _http_finalize preserves the original primary object without formatting arbitrary exceptions or invoking add_note; secondary cleanup cannot replace it. With no primary, cleanup failure still propagates. No producer/SQL/math/rowcounts/transaction/PIT/default route changes occur. The occurrence_io producer76579 remains byte-identical to accepted V2. One retained fake response adds fp/raw ownership metadata to match the actual adapter; every preexisting test assertion AST is preserved.

Focused final15 methods pass (9new response/worker transport methods,5selected existing framing/accounting/HTTPS methods,1exact-old witness). Three omission mutants cause three assertion failures and zero errors. Baseline and development failures are preserved uncredited; no historical full suite is rerun. `python3 -I -S -B evidence/run_bounded.py final` is the runnable memory-only command from author-work; all source calls are definitions/AST projections and injected IO. A30+2s child,512MiBAS and1MiB postcapture check bound this verification, not an admitted FINRA job.

This derivative retains historical a94 sourcebase and filedate2026-10-13. Main7e3 is only the ROOT-reported deployed-current release; no current checkout import or canonical deployment is claimed. Current-main source compatibility must be reconciled before any publication/adoption; native ABI, dedicated roles/cgroup identity/runtime window and live provider/DB/commit/durability acceptance remain unqualified. No new CID/marker/window/admission or production activation is supplied.

## V6: independently owned guardian (SOURCE ONLY; every new test UNRUN)

The V5 caller loop could not stop or reap a separate-session worker after the
caller died. V6 adds a separate-session guardian process in the same pinned
`finra_occurrence_driver.py`; it owns the worker's Popen/PIDFD, bounded packet,
stdout/stderr streams, TERM/KILL, wait/reap and independent finalization. This is
an isolated derivative, not an update to a staged/native package or Hermes.

`NativeRuntime.launch(context, packet_bytes)` consumes the same exclusive attempt
marker once, opens the original caller PIDFD and private nonblocking control
pipe, starts only the guardian and installs the guardian handle before queueing
any packet bytes. The guardian cannot create a worker until `ParentDeathArm`
checks the exact original-parent relationship/PIDFD and private pipe, original
monotonic bound, full hash-bound packet and preflight. It rechecks the arm before
worker spawn. Caller SIGKILL/death makes the original parent PIDFD readable;
private-pipe EOF also latches cancellation. A hard-stopped but living caller does
not need to execute a finally: guardian timers continue independently. Guardian
SIGINT/SIGTERM only latch cancellation; they do not interrupt owned reaping.

The guardian stays OUTSIDE the existing `pids.max=1` worker cgroup. Only its one
worker joins that exact existing leaf. No cgroup/role/permission creation or
relaxation is introduced. Guardian's separate control-plane limits are 512 MiB
RLIMIT_AS, 30 CPU seconds and no core dumps; these are NOT an aggregate RSS or
charged-memory cap for guardian+worker. Worker cgroup memory/swap/CPU/pids/depth
requirements and original worker RLIMITs remain unchanged. Any actual package
qualification must account separately for this additional guardian footprint;
no resource readiness is inferred from this source implementation.

All times remain the ORIGINAL attempt t0: TERM at t0+120, KILL at t0+135, and
reap + observed BOTH pipe EOFs + handle/context finalization before t0+150.
Cancellation may request earlier TERM/KILL but cannot extend any deadline. For a
selected workflow, attempt end must also fit the original outer450 frame.
`OwnedWorker(..., stop_request=None)` preserves the default old loop; guardian
passes its independently pinned arm checker. Caller `GuardianHandle.signal`
only writes a bounded T/K request; it never sends SIGKILL to the guardian that
must reap the worker. Lost/malformed/late guardian output or unknown closure is
terminal UNKNOWN, never a zero-write/retry permission. All commit-attempt and
aggregate evidence consistency rules from V5 remain unchanged.

Guardian returns one bounded closed `finra-independent-guardian-v1` envelope with
exact packet SHA, exact Call, actual worker stop proof and the unchanged worker
receipt. Parent unwraps only this concrete native guardian handle's envelope and
requires zero exit, reaped, both EOFs and no cancellation before existing child
validation/accounting promotion. No new raw stderr/exception/DSN export occurs.
Receipt remains at most the old 1 MiB; envelope overhead can cause refusal rather
than expanding the cap. Saved PENDING, returned finalization and actual worker
accounting retain their distinct meanings; cleanup is not durability/PIT proof.

As a secondary fail-closed guard-loss measure, worker preexec sets
PR_SET_PDEATHSIG(SIGKILL), checks that its direct parent is the same guardian
before and after arming, then joins the leaf and applies original limits. Failure
refuses before packet/provider/DB effects. If the guardian itself is SIGKILLed,
that mechanism aims to stop its worker but cannot prove reaping/EOF afterward;
parent must retain UNKNOWN. It is not a replacement for independently qualifying
PIDFD/PDEATHSIG, signal, exec, TLS/DB cancellation and actual OS behavior. Kernel
stall, unscheduled guardian, interpreter bootstrap/import blocking, hard death of
both supervisor and guard, PID namespace/privilege interactions, native callback
latency and actual charged-memory containment are UNQUALIFIED. No claim of a
mathematical hard real-time guarantee or surviving arbitrary guardian death is
made. Existing unsupported target facts are not repaired by paper flags.

Native API additions are concrete dormant code: ParentDeathArm, NativeArmIO,
GuardianHandle, supervise_guarded_worker, guardian_main, private _spawn_worker,
and closed guardian CLI arguments. No supplied arbitrary PID is signalled.
Original-parent PIDFD is observation-only; worker signals use its own PIDFD.
All descriptors have original fstat pins and close independently where eligible.
The runtime marker remains spent on failed launch; there is no implicit retry.

Prepared F8 tests exercise memory-modeled parent loss, EOF before arm, hard-stop
original timers, descriptor/clock rebinding, late callbacks, concrete native
poll method projection, guardian request routing, arm refusal, both EOFs, late
context close, primary preservation and original outer deadline. Four actual
source omission mutants are prepared. **UNRUN**: no Python, import, parse,
compile, test child, native adapter, socket, provider or DB was executed here.
The frozen V5 tests/assertions remain historical evidence; existing native
launch fixtures require guardian-aware qualification before any future full
suite claim. No earlier passing count is credited to this derivative.

Original Hermes selected caller pins driver717041aa; this new driver is NOT
adopted there. Its original owner must separately update/review a coherent
caller/dependency capsule after this derivative is accepted. Producer76579,
legacy parser/SQL/financial math, Caps, Call, selected workflow budget/expiry and
all frozen/failed/spent artifacts are preserved. Original Claude remains the
sole possible native operator, with no new native authority from this package.

V6 dependency/once refinement: `ctypes.CDLL(None)` and exported libc `prctl`,
`select.poll`, PIDFD/pipe/nonblocking/task-list functions are checked by the real
preflight before any marker/spawn. Availability is not a prctl invocation, ABI
qualification, or evidence from earlier Python3.10 PIDFD checks. The actual
PR_SET_PDEATHSIG operation stays in worker preexec, failing before worker effects.
No such loading/check/operation occurred while authoring this derivative.

The original `.spent` is now written once with its exact packet digest, call ID
and original-parent PID. Guardian checks that immutable consumed record and then
exclusively creates `<same-call-id>.guardian.spent` before worker creation. This
is one additional prospective same-attempt lifecycle marker, not a new call ID,
reset, retry or reuse of old empty markers. Failed/partial records stay spent.
A historical V5 empty marker cannot satisfy this new guard claim. Both future
marker effects require the final native package/receipt-root/authority binding;
no actual marker or config was created here. Target custody and once-only
adoption are still unverified. The prepared set is now14 methods plus4 omissions;
all remain UNRUN. Existing accepted Hermes driver717041aa pin must be reconciled
by its original owner later, with this derivative's final SHA/API pin. No caller
path is edited by this three-path source repair.

## V7 retained static corrections F8-1/F8-2 (UNRUN)

The frozen V6 REVISE is retained verbatim. Its first finding identified a valid
original entry followed by a six-second bootstrap: calling the initial binder
again falsely refused FRESH_ACTUAL_T0. V7 keeps `bind_live`'s original five-second
entry gate. `_bind_live_structure` shares only closed packet/hash/Call/Caps/DB/
runtime/outer-frame validation; it is not a native entry. The separate
`bind_guardian_packet(..., origin=original_t0, arm=original_arm)` validates the
same exact canonical bytes, same Call.t0, actual armed original-parent channel,
original t0+150 end and current monotonic time strictly before original t0+120.
Guardian invokes this continuation validator, never restarts a timer and still
requires the original consumed marker plus exclusive guardian claim. It cannot
create an initial admission, new expiry, fresh Call or new work/cleanup budget.
Prepared boundaries cover +5, +6, just before +120, expiry at +120, missing/unarmed
boundary and reset attempt; the actual guardian_main method is projected under
entirely substituted memory os/signal/runtime bindings to cover its binder call.

The second finding identified a real coupling: LiveHandle.poll delivers queued
input. After observing TERM/KILL, OwnedWorker now calls cancel_packet BEFORE any
ordinary poll. LiveHandle's cancellation latch is set before the fallible stdin
identity/close operation; deliver_packet checks that latch before all callbacks
or writes, and queue_packet cannot reactivate it. The cancellation state is also
retained in the owned loop. Both EOF/reap/close obligations and original bounds
remain. An already dispatched prefix is not rolled back or reported as zero
network/DB activity. Missing evidence stays UNKNOWN. Closing partial stdin means
an incomplete hash-bound frame cannot be silently completed by a later poll.

The additional seven tests are prepared, not executed: initial vs continuation
boundaries; missing/reset/expired arms; actual guardian_main six-second bootstrap
projection; normal actual LiveHandle delivery; actual queued TERM/KILL with
parent lost between final arm and returned spawn; partial frame suppression;
and sticky suppression through close failure/later poll. All fourteen V6 test
methods and four original omission variants remain text-identical. Two new
functional omissions target the actual guardian_main binder and owned-loop
cancel-before-delivery branch. No test/mutant/syntax/AST/compile outcome is claimed.
The existing parser/SQL/producer/Caps/Call, all spent/failed records, native
qualification holds and sole OriginalClaude operator are unchanged. Hermes717
caller integration remains a separate original-owner source gate, not adoption.

## V8 strict Boolean packet guard; actual slot116 failure retained

The actual approved sequence stopped after slot116: F8-focused14 passed,
original4 omission mutants produced assertion rejections, and F8-v7-focused
reported6 passing methods and1 assertion failure with0 errors/prohibited events.
The failing guardian_main projection expected result0 plus exact claim/forwarded/
write assertions. Slot117 (V7 mutants) remained UNRUN. Those original source,
quiet log, receipts and static verdict remain unchanged.

Static diagnosis identifies a production predicate defect, not an incomplete
fake: require rejects every value other than literal True. The old expression
`type(chunk) is bytes and chunk` evaluates to nonempty bytes for a valid read,
so guardian_main catches Refused and returns1 before binding/preflight. Empty
bytes also refuse. The quiet log hides the precise failing assertion, but this
source branch necessarily rejects the fixture's nonempty packet. No rerun,
traceback recovery, parser or interpreter was used to establish that fact.

The sole driver change is `type(chunk) is bytes and len(chunk)>0`, producing a
strict bool and retaining rejection of empty or non-bytes input. Original packet
size, single-newline/hash binding, initial vs continuation freshness, arm/marker,
original120/135/150/outer450, cancellation/reap/bothEOF/close and all accounting
and qualification rules are unchanged. Producer/caller/native package untouched.

All seven original V7 methods and success assertions are text-preserved. The
fixture byte source points to the existing protected V7 producer76579 input;
there is no producer/data change or additional V8 input leaf. Two new prepared
actual-source predicate projections cover nonempty/empty/wrong-type/foreign
objects and unchanged frame-newline/SHA negatives. One prepared mutant restores
the defective byte-valued predicate. Every new projection/mutant/outcome is
UNRUN, including the original whole guardian-main projection against this fix.
No pass or native readiness is inferred from static repair. Historical statuses
above and earlier UNRUN source-freeze descriptions apply to their dated packets.

Actual Python starts remain116 against ceiling117; unused117 is NOT a repair-test
allowance. No interpreter/import/parser/compile/test/native/provider/DB/SSH or
runtime effect occurred for V8. Same retained reviewer performs static review;
future test or target actions require their own applicable authority. Hermes717
pin adoption, target F1–F8 qualification and sole OriginalClaude operator holds
remain separate and unresolved; old spent calls cannot be reused.

### PR857 CI fixture subject correction (source preparation only)

The retained worker-transport fixtures expected cgroup join, three worker rlimits,
`api.handle`, and worker partial-handle kill/reap semantics. They now explicitly
exercise the actual guardian-private `_spawn_worker` body with a real
`ParentDeathArm` backed by inert `GuardIO`. Its nested ctypes import is projected
in a private function-globals dictionary; it never loads host libc or calls
host prctl, and it does not patch process-global modules. The production body,
arm/type/deadline checks, handle/EOF checks and every original assertion remain.

Separate prepared top-level `NativeRuntime.launch` body projections cover the
exclusive/fsynced marker, guardian argv/channels and deferred packet, failure
before child ownership, and partial-owned-guardian cleanup without raw killing
that guardian. A precreated fake child is deliberately left open when Popen was
never reached; closing an unlaunched fixture would be false cleanup evidence.
The lifecycle complete/missing-EOF pair retains its original pipe-closure checks
against the owned worker route. This is synthetic source coverage, not native
qualification or full-suite success. These corrections/new cases are UNRUN;
the actual four CI failures remain preserved. Driver4bb/producer765 and caller
pins/default behavior are unchanged. Any later validation/CI requires its own
exact allocation; no service or execution authority is added here.
