# PR933 current-main source preparation

This local candidate is a proposal for the existing PR933 vehicle. Receiving
main is `3b55d69b9147d1702d3453ca99154310e2c19d1a` (tree
`e02826f16aa255452424fbc9b9a1d243edaa0a84`), after PR932. Original PR933
`0afc9cc4926470607ae5e15e8bed9d8b972b0cc3` remains published unchanged.

## Proposed policy and lineage

Original PR933 creates a new releases directory with mode0755 and forcibly
sets the copied candidate root to0755. That accepts an unsafe legacy root,
can widen private0700 access, and does not qualify nested/build-created paths,
Git metadata, marker writers, rollback or service bindings.

This candidate instead proposes the already reviewed strict refusal policy
from `08cec9767e595b9a1dae02c1c942eb29f42365a8`. It selects the14deployment
paths relative to `5bb533e58da29f6fda5bf71fac3e8568f4db6aba`, rather than
merging the older entire tree. The accepted inventory section is appended to
current main's documentation, preserving the Schwab changes. Other selected
postimages are exact. The source policy protects Git inputs and tracked bytes,
marker writers, rollback targets and service bindings, preserves private modes
and mutable ignored runtime data, and refuses unsafe or unresolved inputs.
It never repairs existing permissions. Production selection of this policy
over original PR933's normalization remains an explicit decision.

The shared policy's explicit `pin` operation still performs the existing
reviewed exact-SHA fetch/reset when an authorized deployment invokes it. No
deployment operation was invoked in preparing this candidate. Deployment still
has its existing install/build/migration/swap/service effects; this source
proposal does not authorize them or change their release sequence.

## Aggregate metadata diagnostic

`scripts/deploy_metadata_preflight.py` retains the reviewed self-contained
source SHA256`8d0de6b38c5acf47ad2b83598c27ba30f94d3475283af01045c06c092088398f`.
Its44mocked controls are relocated to `tests/deploy`; both source references
in the test use the new location. The test audit fence is active only during
its own test case, with context reset registered as cleanup, so importing the
module cannot block unrelated repository tests. Four import/scope controls
preserve that isolation and refusal of every protected event category. The collector is standalone and is not wired
into automatic deployment or introduced as a new mandatory release gate.

It aggregates33fixed path/runner metadata nodes, reports separate group/other
write and ownership/type/link/race findings, and blocks unsafe descent while
continuing independent checks. It reads directory ACL metadata and fixed
screened unit properties, never regular file bodies, credentials, DB, native
driver or product modules. Bounds remain30s cooperative,8192visits,32levels,
64findings,65536output bytes; fixed unit reads are2s/4096stdout/zero accepted
stderr, with a shared0.5s owned-group cleanup bound. A future exact operator
admission must supply the independent35s+2s hard transport bound, private
output, reservation, unique attempt and applicable local gate. No such live
admission or execution is included here. Snapshots remain non-atomic.

The separately approved N3 run41697161229 qualified14native synthetic metadata
fixtures, zero failures/errors/skips and successful cleanup on exact
`a006d4075d076ac527861349213546d3a10c7da2`. Those results are retained evidence;
they are not rerun results for this tree or qualification of the fixed
production profile. Local caller fixtures explicitly substitute inherited
`/tmp` ancestry and toy non-Git identity; their scope is stated in the fixture.

## Receiving and publication limits

All committed paths stay protected by the retained payload contract. Its28
declared external aliases (16workflow configuration,2model,2cache,8output/state)
still lack accepted external owner/integrity contracts and refuse. Generated
PWA/executable and toolchain closure is not attested. This candidate neither
omits executable inputs nor creates an integrity exception. It must not be
merged/deployed as a qualified receiving payload until those concrete bindings
are resolved or an explicitly reviewed narrower deployment policy is selected.

The current-main workflow bytes are preserved. A PR933 update targeting main
can trigger GRID Tests, forward/HOLD PG14/15, Quiver PG14, the base-sourced
freeze guard and optional Claude review. Python-floor qualification is
path-filtered and none of its paths change here. Pushes to this feature branch
alone do not trigger the main/develop push workflows; PR synchronization does.
The native/Quiver jobs use hosted disposable containers. GRID Tests does not
currently have a PR933-specific hosted predicate: with TEST_RUNNER set it can
select a self-hosted runner and reset its persistent test schema. Therefore
publication requires a verified disposable route (or a separately reviewed
PR933-only source condition) and verification that optional external Claude
review is disabled, or explicit authorization of its data transmission.
Neither variable state nor execution is inferred from past skipped runs.
No workflow/ref/settings change or CI execution occurred in this preparation.

Future action must recheck main, old PR head, receiving tree and review pins.
The exact candidate's source review is independent from publication, hosted CI,
installed path qualification and production release approval. Original PR933,
08cec, prior qualification failures, N3 and all spent observations remain
immutable. PR918/931/932 merged source and frozen evaluation/scoring constraints
are preserved.
