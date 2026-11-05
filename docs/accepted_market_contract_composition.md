# Accepted market contract composition

This isolated composition uses the accepted feed, numeric/session and OR/VWAP,
temporal, and fleet modules together with their original bytes. It adds a joint
synthetic test and this usage example. Main is
`e8d68da7ad4c71b8c308cb7e5f685ee0758e8ce5` (tree
`10aec2c8183de68b512f1a868f6920ed6ac9604c`). The OR patch supersedes the numeric
full patch: applying numeric then its accepted mathematics delta independently
produces the same four OR files. Applying both full additions together is invalid.

The modules have separate responsibilities:

| Public API | What the supplied facts establish |
| --- | --- |
| `evaluate_feed_quality` | Caller-qualified presence, identity, evidence, clocks, finality and required quality checks |
| `validate_numeric_bar`, `validate_bar_grid` | Numeric structure, exact observation duration, basis/session identity and declared coverage |
| `opening_range`, `trade_vwap`, `ohlcv_hlc3_volume_proxy` | Qualified high/low or exact rational arithmetic after all independent prerequisites pass |
| `classify`, `select_latest_as_of` | Temporal diagnostics under a caller-declared versioned duration/clock policy; no numerical or trading authority |
| `run_fleet_heartbeat` | Fleet monitoring/alerting; default sweeps do not heal; primary GPU interventions require operator handling |

A structural pass does not grant feed eligibility. Missing/unknown facts stay
unknown and feature numbers remain `None` when prerequisites do not pass. Trade
VWAP uses supplied compatible trade notional divided by shares. The HLC3 volume
proxy has a different explicit basis and method, and cannot be silently called
trade VWAP. Decimal inputs are converted to exact fractions without rounding a
nonterminating ratio into a fabricated observed decimal.

The feed's event, receipt, as-of, availability and recording clocks are distinct.
The feature decision clock equals its cutoff; the evaluation clock is explicitly
injected. Research age checks retain the decision clock when evaluation happens
later; CURRENT mode ages against the evaluation clock and also requires lifecycle
facts. A current-mode event at the exact 300-second boundary becomes ineligible
one microsecond later without changing historical research eligibility.
Session/calendar, grid, adjustment and trade-notional references each
have their own knowledge clocks. Equality at cutoff is admitted; a microsecond
later withholds the result. Durations bind the observation's declared interval to
actual elapsed geometry, including unequal intervals. No exchange/provider/session
mapping is invented here: every identity and calendar is caller-supplied synthetic
`TEST:SPY` data, and hashes are opaque declarations, not authenticity claims.

The runnable example below uses the joint test's `inputs()` and `temporal_row()`
synthetic fixture constructors under the packet's **pre-import offline harness**.
The collected test module defines twelve ordinary wrappers without changing shared
modules, process functions, package paths or audit hooks. At test execution, one
isolated child per unittest class installs the permanent effect fences and fake
fleet dependencies, then loads all four real accepted modules under canonical
identities. Each wrapper reports its matching original functional method result;
all twelve original functional bodies and assertions remain unchanged.
The helper constructors only supply DTO facts; all evaluation and mathematics
below use actual public production APIs. It can be run with the existing Python
3.11 interpreter as `python3.11 -B tests/test_accepted_market_contract_composition.py --isolated-example` from the composed repository, or through the repair packet's
inspected offline runner. Normal unittest/pytest execution launches the same
fenced child once and reports twelve wrapper results. Aggregate collection itself
launches no child and imports none of the accepted production modules. It does not need a database or a provider.

```python
bars, grid, feed_policy, session_facts = inputs()
observations = tuple(bar.observation for bar in bars)
feed = Q.evaluate_feed_quality(
    observations, feed_policy, now=CUTOFF, decision_at=CUTOFF,
    sessions=session_facts,
)
assert all(cap.state is Q.State.PASS for row in feed.instruments for cap in row.capabilities)
assert all(N.validate_numeric_bar(bar).state is Q.State.PASS for bar in bars)
assert N.validate_bar_grid(bars, grid).state is Q.State.PASS

clocks = dict(cutoff=CUTOFF, evaluation_at=CUTOFF, decision_at=CUTOFF,
              quality_policy=feed_policy, session_facts=session_facts)
opening = N.opening_range(bars, grid, minutes=5, **clocks)
trade = N.trade_vwap(bars, grid, **clocks)
assert (opening.high, opening.low) == (Decimal("15"), Decimal("8"))
assert trade.vwap == Fraction(59, 5)
assert trade.inputs[0].observation is observations[0]
assert trade.quality_policy is feed_policy

# The caller declares a distinct proxy basis. No substitution or hidden fallback.
proxy_basis = replace(grid.basis, notional_basis=N.NotionalBasis.OHLCV_PROXY)
proxy_bars = tuple(replace(bar, basis=proxy_basis) for bar in bars)
proxy_grid = replace(grid, basis=proxy_basis)
proxy = N.ohlcv_hlc3_volume_proxy(proxy_bars, proxy_grid, **clocks)
assert proxy.method is N.PriceFeatureMethod.OHLCV_HLC3_VOLUME_PROXY
assert proxy.vwap == Fraction(56, 5)
assert N.trade_vwap(proxy_bars, proxy_grid, **clocks).vwap is None

# Separate caller-declared temporal row and policy; no automatic price conversion.
row, temporal_policy = temporal_row()
diagnostic = T.classify(row, cutoff=CUTOFF, policy=temporal_policy)
assert diagnostic.status is T.Status.CURRENT
assert diagnostic.row is row
assert diagnostic.row.payload_json is row.payload_json
assert T.classify(row, cutoff=CUTOFF + US, policy=temporal_policy).status is T.Status.STALE
assert T.classify(row, cutoff=CUTOFF, policy=None).status is T.Status.UNKNOWN

# The caller supplies revisions; an unavailable later vintage cannot hide current.
late_revision = replace(row, row_id="row:2", vintage=CUTOFF + US)
selection = T.select_latest_as_of(
    (late_revision, row), key=row.key, cutoff=CUTOFF, policy=temporal_policy,
    selection_policy=T.SelectionPolicy("selection-v1", "LATEST_AS_OF"),
)
assert selection.current is row
assert selection.history == (late_revision, row)
```

Temporal policies use exact nonnegative `timedelta`, never guessed seconds or
family SLAs. Observation, release, availability and vintage each retain their
role. A precomputed future event with explicit permission/horizon goes into
`known_future`, never current. For caller-supplied derived-score diagnostics,
every declared direct support leaf must also have been known at the supplied
computation clock; being current at the later decision alone is insufficient.
This does not add a scoring bridge, calculate a score, fetch automatic revisions,
authenticate payloads, or change the canonical database PIT reader.

Fleet tests run in the same process as the market modules but use fake probes,
a SQL-shaped in-memory engine, captured commands and captured email. They check
both default monitor-only behavior and explicit primary-GPU refusal, plus a
permitted remote fake-command positive control. They do not qualify host/runtime
capabilities or authorize live fleet invocation.

The packet records the accepted individual suites separately from the 12 joint
test methods and the executable example. Mutation controls corrupt isolated
copies of actual source and must fail functional assertions without import/test
errors. Older numeric-only scope assertions that predate the admitted mathematics
remain immutable; any failure is retained and disclosed in the packet, with
functional and successor AST checks reported separately. Full CI, DB/provider,
network, native/runtime tests, merge, publication, deployment and trading remain
outside this composition. Independent composition review is required after freeze.


## Collection isolation repair

The V1 standalone results remain historical evidence, but V1 was held when
repository integration showed that its import installed permanent process fences
and replaced canonical dependency/package state in the collecting process. V2
confines all of that state to a short-lived child process. Parent import and
collection perform no `sys.modules` replacement, package-shell installation,
process-function reassignment or audit-hook registration. The child retains
pre-import fences, fake fleet effects and strong negative controls. The executable
example also runs in that child; it creates no automatic live integration.

Before/after collection-state probes and supported repository checks are recorded
in the V2 packet. Same-reviewer acceptance and dependency-complete full CI are
separate gates. Missing local CI dependencies are reported as unavailable; no
installation, source fallback, host qualification, publication or release is
implied by this repair.
