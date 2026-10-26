# P2 reconciliation protocol — no engine selection

Status: plan only. No canonical-engine choice, engine edits, new data collection,
production queries, migration, timer activation or deployment. Paid APIs remain
off. Keep the frozen GEX-levels v1 paper log and inputs untouched. Coordinator
reviews and merges; the September 29 13:30Z freeze remains until the owner
confirms GEM containment.

## Question and evidence boundary

Determine why GRID, Gamma Watch and an independent vendor/recompute disagree
when given identical admissible inputs. Arithmetic agreement is not evidence
of actual dealer positions or profitable directional prediction. Report those
as separate, untested hypotheses. No engine wins because it matches a headline.

Three comparison classes must never be pooled:

1. **Exact-input arithmetic:** both local implementations and an independently
   written reference receive the same immutable normalized rows and parameters.
2. **Matched vendor reproduction:** vendor supplies the underlying snapshot,
   contract universe, OI vintage, timestamp, units and methodology needed to
   reproduce its output. Only then claim an identical-input vendor comparison.
3. **External context only:** delayed headline levels or rounded broker gamma
   with unknown universe/model. Record disagreement but label NOT_COMPARABLE
   for exact reproduction. Cboe chain data can feed both local engines and an
   independent recompute; doing so does not validate a proprietary vendor model.

If existing no-cost sources cannot supply class 2, leave it unavailable. Do not
buy an API, infer hidden inputs, time-shift a headline or backfill a missing chain.

## Code baseline to pin before the experiment

Inspected GRID main at `4422d2e` and collector import #723 at
`80373d585d957401cd67a1fef3b8c6a89942f984`. These are inspection baselines,
not a deployed-version assertion. Pin full commit, file hashes and dependency
versions again when implementing the harness. Preserve native outputs first.

| Dimension | GRID `physics/dealer_gamma.py` | Gamma Watch `broker.py:curves` |
|---|---|---|
| Dealer signs | calls +1, puts -1 | calls +1, puts -1 |
| Raw exposure scale | gamma × OI × 100 × spot | gamma × OI × 100 × spot² × .01 / 1e9 |
| Rate/dividend defaults | r=.05; q=0 in current gamma path | r=.04; q=.012 |
| Time input | integer calendar DTE from loader; excludes DTE=0 | seconds to fixed 20:00Z expiry; excludes expired rows |
| Spot path | verified prior completed unadjusted SPY close | current RTD snapshot price |
| IV | direct IV; loader filters nonpositive; internal helpers retain .25 fallback | direct/recovered paired OTM variants; shocks 0/.2/.4 |
| Universe | eligible completed daily capture | selected expiries/strikes; subscription coverage gates |
| Roots | nearest plus count; 0.1% spot scan and exact-zero handling | all strict sign-change roots on $0.25 grid; rounded output |
| Output limits | public per-strike output truncated to 30 rows | rounded curves; broker gamma diagnostic separately rounded |

The frozen `server.py:model` is a **third local scenario**, not the live RTD
model: fixed chain, different grid, hard-coded OTM-selection reference spot,
and wall-clock aging. Preserve its input hash and original valuation instant;
do not rerun today and call that a historical observation. Never import the
server for this offline test: import initializes its journal.

## Immutable matched-input packet

Each packet has a UUID, exact raw bytes and SHA256, normalized-row SHA256,
source contract/version, code/config hashes, receipt lineage, and exclusion
ledger. Keep raw files outside Git when licensing or size requires it; commit
synthetic fixtures and a manifest, never broker accounts or secrets.

Required row identity: underlying, expiry instant/timezone, strike, call/put,
multiplier and deliverable. Include IV as a decimal, OI and OI-as-of (or unknown),
quote fields and source times, field receipt times, IV origin/donor, corporate
action/adjustment basis and row validity. Reject duplicate identity conflicts.
Unknown multiplier/deliverable is not silently 100. Initial scope is standard
SPY contracts; adjusted contracts and SPX/ES offsets are excluded explicitly.

Packet parameters: fixed valuation instant, unadjusted spot and receipt, r, q,
day-count, expiry calendar/version (DST and early closes), IV policy, dealer-sign
vector, expiry/strike universe, common spot grid and numerical precision.

**RTD available_at is grid-svr's completed pull receipt**, not ANIK's Windows
clock. Retain Windows timestamps as untrusted source metadata until monitored
clock offset/drift evidence is accepted in a separate change. Server clock
health must also be evidenced; if unavailable, timing validation is unknown.
Pull time proves when GRID had the bytes, not exchange freshness or OI age.
Model available_at is no earlier than all parents and its computation receipt.

Do not use matching display timestamps as evidence of matching packets. Each
engine must attest to exactly the same accepted row hashes and ordered parameter
manifest. Compare the common intersection separately from each native universe;
report all omitted rows and their OI mass. Missing OI mass stays unknown, not 0.
No snapshot joining with data first received after the experiment cutoff.

## Units and staged reconciliation

Reference unit is modeled hedge-notional change in USD for a 1% spot move:

`G(S) = sum(sign_i * gamma_i(S) * OI_i * multiplier_i * S^2 * 0.01)`.

This is an exposure convention, not an options P&L estimate. Preserve each
native unit alongside the normalized value. For GRID's current scale multiply
by `S * .01` **at every grid spot**; Gamma Watch billions multiply by `1e9`.
Positive scale conversion preserves mathematical roots, but can alter linear
interpolation error. Compare unrounded values and refine roots, not chart text.

Execute in this order in a later, separately reviewed offline harness PR:

1. Save native behavior on its own packet, including unavailable results.
2. Exact positive-IV, nonzero-T synthetic fixtures with r=.04, q=0 and explicit
   fractional T; no recovery. Feed native arithmetic helpers via isolated
   adapters, bypassing DB/SSH/live clocks. Document adapter transformations.
   Gamma Watch needs a pure parameterized extraction; do not call a reimplemented
   copy of its formula and claim to have tested the deployed function.
3. Apply only unit normalization. Compare contract gamma, signed contribution,
   expiry subtotal, strike subtotal and total before comparing walls/roots.
4. Change one factor at a time: r, q, integer/fractional T, calendar expiry,
   prior-close/current spot, direct/paired IV, expiry scope, wings, and OI vintage.
   Save each delta; mark interaction residuals rather than claiming additive
   attribution when factors interact. q=.012 is a sensitivity until GRID's
   production path can represent it; do not silently rewrite that path.
5. Compare matched real packets only after synthetic invariants pass. Report
   0DTE separately: current GRID loader excludes it, so native equivalence is
   NOT_SUPPORTED, not a zero exposure or successful comparison. Fractional-T
   kernel tests do not imply production 0DTE support.
6. Add vendor reproduction only after its comparability gate passes. Otherwise
   keep headline/vendor diagnostics in a separate appendix.

Dealer signs are named scenarios, not fitted parameters. Include calls+/puts-,
global sign reversal and per-leg perturbations to expose identifiability. A
global reversal negates exposure but leaves roots unchanged; matching flip
prices therefore cannot validate dealer signs. Never fit signs to subsequent
price movement and present that as out-of-sample evidence.

## Preregistered comparisons and acceptance

Proposed numerical tolerances below apply to exact-input float64 fixtures, not
to broker rounding or vendor output. Review/freeze them before viewing real
comparison results; any revision gets a new protocol version with rationale.

- Per-contract gamma: `abs(error) <= 1e-12 + 1e-8 * abs(reference)`.
- Aggregates/curve: `abs(error) <= $0.01 + 1e-8 * gross_absolute_GEX` at every
  point. Scale by gross exposure to avoid dividing by a near-zero net value.
- Retain raw error and relative-to-gross error even on passes. Near cancellation,
  an error-bound interval containing zero yields INDETERMINATE sign, not neutral.
- On a common grid, bracket all roots and refine to $0.001 bracket width.
  Root-set match requires equal counts and one-to-one separation <=$0.01;
  include sign on both sides and residual. Test no root, multiple roots, exact
  zero, tangency, flat zero interval and boundary zero separately. Tangency is
  not a regime crossing. No sign-change bracket means no asserted flip.
- Define walls explicitly: max positive call exposure, most negative put
  exposure, and max absolute net strike exposure. Preserve ties within the
  exposure tolerance as a set. Compare full untruncated strike rows, not only
  displayed levels. Vendor wall definitions must match before scoring them.
- Max pain is a separate OI-payoff statistic, not a gamma-wall/root test. Vanna,
  charm and hedge-flow forecasts are outside this first reconciliation phase.

Fixture matrix: single call/put; equal offsetting pair; multiple expiries at one
strike; unequal IVs; zero OI; missing/nonfinite/negative inputs; expiry boundary;
DST/early close; percent-vs-decimal IV; multiplier variation; exact/near-zero
net; multiple/no roots; out-of-window spot; missing wings; duplicate contracts;
stale/replayed receipt and unavailable clock provenance. Independent finite
difference of delta checks gamma with step-size convergence. The reference must
not import either implementation's gamma helper.

Result statuses: PASS_NUMERICAL, FAIL_NUMERICAL, NOT_COMPARABLE,
NOT_SUPPORTED, INPUT_REJECTED, INDETERMINATE. Every run emits a machine-readable
manifest/results/exclusions plus concise human report, including zero eligible
packets. No post-hoc exclusion of failures. Store original and transformed
outputs and each transformation version, with no mutable overwrite.

## Deliverables and decision hold

- [ ] P2-A: pure offline adapters and independent reference, pinned fixtures,
  frozen tolerance config and deterministic replay tests; no production imports
  with side effects, network calls or DB writes.
- [ ] P2-B: admissible matched real packet report; separate universe/time/model
  attribution and vendor comparability table. Collection/export is separately
  scoped; no GEM/options_snapshots locks or frozen-v1 changes.
- [ ] P2-C: independent review of discrepancies and proposed corrections. Only
  after that may a separate owner-reviewed decision propose a canonical engine.
  Numerical equality alone does not choose one or establish a trading edge.

## Carried P1/P4 review notes — explicitly pending

- [ ] Migration promotion: remove/rework the environment guard so approved
  deployment's ordinary Alembic upgrade does not fail. Approval belongs before
  promotion/execution, not an undocumented production environment dependency.
- [ ] systemd: EnvironmentFile overrides Environment entries. The false line is
  not an activation lock. Current placeholder refusing true is the actual code
  stop; future activation needs a separately reviewed effective-config gate and
  tests covering environment-file true/blank/invalid overrides.
- [ ] RTD clock: implement grid-svr pull-receipt available_at as specified above;
  do not grandfather historical Windows callback times as trusted availability.
- [ ] Real isolated PostgreSQL tests remain pending: DDL/FKs/checks, immutable
  UPDATE/DELETE/TRUNCATE, source seed collision rollback, cursor transaction and
  concurrent replay, permissions and actual Alembic promotion path. No scratch
  database on production and no migration application in this plan.
- [ ] P4 GET side effects: scenario GET creates an artifact; /api/journal sets
  WAL mode. Inventory imports and request paths, separate query reads from
  writes, and test read-only mode without changing production in this PR.
- [ ] P4 exclusions: inspect snapshot.tmp.json, actual log filenames and SQLite
  sidecars against .gitignore; test with git check-ignore and secret scanning.
  Existing *.log is not proof every runtime file is excluded. Never commit a
  real snapshot to prove the ignore test.

These are tracked work, not claims of fixes made by this documentation PR.
