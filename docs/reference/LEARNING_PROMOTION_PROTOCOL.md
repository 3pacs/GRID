# Learning Promotion Protocol (held-out, GRID W7)

> Load when: deciding whether a research output (a signal weight change, a model,
> a signal policy) is eligible to be recommended into `governance/promotion_ledger.py`,
> or when reviewing a recommendation for approval. Written 2026-09-18 as part of the
> W7 safeguards workstream. This is a protocol document, not code — it constrains what a
> human/process may recommend or approve; it implements nothing itself.

## Why this exists

The 2026-09-17 vault audit found that `intelligence/signal_weight_overrides.py` was
applying live weight cuts (e.g. vol ×0.30, fx ×0.40) derived from a `trade_postmortems`
corpus whose scoring is contaminated: four incompatible scoring regimes coexist in
`signal_sources`, an April 2026 synthetic batch dominates the scored predictions used to
compute those ratios, and oracle prediction scoring stopped entirely on 2026-05-16. None
of that was visible from the override table itself — it read as "empirically perfect"
(1826 right / 0 wrong) or "empirically 0 right" data.

The promotion ledger (`governance/promotion_ledger.py`) is the mechanical gate: nothing
gets applied without an approved record. This document is the *judgment* gate that has
to happen before someone writes that approval — otherwise the ledger just becomes a
rubber stamp with an audit trail.

## Frozen train / validation / held-out periods

Before any candidate (weight override set, model, signal policy) is evaluated for
promotion, three non-overlapping calendar periods must be fixed **in the recommendation
record's `evidence_ref`** before evaluation starts, not chosen after seeing results:

- **Train**: the period used to fit/derive the candidate.
- **Validation**: used for iterative tuning during development. Any number of looks are
  allowed here — this is exactly the data repeated search consumes (see below).
- **Held-out**: touched exactly once, after the candidate is frozen, to produce the
  number that goes into the promotion recommendation. A held-out period that has already
  been looked at (even once, even informally) is no longer held-out — start a new one.

Freezing is enforced by convention, not by code, at this layer. The `evidence_ref` in
`promotion_ledger.recommend()` must point at a document/commit that records the exact
train/validation/held-out date boundaries *before* the held-out period was evaluated.
A reviewer approving a recommendation should reject it if that evidence doesn't exist or
if the dates were plausibly chosen after seeing held-out performance.

## Walk-forward with embargo

Validation and held-out evaluation should use walk-forward evaluation, not a single
train/test split — see `validation/backtest.py`'s `WalkForwardBacktest.run_validation`
for the existing non-embargo implementation (n_splits, era-based evaluation).

An **embargo period** between the end of a training window and the start of the
following evaluation window is required for any candidate whose features have
autocorrelated lookback (rolling windows, EWMAs, anything computed over trailing days) —
without it, a training window's tail leaks into the adjacent evaluation window's early
observations through shared feature history, inflating apparent performance.

Per draft PR #556 (not yet merged into this branch/base as of 2026-09-18 —
`validation/backtest.py` in this codebase does not yet have `fit_fn`/`embargo_days`
parameters), the intended mechanism is: `run_validation` (or a successor) takes a
`fit_fn` callable, refit per split, plus an `embargo_days` parameter that excludes that
many days immediately following each training window's end from the following
evaluation window. **Do not implement this here** — this document only requires that any
candidate relying on autocorrelated features cite an embargo-respecting walk-forward
result (from #556's mechanism once merged, or an equivalent one), and states the
embargo length used, in its `evidence_ref`.

## Repeated-search control

Every promotion candidate arose from *some* search — a grid over thresholds, a set of
hypotheses tried against validation, a family of signal families tested. The
recommendation's `evidence_ref` must record:

- **N**: the number of hypotheses / configurations / thresholds evaluated against
  validation (or held-out, if it was touched more than once — which itself should be
  rare and justified) before arriving at the recommended candidate.
- Enough detail to reconstruct N independently (a search log, a notebook, a commit
  history of configs tried) — a bare number with no trail does not satisfy this.

**Nominal significance after N searches is not a gate.** A candidate that clears
p < 0.05 (or an equivalent hit-rate/Sharpe threshold) on its Nth look is not thereby
promotable — multiple-comparison correction (Bonferroni, false discovery rate, or a
pre-registered stricter threshold scaled to N) must be applied and the corrected
result — not the nominal one — is what a reviewer checks against the promotion bar. If N
is large enough that no reasonable correction survives, the honest conclusion is "not
promotable from this search," not "promote anyway, it's close."

## Correlated-observation uncertainty

Trading/signal outcomes are not i.i.d. draws — the same regime, the same macro event, or
the same crowded trade can move many "independent" observations together, which makes a
naive standard error (assuming independence) too small and overstates confidence.

**Method to be used** (stated here as a requirement on future implementation — this
document does not implement it): variance estimates for any promotion-relevant
performance statistic (hit rate, alpha, Sharpe) must account for cross-observation
correlation via a block/cluster method appropriate to the correlation structure —
e.g. a block bootstrap sized to the empirical autocorrelation length, Newey-West/HAC
standard errors for time-series statistics, or clustering by regime-period/event rather
than by observation. Whichever is chosen, it must be named and its clustering unit
stated in the `evidence_ref` — "we used the naive standard error" is not compliant.
This is intentionally not implemented as code in this workstream; it belongs in
`validation/backtest.py` or a dedicated statistics module, and should itself go through
this same promotion protocol once written, since a wrong or unvalidated variance
estimator is exactly the kind of research output this protocol exists to gate.

## Resource budget

Each promotion recommendation should state the compute/data budget it consumed to reach
its result (approx. wall-clock, number of backtest runs, DB query volume against
production tables) in its `evidence_ref`, both so reviewers can sanity-check "this much
searching for this much result" against the repeated-search N above, and so repeated
re-evaluation of the same candidate doesn't silently become a second, uncounted search.

## Canary + rollback

Every `approve()` call should set `canary_scope` (a description of what's exposed to the
change first — a ticker subset, a signal family, a time-boxed shadow period — never "all
production traffic immediately") and `rollback_ref` (how to undo it — e.g. "supersede
with a new recommendation restoring the prior override table's subject_hash," or a
specific config/flag flip). `governance/promotion_ledger.py` stores both as plain
columns; this document is what requires them to be filled in meaningfully rather than
left as placeholders. Widening canary scope after initial approval is a new promotion
(a new recommend()/approve() pair referencing the canary result as evidence), not an edit
to the existing approval — the ledger has no update path for exactly this reason.

## Controller-decision list

The following are standing, unresolved controller decisions surfaced by the 2026-09-17
audit. They are listed here because a promotion recommendation that silently assumes an
answer to one of these is out of scope for this protocol — the controller decision has
to be made (and recorded) separately, not smuggled in via a weight-override approval.
**Affected populations are explicitly left as "to be measured"** — no counts are stated
here because none have been independently verified as of this writing; inventing a
number to make this document look more complete would be exactly the kind of
unsubstantiated precision this workstream exists to remove.

1. **Historical reconstruction** — whether/how to reconstruct a corrected scoring history
   for the affected `trade_postmortems`/prediction-scoring period, vs. treating pre-fix
   history as permanently unusable evidence. Affected population: to be measured.
2. **Exclusions** — which cohorts (by scoring regime, by origin, by date range) get
   excluded from any future promotion evidence entirely, vs. down-weighted. Affected
   population: to be measured.
3. **Backfill / rescoring** — whether the oracle prediction scoring pipeline (stopped
   since 2026-05-16) gets restarted with rescoring of the gap, or the gap is treated as
   a permanent hole in the evidence base. Affected population: to be measured.
4. **Weight disabling** — beyond this workstream's default-OFF change to
   `GRID_SIGNAL_OVERRIDES_ENABLED`, whether any *other* auto-weight paths found during
   the W7 grep (see the workstream's commit/report for the file:line list) should be
   permanently retired rather than re-gated. Affected population: to be measured.
5. **NEUTRAL policy** — the standing policy for how NEUTRAL/regime signals (e.g.
   `alpha_research:vix_exposure`, `alpha_research:credit_cycle`, already reclassified as
   regime rather than directional per the 2026-04-28 fix) should be treated by *any*
   future override or promotion — as out-of-scope for directional weighting entirely, or
   eligible under a different evaluation track. Affected population: to be measured.
6. **First automatic no_data close-out** — the policy decision on whether/when a
   prediction or signal lifecycle may be automatically closed out as `no_data` (rather
   than left pending or requiring manual review), and what that implies for any
   downstream statistic computed over "resolved" predictions. Affected population: to be
   measured.
7. **Two-knob flip (GRID W7b, held)** — `intelligence/signal_weight_overrides.py` now
   ships two independent knobs, both defaulting to the pre-existing production
   behaviour: `GRID_SIGNAL_OVERRIDES_ENABLED` (default **True**, unchanged from before
   this workstream) and the new `GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER` (default
   **False** — no ledger check, matching legacy behaviour, with one WARNING logged
   naming the knob). Flipping both defaults (`ENABLED=False`,
   `REQUIRE_LEDGER=True`) is a live weight-policy change — it stops today's override
   cuts/boosts from applying at all until a `weight_override` promotion is recommended
   and approved — and is held on `fable/overrides-policy-20260918` pending explicit
   operator approval; see that branch's
   `docs/handoffs/2026-09-18/fable-w7-held-policy-flip.md` for the rollback path.
   Affected population: to be measured.

Each of these, once decided, should itself be recorded as a `signal_policy` kind
recommendation in the promotion ledger — the decision is exactly the kind of durable,
attributable record this system exists to produce.
