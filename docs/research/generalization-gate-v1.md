# Generalization gate v1: how a sector finding earns, and passes, a cross-sector test

Status: repo doc for `analysis/generalization_gate.py` (GD10b). This is **not** a
preregistration. Each construct's own prereg (sectors-v6 for the VS1 construct,
the GD8/GD9 family preregs) cites this gate by the sha256 of `GateSpec` v1
(`analysis.generalization_gate.GATE_SPEC_V1_SHA256`, pinned by
`tests/test_generalization_gate.py`).

Plan source: `GRID-GRANULAR-DISCOVERY-PLAN-20260927` section 2.5 (all five
conditions) and section 4 row GD10.

## 1. The route (how a construct gets to the gate)

A Technology-only result (VS1 v8, or any later sector-first construct) earns a
cross-sector test only through this chain:

1. **Freeze before the second sector.** The construct's parameters (windows,
   half-lives, codes, stages, lags, horizons, direction, confirmatory trial) are
   frozen in a hash-pinned prereg *before* any second sector is scanned. For the
   VS1 construct that prereg is sectors-v6 (GD10a), registered after v8's
   registration witness and before v8 `discovery_opened`. For GD8/GD9 constructs
   it is their family prereg, which lists every sector up front.
2. **No per-sector tuning.** Per-sector tests run under that one prereg. A
   sector that fails its Stage-0 power gate is untestable: it is never opened
   and it counts as a non-survivor.
3. **Once, at the end.** The gate is computed once, after every declared
   sector's holdout is terminal and witnessed. The function refuses a partial
   input set, so it cannot be used to look at some sectors' outcomes before the
   others reach their registered terminal record. The verdict is recorded in
   the construct's own registry.
4. **Until it passes**, results are reported per sector and per density bucket
   only. The allocator may move budget toward promising sectors, but nothing is
   quoted as "the market", "general" or "cross-sector".

Even `GENERAL_REVIEW_REQUIRED` keeps `promotion_allowed=false`. It means "worth
a forward-capital discussion with the owner", never "trade". Using a verdict to
change anything live (real allocation, UI claims) is an owner decision.

## 2. Inputs

`SectorResult`, copied from each sector's witnessed terminal registry record:
terminal record sha256 and kind (`holdout_result`, `stage0_untestable`, or
`stop` for Technology only), prereg sha256, pre-registered direction, sealed
holdout one-sided p, permutation block, the sealed per-date holdout IC series,
per-entity IC contributions (summing to the IC sum), the coverage-stable IC
series, and the forward verdict with its own record sha256.

The caller injects `witness_check(record_sha256, result)`. It must return the
plain `True` only when the record is witnessed on the vault **and** `result`'s
sealed fields are the ones recorded under it (`content_sha256(result)`); the
verdict lists every input's `content_sha256`, so a swapped IC series or a
sector relabelled as untestable under a witnessed sha is refused. The caller
also passes `expected_spec_sha256` from the construct's prereg; any other spec
(even a "v1" with different perms or seed) is refused. The verdict embeds the
sha256 of the gate module's source (`implementation_sha256`).

The gate refuses (`REFUSED`) when: any terminal or forward record sha is
missing, malformed or fails `witness_check` for that content; the spec is not
the prereg's; a declared sector is
missing or duplicated; inputs come from different preregs or directions; a
non-Technology sector claims STOP; an untestable record carries holdout
statistics; IC series are non-finite or duplicated; contributions do not add up
to the IC sum; dates are not ISO strings; the sectors do not share a common
decision grid (alignment below 0.80); or the grid has fewer than 8 sign blocks
(the joint null would be degenerate).

It never opens a holdout, reads prices or labels, or recomputes an IC.

## 3. The five conditions and their thresholds

| # | Condition | v1 rule | Why this threshold |
|---|---|---|---|
| 1 | Breadth | survivors (holdout one-sided p < 0.10, pre-registered sign) >= max(k_binomial, k_permutation) | k_binomial = 4 of 11: P(X >= 4 \| 11, 0.10) = 0.0185, the smallest k with chance <= 0.05 (P(X >= 3) = 0.090). k_permutation: the smallest k whose rate under the sector-block permutation null is <= 0.05. |
| 1b | 10-sector branch (v8 STOP) | 4 of 10 | P(X >= 4 \| 10, 0.10) = 0.0128; still the smallest k with chance <= 0.05 (P(X >= 3) = 0.070). Pre-declared here so GD10a can cite it. |
| 2a | LOSO | for every testable sector j, the pooled IC of the others keeps the sign with two-sided p < 0.05 | v1 method `cluster_t`: equal-weight per-date pooled IC (absorbs same-date cross-sector correlation), CR1 cluster-robust t over blocks of consecutive dates (the clusters are date blocks, not sectors), df = clusters - 1. `sector_bootstrap` (percentile bootstrap over whole sectors) exists but is anti-conservative with ~10 clusters and ignores time dependence; a later spec must calibrate it before selecting it. |
| 2b | Top entity | in every surviving sector, the top entity's contribution < 25% of the IC sum | plan section 2.5 |
| 3 | Forward | >= 2 surviving sectors with a witnessed `FORWARD_SUPPORTED_REVIEW_REQUIRED` | plan section 2.5; one gives `FORWARD_PENDING` |
| 4 | Coverage honesty | at least the required number of survivors still survive on their coverage-stable IC series (block sign-flip, same sign, p < 0.10) | the GD5 coverage guard; a coverage jump must not read as a density jump. The p uses the spec's seed/perms, so a sector near 0.10 can differ from its sealed p by Monte Carlo noise. |

Verdict precedence: `REFUSED` > `INSUFFICIENT_SECTORS` (breadth or coverage) >
`SECTOR_SPECIFIC` (dominance) > `FORWARD_PENDING` > `GENERAL_REVIEW_REQUIRED`.
Every condition is reported regardless.

## 4. The breadth null, and a correction to the plan's sketch

The plan sketches the count's null as "shift each sector's IC series by a random
circular offset". That cannot calibrate this count: a circular shift leaves a
sector's mean IC, and therefore its holdout survival, unchanged, so the
"null" count always equals the observed count (a degenerate null). The
correlated worlds below show why a real null matters: the binomial threshold
is anti-conservative exactly when sectors share a factor.

v1 therefore uses a **joint block sign-flip** (still a sector-block permutation
in the sense that blocks of dates are permuted in sign, identically across
sectors): blocks of consecutive decision dates on the common grid get one
random sign shared by every sector (VS1's `block_signs`, the largest sector
block). This keeps each sector's autocorrelation within blocks and keeps the
cross-sector dependence. Each series is centred first, so the dependence is the
residuals' and real effects in several sectors do not masquerade as a shared
factor. In each null draw a sector "survives" when its null mean is in the top
10% of its own null distribution; the count's tail rates give k_permutation.

Calibration (synthetic, `tests/test_generalization_gate.py`, 80 decision dates,
11 sectors, IC = sqrt(rho) f + sqrt(1 - rho) e):

| World | binomial rule (>= 4) false-breadth rate | permutation rule false-breadth rate |
|---|---|---|
| independent | ~0.02-0.03 (theory 0.0185) | same (k_permutation = 4 in > 90% of worlds) |
| rho = 0.3 | ~0.075-0.087 | ~0.03-0.04 |
| rho = 0.6 | ~0.10-0.12 | ~0.02-0.05 |

(Ranges over the author's 2000-world runs and the reviewer's 700-world probes
with other seeds; the tests assert the permutation rate is <= 0.05 up to two
Monte Carlo standard errors at 1200 worlds and that the binomial rate is not.)

The stricter of the two thresholds is used.

## 5. Versioning

`GateSpec` is frozen. Any change (a threshold, the LOSO method, perms, the seed,
the sector list) is a new version with a new sha256, never an edit of v1. The
prereg that cites a spec sha is bound to that exact spec; GD10a's reviewer
checks the cited sha against `GATE_SPEC_V1_SHA256`.
