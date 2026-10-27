# P2-A offline reference and identical-input packets

Tests/scripts only. No runtime changes or canonical-engine choice. This is
arithmetic evidence on synthetic packets, not a real-market reconciliation,
vendor validation, observed dealer position or directional edge.

```text
python -m pytest tests/test_gex_p2a.py -q --noconftest
python -m scripts.gex_p2a.harness tests/fixtures/gex_p2a/identical.json --output <new-local-directory>
```

The CLI reads one named local packet and creates a new directory containing
the exact input bytes and JSON result. It refuses an existing output directory.
Do not commit real market snapshots. No HTTP, SSH, broker, SQLite or PostgreSQL
access; no server import, timer, deployment or change to frozen GEX v1 inputs.
Coordinator reviews/merges only. September 29 13:30Z freeze remains until the
owner confirms GEM containment.

## What is compared

The independent Decimal reference imports neither engine. It recomputes gamma
at 80 and 110 decimal digits. One normalized packet, stable row ordering and
parameter hash is passed to GRID's actual shared gamma primitive and the actual
Gamma Watch d1/gamma AST extracted from `broker.py:curves`. Collector module
code is never imported/executed. A disclosed test-only AST transformation turns
hard-coded r/q constants into packet parameters; no production edit occurs.
The selected AST fingerprint is pinned and source drift fails closed for review.

This tests the arithmetic kernels, not native loaders or the entire exposure
pipeline. Native GRID dealer loader rejects 0DTE, uses prior close and q=0;
this harness does not claim that behavior has changed. Native Gamma Watch IV
recovery, shocks, universe gates and live-clock behavior are bypassed explicitly.
GRID primitive's T floor is NOT_SUPPORTED below the floor, not numeric failure.
Both kernel results use the same explicit signed OI/multiplier/spot²/1% scaling;
native engine aggregate reductions, walls and root searches remain future tests.

Packet fields include explicit expiry instants and calendar version, unadjusted
spot grid, fixed valuation time, direct IV, OI/date-or-null, multiplier,
deliverable and source. RTD availability accepts only grid-svr pull receipt,
never the unmonitored ANIK clock. This supplied timestamp is a fixture assertion;
the offline harness cannot authenticate a real server receipt. Expired rows are
excluded with reasons; missing/bad input rejects the packet. OI=0 is retained.

## Error propagation, rather than asserted tolerances

No fixed dollar, relative or gamma pass epsilon is fitted to results. Inputs
are exact represented binary64 values; decimal-to-binary ingestion differences
are outside this arithmetic test. Each primitive operation propagates the full
input interval and adds its own rounding allowance. With unit roundoff
u=2^-53, basic operations add `u/(1-u) * max(abs(endpoints))` plus one smallest
subnormal. Exp/log/sqrt assume an error of at most two ulp, bounded by
`4u/(1-4u) * max(abs(endpoints))` plus one smallest subnormal. Pi representation
error and a guard for high-precision Decimal working arithmetic are included.
Interval division through zero fails. This naturally widens the bound for
ill-conditioned cancellation, short expiry and nonlinear exponentiation.

The bound is propagated through the actual extracted Gamma Watch expression
and a separate GRID primitive operation tree. The 80/110-digit convergence
difference is added explicitly; formula disagreement never enlarges tolerance.
Contract exposure multiplication propagates its own error. For n signed terms,
sequential reduction adds gamma_(n-1) times the sum of absolute perturbed terms,
where gamma_k=ku/(1-ku). Therefore offsetting terms cannot create a spuriously
tiny relative tolerance by making the net exposure nearly zero. A zero-spanning
error interval is INDETERMINATE sign.

**Conditional engineering bound, not a formal interval proof:** Python/NumPy
do not universally guarantee <=2 ulp for every libm/platform. The result records
this assumption and Python/NumPy/platform versions. Unsupported overflow or
singular intervals reject the run. Independent delta finite-difference
convergence, cancellation fixtures and fault injection test sensitivity; they
do not certify libm globally. This is numerical error only, not quote, IV, OI,
clock, vendor-methodology or model uncertainty.

## Evidence and remaining boundaries

Results retain raw/normalized/code hashes, full supplied metadata, exclusions,
per-contract errors/bounds, normalized aggregate errors, gross exposure,
reduction bound and statuses. Deterministic replay excludes wall-clock run time.
No in-place overwrite; filesystem-level immutability is not claimed.
Vendor reproduction is NOT_COMPARABLE until identical vendor inputs exist.
Full production pipeline adapters, root/wall tests, real packet acquisition,
independent review and PostgreSQL tests remain outside this P2-A kernel slice.
No canonical choice or activation follows a PASS_NUMERICAL result.
