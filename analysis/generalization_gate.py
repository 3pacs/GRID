"""Generalization gate (GD10b): when a sector finding may be called "general".

A pure, deterministic, versioned function of pre-registered, witnessed,
terminal per-sector results (plan 2026-09-27 section 2.5). Every report calls
:func:`evaluate_gate` before using the words "general" or "cross-sector". The
route a construct takes to get here is in ``docs/research/generalization-gate-v1.md``.

What it consumes, and what it never does
----------------------------------------
* Inputs are :class:`SectorResult` objects copied from **witnessed terminal**
  registry records: each carries the sha256 of its terminal record. The
  injected ``witness_check(kind, record_sha256, content_sha256)`` (tests stay
  offline) must confirm both that the record is witnessed and that it seals
  exactly that content, and must return the plain ``True``. ``kind`` is
  ``"terminal"`` (content :func:`terminal_content_sha256`: kind, p, IC series,
  contributions, coverage series; sealed at the holdout, before any forward
  verdict exists) or ``"forward"`` (content :func:`forward_content_sha256`:
  sector, prereg, terminal record, forward verdict). The verdict carries both
  content hashes of every input. An
  unwitnessed or altered input, a missing sector, or a mixed prereg/direction
  set is ``REFUSED``.
* The caller passes ``expected_spec_sha256`` from the construct's prereg; a
  spec that does not hash to it is ``REFUSED``, so a favourable variant spec
  cannot be picked after the fact. The verdict also carries the sha256 of this
  module's source, so an algorithm change cannot hide behind an unchanged spec.
* Every declared sector must be present and terminal. The gate never runs on a
  subset, so it cannot be used to look at some sectors' outcomes before the
  others are terminal.
* It never opens a holdout, never reads prices or labels and never recomputes a
  sector's IC. It uses the sealed per-date holdout IC series, the sealed
  per-entity IC contributions and the sealed holdout p-values only.
* ``promotion_allowed`` is always false. ``GENERAL_REVIEW_REQUIRED`` means "worth
  a forward-capital discussion with the owner", never "trade".

The five conditions (spec :data:`GATE_SPEC_V1`)
-----------------------------------------------
1. **Breadth.** Survivors are sectors whose witnessed holdout one-sided p is
   below ``holdout_alpha`` (0.10) in the pre-registered sign. The count must
   reach ``max(k_binomial, k_permutation)``:

   * ``k_binomial`` = 4 of 11 (chance P(X >= 4 | 11, 0.10) = 0.0185); in the
     pre-declared 10-sector branch (VS1 v8 ends in STOP, so Technology has no
     holdout) it is 4 of 10 (chance 0.0128). In both, it is the smallest k whose
     binomial chance is at most ``breadth_alpha`` (0.05).
   * ``k_permutation`` is the smallest k whose rate under the
     **sector-block permutation null** is at most ``breadth_alpha``. That null
     is a *joint* block sign-flip: blocks of consecutive decision dates on the
     common grid get one random sign, shared by every sector. It keeps each
     sector's autocorrelation (within blocks) and, unlike independent
     resampling, keeps the cross-sector dependence of the IC series (a shared
     factor), which is exactly what inflates the survivor count above the
     binomial chance. Each sector's survival under a null draw is judged
     against that sector's own null distribution, so each marginal survives at
     about ``holdout_alpha``. Each series is centred before flipping, so the
     null's cross-sector dependence is that of the residuals: real effects in
     several sectors do not masquerade as a shared factor (under the exact
     null the means are zero and centring changes little).

   Why not "shift each sector's IC series by a random circular offset" (the
   plan's first sketch): a circular shift leaves every sector's mean IC, hence
   its survival, unchanged, so that "null" count is degenerate (it always
   equals the observed count) and calibrates nothing. The joint sign-flip is a
   null that keeps both within-sector autocorrelation and cross-sector
   dependence; tests 4.1 show it controls the false-breadth rate where the
   binomial threshold does not. The null needs at least ``MIN_BLOCKS`` (8)
   sign blocks on the common grid, else the gate refuses.
2. **No dominance.**
   * Leave-one-sector-out: for every testable sector j, the equal-weight pooled
     IC of the other testable sectors keeps the pre-registered sign with
     two-sided p < ``loso_alpha`` (0.05). Method ``cluster_t`` (v1): per-date
     pooled ICs (which absorbs same-date cross-sector correlation), with a
     cluster-robust (CR1) t over blocks of consecutive dates, df = clusters - 1
     (the clusters are date blocks; pooling per date is what makes it robust to
     sector co-movement). Method ``sector_bootstrap`` (not used by v1): a
     percentile bootstrap over whole sectors; with about 10 clusters and no
     time dependence it is anti-conservative, so a later spec should not pick
     it without a calibration.
   * Within each surviving sector, the top entity's contribution in the
     pre-registered sign is below ``top_entity_max_share`` (25%) of the
     sector's IC sum. Contributions must add up to the sealed IC sum.
3. **Forward.** At least ``min_forward`` (2) surviving sectors have a witnessed
   forward verdict ``FORWARD_SUPPORTED_REVIEW_REQUIRED`` (S10 or the GD8 panel
   forward log).
4. **Coverage honesty.** On the sealed coverage-stable IC series (entity-dates
   with a stable channel-coverage set, GD5 guard), at least the required number
   of surviving sectors still survive (block sign-flip, same sign, p below
   ``holdout_alpha``). That p uses the spec's seed and perms, not the sector's
   sealed ones, so a sector near p = 0.10 can differ by Monte Carlo noise.

Verdict precedence: ``REFUSED`` > ``INSUFFICIENT_SECTORS`` (breadth or coverage)
> ``SECTOR_SPECIFIC`` (dominance) > ``FORWARD_PENDING`` >
``GENERAL_REVIEW_REQUIRED``. Every condition is still reported.

A spec change is a new :class:`GateSpec` version (and a new sha256), never an
edit of v1: ``GATE_SPEC_V1_SHA256`` is pinned by a test.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path

import numpy as np
from scipy.stats import binom
from scipy.stats import t as student_t

from analysis.offline_research_proof import MIN_BLOCKS
from analysis.panel_insider_density import block_signs, signflip_pvalues

#: sha256 of this module's source (LF-normalized), embedded in every verdict. Read at
#: import, so a .pyc-only deploy fails closed (ImportError) rather than mislabelling.
IMPLEMENTATION_SHA256 = hashlib.sha256(
    Path(__file__).read_bytes().replace(b"\r\n", b"\n")
).hexdigest()

GATE_VERSION = "generalization-gate-v1"
SUPPORTED = "FORWARD_SUPPORTED_REVIEW_REQUIRED"  # analysis.research_forward_log.SUPPORTED
TERMINAL_KINDS = ("holdout_result", "stage0_untestable", "stop")
VERDICTS = (
    "GENERAL_REVIEW_REQUIRED",
    "SECTOR_SPECIFIC",
    "INSUFFICIENT_SECTORS",
    "FORWARD_PENDING",
    "REFUSED",
)
LOSO_METHODS = ("cluster_t", "sector_bootstrap")
# Method choices are spec fields so the spec sha256 binds them, not only the numbers.
BREADTH_NULL = "joint_block_signflip_centred_union_grid_max_block"
LOSO_POOL = "all_testable_sectors_equal_weight_per_date"
FORWARD_SCOPE = "surviving_sectors_witnessed_forward_record"
COVERAGE_RULE = "survivors_resurvive_on_coverage_stable_series_count_ge_required"
_SHA = re.compile(r"^[0-9a-f]{64}$")

#: The 11 equity sectors of plan section 2.5 (VS1 v1 ``EQUITY_SECTORS``; a test pins it).
EQUITY_SECTORS: tuple[str, ...] = (
    "Technology",
    "Energy",
    "Financials",
    "Healthcare",
    "Industrials",
    "Consumer Discretionary",
    "Consumer Staples",
    "Real Estate",
    "Utilities",
    "Communication Services",
    "Materials",
)


@dataclass(frozen=True)
class GateSpec:
    """Frozen gate thresholds. A change is a new version, never an edit."""

    version: str = GATE_VERSION
    sectors: tuple[str, ...] = EQUITY_SECTORS
    stop_sector: str = "Technology"  # the only sector whose chain may end in STOP (VS1 v8)
    holdout_alpha: float = 0.10
    breadth_alpha: float = 0.05
    min_survivors: int = 4  # of 11; chance 0.0185
    branch_10_min_survivors: int = 4  # of 10 (v8 STOP); chance 0.0128
    breadth_null: str = BREADTH_NULL
    loso_method: str = "cluster_t"
    loso_pool: str = LOSO_POOL
    loso_alpha: float = 0.05
    top_entity_max_share: float = 0.25
    min_forward: int = 2
    forward_verdict: str = SUPPORTED
    forward_scope: str = FORWARD_SCOPE
    coverage_rule: str = COVERAGE_RULE
    min_grid_alignment: float = 0.80
    perms: int = 9999
    bootstrap: int = 9999
    seed: int = 20261001
    promotion_allowed: bool = False

    def validate(self) -> None:
        if (
            self.version != GATE_VERSION
            or len(set(self.sectors)) != len(self.sectors)
            or self.stop_sector not in self.sectors
            or not 0 < self.holdout_alpha < 1
            or not 0 < self.breadth_alpha < 1
            or not 0 < self.loso_alpha < 1
            or not 0 < self.top_entity_max_share < 1
            or self.loso_method not in LOSO_METHODS
            or self.breadth_null != BREADTH_NULL
            or self.loso_pool != LOSO_POOL
            or self.forward_scope != FORWARD_SCOPE
            or self.coverage_rule != COVERAGE_RULE
            or self.forward_verdict != SUPPORTED
            or self.min_forward < 1
            or not 0 < self.min_grid_alignment <= 1
            or self.perms < 199
            or self.bootstrap < 199
            or self.promotion_allowed is not False
        ):
            raise ValueError("invalid gate spec")
        for n, k in (
            (len(self.sectors), self.min_survivors),
            (len(self.sectors) - 1, self.branch_10_min_survivors),
        ):
            if not 1 <= k <= n or binomial_chance(n, k, self.holdout_alpha) > self.breadth_alpha:
                raise ValueError(
                    f"declared threshold {k} of {n} has binomial chance above breadth_alpha"
                )


def canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def spec_sha256(spec: GateSpec) -> str:
    return hashlib.sha256(canonical(asdict(spec))).hexdigest()


GATE_SPEC_V1 = GateSpec()
GATE_SPEC_V1_SHA256 = spec_sha256(GATE_SPEC_V1)


@dataclass(frozen=True)
class SectorResult:
    """One sector's sealed terminal result (copied from its witnessed registry record).

    ``ic_series``: the holdout per-date rank ICs as ``(iso_date, ic)`` pairs.
    ``entity_contributions``: ``(entity_id, c_e)`` with ``sum(c_e) == sum(ic)``
    (each date's IC split into its entities' centred-rank products).
    ``coverage_stable_ic_series``: the same IC restricted to coverage-stable
    entity-dates. ``block``: the permutation block the sector's holdout used.
    """

    sector: str
    terminal_record_sha256: str
    terminal_kind: str
    prereg_sha256: str
    direction: int
    holdout_p_one_sided: float | None = None
    block: int = 1
    ic_series: tuple[tuple[str, float], ...] = ()
    entity_contributions: tuple[tuple[str, float], ...] = ()
    coverage_stable_ic_series: tuple[tuple[str, float], ...] = ()
    forward_verdict: str | None = None
    forward_record_sha256: str | None = None

    @property
    def testable(self) -> bool:
        return self.terminal_kind == "holdout_result"


FORWARD_FIELDS = ("forward_verdict", "forward_record_sha256")


def terminal_content_sha256(result: SectorResult) -> str:
    """sha256 of the fields the terminal (holdout / Stage-0 / STOP) record seals."""
    fields = {k: v for k, v in asdict(result).items() if k not in FORWARD_FIELDS}
    return hashlib.sha256(canonical(fields)).hexdigest()


def forward_content_sha256(result: SectorResult) -> str:
    """sha256 of what the forward-log record seals: which sector, prereg and verdict."""
    fields = {
        "sector": result.sector,
        "prereg_sha256": result.prereg_sha256,
        "terminal_record_sha256": result.terminal_record_sha256,
        "forward_verdict": result.forward_verdict,
    }
    return hashlib.sha256(canonical(fields)).hexdigest()


@dataclass(frozen=True)
class GateVerdict:
    verdict: str
    reasons: tuple[str, ...]
    payload: dict = field(compare=False)

    def to_json(self) -> bytes:
        return canonical(self.payload)

    def sha256(self) -> str:
        return hashlib.sha256(self.to_json()).hexdigest()


# --- building blocks ----------------------------------------------------------------


def binomial_chance(n: int, k: int, alpha: float) -> float:
    """P(X >= k) for X ~ Binomial(n, alpha): the independence chance level."""
    return float(binom.sf(k - 1, n, alpha))


def _values(series: Sequence[tuple[str, float]]) -> np.ndarray:
    return np.asarray([v for _d, v in series], dtype=float)


def _r(x: float | None, nd: int = 12) -> float | None:
    if x is None or not math.isfinite(x):
        return None
    return round(float(x), nd)


def survives(result: SectorResult, alpha: float) -> bool:
    """Witnessed holdout one-sided p (pre-registered sign) below ``alpha``."""
    return (
        result.testable
        and result.holdout_p_one_sided is not None
        and result.holdout_p_one_sided < alpha
        and float(np.mean(_values(result.ic_series))) * result.direction > 0
    )


def breadth_count(results: Iterable[SectorResult], alpha: float) -> int:
    return sum(1 for r in results if survives(r, alpha))


def _grid(results: Sequence[SectorResult]) -> tuple[list[str], float]:
    """Union decision grid of the testable sectors and its alignment |inter| / |union|."""
    sets = [{d for d, _v in r.ic_series} for r in results if r.testable]
    if not sets:
        return [], 1.0
    union = set().union(*sets)
    inter = set.intersection(*sets)
    return sorted(union), len(inter) / len(union)


def joint_null_means(
    results: Sequence[SectorResult], perms: int, seed: int
) -> tuple[list[SectorResult], np.ndarray]:
    """perms x sectors null mean ICs under one joint block sign-flip on the union grid."""
    testable = [r for r in results if r.testable]
    grid, _alignment = _grid(testable)
    if not testable:
        return [], np.zeros((perms, 0))
    position = {d: i for i, d in enumerate(grid)}
    block = max(r.block for r in testable)
    signs = block_signs(len(grid), block, perms, seed)
    out = np.empty((perms, len(testable)))
    for j, r in enumerate(testable):
        idx = np.asarray([position[d] for d, _v in r.ic_series])
        values = _values(r.ic_series)
        # centred: the null's cross-sector dependence is the residuals' (a real
        # effect in several sectors must not read as a shared factor)
        out[:, j] = signs[:, idx] @ (values - values.mean()) / len(idx)
    return testable, out


def sector_block_permutation_null(
    results: Sequence[SectorResult], perms: int, seed: int, *, alpha: float = 0.10
) -> np.ndarray:
    """Survivor counts under the joint (sector-aligned) block sign-flip null.

    For each null draw, a sector survives when its null mean IC lies in the top
    ``alpha`` of its own null distribution in its pre-registered direction.
    Untestable sectors never survive. Returns ``perms`` integer counts.
    """
    testable, means = joint_null_means(results, perms, seed)
    counts = np.zeros(perms, dtype=int)
    for j, r in enumerate(testable):
        x = r.direction * means[:, j]
        ordered = np.sort(x)
        # one-sided p of each draw within its own null: share of draws >= it
        ge = perms - np.searchsorted(ordered, x - 1e-12, side="left")
        counts += (ge / perms < alpha).astype(int)
    return counts


def permutation_threshold(counts: np.ndarray, n_sectors: int, level: float) -> tuple[int, dict]:
    """Smallest k with (1 + #{count >= k}) / (perms + 1) <= level, and the tail rates."""
    perms = len(counts)
    rates = {k: (1 + int((counts >= k).sum())) / (perms + 1) for k in range(1, n_sectors + 1)}
    k = next((k for k in range(1, n_sectors + 1) if rates[k] <= level), n_sectors + 1)
    return k, rates


def _cluster_t(series: np.ndarray, block: int) -> tuple[float, float]:
    """Mean and two-sided p of a per-date series, CR1 clusters of ``block`` dates."""
    n = len(series)
    groups = np.arange(n) // max(block, 1)
    g = int(groups.max()) + 1 if n else 0
    if g < 2:
        return float(np.mean(series)) if n else float("nan"), 1.0
    mean = float(series.mean())
    sums = np.bincount(groups, weights=series - mean)
    var = (g / (g - 1)) * float((sums**2).sum()) / n**2
    if var <= 0:
        return mean, 0.0 if mean != 0 else 1.0
    t = mean / math.sqrt(var)
    return mean, float(2 * student_t.sf(abs(t), g - 1))


def _pooled_series(results: Sequence[SectorResult]) -> np.ndarray:
    """Equal-weight per-date mean IC over the given sectors (dates with any present)."""
    grid, _ = _grid(results)
    position = {d: i for i, d in enumerate(grid)}
    total = np.zeros(len(grid))
    count = np.zeros(len(grid))
    for r in results:
        idx = np.asarray([position[d] for d, _v in r.ic_series])
        total[idx] += _values(r.ic_series)
        count[idx] += 1
    keep = count > 0
    return total[keep] / count[keep]


def _sector_bootstrap(results: Sequence[SectorResult], draws: int, seed: int) -> tuple[float, float]:
    means = np.asarray([float(np.mean(_values(r.ic_series))) for r in results])
    point = float(means.mean())
    rng = np.random.default_rng([seed, len(means), draws, 3])
    boot = means[rng.integers(0, len(means), size=(draws, len(means)))].mean(axis=1)
    below = (1 + int((boot <= 0).sum())) / (draws + 1)
    above = (1 + int((boot >= 0).sum())) / (draws + 1)
    return point, min(1.0, 2 * min(below, above))


def loso_pooled(
    results: Sequence[SectorResult], method: str, *, alpha: float, draws: int, seed: int
) -> dict:
    """Leave-one-sector-out pooled IC over the testable sectors."""
    testable = [r for r in results if r.testable]
    if len(testable) < 3:
        return {"passed": False, "reason": "fewer than 3 testable sectors", "left_out": {}}
    direction = testable[0].direction
    left_out = {}
    for j in testable:
        rest = [r for r in testable if r is not j]
        if method == "cluster_t":
            mean, p = _cluster_t(_pooled_series(rest), max(r.block for r in rest))
        elif method == "sector_bootstrap":
            mean, p = _sector_bootstrap(rest, draws, seed)
        else:
            raise ValueError(f"unknown LOSO method {method!r}")
        ok = math.isfinite(mean) and mean * direction > 0 and p < alpha
        left_out[j.sector] = {"pooled_ic": _r(mean), "p_two_sided": _r(p), "passed": bool(ok)}
    failed = sorted(k for k, v in left_out.items() if not v["passed"])
    return {
        "method": method,
        "passed": not failed,
        "failed_when_leaving_out": failed,
        "left_out": left_out,
    }


def top_entity_share(result: SectorResult) -> float:
    """Top entity's contribution (pre-registered sign) over the sector's IC sum."""
    c = np.asarray([v for _e, v in result.entity_contributions], dtype=float)
    total = float(result.direction * c.sum())
    if not len(c) or total <= 0:
        return float("inf")
    return float((result.direction * c).max() / total)


def forward_confirmations(results: Iterable[SectorResult], verdict: str, alpha: float) -> list[str]:
    """Surviving sectors with a forward verdict equal to ``verdict``."""
    return sorted(r.sector for r in results if survives(r, alpha) and r.forward_verdict == verdict)


def coverage_honesty(results: Sequence[SectorResult], *, alpha: float, perms: int, seed: int) -> dict:
    """Survivors that still survive on their coverage-stable IC series."""
    out = {}
    for r in results:
        if not survives(r, alpha):
            continue
        ic = _values(r.coverage_stable_ic_series)
        if len(ic) < 2:
            out[r.sector] = {"p_one_sided": None, "passed": False}
            continue
        mean, _two, one = signflip_pvalues(ic, max(r.block, 1), perms, seed, r.direction)
        out[r.sector] = {
            "mean_ic": _r(mean),
            "p_one_sided": _r(one),
            "passed": bool(mean * r.direction > 0 and one < alpha),
        }
    return out


# --- input validation ---------------------------------------------------------------


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _iso_dates_ok(series) -> bool:
    try:
        for d, _v in series:
            if not isinstance(d, str):
                return False
            if date.fromisoformat(d).isoformat() != d:
                return False
    except (TypeError, ValueError):
        return False
    return True


def _check_inputs(
    results: Sequence[SectorResult],
    spec: GateSpec,
    witness_check: Callable[[str, str, str], bool],
) -> list[str]:
    reasons = []
    if any(not isinstance(r, SectorResult) or not isinstance(r.sector, str) for r in results):
        return ["every input must be a SectorResult with a sector name"]
    sectors = [r.sector for r in results]
    if len(set(sectors)) != len(sectors):
        reasons.append("duplicate sector results")
    missing = sorted(set(spec.sectors) - set(sectors))
    extra = sorted(set(sectors) - set(spec.sectors))
    if missing:
        reasons.append(f"missing terminal results for {missing}: the gate runs once, on all sectors")
    if extra:
        reasons.append(f"undeclared sectors {extra}")
    if len({r.prereg_sha256 for r in results}) > 1:
        reasons.append("results come from different preregistrations")
    if len({r.direction for r in results}) > 1 or any(
        not _is_int(r.direction) or r.direction not in (-1, 1) for r in results
    ):
        reasons.append("pre-registered direction must be one sign, +1 or -1, for every sector")
    for r in results:
        tag = r.sector
        try:
            contents = {"terminal": terminal_content_sha256(r), "forward": forward_content_sha256(r)}
        except (TypeError, ValueError):
            reasons.append(f"{tag}: sealed fields are not canonical JSON (non-finite or wrong type)")
            continue
        if r.terminal_kind not in TERMINAL_KINDS:
            reasons.append(f"{tag}: unknown terminal kind {r.terminal_kind!r}")
        if r.terminal_kind == "stop" and r.sector != spec.stop_sector:
            reasons.append(f"{tag}: only {spec.stop_sector} may end in STOP")
        if not isinstance(r.prereg_sha256, str) or not _SHA.match(r.prereg_sha256):
            reasons.append(f"{tag}: prereg sha256 missing or malformed")
        for kind, sha in (("terminal", r.terminal_record_sha256), ("forward", r.forward_record_sha256)):
            if kind == "forward" and sha is None and r.forward_verdict is None:
                continue
            if not isinstance(sha, str) or not _SHA.match(sha):
                reasons.append(f"{tag}: {kind} record sha256 missing or malformed")
                continue
            try:
                ok = witness_check(kind, sha, contents[kind]) is True
            except Exception as exc:  # noqa: BLE001 - a failing witness is a refusal, never a pass
                ok = False
                reasons.append(f"{tag}: witness_check raised {type(exc).__name__}")
            if not ok:
                reasons.append(
                    f"{tag}: {kind} record {sha[:12]} is not witnessed with this content"
                )
        if r.testable:
            if not _iso_dates_ok(r.ic_series) or not _iso_dates_ok(r.coverage_stable_ic_series):
                reasons.append(f"{tag}: IC dates must be ISO date strings")
                continue
            ic = _values(r.ic_series)
            dates = [d for d, _v in r.ic_series]
            if not len(ic) or not np.all(np.isfinite(ic)):
                reasons.append(f"{tag}: sealed IC series empty or non-finite")
            if len(set(dates)) != len(dates):
                reasons.append(f"{tag}: duplicate IC dates")
            cs = _values(r.coverage_stable_ic_series)
            if len(cs) and not np.all(np.isfinite(cs)):
                reasons.append(f"{tag}: coverage-stable IC series non-finite")
            p = r.holdout_p_one_sided
            if p is None or not (0 < p <= 1):
                reasons.append(f"{tag}: holdout one-sided p missing or outside (0, 1]")
            if not _is_int(r.block) or r.block < 1:
                reasons.append(f"{tag}: block must be a positive int")
            c = np.asarray([v for _e, v in r.entity_contributions], dtype=float)
            if not len(c) or not np.all(np.isfinite(c)):
                reasons.append(f"{tag}: entity contributions missing or non-finite")
            elif len(ic) and abs(float(c.sum()) - float(ic.sum())) > 1e-6 * max(1.0, abs(float(ic.sum()))):
                reasons.append(f"{tag}: entity contributions do not add up to the sealed IC sum")
        elif r.ic_series or r.holdout_p_one_sided is not None:
            reasons.append(f"{tag}: a {r.terminal_kind} record cannot carry holdout statistics")
        if r.forward_verdict is not None and r.forward_record_sha256 is None:
            reasons.append(f"{tag}: forward verdict without a forward record sha256")
    if not reasons:
        grid, alignment = _grid(results)
        if alignment < spec.min_grid_alignment:
            reasons.append(
                f"sector IC series share {alignment:.2f} of their decision dates (< "
                f"{spec.min_grid_alignment}): the joint null needs a common grid"
            )
        blocks = [r.block for r in results if r.testable]
        if blocks and len(grid) // max(blocks) < MIN_BLOCKS:
            reasons.append(
                f"{len(grid)} decision dates in blocks of {max(blocks)} give fewer than "
                f"{MIN_BLOCKS} sign blocks: the joint null would be degenerate"
            )
    return reasons


# --- the gate -----------------------------------------------------------------------


def evaluate_gate(
    results: Sequence[SectorResult],
    spec: GateSpec = GATE_SPEC_V1,
    *,
    witness_check: Callable[[str, str, str], bool],
    expected_spec_sha256: str,
) -> GateVerdict:
    """The generalization verdict over every declared sector's terminal result.

    ``witness_check(kind, record_sha256, content_sha256)`` must return the
    plain ``True`` only when that record is witnessed and seals that content
    (``kind`` is ``"terminal"`` or ``"forward"``). ``expected_spec_sha256``
    comes from the construct's prereg; any other spec is refused.
    """
    spec.validate()
    results = sorted(results, key=lambda r: str(getattr(r, "sector", "")))
    sha = spec_sha256(spec)

    def _content(r, fn) -> str | None:
        try:
            return fn(r)
        except (TypeError, ValueError, AttributeError):
            return None

    payload: dict = {
        "gate_version": GATE_VERSION,
        "implementation_sha256": IMPLEMENTATION_SHA256,
        "spec": asdict(spec),
        "spec_sha256": sha,
        "expected_spec_sha256": expected_spec_sha256,
        "promotion_allowed": False,
        "inputs": [
            {
                "sector": getattr(r, "sector", None),
                "terminal_kind": getattr(r, "terminal_kind", None),
                "terminal_record_sha256": getattr(r, "terminal_record_sha256", None),
                "forward_record_sha256": getattr(r, "forward_record_sha256", None),
                "prereg_sha256": getattr(r, "prereg_sha256", None),
                "terminal_content_sha256": _content(r, terminal_content_sha256),
                "forward_content_sha256": (
                    _content(r, forward_content_sha256)
                    if getattr(r, "forward_verdict", None) is not None
                    else None
                ),
            }
            for r in results
        ],
    }
    refusals = []
    if expected_spec_sha256 != sha:
        refusals.append(
            f"spec sha256 {sha[:12]} is not the prereg's {str(expected_spec_sha256)[:12]}"
        )
    refusals += _check_inputs(results, spec, witness_check)
    if refusals:
        payload.update(verdict="REFUSED", reasons=refusals)
        return GateVerdict("REFUSED", tuple(refusals), payload)

    stop = any(r.terminal_kind == "stop" for r in results)
    counted = [r for r in results if r.terminal_kind != "stop"]
    n = len(counted)
    k_binomial = spec.branch_10_min_survivors if stop else spec.min_survivors
    counts = sector_block_permutation_null(counted, spec.perms, spec.seed, alpha=spec.holdout_alpha)
    k_perm, rates = permutation_threshold(counts, n, spec.breadth_alpha)
    required = max(k_binomial, k_perm)
    survivors = sorted(r.sector for r in counted if survives(r, spec.holdout_alpha))
    observed = breadth_count(counted, spec.holdout_alpha)
    _grid_dates, alignment = _grid(counted)
    breadth = {
        "branch": "10-sector (v8 STOP)" if stop else "11-sector",
        "n_sectors": n,
        "survivors": survivors,
        "count": observed,
        "required_binomial": k_binomial,
        "binomial_chance": _r(binomial_chance(n, k_binomial, spec.holdout_alpha)),
        "required_permutation": k_perm,
        "permutation_tail_rates": {str(k): _r(v) for k, v in rates.items()},
        "permutation_p_observed": _r(rates.get(observed, 1.0) if observed else 1.0),
        "binomial_p_observed": _r(binomial_chance(n, observed, spec.holdout_alpha) if observed else 1.0),
        "required": required,
        "grid_alignment": _r(alignment),
        "untestable": sorted(r.sector for r in counted if not r.testable),
        "passed": observed >= required,
    }
    loso = loso_pooled(counted, spec.loso_method, alpha=spec.loso_alpha, draws=spec.bootstrap, seed=spec.seed)
    shares = {
        r.sector: _r(top_entity_share(r))
        for r in counted
        if r.sector in survivors
    }
    top_ok = all(s is not None and s < spec.top_entity_max_share for s in shares.values())
    dominance = {"loso": loso, "top_entity_share": shares, "top_entity_passed": top_ok,
                 "passed": bool(loso["passed"] and top_ok)}
    confirmations = forward_confirmations(counted, spec.forward_verdict, spec.holdout_alpha)
    forward = {"supported": confirmations, "required": spec.min_forward,
               "passed": len(confirmations) >= spec.min_forward}
    cov = coverage_honesty(counted, alpha=spec.holdout_alpha, perms=spec.perms, seed=spec.seed)
    cov_survivors = sorted(k for k, v in cov.items() if v["passed"])
    coverage = {"sectors": cov, "survivors": cov_survivors, "required": required,
                "passed": len(cov_survivors) >= required}

    reasons = []
    if not breadth["passed"]:
        reasons.append(f"breadth: {observed} survivors < required {required}")
    if not coverage["passed"]:
        reasons.append(f"coverage: {len(cov_survivors)} coverage-stable survivors < required {required}")
    if not loso["passed"]:
        reasons.append(f"dominance: LOSO fails leaving out {loso.get('failed_when_leaving_out')}")
    if not top_ok:
        reasons.append("dominance: a surviving sector's top entity carries >= "
                       f"{spec.top_entity_max_share:.0%} of its IC sum")
    if not forward["passed"]:
        reasons.append(f"forward: {len(confirmations)} supported sectors < {spec.min_forward}")
    if not breadth["passed"] or not coverage["passed"]:
        verdict = "INSUFFICIENT_SECTORS"
    elif not dominance["passed"]:
        verdict = "SECTOR_SPECIFIC"
    elif not forward["passed"]:
        verdict = "FORWARD_PENDING"
    else:
        verdict = "GENERAL_REVIEW_REQUIRED"
        reasons.append("all conditions pass: owner review required; promotion_allowed=false")
    payload.update(
        verdict=verdict,
        reasons=reasons,
        breadth=breadth,
        dominance=dominance,
        forward=forward,
        coverage=coverage,
    )
    return GateVerdict(verdict, tuple(reasons), payload)
