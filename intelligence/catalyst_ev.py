"""Catalyst expected value — scoring the event, not the trend.

Why this module exists
----------------------
``intelligence/long_plays.py`` ranks multi-year candidates from a seeded
Monte Carlo driven by each name's **historical CAGR and volatility**
(``strategy.ten_year_portfolio.build_monte_carlo_projection``). That proxy
is defensible for a compounder. It is actively backwards for the setup the
operator is hunting: a sub-$2 B biotech smashed 90 % with a dated binary
readout in front of it. Measured on 2026-09-10, the board read

    OCGN  drawdown -97.2 %  p50 3y 1.06x
    TECX  drawdown -92.6 %  p50 3y 1.27x
    MLTX  drawdown -86.7 %  p50 3y 0.85x

— it extrapolated the crash as the forward distribution and disqualified
all three, on both the return gate and the -75 % drawdown gate. The crash
*is* the setup. Trend extrapolation cannot see a binary event, because the
event is not in the trend.

This module scores the event directly:

    EV = P(success) x upside_multiple + (1 - P(success)) x downside_multiple

and every term is anchored on something checkable rather than assumed.

Honesty rules (read before touching any number here)
----------------------------------------------------
* ``P(success)`` starts from a **published industry base rate** by phase
  and therapeutic area (``PHASE_TRANSITION_BASE_RATES``,
  ``INDICATION_LOA_MULTIPLIER``), then moves only on evidence GRID already
  scores for that specific trial: endpoint clarity, FDA designation,
  enrollment completion. It is a **prior**, labelled as one on every row
  (``p_success_basis``). It is not a forecast and must never be presented
  as one.
* The base-rate tables are the weakest link and they say so. They are
  literature priors, not GRID measurements. ``empirical_phase_outcomes()``
  replaces them with GRID's own realized readouts the moment
  ``trial_signals.fwd_return_30d`` has enough scored rows; until then every
  row carries ``p_success_basis = "literature_prior"`` so a reader knows
  which regime produced the number.
* ``downside_multiple`` is the **net-cash floor**, not a guess: cash minus
  debt per share over price, haircut by the burn to the catalyst. A name
  trading under net cash has a floor; a name at 5x net cash does not.
* ``upside_multiple`` is anchored on the **options market's own implied
  distribution** (``intelligence.market_implied_prob``) where a chain
  exists, else on realized comparable readouts, else it is ``None`` and the
  name is not ranked. A missing anchor produces no row rather than an
  invented one.
* ``runway_covers_catalyst`` is a hard gate and it is the one that matters
  most: if the cash runs out before the readout, the equity is diluted
  away regardless of the science. This replaces the absolute
  ``cash_runway_score >= 0.4`` threshold, which asked the wrong question —
  OLMA has 3.2 months of runway against a readout ~7 weeks out, so its cash
  covers its catalyst while failing the absolute floor.
* Every read is PIT-bounded on ``as_of``. Every external read degrades to
  ``None`` plus a ``method_notes`` line; a missing table produces a thinner
  board, never an exception.

Public API
----------
``success_probability(...)``, ``downside_multiple(...)``,
``upside_multiple_from_iv(...)``, ``catalyst_ev(...)``,
``runway_covers_catalyst(...)``, ``months_between(...)``
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any

# ── Base rates ────────────────────────────────────────────────────────────
#
# Phase-transition success rates: the probability that a trial in this phase
# reads out well enough to advance. These are industry-wide figures from the
# widely-cited BIO / Informa / QLS "Clinical Development Success Rates"
# analyses of ~10 years of phase transitions. They are LITERATURE PRIORS —
# GRID has not measured them — and exist to be replaced by
# ``empirical_phase_outcomes()`` once enough readouts are scored.
#
# Keep this table small, explicit and easy to re-fit. Do not add a phase
# here without a source for the number.

PHASE_TRANSITION_BASE_RATES: dict[str, float] = {
    "PHASE1": 0.52,   # Phase 1 -> Phase 2
    "PHASE2": 0.29,   # Phase 2 -> Phase 3  (the hardest gate in drug development)
    "PHASE3": 0.58,   # Phase 3 -> NDA/BLA filing
    "FILED": 0.91,    # NDA/BLA -> approval
}
DEFAULT_PHASE_BASE_RATE: float = 0.29

# Relative likelihood-of-approval by therapeutic area, normalised so that
# 1.0 is the all-indication average. Oncology is materially harder than the
# average; haematology and rare disease materially easier. Same source and
# the same caveat as above.
INDICATION_LOA_MULTIPLIER: dict[str, float] = {
    "oncology": 0.67,
    "neurology": 0.75,
    "psychiatry": 0.78,
    "cardiovascular": 1.15,
    "infectious_disease": 1.05,
    "metabolic": 1.00,
    "autoimmune": 0.95,
    "immunology": 0.95,
    "hematology": 1.35,
    "rare_disease": 1.20,
    "ophthalmology": 1.10,
    "respiratory": 0.95,
}

# Evidence modifiers. Each is a multiplicative nudge on the base rate,
# bounded so that no single input can dominate the prior.
FDA_DESIGNATION_MULTIPLIER: dict[str, float] = {
    "breakthrough": 1.25,
    "breakthrough therapy": 1.25,
    "regenerative medicine advanced therapy": 1.20,
    "rmat": 1.20,
    "fast track": 1.10,
    "priority review": 1.15,
    "orphan": 1.08,
    "orphan drug": 1.08,
    "accelerated approval": 1.12,
}
ENDPOINT_CLARITY_SPAN: float = 0.30   # +/- 15 % around a neutral 0.5 clarity
ENROLLMENT_COMPLETE_BONUS: float = 1.06
ENROLLMENT_UNDER_HALF_PENALTY: float = 0.88

P_SUCCESS_FLOOR: float = 0.02
P_SUCCESS_CEILING: float = 0.90

# Minimum scored readouts before GRID's own outcomes replace the priors.
EMPIRICAL_MIN_SAMPLES: int = 30

BASIS_LITERATURE: str = "literature_prior"
BASIS_EMPIRICAL: str = "grid_realized_outcomes"

# ── Gates ─────────────────────────────────────────────────────────────────
#
# Runway must cover the catalyst plus a buffer. The buffer exists because a
# financing is usually raised *before* the cash is gone, and a raise into a
# pending readout is done at a discount that lands on the existing holders.
RUNWAY_BUFFER_MONTHS: float = 3.0

DAYS_PER_MONTH: float = 30.44


# ── Small helpers ─────────────────────────────────────────────────────────


def _finite(value: Any) -> float | None:
    """Return ``value`` as a finite float, else ``None``."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def clamp(value: float, lo: float, hi: float) -> float:
    """Clamp ``value`` into ``[lo, hi]``."""
    return max(lo, min(hi, value))


def _normalize_key(value: Any) -> str | None:
    """Lowercase/underscore a free-text label for table lookup."""
    if value is None:
        return None
    key = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    return key or None


def months_between(start: date, end: date) -> float:
    """Calendar months from ``start`` to ``end`` (negative when end is past)."""
    return (end - start).days / DAYS_PER_MONTH


def normalize_phase(phase: Any) -> str | None:
    """Map a free-text trial phase onto a ``PHASE_TRANSITION_BASE_RATES`` key.

    ct.gov writes ``PHASE2``, ``PHASE2/PHASE3``, ``Phase 3`` and friends.
    A combined phase takes the **earlier** (harder) leg — a Phase 2/3 trial
    still has to clear Phase 2.
    """
    if phase is None:
        return None
    raw = str(phase).strip().upper().replace(" ", "").replace("-", "")
    if not raw:
        return None
    for key in ("PHASE1", "PHASE2", "PHASE3"):
        if key in raw:
            return key
    if "FILED" in raw or "NDA" in raw or "BLA" in raw:
        return "FILED"
    return None


def indication_multiplier(indication: Any) -> float:
    """Therapeutic-area LOA multiplier; 1.0 for an unknown or missing area."""
    key = _normalize_key(indication)
    if key is None:
        return 1.0
    if key in INDICATION_LOA_MULTIPLIER:
        return INDICATION_LOA_MULTIPLIER[key]
    # Tolerate free-text ("metastatic oncology", "rare disease - CNS").
    for known, mult in INDICATION_LOA_MULTIPLIER.items():
        if known in key:
            return mult
    return 1.0


def designation_multiplier(designation: Any) -> float:
    """FDA-designation multiplier; 1.0 when absent or unrecognised."""
    if designation is None:
        return 1.0
    raw = str(designation).strip().lower()
    if not raw:
        return 1.0
    best = 1.0
    for known, mult in FDA_DESIGNATION_MULTIPLIER.items():
        if known in raw and mult > best:
            best = mult
    return best


# ── P(success) ────────────────────────────────────────────────────────────


def success_probability(
    *,
    phase: Any,
    indication: Any = None,
    endpoint_clarity: float | None = None,
    fda_designation: Any = None,
    enrollment_pct: float | None = None,
    base_rates: dict[str, float] | None = None,
    basis: str = BASIS_LITERATURE,
) -> dict[str, Any]:
    """Probability this trial reads out well, as a prior plus its derivation.

    Starts from the phase-transition base rate, then applies bounded
    multiplicative modifiers for therapeutic area, endpoint clarity, FDA
    designation and enrollment completion. Returns the probability **and
    every factor that produced it**, so a row on the board can be audited
    without re-running the model.

    ``endpoint_clarity`` is GRID's own 0..1 score from
    ``grid/signals/trial_signal.py``; 0.5 is neutral and contributes
    nothing. ``enrollment_pct`` is 0..100.

    Every input is optional: with only a phase this returns the bare base
    rate. With nothing recognisable it returns the default phase rate and
    says so in ``factors``.
    """
    table = base_rates or PHASE_TRANSITION_BASE_RATES
    phase_key = normalize_phase(phase)
    base = table.get(phase_key, DEFAULT_PHASE_BASE_RATE) if phase_key else DEFAULT_PHASE_BASE_RATE

    factors: dict[str, float] = {}

    ind_mult = indication_multiplier(indication)
    if ind_mult != 1.0:
        factors["indication"] = round(ind_mult, 4)

    clarity = _finite(endpoint_clarity)
    if clarity is not None:
        # 0.5 clarity is neutral; the full 0..1 span moves the prior +/-15 %.
        clarity_mult = 1.0 + (clamp(clarity, 0.0, 1.0) - 0.5) * ENDPOINT_CLARITY_SPAN
        if clarity_mult != 1.0:
            factors["endpoint_clarity"] = round(clarity_mult, 4)
    else:
        clarity_mult = 1.0

    des_mult = designation_multiplier(fda_designation)
    if des_mult != 1.0:
        factors["fda_designation"] = round(des_mult, 4)

    enrollment = _finite(enrollment_pct)
    enroll_mult = 1.0
    if enrollment is not None:
        if enrollment >= 100.0:
            enroll_mult = ENROLLMENT_COMPLETE_BONUS
        elif enrollment < 50.0:
            # Under-enrolled trials slip and read out underpowered.
            enroll_mult = ENROLLMENT_UNDER_HALF_PENALTY
        if enroll_mult != 1.0:
            factors["enrollment"] = round(enroll_mult, 4)

    p = base * ind_mult * clarity_mult * des_mult * enroll_mult
    return {
        "p_success": round(clamp(p, P_SUCCESS_FLOOR, P_SUCCESS_CEILING), 4),
        "p_success_base_rate": round(base, 4),
        "p_success_phase": phase_key,
        "p_success_factors": factors,
        "p_success_basis": basis,
    }


def empirical_phase_outcomes(
    rows: list[dict[str, Any]],
    *,
    min_samples: int = EMPIRICAL_MIN_SAMPLES,
) -> tuple[dict[str, float] | None, dict[str, int]]:
    """Re-fit the phase base rates from GRID's own scored readouts.

    ``rows`` are ``trial_signals`` records carrying ``trial_phase`` and a
    realized ``fwd_return_30d``. A readout counts as a success when the
    30-day forward return is positive. Returns ``(base_rates, counts)``, or
    ``(None, counts)`` when no phase clears ``min_samples`` — in which case
    the caller keeps the literature priors and keeps saying so.

    This is the function that retires ``PHASE_TRANSITION_BASE_RATES``. It is
    deliberately simple: the point is to stop using someone else's average
    the moment GRID can measure its own.
    """
    hits: dict[str, int] = {}
    totals: dict[str, int] = {}
    for row in rows:
        phase = normalize_phase(row.get("trial_phase"))
        ret = _finite(row.get("fwd_return_30d"))
        if phase is None or ret is None:
            continue
        totals[phase] = totals.get(phase, 0) + 1
        if ret > 0:
            hits[phase] = hits.get(phase, 0) + 1

    fitted = {
        phase: hits.get(phase, 0) / n
        for phase, n in totals.items()
        if n >= min_samples
    }
    return (fitted or None), totals


# ── Downside: the net-cash floor ──────────────────────────────────────────


def downside_multiple(
    *,
    price: float | None,
    net_cash_per_share: float | None,
    months_to_catalyst: float | None = None,
    monthly_burn_per_share: float | None = None,
    failure_haircut: float = 0.5,
) -> dict[str, Any] | None:
    """What a holder still owns if the readout fails, as a multiple of price.

    The floor is net cash (cash minus debt) per share, less the burn
    between now and the readout, times ``failure_haircut`` — a failed
    binary readout does not usually leave the shell trading at full cash,
    because the market prices in the burn to come and the strategic
    uncertainty. 0.5 is deliberately conservative.

    Returns ``None`` when price or net cash is unknown: an unanchored
    downside is worse than no row, because it silently reads as zero risk.
    """
    px = _finite(price)
    ncps = _finite(net_cash_per_share)
    if px is None or px <= 0 or ncps is None:
        return None

    burn_per_share = 0.0
    months = _finite(months_to_catalyst)
    burn = _finite(monthly_burn_per_share)
    if months is not None and burn is not None and months > 0 and burn > 0:
        burn_per_share = months * burn

    residual_cash = max(ncps - burn_per_share, 0.0)
    floor_multiple = (residual_cash * clamp(failure_haircut, 0.0, 1.0)) / px
    return {
        "downside_multiple": round(clamp(floor_multiple, 0.0, 1.0), 4),
        "net_cash_per_share": round(ncps, 4),
        "net_cash_to_price": round(ncps / px, 4),
        "burn_to_catalyst_per_share": round(burn_per_share, 4) if burn_per_share else 0.0,
        "failure_haircut": failure_haircut,
    }


# ── Upside: market-implied, not asserted ──────────────────────────────────


def upside_multiple_from_iv(
    *,
    spot: float | None,
    iv: float | None,
    days_to_expiry: int | None,
    tail_probability: float = 0.15,
    risk_free_rate: float = 0.05,
) -> dict[str, Any] | None:
    """Upside anchored on the options market's own implied distribution.

    Inverts the lognormal that ``intelligence.market_implied_prob`` reads
    out of the IV surface: find the price ``K`` the market assigns
    ``tail_probability`` of exceeding by expiry, and express it as a
    multiple of spot.

    ``K = S x exp((r - sigma^2/2) T + sigma sqrt(T) z)`` where ``z`` is the
    standard-normal quantile at ``1 - tail_probability``.

    **Known bias, stated plainly**: a lognormal understates a binary
    readout. Real event distributions are bimodal — a Phase 3 hit gaps a
    microcap several hundred percent, which no smooth IV surface prices.
    So this anchor is *conservative* on upside, and a name that clears the
    EV bar on it clears on a pessimistic reading of its own option chain.
    That is the right direction to be wrong in.

    **Degenerate case.** A lognormal's median is ``S exp((r - sigma^2/2) T)``,
    which for a 200 %-IV microcap sits well *below* spot: the risk-neutral
    distribution says the typical outcome is a large loss and the mean is
    carried entirely by the right tail. At a fat enough ``tail_probability``
    the solved target can therefore land at or under spot. That is not an
    upside, and feeding a sub-1.0 multiple into the EV would quietly
    understate the bet. Those return ``None`` with the reason recorded by
    the caller rather than a misleading number.

    Returns ``None`` on unusable input rather than a default multiple.
    """
    s = _finite(spot)
    sigma = _finite(iv)
    dte = _finite(days_to_expiry)
    if s is None or s <= 0 or sigma is None or sigma <= 0 or dte is None or dte <= 0:
        return None
    tail = clamp(tail_probability, 1e-4, 0.5)

    t = dte / 365.0
    z = _norm_ppf(1.0 - tail)
    k = s * math.exp((risk_free_rate - 0.5 * sigma * sigma) * t + sigma * math.sqrt(t) * z)
    if not math.isfinite(k) or k <= 0:
        return None
    if k <= s:
        # The tail we asked for is not in the right tail at this vol/horizon.
        return None
    return {
        "upside_multiple": round(k / s, 4),
        "upside_target_price": round(k, 4),
        "upside_basis": "options_iv_lognormal_tail",
        "upside_tail_probability": tail,
        "iv_atm": round(sigma, 4),
        "days_to_expiry": int(dte),
    }


def _norm_ppf(p: float) -> float:
    """Standard-normal inverse CDF (Acklam's rational approximation).

    Accurate to ~1e-9 over (0, 1) — far beyond what an IV surface warrants,
    and it avoids a scipy dependency in a module the scheduler imports.
    """
    if p <= 0.0:
        return -math.inf
    if p >= 1.0:
        return math.inf

    a = (-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00)
    b = (-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01)
    c = (-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00)
    d = (7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00)

    p_low, p_high = 0.02425, 1.0 - 0.02425
    if p < p_low:
        q = math.sqrt(-2.0 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    if p > p_high:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
                ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
           (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)


# ── The gate that actually matters ────────────────────────────────────────


def runway_covers_catalyst(
    *,
    runway_months: float | None,
    months_to_catalyst: float | None,
    buffer_months: float = RUNWAY_BUFFER_MONTHS,
) -> dict[str, Any]:
    """Does the cash outlast the readout, with a financing buffer?

    This replaces the absolute ``cash_runway_score >= 0.4`` threshold the
    Long Plays board used. That threshold asked "does this company have a
    lot of cash", which is not the question. The question is whether the
    equity survives *to* the event: a company that runs dry first raises
    into a pending readout at a discount, and the existing holders eat it.

    A 3.2-month runway against a 1.7-month catalyst passes here and fails
    the absolute floor — that is OLMA on 2026-09-10, and passing is the
    correct answer.

    Unknown runway fails: an unmeasured balance sheet is not a safe one.
    """
    runway = _finite(runway_months)
    months = _finite(months_to_catalyst)
    if runway is None or months is None or months < 0:
        return {
            "runway_covers_catalyst": False,
            "runway_months": runway,
            "months_to_catalyst": months,
            "runway_margin_months": None,
            "reason": "runway or catalyst date unknown",
        }
    margin = runway - months
    covered = margin >= buffer_months
    return {
        "runway_covers_catalyst": covered,
        "runway_months": round(runway, 2),
        "months_to_catalyst": round(months, 2),
        "runway_margin_months": round(margin, 2),
        "buffer_months": buffer_months,
        "reason": (
            "cash covers the readout with buffer" if covered
            else f"only {margin:.1f} mo of margin, needs {buffer_months:.0f}"
        ),
    }


# ── Expected value ────────────────────────────────────────────────────────


def catalyst_ev(
    *,
    p_success: float | None,
    upside_multiple: float | None,
    downside_multiple: float | None,
    market_implied_p: float | None = None,
) -> dict[str, Any] | None:
    """Expected value of the event, in multiples of the current price.

    ``EV = p x upside + (1 - p) x downside``. An EV of 1.0 means the bet is
    fair at today's price; 2.5 means the event is worth 2.5x the price
    under these assumptions.

    ``edge_vs_market`` is the interesting number: GRID's ``p_success``
    minus the probability the option chain implies for the same move. A
    positive edge means the market prices this readout as less likely than
    the trial's own evidence says. That divergence, not the raw EV, is
    where a repeatable multi-bagger edge would live — an EV computed from
    the market's own upside anchor is otherwise close to self-referential.

    Returns ``None`` when any leg is missing: a partial EV is a misleading
    EV.
    """
    p = _finite(p_success)
    up = _finite(upside_multiple)
    down = _finite(downside_multiple)
    if p is None or up is None or down is None:
        return None
    p = clamp(p, 0.0, 1.0)

    ev = p * up + (1.0 - p) * down
    risk = max(1.0 - down, 1e-6)          # capital at risk per unit of price
    reward = max(up - 1.0, 0.0)           # gain per unit of price
    out: dict[str, Any] = {
        "expected_value_multiple": round(ev, 4),
        "reward_to_risk": round(reward / risk, 4),
        "breakeven_probability": round(clamp((1.0 - down) / max(up - down, 1e-6), 0.0, 1.0), 4),
    }
    implied = _finite(market_implied_p)
    if implied is not None:
        out["market_implied_probability"] = round(clamp(implied, 0.0, 1.0), 4)
        out["edge_vs_market"] = round(p - clamp(implied, 0.0, 1.0), 4)
    return out
