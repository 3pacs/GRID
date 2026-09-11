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
* A name with **no trial phase on file** is returned under ``unranked``
  with ``no_trial_evidence``. It must never inherit
  ``DEFAULT_PHASE_BASE_RATE`` and compete on it: doing so ranks the
  unmeasured above a measured name whose indication multiplier honestly
  marks it harder, which rewards the absence of evidence. The phase comes
  from GRID's own scored ``trial_signals`` where that exists and from the
  raw ct.gov study in ``trial_cache`` otherwise, and every row says which
  (``trial_phase_source``).
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
  distribution** (``intelligence.market_implied_prob``). A name with no
  listed chain has no upside anchor and is returned under ``unranked``
  with ``no_option_chain``, never with a substituted default: "we cannot
  price this yet" and "this is a bad bet" are different answers and the
  board must not blur them. Its downside floor is still computed, because
  spot comes from the PIT price history rather than the chain.
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
``runway_covers_catalyst(...)``, ``months_between(...)``,
``build_catalyst_board(engine, *, as_of=None, ...)``
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Sequence, TypeVar

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

T = TypeVar("T")

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

# ct.gov names a *disease*, not a therapeutic area. This maps the condition
# strings that actually appear in ``trial_cache`` onto the areas above.
# Ordered: the first substring hit wins, so haematologic MALIGNANCIES sit in
# the oncology block deliberately — leukemia and myeloma carry solid-tumour-like
# development risk, not the 1.35 of non-malignant haematology (sickle cell,
# haemophilia), and mapping them to "hematology" would inflate the prior on
# exactly the names this board ranks. Terms are matched against the
# underscored key, so multi-word terms are written with underscores.
INDICATION_TERM_TO_AREA: dict[str, str] = {
    # oncology (incl. haematologic malignancy)
    "neoplasm": "oncology", "cancer": "oncology", "carcinoma": "oncology",
    "tumor": "oncology", "tumour": "oncology", "melanoma": "oncology",
    "sarcoma": "oncology", "glioma": "oncology", "glioblastoma": "oncology",
    "lymphoma": "oncology", "leukemia": "oncology", "leukaemia": "oncology",
    "myeloma": "oncology", "myelodysplastic": "oncology", "mesothelioma": "oncology",
    "metasta": "oncology",
    # non-malignant haematology
    "sickle_cell": "hematology", "hemophilia": "hematology",
    "haemophilia": "hematology", "thalassemia": "hematology",
    "thrombocytopenia": "hematology", "von_willebrand": "hematology",
    "anemia": "hematology", "anaemia": "hematology",
    # neurology
    "alzheimer": "neurology", "parkinson": "neurology", "huntington": "neurology",
    "amyotrophic": "neurology", "multiple_sclerosis": "neurology",
    "epilep": "neurology", "dementia": "neurology", "migraine": "neurology",
    "neuropath": "neurology", "muscular_dystrophy": "neurology",
    "spinal_muscular": "neurology", "stroke": "neurology",
    # psychiatry
    "depress": "psychiatry", "schizophren": "psychiatry", "bipolar": "psychiatry",
    "anxiety": "psychiatry", "post_traumatic": "psychiatry",
    # cardiovascular
    "heart_failure": "cardiovascular", "hypertension": "cardiovascular",
    "cardiomyopathy": "cardiovascular", "atrial_fibrillation": "cardiovascular",
    "coronary": "cardiovascular", "myocardial": "cardiovascular",
    "amyloidosis": "cardiovascular",
    # infectious disease
    "infection": "infectious_disease", "hiv": "infectious_disease",
    "hepatitis": "infectious_disease", "influenza": "infectious_disease",
    "covid": "infectious_disease", "tuberculosis": "infectious_disease",
    "malaria": "infectious_disease", "viral": "infectious_disease",
    "bacterial": "infectious_disease", "vaccine": "infectious_disease",
    # metabolic
    "diabet": "metabolic", "obesity": "metabolic", "steatohepatitis": "metabolic",
    "nash": "metabolic", "hyperlipid": "metabolic", "cholesterol": "metabolic",
    "hypercholesterol": "metabolic",
    # autoimmune / immunology
    "lupus": "autoimmune", "psoriasis": "autoimmune", "psoriatic": "autoimmune",
    "rheumatoid": "autoimmune", "crohn": "autoimmune", "colitis": "autoimmune",
    "atopic_dermatitis": "autoimmune", "vitiligo": "autoimmune",
    "myasthenia": "autoimmune", "celiac": "autoimmune",
    "inflammatory_bowel": "autoimmune", "alopecia": "autoimmune",
    # ophthalmology
    "macular": "ophthalmology", "retinit": "ophthalmology",
    "retinopathy": "ophthalmology", "glaucoma": "ophthalmology",
    "uveitis": "ophthalmology", "dry_eye": "ophthalmology",
    "ophthalm": "ophthalmology",
    # respiratory
    "asthma": "respiratory", "copd": "respiratory",
    "chronic_obstructive": "respiratory", "cystic_fibrosis": "respiratory",
    "pulmonary_fibrosis": "respiratory", "bronchiectasis": "respiratory",
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

# ── Trade shape ───────────────────────────────────────────────────────────
# EV and edge_vs_market answer different questions and a board that shows
# only one of them misleads in opposite directions.
#
#   expected_value_multiple < 1.0  -> holding the EQUITY is negative EV. The
#     net-cash floor on a busted biotech is near zero, so a failed readout
#     takes almost everything; a 25% shot at 1.5x does not pay for that.
#   edge_vs_market > 0             -> GRID's p_success exceeds the probability
#     the option chain assigns the same move. Buying that tail with DEFINED
#     RISK can be positive EV even when the equity is not, because the option
#     caps the loss at premium while the equity does not.
#
# Those are compatible: most rows on a real board are TRADE_SHAPE_OPTION_TAIL.
# Ranking by EV alone buries them; ranking by edge alone hides that the
# underlying is a falling knife. Every ranked row now carries both.
TRADE_SHAPE_EQUITY: str = "equity_or_option"      # EV > 1 and edge > 0
TRADE_SHAPE_OPTION_TAIL: str = "defined_risk_option_only"  # edge > 0, EV <= 1
TRADE_SHAPE_NONE: str = "no_trade"                # edge <= 0

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
    """Therapeutic-area LOA multiplier; 1.0 for an unknown or missing area.

    Accepts an area name (``trial_signals.primary_indication``) or a plain
    disease name (a ct.gov condition — "Melanoma", "Sickle Cell Disease"),
    which ``INDICATION_TERM_TO_AREA`` maps onto an area first. Without that
    map every registry-sourced name would silently score 1.0, which is the
    same reward-the-absence-of-evidence failure the phase gate exists to
    stop: an oncology trial must be marked hard whether GRID or ct.gov
    named the disease.
    """
    key = _normalize_key(indication)
    if key is None:
        return 1.0
    if key in INDICATION_LOA_MULTIPLIER:
        return INDICATION_LOA_MULTIPLIER[key]
    # Tolerate free-text ("metastatic oncology", "rare disease - CNS").
    for known, mult in INDICATION_LOA_MULTIPLIER.items():
        if known in key:
            return mult
    for term, area in INDICATION_TERM_TO_AREA.items():
        if term in key:
            return INDICATION_LOA_MULTIPLIER.get(area, 1.0)
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
    rate. With no recognisable phase it returns ``DEFAULT_PHASE_BASE_RATE``
    with ``p_success_phase = None`` — the caller's signal that the number is
    a placeholder standing in for absent evidence, not a prior about this
    name. ``build_catalyst_board`` refuses to rank such a row
    (``no_trial_evidence``); any new caller must make the same distinction.
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


# ── Board assembly ────────────────────────────────────────────────────────
#
# Everything above is pure. Everything below reads the database, PIT-bounded
# on ``as_of``, and degrades a failing source to a ``method_notes`` line
# rather than an exception.

DEFAULT_HORIZON_DAYS: int = 548          # 18 months, the gate's horizon
DEFAULT_TOP_K: int = 40
OPTION_TENOR_BUFFER_DAYS: int = 30       # the contract must outlive the event
PROFILE_KEYS: tuple[str, ...] = (
    "cash", "total_debt", "shares_outstanding", "quarterly_burn",
    "cash_runway_months", "market_cap", "sector", "industry",
)
# Applied on top of the normal failure haircut when total_debt is unmeasured.
# Ignoring debt overstates a cash floor, and overstating the floor understates
# the risk — so an unmeasured balance sheet is penalised, never assumed clean.
UNKNOWN_DEBT_HAIRCUT: float = 0.6

_CATALYST_NAMES_SQL = text(
    """
    SELECT DISTINCT ON (cc.ticker, cc.expected_date)
           cc.ticker,
           cc.event_type,
           cc.expected_date,
           ts.trial_phase,
           ts.primary_indication,
           ts.fda_designation,
           ts.endpoint_clarity,
           ts.enrollment_pct,
           ts.trial_strength_score,
           ts.signal_type,
           ts.market_cap_mm,
           ts.cash_runway_months,
           tc.raw_json #> '{protocolSection,designModule,phases}',
           tc.raw_json #> '{protocolSection,conditionsModule,conditions}'
    FROM catalyst_calendar cc
    LEFT JOIN trial_signals ts
           ON ts.ticker = cc.ticker
          AND ts.created_at <= :as_of_ts
    -- The registry fallback. ``trial_signals`` only covers names GRID has
    -- scored; ``trial_cache`` holds the raw ct.gov study for every catalyst
    -- the ingestor wrote, keyed by the same nct_id the calendar row carries.
    -- Without this join a name we *have* the phase for reads as phase-unknown.
    LEFT JOIN trial_cache tc
           ON tc.nct_id = cc.nct_id
          AND tc.parsed_at <= :as_of_ts
    WHERE cc.is_active
      AND cc.expected_date >= :as_of
      AND cc.expected_date <= :max_date
      AND cc.ticker ~ '^[A-Z.-]{1,6}$'
    ORDER BY cc.ticker, cc.expected_date ASC, ts.created_at DESC
    """
)

_PROFILE_SQL = text(
    """
    SELECT ticker, profile
    FROM company_profiles
    WHERE ticker IS NOT NULL
      AND profile IS NOT NULL
      AND (last_analyzed IS NULL OR last_analyzed <= :as_of_ts)
    """
)

_OPTIONS_SNAPSHOT_SQL = text(
    """
    SELECT DISTINCT ON (ticker) ticker, spot_price, iv_atm, signal_date
    FROM options_daily_signals
    WHERE ticker = ANY(:tickers)
      AND signal_date >= :start
      AND signal_date <= :as_of
      AND iv_atm IS NOT NULL
      AND spot_price IS NOT NULL
    ORDER BY ticker, signal_date DESC
    """
)

OPTIONS_SNAPSHOT_LOOKBACK_DAYS: int = 10


def _safe(name: str, fn: Callable[[], T], notes: list[str], default: T) -> T:
    """Run ``fn``; on any exception log, note it, and return ``default``."""
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001
        log.warning("catalyst_ev: {n} unavailable: {e}", n=name, e=str(exc))
        notes.append(f"{name}: unavailable ({type(exc).__name__}: {str(exc)[:120]})")
        return default


def _as_of_timestamp(as_of: date) -> datetime:
    """End of the ``as_of`` day in UTC — the PIT cut-off for timestamp columns."""
    return datetime.combine(as_of, datetime.max.time()).replace(tzinfo=timezone.utc)


def _norm_ticker(value: Any) -> str | None:
    if value is None:
        return None
    out = str(value).strip().upper()
    return out or None


PHASE_SOURCE_SIGNAL: str = "trial_signals"
PHASE_SOURCE_REGISTRY: str = "trial_registry"


def _registry_phase(value: Any) -> str | None:
    """ct.gov ``designModule.phases`` (a JSON array) as one normalisable string.

    ``["PHASE2", "PHASE3"]`` becomes ``"PHASE2/PHASE3"``, which
    ``normalize_phase`` reads as the earlier (harder) leg. ``["NA"]`` and
    friends normalise to ``None`` and are therefore treated as no evidence.
    """
    if value is None:
        return None
    if isinstance(value, (str, bytes)):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            value = [value]
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, Sequence):
        return None
    parts = [str(v).strip() for v in value if str(v or "").strip()]
    return "/".join(parts) or None


def _registry_indication(value: Any) -> str | None:
    """First ct.gov condition — the therapeutic area the LOA multiplier reads."""
    if value is None:
        return None
    if isinstance(value, (str, bytes)):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return str(value).strip() or None
    if isinstance(value, str):
        return value.strip() or None
    if not isinstance(value, Sequence):
        return None
    for item in value:
        text_value = str(item or "").strip()
        if text_value:
            return text_value
    return None


def _load_catalyst_names(
    engine: Engine, as_of: date, horizon_days: int
) -> list[dict[str, Any]]:
    """Dated catalysts inside the horizon, with their trial evidence attached.

    One row per (ticker, catalyst date), carrying the latest ``trial_signals``
    record known at ``as_of`` — the phase, indication, designation, endpoint
    clarity and enrollment that ``success_probability`` reads.

    Where GRID has not scored the name, the phase and indication fall back to
    the raw ct.gov study in ``trial_cache`` for the calendar row's own
    ``nct_id``. Both paths are labelled on the row (``trial_phase_source``,
    ``primary_indication_source``) so a reader can tell GRID's own scored
    evidence from registry metadata — and so a name with neither is visibly
    evidence-free rather than silently scored on a default.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            _CATALYST_NAMES_SQL,
            {
                "as_of": as_of,
                "as_of_ts": _as_of_timestamp(as_of),
                "max_date": as_of + timedelta(days=int(horizon_days)),
            },
        ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        ticker = _norm_ticker(row[0])
        if not ticker:
            continue
        expected = row[2]
        expected_date = expected.date() if isinstance(expected, datetime) else expected
        if not isinstance(expected_date, date):
            continue
        phase = row[3]
        phase_source: str | None = PHASE_SOURCE_SIGNAL if normalize_phase(phase) else None
        if phase_source is None:
            registry_phase = _registry_phase(row[12] if len(row) > 12 else None)
            if normalize_phase(registry_phase):
                phase, phase_source = registry_phase, PHASE_SOURCE_REGISTRY

        indication = row[4]
        indication_source: str | None = PHASE_SOURCE_SIGNAL if indication else None
        if indication_source is None:
            registry_indication = _registry_indication(row[13] if len(row) > 13 else None)
            if registry_indication:
                indication, indication_source = registry_indication, PHASE_SOURCE_REGISTRY

        out.append(
            {
                "ticker": ticker,
                "event_type": row[1],
                "catalyst_date": expected_date,
                "trial_phase": phase,
                "trial_phase_source": phase_source,
                "primary_indication": indication,
                "primary_indication_source": indication_source,
                "fda_designation": row[5],
                "endpoint_clarity": _finite(row[6]),
                "enrollment_pct": _finite(row[7]),
                "trial_strength_score": _finite(row[8]),
                "signal_type": (str(row[9]).strip().upper() if row[9] else None),
                "market_cap_mm": _finite(row[10]),
                "trial_runway_months": _finite(row[11]),
            }
        )
    return out


def _load_profiles(engine: Engine, as_of: date) -> dict[str, dict[str, Any]]:
    """Balance-sheet fundamentals per ticker (PIT: ``last_analyzed <= as_of``).

    Reads the profile JSONB directly rather than through
    ``long_plays.FUNDAMENTALS_KEYS``, which omits ``total_debt`` and
    ``quarterly_burn`` — the two fields the net-cash floor and the burn to
    the catalyst are built from.
    """
    with engine.connect() as conn:
        rows = conn.execute(_PROFILE_SQL, {"as_of_ts": _as_of_timestamp(as_of)}).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = _norm_ticker(row[0])
        if not ticker:
            continue
        profile = row[1]
        if isinstance(profile, (str, bytes)):
            try:
                profile = json.loads(profile)
            except (TypeError, ValueError):
                profile = {}
        if not isinstance(profile, dict):
            profile = {}
        entry: dict[str, Any] = {}
        for key in PROFILE_KEYS:
            value = profile.get(key)
            entry[key] = value if key in ("sector", "industry") else _finite(value)
        out[ticker] = entry
    return out


def _load_options_snapshot(
    engine: Engine, tickers: Sequence[str], as_of: date
) -> dict[str, dict[str, Any]]:
    """Latest spot + ATM IV per ticker at or before ``as_of``.

    Bounded on both sides of ``signal_date``. An empty result is the normal
    state for a name with no listed chain — the caller records that the
    upside anchor is unavailable rather than substituting one.
    """
    wanted = sorted({t for t in (_norm_ticker(x) for x in tickers) if t})
    if not wanted:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(
            _OPTIONS_SNAPSHOT_SQL,
            {
                "tickers": wanted,
                "start": as_of - timedelta(days=OPTIONS_SNAPSHOT_LOOKBACK_DAYS),
                "as_of": as_of,
            },
        ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = _norm_ticker(row[0])
        spot = _finite(row[1])
        iv = _finite(row[2])
        if ticker and spot and spot > 0 and iv and iv > 0:
            signal_date = row[3]
            out[ticker] = {
                "spot": spot,
                "iv_atm": iv,
                "signal_date": (
                    signal_date.isoformat() if hasattr(signal_date, "isoformat") else signal_date
                ),
            }
    return out


def _load_spot_prices(
    engine: Engine, tickers: Sequence[str], as_of: date
) -> dict[str, float]:
    """Latest adjusted close per ticker at or before ``as_of``.

    Reuses ``long_plays._load_adj_close`` — the PIT-correct reader this repo
    already uses for the same universe — rather than opening a second path
    to the same series.
    """
    from intelligence.long_plays import _load_adj_close

    history = _load_adj_close(engine, tickers, 1, as_of)
    out: dict[str, float] = {}
    for ticker, points in history.items():
        if not points:
            continue
        last = _finite(points[-1][1])
        if last is not None and last > 0:
            out[ticker] = last
    return out


def _fitted_base_rates(engine: Engine, as_of: date, notes: list[str]) -> tuple[dict[str, float] | None, str]:
    """GRID's own phase base rates when enough readouts are scored.

    Returns ``(table, basis)``. Until the scorer has run and cleared the
    sample floor this returns ``(None, "literature_prior")`` and says so on
    the board, so nobody mistakes a borrowed industry average for a
    measurement.
    """
    from intelligence.trial_outcomes import load_scored_outcomes

    scored = load_scored_outcomes(engine, as_of=as_of)
    fitted, counts = empirical_phase_outcomes(scored)
    if fitted:
        notes.append(
            f"p_success: fitted from {sum(counts.values())} scored GRID readouts {counts}"
        )
        return fitted, BASIS_EMPIRICAL
    notes.append(
        f"p_success: literature priors — only {sum(counts.values())} scored readouts "
        f"{counts}, floor is {EMPIRICAL_MIN_SAMPLES} per phase"
    )
    return None, BASIS_LITERATURE


def classify_trade_shape(
    *,
    expected_value_multiple: float | None,
    edge_vs_market: float | None,
) -> str:
    """Which instrument, if any, the two numbers actually support.

    A negative edge is ``no_trade`` regardless of EV: without an edge over
    the chain there is nothing to harvest. A positive edge with EV at or
    below 1.0 is tradable only with defined risk, because the equity leg
    loses money on these odds.
    """
    ev = _finite(expected_value_multiple)
    edge = _finite(edge_vs_market)
    if edge is None or edge <= 0.0:
        return TRADE_SHAPE_NONE
    if ev is not None and ev > 1.0:
        return TRADE_SHAPE_EQUITY
    return TRADE_SHAPE_OPTION_TAIL


def build_catalyst_board(
    engine: Engine,
    *,
    as_of: date | None = None,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    top_k: int = DEFAULT_TOP_K,
    tail_probability: float = 0.15,
    rank_by: str = "edge_vs_market",
) -> dict[str, Any]:
    """Rank dated catalysts by their edge over the chain, evidence attached.

    Ranked on ``rank_by`` descending — ``edge_vs_market`` by default, which
    is the number this module has always said is the interesting one: an EV
    built from the option chain's own upside anchor is close to
    self-referential, while ``p_success - market_implied_probability`` is a
    genuine disagreement with the market. Pass
    ``rank_by="expected_value_multiple"`` for the previous ordering.

    Every ranked row carries ``trade_shape`` (see ``classify_trade_shape``)
    so a reader cannot mistake a name whose equity leg is negative EV for
    one that is outright attractive. ``actionable`` mirrors it as a bool.

    A name is **ranked**
    only when every leg is anchored — P(success), an upside from its own
    option chain, and a net-cash downside. Names missing a leg are returned
    under ``unranked`` with the reason, because "we cannot price this yet"
    and "this is a bad bet" are different answers and the operator needs to
    tell them apart.

    Never raises. Every failing source degrades to a ``method_notes`` line.
    """
    as_of = as_of or date.today()
    notes: list[str] = [
        f"as_of={as_of.isoformat()}; every read PIT-bounded on that date",
        "EV = p x upside + (1-p) x downside, in multiples of the current price; not a forecast",
        f"upside: the price the option chain puts at a {tail_probability:.0%} tail by an expiry "
        f"{OPTION_TENOR_BUFFER_DAYS} d past the catalyst — a lognormal understates a binary "
        "readout, so this anchor is deliberately conservative",
        "downside: net cash per share less burn to the catalyst, haircut for a failed readout",
    ]

    base_rates, basis = _safe(
        "trial_outcomes", lambda: _fitted_base_rates(engine, as_of, notes), notes,
        (None, BASIS_LITERATURE),
    )
    catalysts = _safe(
        "catalyst_calendar", lambda: _load_catalyst_names(engine, as_of, horizon_days), notes, []
    )
    profiles = _safe("company_profiles", lambda: _load_profiles(engine, as_of), notes, {})
    tickers = sorted({c["ticker"] for c in catalysts})
    options = _safe(
        "options_daily_signals", lambda: _load_options_snapshot(engine, tickers, as_of), notes, {}
    )
    # Spot must not depend on the option chain: a name without a chain still
    # has a price, and therefore still has a computable net-cash floor. Only
    # the *upside* anchor is chain-gated.
    prices = _safe("price_history", lambda: _load_spot_prices(engine, tickers, as_of), notes, {})

    if tickers and not options:
        notes.append(
            "options_daily_signals: no chain for any catalyst name — the upside anchor is "
            "unavailable, so nothing can be ranked. Run the options puller over the catalyst "
            "universe (ingestion/options.py catalyst_options_universe)"
        )

    if catalysts:
        scored = sum(1 for c in catalysts if c.get("trial_phase_source") == PHASE_SOURCE_SIGNAL)
        registry = sum(
            1 for c in catalysts if c.get("trial_phase_source") == PHASE_SOURCE_REGISTRY
        )
        notes.append(
            f"trial phase: {scored} from GRID's own trial_signals, {registry} from the ct.gov "
            f"registry (trial_cache), {len(catalysts) - scored - registry} with none — the last "
            "group is unranked (no_trial_evidence), never scored on the default base rate"
        )

    ranked: list[dict[str, Any]] = []
    unranked: list[dict[str, Any]] = []

    for entry in catalysts:
        try:
            row = _score_catalyst(
                entry,
                profile=profiles.get(entry["ticker"]),
                option=options.get(entry["ticker"]),
                spot=prices.get(entry["ticker"]),
                as_of=as_of,
                base_rates=base_rates,
                basis=basis,
                tail_probability=tail_probability,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("catalyst_ev: {t} skipped: {e}", t=entry["ticker"], e=str(exc))
            notes.append(f"{entry['ticker']}: scoring failed ({type(exc).__name__}); skipped")
            continue
        (ranked if row.get("expected_value_multiple") is not None else unranked).append(row)

    for row in ranked:
        shape = classify_trade_shape(
            expected_value_multiple=row.get("expected_value_multiple"),
            edge_vs_market=row.get("edge_vs_market"),
        )
        row["trade_shape"] = shape
        row["actionable"] = shape != TRADE_SHAPE_NONE

    if rank_by not in {"edge_vs_market", "expected_value_multiple"}:
        notes.append(f"rank_by={rank_by!r} not recognised; ranked on edge_vs_market")
        rank_by = "edge_vs_market"

    def _key(r: dict[str, Any]) -> tuple[float, str]:
        v = _finite(r.get(rank_by))
        # Missing sort key sinks rather than crashes or floats to the top.
        return (-(v if v is not None else float("-inf")), r["ticker"])

    ranked.sort(key=_key)
    unranked.sort(key=lambda r: r["ticker"])

    actionable_total = sum(1 for r in ranked if r["actionable"])
    equity_ok = sum(1 for r in ranked if r["trade_shape"] == TRADE_SHAPE_EQUITY)
    notes.append(
        f"ranked on {rank_by} descending; {actionable_total}/{len(ranked)} carry a positive "
        f"edge over the chain, of which {equity_ok} also clear EV > 1.0"
    )
    notes.append(
        "trade_shape: expected_value_multiple <= 1.0 means the EQUITY leg is negative EV — "
        "the net-cash floor is near zero, so a failed readout takes almost everything. A "
        "positive edge_vs_market with EV <= 1.0 is tradable only with defined risk "
        "(the option caps the loss at premium; the equity does not). Never read this board "
        "as a buy-the-stock list."
    )

    return {
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "horizon_days": int(horizon_days),
        "p_success_basis": basis,
        "catalysts_considered": len(catalysts),
        "rank_by": rank_by,
        "ranked": ranked[: max(1, int(top_k))],
        "ranked_total": len(ranked),
        "actionable_total": actionable_total,
        "equity_grade_total": equity_ok,
        "unranked": unranked,
        "unranked_reasons": _reason_counts(unranked),
        "method_notes": notes,
    }


def _reason_counts(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
    """Tally of why names could not be ranked — the coverage report."""
    counts: dict[str, int] = {}
    for row in rows:
        for reason in row.get("blocking_reasons") or ["unknown"]:
            counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def _score_catalyst(
    entry: dict[str, Any],
    *,
    profile: dict[str, Any] | None,
    option: dict[str, Any] | None,
    spot: float | None,
    as_of: date,
    base_rates: dict[str, float] | None,
    basis: str,
    tail_probability: float,
) -> dict[str, Any]:
    """Score one dated catalyst. Always returns a row, ranked or not."""
    ticker = entry["ticker"]
    catalyst_date = entry["catalyst_date"]
    months = months_between(as_of, catalyst_date)
    days_to_catalyst = (catalyst_date - as_of).days
    profile = profile or {}
    blocking: list[str] = []

    # No phase, no rank. Without a phase ``success_probability`` falls back to
    # DEFAULT_PHASE_BASE_RATE — a number invented for this name — which ranks
    # the unmeasured ABOVE a measured one whose indication multiplier honestly
    # marks it harder (IDYA, Phase 2 melanoma, p 0.247, sat below three names
    # with no trial evidence at all on 2026-09-11). That is the same failure
    # the module refuses for the upside anchor: "we cannot price this yet" and
    # "this is a bad bet" are different answers.
    phase_key = normalize_phase(entry.get("trial_phase"))
    if phase_key is None:
        blocking.append("no_trial_evidence")

    prob = success_probability(
        phase=entry.get("trial_phase"),
        indication=entry.get("primary_indication"),
        endpoint_clarity=entry.get("endpoint_clarity"),
        fda_designation=entry.get("fda_designation"),
        enrollment_pct=entry.get("enrollment_pct"),
        base_rates=base_rates,
        basis=basis,
    )

    runway_months = profile.get("cash_runway_months")
    if runway_months is None:
        runway_months = entry.get("trial_runway_months")
    runway = runway_covers_catalyst(runway_months=runway_months, months_to_catalyst=months)
    if not runway["runway_covers_catalyst"]:
        blocking.append(
            "runway_short" if runway["runway_margin_months"] is not None else "runway_unknown"
        )

    # Prefer the option snapshot's spot so price and IV sit on one surface;
    # fall back to the PIT close so a name without a chain still gets a floor.
    spot = (option or {}).get("spot") or spot
    if spot is None:
        blocking.append("no_price")

    shares = profile.get("shares_outstanding")
    cash = profile.get("cash")
    debt = profile.get("total_debt")
    burn_q = profile.get("quarterly_burn")

    net_cash_ps: float | None = None
    monthly_burn_ps: float | None = None
    debt_known = debt is not None
    if cash is not None and shares is not None and shares > 0:
        net_cash_ps = (cash - (debt or 0.0)) / shares
        if burn_q is not None and burn_q > 0:
            monthly_burn_ps = (burn_q / 3.0) / shares
    else:
        blocking.append("balance_sheet_unknown")

    down = downside_multiple(
        price=spot,
        net_cash_per_share=net_cash_ps,
        months_to_catalyst=months,
        monthly_burn_per_share=monthly_burn_ps,
        failure_haircut=0.5 if debt_known else UNKNOWN_DEBT_HAIRCUT * 0.5,
    )
    if down is None and not ({"balance_sheet_unknown", "no_price"} & set(blocking)):
        blocking.append("downside_unanchored")

    up = None
    if option is not None:
        up = upside_multiple_from_iv(
            spot=spot,
            iv=option.get("iv_atm"),
            days_to_expiry=days_to_catalyst + OPTION_TENOR_BUFFER_DAYS,
            tail_probability=tail_probability,
        )
        if up is None:
            blocking.append("iv_tail_below_spot")
    else:
        blocking.append("no_option_chain")

    market_implied: float | None = None
    if up is not None and option is not None:
        # The chain's own probability of reaching the same target — the
        # number GRID's p_success is measured against.
        market_implied = tail_probability

    ev = catalyst_ev(
        p_success=prob["p_success"],
        upside_multiple=(up or {}).get("upside_multiple"),
        downside_multiple=(down or {}).get("downside_multiple"),
        market_implied_p=market_implied,
    )
    # Hard gates. A name whose cash does not reach its own readout, or whose
    # trial phase is unknown, is not ranked however attractive the arithmetic
    # looks — the first is diluted away regardless of the science, the second
    # has no science on file to price.
    if ev is not None and (not runway["runway_covers_catalyst"] or phase_key is None):
        ev = None

    row: dict[str, Any] = {
        "ticker": ticker,
        "event_type": entry.get("event_type"),
        "catalyst_date": catalyst_date.isoformat(),
        "months_to_catalyst": round(months, 2),
        "trial_phase": entry.get("trial_phase"),
        "trial_phase_source": entry.get("trial_phase_source"),
        "primary_indication": entry.get("primary_indication"),
        "primary_indication_source": entry.get("primary_indication_source"),
        "fda_designation": entry.get("fda_designation"),
        "signal_type": entry.get("signal_type"),
        "market_cap_usd": (
            entry["market_cap_mm"] * 1e6 if entry.get("market_cap_mm") else profile.get("market_cap")
        ),
        "spot": spot,
        "sector": profile.get("sector"),
        "net_cash_per_share": round(net_cash_ps, 4) if net_cash_ps is not None else None,
        "total_debt_known": debt_known,
        "blocking_reasons": blocking,
        **prob,
        **runway,
        **(down or {}),
        **(up or {}),
        **(ev or {}),
    }
    row["expected_value_multiple"] = (ev or {}).get("expected_value_multiple")
    return row
