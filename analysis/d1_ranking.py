"""D1 ranking implementation, frozen in source and tests (#1129; contract §5.1 "Ranking rule").

The owner-signed D0 contract (``docs/research/d0-research-contract-v0.md`` §5.1) freezes the D1
ranking rule and says its implementation is "frozen in source and tests before admission". This
module is that implementation. It is pure: no database, provider, clock or outcome is read, and
nothing here runs a look. Every number below is either copied from §5.1 or is an implementation
choice that §5.1 leaves open; the second kind is listed in :data:`RANKING_SPEC` under
``implementation_choices`` so a reviewer sees exactly what the source decided.

Contract rules implemented (§5.1):

* Order: data quality tier, then event strength, then liquidity. No composite probability.
* Event strength (F1 and F2) = |C0 / P-1 - 1| divided by the issuer's 60-session close-to-close
  volatility ending at P-1.
* One event per accession: an 8-K carrying Item 2.02 is an F1 event only; otherwise one carrying
  any of Items 1.01, 2.01 or 1.02 is one F2 event (:func:`family_for_items`).
* Ranking bucket: four fixed strength breakpoints b1 < b2 < b3 < b4 per family, computed on the
  development window only. An event's quintile is 1 + the number of breakpoints at or below its
  strength (:func:`quintile`), so a strength equal to a breakpoint goes to the higher quintile.
* Within a bucket, ties break by liquidity (higher first), then accession number ascending.
* Quantile estimator: one named estimator, fixed here (:data:`QUANTILE_ESTIMATOR`).
* Zero or missing volatility: no strength; the event is ``EXCLUDED_INPUT``, never given a
  substitute volatility.
* Duplicate breakpoints: if strict b1 < b2 < b3 < b4 do not exist the family is HOLD with the
  cause recorded. Cutoffs are never invented.

Implementation choices §5.1 leaves open (frozen here, before any development data is read):

* **Volatility** is the sample standard deviation (n - 1 divisor) of the 60 daily *simple*
  close-to-close returns ``P[t] / P[t-1] - 1`` whose last return ends at P-1, so it needs 61
  closes. Simple returns, because the numerator is a simple return. Not annualised (numerator and
  denominator are both daily). Any missing, non-finite or non-positive close in the window makes
  the volatility missing.
* **Estimator** is Hyndman and Fan type 7 (linear interpolation of the order statistics at
  ``(n - 1) p``), the R default and ``numpy.quantile(method="linear")``. Computed in pure Python
  so the breakpoints do not depend on a library version.
* **Order inside the contract's "Order" line** is read as: tier ascending (lower is better), then
  quintile descending, then liquidity descending, then accession ascending. Strength enters
  through its quintile, never as a raw tie-breaker inside a bucket, so the order of an event is
  fixed by what is known at its own deadline D and the frozen breakpoints.
* **Tier** is an integer the caller must state. The contract does not define tiers, so there is
  no default: a caller that does not know an event's tier cannot rank it. Defining the tiers is
  a contract revision (recorded as an open item in the M2 contract-prep note).
* **Missing liquidity** (the trailing 60-session median dollar volume) makes the event
  ``EXCLUDED_INPUT``, as the §5.3 operational input list requires a liquidity window.
* **Fewer than two development strengths** cannot define a quantile: HOLD ``TOO_FEW_DEVELOPMENT_EVENTS``.
  No larger minimum is invented here; sample floors belong to §5.3.

``tests/test_d1_ranking.py`` pins every one of these: golden vectors, the contract's tie and
exclusion rules, an estimator cross-check, a digest of :data:`RANKING_SPEC` and a digest of a
seeded synthetic panel's whole ranking. A change to any of them fails a test and is a new
ranking version, never an edit.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any, Iterable, Sequence

RANKING_VERSION = "d1-ranking-v1"
QUANTILE_ESTIMATOR = "hyndman_fan_type_7"
BREAKPOINT_PROBS: tuple[float, ...] = (0.2, 0.4, 0.6, 0.8)
VOL_RETURNS = 60  # daily simple returns; needs VOL_RETURNS + 1 closes
EXCLUDED_INPUT = "EXCLUDED_INPUT"
HOLD = "HOLD"
OK = "OK"
CAUSE_DUPLICATE_BREAKPOINTS = "DUPLICATE_BREAKPOINTS"
CAUSE_TOO_FEW = "TOO_FEW_DEVELOPMENT_EVENTS"
CAUSE_BAD_STRENGTH = "NON_FINITE_OR_NEGATIVE_DEVELOPMENT_STRENGTH"

F1, F2 = "D1-F1", "D1-F2"
F1_ITEMS = frozenset({"2.02"})
F2_ITEMS = frozenset({"1.01", "2.01", "1.02"})

#: Everything that defines the ranking, as data. Its digest is pinned in the tests.
RANKING_SPEC: dict[str, Any] = {
    "version": RANKING_VERSION,
    "basis": "contract d0-contract-v0 section 5.1, Ranking rule",
    "strength": "abs(C0 / P-1 - 1) / volatility",
    "volatility": {"returns": "daily simple close-to-close", "count": VOL_RETURNS, "divisor": "n - 1",
                   "annualised": False, "ends_at": "P-1"},
    "quantile_estimator": QUANTILE_ESTIMATOR,
    "breakpoint_probs": list(BREAKPOINT_PROBS),
    "breakpoints_strict": True,
    "quintile": "1 + count(breakpoints <= strength)",
    "order": ["tier ascending", "quintile descending", "liquidity descending", "accession ascending"],
    "families": {F1: sorted(F1_ITEMS), F2: sorted(F2_ITEMS), "precedence": "2.02 makes an accession F1 only"},
    "exclusions": {"zero_or_missing_volatility": EXCLUDED_INPUT, "bad_c0_or_p_prev": EXCLUDED_INPUT,
                   "missing_liquidity": EXCLUDED_INPUT},
    "holds": [CAUSE_DUPLICATE_BREAKPOINTS, CAUSE_TOO_FEW, CAUSE_BAD_STRENGTH],
    "implementation_choices": ["volatility definition", "estimator", "order reading", "tier has no default",
                               "missing liquidity excludes", "minimum development events = 2"],
}


def canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def spec_digest() -> str:
    return hashlib.sha256(canonical(RANKING_SPEC)).hexdigest()


def _num(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def _price(x: Any) -> bool:
    return _num(x) and x > 0


# ---------------------------------------------------------------- per-event strength
@dataclass(frozen=True)
class Strength:
    """A strength, or why the event has none (``excluded`` = ``EXCLUDED_INPUT`` plus ``detail``)."""

    value: float | None
    excluded: str | None = None
    detail: str | None = None


def daily_volatility(closes: Sequence[Any]) -> float | None:
    """Sample std of the 60 daily simple returns ending at the last close; None if missing.

    ``closes`` is oldest first and must hold exactly 61 valid closes ending at P-1: fewer, more, or
    any missing / non-finite / non-positive close gives None. Zero volatility is returned as 0.0
    (the caller refuses it).
    """
    if not isinstance(closes, (list, tuple)) or len(closes) != VOL_RETURNS + 1 or not all(_price(c) for c in closes):
        return None
    rets = [closes[i] / closes[i - 1] - 1.0 for i in range(1, len(closes))]
    mean = math.fsum(rets) / len(rets)
    var = math.fsum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    return math.sqrt(var)


def event_strength(closes: Sequence[Any], c0: Any) -> Strength:
    """|C0 / P-1 - 1| / volatility, with P-1 = ``closes[-1]``. Never substitutes a missing input."""
    if not isinstance(closes, (list, tuple)) or not closes or not _price(closes[-1]):
        return Strength(None, EXCLUDED_INPUT, "P-1 missing, zero or not a number")
    if not _price(c0):
        return Strength(None, EXCLUDED_INPUT, "C0 missing, zero or not a number")
    vol = daily_volatility(closes)
    if vol is None:
        return Strength(None, EXCLUDED_INPUT, "volatility missing (need 61 valid closes ending at P-1)")
    if vol == 0.0:
        return Strength(None, EXCLUDED_INPUT, "volatility is zero")
    return Strength(abs(c0 / closes[-1] - 1.0) / vol)


def family_for_items(items: Iterable[str]) -> str | None:
    """One event per accession: Item 2.02 makes it F1 only; else any of 1.01 / 2.01 / 1.02 makes it F2."""
    s = {str(i).strip() for i in items}
    if s & F1_ITEMS:
        return F1
    if s & F2_ITEMS:
        return F2
    return None


# ---------------------------------------------------------------- breakpoints
def quantile_type7(sorted_values: Sequence[float], p: float) -> float:
    """Hyndman-Fan type 7: linear interpolation at index (n - 1) * p of the sorted values."""
    n = len(sorted_values)
    if n == 0:
        raise ValueError("no values")
    h = (n - 1) * p
    lo = math.floor(h)
    hi = min(lo + 1, n - 1)
    return sorted_values[lo] + (h - lo) * (sorted_values[hi] - sorted_values[lo])


@dataclass(frozen=True)
class Breakpoints:
    status: str  # OK or HOLD
    values: tuple[float, float, float, float] | None
    cause: str | None = None
    n: int = 0


def compute_breakpoints(development_strengths: Iterable[Any]) -> Breakpoints:
    """b1 .. b4 from the development window's strengths, or HOLD with the cause. Never invents a cutoff."""
    vals = list(development_strengths)
    if any(not _num(v) or v < 0 for v in vals):
        return Breakpoints(HOLD, None, CAUSE_BAD_STRENGTH, len(vals))
    if len(vals) < 2:
        return Breakpoints(HOLD, None, CAUSE_TOO_FEW, len(vals))
    s = sorted(float(v) for v in vals)
    b = tuple(quantile_type7(s, p) for p in BREAKPOINT_PROBS)
    if not (b[0] < b[1] < b[2] < b[3]):
        return Breakpoints(HOLD, None, CAUSE_DUPLICATE_BREAKPOINTS, len(vals))
    return Breakpoints(OK, b, None, len(vals))  # type: ignore[arg-type]


def quintile(strength: float, breakpoints: Sequence[float]) -> int:
    """1 + the number of breakpoints at or below the strength (a strength equal to b_k goes up)."""
    if len(breakpoints) != len(BREAKPOINT_PROBS) or not all(breakpoints[i] < breakpoints[i + 1] for i in range(3)):
        raise ValueError("breakpoints must be four strictly increasing values")
    if not _num(strength) or strength < 0:
        raise ValueError("strength must be a finite non-negative number")
    return 1 + sum(1 for b in breakpoints if b <= strength)


# ---------------------------------------------------------------- ordering
@dataclass(frozen=True)
class RankedEvent:
    accession: str
    tier: int
    strength: float
    quintile: int
    liquidity: float


def rank_events(events: Iterable[dict[str, Any]], breakpoints: Breakpoints) -> tuple[list[RankedEvent], list[dict[str, Any]]]:
    """Order events by (tier asc, quintile desc, liquidity desc, accession asc); also the excluded ones.

    Each event is ``{"accession", "tier", "strength", "liquidity"}``. Returns (ranked, excluded);
    an event with a missing liquidity or strength is excluded as ``EXCLUDED_INPUT`` and counted by
    the caller, never ranked on a substitute. A HOLD family ranks nothing (ValueError). A duplicate
    accession or a missing tier is a caller error (ValueError): one event per accession.
    """
    if breakpoints.status != OK or breakpoints.values is None:
        raise ValueError(f"family is on HOLD ({breakpoints.cause}); nothing is ranked")
    seen: set[str] = set()
    ranked: list[RankedEvent] = []
    excluded: list[dict[str, Any]] = []
    for ev in events:
        acc = ev.get("accession")
        if not isinstance(acc, str) or not acc:
            raise ValueError("event without an accession")
        if acc in seen:
            raise ValueError(f"accession {acc} appears more than once (one event per accession)")
        seen.add(acc)
        tier = ev.get("tier")
        if not (isinstance(tier, int) and not isinstance(tier, bool)):
            raise ValueError(f"{acc}: tier must be an integer stated by the caller")
        st, liq = ev.get("strength"), ev.get("liquidity")
        if not _num(st) or st < 0:
            excluded.append({"accession": acc, "reason": EXCLUDED_INPUT, "detail": "no strength"})
        elif not _num(liq) or liq < 0:
            excluded.append({"accession": acc, "reason": EXCLUDED_INPUT, "detail": "no liquidity"})
        else:
            ranked.append(RankedEvent(acc, tier, float(st), quintile(st, breakpoints.values), float(liq)))
    ranked.sort(key=lambda r: (r.tier, -r.quintile, -r.liquidity, r.accession))
    return ranked, excluded
