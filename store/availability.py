"""Availability and provenance contract for analytical outputs.

The 2026-09-17 fake-data audit (docs/audits/fake-data-2026-09-17/) found
one shape behind most of its 173 confirmed findings: when an input was
missing, failed, stale or never computed, the code emitted a *number* that
was indistinguishable from a measurement (``0.5``, ``50``, ``0``,
``"NEUTRAL"``, ``"moderate"``, ``as_of = now()``, ``confidence: "confirmed"``).
This module is the shared vocabulary for saying "we do not know" instead.

Contract (also in docs/reference/AVAILABILITY_CONTRACT.md):

* A result is either **available** (a value with provenance) or
  **unavailable** (``available: False``, ``status: "unavailable"``, a
  ``reason``, and ``None`` for every measured field). There is no third
  state where a placeholder number stands in for the missing value.
* ``as_of`` is the observation/valuation date of the *data*, never the wall
  clock at response time. An unavailable result has ``as_of: None``.
* A modelled, assumed or heuristic number is labelled at the field level
  (``basis``, ``estimated: True``) so a client can tell it from an
  observation without reading code.
* Unavailable results are served, not cached as observations, not written
  to snapshot tables, and never averaged into a score.

Helpers here build the payloads; they carry no I/O.

NOTE (godview lane, fable/godview-20260918): this file is vendored
verbatim from ``feat/availability-provenance-contract`` (#536, tip
b5babef4) because that branch has not yet merged into this worktree's
ancestry and ``godview/cftc_pillar.py`` needs ``measured_or_none`` for
per-field provenance. Content is byte-for-byte the upstream module —
de-duplicate (drop this file, keep the merged one) once that branch lands.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

STATUS_OK = "ok"
STATUS_UNAVAILABLE = "unavailable"
STATUS_PARTIAL = "partial"


@dataclass(frozen=True)
class Provenance:
    """Where a value came from and how fresh it is.

    ``source``  – the feed / table / model that produced it (e.g. ``"raw_series:WALCL"``).
    ``as_of``   – observation date of the data (never response time).
    ``vintage`` – when that observation was pulled/computed, if known.
    ``basis``   – how a modelled number was derived (``"black_scholes sigma=0.25"``,
                  ``"assumed_split_50_30"``); ``None`` for a direct observation.
    ``estimated`` – True when ``basis`` describes an assumption rather than a measurement.
    """

    source: str
    as_of: date | None = None
    vintage: datetime | None = None
    basis: str | None = None
    estimated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "as_of": self.as_of.isoformat() if self.as_of else None,
            "vintage": self.vintage.isoformat() if self.vintage else None,
            "basis": self.basis,
            "estimated": bool(self.estimated),
        }


@dataclass(frozen=True)
class Freshness:
    as_of: date | None
    age_days: int | None
    stale: bool | None  # None when there is no as_of to judge
    stale_after_days: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat() if self.as_of else None,
            "age_days": self.age_days,
            "stale": self.stale,
            "stale_after_days": self.stale_after_days,
        }


def freshness(as_of: date | datetime | None, *, stale_after_days: int, today: date | None = None) -> Freshness:
    """Age a data date against a threshold. ``as_of=None`` -> ``stale=None`` (unknown), never fresh."""
    if isinstance(as_of, datetime):
        as_of = as_of.date()
    if as_of is None:
        return Freshness(None, None, None, stale_after_days)
    today = today or datetime.now(timezone.utc).date()
    age = (today - as_of).days
    return Freshness(as_of, age, age > stale_after_days, stale_after_days)


def unavailable(reason: str, *, source: str | None = None, **fields: Any) -> dict[str, Any]:
    """Build an honest unavailable payload.

    Every name passed in ``fields`` is emitted with the value ``None`` so the
    response keeps its documented shape (clients can key on it) without any
    number in it. ``reason`` is required and must say *why* (``"no VIX close
    series in resolved_series"``), not just ``"error"``.
    """
    if not reason or not reason.strip():
        raise ValueError("unavailable() requires a non-empty reason")
    out: dict[str, Any] = {
        "available": False,
        "status": STATUS_UNAVAILABLE,
        "reason": reason,
        "as_of": None,
    }
    if source is not None:
        out["source"] = source
    for name in fields:
        out[name] = None
    return out


def available(*, provenance: Provenance, **fields: Any) -> dict[str, Any]:
    """Build an available payload: the measured fields plus provenance."""
    out: dict[str, Any] = {
        "available": True,
        "status": STATUS_OK,
        "as_of": provenance.as_of.isoformat() if provenance.as_of else None,
        "provenance": provenance.to_dict(),
    }
    out.update(fields)
    return out


def partial(*, provenance: Provenance, missing: list[str], **fields: Any) -> dict[str, Any]:
    """Available with named gaps: ``missing`` lists the fields that are ``None`` and why the
    consumer must not treat the result as complete (e.g. neutral-filled components)."""
    out = available(provenance=provenance, **fields)
    out["status"] = STATUS_PARTIAL
    out["missing"] = list(missing)
    return out


def is_unavailable(payload: dict[str, Any] | None) -> bool:
    return payload is None or payload.get("available") is False or payload.get("status") == STATUS_UNAVAILABLE


def measured_or_none(value: Any) -> float | None:
    """Coerce a nullable numeric column without inventing a midpoint.

    Replaces the ``float(x) if x else 0.5`` pattern: ``None`` stays ``None``,
    a real ``0`` stays ``0.0`` (the falsy test used to rewrite it), NaN/inf
    become ``None``.
    """
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def mean_of_available(values: list[float | None]) -> tuple[float | None, int]:
    """Average only measured values; ``(None, 0)`` when nothing was measured.

    Never lets a ``None`` count as 0 or 0.5. Returns the sample size so the
    caller can publish it next to the average.
    """
    xs = [v for v in values if v is not None]
    if not xs:
        return None, 0
    return sum(xs) / len(xs), len(xs)


@dataclass(frozen=True)
class CacheDecision:
    cache: bool
    why: str = field(default="")


def cacheable(payload: dict[str, Any]) -> CacheDecision:
    """Whether a payload may be pinned in a TTL cache or written to a snapshot table.

    Unavailable payloads are served but never pinned or persisted as
    observations; partial payloads may be cached (they are labelled).
    """
    if is_unavailable(payload):
        return CacheDecision(False, "unavailable payload must not be cached or snapshotted")
    return CacheDecision(True)
