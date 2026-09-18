"""Per-field availability + provenance record (W2b extension of store/availability.py).

``store/availability.py`` (feat/availability-provenance-contract, #536) defines the
contract for a whole *result*: available / partial / unavailable, with a single
``Provenance`` attached. The DAG audit in
``docs/reference/DATASET_CONSUMER_DAG.md`` ("Contract extension proposal") found
that individual *fields* inside a result — one source's freshness row, one
series in a multi-series response — need a richer, self-contained record: a
published/available/ingested timeline, a revision id, a coverage fraction,
and a machine-readable stale reason that ``Freshness``/``unavailable()``
don't model on their own.

This module is purely additive. It does not modify, rename, or reimplement
anything in ``store/availability.py`` — it imports and reuses what already
fits (``measured_or_none``, ``STATUS_OK``/``STATUS_UNAVAILABLE``) and adds the
new ``FieldRecord`` type alongside it. See
``docs/reference/AVAILABILITY_FIELDS.md`` for the field table and the rule
this module exists to enforce.

Helpers here build records; they carry no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from store.availability import measured_or_none

# ---------------------------------------------------------------------------
# Two independent axes
# ---------------------------------------------------------------------------

#: How a value was obtained. Only meaningful when ``availability == "available"``;
#: ``None`` for unavailable/invalid fields (there is no provenance for a value
#: that does not exist or was rejected).
FIELD_PROVENANCE_MEASURED = "measured"
FIELD_PROVENANCE_DERIVED = "derived"
FIELD_PROVENANCE_MODELED = "modeled"
FIELD_PROVENANCE_VALUES = frozenset(
    {FIELD_PROVENANCE_MEASURED, FIELD_PROVENANCE_DERIVED, FIELD_PROVENANCE_MODELED}
)

#: Whether the field has a usable value at all. This is a SEPARATE axis from
#: ``provenance`` above — e.g. a field can be ``available`` + ``modeled``
#: (a Black-Scholes estimate), but never ``unavailable`` + ``measured``.
FIELD_AVAILABILITY_AVAILABLE = "available"
FIELD_AVAILABILITY_UNAVAILABLE = "unavailable"
FIELD_AVAILABILITY_INVALID = "invalid"
FIELD_AVAILABILITY_VALUES = frozenset(
    {FIELD_AVAILABILITY_AVAILABLE, FIELD_AVAILABILITY_UNAVAILABLE, FIELD_AVAILABILITY_INVALID}
)

#: Machine-readable reason a field is stale, missing, or otherwise not a
#: normal fresh measurement. Distinct from ``availability``: a field can be
#: ``available`` and still carry a ``stale_reason`` (e.g. a successful pull
#: from 40 days ago -> ``availability="available"``, ``stale_reason="stale"``).
STALE_NEVER_CONFIGURED = "never_configured"
STALE_FETCH_FAILED = "fetch_failed"
STALE_STALE = "stale"
STALE_RATE_LIMITED = "rate_limited"
STALE_NOT_PUBLISHED_YET = "not_published_yet"
STALE_PARSER_ERROR = "parser_error"
STALE_EMPTY_SOURCE = "empty_source"
STALE_PARTIAL_HISTORY = "partial_history"
STALE_MATERIALIZER_FAILED = "materializer_failed"
STALE_CONSUMER_QUERY_MISMATCH = "consumer_query_mismatch"
STALE_UNKNOWN = "unknown"
STALE_REASONS = frozenset(
    {
        STALE_NEVER_CONFIGURED,
        STALE_FETCH_FAILED,
        STALE_STALE,
        STALE_RATE_LIMITED,
        STALE_NOT_PUBLISHED_YET,
        STALE_PARSER_ERROR,
        STALE_EMPTY_SOURCE,
        STALE_PARTIAL_HISTORY,
        STALE_MATERIALIZER_FAILED,
        STALE_CONSUMER_QUERY_MISMATCH,
        STALE_UNKNOWN,
    }
)


@dataclass(frozen=True)
class FieldRecord:
    """A single field's availability + provenance, as its own record.

    Two axes describe every field, and they are independent — a field is
    never scored by conflating them:

    * ``availability`` — is there a usable value at all? ``available`` /
      ``unavailable`` / ``invalid``.
    * ``provenance`` — *how* an available value was obtained: ``measured``
      (a direct observation), ``derived`` (arithmetic/aggregation over
      measured values), ``modeled`` (a model or assumption). ``None`` when
      ``availability`` is not ``available``.

    There is deliberately **no boolean confidence field**. A caller that
    wants to know "can I trust this" reads ``availability`` +
    ``stale_reason``, never a single yes/no flag layered on top.

    Coverage, and every timestamp/id below, use ``None`` for "unknown" —
    never ``0`` or ``""``. ``0`` and ``0.0`` are real measurements
    (``coverage_fraction=0.0`` means "measured, and the coverage really is
    zero"); ``None`` means "we do not know".

    ``to_dict()`` key table (stable names, safe to persist or serialise):

    | key                    | type            | meaning                                                              |
    |------------------------|-----------------|-----------------------------------------------------------------------|
    | ``availability``       | str             | ``available`` / ``unavailable`` / ``invalid``                        |
    | ``provenance``         | str \\| None    | ``measured`` / ``derived`` / ``modeled``; ``None`` unless available   |
    | ``value``              | Any             | the field's value; ``None`` unless ``availability == "available"``   |
    | ``unit``               | str \\| None    | unit of ``value`` (e.g. ``"USD"``, ``"pct"``, ``"index_points"``)     |
    | ``obs_date``           | str \\| None    | ISO date of a point-in-time observation                              |
    | ``obs_start``          | str \\| None    | ISO date, start of an observation period                             |
    | ``obs_end``            | str \\| None    | ISO date, end of an observation period                               |
    | ``published_at``       | str \\| None    | ISO datetime the source published/released the data                  |
    | ``available_at``       | str \\| None    | ISO datetime this system first could have acquired it                |
    | ``ingested_at``        | str \\| None    | ISO datetime this system actually pulled/wrote it                    |
    | ``revision``           | str \\| None    | revision/vintage id (e.g. a `pull_timestamp` or release-vintage tag)  |
    | ``source_catalog``     | str \\| None    | `source_catalog` name the value came from                            |
    | ``series_id``          | str \\| None    | series id within that source                                         |
    | ``calculation_version``| str \\| None    | version id of the code/formula that produced a derived/modeled value |
    | ``coverage_fraction``  | float \\| None  | fraction of expected data present, ``0.0``-``1.0``                   |
    | ``coverage_count``     | int \\| None    | count of data points present                                          |
    | ``coverage_expected``  | int \\| None    | count of data points expected                                         |
    | ``stale_reason``       | str \\| None    | one of ``STALE_REASONS``; explains a stale/missing/invalid field      |
    """

    availability: str
    provenance: str | None = None
    value: Any = None
    unit: str | None = None
    obs_date: date | None = None
    obs_start: date | None = None
    obs_end: date | None = None
    published_at: datetime | None = None
    available_at: datetime | None = None
    ingested_at: datetime | None = None
    revision: str | None = None
    source_catalog: str | None = None
    series_id: str | None = None
    calculation_version: str | None = None
    coverage_fraction: float | None = None
    coverage_count: int | None = None
    coverage_expected: int | None = None
    stale_reason: str | None = None

    def __post_init__(self) -> None:
        if self.availability not in FIELD_AVAILABILITY_VALUES:
            raise ValueError(f"unknown availability: {self.availability!r}")
        if self.provenance is not None and self.provenance not in FIELD_PROVENANCE_VALUES:
            raise ValueError(f"unknown provenance: {self.provenance!r}")
        if self.availability != FIELD_AVAILABILITY_AVAILABLE and self.provenance is not None:
            raise ValueError(
                f"provenance must be None when availability={self.availability!r} "
                "(there is no provenance for a value that does not exist)"
            )
        if self.availability == FIELD_AVAILABILITY_AVAILABLE and self.provenance is None:
            raise ValueError("an available field must name its provenance (measured/derived/modeled)")
        if self.stale_reason is not None and self.stale_reason not in STALE_REASONS:
            raise ValueError(f"unknown stale_reason: {self.stale_reason!r}")
        if self.coverage_fraction is not None and not (0.0 <= self.coverage_fraction <= 1.0):
            raise ValueError(f"coverage_fraction out of range [0,1]: {self.coverage_fraction!r}")

    def to_dict(self) -> dict[str, Any]:
        def _iso(v: date | datetime | None) -> str | None:
            return v.isoformat() if v is not None else None

        return {
            "availability": self.availability,
            "provenance": self.provenance,
            "value": self.value,
            "unit": self.unit,
            "obs_date": _iso(self.obs_date),
            "obs_start": _iso(self.obs_start),
            "obs_end": _iso(self.obs_end),
            "published_at": _iso(self.published_at),
            "available_at": _iso(self.available_at),
            "ingested_at": _iso(self.ingested_at),
            "revision": self.revision,
            "source_catalog": self.source_catalog,
            "series_id": self.series_id,
            "calculation_version": self.calculation_version,
            "coverage_fraction": self.coverage_fraction,
            "coverage_count": self.coverage_count,
            "coverage_expected": self.coverage_expected,
            "stale_reason": self.stale_reason,
        }


def _field(availability: str, provenance: str | None, *, value: Any = None, **meta: Any) -> FieldRecord:
    known = {f_.name for f_ in FieldRecord.__dataclass_fields__.values()}
    extra = set(meta) - known
    if extra:
        raise TypeError(f"FieldRecord has no field(s): {sorted(extra)}")
    return FieldRecord(availability=availability, provenance=provenance, value=value, **meta)


def measured_field(value: Any, **meta: Any) -> FieldRecord:
    """An ``available`` field that is a direct observation (e.g. a `raw_series` row)."""
    return _field(FIELD_AVAILABILITY_AVAILABLE, FIELD_PROVENANCE_MEASURED, value=value, **meta)


def derived_field(value: Any, **meta: Any) -> FieldRecord:
    """An ``available`` field computed from measured values (a spread, a ratio, an aggregate)."""
    return _field(FIELD_AVAILABILITY_AVAILABLE, FIELD_PROVENANCE_DERIVED, value=value, **meta)


def modeled_field(value: Any, **meta: Any) -> FieldRecord:
    """An ``available`` field from a model or assumption (e.g. Black-Scholes, a heuristic)."""
    return _field(FIELD_AVAILABILITY_AVAILABLE, FIELD_PROVENANCE_MODELED, value=value, **meta)


def unavailable_field(stale_reason: str, **meta: Any) -> FieldRecord:
    """A field with no value at all. ``value`` is always ``None`` — never invented.

    ``stale_reason`` is required and must be one of ``STALE_REASONS``.
    """
    meta.pop("value", None)
    return _field(FIELD_AVAILABILITY_UNAVAILABLE, None, value=None, stale_reason=stale_reason, **meta)


def invalid_field(stale_reason: str = STALE_PARSER_ERROR, **meta: Any) -> FieldRecord:
    """A field where an attempt produced data that failed validation (e.g. a negative
    population, a value outside a series' physical range, a parse that returned the
    wrong type). ``provenance`` is ``None`` — an invalid value carries no trustworthy
    provenance even if ``value`` is populated for debugging."""
    return _field(FIELD_AVAILABILITY_INVALID, None, stale_reason=stale_reason, **meta)


def coverage_or_none(count: Any, expected: Any) -> float | None:
    """Compute a coverage fraction without letting an unknown count/expected become 0.

    Mirrors ``store.availability.measured_or_none``: if either input is
    missing/non-numeric, or ``expected`` is 0 (undefined ratio), the result
    is ``None`` — never ``0.0``.
    """
    c = measured_or_none(count)
    e = measured_or_none(expected)
    if c is None or e is None or e == 0:
        return None
    return c / e
