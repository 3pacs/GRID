"""Contamination classifier for signal/prediction rows (GRID W7).

Context (vault audit, verified live 2026-09-17): four incompatible
scoring regimes coexist in ``signal_sources``; a synthetic batch from
April 2026 dominates the scored predictions used elsewhere in the
codebase to derive live signal weight overrides
(``intelligence/signal_weight_overrides.py``); oracle prediction
scoring stopped 2026-05-16. Any consumer of "scored" rows needs an
honest answer to "was this row scored under a known regime, and is it
real market data" — and an honest "I don't know" when the evidence
isn't there, rather than a guess dressed up as a fact.

This module classifies each row (a plain dict — a signal or
prediction record, however the caller sourced it) along two axes:

* ``scoring_version`` ∈ {v_may, v_jun, v_jul, v_sep, unknown}
* ``origin``          ∈ {synthetic, backfill, live, unknown}

Evidence policy (read this before changing the mappings below)
-----------------------------------------------------------------
Classification uses ONLY explicit, named evidence fields — never an
inferred "scored_at falls in this date range" heuristic. That
restriction is deliberate: scored_at ranges are exactly the kind of
implicit, backfill-blind evidence that let a contaminated corpus look
authoritative in the first place.

``scoring_version`` — audited against schema.sql directly. None of
the tables that carry scored predictions or signals define an explicit
scoring/evaluation-version column:

* ``astrogrid.prediction_run``    — has ``weight_version``,
  ``model_version``, ``scoring_class`` (a market-liquidity category,
  not a scoring *regime*), but no version field for which of the four
  incompatible scoring regimes was used.
* ``astrogrid.prediction_score``  — ``scored_at``, ``verdict``,
  ``invalidation_status``; no version field.
* ``signal_sources``, ``signal_data``, ``signal_registry``,
  ``options_daily_signals`` — no version field either.

Because no such field exists anywhere in schema.sql,
``classify_scoring_version()`` always returns ``"unknown"``. This is
not a placeholder pending a smarter rule — it is the correct answer
given the current schema, and it stays correct until a real
scoring_version column is added and populated. If/when the four
regimes get an explicit column, wire it in here (and note which
regime name maps to v_may/v_jun/v_jul/v_sep — that mapping does not
exist in the codebase today either, so don't invent it).

``origin`` — the only explicit, named field in schema.sql that
plausibly carries origin information is
``astrogrid.prediction_run.live_or_local``, a real enum:
``CHECK (live_or_local IN ('live', 'local', 'archive', 'hybrid'))``.
Only the ``'live'`` value maps unambiguously onto this module's
``origin`` taxonomy — ``'local'``, ``'archive'``, and ``'hybrid'`` are
NOT defined anywhere as synonyms for "backfill" or "synthetic", so
guessing that mapping would be exactly the kind of unlabeled
relabeling this module exists to avoid. JSONB ``metadata`` columns
exist on several tables (``lever_pullers``, ``signal_registry``,
``source_catalog``-adjacent tables) but no key inside them is
documented or used anywhere in the codebase to mean "synthetic" or
"backfill" (grepped; no hits). A row is also allowed to carry an
explicit top-level ``"origin"`` key directly (for callers who've
already attached one, e.g. an ingestion pipeline that knows it wrote
synthetic rows) — if present and one of the three known values, it is
trusted as-is.

Everything else is ``"unknown"``.

Hard guarantees
----------------
* Never touches the database — pure functions over dicts already in
  memory.
* Never mutates or relabels the input row — every ``row.get(...)`` is
  read-only; nothing here writes back into ``row``.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable

SCORING_VERSIONS: tuple[str, ...] = ("v_may", "v_jun", "v_jul", "v_sep", "unknown")
ORIGINS: tuple[str, ...] = ("synthetic", "backfill", "live", "unknown")

_KNOWN_ORIGIN_VALUES = {"synthetic", "backfill", "live"}

# The one schema.sql field with an explicit, documented enum that maps
# onto our origin taxonomy — and only for this one value. See the
# module docstring for why the other three enum values are NOT mapped.
_LIVE_OR_LOCAL_LIVE_VALUE = "live"


@dataclass(frozen=True)
class ClassificationResult:
    """One row's classification, plus which field (if any) supplied
    the evidence — so a caller / reviewer can audit *why* a row landed
    where it did without re-deriving it."""

    scoring_version: str
    origin: str
    scoring_version_evidence_field: str | None
    origin_evidence_field: str | None


def classify_scoring_version(row: dict[str, Any]) -> tuple[str, str | None]:
    """Always returns ``("unknown", None)``.

    See the module docstring's evidence-policy section: schema.sql has
    no explicit scoring/evaluation-version column on any table that
    carries scored signals or predictions, and scored_at date-range
    inference is explicitly disallowed as sole evidence. Kept as a
    real function (rather than inlined) so the "no evidence exists"
    fact is a single, greppable, testable statement — and so it's the
    one place to change if a version column is ever added.
    """
    return "unknown", None


def classify_origin(row: dict[str, Any]) -> tuple[str, str | None]:
    """Classify ``origin`` from explicit, named fields only.

    Checked in order:
    1. An explicit top-level ``"origin"`` key already on the row,
       if its value is one of {synthetic, backfill, live}.
    2. ``astrogrid.prediction_run.live_or_local == "live"`` (the only
       unambiguous enum mapping available in schema.sql today).

    Anything else — missing fields, an unrecognized value, a
    ``live_or_local`` of 'local'/'archive'/'hybrid' — is ``"unknown"``.
    """
    explicit = row.get("origin")
    if isinstance(explicit, str) and explicit.strip().lower() in _KNOWN_ORIGIN_VALUES:
        return explicit.strip().lower(), "origin"

    live_or_local = row.get("live_or_local")
    if isinstance(live_or_local, str) and live_or_local.strip().lower() == _LIVE_OR_LOCAL_LIVE_VALUE:
        return "live", "live_or_local"

    return "unknown", None


def classify_row(row: dict[str, Any]) -> ClassificationResult:
    """Classify a single row along both axes. Read-only — never
    mutates ``row``."""
    scoring_version, sv_field = classify_scoring_version(row)
    origin, origin_field = classify_origin(row)
    return ClassificationResult(
        scoring_version=scoring_version,
        origin=origin,
        scoring_version_evidence_field=sv_field,
        origin_evidence_field=origin_field,
    )


@dataclass
class CohortReport:
    """Counts per class for a cohort of rows, with explicit unknown
    buckets called out (never silently folded into a class)."""

    total: int
    scoring_version_counts: dict[str, int]
    origin_counts: dict[str, int]
    unknown_scoring_version: int
    unknown_origin: int
    fully_unknown: int  # both axes unknown
    results: list[ClassificationResult] = field(default_factory=list, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "scoring_version_counts": dict(self.scoring_version_counts),
            "origin_counts": dict(self.origin_counts),
            "unknown_scoring_version": self.unknown_scoring_version,
            "unknown_origin": self.unknown_origin,
            "fully_unknown": self.fully_unknown,
        }


def build_cohort_report(rows: Iterable[dict[str, Any]]) -> CohortReport:
    """Classify every row in ``rows`` and return counts per class.

    Never writes to the database and never mutates any row in
    ``rows`` — this is a pure read-and-count pass, safe to run
    speculatively over any cohort without side effects.
    """
    scoring_version_counter: Counter[str] = Counter()
    origin_counter: Counter[str] = Counter()
    fully_unknown = 0
    total = 0
    results: list[ClassificationResult] = []

    for row in rows:
        total += 1
        result = classify_row(row)
        results.append(result)
        scoring_version_counter[result.scoring_version] += 1
        origin_counter[result.origin] += 1
        if result.scoring_version == "unknown" and result.origin == "unknown":
            fully_unknown += 1

    # Ensure every known class appears in the counts, even at zero —
    # a report with a silently-missing bucket is worse than one that
    # spells out "0 of these".
    for version in SCORING_VERSIONS:
        scoring_version_counter.setdefault(version, 0)
    for origin in ORIGINS:
        origin_counter.setdefault(origin, 0)

    return CohortReport(
        total=total,
        scoring_version_counts=dict(scoring_version_counter),
        origin_counts=dict(origin_counter),
        unknown_scoring_version=scoring_version_counter["unknown"],
        unknown_origin=origin_counter["unknown"],
        fully_unknown=fully_unknown,
        results=results,
    )
