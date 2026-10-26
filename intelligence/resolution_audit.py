"""
GRID resolution audit supervisor.

Verifies data quality AFTER the bulk resolver (or incremental resolver) runs.
Every resolved row is checked for correctness across six dimensions:
  1. Duplicate detection — conflicting values for same feature+date
  2. Stale data detection — features with no recent observations
  3. Value sanity — obviously wrong values (negative prices, NaN, etc.)
  4. Coverage completeness — suspicious gaps, jumps, stuck data
  5. Entity map consistency — verify every mapping actually works
  6. Cross-source agreement — divergence between sources for same feature

Results are persisted to the ``resolution_audits`` table and surfaced
via the ``/api/v1/system/resolution-audit`` endpoint.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from normalization.entity_map import SEED_MAPPINGS, NEW_MAPPINGS_V2
from normalization.resolver import CONFLICT_THRESHOLD, FAMILY_CONFLICT_THRESHOLDS

# ---------------------------------------------------------------------------
# Table DDL — idempotent
# ---------------------------------------------------------------------------

_AUDIT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS resolution_audits (
    id            SERIAL PRIMARY KEY,
    check_type    TEXT NOT NULL,
    severity      TEXT NOT NULL,
    feature       TEXT,
    description   TEXT,
    evidence      JSONB,
    auto_fixed    BOOLEAN DEFAULT FALSE,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
"""


def _ensure_table(engine: Engine) -> None:
    """Create the resolution_audits table if it does not exist."""
    with engine.begin() as conn:
        conn.execute(text(_AUDIT_TABLE_DDL))


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class AuditFinding:
    """A single audit finding from a resolution quality check."""

    check_type: str  # 'duplicate', 'stale', 'sanity', 'gap', 'mapping', 'divergence'
    severity: str    # 'critical', 'warning', 'info'
    feature: str
    description: str
    evidence: dict = field(default_factory=dict)
    suggested_fix: str = ""


# ---------------------------------------------------------------------------
# Value-sanity rules keyed by feature family
# ---------------------------------------------------------------------------

# Each rule returns True if the value is INVALID.
_FAMILY_SANITY = {
    "equity":     lambda v: v < 0,                           # prices positive
    "commodity":  lambda v: v < 0,                           # prices positive
    "crypto":     lambda v: v < 0,                           # prices positive
    "vol":        lambda v: v < 0 or v > 200,               # VIX 0-200 realistic
    "rates":      lambda v: v < -20 or v > 100,             # rates in pct
    "credit":     lambda v: v < -100 or v > 10000,          # spreads in bps
    "fx":         lambda v: v <= 0,                          # FX rates positive
    "sentiment":  lambda v: v < -100 or v > 10000,          # index values
    "macro":      lambda v: False,                           # too heterogeneous
    "flows":      lambda v: False,                           # signed values ok
    "alternative": lambda v: False,                          # heterogeneous
    "systemic":   lambda v: v < -100 or v > 10000,
    "trade":      lambda v: False,
    "breadth":    lambda v: v < -100 or v > 10000,
    "earnings":   lambda v: False,
}


def _is_nan_or_inf(v: float) -> bool:
    """Return True if value is NaN or Infinity."""
    if v is None:
        return True
    try:
        return math.isnan(v) or math.isinf(v)
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------


def check_duplicates(engine: Engine, feature_filter: list[str] | None = None) -> list[AuditFinding]:
    """Check 1: Duplicate detection — same feature_id + obs_date with different values."""
    findings: list[AuditFinding] = []

    query = text("""
        SELECT fr.name, rs.obs_date, COUNT(*), MIN(rs.value), MAX(rs.value)
        FROM resolved_series rs
        JOIN feature_registry fr ON fr.id = rs.feature_id
        GROUP BY fr.name, rs.obs_date
        HAVING COUNT(*) > 1 AND MIN(rs.value) != MAX(rs.value)
        ORDER BY fr.name, rs.obs_date DESC
        LIMIT 500
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    for row in rows:
        name, obs_date, cnt, min_val, max_val = row
        if feature_filter and name not in feature_filter:
            continue
        findings.append(AuditFinding(
            check_type="duplicate",
            severity="critical",
            feature=name,
            description=(
                f"Duplicate values for {name} on {obs_date}: "
                f"{cnt} rows, min={min_val}, max={max_val}"
            ),
            evidence={
                "obs_date": str(obs_date),
                "count": cnt,
                "min_value": min_val,
                "max_value": max_val,
            },
            suggested_fix=(
                "Not a true duplicate -- resolved_series' unique index is "
                "(feature_id, obs_date, vintage_date), so these rows are "
                "distinct vintages. Investigate the source data; do not "
                "delete (see PR #688)."
            ),
        ))

    log.info("Duplicate check: {n} findings", n=len(findings))
    return findings


def check_stale_data(engine: Engine, feature_filter: list[str] | None = None) -> list[AuditFinding]:
    """Check 2: Stale data detection — features where latest obs_date is >7 days old."""
    findings: list[AuditFinding] = []

    query = text("""
        SELECT fr.name, fr.family, MAX(rs.obs_date) AS latest
        FROM feature_registry fr
        JOIN resolved_series rs ON rs.feature_id = fr.id
        GROUP BY fr.name, fr.family
        HAVING MAX(rs.obs_date) < CURRENT_DATE - 7
        ORDER BY MAX(rs.obs_date) ASC
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    for row in rows:
        name, family, latest = row
        if feature_filter and name not in feature_filter:
            continue
        days_stale = (date.today() - latest).days if latest else None
        # Monthly/weekly data is expected to be less frequent
        if family in ("macro", "trade", "alternative") and days_stale and days_stale <= 35:
            severity = "info"
        elif days_stale and days_stale > 30:
            severity = "critical"
        else:
            severity = "warning"

        findings.append(AuditFinding(
            check_type="stale",
            severity=severity,
            feature=name,
            description=(
                f"{name} ({family}) latest data is {days_stale} days old "
                f"(last obs: {latest})"
            ),
            evidence={
                "family": family,
                "latest_obs_date": str(latest),
                "days_stale": days_stale,
            },
            suggested_fix="Flag feature for re-pull; check if source is still active.",
        ))

    log.info("Stale data check: {n} findings", n=len(findings))
    return findings


def check_value_sanity(engine: Engine, feature_filter: list[str] | None = None) -> list[AuditFinding]:
    """Check 3: Value sanity — detect obviously wrong values."""
    findings: list[AuditFinding] = []

    # Check for NaN/Infinity first (Postgres stores them as 'NaN'/'Infinity')
    nan_query = text("""
        SELECT fr.name, rs.obs_date, rs.value
        FROM resolved_series rs
        JOIN feature_registry fr ON fr.id = rs.feature_id
        WHERE rs.value = 'NaN'::DOUBLE PRECISION
           OR rs.value = 'Infinity'::DOUBLE PRECISION
           OR rs.value = '-Infinity'::DOUBLE PRECISION
        ORDER BY rs.obs_date DESC
        LIMIT 200
    """)

    try:
        with engine.connect() as conn:
            nan_rows = conn.execute(nan_query).fetchall()

        for row in nan_rows:
            name, obs_date, value = row
            if feature_filter and name not in feature_filter:
                continue
            findings.append(AuditFinding(
                check_type="sanity",
                severity="critical",
                feature=name,
                description=f"NaN/Infinity value for {name} on {obs_date}",
                evidence={"obs_date": str(obs_date), "value": str(value)},
                suggested_fix="Remove NaN/Infinity values from resolved_series.",
            ))
    except Exception as exc:
        log.warning("NaN/Inf check query failed (may not be supported): {e}", e=str(exc))

    # Per-family sanity checks — sample recent data
    family_query = text("""
        SELECT fr.name, fr.family, rs.obs_date, rs.value
        FROM resolved_series rs
        JOIN feature_registry fr ON fr.id = rs.feature_id
        WHERE rs.obs_date >= CURRENT_DATE - 90
        ORDER BY rs.obs_date DESC
        LIMIT 50000
    """)

    with engine.connect() as conn:
        rows = conn.execute(family_query).fetchall()

    for row in rows:
        name, family, obs_date, value = row
        if feature_filter and name not in feature_filter:
            continue
        if _is_nan_or_inf(value):
            # Already caught by the NaN query above
            continue

        rule = _FAMILY_SANITY.get(family)
        if rule and rule(value):
            findings.append(AuditFinding(
                check_type="sanity",
                severity="warning",
                feature=name,
                description=(
                    f"Suspicious value for {name} ({family}): "
                    f"{value} on {obs_date}"
                ),
                evidence={
                    "obs_date": str(obs_date),
                    "value": value,
                    "family": family,
                },
                suggested_fix="Verify value against source; remove if erroneous.",
            ))

    log.info("Value sanity check: {n} findings", n=len(findings))
    return findings


def check_coverage_completeness(
    engine: Engine, feature_filter: list[str] | None = None,
) -> list[AuditFinding]:
    """Check 4: Coverage completeness — gaps, jumps, stuck data."""
    findings: list[AuditFinding] = []

    # Get features that should have daily (market) data
    query = text("""
        SELECT fr.id, fr.name, fr.family
        FROM feature_registry fr
        WHERE fr.family IN ('equity', 'vol', 'fx', 'commodity', 'crypto', 'credit')
    """)

    with engine.connect() as conn:
        features = conn.execute(query).fetchall()

    for fid, fname, family in features:
        if feature_filter and fname not in feature_filter:
            continue

        # Get last 90 days of data for this feature
        data_query = text("""
            SELECT obs_date, value
            FROM resolved_series
            WHERE feature_id = :fid
              AND obs_date >= CURRENT_DATE - 90
            ORDER BY obs_date ASC
        """)

        with engine.connect() as conn:
            rows = conn.execute(data_query, {"fid": fid}).fetchall()

        if len(rows) < 2:
            continue

        dates = [r[0] for r in rows]
        values = [r[1] for r in rows]

        # Sub-check 4a: Missing weekdays (Mon-Fri gaps for market data)
        if family != "crypto":  # crypto trades 7 days
            missing_weekdays = 0
            for i in range(1, len(dates)):
                gap = (dates[i] - dates[i - 1]).days
                if gap > 3:  # More than a long weekend
                    missing_weekdays += gap - 1
            if missing_weekdays > 10:
                findings.append(AuditFinding(
                    check_type="gap",
                    severity="warning",
                    feature=fname,
                    description=(
                        f"{fname}: ~{missing_weekdays} missing days "
                        f"in last 90 days"
                    ),
                    evidence={
                        "missing_weekday_count": missing_weekdays,
                        "date_range": f"{dates[0]} to {dates[-1]}",
                        "total_rows": len(rows),
                    },
                    suggested_fix="Check if source had outages; backfill missing dates.",
                ))

        # Sub-check 4b: Sudden jumps > 50% day-over-day
        for i in range(1, len(values)):
            prev_val = values[i - 1]
            curr_val = values[i]
            if prev_val == 0 or _is_nan_or_inf(prev_val) or _is_nan_or_inf(curr_val):
                continue
            pct_change = abs(curr_val - prev_val) / abs(prev_val)
            if pct_change > 0.5:
                findings.append(AuditFinding(
                    check_type="gap",
                    severity="warning",
                    feature=fname,
                    description=(
                        f"{fname}: {pct_change:.0%} jump on {dates[i]} "
                        f"({prev_val} -> {curr_val})"
                    ),
                    evidence={
                        "obs_date": str(dates[i]),
                        "prev_date": str(dates[i - 1]),
                        "prev_value": prev_val,
                        "curr_value": curr_val,
                        "pct_change": round(pct_change, 4),
                    },
                    suggested_fix="Verify this is a real move, not a data error.",
                ))
                break  # Only report first big jump per feature

        # Sub-check 4c: Constant values for > 30 days
        if len(values) >= 30:
            constant_streak = 1
            max_streak = 1
            for i in range(1, len(values)):
                if values[i] == values[i - 1]:
                    constant_streak += 1
                    max_streak = max(max_streak, constant_streak)
                else:
                    constant_streak = 1
            if max_streak > 30:
                findings.append(AuditFinding(
                    check_type="gap",
                    severity="warning",
                    feature=fname,
                    description=(
                        f"{fname}: constant value for {max_streak} consecutive days "
                        f"(possible stale/stuck data)"
                    ),
                    evidence={
                        "max_constant_streak": max_streak,
                        "constant_value": values[-1] if constant_streak == max_streak else None,
                    },
                    suggested_fix="Verify source is still updating; may need re-pull.",
                ))

    log.info("Coverage completeness check: {n} findings", n=len(findings))
    return findings


def check_entity_map_consistency(engine: Engine) -> list[AuditFinding]:
    """Check 5: Entity map consistency — verify every mapping actually works."""
    findings: list[AuditFinding] = []

    # Merge both mapping dicts
    combined: dict[str, str] = {}
    combined.update(SEED_MAPPINGS)
    combined.update(NEW_MAPPINGS_V2)

    # Load feature registry
    with engine.connect() as conn:
        fr_rows = conn.execute(
            text("SELECT id, name FROM feature_registry")
        ).fetchall()
    name_to_id = {r[1]: r[0] for r in fr_rows}

    # Load series_ids that have data in raw_series
    with engine.connect() as conn:
        raw_rows = conn.execute(
            text("SELECT DISTINCT series_id FROM raw_series WHERE pull_status = 'SUCCESS'")
        ).fetchall()
    raw_series_ids = {r[0] for r in raw_rows}

    # Load feature_ids that have data in resolved_series
    with engine.connect() as conn:
        resolved_rows = conn.execute(
            text("SELECT DISTINCT feature_id FROM resolved_series")
        ).fetchall()
    resolved_feature_ids = {r[0] for r in resolved_rows}

    for series_id, feature_name in combined.items():
        feature_id = name_to_id.get(feature_name)

        # Check: target feature exists in registry?
        if feature_id is None:
            findings.append(AuditFinding(
                check_type="mapping",
                severity="critical",
                feature=feature_name,
                description=(
                    f"Mapping target '{feature_name}' does not exist in feature_registry "
                    f"(series_id='{series_id}')"
                ),
                evidence={"series_id": series_id, "target_feature": feature_name},
                suggested_fix="Add feature to feature_registry or remove stale mapping.",
            ))
            continue

        # Check: source series_id has data in raw_series?
        has_raw = series_id in raw_series_ids

        # Check: resolved_series has data for this feature?
        has_resolved = feature_id in resolved_feature_ids

        if not has_raw and not has_resolved:
            findings.append(AuditFinding(
                check_type="mapping",
                severity="warning",
                feature=feature_name,
                description=(
                    f"Mapping '{series_id}' -> '{feature_name}': "
                    f"no raw data AND no resolved data"
                ),
                evidence={
                    "series_id": series_id,
                    "feature_id": feature_id,
                    "has_raw_data": False,
                    "has_resolved_data": False,
                },
                suggested_fix="Check if ingestion is configured for this source.",
            ))
        elif has_raw and not has_resolved:
            findings.append(AuditFinding(
                check_type="mapping",
                severity="warning",
                feature=feature_name,
                description=(
                    f"Mapping '{series_id}' -> '{feature_name}': "
                    f"has raw data but NO resolved data"
                ),
                evidence={
                    "series_id": series_id,
                    "feature_id": feature_id,
                    "has_raw_data": True,
                    "has_resolved_data": False,
                },
                suggested_fix="Run resolver/bulk_resolve for this feature.",
            ))

    log.info("Entity map consistency check: {n} findings", n=len(findings))
    return findings


def check_cross_source_agreement(
    engine: Engine, feature_filter: list[str] | None = None,
) -> list[AuditFinding]:
    """Check 6: Cross-source agreement — compare values from different sources."""
    findings: list[AuditFinding] = []

    # Find features with conflicting resolved rows
    query = text("""
        SELECT
            fr.name,
            fr.family,
            rs.obs_date,
            rs.value,
            rs.conflict_detail
        FROM resolved_series rs
        JOIN feature_registry fr ON fr.id = rs.feature_id
        WHERE rs.conflict_flag = TRUE
          AND rs.obs_date >= CURRENT_DATE - 30
        ORDER BY rs.obs_date DESC
        LIMIT 500
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    for row in rows:
        name, family, obs_date, winning_value, conflict_detail = row
        if feature_filter and name not in feature_filter:
            continue

        # Parse conflict detail if available
        detail_parsed: dict[str, Any] = {}
        if conflict_detail:
            try:
                detail_parsed = (
                    json.loads(conflict_detail)
                    if isinstance(conflict_detail, str)
                    else conflict_detail
                )
            except (json.JSONDecodeError, TypeError):
                pass

        sources = detail_parsed.get("sources", [])
        threshold = FAMILY_CONFLICT_THRESHOLDS.get(
            family or "", CONFLICT_THRESHOLD,
        )

        # Determine which source appears more accurate (lowest priority_rank)
        best_source = None
        if sources:
            sorted_sources = sorted(sources, key=lambda s: s.get("priority_rank", 999))
            best_source = sorted_sources[0].get("source_name", "unknown")

        max_divergence = 0.0
        for src in sources:
            if winning_value and winning_value != 0:
                div = abs(src.get("value", 0) - winning_value) / abs(winning_value)
                max_divergence = max(max_divergence, div)

        severity = "critical" if max_divergence > threshold * 3 else "warning"

        findings.append(AuditFinding(
            check_type="divergence",
            severity=severity,
            feature=name,
            description=(
                f"{name} on {obs_date}: sources disagree by {max_divergence:.2%} "
                f"(threshold={threshold:.2%}, family={family})"
            ),
            evidence={
                "obs_date": str(obs_date),
                "winning_value": winning_value,
                "max_divergence": round(max_divergence, 6),
                "threshold": threshold,
                "family": family,
                "sources": sources,
                "most_trusted_source": best_source,
            },
            suggested_fix=f"Trust {best_source} (highest priority); investigate other sources.",
        ))

    log.info("Cross-source agreement check: {n} findings", n=len(findings))
    return findings


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------


def _persist_findings(engine: Engine, findings: list[AuditFinding]) -> int:
    """Write findings to the resolution_audits table. Returns count inserted."""
    if not findings:
        return 0

    _ensure_table(engine)

    with engine.begin() as conn:
        for f in findings:
            conn.execute(
                text("""
                    INSERT INTO resolution_audits
                        (check_type, severity, feature, description, evidence)
                    VALUES (:ct, :sev, :feat, :desc, :ev)
                """),
                {
                    "ct": f.check_type,
                    "sev": f.severity,
                    "feat": f.feature,
                    "desc": f.description,
                    "ev": json.dumps(f.evidence),
                },
            )

    return len(findings)


def _load_latest_findings(engine: Engine, limit: int = 200) -> list[dict]:
    """Load the most recent audit findings from the DB."""
    _ensure_table(engine)

    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, check_type, severity, feature, description,
                       evidence, auto_fixed, created_at
                FROM resolution_audits
                ORDER BY created_at DESC
                LIMIT :lim
            """),
            {"lim": limit},
        ).fetchall()

    return [
        {
            "id": r[0],
            "check_type": r[1],
            "severity": r[2],
            "feature": r[3],
            "description": r[4],
            "evidence": r[5],
            "auto_fixed": r[6],
            "created_at": r[7].isoformat() if r[7] else None,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Auto-fix logic
# ---------------------------------------------------------------------------

# PR #688 review (2026-09-27): this module used to DELETE dedup "losers"
# here, keeping only the row with the best source_priority_used per
# (feature_id, obs_date). resolved_series' actual unique index is
# (feature_id, obs_date, vintage_date) -- every row sharing (feature_id,
# obs_date) with another is therefore ALWAYS a distinct vintage, never a
# true duplicate: an exact duplicate (same feature_id, obs_date,
# vintage_date, AND value) is impossible, the index forbids it.
# store/pit.py's FIRST_RELEASE and LATEST_AS_OF vintage policies read
# across exactly these rows; deleting all but the best-priority row per
# (feature_id, obs_date) collapses point-in-time history and, measured
# against production, would have destroyed the FIRST_RELEASE row for
# ~52% of eligible groups. There is therefore no delete path here at all
# anymore -- the predicate below is now a plain COUNT, used only to
# report the multi-vintage group's size (see the "duplicate" branch of
# auto_fix_issues below). (A prior revision of this comment described a
# `:od::date`-cast bind-parameter trap and an ambiguous bare `id` column
# in the now-removed delete predicate; that history is preserved in git
# blame, not repeated here since the predicate itself is gone. Note we
# deliberately do NOT carry that trap's fix into any new delete -- see
# the NaN branch below for why.)
_MULTI_VINTAGE_COUNT_PREDICATE = """
    feature_id = (
        SELECT id FROM feature_registry WHERE name = :fname
    )
    AND obs_date = :od ::date
"""

_RETRACTED_KEY_MATCH = """
    EXISTS (
        SELECT 1 FROM resolved_series_retractions rsr
        WHERE rsr.feature_id = resolved_series.feature_id
          AND rsr.obs_date = resolved_series.obs_date
          AND rsr.vintage_date = resolved_series.vintage_date
    )
"""

_MULTI_VINTAGE_COUNT_SQL = (
    "SELECT COUNT(*) FROM resolved_series WHERE " + _MULTI_VINTAGE_COUNT_PREDICATE
)
_MULTI_VINTAGE_RETRACTED_COUNT_SQL = (
    _MULTI_VINTAGE_COUNT_SQL + " AND " + _RETRACTED_KEY_MATCH
)


def _retractions_table_exists(conn: Any) -> bool:
    """True if resolved_series_retractions exists (PR #683's migration).

    No longer used to guard a delete (there is none, see the module note
    above) -- kept only so the "duplicate" branch of auto_fix_issues can
    report how many of a multi-vintage group's rows are also retracted,
    for operator context. Runs its own SAVEPOINT (PR #688 review: this
    helper previously ran its probe query directly on the caller's
    transaction, so a failure here -- e.g. insufficient privilege, or a
    concurrent DDL change -- would poison that whole transaction instead
    of being contained) so any failure here is treated as "table absent"
    without affecting the caller. Unqualified (relies on search_path),
    matching how every other query in this module addresses
    resolved_series itself -- a hardcoded "public." prefix would silently
    miss the table on a non-default search_path (e.g. a test's scratch
    schema).
    """
    try:
        with conn.begin_nested():
            return bool(
                conn.execute(
                    text("SELECT to_regclass(:table_name) IS NOT NULL"),
                    {"table_name": "resolved_series_retractions"},
                ).scalar()
            )
    except Exception:
        return False


def auto_fix_issues(
    engine: Engine,
    findings: list[AuditFinding],
    dry_run: bool = True,
) -> dict[str, Any]:
    """Attempt automatic fixes for common issues.

    Fixes applied:
    - Duplicate ("multi-vintage") groups: report-only, never deletes (PR
      #688 review, 2026-09-27) -- see the module note above
      _retractions_table_exists for why: rows sharing (feature_id,
      obs_date) are always distinct vintages, never true duplicates, and
      deleting them destroys point-in-time history.
    - Flag stale features for re-pull (log only)
    - NaN/Infinity values: report-only, never deletes (same PIT concern --
      see the "sanity"/NaN branch below).

    Parameters:
        engine: SQLAlchemy engine.
        findings: List of AuditFinding to attempt fixing.
        dry_run: If True, report what would be fixed without making changes.
            Note: the "duplicate" and NaN branches below are report-only
            regardless of dry_run -- there is no destructive path left to
            gate.

    Returns:
        dict with keys: duplicates_fixed, nan_removed, stale_flagged,
        dry_run, plus multi_vintage_groups_reported and
        nan_values_reported. duplicates_fixed and nan_removed are always
        0 (nothing is ever deleted); the *_reported counters carry the
        actual counts.
    """
    result: dict[str, Any] = {
        "duplicates_fixed": 0,
        "nan_removed": 0,
        "stale_flagged": 0,
        "dry_run": dry_run,
        "multi_vintage_groups_reported": 0,
        "nan_values_reported": 0,
        "details": [],
    }

    for f in findings:
        if f.check_type == "duplicate":
            # NOT a duplicate -- see the module note above
            # _retractions_table_exists. This is a multi-vintage group:
            # distinct (feature_id, obs_date, vintage_date) rows sharing
            # (feature_id, obs_date). Report-only: count the group's rows
            # and how many are already retracted, for operator context,
            # and delete nothing.
            obs_date = f.evidence.get("obs_date")
            if not obs_date:
                continue

            result["multi_vintage_groups_reported"] += 1
            try:
                with engine.connect() as conn:
                    params = {"fname": f.feature, "od": obs_date}
                    vintage_count = conn.execute(
                        text(_MULTI_VINTAGE_COUNT_SQL), params
                    ).scalar() or 0
                    retracted_count = 0
                    if _retractions_table_exists(conn):
                        retracted_count = conn.execute(
                            text(_MULTI_VINTAGE_RETRACTED_COUNT_SQL), params
                        ).scalar() or 0
                log.warning(
                    "Multi-vintage group for {f} on {d}: {n} resolved_series "
                    "row(s) ({ret} already retracted) -- report-only, "
                    "nothing deleted (distinct vintages, not duplicates; "
                    "see PR #688)",
                    f=f.feature, d=obs_date, n=vintage_count, ret=retracted_count,
                )
                result["details"].append(
                    f"Multi-vintage group for {f.feature} on {obs_date}: "
                    f"{vintage_count} row(s), {retracted_count} retracted "
                    f"(left in place, not deleted)"
                )
            except Exception as exc:
                log.warning(
                    "Could not count multi-vintage rows for {f} on {d}: {e}",
                    f=f.feature, d=obs_date, e=str(exc),
                )

        elif f.check_type == "sanity" and "NaN" in f.description:
            # Report-only, deliberately never deletes (PR #688 review,
            # 2026-09-27). This delete's WHERE clause matches every
            # resolved_series row for (feature_id, obs_date) with a
            # NaN/Infinity value -- it has no vintage_date filter, so on a
            # feature/date with more than one vintage (the normal case --
            # see the "duplicate" branch above) it could delete a
            # legitimate historical vintage rather than a bad value,
            # destroying point-in-time history exactly like the dedup
            # delete this same review fixed. It has also never actually
            # run in production: its date cast was written `:od::date`
            # (no space), which SQLAlchemy 2.0's bind-parameter regex
            # mis-parses -- silently binding a truncated parameter name so
            # "od" was never substituted and Postgres received the
            # literal text `:od::date`, a syntax error -- caught by the
            # bare except below and logged at error level, so this delete
            # has been silently failing every time it "ran". Deliberately
            # NOT fixing that cast to bring this delete back to life:
            # left disabled, report-only, until a vintage-scoped,
            # PIT-safe version is designed.
            obs_date = f.evidence.get("obs_date")
            if not obs_date:
                continue

            result["nan_values_reported"] += 1
            log.warning(
                "NaN/Infinity resolved_series value(s) reported for {f} on "
                "{d}: report-only, nothing deleted (see PR #688)",
                f=f.feature, d=obs_date,
            )
            result["details"].append(
                f"NaN/Infinity value(s) reported for {f.feature} on "
                f"{obs_date} (left in place, not deleted)"
            )

        elif f.check_type == "stale":
            result["stale_flagged"] += 1
            result["details"].append(
                f"Flagged stale: {f.feature} (last data: {f.evidence.get('latest_obs_date')})"
            )

    # Mark auto-fixed findings in the audit table. duplicates_fixed and
    # nan_removed are always 0 now (PR #688: both branches are
    # report-only, see above) so this never fires for "duplicate"/"sanity"
    # today -- left in place, unreachable-but-harmless, for any future
    # check_type that performs a real fix.
    if not dry_run:
        fixed_types = set()
        if result["duplicates_fixed"] > 0:
            fixed_types.add("duplicate")
        if result["nan_removed"] > 0:
            fixed_types.add("sanity")

        if fixed_types:
            _ensure_table(engine)
            with engine.begin() as conn:
                for ct in fixed_types:
                    conn.execute(
                        text("""
                            UPDATE resolution_audits
                            SET auto_fixed = TRUE
                            WHERE check_type = :ct
                              AND auto_fixed = FALSE
                              AND created_at >= NOW() - INTERVAL '1 hour'
                        """),
                        {"ct": ct},
                    )

    log.info(
        "Auto-fix results (dry_run={dr}): duplicates_fixed={d}, nan_removed={n}, "
        "stale={s}, multi_vintage_groups_reported={mv}, nan_values_reported={nr}",
        dr=dry_run,
        d=result["duplicates_fixed"],
        n=result["nan_removed"],
        s=result["stale_flagged"],
        mv=result["multi_vintage_groups_reported"],
        nr=result["nan_values_reported"],
    )
    return result


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def run_full_audit(engine: Engine) -> dict[str, Any]:
    """Run all six audit checks. Returns findings grouped by severity.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict with keys: findings (list), summary (counts by severity/type),
        persisted_count.
    """
    _ensure_table(engine)
    log.info("Starting full resolution audit")

    all_findings: list[AuditFinding] = []

    checks = [
        ("duplicates", check_duplicates),
        ("stale_data", check_stale_data),
        ("value_sanity", check_value_sanity),
        ("coverage", check_coverage_completeness),
        ("entity_map", check_entity_map_consistency),
        ("cross_source", check_cross_source_agreement),
    ]

    check_results: dict[str, int] = {}
    for name, fn in checks:
        try:
            if name == "entity_map":
                results = fn(engine)
            else:
                results = fn(engine)
            all_findings.extend(results)
            check_results[name] = len(results)
        except Exception as exc:
            log.error("Audit check '{c}' failed: {e}", c=name, e=str(exc))
            check_results[name] = -1

    # Group by severity
    by_severity: dict[str, int] = {"critical": 0, "warning": 0, "info": 0}
    for f in all_findings:
        by_severity[f.severity] = by_severity.get(f.severity, 0) + 1

    # Persist
    persisted = _persist_findings(engine, all_findings)

    summary = {
        "total_findings": len(all_findings),
        "by_severity": by_severity,
        "by_check": check_results,
        "persisted_count": persisted,
    }

    log.info(
        "Full audit complete: {total} findings "
        "(critical={c}, warning={w}, info={i})",
        total=len(all_findings),
        c=by_severity["critical"],
        w=by_severity["warning"],
        i=by_severity["info"],
    )

    return {
        "findings": [asdict(f) for f in all_findings],
        "summary": summary,
    }


def audit_after_resolve(
    engine: Engine,
    features_resolved: list[str] | None = None,
) -> dict[str, Any]:
    """Run targeted audit on recently resolved features only.

    Called automatically after every resolver/bulk_resolve cycle.

    Parameters:
        engine: SQLAlchemy engine.
        features_resolved: List of feature names that were just resolved.
            If None, runs a lightweight version of all checks.

    Returns:
        dict with findings and summary, same structure as run_full_audit.
    """
    _ensure_table(engine)
    log.info(
        "Running post-resolve audit (features={n})",
        n=len(features_resolved) if features_resolved else "all",
    )

    all_findings: list[AuditFinding] = []

    # Run targeted checks (skip entity_map — that is a full-system check)
    targeted_checks = [
        ("duplicates", lambda: check_duplicates(engine, features_resolved)),
        ("stale_data", lambda: check_stale_data(engine, features_resolved)),
        ("value_sanity", lambda: check_value_sanity(engine, features_resolved)),
        ("coverage", lambda: check_coverage_completeness(engine, features_resolved)),
        ("cross_source", lambda: check_cross_source_agreement(engine, features_resolved)),
    ]

    check_results: dict[str, int] = {}
    for name, fn in targeted_checks:
        try:
            results = fn()
            all_findings.extend(results)
            check_results[name] = len(results)
        except Exception as exc:
            log.error("Post-resolve audit check '{c}' failed: {e}", c=name, e=str(exc))
            check_results[name] = -1

    by_severity: dict[str, int] = {"critical": 0, "warning": 0, "info": 0}
    for f in all_findings:
        by_severity[f.severity] = by_severity.get(f.severity, 0) + 1

    persisted = _persist_findings(engine, all_findings)

    summary = {
        "total_findings": len(all_findings),
        "by_severity": by_severity,
        "by_check": check_results,
        "persisted_count": persisted,
    }

    log.info(
        "Post-resolve audit complete: {total} findings "
        "(critical={c}, warning={w}, info={i})",
        total=len(all_findings),
        c=by_severity["critical"],
        w=by_severity["warning"],
        i=by_severity["info"],
    )

    return {
        "findings": [asdict(f) for f in all_findings],
        "summary": summary,
    }


def get_latest_audit_results(engine: Engine, limit: int = 200) -> dict[str, Any]:
    """Load the most recent audit findings from the database.

    Parameters:
        engine: SQLAlchemy engine.
        limit: Maximum number of findings to return.

    Returns:
        dict with findings list and summary counts.
    """
    _ensure_table(engine)
    findings = _load_latest_findings(engine, limit=limit)

    by_severity: dict[str, int] = {"critical": 0, "warning": 0, "info": 0}
    by_check: dict[str, int] = {}
    for f in findings:
        sev = f.get("severity", "info")
        by_severity[sev] = by_severity.get(sev, 0) + 1
        ct = f.get("check_type", "unknown")
        by_check[ct] = by_check.get(ct, 0) + 1

    return {
        "findings": findings,
        "summary": {
            "total_findings": len(findings),
            "by_severity": by_severity,
            "by_check": by_check,
        },
    }
