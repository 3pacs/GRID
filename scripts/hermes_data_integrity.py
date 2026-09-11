"""
GRID Hermes Operator — data-integrity watchdogs and self-healing checks.

Every pipeline failure found on 2026-09-11 had the same shape: something that
should have been writing stopped, and nothing said so. `resolved_series` had no
writer for five months. The alembic step was swallowed by `|| echo`. The
`analytical_snapshots` FTS trigger was dropped and search died with nothing
erroring. Unmapped series scrolled past as log lines nobody aggregated.

None of those are exotic. They are all "the thing is quiet, and quiet looks
exactly like healthy". This module gives Hermes the checks that make quiet
loud, so the daemon notices them on the next cycle instead of a human noticing
them a quarter later:

  - check_writer_freshness   — a table that should be growing isn't
  - register_ghost_features  — entity_map names with no feature_registry row
  - report_unmapped_series   — unmapped series_ids aggregated, not logged away
  - collect_failed_ranges    — backfill ranges recorded and never retried

Each returns a plain dict for the Hermes action log, and logs an operator issue
rather than raising, so one failing check never takes down a cycle.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger as log

from scripts.hermes_health import log_issue


# Tables whose silence is a bug, with how long they may plausibly go quiet.
#
# `signal` names the column that advances when a row is written. Most GRID
# tables predate any notion of a write timestamp — `resolved_series` has
# obs_date, release_date and vintage_date but nothing saying *when the row was
# inserted*, which is precisely why its five-month stall was invisible. Where
# that is the case the BIGSERIAL primary key is the progress counter: it only
# ever goes up, and comparing it against the previous cycle answers "did
# anything get written" without needing a schema change.
EXPECTED_WRITERS: dict[str, dict[str, Any]] = {
    "resolved_series": {
        "signal": "id",
        "max_quiet_hours": 6,
        "writer": "normalization.resolver via the Hermes resolution step",
        "why": (
            "resolved_series is what every PIT consumer reads. It had no writer "
            "from 2026-04-04 to 2026-09-11 and nothing noticed."
        ),
    },
    "regime_history": {
        "signal": "id",
        "max_quiet_hours": 48,
        "writer": "scripts.auto_regime",
        "why": "A stalled regime writer makes every surface show a stale regime as current.",
    },
    "analytical_snapshots": {
        "signal": "id",
        "max_quiet_hours": 12,
        "writer": "api.routers.flows sector snapshot persist",
        "why": (
            "Writes failed silently for a day in 2026-09 behind a trigger that "
            "named columns the table lacks."
        ),
    },
}


def _ensure_watermark_table(engine: Any) -> None:
    """Create the writer-watermark table if it is missing.

    Follows the same shape as ``_ensure_issues_table``: cheap, idempotent, and
    safe to call on every cycle.
    """
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS hermes_writer_watermarks (
                table_name   TEXT PRIMARY KEY,
                signal_value BIGINT NOT NULL,
                observed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))


def check_writer_freshness(
    engine: Any,
    table: str | None = None,
    cycle_number: int | None = None,
) -> dict[str, Any]:
    """Report any expected-writer table that has stopped advancing.

    Reads each table's progress signal, compares it with the value stored on a
    previous cycle, and logs an operator issue when a table has been quiet
    longer than it should be. The watermark is then updated, so the next cycle
    measures from here.

    A first observation can only record the baseline — there is nothing to
    compare against yet — so it reports ``status="baseline"`` rather than
    inventing a verdict.

    Args:
        engine: SQLAlchemy engine for the GRID database.
        table: Restrict to one table. Must be a key of ``EXPECTED_WRITERS``;
            anything else is refused rather than interpolated, since a table
            name cannot travel as a bound parameter.
        cycle_number: Hermes cycle number, recorded on any issue logged.

    Returns:
        dict with one entry per table checked.
    """
    from sqlalchemy import text

    if table is not None and table not in EXPECTED_WRITERS:
        # Never interpolate an arbitrary name into SQL. The allowlist is the
        # validation, and an unknown name is an error, not a silent no-op.
        return {
            "status": "error",
            "error": f"{table!r} is not a declared expected-writer table",
            "known": sorted(EXPECTED_WRITERS),
        }

    targets = [table] if table else sorted(EXPECTED_WRITERS)
    _ensure_watermark_table(engine)
    now = datetime.now(timezone.utc)
    results: dict[str, Any] = {}

    for name in targets:
        spec = EXPECTED_WRITERS[name]
        # `name` and `signal` come from the module-level declaration above, not
        # from any caller, so this f-string cannot carry untrusted input.
        signal_sql = f"SELECT max({spec['signal']}) FROM {name}"  # noqa: S608
        try:
            with engine.begin() as conn:
                current = conn.execute(text(signal_sql)).scalar()
                prior = conn.execute(
                    text(
                        "SELECT signal_value, observed_at "
                        "FROM hermes_writer_watermarks WHERE table_name = :t"
                    ),
                    {"t": name},
                ).fetchone()
                conn.execute(
                    text("""
                        INSERT INTO hermes_writer_watermarks
                            (table_name, signal_value, observed_at)
                        VALUES (:t, :v, :o)
                        ON CONFLICT (table_name) DO UPDATE
                            SET signal_value = EXCLUDED.signal_value,
                                observed_at  = EXCLUDED.observed_at
                    """),
                    {"t": name, "v": int(current or 0), "o": now},
                )
        except Exception as exc:
            log.warning("Writer freshness check failed for {t}: {e}", t=name, e=str(exc))
            results[name] = {"status": "error", "error": str(exc)[:300]}
            continue

        if prior is None:
            results[name] = {"status": "baseline", "signal": int(current or 0)}
            continue

        prior_value, prior_at = int(prior[0]), prior[1]
        quiet_h = (now - prior_at).total_seconds() / 3600.0
        advanced = int(current or 0) - prior_value

        if advanced > 0:
            results[name] = {
                "status": "ok", "advanced_by": advanced,
                "quiet_hours": round(quiet_h, 2),
            }
            continue

        results[name] = {
            "status": "stalled" if quiet_h >= spec["max_quiet_hours"] else "quiet",
            "advanced_by": 0,
            "quiet_hours": round(quiet_h, 2),
            "max_quiet_hours": spec["max_quiet_hours"],
        }
        if quiet_h >= spec["max_quiet_hours"]:
            log_issue(
                engine,
                category="data_integrity",
                severity="ERROR",
                title=f"{name} has not been written for {quiet_h:.1f}h",
                detail=(
                    f"Expected writer: {spec['writer']}. "
                    f"Threshold {spec['max_quiet_hours']}h. {spec['why']}"
                ),
                source=name,
                cycle_number=cycle_number,
            )
    return {"status": "ok", "tables": results}


def register_ghost_features(
    engine: Any,
    cycle_number: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Register entity_map targets that have no feature_registry row.

    A "ghost mapping" is a name the entity map resolves to which the registry
    has never heard of. Every observation for it is silently dropped: the
    resolver logs one warning per series_id and counts the rest. `shy_full` and
    `mub_full` were both found this way, months after the mappings landed.

    Registering the missing row is unambiguous — the mapping already asserts the
    feature is wanted — so this acts rather than merely reporting.

    Args:
        engine: SQLAlchemy engine.
        cycle_number: Hermes cycle number, recorded on any issue logged.
        dry_run: Report what would be registered without writing.

    Returns:
        dict naming the features registered (or that would be).
    """
    from sqlalchemy import text

    try:
        from normalization.entity_map import EntityMap
        mapped = set(EntityMap(db_engine=engine).get_all_mappings().values())
    except Exception as exc:
        # An entity map that will not load is its own problem; say so plainly
        # rather than reporting "no ghosts found", which would read as healthy.
        log.warning("Ghost-feature scan could not load the entity map: {e}", e=str(exc))
        return {"status": "error", "error": str(exc)[:300]}

    with engine.begin() as conn:
        known = {
            r[0] for r in conn.execute(text("SELECT name FROM feature_registry"))
        }
    ghosts = sorted(mapped - known)
    if not ghosts:
        return {"status": "ok", "ghosts": [], "registered": 0}

    if dry_run:
        return {"status": "ok", "ghosts": ghosts, "registered": 0, "dry_run": True}

    registered: list[str] = []
    for feature in ghosts:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO feature_registry
                            (name, family, description, transformation,
                             normalization, missing_data_policy, eligible_from_date)
                        VALUES (:n, 'unclassified',
                                'Auto-registered by Hermes: mapped in entity_map '
                                'with no registry row, so every observation was '
                                'being dropped.',
                                'RAW', 'ZSCORE', 'FORWARD_FILL', '2020-01-01')
                        ON CONFLICT (name) DO NOTHING
                    """),
                    {"n": feature},
                )
            registered.append(feature)
        except Exception as exc:
            log.warning("Could not register ghost feature {f}: {e}", f=feature, e=str(exc))

    if registered:
        log_issue(
            engine,
            category="data_integrity",
            severity="WARNING",
            title=f"Registered {len(registered)} ghost-mapped feature(s)",
            detail=(
                "These were mapped in entity_map with no feature_registry row, so "
                "their observations were being dropped: " + ", ".join(registered)
                + ". Registered as family 'unclassified' — set the real family."
            ),
            source="entity_map",
            cycle_number=cycle_number,
        )
    return {"status": "ok", "ghosts": ghosts, "registered": len(registered),
            "names": registered}


def report_unmapped_series(
    engine: Any,
    cycle_number: int | None = None,
    limit: int = 25,
) -> dict[str, Any]:
    """Aggregate the series_ids a resolution pass skipped for having no mapping.

    ``EntityMap.missing_feature_report()`` already counts these — the resolver
    calls it once at the end of a pass — but the result only ever reached a log
    line, which scrolls away and is nobody's job. Recording it as an operator
    issue turns a backlog that silently grows into one that can be worked down.
    4,984 series_ids were being skipped per chunk when this was written, and
    ~19,459 ``ais:*`` ids have no entity_map entry at all.

    This reports; it does not guess mappings. What an unmapped series *should*
    map to is a judgement about what the data is for, and that belongs to the
    operator or the mapping_steward subagent, not to a fixer that runs
    unattended.

    Args:
        engine: SQLAlchemy engine.
        cycle_number: Hermes cycle number, recorded on any issue logged.
        limit: Cap on offenders named in the issue detail.

    Returns:
        dict carrying the miss counts and the worst offenders.
    """
    try:
        from normalization.entity_map import EntityMap

        emap = EntityMap(db_engine=engine)
        # Exercise the map over what has actually been pulled recently, so the
        # miss counters reflect live traffic rather than an empty instance.
        from sqlalchemy import text

        with engine.begin() as conn:
            conn.execute(text("SELECT set_config('statement_timeout','60000',true)"))
            recent = conn.execute(text("""
                SELECT DISTINCT rs.series_id
                FROM raw_series rs
                WHERE rs.pull_status = 'SUCCESS'
                  AND rs.pull_timestamp >= NOW() - make_interval(days => 2)
            """)).fetchall()
        for row in recent:
            emap.get_feature_id(row[0])
        report = emap.missing_feature_report()
    except Exception as exc:
        # A scan that cannot run must not read as "nothing is wrong".
        log.warning("Unmapped-series report failed: {e}", e=str(exc))
        return {"status": "error", "error": str(exc)[:300]}

    offenders = report.get("top_series", [])[: max(1, min(int(limit), 200))]
    if report.get("series_ids"):
        log_issue(
            engine,
            category="data_integrity",
            severity="WARNING",
            title=(
                f"{report['series_ids']} unmapped series_id(s) skipped at resolution"
            ),
            detail=(
                f"{report['lookups_missed']} lookup(s) missed over the last 2 days. "
                "These are pulled successfully and then dropped because entity_map "
                "has no entry. Worst offenders: "
                + ", ".join(f"{sid} ({n})" for sid, n in offenders)
                + (
                    "; mapped-but-unregistered: "
                    + ", ".join(report.get("unregistered_features", []))
                    if report.get("unregistered_features") else ""
                )
            ),
            source="entity_map",
            cycle_number=cycle_number,
        )
    return {"status": "ok", **report}


def collect_failed_ranges(summary: dict[str, Any]) -> list[dict[str, str]]:
    """Extract retryable ranges from a ``resolve_range`` summary.

    ``resolve_range`` records every window it could not finish in
    ``failed_ranges`` and walks on, which keeps one bad chunk from discarding a
    whole backfill — but nothing ever went back for them, so a run could report
    success having skipped its heaviest days.

    Kept deliberately small and pure so it can be tested without a database,
    and so the retry policy lives with the caller that owns the resolver.
    """
    ranges = summary.get("failed_ranges") or []
    return [
        {"since": r["since"], "until": r["until"]}
        for r in ranges
        if r.get("since") and r.get("until")
    ]
