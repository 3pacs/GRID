#!/usr/bin/env python3
"""Change ``feature_registry.model_eligible`` for core and astrological features, keeping a pre-image (#1301).

``model_eligible`` is a global switch read by the Oracle, inference, discovery, feature selection and the
auto_regime BUY gate. The migration ``eligibility_core_20261122`` only creates the change log
(``feature_eligibility_changes``); this operator script is the only thing that flips the flags.

Plan ``core_on_astro_off_20261122``: turn on 13 core market and flow features (among them vix_spot and
yld_curve_2s10s, which the retired ``scripts/fix_model_eligible.py`` switched off) and turn off ephemeris
rows and 22 astrological ones. Only rows that are not deprecated and whose flag differs are touched.

Modes (nothing is written without ``--apply``)::

    python scripts/apply_eligibility_change.py                         # dry run of the plan
    python scripts/apply_eligibility_change.py --apply                 # apply it as a new change set
    python scripts/apply_eligibility_change.py --rollback <uuid>       # dry run of rolling one set back
    python scripts/apply_eligibility_change.py --rollback <uuid> --apply

The database URL comes from the environment variable named by ``--db-url-env`` (default
``GRID_ELIGIBILITY_DB_URL``, which keeps it out of argv), else ``config.settings.DB_URL``. The script prints a
JSON report and exits 0, 2 when it refused, or 1 on an error.

**Prerequisite gate (apply only).** The plan changes what the Oracle and auto_regime act on, so ``--apply``
refuses unless every deployed code tree declares both contract constants as module-level string literals with
the exact values in ``PREREQUISITES``:

* ``oracle/engine.py``: ``SIGNAL_POLARITY_RULE = "oracle-signal-polarity-v1"``, added by the Oracle polarity
  fix (branch ``claude/fp-oracle-polarity-20261122``);
* ``scripts/auto_regime.py``: ``REGIME_HYSTERESIS_RULE = "auto-regime-own-bindings-hysteresis-v1"``, added by
  auto_regime's own declared bindings plus hysteresis (#1307).

The files are read with ``ast``; nothing is imported or executed. The trees checked are this script's own,
every ``--code-root``, and the ``WorkingDirectory`` of every unit in ``SERVICE_UNITS``, the deployed service
inventory, as systemd reports it (the scheduler and grid-realtime are pinned to exact releases). The gate needs
positive evidence: a host without systemctl, an inventory unit that is not loaded, a loaded unit whose tree
cannot be named, a missing file, a missing or non-literal constant, or a different value all refuse. Run
``--apply`` on the host that runs the GRID units; a remote caller is refused.

**Migration authority (every write).** Checked in ``pg_catalog`` on the open connection, never taken from a
flag. The connected role must own ``feature_eligibility_changes`` or be a member of its owner role, and must be
able to UPDATE ``feature_registry``. The application role ``grid`` must exist and must not be able to write a
pre-image: it may not be the owner or a member of it (a superuser is a member of every role), may not hold
INSERT, UPDATE, DELETE, TRUNCATE or TRIGGER on the log or USAGE/UPDATE on its sequence, and no trigger on
``feature_registry`` or the log may run a function it controls (a trigger runs with the privileges of the role
whose write fired it). When ``grid`` itself ran the migration it owns the log, and every write is refused until
a superuser runs the statements the report lists under ``ownership_fix_sql``.

**One change set per run.** An apply takes a transaction-scoped advisory lock, records each changed row's
pre-image under a new ``change_set`` uuid and flips the flag in the same statement, reads the set back and
compares it with what it changed. A run that finds nothing to change records nothing.

**Scoped rollback.** ``--rollback X`` restores only change set X's rows. Each row must still be what X left:
the feature id exists, its name is the recorded name, its flag equals X's recorded new value, and no later
change set that is still in effect changed it. If any row diverges the whole rollback is refused and the
divergent rows are reported; nothing is partly restored. A set can be rolled back once, a rollback set cannot
itself be rolled back (apply again instead), and the rollback is recorded as its own change set
(``action = 'rollback'``, ``rolls_back = X``). The prerequisite gate does not apply to a rollback.

**Bounded waits and blockers.** Every run sets ``lock_timeout`` (``--lock-timeout``, default 5 s) and
``statement_timeout`` (``--statement-timeout``, default 60 s) for its transaction. Before a write takes any lock,
it lists other sessions holding or waiting for anything stronger than a read lock on ``feature_registry`` or the
log, or holding this script's advisory lock, and refuses while any exists. A dry run reports them under
``blockers``. A lock wait that still runs out is reported as a refusal with the blockers at that moment, and
nothing is written.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import shutil
import subprocess  # nosec B404 - fixed argv to systemctl, no shell
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError

ROOT = Path(__file__).resolve().parents[1]

APP_ROLE = "grid"
DB_URL_ENV = "GRID_ELIGIBILITY_DB_URL"
# Serializes apply and rollback runs (pg_advisory_xact_lock, released at commit or rollback).
ADVISORY_LOCK_KEY = 130120261122
LOG_TRIGGER_FUNCTION = "feature_eligibility_changes_append_only"


@dataclass(frozen=True)
class Plan:
    name: str
    enable: tuple[str, ...]
    disable: tuple[str, ...]
    disable_prefix: str


PLAN = Plan(
    name="core_on_astro_off_20261122",
    enable=(
        "vix_spot", "vix9d_spot", "vix3m_spot", "vvix", "implied_correlation_3m",
        "yld_curve_2s10s", "yld_curve_3m10y", "fed_funds_rate",
        "finra_dix_sp500", "finra_short_volume_ratio_market",
        "finra_short_volume_ratio_spy", "finra_short_volume_ratio_qqq",
        "finra_short_volume_ratio_iwm",
    ),
    disable=(
        "days_to_full_moon", "days_to_new_moon", "jupiter_saturn_angle",
        "lunar_eclipse_proximity", "lunar_illumination", "lunar_phase",
        "mars_volatility_index", "mercury_retrograde", "planetary_stress_index",
        "solar_eclipse_proximity", "venus_cycle_phase", "chinese_element",
        "chinese_lunar_month", "chinese_yin_yang", "chinese_zodiac_year",
        "dasha_cycle_phase", "feng_shui_flying_star", "iching_hexagram_of_day",
        "nakshatra_index", "nakshatra_quality", "rahu_ketu_axis", "tithi",
    ),
    disable_prefix="ephemeris",
)


@dataclass(frozen=True)
class Prerequisite:
    path: str
    constant: str
    expected: str
    provided_by: str


PREREQUISITES = (
    Prerequisite("oracle/engine.py", "SIGNAL_POLARITY_RULE", "oracle-signal-polarity-v1",
                 "the Oracle polarity fix (branch claude/fp-oracle-polarity-20261122)"),
    Prerequisite("scripts/auto_regime.py", "REGIME_HYSTERESIS_RULE", "auto-regime-own-bindings-hysteresis-v1",
                 "auto_regime's own declared bindings plus hysteresis (#1307)"),
)

# Units whose code reads model_eligible (directly or through the Oracle, inference, discovery or auto_regime).
# The deployed service inventory the gate needs positive evidence for: every long-running GRID unit that runs
# analysis code and so reads feature_registry.model_eligible (grid-svr, 2026-11-22). Each must be loaded with an
# absolute WorkingDirectory, and that tree must carry the prerequisites. grid-scheduler and grid-realtime are
# pinned to exact releases, so a deploy alone does not move them.
SERVICE_UNITS = (
    "grid-api.service", "grid-hermes.service", "grid-scheduler.service",
    "grid-coordinator.service", "grid-worker.service", "grid-intelligence.service",
    "grid-realtime.service",
)


class Refused(Exception):
    """A check failed; nothing was written."""

    def __init__(self, reasons: Sequence[str], **details: Any) -> None:
        super().__init__("; ".join(reasons))
        self.reasons = list(reasons)
        self.details = details


# ── prerequisite gate (no database) ─────────────────────────────────────────────────────────────────────────


def read_string_constant(path: Path, name: str) -> tuple[Optional[str], Optional[str]]:
    """Return ``(value, problem)`` for a module-level ``name = "literal"`` in ``path``, without importing it."""
    try:
        source = path.read_text(encoding="utf-8")
    except OSError as exc:
        return None, f"cannot read {path}: {exc.strerror or exc}"
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        return None, f"{path} does not parse: {exc.msg}"
    value: Optional[str] = None
    problem: Optional[str] = f"{name} is not defined at module level in {path}"
    for node in tree.body:  # the last top-level binding wins, as at import
        if isinstance(node, ast.Assign):
            targets, bound = node.targets, node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets, bound = [node.target], node.value
        else:
            continue
        if not any(isinstance(t, ast.Name) and t.id == name for t in targets):
            continue
        if isinstance(bound, ast.Constant) and isinstance(bound.value, str):
            value, problem = bound.value, None
        else:
            value, problem = None, f"{name} in {path} is not a string literal"
    return value, problem


UnitShow = Callable[[str], Optional[dict[str, str]]]


def systemctl_show(unit: str) -> Optional[dict[str, str]]:
    """LoadState, ActiveState and WorkingDirectory of ``unit``; None when this host has no systemctl."""
    exe = shutil.which("systemctl")
    if exe is None:
        return None
    proc = subprocess.run(  # nosec B603 - fixed argv, no shell
        [exe, "show", "--property=LoadState", "--property=ActiveState", "--property=WorkingDirectory", unit],
        capture_output=True, text=True, timeout=15, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"systemctl show {unit} exited {proc.returncode}: {proc.stderr.strip()}")
    return dict(line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line)


def code_roots(extra: Sequence[Path] = (), units: Sequence[str] = SERVICE_UNITS,
               show: Optional[UnitShow] = None) -> tuple[list[tuple[str, Path]], list[str], list[str]]:
    """Every deployed code tree: this script's, each ``extra`` and each inventory unit's. Returns (roots, problems, notes).

    The gate needs positive evidence. No systemctl on this host, or an inventory unit that is not loaded, is a
    problem (a refusal), never a note: a laptop with the right literals must not certify the production database.
    """
    show = show or systemctl_show
    roots: list[tuple[str, Path]] = [("this script's tree", ROOT)]
    roots += [("--code-root", Path(p)) for p in extra]
    problems: list[str] = []
    notes: list[str] = []
    for unit in units:
        try:
            props = show(unit)
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            problems.append(f"cannot ask systemd about {unit}: {exc}")
            continue
        if props is None:
            problems.append("systemctl is not available on this host, so the deployed service inventory cannot be "
                            "checked; run --apply on the host that runs the GRID units")
            break
        if props.get("LoadState") != "loaded":
            problems.append(f"{unit} is in the deployed service inventory but is "
                            f"{props.get('LoadState') or 'unknown'} here, so its code tree cannot be checked")
            continue
        directory = (props.get("WorkingDirectory") or "").lstrip("-!")
        if not directory.startswith("/"):
            problems.append(f"{unit} is loaded but has no absolute WorkingDirectory, so its code tree cannot be checked")
            continue
        roots.append((f"{unit} ({props.get('ActiveState') or 'unknown'})", Path(directory)))
    merged: dict[str, tuple[list[str], Path]] = {}
    for label, path in roots:
        key = str(path.resolve())
        merged.setdefault(key, ([], path))[0].append(label)
    return [(", ".join(labels), path) for labels, path in merged.values()], problems, notes


def check_prerequisites(roots: Sequence[tuple[str, Path]], problems: Sequence[str] = (),
                        notes: Sequence[str] = ()) -> dict[str, Any]:
    """The gate: every root must carry every PREREQUISITES constant with its exact value."""
    checks = []
    refusals = list(problems)
    for label, root in roots:
        for req in PREREQUISITES:
            found, problem = read_string_constant(root / req.path, req.constant)
            ok = problem is None and found == req.expected
            if problem is None and not ok:
                problem = f"{req.constant} in {root / req.path} is {found!r}, not {req.expected!r}"
            checks.append({"tree": str(root), "used_by": label, "file": req.path, "constant": req.constant,
                           "expected": req.expected, "found": found, "ok": ok})
            if not ok:
                refusals.append(f"{problem} (needs {req.provided_by})")
    if not roots:
        refusals.append("no code tree to check")
    return {"ok": not refusals, "checks": checks, "refusals": refusals, "notes": list(notes)}


def deployed_prerequisites(extra: Sequence[Path] = (), show: Optional[UnitShow] = None) -> dict[str, Any]:
    roots, problems, notes = code_roots(extra, show=show)
    return check_prerequisites(roots, problems, notes)


# ── migration authority (pg_catalog) ───────────────────────────────────────────────────────────────────────

AUTHORITY_SQL = text("""
SELECT pg_get_userbyid(c.relowner) AS owner,
       current_user::text AS connected_role,
       pg_has_role(current_user, c.relowner, 'MEMBER') AS connected_is_authority,
       coalesce(has_table_privilege(current_user, to_regclass('feature_registry'), 'UPDATE'), FALSE)
           AS connected_can_update_registry,
       EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :app) AS app_exists
FROM pg_class c
WHERE c.oid = to_regclass('feature_eligibility_changes')
""")

APP_PRIVILEGES_SQL = text("""
SELECT pg_has_role(:app, c.relowner, 'MEMBER') AS app_is_authority,
       array_remove(ARRAY[
           CASE WHEN has_table_privilege(:app, c.oid, 'INSERT') THEN 'INSERT' END,
           CASE WHEN has_table_privilege(:app, c.oid, 'UPDATE') THEN 'UPDATE' END,
           CASE WHEN has_table_privilege(:app, c.oid, 'DELETE') THEN 'DELETE' END,
           CASE WHEN has_table_privilege(:app, c.oid, 'TRUNCATE') THEN 'TRUNCATE' END,
           CASE WHEN has_table_privilege(:app, c.oid, 'TRIGGER') THEN 'TRIGGER' END,
           CASE WHEN has_sequence_privilege(:app, s.seq, 'USAGE') THEN 'SEQUENCE USAGE' END,
           CASE WHEN has_sequence_privilege(:app, s.seq, 'UPDATE') THEN 'SEQUENCE UPDATE' END
       ], NULL) AS app_write_privileges
FROM pg_class c
CROSS JOIN LATERAL (
    SELECT to_regclass(pg_get_serial_sequence('feature_eligibility_changes', 'id')) AS seq
) s
WHERE c.oid = to_regclass('feature_eligibility_changes')
""")

APP_CONTROLLED_TRIGGERS_SQL = text("""
SELECT c.relname::text AS table_name, t.tgname::text AS trigger_name, p.proname::text AS function_name,
       pg_get_userbyid(p.proowner) AS function_owner
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_proc p ON p.oid = t.tgfoid
WHERE NOT t.tgisinternal
  AND t.tgrelid IN (to_regclass('feature_registry'), to_regclass('feature_eligibility_changes'))
  AND pg_has_role(:app, p.proowner, 'MEMBER')
ORDER BY 1, 2
""")

OWNERSHIP_FIX_SQL = (
    "-- as a superuser on this database, once; grid_eligibility_owner is an example name (grid must not be a member)",
    "CREATE ROLE grid_eligibility_owner NOLOGIN;",
    "ALTER TABLE feature_eligibility_changes OWNER TO grid_eligibility_owner;  -- its id sequence moves with it",
    "ALTER FUNCTION feature_eligibility_changes_append_only() OWNER TO grid_eligibility_owner;",
    "REVOKE ALL ON feature_eligibility_changes FROM grid;",
    "REVOKE ALL ON SEQUENCE feature_eligibility_changes_id_seq FROM grid;",
    "GRANT SELECT ON feature_eligibility_changes TO grid;",
    "GRANT SELECT, UPDATE ON feature_registry TO grid_eligibility_owner;",
    "GRANT grid_eligibility_owner TO <an operator login role that is not grid>;",
)


def check_authority(conn: Connection, app_role: Optional[str] = None) -> dict[str, Any]:
    app_role = app_role or APP_ROLE
    row = conn.execute(AUTHORITY_SQL, {"app": app_role}).mappings().one_or_none()
    if row is None:
        return {"change_log_exists": False, "ok": False,
                "refusals": ["feature_eligibility_changes does not exist (run alembic upgrade head first)"]}
    report: dict[str, Any] = {"change_log_exists": True, **dict(row), "app_role": app_role}
    refusals: list[str] = []
    if not row["connected_is_authority"]:
        refusals.append(f"connected as {row['connected_role']}, which neither owns feature_eligibility_changes "
                        f"(owner {row['owner']}) nor is a member of its owner")
    if not row["connected_can_update_registry"]:
        refusals.append(f"{row['connected_role']} cannot UPDATE feature_registry")
    separation_broken = False
    if not row["app_exists"]:
        refusals.append(f"the application role {app_role} does not exist, so its privileges cannot be verified")
    else:
        app = conn.execute(APP_PRIVILEGES_SQL, {"app": app_role}).mappings().one()
        triggers = [dict(t) for t in conn.execute(APP_CONTROLLED_TRIGGERS_SQL, {"app": app_role}).mappings()]
        report.update(app_is_authority=app["app_is_authority"],
                      app_write_privileges=list(app["app_write_privileges"] or []),
                      app_controlled_triggers=triggers)
        if app["app_is_authority"]:
            separation_broken = True
            refusals.append(f"the application role {app_role} owns feature_eligibility_changes or is a member of "
                            f"its owner {row['owner']} (or is a superuser), so it could write a pre-image")
        if app["app_write_privileges"]:
            separation_broken = True
            refusals.append(f"the application role {app_role} holds {', '.join(app['app_write_privileges'])} "
                            "on feature_eligibility_changes or its sequence")
        if triggers:
            separation_broken = True
            refusals.append(f"{len(triggers)} trigger(s) on feature_registry or feature_eligibility_changes run a "
                            f"function {app_role} controls: "
                            + ", ".join(f"{t['table_name']}.{t['trigger_name']}" for t in triggers))
    if separation_broken:
        report["ownership_fix_sql"] = list(OWNERSHIP_FIX_SQL)
    report["ok"] = not refusals
    report["refusals"] = refusals
    return report


# ── apply ────────────────────────────────────────────────────────────────────────────────────────────────────

TIMEOUTS_SQL = text("SELECT set_config('lock_timeout', :lock, true), set_config('statement_timeout', :stmt, true)")

# ── bounded waits and blockers ──────────────────────────────────────────────────────────────────────────────
# Every run sets lock_timeout and statement_timeout for its own transaction (TIMEOUTS_SQL), so no wait is
# unbounded. Before a write takes any lock, BLOCKERS_SQL lists every other session that holds or waits for a
# lock stronger than AccessShareLock on feature_registry or the change log, or that holds this script's advisory
# lock. A write refuses while any exists ("retry when they finish") rather than queueing behind it. Plain reads
# (AccessShareLock) never block an apply. A lock wait that still runs out (a blocker that arrived after the
# preflight) becomes a refusal with the blockers at that moment, never a traceback.
LOCK_TIMEOUT_DEFAULT_S, STATEMENT_TIMEOUT_DEFAULT_S = 5, 60
BLOCKERS_SQL = text("""
SELECT l.pid, a.usename, a.application_name, a.state, l.locktype, l.mode, l.granted,
       coalesce(c.relname, '') AS relation,
       extract(epoch FROM (clock_timestamp() - a.xact_start))::int AS xact_seconds
FROM pg_locks l
LEFT JOIN pg_stat_activity a ON a.pid = l.pid
LEFT JOIN pg_class c ON c.oid = l.relation
WHERE l.pid <> pg_backend_pid()
  AND (
        (l.locktype = 'relation'
         AND l.relation IN (to_regclass('feature_registry'), to_regclass('feature_eligibility_changes'))
         AND l.mode <> 'AccessShareLock')
     OR (l.locktype = 'advisory' AND l.objsubid = 1
         AND ((l.classid::bigint << 32) | l.objid::bigint) = :key)
      )
ORDER BY l.pid, l.mode
""")
LOCK_WAIT_SQLSTATES = {"55P03": "lock_timeout", "57014": "statement_timeout"}


def find_blockers(conn: Connection) -> list[dict[str, Any]]:
    """Other sessions that would make a write wait, or that a write would queue behind."""
    return [dict(r) for r in conn.execute(BLOCKERS_SQL, {"key": ADVISORY_LOCK_KEY}).mappings()]
LOCK_SQL = text("SELECT pg_advisory_xact_lock(:key)")

PLANNED_SQL = text("""
SELECT id AS feature_id, name, model_eligible AS old_eligible, (name = ANY(:enable)) AS new_eligible
FROM feature_registry
WHERE deprecated_at IS NULL
  AND ((name = ANY(:enable) AND model_eligible IS DISTINCT FROM TRUE)
    OR ((name LIKE :prefix OR name = ANY(:disable)) AND model_eligible IS DISTINCT FROM FALSE))
ORDER BY id
""")

# One statement: lock the rows, record their pre-image under the new change set, flip them.
APPLY_SQL = text("""
WITH targets AS (
    SELECT id, name, model_eligible AS old_eligible, (name = ANY(:enable)) AS new_eligible
    FROM feature_registry
    WHERE deprecated_at IS NULL
      AND ((name = ANY(:enable) AND model_eligible IS DISTINCT FROM TRUE)
        OR ((name LIKE :prefix OR name = ANY(:disable)) AND model_eligible IS DISTINCT FROM FALSE))
    FOR UPDATE
), recorded AS (
    INSERT INTO feature_eligibility_changes
        (change_set, action, feature_id, name, old_eligible, new_eligible, plan)
    SELECT CAST(:change_set AS uuid), 'apply', id, name, old_eligible, new_eligible, :plan FROM targets
)
UPDATE feature_registry f SET model_eligible = t.new_eligible
FROM targets t WHERE f.id = t.id
RETURNING f.id AS feature_id, f.name, t.old_eligible, t.new_eligible
""")

SET_ROWS_SQL = text("""
SELECT feature_id, name, old_eligible, new_eligible, action, rolls_back::text AS rolls_back, plan, id
FROM feature_eligibility_changes
WHERE change_set = CAST(:change_set AS uuid)
ORDER BY feature_id
""")


def _rows(result) -> list[dict[str, Any]]:
    return sorted(({k: r[k] for k in ("feature_id", "name", "old_eligible", "new_eligible")}
                   for r in result.mappings()), key=lambda r: r["feature_id"])


def planned_changes(conn: Connection, plan: Plan = PLAN) -> list[dict[str, Any]]:
    return _rows(conn.execute(PLANNED_SQL, _plan_params(plan)))


def _plan_params(plan: Plan) -> dict[str, Any]:
    return {"enable": list(plan.enable), "disable": list(plan.disable), "prefix": plan.disable_prefix + "%"}


def apply_plan(conn: Connection, change_set: uuid.UUID, plan: Plan = PLAN) -> list[dict[str, Any]]:
    changed = _rows(conn.execute(APPLY_SQL, {**_plan_params(plan), "change_set": str(change_set), "plan": plan.name}))
    recorded = _rows(conn.execute(SET_ROWS_SQL, {"change_set": str(change_set)}))
    if recorded != changed:
        raise RuntimeError(f"change set {change_set}: recorded rows {recorded} differ from changed rows {changed}")
    return changed


# ── rollback ─────────────────────────────────────────────────────────────────────────────────────────────────

ROLLED_BACK_BY_SQL = text("""
SELECT DISTINCT change_set::text FROM feature_eligibility_changes WHERE rolls_back = CAST(:change_set AS uuid)
""")

CURRENT_SQL = text("SELECT id, name, model_eligible FROM feature_registry WHERE id = ANY(:ids)")
CURRENT_FOR_UPDATE_SQL = text("SELECT id, name, model_eligible FROM feature_registry WHERE id = ANY(:ids) FOR UPDATE")

# A later change set that changed the same feature and has not itself been rolled back.
LATER_LIVE_SQL = text("""
SELECT c.feature_id, c.change_set::text AS change_set
FROM feature_eligibility_changes c
WHERE c.feature_id = ANY(:ids)
  AND c.action = 'apply'
  AND c.change_set <> CAST(:change_set AS uuid)
  AND c.id > (SELECT max(id) FROM feature_eligibility_changes WHERE change_set = CAST(:change_set AS uuid))
  AND NOT EXISTS (SELECT 1 FROM feature_eligibility_changes r WHERE r.rolls_back = c.change_set)
ORDER BY c.id
""")

ROLLBACK_SQL = text("""
WITH original AS (
    SELECT feature_id, name, old_eligible, new_eligible, plan
    FROM feature_eligibility_changes
    WHERE change_set = CAST(:target AS uuid) AND action = 'apply'
), targets AS (
    SELECT f.id, f.name, f.model_eligible AS current_eligible, o.old_eligible AS restore_to, o.plan
    FROM feature_registry f
    JOIN original o ON o.feature_id = f.id AND o.name = f.name
    WHERE f.model_eligible IS NOT DISTINCT FROM o.new_eligible
    FOR UPDATE OF f
), recorded AS (
    INSERT INTO feature_eligibility_changes
        (change_set, action, rolls_back, feature_id, name, old_eligible, new_eligible, plan)
    SELECT CAST(:change_set AS uuid), 'rollback', CAST(:target AS uuid), id, name, current_eligible, restore_to, plan
    FROM targets
)
UPDATE feature_registry f SET model_eligible = t.restore_to
FROM targets t WHERE f.id = t.id
RETURNING f.id AS feature_id, f.name, t.current_eligible AS old_eligible, t.restore_to AS new_eligible
""")


def plan_rollback(conn: Connection, target: uuid.UUID, *, lock: bool) -> list[dict[str, Any]]:
    """Rows a rollback of ``target`` would restore (as old -> new). Raises Refused on any divergence."""
    rows = conn.execute(SET_ROWS_SQL, {"change_set": str(target)}).mappings().all()
    if not rows:
        raise Refused([f"no change set {target} is recorded"])
    if any(r["action"] != "apply" for r in rows):
        raise Refused([f"{target} is a rollback (of {rows[0]['rolls_back']}); run --apply again instead"])
    rolled_back_by = conn.execute(ROLLED_BACK_BY_SQL, {"change_set": str(target)}).scalars().all()
    if rolled_back_by:
        raise Refused([f"{target} was already rolled back by {', '.join(rolled_back_by)}"])
    ids = [r["feature_id"] for r in rows]
    current = {r.id: r for r in conn.execute(CURRENT_FOR_UPDATE_SQL if lock else CURRENT_SQL, {"ids": ids})}
    later: dict[int, list[str]] = {}
    for r in conn.execute(LATER_LIVE_SQL, {"ids": ids, "change_set": str(target)}):
        later.setdefault(r.feature_id, []).append(r.change_set)
    divergent = []
    for r in rows:
        now = current.get(r["feature_id"])
        reasons = []
        if now is None:
            reasons.append("the feature id no longer exists")
        else:
            if now.name != r["name"]:
                reasons.append(f"its name is now {now.name!r}")
            if now.model_eligible != r["new_eligible"]:
                reasons.append(f"its flag is {now.model_eligible}, not the {r['new_eligible']} this set left")
        if r["feature_id"] in later:
            reasons.append(f"later change set(s) {', '.join(later[r['feature_id']])} changed it and are still in "
                           "effect (roll those back first)")
        if reasons:
            divergent.append({"feature_id": r["feature_id"], "recorded_name": r["name"],
                              "recorded_new_eligible": r["new_eligible"],
                              "current_name": None if now is None else now.name,
                              "current_eligible": None if now is None else now.model_eligible,
                              "reasons": reasons})
    if divergent:
        raise Refused([f"{len(divergent)} of {len(rows)} row(s) of change set {target} diverged; nothing restored"],
                      divergent=divergent)
    return [{"feature_id": r["feature_id"], "name": r["name"], "old_eligible": r["new_eligible"],
             "new_eligible": r["old_eligible"]} for r in rows]


def apply_rollback(conn: Connection, target: uuid.UUID, change_set: uuid.UUID) -> list[dict[str, Any]]:
    expected = plan_rollback(conn, target, lock=True)
    restored = _rows(conn.execute(ROLLBACK_SQL, {"target": str(target), "change_set": str(change_set)}))
    recorded = _rows(conn.execute(SET_ROWS_SQL, {"change_set": str(change_set)}))
    if not (restored == recorded == expected):
        raise RuntimeError(f"rollback of {target}: restored {restored}, recorded {recorded}, expected {expected}")
    return restored


# ── run ──────────────────────────────────────────────────────────────────────────────────────────────────────


def run(engine: Engine, *, write: bool, rollback_of: Optional[uuid.UUID] = None,
        prerequisites: Optional[dict[str, Any]] = None, app_role: Optional[str] = None, plan: Plan = PLAN,
        lock_timeout: str = f"{LOCK_TIMEOUT_DEFAULT_S}s",
        statement_timeout: str = f"{STATEMENT_TIMEOUT_DEFAULT_S}s") -> dict[str, Any]:
    """One dry run, apply or rollback in one transaction. ``prerequisites`` is ``deployed_prerequisites()``."""
    mode = ("rollback" if rollback_of else "apply") + ("" if write else "_dry_run")
    report: dict[str, Any] = {"mode": mode, "plan": plan.name, "writes_to_database": False, "refusals": []}
    if rollback_of is None:
        if prerequisites is None:
            raise ValueError("an apply needs the prerequisite gate's result")
        report["prerequisites"] = prerequisites
        if write and not prerequisites["ok"]:
            report.update(status="refused", refusals=list(prerequisites["refusals"]))
            return report  # refused before any connection is opened
    else:
        report["rollback_of"] = str(rollback_of)
    would_refuse = [] if rollback_of is not None else list(prerequisites["refusals"])
    try:
        with engine.connect() as conn, conn.begin():
            if not write:
                conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(TIMEOUTS_SQL, {"lock": lock_timeout, "stmt": statement_timeout})
            blockers = find_blockers(conn)
            report["blockers"] = blockers
            if blockers:
                reason = (f"{len({b['pid'] for b in blockers})} other session(s) hold or wait for locks on "
                          "feature_registry or the change log (or this script's advisory lock); retry when they finish")
                if write:
                    raise Refused([reason])
                would_refuse.append(reason)
            if write:
                conn.execute(LOCK_SQL, {"key": ADVISORY_LOCK_KEY})
            authority = check_authority(conn, app_role)
            report["authority"] = authority
            if write and not authority["ok"]:
                raise Refused(authority["refusals"])
            would_refuse += authority["refusals"]
            if not authority["change_log_exists"]:
                raise Refused(authority["refusals"])
            if rollback_of is None:
                if not write:
                    report.update(status="dry_run", changes=planned_changes(conn, plan))
                else:
                    change_set = uuid.uuid4()
                    changes = apply_plan(conn, change_set, plan)
                    if changes:
                        report.update(status="applied", change_set=str(change_set), changes=changes,
                                      writes_to_database=True)
                    else:
                        report.update(status="nothing_to_change", changes=[])
            else:
                if not write:
                    report.update(status="dry_run", changes=plan_rollback(conn, rollback_of, lock=False))
                else:
                    change_set = uuid.uuid4()
                    report.update(status="rolled_back", change_set=str(change_set),
                                  changes=apply_rollback(conn, rollback_of, change_set), writes_to_database=True)
    except Refused as refused:
        report.update(status="refused", refusals=refused.reasons, **refused.details)
        report["writes_to_database"] = False
        return report
    except DBAPIError as exc:
        which = LOCK_WAIT_SQLSTATES.get(getattr(getattr(exc, "orig", None), "pgcode", None))
        if which is None:
            raise
        limit = lock_timeout if which == "lock_timeout" else statement_timeout
        with engine.connect() as conn:
            blockers = find_blockers(conn)
        report.update(status="refused", writes_to_database=False, blockers=blockers,
                      refusals=[f"a lock wait exceeded {which} {limit}; the transaction was rolled back and "
                                "nothing was written; retry when the blocking sessions finish"])
        return report
    if not write:
        report["would_refuse"] = would_refuse
    return report


def _bounded_seconds(low: int, high: int):
    def parse(raw: str) -> int:
        value = int(raw)
        if not low <= value <= high:
            raise argparse.ArgumentTypeError(f"must be between {low} and {high} seconds")
        return value
    return parse


def _redacted(url: str) -> str:
    from sqlalchemy.engine import make_url

    return make_url(url).render_as_string(hide_password=True)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run); with --rollback, roll back")
    ap.add_argument("--rollback", type=uuid.UUID, metavar="CHANGE_SET", help="roll back this change set")
    ap.add_argument("--code-root", type=Path, action="append", default=[],
                    help="another deployed code tree the prerequisite gate must also find (repeatable)")
    ap.add_argument("--lock-timeout", type=_bounded_seconds(1, 60), default=LOCK_TIMEOUT_DEFAULT_S,
                    help=f"seconds any lock may be waited for (1-60, default {LOCK_TIMEOUT_DEFAULT_S})")
    ap.add_argument("--statement-timeout", type=_bounded_seconds(10, 600), default=STATEMENT_TIMEOUT_DEFAULT_S,
                    help=f"seconds any statement may run (10-600, default {STATEMENT_TIMEOUT_DEFAULT_S})")
    ap.add_argument("--db-url-env", default=DB_URL_ENV,
                    help=f"environment variable holding the database URL (default {DB_URL_ENV}); "
                         "unset falls back to config.settings.DB_URL")
    args = ap.parse_args(argv)

    db_url = os.environ.get(args.db_url_env)
    if not db_url:
        from config import settings

        db_url = settings.DB_URL
    from sqlalchemy import create_engine

    prerequisites = None if args.rollback else deployed_prerequisites(args.code_root)
    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        report = run(engine, write=args.apply, rollback_of=args.rollback, prerequisites=prerequisites,
                     lock_timeout=f"{args.lock_timeout}s", statement_timeout=f"{args.statement_timeout}s")
    finally:
        engine.dispose()
    report["database"] = _redacted(db_url)
    print(json.dumps(report, indent=2, default=str))
    return 2 if report["status"] == "refused" else 0


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    sys.exit(main())
