"""Run one profiled PostgreSQL module in a pre-owned disposable local database.

This runner never creates or drops a database. Provision one separately and set
its comment to ``grid_ci_disposable_v1:<owner-token>:<database-name>``. The token
and URL are supplied through environment variables, never output artifacts.
The module's fixture owns any schema bootstrap and cleanup inside that database.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import SQLAlchemyError

if __package__:
    from scripts.ci_pg_isolation_plan import LOCAL_HOSTS, URL_VARIABLES
    from scripts.ci_pg_test_fidelity import _allowed
else:
    from ci_pg_isolation_plan import LOCAL_HOSTS, URL_VARIABLES
    from ci_pg_test_fidelity import _allowed


PILOT_MODULE = "tests/test_watchlist_overview_readonly_pg.py"
TOKEN_PATTERN = re.compile(r"[a-f0-9]{32,64}\Z")
DB_PATTERN = re.compile(r"grid_ci_[a-z0-9_]+\Z")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source_sha(path: Path) -> str:
    # Match the plan generator's read_text()/encode() normalization on Windows.
    return hashlib.sha256(path.read_text(encoding="utf-8").encode("utf-8")).hexdigest()


def _entry(plan: dict[str, object], module: str, repo_root: Path) -> dict[str, object]:
    if plan.get("version") != 1 or plan.get("module_count") != len(plan.get("modules", [])):
        raise ValueError("malformed isolation plan")
    if plan.get("status") != "plan_only_no_database_created_or_test_executed":
        raise ValueError("isolation plan has unexpected status")
    if plan.get("host") not in LOCAL_HOSTS or not isinstance(plan.get("port"), int):
        raise ValueError("isolation plan is not local")
    matches = [entry for entry in plan["modules"] if entry.get("module") == module]
    if len(matches) != 1 or module != PILOT_MODULE:
        raise ValueError("module is not the selected pilot")
    entry = matches[0]
    if entry.get("profile", {}).get("profile") != "module_owned_schema":
        raise ValueError("pilot requires module-owned schema profile")
    if (entry["profile"].get("bootstrap") != "module_creates_unique_schema_and_tables"
            or entry["profile"].get("destructive_scope") != "module_unique_schema"):
        raise ValueError("pilot has an unexpected bootstrap or destructive scope")
    source = (repo_root / module).resolve()
    if not source.is_relative_to(repo_root / "tests") or not source.is_file():
        raise ValueError("pilot module is outside tests or missing")
    if entry.get("source_sha256") != _source_sha(source):
        raise ValueError("pilot source differs from isolation plan")
    prefix = plan.get("database_prefix")
    expected_db = f"{prefix}_{hashlib.sha256(module.encode()).hexdigest()[:12]}"
    if not isinstance(prefix, str) or entry.get("database_name") != expected_db:
        raise ValueError("pilot database identity differs from isolation plan allocation")
    return entry


def _database_identity(url: str, plan: dict[str, object], entry: dict[str, object],
                       token: str) -> dict[str, object]:
    try:
        parsed = make_url(url)
    except SQLAlchemyError:
        raise ValueError("malformed disposable database URL") from None
    database = entry["database_name"]
    if parsed.drivername not in ("postgresql", "postgresql+psycopg2", "postgresql+psycopg"):
        raise ValueError("pilot requires PostgreSQL URL")
    if (parsed.host not in LOCAL_HOSTS or parsed.host != plan["host"]
            or parsed.port != plan["port"] or parsed.database != database
            or not isinstance(database, str) or not DB_PATTERN.fullmatch(database)):
        raise ValueError("database URL does not match local disposable plan identity")
    if parsed.query or not parsed.username or not parsed.password:
        raise ValueError("database URL needs credentials and no override query")
    if not TOKEN_PATTERN.fullmatch(token):
        raise ValueError("disposable owner token must be 32-64 lowercase hex characters")
    expected_comment = f"grid_ci_disposable_v1:{token}:{database}"
    try:
        engine = create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 3})
    except SQLAlchemyError:
        raise ValueError("disposable database identity check failed") from None
    try:
        try:
            with engine.connect() as conn:
                row = conn.execute(text("""
                    SELECT current_database(), current_user, d.oid, r.rolname,
                           shobj_description(d.oid, 'pg_database'),
                           d.datistemplate, d.datallowconn,
                           r.rolsuper, r.rolcreatedb
                    FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba
                    WHERE d.datname = current_database()
                """)).one()
        except SQLAlchemyError:
            raise ValueError("disposable database identity check failed") from None
        if (row[0] != database or row[1] != row[3] or row[4] != expected_comment
                or row[5] or not row[6] or row[7] or row[8]):
            raise ValueError("database ownership marker, owner, or identity mismatch")
        return {"database": row[0], "database_oid": row[2], "database_owner": row[3]}
    finally:
        engine.dispose()


def run_module(repo_root: Path, plan_path: Path, output_dir: Path, module: str,
               url: str, token: str, allowlist_path: Path) -> int:
    """Run the verified module and preserve process exit plus exact artifacts."""
    repo_root = repo_root.resolve()
    _allowed(allowlist_path)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    entry = _entry(plan, module, repo_root)
    identity = _database_identity(url, plan, entry, token)
    output_dir = output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError("output directory must be empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    for name in ("PGSERVICE", "PGSERVICEFILE", "PGHOSTADDR", "PGOPTIONS", "PGDATABASE",
                 "PGUSER", "PGPASSWORD", "PGHOST", "PGPORT", "DATABASE_URL",
                 "SQLALCHEMY_DATABASE_URL", "TEST_DATABASE_URL"):
        env.pop(name, None)
    for name in URL_VARIABLES:
        env[name] = url
    env.update({"DB_HOST": plan["host"], "DB_PORT": str(plan["port"]),
                "DB_NAME": identity["database"], "DB_USER": identity["database_owner"],
                "DB_PASSWORD": make_url(url).password,
                "PYTHONDONTWRITEBYTECODE": "1"})
    junit = output_dir / "junit.xml"
    report = output_dir / "exact.json"
    discovery = output_dir / "discovery.json"
    audit = output_dir / "audit.json"
    pytest_cmd = [sys.executable, "-m", "pytest", module, "-q", "-p", "no:cacheprovider",
                  "-p", "scripts.ci_pytest_report", f"--junitxml={junit}",
                  f"--ci-report={report}", f"--basetemp={output_dir / 'pytest-tmp'}"]
    pytest_run = subprocess.run(pytest_cmd, cwd=repo_root, env=env, check=False)
    discover_run = subprocess.run(
        [sys.executable, str(repo_root / "scripts" / "ci_pg_test_fidelity.py"),
         "discover", "--tests-root", module, "--output", str(discovery)],
        cwd=repo_root, env=env, check=False,
    )
    audit_run = None
    if discover_run.returncode == 0 and junit.is_file() and report.is_file():
        audit_run = subprocess.run(
            [sys.executable, str(repo_root / "scripts" / "ci_pg_test_fidelity.py"),
             "check-skips", "--junit", str(junit), "--report", str(report),
             "--discovery", str(discovery), "--allowlist", str(allowlist_path),
             "--output", str(audit)],
            cwd=repo_root, env=env, check=False,
        )
    artifact_hashes = {name: _sha(path) for name, path in
                       (("junit", junit), ("report", report), ("discovery", discovery),
                        ("audit", audit)) if path.is_file()}
    result = {
        "version": 1, "module": module, "module_sha256": entry["source_sha256"],
        "plan_sha256": _sha(plan_path), "allowlist_sha256": _sha(allowlist_path),
        "database": identity, "artifacts": artifact_hashes,
        "pytest_process_exit": pytest_run.returncode,
        "discovery_process_exit": discover_run.returncode,
        "audit_process_exit": audit_run.returncode if audit_run else None,
        "ok": pytest_run.returncode == 0 and discover_run.returncode == 0
              and audit_run is not None and audit_run.returncode == 0,
    }
    (output_dir / "run.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    if pytest_run.returncode:
        return pytest_run.returncode
    return 0 if result["ok"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--module", default=PILOT_MODULE)
    parser.add_argument("--allowlist", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        url = os.environ["GRID_CI_DISPOSABLE_DB_URL"]
        token = os.environ["GRID_CI_DISPOSABLE_OWNER_TOKEN"]
        return run_module(args.repo_root, args.plan, args.output_dir,
                          args.module, url, token, args.allowlist)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f"CI PostgreSQL pilot refused: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
