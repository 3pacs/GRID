"""Build a source-backed, non-executing per-module PostgreSQL isolation plan.

This allocates database identities and route variables only. It never creates,
drops, connects to, or bootstraps a database. Profiles are source-review notes,
not a claim that a module passes on an empty database.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path


LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
URL_VARIABLES = (
    "GRID_TEST_DB_URL",
    "GRID_BRIEFING_ROUTE_TEST_DB_URL",
    "GRID_WATCHLIST_EDGE_TEST_DB_URL",
    "DB_URL",
)
CONFIG_VARIABLES = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
SQL_EFFECT = re.compile(r"\b(DROP SCHEMA|DROP TABLE|TRUNCATE|DELETE FROM|ALTER TABLE)\b", re.I)


def _profile_modules(profiles: dict[str, object]) -> dict[str, dict[str, str]]:
    if set(profiles) != {"version", "status", "profiles"} or profiles["version"] != 1:
        raise ValueError("unsupported or malformed isolation profile manifest")
    result: dict[str, dict[str, str]] = {}
    for profile_name, profile in profiles["profiles"].items():
        if set(profile) != {"bootstrap", "destructive_scope", "modules"}:
            raise ValueError(f"malformed profile {profile_name}")
        for module in profile["modules"]:
            if module in result:
                raise ValueError(f"duplicate module profile: {module}")
            result[module] = {
                "profile": profile_name,
                "bootstrap": profile["bootstrap"],
                "destructive_scope": profile["destructive_scope"],
            }
    return result


def build_plan(repo_root: Path, manifest_path: Path, host: str, port: int,
               db_prefix: str, discovery_path: Path | None = None) -> dict[str, object]:
    if host not in LOCAL_HOSTS:
        raise ValueError("database host must be literal localhost, 127.0.0.1, or ::1")
    if not 1 <= port <= 65535:
        raise ValueError("database port must be in 1..65535")
    if not re.fullmatch(r"grid_ci_[a-z0-9_]+", db_prefix) or len(db_prefix) > 49:
        raise ValueError("database prefix must be a short grid_ci_ test identity")
    repo_root = repo_root.resolve()
    profiles = json.loads(manifest_path.read_text(encoding="utf-8"))
    modules = _profile_modules(profiles)
    discovered_pg = {path.relative_to(repo_root).as_posix()
                     for path in (repo_root / "tests").glob("test_*pg.py")}
    unprofiled = sorted(discovered_pg - modules.keys())
    if unprofiled:
        raise ValueError(f"new PostgreSQL modules need profile review: {unprofiled}")
    if discovery_path is not None:
        discovery = json.loads(discovery_path.read_text(encoding="utf-8"))
        if discovery.get("version") != 1 or not isinstance(discovery.get("pg_tests"), list):
            raise ValueError("invalid PostgreSQL test discovery input")
        fixture_modules = {entry["nodeid"].split("::", 1)[0]
                           for entry in discovery["pg_tests"]
                           if "pg_filename" in entry["signals"]
                           or "pg_engine_fixture" in entry["signals"]}
        uncovered = sorted(fixture_modules - modules.keys())
        if uncovered:
            raise ValueError(f"collected PostgreSQL fixture modules need profile review: {uncovered}")
    entries = []
    for module, profile in sorted(modules.items()):
        path = (repo_root / module).resolve()
        if not path.is_relative_to(repo_root / "tests") or not path.is_file():
            raise ValueError(f"profiled module is missing or outside tests: {module}")
        source = path.read_text(encoding="utf-8")
        digest = hashlib.sha256(module.encode("utf-8")).hexdigest()[:12]
        db_name = f"{db_prefix}_{digest}"
        if len(db_name) > 63 or not db_name.startswith("grid_ci_"):
            raise ValueError(f"unsafe generated database name: {db_name}")
        url_host = f"[{host}]" if host == "::1" else host
        url = (f"postgresql://${{CI_DB_USER}}:${{CI_DB_PASSWORD}}"
               f"@{url_host}:{port}/{db_name}")
        env = {name: url for name in URL_VARIABLES}
        env.update({
            "DB_HOST": host,
            "DB_PORT": str(port),
            "DB_NAME": db_name,
            "DB_USER": "${CI_DB_USER}",
            "DB_PASSWORD": "${CI_DB_PASSWORD}",
            "PGHOST": host,
            "PGPORT": str(port),
            "PGDATABASE": db_name,
        })
        hits = [{"line": index, "text": line.strip()[:160]}
                for index, line in enumerate(source.splitlines(), 1)
                if SQL_EFFECT.search(line)]
        entries.append({
            "module": module,
            "source_sha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
            "database_name": db_name,
            "profile": profile,
            "url_inputs_mentioned": [name for name in URL_VARIABLES if name in source],
            "uses_pg_engine_name": "pg_engine" in source,
            "lexical_sql_effects": hits,
            "lexical_hit_count": len(hits),
            "environment_template": env,
            "test_command": ["python", "-m", "pytest", module, "-q"],
            "bootstrap_validated_on_disposable_database": False,
        })
    return {
        "version": 1,
        "status": "plan_only_no_database_created_or_test_executed",
        "profile_status": profiles["status"],
        "host": host,
        "port": port,
        "database_prefix": db_prefix,
        "module_count": len(entries),
        "discovery_cross_checked": discovery_path is not None,
        "modules": entries,
        "preserve_existing_dedicated_ci_steps": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--manifest", type=Path,
                        default=Path(__file__).with_name("ci_pg_isolation_profiles.json"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--db-prefix", required=True)
    parser.add_argument("--discovery", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = build_plan(args.repo_root, args.manifest, args.host, args.port,
                            args.db_prefix, args.discovery)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return 0
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        print(f"CI PostgreSQL isolation plan refused: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
