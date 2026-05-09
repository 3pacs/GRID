"""Preflight checks for GRID audit database targets.

This catches the easy-to-miss failure mode where local audit/backtest commands
connect to the empty Docker dev database instead of the live GRID database.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql


@dataclass(frozen=True)
class TableExpectation:
    name: str
    min_count: int
    required: bool = True


@dataclass(frozen=True)
class TableCheck:
    name: str
    observed: int | None
    min_count: int
    ok: bool
    message: str


AUDIT_EXPECTATIONS: tuple[TableExpectation, ...] = (
    TableExpectation("oracle_predictions", 10_000),
    TableExpectation("resolved_series", 10_000),
    TableExpectation("signal_sources", 1_000),
    TableExpectation("feature_registry", 100),
)


def assess_counts(
    counts: dict[str, int | None],
    expectations: Iterable[TableExpectation] = AUDIT_EXPECTATIONS,
    *,
    allow_empty: bool = False,
) -> list[TableCheck]:
    checks: list[TableCheck] = []
    for expected in expectations:
        observed = counts.get(expected.name)
        if observed is None:
            ok = not expected.required
            message = "missing table" if expected.required else "missing optional table"
        elif allow_empty:
            ok = True
            message = f"{observed} rows; allow_empty enabled"
        elif observed < expected.min_count:
            ok = False
            message = f"{observed} rows; expected at least {expected.min_count}"
        else:
            ok = True
            message = f"{observed} rows"
        checks.append(
            TableCheck(
                name=expected.name,
                observed=observed,
                min_count=expected.min_count,
                ok=ok,
                message=message,
            )
        )
    return checks


def load_optional_env_file(path: str | None) -> Path | None:
    if not path:
        return None
    env_path = Path(path).expanduser()
    if not env_path.exists():
        raise FileNotFoundError(f"GRID DB env file does not exist: {env_path}")
    load_dotenv(env_path, override=True)
    return env_path


def _connect():
    from config import settings

    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        connect_timeout=5,
        application_name="grid_db_preflight",
        options=f"-c statement_timeout={os.getenv('GRID_DB_PREFLIGHT_TIMEOUT_MS', '10000')}",
    )


def _target() -> dict[str, str | int]:
    from config import settings

    return {
        "host": settings.DB_HOST,
        "port": settings.DB_PORT,
        "database": settings.DB_NAME,
        "user": settings.DB_USER,
    }


def fetch_counts(table_names: Iterable[str]) -> dict[str, int | None]:
    counts: dict[str, int | None] = {}
    with _connect() as conn:
        with conn.cursor() as cur:
            for table_name in table_names:
                cur.execute("select to_regclass(%s)", (f"public.{table_name}",))
                if cur.fetchone()[0] is None:
                    counts[table_name] = None
                    continue
                cur.execute(sql.SQL("select count(*) from {}").format(sql.Identifier(table_name)))
                counts[table_name] = int(cur.fetchone()[0])
    return counts


def render_text(target: dict[str, str | int], checks: list[TableCheck]) -> str:
    lines = [
        "GRID DB preflight",
        f"target: {target['user']}@{target['host']}:{target['port']}/{target['database']}",
    ]
    for check in checks:
        status = "ok" if check.ok else "FAIL"
        lines.append(f"{status}: {check.name}: {check.message}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="grid_db_preflight")
    parser.add_argument(
        "--env-file",
        default=os.getenv("GRID_AUDIT_ENV_FILE"),
        help="Optional dotenv file loaded before GRID config, e.g. ~/.config/grid/live-db.env.",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Report counts but do not fail on empty audit tables.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args(argv)

    env_path = load_optional_env_file(args.env_file)
    counts = fetch_counts(expectation.name for expectation in AUDIT_EXPECTATIONS)
    checks = assess_counts(counts, allow_empty=args.allow_empty)
    ok = all(check.ok for check in checks)
    target = _target()

    if args.json:
        print(
            json.dumps(
                {
                    "ok": ok,
                    "target": target,
                    "env_file": str(env_path) if env_path else None,
                    "checks": [asdict(check) for check in checks],
                },
                indent=2,
                sort_keys=True,
                default=str,
            )
        )
    else:
        print(render_text(target, checks))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
