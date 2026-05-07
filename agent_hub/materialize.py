from __future__ import annotations

import argparse
import os
import re
import shlex
import shutil
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras


DEFAULT_OUT_ROOT = "/tmp/agent-reports-out"
DEFAULT_DEST = (
    "anikdang@100.120.20.120:/Users/anikdang/Documents/Obsidian Vault/00-Agent-Reports"
)
_SLUG_RE = re.compile(r"[^A-Za-z0-9._-]+")


def yesterday_local() -> date:
    return datetime.now().astimezone().date() - timedelta(days=1)


def slugify(value: str) -> str:
    slug = _SLUG_RE.sub("-", value.strip().lower()).strip(".-")
    return slug[:90] or "untitled"


def _connect():
    from config import settings

    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def fetch_reports(report_date: date) -> list[dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id::text AS id, date, agent, host, title, body_md,
                       body_json, tags, created_at, report_uri, idempotency_key
                FROM agent_reports
                WHERE date = %s
                ORDER BY agent, host, created_at, title
                """,
                (report_date,),
            )
            return [dict(row) for row in cur.fetchall()]


def render_report(row: dict[str, Any]) -> str:
    tags = ", ".join(row.get("tags") or [])
    created_at = row.get("created_at")
    created_str = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at)
    return "\n".join(
        [
            "---",
            f"id: {row['id']}",
            f"date: {row['date']}",
            f"agent: {row['agent']}",
            f"host: {row['host']}",
            f"title: {row['title']}",
            f"created_at: {created_str}",
            f"report_uri: {row['report_uri']}",
            f"idempotency_key: {row['idempotency_key']}",
            f"tags: [{tags}]",
            "---",
            "",
            f"# {row['title']}",
            "",
            row.get("body_md") or "",
            "",
        ]
    )


def materialize_reports(
    rows: list[dict[str, Any]],
    report_date: date,
    out_root: Path,
) -> Path:
    day_dir = out_root / report_date.isoformat()
    if day_dir.exists():
        shutil.rmtree(day_dir)
    day_dir.mkdir(parents=True, exist_ok=True)

    index_lines = [f"# Agent Reports - {report_date.isoformat()}", ""]
    used_names: set[str] = set()
    for row in rows:
        base = f"{slugify(row['agent'])}__{slugify(row['host'])}__{slugify(row['title'])}"
        filename = f"{base}.md"
        counter = 2
        while filename in used_names:
            filename = f"{base}-{counter}.md"
            counter += 1
        used_names.add(filename)
        (day_dir / filename).write_text(render_report(row), encoding="utf-8")
        index_lines.append(
            f"- [{row['title']}]({filename}) - {row['agent']} on {row['host']}"
        )

    if len(index_lines) == 2:
        index_lines.append("- No reports captured for this date.")
    (day_dir / "_INDEX.md").write_text("\n".join(index_lines) + "\n", encoding="utf-8")
    return day_dir


def rsync_to_obsidian(day_dir: Path, dest_base: str) -> None:
    dest = f"{dest_base.rstrip('/')}/{day_dir.name}/"
    if ":" in dest and not dest.startswith("/"):
        host, remote_path = dest.split(":", 1)
        subprocess.run(
            ["ssh", host, f"mkdir -p {shlex.quote(remote_path)}"],
            check=True,
        )
        dest = f"{host}:{remote_path}"

    subprocess.run(
        ["rsync", "-az", f"{day_dir}/", dest],
        check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize agent reports to Obsidian")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD, defaults to yesterday")
    parser.add_argument("--out-root", default=os.getenv("AGENT_HUB_OUT_ROOT", DEFAULT_OUT_ROOT))
    parser.add_argument("--dest", default=os.getenv("AGENT_HUB_OBSIDIAN_DEST", DEFAULT_DEST))
    parser.add_argument("--no-rsync", action="store_true", help="Render locally without rsync")
    args = parser.parse_args()

    report_date = date.fromisoformat(args.date) if args.date else yesterday_local()
    rows = fetch_reports(report_date)
    day_dir = materialize_reports(rows, report_date, Path(args.out_root))
    if not args.no_rsync:
        rsync_to_obsidian(day_dir, args.dest)
    print(f"materialized {len(rows)} reports for {report_date.isoformat()} to {day_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
