#!/usr/bin/env python3
"""GRID storage curator for Hermes.

Bounded, report-first maintenance for large GRID data roots.  The curator
intentionally defaults to read-only: it inventories active data folders,
detects archive/ingest gaps, and writes a cleanup/move plan instead of
deleting or moving files without a manifest-backed follow-up.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from loguru import logger as log


ARCHIVE_SUFFIXES: tuple[str, ...] = (
    ".zip",
    ".gz",
    ".bz2",
    ".xz",
    ".zst",
    ".tar",
    ".tgz",
    ".7z",
)

DEFAULT_ACTIVE_ROOTS: tuple[Path, ...] = (
    Path("/data/gdelt"),
    Path("/data/bulk_data"),
    Path("/data/datasets"),
    Path("/data/grid/bulk"),
    Path("/data/archive"),
)

DEFAULT_COLD_ROOT = Path(os.environ.get("GRID_COLD_STORAGE_ROOT", "/mirror"))
DEFAULT_REPORT_DIR = Path("outputs/storage_maintenance")


@dataclass
class FilesystemSummary:
    path: str
    exists: bool
    total_bytes: int | None = None
    used_bytes: int | None = None
    free_bytes: int | None = None
    use_pct: float | None = None
    error: str | None = None


@dataclass
class ArchiveSample:
    path: str
    size_bytes: int
    suffix: str
    cold_storage_path: str | None = None
    cold_storage_exists: bool = False
    cold_storage_size_match: bool = False
    recommendation: str = "inventory_only"


@dataclass
class RootScan:
    path: str
    exists: bool
    files_seen: int = 0
    dirs_seen: int = 0
    archives_seen: int = 0
    archive_bytes_seen: int = 0
    truncated: bool = False
    largest_archives: list[ArchiveSample] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass
class GdeltAssessment:
    base_path: str
    exists: bool
    state_counts: dict[str, int] = field(default_factory=dict)
    directory_file_counts: dict[str, int] = field(default_factory=dict)
    db_tables: dict[str, Any] = field(default_factory=dict)
    ingest_status: str = "unknown"
    recommendations: list[str] = field(default_factory=list)


@dataclass
class StorageMaintenanceReport:
    status: str
    generated_at: str
    target_id: str
    active_roots: list[str]
    cold_storage_root: str
    filesystems: list[FilesystemSummary]
    roots: list[RootScan]
    gdelt: GdeltAssessment
    cleanup_plan: list[dict[str, Any]]
    ingest_plan: list[dict[str, Any]]
    json_path: str | None = None
    markdown_path: str | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _is_archive(path: Path) -> bool:
    name = path.name.lower()
    return any(name.endswith(suffix) for suffix in ARCHIVE_SUFFIXES)


def _safe_relative(path: Path, roots: Iterable[Path]) -> Path:
    resolved = path.resolve(strict=False)
    for root in roots:
        root_resolved = root.resolve(strict=False)
        try:
            return resolved.relative_to(root_resolved.parent)
        except ValueError:
            continue
    return Path(path.name)


def filesystem_summary(path: Path) -> FilesystemSummary:
    if not path.exists():
        return FilesystemSummary(path=str(path), exists=False)
    try:
        usage = shutil.disk_usage(path)
        used = usage.total - usage.free
        return FilesystemSummary(
            path=str(path),
            exists=True,
            total_bytes=usage.total,
            used_bytes=used,
            free_bytes=usage.free,
            use_pct=(used / usage.total) if usage.total else None,
        )
    except Exception as exc:
        return FilesystemSummary(path=str(path), exists=True, error=str(exc))


def _walk_bounded(root: Path, *, max_files: int, max_depth: int) -> Iterable[Path]:
    root = root.resolve(strict=False)
    seen = 0
    for dirpath, dirnames, filenames in os.walk(root):
        current = Path(dirpath)
        try:
            rel_parts = current.relative_to(root).parts
        except ValueError:
            rel_parts = ()
        if len(rel_parts) >= max_depth:
            dirnames[:] = []
        for filename in filenames:
            yield current / filename
            seen += 1
            if seen >= max_files:
                return


def scan_root(
    root: Path,
    *,
    cold_root: Path,
    active_roots: Iterable[Path],
    max_files: int = 25_000,
    max_depth: int = 4,
    largest_limit: int = 25,
) -> RootScan:
    if not root.exists():
        return RootScan(path=str(root), exists=False, notes=["root_missing"])

    scan = RootScan(path=str(root), exists=True)
    largest: list[ArchiveSample] = []
    files_seen = 0
    dirs_seen = 0
    archives_seen = 0
    archive_bytes = 0

    try:
        for _dirpath, dirnames, _filenames in os.walk(root):
            dirs_seen += len(dirnames)
            # ``os.walk`` is only for directory count here. Stop early; file scan
            # below handles the bounded walk.
            if dirs_seen > max_files:
                break
    except Exception as exc:
        scan.notes.append(f"dir_count_error:{exc}")

    for path in _walk_bounded(root, max_files=max_files, max_depth=max_depth):
        files_seen += 1
        if not _is_archive(path):
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        archives_seen += 1
        archive_bytes += size
        rel = _safe_relative(path, active_roots)
        cold_path = cold_root / rel
        cold_exists = cold_path.exists()
        cold_size_match = False
        if cold_exists:
            try:
                cold_size_match = cold_path.stat().st_size == size
            except OSError:
                cold_size_match = False
        sample = ArchiveSample(
            path=str(path),
            size_bytes=size,
            suffix="".join(path.suffixes[-2:]) if path.name.lower().endswith(".csv.zip") else path.suffix.lower(),
            cold_storage_path=str(cold_path),
            cold_storage_exists=cold_exists,
            cold_storage_size_match=cold_size_match,
            recommendation=(
                "verify_existing_cold_copy_after_ingest_proof"
                if cold_size_match
                else "copy_to_cold_after_ingest_proof"
            ),
        )
        largest.append(sample)
        largest.sort(key=lambda item: item.size_bytes, reverse=True)
        if len(largest) > largest_limit:
            largest.pop()

    scan.files_seen = files_seen
    scan.dirs_seen = dirs_seen
    scan.archives_seen = archives_seen
    scan.archive_bytes_seen = archive_bytes
    scan.truncated = files_seen >= max_files
    scan.largest_archives = largest
    if scan.truncated:
        scan.notes.append(f"bounded_scan_truncated_at_{max_files}_files")
    return scan


def _count_lines(path: Path) -> int | None:
    try:
        with path.open("rb") as handle:
            return sum(1 for _ in handle)
    except OSError:
        return None


def _count_files_fast(path: Path, *, timeout_s: int = 20) -> int | None:
    if not path.exists():
        return None
    try:
        proc = subprocess.run(
            ["find", str(path), "-maxdepth", "1", "-type", "f"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return len(proc.stdout.splitlines())


def _table_count(engine: Any, table: str) -> dict[str, Any]:
    from sqlalchemy import text as sa_text

    with engine.connect() as conn:
        exists = bool(conn.execute(
            sa_text("SELECT to_regclass(:table_name) IS NOT NULL"),
            {"table_name": f"public.{table}"},
        ).scalar())
        if not exists:
            return {"exists": False, "rows": None}
        rows = conn.execute(sa_text(f"SELECT COUNT(*) FROM {table}")).scalar()
    return {"exists": True, "rows": int(rows or 0)}


def assess_gdelt(
    *,
    engine: Any | None,
    gdelt_base: Path = Path("/data/gdelt"),
    parser_root: Path = Path("/data/grid/bulk/gdelt"),
) -> GdeltAssessment:
    assessment = GdeltAssessment(base_path=str(gdelt_base), exists=gdelt_base.exists())
    if not gdelt_base.exists():
        assessment.ingest_status = "gdelt_root_missing"
        assessment.recommendations.append("Confirm whether GDELT archive lives on another node or under /mirror.")
        return assessment

    state_dir = gdelt_base / ".state"
    for state_file in ("v2_english.done", "v2_translation.done", "v1_events.done", "v1_gkg.done"):
        line_count = _count_lines(state_dir / state_file)
        if line_count is not None:
            assessment.state_counts[state_file] = line_count

    for label, path in {
        "v2_english": gdelt_base / "v2" / "english",
        "v2_translation": gdelt_base / "v2" / "translation",
        "v1_events": gdelt_base / "v1" / "events",
        "v1_gkg": gdelt_base / "v1" / "gkg",
        "parser_root": parser_root,
    }.items():
        count = _count_files_fast(path)
        if count is not None:
            assessment.directory_file_counts[label] = count

    if engine is not None:
        try:
            assessment.db_tables["gdelt_events"] = _table_count(engine, "gdelt_events")
            assessment.db_tables["gdelt_daily_summary"] = _table_count(engine, "gdelt_daily_summary")
        except Exception as exc:
            assessment.db_tables["error"] = str(exc)

    events_table = assessment.db_tables.get("gdelt_events", {})
    summary_table = assessment.db_tables.get("gdelt_daily_summary", {})
    parser_files = assessment.directory_file_counts.get("parser_root", 0)
    full_v1_files = assessment.directory_file_counts.get("v1_events", 0)
    full_v2_files = assessment.directory_file_counts.get("v2_english", 0)

    if events_table.get("exists") is False or summary_table.get("exists") is False:
        assessment.ingest_status = "archives_present_db_tables_missing"
        assessment.recommendations.append(
            "Create/extend a GDELT bulk parser job before moving archives; live DB lacks gdelt_events/gdelt_daily_summary."
        )
    elif int(events_table.get("rows") or 0) == 0:
        assessment.ingest_status = "tables_empty"
        assessment.recommendations.append("Run a bounded GDELT parser smoke before any archive cleanup.")
    else:
        assessment.ingest_status = "tables_populated"

    if parser_files and full_v1_files and full_v1_files > parser_files:
        assessment.recommendations.append(
            "scripts/parse_gdelt.py only reads /data/grid/bulk/gdelt; /data/gdelt/v1/events has more files and needs parser coverage or a curated ingest symlink."
        )
    if full_v2_files:
        assessment.recommendations.append(
            "GDELT v2 English/translation archives are massive; keep them on /mirror after parser/index coverage is proven."
        )
    return assessment


def build_cleanup_plan(
    roots: list[RootScan],
    *,
    cold_root: Path,
    min_archive_bytes: int,
) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    for root in roots:
        for archive in root.largest_archives:
            if archive.size_bytes < min_archive_bytes:
                continue
            action = "copy_to_cold_storage_then_verify_manifest"
            reason = (
                "Large archive on active data disk. Keep source until an ingest "
                "record and checksum manifest prove cold copy is safe."
            )
            if archive.cold_storage_size_match:
                action = "verify_existing_cold_copy_then_consider_active_removal"
                reason = (
                    "Large archive appears to already have a same-size cold copy. "
                    "Do not remove source until checksum and ingest manifest are verified."
                )
            plan.append({
                "action": action,
                "source": archive.path,
                "destination": archive.cold_storage_path,
                "size_bytes": archive.size_bytes,
                "cold_storage_exists": archive.cold_storage_exists,
                "cold_storage_size_match": archive.cold_storage_size_match,
                "delete_source": False,
                "reason": reason,
            })
    if not cold_root.exists():
        plan.insert(0, {
            "action": "create_cold_storage_root",
            "destination": str(cold_root),
            "delete_source": False,
            "reason": "Cold storage root missing; create before any archive moves.",
        })
    return plan


def build_ingest_plan(gdelt: GdeltAssessment) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    if gdelt.ingest_status in {"archives_present_db_tables_missing", "tables_empty"}:
        plan.append({
            "action": "run_gdelt_parser_smoke",
            "command": "python3 scripts/parse_gdelt.py",
            "current_parser_root": "/data/grid/bulk/gdelt",
            "blocked_by": (
                "Parser currently ignores /data/gdelt/v1/events and GDELT v2 roots; "
                "extend parser or intentionally symlink a bounded subset first."
            ),
        })
    return plan


def build_storage_maintenance_report(
    *,
    engine: Any | None = None,
    target_id: str = "grid-svr-data",
    active_roots: Iterable[Path] = DEFAULT_ACTIVE_ROOTS,
    cold_root: Path = DEFAULT_COLD_ROOT,
    max_files_per_root: int = 25_000,
    max_depth: int = 4,
    min_archive_bytes: int = 512 * 1024 * 1024,
) -> StorageMaintenanceReport:
    roots = tuple(Path(root) for root in active_roots)
    filesystems = [filesystem_summary(Path("/data")), filesystem_summary(Path("/mirror"))]
    root_scans = [
        scan_root(
            root,
            cold_root=cold_root,
            active_roots=roots,
            max_files=max_files_per_root,
            max_depth=max_depth,
        )
        for root in roots
    ]
    gdelt = assess_gdelt(engine=engine)
    cleanup_plan = build_cleanup_plan(root_scans, cold_root=cold_root, min_archive_bytes=min_archive_bytes)
    ingest_plan = build_ingest_plan(gdelt)

    status = "ok"
    if gdelt.ingest_status in {"archives_present_db_tables_missing", "tables_empty"}:
        status = "ingest_gap"
    if any(fs.use_pct is not None and fs.use_pct >= 0.85 for fs in filesystems):
        status = "disk_pressure"

    return StorageMaintenanceReport(
        status=status,
        generated_at=_utcnow().isoformat(),
        target_id=target_id,
        active_roots=[str(root) for root in roots],
        cold_storage_root=str(cold_root),
        filesystems=filesystems,
        roots=root_scans,
        gdelt=gdelt,
        cleanup_plan=cleanup_plan,
        ingest_plan=ingest_plan,
    )


def _bytes_gib(value: int | None) -> float | None:
    if value is None:
        return None
    return round(value / 1024 / 1024 / 1024, 3)


def markdown_report(report: StorageMaintenanceReport) -> str:
    lines = [
        f"# GRID Storage Maintenance — {report.generated_at}",
        "",
        f"- Status: `{report.status}`",
        f"- Target: `{report.target_id}`",
        f"- Cold storage root: `{report.cold_storage_root}`",
        "",
        "## Filesystems",
        "",
    ]
    for fs in report.filesystems:
        lines.append(
            f"- `{fs.path}` exists={fs.exists} used={_bytes_gib(fs.used_bytes)}GiB "
            f"free={_bytes_gib(fs.free_bytes)}GiB use_pct={round(fs.use_pct * 100, 1) if fs.use_pct is not None else None}"
        )
    lines.extend(["", "## GDELT", ""])
    lines.append(f"- Ingest status: `{report.gdelt.ingest_status}`")
    if report.gdelt.state_counts:
        lines.append(f"- State counts: `{json.dumps(report.gdelt.state_counts, sort_keys=True)}`")
    if report.gdelt.directory_file_counts:
        lines.append(f"- Directory file counts: `{json.dumps(report.gdelt.directory_file_counts, sort_keys=True)}`")
    if report.gdelt.db_tables:
        lines.append(f"- DB tables: `{json.dumps(report.gdelt.db_tables, sort_keys=True)}`")
    for rec in report.gdelt.recommendations:
        lines.append(f"- Recommendation: {rec}")

    lines.extend(["", "## Root Scans", ""])
    for root in report.roots:
        lines.append(
            f"- `{root.path}` exists={root.exists} files_seen={root.files_seen} "
            f"archives_sampled={len(root.largest_archives)} archive_seen={_bytes_gib(root.archive_bytes_seen)}GiB "
            f"truncated={root.truncated}"
        )
        for sample in root.largest_archives[:5]:
            lines.append(f"  - {round(sample.size_bytes / 1024 / 1024 / 1024, 2)}GiB `{sample.path}`")

    lines.extend(["", "## Ingest Plan", ""])
    if report.ingest_plan:
        for item in report.ingest_plan:
            lines.append(f"- `{item['action']}`: {item.get('command', item.get('blocked_by', ''))}")
    else:
        lines.append("- No immediate ingest plan generated.")

    lines.extend(["", "## Cleanup Plan", ""])
    if report.cleanup_plan:
        for item in report.cleanup_plan[:25]:
            lines.append(
                f"- `{item['action']}`: `{item.get('source', item.get('destination'))}` -> "
                f"`{item.get('destination')}` delete_source={item.get('delete_source')}"
            )
    else:
        lines.append("- No cleanup candidates above threshold in bounded scan.")
    return "\n".join(lines) + "\n"


def write_storage_maintenance_report(
    report: StorageMaintenanceReport,
    *,
    output_dir: Path = DEFAULT_REPORT_DIR,
) -> StorageMaintenanceReport:
    from outputs.path_utils import ensure_output_dir

    out = ensure_output_dir(output_dir)
    stamp = datetime.fromisoformat(report.generated_at).strftime("%Y%m%dT%H%M%SZ")
    json_path = out / f"storage_maintenance_{stamp}.json"
    markdown_path = out / f"storage_maintenance_{stamp}.md"
    latest_json = out / "storage_maintenance_latest.json"
    latest_md = out / "storage_maintenance_latest.md"

    payload = asdict(report)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    markdown = markdown_report(report)
    markdown_path.write_text(markdown, encoding="utf-8")
    latest_json.write_text(json_path.read_text(encoding="utf-8"), encoding="utf-8")
    latest_md.write_text(markdown, encoding="utf-8")
    report.json_path = str(json_path)
    report.markdown_path = str(markdown_path)
    return report


def run_storage_maintenance(
    engine: Any | None = None,
    *,
    target_id: str = "grid-svr-data",
    write_report: bool = True,
    **kwargs: Any,
) -> dict[str, Any]:
    report = build_storage_maintenance_report(engine=engine, target_id=target_id, **kwargs)
    if write_report:
        report = write_storage_maintenance_report(report)
    return {
        "status": report.status,
        "target_id": report.target_id,
        "gdelt_ingest_status": report.gdelt.ingest_status,
        "cleanup_candidates": len(report.cleanup_plan),
        "ingest_actions": len(report.ingest_plan),
        "json_path": report.json_path,
        "markdown_path": report.markdown_path,
        "summary": {
            "filesystems": [asdict(fs) for fs in report.filesystems],
            "gdelt": asdict(report.gdelt),
            "top_cleanup": report.cleanup_plan[:10],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run bounded GRID storage maintenance scan.")
    parser.add_argument("--target-id", default="grid-svr-data")
    parser.add_argument("--max-files-per-root", type=int, default=25_000)
    parser.add_argument("--max-depth", type=int, default=4)
    parser.add_argument("--no-db", action="store_true")
    parser.add_argument("--json", action="store_true", help="Print JSON payload.")
    args = parser.parse_args()

    engine = None
    if not args.no_db:
        try:
            from db import get_engine

            engine = get_engine()
        except Exception as exc:
            log.warning("Storage curator continuing without DB engine: {e}", e=str(exc))

    result = run_storage_maintenance(
        engine,
        target_id=args.target_id,
        max_files_per_root=args.max_files_per_root,
        max_depth=args.max_depth,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    else:
        print(f"{result['status']} markdown={result['markdown_path']}")


if __name__ == "__main__":
    main()
