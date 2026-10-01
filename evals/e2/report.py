"""Read-only views of the E2 ledger: the latest snapshot as JSON or markdown.

Nothing here writes, creates or locks anything: :func:`load_board` opens the
ledger files read-only and verifies the chain; a missing board directory is
reported as ``not_initialized`` and is not created. The API endpoint
(``api/routers/evals_e2.py``) and ``python -m evals.e2 report`` both use it.
"""

from __future__ import annotations

from pathlib import Path

from evals.e2.chain import Ledger


def load_board(board_dir: Path, version: str) -> dict:
    """``{"status", "version", "chain", "snapshot"}`` from the ledger (verified), read-only."""
    ledger = Ledger(Path(board_dir), version)
    if not ledger.path.exists():
        return {"status": "not_initialized", "version": version, "chain": None, "snapshot": None,
                "detail": f"no ledger at {ledger.path.name} (the E2 job has not run on this host)"}
    check = ledger.verify()
    if not check["ok"]:
        return {"status": "chain_broken", "version": version, "chain": check, "snapshot": None,
                "detail": "the ledger fails verification; no scores are served from it"}
    snapshot = None
    header = None
    for record in ledger.read_all():
        if record.get("kind") == "snapshot":
            snapshot = record
        elif record.get("kind") == "header":
            header = record
    return {"status": "ok", "version": version, "chain": check, "header": header, "snapshot": snapshot}


def _fmt(value, digits: int = 4) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _ci(row: dict) -> str:
    ci = row.get("wilson_ci") or row.get("bootstrap_ci")
    return f"[{_fmt(ci[0])}, {_fmt(ci[1])}]" if ci else "-"


def render_markdown(snapshot: dict | None) -> str:
    if not snapshot:
        return "# E2 forward scoreboard\n\nNo snapshot yet.\n"
    lines = [
        "# E2 forward scoreboard",
        "",
        f"Snapshot {snapshot['run_at']} ({snapshot['e2_version']}). {snapshot['label']}.",
        "This file is a regenerated view of the ledger, not an anchor.",
        "",
        "## Streams",
        "",
    ]
    for name, s in sorted(snapshot["streams"].items()):
        if not s.get("ok"):
            lines.append(f"- **{name}**: NOT READ ({s.get('error')})")
            continue
        lines.append(f"- **{name}**: {s['source_records_seen']} source records seen "
                     f"(head `{(s['source_head_sha256'] or '-')[:12]}`); activity {s['activity']}")
    c = snapshot["counts"]
    lines += ["", "## Counts", "",
              f"- predictions {c['predictions']}, resolutions {c['resolutions']}, pending {c['pending']}, "
              f"scores {c['scores']} (official {c['official_scores']}), integrity alerts {c['integrity_alerts']}",
              f"- void by reason: {c['void_by_reason'] or '{}'}", ""]
    for bucket, title in (("official", "Official scores (outcome observable after the rules were registered)"),
                          ("pre_registration", "PRE-REGISTRATION bucket (outcome observable before the rules "
                                               "were registered; never official)")):
        rows = [r for r in snapshot["aggregates"] if r["bucket"] == bucket and r["window"] == "all"]
        lines += [f"## {title}", ""]
        if not rows:
            lines += ["None yet.", ""]
            continue
        lines += ["| group | rule | metric | n | mean | 95% CI | interim |", "|---|---|---|---|---|---|---|"]
        for r in rows:
            group = "/".join(str(v) for v in r["group"].values())
            lines.append(f"| {group} | {r['rule_id']} | {r['metric']} | {r['n']} | {_fmt(r['mean'])} | {_ci(r)} | "
                         f"{'INTERIM' if r['interim'] else ''} |")
        lines.append("")
    lines.append(f"aggregates sha256 `{snapshot['aggregates_sha256']}`")
    return "\n".join(lines) + "\n"
