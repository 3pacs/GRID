"""Unread receipt for a registration sealed before ``edge_read`` records existed.

A registration sealed under policy 4 or later carries its own proof: every read of its
window is an ``edge_read`` record in the ledger. One sealed earlier has none, so freeing
its window needs a receipt reconciled from every source that could show a read:

* the ledger (hash chain + anchor): the registration, its abandonment, no look and no
  ``edge_read`` record referencing it;
* ``cycle.log``: every cycle that started between its registration and its abandonment,
  each one's summary (not a dry run, no look of it), and no ``edge_stream look`` line for
  its proposal anywhere;
* the code that ran the abandoning cycle (``runner.py`` of that release): the code-hash
  check comes before any read in ``compute_look``;
* the data dir: no staged report and no failed attempt for it;
* every bar cache the stream (or a copy of it) wrote: the sessions of its instruments on or
  after its window start, with when they were fetched.

The receipt never decides more than the sources show: any session found on or after the
window start stays consumed (``consumed_through_exclusive``), and a failed check is a
``REFUSED`` receipt. Read-only: SQLite caches are opened ``mode=ro``; the receipt file is
created with ``x`` and set 0444.
"""

from __future__ import annotations

import ast
import json
import os
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from . import ledger as L
from . import policy as P

SCHEMA = "edge-unread-receipt/1"
_LINE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| ")
_CYCLE_START = "GitSink attached"
_READ_CALLS = frozenset({"_read", "_event_look", "forward_progress", "collect", "sessions", "daily"})


def _ts(text: str) -> datetime:
    """A ``cycle.log`` line time (UTC, no offset written)."""
    return datetime.fromisoformat(text.replace(" ", "T")).replace(tzinfo=timezone.utc)


def cycles(log_text: str) -> list[dict]:
    """Each cycle in ``cycle.log``: its start time, its log lines and its JSON summary."""
    out: list[dict] = []
    cur: dict | None = None
    block: list[str] | None = None
    for line in log_text.splitlines():
        m = _LINE.match(line)
        if m and _CYCLE_START in line:
            cur = {"start": _ts(m.group(1)).isoformat(), "lines": [], "summary": None}
            out.append(cur)
            block = None
            continue
        if cur is None:
            continue
        if block is not None:
            block.append(line)
            if line == "}":
                cur["summary"] = json.loads("\n".join(block))
                block = None
            continue
        if line == "{":
            block = [line]
        elif m:
            cur["lines"].append(line)
    return out


def code_check_precedes_reads(runner_source: str) -> dict:
    """In ``compute_look``: the ``CodeChanged`` raise comes before any call that reads data."""
    tree = ast.parse(runner_source)
    fn = next((n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "compute_look"), None)
    if fn is None:
        return {"ok": False, "detail": "no compute_look in the runner source"}
    for i, stmt in enumerate(fn.body):
        names = {getattr(c.func, "id", getattr(c.func, "attr", None)) for c in ast.walk(stmt)
                 if isinstance(c, ast.Call)}
        raises = any(isinstance(n, ast.Raise) and isinstance(n.exc, ast.Call)
                     and getattr(n.exc.func, "id", None) == "CodeChanged" for n in ast.walk(stmt))
        if raises or "_check_code" in names:
            return {"ok": True, "detail": f"code check is statement {i + 1} of compute_look, before any read"}
        if names & _READ_CALLS:
            return {"ok": False, "detail": f"statement {i + 1} calls {sorted(names & _READ_CALLS)} first"}
    return {"ok": False, "detail": "no code-hash check found in compute_look"}


def cache_sessions(path: Path, instruments: list[str], start: str) -> dict:
    """Sessions and daily rows of ``instruments`` on or after ``start`` in one bar cache."""
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        rows: list[tuple] = []
        daily: list[tuple] = []
        for t in instruments:
            if "sessions" in tables:
                rows += con.execute("SELECT ticker, day, fetched_at FROM sessions WHERE ticker = ? AND day >= ?",
                                    (t, start)).fetchall()
            if "daily" in tables:
                daily += con.execute("SELECT ticker, start, end, fetched_at FROM daily WHERE ticker = ? AND end > ?",
                                     (t, start)).fetchall()
    finally:
        con.close()
    days = sorted({r[1] for r in rows})
    return {
        "cache": str(path),
        "sessions_on_or_after_start": len(rows),
        "days": days,
        "fetched_at": [min(r[2] for r in rows), max(r[2] for r in rows)] if rows else [],
        "daily_rows_reaching_start": [list(r) for r in daily],
    }


def build(ledger: L.Ledger, reg_sha: str, data_dir: Path, caches: list[Path], runner_source: str,
          release_sha: str, now: datetime | None = None) -> dict:
    """The receipt for ``reg_sha``. ``verdict`` is ``UNREAD`` (nothing on or after the window
    start anywhere), ``UNREAD_EXCEPT`` (only the listed sessions, which stay consumed), or
    ``REFUSED`` with the failed checks."""
    state = L.load_state(ledger)  # refuses a broken chain or anchor
    checks: dict[str, dict] = {}
    reg = state.registrations.get(reg_sha)
    if reg is None:
        return {"schema": SCHEMA, "registration_sha256": reg_sha, "verdict": "REFUSED",
                "checks": {"ledger": {"ok": False, "detail": "unknown registration"}}}
    ab = state.abandoned.get(reg_sha)
    refs = [r for r in ledger.records[2:] if r.get("registration_sha256") == reg_sha]
    ok = (reg["lane"] == "history" and reg["stage"] == 1 and ab is not None and reg_sha not in state.looks
          and not state.reads.get(reg_sha) and [r["kind"] for r in refs] == [L.ABANDONED_KIND]
          and reg_sha not in state.marked)
    checks["ledger"] = {
        "ok": ok,
        "detail": {"records_referencing": [r["kind"] for r in refs], "lane": reg["lane"], "stage": reg["stage"],
                   "registered_at": reg["recorded_at"], "abandoned_at": ab and ab["recorded_at"],
                   "abandon_reason": ab and ab["reason"], "look": reg_sha in state.looks,
                   "edge_read": sorted(state.reads.get(reg_sha, ())), "sealed_under_read_records": reg_sha in state.marked,
                   "ledger_head": ledger.line_sha(ledger.records[-1]["seq"]), "records": len(ledger.records)},
    }
    if not ok:
        return {"schema": SCHEMA, "registration_sha256": reg_sha, "verdict": "REFUSED", "checks": checks}

    reg_at, ab_at = datetime.fromisoformat(reg["recorded_at"]), datetime.fromisoformat(ab["recorded_at"])
    log_path = data_dir / "cycle.log"
    cyc = cycles(log_path.read_text()) if log_path.exists() else []
    pid = reg["proposal_id"]
    registering = [c for c in cyc if datetime.fromisoformat(c["start"]) <= reg_at
                   and any(f"registered {pid} " in ln for ln in c["lines"])]
    # cycles after the one that registered it (log times are whole seconds; its own cycle may
    # share the registration's second) up to and including the one that abandoned it
    between = [c for c in cyc if c not in registering and reg_at <= datetime.fromisoformat(c["start"]) <= ab_at]
    look_lines = [ln for c in cyc for ln in c["lines"] if f"edge_stream look {pid}" in ln]
    abandoning = between[-1] if between else None
    summ = (abandoning or {}).get("summary") or {}
    ok = (len(registering) == 1 and len(between) == 1 and not look_lines and summ.get("dry_run") is False
          and summ.get("looks") == []
          and any(a.get("registration") == reg_sha[:16] for a in summ.get("abandoned", [])))
    checks["cycle_log"] = {
        "ok": ok,
        "detail": {"cycles_in_log": len(cyc), "registering_cycle": registering[0]["start"] if registering else None,
                   "cycles_between_registration_and_abandonment": [c["start"] for c in between],
                   "abandoning_cycle_summary": {k: summ.get(k) for k in ("dry_run", "looks", "abandoned")},
                   "look_lines_for_proposal": look_lines},
    }
    checks["code_order"] = {**code_check_precedes_reads(runner_source), "release_sha": release_sha}
    looks_dir = data_dir / "looks"
    staged = sorted(p.name for p in looks_dir.glob(f"{reg_sha[:16]}*")) if looks_dir.exists() else []
    attempts = [a for a in _jsonl(data_dir / "attempts.jsonl") if a.get("registration_sha256") == reg_sha]
    checks["data_dir"] = {"ok": not staged and not attempts,
                          "detail": {"staged_reports": staged, "failed_attempts": len(attempts)}}

    start = reg["window"]["start"]
    found = [cache_sessions(c, list(reg["instruments"]), start) for c in caches]
    read_days = sorted({d for f in found for d in f["days"]})
    daily = [f for f in found if f["daily_rows_reaching_start"]]
    checks["caches"] = {"ok": not daily, "detail": found}
    refused = [k for k, v in checks.items() if not v["ok"]]
    if refused:
        verdict = "REFUSED"
    else:
        verdict = "UNREAD_EXCEPT" if read_days else "UNREAD"
    through = (date.fromisoformat(read_days[-1]) + timedelta(days=1)).isoformat() if read_days else start
    return {
        "schema": SCHEMA,
        "registration_sha256": reg_sha,
        "proposal_id": pid,
        "window": reg["window"],
        "instruments": list(reg["instruments"]),
        "verdict": verdict,
        "failed": refused,
        "sessions_read_on_or_after_start": read_days,
        "consumed_through_exclusive": through,
        "checks": checks,
        "policy_in_force": P.POLICY_VERSION if state.policy_sha256 == P.policy_sha256() else state.policy["policy_version"],
        "built_at": (now or datetime.now(timezone.utc)).isoformat(),
        "scope": "the stream's own records and every bar cache listed; the stream's Polygon key exists on grid-svr only",
        "receipt_code_sha256": L.sha256_bytes(Path(__file__).read_bytes()),
    }


def _jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_bytes().splitlines() if x.strip()]


def write(receipt: dict, out: Path) -> str:
    """Write the receipt once (``x``), 0444; returns its sha256."""
    body = P.canonical_bytes(receipt) + b"\n"
    with open(out, "xb") as fh:
        fh.write(body)
        fh.flush()
        os.fsync(fh.fileno())
    os.chmod(out, 0o444)
    return L.sha256_bytes(body)
