"""Unread receipt for a registration sealed before ``edge_read`` records existed.

A registration sealed under policy 4 or later carries its own proof: every read of its
window is an ``edge_read`` record in the ledger. One sealed earlier has none, so freeing
its window needs a receipt reconciled from every source that could show a read. The
builder finds each input itself; nothing the caller supplies is trusted:

* **ledger** (hash chain + anchor): the registration, its abandonment, no look and no
  ``edge_read`` record referencing it. The receipt pins the ledger prefix it read (record
  count and the sha256 of its last line) and copies the abandonment record's fields.
* **release bytes**: the policy-3 code hash covered the AST of every evaluation module, so
  it identifies the code that ran. The builder reads the modules from git blobs and
  rebuilds that hash. The registration's own ``code_sha256`` must come out of the
  registering release. A registration sealed in the abandoning cycle (the *witness*) must
  come out of the abandoning release. The abandoned family's hash under the abandoning
  release must differ from its registered one, which is the exact ``CodeChanged`` cause.
  That release's ``compute_look`` must check the code hash before any read.
* **cycle.log**: every cycle that started between the registration and its abandonment,
  each one's summary (not a dry run, no look of it), and no ``edge_stream look`` line for
  its proposal anywhere. The log's sha256 is pinned.
* **data dir**: no staged report and no failed attempt for it.
* **the owner-approved cache list**: which caches exist cannot be proven from disk, so the
  owner decision names them (:func:`verify` holds the receipt to that list). For each one the
  receipt takes one coherent, hashed snapshot and shows that what the cache holds now is
  all it ever held for the registration's instruments (see the inventory section below).
  The stream's own caches must be on the list. Names containing ``ocmri`` are refused.
* **the cache writers**: every committed ``data.py`` and ``families.py`` up to the release
  named, so that the snapshot's rows are the complete history.

The receipt never decides more than the sources show. Any session found on or after the
window start stays consumed (``consumed_through_exclusive``). Any failed or missing check,
including an unreadable file, is a ``REFUSED`` receipt. Read-only: SQLite files are opened
``mode=ro`` and copied under their own read lock, and the receipt file is created with ``x``
and set 0444.

At run time :func:`verify` re-reads the pinned receipt bytes and compares them with the
ledger and the attestation. The ledger frees nothing when the bytes, the ledger prefix, the
registration, the abandonment record, the witness or the cache list disagree.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

from . import ledger as L
from . import policy as P

SCHEMA = "edge-unread-receipt/3"
FREEING_VERDICTS = ("UNREAD", "UNREAD_EXCEPT")
RECEIPTS_DIR = Path(__file__).resolve().parent / "receipts"
_LINE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| ")
_CYCLE_START = "GitSink attached"
_READ_CALLS = frozenset({"_read", "_event_look", "forward_progress", "collect", "sessions", "daily"})
#: the evaluation modules the policy-3 code hash covered, in its order
V3_MODULES = ("families", "stats", "runner", "data", "policy")
_PKG = "alpha_research/edge_stream"
_COMMIT = re.compile(r"^[0-9a-f]{7,40}$")
EXCLUDE = "ocmri"
#: the cache schemas the stream writes (data.py): table -> columns a bar cache carries
CACHE_SCHEMAS = {"sessions": frozenset({"ticker", "day", "fetched_at"}),
                 "daily": frozenset({"ticker", "start", "end", "fetched_at"}),
                 "edgar_events": frozenset({"ticker", "start", "end"})}


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


# --- release bytes ------------------------------------------------------------------


def v3_ast_digest(source: str) -> str:
    """The module digest of the policy-3 code hash (``families._ast_digest`` up to policy 3):
    docstrings dropped, the AST dumped without positions. The dump format belongs to the
    Python minor version, so the builder must run on the stream's interpreter."""
    tree = ast.parse(source)
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(body, list) and body and isinstance(body[0], ast.Expr) \
                and isinstance(getattr(body[0], "value", None), ast.Constant) and isinstance(body[0].value.value, str):
            node.body = body[1:] or [ast.Pass()]
    return hashlib.sha256(ast.dump(tree, include_attributes=False).encode()).hexdigest()


def _git(repo: Path, *args: str) -> bytes:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True).stdout


def release_code(repo: Path, commit: str, family: str, version: int, policy_sha256: str, numpy_mm: str) -> dict:
    """The policy-3 code hash of ``family`` rebuilt from the git blobs of ``commit``."""
    if not _COMMIT.match(commit):
        raise ValueError(f"not a commit id: {commit!r}")
    full = _git(repo, "rev-parse", "--verify", f"{commit}^{{commit}}").decode().strip()
    parts = [f"family={family}/v{version}", f"numpy={numpy_mm}", f"policy={policy_sha256}"]
    modules = {}
    for m in V3_MODULES:
        path = f"{_PKG}/{m}.py"
        blob = _git(repo, "rev-parse", f"{full}:{path}").decode().strip()
        digest = v3_ast_digest(_git(repo, "cat-file", "blob", blob).decode())
        modules[m] = {"git_blob": blob, "ast_sha256": digest}
        parts.append(f"{m}={digest}")
    return {"commit": full, "family": f"{family}/v{version}", "policy_sha256": policy_sha256,
            "code_sha256": hashlib.sha256("\n".join(parts).encode()).hexdigest(), "modules": modules}


# --- cache inventory (bounded, owner-approved) ----------------------------------------
#
# Which caches exist is not provable from disk (a deleted cache leaves nothing), so the
# list is the owner's: the decision names it and :func:`verify` holds the receipt to it.
# What the receipt proves about each listed cache is that what it holds now is everything
# it ever held for the registration's instruments:
#
# * the writers: every committed ``data.py`` in the release's history writes the caches
#   only with :data:`CACHE_WRITES`. Sessions and EDGAR events are only ever added (a
#   REPLACE of an event leaves a row for that ticker). The one DELETE removes a daily row
#   only for a ``require_complete`` read, and every committed ``families.py`` asks that
#   only of the hedge instrument, which is not one of the registration's instruments. A
#   daily REPLACE can narrow a ticker's range, so any daily row of an instrument refuses.
# * the image: one coherent snapshot per cache, taken under a SHARED lock (no writer can
#   commit) of a rollback-journal file with no hot journal and no WAL beside it. The bytes
#   copied are the bytes hashed and the bytes inspected.
# * the file: its inode birth time (the live cache must predate the registration), and
#   corroboration that nothing was removed: no free pages, no auto-vacuum, and every
#   table's row count equal to its largest rowid.

#: the only cache writes the stream's code may contain (every committed data.py)
CACHE_WRITES = frozenset({"INSERT OR IGNORE INTO sessions", "INSERT OR REPLACE INTO daily",
                          "DELETE FROM daily", "INSERT OR REPLACE INTO edgar_events"})
_WRITE_SQL = re.compile(r"\b(DELETE\s+FROM|INSERT\s+OR\s+[A-Z]+\s+INTO|INSERT\s+INTO|REPLACE\s+INTO|UPDATE|"
                        r"DROP\s+TABLE|ALTER\s+TABLE|VACUUM|TRUNCATE)\s*([A-Za-z_]*)", re.I)
_SIDE_FILES = ("-journal", "-wal", "-shm")


class SnapshotError(Exception):
    """A cache that cannot give one coherent image."""


def _excluded(name: str) -> bool:
    return EXCLUDE in name.lower()


def cache_writes(source: str) -> list[str]:
    """The SQL write statements in one module's source, normalised (``DELETE FROM daily``)."""
    return sorted({f"{' '.join(m.group(1).upper().split())} {m.group(2)}".strip()
                   for m in _WRITE_SQL.finditer(source)})


def _hedge_only_complete_reads(source: str) -> bool:
    """Every ``.daily(..., require_complete=True)`` call reads ``self.hedge``."""
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "daily" and any(
                k.arg == "require_complete" and not (isinstance(k.value, ast.Constant) and k.value.value is False)
                for k in node.keywords):
            first = node.args[0] if node.args else None
            if not (isinstance(first, ast.Attribute) and first.attr == "hedge"
                    and isinstance(first.value, ast.Name) and first.value.id == "self"):
                return False
    return True


_CACHE_TABLES = ("sessions", "daily", "edgar_events")


def _tree_check(tree: Path) -> dict:
    """A staged code tree (a rehearsal's ``PYTHONPATH``): its ``data.py`` writes only
    :data:`CACHE_WRITES`, no other module writes a cache table, and its ``families.py``
    reads only the hedge complete."""
    row: dict = {"tree": str(tree), "data": None, "other_cache_writes": [], "complete_reads_hedge_only": True}
    for f in sorted(tree.rglob("*.py")):
        src = f.read_text()
        writes = cache_writes(src)
        if f.name == "data.py" and f.parent.name == "edge_stream":
            row["data"] = writes
        elif hits := [w for w in writes if w.split()[-1] in _CACHE_TABLES]:
            row["other_cache_writes"].append([str(f.relative_to(tree)), hits])
        if f.name == "families.py" and not _hedge_only_complete_reads(src):
            row["complete_reads_hedge_only"] = False
    row["ok"] = (row["data"] is not None and set(row["data"]) <= CACHE_WRITES and not row["other_cache_writes"]
                 and row["complete_reads_hedge_only"])
    return row


def _deployed_since(repo: Path, full: str, since: datetime) -> dict:
    """Every commit the repo's HEAD reflog moved to on or after ``since``, and those not in
    ``full``'s history (a deployed release whose writers the history check would miss)."""
    out = _git(repo, "reflog", "--date=iso-strict", "--format=%H %gd").decode().splitlines()
    seen = []
    for line in out:
        commit, _, ref = line.partition(" ")
        m = re.search(r"@\{(.+)\}", ref)
        if m and datetime.fromisoformat(m.group(1)) >= since:
            seen.append(commit)
    seen = sorted(set(seen))
    outside = [c for c in seen if subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", c, full],
                                                 capture_output=True).returncode != 0]
    return {"since": since.isoformat(), "commits": len(seen), "not_in_history": outside}


def writer_check(repo: Path, through: str, instruments: list[str], trees: list[Path] = (),
                 deployed_since: datetime | None = None) -> dict:
    """Every cache writer that ran against the rules above: each committed ``data.py`` and
    ``families.py`` in ``through``'s history, every release the reflog deployed since
    ``deployed_since`` inside that history, and each staged tree a rehearsal ran."""
    if not _COMMIT.match(through):
        raise ValueError(f"not a commit id: {through!r}")
    full = _git(repo, "rev-parse", "--verify", f"{through}^{{commit}}").decode().strip()
    out: dict = {"through": full, "data": [], "families": []}
    for mod in ("data", "families"):
        path = f"{_PKG}/{mod}.py"
        for c in _git(repo, "log", "--format=%H", full, "--", path).decode().split():
            try:
                src = _git(repo, "cat-file", "blob", f"{c}:{path}").decode()
            except subprocess.CalledProcessError:  # the commit that deleted the file
                continue
            row = {"commit": c}
            if mod == "data":
                row["writes"] = cache_writes(src)
            else:
                row["complete_reads_hedge_only"] = _hedge_only_complete_reads(src)
            out[mod].append(row)
    hedge = _git(repo, "cat-file", "blob", f"{full}:{_PKG}/policy.py").decode()
    m = re.search(r'^HEDGE_INSTRUMENT\s*=\s*"([A-Z.]+)"', hedge, re.M)
    out["hedge_instrument"] = m.group(1) if m else None
    out["deployed"] = _deployed_since(repo, full, deployed_since) if deployed_since else None
    out["trees"] = [_tree_check(Path(t)) for t in trees]
    out["ok"] = (bool(out["data"]) and all(set(r["writes"]) <= CACHE_WRITES for r in out["data"])
                 and all(r["complete_reads_hedge_only"] for r in out["families"])
                 and out["hedge_instrument"] is not None and out["hedge_instrument"] not in instruments
                 and out["deployed"] is not None and not out["deployed"]["not_in_history"]
                 and all(t["ok"] for t in out["trees"]))
    return out


def _birth(path: str) -> int | None:
    """The inode's birth time (statx), or None where the filesystem does not keep one."""
    got = subprocess.run(["stat", "-c", "%W", "--", path], capture_output=True, text=True)
    try:
        t = int(got.stdout.strip())
    except ValueError:
        return None
    return t if got.returncode == 0 and t > 0 else None


def _iso(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).isoformat()


def snapshot(path: str, dest: Path) -> dict:
    """One coherent image of ``path`` copied to ``dest``: a rollback-journal SQLite file with no
    hot journal and no WAL beside it, read in full under a SHARED lock, which no writer can
    commit through. The file must not move or change while it is copied."""
    if not stat.S_ISREG(os.stat(path).st_mode):  # a missing file raises here, not as a new empty DB
        raise SnapshotError("not a regular file")
    side = [x for x in _SIDE_FILES if os.path.lexists(path + x)]
    if side:
        raise SnapshotError(f"{side} beside it")
    con = sqlite3.connect(f"file:{quote(path)}?mode=ro", uri=True, isolation_level=None)
    try:
        mode = con.execute("PRAGMA journal_mode").fetchone()[0]
        if mode != "delete":
            raise SnapshotError(f"journal_mode {mode}")
        con.execute("BEGIN")
        con.execute("SELECT count(*) FROM sqlite_master").fetchone()  # holds SHARED until COMMIT
        before = os.stat(path)
        h = hashlib.sha256()
        with open(path, "rb") as src, open(dest, "xb") as out:
            for chunk in iter(lambda: src.read(1 << 20), b""):
                h.update(chunk)
                out.write(chunk)
        after = os.stat(path)
        side = [x for x in _SIDE_FILES if os.path.lexists(path + x)]
        con.execute("COMMIT")
    finally:
        con.close()
    if side or (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
        raise SnapshotError("changed while copied")
    if dest.stat().st_size != before.st_size:
        raise SnapshotError("short copy")
    born = _birth(path)
    if born is None:
        raise SnapshotError("no inode birth time")
    return {"sha256": h.hexdigest(), "bytes": before.st_size, "inode": before.st_ino, "born": _iso(born),
            "modified": _iso(before.st_mtime), "journal_mode": mode}


def _inspect_open(open_path: str, path: str, instruments: list[str], start: str) -> dict:
    con = sqlite3.connect(f"file:{quote(open_path)}?mode=ro", uri=True)
    try:
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        cols = {t: {r[0] for r in con.execute("SELECT name FROM pragma_table_info(?)", (t,))} for t in tables}
        kinds = sorted(t for t, need in CACHE_SCHEMAS.items() if t in cols and need <= cols[t])
        if not kinds:
            return {"tables": len(tables)}
        rows: list[tuple] = []
        daily: list[tuple] = []
        events: list[tuple] = []
        for t in instruments:
            if "sessions" in kinds:
                rows += con.execute("SELECT ticker, day, fetched_at FROM sessions WHERE ticker = ? AND day >= ?",
                                    (t, start)).fetchall()
            if "daily" in kinds:
                daily += con.execute("SELECT ticker, start, end, fetched_at FROM daily WHERE ticker = ?",
                                     (t,)).fetchall()
            if "edgar_events" in kinds:
                events += con.execute("SELECT ticker, start, end FROM edgar_events WHERE ticker = ? AND end > ?",
                                      (t, start)).fetchall()
        counts = {t: list(con.execute(f'SELECT count(*), coalesce(max(rowid), 0) FROM "{t}"').fetchone())  # noqa: S608
                  for t in tables if t in CACHE_SCHEMAS}  # names come from sqlite_master and CACHE_SCHEMAS
        retention = {"freelist_count": con.execute("PRAGMA freelist_count").fetchone()[0],
                     "auto_vacuum": con.execute("PRAGMA auto_vacuum").fetchone()[0],
                     "rows_and_max_rowid": counts}
    finally:
        con.close()
    days = sorted({r[1] for r in rows})
    retention["nothing_removed"] = (retention["freelist_count"] == 0 and retention["auto_vacuum"] == 0
                                    and all(n == top for n, top in counts.values()))
    return {
        "cache": path,
        "schema": kinds,
        "sessions_on_or_after_start": len(rows),
        "days": days,
        "fetched_at": [min(r[2] for r in rows), max(r[2] for r in rows)] if rows else [],
        "daily_rows_of_instruments": [list(r) for r in daily],
        "event_rows_reaching_start": [list(r) for r in events],
        "retention": retention,
    }


def inspect(path: str, instruments: list[str], start: str) -> dict:
    """One listed cache from its coherent snapshot: the snapshot's facts, its sessions of
    ``instruments`` on or after ``start``, daily and event rows, and retention facts."""
    if _excluded(path):
        raise SnapshotError("excluded name")
    with tempfile.TemporaryDirectory(prefix="edge-receipt-") as tmp:
        copy = Path(tmp) / "snapshot.sqlite3"
        snap = snapshot(path, copy)
        got = _inspect_open(str(copy), path, instruments, start)
    return {"cache": path, **got, "snapshot": snap}


def inventory(caches: list[str], instruments: list[str], start: str, data_dir: Path, registered_at: datetime) -> dict:
    """Every listed cache inspected; ``ok`` only when each is a readable bar cache with full
    retention evidence and the stream's own caches are on the list."""
    listed = sorted({str(Path(c)) for c in caches})
    live = [str(data_dir / "bars.sqlite3")] + ([str(data_dir / "preflight" / "bars.sqlite3")]
                                               if (data_dir / "preflight" / "bars.sqlite3").exists() else [])
    got, errors = [], []
    for c in listed:
        try:
            r = inspect(c, instruments, start)
        except (OSError, sqlite3.Error, SnapshotError) as exc:
            errors.append(f"{c}: {type(exc).__name__}: {exc}"[:300])
            continue
        if "schema" not in r:
            errors.append(f"{c}: not a bar cache")
            continue
        got.append(r)
    problems = []
    if missing := [x for x in live if x not in listed]:
        problems.append(f"stream caches not listed: {missing}")
    for r in got:
        if not r["retention"]["nothing_removed"]:
            problems.append(f"{r['cache']}: rows or pages removed")
        if r["daily_rows_of_instruments"]:
            problems.append(f"{r['cache']}: daily rows of the instruments")
        if r["event_rows_reaching_start"]:
            problems.append(f"{r['cache']}: event rows reaching the window")
    live_born = [r["snapshot"]["born"] for r in got if r["cache"] == live[0]]
    if not live_born or datetime.fromisoformat(live_born[0]) >= registered_at:
        problems.append("the live cache is not the file that existed when the registration was sealed")
    return {"listed": listed, "stream_caches": live, "caches": got, "errors": errors, "problems": problems,
            "ok": bool(listed) and not errors and not problems}


# --- the receipt --------------------------------------------------------------------


def _refused(reg_sha: str, checks: dict) -> dict:
    return {"schema": SCHEMA, "registration_sha256": reg_sha, "verdict": "REFUSED",
            "failed": [k for k, v in checks.items() if not v["ok"]], "checks": checks}


def build(ledger: L.Ledger, reg_sha: str, data_dir: Path, *, repo: Path, registered_release: str,
          abandoning_release: str, caches: list[str], writers_through: str, writer_trees: list[str] = (),
          numpy_mm: str | None = None, now: datetime | None = None) -> dict:
    """The receipt for ``reg_sha``. ``verdict`` is ``UNREAD`` (nothing on or after the window
    start in any listed cache), ``UNREAD_EXCEPT`` (only the listed sessions, which stay
    consumed), or ``REFUSED`` with the failed checks. ``caches`` is the owner-approved cache
    list; ``writers_through`` the release whose history holds every deployed cache writer;
    ``writer_trees`` the staged code trees rehearsals ran against listed caches."""
    if numpy_mm is None:
        import numpy

        numpy_mm = ".".join(numpy.__version__.split(".")[:2])
    state = L.load_state(ledger)  # refuses a broken chain or anchor
    checks: dict[str, dict] = {}
    reg = state.registrations.get(reg_sha)
    if reg is None:
        return _refused(reg_sha, {"ledger": {"ok": False, "detail": "unknown registration"}})
    ab = state.abandoned.get(reg_sha)
    refs = [r for r in ledger.records[2:] if r.get("registration_sha256") == reg_sha]
    ok = (reg["lane"] == "history" and reg["stage"] == 1 and ab is not None and reg_sha not in state.looks
          and not state.reads.get(reg_sha) and [r["kind"] for r in refs] == [L.ABANDONED_KIND]
          and reg_sha not in state.marked)
    checks["ledger"] = {
        "ok": ok,
        "detail": {"records_referencing": [r["kind"] for r in refs], "lane": reg["lane"], "stage": reg["stage"],
                   "look": reg_sha in state.looks, "edge_read": sorted(state.reads.get(reg_sha, ())),
                   "sealed_under_read_records": reg_sha in state.marked},
    }
    if not ok:
        return _refused(reg_sha, checks)

    reg_at, ab_at = datetime.fromisoformat(reg["recorded_at"]), datetime.fromisoformat(ab["recorded_at"])
    log_path = data_dir / "cycle.log"
    log_bytes = log_path.read_bytes() if log_path.exists() else b""
    cyc = cycles(log_bytes.decode())
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
        "detail": {"sha256": L.sha256_bytes(log_bytes), "cycles_in_log": len(cyc),
                   "registering_cycle": registering[0]["start"] if registering else None,
                   "cycles_between_registration_and_abandonment": [c["start"] for c in between],
                   "abandoning_cycle_summary": {k: summ.get(k) for k in ("dry_run", "looks", "abandoned")},
                   "look_lines_for_proposal": look_lines},
    }

    checks["release"] = _release_check(state, reg_sha, reg, ab, abandoning, repo, registered_release,
                                       abandoning_release, numpy_mm)

    looks_dir = data_dir / "looks"
    staged = sorted(p.name for p in looks_dir.glob(f"{reg_sha[:16]}*")) if looks_dir.exists() else []
    attempts = [a for a in _jsonl(data_dir / "attempts.jsonl") if a.get("registration_sha256") == reg_sha]
    checks["data_dir"] = {"ok": not staged and not attempts,
                          "detail": {"staged_reports": staged, "failed_attempts": len(attempts)}}

    start = reg["window"]["start"]
    inv = inventory(caches, list(reg["instruments"]), start, data_dir, reg_at)
    try:
        born = [datetime.fromisoformat(c["snapshot"]["born"]) for c in inv["caches"]]
        writers = writer_check(repo, writers_through, list(reg["instruments"]), [Path(t) for t in writer_trees],
                               min(born) if born else reg_at)
    except (subprocess.CalledProcessError, ValueError, OSError) as exc:
        writers = {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:200]}"}
    checks["writers"] = {"ok": writers["ok"], "detail": writers}
    read_days = sorted({d for c in inv["caches"] for d in c["days"]})
    checks["caches"] = {"ok": inv["ok"], "detail": inv}

    refused = [k for k, v in checks.items() if not v["ok"]]
    verdict = "REFUSED" if refused else ("UNREAD_EXCEPT" if read_days else "UNREAD")
    through = (date.fromisoformat(read_days[-1]) + timedelta(days=1)).isoformat() if read_days else start
    return {
        "schema": SCHEMA,
        "registration_sha256": reg_sha,
        "registration": {k: reg[k] for k in ("proposal_id", "family", "family_version", "lane", "stage",
                                             "recorded_at", "code_sha256", "policy_sha256")}
                        | {"window": reg["window"], "instruments": list(reg["instruments"])},
        "abandonment": {"seq": ab["seq"], "recorded_at": ab["recorded_at"], "reason": ab["reason"],
                        "cause": ab.get("cause")},
        "ledger": {"records": len(ledger.records), "head_line_sha256": ledger.line_sha(ledger.records[-1]["seq"])},
        "verdict": verdict,
        "failed": refused,
        "sessions_read_on_or_after_start": read_days,
        "consumed_through_exclusive": through,
        "checks": checks,
        "policy_in_force": state.policy["policy_version"],
        "built_at": (now or datetime.now(timezone.utc)).isoformat(),
        "built_with": {"python": sys.version.split()[0], "numpy": numpy_mm,
                       "receipt_code_sha256": L.sha256_bytes(Path(__file__).read_bytes())},
        "scope": "the stream's own records, the releases' git blobs, and one coherent snapshot of each cache on the "
                 "owner-approved list (which caches exist is the owner's attestation, not this receipt's)",
    }


def _release_check(state, reg_sha, reg, ab, abandoning, repo, registered_release, abandoning_release,
                   numpy_mm) -> dict:
    """The registering and abandoning releases' bytes, rebuilt from git, against the ledger."""
    if abandoning is None:
        return {"ok": False, "detail": "no abandoning cycle in cycle.log"}
    cyc_start = datetime.fromisoformat(abandoning["start"])
    witnesses = [s for s, r in state.registrations.items() if s != reg_sha
                 and datetime.fromisoformat(r["recorded_at"]) >= cyc_start
                 and r["recorded_at"][:10] == ab["recorded_at"][:10]]
    try:
        registered = release_code(repo, registered_release, reg["family"], reg["family_version"],
                                  reg["policy_sha256"], numpy_mm)
        wit = [(s, release_code(repo, abandoning_release, state.registrations[s]["family"],
                                state.registrations[s]["family_version"], state.registrations[s]["policy_sha256"],
                                numpy_mm)) for s in witnesses]
        policy_ran = state.registrations[witnesses[0]]["policy_sha256"] if witnesses else None
        cause = release_code(repo, abandoning_release, reg["family"], reg["family_version"], policy_ran,
                             numpy_mm) if policy_ran else None
        runner_blob = _git(repo, "cat-file", "blob", f"{abandoning_release}:{_PKG}/runner.py").decode()
    except (subprocess.CalledProcessError, ValueError, OSError) as exc:
        return {"ok": False, "detail": f"git: {type(exc).__name__}: {str(exc)[:200]}"}
    order = code_check_precedes_reads(runner_blob)
    wit_ok = bool(wit) and all(state.registrations[s]["code_sha256"] == w["code_sha256"] for s, w in wit)
    ok = (registered["code_sha256"] == reg["code_sha256"] and wit_ok and cause is not None
          and cause["code_sha256"] != reg["code_sha256"] and order["ok"])
    return {
        "ok": ok,
        "detail": {
            "registered_release": registered,
            "registered_matches_ledger": registered["code_sha256"] == reg["code_sha256"],
            "abandoning_release": cause,
            "witnesses": [{"registration_sha256": s, "proposal_id": state.registrations[s]["proposal_id"],
                           "recorded_at": state.registrations[s]["recorded_at"],
                           "code_sha256": state.registrations[s]["code_sha256"],
                           "rebuilt": w["code_sha256"]} for s, w in wit],
            "code_changed": cause is not None and cause["code_sha256"] != reg["code_sha256"],
            "code_order": order,
        },
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


# --- run-time check -----------------------------------------------------------------


def pinned_bytes(entry: dict) -> bytes | None:
    """The receipt an attestation names, from :data:`RECEIPTS_DIR`, when its bytes hash to the
    pinned sha256; ``None`` when the name is not a plain file name, the file is missing, or
    the bytes differ."""
    name = str(entry.get("receipt", ""))
    if not name or Path(name).name != name or name.startswith("."):
        return None
    try:
        body = (RECEIPTS_DIR / name).read_bytes()
    except OSError:
        return None
    return body if L.sha256_bytes(body) == entry.get("receipt_sha256") else None


def verify(state: L.StreamState, entry: dict) -> list[str]:
    """Why the pinned receipt of ``entry`` does not bind to this ledger ([] when it does)."""
    body = pinned_bytes(entry)
    if body is None:
        return ["receipt bytes missing or not the pinned sha256"]
    try:
        rc = json.loads(body)
    except ValueError:
        return ["receipt is not JSON"]
    sha = entry.get("registration_sha256")
    reg, ab = state.registrations.get(sha), state.abandoned.get(sha)
    bad = []
    if rc.get("schema") != SCHEMA:
        bad.append("schema")
    if rc.get("registration_sha256") != sha or reg is None or ab is None:
        bad.append("registration")
        return bad
    if rc.get("verdict") not in FREEING_VERDICTS or rc.get("verdict") != entry.get("receipt_verdict") \
            or rc.get("failed") != [] or not all(c.get("ok") is True for c in (rc.get("checks") or {"x": {}}).values()):
        bad.append("verdict")
    if rc.get("consumed_through_exclusive") != entry.get("consumed_through_exclusive"):
        bad.append("consumed_through_exclusive")
    listed = (((rc.get("checks") or {}).get("caches") or {}).get("detail") or {}).get("listed")
    if not listed or listed != sorted(entry.get("inventory") or ()):
        bad.append("cache inventory")
    want = {k: reg.get(k) for k in ("proposal_id", "family", "family_version", "lane", "stage", "recorded_at",
                                    "code_sha256", "policy_sha256")} | {
        "window": reg.get("window"), "instruments": list(reg.get("instruments") or ())}
    if rc.get("registration") != want:
        bad.append("registration fields")
    if rc.get("abandonment") != {"seq": ab.get("seq"), "recorded_at": ab.get("recorded_at"),
                                 "reason": ab.get("reason"), "cause": ab.get("cause")}:
        bad.append("abandonment record")
    led = rc.get("ledger") or {}
    n = led.get("records")
    if not isinstance(n, int) or not 0 < n <= len(state.line_shas) \
            or state.line_shas[n - 1] != led.get("head_line_sha256"):
        bad.append("ledger prefix")
    rel = ((rc.get("checks") or {}).get("release") or {}).get("detail") or {}
    wits = rel.get("witnesses") or []
    if not wits or any((state.registrations.get(w.get("registration_sha256")) or {}).get("code_sha256")
                       != w.get("code_sha256") or w.get("code_sha256") != w.get("rebuilt") for w in wits) \
            or (rel.get("abandoning_release") or {}).get("code_sha256") in (None, reg.get("code_sha256")) \
            or (rel.get("registered_release") or {}).get("code_sha256") != reg.get("code_sha256"):
        bad.append("release witness")
    return bad
