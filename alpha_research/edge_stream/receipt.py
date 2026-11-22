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
* **every bar cache on the host**: the builder reads the mount table itself and walks every
  local filesystem. It opens every SQLite file changed since the registration was sealed
  (a file unchanged since then cannot hold a later read). A file is a bar cache when its
  schema is one the stream writes (``sessions`` or ``daily``). Each bar cache's sessions
  of the registration's instruments on or after the window start are listed. The stream's
  own cache must be among them. Directories and files whose name contains ``ocmri`` are
  never opened (PHI floor); they are listed as excluded. Other SQLite files are listed by
  the sha256 of their path.

The receipt never decides more than the sources show. Any session found on or after the
window start stays consumed (``consumed_through_exclusive``). Any failed or missing check,
including an unreadable file, is a ``REFUSED`` receipt. Read-only: SQLite files are opened
``mode=ro``, and the receipt file is created with ``x`` and set 0444.

At run time :func:`verify` re-reads the pinned receipt bytes and compares them with the
ledger. The ledger frees nothing when the bytes, the ledger prefix, the registration, the
abandonment record or the witness disagree.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

from . import ledger as L
from . import policy as P

SCHEMA = "edge-unread-receipt/2"
FREEING_VERDICTS = ("UNREAD", "UNREAD_EXCEPT")
RECEIPTS_DIR = Path(__file__).resolve().parent / "receipts"
_LINE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| ")
_CYCLE_START = "GitSink attached"
_READ_CALLS = frozenset({"_read", "_event_look", "forward_progress", "collect", "sessions", "daily"})
#: the evaluation modules the policy-3 code hash covered, in its order
V3_MODULES = ("families", "stats", "runner", "data", "policy")
_PKG = "alpha_research/edge_stream"
_COMMIT = re.compile(r"^[0-9a-f]{7,40}$")
#: filesystems a cache file could live on; overlay mounts show layers that live under /var/lib/docker
DISK_FS = frozenset({"ext2", "ext3", "ext4", "xfs", "btrfs", "zfs", "vfat", "exfat", "f2fs", "ntfs3", "fuseblk",
                     "tmpfs", "nfs", "nfs4", "cifs"})
EXCLUDE = "ocmri"
_SQLITE_MAGIC = b"SQLite format 3\x00"
#: the cache schemas the stream writes (data.py): table -> columns a bar cache carries
CACHE_SCHEMAS = {"sessions": frozenset({"ticker", "day", "fetched_at"}),
                 "daily": frozenset({"ticker", "start", "end", "fetched_at"})}


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


# --- cache inventory ----------------------------------------------------------------


def _unescape(field: str) -> str:
    return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), field)


def mounts(mountinfo_text: str) -> dict[str, str]:
    """Every mount point in ``/proc/self/mountinfo`` and its filesystem type."""
    out: dict[str, str] = {}
    for line in mountinfo_text.splitlines():
        left, _, right = line.partition(" - ")
        fields, rfields = left.split(), right.split()
        if len(fields) >= 5 and rfields:
            out[_unescape(fields[4])] = rfields[0]
    return out


def disk_mounts(mountinfo_text: str) -> list[str]:
    """Mount points of every filesystem in :data:`DISK_FS` (``/proc/self/mountinfo``)."""
    return sorted(m for m, fs in mounts(mountinfo_text).items() if fs in DISK_FS)


def _excluded(name: str) -> bool:
    return EXCLUDE in name.lower()


def _is_sqlite(path: str) -> bool:
    with open(path, "rb") as fh:
        return fh.read(16) == _SQLITE_MAGIC


def walk(root: str, since: float, boundaries: frozenset[str] = frozenset()) -> dict:
    """Every regular file on ``root``'s filesystem (other mounts are their own roots): counts,
    the SQLite files whose inode changed at or after ``since``, exclusions and errors.
    ``boundaries`` are the mount points in the mount table: the walk stops at them without
    touching them (a disk mount is walked as its own root; anything else is not a disk)."""
    out: dict = {"root": root, "dirs": 0, "files": 0, "changed_since_registration": 0, "vanished": 0,
                 "mount_boundaries": 0, "sqlite": [], "excluded": [], "errors": []}
    if any(_excluded(p) for p in Path(root).parts):
        out["excluded"].append(root)
        return out
    dev = os.lstat(root).st_dev
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            it = os.scandir(d)
        except FileNotFoundError:
            out["vanished"] += 1
            continue
        except OSError as exc:
            out["errors"].append(f"{d}: {type(exc).__name__}")
            continue
        with it:
            for e in it:
                if e.path in boundaries:
                    out["mount_boundaries"] += 1
                    continue
                try:
                    st = e.stat(follow_symlinks=False)
                except FileNotFoundError:
                    out["vanished"] += 1
                    continue
                except OSError as exc:
                    out["errors"].append(f"{e.path}: {type(exc).__name__}")
                    continue
                if _excluded(e.name) and (stat.S_ISDIR(st.st_mode) or stat.S_ISREG(st.st_mode)):
                    out["excluded"].append(e.path)
                elif stat.S_ISDIR(st.st_mode):
                    if st.st_dev == dev:
                        out["dirs"] += 1
                        stack.append(e.path)
                elif stat.S_ISREG(st.st_mode):
                    out["files"] += 1
                    if st.st_ctime >= since:
                        out["changed_since_registration"] += 1
                        try:
                            if st.st_size >= 512 and _is_sqlite(e.path):
                                out["sqlite"].append(e.path)
                        except FileNotFoundError:
                            out["vanished"] += 1
                        except OSError as exc:
                            out["errors"].append(f"{e.path}: {type(exc).__name__}")
    out["sqlite"].sort()
    out["excluded"].sort()
    return out


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def inspect(path: str, instruments: list[str], start: str) -> dict:
    """One SQLite file: a bar cache (a schema the stream writes) with its sessions and daily
    rows of ``instruments`` on or after ``start``, or another file (tables counted only).

    A WAL-mode file (header bytes 18-19 = 2) is copied with its ``-wal`` into a private temp
    dir and opened there: a read-only open in place may create a ``-shm`` beside it, owned by
    whoever runs the builder."""
    with open(path, "rb") as fh:
        header = fh.read(100)
    if len(header) == 100 and 2 in (header[18], header[19]):
        with tempfile.TemporaryDirectory(prefix="edge-receipt-") as tmp:
            copy = Path(tmp) / "db.sqlite3"
            shutil.copyfile(path, copy)
            if os.path.exists(path + "-wal"):
                shutil.copyfile(path + "-wal", str(copy) + "-wal")
            got = _inspect_open(str(copy), path, instruments, start)
        return got | {"opened": "copy (WAL)"} if "cache" in got else got
    return _inspect_open(path, path, instruments, start)


def _inspect_open(open_path: str, path: str, instruments: list[str], start: str) -> dict:
    con = sqlite3.connect(f"file:{quote(open_path)}?mode=ro", uri=True)
    try:
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        cols = {t: {r[0] for r in con.execute("SELECT name FROM pragma_table_info(?)", (t,))} for t in tables}
        kinds = sorted(t for t, need in CACHE_SCHEMAS.items() if t in cols and need <= cols[t])
        if not kinds:
            return {"path_sha256": hashlib.sha256(path.encode()).hexdigest(), "tables": len(tables)}
        rows: list[tuple] = []
        daily: list[tuple] = []
        for t in instruments:
            if "sessions" in kinds:
                rows += con.execute("SELECT ticker, day, fetched_at FROM sessions WHERE ticker = ? AND day >= ?",
                                    (t, start)).fetchall()
            if "daily" in kinds:
                daily += con.execute("SELECT ticker, start, end, fetched_at FROM daily WHERE ticker = ? AND end > ?",
                                     (t, start)).fetchall()
    finally:
        con.close()
    days = sorted({r[1] for r in rows})
    return {
        "cache": path,
        "schema": kinds,
        "sha256": _sha256_file(Path(path)),
        "sessions_on_or_after_start": len(rows),
        "days": days,
        "fetched_at": [min(r[2] for r in rows), max(r[2] for r in rows)] if rows else [],
        "daily_rows_reaching_start": [list(r) for r in daily],
    }


def inventory(roots: list[str], since: float, instruments: list[str], start: str, live_cache: Path,
              mount_table: dict[str, str] | None = None) -> dict:
    """Walk every root, inspect every changed SQLite file, and say whether it is complete.
    Excluded paths are counted and hashed, never named: their names are not this receipt's
    business. ``mount_table`` (mount point -> filesystem type) sets the walk boundaries;
    the mounts it skips that are not disks are listed by type."""
    table = mount_table or {}
    walks = [walk(r, since, frozenset(table)) for r in roots]
    caches, other, errors = [], [], [e for w in walks for e in w["errors"]]
    for p in sorted({p for w in walks for p in w["sqlite"]}):
        try:
            got = inspect(p, instruments, start)
        except (OSError, sqlite3.Error) as exc:
            errors.append(f"{p}: {type(exc).__name__}")
            continue
        (caches if "cache" in got else other).append(got)
    live = str(live_cache.resolve())
    live_found = any(str(Path(c["cache"]).resolve()) == live for c in caches)
    return {
        "since_ctime": datetime.fromtimestamp(since, timezone.utc).isoformat(),
        "roots": [{k: w[k] for k in ("root", "dirs", "files", "changed_since_registration", "vanished",
                                     "mount_boundaries")}
                  | {"sqlite_changed": len(w["sqlite"]), "excluded": len(w["excluded"]),
                     "excluded_sha256": hashlib.sha256("\n".join(w["excluded"]).encode()).hexdigest()}
                  for w in walks],
        "non_disk_mounts": dict(sorted(Counter(fs for m, fs in table.items()
                                               if fs not in DISK_FS and not _excluded(m)).items())),
        "bar_caches": caches,
        "other_sqlite": other,
        "errors": errors,
        "live_cache": live,
        "live_cache_found": live_found,
    }


# --- the receipt --------------------------------------------------------------------


def _refused(reg_sha: str, checks: dict) -> dict:
    return {"schema": SCHEMA, "registration_sha256": reg_sha, "verdict": "REFUSED",
            "failed": [k for k, v in checks.items() if not v["ok"]], "checks": checks}


def build(ledger: L.Ledger, reg_sha: str, data_dir: Path, *, repo: Path, registered_release: str,
          abandoning_release: str, roots: list[str] | None = None, numpy_mm: str | None = None,
          now: datetime | None = None) -> dict:
    """The receipt for ``reg_sha``. ``verdict`` is ``UNREAD`` (nothing on or after the window
    start anywhere), ``UNREAD_EXCEPT`` (only the listed sessions, which stay consumed), or
    ``REFUSED`` with the failed checks. ``roots`` defaults to every local filesystem in the
    mount table; tests pass their own."""
    if numpy_mm is None:
        import numpy

        numpy_mm = ".".join(numpy.__version__.split(".")[:2])
    table: dict[str, str] = {}
    if roots is None:
        text = Path("/proc/self/mountinfo").read_text()
        roots, table = disk_mounts(text), mounts(text)
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
    inv = inventory(list(roots), reg_at.timestamp(), list(reg["instruments"]), start, data_dir / "bars.sqlite3",
                    table)
    read_days = sorted({d for c in inv["bar_caches"] for d in c["days"]})
    daily = [c["cache"] for c in inv["bar_caches"] if c["daily_rows_reaching_start"]]
    checks["caches"] = {"ok": inv["live_cache_found"] and not inv["errors"] and not daily, "detail": inv}

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
        "scope": "the stream's own records, the releases' git blobs, and every SQLite file on this host's local "
                 "filesystems changed since the registration was sealed (names containing 'ocmri' never opened)",
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
