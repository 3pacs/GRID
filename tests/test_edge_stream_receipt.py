"""Unread receipt (alpha_research.edge_stream.receipt): each source it reconciles, how it fails,
and the run-time check that binds the pinned receipt bytes to the ledger."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import stat
import subprocess
import time
from datetime import date
from pathlib import Path

import numpy
import pytest

from alpha_research.edge_stream import families as F
from alpha_research.edge_stream import ledger as L
from alpha_research.edge_stream import policy as P
from alpha_research.edge_stream import receipt as RC
from alpha_research.edge_stream import sanity as S
from alpha_research.edge_stream import template_queue as TQ

TODAY = date(2026, 11, 13)
NP = ".".join(numpy.__version__.split(".")[:2])
REASON = "family IM1 code differs from the registered sha"
OLD_RUNNER = '''
def compute_look(reg_sha, reg, source, today, extras=None):
    family = FAMILIES.get(reg["family"])
    if family is None or code_sha256(family) != reg["code_sha256"]:
        raise CodeChanged("family x code differs from the registered sha")
    bars, start, end = _read(reg, source, today)
'''
READS_FIRST = '''
def compute_look(reg_sha, reg, source, today, extras=None):
    bars, start, end = _read(reg, source, today)
    if code_sha256(FAMILIES[reg["family"]]) != reg["code_sha256"]:
        raise CodeChanged("x")
'''


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), "-c", "commit.gpgsign=false", "-c", "user.email=t@example.com",
                           "-c", "user.name=t", *args], check=True, capture_output=True, text=True).stdout.strip()


def _repo(root: Path, runner_c: str = OLD_RUNNER) -> tuple[Path, str, str]:
    """Two commits of the five evaluation modules: R registered, C changed families.py."""
    pkg = root / "alpha_research" / "edge_stream"
    pkg.mkdir(parents=True)
    _git(root, "init", "-q")
    for m in RC.V3_MODULES:
        (pkg / f"{m}.py").write_text(OLD_RUNNER if m == "runner" else f'"""{m}"""\nX = 1\n')
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "registered")
    r = _git(root, "rev-parse", "HEAD")
    (pkg / "families.py").write_text('"""families"""\nX = 2\n')
    (pkg / "runner.py").write_text(runner_c)
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "abandoning")
    return root, r, _git(root, "rev-parse", "HEAD")


def _cache(path: Path, rows, table: str = "sessions") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    if table == "sessions":
        con.execute("CREATE TABLE sessions (ticker TEXT, day TEXT, bars TEXT, fetched_at TEXT, PRIMARY KEY (ticker, day))")
        con.executemany("INSERT INTO sessions VALUES (?, ?, '[]', ?)", rows)
    else:
        con.execute("CREATE TABLE other (ticker TEXT, day TEXT)")
        con.executemany("INSERT INTO other VALUES (?, ?)", [r[:2] for r in rows])
    con.commit()
    con.close()
    return path


def _v3():
    pol = json.loads(P.canonical_bytes(P.policy_payload()))
    pol.pop("unread_abandonment")
    pol["owner_decisions"] = pol["owner_decisions"][:1]
    pol["policy_version"] = "edge-stream-policy/3"
    return pol


def _world(tmp_path: Path, *, witness: bool = True, runner_c: str = OLD_RUNNER):
    """A policy-3-style ledger: metals registered under release R, abandoned by release C in a
    cycle that also sealed energy (the witness); the host's data dir, cycle.log and live cache."""
    repo, r, c = _repo(tmp_path / "repo", runner_c)
    data = tmp_path / "host" / "data"
    data.mkdir(parents=True)
    pol = _v3()
    with pytest.MonkeyPatch.context() as m:
        m.setattr(P, "policy_payload", lambda: pol)
        m.setattr(P, "policy_sha256", lambda: L.sha256_bytes(P.canonical_bytes(pol)))
        led = L.memory("2026-11-13T06:27:00+00:00")
        psha = L.load_state(led).policy_sha256
        prop = F.build_proposal(dict(TQ.QUEUE[0]), TODAY)
        prop["code_sha256"] = RC.release_code(repo, r, "IM1", prop["family_version"], psha, NP)["code_sha256"]
        rep = S.check(prop, L.load_state(led), TODAY)
        sha = L.append_registration(led, {**prop, "sanity": rep, "recorded_at": "2026-11-13T11:03:50+00:00"})
        L.append_abandoned(led, sha, REASON, "2026-11-16T11:05:50+00:00")
        wsha = None
        if witness:
            w = F.build_proposal(dict(TQ.QUEUE[1]), date(2026, 11, 16))
            w["code_sha256"] = RC.release_code(repo, c, "IM1", w["family_version"], psha, NP)["code_sha256"]
            wrep = S.check(w, L.load_state(led), date(2026, 11, 16))
            wsha = L.append_registration(led, {**w, "sanity": wrep, "recorded_at": "2026-11-16T11:05:51+00:00"})
    pid = prop["proposal_id"]
    summary = lambda **kw: json.dumps({"dry_run": False, "looks": [], "abandoned": [], **kw}, indent=1)  # noqa: E731
    (data / "cycle.log").write_text("\n".join([
        "2026-11-13 11:03:50 | INFO     | config:<module>:1 — GitSink attached",
        f"2026-11-13 11:15:11 | INFO     | runner:_register_or_reject:1 — edge_stream registered {pid} ({sha[:12]})",
        summary(registered=[{"registration": sha[:16]}]),
        "2026-11-16 11:05:50 | INFO     | config:<module>:1 — GitSink attached",
        summary(abandoned=[{"registration": sha[:16], "reason": REASON}]),
    ]) + "\n")
    _cache(data / "bars.sqlite3", [("GLD", "2024-11-25", "2026-11-13T11:10:00+00:00")])  # pre-window only
    return {"led": led, "sha": sha, "pid": pid, "wsha": wsha, "repo": repo, "r": r, "c": c, "data": data,
            "host": tmp_path / "host"}


def _build(w: dict, **kw) -> dict:
    args = {"repo": w["repo"], "registered_release": w["r"], "abandoning_release": w["c"],
            "roots": [str(w["host"])], "numpy_mm": NP} | kw
    return RC.build(w["led"], w["sha"], w["data"], **args)


def test_unread_when_no_source_shows_a_read(tmp_path):
    w = _world(tmp_path)
    rc = _build(w)
    assert rc["verdict"] == "UNREAD" and rc["failed"] == [], rc["failed"]
    assert rc["consumed_through_exclusive"] == rc["registration"]["window"]["start"]
    assert rc["checks"]["cycle_log"]["detail"]["cycles_between_registration_and_abandonment"] == [
        "2026-11-16T11:05:50+00:00"]
    rel = rc["checks"]["release"]["detail"]
    assert rel["registered_matches_ledger"] and rel["code_changed"] and rel["code_order"]["ok"]
    assert [x["registration_sha256"] for x in rel["witnesses"]] == [w["wsha"]]
    assert rel["abandoning_release"]["commit"] == _git(w["repo"], "rev-parse", w["c"])
    inv = rc["checks"]["caches"]["detail"]
    assert inv["live_cache_found"] and [c["cache"] for c in inv["bar_caches"]] == [str(w["data"] / "bars.sqlite3")]
    assert rc["abandonment"] == {"seq": 3, "recorded_at": "2026-11-16T11:05:50+00:00", "reason": REASON, "cause": None}


def test_the_release_bytes_are_rebuilt_from_git_not_supplied(tmp_path):
    """The rebuilt hash is the one the ledger sealed: changing any evaluation module's code
    changes it, a docstring does not."""
    repo, r, c = _repo(tmp_path / "repo")
    a = RC.release_code(repo, r, "IM1", 1, "p" * 64, NP)
    assert a["code_sha256"] != RC.release_code(repo, c, "IM1", 1, "p" * 64, NP)["code_sha256"]
    assert a["code_sha256"] != RC.release_code(repo, r, "IM1", 1, "q" * 64, NP)["code_sha256"]
    pkg = repo / "alpha_research" / "edge_stream"
    _git(repo, "checkout", "-q", r)
    (pkg / "stats.py").write_text('"""another docstring"""\nX = 1\n')
    _git(repo, "commit", "-qam", "docstring only")
    doc_only = _git(repo, "rev-parse", "HEAD")
    assert RC.release_code(repo, doc_only, "IM1", 1, "p" * 64, NP)["code_sha256"] == a["code_sha256"]
    with pytest.raises(ValueError):
        RC.release_code(repo, "HEAD; rm -rf /", "IM1", 1, "p" * 64, NP)


def test_sessions_found_in_the_window_stay_consumed(tmp_path):
    w = _world(tmp_path)
    start = L.load_state(w["led"]).registrations[w["sha"]]["window"]["start"]
    _cache(w["host"] / "elsewhere" / "copy.db", [("SLV", start, "2026-11-22T03:41:00+00:00")])
    rc = _build(w)
    assert rc["verdict"] == "UNREAD_EXCEPT" and rc["sessions_read_on_or_after_start"] == [start]
    assert rc["consumed_through_exclusive"] > start


def test_other_schemas_and_excluded_names_are_listed_not_counted(tmp_path):
    w = _world(tmp_path)
    start = L.load_state(w["led"]).registrations[w["sha"]]["window"]["start"]
    _cache(w["host"] / "misc" / "unrelated.sqlite3", [("SLV", start, "x")], table="other")
    (w["host"] / "OCMRI-private").mkdir()
    (w["host"] / "OCMRI-private" / "x.sqlite3").write_bytes(b"SQLite format 3\x00" + b"\x00" * 600)  # never opened
    rc = _build(w)
    assert rc["verdict"] == "UNREAD", rc["failed"]
    inv = rc["checks"]["caches"]["detail"]
    assert len(inv["other_sqlite"]) == 1 and set(inv["other_sqlite"][0]) == {"path_sha256", "tables"}
    root = inv["roots"][0]
    assert root["excluded"] == 1 and "OCMRI" not in json.dumps(inv)  # counted and hashed, never named
    assert root["excluded_sha256"] == hashlib.sha256(str(w["host"] / "OCMRI-private").encode()).hexdigest()


@pytest.mark.parametrize("break_it", [
    "look_line", "extra_cycle", "dry_run", "attempt", "staged", "reads_first", "no_witness", "wrong_registered",
    "same_code", "no_live_cache", "unreadable_sqlite", "daily_rows", "bad_commit"])
def test_any_missing_or_contrary_evidence_refuses(tmp_path, break_it):
    w = _world(tmp_path, witness=break_it != "no_witness",
               runner_c=READS_FIRST if break_it == "reads_first" else OLD_RUNNER)
    log = w["data"] / "cycle.log"
    kw = {}
    if break_it == "look_line":
        log.write_text(log.read_text() + f"2026-11-16 11:06:00 | INFO     | runner:x:1 — edge_stream look {w['pid']}: FAIL\n")
    elif break_it == "extra_cycle":
        log.write_text(log.read_text().replace(
            "2026-11-16 11:05:50 | INFO     | config:<module>:1 — GitSink attached",
            "2026-11-14 11:05:00 | INFO     | config:<module>:1 — GitSink attached\n{\n}\n"
            "2026-11-16 11:05:50 | INFO     | config:<module>:1 — GitSink attached"))
    elif break_it == "dry_run":
        log.write_text(log.read_text().replace('"dry_run": false', '"dry_run": true'))
    elif break_it == "attempt":
        (w["data"] / "attempts.jsonl").write_text(json.dumps({"registration_sha256": w["sha"], "at": "x"}) + "\n")
    elif break_it == "staged":
        (w["data"] / "looks").mkdir()
        (w["data"] / "looks" / f"{w['sha'][:16]}.json").write_text("{}")
    elif break_it == "wrong_registered":
        kw["registered_release"] = w["c"]
    elif break_it == "same_code":
        kw["abandoning_release"] = w["r"]
    elif break_it == "no_live_cache":
        (w["data"] / "bars.sqlite3").unlink()
    elif break_it == "unreadable_sqlite":
        (w["host"] / "broken.sqlite3").write_bytes(b"SQLite format 3\x00" + b"\xff" * 600)
    elif break_it == "daily_rows":
        con = sqlite3.connect(w["host"] / "daily.sqlite3")
        con.execute("CREATE TABLE daily (ticker TEXT PRIMARY KEY, start TEXT, end TEXT, bars TEXT, fetched_at TEXT)")
        con.execute("INSERT INTO daily VALUES ('GLD', '2020-01-01', '2026-11-01', '[]', 'x')")
        con.commit()
        con.close()
    elif break_it == "bad_commit":
        kw["abandoning_release"] = "0" * 40
    rc = _build(w, **kw)
    assert rc["verdict"] == "REFUSED" and rc["failed"], break_it


def test_a_ledger_read_or_look_refuses(tmp_path):
    w = _world(tmp_path)
    other = L.memory()
    prop = F.build_proposal(dict(TQ.QUEUE[0]), TODAY)
    osha = L.append_registration(other, {**prop, "sanity": S.check(prop, L.load_state(other), TODAY),
                                         "recorded_at": "2026-11-13T12:00:00+00:00"})
    L.append_read(other, osha, "look", "2026-11-14T11:00:00+00:00")
    L.append_abandoned(other, osha, "x", "2026-11-16T11:00:00+00:00", cause=L.CODE_CHANGED,
                       read_evidence={"ledger_reads": ["look"], "failed_attempts": 0, "report_staged": False})
    args = {"repo": w["repo"], "registered_release": w["r"], "abandoning_release": w["c"], "roots": [str(w["host"])],
            "numpy_mm": NP}
    assert RC.build(other, osha, w["data"], **args)["verdict"] == "REFUSED"
    assert RC.build(w["led"], "0" * 64, w["data"], **args)["verdict"] == "REFUSED"


def test_the_code_order_check_reads_the_shipped_runner():
    here = Path(F.__file__).resolve().parent / "runner.py"
    assert RC.code_check_precedes_reads(here.read_text())["ok"]
    assert RC.code_check_precedes_reads(OLD_RUNNER)["ok"]
    assert not RC.code_check_precedes_reads(READS_FIRST)["ok"]
    assert not RC.code_check_precedes_reads("def other():\n    pass\n")["ok"]


def test_the_mount_table_decides_the_roots():
    text = "\n".join([
        "24 1 253:0 / / rw,relatime shared:1 - ext4 /dev/mapper/root rw",
        "30 24 8:33 / /data rw,relatime shared:9 - ext4 /dev/sdc1 rw",
        "31 24 0:26 / /dev/shm rw shared:3 - tmpfs tmpfs rw",
        "32 24 0:5 / /proc rw - proc proc rw",
        "33 24 0:50 / /var/lib/docker/overlay2/x/merged rw - overlay overlay rw",
        r"34 24 259:1 /ocmri/private /home/grid/ocmri\040x rw - ext4 /dev/nvme1n1p1 rw",
    ])
    assert RC.disk_mounts(text) == ["/", "/data", "/dev/shm", "/home/grid/ocmri x"]
    assert RC.mounts(text)["/proc"] == "proc" and RC.mounts(text)["/var/lib/docker/overlay2/x/merged"] == "overlay"


def test_the_walk_stops_at_mount_points_without_touching_them(tmp_path):
    # /run/user/1000/doc (the desktop document portal, FUSE) refuses even root's stat
    portal = tmp_path / "doc"
    portal.mkdir()
    (portal / "inside.sqlite3").write_bytes(b"SQLite format 3\x00" + b"\x00" * 600)
    out = RC.walk(str(tmp_path), time.time() - 3600, frozenset({str(portal)}))
    assert out["mount_boundaries"] == 1 and out["dirs"] == 0 and out["sqlite"] == [] and out["errors"] == []
    assert RC.walk(str(tmp_path), time.time() - 3600)["sqlite"] == [str(portal / "inside.sqlite3")]


def test_files_unchanged_since_the_registration_are_not_opened(tmp_path):
    _cache(tmp_path / "a.sqlite3", [("GLD", "2026-01-02", "x")])
    assert RC.walk(str(tmp_path), time.time() - 3600)["sqlite"] == [str(tmp_path / "a.sqlite3")]
    later = RC.walk(str(tmp_path), time.time() + 3600)
    assert later["sqlite"] == [] and later["files"] == 1 and later["changed_since_registration"] == 0


def test_caches_are_opened_read_only(tmp_path):
    c = _cache(tmp_path / "c.sqlite3", [("GLD", "2026-01-02", "x")])
    before = c.read_bytes()
    got = RC.inspect(str(c), ["GLD", "SLV"], "2026-01-01")
    assert got["days"] == ["2026-01-02"] and got["schema"] == ["sessions"] and c.read_bytes() == before
    with pytest.raises(OSError):
        RC.inspect(str(tmp_path / "missing.sqlite3"), ["GLD"], "2026-01-01")


def test_a_wal_file_is_read_from_a_copy(tmp_path):
    """Nothing is created beside a WAL-mode file, and its uncheckpointed rows are still read."""
    db = tmp_path / "w.sqlite3"
    con = sqlite3.connect(db)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA wal_autocheckpoint=0")
    con.execute("CREATE TABLE sessions (ticker TEXT, day TEXT, bars TEXT, fetched_at TEXT)")
    con.execute("INSERT INTO sessions VALUES ('GLD', '2026-01-02', '[]', 'x')")
    con.commit()
    before = sorted(p.name for p in tmp_path.iterdir())
    got = RC.inspect(str(db), ["GLD"], "2026-01-01")
    after = sorted(p.name for p in tmp_path.iterdir())
    con.close()
    assert got["days"] == ["2026-01-02"] and got["opened"] == "copy (WAL)" and after == before


def test_the_receipt_is_written_once_and_read_only(tmp_path):
    out = tmp_path / "r.json"
    sha = RC.write({"schema": RC.SCHEMA, "verdict": "UNREAD"}, out)
    assert L.sha256_bytes(out.read_bytes()) == sha
    assert stat.S_IMODE(out.stat().st_mode) == 0o444
    with pytest.raises(FileExistsError):
        RC.write({"schema": RC.SCHEMA}, out)


# --- run time: the pinned bytes against the ledger -----------------------------------


def _pinned(tmp_path, monkeypatch, w: dict, rc: dict | None = None) -> dict:
    rc = rc or _build(w)
    rdir = tmp_path / "receipts"
    rdir.mkdir(exist_ok=True)
    monkeypatch.setattr(RC, "RECEIPTS_DIR", rdir)
    name = f"r-{len(list(rdir.iterdir()))}.json"
    sha = RC.write(rc, rdir / name)
    return {"registration_sha256": w["sha"], "lane": "history", "frees_window": True, "decision": "GRID#1379",
            "receipt": name, "receipt_sha256": sha, "receipt_verdict": rc["verdict"],
            "consumed_through_exclusive": rc["consumed_through_exclusive"]}


def test_verify_binds_the_bytes_to_this_ledger(tmp_path, monkeypatch):
    w = _world(tmp_path)
    entry = _pinned(tmp_path, monkeypatch, w)
    state = L.load_state(w["led"])
    assert RC.verify(state, entry) == []
    L.append_abandoned(w["led"], w["wsha"], "x", "2026-11-17T11:00:00+00:00")  # the ledger grows past the prefix
    assert RC.verify(L.load_state(w["led"]), entry) == []


@pytest.mark.parametrize("tamper", ["bytes", "sha", "name", "verdict", "through", "prefix", "abandonment",
                                    "registration", "witness", "refused"])
def test_verify_refuses_any_disagreement(tmp_path, monkeypatch, tamper):
    w = _world(tmp_path)
    rc = _build(w)
    if tamper == "refused":
        rc = {**rc, "verdict": "REFUSED", "failed": ["caches"]}
    entry = _pinned(tmp_path, monkeypatch, w, rc)
    state = L.load_state(w["led"])
    if tamper == "bytes":
        p = RC.RECEIPTS_DIR / entry["receipt"]
        p.chmod(0o644)
        p.write_bytes(p.read_bytes().replace(b"UNREAD", b"UNREAX", 1))
    elif tamper == "sha":
        entry["receipt_sha256"] = "0" * 64
    elif tamper == "name":
        entry["receipt"] = "../" + entry["receipt"]
    elif tamper == "verdict":
        entry["receipt_verdict"] = "UNREAD_EXCEPT"
    elif tamper == "through":
        entry["consumed_through_exclusive"] = "2030-01-01"
    elif tamper == "prefix":
        state.line_shas = ["0" * 64] * len(state.line_shas)
    elif tamper == "abandonment":
        state.abandoned[w["sha"]] = {**state.abandoned[w["sha"]], "reason": "another reason"}
    elif tamper == "registration":
        state.registrations[w["sha"]] = {**state.registrations[w["sha"]], "code_sha256": "f" * 64}
    elif tamper == "witness":
        state.registrations[w["wsha"]] = {**state.registrations[w["wsha"]], "code_sha256": "f" * 64}
    assert RC.verify(state, entry), tamper


def test_the_ledger_frees_the_window_only_through_a_verified_receipt(tmp_path, monkeypatch):
    w = _world(tmp_path)
    start = L.load_state(w["led"]).registrations[w["sha"]]["window"]["start"]
    _cache(w["host"] / "elsewhere" / "copy.db", [("GLD", start, "2026-11-22T03:41:00+00:00")])
    entry = _pinned(tmp_path, monkeypatch, w)
    pol = json.loads(P.canonical_bytes(P.policy_payload()))
    pol["unread_abandonment"]["attested"] = [entry]
    state = L.load_state(w["led"])
    state.policy = pol
    assert state.reopens_unread() and state.unread_abandoned(w["sha"]) and state.reregistrable(w["sha"])
    kept = [(c["start"], c["end"]) for c in state.consumption() if c["source"] == w["sha"]]
    assert kept == [(start, entry["consumed_through_exclusive"])]
    (RC.RECEIPTS_DIR / entry["receipt"]).chmod(0o644)
    (RC.RECEIPTS_DIR / entry["receipt"]).unlink()  # the bytes are gone: nothing is freed
    assert not state.unread_abandoned(w["sha"]) and not state.reregistrable(w["sha"])
    assert any(c["source"] == w["sha"] and c["end"] == state.registrations[w["sha"]]["window"]["end"]
               for c in state.consumption())
