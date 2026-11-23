"""Unread receipt (alpha_research.edge_stream.receipt): each source it reconciles, how it fails,
and the run-time check that binds the pinned receipt bytes to the ledger."""

from __future__ import annotations

import json
import sqlite3
import stat
import subprocess
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


MODULE_SRC = {"runner": OLD_RUNNER, "policy": '"""policy"""\nHEDGE_INSTRUMENT = "SPY"\n',
              "data": '"""data"""\nQ = "INSERT OR IGNORE INTO sessions VALUES (?, ?, ?, ?)"\n'}


def _repo(root: Path, runner_c: str = OLD_RUNNER) -> tuple[Path, str, str]:
    """Two commits of the five evaluation modules: R registered, C changed families.py."""
    pkg = root / "alpha_research" / "edge_stream"
    pkg.mkdir(parents=True)
    _git(root, "init", "-q")
    for m in RC.V3_MODULES:
        (pkg / f"{m}.py").write_text(MODULE_SRC.get(m, f'"""{m}"""\nX = 1\n'))
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


@pytest.fixture(autouse=True)
def _old_inodes(monkeypatch):
    """Test files are born now; the ledger's registration is dated 2026-11-13. Every inode
    is reported born in 2023 unless a test says otherwise."""
    monkeypatch.setattr(RC, "_birth", lambda path: 1_700_000_000)


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
            "host": tmp_path / "host", "caches": [str(data / "bars.sqlite3")]}


def _build(w: dict, **kw) -> dict:
    args = {"repo": w["repo"], "registered_release": w["r"], "abandoning_release": w["c"], "caches": w["caches"],
            "writers_through": w["c"], "numpy_mm": NP} | kw
    return RC.build(w["led"], w["sha"], w["data"], **args)


def _start(w: dict) -> str:
    return L.load_state(w["led"]).registrations[w["sha"]]["window"]["start"]


def test_unread_when_no_source_shows_a_read(tmp_path):
    w = _world(tmp_path)
    rc = _build(w)
    assert rc["verdict"] == "UNREAD" and rc["failed"] == [], rc["failed"]
    assert rc["schema"] == "edge-unread-receipt/3"
    assert rc["consumed_through_exclusive"] == rc["registration"]["window"]["start"]
    assert rc["checks"]["cycle_log"]["detail"]["cycles_between_registration_and_abandonment"] == [
        "2026-11-16T11:05:50+00:00"]
    rel = rc["checks"]["release"]["detail"]
    assert rel["registered_matches_ledger"] and rel["code_changed"] and rel["code_order"]["ok"]
    assert [x["registration_sha256"] for x in rel["witnesses"]] == [w["wsha"]]
    assert rel["abandoning_release"]["commit"] == _git(w["repo"], "rev-parse", w["c"])
    inv = rc["checks"]["caches"]["detail"]
    live = str(w["data"] / "bars.sqlite3")
    assert inv["listed"] == [live] and inv["stream_caches"] == [live] and inv["errors"] == inv["problems"] == []
    snap = inv["caches"][0]["snapshot"]
    assert snap["sha256"] == L.sha256_bytes(Path(live).read_bytes()) and snap["journal_mode"] == "delete"
    assert inv["caches"][0]["retention"]["nothing_removed"]
    wr = rc["checks"]["writers"]["detail"]
    assert wr["ok"] and wr["hedge_instrument"] == "SPY" and [x["writes"] for x in wr["data"]] == [
        ["INSERT OR IGNORE INTO sessions"]]
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


def test_sessions_found_in_a_listed_cache_stay_consumed(tmp_path):
    w = _world(tmp_path)
    w["caches"].append(str(_cache(w["host"] / "elsewhere" / "copy.db", [("SLV", _start(w), "2026-11-22T03:41:00+00:00")])))
    rc = _build(w)
    assert rc["verdict"] == "UNREAD_EXCEPT" and rc["sessions_read_on_or_after_start"] == [_start(w)]
    assert rc["consumed_through_exclusive"] > _start(w)


def test_a_cache_off_the_list_is_not_opened(tmp_path):
    """Which caches exist is the owner's list: the receipt neither finds nor counts others."""
    w = _world(tmp_path)
    _cache(w["host"] / "elsewhere" / "copy.db", [("SLV", _start(w), "2026-11-22T03:41:00+00:00")])
    rc = _build(w)
    assert rc["verdict"] == "UNREAD" and "elsewhere" not in json.dumps(rc)


def _daily(path: Path, ticker: str) -> None:
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE IF NOT EXISTS daily (ticker TEXT PRIMARY KEY, start TEXT, end TEXT, bars TEXT, fetched_at TEXT)")
    con.execute("INSERT OR REPLACE INTO daily VALUES (?, '2020-01-01', '2021-01-01', '[]', 'x')", (ticker,))
    con.commit()
    con.close()


@pytest.mark.parametrize("break_it", [
    "look_line", "extra_cycle", "dry_run", "attempt", "staged", "reads_first", "no_witness", "wrong_registered",
    "same_code", "bad_commit", "live_unlisted", "preflight_unlisted", "unreadable", "not_a_cache", "ocmri_name",
    "daily_row", "event_row", "rows_removed", "wal_mode", "journal_beside", "live_born_after", "no_birth",
    "writer_deletes_sessions", "complete_read_not_hedge", "hedge_is_instrument", "tree_writes_sessions",
    "tree_without_data", "deployed_outside_history"])
def test_any_missing_or_contrary_evidence_refuses(tmp_path, monkeypatch, break_it):
    w = _world(tmp_path, witness=break_it != "no_witness",
               runner_c=READS_FIRST if break_it == "reads_first" else OLD_RUNNER)
    log, live, pkg = w["data"] / "cycle.log", w["data"] / "bars.sqlite3", w["repo"] / "alpha_research" / "edge_stream"
    kw = {}

    def commit(module: str, text: str) -> None:
        (pkg / f"{module}.py").write_text(text)
        _git(w["repo"], "commit", "-qam", module)
        kw["writers_through"] = _git(w["repo"], "rev-parse", "HEAD")

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
    elif break_it == "bad_commit":
        kw["abandoning_release"] = "0" * 40
    elif break_it == "live_unlisted":
        w["caches"] = [str(_cache(w["host"] / "elsewhere" / "copy.db", [("GLD", "2024-11-25", "x")]))]
    elif break_it == "preflight_unlisted":
        _cache(w["data"] / "preflight" / "bars.sqlite3", [("GLD", "2024-11-25", "x")])
    elif break_it == "unreadable":
        bad = w["host"] / "broken.sqlite3"
        bad.write_bytes(b"SQLite format 3\x00" + b"\xff" * 600)
        w["caches"].append(str(bad))
    elif break_it == "not_a_cache":
        w["caches"].append(str(_cache(w["host"] / "misc.sqlite3", [("GLD", "2026-01-01", "x")], table="other")))
    elif break_it == "ocmri_name":
        w["caches"].append(str(_cache(w["host"] / "OCMRI-x" / "bars.sqlite3", [("GLD", "2024-11-25", "x")])))
    elif break_it == "daily_row":
        _daily(live, "GLD")  # a REPLACE could have narrowed its range: any daily row of an instrument refuses
    elif break_it == "event_row":
        con = sqlite3.connect(live)
        con.execute("CREATE TABLE edgar_events (ticker TEXT PRIMARY KEY, start TEXT, end TEXT, events TEXT, fetched_at TEXT)")
        con.execute("INSERT INTO edgar_events VALUES ('SLV', '2024-01-01', '2026-01-01', '[]', 'x')")
        con.commit()
        con.close()
    elif break_it == "rows_removed":
        con = sqlite3.connect(live)
        con.executemany("INSERT INTO sessions VALUES (?, ?, '[]', 'x')", [("GLD", "2024-11-26"), ("GLD", "2024-11-27")])
        con.execute("DELETE FROM sessions WHERE day = '2024-11-26'")
        con.commit()
        con.close()
    elif break_it == "wal_mode":
        con = sqlite3.connect(live)
        con.execute("PRAGMA journal_mode=WAL")
        con.close()
    elif break_it == "journal_beside":
        Path(str(live) + "-journal").write_bytes(b"\x00" * 512)
    elif break_it == "live_born_after":
        monkeypatch.setattr(RC, "_birth", lambda path: 1_800_000_000)  # 2027: after the registration
    elif break_it == "no_birth":
        monkeypatch.setattr(RC, "_birth", lambda path: None)
    elif break_it == "writer_deletes_sessions":
        commit("data", 'Q = "DELETE FROM sessions WHERE ticker = ?"\n')
    elif break_it == "complete_read_not_hedge":
        commit("families", "def f(self, daily, t):\n    return daily.daily(t, 1, 2, require_complete=True)\n")
    elif break_it == "hedge_is_instrument":
        commit("policy", 'HEDGE_INSTRUMENT = "GLD"\n')
    elif break_it in ("tree_writes_sessions", "tree_without_data"):
        tree = _tree(tmp_path / "tree")
        if break_it == "tree_writes_sessions":
            (tree / "rehearse.py").write_text('db.execute("DELETE FROM sessions")\n')
        else:
            (tree / "alpha_research" / "edge_stream" / "data.py").unlink()
        kw["writer_trees"] = [str(tree)]
    elif break_it == "deployed_outside_history":
        _git(w["repo"], "checkout", "-q", "-b", "side", w["r"])
        commit("stats", "X = 3\n")
        _git(w["repo"], "checkout", "-q", w["c"])
        kw["writers_through"] = w["c"]
    rc = _build(w, **kw)
    assert rc["verdict"] == "REFUSED" and rc["failed"] == [EXPECT.get(break_it, "caches")], (break_it, rc["failed"])


EXPECT = {"look_line": "cycle_log", "extra_cycle": "cycle_log", "dry_run": "cycle_log", "attempt": "data_dir",
          "staged": "data_dir", "reads_first": "release", "no_witness": "release", "wrong_registered": "release",
          "same_code": "release", "bad_commit": "release", "writer_deletes_sessions": "writers",
          "complete_read_not_hedge": "writers", "hedge_is_instrument": "writers", "tree_writes_sessions": "writers",
          "tree_without_data": "writers", "deployed_outside_history": "writers"}


def _tree(root: Path) -> Path:
    """A staged rehearsal tree carrying the shipped cache writers."""
    here = Path(F.__file__).resolve().parent
    pkg = root / "alpha_research" / "edge_stream"
    pkg.mkdir(parents=True)
    for m in ("data", "families"):
        (pkg / f"{m}.py").write_text((here / f"{m}.py").read_text())
    (root / "rehearse.py").write_text('con.execute("SELECT count(*) FROM sessions")\n')
    return root


def test_a_staged_tree_with_the_shipped_writers_passes(tmp_path):
    w = _world(tmp_path)
    rc = _build(w, writer_trees=[str(_tree(tmp_path / "tree"))])
    assert rc["verdict"] == "UNREAD", rc["failed"]
    t = rc["checks"]["writers"]["detail"]["trees"][0]
    assert t["ok"] and set(t["data"]) == RC.CACHE_WRITES and t["other_cache_writes"] == []
    assert rc["checks"]["writers"]["detail"]["deployed"]["not_in_history"] == []


def test_the_hedge_may_be_read_complete(tmp_path):
    w = _world(tmp_path)
    pkg = w["repo"] / "alpha_research" / "edge_stream"
    (pkg / "families.py").write_text(
        "class E:\n    def f(self, daily):\n        return daily.daily(self.hedge, 1, 2, require_complete=True)\n")
    _git(w["repo"], "commit", "-qam", "hedge read")
    assert _build(w, writers_through=_git(w["repo"], "rev-parse", "HEAD"))["verdict"] == "UNREAD"


def test_a_ledger_read_or_look_refuses(tmp_path):
    w = _world(tmp_path)
    other = L.memory()
    prop = F.build_proposal(dict(TQ.QUEUE[0]), TODAY)
    osha = L.append_registration(other, {**prop, "sanity": S.check(prop, L.load_state(other), TODAY),
                                         "recorded_at": "2026-11-13T12:00:00+00:00"})
    L.append_read(other, osha, "look", "2026-11-14T11:00:00+00:00")
    L.append_abandoned(other, osha, "x", "2026-11-16T11:00:00+00:00", cause=L.CODE_CHANGED,
                       read_evidence={"ledger_reads": ["look"], "failed_attempts": 0, "report_staged": False})
    args = {"repo": w["repo"], "registered_release": w["r"], "abandoning_release": w["c"], "caches": w["caches"],
            "writers_through": w["c"], "numpy_mm": NP}
    assert RC.build(other, osha, w["data"], **args)["verdict"] == "REFUSED"
    assert RC.build(w["led"], "0" * 64, w["data"], **args)["verdict"] == "REFUSED"


def test_the_code_order_check_reads_the_shipped_runner():
    here = Path(F.__file__).resolve().parent / "runner.py"
    assert RC.code_check_precedes_reads(here.read_text())["ok"]
    assert RC.code_check_precedes_reads(OLD_RUNNER)["ok"]
    assert not RC.code_check_precedes_reads(READS_FIRST)["ok"]
    assert not RC.code_check_precedes_reads("def other():\n    pass\n")["ok"]


def test_the_shipped_writers_pass_the_writer_rules():
    here = Path(F.__file__).resolve().parent
    assert set(RC.cache_writes((here / "data.py").read_text())) == RC.CACHE_WRITES
    assert RC._hedge_only_complete_reads((here / "families.py").read_text())
    assert P.HEDGE_INSTRUMENT == "SPY"


def test_the_snapshot_is_the_hashed_image_and_leaves_the_cache_alone(tmp_path):
    c = _cache(tmp_path / "c.sqlite3", [("GLD", "2026-01-02", "x")])
    before = sorted(p.name for p in tmp_path.iterdir()), c.read_bytes()
    got = RC.inspect(str(c), ["GLD", "SLV"], "2026-01-01")
    assert got["days"] == ["2026-01-02"] and got["schema"] == ["sessions"]
    assert got["snapshot"]["sha256"] == L.sha256_bytes(before[1]) and got["snapshot"]["bytes"] == len(before[1])
    assert (sorted(p.name for p in tmp_path.iterdir()), c.read_bytes()) == before
    with pytest.raises(OSError):
        RC.inspect(str(tmp_path / "missing.sqlite3"), ["GLD"], "2026-01-01")


def test_a_writer_cannot_commit_while_the_snapshot_holds_its_lock(tmp_path, monkeypatch):
    """The copy runs under the snapshot connection's SHARED lock: a writer that tries to
    commit mid-copy is refused, so the copied image is one committed state."""
    c = _cache(tmp_path / "c.sqlite3", [("GLD", "2026-01-02", "x")])
    tried = []
    real_open = open

    def spying_open(path, mode="r", *a, **k):
        if str(path) == str(c) and mode == "rb":
            w = sqlite3.connect(c, timeout=0)
            try:
                w.execute("INSERT INTO sessions VALUES ('GLD', '2026-01-05', '[]', 'y')")
                w.commit()
                tried.append("committed")
            except sqlite3.OperationalError:
                tried.append("locked")
            finally:
                w.close()
        return real_open(path, mode, *a, **k)

    monkeypatch.setattr("builtins.open", spying_open)
    got = RC.inspect(str(c), ["GLD"], "2026-01-01")
    assert tried == ["locked"] and got["days"] == ["2026-01-02"]


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
            "consumed_through_exclusive": rc["consumed_through_exclusive"],
            "inventory": rc["checks"]["caches"]["detail"]["listed"] if "caches" in rc["checks"] else []}


def test_verify_binds_the_bytes_to_this_ledger(tmp_path, monkeypatch):
    w = _world(tmp_path)
    entry = _pinned(tmp_path, monkeypatch, w)
    state = L.load_state(w["led"])
    assert RC.verify(state, entry) == []
    L.append_abandoned(w["led"], w["wsha"], "x", "2026-11-17T11:00:00+00:00")  # the ledger grows past the prefix
    assert RC.verify(L.load_state(w["led"]), entry) == []


@pytest.mark.parametrize("tamper", ["bytes", "sha", "name", "verdict", "through", "prefix", "abandonment",
                                    "registration", "witness", "refused", "inventory"])
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
    elif tamper == "inventory":
        entry["inventory"] = [*entry["inventory"], "/another/bars.sqlite3"]
    assert RC.verify(state, entry), tamper


def test_the_ledger_frees_the_window_only_through_a_verified_receipt(tmp_path, monkeypatch):
    w = _world(tmp_path)
    start = _start(w)
    w["caches"].append(str(_cache(w["host"] / "elsewhere" / "copy.db", [("GLD", start, "2026-11-22T03:41:00+00:00")])))
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
