"""Pure tests for scripts/run_godview_writers.py and the deploy/systemd god view units (G7).

No database: the writer is replaced by a stub, the engine by a fake. The
real-PostgreSQL dry-run / commit / timeout behaviour is in
tests/godview/test_cftc_positioning_writer_pg.py.
"""

from __future__ import annotations

import configparser
import pathlib
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

import scripts.run_godview_writers as runner
import tests.test_godview_no_incident_imports as guard

REPO = pathlib.Path(__file__).resolve().parent.parent
UNIT_DIR = REPO / "deploy" / "systemd"


# ── arguments and code sha ────────────────────────────────────────────────


def test_pillar_and_a_code_sha_source_are_required():
    with pytest.raises(SystemExit):
        runner.parse_args(["--code-sha", "abcdef1"])
    with pytest.raises(SystemExit):
        runner.parse_args(["--pillar", "cftc"])
    with pytest.raises(SystemExit):
        runner.parse_args(["--pillar", "gex", "--code-sha", "abcdef1"])
    with pytest.raises(SystemExit):
        runner.parse_args(["--pillar", "fed", "--code-sha", "abcdef1", "--code-sha-from-git"])


def test_parse_args_defaults_and_aware_as_of_ts():
    a = runner.parse_args(["--pillar", "fed", "--code-sha", "ABCDEF1", "--as-of-ts", "2026-09-25T20:00:00"])
    assert a.dry_run is False and a.start is None
    assert a.as_of_ts == datetime(2026, 9, 25, 20, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("bad", ["", "xyz", "abc", "g" * 40, "a" * 41, "--dirty"])
def test_invalid_code_sha_is_refused(bad):
    with pytest.raises(runner.CodeShaError):
        runner.validate_code_sha(bad)


def test_code_sha_from_git_returns_head_when_clean():
    calls = []

    def git(args):
        calls.append(args)
        return "0123456789abcdef0123456789abcdef01234567\n" if args[0] == "rev-parse" else ""

    assert runner.code_sha_from_git(git) == "0123456789abcdef0123456789abcdef01234567"
    status = calls[1]
    assert status[:3] == ["status", "--porcelain", "--untracked-files=no"]
    assert "godview" in status and "scripts/run_godview_writers.py" in status


def test_code_sha_from_git_refuses_a_modified_import_surface():
    def git(args):
        return "0123456789abcdef0123456789abcdef01234567" if args[0] == "rev-parse" else " M godview/x.py\n"

    with pytest.raises(runner.CodeShaError, match="modified"):
        runner.code_sha_from_git(git)


def test_code_sha_from_git_refuses_when_git_fails():
    def git(args):
        raise subprocess.CalledProcessError(128, ["git"], stderr="dubious ownership")

    with pytest.raises(runner.CodeShaError):
        runner.code_sha_from_git(git)


# ── transaction wrapper ───────────────────────────────────────────────────


class _FakeTrans:
    def __init__(self, log):
        self.log = log

    def commit(self):
        self.log.append("commit")

    def rollback(self):
        self.log.append("rollback")


class _FakeConn:
    def __init__(self, log):
        self.log = log

    def begin(self):
        self.log.append("begin")
        return _FakeTrans(self.log)

    def execute(self, stmt, *a, **k):
        self.log.append(str(stmt))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.log.append("close")
        return False


class _FakeEngine:
    def __init__(self):
        self.log: list[str] = []

    def connect(self):
        return _FakeConn(self.log)


@pytest.mark.parametrize("commit,final", [(True, "commit"), (False, "rollback")])
def test_transaction_engine_sets_timeouts_then_commits_or_rolls_back(commit, final):
    eng = _FakeEngine()
    with runner.TransactionEngine(eng, commit=commit).begin() as conn:
        conn.execute("UPSERT")
    assert eng.log == [
        "begin",
        f"SET LOCAL lock_timeout = '{runner.LOCK_TIMEOUT}'",
        f"SET LOCAL statement_timeout = '{runner.STATEMENT_TIMEOUT}'",
        "UPSERT",
        final,
        "close",
    ]


def test_transaction_engine_rolls_back_on_error_even_when_committing():
    eng = _FakeEngine()
    with pytest.raises(RuntimeError), runner.TransactionEngine(eng, commit=True).begin():
        raise RuntimeError("boom")
    assert "commit" not in eng.log and eng.log[-2:] == ["rollback", "close"]


# ── run(): dispatch, summary, exit codes ──────────────────────────────────


@dataclass(frozen=True)
class _Row:
    status: str
    reason: str | None = None


@dataclass(frozen=True)
class _Result:
    status: str
    rows_written: int = 0
    rows_skipped: int = 0
    rows: tuple = ()
    code_sha: str | None = None
    run_id: str | None = "r1"
    message: str = ""


@pytest.mark.parametrize(
    "status,code",
    [
        ("SUCCESS", runner.EXIT_OK),
        ("SUCCESS_NOOP", runner.EXIT_OK),
        ("PARTIAL_BLOCKED_BY_LEGACY", runner.EXIT_OK),
        ("EMPTY", runner.EXIT_EMPTY),
        ("FAILED", runner.EXIT_FAILED),
        ("SOMETHING_NEW", runner.EXIT_FAILED),
    ],
)
def test_exit_code_for(status, code):
    assert runner.exit_code_for(status) == code


@pytest.mark.parametrize("pillar", runner.PILLARS)
def test_run_dispatches_with_sha_and_wrapped_engine(monkeypatch, pillar):
    seen = {}

    def fake_writer(engine, *, code_sha, start, as_of_ts):
        seen.update(engine=engine, code_sha=code_sha, start=start, as_of_ts=as_of_ts)
        return _Result(
            status="PARTIAL_BLOCKED_BY_LEGACY",
            rows_written=1,
            rows_skipped=2,
            rows=(_Row("written"), _Row("noop"), _Row("skipped", "x"), _Row("skipped", "x")),
            code_sha=code_sha,
        )

    monkeypatch.setattr(runner, "_writer", lambda p: fake_writer)
    args = runner.parse_args(["--pillar", pillar, "--code-sha", "abcdef1", "--dry-run"])
    code, summary = runner.run(args, engine=object())
    assert code == runner.EXIT_OK
    assert isinstance(seen["engine"], runner.TransactionEngine) and seen["engine"].commit is False
    assert seen["code_sha"] == "abcdef1"
    assert summary["pillar"] == pillar and summary["dry_run"] is True and summary["persisted"] is False
    assert summary["rows_unchanged"] == 1 and summary["skip_reasons"] == {"x": 2}


def test_writer_dispatch_targets_the_g3_and_g4_writers():
    from godview.cftc_positioning import materialize_cftc_positioning
    from godview.fed_liquidity import materialize_fed_liquidity

    assert runner._writer("fed") is materialize_fed_liquidity
    assert runner._writer("cftc") is materialize_cftc_positioning


def test_timeout_literals_match_constants():
    src = (REPO / "scripts" / "run_godview_writers.py").read_text(encoding="utf-8")
    assert f"SET LOCAL lock_timeout = '{runner.LOCK_TIMEOUT}'" in src
    assert f"SET LOCAL statement_timeout = '{runner.STATEMENT_TIMEOUT}'" in src


# ── guard self-test ───────────────────────────────────────────────────────


def test_incident_guard_detects_an_incident_import(tmp_path):
    f = tmp_path / "x.py"
    f.write_text(
        "from ingestion.altdata import cftc_materializer\n"
        "import derivatives.dealer_gex_engine\n"
        "from api.routers.god_view import router\n"
        "importlib.import_module('ingestion.god_view_materializer')\n",
        encoding="utf-8",
    )
    names = guard._imported_names(f)
    assert sum(guard._is_incident(n) for n in names) >= 4


# ── systemd unit templates ────────────────────────────────────────────────


def _unit(name: str) -> configparser.ConfigParser:
    cp = configparser.ConfigParser(strict=False, interpolation=None, comment_prefixes=("#", ";"))
    cp.optionxform = str  # keep case
    # systemd allows repeated keys (OnCalendar); collect them.
    text = (UNIT_DIR / name).read_text(encoding="utf-8")
    cp.read_string(re.sub(r"^OnCalendar=", lambda m: f"OnCalendar{next(_counter)}=", text, flags=re.MULTILINE))
    return cp


_counter = iter(range(10_000))


@pytest.mark.parametrize("pillar", runner.PILLARS)
def test_service_unit_runs_the_runner_from_the_release_tree(pillar):
    svc = _unit(f"grid-godview-{pillar}.service")["Service"]
    assert svc["Type"] == "oneshot"
    assert svc["User"] == "grid"
    assert svc["WorkingDirectory"] == "/data/grid_v4/grid_release"
    assert svc["EnvironmentFile"].startswith("/")
    exec_start = svc["ExecStart"]
    assert exec_start.startswith("/usr/bin/flock -w ")
    assert "/tmp/grid-godview-writers.lock" in exec_start
    assert f"scripts/run_godview_writers.py --pillar {pillar} --code-sha-from-git" in exec_start
    assert "--dry-run" not in exec_start


@pytest.mark.parametrize(
    "pillar,calendars",
    [
        ("fed", ["Thu *-*-* 17:30:00 America/New_York", "Fri *-*-* 09:00:00 America/New_York"]),
        ("cftc", ["Fri *-*-* 16:00:00 America/New_York", "Sat *-*-* 14:00:00 America/New_York"]),
    ],
)
def test_timer_schedule(pillar, calendars):
    timer = _unit(f"grid-godview-{pillar}.timer")
    t = timer["Timer"]
    assert [v for k, v in t.items() if k.startswith("OnCalendar")] == calendars
    assert t["Unit"] == f"grid-godview-{pillar}.service"
    assert t["Persistent"] == "true"
    assert timer["Install"]["WantedBy"] == "timers.target"


def test_units_are_templates_not_installed_by_any_workflow():
    for name in ("grid-godview-fed", "grid-godview-cftc"):
        for ext in ("service", "timer"):
            assert "TEMPLATE ONLY" in (UNIT_DIR / f"{name}.{ext}").read_text(encoding="utf-8")
    for wf in (REPO / ".github" / "workflows").glob("*.yml"):
        text = wf.read_text(encoding="utf-8")
        assert not re.search(r"grid-godview-(fed|cftc)\.(service|timer)", text), wf.name
        assert "systemctl enable" not in text or "grid-godview" not in text, wf.name
