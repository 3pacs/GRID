"""Schwab mode: the full-universe options run on its own timer, and the scheduler writers hand it off.

Pins (2026-11-22, pre-open list item 7):

* ``scripts/run_options_dedicated.py`` runs only with schwab mode fully on and
  GRID_OPTIONS_DEDICATED_TIMER=1, under its own writer label, with its own wall
  budget and the module's 1,000-request Yahoo cap (never a window-sized one),
  and still honours the cap, the 429 latch, the hold marker and the cursor;
* with schwab mode and the flag, the smart scheduler's and the daily
  scheduler's (from 10:20 New York) full-universe runs request nothing, and
  every subset writer is unaffected;
* without the flag, or on any other provider, nothing changes (A/B);
* the unit and timer templates are what the install note says.

Offline: every client is a stand-in; every state file is under tmp_path.
"""

from __future__ import annotations

import ast
import inspect
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

import ingestion.smart_scheduler as ss
from ingestion import options, options_schwab, scheduler
from scripts import gem_daily_capture as gem
from scripts import run_options_dedicated as rod
from tests.test_options_schwab_provider import SESSION_NOW, _body, _client, _FakeYahoo, _puller

CORE = ["KO"]
TAIL = ["ACIU", "AGEN", "NUOX", "OLMA"]
MORNING = datetime(2026, 11, 5, 14, 40, tzinfo=timezone.utc)     # 09:40 New York, inside GEM's window
REPO = Path(__file__).resolve().parent.parent
SYSTEMD = REPO / "deploy" / "systemd"
_REAL_PULLER = options.OptionsPuller   # _setup patches the module's name; each setup starts from the class


@pytest.fixture(autouse=True)
def _state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(options, "_utc_now", lambda: SESSION_NOW)            # 15:30 New York
    monkeypatch.setenv("GRID_OPTIONS_YAHOO_HOLD_FILE", str(tmp_path / "hold.json"))
    monkeypatch.setenv("GRID_OPTIONS_DEDICATED_LOCK_FILE", str(tmp_path / "dedicated.lock"))
    monkeypatch.setattr(options_schwab.time, "sleep", lambda _s: None)


def _schwab_env(monkeypatch: pytest.MonkeyPatch, *, flag: str | None = "1") -> None:
    monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", "schwab")
    monkeypatch.setenv("GRID_OPTIONS_SCHWAB_READERS", "admin-only")
    if flag is None:
        monkeypatch.delenv("GRID_OPTIONS_DEDICATED_TIMER", raising=False)
    else:
        monkeypatch.setenv("GRID_OPTIONS_DEDICATED_TIMER", flag)


def _setup(monkeypatch: pytest.MonkeyPatch, *answers: Any, block: str | None = None,
           yahoo: _FakeYahoo | None = None, catalysts: list[str] | None = None) -> SimpleNamespace:
    """The real pull_all in schwab mode with stand-in clients (tests/test_options_schwab_provider.py's shape)."""
    monkeypatch.setattr(options, "EQUITY_TICKERS", CORE)
    catalyst_calls: list[int] = []
    monkeypatch.setattr(options, "catalyst_options_universe",
                        lambda *_a, **_kw: catalyst_calls.append(1) or list(TAIL if catalysts is None else catalysts))
    writers: list[str | None] = []
    monkeypatch.setattr(options_schwab, "schwab_core_block",
                        lambda _now, *, writer=None: writers.append(writer) or block)
    schwab, market, _sleeps = _client(*answers, max_expirations=2)
    monkeypatch.setattr(options_schwab, "SchwabOptionsClient", lambda **_kw: schwab)
    yahoo = yahoo if yahoo is not None else _FakeYahoo()
    monkeypatch.setattr(options, "YahooOptionsClient", lambda: yahoo)
    monkeypatch.setattr(options, "CboeFirstOptionsClient", lambda: pytest.fail("no Cboe client under schwab"))
    monkeypatch.setattr(options, "OptionsPuller", _REAL_PULLER)
    puller = _puller(monkeypatch)
    published: list[bool] = []
    monkeypatch.setattr(puller, "_mark_catalog_pulled", lambda **_kw: published.append(True) or True)
    monkeypatch.setattr(options, "OptionsPuller", lambda **_kw: puller)
    return SimpleNamespace(puller=puller, market=market, yahoo=yahoo, writers=writers,
                           published=published, catalyst_calls=catalyst_calls)


def _main(capsys: pytest.CaptureFixture[str], argv: list[str] | None = None,
          clock: Any = None) -> tuple[int, dict[str, Any]]:
    kwargs: dict[str, Any] = {"engine": object()}
    if clock is not None:
        kwargs["clock"] = clock
    rc = rod.main(argv or [], **kwargs)
    line = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("OPTIONS_DEDICATED ")][-1]
    return rc, json.loads(line.removeprefix("OPTIONS_DEDICATED "))


def _tail_order(yahoo: _FakeYahoo) -> list[str]:
    seen: list[str] = []
    for ticker, _expiry in yahoo.calls:
        if ticker not in seen:
            seen.append(ticker)
    return seen


# ── the runner refuses outside schwab mode ───────────────────────────────


@pytest.mark.parametrize("env", [
    {"GRID_OPTIONS_PRIMARY_PROVIDER": None},
    {"GRID_OPTIONS_PRIMARY_PROVIDER": "cboe"},
    {"GRID_OPTIONS_PRIMARY_PROVIDER": "yahoo"},
    {"GRID_OPTIONS_SCHWAB_READERS": None},
    {"GRID_OPTIONS_SCHWAB_READERS": "everyone"},
    {"GRID_OPTIONS_DEDICATED_TIMER": None},
    {"GRID_OPTIONS_DEDICATED_TIMER": "0"},
    {"GRID_OPTIONS_DEDICATED_TIMER": "true"},
], ids=lambda e: ",".join(f"{k.removeprefix('GRID_OPTIONS_')}={v}" for k, v in e.items()))
def test_the_runner_refuses_unless_schwab_mode_and_the_timer_are_on(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path, env: dict) -> None:
    _schwab_env(monkeypatch)
    for key, value in env.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)
    monkeypatch.setattr(options, "OptionsPuller", lambda **_kw: pytest.fail("no writer outside schwab mode"))
    rc, out = _main(capsys)
    assert rc == 0 and out["status"] == "REFUSED"
    expected = "dedicated timer not declared" if "GRID_OPTIONS_DEDICATED_TIMER" in env else "not in schwab mode"
    assert out["reason"].startswith(expected)
    assert not (tmp_path / "dedicated.lock").exists()                         # no lock, no state touched


def test_the_selector_must_confirm_schwab(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _schwab_env(monkeypatch)
    monkeypatch.setattr(options, "options_primary_provider", lambda: "cboe")
    monkeypatch.setattr(options, "OptionsPuller", lambda **_kw: pytest.fail("selector said cboe"))
    rc, out = _main(capsys)
    assert (rc, out["reason"]) == (0, "not in schwab mode: the provider selector did not choose schwab")


def test_a_second_run_while_one_holds_the_lock_does_nothing(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    _schwab_env(monkeypatch)
    monkeypatch.setattr(options, "OptionsPuller", lambda **_kw: pytest.fail("lock is held"))
    held = rod._acquire(tmp_path / "dedicated.lock")
    assert held is not None
    try:
        rc, out = _main(capsys)
    finally:
        rod._release(held)
    assert (rc, out["status"], out["reason"]) == (0, "REFUSED", "another dedicated run holds the lock")
    assert oct((tmp_path / "dedicated.lock").stat().st_mode & 0o777) == "0o600"


@pytest.mark.parametrize("raw", ["59", "1801", "abc"])
def test_the_budget_is_bounded(raw: str) -> None:
    with pytest.raises(SystemExit) as exc:
        rod.main(["--budget-seconds", raw], engine=object())
    assert exc.value.code == 2


# ── the runner's run ─────────────────────────────────────────────────────


def test_a_full_run_uses_its_own_writer_and_the_module_yahoo_cap(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _schwab_env(monkeypatch)
    monkeypatch.setattr(options_schwab, "yahoo_budget_for_window",
                        lambda *_a: pytest.fail("no scheduler window: the Yahoo budget is the module cap"))
    env = _setup(monkeypatch, _body())
    rc, out = _main(capsys)
    assert rc == 0 and out["status"] == "SUCCESS" and out["writer"] == "options_dedicated"
    assert out["budget_seconds"] == rod.DEFAULT_BUDGET_SECONDS == 25 * 60
    assert env.writers == ["options_dedicated"]                               # the per-writer Schwab gap
    assert len(env.market.calls) == 1 and _tail_order(env.yahoo) == TAIL
    assert env.puller._yahoo.yahoo_request_budget is None                     # YAHOO_RUN_REQUEST_BUDGET applies
    assert env.published == [True]
    cursor = options_schwab.tail_cursor_file("options_dedicated")
    assert cursor.name == "options_tail_cursor.options_dedicated.json" and cursor.exists()
    assert not options_schwab.tail_cursor_file("smart_scheduler").exists()


def test_the_call_passes_no_deadline_and_a_wall_budget(monkeypatch: pytest.MonkeyPatch,
                                                       capsys: pytest.CaptureFixture[str]) -> None:
    _schwab_env(monkeypatch)
    seen: dict[str, Any] = {}

    class Puller:
        def __init__(self, db_engine: Any) -> None:
            seen["engine"] = db_engine

        def pull_all(self, **kwargs: Any) -> options.OptionsPullResults:
            seen.update(kwargs)
            return options.OptionsPullResults([{"ticker": "KO", "status": "FAILED", "rows_inserted": 0,
                                                "error": "x"}], full_universe=True)

    monkeypatch.setattr(options, "OptionsPuller", Puller)
    now = {"t": 100.0}
    rc, out = _main(capsys, ["--budget-seconds", "600"], clock=lambda: now["t"])
    assert rc == 1 and out["status"] == "FAILED"                              # FAILED turns the unit red
    assert set(seen) == {"engine", "should_continue", "capture_source"}       # no deadline_at, full universe
    assert seen["capture_source"] == "options_dedicated"
    assert seen["should_continue"]() is True
    now["t"] = 699.0
    assert seen["should_continue"]() is True
    now["t"] = 700.0
    assert seen["should_continue"]() is False


def test_the_wall_budget_defers_the_rest_and_the_cursor_resumes_it(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _schwab_env(monkeypatch)
    now = {"t": 0.0}

    class SlowYahoo(_FakeYahoo):
        def get_options(self, ticker: str, expiry_ts: int | None = None) -> Any:
            page = super().get_options(ticker, expiry_ts)
            if ticker == "AGEN" and expiry_ts is not None:
                now["t"] = 10_000.0                                           # the wall budget runs out
            return page

    env = _setup(monkeypatch, _body(), yahoo=SlowYahoo())
    rc, out = _main(capsys, clock=lambda: now["t"])
    assert rc == 0 and out["status"] == "PARTIAL" and out["tickers_deferred"] >= 2
    assert env.published == []
    assert _tail_order(env.yahoo)[:2] == ["ACIU", "AGEN"]

    now["t"] = 0.0
    env2 = _setup(monkeypatch, block="schwab_recent_capture")                 # the writer's own 45-min gap
    rc, out = _main(capsys, clock=lambda: now["t"])
    assert rc == 0
    assert _tail_order(env2.yahoo)[0] == "AGEN"                               # resumes where the budget cut it


def test_the_request_cap_still_applies_and_the_next_run_resumes(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _schwab_env(monkeypatch)
    monkeypatch.setattr(options_schwab, "YAHOO_RUN_REQUEST_BUDGET", 3)        # two names a run
    env = _setup(monkeypatch, _body())
    rc, out = _main(capsys)
    assert rc == 0 and out["status"] == "PARTIAL"
    assert _tail_order(env.yahoo) == ["ACIU", "AGEN"]
    env2 = _setup(monkeypatch, block="schwab_recent_capture")
    _main(capsys)
    assert _tail_order(env2.yahoo) == ["NUOX", "OLMA"]


def test_a_429_latches_the_run_and_holds_the_next(monkeypatch: pytest.MonkeyPatch,
                                                  capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    _schwab_env(monkeypatch)
    env = _setup(monkeypatch, _body(), yahoo=_FakeYahoo(limited_at_request=2))
    rc, out = _main(capsys)
    assert rc == 0 and out["status"] == "PARTIAL"
    assert _tail_order(env.yahoo) == ["ACIU"]                                 # nothing after the 429
    assert json.loads((tmp_path / "hold.json").read_text())["status"] == 429
    env2 = _setup(monkeypatch, block="schwab_recent_capture")
    rc, out = _main(capsys)
    assert rc == 0 and out["status"] == "SKIPPED" and env2.yahoo.calls == []
    assert "yahoo_recent_rate_limit" in out["reason"]


# ── the hand-off ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("writer,now,handed_off", [
    ("smart_scheduler", SESSION_NOW, True),
    ("smart_scheduler", MORNING, True),
    ("daily_scheduler", SESSION_NOW, True),
    ("daily_scheduler", datetime(2026, 11, 5, 15, 20, tzinfo=timezone.utc), True),    # 10:20 New York
    ("daily_scheduler", datetime(2026, 11, 5, 15, 19, tzinfo=timezone.utc), False),   # 10:19: GEM's window
    ("daily_scheduler", MORNING, False),
    ("options_dedicated", SESSION_NOW, False),
    ("gem", SESSION_NOW, False),
    ("oracle_forward", SESSION_NOW, False),
    ("options_puller", SESSION_NOW, False),
])
def test_only_the_scheduler_writers_hand_off(monkeypatch: pytest.MonkeyPatch, writer: str,
                                             now: datetime, handed_off: bool) -> None:
    _schwab_env(monkeypatch)
    assert options.dedicated_timer_handoff(writer, now) == ("dedicated_options_timer" if handed_off else None)


@pytest.mark.parametrize("provider,readers,flag", [
    ("schwab", "admin-only", None), ("schwab", "admin-only", "0"), ("schwab", "admin-only", "yes"),
    ("schwab", None, "1"), (None, "admin-only", "1"), ("cboe", "admin-only", "1"), ("yahoo", "admin-only", "1"),
])
def test_no_hand_off_unless_both_are_set(monkeypatch: pytest.MonkeyPatch, provider: str | None,
                                         readers: str | None, flag: str | None) -> None:
    for key, value in (("GRID_OPTIONS_PRIMARY_PROVIDER", provider), ("GRID_OPTIONS_SCHWAB_READERS", readers),
                       ("GRID_OPTIONS_DEDICATED_TIMER", flag)):
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)
    if provider != "schwab" or flag != "1":
        monkeypatch.setattr(options, "options_primary_provider", lambda: pytest.fail("not even consulted"))
    for writer in ("smart_scheduler", "daily_scheduler"):
        assert options.dedicated_timer_handoff(writer, SESSION_NOW) is None


@pytest.mark.parametrize("writer", ["smart_scheduler", "daily_scheduler"])
def test_a_handed_off_run_requests_nothing(monkeypatch: pytest.MonkeyPatch, writer: str) -> None:
    _schwab_env(monkeypatch)
    env = _setup(monkeypatch, _body())
    log = MagicMock()
    monkeypatch.setattr(options, "log", log)
    results = env.puller.pull_all(capture_source=writer)
    assert [(r["ticker"], r["status"], r["reason"]) for r in results] == [("KO", "SKIPPED", "dedicated_options_timer")]
    assert results.summary["status"] == "SKIPPED" and "dedicated_options_timer" in results.summary["reason"]
    assert env.market.calls == [] and env.yahoo.calls == [] and env.catalyst_calls == [] and env.writers == []
    assert not options_schwab.tail_cursor_file(writer).exists()
    message = log.info.call_args.args[0]
    assert "handed off" in message and not any(marker in message for marker in gem._FAILURE_MARKERS)


def test_the_smart_scheduler_job_reads_the_hand_off_as_a_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    _schwab_env(monkeypatch)
    env = _setup(monkeypatch, _body())
    adapter = ss._OptionsSchedulerAdapter.__new__(ss._OptionsSchedulerAdapter)
    adapter._engine, adapter._puller = None, env.puller
    summary = adapter.pull(lambda: True, deadline_at=1840.0)
    assert ss._classify_outcome(summary)[0] == ss.OUTCOME_SKIPPED           # SKIP_RETRY_MINUTES, no failure streak
    assert env.market.calls == [] and env.yahoo.calls == []


def test_the_daily_morning_pull_stays_for_gem(monkeypatch: pytest.MonkeyPatch) -> None:
    _schwab_env(monkeypatch)
    monkeypatch.setattr(options, "_utc_now", lambda: MORNING)
    env = _setup(monkeypatch, _body())
    results = env.puller.pull_all(capture_source="daily_scheduler")
    assert results.summary["status"] == "SUCCESS" and env.writers == ["daily_scheduler"]
    assert _tail_order(env.yahoo) == TAIL


@pytest.mark.parametrize("kwargs", [
    {"tickers": ["KO", "OLMA"], "include_catalyst_universe": False, "max_expirations": 6, "capture_source": "gem"},
    {"tickers": ["KO"], "capture_source": "oracle_forward"},
    {"include_catalyst_universe": False, "capture_source": "smart_scheduler"},
    {"max_expirations": 6, "capture_source": "daily_scheduler"},
])
def test_subset_writers_are_unaffected(monkeypatch: pytest.MonkeyPatch, kwargs: dict) -> None:
    _schwab_env(monkeypatch)
    env = _setup(monkeypatch, _body())
    results = env.puller.pull_all(**kwargs)
    assert all(r.get("reason") != "dedicated_options_timer" for r in results)
    assert len(env.market.calls) == 1                                         # KO was requested


def _daily_block() -> ast.Module:
    nodes = ast.parse(inspect.getsource(scheduler._run_equity_pulls)).body[0].body
    block = next(node for node in nodes if isinstance(node, ast.Try)
                 and any(isinstance(item, ast.ImportFrom) and item.module == "ingestion.options"
                         for item in node.body))
    return ast.Module(body=[block], type_ignores=[])


@pytest.mark.parametrize("rows,expected", [
    ([{"ticker": "KO", "status": "SKIPPED", "rows_inserted": 0, "reason": "dedicated_options_timer"}],
     "Options daily pull handed off to the dedicated options timer ({n} tickers not requested here)"),
    ([{"ticker": "KO", "status": "SUCCESS", "snapshots": 2}],
     "Options daily pull complete — {ok}/{total} tickers, {snaps} snapshots"),
    ([{"ticker": "KO", "status": "SKIPPED", "rows_inserted": 0, "reason": "post-session"}],
     "Options daily pull complete — {ok}/{total} tickers, {snaps} snapshots"),
    ([], "Options daily pull complete — {ok}/{total} tickers, {snaps} snapshots"),
])
def test_the_daily_slot_logs_a_hand_off_without_the_line_gem_reads(
        monkeypatch: pytest.MonkeyPatch, rows: list[dict], expected: str) -> None:
    monkeypatch.setenv("TESTING", "true")
    monkeypatch.setenv("DB_PASSWORD", "synthetic-test-only")
    import db

    puller = MagicMock()
    puller.pull_all.return_value = rows
    monkeypatch.setattr(db, "get_engine", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(options, "OptionsPuller", lambda **_kwargs: puller)
    log = MagicMock()
    exec(compile(_daily_block(), "actual-daily-options-block", "exec"), {**vars(scheduler), "log": log})
    puller.pull_all.assert_called_once_with(capture_source="daily_scheduler")
    assert [c.args[0] for c in log.info.call_args_list] == [expected]
    log.error.assert_not_called()
    if "handed off" in expected:
        assert not any(marker in expected for marker in gem._FAILURE_MARKERS)
        assert not gem._COMPLETE_RE.search(expected) and "Options daily pull complete" not in expected


def test_the_daily_cut_off_is_gems_absolute_deadline() -> None:
    day = SESSION_NOW.date()
    deadline = gem._session_deadline(day).astimezone(options._EQUITY_TZ)
    assert (deadline.hour, deadline.minute) == options.GEM_TRIGGER_WINDOW_END_ET


# ── the default path is untouched (A/B) ──────────────────────────────────


def _default_runs(monkeypatch: pytest.MonkeyPatch, writer: str, flags: list[str | None]) -> list[tuple]:
    monkeypatch.setattr(options, "EQUITY_TICKERS", CORE)
    monkeypatch.setattr(options, "catalyst_options_universe", lambda *_a, **_kw: TAIL)
    runs: list[tuple] = []
    for flag in flags:
        if flag is None:
            monkeypatch.delenv("GRID_OPTIONS_DEDICATED_TIMER", raising=False)
        else:
            monkeypatch.setenv("GRID_OPTIONS_DEDICATED_TIMER", flag)
        yahoo = _FakeYahoo()
        monkeypatch.setattr(options, "CboeFirstOptionsClient", lambda y=yahoo: y)
        monkeypatch.setattr(options, "YahooOptionsClient", lambda y=yahoo: y)
        puller = _puller(monkeypatch)
        monkeypatch.setattr(puller, "_mark_catalog_pulled", lambda **_kw: True)
        results = puller.pull_all(capture_source=writer)
        runs.append(([{k: v for k, v in r.items() if k != "capture_batch_id"} for r in results],
                     list(yahoo.calls), results.summary))
    return runs


@pytest.mark.parametrize("writer", ["smart_scheduler", "daily_scheduler", "options_puller"])
@pytest.mark.parametrize("provider", [None, "cboe", "yahoo"])
def test_the_default_path_is_identical_with_or_without_the_flag(
        monkeypatch: pytest.MonkeyPatch, provider: str | None, writer: str) -> None:
    if provider is None:
        monkeypatch.delenv("GRID_OPTIONS_PRIMARY_PROVIDER", raising=False)
    else:
        monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", provider)
    monkeypatch.setenv("GRID_OPTIONS_SCHWAB_READERS", "admin-only")          # inert without the schwab provider
    runs = _default_runs(monkeypatch, writer, [None, "1", "0", None])
    first = runs[0]
    assert [r["ticker"] for r in first[0]] == CORE + TAIL and first[2]["status"] == "SUCCESS"
    assert all(run == first for run in runs)
    assert not Path(os.environ["GRID_OPTIONS_TAIL_CURSOR_DIR"]).exists()


@pytest.mark.parametrize("provider", [None, "cboe", "yahoo", "schwab"])
def test_without_the_flag_the_hand_off_is_never_consulted(monkeypatch: pytest.MonkeyPatch,
                                                          provider: str | None) -> None:
    if provider is None:
        monkeypatch.delenv("GRID_OPTIONS_PRIMARY_PROVIDER", raising=False)
    elif provider == "schwab":
        _schwab_env(monkeypatch, flag=None)
    else:
        monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", provider)
    monkeypatch.setattr(options, "dedicated_timer_handoff", lambda *_a, **_k: pytest.fail("flag unset"))
    if provider == "schwab":
        env = _setup(monkeypatch, _body())
        for writer in ("smart_scheduler", "daily_scheduler"):
            env.puller.pull_all(capture_source=writer)
        assert env.writers == ["smart_scheduler", "daily_scheduler"]          # both ran as before
    else:
        _default_runs(monkeypatch, "smart_scheduler", [None])


# ── the unit and timer templates ─────────────────────────────────────────


def _unit(name: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for raw in (SYSTEMD / name).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("["):
            continue
        key, _, value = line.partition("=")
        pairs.append((key, value))
    return pairs


def _seconds(value: str) -> int:
    match = re.fullmatch(r"(\d+)(min|s)", value)
    assert match, value
    return int(match.group(1)) * (60 if match.group(2) == "min" else 1)


def test_the_service_template_is_a_bounded_unprivileged_oneshot() -> None:
    text = (SYSTEMD / "grid-options-dedicated.service.template").read_text(encoding="utf-8")
    assert [s for s in re.findall(r"^\[(\w+)\]$", text, re.M)] == ["Unit", "Service"]
    unit = dict(_unit("grid-options-dedicated.service.template"))
    assert unit["Type"] == "oneshot" and unit["User"] == "grid"
    assert unit["WorkingDirectory"] == "/data/grid_v4/grid_release"
    assert unit["EnvironmentFile"] == "/home/grid/grid_v4/grid_repo/.env"
    assert unit["ExecStart"] == ("/data/grid_v4/grid_venv-314/bin/python "
                                 "/data/grid_v4/grid_release/scripts/run_options_dedicated.py")
    # RuntimeMaxSec has no effect on Type=oneshot; TimeoutStartSec bounds the whole run.
    assert "RuntimeMaxSec" not in unit
    assert _seconds(unit["TimeoutStartSec"]) >= rod.MAX_BUDGET_SECONDS + 5 * 60 > rod.DEFAULT_BUDGET_SECONDS
    assert unit["NoNewPrivileges"] == "yes" and unit["KillMode"] == "control-group"
    # No root actions: no privileged prefixes, no extra commands, no root user or group.
    keys = [k for k, _v in _unit("grid-options-dedicated.service.template")]
    assert not {"ExecStartPre", "ExecStartPost", "ExecStopPost", "ExecStop", "PermissionsStartOnly",
                "Group", "SupplementaryGroups", "AmbientCapabilities"} & set(keys)
    assert not unit["ExecStart"].startswith(("+", "!", "-", "@", ":"))
    assert "# deploy-sync: managed" in text


def test_the_timer_runs_the_schedulers_afternoon_slots_without_catch_up() -> None:
    pairs = _unit("grid-options-dedicated.timer.template")
    calendars = [v for k, v in pairs if k == "OnCalendar"]
    assert calendars == ["Mon..Fri *-*-* 16:10:00 UTC", "Mon..Fri *-*-* 20:10:00 UTC"]
    # Each is one of the daily scheduler's in-session UTC slots, 10 minutes later; never 22:00Z.
    slots = {f"{int(s[:2]):02d}:{int(s[3:]) + 10:02d}" for s in scheduler.DOMESTIC_UTC_SLOTS if s != "22:00"}
    assert {c.split()[2][:5] for c in calendars} == slots
    timer = dict(pairs)
    assert timer["Persistent"] == "false" and timer["Unit"] == "grid-options-dedicated.service"
    assert timer["WantedBy"] == "timers.target"


@pytest.mark.skipif(shutil.which("systemd-analyze") is None, reason="systemd-analyze not installed")
def test_the_timer_calendars_parse() -> None:
    for spec in ("Mon..Fri *-*-* 16:10:00 UTC", "Mon..Fri *-*-* 20:10:00 UTC"):
        done = subprocess.run(["systemd-analyze", "calendar", spec], capture_output=True, text=True, timeout=30)
        assert done.returncode == 0, done.stderr
        assert f"Normalized form: {spec}" in done.stdout


# ── the automatic-writer policy recognises the dedicated writer ──────────


class _ObservedEngine:
    """The guard's read-only observation: counts connections, answers (0, 0)."""

    def __init__(self) -> None:
        self.connections = 0

    def connect(self) -> _ObservedEngine:
        self.connections += 1
        return self

    def __enter__(self) -> _ObservedEngine:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, *_args: Any) -> _ObservedEngine:
        return self

    def fetchone(self) -> tuple[int, int]:
        return (0, 0)


def test_policy_unset_lets_the_dedicated_writer_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", raising=False)
    for flag in (None, "1"):
        _schwab_env(monkeypatch, flag=flag)
        engine = _ObservedEngine()
        assert options.automatic_options_capture_guard(engine, writer="options_dedicated") is None
        assert engine.connections == 0


def test_daily_scheduler_policy_accepts_the_dedicated_writer_only_with_the_hand_off(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", "daily_scheduler")
    _schwab_env(monkeypatch)
    engine = _ObservedEngine()
    assert options.automatic_options_capture_guard(engine, writer="options_dedicated") is None
    assert engine.connections == 0
    # Every other writer is still refused while the hand-off is active.
    for writer in (None, "smart_scheduler", "oracle_forward", "options_puller"):
        refused = options.automatic_options_capture_guard(_ObservedEngine(), writer=writer)
        assert refused is not None and refused["reason"] == "daily_scheduler_owns_options"


@pytest.mark.parametrize("change", [
    {"GRID_OPTIONS_DEDICATED_TIMER": None}, {"GRID_OPTIONS_DEDICATED_TIMER": "0"},
    {"GRID_OPTIONS_PRIMARY_PROVIDER": "cboe"}, {"GRID_OPTIONS_SCHWAB_READERS": None},
], ids=lambda c: ",".join(f"{k.removeprefix('GRID_OPTIONS_')}={v}" for k, v in c.items()))
def test_daily_scheduler_policy_refuses_the_dedicated_writer_without_the_hand_off(
        monkeypatch: pytest.MonkeyPatch, change: dict) -> None:
    monkeypatch.setenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", "daily_scheduler")
    _schwab_env(monkeypatch)
    for key, value in change.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)
    engine = _ObservedEngine()
    refused = options.automatic_options_capture_guard(engine, writer="options_dedicated")
    assert refused is not None and refused["status"] == "SKIPPED"
    assert refused["reason"] == "daily_scheduler_owns_options" and engine.connections == 1


def test_an_invalid_policy_refuses_the_dedicated_writer_too(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", "bogus")
    _schwab_env(monkeypatch)
    refused = options.automatic_options_capture_guard(_ObservedEngine(), writer="options_dedicated")
    assert refused is not None and refused["reason"] == "invalid_options_writer_policy"


def test_the_runner_runs_under_the_daily_scheduler_policy(monkeypatch: pytest.MonkeyPatch,
                                                          capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", "daily_scheduler")
    _schwab_env(monkeypatch)
    env = _setup(monkeypatch, _body())
    rc = rod.main([], engine=_ObservedEngine())
    out = json.loads(capsys.readouterr().out.splitlines()[-1].removeprefix("OPTIONS_DEDICATED "))
    assert rc == 0 and out["status"] == "SUCCESS" and env.writers == ["options_dedicated"]


def test_the_runner_obeys_a_refusing_policy(monkeypatch: pytest.MonkeyPatch,
                                            capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", "bogus")
    _schwab_env(monkeypatch)
    monkeypatch.setattr(options, "OptionsPuller", lambda **_kw: pytest.fail("the policy refused"))
    rc, out = _main(capsys)
    assert (rc, out["status"], out["reason"]) == (0, "REFUSED", "writer policy: invalid_options_writer_policy")
