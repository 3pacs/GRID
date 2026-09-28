"""Tests for scripts/backfill_adr_market_caps.py.

This script was accidentally invoked twice in prod (2026-09-28) because it
had no argparse: any invocation -- including `--help` -- ran the real
UPDATE against ticker_metrics_daily. These tests cover, offline (no live
DB, no network):

  * --help / argument parsing never reaches the database.
  * The default (no --execute) is a read-only dry-run.
  * --execute actually writes and records a run-ledger.
  * A second --execute is refused once a ledger shows a completed run,
    unless --i-know-this-has-not-run is also passed.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.backfill_adr_market_caps import (
    build_parser,
    load_ledger,
    main,
    ratios_to_apply,
    run,
)


# ── ratios_to_apply ──────────────────────────────────────────────────────


def test_ratios_to_apply_excludes_1to1_tickers():
    ratios = ratios_to_apply()
    assert ratios["TSM"] == 5.0
    assert ratios["BABA"] == 8.0
    assert ratios["AZN"] == 0.5
    # 1:1 listings (NVO, UL, SAN, ...) must not appear -- nothing to divide.
    assert "NVO" not in ratios
    assert "UL" not in ratios


# ── --help / argparse never touches the DB ──────────────────────────────


def test_help_exits_cleanly_without_importing_db():
    parser = build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["--help"])
    assert exc.value.code == 0


def test_main_help_never_calls_get_engine():
    with patch("scripts.backfill_adr_market_caps._get_engine") as get_engine:
        with pytest.raises(SystemExit):
            main(["--help"])
        get_engine.assert_not_called()


def test_main_no_args_defaults_to_dry_run_and_returns_zero(tmp_path):
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = 3
    engine.connect.return_value.__enter__.return_value = conn

    ledger_path = tmp_path / "ledger.json"
    with patch("scripts.backfill_adr_market_caps._get_engine", return_value=engine):
        code = main(["--ledger-path", str(ledger_path)])

    assert code == 0
    engine.begin.assert_not_called()
    assert not ledger_path.exists()  # dry-run never writes the ledger either


# ── run(): dry-run is read-only ──────────────────────────────────────────


def test_dry_run_only_counts_never_writes(tmp_path):
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = 7
    engine.connect.return_value.__enter__.return_value = conn

    ledger_path = tmp_path / "ledger.json"
    with patch("scripts.backfill_adr_market_caps._get_engine", return_value=engine):
        summary = run(execute=False, ledger_path=ledger_path)

    assert summary["executed"] is False
    assert summary["blocked_reason"] is None
    assert summary["total_rows"] == 7 * len(ratios_to_apply())
    engine.begin.assert_not_called()
    assert not ledger_path.exists()


# ── run(): --execute writes and ledgers ──────────────────────────────────


def _engine_with_rowcount(n: int) -> MagicMock:
    engine = MagicMock()
    begin_conn = MagicMock()
    begin_conn.execute.return_value.rowcount = n
    engine.begin.return_value.__enter__.return_value = begin_conn
    return engine


def test_execute_first_run_writes_and_creates_ledger(tmp_path):
    engine = _engine_with_rowcount(4)
    ledger_path = tmp_path / "state" / "ledger.json"  # nested dir must be created

    with patch("scripts.backfill_adr_market_caps._get_engine", return_value=engine):
        summary = run(execute=True, ledger_path=ledger_path)

    assert summary["executed"] is True
    assert summary["blocked_reason"] is None
    n_ratios = len(ratios_to_apply())
    assert summary["total_rows"] == 4 * n_ratios
    # One UPDATE per non-1:1 ticker, one begin() transaction covering all of them.
    assert engine.begin.return_value.__enter__.return_value.execute.call_count == n_ratios
    assert engine.begin.call_count == 1

    assert ledger_path.exists()
    ledger = json.loads(ledger_path.read_text())
    assert ledger["last"]["total_rows"] == 4 * n_ratios
    assert ledger["last"]["override"] is False
    assert len(ledger["history"]) == 1


def test_second_execute_without_override_is_blocked_and_never_touches_db(tmp_path):
    ledger_path = tmp_path / "ledger.json"
    engine1 = _engine_with_rowcount(2)
    with patch("scripts.backfill_adr_market_caps._get_engine", return_value=engine1):
        first = run(execute=True, ledger_path=ledger_path)
    assert first["executed"] is True

    with patch("scripts.backfill_adr_market_caps._get_engine") as get_engine2:
        second = run(execute=True, ledger_path=ledger_path)
        # Blocked before ever asking for a DB engine -- no risk of a stray write.
        get_engine2.assert_not_called()

    assert second["executed"] is False
    assert second["blocked_reason"] is not None
    assert "ledger" in second["blocked_reason"].lower()
    assert second["total_rows"] == 0


def test_second_execute_with_override_appends_ledger_history(tmp_path):
    ledger_path = tmp_path / "ledger.json"
    engine1 = _engine_with_rowcount(2)
    with patch("scripts.backfill_adr_market_caps._get_engine", return_value=engine1):
        run(execute=True, ledger_path=ledger_path)

    engine2 = _engine_with_rowcount(1)
    with patch("scripts.backfill_adr_market_caps._get_engine", return_value=engine2):
        second = run(execute=True, ledger_path=ledger_path, i_know_this_has_not_run=True)

    assert second["executed"] is True
    ledger = json.loads(ledger_path.read_text())
    assert len(ledger["history"]) == 2
    assert ledger["last"]["override"] is True


def test_dry_run_is_unaffected_by_an_existing_ledger(tmp_path):
    ledger_path = tmp_path / "ledger.json"
    engine1 = _engine_with_rowcount(2)
    with patch("scripts.backfill_adr_market_caps._get_engine", return_value=engine1):
        run(execute=True, ledger_path=ledger_path)

    engine2 = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = 0
    engine2.connect.return_value.__enter__.return_value = conn
    with patch("scripts.backfill_adr_market_caps._get_engine", return_value=engine2):
        # Dry-run must still work (and stay read-only) even though a
        # completed-run ledger exists -- only --execute is gated.
        summary = run(execute=False, ledger_path=ledger_path)

    assert summary["executed"] is False
    assert summary["blocked_reason"] is None
    engine2.begin.assert_not_called()


def test_main_execute_blocked_by_ledger_returns_exit_code_1(tmp_path):
    ledger_path = tmp_path / "ledger.json"
    engine1 = _engine_with_rowcount(1)
    with patch("scripts.backfill_adr_market_caps._get_engine", return_value=engine1):
        code1 = main(["--execute", "--ledger-path", str(ledger_path)])
    assert code1 == 0

    with patch("scripts.backfill_adr_market_caps._get_engine") as get_engine2:
        code2 = main(["--execute", "--ledger-path", str(ledger_path)])
        get_engine2.assert_not_called()
    assert code2 == 1


# ── load_ledger ───────────────────────────────────────────────────────────


def test_load_ledger_missing_file_returns_none(tmp_path):
    assert load_ledger(tmp_path / "nope.json") is None


def test_load_ledger_corrupt_file_returns_none_not_raise(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{not valid json")
    assert load_ledger(p) is None
