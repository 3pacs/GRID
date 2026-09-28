"""Tests for scripts/backfill_adr_market_caps.py.

The script is RETIRED (2026-09-28): the one-off ADR ratio correction it
existed to apply already ran successfully on 2026-04-12 (TSM, BHP), and
the sec_xbrl_shares puller now keeps every new row correct on its own --
there is nothing left for this script to do.

It was also run twice in prod today because it had no argparse: any
invocation, including --help, ran the real UPDATE unconditionally. These
tests confirm the retired script still parses arguments (so --help and
--execute don't error) but performs zero database work under any
invocation, including --execute.
"""

from __future__ import annotations

import inspect
import re

import pytest

from scripts import backfill_adr_market_caps as mod
from scripts.backfill_adr_market_caps import build_parser, main


# ── regression guard: stay fully decoupled from the database ────────────


def test_module_source_never_touches_db_access():
    """The retired script must not import db, get_engine, or get_connection
    -- if a future edit reintroduces a DB path, this test should catch it
    before argparse/--help could ever reach it again."""
    src = inspect.getsource(mod)
    assert not re.search(r"^\s*(from db import|import db\b)", src, re.M)
    assert not re.search(r"^\s*(from ingestion|import ingestion)", src, re.M)
    assert "get_engine" not in src
    assert "get_connection" not in src
    assert "sqlalchemy" not in src.lower()


# ── --help keeps working and touches nothing ─────────────────────────────


def test_help_exits_zero():
    parser = build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["--help"])
    assert exc.value.code == 0


def test_main_help_exits_zero_without_error():
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0


# ── every real invocation refuses and exits non-zero ─────────────────────


def test_main_no_args_refuses_and_exits_nonzero(capsys):
    code = main([])
    assert code != 0
    combined = "".join(capsys.readouterr())
    assert "2026-04-12" in combined
    assert "TSM" in combined
    assert "BHP" in combined
    assert "RETIRED" in combined


def test_main_execute_also_refuses_and_exits_nonzero(capsys):
    code = main(["--execute"])
    assert code != 0
    combined = "".join(capsys.readouterr())
    assert "--execute" in combined


def test_main_message_cites_todays_incident(capsys):
    main([])
    combined = "".join(capsys.readouterr())
    assert "2026-09-28" in combined or "twice in prod" in combined


def test_main_unknown_flag_errors_via_argparse_not_via_db():
    with pytest.raises(SystemExit) as exc:
        main(["--bogus-flag-that-does-not-exist"])
    assert exc.value.code != 0


def test_main_returns_same_nonzero_code_regardless_of_execute_flag():
    assert main([]) == main(["--execute"])
