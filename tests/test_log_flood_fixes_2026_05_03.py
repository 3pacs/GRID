"""
Regression tests for the 2026-05-03 errors.jsonl triage pass.

Each test pins a single behaviour change so a future refactor doesn't
silently re-open one of the patched log floods. Every fix targets a
concrete pattern observed in `.server-logs/errors.jsonl` between
2026-04-29 and 2026-05-03 and survives the next incident triage.

Run:
    python -m pytest tests/test_log_flood_fixes_2026_05_03.py -v
"""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fix #1 — server_log.git_sink: pass user.name/email to every git call
# ---------------------------------------------------------------------------


class TestGitSinkAuthorIdentity:
    """Without -c user.name/email, every commit fails on hosts that have
    no global git identity (the production grid-svr account). That
    cascade was responsible for ~70 ERROR rows in the May-2026 window.
    """

    @patch("server_log.git_sink.subprocess.run")
    def test_git_call_includes_identity(self, mock_run, tmp_path):
        from server_log.git_sink import _git

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        (tmp_path / ".git").mkdir()
        _git(["commit", "-m", "x"], tmp_path)

        argv = mock_run.call_args[0][0]
        # -c user.name=...  -c user.email=...  must appear before the
        # subcommand so they apply to the commit.
        sub_idx = argv.index("commit")
        prelude = argv[1:sub_idx]
        assert "-c" in prelude
        assert any(a.startswith("user.name=") for a in prelude)
        assert any(a.startswith("user.email=") for a in prelude)


# ---------------------------------------------------------------------------
# Fix #5 — ingestion.tiingo_pull: detect IndexCorrupted, trip a breaker
# ---------------------------------------------------------------------------


class TestTiingoIndexCorruptedBreaker:
    """When uq_raw_series_composite is corrupted, every ticker in the
    cycle fails with the same IndexCorrupted error. A single ERROR row
    plus a process-scoped breaker is enough — operators only need to
    see "REINDEX required" once, not 30+ times.
    """

    def test_breaker_module_export(self):
        # The breaker variable must exist at the module level so
        # pull_all() can short-circuit subsequent tickers.
        import importlib
        mod = importlib.import_module("ingestion.tiingo_pull")
        assert hasattr(mod, "_INDEX_CORRUPTED_BREAKER")
        assert mod._INDEX_CORRUPTED_BREAKER in (False, True)

    def test_reindex_runbook_exists(self):
        # The error message points operators at this script — it must
        # be present and self-contained so the runbook works.
        from pathlib import Path
        runbook = (
            Path(__file__).resolve().parent.parent
            / "scripts"
            / "migrations"
            / "reindex_raw_series.sql"
        )
        assert runbook.exists()
        body = runbook.read_text()
        assert "REINDEX INDEX CONCURRENTLY uq_raw_series_composite" in body


# ---------------------------------------------------------------------------
# Fix #6 — ingestion.edgar.pull_8k_counts: trip a breaker on 403
# ---------------------------------------------------------------------------


class TestEdgar8KBreaker:
    """SEC's full-index 403s when the current quarter's index isn't yet
    generated. Without a cooldown the puller hits it twice/day.
    """

    def test_breaker_attrs_exist(self):
        EDGARPuller = pytest.importorskip(
            "ingestion.edgar"
        ).EDGARPuller  # requires the optional `edgartools` package
        # The breaker is a class attribute so all instances share it
        # for the lifetime of the process.
        assert hasattr(EDGARPuller, "_PULL_8K_BREAKER")
        assert hasattr(EDGARPuller, "_PULL_8K_BREAKER_TS")
        assert hasattr(EDGARPuller, "_PULL_8K_BREAKER_TTL_S")
        assert EDGARPuller._PULL_8K_BREAKER_TTL_S >= 3600  # at least 1h


# ---------------------------------------------------------------------------
# Fix #7 — ingestion.international.mas: cycle-scoped cooldown
# ---------------------------------------------------------------------------


class TestMASCooldown:
    """All four MAS resources share one upstream API — when it's sick,
    the second/third/fourth calls don't add information.
    """

    def test_cooldown_globals_exist(self):
        from ingestion.international import mas
        assert hasattr(mas, "_API_COOLDOWN_UNTIL")
        assert hasattr(mas, "_API_COOLDOWN_SECONDS")
        # Must be at least an hour so we don't immediately retry.
        assert mas._API_COOLDOWN_SECONDS >= 3600


# ---------------------------------------------------------------------------
# Fix #3 — supply_chain.Freightos: parse JSON outside RequestException
# ---------------------------------------------------------------------------


class TestFreightosJSONParse:
    """Parsing was inside the RequestException handler, so JSONDecodeError
    (subclass of RequestException in modern requests) re-raised and burned
    3 retries before logging an ERROR.
    """

    def test_json_decode_returns_empty_not_raises(self):
        # Verify the decode path returns [] rather than raising — without
        # importing the full grid dependency stack we exercise the local
        # method via a mocked instance.
        from unittest.mock import MagicMock
        from ingestion.altdata.supply_chain import SupplyChainPuller

        instance = SupplyChainPuller.__new__(SupplyChainPuller)
        # Patch requests.get to return a non-JSON body.
        non_json_resp = MagicMock()
        non_json_resp.status_code = 200
        non_json_resp.raise_for_status = MagicMock()
        non_json_resp.json.side_effect = json.JSONDecodeError("Expecting value", "<html>", 0)

        with patch("ingestion.altdata.supply_chain.requests.get", return_value=non_json_resp):
            out = SupplyChainPuller._fetch_freightos_current.__wrapped__(instance)

        assert out == []
