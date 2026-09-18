"""Hardening tests for ``ingestion/altdata/lme_warehouse.py`` (GRID W5 slice 1).

This is a companion to the existing ``tests/test_lme_warehouse.py`` (which
already covers the parser and a MagicMock-based happy/fallback/failure
matrix in depth) -- it does not re-derive that coverage. It adds:

* A pure-Python fake store (``tests/fixtures/sources/fake_store.py``)
  instead of ad hoc ``MagicMock`` engine wiring, per this workstream's
  "fake store object that records writes" requirement.
* Status/timestamp/idempotency/revision checks framed the same way as
  ``tests/test_source_eia.py``, so the two sources are directly comparable
  in the handoff report.
* A regression test proving + fixing the silent "malformed date falls back
  to today() with no trace" defect in ``_parse_date_from_header``.
* A source-truth test that pins down where (and whether) this puller is
  actually registered with a scheduler -- see ``TestSchedulingTruth``.

No real network, no real database, no live LME endpoint is ever called.
Fixtures under ``tests/fixtures/sources/lme/`` are CONSTRUCTED from this
module's own documented parse contract:

* The JSON shape follows the docstring at
  ``ingestion/altdata/lme_warehouse.py:405-424`` (``_parse_lme_json``),
  which the module itself labels a *probe* -- "the exact URL is subject to
  change and has not been confirmed by a network capture"
  (``ingestion/altdata/lme_warehouse.py:22-24``). No live LME warehouse
  stock figures are asserted as real; the tonnages are synthetic.
* The HTML shape follows the column-keyword scheme documented at
  ``ingestion/altdata/lme_warehouse.py:113-126`` (``_TOTAL_COLUMN_KEYWORDS``
  / ``_CANCELLED_COLUMN_KEYWORDS``) and mirrors the table already used by
  ``tests/test_lme_warehouse.py``'s own canned HTML.
"""

from __future__ import annotations

import importlib.util
import inspect
import json
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.lme_warehouse import (
    LMEWarehousePuller,
    _parse_date_from_header,
    run_lme_warehouse_puller,
)

_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "sources"


def _load_module(name: str, rel_path: str):
    spec = importlib.util.spec_from_file_location(name, _FIXTURES_DIR / rel_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


_fake_store = _load_module("fake_store_for_lme", "fake_store.py")
FakeEngine = _fake_store.FakeEngine

_LME_FIXTURES = _FIXTURES_DIR / "lme"


def _load_json(name: str) -> dict:
    return json.loads((_LME_FIXTURES / name).read_text(encoding="utf-8"))


def _load_html(name: str) -> str:
    return (_LME_FIXTURES / name).read_text(encoding="utf-8")


def _resp_json(body: dict):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json = MagicMock(return_value=body)
    return resp


def _resp_html(html_text: str):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.text = html_text
    return resp


@pytest.fixture
def engine() -> FakeEngine:
    return FakeEngine()


def _rows_for_metal(engine: FakeEngine, metal: str, kind: str = "stocks_total_mt"):
    sid = f"lme:{kind}:{metal}"
    return engine.store.rows_for(sid)


# ---------------------------------------------------------------------------
# 1. Good response (JSON probe path) -> 6 metals x 4 series, units correct
# ---------------------------------------------------------------------------


class TestGoodResponseJson:
    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_writes_four_series_per_metal(self, mock_get, engine):
        good = _load_json("good_response.json")
        mock_get.return_value = _resp_json(good)

        result = run_lme_warehouse_puller(engine)

        assert result["source"] == "json"
        assert result["fetched"] == 6
        assert result["inserted"] == 24  # 6 metals x 4 derived series
        copper_total = _rows_for_metal(engine, "copper", "stocks_total_mt")
        assert len(copper_total) == 1
        assert copper_total[0]["value"] == 120000.0
        assert copper_total[0]["obs_date"] == date(2026, 9, 16)
        assert copper_total[0]["pull_status"] == "SUCCESS"
        copper_ratio = _rows_for_metal(engine, "copper", "cancelled_ratio")
        assert copper_ratio[0]["value"] == pytest.approx(0.25)

    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_no_distinct_release_date_only_obs_date(self, mock_get, engine):
        """Same gap as EIA: LME's save_to_db (lme_warehouse.py:629-643)
        inserts series_id/source_id/obs_date/value/raw_payload/pull_status
        only -- there is no separate release/publish-date column. The
        report date IS the observation date here (LME publishes same-day),
        so this is a smaller gap than EIA's, but it is still not a
        *distinct* release_date field.
        """
        good = _load_json("good_response.json")
        mock_get.return_value = _resp_json(good)
        run_lme_warehouse_puller(engine)
        row = _rows_for_metal(engine, "copper")[0]
        assert set(row.keys()) == {
            "series_id", "source_id", "obs_date", "value",
            "raw_payload", "pull_status", "pull_timestamp",
        }


# ---------------------------------------------------------------------------
# 2. Good response (HTML fallback path)
# ---------------------------------------------------------------------------


class TestGoodResponseHtmlFallback:
    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_json_probe_empty_falls_back_to_html(self, mock_get, engine):
        empty_json = _load_json("empty_response.json")
        html = _load_html("good_response.html")
        mock_get.side_effect = [_resp_json(empty_json), _resp_html(html)]

        result = run_lme_warehouse_puller(engine)

        assert result["source"] == "html"
        assert result["fetched"] == 6
        assert result["inserted"] == 24


# ---------------------------------------------------------------------------
# 3. Empty (both paths empty) -> zero rows, no fabricated SUCCESS row
# ---------------------------------------------------------------------------


class TestEmptyResponse:
    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_both_paths_empty_writes_nothing(self, mock_get, engine):
        empty_json = _load_json("empty_response.json")
        mock_get.side_effect = [
            _resp_json(empty_json),
            _resp_html("<html><body>no table here</body></html>"),
        ]

        result = run_lme_warehouse_puller(engine)

        assert result["fetched"] == 0
        assert result["inserted"] == 0
        assert result["metals"] == {}
        assert result["source"] == "none"
        assert engine.store.rows == []


# ---------------------------------------------------------------------------
# 4. Malformed response -> bounded, partial data survives, no crash
# ---------------------------------------------------------------------------


class TestMalformedResponse:
    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_bad_entries_skipped_good_entry_survives(self, mock_get, engine):
        malformed = _load_json("malformed_response.json")
        mock_get.return_value = _resp_json(malformed)

        result = run_lme_warehouse_puller(engine)

        # Copper (non-numeric "N/A" + null) and "Unobtainium" (unknown
        # metal) are both skipped; only zinc survives.
        assert result["fetched"] == 1
        assert set(result["metals"].keys()) == {"zinc"}
        assert engine.store.rows_for("lme:stocks_total_mt:zinc")[0]["value"] == 250000.0
        assert engine.store.rows_for("lme:stocks_total_mt:copper") == []

    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_malformed_report_date_logs_warning_not_silent(self, mock_get, engine):
        """Defect proved + fixed in this commit.

        Before the fix, `_parse_date_from_header` silently substituted
        `fallback` (`date.today()`) for any non-empty, unparseable date
        string with no log line at all -- a malformed `report_date` (as in
        this fixture, "not-a-real-date") would silently mis-date every row
        to today with zero trace in `.server-logs/errors.jsonl`. That is a
        PIT-correctness hazard per this repo's own data-integrity rules.
        The fix adds a `log.warning` on that fallback path.
        """
        malformed = _load_json("malformed_response.json")
        mock_get.return_value = _resp_json(malformed)

        with patch("ingestion.altdata.lme_warehouse.log") as mock_log:
            run_lme_warehouse_puller(engine)

        assert mock_log.warning.call_count >= 1
        all_warn_text = " ".join(
            str(call) for call in mock_log.warning.call_args_list
        )
        assert "could not parse observation date" in all_warn_text

    def test_parse_date_from_header_fallback_direct(self):
        fallback = date(2020, 1, 1)
        with patch("ingestion.altdata.lme_warehouse.log") as mock_log:
            result = _parse_date_from_header("totally not a date", fallback)
        assert result == fallback
        mock_log.warning.assert_called_once()

    def test_parse_date_from_header_empty_text_is_silent(self):
        """The "no text at all" case is a different, expected situation
        (nothing to parse) and intentionally stays silent -- only a
        non-empty-but-unparseable string should warn."""
        fallback = date(2020, 1, 1)
        with patch("ingestion.altdata.lme_warehouse.log") as mock_log:
            result = _parse_date_from_header("", fallback)
        assert result == fallback
        mock_log.warning.assert_not_called()


# ---------------------------------------------------------------------------
# 5. Revised value, same obs_date -- dedup-window contract
# ---------------------------------------------------------------------------


class TestRevisedValue:
    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_revised_value_within_dedup_window_is_not_overwritten(
        self, mock_get, engine
    ):
        """SOURCE_CONFIG['revision_behavior'] == 'NEVER'
        (ingestion/altdata/lme_warehouse.py:528). save_to_db's dedup check
        (`_row_exists(..., dedup_hours=24*7)`, lme_warehouse.py:619) treats
        any row already written for this series_id+obs_date within the
        last 7 days as authoritative and skips the new value -- so a same-
        day "revised" copper total does not overwrite the first write.
        """
        good = _load_json("good_response.json")
        mock_get.return_value = _resp_json(good)
        run_lme_warehouse_puller(engine)
        original = engine.store.rows_for("lme:stocks_total_mt:copper")[0]["value"]
        assert original == 120000.0

        revised = _load_json("revised_response.json")
        mock_get.return_value = _resp_json(revised)
        result = run_lme_warehouse_puller(engine)

        rows = engine.store.rows_for("lme:stocks_total_mt:copper")
        assert len(rows) == 1  # still just the one row
        assert rows[0]["value"] == 120000.0  # not the revised 125000.0
        assert result["inserted"] == 0


# ---------------------------------------------------------------------------
# 6. Idempotent double-run (identical response) -- no duplicates
# ---------------------------------------------------------------------------


class TestIdempotentRerun:
    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_running_twice_does_not_duplicate(self, mock_get, engine):
        good = _load_json("good_response.json")
        mock_get.return_value = _resp_json(good)

        run_lme_warehouse_puller(engine)
        second = run_lme_warehouse_puller(engine)

        assert second["inserted"] == 0
        assert len(engine.store.rows) == 24  # still just the first run's rows


# ---------------------------------------------------------------------------
# 7. Dry-run mode: verified absent
# ---------------------------------------------------------------------------


class TestDryRunMode:
    def test_no_dry_run_parameter_anywhere_in_the_call_path(self):
        for fn in (
            LMEWarehousePuller.__init__,
            LMEWarehousePuller.pull,
            LMEWarehousePuller.save_to_db,
            run_lme_warehouse_puller,
        ):
            params = inspect.signature(fn).parameters
            assert "dry_run" not in params, f"{fn.__qualname__} unexpectedly has dry_run"


# ---------------------------------------------------------------------------
# 8. Scheduling truth: is this puller "registered in the scheduler" or not?
# ---------------------------------------------------------------------------


class TestSchedulingTruth:
    """Resolves the apparently contradictory claims in the W5 brief:

    (a) "lme_warehouse.py ... is not registered in the scheduler", and
    (b) a TODO that the "09:00 UTC LME job has never written a row".

    Both are about *a* scheduler, but not the same one. This project has
    two: `ingestion/scheduler.py` (CLAUDE.md calls it "the authoritative
    scheduler" for data-pulling; gotcha #39) and `intelligence/scheduler.py`
    (a separate background loop for briefings/research/etc., run by its own
    systemd unit). Source says:

    * `ingestion/scheduler.py` never mentions LME at all -- claim (a) is
      TRUE with respect to the authoritative *ingestion* scheduler.
    * `intelligence/scheduler.py` DOES register it: `_lme_warehouse_daily`
      is defined and wired with
      `_sched.every().day.at("09:00").do(_lme_warehouse_daily)` at
      intelligence/scheduler.py:892 (job body: lines 842-855). That
      function only runs inside `run_intelligence_loop()`
      (intelligence/scheduler.py:20), which only executes under
      `if __name__ == "__main__":` (line 1197-1198) -- i.e. when the
      process is launched as `python3 -m intelligence.scheduler`, which is
      exactly `server_setup/grid-intelligence.service`'s ExecStart. So
      claim (a) is FALSE in the absolute sense: the puller IS registered
      and wired to run daily -- just on a different scheduler/service than
      the one usually meant by "the scheduler" in this codebase.

    Why the TODO's "never written a row" could still be true despite that
    wiring being correct is NOT resolved by this test -- it would require
    either a live network capture of the real (possibly JS-rendered)
    warehouse-stocks-report page, or DB access to `raw_series`, both out
    of bounds for this workstream. See the handoff report for that gap.
    """

    _REPO_ROOT = Path(__file__).resolve().parents[1]

    def test_lme_absent_from_ingestion_present_in_intelligence_scheduler(self):
        """Single pinned assertion of both halves of the scheduling truth,
        read directly from source text (no DB, no network):

        1. `ingestion/scheduler.py` -- the authoritative ingestion scheduler
           per CLAUDE.md gotcha #39 -- never mentions "lme" anywhere.
        2. `intelligence/scheduler.py` DOES register it: the job function
           `_lme_warehouse_daily`, the daily 09:00 registration line, and
           the `if __name__ == "__main__": run_intelligence_loop()` gate
           that determines when (if ever) that registration takes effect
           are all present.

        The per-fact detail tests below (`test_ingestion_scheduler_never_
        mentions_lme`, `test_intelligence_scheduler_registers_lme_daily_
        job`, `test_intelligence_loop_only_runs_as_main_entrypoint`) are
        kept for granular failure messages; this test exists so the two
        halves of the claim are visibly asserted together in one place.
        """
        ingestion_src = (self._REPO_ROOT / "ingestion/scheduler.py").read_text(
            encoding="utf-8"
        )
        intelligence_src = (self._REPO_ROOT / "intelligence/scheduler.py").read_text(
            encoding="utf-8"
        )

        # (1) absent from the authoritative ingestion scheduler
        assert "lme" not in ingestion_src.lower()

        # (2) present in intelligence/scheduler.py: job body, 09:00
        # registration, and the __main__ gate that governs whether the
        # registration ever actually runs.
        assert "_lme_warehouse_daily" in intelligence_src
        assert (
            '_sched.every().day.at("09:00").do(_lme_warehouse_daily)'
            in intelligence_src
        )
        assert 'if __name__ == "__main__":' in intelligence_src
        assert "run_intelligence_loop()" in intelligence_src

    def test_ingestion_scheduler_never_mentions_lme(self):
        src = (self._REPO_ROOT / "ingestion/scheduler.py").read_text(encoding="utf-8")
        assert "lme" not in src.lower()

    def test_intelligence_scheduler_registers_lme_daily_job(self):
        src = (self._REPO_ROOT / "intelligence/scheduler.py").read_text(encoding="utf-8")
        assert "_lme_warehouse_daily" in src
        assert '_sched.every().day.at("09:00").do(_lme_warehouse_daily)' in src
        assert "from ingestion.altdata.lme_warehouse import run_lme_warehouse_puller" in src

    def test_intelligence_loop_only_runs_as_main_entrypoint(self):
        src = (self._REPO_ROOT / "intelligence/scheduler.py").read_text(encoding="utf-8")
        assert 'if __name__ == "__main__":' in src
        assert "run_intelligence_loop()" in src

    def test_grid_intelligence_service_runs_that_module(self):
        unit = (self._REPO_ROOT / "server_setup/grid-intelligence.service").read_text(
            encoding="utf-8"
        )
        assert "python3 -m intelligence.scheduler" in unit
