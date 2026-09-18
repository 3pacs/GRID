"""Hardening tests for ``ingestion/altdata/eia_puller.py`` (GRID W5 slice 1).

Scope and boundaries (see docs/handoffs/2026-09-18/ for the full report):

* No real network, no real database. HTTP is faked by patching
  ``requests.get`` with a fixture-backed response; the store is
  ``tests/fixtures/sources/fake_store.py``'s pure-Python
  ``FakeEngine`` / ``FakeRawSeriesStore`` -- never a live Postgres
  connection.
* Fixtures under ``tests/fixtures/sources/eia/`` are CONSTRUCTED, not a
  live capture. The envelope shape (``response.data[]`` with a ``period``
  and ``value`` field, plus a sibling ``*-units`` convention) follows
  EIA's own API v2 documentation at
  https://www.eia.gov/opendata/documentation.php, which states the API
  "will echo back what it understood to be our request" inside a
  ``response`` object, and shows examples where "data values as strings"
  are returned alongside a units field. The ``period``/``value`` field
  names themselves are drawn directly from what
  ``ingestion/altdata/eia_puller.py::EIAPuller._fetch_series`` /
  ``pull()`` already read (``rec.get("value")``, ``rec.get("period")``,
  ``resp.json().get("response", {}).get("data", [])``) -- i.e. ground
  truth from the code under test, not an invented shape. No live Brent/WTI
  price is asserted as real; the numeric values in the fixtures are
  synthetic.
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

from ingestion.altdata.eia_puller import EIAPuller, _redact_api_key

# ---------------------------------------------------------------------------
# Load the shared fake store/engine without relying on package-style imports
# (tests/fixtures/ has no __init__.py, and this workstream's edit allowlist
# does not include adding one outside tests/fixtures/sources/**).
# ---------------------------------------------------------------------------

_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "sources"


def _load_module(name: str, rel_path: str):
    spec = importlib.util.spec_from_file_location(name, _FIXTURES_DIR / rel_path)
    module = importlib.util.module_from_spec(spec)
    # dataclasses (3.13+) resolves annotations via sys.modules[cls.__module__],
    # so the module must be registered before exec_module runs its body.
    sys.modules[name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


_fake_store = _load_module("fake_store_for_eia", "fake_store.py")
FakeEngine = _fake_store.FakeEngine
FakeRawSeriesStore = _fake_store.FakeRawSeriesStore

_EIA_FIXTURES = _FIXTURES_DIR / "eia"


def _load_json(name: str) -> dict:
    return json.loads((_EIA_FIXTURES / name).read_text(encoding="utf-8"))


def _resp(json_body: dict | None = None, json_error: Exception | None = None):
    """Build a fake ``requests.Response`` for one ``requests.get()`` call."""
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    if json_error is not None:
        resp.json = MagicMock(side_effect=json_error)
    else:
        resp.json = MagicMock(return_value=json_body)
    return resp


@pytest.fixture
def engine() -> FakeEngine:
    return FakeEngine()


# ---------------------------------------------------------------------------
# 1. Missing API key -- graceful degradation, no DB access, no key leaked
# ---------------------------------------------------------------------------


class TestMissingApiKey:
    def test_returns_failed_without_touching_engine(self, engine, monkeypatch):
        monkeypatch.delenv("EIA_API_KEY", raising=False)
        puller = EIAPuller(db_engine=engine)
        result = puller.pull()

        assert result["status"] == "FAILED"
        assert result["rows_inserted"] == 0
        assert "EIA_API_KEY not set" in result["error"]
        # The only DB touch allowed is the constructor's source_id resolve;
        # pull() itself must short-circuit before ever opening a transaction.
        assert engine.store.rows == []


# ---------------------------------------------------------------------------
# 2. Good response -> expected rows, units/series preserved via raw_payload
# ---------------------------------------------------------------------------


class TestGoodResponse:
    @patch("ingestion.altdata.eia_puller.requests.get")
    def test_writes_expected_rows(self, mock_get, engine, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "test-key-not-real")
        good = _load_json("good_response.json")
        empty = _load_json("empty_response.json")
        # _SERIES_MAP order is RBRTE then RWTC.
        mock_get.side_effect = [_resp(good), _resp(empty)]

        puller = EIAPuller(db_engine=engine)
        result = puller.pull()

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 3
        rows = engine.store.rows_for("eia.brent_spot")
        assert len(rows) == 3
        assert {r["obs_date"] for r in rows} == {
            date(2026, 9, 14),
            date(2026, 9, 15),
            date(2026, 9, 16),
        }
        by_date = {r["obs_date"]: r for r in rows}
        assert by_date[date(2026, 9, 15)]["value"] == pytest.approx(69.10)
        assert all(r["pull_status"] == "SUCCESS" for r in rows)
        # raw_payload records which EIA facet produced the row.
        payload = json.loads(by_date[date(2026, 9, 14)]["raw_payload"])
        assert payload["facet"] == "RBRTE"

    @patch("ingestion.altdata.eia_puller.requests.get")
    def test_no_distinct_release_date_column(self, mock_get, engine, monkeypatch):
        """GAP: BasePuller._insert_raw (ingestion/base.py:583-624) has no
        release/publish-date column at all -- only series_id, source_id,
        obs_date, value, raw_payload, pull_status are ever inserted. EIA's
        `period` becomes `obs_date`; there is nothing that plays the role
        of a distinct `release_date`. This test pins that gap down instead
        of asserting a preservation behaviour that does not exist.
        """
        monkeypatch.setenv("EIA_API_KEY", "test-key-not-real")
        good = _load_json("good_response.json")
        empty = _load_json("empty_response.json")
        mock_get.side_effect = [_resp(good), _resp(empty)]

        EIAPuller(db_engine=engine).pull()

        row = engine.store.rows_for("eia.brent_spot")[0]
        assert set(row.keys()) == {
            "series_id",
            "source_id",
            "obs_date",
            "value",
            "raw_payload",
            "pull_status",
            "pull_timestamp",
        }


# ---------------------------------------------------------------------------
# 3. Empty response -> no rows, no fabricated value=0 SUCCESS row
# ---------------------------------------------------------------------------


class TestEmptyResponse:
    @patch("ingestion.altdata.eia_puller.requests.get")
    def test_empty_writes_nothing(self, mock_get, engine, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "test-key-not-real")
        empty = _load_json("empty_response.json")
        mock_get.side_effect = [_resp(empty), _resp(empty)]

        result = EIAPuller(db_engine=engine).pull()

        assert result["rows_inserted"] == 0
        assert engine.store.rows == []
        # NOTE (report this, do not silently rely on it): pull()'s overall
        # "status" is unconditionally SUCCESS whenever the API key is
        # present, even when every facet returned zero rows -- there is no
        # distinct "empty" status. What IS verified true: no row with
        # value=0 masquerading as a real observation is ever written.
        assert result["status"] == "SUCCESS"


# ---------------------------------------------------------------------------
# 4. Malformed response -> bounded, no credential leak, WARNING not ERROR
# ---------------------------------------------------------------------------


class TestMalformedResponse:
    @patch("ingestion.altdata.eia_puller.requests.get")
    def test_bad_records_are_skipped_not_crashed(self, mock_get, engine, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "test-key-not-real")
        malformed = _load_json("malformed_records.json")
        empty = _load_json("empty_response.json")
        mock_get.side_effect = [_resp(malformed), _resp(empty)]

        result = EIAPuller(db_engine=engine).pull()

        # Only the one genuinely well-formed record (period=2026-09-16,
        # value=68.95) should survive; None value, empty period, bad date,
        # and non-numeric value are all skipped without raising.
        assert result["rows_inserted"] == 1
        rows = engine.store.rows_for("eia.brent_spot")
        assert len(rows) == 1
        assert rows[0]["obs_date"] == date(2026, 9, 16)

    @patch("ingestion.altdata.eia_puller.requests.get")
    def test_non_json_body_logs_warning_and_redacts_api_key(
        self, mock_get, engine, monkeypatch
    ):
        """Defect proved + fixed in this commit.

        Before the fix: a non-JSON / HTTP-error response was logged with
        `log.error(...str(exc)...)`. `requests`' own exception text embeds
        the full request URL (`HTTPError.__str__` -> `response.url`), and
        that URL carries our live `api_key=...` query param straight into
        the log. It was also logged at ERROR, contradicting this project's
        own log-level convention (CLAUDE.md "Log levels": WARNING for
        upstream/transient failures, ERROR reserved for code bugs; see also
        ingestion/base.py::log_pull_failure, which this puller does not
        use). Both are fixed together: severity is WARNING, and the
        message is redacted through `_redact_api_key()`.
        """
        monkeypatch.setenv("EIA_API_KEY", "SECRET-REAL-KEY-999")
        leaking_exc = ValueError(
            "404 Client Error: Not Found for url: "
            "https://api.eia.gov/v2/petroleum/pri/spt/data/"
            "?api_key=SECRET-REAL-KEY-999&frequency=daily&facets%5Bseries%5D%5B%5D=RBRTE"
        )
        empty = _load_json("empty_response.json")
        mock_get.side_effect = [_resp(json_error=leaking_exc), _resp(empty)]

        with patch("ingestion.altdata.eia_puller.log") as mock_log:
            result = EIAPuller(db_engine=engine).pull()

        assert engine.store.rows == []  # no metadata / observation writes
        assert mock_log.error.call_count == 0
        assert mock_log.warning.call_count == 1
        logged_text = " ".join(
            str(a) for a in mock_log.warning.call_args.args
        ) + " ".join(
            f"{k}={v}" for k, v in mock_log.warning.call_args.kwargs.items()
        )
        assert "SECRET-REAL-KEY-999" not in logged_text
        assert "***" in logged_text

    def test_redact_helper_direct(self):
        assert _redact_api_key("...&api_key=abc123&other=1") == "...&api_key=***&other=1"
        assert _redact_api_key("no key here") == "no key here"


# ---------------------------------------------------------------------------
# 5. Duplicate period within a single response -- defect proved + fixed
# ---------------------------------------------------------------------------


class TestDuplicateWithinBatch:
    @patch("ingestion.altdata.eia_puller.requests.get")
    def test_duplicate_period_in_one_response_inserts_once(
        self, mock_get, engine, monkeypatch
    ):
        """Defect proved + fixed in this commit.

        Before the fix: `pull()` computed `existing` (already-stored dates)
        ONCE per facet, before iterating `records`, and never updated it
        while inserting. Two records for the same `period` in a single
        API response (EIA has been observed to do this on retried pages)
        would both pass the `if obs in existing` check and both get
        inserted -- a same-run duplicate that `_get_existing_dates`
        (queried once, up front) could not catch. The fix adds the just-
        inserted date to `existing` immediately after each insert.
        """
        monkeypatch.setenv("EIA_API_KEY", "test-key-not-real")
        empty = _load_json("empty_response.json")
        dup = _load_json("duplicate_in_batch.json")
        mock_get.side_effect = [_resp(empty), _resp(dup)]

        result = EIAPuller(db_engine=engine).pull()

        rows = engine.store.rows_for("eia.wti_spot")
        assert len(rows) == 1  # not 2
        assert rows[0]["obs_date"] == date(2026, 9, 16)
        assert result["rows_inserted"] == 1


# ---------------------------------------------------------------------------
# 6. Revised value, same obs_date -- documented NEVER-revision contract
# ---------------------------------------------------------------------------


class TestRevisedValue:
    @patch("ingestion.altdata.eia_puller.requests.get")
    def test_revised_value_same_date_is_not_overwritten(
        self, mock_get, engine, monkeypatch
    ):
        """EIAPuller.SOURCE_CONFIG['revision_behavior'] == 'NEVER'
        (ingestion/altdata/eia_puller.py:42). This pins that contract: a
        later pull that returns a *different* value for a `period` already
        stored as SUCCESS does not overwrite it and does not add a second
        row -- first-write-wins, silently. That is the documented contract,
        not a bug; if EIA's spot-price series ever starts being revised,
        this behaviour would need `_get_existing_dates`
        (ingestion/base.py:400-446) to become vintage-aware.
        """
        monkeypatch.setenv("EIA_API_KEY", "test-key-not-real")
        good = _load_json("good_response.json")
        empty = _load_json("empty_response.json")
        puller = EIAPuller(db_engine=engine)

        mock_get.side_effect = [_resp(good), _resp(empty)]
        puller.pull()
        original = {r["obs_date"]: r["value"] for r in engine.store.rows_for("eia.brent_spot")}

        revised = _load_json("revised_response.json")
        mock_get.side_effect = [_resp(revised), _resp(empty)]
        puller.pull()

        rows = engine.store.rows_for("eia.brent_spot")
        assert len(rows) == 3  # still just the original three, no new/updated rows
        assert {r["obs_date"]: r["value"] for r in rows} == original
        assert original[date(2026, 9, 14)] == pytest.approx(68.42)  # not 70.10


# ---------------------------------------------------------------------------
# 7. Idempotent double-run (identical response) -- no duplicates
# ---------------------------------------------------------------------------


class TestIdempotentRerun:
    @patch("ingestion.altdata.eia_puller.requests.get")
    def test_running_pull_twice_does_not_duplicate(self, mock_get, engine, monkeypatch):
        monkeypatch.setenv("EIA_API_KEY", "test-key-not-real")
        good = _load_json("good_response.json")
        empty = _load_json("empty_response.json")
        puller = EIAPuller(db_engine=engine)

        mock_get.side_effect = [_resp(good), _resp(empty)]
        puller.pull()
        mock_get.side_effect = [_resp(good), _resp(empty)]
        puller.pull()

        assert len(engine.store.rows_for("eia.brent_spot")) == 3


# ---------------------------------------------------------------------------
# 8. Dry-run mode: verified absent, not merely assumed absent
# ---------------------------------------------------------------------------


class TestDryRunMode:
    def test_no_dry_run_parameter_anywhere_in_the_call_path(self):
        for fn in (EIAPuller.__init__, EIAPuller.pull, EIAPuller._fetch_series):
            params = inspect.signature(fn).parameters
            assert "dry_run" not in params, f"{fn.__qualname__} unexpectedly has dry_run"
