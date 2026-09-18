"""Tests for the FINRA Daily Short Sale Volume puller (contract-first, not
scheduled). See ingestion/altdata/finra_short_volume.py's module docstring
for the exact FINRA documentation quotes this module was built from.

Pure Python: no real database, no network. Uses a small in-memory
FakeEngine/FakeConn standing in for Postgres (records INSERT params and
answers the same SELECT DISTINCT obs_date query BasePuller._get_existing_dates
issues), and monkeypatches the puller's fetch method as a fake HTTP layer.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.altdata.finra_short_volume import (
    FINRAShortVolumePuller,
    parse_daily_short_volume_file,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "sources" / "finra_short_volume"


def _load(name: str) -> str:
    return (_FIXTURES / name).read_text()


# ── Fake store (stands in for Postgres; see module docstring) ──────────────


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, store):
        self.store = store

    def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}
        if "SELECT DISTINCT obs_date FROM raw_series" in sql:
            sid, src = params["sid"], params["src"]
            dates = sorted(
                {
                    r["obs_date"]
                    for r in self.store["rows"]
                    if r["series_id"] == sid and r["source_id"] == src
                }
            )
            return _FakeResult([(d,) for d in dates])
        if "INSERT INTO raw_series" in sql:
            self.store["rows"].append(
                {
                    "series_id": params["sid"],
                    "source_id": params["src"],
                    "obs_date": params["od"],
                    "value": params["val"],
                    "raw_payload": params["payload"],
                    "pull_status": params["status"],
                }
            )
            return _FakeResult([])
        raise NotImplementedError(f"FakeConn.execute: unhandled SQL: {sql[:100]!r}")

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class FakeEngine:
    """Fake store: no local Postgres, no sqlite -- just a Python list."""

    def __init__(self):
        self.store = {"rows": []}

    def begin(self):
        return _FakeConn(self.store)

    def connect(self):
        return _FakeConn(self.store)


def _puller(engine: FakeEngine | None = None) -> FINRAShortVolumePuller:
    """Build a puller without touching BasePuller.__init__ (no real DB)."""
    p = FINRAShortVolumePuller.__new__(FINRAShortVolumePuller)
    p.engine = engine or FakeEngine()
    p.source_id = 7
    return p


# ── Parser / schema ─────────────────────────────────────────────────────────


def test_parse_good_file_all_rows_and_fields():
    parsed = parse_daily_short_volume_file(_load("good.txt"))
    assert parsed["skipped"] == 0
    assert len(parsed["rows"]) == 8

    aapl_d = next(
        r for r in parsed["rows"] if r["symbol"] == "AAPL" and r["market"] == "D"
    )
    assert aapl_d["date"] == date(2026, 9, 16)
    assert aapl_d["short_volume"] == 123456.0
    assert aapl_d["short_exempt_volume"] == 1000.0
    assert aapl_d["total_volume"] == 500000.0


def test_parse_empty_file_returns_no_rows():
    parsed = parse_daily_short_volume_file(_load("empty.txt"))
    assert parsed["rows"] == []
    assert parsed["skipped"] == 0


def test_parse_malformed_file_skips_bad_rows_keeps_good_ones():
    parsed = parse_daily_short_volume_file(_load("malformed.txt"))
    # One well-formed row (AAPL); the ABCDEF (non-numeric) row and the
    # 5-field SPY row are both skipped.
    assert len(parsed["rows"]) == 1
    assert parsed["skipped"] == 2
    assert parsed["rows"][0]["symbol"] == "AAPL"


def test_parse_unrecognized_header_raises():
    with pytest.raises(ValueError):
        parse_daily_short_volume_file("this is not a FINRA file\nnope\n")


def test_parse_blank_input_raises():
    with pytest.raises(ValueError):
        parse_daily_short_volume_file("   \n\n  ")


# ── pull() status handling / idempotency / dry-run ──────────────────────────


def test_pull_success_writes_series_id_scheme_and_payload():
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None: _load("good.txt")

    result = p.pull("2026-09-16")

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 8
    assert result["rows_skipped"] == 0
    assert result["dry_run"] is False

    rows = p.engine.store["rows"]
    assert len(rows) == 8
    row = next(r for r in rows if r["series_id"] == "finra:short_volume:AAPL:D")
    assert row["value"] == 123456.0
    payload = json.loads(row["raw_payload"])
    assert payload["short_exempt_volume"] == 1000.0
    assert payload["total_volume"] == 500000.0
    assert payload["market"] == "D"
    assert row["pull_status"] == "SUCCESS"


def test_pull_empty_file_writes_no_value_zero_rows():
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None: _load("empty.txt")

    result = p.pull("2026-09-16")

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 0
    assert p.engine.store["rows"] == []


def test_pull_malformed_rows_are_skipped_not_written():
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None: _load("malformed.txt")

    result = p.pull("2026-09-16")

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 1
    assert result["rows_skipped"] == 2
    assert len(p.engine.store["rows"]) == 1


def test_pull_fetch_failure_returns_failed_status_no_writes():
    p = _puller()

    def _boom(trade_date, url=None):
        raise ConnectionError("simulated network failure contacting FINRA")

    p._fetch_raw_text = _boom

    result = p.pull("2026-09-16")

    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert "error" in result
    assert len(result["error"]) < 400  # bounded, credential-safe
    assert p.engine.store["rows"] == []


def test_pull_unparseable_file_returns_failed_status_no_writes():
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None: "not a finra file at all\n"

    result = p.pull("2026-09-16")

    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert p.engine.store["rows"] == []


def test_pull_is_idempotent_across_repeated_calls():
    engine = FakeEngine()
    p1 = _puller(engine)
    p1._fetch_raw_text = lambda trade_date, url=None: _load("good.txt")
    p1.pull("2026-09-16")

    p2 = _puller(engine)
    p2._fetch_raw_text = lambda trade_date, url=None: _load("good.txt")
    result2 = p2.pull("2026-09-16")

    assert result2["rows_inserted"] == 0  # everything already stored
    assert len(engine.store["rows"]) == 8  # not duplicated to 16


def test_pull_revised_same_date_does_not_overwrite_existing_value():
    engine = FakeEngine()
    p1 = _puller(engine)
    p1._fetch_raw_text = lambda trade_date, url=None: _load("good.txt")
    p1.pull("2026-09-16")

    p2 = _puller(engine)
    p2._fetch_raw_text = lambda trade_date, url=None: _load("revised_same_date.txt")
    result2 = p2.pull("2026-09-16")

    # revised_same_date.txt has the same (date, symbol=AAPL, market=D) key
    # as a row already stored -- it must be skipped, not overwritten.
    assert result2["rows_inserted"] == 0
    stored = [
        r for r in engine.store["rows"] if r["series_id"] == "finra:short_volume:AAPL:D"
    ]
    assert len(stored) == 1
    assert stored[0]["value"] == 123456.0  # original value, not 999999.0


def test_pull_dry_run_writes_nothing():
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None: _load("good.txt")

    result = p.pull("2026-09-16", dry_run=True)

    assert result["status"] == "SUCCESS"
    assert result["dry_run"] is True
    assert result["rows_inserted"] == 0
    assert result["rows_would_insert"] == 8
    assert p.engine.store["rows"] == []


def test_fetch_raw_text_without_url_raises_not_implemented():
    """The unverified placeholder endpoint must never be trusted silently."""
    p = _puller()
    with pytest.raises(NotImplementedError):
        p._fetch_raw_text(date(2026, 9, 16))


def test_series_id_namespace_is_disjoint_from_finra_ats_dot_namespace():
    sid = FINRAShortVolumePuller.series_id("aapl", "d")
    assert sid == "finra:short_volume:AAPL:D"
    # finra_ats.py uses a dot-delimited "finra.<feature>" namespace
    # (finra.ats_total_volume, finra.short_interest_total, ...) -- this
    # module's colon-delimited ids must never collide with that.
    assert not sid.startswith("finra.")
