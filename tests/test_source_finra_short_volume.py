"""Tests for the FINRA Daily Short Sale Volume puller. Registered in the
scheduler as ``finra_short_volume`` (Wave 1 activation, 2026-09-27). See
ingestion/altdata/finra_short_volume.py's module docstring for the exact
FINRA documentation quotes this module was built from.

Pure Python: no real database, no network. Uses a small in-memory
FakeEngine/FakeConn standing in for Postgres (records INSERT params and
answers the batched existence queries BasePuller._get_existing_pairs_in_range
/ _get_existing_source_dates / _file_advisory_lock issue), and monkeypatches
the puller's fetch method as a fake HTTP layer.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
import requests

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

        if "pg_advisory_xact_lock" in sql:
            return _FakeResult([])

        if "SELECT series_id, obs_date FROM raw_series" in sql:
            # BasePuller._get_existing_pairs_in_range: one query for the
            # whole file, bounded by obs_date range (not by series_id).
            src = params["src"]
            start, end = params["start_date"], params["end_date"]
            rows = [
                (r["series_id"], r["obs_date"])
                for r in self.store["rows"]
                if r["source_id"] == src
                and r["pull_status"] == "SUCCESS"
                and start <= r["obs_date"] <= end
            ]
            return _FakeResult(rows)

        if "SELECT DISTINCT obs_date FROM raw_series" in sql and "series_id" not in sql:
            # BasePuller._get_existing_source_dates: file-level check used
            # by pull_recent's catch-up loop.
            src = params["src"]
            dates = {
                r["obs_date"]
                for r in self.store["rows"]
                if r["source_id"] == src and r["pull_status"] == "SUCCESS"
            }
            if "start_date" in params:
                dates = {d for d in dates if d >= params["start_date"]}
            if "end_date" in params:
                dates = {d for d in dates if d <= params["end_date"]}
            return _FakeResult([(d,) for d in sorted(dates)])

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
    p._fetch_raw_text = lambda trade_date, url=None, **kwargs: _load("good.txt")

    result = p.pull("2026-09-16")

    assert result["status"] == "SUCCESS"
    # good.txt has 8 raw rows: AAPL/MSFT/SPY x multiple markets each.
    # Keyed on symbol ALONE (not symbol+market) -- see module docstring's
    # "Design decision needing owner sign-off" note -- so only the FIRST
    # row per (date, symbol) in file order is kept: 3 symbols -> 3 rows.
    assert result["rows_inserted"] == 3
    assert result["rows_skipped"] == 0
    assert result["dry_run"] is False

    rows = p.engine.store["rows"]
    assert len(rows) == 3
    row = next(r for r in rows if r["series_id"] == "finra:short_volume:AAPL")
    assert row["value"] == 123456.0
    payload = json.loads(row["raw_payload"])
    assert payload["short_exempt_volume"] == 1000.0
    assert payload["total_volume"] == 500000.0
    assert payload["market"] == "D"
    assert row["pull_status"] == "SUCCESS"


def test_pull_empty_file_writes_no_value_zero_rows():
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None, **kwargs: _load("empty.txt")

    result = p.pull("2026-09-16")

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 0
    assert p.engine.store["rows"] == []


def test_pull_malformed_rows_are_skipped_not_written():
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None, **kwargs: _load("malformed.txt")

    result = p.pull("2026-09-16")

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 1
    assert result["rows_skipped"] == 2
    assert len(p.engine.store["rows"]) == 1


def test_pull_fetch_failure_returns_failed_status_no_writes():
    p = _puller()

    def _boom(trade_date, url=None, **kwargs):
        raise ConnectionError("simulated network failure contacting FINRA")

    p._fetch_raw_text = _boom

    result = p.pull("2026-09-16")

    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert "error" in result
    assert len(result["error"]) < 400  # bounded, credential-safe
    assert p.engine.store["rows"] == []


def test_pull_404_returns_skipped_not_failed():
    """A 403/404 means no file exists for this trade_date yet (requested
    too early, or FINRA never files for this date) -- NOT a failure of
    this puller, and must not feed the exponential-backoff cooldown as a
    real failure would (see SmartScheduler._record_result)."""
    p = _puller()

    def _not_found(trade_date, url=None, **kwargs):
        resp = requests.Response()
        resp.status_code = 404
        raise requests.HTTPError("404 Client Error", response=resp)

    p._fetch_raw_text = _not_found

    result = p.pull("2026-09-16")

    assert result["status"] == "SKIPPED"
    assert result["rows_inserted"] == 0
    assert "skipped_reason" in result
    assert "404" in result["skipped_reason"]
    assert p.engine.store["rows"] == []


def test_pull_403_returns_skipped_not_failed():
    p = _puller()

    def _forbidden(trade_date, url=None, **kwargs):
        resp = requests.Response()
        resp.status_code = 403
        raise requests.HTTPError("403 Client Error", response=resp)

    p._fetch_raw_text = _forbidden

    result = p.pull("2026-09-16")

    assert result["status"] == "SKIPPED"
    assert p.engine.store["rows"] == []


def test_pull_unparseable_file_returns_failed_status_no_writes():
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None, **kwargs: "not a finra file at all\n"

    result = p.pull("2026-09-16")

    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert p.engine.store["rows"] == []


def test_pull_is_idempotent_across_repeated_calls():
    engine = FakeEngine()
    p1 = _puller(engine)
    p1._fetch_raw_text = lambda trade_date, url=None, **kwargs: _load("good.txt")
    p1.pull("2026-09-16")

    p2 = _puller(engine)
    p2._fetch_raw_text = lambda trade_date, url=None, **kwargs: _load("good.txt")
    result2 = p2.pull("2026-09-16")

    assert result2["rows_inserted"] == 0  # everything already stored
    assert len(engine.store["rows"]) == 3  # not duplicated to 6


def test_pull_revised_same_date_does_not_overwrite_existing_value():
    engine = FakeEngine()
    p1 = _puller(engine)
    p1._fetch_raw_text = lambda trade_date, url=None, **kwargs: _load("good.txt")
    p1.pull("2026-09-16")

    p2 = _puller(engine)
    p2._fetch_raw_text = lambda trade_date, url=None, **kwargs: _load("revised_same_date.txt")
    result2 = p2.pull("2026-09-16")

    # revised_same_date.txt has the same (date, symbol=AAPL) key as a row
    # already stored -- it must be skipped, not overwritten.
    assert result2["rows_inserted"] == 0
    stored = [
        r for r in engine.store["rows"] if r["series_id"] == "finra:short_volume:AAPL"
    ]
    assert len(stored) == 1
    assert stored[0]["value"] == 123456.0  # original value, not 999999.0


def test_pull_dry_run_writes_nothing():
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None, **kwargs: _load("good.txt")

    result = p.pull("2026-09-16", dry_run=True)

    assert result["status"] == "SUCCESS"
    assert result["dry_run"] is True
    assert result["rows_inserted"] == 0
    # 8 raw rows collapse to 3 (symbol-only keying, see
    # test_pull_success_writes_series_id_scheme_and_payload).
    assert result["rows_would_insert"] == 3
    assert p.engine.store["rows"] == []


def test_fetch_raw_text_builds_documented_url_without_explicit_url(monkeypatch):
    """Without an explicit url=, _fetch_raw_text must build the verified
    <base_url><MarketPrefix>shvol<YYYYMMDD>.txt URL (see module docstring
    and REAL_CAPTURE_NOTE.txt) rather than raising -- the endpoint is
    confirmed live now, unlike the earlier contract-first placeholder."""
    captured = {}

    class _FakeResp:
        text = "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n0\n"

        def raise_for_status(self):
            return None

    def _fake_get(url, headers=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        return _FakeResp()

    monkeypatch.setattr(
        "ingestion.altdata.finra_short_volume.requests.get", _fake_get
    )
    p = _puller()
    p._fetch_raw_text(date(2026, 9, 16))

    assert captured["url"] == (
        "https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260916.txt"
    )
    assert captured["headers"]["User-Agent"] == "GRID/1.0 (aniksrobot@gmail.com)"


def test_fetch_raw_text_honors_explicit_market_prefix(monkeypatch):
    captured = {}

    class _FakeResp:
        text = ""

        def raise_for_status(self):
            return None

    def _fake_get(url, headers=None, timeout=None):
        captured["url"] = url
        return _FakeResp()

    monkeypatch.setattr(
        "ingestion.altdata.finra_short_volume.requests.get", _fake_get
    )
    p = _puller()
    p._fetch_raw_text(date(2026, 9, 16), market_prefix="FORF")

    assert captured["url"] == (
        "https://cdn.finra.org/equity/regsho/daily/FORFshvol20260916.txt"
    )


def test_fetch_raw_text_explicit_url_overrides_built_one(monkeypatch):
    captured = {}

    class _FakeResp:
        text = "ok"

        def raise_for_status(self):
            return None

    def _fake_get(url, headers=None, timeout=None):
        captured["url"] = url
        return _FakeResp()

    monkeypatch.setattr(
        "ingestion.altdata.finra_short_volume.requests.get", _fake_get
    )
    p = _puller()
    p._fetch_raw_text(date(2026, 9, 16), url="https://example.test/override.txt")

    assert captured["url"] == "https://example.test/override.txt"


# ── pull_recent() catch-up loop ──────────────────────────────────────────


def test_pull_recent_walks_back_weekdays_only():
    """anchor on a Tuesday, weekdays_back=5 -> the prior Tue/Wed/Thu/Fri/Mon
    (never a Saturday or Sunday)."""
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None, **kwargs: _load("empty.txt")

    result = p.pull_recent(anchor_date="2026-09-16", weekdays_back=5)  # Wed

    dates = [d["date"] for d in result["dates"]]
    assert dates == [
        "2026-09-10",  # Thu
        "2026-09-11",  # Fri
        "2026-09-14",  # Mon
        "2026-09-15",  # Tue
        "2026-09-16",  # Wed (anchor)
    ]
    assert all(date.fromisoformat(d).weekday() < 5 for d in dates)


def test_pull_recent_skips_dates_already_stored_for_the_source():
    """A date with ANY existing SUCCESS row for this source (any series)
    is skipped without calling pull() again -- the one cheap file-level
    query, not a per-symbol check."""
    engine = FakeEngine()
    engine.store["rows"].append(
        {
            "series_id": "finra:short_volume:ZZZZ",
            "source_id": 7,
            "obs_date": date(2026, 9, 15),
            "value": 1.0,
            "raw_payload": None,
            "pull_status": "SUCCESS",
        }
    )
    p = _puller(engine)
    calls = []

    def _fetch(trade_date, url=None, **kwargs):
        calls.append(trade_date)
        return _load("empty.txt")

    p._fetch_raw_text = _fetch

    result = p.pull_recent(anchor_date="2026-09-16", weekdays_back=2)

    # Only 2026-09-16 fetched; 2026-09-15 short-circuited as already stored.
    assert calls == [date(2026, 9, 16)]
    by_date = {d["date"]: d for d in result["dates"]}
    assert by_date["2026-09-15"]["status"] == "SKIPPED"
    assert by_date["2026-09-15"]["reason"] == "already stored"


def test_pull_recent_aggregates_inserted_rows_across_dates():
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None, **kwargs: _load("good.txt")

    result = p.pull_recent(anchor_date="2026-09-16", weekdays_back=1)

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 3  # 8 raw rows -> 3 unique symbols


def test_pull_recent_one_hard_failure_marks_overall_failed():
    p = _puller()

    def _fetch(trade_date, url=None, **kwargs):
        if trade_date == date(2026, 9, 16):
            raise ConnectionError("boom")
        return _load("empty.txt")

    p._fetch_raw_text = _fetch

    result = p.pull_recent(anchor_date="2026-09-16", weekdays_back=2)

    assert result["status"] == "FAILED"


def test_pull_recent_all_skipped_not_failed():
    """A run where every date is either already-stored or not-yet-
    published (SKIPPED) is a normal outcome, not a failure."""
    p = _puller()

    def _not_found(trade_date, url=None, **kwargs):
        resp = requests.Response()
        resp.status_code = 404
        raise requests.HTTPError("404", response=resp)

    p._fetch_raw_text = _not_found

    result = p.pull_recent(anchor_date="2026-09-16", weekdays_back=2)

    assert result["status"] == "SUCCESS"
    assert all(d["status"] == "SKIPPED" for d in result["dates"])


# ── Real captured fixture (see REAL_CAPTURE_NOTE.txt for provenance) ───────


def test_parse_real_captured_finra_fixture_matches_documented_columns():
    parsed = parse_daily_short_volume_file(_load("real_capture_sample.txt"))
    assert parsed["rows"], "real fixture should contain parsed rows"

    for row in parsed["rows"]:
        assert set(row) == {
            "date",
            "symbol",
            "market",
            "short_volume",
            "short_exempt_volume",
            "total_volume",
        }
        assert row["date"] == date(2026, 9, 16)
        assert row["symbol"]
        assert row["market"]  # may be a single code or a comma-joined list


def test_real_fixture_reconciles_short_le_total():
    """Per the documented semantics, ShortVolume is a subset of TotalVolume
    for the same (date, symbol, market) row -- must hold on real data."""
    parsed = parse_daily_short_volume_file(_load("real_capture_sample.txt"))
    assert parsed["rows"]
    for row in parsed["rows"]:
        assert row["short_volume"] <= row["total_volume"], row


def test_series_id_namespace_is_disjoint_from_finra_ats_dot_namespace():
    sid = FINRAShortVolumePuller.series_id("aapl")
    assert sid == "finra:short_volume:AAPL"
    # finra_ats.py uses a dot-delimited "finra.<feature>" namespace
    # (finra.ats_total_volume, finra.short_interest_total, ...) -- this
    # module's colon-delimited ids must never collide with that.
    assert not sid.startswith("finra.")
