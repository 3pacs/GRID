"""Tests for the SEC Fails-to-Deliver (FTD) puller. Registered in the
scheduler as ``sec_ftd`` (Wave 1 activation, 2026-09-27). See
ingestion/altdata/sec_ftd.py's module docstring for the exact SEC
documentation quotes this module was built from.

Pure Python: no real database, no network. Uses a small in-memory
FakeEngine/FakeConn standing in for Postgres, and monkeypatches the
puller's zip-fetch method as a fake HTTP layer.
"""

from __future__ import annotations

import json
import zipfile
from datetime import date
from io import BytesIO
from pathlib import Path

import pytest
import requests

from ingestion.altdata.finra_short_volume import FINRAShortVolumePuller
from ingestion.altdata.sec_ftd import (
    SECFTDPuller,
    _default_recent_half_months,
    _half_month_date_bounds,
    extract_ftd_text_from_zip,
    parse_ftd_file,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "sources" / "sec_ftd"


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


def _puller(engine: FakeEngine | None = None, source_id: int = 11) -> SECFTDPuller:
    """Build a puller without touching BasePuller.__init__ (no real DB)."""
    p = SECFTDPuller.__new__(SECFTDPuller)
    p.engine = engine or FakeEngine()
    p.source_id = source_id
    return p


# ── Parser / schema ─────────────────────────────────────────────────────────


def test_parse_good_file_all_rows_and_fields():
    parsed = parse_ftd_file(_load("good.txt"))
    assert parsed["skipped"] == 0
    assert len(parsed["rows"]) == 3

    aapl = next(r for r in parsed["rows"] if r["cusip"] == "037833100")
    assert aapl["date"] == date(2026, 7, 31)
    assert aapl["symbol"] == "AAPL"
    assert aapl["quantity_fails"] == 12345.0
    assert aapl["price"] == 180.25


def test_parse_empty_file_returns_no_rows():
    parsed = parse_ftd_file(_load("empty.txt"))
    assert parsed["rows"] == []
    assert parsed["skipped"] == 0


def test_parse_malformed_file_skips_bad_rows_keeps_good_ones():
    parsed = parse_ftd_file(_load("malformed.txt"))
    # NOTANUMBER quantity and the 3-field BADROW line are both skipped;
    # the MSFT row is well-formed.
    assert len(parsed["rows"]) == 1
    assert parsed["skipped"] == 2
    assert parsed["rows"][0]["symbol"] == "MSFT"


def test_parse_blank_input_raises():
    with pytest.raises(ValueError):
        parse_ftd_file("   \n\n  ")


def test_extract_ftd_text_from_zip_roundtrips():
    original = _load("good.txt")
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("cnsfails202607b.txt", original)
    extracted = extract_ftd_text_from_zip(buf.getvalue())
    assert extracted == original


def test_extract_ftd_text_from_empty_zip_raises():
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w"):
        pass
    with pytest.raises(ValueError):
        extract_ftd_text_from_zip(buf.getvalue())


# ── pull_from_text() status handling / idempotency / dry-run ───────────────


def test_pull_from_text_success_writes_balance_series_and_payload():
    p = _puller()

    result = p.pull_from_text(_load("good.txt"), publication_half="2026-07-b")

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 3
    assert result["rows_skipped"] == 0

    rows = p.engine.store["rows"]
    row = next(r for r in rows if r["series_id"] == "sec:ftd_balance:037833100")
    assert row["value"] == 12345.0
    payload = json.loads(row["raw_payload"])
    assert payload["is_outstanding_balance_not_new_fails"] is True
    assert payload["publication_half"] == "2026-07-b"
    assert row["pull_status"] == "SUCCESS"


def test_pull_from_text_empty_file_writes_no_value_zero_rows():
    p = _puller()

    result = p.pull_from_text(_load("empty.txt"))

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 0
    assert p.engine.store["rows"] == []


def test_pull_from_text_malformed_rows_skipped_not_written():
    p = _puller()

    result = p.pull_from_text(_load("malformed.txt"))

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 1
    assert result["rows_skipped"] == 2
    assert len(p.engine.store["rows"]) == 1


def test_pull_fetch_failure_returns_failed_status_no_writes():
    p = _puller()

    def _boom(url=None, yyyymm=None, half=None):
        raise ConnectionError("simulated network failure contacting SEC")

    p._fetch_zip_bytes = _boom

    result = p.pull()

    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert "error" in result
    assert len(result["error"]) < 400  # bounded, credential-safe
    assert p.engine.store["rows"] == []


def test_pull_404_returns_skipped_not_failed():
    """A 403/404 means the half-month zip does not exist yet at this URL
    (requested before SEC's approximate publish date) -- NOT a failure of
    this puller."""
    p = _puller()

    def _not_found(url=None, yyyymm=None, half=None):
        resp = requests.Response()
        resp.status_code = 404
        raise requests.HTTPError("404 Client Error", response=resp)

    p._fetch_zip_bytes = _not_found

    result = p.pull(yyyymm="202609", half="a")

    assert result["status"] == "SKIPPED"
    assert result["rows_inserted"] == 0
    assert "skipped_reason" in result
    assert p.engine.store["rows"] == []


def test_pull_from_text_garbage_line_is_skipped_not_fatal():
    """A single line that doesn't match the 6-field format (no recognizable
    header either) is treated the same as any other malformed row -- it is
    skipped, and the file ends up with zero rows -- rather than being a
    catastrophic parse failure. SEC's docs don't establish a mandatory
    header, so this is the more honest reading of "malformed" for this
    format (contrast with FINRA's file, which does mandate a header and
    treats an unrecognized one as fatal)."""
    p = _puller()

    result = p.pull_from_text("not an SEC FTD file\n")

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 0
    assert p.engine.store["rows"] == []


def test_pull_from_text_blank_input_returns_failed_status_no_writes():
    p = _puller()

    result = p.pull_from_text("   \n\n  ")

    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert p.engine.store["rows"] == []


def test_pull_from_text_is_idempotent_across_repeated_calls():
    engine = FakeEngine()
    p1 = _puller(engine)
    p1.pull_from_text(_load("good.txt"))

    p2 = _puller(engine)
    result2 = p2.pull_from_text(_load("good.txt"))

    assert result2["rows_inserted"] == 0
    assert len(engine.store["rows"]) == 3  # not duplicated to 6


def test_pull_from_text_revised_same_date_does_not_overwrite_existing_balance():
    engine = FakeEngine()
    p1 = _puller(engine)
    p1.pull_from_text(_load("good.txt"))

    p2 = _puller(engine)
    result2 = p2.pull_from_text(_load("revised_same_date.txt"))

    # revised_same_date.txt has the same (settlement date, CUSIP) key as a
    # balance already stored -- it must be skipped, not overwritten.
    assert result2["rows_inserted"] == 0
    stored = [
        r for r in engine.store["rows"] if r["series_id"] == "sec:ftd_balance:037833100"
    ]
    assert len(stored) == 1
    assert stored[0]["value"] == 12345.0  # original balance, not 99999.0


def test_pull_from_text_dry_run_writes_nothing():
    p = _puller()

    result = p.pull_from_text(_load("good.txt"), dry_run=True)

    assert result["status"] == "SUCCESS"
    assert result["dry_run"] is True
    assert result["rows_inserted"] == 0
    assert result["rows_would_insert"] == 3
    assert p.engine.store["rows"] == []


def test_fetch_zip_bytes_without_url_or_yyyymm_half_raises_value_error(monkeypatch):
    """Without an explicit url, both yyyymm and half are required to build
    the documented cnsfails<YYYYMM><a|b>.zip URL."""
    import config

    monkeypatch.setattr(config.settings, "SEC_USER_AGENT", "GRID/1.0 (test@example.com)")
    p = _puller()
    with pytest.raises(ValueError):
        p._fetch_zip_bytes()


def test_fetch_zip_bytes_without_sec_user_agent_fails_closed(monkeypatch):
    """SEC requires a descriptive User-Agent with contact info -- this must
    never silently fall back to a default or send an unidentified request."""
    import config

    monkeypatch.setattr(config.settings, "SEC_USER_AGENT", "")
    p = _puller()
    with pytest.raises(RuntimeError, match="SEC_USER_AGENT"):
        p._fetch_zip_bytes(yyyymm="202608", half="b")


def test_series_id_namespace_is_sec_ftd_balance():
    sid = SECFTDPuller.series_id("037833100")
    assert sid == "sec:ftd_balance:037833100"


def test_fetch_zip_bytes_builds_documented_url(monkeypatch):
    """Without an explicit url=, the documented cnsfails<YYYYMM><a|b>.zip
    URL is built from yyyymm/half -- the endpoint is confirmed live now
    (see module docstring and REAL_CAPTURE_NOTE.txt)."""
    import config

    monkeypatch.setattr(
        config.settings, "SEC_USER_AGENT", "GRID/1.0 (test@example.com)"
    )
    captured = {}

    class _FakeResp:
        content = b"PK\x05\x06" + b"\x00" * 18  # minimal empty-zip EOCD

        def raise_for_status(self):
            return None

    def _fake_get(url, headers=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        return _FakeResp()

    monkeypatch.setattr("ingestion.altdata.sec_ftd.requests.get", _fake_get)
    p = _puller()
    p._fetch_zip_bytes(yyyymm="202608", half="b")

    assert captured["url"] == (
        "https://www.sec.gov/files/data/fails-deliver-data/cnsfails202608b.zip"
    )
    assert captured["headers"]["User-Agent"] == "GRID/1.0 (test@example.com)"


# ── Real captured fixtures (see REAL_CAPTURE_NOTE.txt for provenance) ─────


def test_parse_real_first500_matches_documented_columns():
    parsed = parse_ftd_file(_load("real_capture_first500.txt"))
    assert len(parsed["rows"]) == 500
    assert parsed["skipped"] == 0

    for row in parsed["rows"]:
        assert set(row) == {
            "date",
            "cusip",
            "symbol",
            "quantity_fails",
            "description",
            "price",
        }
        assert row["date"] == date(2026, 8, 17)
        assert len(row["cusip"]) == 9
        assert row["quantity_fails"] >= 0


def test_real_fixture_same_cusip_across_dates_not_summed():
    """Real rows for one CUSIP (Y4000A102 / HQ) across all 11 settlement
    dates in the captured half-month file. Each pull() call must store
    each date's balance independently -- never summed or overwritten --
    per the documented "outstanding balance as of a settlement date"
    semantics."""
    parsed = parse_ftd_file(_load("real_capture_multi_date_cusip.txt"))
    assert len(parsed["rows"]) == 11
    assert len({r["date"] for r in parsed["rows"]}) == 11  # 11 distinct dates
    assert all(r["cusip"] == "Y4000A102" for r in parsed["rows"])

    p = _puller()
    result = p.pull_from_text(
        _load("real_capture_multi_date_cusip.txt"), publication_half="2026-08-real"
    )

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 11

    stored = [
        r for r in p.engine.store["rows"] if r["series_id"] == "sec:ftd_balance:Y4000A102"
    ]
    assert len(stored) == 11
    # Each date's balance is stored as its own row with its own value --
    # never aggregated into a single summed figure.
    stored_by_date = {r["obs_date"]: r["value"] for r in stored}
    assert stored_by_date[date(2026, 8, 17)] == 373.0
    assert stored_by_date[date(2026, 8, 24)] == 145601.0
    assert stored_by_date[date(2026, 8, 31)] == 179.0
    assert sum(stored_by_date.values()) != stored_by_date[date(2026, 8, 17)]


# ── pull_recent() catch-up loop ──────────────────────────────────────────


def test_half_month_date_bounds():
    assert _half_month_date_bounds("202609", "a") == (date(2026, 9, 1), date(2026, 9, 15))
    assert _half_month_date_bounds("202609", "b") == (date(2026, 9, 16), date(2026, 9, 30))
    assert _half_month_date_bounds("202602", "b") == (date(2026, 2, 16), date(2026, 2, 28))


def test_default_recent_half_months_covers_lookback_window():
    periods = _default_recent_half_months(2)
    assert len(periods) == 6  # 3 months (current + 2 back) x 2 halves
    assert all(set(p) == {"yyyymm", "half"} for p in periods)
    assert all(p["half"] in ("a", "b") for p in periods)


def test_pull_recent_skips_half_already_stored_for_the_source():
    """A half whose settlement-date range already has ANY existing
    SUCCESS row for this source is skipped -- a file-level check, not a
    per-CUSIP one."""
    engine = FakeEngine()
    engine.store["rows"].append(
        {
            "series_id": "sec:ftd_balance:ZZZZZZZZZ",
            "source_id": 11,
            "obs_date": date(2026, 9, 5),
            "value": 1.0,
            "raw_payload": None,
            "pull_status": "SUCCESS",
        }
    )
    p = _puller(engine)
    calls = []

    def _fetch(url=None, yyyymm=None, half=None):
        calls.append((yyyymm, half))
        raise ConnectionError("should not be called for an already-stored half")

    p._fetch_zip_bytes = _fetch

    result = p.pull_recent(periods=[{"yyyymm": "202609", "half": "a"}])

    assert calls == []  # never attempted -- already stored
    assert result["status"] == "SUCCESS"
    assert result["periods"][0]["status"] == "SKIPPED"
    assert result["periods"][0]["reason"] == "already stored"


def _zip_bytes(text: str) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("cnsfails.txt", text)
    return buf.getvalue()


def test_pull_recent_tries_and_aggregates_not_yet_stored_halves():
    p = _puller()
    good_bytes = _zip_bytes(_load("good.txt"))
    p._fetch_zip_bytes = lambda url=None, yyyymm=None, half=None: good_bytes

    result = p.pull_recent(
        periods=[
            {"yyyymm": "202607", "half": "a"},
            {"yyyymm": "202607", "half": "b"},
        ]
    )

    assert result["status"] == "SUCCESS"
    # Both periods share the same fixture (good.txt, all rows dated
    # 2026-07-31) via the monkeypatched _fetch_zip_bytes. The FIRST
    # period tried ("a", bounds 2026-07-01..07-15) has nothing stored yet
    # in that window, so it fetches and inserts the 3 rows -- landing them
    # at their real obs_date, 2026-07-31, regardless of "a"'s own nominal
    # window (publication_half is descriptive metadata only, never a row
    # filter). The SECOND period ("b", bounds 2026-07-16..07-31) then sees
    # those same 2026-07-31 rows already present in ITS window and is
    # SKIPPED by the file-level check rather than re-fetched.
    assert len(result["periods"]) == 2
    first, second = result["periods"]
    assert first["yyyymm"] == "202607" and first["half"] == "a"
    assert first["status"] == "SUCCESS"
    assert first["rows_inserted"] == 3
    assert second["status"] == "SKIPPED"
    assert result["rows_inserted"] == 3


def test_pull_recent_hard_failure_marks_overall_failed():
    p = _puller()

    def _boom(url=None, yyyymm=None, half=None):
        raise ConnectionError("simulated network failure")

    p._fetch_zip_bytes = _boom

    result = p.pull_recent(periods=[{"yyyymm": "202609", "half": "a"}])

    assert result["status"] == "FAILED"
    assert result["periods"][0]["status"] == "FAILED"


# ── Cross-source isolation: neither dataset lands under the other's ids ────


def test_finra_short_volume_and_sec_ftd_never_share_series_ids():
    """FINRA short SALE VOLUME and SEC FTD BALANCES are different concepts
    on different identifier axes (symbol+market vs. CUSIP) and must never
    be written under each other's series-id namespace, even when pulled
    into the same store back to back.
    """
    engine = FakeEngine()

    finra_p = FINRAShortVolumePuller.__new__(FINRAShortVolumePuller)
    finra_p.engine = engine
    finra_p.source_id = 7
    finra_fixtures = (
        Path(__file__).parent / "fixtures" / "sources" / "finra_short_volume"
    )
    finra_p._fetch_raw_text = lambda trade_date, url=None, **kwargs: (
        finra_fixtures / "good.txt"
    ).read_text()
    finra_result = finra_p.pull("2026-09-16")

    sec_p = _puller(engine, source_id=11)
    sec_result = sec_p.pull_from_text(_load("good.txt"))

    assert finra_result["rows_inserted"] == 3  # 8 raw rows -> 3 unique symbols
    assert sec_result["rows_inserted"] == 3

    finra_ids = {
        r["series_id"] for r in engine.store["rows"] if r["source_id"] == 7
    }
    sec_ids = {
        r["series_id"] for r in engine.store["rows"] if r["source_id"] == 11
    }

    assert finra_ids and sec_ids
    assert finra_ids.isdisjoint(sec_ids)
    assert all(sid.startswith("finra:short_volume:") for sid in finra_ids)
    assert all(sid.startswith("sec:ftd_balance:") for sid in sec_ids)
