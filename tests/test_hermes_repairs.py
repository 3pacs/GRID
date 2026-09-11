"""Regression coverage for the 2026-09-10 Hermes error-log repairs.

Each class maps to one error pattern seen in the grid-svr journal:

* ``TestPriceGuards`` / ``TestScorePendingMemo`` — 11,109 failing yfinance
  downloads a day from the trust scorer re-fetching dead tickers.
* ``TestExtractorBounds`` — signal_extractor statement timeouts from an
  unbounded ``series_id LIKE`` scan over the raw_series hypertable.
* ``TestWorkerCompletion`` / ``TestCoordinatorIdempotent`` — worker
  ``/complete`` read timeouts followed by 400 "Invalid transition" retries.
* ``TestBtpBund`` — ECB BTP-Bund uq_raw_series_composite violations.
* ``TestRedundancyNoScan`` — source_audit redundancy map DB scan timeouts.
* ``TestImfOutage`` — imfdatapy pointing at the retired IMF SDMX host.
"""

from __future__ import annotations

import inspect
import sys
import types
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from intelligence import signal_extractor as se
from intelligence import source_audit as sa
from intelligence import trust_scorer as ts
from ingestion.international import ecb, imf
from scripts import compute_coordinator as coord
from scripts import worker

UTC = timezone.utc


# ── helpers ────────────────────────────────────────────────────────────────


class _Result:
    def __init__(self, rows=None, one=None):
        self._rows = rows or []
        self._one = one

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        if self._one is not None:
            return self._one
        return self._rows[0] if self._rows else None

    def scalar(self):
        row = self.fetchone()
        return row[0] if row else None


class _Ctx:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, *exc):
        return False


class _RecordingConn:
    """Routes SQL by substring to canned results and records every call."""

    def __init__(self, routes):
        self.routes = routes  # list of (substring, rows)
        self.calls: list[tuple[str, dict | None]] = []

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        self.calls.append((sql, params))
        for needle, rows in self.routes:
            if needle in sql:
                return _Result(rows() if callable(rows) else rows)
        return _Result([])

    def commit(self):
        pass


def _engine(conn):
    return SimpleNamespace(begin=lambda: _Ctx(conn), connect=lambda: _Ctx(conn))


# ── trust scorer price guards ──────────────────────────────────────────────


class TestPriceGuards:
    def setup_method(self):
        ts._YF_NO_DATA.clear()

    @pytest.mark.parametrize("ticker", ["AAPL", "BRK.A", "BRK-B", "BTC-USD", "hvt.a", "GOOGL"])
    def test_priceable(self, ticker):
        assert ts.is_priceable_ticker(ticker)

    @pytest.mark.parametrize("ticker", ["MACRO", "NONE", "", None, "$SPX", "TOO LONG", "12345", "N/A", "ABCDEFG"])
    def test_unpriceable(self, ticker):
        assert not ts.is_priceable_ticker(ticker)

    def test_yf_symbol_uses_dash_for_class_shares(self):
        assert ts.yf_symbol("brk.a") == "BRK-A"
        assert ts.yf_symbol(" AAPL ") == "AAPL"

    def test_negative_cache_expires_after_ttl(self):
        now = datetime(2026, 9, 10, tzinfo=UTC)
        ts._remember_yf_no_data("DEAD", now)
        assert ts._yf_negative_cached("DEAD", now + timedelta(hours=1))
        later = now + timedelta(hours=ts.YF_NO_DATA_TTL_HOURS + 1)
        assert not ts._yf_negative_cached("DEAD", later)
        assert "DEAD" not in ts._YF_NO_DATA

    def _fake_yfinance(self, monkeypatch, downloader):
        fake = types.ModuleType("yfinance")
        fake.download = downloader
        monkeypatch.setitem(sys.modules, "yfinance", fake)

    def test_unpriceable_ticker_never_reaches_yfinance(self, monkeypatch):
        calls = []
        self._fake_yfinance(monkeypatch, lambda *a, **k: calls.append(a))
        assert ts._fetch_yfinance_price("MACRO", date(2026, 9, 1)) is None
        assert calls == []

    def test_empty_download_is_negative_cached_and_symbol_is_dashed(self, monkeypatch):
        pd = pytest.importorskip("pandas")
        calls = []

        def download(sym, **kwargs):
            calls.append(sym)
            return pd.DataFrame()

        self._fake_yfinance(monkeypatch, download)
        assert ts._fetch_yfinance_price("BRK.A", date(2026, 9, 1)) is None
        assert ts._fetch_yfinance_price("BRK.A", date(2026, 9, 2)) is None
        assert calls == ["BRK-A"]
        assert ts._yf_negative_cached("BRK-A")

    def test_download_exception_is_negative_cached(self, monkeypatch):
        def download(sym, **kwargs):
            raise RuntimeError("boom")

        self._fake_yfinance(monkeypatch, download)
        assert ts._fetch_yfinance_price("ZZZZ", date(2026, 9, 1)) is None
        assert ts._yf_negative_cached("ZZZZ")

    def test_last_close_handles_multiindex_columns(self):
        pd = pytest.importorskip("pandas")
        idx = pd.to_datetime(["2026-08-28", "2026-08-31", "2026-09-02"])
        cols = pd.MultiIndex.from_tuples([("Close", "AAPL"), ("Open", "AAPL")])
        frame = pd.DataFrame([[100.0, 99.0], [101.5, 100.0], [110.0, 108.0]], index=idx, columns=cols)
        assert ts._last_close(frame, date(2026, 9, 1)) == 101.5

    def test_last_close_flat_frame_and_nan(self):
        pd = pytest.importorskip("pandas")
        idx = pd.to_datetime(["2026-08-31", "2026-09-01"])
        frame = pd.DataFrame({"Close": [100.0, float("nan")]}, index=idx)
        assert ts._last_close(frame, date(2026, 9, 1)) == 100.0
        assert ts._last_close(pd.DataFrame(), date(2026, 9, 1)) is None

    def test_raw_series_lookup_is_bounded(self):
        conn = _RecordingConn([])
        engine = _engine(conn)
        target = date(2026, 9, 1)
        assert ts._get_price_near_date(engine, "MACRO", target) is None
        raw_sql, params = next((s, p) for s, p in conn.calls if "FROM raw_series" in s)
        assert "obs_date >= :lo" in raw_sql
        assert params["lo"] == target - timedelta(days=ts.PRICE_LOOKBACK_DAYS)


class TestScorePendingMemo:
    def test_one_lookup_per_ticker_date_and_dead_tickers_skip(self, monkeypatch):
        ts._YF_NO_DATA.clear()
        today = date.today()
        sig_date = today - timedelta(days=40)  # past the insider window, inside 90 d
        rows = [
            (1, "insider", "a", "AAPL", "BUY", sig_date, None),
            (2, "insider", "b", "AAPL", "BUY", sig_date, None),
            (3, "insider", "c", "ZZZZ", "BUY", sig_date, None),
            (4, "insider", "d", "ZZZZ", "BUY", sig_date, None),
            (5, "insider", "e", "ZZZZ", "SELL", sig_date, None),
            (6, "insider", "f", "MACRO", "BUY", sig_date, None),
        ]
        conn = _RecordingConn([("FROM signal_sources WHERE outcome", rows)])
        engine = _engine(conn)
        monkeypatch.setattr(ts, "_ensure_tables", lambda e: None)

        lookups: list[tuple[str, date]] = []

        def fake_price(engine_, ticker, d):
            lookups.append((ticker, d))
            if ticker == "ZZZZ":
                ts._remember_yf_no_data("ZZZZ")  # what the live fetch does on no data
                return None
            return 100.0 if d == sig_date else 105.0

        monkeypatch.setattr(ts, "_get_price_near_date", fake_price)

        summary = ts.score_pending_signals(engine)

        assert summary["scored"] == 2 and summary["correct"] == 2
        assert summary["skipped_unpriceable"] == 1
        assert summary["skipped_no_price"] == 3
        assert lookups.count(("ZZZZ", sig_date)) == 1  # dead after the first miss
        assert len([lk for lk in lookups if lk[0] == "AAPL"]) == 2  # entry + eval, memoised
        assert not any(lk[0] == "MACRO" for lk in lookups)
        updates = [s for s, _ in conn.calls if s.startswith("UPDATE signal_sources")]
        assert len(updates) == 2


# ── signal extractor ───────────────────────────────────────────────────────


class TestExtractorBounds:
    def test_raw_series_scan_bounded_by_lookback(self):
        conn = _RecordingConn([])
        engine = _engine(conn)
        se.extract_from_raw_series(engine, lookback_days=10)
        selects = [(s, p) for s, p in conn.calls if "FROM raw_series rs" in s]
        assert len(selects) == len(se.EXTRACTORS)
        for sql, params in selects:
            assert "rs.obs_date >= :since" in sql
            assert params["since"] == date.today() - timedelta(days=10)

    def test_signal_sources_scan_bounded_by_lookback(self):
        conn = _RecordingConn([])
        engine = _engine(conn)
        se.extract_from_signal_sources(engine, lookback_days=7)
        sql, params = next((s, p) for s, p in conn.calls if "FROM signal_sources ss" in s)
        assert "ss.signal_date >= :since" in sql
        assert params["since"] == date.today() - timedelta(days=7)

    def test_since_floor(self):
        assert se._since(0) == date.today() - timedelta(days=1)
        assert se.EXTRACTOR_LOOKBACK_DAYS < se.EXTRACTOR_FULL_SWEEP_DAYS


# ── worker / coordinator ───────────────────────────────────────────────────


class _Resp:
    def __init__(self, status=200, payload=None):
        self.status_code = status
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise worker.requests.exceptions.HTTPError(f"{self.status_code} Client Error")

    def json(self):
        return self._payload


class TestWorkerCompletion:
    def test_retry_after_timeout_treats_recorded_job_as_reported(self, monkeypatch):
        monkeypatch.setattr(worker.time, "sleep", lambda s: None)
        monkeypatch.setattr(worker, "execute_job", lambda job, url: {"output": {"ok": 1}, "metrics": {}})
        posts, gets = [], []

        def post(url, **kwargs):
            posts.append(url)
            if url.endswith("/start"):
                return _Resp()
            if len([u for u in posts if u.endswith("/complete")]) == 1:
                raise worker.requests.exceptions.ReadTimeout("Read timed out")
            return _Resp(400)

        def get(url, **kwargs):
            gets.append(url)
            state = "IN_PROGRESS" if len(gets) == 1 else "COMPLETED"
            return _Resp(payload={"id": 7, "state": state})

        monkeypatch.setattr(worker.requests, "post", post)
        monkeypatch.setattr(worker.requests, "get", get)

        assert worker.run_claimed_job({"id": 7, "job_type": "SIMULATION", "name": "x"}, "http://c", 1) is True
        assert len([u for u in posts if u.endswith("/complete")]) == 2
        assert gets == ["http://c/jobs/7", "http://c/jobs/7"]

    def test_job_already_recorded_false_when_probe_fails(self, monkeypatch):
        def get(url, **kwargs):
            raise RuntimeError("down")

        monkeypatch.setattr(worker.requests, "get", get)
        assert worker.job_already_recorded("http://c", 1) is False

    def test_completion_timeout_widened(self):
        assert worker.COMPLETE_TIMEOUT_S >= 60
        assert {"COMPLETED", "FAILED"} <= set(worker.TERMINAL_JOB_STATES)


class _Cur:
    def __init__(self, fetchones):
        self.fetchones = list(fetchones)
        self.sql: list[str] = []

    def execute(self, q, p=None):
        self.sql.append(q)

    def fetchone(self):
        return self.fetchones.pop(0) if self.fetchones else None


class _Conn:
    def __init__(self, cur):
        self._cur = cur
        self.autocommit = None
        self.rolled_back = False
        self.committed = False
        self.closed = False

    def cursor(self, **kwargs):
        return self._cur

    def rollback(self):
        self.rolled_back = True

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


class TestCoordinatorIdempotent:
    def test_recorded_when_terminal_state_and_result_row(self):
        cur = _Cur([{"state": "COMPLETED"}, (1,)])
        assert coord.completion_already_recorded(cur, 5) == "COMPLETED"

    def test_not_recorded_when_in_progress(self):
        assert coord.completion_already_recorded(_Cur([("IN_PROGRESS",)]), 5) is None

    def test_not_recorded_without_result_row(self):
        assert coord.completion_already_recorded(_Cur([("COMPLETED",), None]), 5) is None

    def test_unknown_job(self):
        assert coord.completion_already_recorded(_Cur([None]), 5) is None

    def test_complete_job_acks_duplicate_without_writing(self, monkeypatch):
        cur = _Cur([{"state": "COMPLETED"}, (1,)])
        conn = _Conn(cur)
        monkeypatch.setattr(coord, "get_conn", lambda: conn)
        out = coord.complete_job(5, coord.JobResult(job_id=5, worker_id=1))
        assert out["status"] == "already_recorded" and out["state"] == "COMPLETED"
        assert conn.rolled_back and conn.closed and not conn.committed
        assert not any("INSERT" in q for q in cur.sql)

    def test_blocking_handlers_run_off_the_event_loop(self):
        for fn in (coord.complete_job, coord.claim_job, coord.start_job,
                   coord.worker_heartbeat, coord.get_job, coord.coordinator_stats):
            assert not inspect.iscoroutinefunction(fn), fn.__name__
        # contract handlers driven with asyncio.run elsewhere stay async
        assert inspect.iscoroutinefunction(coord.create_job)
        assert inspect.iscoroutinefunction(coord.dry_run_job)


# ── ECB BTP-Bund spread ────────────────────────────────────────────────────


class TestBtpBund:
    def test_latest_vintage_join_and_dedupe(self):
        d1, d2 = date(2026, 9, 8), date(2026, 9, 9)
        conn = _RecordingConn([
            ("WITH btp AS", [(d1, 4.0, 2.5), (d2, 4.1, 2.6)]),
            ("SELECT DISTINCT obs_date", [(d1,)]),
        ])
        puller = ecb.ECBPuller.__new__(ecb.ECBPuller)
        puller.source_id = 210
        puller.engine = _engine(conn)

        result = puller._compute_btp_bund_spread()

        assert result["status"] == "SUCCESS" and result["rows_inserted"] == 1
        select_sql = next(s for s, _ in conn.calls if "WITH btp AS" in s)
        assert "DISTINCT ON (obs_date)" in select_sql and "pull_timestamp DESC" in select_sql
        inserts = [(s, p) for s, p in conn.calls if s.startswith("INSERT INTO raw_series")]
        assert len(inserts) == 1
        sql, params = inserts[0]
        assert "ON CONFLICT DO NOTHING" in sql
        assert params["od"] == d2 and params["val"] == pytest.approx(1.5)


# ── source audit ───────────────────────────────────────────────────────────


class TestRedundancyNoScan:
    def test_no_db_scan_and_map_still_populated(self):
        sa._REDUNDANCY_CACHE["data"] = None
        sa._REDUNDANCY_CACHE["ts"] = None
        engine = MagicMock()
        rmap = sa.build_redundancy_map(engine)
        engine.connect.assert_not_called()
        assert "vix_spot" in rmap and len(rmap["vix_spot"]) >= 2


# ── IMF ────────────────────────────────────────────────────────────────────


class TestImfOutage:
    def test_outage_detection(self):
        err = ConnectionError(
            "HTTPConnectionPool(host='dataservices.imf.org', port=80): Max retries exceeded "
            "(Caused by NameResolutionError(...))"
        )
        assert imf.is_upstream_outage(err)
        assert not imf.is_upstream_outage(ValueError("bad shape"))

    def test_fetch_datamapper_parses_values(self, monkeypatch):
        payload = {"values": {"NGDP_RPCH": {
            "USA": {"2024": 2.8, "2025": "1.9", "2027": None, "bad": 1},
            "CHN": {"2024": 5.0},
        }}}

        class R:
            status_code = 200
            content = b"{}"

            def raise_for_status(self):
                pass

            def json(self):
                return payload

        seen = []
        monkeypatch.setattr(imf.requests, "get", lambda url, timeout: seen.append(url) or R())
        out = imf.fetch_datamapper("NGDP_RPCH", ["USA", "CHN"])
        assert out["USA"] == {2024: 2.8, 2025: 1.9}
        assert out["CHN"] == {2024: 5.0}
        assert seen == [f"{imf.DATAMAPPER_BASE_URL}/NGDP_RPCH/USA/CHN"]

    def _weo_puller(self, conn):
        puller = imf.IMFPuller.__new__(imf.IMFPuller)
        puller.engine = _engine(conn)
        puller.source_id = 11
        return puller

    def test_pull_weo_skips_future_years_and_dedupes(self, monkeypatch):
        calls = []

        def fake_fetch(indicator, codes, timeout=30):
            calls.append(indicator)
            return {c: {2023: 1.0, 2024: 2.0, 2099: 9.0} for c in codes}

        monkeypatch.setattr(imf, "fetch_datamapper", fake_fetch)
        conn = _RecordingConn([("SELECT DISTINCT obs_date", [(date(2023, 1, 1),)])])
        res = self._weo_puller(conn).pull_weo(max_year=2026)

        n_targets = len(imf.IMF_WEO_TARGETS)
        assert res["status"] == "SUCCESS"
        assert res["rows_inserted"] == n_targets  # 2023 exists, 2099 is a projection
        assert res["skipped_future"] == n_targets
        assert sorted(calls) == sorted({s for s, _ in imf.IMF_WEO_TARGETS})  # one call per indicator
        inserts = [(s, p) for s, p in conn.calls if s.startswith("INSERT INTO raw_series")]
        assert len(inserts) == n_targets
        assert all("ON CONFLICT DO NOTHING" in s and p["od"] == date(2024, 1, 1) for s, p in inserts)

    def test_pull_weo_partial_when_one_indicator_fails(self, monkeypatch):
        def fake_fetch(indicator, codes, timeout=30):
            if indicator == "PCPIPCH":
                raise imf.requests.HTTPError("503 Server Error")
            return {c: {2024: 2.0} for c in codes}

        monkeypatch.setattr(imf, "fetch_datamapper", fake_fetch)
        conn = _RecordingConn([])
        res = self._weo_puller(conn).pull_weo(max_year=2026)
        assert res["status"] == "PARTIAL"
        assert any("PCPIPCH" in e for e in res["errors"])
        failed = sum(1 for s, _ in imf.IMF_WEO_TARGETS if s == "PCPIPCH")
        assert res["rows_inserted"] == len(imf.IMF_WEO_TARGETS) - failed

    def test_pull_ifs_skips_on_retired_host(self, monkeypatch):
        pkg = types.ModuleType("imfdatapy")
        mod = types.ModuleType("imfdatapy.imf")

        class IFS:
            def __init__(self, **kwargs):
                pass

            def download_data(self):
                raise ConnectionError("Failed to resolve 'dataservices.imf.org'")

        mod.IFS = IFS
        pkg.imf = mod
        monkeypatch.setitem(sys.modules, "imfdatapy", pkg)
        monkeypatch.setitem(sys.modules, "imfdatapy.imf", mod)
        monkeypatch.setattr(imf.time, "sleep", lambda s: None)

        puller = imf.IMFPuller.__new__(imf.IMFPuller)
        puller.engine = None
        puller.source_id = 1
        res = puller.pull_ifs("gross domestic product, real", "Q", "US")
        assert res["status"] == "SKIPPED"
        assert res["rows_inserted"] == 0
