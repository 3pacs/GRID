"""Unit tests for ingestion/altdata/iron_ore_ports.py (CAT-52)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy.engine import Engine

from ingestion.altdata import iron_ore_ports as iop
from ingestion.altdata.iron_ore_ports import (
    AKSHARE_FUNCTION_CANDIDATES,
    CHINESE_PORTS_45,
    IronOrePortSnapshot,
    IronOrePortsPuller,
    MYSTEEL_URL,
    SERIES_DELTA_WOW_AGG,
    SERIES_PORT_STOCKS_AGG,
    SERIES_PORT_STOCKS_PREFIX,
    SERIES_THROUGHPUT_AGG,
    _load_akshare_function,
    _parse_mysteel_html,
    compute_wow_delta,
    run_iron_ore_ports_puller,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine_row_missing() -> MagicMock:
    """Engine mock whose first execute() call returns source_id=42 and
    every subsequent execute() returns None from fetchone (no dup row).
    """
    engine = MagicMock(spec=Engine)
    conn = MagicMock()
    call_count = {"n": 0}

    def exec_side_effect(*args, **kwargs):  # noqa: ANN001
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            result.fetchone.return_value = (42,)  # _resolve_source_id
        else:
            result.fetchone.return_value = None  # row does not exist
        return result

    conn.execute.side_effect = exec_side_effect
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine._mock_conn = conn  # type: ignore[attr-defined]
    return engine


@pytest.fixture
def engine_row_present() -> MagicMock:
    """Engine whose _row_exists always returns True (every row a dup)."""
    engine = MagicMock(spec=Engine)
    conn = MagicMock()
    call_count = {"n": 0}

    def exec_side_effect(*args, **kwargs):  # noqa: ANN001
        call_count["n"] += 1
        result = MagicMock()
        if call_count["n"] == 1:
            result.fetchone.return_value = (42,)  # _resolve_source_id
        else:
            result.fetchone.return_value = (1,)  # duplicate
        return result

    conn.execute.side_effect = exec_side_effect
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine._mock_conn = conn  # type: ignore[attr-defined]
    return engine


# ---------------------------------------------------------------------------
# Constants sanity
# ---------------------------------------------------------------------------


class TestConstants:
    def test_chinese_ports_has_top_10(self) -> None:
        assert isinstance(CHINESE_PORTS_45, tuple)
        assert len(CHINESE_PORTS_45) == 10  # cold-start top-10 subset
        assert "Qingdao" in CHINESE_PORTS_45
        assert "Rizhao" in CHINESE_PORTS_45

    def test_series_namespaces(self) -> None:
        assert SERIES_PORT_STOCKS_AGG == "iron_ore:port_stocks_mt:aggregate"
        assert SERIES_THROUGHPUT_AGG == "iron_ore:daily_throughput_mt:aggregate"
        assert SERIES_DELTA_WOW_AGG == "iron_ore:stocks_delta_wow_mt:aggregate"
        assert SERIES_PORT_STOCKS_PREFIX == "iron_ore:port_stocks_mt:"

    def test_akshare_candidates_probe_order(self) -> None:
        # futures_inventory_em must come first — it's the most reliable.
        assert AKSHARE_FUNCTION_CANDIDATES[0] == "futures_inventory_em"
        assert "futures_inventory_99" in AKSHARE_FUNCTION_CANDIDATES

    def test_mysteel_url_is_public(self) -> None:
        assert MYSTEEL_URL.startswith("https://")
        assert "mysteel.com" in MYSTEEL_URL


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


class TestDataclass:
    def test_aggregate_snapshot_port_none(self) -> None:
        snap = IronOrePortSnapshot(
            date=date(2026, 4, 10),
            port=None,
            total_stocks_mt=135_000_000.0,
            daily_throughput_mt=2_750_000.0,
            delta_wow_mt=-500_000.0,
        )
        assert snap.port is None
        assert snap.total_stocks_mt == 135_000_000.0
        assert snap.daily_throughput_mt == 2_750_000.0

    def test_frozen(self) -> None:
        snap = IronOrePortSnapshot(
            date=date(2026, 4, 10),
            port="Qingdao",
            total_stocks_mt=12_000_000.0,
            daily_throughput_mt=None,
            delta_wow_mt=None,
        )
        with pytest.raises(Exception):
            snap.total_stocks_mt = 99.0  # type: ignore[misc]


# ---------------------------------------------------------------------------
# compute_wow_delta
# ---------------------------------------------------------------------------


class TestComputeWowDelta:
    def test_happy_path_positive(self) -> None:
        assert compute_wow_delta(135_500_000.0, 135_000_000.0) == 500_000.0

    def test_happy_path_negative(self) -> None:
        assert compute_wow_delta(134_500_000.0, 135_000_000.0) == -500_000.0

    def test_none_prior_returns_none(self) -> None:
        assert compute_wow_delta(135_000_000.0, None) is None

    def test_zero_prior_returns_none(self) -> None:
        # Zero prior is almost always a data gap — suppress the delta
        assert compute_wow_delta(135_000_000.0, 0.0) is None


# ---------------------------------------------------------------------------
# _load_akshare_function
# ---------------------------------------------------------------------------


class TestLoadAkshareFunction:
    def test_import_error_returns_none(self) -> None:
        real_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__

        def fake_import(name, *args, **kwargs):  # noqa: ANN001
            if name == "akshare":
                raise ImportError("akshare not installed")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            result = _load_akshare_function("futures_inventory_em")

        assert result is None

    def test_returns_callable_when_present(self) -> None:
        fake_ak = MagicMock()

        def fake_inventory() -> pd.DataFrame:
            return pd.DataFrame()

        fake_ak.futures_inventory_em = fake_inventory

        with patch.dict("sys.modules", {"akshare": fake_ak}):
            result = _load_akshare_function("futures_inventory_em")

        assert result is fake_inventory
        assert callable(result)

    def test_missing_attribute_returns_none(self) -> None:
        # Build a namespace object without the requested attribute.
        class BareAkshare:
            pass

        with patch.dict("sys.modules", {"akshare": BareAkshare()}):
            result = _load_akshare_function("nonexistent_function")

        assert result is None


# ---------------------------------------------------------------------------
# _parse_mysteel_html
# ---------------------------------------------------------------------------


_MYSTEEL_ENGLISH_HTML = """
<html><body>
<h2>Report Date: 2026-04-10</h2>
<table>
<tr><th>Port</th><th>Port Stocks</th><th>Daily Throughput</th></tr>
<tr><td>45-port Total</td><td>135,000,000</td><td>2,750,000</td></tr>
<tr><td>Qingdao</td><td>12,500,000</td><td>280,000</td></tr>
<tr><td>Rizhao</td><td>10,300,000</td><td>240,000</td></tr>
<tr><td>Tianjin</td><td>8,700,000</td><td>190,000</td></tr>
</table>
</body></html>
"""


_MYSTEEL_CHINESE_HTML = """
<html><body>
<p>统计日期: 2026年04月10日</p>
<table>
<tr><th>港口</th><th>港口库存</th><th>日均疏港量</th></tr>
<tr><td>45港合计</td><td>135000000</td><td>2750000</td></tr>
<tr><td>青岛</td><td>12500000</td><td>280000</td></tr>
</table>
</body></html>
"""


class TestParseMysteelHtml:
    def test_empty_html_returns_empty_list(self) -> None:
        assert _parse_mysteel_html("") == []
        assert _parse_mysteel_html("<html></html>") == []

    def test_english_aggregate_plus_three_ports(self) -> None:
        snaps = _parse_mysteel_html(_MYSTEEL_ENGLISH_HTML)
        assert len(snaps) == 4
        # First row is aggregate
        agg = snaps[0]
        assert agg.port is None
        assert agg.total_stocks_mt == 135_000_000.0
        assert agg.daily_throughput_mt == 2_750_000.0
        assert agg.date == date(2026, 4, 10)
        # Per-port rows
        ports = {s.port for s in snaps[1:]}
        assert ports == {"Qingdao", "Rizhao", "Tianjin"}
        qd = next(s for s in snaps if s.port == "Qingdao")
        assert qd.total_stocks_mt == 12_500_000.0
        assert qd.daily_throughput_mt == 280_000.0

    def test_chinese_headers_recognised(self) -> None:
        snaps = _parse_mysteel_html(_MYSTEEL_CHINESE_HTML)
        assert len(snaps) == 2
        agg = snaps[0]
        assert agg.port is None
        assert agg.total_stocks_mt == 135_000_000.0
        assert agg.daily_throughput_mt == 2_750_000.0
        assert agg.date == date(2026, 4, 10)
        per_port = snaps[1]
        assert per_port.port == "青岛"
        assert per_port.total_stocks_mt == 12_500_000.0

    def test_table_without_stocks_header_skipped(self) -> None:
        html = """
        <html><body>
        <table>
        <tr><th>Port</th><th>Unrelated</th></tr>
        <tr><td>Qingdao</td><td>foo</td></tr>
        </table>
        </body></html>
        """
        assert _parse_mysteel_html(html) == []


# ---------------------------------------------------------------------------
# run_iron_ore_ports_puller — akshare happy path
# ---------------------------------------------------------------------------


def _fake_iron_ore_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "品种": "铁矿石",
                "日期": "2026-04-10",
                "港口": "45港合计",
                "库存": 135_000_000.0,
                "日均疏港量": 2_750_000.0,
            },
            {
                "品种": "铁矿石",
                "日期": "2026-04-10",
                "港口": "青岛",
                "库存": 12_500_000.0,
                "日均疏港量": 280_000.0,
            },
        ]
    )


class TestAkshareHappyPath:
    def test_akshare_path_inserts_rows(
        self, engine_row_missing: MagicMock
    ) -> None:
        df = _fake_iron_ore_df()

        def fake_loader(name: str):  # noqa: ANN202
            if name == "futures_inventory_em":
                return lambda *a, **kw: df  # noqa: ARG005
            return None

        with patch.object(iop, "_load_akshare_function", side_effect=fake_loader):
            result = run_iron_ore_ports_puller(engine_row_missing)

        assert result["fetched"] == 2
        # Aggregate row → stock + throughput = 2 rows.
        # Per-port row → stock only = 1 row.
        # Total = 3
        assert result["inserted"] == 3
        assert result["source"] == "akshare:futures_inventory_em"


# ---------------------------------------------------------------------------
# run_iron_ore_ports_puller — akshare miss, HTML fallback
# ---------------------------------------------------------------------------


class TestMysteelFallback:
    def test_html_fallback_when_akshare_empty(
        self, engine_row_missing: MagicMock
    ) -> None:
        # akshare loader returns None for every candidate → fallback
        with patch.object(iop, "_load_akshare_function", return_value=None):
            with patch.object(
                iop, "_http_get", return_value=_MYSTEEL_ENGLISH_HTML
            ):
                result = run_iron_ore_ports_puller(engine_row_missing)

        assert result["fetched"] == 4
        assert result["source"] == "mysteel_html"
        # aggregate: stock + throughput = 2 rows
        # 3 per-port rows: stock only = 3 rows
        # total = 5
        assert result["inserted"] == 5


# ---------------------------------------------------------------------------
# run_iron_ore_ports_puller — total failure
# ---------------------------------------------------------------------------


class TestTotalFailure:
    def test_both_sources_fail_returns_zero(
        self, engine_row_missing: MagicMock
    ) -> None:
        with patch.object(iop, "_load_akshare_function", return_value=None):
            with patch.object(iop, "_http_get", return_value=None):
                result = run_iron_ore_ports_puller(engine_row_missing)

        assert result["fetched"] == 0
        assert result["inserted"] == 0
        assert result["source"] == "none"

    def test_akshare_callable_raises_then_html_fails(
        self, engine_row_missing: MagicMock
    ) -> None:
        def raiser(*args, **kwargs):  # noqa: ANN001, ARG001
            raise RuntimeError("DCE site down")

        def fake_loader(name: str):  # noqa: ANN202
            return raiser

        with patch.object(iop, "_load_akshare_function", side_effect=fake_loader):
            with patch.object(iop, "_http_get", return_value=None):
                result = run_iron_ore_ports_puller(engine_row_missing)

        assert result["fetched"] == 0
        assert result["inserted"] == 0
        assert result["source"] == "none"


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_rerun_does_not_duplicate(
        self, engine_row_present: MagicMock
    ) -> None:
        df = _fake_iron_ore_df()

        def fake_loader(name: str):  # noqa: ANN202
            if name == "futures_inventory_em":
                return lambda *a, **kw: df  # noqa: ARG005
            return None

        with patch.object(iop, "_load_akshare_function", side_effect=fake_loader):
            result = run_iron_ore_ports_puller(engine_row_present)

        # Every (series_id, obs_date) already exists → nothing inserted.
        assert result["fetched"] == 2
        assert result["inserted"] == 0
        assert result["source"] == "akshare:futures_inventory_em"
