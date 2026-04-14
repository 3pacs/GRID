"""Unit tests for ingestion/altdata/pboc_omo.py (CAT-3)."""

from __future__ import annotations

import sys
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ingestion.altdata.pboc_omo import (
    MLFRenewal,
    PBOCOmoPuller,
    PBOCOmoSnapshot,
    SERIES_MLF_NET,
    SERIES_MLF_RATE,
    SERIES_OMO_INJECTION,
    SERIES_OMO_NET,
    SERIES_OMO_WITHDRAWAL,
    SERIES_REVERSE_REPO_7D,
    _parse_float,
    run_pboc_omo_puller,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_engine() -> MagicMock:
    """Return a mock SQLAlchemy engine.

    * ``_resolve_source_id`` → returns 42
    * ``_row_exists`` → returns False (nothing in DB yet)
    * ``.begin()`` and ``.connect()`` yield the same mock connection.
    """
    engine = MagicMock(spec=Engine)
    conn = MagicMock()

    fetchone_result = MagicMock()
    fetchone_result.fetchone.return_value = (42,)  # source_id for _resolve_source_id
    conn.execute.return_value = fetchone_result

    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine._mock_conn = conn  # type: ignore[attr-defined]
    return engine


@pytest.fixture
def engine_row_missing(mock_engine: MagicMock) -> MagicMock:
    """Engine whose ``_row_exists`` always returns False (source_id=42 first)."""
    conn = mock_engine._mock_conn
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
    return mock_engine


# ---------------------------------------------------------------------------
# Sample DataFrames
# ---------------------------------------------------------------------------


def _fake_omo_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"date": "2026-04-07", "投放": 100.0, "回笼": 30.0, "FR007": 1.85},
            {"date": "2026-04-08", "投放": 50.0, "回笼": 80.0, "FR007": 1.87},
            {"date": "2026-04-09", "投放": 200.0, "回笼": 0.0, "FR007": 1.84},
        ]
    )


def _fake_mlf_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"date": "2026-02-15", "到期": 500.0, "操作量": 600.0, "利率": 2.50},
            {"date": "2026-03-15", "到期": 600.0, "操作量": 550.0, "利率": 2.45},
        ]
    )


# ---------------------------------------------------------------------------
# Dataclass round-trip
# ---------------------------------------------------------------------------


class TestDataclasses:
    def test_omo_snapshot_roundtrip(self) -> None:
        snap = PBOCOmoSnapshot(
            date=date(2026, 4, 9),
            injection_cny_bn=200.0,
            withdrawal_cny_bn=0.0,
            net_cny_bn=200.0,
            reverse_repo_7d_rate=1.84,
        )
        assert snap.date == date(2026, 4, 9)
        assert snap.net_cny_bn == 200.0
        assert snap.reverse_repo_7d_rate == 1.84
        # frozen
        with pytest.raises(Exception):
            snap.net_cny_bn = 999.0  # type: ignore[misc]

    def test_mlf_renewal_roundtrip(self) -> None:
        r = MLFRenewal(
            date=date(2026, 3, 15),
            maturing_cny_bn=600.0,
            renewed_cny_bn=550.0,
            rate_pct=2.45,
            net_cny_bn=-50.0,
        )
        assert r.date == date(2026, 3, 15)
        assert r.rate_pct == 2.45
        assert r.net_cny_bn == -50.0
        with pytest.raises(Exception):
            r.rate_pct = 99.0  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


class TestParseFloat:
    def test_parses_plain_number(self) -> None:
        assert _parse_float("123.45") == 123.45
        assert _parse_float(100) == 100.0
        assert _parse_float(0.0) == 0.0

    def test_parses_with_commas_and_percent(self) -> None:
        assert _parse_float("1,234.56") == 1234.56
        assert _parse_float("2.50%") == 2.50

    def test_sentinels_return_none(self) -> None:
        for sentinel in ("—", "-", "--", "N/A", "", None, "nan"):
            assert _parse_float(sentinel) is None

    def test_nan_returns_none(self) -> None:
        assert _parse_float(float("nan")) is None

    def test_garbage_returns_none(self) -> None:
        assert _parse_float("not a number") is None


# ---------------------------------------------------------------------------
# Happy path — run_pboc_omo_puller end-to-end with mocked akshare
# ---------------------------------------------------------------------------


class TestRunPullerHappyPath:
    def test_happy_path_inserts_rows(self, engine_row_missing: MagicMock) -> None:
        fake_ak = MagicMock()
        fake_ak.macro_china_cb_operation.return_value = _fake_omo_df()
        fake_ak.macro_china_mlf_rate.return_value = _fake_mlf_df()

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            result = run_pboc_omo_puller(engine_row_missing)

        assert result["omo_rows"] == 3
        assert result["mlf_rows"] == 2
        # 3 OMO snapshots * (injection + withdrawal + net + rate) = 12 rows
        # 2 MLF renewals * (net + rate) = 4 rows
        # Total expected = 16
        assert result["inserted"] == 16
        # fetched counts cells attempted (3 * 4 + 2 * 2 = 16)
        assert result["fetched"] == 16

    def test_net_omo_is_injection_minus_withdrawal(
        self, engine_row_missing: MagicMock
    ) -> None:
        fake_ak = MagicMock()
        fake_ak.macro_china_cb_operation.return_value = _fake_omo_df()
        fake_ak.macro_china_mlf_rate.return_value = pd.DataFrame()

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            puller = PBOCOmoPuller(engine_row_missing)
            puller.pull()

        # row 1: 100 - 30 = 70
        # row 2: 50 - 80 = -30
        # row 3: 200 - 0 = 200
        nets = [s.net_cny_bn for s in puller._omo_snapshots]
        assert nets == [70.0, -30.0, 200.0]


# ---------------------------------------------------------------------------
# Graceful degradation
# ---------------------------------------------------------------------------


class TestMissingAkshare:
    def test_importerror_zero_rows(self, engine_row_missing: MagicMock) -> None:
        """If akshare cannot be imported, puller returns zero rows and does not crash."""
        # Force the local ``import akshare`` inside the puller to raise ImportError
        # by injecting a sentinel that raises on attribute access. Simpler: use a
        # meta_path finder / or patch builtins.__import__.
        real_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__

        def fake_import(name, *args, **kwargs):  # noqa: ANN001
            if name == "akshare":
                raise ImportError("akshare not installed")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            result = run_pboc_omo_puller(engine_row_missing)

        assert result["omo_rows"] == 0
        assert result["mlf_rows"] == 0
        assert result["inserted"] == 0


class TestAkshareRuntimeError:
    def test_runtime_error_zero_rows(self, engine_row_missing: MagicMock) -> None:
        """If every akshare call raises, return zero rows without crashing."""
        fake_ak = MagicMock()
        fake_ak.macro_china_cb_operation.side_effect = RuntimeError("PBoC site down")
        fake_ak.repo_rate_hist.side_effect = RuntimeError("mirror down too")
        fake_ak.macro_china_mlf_rate.side_effect = RuntimeError("mlf down")
        fake_ak.macro_china_lpr.side_effect = RuntimeError("lpr down")

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            result = run_pboc_omo_puller(engine_row_missing)

        assert result["omo_rows"] == 0
        assert result["mlf_rows"] == 0
        assert result["inserted"] == 0


class TestEmptyDataFrame:
    def test_empty_dfs_zero_rows(self, engine_row_missing: MagicMock) -> None:
        fake_ak = MagicMock()
        fake_ak.macro_china_cb_operation.return_value = pd.DataFrame()
        fake_ak.repo_rate_hist.return_value = pd.DataFrame()
        fake_ak.macro_china_mlf_rate.return_value = pd.DataFrame()
        fake_ak.macro_china_lpr.return_value = pd.DataFrame()

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            result = run_pboc_omo_puller(engine_row_missing)

        assert result["omo_rows"] == 0
        assert result["mlf_rows"] == 0
        assert result["inserted"] == 0


# ---------------------------------------------------------------------------
# Idempotency (re-run on same date)
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_rerun_does_not_duplicate(self, mock_engine: MagicMock) -> None:
        """When ``_row_exists`` returns True, save_to_db must skip the row."""
        conn = mock_engine._mock_conn

        calls = {"n": 0}

        def exec_side_effect(*args, **kwargs):  # noqa: ANN001
            calls["n"] += 1
            result = MagicMock()
            if calls["n"] == 1:
                # first call: _resolve_source_id
                result.fetchone.return_value = (42,)
            else:
                # subsequent SELECTs from _row_exists return a row → duplicate
                result.fetchone.return_value = (1,)
            return result

        conn.execute.side_effect = exec_side_effect

        fake_ak = MagicMock()
        fake_ak.macro_china_cb_operation.return_value = _fake_omo_df()
        fake_ak.macro_china_mlf_rate.return_value = _fake_mlf_df()

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            result = run_pboc_omo_puller(mock_engine)

        # All rows already exist → nothing inserted
        assert result["omo_rows"] == 3
        assert result["mlf_rows"] == 2
        assert result["inserted"] == 0


# ---------------------------------------------------------------------------
# Sentinel handling in data frames
# ---------------------------------------------------------------------------


class TestSentinelHandling:
    def test_dash_and_na_become_zero_or_none(
        self, engine_row_missing: MagicMock
    ) -> None:
        """A row with "—" and "N/A" cells must still be ingested, with
        injection/withdrawal coerced to 0 and rate to None (skipped)."""
        dirty_df = pd.DataFrame(
            [
                {"date": "2026-04-07", "投放": "—", "回笼": "N/A", "FR007": "—"},
                {"date": "2026-04-08", "投放": "100", "回笼": "—", "FR007": "1.85"},
            ]
        )
        fake_ak = MagicMock()
        fake_ak.macro_china_cb_operation.return_value = dirty_df
        fake_ak.macro_china_mlf_rate.return_value = pd.DataFrame()

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            puller = PBOCOmoPuller(engine_row_missing)
            puller.pull()

        assert len(puller._omo_snapshots) == 2
        first = puller._omo_snapshots[0]
        assert first.injection_cny_bn == 0.0
        assert first.withdrawal_cny_bn == 0.0
        assert first.net_cny_bn == 0.0
        assert first.reverse_repo_7d_rate is None

        second = puller._omo_snapshots[1]
        assert second.injection_cny_bn == 100.0
        assert second.withdrawal_cny_bn == 0.0
        assert second.net_cny_bn == 100.0
        assert second.reverse_repo_7d_rate == 1.85


# ---------------------------------------------------------------------------
# Fallback path — macro_china_cb_operation missing, repo_rate_hist used
# ---------------------------------------------------------------------------


class TestFallbackRepoRate:
    def test_uses_repo_rate_hist_when_cb_operation_missing(
        self, engine_row_missing: MagicMock
    ) -> None:
        """When ``macro_china_cb_operation`` does not exist on the akshare
        module, the puller must fall back to ``repo_rate_hist``."""
        fr_df = pd.DataFrame(
            [
                {"date": "2026-04-07", "FR007": 1.85},
                {"date": "2026-04-08", "FR007": 1.87},
            ]
        )

        class FakeAkshare:
            """Only exposes repo_rate_hist / macro_china_lpr."""

            repo_rate_hist = staticmethod(lambda: fr_df)
            macro_china_lpr = staticmethod(lambda: pd.DataFrame())

        fake_ak = FakeAkshare()

        with patch.dict(sys.modules, {"akshare": fake_ak}):
            puller = PBOCOmoPuller(engine_row_missing)
            puller.pull()

        assert len(puller._omo_snapshots) == 2
        # injection / withdrawal default to 0 when the fallback schema only
        # carries a rate column
        assert all(s.injection_cny_bn == 0.0 for s in puller._omo_snapshots)
        assert puller._omo_snapshots[0].reverse_repo_7d_rate == 1.85
        assert puller._omo_snapshots[1].reverse_repo_7d_rate == 1.87


# ---------------------------------------------------------------------------
# Series namespace verification
# ---------------------------------------------------------------------------


class TestSeriesNamespaces:
    def test_all_namespaces_present(self) -> None:
        """Contract check: the six pboc:* namespaces required by CAT-3."""
        assert SERIES_OMO_INJECTION == "pboc:omo_injection_cny_bn"
        assert SERIES_OMO_WITHDRAWAL == "pboc:omo_withdrawal_cny_bn"
        assert SERIES_OMO_NET == "pboc:omo_net_cny_bn"
        assert SERIES_REVERSE_REPO_7D == "pboc:reverse_repo_7d_rate"
        assert SERIES_MLF_RATE == "pboc:mlf_rate"
        assert SERIES_MLF_NET == "pboc:mlf_net_cny_bn"
