"""Unit tests for ingestion/altdata/credit_card_spending.py (CAT-75)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.credit_card_spending import (
    CREDIT_CARD_SERIES,
    CreditCardSnapshot,
    CreditCardSpendingPuller,
    run_credit_card_puller,
)


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────


def _make_fred_response(
    series_id: str,
    rows: list[tuple[str, str]],
) -> dict:
    """Build a canned FRED observations JSON body."""
    return {
        "realtime_start": "2026-04-13",
        "realtime_end": "2026-04-13",
        "observation_start": "2008-01-01",
        "observation_end": "2026-04-13",
        "units": "lin",
        "count": len(rows),
        "observations": [
            {
                "realtime_start": "2026-04-13",
                "realtime_end": "2026-04-13",
                "date": d,
                "value": v,
            }
            for d, v in rows
        ],
    }


def _build_mock_engine() -> MagicMock:
    """Return a MagicMock engine that satisfies BasePuller._resolve_source_id."""
    engine = MagicMock()

    conn = MagicMock()

    # engine.connect() / engine.begin() both return a context manager over conn
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = ctx
    engine.begin.return_value = ctx

    # Row with source_id=42 for _resolve_source_id lookup
    row_mock = MagicMock()
    row_mock.__getitem__ = lambda self, idx: 42

    result_mock = MagicMock()
    result_mock.fetchone.return_value = row_mock
    result_mock.fetchall.return_value = []  # _get_existing_dates -> empty
    conn.execute.return_value = result_mock

    return engine


@pytest.fixture
def mock_engine() -> MagicMock:
    return _build_mock_engine()


# ──────────────────────────────────────────────────────────────────────────
# 1. Dataclass construction
# ──────────────────────────────────────────────────────────────────────────


class TestCreditCardSnapshot:
    def test_construct_full(self) -> None:
        snap = CreditCardSnapshot(
            date=date(2026, 1, 1),
            outstanding_usd=1050.5,
            delinq_pct=2.4,
            charge_off_pct=3.1,
            interest_rate_pct=21.8,
        )
        assert snap.date == date(2026, 1, 1)
        assert snap.outstanding_usd == 1050.5
        assert snap.delinq_pct == 2.4
        assert snap.charge_off_pct == 3.1
        assert snap.interest_rate_pct == 21.8

    def test_construct_partial(self) -> None:
        """Weekly outstanding observation should not require delinq fields."""
        snap = CreditCardSnapshot(date=date(2026, 2, 1), outstanding_usd=1100.0)
        assert snap.outstanding_usd == 1100.0
        assert snap.delinq_pct is None
        assert snap.charge_off_pct is None
        assert snap.interest_rate_pct is None


# ──────────────────────────────────────────────────────────────────────────
# 2. Series configuration
# ──────────────────────────────────────────────────────────────────────────


class TestSeriesConfig:
    def test_expected_series_present(self) -> None:
        expected = {
            "CCLACBW027SBOG",
            "CCLACBM027NBOG",
            "DRCCLACBS",
            "DRCCLACBN",
            "CORCCACBS",
            "TERMCBCCALLNS",
        }
        assert expected == set(CREDIT_CARD_SERIES.keys())

    def test_every_series_has_label(self) -> None:
        for sid, cfg in CREDIT_CARD_SERIES.items():
            assert "label" in cfg, f"{sid} missing label"
            assert "description" in cfg, f"{sid} missing description"


# ──────────────────────────────────────────────────────────────────────────
# 3. Happy path — pull + save
# ──────────────────────────────────────────────────────────────────────────


class TestRunPullerHappyPath:
    def _patch_requests(self) -> MagicMock:
        """Return a MagicMock replacement for requests.get that always succeeds."""
        def _fake_get(url, params=None, timeout=None):  # noqa: ARG001
            sid = params["series_id"]
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.json.return_value = _make_fred_response(
                sid,
                rows=[
                    ("2026-01-01", "1000.5"),
                    ("2026-02-01", "1025.75"),
                    ("2026-03-01", "."),  # missing — should be skipped
                ],
            )
            return resp

        return MagicMock(side_effect=_fake_get)

    def test_run_credit_card_puller_inserts_rows(
        self, mock_engine: MagicMock
    ) -> None:
        fake_get = self._patch_requests()

        with patch(
            "ingestion.altdata.credit_card_spending.requests.get", fake_get
        ), patch(
            "ingestion.altdata.credit_card_spending.time.sleep",
            MagicMock(),
        ), patch(
            "config.settings.FRED_API_KEY", "test-key-123", create=True
        ):
            result = run_credit_card_puller(mock_engine)

        # 6 series x 2 valid rows each = 12 total
        n_series = len(CREDIT_CARD_SERIES)
        assert result["status"] == "SUCCESS"
        assert result["fetched"] == n_series * 2
        assert result["inserted"] == n_series * 2
        assert set(result["series"].keys()) == {
            cfg["label"] for cfg in CREDIT_CARD_SERIES.values()
        }
        # fake_get called once per series
        assert fake_get.call_count == n_series


# ──────────────────────────────────────────────────────────────────────────
# 4. Missing API key — graceful degradation
# ──────────────────────────────────────────────────────────────────────────


class TestMissingApiKey:
    def test_run_with_empty_key_returns_zero_rows(
        self, mock_engine: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from config import settings as _settings

        monkeypatch.setattr(_settings, "FRED_API_KEY", "", raising=False)

        # Any call to requests.get would be a bug — detect via sentinel.
        sentinel = MagicMock(
            side_effect=AssertionError("requests.get should not be called")
        )
        with patch(
            "ingestion.altdata.credit_card_spending.requests.get", sentinel
        ):
            result = run_credit_card_puller(mock_engine)

        assert result["status"] == "SKIPPED_NO_API_KEY"
        assert result["fetched"] == 0
        assert result["inserted"] == 0
        assert set(result["series"].keys()) == {
            cfg["label"] for cfg in CREDIT_CARD_SERIES.values()
        }
        for label, counts in result["series"].items():
            assert counts == {"fetched": 0, "inserted": 0}, label
        sentinel.assert_not_called()

    def test_puller_pull_direct_with_empty_key(
        self, mock_engine: MagicMock
    ) -> None:
        """Calling pull() directly with empty key should return empty dict."""
        puller = CreditCardSpendingPuller(api_key="", db_engine=mock_engine)
        out = puller.pull()
        assert set(out.keys()) == {
            cfg["label"] for cfg in CREDIT_CARD_SERIES.values()
        }
        for rows in out.values():
            assert rows == []


# ──────────────────────────────────────────────────────────────────────────
# 5. Partial failure — one series down, others succeed
# ──────────────────────────────────────────────────────────────────────────


class TestPartialFailure:
    def test_one_series_failure_does_not_block_others(
        self, mock_engine: MagicMock
    ) -> None:
        """When one FRED series 500s, the other five should still succeed."""
        broken_sid = "DRCCLACBS"

        def _fake_get(url, params=None, timeout=None):  # noqa: ARG001
            sid = params["series_id"]
            resp = MagicMock()
            if sid == broken_sid:
                resp.raise_for_status.side_effect = RuntimeError(
                    "FRED 500: simulated outage"
                )
                resp.json.return_value = {}
                return resp
            resp.raise_for_status = MagicMock()
            resp.json.return_value = _make_fred_response(
                sid,
                rows=[
                    ("2026-01-01", "42.0"),
                    ("2026-02-01", "43.5"),
                ],
            )
            return resp

        with patch(
            "ingestion.altdata.credit_card_spending.requests.get",
            MagicMock(side_effect=_fake_get),
        ), patch(
            "ingestion.altdata.credit_card_spending.time.sleep",
            MagicMock(),
        ), patch(
            "config.settings.FRED_API_KEY", "test-key-abc", create=True
        ):
            result = run_credit_card_puller(mock_engine)

        assert result["status"] == "SUCCESS"

        broken_label = CREDIT_CARD_SERIES[broken_sid]["label"]
        assert result["series"][broken_label]["fetched"] == 0
        assert result["series"][broken_label]["inserted"] == 0

        # Every other series must have 2 inserted
        others = [
            cfg["label"]
            for sid, cfg in CREDIT_CARD_SERIES.items()
            if sid != broken_sid
        ]
        for label in others:
            assert result["series"][label]["fetched"] == 2
            assert result["series"][label]["inserted"] == 2


# ──────────────────────────────────────────────────────────────────────────
# 6. save_to_db stand-alone with zero rows
# ──────────────────────────────────────────────────────────────────────────


class TestSaveToDb:
    def test_save_empty_fetched(self, mock_engine: MagicMock) -> None:
        puller = CreditCardSpendingPuller(
            api_key="anything", db_engine=mock_engine
        )
        out = puller.save_to_db(
            {cfg["label"]: [] for cfg in CREDIT_CARD_SERIES.values()}
        )
        assert all(v == 0 for v in out.values())

    def test_save_inserts_new_rows(self, mock_engine: MagicMock) -> None:
        puller = CreditCardSpendingPuller(
            api_key="anything", db_engine=mock_engine
        )
        fake: dict[str, list[dict]] = {
            cfg["label"]: [] for cfg in CREDIT_CARD_SERIES.values()
        }
        fake["outstanding_all_banks_weekly"] = [
            {"date": date(2026, 1, 1), "value": 1000.0},
            {"date": date(2026, 1, 8), "value": 1010.0},
        ]
        out = puller.save_to_db(fake)
        assert out["outstanding_all_banks_weekly"] == 2
        for label, cfg in CREDIT_CARD_SERIES.items():
            if cfg["label"] != "outstanding_all_banks_weekly":
                assert out[cfg["label"]] == 0


# ──────────────────────────────────────────────────────────────────────────
# 7. _fetch_series parsing — malformed rows
# ──────────────────────────────────────────────────────────────────────────


class TestFetchSeriesParsing:
    def test_skips_dot_and_malformed(self, mock_engine: MagicMock) -> None:
        puller = CreditCardSpendingPuller(
            api_key="test-key", db_engine=mock_engine
        )

        body = _make_fred_response(
            "CCLACBW027SBOG",
            rows=[
                ("2026-01-01", "1000.5"),
                ("2026-01-08", "."),  # skipped
                ("not-a-date", "999"),  # skipped (date parse error)
                ("2026-01-15", "1020.75"),
            ],
        )
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = body

        with patch(
            "ingestion.altdata.credit_card_spending.requests.get",
            MagicMock(return_value=resp),
        ):
            obs = puller._fetch_series("CCLACBW027SBOG")

        assert len(obs) == 2
        assert obs[0]["date"] == date(2026, 1, 1)
        assert obs[0]["value"] == 1000.5
        assert obs[1]["date"] == date(2026, 1, 15)
        assert obs[1]["value"] == 1020.75
