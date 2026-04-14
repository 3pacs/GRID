"""Unit tests for ingestion/altdata/buyback_execution.py (CAT-67).

Covers:
    - BuybackSnapshot dataclass ratio computation (happy path + zero-profit).
    - run_buyback_puller top-level entrypoint with a mock engine.
    - Composite execution_ratio materialization (only where both inputs
      exist for the same period).
    - Missing API key → zero insertions, no crash.
    - Partial FRED failure (one series returns HTTP 500) → other series
      still insert.
    - Malformed FRED "." sentinel → coerced/skipped, no crash.
    - Re-run idempotency (existing dates cause zero new inserts).
    - Empty FRED response → zero insertions, no crash.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

from ingestion.altdata.buyback_execution import (
    BUYBACK_SERIES,
    EXECUTION_RATIO_LABEL,
    BuybackExecutionPuller,
    BuybackSnapshot,
    run_buyback_puller,
)


# ---------------------------------------------------------------------------
# Mock engine helper
# ---------------------------------------------------------------------------


def _mock_engine(
    existing_dates: set[date] | None = None,
    source_id: int = 99,
) -> tuple[MagicMock, MagicMock, list]:
    """Build a mock SQLAlchemy engine that supports the puller flow.

    Returns ``(engine, conn, inserts)`` where ``inserts`` is a list that
    captures every INSERT statement the puller executes. Deduplication
    queries (``_get_existing_dates``) return ``existing_dates`` (defaults
    to empty). The ``source_catalog`` lookup returns ``(source_id,)``.

    Parameters:
        existing_dates: Dates to return from ``_get_existing_dates``.
        source_id: ID to return from the ``source_catalog`` lookup.
    """
    existing = existing_dates or set()
    engine = MagicMock()
    conn = MagicMock()

    inserts: list[dict[str, Any]] = []

    def _execute(stmt, params=None):  # noqa: ANN001
        sql = str(stmt).strip().upper()
        result = MagicMock()
        if sql.startswith("SELECT ID FROM SOURCE_CATALOG"):
            result.fetchone.return_value = (source_id,)
            result.fetchall.return_value = []
            return result
        if "DISTINCT OBS_DATE" in sql:
            result.fetchall.return_value = [(d,) for d in existing]
            result.fetchone.return_value = None
            return result
        if sql.startswith("INSERT INTO RAW_SERIES"):
            inserts.append(dict(params or {}))
            result.fetchone.return_value = None
            result.fetchall.return_value = []
            return result
        result.fetchone.return_value = None
        result.fetchall.return_value = []
        return result

    conn.execute.side_effect = _execute

    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn, inserts


def _fred_payload(values: list[tuple[str, str]]) -> dict[str, Any]:
    """Build a minimal FRED observations payload."""
    return {
        "observations": [
            {"date": d, "value": v} for d, v in values
        ]
    }


def _mock_response(payload: dict[str, Any], status_code: int = 200) -> MagicMock:
    """Build a mock ``requests.Response``."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(
            f"{status_code} error"
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# BuybackSnapshot dataclass
# ---------------------------------------------------------------------------


class TestBuybackSnapshot:
    """Dataclass-level ratio math."""

    def test_ratio_computed_when_both_fields_present(self) -> None:
        snap = BuybackSnapshot(
            period_end=date(2025, 3, 31),
            net_repurchases_usd=750.0,
            profits_after_tax_usd=1000.0,
        )
        assert snap.buyback_ratio == pytest.approx(0.75)

    def test_ratio_none_when_profits_zero(self) -> None:
        snap = BuybackSnapshot(
            period_end=date(2025, 3, 31),
            net_repurchases_usd=500.0,
            profits_after_tax_usd=0.0,
        )
        assert snap.buyback_ratio is None

    def test_explicit_ratio_is_preserved(self) -> None:
        """Caller-supplied ratio should not be overwritten by __post_init__."""
        snap = BuybackSnapshot(
            period_end=date(2025, 3, 31),
            net_repurchases_usd=100.0,
            profits_after_tax_usd=200.0,
            buyback_ratio=0.42,
        )
        assert snap.buyback_ratio == 0.42


# ---------------------------------------------------------------------------
# run_buyback_puller — happy path, missing key, empty, partial, idempotent
# ---------------------------------------------------------------------------


class TestRunBuybackPuller:
    """End-to-end tests for the top-level entrypoint."""

    def _patch_fred(
        self, responses: dict[str, MagicMock]
    ) -> Any:
        """Return a ``requests.get`` side effect keyed by FRED series_id."""

        def _side_effect(url, params=None, timeout=None):  # noqa: ANN001
            sid = (params or {}).get("series_id")
            if sid in responses:
                return responses[sid]
            return _mock_response({"observations": []})

        return _side_effect

    def test_happy_path_inserts_all_series_plus_composite(self) -> None:
        """All 4 series + execution_ratio are materialized."""
        engine, _, inserts = _mock_engine()

        responses = {
            "NCBBCCB1Q027S": _mock_response(
                _fred_payload(
                    [
                        ("2024-09-30", "800000.0"),
                        ("2024-12-31", "900000.0"),
                    ]
                )
            ),
            "CPATAX": _mock_response(
                _fred_payload(
                    [
                        ("2024-09-30", "1200.0"),
                        ("2024-12-31", "1500.0"),
                    ]
                )
            ),
            "BOGZ1FU104122005Q": _mock_response(
                _fred_payload([("2024-12-31", "123.0")])
            ),
            "NCBEIAQ027S": _mock_response(
                _fred_payload([("2024-12-31", "456.0")])
            ),
        }

        with patch(
            "ingestion.altdata.buyback_execution.requests.get",
            side_effect=self._patch_fred(responses),
        ):
            result = run_buyback_puller(engine, api_key="fake-key")

        # 2 net_rep + 2 profits + 1 net_eq + 1 capex = 6 raw
        # + 2 composite ratios = 8 inserts
        assert result["fetched"] == 8
        assert result["inserted"] == 8
        assert len(inserts) == 8

        series_ids = {ins["sid"] for ins in inserts}
        assert "buybacks:net_repurchases" in series_ids
        assert "buybacks:profits_after_tax" in series_ids
        assert "buybacks:net_equity_purchases" in series_ids
        assert "buybacks:capex" in series_ids
        assert "buybacks:execution_ratio" in series_ids

        # Verify the composite value is 800000/1200 for the Q3 row.
        ratio_rows = [
            ins
            for ins in inserts
            if ins["sid"] == "buybacks:execution_ratio"
        ]
        assert len(ratio_rows) == 2
        q3 = [r for r in ratio_rows if r["od"] == date(2024, 9, 30)][0]
        assert q3["val"] == pytest.approx(800000.0 / 1200.0)

    def test_composite_only_where_both_inputs_exist(self) -> None:
        """execution_ratio is only written for periods with both inputs."""
        engine, _, inserts = _mock_engine()

        # Net repurchases has Q3 + Q4. Profits has only Q4. Composite
        # should therefore appear only for Q4.
        responses = {
            "NCBBCCB1Q027S": _mock_response(
                _fred_payload(
                    [
                        ("2024-09-30", "800000.0"),
                        ("2024-12-31", "900000.0"),
                    ]
                )
            ),
            "CPATAX": _mock_response(
                _fred_payload([("2024-12-31", "1500.0")])
            ),
        }

        with patch(
            "ingestion.altdata.buyback_execution.requests.get",
            side_effect=self._patch_fred(responses),
        ):
            result = run_buyback_puller(engine, api_key="fake-key")

        ratio_inserts = [
            ins for ins in inserts if ins["sid"] == "buybacks:execution_ratio"
        ]
        assert len(ratio_inserts) == 1
        assert ratio_inserts[0]["od"] == date(2024, 12, 31)
        assert result["series"]["execution_ratio"]["inserted"] == 1

    def test_missing_api_key_zero_insertions_no_crash(self) -> None:
        """Empty api_key must return a zero-row result without raising."""
        engine, _, inserts = _mock_engine()

        with patch(
            "ingestion.altdata.buyback_execution.requests.get"
        ) as mock_get:
            result = run_buyback_puller(engine, api_key="")
            mock_get.assert_not_called()

        assert result["fetched"] == 0
        assert result["inserted"] == 0
        assert inserts == []
        # Every configured series must be present in the summary.
        for label in BUYBACK_SERIES:
            assert label in result["series"]
        assert EXECUTION_RATIO_LABEL in result["series"]

    def test_partial_fred_failure_other_series_still_insert(self) -> None:
        """One FRED series returning HTTP 500 must not block the others."""
        engine, _, inserts = _mock_engine()

        # CPATAX fails, the other three succeed.
        responses = {
            "NCBBCCB1Q027S": _mock_response(
                _fred_payload([("2024-12-31", "900000.0")])
            ),
            "CPATAX": _mock_response({}, status_code=500),
            "BOGZ1FU104122005Q": _mock_response(
                _fred_payload([("2024-12-31", "100.0")])
            ),
            "NCBEIAQ027S": _mock_response(
                _fred_payload([("2024-12-31", "200.0")])
            ),
        }

        with patch(
            "ingestion.altdata.buyback_execution.requests.get",
            side_effect=self._patch_fred(responses),
        ):
            result = run_buyback_puller(engine, api_key="fake-key")

        # 3 successful raw series inserted, profits empty → no composite
        assert result["series"]["net_repurchases"]["inserted"] == 1
        assert result["series"]["profits_after_tax"]["inserted"] == 0
        assert result["series"]["net_equity_purchases"]["inserted"] == 1
        assert result["series"]["capex"]["inserted"] == 1
        assert result["series"]["execution_ratio"]["inserted"] == 0
        assert result["inserted"] == 3
        assert len(inserts) == 3

    def test_malformed_sentinel_value_is_skipped(self) -> None:
        """FRED "." missing-value rows are coerced/skipped, not crashed on."""
        engine, _, inserts = _mock_engine()

        responses = {
            "NCBBCCB1Q027S": _mock_response(
                _fred_payload(
                    [
                        ("2024-09-30", "."),
                        ("2024-12-31", "900000.0"),
                    ]
                )
            ),
            "CPATAX": _mock_response(
                _fred_payload(
                    [
                        ("2024-09-30", "1200.0"),
                        ("2024-12-31", "."),
                    ]
                )
            ),
        }

        with patch(
            "ingestion.altdata.buyback_execution.requests.get",
            side_effect=self._patch_fred(responses),
        ):
            result = run_buyback_puller(engine, api_key="fake-key")

        # net_repurchases: 1 row (Q3 skipped as ".")
        # profits_after_tax: 1 row (Q4 skipped as ".")
        # composite: 0 rows (Q3 has profit but no net_rep, Q4 has net_rep
        # but no profit → no overlap)
        assert result["series"]["net_repurchases"]["inserted"] == 1
        assert result["series"]["profits_after_tax"]["inserted"] == 1
        assert result["series"]["execution_ratio"]["inserted"] == 0
        assert len(inserts) == 2

    def test_rerun_idempotency_no_duplicate_inserts(self) -> None:
        """Re-running over already-existing dates inserts nothing."""
        # Pretend all Q4 2024 rows are already in raw_series.
        existing = {date(2024, 12, 31)}
        engine, _, inserts = _mock_engine(existing_dates=existing)

        responses = {
            "NCBBCCB1Q027S": _mock_response(
                _fred_payload([("2024-12-31", "900000.0")])
            ),
            "CPATAX": _mock_response(
                _fred_payload([("2024-12-31", "1500.0")])
            ),
            "BOGZ1FU104122005Q": _mock_response(
                _fred_payload([("2024-12-31", "123.0")])
            ),
            "NCBEIAQ027S": _mock_response(
                _fred_payload([("2024-12-31", "456.0")])
            ),
        }

        with patch(
            "ingestion.altdata.buyback_execution.requests.get",
            side_effect=self._patch_fred(responses),
        ):
            result = run_buyback_puller(engine, api_key="fake-key")

        assert result["inserted"] == 0
        assert inserts == []
        # ``fetched`` still reports what FRED returned + candidate composites.
        assert result["fetched"] >= 4

    def test_empty_fred_response_zero_insertions(self) -> None:
        """Empty observations list produces zero inserts, no crash."""
        engine, _, inserts = _mock_engine()

        empty = _mock_response({"observations": []})
        with patch(
            "ingestion.altdata.buyback_execution.requests.get",
            return_value=empty,
        ):
            result = run_buyback_puller(engine, api_key="fake-key")

        assert result["fetched"] == 0
        assert result["inserted"] == 0
        assert inserts == []


# ---------------------------------------------------------------------------
# Direct puller smoke test (ensures class wiring works even without the
# run_* entrypoint path).
# ---------------------------------------------------------------------------


class TestBuybackExecutionPuller:
    def test_source_name_attribute(self) -> None:
        engine, _, _ = _mock_engine()
        puller = BuybackExecutionPuller(api_key="fake-key", db_engine=engine)
        assert puller.source_name == "buyback_execution"
        assert puller.SOURCE_NAME == "buyback_execution"

    def test_pull_returns_empty_dict_without_api_key(self) -> None:
        engine, _, _ = _mock_engine()
        puller = BuybackExecutionPuller(api_key="", db_engine=engine)
        fetched = puller.pull()
        assert set(fetched.keys()) == set(BUYBACK_SERIES.keys())
        assert all(v == [] for v in fetched.values())
