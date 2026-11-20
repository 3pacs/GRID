"""Synthetic one-statement diagnostics, never research admission."""

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError
from analysis.options_capture_panel import load_capture_panel

DAY = date(2026, 11, 10)
END = datetime(2026, 11, 10, 22, 20, tzinfo=timezone.utc)
CUTOFF = END + timedelta(minutes=1)


def row():
    return dict(
        batch_id="batch-A",
        ticker="SPY",
        snap_date=DAY,
        ordinal=1,
        row_count=1,
        started_at=END - timedelta(seconds=20),
        completed_at=END,
        registered_at=END + timedelta(seconds=1),
        spot_price=750.0,
        spot_provider="source",
        capture_source="source",
        backfilled=False,
        spot_market_at=END - timedelta(hours=1),
        row_batch_id="batch-A",
        row_ordinal=1,
        row_ticker="SPY",
        row_snap_date=DAY,
        row_started_at=END - timedelta(seconds=20),
        row_completed_at=END,
        row_provider_market_at=END - timedelta(hours=1),
        row_created_at=END,
        expiry=DAY + timedelta(days=7),
        strike=750.0,
        opt_type="call",
        open_interest=100,
        implied_vol=0.2,
        last_price=10.0,
        bid=9.0,
        ask=11.0,
        volume=20,
    )


def run(rows, cutoff=CUTOFF):
    engine = MagicMock()
    execute = engine.connect.return_value.__enter__.return_value.execute
    execute.return_value.mappings.return_value.all.return_value = rows
    result = load_capture_panel(engine, "SPY", "batch-A", cutoff)
    return result, execute


def test_complete_capture_is_diagnostic_not_qualified():
    result, execute = run([row()])
    assert result["diagnostic_complete"] is True
    assert result["available"] is False and result["research_eligible"] is False
    assert result["status"] == "unavailable"
    assert result["capture"]["spot_price"] == 750.0
    assert result["capture"]["spot_market_at"] == row()["spot_market_at"]
    assert (
        result["contracts"][0]["row_provider_market_at"]
        == row()["row_provider_market_at"]
    )
    assert "native_option_quote_clocks_unavailable" in result["reasons"]
    assert "postcommit_visibility_unproven" in result["reasons"]
    assert "freshness_and_skew_policy_not_applied" in result["reasons"]
    execute.assert_called_once()
    query, params = execute.call_args.args
    assert params == {"ticker": "SPY", "batch_id": "batch-A"}
    assert "LEFT JOIN options_snapshots_all" in str(query)
    assert "options_daily_signals" not in str(query)
    assert "resolved_series" not in str(query)


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("row_count", 2, "incomplete_capture"),
        ("row_batch_id", None, "incomplete_capture"),
        ("row_batch_id", "batch-B", "mixed_contract_identity"),
        ("row_ordinal", 2, "mixed_contract_identity"),
        ("row_ticker", "QQQ", "mixed_contract_identity"),
        ("row_snap_date", DAY - timedelta(days=1), "mixed_contract_identity"),
        ("row_completed_at", END - timedelta(seconds=1), "mixed_contract_identity"),
        (
            "registered_at",
            CUTOFF + timedelta(microseconds=1),
            "invalid_or_future_source_clock",
        ),
        (
            "completed_at",
            CUTOFF + timedelta(seconds=1),
            "invalid_or_future_source_clock",
        ),
        ("started_at", END + timedelta(seconds=1), "invalid_or_future_source_clock"),
        (
            "spot_market_at",
            END + timedelta(seconds=1),
            "invalid_or_future_source_clock",
        ),
        ("registered_at", None, "missing_or_naive_source_clock"),
        ("registered_at", END.replace(tzinfo=None), "missing_or_naive_source_clock"),
        ("row_provider_market_at", None, "missing_or_naive_contract_clock"),
        ("row_created_at", CUTOFF + timedelta(seconds=1), "future_contract_clock"),
        ("row_provider_market_at", END + timedelta(seconds=1), "future_contract_clock"),
        ("spot_price", float("inf"), "invalid_registry_spot"),
        ("spot_price", True, "invalid_registry_spot"),
        ("spot_price", 0, "invalid_registry_spot"),
        ("spot_provider", None, "missing_source_identity"),
        ("strike", float("nan"), "invalid_contract_identity"),
        ("opt_type", "CALL", "invalid_contract_identity"),
        ("ordinal", True, "invalid_registry_counts"),
    ],
)
def test_invalid_capture_never_leaks_partial_panel(field, value, reason):
    record = row()
    record[field] = value
    result, _ = run([record])
    assert result["diagnostic_complete"] is False
    assert result["capture"] is None and result["contracts"] == []
    assert result["reasons"] == [reason]


def test_duplicates_fail_even_when_count_matches():
    record = row()
    record["row_count"] = 2
    result, _ = run([record, deepcopy(record)])
    assert result["reasons"] == ["duplicate_contract"]


def test_mixed_registry_rows_fail():
    record = row()
    other = deepcopy(record)
    other["spot_price"] = 751
    assert run([record, other])[0]["reasons"] == ["mixed_registry_metadata"]


def test_no_batch_does_not_fallback():
    result, execute = run([])
    assert result["reasons"] == ["capture_not_found"]
    execute.assert_called_once()


def test_naive_cutoff_does_not_query():
    result, execute = run([row()], CUTOFF.replace(tzinfo=None))
    assert result["reasons"] == ["timezone_aware_cutoff_required"]
    execute.assert_not_called()


def test_db_error_is_unavailable_without_retry():
    engine = MagicMock()
    engine.connect.side_effect = SQLAlchemyError("synthetic")
    result = load_capture_panel(engine, "SPY", "batch-A", CUTOFF)
    assert result["reasons"] == ["capture_read_failed"]
    engine.connect.assert_called_once()


def test_exact_cutoff_is_inclusive_but_not_commit_evidence():
    record = row()
    record["registered_at"] = CUTOFF
    result, _ = run([record])
    assert result["diagnostic_complete"]
    assert not result["research_eligible"]
