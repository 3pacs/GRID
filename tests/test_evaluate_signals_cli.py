"""Typed DATE/TIMESTAMPTZ selection, strict CLI scope, and dry-run tests."""
import io
import sys
import types
from datetime import date, datetime, timezone

import pytest

from scripts.evaluate_signals import (
    RefusalError, SelectedRow, _build_engine, _resolve_known_at, build_arg_parser, run,
    select_signal_sources, to_signal_record,
)


class Result:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value

    def fetchall(self):
        return self.value


class Engine:
    def __init__(self, dtype, rows):
        self.dtype = dtype
        self.rows = rows
        self.calls = []

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params=None):
        self.calls.append((str(query), params))
        if "information_schema" in str(query):
            return Result(self.dtype)
        return Result(self.rows)


def select(engine):
    return select_signal_sources(engine, source_type="news", date_from=date(2026, 9, 1),
                                 date_to=date(2026, 9, 2), limit=10)


def test_date_column_uses_date_bounds_and_native_date():
    engine = Engine("date", [(1, "news", "AAA", date(2026, 9, 1), "BUY", datetime(2026, 9, 1, 20, tzinfo=timezone.utc), None)])
    rows = select(engine)
    sql, params = engine.calls[1]
    assert "signal_date <= :date_to" in sql
    assert "signal_value" in sql
    assert params["date_from"] == date(2026, 9, 1)
    assert rows[0].signal_date == date(2026, 9, 1)


def test_timestamptz_uses_utc_instants_for_market_day():
    engine = Engine("timestamp with time zone", [(1, "news", "AAA", datetime(2026, 9, 2, 3, tzinfo=timezone.utc), "BUY", datetime(2026, 9, 2, 3, tzinfo=timezone.utc), None)])
    rows = select(engine)
    sql, params = engine.calls[1]
    assert "signal_date < :date_to_exclusive" in sql
    assert params["date_from"] == datetime(2026, 9, 1, 4, tzinfo=timezone.utc)
    assert rows[0].signal_date == date(2026, 9, 1)


@pytest.mark.parametrize("dtype", [None, "timestamp without time zone", "text"])
def test_unknown_schema_type_refused(dtype):
    with pytest.raises(RefusalError, match="unverified"):
        select(Engine(dtype, []))


def test_naive_timestamp_and_created_at_refused():
    with pytest.raises(RefusalError, match="naive signal_date"):
        select(Engine("timestamp with time zone", [(1, "news", "AAA", datetime(2026, 9, 1, 12), "BUY", datetime(2026, 9, 1, 12, tzinfo=timezone.utc), None)]))
    with pytest.raises(RefusalError, match="created_at"):
        select(Engine("date", [(1, "news", "AAA", date(2026, 9, 1), "BUY", datetime(2026, 9, 1, 12), None)]))


def test_parser_has_no_persist_option():
    parser = build_arg_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--persist"])


@pytest.mark.parametrize("flag,value", [
    ("--dead-band-pct", "nan"), ("--dead-band-pct", "inf"),
    ("--dead-band-pct", "-1"), ("--dead-band-pct", "101"),
    ("--cost-bps", "nan"), ("--cost-bps", "-1"),
])
def test_parser_refuses_invalid_scoring_numbers(flag, value):
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args(["--db-url", "postgresql://test", "--source-type", "news",
                                       "--date-from", "2026-09-01", "--date-to", "2026-09-02",
                                       flag, value])


def test_cli_cannot_assert_live_origin_without_row_evidence():
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args(["--db-url", "postgresql://test", "--source-type", "news",
                                       "--date-from", "2026-09-01", "--date-to", "2026-09-02",
                                       "--origin-tag", "live"])


def test_empty_read_only_run_prints_provisional_version():
    parser = build_arg_parser()
    args = parser.parse_args(["--db-url", "postgresql://test", "--source-type", "news",
                              "--date-from", "2026-09-01", "--date-to", "2026-09-02"])
    out = io.StringIO()
    result = run(Engine("date", []), args, out=out, today=date(2026, 9, 2))
    assert result["dry_run"] is True
    assert "provisional" in out.getvalue()
    assert "persisted_count" not in result


def test_cli_refuses_unverified_raw_basis_without_price_query():
    args = build_arg_parser().parse_args(["--db-url", "postgresql://test", "--source-type", "news",
                                           "--date-from", "2026-09-01", "--date-to", "2026-09-02"])
    engine = Engine("date", [(1, "news", "AAA", date(2026, 9, 1), "BUY", datetime(2026, 9, 1, 18, tzinfo=timezone.utc), None)])
    result = run(engine, args, out=io.StringIO(), today=date(2026, 9, 8))
    # The CLI never supplies a cutover, so the accessor refuses before any
    # query -- the reason must say so distinctly (B1), not the generic
    # "unsupported instrument" reason (that ticker/instrument is fine).
    assert result["cohort_summary"]["n_ineligible_by_reason"] == {"price_basis_cutover_unverified": 1}
    assert result["cohort_summary"]["origin_tag_counts"] == {"unknown": 1}
    assert result["known_at_source_counts"] == {"created_at_fallback_unverified_source_type": 1}
    assert len(engine.calls) == 2  # schema introspection and source SELECT only


def _selected_row(*, source_type="congressional", signal_value=None, created_at=None):
    return SelectedRow(
        id=1, source_type=source_type, ticker="AAA", signal_date=date(2026, 9, 1),
        signal_type="BUY", created_at=created_at or datetime(2026, 9, 5, 12, tzinfo=timezone.utc),
        signal_value=signal_value,
    )


def test_congressional_disclosure_date_populates_known_at():
    row = _selected_row(signal_value={"disclosure_date": "2026-09-02", "disclosure_lag_days": 30})
    known_at, source = _resolve_known_at(row)
    assert source == "congressional_disclosure_date"
    # time.max in MARKET_TZ on the disclosure date -> after-16:00 rollover
    # applies, matching evaluate_signal()'s own conservative "known at day
    # end" handling rather than assuming an earlier intraday time.
    assert known_at.date() == date(2026, 9, 2)
    assert known_at.tzinfo is not None

    record = to_signal_record(row, horizon_days=5)
    assert record.known_at == known_at
    assert record.metadata["known_at_source"] == "congressional_disclosure_date"


def test_congressional_without_disclosure_date_falls_back_explicitly():
    row = _selected_row(signal_value=None)
    known_at, source = _resolve_known_at(row)
    assert known_at is None
    assert source == "created_at_fallback_missing_disclosure_date"


def test_congressional_unparseable_disclosure_date_falls_back_explicitly():
    row = _selected_row(signal_value={"disclosure_date": "not-a-date"})
    known_at, source = _resolve_known_at(row)
    assert known_at is None
    assert source == "created_at_fallback_unparseable_disclosure_date"


def test_non_congressional_source_type_falls_back_explicitly_even_with_signal_value():
    row = _selected_row(source_type="news", signal_value={"disclosure_date": "2026-09-02"})
    known_at, source = _resolve_known_at(row)
    assert known_at is None
    assert source == "created_at_fallback_unverified_source_type"


def test_cli_output_known_at_source_counts_reflect_congressional_disclosure_date():
    args = build_arg_parser().parse_args(["--db-url", "postgresql://test", "--source-type", "congressional",
                                           "--date-from", "2026-09-01", "--date-to", "2026-09-02"])
    engine = Engine("date", [(1, "congressional", "AAA", date(2026, 9, 1), "BUY",
                              datetime(2026, 9, 1, 18, tzinfo=timezone.utc),
                              {"disclosure_date": "2026-09-01"})])
    result = run(engine, args, out=io.StringIO(), today=date(2026, 9, 8))
    assert result["known_at_source_counts"] == {"congressional_disclosure_date": 1}
    # Truthfulness check (B2): the assumptions string must not claim the
    # ambiguity check ran unconditionally -- it must say the check is gated
    # on a cutover this CLI never supplies.
    assert "cutover" in result["assumptions"]
    assert "price_basis_cutover_unverified" in result["assumptions"]


def test_bounds_required_and_limited():
    with pytest.raises(RefusalError):
        select_signal_sources(Engine("date", []), source_type="news", date_from=date(2026, 9, 1), date_to=date(2026, 9, 2), limit=1001)


def test_engine_requests_read_only_session_without_connecting(monkeypatch):
    calls = []
    monkeypatch.setitem(sys.modules, "sqlalchemy", types.SimpleNamespace(
        create_engine=lambda *a, **kw: calls.append((a, kw))))
    _build_engine("postgresql://placeholder")
    assert "default_transaction_read_only=on" in calls[0][1]["connect_args"]["options"]
    with pytest.raises(RefusalError):
        _build_engine("sqlite:///:memory:")
