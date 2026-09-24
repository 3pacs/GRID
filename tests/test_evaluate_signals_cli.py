"""Typed DATE/TIMESTAMPTZ selection, strict CLI scope, and dry-run tests."""
import io
import sys
import types
from datetime import date, datetime, timezone

import pytest

from scripts.evaluate_signals import RefusalError, _build_engine, build_arg_parser, run, select_signal_sources


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
    engine = Engine("date", [(1, "news", "AAA", date(2026, 9, 1), "BUY", datetime(2026, 9, 1, 20, tzinfo=timezone.utc))])
    rows = select(engine)
    sql, params = engine.calls[1]
    assert "signal_date <= :date_to" in sql
    assert params["date_from"] == date(2026, 9, 1)
    assert rows[0].signal_date == date(2026, 9, 1)


def test_timestamptz_uses_utc_instants_for_market_day():
    engine = Engine("timestamp with time zone", [(1, "news", "AAA", datetime(2026, 9, 2, 3, tzinfo=timezone.utc), "BUY", datetime(2026, 9, 2, 3, tzinfo=timezone.utc))])
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
        select(Engine("timestamp with time zone", [(1, "news", "AAA", datetime(2026, 9, 1, 12), "BUY", datetime(2026, 9, 1, 12, tzinfo=timezone.utc))]))
    with pytest.raises(RefusalError, match="created_at"):
        select(Engine("date", [(1, "news", "AAA", date(2026, 9, 1), "BUY", datetime(2026, 9, 1, 12))]))


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
    engine = Engine("date", [(1, "news", "AAA", date(2026, 9, 1), "BUY", datetime(2026, 9, 1, 18, tzinfo=timezone.utc))])
    result = run(engine, args, out=io.StringIO(), today=date(2026, 9, 8))
    assert result["cohort_summary"]["n_ineligible_by_reason"] == {"unsupported_instrument_history": 1}
    assert result["cohort_summary"]["origin_tag_counts"] == {"unknown": 1}
    assert len(engine.calls) == 2  # schema introspection and source SELECT only


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
