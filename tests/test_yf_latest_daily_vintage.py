"""Unit contract for the yf_latest_daily_vintage materialized view and its refresher.

No database: checks the SQL the migration generates (every YF_TICKER_LIST
ticker x field exactly once, per-series LATERAL probe, latest-vintage ordering,
SUCCESS/source filters, no accidental bind parameters), the revision chain, and
the refresh script's statement order and exit codes against a fake engine.
The real-PostgreSQL behaviour is in tests/test_yf_latest_daily_vintage_pg.py.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from ingestion.yfinance_pull import YF_TICKER_LIST
from migrations.versions import yf_latest_daily_vintage_20261019 as mig
from scripts import refresh_yf_latest_vintage as refresh

REPO = Path(__file__).resolve().parent.parent
FIELDS = ("open", "high", "low", "close", "volume", "adj_close")


def _values_block(sql: str, alias: str) -> list[str]:
    m = re.search(rf"\b{alias}\((?:ticker|field)\) AS \(VALUES (.*?)\)(?:,\n|\n)", sql, re.S)
    assert m, f"no VALUES list for {alias}"
    return re.findall(r"\('((?:[^']|'')*)'\)", m.group(1))


def test_every_ticker_and_field_named_exactly_once():
    sql = mig.build_select_sql()
    tickers = _values_block(sql, "t")
    fields = _values_block(sql, "f")
    expected = list(dict.fromkeys(YF_TICKER_LIST))
    assert tickers == expected                      # imported list, same order, no dupes
    assert len(tickers) == len(set(tickers))
    assert fields == list(FIELDS)
    assert len(tickers) * len(fields) == len(expected) * 6
    # Each literal appears once in the whole statement (so no ticker is also
    # hidden in a second VALUES list or a hand-copied constant).
    for ticker in expected:
        assert sql.count(f"('{ticker}')") == 1, ticker
    for field in FIELDS:
        assert sql.count(f"('{field}')") == 1, field


def test_ticker_list_is_imported_not_hand_copied():
    src = Path(mig.__file__).read_text()
    assert "from ingestion.yfinance_pull import YF_TICKER_LIST" in src
    for sample in ("'SPY'", "'^GSPC'", "'HG=F'", "'BTC-USD'"):
        assert sample not in src


def test_per_series_lateral_probe_pattern():
    sql = mig.build_select_sql()
    assert "CROSS JOIN LATERAL" in sql
    assert "r.series_id = 'YF:' || t.ticker || ':' || f.field" in sql
    assert "r.source_id = 2" in sql
    assert "r.pull_status = 'SUCCESS'" in sql
    assert "r.obs_date >= DATE '2026-01-01'" in sql
    assert "SELECT DISTINCT ON (r.obs_date)" in sql
    assert "ORDER BY r.obs_date, r.pull_timestamp DESC, r.id DESC" in sql
    # No whole-table scans that time out on 1.9e9 rows.
    assert "LIKE" not in sql.upper()
    assert "series_id IN" not in sql


def test_output_columns():
    sql = mig.build_select_sql()
    for fragment in (
        "x.series_id,", "t.ticker,", "f.field,", "x.obs_date,", "x.id AS raw_series_id,",
        "x.pull_timestamp,", "x.value,",
        "x.raw_payload->>'capture_state' AS capture_state",
        "x.raw_payload->>'capture_policy' AS capture_policy",
        "x.raw_payload ? 'capture_policy'",
        "AS policy_marked",
        "x.raw_payload->>'repair_run_tag' AS repair_run_tag",
    ):
        assert fragment in sql, fragment


def test_create_is_no_data_with_unique_index():
    create = mig.build_create_sql(["SPY"])
    assert create.startswith("CREATE MATERIALIZED VIEW IF NOT EXISTS public.yf_latest_daily_vintage AS")
    assert create.rstrip().endswith("WITH NO DATA")
    index = mig.build_index_sql()
    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in index
    assert "ON public.yf_latest_daily_vintage (series_id, obs_date)" in index


def test_generated_sql_has_no_bind_parameters_or_percent():
    for sql in (mig.build_create_sql(), mig.build_index_sql()):
        assert text(sql).compile().params == {}
        assert "%" not in sql


def test_sql_literal_escapes_quotes_and_unsafe_tickers_refused(monkeypatch):
    assert mig._sql_literal("a'b") == "'a''b'"
    import ingestion.yfinance_pull as yp

    monkeypatch.setattr(yp, "YF_TICKER_LIST", ["SPY", "X'; DROP TABLE raw_series;--"])
    with pytest.raises(RuntimeError, match="unsafe ticker"):
        mig.load_tickers()
    monkeypatch.setattr(yp, "YF_TICKER_LIST", [])
    with pytest.raises(RuntimeError, match="empty"):
        mig.load_tickers()


def test_default_ticker_list_passes_validation():
    assert mig.load_tickers() == list(dict.fromkeys(YF_TICKER_LIST))


def test_migration_revision_chain():
    assert mig.revision == "yf_latest_daily_vintage_20261019"
    assert (REPO / "migrations" / "versions" / f"{mig.down_revision}.py").exists()
    # Single sibling pointing at the same parent would fork the chain.
    siblings = [
        p.name for p in (REPO / "migrations" / "versions").glob("*.py")
        if re.search(rf'^down_revision\s*=\s*"{mig.down_revision}"', p.read_text(), re.M)
    ]
    assert siblings == [Path(mig.__file__).name]


def test_planner_hints_are_set_local():
    assert mig.PLANNER_HINTS == (
        "SET LOCAL enable_hashjoin = off",
        "SET LOCAL enable_mergejoin = off",
        "SET LOCAL enable_seqscan = off",
        "SET LOCAL enable_bitmapscan = off",
    )


def test_refresh_statements_order_and_mode():
    first = refresh.refresh_statements(populated=False)
    later = refresh.refresh_statements(populated=True)
    assert first[0] == "SET LOCAL statement_timeout = '1200s'"
    assert first[2:6] == list(mig.PLANNER_HINTS)
    assert first[-1] == "REFRESH MATERIALIZED VIEW public.yf_latest_daily_vintage"
    assert later[-1] == "REFRESH MATERIALIZED VIEW CONCURRENTLY public.yf_latest_daily_vintage"
    assert first[:-1] == later[:-1]


def test_dry_run_prints_sql_and_never_connects(capsys):
    engine = MagicMock()
    assert refresh.main(["--dry-run"], engine=engine) == 0
    out = capsys.readouterr().out
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY public.yf_latest_daily_vintage" in out
    assert "SET LOCAL enable_hashjoin = off" in out
    engine.connect.assert_not_called()


def _fake_engine(populated):
    """Engine whose first query returns `populated` (None = view missing)."""
    executed: list[str] = []
    conn = MagicMock()
    conn.__enter__.return_value = conn

    def execute(stmt, params=None):
        sql = str(stmt)
        executed.append(sql)
        res = MagicMock()
        res.scalar.return_value = populated if "relispopulated" in sql else 1234
        return res

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine, executed


def test_main_missing_view_exits_2():
    engine, executed = _fake_engine(None)
    assert refresh.main([], engine=engine) == 2
    assert not any("REFRESH" in s for s in executed)


@pytest.mark.parametrize("populated,expected", [
    (False, "REFRESH MATERIALIZED VIEW public.yf_latest_daily_vintage"),
    (True, "REFRESH MATERIALIZED VIEW CONCURRENTLY public.yf_latest_daily_vintage"),
])
def test_main_refresh_mode(populated, expected, capsys):
    engine, executed = _fake_engine(populated)
    assert refresh.main([], engine=engine) == 0
    refreshes = [s for s in executed if s.startswith("REFRESH")]
    assert refreshes == [expected]
    assert any("statement_timeout = '1200s'" in s for s in executed)
    assert "1234 rows" in capsys.readouterr().out


def test_main_refresh_failure_exits_1():
    engine, _ = _fake_engine(True)
    conn = engine.connect.return_value
    original = conn.execute.side_effect

    def boom(stmt, params=None):
        if str(stmt).startswith("REFRESH"):
            raise RuntimeError("canceling statement due to statement timeout")
        return original(stmt, params)

    conn.execute.side_effect = boom
    assert refresh.main([], engine=engine) == 1


def test_systemd_templates_present_and_not_installed():
    d = REPO / "deploy" / "systemd"
    service = (d / "grid-yf-latest-vintage.service.template").read_text()
    timer = (d / "grid-yf-latest-vintage.timer.template").read_text()
    assert "TEMPLATE — NOT INSTALLED" in service and "TEMPLATE — NOT INSTALLED" in timer
    assert "scripts/refresh_yf_latest_vintage.py" in service
    assert "OnCalendar=*-*-* 10:45:00 UTC" in timer
    assert "Unit=grid-yf-latest-vintage.service" in timer


def test_module_importable_fresh():
    importlib.reload(mig)
