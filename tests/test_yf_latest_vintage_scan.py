"""Unit contract for yf_latest_vintage_scan_20261026 (no database).

The redefinition keeps the view's rows, columns and unique index and replaces
the "read every vintage" SELECT with: (1) newest pull_timestamp per date from
the unique index alone, (2) one winner probe per date. Behavioural equivalence
with the previous definition is proved on real PostgreSQL in
tests/test_yf_latest_daily_vintage_pg.py.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from sqlalchemy import text

from ingestion.yfinance_pull import YF_TICKER_LIST
from migrations.versions import yf_latest_daily_vintage_20261019 as v1
from migrations.versions import yf_latest_vintage_scan_20261026 as scan

REPO = Path(__file__).resolve().parent.parent
FIELDS = ("open", "high", "low", "close", "volume", "adj_close")


def _values(sql: str, alias: str) -> list[str]:
    m = re.search(rf"\b{alias}\((?:ticker|field)\) AS \(VALUES (.*?)\)(?:,\n|\n)", sql, re.S)
    assert m, alias
    return re.findall(r"\('((?:[^']|'')*)'\)", m.group(1))


def test_every_ticker_and_field_named_exactly_once():
    sql = scan.build_select_sql()
    assert _values(sql, "t") == list(dict.fromkeys(YF_TICKER_LIST))
    assert _values(sql, "f") == list(FIELDS)
    for ticker in dict.fromkeys(YF_TICKER_LIST):
        assert sql.count(f"('{ticker}')") == 1, ticker


def test_two_step_shape_newest_per_date_then_one_winner_probe():
    sql = scan.build_select_sql(["SPY"])
    assert sql.count("CROSS JOIN LATERAL") == 2
    # Step 1: newest timestamp per date; nothing but index columns, no payload, no status.
    step1 = sql[sql.index("SELECT r.obs_date, max(r.pull_timestamp) AS newest"):sql.index(") d\n")]
    assert "GROUP BY r.obs_date" in step1
    assert "r.source_id = 2" in step1 and "r.obs_date >= DATE '2026-01-01'" in step1
    for forbidden in ("raw_payload", "pull_status", "r.id", "r.value"):
        assert forbidden not in step1, forbidden
    # Step 2: one probe per date, latest SUCCESS vintage of source 2 at or before that timestamp.
    step2 = sql[sql.index("SELECT r.series_id, r.id, r.pull_timestamp"):]
    assert "r.obs_date = d.obs_date" in step2
    assert "r.pull_timestamp <= d.newest" in step2
    assert "r.pull_status = 'SUCCESS'" in step2 and "r.source_id = 2" in step2
    assert "ORDER BY r.pull_timestamp DESC\n    LIMIT 1" in step2
    # The old shape that read every vintage is gone.
    assert "DISTINCT ON" not in sql
    assert "LIKE" not in sql.upper()


def test_id_tiebreak_is_unnecessary_because_the_composite_index_is_unique():
    # (series_id, source_id, obs_date, pull_timestamp) is UNIQUE in schema.sql, so no tie exists.
    schema = (REPO / "schema.sql").read_text()
    assert re.search(r"CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_series_composite\s+ON raw_series "
                     r"\(series_id, source_id, obs_date, pull_timestamp\)", schema)
    assert "r.id DESC" not in scan.build_select_sql(["SPY"])


def test_output_columns_match_the_previous_definition():
    def cols(sql):
        head = sql[sql.index("SELECT\n") + 7:sql.index("\nFROM t")]
        return [re.split(r"\s+AS\s+", c.strip().rstrip(","))[-1].split(".")[-1] for c in head.split("\n") if c.strip()]

    assert cols(scan.build_select_sql(["SPY"])) == cols(v1.build_select_sql(["SPY"]))
    assert cols(scan.build_select_sql(["SPY"])) == [
        "series_id", "ticker", "field", "obs_date", "raw_series_id", "pull_timestamp", "value",
        "capture_state", "capture_policy", "policy_marked", "repair_run_tag",
    ]


def test_create_sql_is_no_data_and_has_no_bind_parameters():
    create = scan.build_create_sql(["SPY"])
    assert create.startswith("CREATE MATERIALIZED VIEW IF NOT EXISTS public.yf_latest_daily_vintage AS")
    assert create.rstrip().endswith("WITH NO DATA")
    for sql in (create, scan.build_index_sql(), scan._grant_sql()):
        assert text(sql).compile().params == {}
        assert "%" not in sql
    assert scan.build_index_sql() == v1.build_index_sql()      # same unique index (series_id, obs_date)


def test_revision_chain_single_child_of_the_merge():
    assert scan.revision == "yf_latest_vintage_scan_20261026" and len(scan.revision) <= 32
    assert scan.down_revision == "merge_yf_finra_20261023"
    assert scan.branch_labels is None and scan.depends_on is None
    children = [
        p.name for p in (REPO / "migrations" / "versions").glob("*.py")
        if re.search(r'^down_revision\s*=\s*"merge_yf_finra_20261023"', p.read_text(), re.M)
    ]
    assert children == [Path(scan.__file__).name]


class _Op:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, stmt):
        self.statements.append(str(getattr(stmt, "text", stmt)))


@pytest.fixture()
def fake_op(monkeypatch):
    op = _Op()
    monkeypatch.setattr(scan, "op", op)
    monkeypatch.setattr(scan, "load_tickers", lambda: ["SPY", "^GSPC"])
    monkeypatch.setattr(v1, "load_tickers", lambda: ["SPY", "^GSPC"])
    return op


def test_upgrade_replaces_the_view_in_one_catalog_only_sequence(fake_op):
    scan.upgrade()
    s = fake_op.statements
    assert s[0] == "SET LOCAL lock_timeout = '5s'" and s[1] == "SET LOCAL statement_timeout = '60s'"
    assert s[2] == "DROP MATERIALIZED VIEW IF EXISTS public.yf_latest_daily_vintage"
    assert s[3].startswith("CREATE MATERIALIZED VIEW IF NOT EXISTS public.yf_latest_daily_vintage")
    assert s[3].rstrip().endswith("WITH NO DATA") and "max(r.pull_timestamp)" in s[3]
    assert s[4].startswith("CREATE UNIQUE INDEX IF NOT EXISTS uq_yf_latest_daily_vintage_series_obs")
    assert "GRANT SELECT ON public.yf_latest_daily_vintage TO grid" in s[5]
    assert len(s) == 6
    # No REFRESH, no index on raw_series, no touch of raw_series DDL.
    assert not any(re.search(r"REFRESH|ALTER TABLE|CREATE INDEX|raw_series\s*\(", x) for x in s)


def test_downgrade_restores_the_previous_definition_unpopulated(fake_op):
    scan.downgrade()
    s = fake_op.statements
    assert s[2] == "DROP MATERIALIZED VIEW IF EXISTS public.yf_latest_daily_vintage"
    assert "DISTINCT ON (r.obs_date)" in s[3] and s[3].rstrip().endswith("WITH NO DATA")
    assert "max(r.pull_timestamp)" not in s[3]


def test_previous_revision_is_untouched():
    assert v1.revision == "yf_latest_daily_vintage_20261019"
    assert "DISTINCT ON (r.obs_date)" in v1.build_select_sql(["SPY"])


def test_refresh_script_contract_is_unchanged():
    from scripts import refresh_yf_latest_vintage as refresh

    assert refresh.STATEMENT_TIMEOUT_S == 1200
    assert refresh.refresh_statements(True)[-1] == "REFRESH MATERIALIZED VIEW CONCURRENTLY public.yf_latest_daily_vintage"
    assert refresh.refresh_statements(False)[-1] == "REFRESH MATERIALIZED VIEW public.yf_latest_daily_vintage"
    assert tuple(refresh.refresh_statements(True)[2:6]) == scan.PLANNER_HINTS
    # The systemd templates keep their 10:45Z / persistent behaviour.
    timer = (REPO / "deploy" / "systemd" / "grid-yf-latest-vintage.timer.template").read_text()
    assert "OnCalendar=*-*-* 10:45:00 UTC" in timer and "Persistent=true" in timer


def test_statement_timeout_flag_defaults_to_1200_and_is_bounded(capsys):
    from unittest.mock import MagicMock

    from scripts import refresh_yf_latest_vintage as refresh

    assert refresh.refresh_statements(True)[0] == "SET LOCAL statement_timeout = '1200s'"
    assert refresh.refresh_statements(False, timeout_s=3600)[0] == "SET LOCAL statement_timeout = '3600s'"
    assert refresh.main(["--dry-run", "--statement-timeout-s", "3600"], engine=MagicMock()) == 0
    assert "statement_timeout = '3600s'" in capsys.readouterr().out
    for bad in ("59", "7201", "0"):
        assert refresh.main(["--dry-run", "--statement-timeout-s", bad], engine=MagicMock()) == 2
