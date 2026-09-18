"""Pure-Python tests for godview/sec_ftd_pillar.py — no database, no network."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

import godview.sec_ftd_pillar as sec_ftd_pillar
from godview.sec_ftd_pillar import (
    AGE_NOTE,
    NOT_A_TIMELINE_NOTE,
    RELEASE_RULE_ID,
    compute_age_days,
    compute_release_date,
    compute_total_failed_usd,
    materialize_sec_ftd_pillar,
    resolve_display_symbol,
)


def test_not_a_timeline_note_forbids_buyin_and_squeeze():
    assert "T+35" in NOT_A_TIMELINE_NOTE
    assert "squeeze" in NOT_A_TIMELINE_NOTE
    assert "never summed across dates" in NOT_A_TIMELINE_NOTE


def test_age_note_distinguishes_observation_age_from_fail_age():
    assert "NOT the age of the underlying fails" in AGE_NOTE


@pytest.mark.parametrize(
    "settlement_date,expected_release",
    [
        (date(2026, 8, 1), date(2026, 8, 31)),   # first half -> month-end
        (date(2026, 8, 15), date(2026, 8, 31)),  # boundary day, still first half
        (date(2026, 8, 16), date(2026, 9, 15)),  # second half -> 15th of next month
        (date(2026, 8, 31), date(2026, 9, 15)),  # last day of month, second half
        (date(2026, 12, 20), date(2027, 1, 15)), # December wraps to January
    ],
)
def test_compute_release_date_half_month_rule(settlement_date, expected_release):
    release_date, source_ref = compute_release_date(settlement_date)
    assert release_date == expected_release
    assert RELEASE_RULE_ID in source_ref


def test_compute_total_failed_usd_is_shares_times_price():
    assert compute_total_failed_usd(1000.0, 16.99) == pytest.approx(16990.0)


def test_compute_total_failed_usd_none_without_a_price():
    assert compute_total_failed_usd(1000.0, None) is None


def test_resolve_display_symbol_prefers_the_ftd_files_own_symbol():
    symbol, source = resolve_display_symbol("Y4000A102", "HQ")
    assert symbol == "HQ"
    assert source == "ftd_file_symbol"


def test_resolve_display_symbol_falls_back_to_cusip_and_says_so():
    symbol, source = resolve_display_symbol("Y4000A102", None)
    assert symbol == "Y4000A102"
    assert source == "cusip_fallback"

    symbol2, source2 = resolve_display_symbol("Y4000A102", "   ")
    assert symbol2 == "Y4000A102"
    assert source2 == "cusip_fallback"


def test_compute_age_days_is_calendar_days_since_settlement():
    assert compute_age_days(date(2026, 8, 17), date(2026, 9, 18)) == 32
    assert compute_age_days(date(2026, 9, 18), date(2026, 9, 18)) == 0


# ---------------------------------------------------------------------------
# Regression fake: real-Postgres run 4 (composition 0f0451aa) found
# materialize_sec_ftd_pillar returning FAILED with
# psycopg2.errors.NotNullViolation on mandatory_buyin_date -- the pillar
# deliberately writes NULL there (no T+35 buy-in timeline, see
# NOT_A_TIMELINE_NOTE) but god_view_market_tables_20260918 declared that
# column NOT NULL. The bare fed_liquidity_pillar_pure.py-style _FakeConn (a
# no-op that returns an empty result for any SQL, never inspecting params)
# would have let this defect through silently. This fake actually enforces
# NOT NULL on INSERTs into sec_regsho_ftd_cns, so this class of defect
# cannot pass a pure test again without a real database.
# ---------------------------------------------------------------------------


class _FakeNotNullViolation(Exception):
    pass


#: The REAL, CURRENT schema's NOT NULL columns for sec_regsho_ftd_cns, after
#: migrations/versions/godview_pit_secftd_0918.py's 2026-09-18 fix DROPped
#: the constraint on mandatory_buyin_date. days_remaining/squeeze_risk_score
#: were already nullable in god_view_market_tables_20260918 -- checked, not
#: guessed.
_FIXED_NOT_NULL_COLUMNS = frozenset({"settlement_date", "ticker", "failed_shares"})


class _FakeResult:
    def __init__(self, rowcount: int = 1):
        self.rowcount = rowcount

    def mappings(self):
        return self

    def all(self):
        return []

    def fetchall(self):
        return []

    def fetchone(self):
        return None

    def scalar(self):
        return 1


class _FakeConn:
    """Unlike the bare fed-liquidity fake, this one enforces NOT NULL on the
    one statement that matters here: the raw INSERT into sec_regsho_ftd_cns
    (every other DB call the materializer makes is monkeypatched away at the
    helper-function level below, mirroring test_fed_liquidity_pillar_pure.py's
    pattern). It also tracks (settlement_date, ticker) pairs it has already
    "inserted" and reports ``rowcount == 0`` for a repeat -- exactly how
    Postgres reports an ``ON CONFLICT (settlement_date, ticker) DO NOTHING``
    that silently dropped a row -- so the materializer's own
    attempted-vs-landed accounting can be exercised without a real database.
    """

    def __init__(self, not_null_columns: frozenset[str]):
        self._not_null_columns = not_null_columns
        self._inserted_keys: set[tuple] = set()

    def execute(self, stmt, params=None):
        if params is not None and "INSERT INTO sec_regsho_ftd_cns" in str(stmt):
            for column in self._not_null_columns:
                if params.get(column) is None:
                    raise _FakeNotNullViolation(
                        f'null value in column "{column}" of relation '
                        f'"sec_regsho_ftd_cns" violates not-null constraint'
                    )
            key = (params["settlement_date"], params["ticker"])
            if key in self._inserted_keys:
                return _FakeResult(rowcount=0)  # ON CONFLICT DO NOTHING skipped it
            self._inserted_keys.add(key)
            return _FakeResult(rowcount=1)
        return _FakeResult()


class _FakeEngineCtx:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *exc):
        return False


class _FakeEngine:
    def __init__(self, not_null_columns: frozenset[str]):
        self._not_null_columns = not_null_columns

    def begin(self):
        return _FakeEngineCtx(_FakeConn(self._not_null_columns))


def _patched_materialize(monkeypatch, *, not_null_columns: frozenset[str]):
    settlement_date = date(2026, 8, 20)
    pull_timestamp = datetime(2026, 9, 15, 6, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(sec_ftd_pillar, "_discover_cusips", lambda conn, as_of: ["Y4000A102"])
    monkeypatch.setattr(
        sec_ftd_pillar,
        "_read_cusip_history",
        lambda conn, cusip, as_of: {
            settlement_date: {
                "failed_shares": 373.0,
                "symbol": "AAPL",
                "price": 16.99,
                "pull_timestamp": pull_timestamp,
            }
        },
    )
    monkeypatch.setattr(sec_ftd_pillar, "_existing_settlement_dates", lambda conn, ticker, cusip: set())
    monkeypatch.setattr(sec_ftd_pillar, "_distinct_pull_count", lambda conn, cusip, obs_date: 1)
    monkeypatch.setattr(sec_ftd_pillar, "record_generation", lambda *a, **k: None)

    return materialize_sec_ftd_pillar(_FakeEngine(not_null_columns), as_of=date(2026, 9, 18))


def test_materializer_null_mandatory_buyin_date_fails_pre_fix_schema_passes_fixed_schema(monkeypatch):
    """Fails before the 2026-09-18 fix's column set is used, passes after.

    Run with the PRE-FIX column set (mandatory_buyin_date still NOT NULL,
    matching god_view_market_tables_20260918 before
    godview_pit_secftd_0918's ALTER), the materializer's real row --
    mandatory_buyin_date deliberately None -- fails exactly the way real
    Postgres did on composition 0f0451aa. Run with the FIXED column set (that
    migration's DROP NOT NULL applied), the identical row succeeds.
    """
    pre_fix_columns = _FIXED_NOT_NULL_COLUMNS | {"mandatory_buyin_date"}

    result_before = _patched_materialize(monkeypatch, not_null_columns=pre_fix_columns)
    assert result_before.status == "FAILED"
    assert "mandatory_buyin_date" in result_before.message

    result_after = _patched_materialize(monkeypatch, not_null_columns=_FIXED_NOT_NULL_COLUMNS)
    assert result_after.status == "SUCCESS"
    assert result_after.rows_written == 1


def test_materializer_counts_and_reports_rows_skipped_by_a_settlement_date_ticker_conflict(monkeypatch):
    """Real-Postgres run (composition d7ffa7f1): two different CUSIPs that
    happen to report the SAME display symbol on the SAME settlement date
    collide on sec_regsho_ftd_cns's real unique key -- (settlement_date,
    ticker), not cusip -- and the second one is silently dropped by
    ON CONFLICT (settlement_date, ticker) DO NOTHING. Before this fix,
    MaterializationResult had no way to see that: rows_written counted
    every ATTEMPTED row, not every row that actually landed. This proves
    the materializer now counts the discard and surfaces it.
    """
    settlement_date = date(2026, 8, 20)
    pull_timestamp = datetime(2026, 9, 15, 6, 0, tzinfo=timezone.utc)
    shared_symbol = "DUPTICK"

    monkeypatch.setattr(sec_ftd_pillar, "_discover_cusips", lambda conn, as_of: ["CUSIP_A", "CUSIP_B"])
    monkeypatch.setattr(
        sec_ftd_pillar,
        "_read_cusip_history",
        lambda conn, cusip, as_of: {
            settlement_date: {
                "failed_shares": 100.0,
                "symbol": shared_symbol,
                "price": 10.0,
                "pull_timestamp": pull_timestamp,
            }
        },
    )
    monkeypatch.setattr(sec_ftd_pillar, "_existing_settlement_dates", lambda conn, ticker, cusip: set())
    monkeypatch.setattr(sec_ftd_pillar, "_distinct_pull_count", lambda conn, cusip, obs_date: 1)
    monkeypatch.setattr(sec_ftd_pillar, "record_generation", lambda *a, **k: None)

    result = materialize_sec_ftd_pillar(_FakeEngine(_FIXED_NOT_NULL_COLUMNS), as_of=date(2026, 9, 18))

    assert result.status == "SUCCESS"
    assert result.rows_written == 1
    assert result.rows_skipped_conflict == 1
    assert "skipped" in result.message
    assert "settlement_date, ticker" in result.message
