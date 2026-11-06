"""W2-10: split-aware prices ("NVDA isn't down 92%").

Measured on griddb 2026-11-06 (read only): ``YF:NVDA:close`` holds TIINGO's
as-traded closes for history (2024-06-07 1208.88, 2024-06-10 121.79) on top
of an older yfinance split-adjusted vintage (2024-06-05 122.44), and
``/api/v1/ten-year-portfolio/weekly`` reported NVDA max_drawdown 0.922 --
the 10-for-1 split on 2024-06-10 read as a crash (1224.40 -> 94.31).

Pinned here, on SQLite stand-ins of raw_series / source_catalog:

* NVDA 10-for-1 on 2024-06-10 and a 1-for-10 reverse split re-base prices;
* a prediction spanning a split is scored on the entry's share basis (live
  scorer, OracleEngine, additive split rescore);
* point in time: a split never applies before its effective date, and a split
  row pulled after ``as_of_ts`` is invisible;
* a ticker with no recorded split is unchanged;
* a yfinance vintage pulled after a split (already adjusted) is restored to
  as-traded, and an already-adjusted history is never divided twice.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, time, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

import oracle.engine  # noqa: F401  (import the real oracle package first; see test_oracle_null_readers)
import scripts.score_oracle_trades as sot
from store.observations import (
    SplitEvent,
    adjust_history_for_splits,
    cumulative_split_factor,
    price_in_basis,
    read_splits,
    split_adjusted_return,
    split_jump_confirmed,
    split_series_id,
)

UTC = timezone.utc
NY = ZoneInfo("America/New_York")
sqlite3.register_adapter(date, date.isoformat)
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))
sqlite3.register_converter("DATE", lambda b: date.fromisoformat(b.decode()))

NVDA_SPLIT = date(2024, 6, 10)
NVDA = [SplitEvent("NVDA", NVDA_SPLIT, 10.0, "TIINGO")]
REVERSE_SPLIT = date(2025, 3, 3)
REV = [SplitEvent("RVS", REVERSE_SPLIT, 0.1, "TIINGO")]


def _engine():
    eng = create_engine(
        "sqlite://", poolclass=StaticPool,
        connect_args={"detect_types": sqlite3.PARSE_DECLTYPES, "check_same_thread": False},
    )
    @event.listens_for(eng, "connect")
    def _now(dbapi_conn, _record):
        dbapi_conn.create_function("NOW", 0, lambda: "2026-11-06 00:00:00")

    with eng.begin() as c:
        c.execute(text("CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT)"))
        c.execute(text(
            "CREATE TABLE raw_series (id INTEGER PRIMARY KEY, series_id TEXT, source_id INTEGER, "
            "obs_date DATE, value DOUBLE PRECISION, pull_timestamp TEXT, pull_status TEXT, "
            "raw_payload TEXT)"
        ))
        c.execute(text("INSERT INTO source_catalog (id, name) VALUES (1, 'TIINGO'), (2, 'yfinance')"))
    return eng


def _raw(c, sid: str, src: int, d: date, value: float, pulled: datetime, status: str = "SUCCESS"):
    c.execute(text(
        "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_timestamp, pull_status) "
        "VALUES (:s, :src, :d, :v, :p, :st)"
    ), {"s": sid, "src": src, "d": d, "v": value, "p": pulled.astimezone(UTC), "st": status})


def _settled(d: date) -> datetime:
    return datetime.combine(d, time(19, 30), NY)


# ── pure math ───────────────────────────────────────────────────────────────

def test_nvda_ten_for_one_rebases_post_split_close_to_entry_shares():
    # Entry 2024-06-05 at the as-traded 1224.40; exit 2024-06-12 TIINGO 125.20.
    exit_in_entry_shares = price_in_basis(
        125.20, date(2024, 6, 12), basis_date=date(2024, 6, 5), splits=NVDA, source="TIINGO",
    )
    assert exit_in_entry_shares == pytest.approx(1252.0)
    assert exit_in_entry_shares / 1224.40 - 1 == pytest.approx(0.0225, abs=1e-4)
    # The other direction: a pre-split close in post-split shares.
    assert price_in_basis(1224.40, date(2024, 6, 5), basis_date=date(2026, 11, 5),
                          splits=NVDA) == pytest.approx(122.44)


def test_reverse_split_rebases_the_other_way():
    # 1-for-10 reverse: $1.00 before, $9.50 after is a 5% loss, not +850%.
    exit_ = price_in_basis(9.50, date(2025, 3, 5), basis_date=date(2025, 2, 20), splits=REV)
    assert exit_ == pytest.approx(0.95)
    assert exit_ / 1.00 - 1 == pytest.approx(-0.05)


def test_split_is_never_applied_before_its_effective_date():
    # Both prices before the split: factor 1, value untouched.
    assert cumulative_split_factor(NVDA, date(2024, 5, 1), date(2024, 6, 7)) == 1.0
    assert price_in_basis(1208.88, date(2024, 6, 7), basis_date=date(2024, 6, 5),
                          splits=NVDA) == pytest.approx(1208.88)
    # The effective date itself is post-split; the day before is not.
    assert cumulative_split_factor(NVDA, date(2024, 6, 7), NVDA_SPLIT) == 10.0
    assert cumulative_split_factor(NVDA, NVDA_SPLIT, date(2024, 6, 12)) == 1.0
    # A split after the window end is never used.
    assert cumulative_split_factor(NVDA, date(2024, 1, 1), date(2024, 6, 9)) == 1.0


def test_no_split_ticker_is_unchanged():
    assert price_in_basis(500.0, date(2026, 1, 5), basis_date=date(2025, 1, 5), splits=[]) == 500.0
    pts = [(date(2026, 1, 1) + timedelta(days=i), 100.0 + i) for i in range(10)]
    assert adjust_history_for_splits(pts, []) == (pts, [])


def test_yfinance_vintage_pulled_after_the_split_is_restored_to_as_traded():
    # yfinance's 2026-03-29 backfill stored 2024-06-05 as 122.44 (already /10).
    as_traded = price_in_basis(
        122.44, date(2024, 6, 5), basis_date=date(2024, 6, 5), splits=NVDA,
        source="yfinance", pull_timestamp=datetime(2026, 3, 29, 8, 29, tzinfo=UTC),
    )
    assert as_traded == pytest.approx(1224.40)
    # A yfinance close pulled the same day (no later split) is already as-traded.
    assert price_in_basis(
        121.79, date(2024, 6, 10), basis_date=date(2024, 6, 10), splits=NVDA,
        source="yfinance", pull_timestamp=datetime(2024, 6, 10, 23, tzinfo=UTC),
    ) == pytest.approx(121.79)


def test_history_back_adjustment_fixes_the_92_percent_drawdown():
    from strategy.ten_year_portfolio import compute_chart_metrics

    # Weekly NVDA-shaped history: raw ~1200 before 2024-06-10, ~120 after,
    # a dip to 94.31 in April 2025, 181.60 today.
    start = date(2023, 6, 5)
    pts = []
    d, v = start, 400.0
    while d < NVDA_SPLIT:
        pts.append((d, v))
        v *= 1.02
        d += timedelta(days=7)
    v = v / 10.0
    while d <= date(2026, 11, 5):
        pts.append((d, v))
        v *= 1.003
        d += timedelta(days=7)
    raw_metrics = compute_chart_metrics("NVDA", pts)
    assert raw_metrics["max_drawdown"] > 0.85          # the bug: a split read as a crash
    adjusted, applied = adjust_history_for_splits(pts, NVDA)
    assert [e.effective_date for e in applied] == [NVDA_SPLIT]
    adj_metrics = compute_chart_metrics("NVDA", adjusted)
    assert adj_metrics["max_drawdown"] < 0.10
    assert adjusted[-1] == pts[-1]                       # today's price is the real one


def test_already_adjusted_history_is_never_divided_twice():
    pts = [(date(2024, 6, 3) + timedelta(days=i), 120.0 + 0.1 * i) for i in range(14)]
    assert not split_jump_confirmed(pts, NVDA[0])
    assert adjust_history_for_splits(pts, NVDA) == (pts, [])
    # verify=False is the explicit opt-out.
    forced, applied = adjust_history_for_splits(pts, NVDA, verify=False)
    assert applied and forced[0][1] == pytest.approx(pts[0][1] / 10)


def test_history_ignores_a_split_after_its_last_point():
    pts = [(date(2024, 6, 3) + timedelta(days=i), 1200.0) for i in range(5)]  # ends 06-07
    assert adjust_history_for_splits(pts, NVDA) == (pts, [])


# ── raw_series reads ────────────────────────────────────────────────────────

def _seed_nvda(c) -> None:
    sid = "YF:NVDA:close"
    _raw(c, sid, 1, date(2024, 6, 5), 1224.40, _settled(date(2024, 6, 5)))
    _raw(c, sid, 1, date(2024, 6, 7), 1208.88, _settled(date(2024, 6, 7)))
    _raw(c, sid, 1, NVDA_SPLIT, 121.79, _settled(NVDA_SPLIT))
    _raw(c, sid, 1, date(2024, 6, 12), 125.20, _settled(date(2024, 6, 12)))
    _raw(c, split_series_id("NVDA"), 1, NVDA_SPLIT, 10.0, datetime(2024, 6, 10, 23, tzinfo=UTC))


def test_read_splits_is_pit_and_drops_unit_and_failed_rows():
    eng = _engine()
    with eng.begin() as c:
        _seed_nvda(c)
        _raw(c, split_series_id("NVDA"), 2, NVDA_SPLIT, 10.0, datetime(2026, 11, 7, tzinfo=UTC))
        _raw(c, split_series_id("NVDA"), 1, date(2021, 7, 20), 1.0, datetime(2026, 11, 7, tzinfo=UTC))
        _raw(c, split_series_id("NVDA"), 1, date(2021, 7, 20), 0.0, datetime(2026, 11, 7, tzinfo=UTC),
             "FAILED")
    with eng.connect() as c:
        events = read_splits(c, "NVDA")
        assert [(e.effective_date, e.ratio) for e in events] == [(NVDA_SPLIT, 10.0)]
        assert events[0].source == "yfinance"                         # latest vintage wins
        assert read_splits(c, "NVDA", as_of_ts=datetime(2024, 6, 1, tzinfo=UTC)) == []
        assert read_splits(c, "AAPL") == []


def test_split_adjusted_return_across_nvda_split():
    eng = _engine()
    with eng.begin() as c:
        _seed_nvda(c)
    with eng.connect() as c:
        r = split_adjusted_return(c, "NVDA", date(2024, 6, 5), date(2024, 6, 12), source="TIINGO")
        assert r == pytest.approx(125.20 * 10 / 1224.40 - 1)
        # Before the split row was known, nothing is applied (PIT).
        pit = split_adjusted_return(c, "NVDA", date(2024, 6, 5), date(2024, 6, 7), source="TIINGO",
                                    as_of_ts=datetime(2024, 6, 8, tzinfo=UTC))
        assert pit == pytest.approx(1208.88 / 1224.40 - 1)


def test_split_adjusted_return_no_split_ticker_equals_plain_return():
    eng = _engine()
    with eng.begin() as c:
        _raw(c, "YF:KO:close", 1, date(2026, 1, 5), 60.0, _settled(date(2026, 1, 5)))
        _raw(c, "YF:KO:close", 1, date(2026, 2, 5), 63.0, _settled(date(2026, 2, 5)))
    with eng.connect() as c:
        assert split_adjusted_return(c, "KO", date(2026, 1, 5), date(2026, 2, 5),
                                     source="TIINGO") == pytest.approx(0.05)
        assert split_adjusted_return(c, "KO", date(2025, 1, 5), date(2026, 2, 5),
                                     source="TIINGO") is None


# ── oracle scorer (scripts/score_oracle_trades.py) ─────────────────────────

_PRED_DDL = """
CREATE TABLE oracle_predictions (
    id TEXT PRIMARY KEY, created_at TEXT, ticker TEXT NOT NULL, prediction_type TEXT,
    direction TEXT NOT NULL, target_price DOUBLE PRECISION, entry_price DOUBLE PRECISION,
    expiry DATE NOT NULL, confidence DOUBLE PRECISION, expected_move_pct DOUBLE PRECISION,
    model_name TEXT, signals TEXT, verdict TEXT DEFAULT 'pending',
    actual_price DOUBLE PRECISION, actual_move_pct DOUBLE PRECISION, pnl_pct DOUBLE PRECISION,
    scored_at TEXT, score_notes TEXT, dedup_keep BOOLEAN NOT NULL DEFAULT 1,
    null_write_policy TEXT
)
"""


def _pred_engine():
    eng = _engine()
    with eng.begin() as c:
        c.execute(text(_PRED_DDL))
        c.execute(text(
            "CREATE TABLE oracle_models (name TEXT PRIMARY KEY, predictions_made INTEGER DEFAULT 0, "
            "hits INTEGER DEFAULT 0, misses INTEGER DEFAULT 0, partials INTEGER DEFAULT 0, "
            "cumulative_pnl DOUBLE PRECISION DEFAULT 0, last_updated TEXT)"
        ))
        c.execute(text("INSERT INTO oracle_models (name) VALUES ('m')"))
    return eng


def _pred(c, pid: str, ticker: str, expiry: date, *, entry: float, created: str,
          direction: str = "PUT", verdict: str = "pending", expected: float = 2.0,
          notes: str | None = None) -> None:
    c.execute(text(
        "INSERT INTO oracle_predictions (id, created_at, ticker, direction, entry_price, expiry, "
        "confidence, expected_move_pct, model_name, signals, verdict, score_notes) "
        "VALUES (:id, :cr, :t, :dir, :e, :x, 0.6, :exp, 'm', '{}', :v, :n)"
    ), {"id": pid, "cr": created, "t": ticker, "dir": direction, "e": entry, "x": expiry,
        "exp": expected, "v": verdict, "n": notes})


def test_scorer_split_adjusts_a_prediction_spanning_the_split():
    eng = _pred_engine()
    with eng.begin() as c:
        _seed_nvda(c)
        # PUT at 1224.40 on 06-05: raw close 125.20 would score -89.8% (a "hit").
        _pred(c, "spans", "NVDA", date(2024, 6, 12), entry=1224.40, created="2024-06-05 15:00:00")
        # Created after the split: no adjustment. (Both rows lose, which keeps
        # the success-lesson writer out of the way; lessons are not under test.)
        _pred(c, "after", "NVDA", date(2024, 6, 12), entry=121.79, created="2024-06-10 20:30:00")
    with eng.connect() as c:
        book = sot.load_expiry_closes(c, ["NVDA"], date(2024, 6, 1), date(2024, 6, 12))
        splits = sot.load_split_book(c, ["NVDA"])
    with eng.begin() as c:
        counters = sot.score_one_chunk(
            c, engine=eng, prices=book, today=date(2024, 6, 13), chunk_size=10,
            now=datetime(2024, 6, 13, tzinfo=UTC), splits=splits,
        )
    assert counters["split_adjusted"] == 1
    with eng.connect() as c:
        rows = {r[0]: r for r in c.execute(text(
            "SELECT id, verdict, actual_price, actual_move_pct, score_notes FROM oracle_predictions"
        ))}
    assert rows["spans"][1] == "miss"
    assert rows["spans"][2] == pytest.approx(1252.0)
    assert rows["spans"][3] == pytest.approx(2.25, abs=0.01)
    assert sot.SPLIT_RULE in rows["spans"][4] and "x10 split" in rows["spans"][4]
    assert rows["after"][2] == pytest.approx(125.20)
    assert sot.SPLIT_RULE not in rows["after"][4]


def test_scorer_reverse_split_and_no_split_tickers():
    splits = {"RVS": REV}
    res = sot.ExpiryClose("priced", price=9.50, session_date=date(2025, 3, 5), source="TIINGO")
    adj, factor = sot.split_adjusted_close(splits, "RVS", res.price, res.session_date,
                                           created_at="2025-02-20 15:00:00", source="TIINGO")
    assert (adj, factor) == (pytest.approx(0.95), pytest.approx(0.1))
    assert sot.classify_outcome("CALL", 1.00, adj, 2.0)[0] == "miss"
    assert sot.split_adjusted_close(splits, "KO", 63.0, date(2025, 3, 5),
                                    created_at="2025-02-20 15:00:00") == (63.0, 1.0)


def test_split_rescore_is_additive_and_only_covers_spanning_rows():
    eng = _pred_engine()
    with eng.begin() as c:
        _seed_nvda(c)
        _pred(c, "off-span", "NVDA", date(2024, 6, 12), entry=1224.40,
              created="2024-06-05 15:00:00", verdict="hit", notes="old")
        _pred(c, "off-pre", "NVDA", date(2024, 6, 7), entry=1224.40,
              created="2024-06-05 15:00:00", verdict="hit", notes="old")
        _pred(c, "off-post", "NVDA", date(2024, 6, 12), entry=121.79,
              created="2024-06-10 20:30:00", verdict="miss", notes="old")
    with eng.connect() as c:
        before = [tuple(r) for r in c.execute(text("SELECT * FROM oracle_predictions ORDER BY id"))]
    run = datetime(2026, 11, 7, tzinfo=UTC)
    dry = sot.rescore_splits(eng, apply=False, now=run)
    assert dry["candidates_spanning_split"] == 1 and dry["rows_to_append"] == 1
    assert dry["tally"] == {"official:dedup_keep": {"spanning_split": 1, "hit->miss": 1}}
    first = sot.rescore_splits(eng, apply=True, now=run)
    second = sot.rescore_splits(eng, apply=True, now=run)
    assert first["rows_appended"] == 1 and second["rows_appended"] == 0
    with eng.connect() as c:
        after = [tuple(r) for r in c.execute(text("SELECT * FROM oracle_predictions ORDER BY id"))]
        row = c.execute(text(
            "SELECT prediction_id, scoring_version, prior_verdict, verdict, actual_price, official "
            "FROM oracle_prediction_rescores"
        )).fetchall()
    assert after == before, "oracle_predictions must never be updated by a rescore"
    assert len(row) == 1
    pid, version, prior, verdict, actual, official = row[0]
    assert (pid, version, prior, verdict) == ("off-span", sot.SPLIT_RESCORE_SCORING_VERSION, "hit", "miss")
    assert actual == pytest.approx(1252.0) and not official


def test_split_rescore_falls_back_to_the_recorded_expiry_price():
    """IWF 4-for-1 (2026-04-29): verdicts priced from spot, no settled close row."""
    eng = _pred_engine()
    with eng.begin() as c:
        _raw(c, split_series_id("IWF"), 1, date(2026, 4, 29), 4.0, datetime(2026, 11, 7, tzinfo=UTC))
        _pred(c, "iwf", "IWF", date(2026, 5, 15), entry=476.41, created="2026-04-25 22:42:35",
              direction="CALL", verdict="miss", notes="Entry $476.41 → Actual $124.37 (-73.9%)")
        c.execute(text("UPDATE oracle_predictions SET actual_price = 124.37 WHERE id = 'iwf'"))
    proj = sot.rescore_splits(eng, apply=True, now=datetime(2026, 11, 7, tzinfo=UTC))
    assert proj["tally"] == {"official:dedup_keep": {
        "spanning_split": 1, "stored_price": 1, "miss->hit": 1}}
    with eng.connect() as c:
        row = c.execute(text(
            "SELECT verdict, actual_price, actual_move_pct, price_source FROM oracle_prediction_rescores"
        )).one()
    assert row[0] == "hit" and row[1] == pytest.approx(497.48)
    assert row[2] == pytest.approx(4.42, abs=0.01) and row[3] == sot.STORED_PRICE_SOURCE


def test_split_rescore_args_are_mutually_exclusive():
    with pytest.raises(SystemExit):
        sot._parse_args(["--rescore-splits", "--rescore-no-data"])
    assert sot._parse_args(["--rescore-splits", "--apply"]).apply


# ── OracleEngine (Hermes 6-hour scoring path) ──────────────────────────────

def test_oracle_engine_split_adjusts_exit_price():
    from oracle.engine import OracleEngine

    book = {"NVDA": NVDA}
    created = datetime(2024, 6, 5, 15, tzinfo=UTC)
    assert OracleEngine._split_adjust_exit(book, "NVDA", 125.20, date(2024, 6, 12), created) \
        == pytest.approx(1252.0)
    assert OracleEngine._split_adjust_exit(book, "NVDA", 1208.88, date(2024, 6, 7), created) \
        == pytest.approx(1208.88)
    assert OracleEngine._split_adjust_exit(book, "KO", 63.0, date(2024, 6, 12), created) == 63.0
    # 2024-06-11 01:00Z is 06-10 21:00 New York: the split day, post-split basis.
    assert OracleEngine._split_adjust_exit(
        book, "NVDA", 125.20, date(2024, 6, 12), datetime(2024, 6, 11, 1, tzinfo=UTC),
    ) == pytest.approx(125.20)


def test_oracle_engine_creation_on_split_day_uses_post_split_basis():
    from oracle.engine import OracleEngine

    created = datetime(2024, 6, 10, 20, tzinfo=UTC)  # 16:00 NY on the split day
    assert OracleEngine._split_adjust_exit({"NVDA": NVDA}, "NVDA", 125.20, date(2024, 6, 12),
                                           created) == pytest.approx(125.20)


# ── ten-year portfolio chart loader ────────────────────────────────────────

def test_ten_year_portfolio_applies_recorded_splits(monkeypatch):
    from api.routers import ten_year_portfolio as typ

    eng = _engine()
    with eng.begin() as c:
        _raw(c, split_series_id("NVDA"), 1, NVDA_SPLIT, 10.0, datetime(2026, 11, 7, tzinfo=UTC))
        _raw(c, split_series_id("AVGO"), 1, date(2024, 7, 15), 10.0, datetime(2026, 11, 7, tzinfo=UTC))
    history = {
        "NVDA": [(date(2024, 6, 6), 1209.98), (date(2024, 6, 7), 1208.88),
                 (NVDA_SPLIT, 121.79), (date(2024, 6, 11), 120.91)],
        # Already adjusted upstream: no jump at the split, left alone.
        "AVGO": [(date(2024, 7, 11), 170.0), (date(2024, 7, 12), 171.0), (date(2024, 7, 15), 172.0)],
        "KO": [(date(2024, 6, 6), 60.0), (date(2024, 6, 7), 61.0)],
    }
    out = typ._apply_recorded_splits(eng, history)
    assert [v for _, v in out["NVDA"]] == pytest.approx([120.998, 120.888, 121.79, 120.91])
    assert out["AVGO"] == history["AVGO"]
    assert out["KO"] == history["KO"]
    assert typ._history_source_metadata(out)["split_basis"] == typ.SPLIT_BASIS


def test_ten_year_portfolio_split_read_failure_leaves_history(monkeypatch):
    from api.routers import ten_year_portfolio as typ

    class _Broken:
        def connect(self):
            raise RuntimeError("db down")

    history = {"NVDA": [(date(2024, 6, 7), 1208.88), (NVDA_SPLIT, 121.79)]}
    assert typ._apply_recorded_splits(_Broken(), history) == history


# ── pullers record split events ────────────────────────────────────────────

def test_tiingo_records_only_non_unit_split_factors():
    from ingestion.tiingo_pull import TiingoPuller

    puller = TiingoPuller.__new__(TiingoPuller)
    puller.source_id = 7
    df = pd.DataFrame({
        "date": [date(2024, 6, 7), NVDA_SPLIT, date(2024, 6, 11)],
        "splitFactor": [1.0, 10.0, 1.0],
    })
    assert puller._split_rows("NVDA", df) == [
        {"sid": "YF:NVDA:split_factor", "src": 7, "od": NVDA_SPLIT, "val": 10.0},
    ]
    assert puller._split_rows("NVDA", df.drop(columns=["splitFactor"])) == []


def test_yfinance_split_history_dry_run_and_additive_apply(monkeypatch):
    import ingestion.yfinance_pull as yp

    idx = pd.to_datetime(["2021-07-20", "2024-06-10"]).tz_localize("America/New_York")
    monkeypatch.setattr(yp.yf, "Ticker", lambda t: SimpleNamespace(splits=pd.Series([4.0, 10.0], index=idx)))
    eng = _engine()
    puller = yp.YFinancePuller.__new__(yp.YFinancePuller)
    puller.engine, puller.source_id = eng, 2

    dry = puller.pull_split_history("NVDA")
    assert dry["splits"] == [("2021-07-20", 4.0), ("2024-06-10", 10.0)]
    assert dry["rows_inserted"] == 0
    assert puller.pull_split_history("NVDA", apply=True)["rows_inserted"] == 2
    assert puller.pull_split_history("NVDA", apply=True)["rows_inserted"] == 0   # idempotent
    with eng.connect() as c:
        assert [(e.effective_date, e.ratio) for e in read_splits(c, "NVDA")] == [
            (date(2021, 7, 20), 4.0), (NVDA_SPLIT, 10.0),
        ]


def test_yfinance_split_history_provider_failure_is_reported(monkeypatch):
    import ingestion.yfinance_pull as yp

    def _boom(_t):
        raise ConnectionError("yahoo down")

    monkeypatch.setattr(yp.yf, "Ticker", _boom)
    puller = yp.YFinancePuller.__new__(yp.YFinancePuller)
    puller.engine, puller.source_id = None, 2
    assert puller.pull_split_history("NVDA")["status"] == "FAILED"
