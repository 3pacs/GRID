"""The provisional record's availability cutoff is the session's close, not the next UTC midnight (#1312, Codex HOLD).

A delayed or ad hoc rerun after the close must not show post-close information as "known at the close". Every
read the record makes is cut at the session's close timestamp, each on the clock its store keeps:

* ``raw_series``: ``pull_timestamp`` (``store.observations.read_window_known_at(as_of_ts=...)``);
* ``fred_vintages``: ``fetched_at`` (``store.fred_vintages.load(fetched_by=...)``);
* ``dealer_gex_daily``: ``created_at``;
* resolved ``spy_full``: ``release_date`` before the session's day (the table has no write clock).

The decision record (no cutoff) is unchanged. Real ``compute_market_state`` on in-memory SQLite; no provider call.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    text,
)

from intelligence.regime import market_state as M
from store import fred_vintages as FV
from store.observations import PublicationLag, read_window_known_at

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("state_writer_cutoff", ROOT / "scripts/run_market_state.py")
W = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(W)

DAY, PREV = date(2026, 11, 20), date(2026, 11, 19)
CLOSE = datetime(2026, 11, 20, 21, 0, tzinfo=timezone.utc)
CBOE = 2


def z(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _bdays(end: date, n: int) -> list[date]:
    out, d = [], end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return sorted(out)


@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    md = MetaData()
    Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String, nullable=False))
    Table("raw_series", md,
          Column("series_id", String, nullable=False), Column("source_id", Integer, ForeignKey("source_catalog.id")),
          Column("obs_date", Date, nullable=False), Column("pull_timestamp", DateTime, nullable=False),
          Column("value", Float, nullable=False), Column("raw_payload", Text), Column("pull_status", String, nullable=False))
    Table("dealer_gex_daily", md, Column("obs_date", Date), Column("ticker", String), Column("spot_price", Float),
          Column("net_gex_usd_m", Float), Column("spot_to_flip_pct", Float), Column("gamma_flip_strike", Float),
          Column("gex_regime", String), Column("created_at", String))
    md.create_all(eng)
    FV.ensure_table(eng)
    with eng.begin() as c:
        c.execute(text("INSERT INTO source_catalog (id, name) VALUES (:i, :n)"), [{"i": CBOE, "n": "CBOE"}])
    # VIX through the previous session, every row pulled before the close.
    _raw(eng, [(d, 15.0 + (i % 7) * 0.5, "2026-11-20 05:00:00") for i, d in enumerate(_bdays(PREV, 300))])
    _gex(eng, PREV, "SHORT_GAMMA", "2026-11-19 22:00:00")
    return eng


def _raw(eng, rows, series="CBOE:VIX"):
    with eng.begin() as c:
        c.execute(text("INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, "
                       "pull_status) VALUES (:s, :src, :d, :ts, :v, '{}', 'SUCCESS')"),
                  [{"s": series, "src": CBOE, "d": d.isoformat(), "ts": ts, "v": v} for d, v, ts in rows])


def _gex(eng, day, regime, created_at):
    with eng.begin() as c:
        c.execute(text("INSERT INTO dealer_gex_daily VALUES (:d, 'SPY', 600.0, -500.0, -1.0, 606.0, :g, :c)"),
                  {"d": day.isoformat(), "g": regime, "c": created_at})


def _delayed_inputs(eng):
    """Everything here reached GRID after the 21:00Z close of 2026-11-20."""
    _raw(eng, [(DAY, 80.0, "2026-11-20 23:00:00"),            # the session's own print, pulled late
               (PREV, 79.0, "2026-11-20 23:00:00")])          # a revision of the previous print, pulled late
    _gex(eng, DAY, "LONG_GAMMA", "2026-11-20 22:30:00")       # the nightly dealer row for the session
    with eng.begin() as c:                                     # the nightly vintage refresh at 23:15Z
        c.execute(text("INSERT INTO fred_vintages (series_id, obs_date, realtime_start, realtime_end, value, "
                       "fetched_at) VALUES ('USEPUINDXD', :o, :rs, :re, 300.0, '2026-11-20 23:15:00')"),
                  {"o": PREV.isoformat(), "rs": DAY.isoformat(), "re": FV.OPEN_END.isoformat()})


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    import ingestion.fred_vintages as F

    for name in ("refresh", "backfill", "fetch"):
        monkeypatch.setattr(F, name, lambda *a, _n=name, **kw: pytest.fail(f"provisional run called {_n}"))
    monkeypatch.setattr(M, "load_calendar", dict)


def _provisional(engine, monkeypatch, capsys, now: str, *extra: str) -> dict:
    monkeypatch.setattr(W, "utc_now", lambda: z(now))
    assert W.main(["--provisional", "--json", *extra], engine) == 0     # no --no-refresh: it never refreshes
    capsys.readouterr()
    with engine.connect() as c:
        return json.loads(c.execute(text("SELECT record FROM market_state_provisional")).scalar_one())


def _facts(rec: dict) -> dict:
    return {"variables": rec["variables"], "gamma_state": rec["gamma_state"], "news_shock": rec["news_shock"],
            "coverage": rec["coverage"]}


# ── the delayed same-day input ───────────────────────────────────────────────

def test_a_delayed_same_day_input_does_not_change_the_provisional_record(engine, monkeypatch, capsys):
    at_close = _provisional(engine, monkeypatch, capsys, "2026-11-20T21:15:00Z")
    _delayed_inputs(engine)
    late = _provisional(engine, monkeypatch, capsys, "2026-11-20T23:30:00Z", "--replace")
    assert _facts(late) == _facts(at_close)
    vix = late["variables"]["vix"]
    assert vix["obs_date"] == "2026-11-19" and vix["value"] != 79.0 and vix["value"] != 80.0
    assert late["gamma_state"]["obs_date"] == "2026-11-19" and late["gamma_state"]["regime"] == "SHORT_GAMMA"
    assert late["variables"]["news_first_print"]["status"] == "unavailable"
    # The clocks are labelled: the cutoff is the close, the run is not.
    assert late["availability_cutoff"] == at_close["availability_cutoff"] == "2026-11-20T21:00:00+00:00"
    assert late["computed_at"] == "2026-11-20T23:30:00+00:00" and late["minutes_after_close"] == 150.0
    assert at_close["minutes_after_close"] == 15.0
    lineage = late["source_lineage"]["availability_cutoff"]
    assert lineage["at"] == "2026-11-20T21:00:00+00:00" and set(lineage["reads"]) == {
        "raw_series", "fred_vintages", "dealer_gex_daily", "resolved_series"}


def test_the_decision_record_still_reads_to_the_end_of_its_day(engine):
    """Control: without a cutoff (market_state_daily's rule) the same delayed inputs are read, as before."""
    _delayed_inputs(engine)
    rec = M.compute_market_state(engine, DAY, calendar={}).to_dict()
    assert rec["variables"]["vix"]["obs_date"] == "2026-11-20" and rec["variables"]["vix"]["value"] == 80.0
    assert rec["gamma_state"]["obs_date"] == "2026-11-20" and rec["gamma_state"]["regime"] == "LONG_GAMMA"
    assert rec["variables"]["news_first_print"]["status"] == "no_baseline"
    assert "availability_cutoff" not in rec["source_lineage"]
    assert "as_of_ts" not in rec["source_lineage"]["raw_series"]["CBOE:VIX"]


def test_compute_refuses_a_cutoff_that_is_naive_or_past_the_day(engine):
    with pytest.raises(ValueError, match="timezone-aware"):
        M.compute_market_state(engine, DAY, calendar={}, known_by=datetime(2026, 11, 20, 21, 0))  # noqa: DTZ001
    with pytest.raises(ValueError, match="before the end of"):
        M.compute_market_state(engine, DAY, calendar={}, known_by=z("2026-11-21T00:00:00Z"))


# ── the read paths, one by one ───────────────────────────────────────────────

def test_raw_reads_cut_at_the_instant_by_pull_and_by_modeled_lag(engine):
    lag = PublicationLag(1, "business", "next business day")
    _delayed_inputs(engine)
    with engine.connect() as c:
        day = read_window_known_at(c, "CBOE:VIX", as_of=DAY, lag=lag, source="CBOE")
        cut = read_window_known_at(c, "CBOE:VIX", as_of=DAY, lag=lag, source="CBOE", as_of_ts=CLOSE)
        late_cut = read_window_known_at(c, "CBOE:VIX", as_of=DAY, lag=lag, source="CBOE",
                                        as_of_ts=z("2026-11-20T23:59:59Z"))
    assert (day[-1].obs_date, day[-1].value) == (DAY, 80.0)
    assert cut[-1].obs_date == PREV and cut[-1].value not in (79.0, 80.0)
    assert len(day) == len(cut) + 1 and day[:-2] == cut[:-1]
    assert (late_cut[-1].obs_date, late_cut[-1].value) == (DAY, 80.0)
    # Modeled lag (backfilled history): a date-only clock counts by the end of its day, never earlier.
    _raw(engine, [(date(2026, 11, 18), 1.0, "2026-11-21 06:00:00")], series="CBOE:VIX3M")
    with engine.connect() as c:
        def known(ts):
            return [o.obs_date for o in read_window_known_at(c, "CBOE:VIX3M", as_of=DAY, lag=lag, source="CBOE",
                                                             as_of_ts=ts)]
        assert known(z("2026-11-19T23:59:59Z")) == []                 # modeled known 11-19: not before its end
        assert known(z("2026-11-20T00:00:00Z")) == [date(2026, 11, 18)]
        assert [o.obs_date for o in read_window_known_at(c, "CBOE:VIX3M", as_of=PREV, lag=lag,
                                                         source="CBOE")] == [date(2026, 11, 18)]
    with pytest.raises(ValueError):
        read_window_known_at(engine.connect(), "CBOE:VIX", as_of=DAY, lag=lag, as_of_ts=datetime(2026, 11, 20))  # noqa: DTZ001


def test_raw_cutoff_keeps_an_embargo_until_the_end_of_its_release_day(engine):
    lag = PublicationLag(1, "calendar", "x", release_overrides=((date(2026, 11, 1), DAY),))
    _raw(engine, [(date(2026, 11, 1), 4.2, "2026-11-20 12:00:00")], series="UNRATE")
    with engine.connect() as c:
        assert read_window_known_at(c, "UNRATE", as_of=DAY, lag=lag, source="CBOE", as_of_ts=CLOSE) == []
        assert len(read_window_known_at(c, "UNRATE", as_of=DAY, lag=lag, source="CBOE")) == 1


def test_vintages_cut_at_their_fetch_time(engine):
    _delayed_inputs(engine)
    assert len(FV.load(engine, "USEPUINDXD", PREV, DAY, DAY)) == 1
    assert FV.load(engine, "USEPUINDXD", PREV, DAY, DAY, fetched_by=CLOSE).empty
    assert len(FV.load(engine, "USEPUINDXD", PREV, DAY, DAY, fetched_by=z("2026-11-20T23:15:00Z"))) == 1
    with pytest.raises(ValueError, match="timezone-aware"):
        FV.load(engine, "USEPUINDXD", PREV, DAY, DAY, fetched_by=datetime(2026, 11, 20, 21))  # noqa: DTZ001


# ── vintages: a refresh extends realtime_end in place (Codex HOLD on #1360 at d58cb1ec) ─────────────────────────
#
# store.fred_vintages.upsert keeps a row's fetched_at and value but moves its realtime_end later whenever a refresh
# sees the version still live. A version GRID fetched before the close, stored with an end capped at that fetch's
# range, is therefore "extended" by the next evening's refresh. A read cut at the close must not change with it.

_O, _RS = date(2026, 11, 18), date(2026, 11, 19)          # EPU for 11-18, first printed 11-19


def _vintage_fetched_before_the_close(eng, end: date = date(2026, 11, 19)) -> None:
    with eng.begin() as c:
        c.execute(text("INSERT INTO fred_vintages (series_id, obs_date, realtime_start, realtime_end, value, "
                       "fetched_at) VALUES ('USEPUINDXD', :o, :rs, :re, 140.0, '2026-11-19 23:15:00')"),
                  {"o": _O.isoformat(), "rs": _RS.isoformat(), "re": end.isoformat()})


def _refresh_extends_it(eng) -> None:
    """What the 23:15Z refresh does: the real upsert, seeing the same version still live."""
    FV.upsert(eng, "USEPUINDXD", [{"obs_date": _O, "realtime_start": _RS, "realtime_end": FV.OPEN_END,
                                   "value": 140.0}])
    with eng.connect() as c:
        end, fetched = c.execute(text("SELECT realtime_end, fetched_at FROM fred_vintages WHERE obs_date = :o"),
                                 {"o": _O.isoformat()}).one()
    assert str(end)[:10] == "9999-12-31" and str(fetched).startswith("2026-11-19 23:15")   # in place, same clock


def test_a_late_in_place_extension_does_not_change_a_same_cutoff_vintage_read(engine):
    _vintage_fetched_before_the_close(engine)
    before = FV.load(engine, "USEPUINDXD", _O, DAY, DAY, fetched_by=CLOSE)
    seen_before = FV.values_as_of(before, [_O], DAY)
    _refresh_extends_it(engine)
    after = FV.load(engine, "USEPUINDXD", _O, DAY, DAY, fetched_by=CLOSE)
    assert after.equals(before) and FV.values_as_of(after, [_O], DAY) == seen_before == {_O: 140.0}
    # Without a cutoff (the decision record) the stored end is read, as before.
    assert FV.load(engine, "USEPUINDXD", _O, DAY, DAY)["realtime_end"].tolist() == [FV.OPEN_END]


def test_vintage_intervals_under_a_cutoff_come_from_the_versions_known_then(engine):
    """Each version ends the day before the next version GRID had fetched by the cutoff, else it is open."""
    with engine.begin() as c:
        c.execute(text("INSERT INTO fred_vintages (series_id, obs_date, realtime_start, realtime_end, value, fetched_at) "
                       "VALUES ('USEPUINDXD', '2026-11-10', '2026-11-11', '9999-12-31', 50.0, '2026-11-11 23:15:00'),"
                       " ('USEPUINDXD', '2026-11-10', '2026-11-14', '2026-11-16', 60.0, '2026-11-14 23:15:00'),"
                       " ('USEPUINDXD', '2026-11-10', '2026-11-17', '9999-12-31', 70.0, '2026-11-20 23:15:00')"))
    got = FV.load(engine, "USEPUINDXD", date(2026, 11, 10), DAY, DAY, fetched_by=CLOSE)
    assert [(str(r.realtime_start), str(r.realtime_end), r.value) for r in got.itertuples()] == [
        ("2026-11-11", "2026-11-13", 50.0),     # ended by the next known version, not its stored open end
        ("2026-11-14", "9999-12-31", 60.0)]     # the 11-17 version was fetched after the cutoff: open as known then
    assert FV.values_as_of(got, [date(2026, 11, 10)], DAY) == {date(2026, 11, 10): 60.0}
    assert FV.values_as_of(FV.load(engine, "USEPUINDXD", date(2026, 11, 10), DAY, DAY), [date(2026, 11, 10)],
                           DAY) == {date(2026, 11, 10): 70.0}


def test_a_late_in_place_extension_does_not_change_the_provisional_record(engine, monkeypatch, capsys):
    _vintage_fetched_before_the_close(engine)
    at_close = _provisional(engine, monkeypatch, capsys, "2026-11-20T21:15:00Z")
    _refresh_extends_it(engine)
    late = _provisional(engine, monkeypatch, capsys, "2026-11-20T23:30:00Z", "--replace")
    assert late["variables"]["news_first_print"] == at_close["variables"]["news_first_print"]
    assert late["news_shock"] == at_close["news_shock"]
    # The reading is the version GRID held at the close, for the session itself.
    assert late["variables"]["news_first_print"]["obs_date"] == "2026-11-20"
    assert late["variables"]["news_first_print"]["value"] == 140.0


def test_dealer_rows_cut_at_their_creation_time_and_unclocked_rows_are_not_known(engine):
    _delayed_inputs(engine)
    _gex(engine, date(2026, 11, 18), "SHORT_GAMMA", None)
    reads = M._Reads(engine, DAY, 30, known_by=CLOSE)
    assert [str(d) for d in reads.gex_rows().index] == ["2026-11-19"]
    assert [str(d) for d in M._Reads(engine, DAY, 30).gex_rows().index] == ["2026-11-18", "2026-11-19", "2026-11-20"]


def test_resolved_spy_counts_only_rows_released_before_the_session_day(monkeypatch):
    seen = {}

    def fake(engine, as_of, start, **kw):
        seen.update(as_of=as_of, **kw)

    import intelligence.regime.state_vector as SV

    monkeypatch.setattr(SV, "_fetch_resolved_spy_full", fake)
    reads = M._Reads(create_engine("sqlite://"), DAY, 30, known_by=CLOSE)
    reads.spy()
    assert seen == {"as_of": DAY, "release_as_of": PREV}
    seen.clear()
    M._Reads(create_engine("sqlite://"), DAY, 30).spy()
    assert seen == {"as_of": DAY}                               # the decision record's call is unchanged
