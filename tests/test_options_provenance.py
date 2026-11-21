"""Schwab-captured options rows reach admin requests only (store/options_provenance.py).

SQLite runs the real fragments: it proves the predicate and the substitution
(an earlier non-Schwab day instead of the Schwab one), not PostgreSQL plans.
Offline: no provider, network or PostgreSQL access.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, text

from store import options_provenance as op

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_DAY = date(2026, 11, 20)      # Cboe capture
SCHWAB_DAY = date(2026, 11, 23)      # Schwab capture
UNKNOWN_DAY = date(2026, 11, 24)     # provider not recorded, after the cutoff
LEGACY_DAY = date(2026, 11, 10)      # provider not recorded, before Schwab existed

SCHEMA = (
    """CREATE TABLE options_capture_batches (
        capture_batch_id TEXT PRIMARY KEY, ticker TEXT, snap_date DATE, capture_ordinal INTEGER,
        capture_started_at TIMESTAMPTZ, capture_completed_at TIMESTAMPTZ, registered_at TIMESTAMPTZ,
        row_count INTEGER,
        spot_price REAL, capture_source TEXT, provider_regular_market_at TEXT, spot_provider TEXT,
        clock_metadata TEXT, backfilled BOOLEAN DEFAULT 0)""",
    """CREATE TABLE options_snapshots_all (
        ticker TEXT, snap_date TEXT, expiry TEXT, opt_type TEXT, strike REAL, last_price REAL,
        bid REAL, ask REAL, volume INTEGER, open_interest INTEGER, implied_vol REAL,
        in_the_money BOOLEAN, created_at TEXT, capture_batch_id TEXT, capture_ordinal INTEGER,
        capture_started_at TEXT, capture_completed_at TEXT, provider_regular_market_at TEXT)""",
    # Same shape as migrations/versions/options_append_only_20260930.py.
    """CREATE VIEW options_snapshots AS
        SELECT s.*, s.implied_vol AS implied_volatility FROM options_snapshots_all s
        WHERE NOT EXISTS (SELECT 1 FROM options_capture_batches b
            WHERE b.ticker = s.ticker AND b.snap_date = s.snap_date
              AND b.capture_batch_id IS NOT s.capture_batch_id
              AND b.capture_ordinal >= COALESCE(s.capture_ordinal, 0))""",
    """CREATE TABLE options_daily_signals (
        ticker TEXT, signal_date TEXT, put_call_ratio REAL, max_pain REAL, iv_skew REAL,
        total_oi INTEGER, total_volume INTEGER, near_expiry TEXT, spot_price REAL, iv_atm REAL,
        iv_25d_put REAL, iv_25d_call REAL, iv_m90_put_v1 REAL, iv_m110_call_v1 REAL,
        wing_iv_metadata TEXT, term_structure_slope REAL, oi_concentration REAL, created_at TEXT)""",
)


def _ts(day: date, hour: int = 15) -> str:
    return datetime(day.year, day.month, day.day, hour, tzinfo=timezone.utc).isoformat(sep=" ")


def _batch(conn: Any, batch_id: str, ticker: str, day: date, provider: str | None,
           ordinal: int = 1, metadata_provider: str | None = None) -> None:
    conn.execute(text(
        "INSERT INTO options_capture_batches (capture_batch_id, ticker, snap_date, capture_ordinal, "
        "capture_started_at, capture_completed_at, registered_at, row_count, spot_price, capture_source, "
        "spot_provider, clock_metadata) VALUES (:id, :t, :d, :o, :s, :c, :r, 2, :spot, 'daily_scheduler', "
        ":p, :m)"),
        {"id": batch_id, "t": ticker, "d": day.isoformat(), "o": ordinal, "s": _ts(day, 14),
         "c": _ts(day, 15), "r": _ts(day, 15), "spot": 100.0 + ordinal, "p": provider,
         "m": json.dumps({"provider": metadata_provider}) if metadata_provider else None})
    for opt_type in ("call", "put"):
        conn.execute(text(
            "INSERT INTO options_snapshots_all (ticker, snap_date, expiry, opt_type, strike, bid, ask, "
            "volume, open_interest, implied_vol, capture_batch_id, capture_ordinal) "
            "VALUES (:t, :d, :e, :ot, 100, 1, 2, 10, :oi, 0.2, :id, :o)"),
            {"t": ticker, "d": day.isoformat(), "e": (day + timedelta(days=30)).isoformat(),
             "ot": opt_type, "oi": 500 + ordinal, "id": batch_id, "o": ordinal})


def _signal(conn: Any, ticker: str, day: date, spot: float, source: str | None = None) -> None:
    conn.execute(text(
        "INSERT INTO options_daily_signals (ticker, signal_date, put_call_ratio, total_oi, spot_price, "
        "iv_atm, wing_iv_metadata, created_at) VALUES (:t, :d, 0.9, 5000, :spot, 0.25, :w, :c)"),
        {"t": ticker, "d": day.isoformat(), "spot": spot, "c": _ts(day, 16),
         "w": json.dumps({"source": source}) if source else None})


def _sqlite():
    """SQLite that returns DATE / TIMESTAMPTZ columns as date / aware datetime, like psycopg2."""
    sqlite3.register_converter("DATE", lambda raw: date.fromisoformat(raw.decode()))
    sqlite3.register_converter("TIMESTAMPTZ", lambda raw: datetime.fromisoformat(raw.decode()))
    return create_engine("sqlite://", connect_args={"detect_types": sqlite3.PARSE_DECLTYPES})


@pytest.fixture
def db():
    engine = _sqlite()
    with engine.begin() as conn:
        for ddl in SCHEMA:
            conn.execute(text(ddl))
        _batch(conn, "spy-cboe", "SPY", PUBLIC_DAY, "cboe")
        _signal(conn, "SPY", PUBLIC_DAY, 590.0, "cboe")
        _batch(conn, "spy-schwab", "SPY", SCHWAB_DAY, "schwab", metadata_provider="schwab")
        _signal(conn, "SPY", SCHWAB_DAY, 595.0, "schwab")
        _batch(conn, "qqq-unknown", "QQQ", UNKNOWN_DAY, None)
        _signal(conn, "QQQ", UNKNOWN_DAY, 500.0)
        _batch(conn, "qqq-legacy", "QQQ", LEGACY_DAY, None)
        _signal(conn, "QQQ", LEGACY_DAY, 480.0)
        # A non-core name read from Yahoo the same day as the Schwab capture.
        _batch(conn, "nuox-yahoo", "NUOX", SCHWAB_DAY, "yahoo")
        _signal(conn, "NUOX", SCHWAB_DAY, 7.0, "yahoo")
        # Wing metadata names Schwab but no batch row survived: still Schwab.
        _signal(conn, "IWM", SCHWAB_DAY, 230.0, "schwab")
    yield engine
    engine.dispose()


def _signal_days(engine: Any, include: Any) -> set[tuple[str, str]]:
    with engine.connect() as conn:
        rows = conn.execute(op.visible_sql(
            "SELECT ticker, signal_date FROM " + op.VISIBLE_DAILY_SIGNALS + " ods", include)).fetchall()
    return {(r[0], str(r[1])) for r in rows}


# ── The fragments ─────────────────────────────────────────────────────


CONSTANTS = {
    "VISIBLE_DAILY_SIGNALS": op.VISIBLE_DAILY_SIGNALS, "VISIBLE_SNAPSHOTS": op.VISIBLE_SNAPSHOTS,
    "VISIBLE_SNAPSHOTS_ALL": op.VISIBLE_SNAPSHOTS_ALL, "VISIBLE_CAPTURE_BATCHES": op.VISIBLE_CAPTURE_BATCHES,
    "VISIBLE_CAPTURE_BATCH_DAYS": op.VISIBLE_CAPTURE_BATCH_DAYS, "LATEST_DAILY_SIGNALS": op.LATEST_DAILY_SIGNALS,
    "LATEST_SIGNAL_EACH_TICKER": op.LATEST_SIGNAL_EACH_TICKER,
}


@pytest.mark.parametrize("name", sorted(CONSTANTS))
def test_fragments_bind_their_one_value_and_have_no_or_around_the_filter(name: str) -> None:
    fragment = CONSTANTS[name]
    assert set(re.findall(r"(?<!:):(\w+)", fragment)) == {"schwab_from"}, name
    assert "{" not in fragment and "%" not in fragment and ":include_schwab" not in fragment, name
    # The filter is a top-level NOT EXISTS / NOT (...) (an anti-join), never
    # "(:flag OR NOT ...)", which PostgreSQL cannot plan as one.
    assert not re.search(r"\(\s*\S+\s+OR\s+NOT\b", fragment), name


def test_no_constant_hides_inside_another_or_inside_an_admin_form() -> None:
    texts = list(CONSTANTS.values())
    for a in texts:
        for b in texts + list(op._ADMIN_FORM.values()):
            assert a is b or a not in b


PLAIN = {
    "VISIBLE_DAILY_SIGNALS": "options_daily_signals", "VISIBLE_SNAPSHOTS": "options_snapshots",
    "VISIBLE_SNAPSHOTS_ALL": "options_snapshots_all", "VISIBLE_CAPTURE_BATCHES": "options_capture_batches",
    "VISIBLE_CAPTURE_BATCH_DAYS": "options_capture_batches", "LATEST_DAILY_SIGNALS": op.LATEST_DAILY_SIGNALS_ALL,
    "LATEST_SIGNAL_EACH_TICKER": "options_daily_signals",
}


@pytest.mark.parametrize("name", sorted(CONSTANTS))
def test_admin_reads_the_plain_table_and_binds_nothing(name: str) -> None:
    sql = "SELECT * FROM " + CONSTANTS[name] + " x WHERE ticker = :t"
    plain = "SELECT * FROM " + PLAIN[name] + " x WHERE ticker = :t"
    assert op.visible_sql_text(sql, True) == plain
    assert op.visible_sql(sql, True).text == plain and ":schwab_from" not in plain
    assert op.visible_sql(sql, False).text == sql   # everyone else: the filtered SQL as written


@pytest.mark.parametrize("flag", [False, None, 1, "true", object()])
def test_only_an_explicit_true_admits_schwab_rows(flag: Any) -> None:
    assert op.include_schwab_rows(flag) is False
    assert op.visible_sql_text("SELECT 1 FROM " + op.VISIBLE_DAILY_SIGNALS + " ods", flag) != (
        "SELECT 1 FROM options_daily_signals ods")


def test_admin_sees_every_day_and_contributor_never_a_schwab_one(db) -> None:
    every = _signal_days(db, True)
    public = _signal_days(db, False)
    assert ("SPY", SCHWAB_DAY.isoformat()) in every and ("IWM", SCHWAB_DAY.isoformat()) in every
    assert public == {
        ("SPY", PUBLIC_DAY.isoformat()),        # Cboe day stays
        ("QQQ", LEGACY_DAY.isoformat()),        # unknown provider before Schwab existed stays
        ("NUOX", SCHWAB_DAY.isoformat()),       # Yahoo name on a Schwab day stays
    }
    # Unknown provider after the cutoff fails closed; wing metadata alone hides IWM.
    assert ("QQQ", UNKNOWN_DAY.isoformat()) not in public
    assert ("IWM", SCHWAB_DAY.isoformat()) not in public


def test_batches_are_filtered_per_batch_and_snapshots_per_day(db) -> None:
    with db.begin() as conn:
        _batch(conn, "spy-cboe-late", "SPY", SCHWAB_DAY, "cboe", ordinal=2)
    with db.connect() as conn:
        batches = {r[0] for r in conn.execute(op.visible_sql(
            "SELECT capture_batch_id FROM " + op.VISIBLE_CAPTURE_BATCHES + " b WHERE ticker = 'SPY'"))}
        batch_days = {r[0] for r in conn.execute(op.visible_sql(
            "SELECT capture_batch_id FROM " + op.VISIBLE_CAPTURE_BATCH_DAYS + " b WHERE ticker = 'SPY'"))}
        days = {str(r[0]) for r in conn.execute(op.visible_sql(
            "SELECT DISTINCT snap_date FROM " + op.VISIBLE_SNAPSHOTS_ALL + " s WHERE ticker = 'SPY'"))}
        latest_public = conn.execute(op.visible_sql(
            "SELECT MAX(snap_date) FROM " + op.VISIBLE_SNAPSHOTS + " s WHERE ticker = 'SPY'")).scalar()
        latest_admin = conn.execute(op.visible_sql(
            "SELECT MAX(snap_date) FROM " + op.VISIBLE_SNAPSHOTS + " s WHERE ticker = 'SPY'", True)).scalar()
    # The same-day Cboe batch is listed, but its day's contracts and signals stay
    # hidden, and a reader that picks a batch to load its chain skips that day.
    assert batches == {"spy-cboe", "spy-cboe-late"}
    assert batch_days == {"spy-cboe"}
    assert days == {PUBLIC_DAY.isoformat()}
    assert str(latest_public) == PUBLIC_DAY.isoformat() and str(latest_admin) == SCHWAB_DAY.isoformat()


@pytest.mark.parametrize(("provider", "metadata", "day", "schwab"), [
    ("schwab", None, LEGACY_DAY, True),
    ("cboe", {"provider": "schwab"}, PUBLIC_DAY, True),
    ("cboe", None, UNKNOWN_DAY, False),
    ("yahoo", None, UNKNOWN_DAY, False),
    (None, None, UNKNOWN_DAY, True),
    ("provider", None, UNKNOWN_DAY, True),
    (None, None, LEGACY_DAY, False),
])
def test_batch_rule(provider, metadata, day, schwab) -> None:
    engine = _sqlite()
    with engine.begin() as conn:
        conn.execute(text(SCHEMA[0]))
        conn.execute(text(
            "INSERT INTO options_capture_batches (capture_batch_id, ticker, snap_date, spot_provider, "
            "clock_metadata) VALUES ('b', 'SPY', :d, :p, :m)"),
            {"d": day.isoformat(), "p": provider, "m": json.dumps(metadata) if metadata else None})
        visible = conn.execute(op.visible_sql(
            "SELECT COUNT(*) FROM " + op.VISIBLE_CAPTURE_BATCHES + " b")).scalar()
    assert visible == (0 if schwab else 1)


# ── Auth: the viewer's role ───────────────────────────────────────────


def test_viewer_is_admin_reads_the_token_role(monkeypatch) -> None:
    import asyncio

    from fastapi.security import HTTPAuthorizationCredentials

    from api import auth

    def viewer(token: str | None) -> bool:
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token) if token else None
        return asyncio.run(auth.viewer_is_admin(creds))

    assert viewer(auth.create_token(role="admin", username="operator")) is True
    assert viewer(auth.create_token(role="contributor", username="dad")) is False
    assert viewer("not-a-jwt") is False
    assert viewer(None) is False
    assert auth.token_role(None) == "contributor"


# ── Routes ────────────────────────────────────────────────────────────


def test_options_signals_route(db, monkeypatch) -> None:
    from api.routers import options

    monkeypatch.setattr(options, "get_db_engine", lambda: db)
    admin = options.get_options_signals(ticker="SPY", limit=50, _token="t", include_schwab=True)
    contributor = options.get_options_signals(ticker="SPY", limit=50, _token="t", include_schwab=False)
    assert [s["signal_date"] for s in admin["signals"]] == [SCHWAB_DAY.isoformat(), PUBLIC_DAY.isoformat()]
    assert [s["signal_date"] for s in contributor["signals"]] == [PUBLIC_DAY.isoformat()]
    assert contributor["signals"][0]["spot_price"] == 590.0
    # Called as a plain function, the dependency default is not True: excluded.
    everyone = options.get_options_signals(ticker=None, limit=50, _token="t")
    assert {s["ticker"] for s in everyone["signals"]} == {"SPY", "QQQ", "NUOX"}
    assert all(s["signal_date"] != SCHWAB_DAY.isoformat() or s["ticker"] == "NUOX" for s in everyone["signals"])


def test_derivatives_batch_listing_and_heatmap(db, monkeypatch) -> None:
    from api.routers import derivatives

    monkeypatch.setattr(derivatives, "get_db_engine", lambda: db)
    admin = derivatives.get_options_batches("spy", snap_date=SCHWAB_DAY, include_schwab=True)
    contributor = derivatives.get_options_batches("spy", snap_date=SCHWAB_DAY, include_schwab=False)
    assert [b["capture_batch_id"] for b in admin["batches"]] == ["spy-schwab"]
    assert contributor["batches"] == []

    assert derivatives.get_oi_heatmap("spy", include_schwab=True)["snap_date"] == SCHWAB_DAY.isoformat()
    heat = derivatives.get_oi_heatmap("spy", include_schwab=False)
    assert heat["snap_date"] == PUBLIC_DAY.isoformat() and heat["heatmap"][0]["call_oi"] == 501


def test_gex_routes_hand_the_viewer_flag_to_the_engine(monkeypatch) -> None:
    from api.routers import derivatives

    seen: list[Any] = []

    class _Engine:
        def compute_latest_completed_gex_profile(self, ticker):
            return {"ticker": ticker}

        def compute_gex_profile(self, ticker, **_kw):
            return {"ticker": ticker, "error": "none"}

    def factory(include_schwab=False):
        seen.append(include_schwab)
        return _Engine()

    monkeypatch.setattr(derivatives, "_get_gex_engine", factory)
    derivatives.get_gex("spy", snap_date=None, capture_batch_id=None, include_schwab=True)
    derivatives.get_gex("spy", snap_date=None, capture_batch_id=None, include_schwab=False)
    derivatives.get_walls("spy", snap_date=None, capture_batch_id=None, include_schwab=False)
    derivatives.get_regime(include_schwab=False)
    assert seen == [True, False, False, False]


def test_dealer_gamma_selects_the_latest_non_schwab_batch(db) -> None:
    from physics.dealer_gamma import DealerGammaEngine

    cutoff = datetime(2026, 11, 25, tzinfo=timezone.utc)
    admin = DealerGammaEngine(db, include_schwab=True).latest_completed_capture_batch("SPY", UNKNOWN_DAY, cutoff)
    public = DealerGammaEngine(db).latest_completed_capture_batch("SPY", UNKNOWN_DAY, cutoff)
    assert admin == (SCHWAB_DAY, "spy-schwab")
    assert public == (PUBLIC_DAY, "spy-cboe")


# ── Consumers that are not requests ──────────────────────────────────


def test_oracle_publication_entry_price_skips_the_schwab_day(db) -> None:
    from oracle.publish import _measured_entry_price

    price, basis = _measured_entry_price(db, "SPY", SCHWAB_DAY)
    assert price == 590.0 and basis["observed_on"] == PUBLIC_DAY.isoformat()


def test_oracle_quote_reference_skips_the_schwab_batch(db) -> None:
    from oracle.entry_price_policy import load_quote_reference

    with db.connect() as conn:
        receipt = load_quote_reference(conn, "SPY", as_of=datetime(2026, 11, 23, 16, tzinfo=timezone.utc))
    assert receipt["capture_batch_id"] == "spy-cboe" and receipt["provider"] == "cboe"


def test_scanner_and_digest_never_read_a_schwab_day(db, monkeypatch) -> None:
    from alerts import hundredx_digest
    from discovery import options_scanner

    scanner = options_scanner.OptionsScanner(db)
    assert scanner.include_schwab is False
    assert set(scanner._get_available_tickers()) == {"SPY", "QQQ", "NUOX"}
    current = scanner._get_current_signals("SPY", SCHWAB_DAY)
    assert current["spot_price"] == 590.0 and str(current["signal_date"]) == PUBLIC_DAY.isoformat()
    admin = options_scanner.OptionsScanner(db, include_schwab=True)
    assert admin._get_current_signals("SPY", SCHWAB_DAY)["spot_price"] == 595.0

    built: list[dict[str, Any]] = []

    class _Scanner:
        def __init__(self, *args, **kwargs):
            built.append(kwargs)

        def scan_all(self):
            return []

    monkeypatch.setattr(hundredx_digest, "_digest_paused", lambda: None)
    monkeypatch.setattr(hundredx_digest, "_get_engine", lambda: db)
    monkeypatch.setattr(options_scanner, "OptionsScanner", _Scanner)
    assert hundredx_digest.run_100x_digest()["reason"] == "no_opportunities"
    assert built and all(kwargs.get("include_schwab") is not True for kwargs in built)


def test_digest_settlement_and_marks_skip_schwab_rows(db) -> None:
    from trading import options_tracker

    assert options_tracker._settlement_spot(db, "SPY", SCHWAB_DAY, SCHWAB_DAY) == (590.0, PUBLIC_DAY.isoformat())
    mark = options_tracker._contract_mark(db, "SPY", "CALL", PUBLIC_DAY + timedelta(days=30), 100.0,
                                          PUBLIC_DAY, UNKNOWN_DAY)
    assert mark == (1.5, PUBLIC_DAY.isoformat())


def test_chat_context_builds_the_gex_engine_without_schwab(monkeypatch) -> None:
    from api.routers import chat
    import physics.dealer_gamma as dealer_gamma

    built: list[dict[str, Any]] = []

    class _Engine:
        def __init__(self, *_a, **kwargs):
            built.append(kwargs)

        def compute_gex_profile(self, _ticker):
            return {"error": "none"}

    monkeypatch.setattr(chat, "_get_db_engine", lambda: object())
    monkeypatch.setattr(dealer_gamma, "DealerGammaEngine", _Engine)
    chat._gather_gex("SPY")
    assert built == [{"include_schwab": False}]


# ── Writer: Schwab captures stay out of resolved_series ──────────────


def test_provider_is_schwab_reads_either_field() -> None:
    assert op.provider_is_schwab("schwab")
    assert op.provider_is_schwab("cboe", {"provider": "Schwab"})
    assert op.provider_is_schwab(None, '{"provider": "schwab"}')
    assert not op.provider_is_schwab("cboe", None) and not op.provider_is_schwab(None, "not json")
