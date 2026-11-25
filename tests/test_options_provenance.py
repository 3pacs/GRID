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
    # Same shape as migrations/versions/options_append_only_20260930.py: no
    # ``implied_volatility`` alias (one here once hid a reader's bad column).
    """CREATE VIEW options_snapshots AS
        SELECT s.* FROM options_snapshots_all s
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
    "VISIBLE_SNAPSHOTS_ALL": op.VISIBLE_SNAPSHOTS_ALL, "VISIBLE_BATCH_SNAPSHOTS": op.VISIBLE_BATCH_SNAPSHOTS,
    "VISIBLE_CAPTURE_BATCHES": op.VISIBLE_CAPTURE_BATCHES,
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
    "VISIBLE_SNAPSHOTS_ALL": "options_snapshots_all", "VISIBLE_BATCH_SNAPSHOTS": "options_snapshots_all",
    "VISIBLE_CAPTURE_BATCHES": "options_capture_batches",
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


def test_visible_batch_snapshots_admits_only_public_batches_on_mixed_day(db) -> None:
    with db.begin() as conn:
        _batch(conn, "spy-yahoo-gem", "SPY", SCHWAB_DAY, "yahoo", ordinal=2)
        # An unbatched row (capture_batch_id IS NULL) must fail closed.
        conn.execute(text(
            "INSERT INTO options_snapshots_all (ticker, snap_date, expiry, opt_type, strike, bid, ask, "
            "volume, open_interest, implied_vol, capture_batch_id, capture_ordinal) "
            "VALUES ('SPY', :d, :e, 'call', 100, 1, 2, 10, 500, 0.2, NULL, NULL)"),
            {"d": PUBLIC_DAY.isoformat(), "e": (PUBLIC_DAY + timedelta(days=30)).isoformat()})
    with db.connect() as conn:
        public_batches = {r[0] for r in conn.execute(op.visible_sql(
            "SELECT DISTINCT capture_batch_id FROM " + op.VISIBLE_BATCH_SNAPSHOTS + " s"))}
        admin_batches = {r[0] for r in conn.execute(op.visible_sql(
            "SELECT DISTINCT capture_batch_id FROM " + op.VISIBLE_BATCH_SNAPSHOTS + " s", True))}
        # Day-level VISIBLE_SNAPSHOTS_ALL keeps its stricter day-wide rule unchanged.
        day_level = {str(r[0]) for r in conn.execute(op.visible_sql(
            "SELECT DISTINCT snap_date FROM " + op.VISIBLE_SNAPSHOTS_ALL + " s WHERE ticker = 'SPY'"))}
    assert public_batches == {"spy-cboe", "spy-yahoo-gem", "qqq-legacy", "nuox-yahoo"}
    assert None not in public_batches
    assert "spy-schwab" not in public_batches and "qqq-unknown" not in public_batches
    assert "spy-schwab" in admin_batches and "spy-yahoo-gem" in admin_batches
    assert day_level == {PUBLIC_DAY.isoformat()}


def test_dealer_gamma_reads_public_yahoo_batch_on_mixed_day() -> None:
    from physics.dealer_gamma import DealerGammaEngine

    engine = _sqlite()
    day = SCHWAB_DAY  # 2026-11-23, a regular NYSE session
    yahoo_id = "11111111-1111-4111-8111-111111111111"
    schwab_id = "22222222-2222-4222-8222-222222222222"
    started = datetime(day.year, day.month, day.day, 14, 35, tzinfo=timezone.utc)
    completed = datetime(day.year, day.month, day.day, 14, 40, tzinfo=timezone.utc)
    quote = datetime(day.year, day.month, day.day, 14, 39, tzinfo=timezone.utc)
    cutoff = datetime(day.year, day.month, day.day, 23, 59, tzinfo=timezone.utc)

    with engine.begin() as conn:
        for ddl in SCHEMA:
            # _load_chain validates aware datetime objects on options_snapshots_all timestamps (TIMESTAMPTZ on Postgres).
            if "CREATE TABLE options_snapshots_all" in ddl:
                ddl = (ddl
                       .replace("created_at TEXT", "created_at TIMESTAMPTZ")
                       .replace("capture_started_at TEXT", "capture_started_at TIMESTAMPTZ")
                       .replace("capture_completed_at TEXT", "capture_completed_at TIMESTAMPTZ")
                       .replace("provider_regular_market_at TEXT", "provider_regular_market_at TIMESTAMPTZ"))
            conn.execute(text(ddl))
        for bid, ordinal, provider, src, oi_base in (
            (yahoo_id, 1, "yahoo", "gem", 600),
            (schwab_id, 2, "schwab", "daily_scheduler", 900),
        ):
            conn.execute(text(
                "INSERT INTO options_capture_batches (capture_batch_id, ticker, snap_date, capture_ordinal, "
                "capture_started_at, capture_completed_at, registered_at, row_count, spot_price, capture_source, "
                "provider_regular_market_at, spot_provider, clock_metadata) "
                "VALUES (:id, 'SPY', :d, :o, :s, :c, :r, 2, 600.0, :src, :q, :p, :m)"),
                {"id": bid, "d": day.isoformat(), "o": ordinal, "s": started.isoformat(sep=" "),
                 "c": completed.isoformat(sep=" "), "r": completed.isoformat(sep=" "),
                 "src": src, "q": quote.isoformat(sep=" "), "p": provider,
                 "m": json.dumps({"provider": provider})})
            for opt_type, strike in (("call", 610.0), ("put", 590.0)):
                conn.execute(text(
                    "INSERT INTO options_snapshots_all (ticker, snap_date, expiry, opt_type, strike, bid, ask, "
                    "volume, open_interest, implied_vol, created_at, capture_batch_id, capture_ordinal, "
                    "capture_started_at, capture_completed_at, provider_regular_market_at) "
                    "VALUES ('SPY', :d, :e, :ot, :k, 1.0, 2.0, 10, :oi, 0.2, :cr, :id, :o, :s, :c, :q)"),
                    {"d": day.isoformat(), "e": date(2027, 1, 15).isoformat(), "ot": opt_type,
                     "k": strike, "oi": oi_base, "cr": completed.isoformat(sep=" "), "id": bid, "o": ordinal,
                     "s": started.isoformat(sep=" "), "c": completed.isoformat(sep=" "),
                     "q": quote.isoformat(sep=" ")})

    try:
        # Admin picks ordinal 2 (Schwab); public picks ordinal 1 (public Yahoo GEM batch).
        assert DealerGammaEngine(engine, include_schwab=True).latest_completed_capture_batch("SPY", day, cutoff) == (
            day, schwab_id)
        assert DealerGammaEngine(engine).latest_completed_capture_batch("SPY", day, cutoff) == (day, yahoo_id)

        # Public registered-batch replay loads the Yahoo batch and refuses the Schwab batch.
        pub_engine = DealerGammaEngine(engine)
        yahoo_chain = pub_engine._load_chain("SPY", day, capture_batch_id=yahoo_id, cutoff=cutoff)
        schwab_chain = pub_engine._load_chain("SPY", day, capture_batch_id=schwab_id, cutoff=cutoff)
        assert len(yahoo_chain) == 2 and set(yahoo_chain["open_interest"]) == {600}
        assert schwab_chain.empty
        # Default latest-batch view (capture_batch_id=None) stays day-level hidden.
        assert pub_engine._load_chain("SPY", day, capture_batch_id=None).empty
    finally:
        engine.dispose()


def test_gex_batch_replay_resolves_public_yahoo_batch_and_refuses_schwab_on_mixed_day() -> None:
    from paper_log.gex_batch_replay import BatchReplayError, resolve_batch

    # Plain SQLite so SQLAlchemy's .columns(snap_date=Date()) decodes ISO strings once.
    engine = create_engine("sqlite://")
    day = SCHWAB_DAY
    yahoo_id = "11111111-1111-4111-8111-111111111111"
    schwab_id = "22222222-2222-4222-8222-222222222222"
    with engine.begin() as conn:
        for ddl in SCHEMA:
            conn.execute(text(ddl))
        _batch(conn, yahoo_id, "SPY", day, "yahoo", ordinal=1)
        _batch(conn, schwab_id, "SPY", day, "schwab", ordinal=2, metadata_provider="schwab")
    try:
        resolved = resolve_batch(engine, "SPY", yahoo_id, 1)
        assert resolved.capture_batch_id == yahoo_id and resolved.snap_date == day
        with pytest.raises(BatchReplayError):
            resolve_batch(engine, "SPY", schwab_id, 2)
    finally:
        engine.dispose()


# ── Consumers that are not requests ──────────────────────────────────


def test_oracle_publication_entry_price_skips_the_schwab_day(db) -> None:
    from oracle.publish import _measured_entry_price

    price, basis = _measured_entry_price(db, "SPY", SCHWAB_DAY)
    assert price == 590.0 and basis["observed_on"] == PUBLIC_DAY.isoformat()


def test_oracle_quote_reference_skips_the_schwab_batch(db) -> None:
    """The default viewer (the API's tracking read) still never sees a Schwab batch."""
    from oracle.entry_price_policy import load_quote_reference

    with db.connect() as conn:
        receipt = load_quote_reference(conn, "SPY", as_of=datetime(2026, 11, 23, 16, tzinfo=timezone.utc))
    assert receipt["capture_batch_id"] == "spy-cboe" and receipt["provider"] == "cboe"


# ── The Oracle engine: the owner's private use (2026-11-23) ──────────


@pytest.fixture
def oracle_on(monkeypatch):
    """The operator switch on grid-hermes: the Oracle reads Schwab captures."""
    monkeypatch.setenv(op.ORACLE_SCHWAB_ENV, op.ORACLE_SCHWAB_ON)


@pytest.mark.parametrize("value,on", [(None, False), ("", False), ("1", False), ("true", False),
                                      ("admin-only", False), ("private-use", True), (" Private-Use ", True)])
def test_oracle_switch_is_off_unless_the_operator_sets_it(monkeypatch, value, on) -> None:
    if value is None:
        monkeypatch.delenv(op.ORACLE_SCHWAB_ENV, raising=False)
    else:
        monkeypatch.setenv(op.ORACLE_SCHWAB_ENV, value)
    assert op.oracle_reads_schwab() is on
    assert op.include_schwab_rows(op.oracle_reads_schwab()) is on


def _schwab_batch_is_newest(db: Any) -> None:
    """capture_ordinal is one global sequence in GRID: the later capture has the higher one."""
    with db.begin() as conn:
        conn.execute(text("UPDATE options_capture_batches SET capture_ordinal = 2 "
                          "WHERE capture_batch_id = 'spy-schwab'"))


def test_oracle_engine_quote_reference_reads_the_schwab_batch(db, oracle_on) -> None:
    from oracle.entry_price_policy import load_quote_reference

    _schwab_batch_is_newest(db)
    with db.connect() as conn:
        receipt = load_quote_reference(conn, "SPY", as_of=datetime(2026, 11, 23, 16, tzinfo=timezone.utc),
                                       include_schwab=op.oracle_reads_schwab())
    assert receipt["capture_batch_id"] == "spy-schwab" and receipt["provider"] == "schwab"
    assert receipt["observed_on"] == SCHWAB_DAY.isoformat()


@pytest.mark.parametrize("flag", [None, 1, "true", "admin-only"])
def test_quote_reference_flag_must_be_exactly_true(db, flag) -> None:
    from oracle.entry_price_policy import load_quote_reference

    _schwab_batch_is_newest(db)
    with db.connect() as conn:
        receipt = load_quote_reference(conn, "SPY", as_of=datetime(2026, 11, 23, 16, tzinfo=timezone.utc),
                                       include_schwab=flag)
    assert receipt["capture_batch_id"] == "spy-cboe"


def _bare_oracle(db: Any) -> Any:
    from oracle.engine import OracleEngine

    oracle = object.__new__(OracleEngine)  # skip __init__: no schema DDL, no model load
    oracle.engine = db
    return oracle


@pytest.mark.parametrize("switch,flag,batch", [(True, True, "spy-schwab"), (False, False, "spy-cboe")])
def test_oracle_engine_freezes_its_entry_quote_per_the_switch(db, monkeypatch, switch, flag, batch) -> None:
    import oracle.entry_price_policy as policy

    if switch:
        monkeypatch.setenv(op.ORACLE_SCHWAB_ENV, op.ORACLE_SCHWAB_ON)
    else:
        monkeypatch.delenv(op.ORACLE_SCHWAB_ENV, raising=False)
    seen: list[Any] = []
    real = policy.load_quote_reference

    def spy(conn, ticker, *, as_of, include_schwab=False):
        seen.append(include_schwab)
        return real(conn, ticker, as_of=as_of, include_schwab=include_schwab)

    monkeypatch.setattr(policy, "load_quote_reference", spy)
    _schwab_batch_is_newest(db)
    receipt = _bare_oracle(db)._get_entry_price_reference("SPY")
    assert seen == [flag]
    assert receipt["capture_batch_id"] == batch


class _NoFeatures:
    """The real SQLite engine, except the feature_registry read (``= ANY(:fams)`` is PostgreSQL)."""

    def __init__(self, real: Any) -> None:
        self.real = real
        self.sql: list[str] = []

    def connect(self):
        real, seen = self.real, self.sql

        class _Ctx:
            def __enter__(self_inner):
                self_inner.conn = real.connect()
                conn = self_inner.conn

                class _Conn:
                    def execute(self, stmt, params=None):
                        seen.append(str(stmt))
                        if "feature_registry" in str(stmt):
                            return type("Empty", (), {"fetchall": lambda _s: []})()
                        return conn.execute(stmt, params or {})

                return _Conn()

            def __exit__(self_inner, *exc):
                self_inner.conn.close()
                return False

        return _Ctx()


def _spy_schwab_pcr(db: Any) -> None:
    with db.begin() as conn:
        conn.execute(text("UPDATE options_daily_signals SET put_call_ratio = 1.3 "
                          "WHERE ticker = 'SPY' AND signal_date = :d"), {"d": SCHWAB_DAY.isoformat()})


def test_oracle_engine_options_signals_use_the_schwab_day(db, oracle_on) -> None:
    _spy_schwab_pcr(db)
    signals = _bare_oracle(_NoFeatures(db))._gather_signals("SPY", ["sentiment"])
    pcr = [s.value for s in signals if s.name == "pcr"]
    assert pcr == [1.3]  # the Schwab day's 1.3, not the Cboe day's 0.9


def test_oracle_engine_options_signals_stay_public_with_the_switch_off(db, monkeypatch) -> None:
    monkeypatch.delenv(op.ORACLE_SCHWAB_ENV, raising=False)
    _spy_schwab_pcr(db)
    signals = _bare_oracle(_NoFeatures(db))._gather_signals("SPY", ["sentiment"])
    assert [s.value for s in signals if s.name == "pcr"] == [0.9]


def test_options_read_carries_the_lineage_of_the_row_it_returns(db, oracle_on, monkeypatch) -> None:
    _spy_schwab_pcr(db)
    oracle = _bare_oracle(db)
    spy = oracle._read_options_signal("SPY")
    assert spy.values[0] == 1.3 and spy.schwab_private is True
    nuox = oracle._read_options_signal("NUOX")
    assert nuox.values is not None and nuox.schwab_private is False
    assert oracle._read_options_signal("NONE").values is None
    assert oracle._read_options_signal("NONE").schwab_private is False
    iwm = oracle._read_options_signal("IWM")  # wing metadata names Schwab, no batch row
    assert iwm.schwab_private is True
    monkeypatch.delenv(op.ORACLE_SCHWAB_ENV, raising=False)
    public = oracle._read_options_signal("SPY")  # switch off: the Cboe day, not private
    assert public.values[0] == 0.9 and public.schwab_private is False


def test_gathering_uses_the_read_the_tag_came_from(db, oracle_on) -> None:
    """A Schwab row that lands after the tagged read is not consumed by that pass."""
    from oracle.engine import OptionsSignalRead

    oracle = _bare_oracle(_NoFeatures(db))
    first = oracle._read_options_signal("NUOX")
    assert first.schwab_private is False
    later = SCHWAB_DAY + timedelta(days=2)
    with db.begin() as conn:  # a newer Schwab capture arrives mid-pass
        _batch(conn, "nuox-schwab", "NUOX", later, "schwab", ordinal=3)
        _signal(conn, "NUOX", later, 7.5, "schwab")
        conn.execute(text("UPDATE options_daily_signals SET put_call_ratio = 2.2 "
                          "WHERE ticker = 'NUOX' AND signal_date = :d"), {"d": later.isoformat()})
    oracle.engine.sql.clear()
    signals = oracle._gather_signals("NUOX", ["sentiment"], options_read=first)
    assert [s.value for s in signals if s.name == "pcr"] == [0.9]  # the tagged read, not 2.2
    assert not any("options_daily_signals" in sql for sql in oracle.engine.sql)
    fresh = oracle._read_options_signal("NUOX")
    assert fresh.values[0] == 2.2 and fresh.schwab_private is True
    supplied = OptionsSignalRead((2.2, 0.25, None, None, 7.5, 5000, None, None), True)
    signals = oracle._gather_signals("NUOX", ["sentiment"], options_read=supplied)
    assert [s.value for s in signals if s.name == "pcr"] == [2.2]


def test_only_the_oracle_engine_turns_on_the_private_use_flag() -> None:
    """oracle_reads_schwab() admits Schwab rows, so only the Oracle engine may use it in code."""
    import ast

    users: dict[str, int] = {}
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        if (rel.split("/")[0] in {"tests", "migrations", ".git", ".venv", "venv", "node_modules"}
                or "site-packages" in rel or rel == "store/options_provenance.py"):
            continue
        source = path.read_text(encoding="utf-8", errors="ignore")
        if "oracle_reads_schwab" not in source and "ORACLE_SCHWAB" not in source:
            continue
        uses = sum(1 for node in ast.walk(ast.parse(source))
                   if (isinstance(node, ast.Name) and node.id in {"oracle_reads_schwab", "ORACLE_SCHWAB_ON"})
                   or (isinstance(node, ast.Attribute)
                       and node.attr in {"oracle_reads_schwab", "ORACLE_SCHWAB_ON"}))
        if uses:
            users[rel] = uses
        assert "GRID_ORACLE_SCHWAB" not in source, f"{rel} reads the Oracle switch directly"
    # The read sites: the options-signal row (with its lineage) and the frozen entry quote.
    assert users == {"oracle/engine.py": 2}


# ── The engine tags what it built from Schwab rows ────────────────────


def _public_read() -> Any:
    from oracle.engine import OptionsSignalRead

    return OptionsSignalRead((0.9,) + (None,) * 7, False)


def _schwab_read() -> Any:
    from oracle.engine import OptionsSignalRead

    return OptionsSignalRead((1.3,) + (None,) * 7, True)


def test_schwab_receipt_tags_the_prediction(db) -> None:
    assert _bare_oracle(db)._schwab_private_inputs("NUOX", {"capture_batch_id": "spy-schwab"},
                                                   _public_read()) is True


def test_unqualified_schwab_receipt_still_tags(db) -> None:
    """An unavailable receipt keeps its price in flow_context, so its status does not matter."""
    receipt = {"status": "unavailable", "capture_batch_id": "spy-schwab", "price": 595.0}
    assert _bare_oracle(db)._schwab_private_inputs("NUOX", receipt, _public_read()) is True


def test_schwab_options_row_tags_even_with_a_public_receipt(db) -> None:
    assert _bare_oracle(db)._schwab_private_inputs("SPY", {"capture_batch_id": "spy-cboe"},
                                                   _schwab_read()) is True


def test_public_inputs_are_not_tagged(db) -> None:
    oracle = _bare_oracle(db)
    assert oracle._schwab_private_inputs("NUOX", {"capture_batch_id": "nuox-yahoo", "provider": "yahoo"},
                                         _public_read()) is False
    assert oracle._schwab_private_inputs("NUOX", {"status": "unavailable"}, _public_read()) is False


@pytest.mark.parametrize("receipt,options_read", [
    ({"capture_batch_id": "nuox-yahoo"}, None),                        # options read failed
    ({"price": 7.0, "status": "measured"}, "public"),                   # a price of unknown origin
    ({"provider": "schwab", "status": "unavailable"}, "public"),        # names Schwab, no batch
    ({"capture_batch_id": "nuox-yahoo", "provider": " Schwab "}, "public"),
    (None, "public"),                                                   # no receipt shape at all
])
def test_unknown_origin_tags(db, receipt, options_read) -> None:
    read = _public_read() if options_read == "public" else None
    assert _bare_oracle(db)._schwab_private_inputs("NUOX", receipt, read) is True


def test_schwab_input_check_fails_closed() -> None:
    class _Broken:
        def connect(self):
            raise RuntimeError("database down")

    assert _bare_oracle(_Broken())._schwab_private_inputs(
        "NUOX", {"capture_batch_id": "nuox-yahoo"}, _public_read()) is True


def _placeholder_oracle(db: Any, receipt: dict[str, Any]) -> Any:
    from types import SimpleNamespace

    oracle = _bare_oracle(db)
    oracle.models = [SimpleNamespace(name="m1", version="v1"), SimpleNamespace(name="m2", version="v1")]
    oracle._get_flow_context = lambda _t: {}
    oracle._get_entry_price_reference = lambda _t: receipt
    return oracle


def _tagged(predictions: list[Any]) -> list[bool]:
    return [p.flow_context.get(op.ORACLE_PRIVATE_USE_KEY) == op.ORACLE_PRIVATE_USE_SCHWAB for p in predictions]


NOW = datetime(2026, 11, 23, 16, tzinfo=timezone.utc)


def test_no_data_placeholders_carry_the_tag(db) -> None:
    oracle = _placeholder_oracle(db, {"status": "unavailable", "capture_batch_id": "spy-schwab", "price": 595.0,
                                      "reason": "quote is older than the 20-minute reference limit"})
    placeholders = oracle._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)
    assert _tagged(placeholders) == [True, True]


def test_public_placeholders_are_not_tagged(db, oracle_on) -> None:
    oracle = _placeholder_oracle(db, {"status": "unavailable", "capture_batch_id": "nuox-yahoo"})
    assert _tagged(oracle._oracle_one_ticker(1, "NUOX", 1, NOW, {}, horizon_days=1)) == [False, False]


def test_a_consumed_schwab_options_row_tags_with_a_public_receipt(db, oracle_on) -> None:
    oracle = _placeholder_oracle(db, {"status": "unavailable", "capture_batch_id": "spy-cboe"})
    assert _tagged(oracle._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)) == [True, True]


def test_switch_off_spy_pass_is_public(db, monkeypatch) -> None:
    monkeypatch.delenv(op.ORACLE_SCHWAB_ENV, raising=False)
    oracle = _placeholder_oracle(db, {"status": "unavailable", "capture_batch_id": "spy-cboe"})
    assert _tagged(oracle._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)) == [False, False]


def test_a_failed_options_read_tags_the_pass(db, oracle_on) -> None:
    oracle = _placeholder_oracle(db, {"status": "unavailable", "capture_batch_id": "nuox-yahoo"})

    def broken(_t):
        raise RuntimeError("database blip")

    oracle._read_options_signal = broken
    assert _tagged(oracle._oracle_one_ticker(1, "NUOX", 1, NOW, {}, horizon_days=1)) == [True, True]


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
