"""PostgreSQL contract: the bound-id ``resolved_series`` reads return what the name joins did.

2026-11-23 (griddb, read-only): reads that matched the feature through
``JOIN feature_registry fr ... WHERE fr.name = :n`` were planned for an
average feature and read every vintage of a large one before keeping a row
(120-970 ms per latest read). The readers now look the id up first and bind
it (``store/feature_ids.py``). Reads across several features take each
feature's newest row with a LATERAL subquery over the bound ids, and the
``LIKE`` related-feature reads compute each feature's ``MAX(obs_date)``
once (LATERAL) instead of once per row. Every PIT fence and ORDER BY of
the old statements is kept.

Each test seeds a disposable schema and runs the frozen legacy statement
next to the new code on the same rows. Where the function is cheap to call
on its own (claim_verifier, cross_reference, psi_model, physics.verify,
signal_executor, options_recommender._get_spot, vol_surface, dad, celestial
briefing), the production function is called. For statements embedded in
large endpoints, the new SQL is taken from the module's source (the string
literal inside the named function), so the test runs exactly what ships.

Ties: the legacy plain ``ORDER BY obs_date DESC LIMIT n`` reads do not order
vintages of one date, so which vintage they return depends on the plan. The
seed gives every vintage of a date the same value wherever such a read
looks, so the comparison is exact; the DISTINCT ON reads, which do order
vintages, get distinct vintage values.

Skips without ``GRID_TEST_DB_URL`` (a local throwaway database); fails
instead when ``REQUIRE_BOUND_ID_READS_PG=1`` (CI).
"""

from __future__ import annotations

import ast
import os
import pathlib
from datetime import date, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

pytestmark = pytest.mark.xdist_group("postgres")

_REQUIRE_ENV = "REQUIRE_BOUND_ID_READS_PG"
REPO = pathlib.Path(__file__).resolve().parent.parent

TODAY = date.today()
D0 = TODAY - timedelta(days=2)  # newest stored date for most features


def _d(n: int) -> date:
    return D0 - timedelta(days=n)


# ── fixture ───────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def pg():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required ({_REQUIRE_ENV}=1)")
        pytest.skip("GRID_TEST_DB_URL is required for the disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("bound-id reads contract requires a local disposable test database")

    schema = "bound_id_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE feature_registry (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL, "
            "family TEXT, model_eligible BOOLEAN DEFAULT TRUE)"
        ))
        conn.execute(text(
            "CREATE TABLE resolved_series (id SERIAL PRIMARY KEY, feature_id INTEGER NOT NULL, "
            "obs_date DATE NOT NULL, release_date DATE, vintage_date DATE NOT NULL, "
            "value DOUBLE PRECISION, UNIQUE (feature_id, obs_date, vintage_date))"
        ))
        # griddb's read indexes, so the new statements plan the way they do there.
        conn.execute(text(
            "CREATE INDEX ON resolved_series (feature_id, obs_date DESC)"
        ))
        conn.execute(text(
            "CREATE INDEX ON resolved_series (feature_id, obs_date, vintage_date DESC) "
            "INCLUDE (value, release_date) WHERE release_date IS NOT NULL"
        ))
        # vol_surface reads options_daily_signals (through the Schwab-visibility
        # filter) before resolved_series; empty here, so it falls through.
        conn.execute(text(
            "CREATE TABLE options_daily_signals (ticker TEXT, signal_date DATE, "
            "put_call_ratio DOUBLE PRECISION, spot_price DOUBLE PRECISION, "
            "max_pain DOUBLE PRECISION, iv_atm DOUBLE PRECISION, wing_iv_metadata JSONB)"
        ))
        conn.execute(text(
            "CREATE TABLE options_capture_batches (capture_batch_id TEXT PRIMARY KEY, ticker TEXT, "
            "snap_date DATE, spot_provider TEXT, clock_metadata JSONB)"
        ))
    _seed(engine)
    yield engine
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def _feature(conn, name: str, family: str = "equity", eligible: bool = True) -> int:
    return conn.execute(
        text("INSERT INTO feature_registry (name, family, model_eligible) VALUES (:n, :f, :e) RETURNING id"),
        {"n": name, "f": family, "e": eligible},
    ).scalar_one()


def _rows(conn, fid: int, rows: list[tuple[date, float | None]], vintages: int = 1,
          vintage_values: bool = False) -> None:
    """Insert *rows*; each date gets *vintages* vintages (vintage k one day later).

    Same value on every vintage unless *vintage_values* (then value + k/10).
    """
    for obs, value in rows:
        for k in range(vintages):
            v = obs + timedelta(days=k)
            val = None if value is None else value + (k / 10 if vintage_values else 0)
            conn.execute(
                text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                     "VALUES (:f, :o, :v, :v, :val)"),
                {"f": fid, "o": obs, "v": v, "val": val},
            )


def _seed(engine) -> None:
    with engine.begin() as conn:
        # A large feature: several vintages a date, equal values per date.
        _rows(conn, _feature(conn, "spy_full"), [(_d(i), 400.0 + i) for i in range(40)], vintages=3)
        _rows(conn, _feature(conn, "spy_pcr", "options"), [(_d(i), 1.0 + i / 100) for i in range(5)])
        _rows(conn, _feature(conn, "spy_rsi", "technical"), [(_d(i), 50.0 + i) for i in range(5)], vintages=2)
        _feature(conn, "spy_empty")  # registered, no rows
        # Two features behind one ticker, newest in different places.
        _rows(conn, _feature(conn, "aapl_full"), [(_d(0), 230.0), (_d(1), 229.0), (_d(35), 200.0)], vintages=2)
        _rows(conn, _feature(conn, "aapl"), [(_d(1), 228.5), (_d(32), 201.0)])
        # Distinct vintage values: for the DISTINCT ON reads.
        _rows(conn, _feature(conn, "xle_full"), [(_d(i), 90.0 + i) for i in range(6)], vintages=3,
              vintage_values=True)
        _rows(conn, _feature(conn, "xle_pcr", "options"), [(_d(0), 0.8)])
        # chat's value IS NOT NULL: the newest btc_usd_full row is NULL.
        _rows(conn, _feature(conn, "btc_usd_full"), [(_d(0), None), (_d(1), 50.0), (_d(2), 49.0)])
        _rows(conn, _feature(conn, "btc_full"), [(_d(2), 40.0)])
        # Two-name spot reads: {t}_close and bare {t}.
        _rows(conn, _feature(conn, "smh_close"), [(_d(i), 300.0 - i) for i in range(4)], vintages=2)
        _rows(conn, _feature(conn, "smh"), [(_d(10), 250.0)])
        # Single-name readers' features.
        for name, base in (("vix_spot", 18.0), ("ofr_financial_stress", -1.0), ("gold_full", 2400.0),
                           ("planetary_stress_index", 3.0), ("china_indpro_yoy", 5.0)):
            _rows(conn, _feature(conn, name, "macro"), [(_d(i), base + i / 10) for i in range(8)], vintages=3)
        # Celestial briefing: eligible features, a future-dated row, an ineligible one.
        _rows(conn, _feature(conn, "lunar_phase_t", "celestial"),
              [(TODAY + timedelta(days=20), 0.9), (_d(0), 0.4), (_d(1), 0.3)], vintages=2)
        _rows(conn, _feature(conn, "mercury_retro_t", "celestial"), [(_d(3), 1.0), (_d(4), 0.0)])
        _rows(conn, _feature(conn, "solar_cycle_t", "celestial", eligible=False), [(_d(0), 7.0)])


def _q(conn, sql: str, params: dict) -> list[tuple]:
    return [tuple(r) for r in conn.execute(text(sql), params).fetchall()]


def _ids(conn, names: list[str]) -> list[int]:
    from store.feature_ids import feature_ids

    return feature_ids(conn, names)


# ── the new statements, as they ship ──────────────────────────────────────


def _shipped_sql(module: str, function: str, marker: str) -> str:
    """The one string literal in *function* of *module* that contains *marker*."""
    tree = ast.parse((REPO / module).read_text(encoding="utf-8"))
    funcs = [n for n in ast.walk(tree)
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == function]
    assert funcs, f"{module}::{function} not found"
    found = [n.value for f in funcs for n in ast.walk(f)
             if isinstance(n, ast.Constant) and isinstance(n.value, str) and marker in n.value]
    assert len(found) == 1, f"{module}::{function}: {len(found)} literals contain {marker!r}"
    return found[0]


def _like_sql(module: str, function: str, tail_marker: str, n_patterns: int) -> str:
    head = _shipped_sql(module, function, "max_obs")
    tail = _shipped_sql(module, function, tail_marker)
    return head + " OR ".join(f"fr.name LIKE :p{i}" for i in range(n_patterns)) + tail


# ── legacy statements (verbatim from the code before 2026-11-23) ──────────

LEGACY_LATEST_BY_NAME = (
    "SELECT rs.value, rs.obs_date FROM resolved_series rs "
    "JOIN feature_registry fr ON fr.id = rs.feature_id "
    "WHERE fr.name = :name ORDER BY rs.obs_date DESC LIMIT 1"
)
LEGACY_LATEST_N_BY_NAME = (
    "SELECT rs.value FROM resolved_series rs "
    "JOIN feature_registry fr ON fr.id = rs.feature_id "
    "WHERE fr.name = :name ORDER BY rs.obs_date DESC LIMIT :n"
)
LEGACY_LATEST_LE_D = (
    "SELECT rs.value FROM resolved_series rs "
    "JOIN feature_registry fr ON rs.feature_id = fr.id "
    "WHERE fr.name = :name AND rs.obs_date <= :d ORDER BY rs.obs_date DESC LIMIT 1"
)
LEGACY_LATEST_LE_TODAY = (
    "SELECT rs.value, rs.obs_date FROM resolved_series rs "
    "JOIN feature_registry fr ON rs.feature_id = fr.id "
    "WHERE fr.name = :name AND rs.obs_date <= CURRENT_DATE ORDER BY rs.obs_date DESC LIMIT 1"
)
LEGACY_SIGNAL_EXECUTOR = (
    "SELECT DISTINCT ON (obs_date) obs_date, value FROM resolved_series rs "
    "JOIN feature_registry fr ON fr.id = rs.feature_id "
    "WHERE fr.name = :feature_name ORDER BY obs_date DESC, vintage_date DESC LIMIT :n"
)
LEGACY_SPOT_OR = (
    "SELECT rs.value FROM resolved_series rs JOIN feature_registry fr ON rs.feature_id = fr.id "
    "WHERE fr.name = :fname OR fr.name = :fname2 ORDER BY rs.obs_date DESC LIMIT 1"
)
LEGACY_SPOT_OR_LE_D = (
    "SELECT rs.value FROM resolved_series rs JOIN feature_registry fr ON rs.feature_id = fr.id "
    "WHERE (fr.name = :name1 OR fr.name = :name2) AND rs.obs_date <= :d "
    "ORDER BY rs.obs_date DESC LIMIT 1"
)
LEGACY_ANY_LATEST = (
    "SELECT rs.value, rs.obs_date FROM resolved_series rs "
    "JOIN feature_registry fr ON fr.id = rs.feature_id "
    "WHERE fr.name = ANY(:names) ORDER BY rs.obs_date DESC LIMIT 1"
)
LEGACY_ANY_PREV = (
    "SELECT rs.value FROM resolved_series rs "
    "JOIN feature_registry fr ON fr.id = rs.feature_id "
    "WHERE fr.name = ANY(:names) AND rs.obs_date <= :d30 ORDER BY rs.obs_date DESC LIMIT 1"
)
LEGACY_CHAT = (
    "SELECT rs.value FROM resolved_series rs "
    "JOIN feature_registry fr ON fr.id = rs.feature_id "
    "WHERE (fr.name = :n1 OR fr.name = :n2 OR fr.name = :n3) "
    "AND rs.value IS NOT NULL ORDER BY rs.obs_date DESC LIMIT 1"
)
LEGACY_QUOTE = (
    "WITH winner AS ("
    "  SELECT rs.feature_id FROM resolved_series rs "
    "  JOIN feature_registry fr ON fr.id = rs.feature_id "
    "  WHERE fr.name = ANY(:names) "
    "  ORDER BY rs.obs_date DESC, rs.vintage_date DESC LIMIT 1"
    ") "
    "SELECT DISTINCT ON (rs.obs_date) rs.value, rs.obs_date "
    "FROM resolved_series rs "
    "WHERE rs.feature_id = (SELECT feature_id FROM winner) "
    "ORDER BY rs.obs_date DESC, rs.vintage_date DESC LIMIT 2"
)


def _legacy_like(columns: str, n_patterns: int, tail: str) -> str:
    return (
        f"SELECT {columns} FROM resolved_series rs "
        "JOIN feature_registry fr ON fr.id = rs.feature_id "
        "WHERE (" + " OR ".join(f"fr.name LIKE :p{i}" for i in range(n_patterns)) + ") "
        "AND rs.obs_date = ("
        "  SELECT MAX(rs2.obs_date) FROM resolved_series rs2 "
        "  WHERE rs2.feature_id = rs.feature_id"
        ") " + tail
    )


LEGACY_CELESTIAL = (
    "SELECT fr.name, rs.value, rs.obs_date FROM resolved_series rs "
    "JOIN feature_registry fr ON fr.id = rs.feature_id "
    "WHERE fr.model_eligible = TRUE "
    "AND ("
    "  LOWER(fr.name) LIKE '%lunar%' OR LOWER(fr.name) LIKE '%moon%' "
    "  OR LOWER(fr.name) LIKE '%mercury%' OR LOWER(fr.name) LIKE '%jupiter%' "
    "  OR LOWER(fr.name) LIKE '%mars%' OR LOWER(fr.name) LIKE '%planetary%' "
    "  OR LOWER(fr.name) LIKE '%venus%' OR LOWER(fr.name) LIKE '%sunspot%' "
    "  OR LOWER(fr.name) LIKE '%solar%' OR LOWER(fr.name) LIKE '%geomagnetic%' "
    "  OR LOWER(fr.name) LIKE '%nakshatra%' OR LOWER(fr.name) LIKE '%tithi%' "
    "  OR LOWER(fr.name) LIKE '%rahu%' OR LOWER(fr.name) LIKE '%dasha%' "
    "  OR LOWER(fr.name) LIKE '%eclipse%' OR LOWER(fr.name) LIKE '%celestial%' "
    ") "
    "AND rs.obs_date <= CURRENT_DATE "
    "AND rs.obs_date = ("
    "  SELECT MAX(obs_date) FROM resolved_series "
    "  WHERE feature_id = rs.feature_id AND obs_date <= CURRENT_DATE"
    ") "
    "ORDER BY fr.name"
)

SINGLE_NAMES = ["spy_full", "vix_spot", "ofr_financial_stress", "gold_full", "spy_empty", "not_registered"]


# ── single-name readers: production functions vs the legacy name join ────


@pytest.mark.parametrize("name", SINGLE_NAMES)
def test_single_name_latest_readers_match_legacy(pg, name):
    from intelligence.cross_reference import _get_feature_value
    from oracle.claim_verifier import _lookup_feature_value
    from oracle.psi_model import _load_latest_value
    from physics.verify import MarketPhysicsVerifier

    with pg.connect() as conn:
        legacy = _q(conn, LEGACY_LATEST_BY_NAME, {"name": name})
        legacy_le_today = _q(conn, LEGACY_LATEST_LE_TODAY, {"name": name})
        legacy_le_d = _q(conn, LEGACY_LATEST_LE_D, {"name": name, "d": _d(3)})

    value, obs = _get_feature_value(pg, name)
    assert (value, obs) == ((legacy[0][0], legacy[0][1]) if legacy else (None, None))

    value, obs = _lookup_feature_value(pg, name)
    assert (value, obs) == ((legacy[0][0], str(legacy[0][1])) if legacy else (None, None))

    assert _load_latest_value(pg, name) == (legacy_le_today[0][0] if legacy_le_today else None)

    verifier = MarketPhysicsVerifier(pg, pit_store=None)
    assert verifier._get_latest_value(name, _d(3)) == (legacy_le_d[0][0] if legacy_le_d else None)


@pytest.mark.parametrize("ticker", ["spy", "btc", "aapl", "zzz"])
def test_claim_verifier_ticker_reads_match_legacy(pg, ticker):
    from oracle.claim_verifier import _lookup_latest_value, _lookup_price_change

    candidates = [ticker, f"{ticker}_full", f"{ticker}_usd_full"]
    with pg.connect() as conn:
        latest, change = (None, None, None), (None, None)
        for name in candidates:
            row = _q(conn, LEGACY_LATEST_BY_NAME, {"name": name})
            if row and row[0][0] is not None:
                latest = (float(row[0][0]), str(row[0][1]), name)
                break
        try:
            for name in candidates:
                rows = _q(conn, LEGACY_LATEST_N_BY_NAME, {"name": name, "n": 2})
                if len(rows) >= 2:
                    change = (float(rows[0][0]), float(rows[-1][0]))
                    break
        except TypeError:
            # A NULL value among the two rows: the function's except returns
            # (None, None) (btc_usd_full's newest row is NULL in the seed).
            change = (None, None)
    assert _lookup_latest_value(pg, ticker) == latest
    assert _lookup_price_change(pg, ticker, periods=2) == change


@pytest.mark.parametrize("name, n", [("xle_full", 2), ("xle_full", 1), ("spy_full", 2), ("missing", 2)])
def test_signal_executor_latest_prices_match_legacy(pg, name, n):
    from trading.signal_executor import _get_latest_prices

    with pg.connect() as conn:
        legacy = [(r[0], float(r[1])) for r in _q(conn, LEGACY_SIGNAL_EXECUTOR, {"feature_name": name, "n": n})]
        assert _get_latest_prices(conn, name, n) == legacy
    if name == "xle_full":
        # Distinct vintage values: the newest vintage of each date (vintage 3 -> +0.2).
        assert legacy[0] == (_d(0), pytest.approx(90.2))


@pytest.mark.parametrize("ticker", ["smh", "spy", "zzz"])
def test_two_name_spot_reads_match_legacy(pg, ticker):
    from analysis.vol_surface import VolSurfaceEngine
    from trading.options_recommender import OptionsRecommender

    with pg.connect() as conn:
        legacy = _q(conn, LEGACY_SPOT_OR, {"fname": f"{ticker}_close", "fname2": ticker})
        legacy_le_d = _q(conn, LEGACY_SPOT_OR_LE_D, {"name1": f"{ticker}_close", "name2": ticker, "d": _d(2)})

    assert OptionsRecommender._get_spot(None, pg, ticker) == (float(legacy[0][0]) if legacy else 0.0)
    expected = float(legacy_le_d[0][0]) if legacy_le_d else 0.0
    assert VolSurfaceEngine(pg)._get_spot_price(ticker.upper(), _d(2)) == expected


def test_research_agent_actor_read_matches_legacy(pg):
    sql = _shipped_sql("analysis/research_agent.py", "research_actor", ":fid")
    with pg.connect() as conn:
        for name in ("spy_full", "aapl_full", "gold_full"):
            (fid,) = _ids(conn, [name])
            assert _q(conn, sql, {"fid": fid}) == _q(conn, LEGACY_LATEST_BY_NAME, {"name": name})


# ── reads across several features: LATERAL over the bound ids ─────────────


def _watchlist_names(ticker: str) -> list[str]:
    from api.routers.watchlist_helpers import _resolve_feature_names

    return _resolve_feature_names(ticker)


@pytest.mark.parametrize("ticker", ["SPY", "AAPL", "BTC", "ZZZ"])
def test_multi_feature_latest_reads_match_legacy(pg, ticker):
    names = _watchlist_names(ticker)
    d30 = TODAY - timedelta(days=30)
    core_latest = _shipped_sql("api/routers/watchlist_core.py", "list_watchlist_enriched", "x.value, x.obs_date")
    core_prev = _shipped_sql("api/routers/watchlist_core.py", "list_watchlist_enriched", ":d30")
    overview = _shipped_sql("api/routers/watchlist_overview.py", "get_ticker_overview", "unnest(")
    alerts = _shipped_sql("api/routers/price_alerts.py", "_from_grid", "unnest(")
    with pg.connect() as conn:
        fids = _ids(conn, names)
        legacy_latest = _q(conn, LEGACY_ANY_LATEST, {"names": names})
        legacy_prev = _q(conn, LEGACY_ANY_PREV, {"names": names, "d30": d30})
        if not fids:
            # No registered candidate: the new code skips the read; the join found nothing.
            assert legacy_latest == legacy_prev == []
            return
        assert _q(conn, core_latest, {"fids": fids}) == legacy_latest
        assert _q(conn, overview, {"fids": fids}) == legacy_latest
        assert _q(conn, alerts, {"fids": fids}) == [r[:1] for r in legacy_latest]
        assert _q(conn, core_prev, {"fids": fids, "d30": d30}) == legacy_prev
    if ticker == "AAPL":
        # The newest row is aapl_full's, the 30-day-old one is the bare feature's.
        assert legacy_latest[0][0] == 230.0 and legacy_prev[0][0] == 201.0


@pytest.mark.parametrize("ticker", ["SPY", "AAPL", "XLE", "BTC", "ZZZ"])
def test_quote_winner_read_matches_legacy(pg, ticker):
    # DISTINCT ON ... vintage_date DESC orders vintages, so xle_full's distinct
    # vintage values are fair game here.
    names = _watchlist_names(ticker)
    quote = _shipped_sql("api/routers/watchlist_overview.py", "get_ticker_quote", "WITH winner AS")
    with pg.connect() as conn:
        fids = _ids(conn, names)
        legacy = _q(conn, LEGACY_QUOTE, {"names": names})
        assert (_q(conn, quote, {"fids": fids}) if fids else []) == legacy
    if ticker == "XLE":
        assert legacy == [(pytest.approx(90.2), _d(0)), (pytest.approx(91.2), _d(1))]


@pytest.mark.parametrize("ticker", ["btc", "spy", "aapl", "zzz"])
def test_chat_price_check_read_matches_legacy(pg, ticker):
    sql = _shipped_sql("api/routers/chat.py", "_sanity_check_llm_response", "unnest(")
    names = [f"{ticker}_full", f"{ticker}_usd_full", ticker]
    with pg.connect() as conn:
        legacy = _q(conn, LEGACY_CHAT, {"n1": names[0], "n2": names[1], "n3": names[2]})
        fids = _ids(conn, names)
        assert (_q(conn, sql, {"fids": fids}) if fids else []) == legacy
    if ticker == "btc":
        assert legacy == [(50.0,)]  # the NULL newest row is skipped


# ── LIKE related features: MAX(obs_date) per feature, once ───────────────


_LIKE_SITES = [
    # module, function, tail literal marker, legacy columns, legacy ORDER BY tail
    ("api/routers/watchlist_overview.py", "get_ticker_overview", "ORDER BY fr.name LIMIT 10",
     "fr.name, rs.value, rs.obs_date", "ORDER BY fr.name LIMIT 10"),
    ("api/routers/watchlist_analysis.py", "get_ticker_analysis", ") ORDER BY fr.name",
     "fr.name, fr.family, rs.value, rs.obs_date", "ORDER BY fr.name"),
    ("api/routers/watchlist_helpers.py", "_preload_one", ") ORDER BY fr.name",
     "fr.name, fr.family, rs.value, rs.obs_date", "ORDER BY fr.name"),
]


@pytest.mark.parametrize("site", _LIKE_SITES, ids=lambda s: s[0].rsplit("/", 1)[-1])
@pytest.mark.parametrize("patterns", [["spy%"], ["aapl%"], ["xle%", "lunar%"], ["zzz%"]])
def test_like_related_feature_reads_match_legacy(pg, site, patterns):
    module, function, tail_marker, columns, legacy_tail = site
    params = {f"p{i}": p for i, p in enumerate(patterns)}
    new_sql = _like_sql(module, function, tail_marker, len(patterns))
    with pg.connect() as conn:
        legacy = _q(conn, _legacy_like(columns, len(patterns), legacy_tail), params)
        new = _q(conn, new_sql, params)
    # ORDER BY fr.name leaves the vintages of one name unordered: same rows,
    # and the same name order.
    assert sorted(new, key=repr) == sorted(legacy, key=repr)
    assert [r[0] for r in new] == [r[0] for r in legacy]
    if patterns == ["spy%"]:
        # spy_empty has no rows; spy_rsi's newest date has two vintages.
        assert {r[0] for r in legacy} == {"spy_full", "spy_pcr", "spy_rsi"}
        assert sum(r[0] == "spy_rsi" for r in legacy) == 2


@pytest.mark.parametrize("ticker", ["SPY", "XLE", "ZZZ"])
def test_dad_market_context_features_match_legacy(pg, ticker):
    from api.routers.dad import _grid_market_context

    like = [f"{ticker.lower()}%"]
    names = _watchlist_names(ticker)
    if names:
        base = f"{names[0].rsplit('_', 1)[0]}%"
        if base not in like:
            like.append(base)
    params = {f"p{i}": p for i, p in enumerate(like)}
    with pg.connect() as conn:
        legacy = _q(conn, _legacy_like("fr.name, fr.family, rs.value, rs.obs_date", len(like),
                                       "ORDER BY fr.family, fr.name LIMIT :feature_limit"),
                    {**params, "feature_limit": 20})
    ctx = _grid_market_context(pg, ticker, feature_limit=20)
    got = [(f["name"], f["family"], f["value"]) for f in ctx["features"]]
    assert sorted(got, key=repr) == sorted([(r[0], r[1], r[2]) for r in legacy], key=repr)


def test_celestial_briefing_matches_legacy(pg):
    from ollama.celestial_briefing import _gather_celestial_state

    with pg.connect() as conn:
        legacy = _q(conn, LEGACY_CELESTIAL, {})
    state = _gather_celestial_state(pg)
    expected = {name: {"value": round(float(v), 4), "obs_date": str(d)} for name, v, d in legacy}
    got = state["features"]
    assert got == expected
    assert {"lunar_phase_t", "mercury_retro_t", "planetary_stress_index"} <= set(got)
    assert got["lunar_phase_t"]["obs_date"] == str(_d(0))  # the future row is not served
    assert "solar_cycle_t" not in got  # not model-eligible


# ── the id lookup itself ──────────────────────────────────────────────────


def test_feature_ids_keep_order_drop_unregistered_and_duplicates(pg):
    from store.feature_ids import feature_id, feature_ids

    with pg.connect() as conn:
        spy = feature_id(conn, "spy_full")
        aapl = feature_id(conn, "aapl")
        assert spy and aapl and spy != aapl
        assert feature_id(conn, "not_registered") is None
        assert feature_ids(conn, ["aapl", "nope", "spy_full", "aapl", ""]) == [aapl, spy]
        assert feature_ids(conn, []) == []
