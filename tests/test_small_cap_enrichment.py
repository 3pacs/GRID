"""``ingestion/altdata/small_cap_enrichment.py`` — runway math, source order
and degradation, JSONB merge upsert, SEC companyfacts parsing, Hermes pins.
No network (injected ``http_get``), no DB (MagicMock engine).
"""
from __future__ import annotations

import inspect
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

import ingestion.altdata.small_cap_enrichment as sce

AS_OF = date(2026, 9, 10)


# ── runway math ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "cash,burn,expected",
    [
        (120e6, 30e6, 12.0),          # 120 / (30/3) = 12 months
        (50e6, 7.5e6, 20.0),
        (100e6, 0.0, None),           # cash-flow positive -> no runway concept
        (100e6, None, None),
        (None, 30e6, None),
        (100e6, -5.0, None),
        (10.0, 1.0, 10.0),            # max(burn/3, 1) floor
        ("120000000", "30000000", 12.0),
    ],
)
def test_compute_runway_months(cash: Any, burn: Any, expected: float | None) -> None:
    assert sce.compute_runway_months(cash, burn) == expected


# ── SEC companyfacts parsing (fixture dict) ───────────────────────────────


def _dur(start: str, end: str, val: float, frame: str | None = None, form: str = "10-Q") -> dict[str, Any]:
    d = {"start": start, "end": end, "val": val, "form": form, "filed": end}
    if frame:
        d["frame"] = frame
    return d


def _inst(end: str, val: float, filed: str | None = None) -> dict[str, Any]:
    return {"end": end, "val": val, "form": "10-Q", "filed": filed or end}


_FACTS = {
    "entityName": "Smallex Bio Inc",
    "facts": {
        "dei": {
            "EntityCommonStockSharesOutstanding": {"units": {"shares": [
                _inst("2026-03-31", 48_000_000), _inst("2026-06-30", 50_000_000),
            ]}},
        },
        "us-gaap": {
            "CashAndCashEquivalentsAtCarryingValue": {"units": {"USD": [
                _inst("2025-12-31", 150e6), _inst("2026-06-30", 120e6), _inst("2026-03-31", 135e6),
            ]}},
            "LongTermDebt": {"units": {"USD": [_inst("2026-06-30", 25e6)]}},
            "Revenues": {"units": {"USD": [
                _dur("2025-01-01", "2025-12-31", 10e6, "CY2025", "10-K"),
                _dur("2025-07-01", "2025-09-30", 3e6, "CY2025Q3"),
                _dur("2025-10-01", "2025-12-31", 3e6, "CY2025Q4"),
                _dur("2026-01-01", "2026-03-31", 4e6, "CY2026Q1"),
                _dur("2026-04-01", "2026-06-30", 5e6, "CY2026Q2"),
            ]}},
            "NetIncomeLoss": {"units": {"USD": [
                _dur("2024-01-01", "2024-12-31", -90e6, "CY2024", "10-K"),
                _dur("2025-01-01", "2025-12-31", -110e6, "CY2025", "10-K"),
                _dur("2026-01-01", "2026-03-31", -30e6, "CY2026Q1"),  # only one quarter -> annual used
            ]}},
            "NetCashProvidedByUsedInOperatingActivities": {"units": {"USD": [
                _dur("2025-01-01", "2025-12-31", -100e6, "CY2025", "10-K"),
                _dur("2026-04-01", "2026-06-30", -27e6, "CY2026Q2"),
            ]}},
        },
    },
}


def test_parse_sec_companyfacts_fixture() -> None:
    out = sce.parse_sec_companyfacts(_FACTS)
    assert out["name"] == "Smallex Bio Inc"
    assert out["cash"] == 120e6 and out["fiscal_period_end"] == "2026-06-30"   # latest end, not list order
    assert out["shares_outstanding"] == 50_000_000
    assert out["total_debt"] == 25e6
    assert out["revenue_ttm"] == 15e6            # four consecutive quarters summed
    assert out["net_income_ttm"] == -110e6       # no 4 quarters -> latest annual
    assert out["quarterly_burn"] == 27e6         # latest quarter operating outflow, positive
    assert sce.compute_runway_months(out["cash"], out["quarterly_burn"]) == pytest.approx(13.3, abs=0.05)


def test_parse_sec_companyfacts_handles_missing_and_positive_cashflow() -> None:
    assert sce.parse_sec_companyfacts({}) == {
        "cash": None, "total_debt": None, "shares_outstanding": None, "revenue_ttm": None,
        "net_income_ttm": None, "quarterly_burn": None, "name": None, "fiscal_period_end": None,
    }
    facts = {"facts": {"us-gaap": {"NetCashProvidedByUsedInOperatingActivities": {"units": {"USD": [
        _dur("2026-04-01", "2026-06-30", 12e6, "CY2026Q2"),
    ]}}}}}
    assert sce.parse_sec_companyfacts(facts)["quarterly_burn"] == 0.0
    # annual-only operating cash flow -> quarterly burn = annual / 4
    facts = {"facts": {"us-gaap": {"NetCashProvidedByUsedInOperatingActivities": {"units": {"USD": [
        _dur("2025-01-01", "2025-12-31", -100e6, "CY2025", "10-K"),
    ]}}}}}
    assert sce.parse_sec_companyfacts(facts)["quarterly_burn"] == 25e6


# ── source order and degradation ──────────────────────────────────────────


def test_merge_sources_order_fmp_then_tiingo_then_sec() -> None:
    fmp = {"market_cap": 900e6, "cash": 100e6, "quarterly_burn": None, "sector": "Healthcare", "name": "Smallex"}
    sec = {"market_cap": None, "cash": 120e6, "quarterly_burn": 25e6, "revenue_ttm": 15e6, "name": "SMALLEX BIO INC",
           "fiscal_period_end": "2026-06-30"}
    merged = sce.merge_sources(fmp, 1.5e9, sec)
    assert merged["market_cap"] == 900e6          # FMP wins over Tiingo
    assert merged["cash"] == 100e6                # FMP wins over SEC
    assert merged["quarterly_burn"] == 25e6       # SEC fills the gap
    assert merged["revenue_ttm"] == 15e6 and merged["name"] == "Smallex"
    assert merged["cash_runway_months"] == 12.0   # derived from the merged pair
    assert merged["enrichment_source"] == "fmp,sec_xbrl" and merged["fiscal_period_end"] == "2026-06-30"

    merged = sce.merge_sources({}, 1.5e9, sec)
    assert merged["market_cap"] == 1.5e9 and merged["enrichment_source"] == "tiingo,sec_xbrl"
    assert sce.merge_sources({}, None, {})["enrichment_source"] is None


def test_shape_enrichment_row_truncates_description_and_stamps_source() -> None:
    now = datetime(2026, 9, 10, 6, tzinfo=timezone.utc)
    row = sce.shape_enrichment_row("smlx", {"market_cap": 9e8, "description": "x" * 1000, "sector": "Healthcare",
                                            "name": "Smallex", "enrichment_source": "fmp"}, now=now)
    assert row is not None and row["ticker"] == "SMLX" and row["name"] == "Smallex" and row["sector"] == "Healthcare"
    assert len(row["profile"]["description"]) == sce.DESCRIPTION_MAX == 400
    assert row["profile"]["enriched_at"] == now.isoformat() and row["profile"]["enrichment_source"] == "fmp"
    assert set(sce.ENRICHED_FIELDS) <= set(row["profile"])
    assert sce.shape_enrichment_row("SMLX", {f: None for f in sce.ENRICHED_FIELDS}) is None
    assert sce.shape_enrichment_row("", {"market_cap": 1.0}) is None


def _puller(api_key: str = "", http_get: Any = None) -> tuple[sce.SmallCapEnrichmentPuller, MagicMock]:
    engine = MagicMock()
    p = sce.SmallCapEnrichmentPuller(engine, api_key=api_key, http_get=http_get or MagicMock(), sleep=lambda s: None)
    return p, engine


def test_no_fmp_key_means_fmp_is_never_constructed() -> None:
    p, _ = _puller(api_key="")
    assert p.fmp is None and p.fmp_fields("SMLX") == {} and p._fmp_calls == 0


def test_sec_fields_degrade_on_network_failure_or_unknown_cik(monkeypatch: pytest.MonkeyPatch) -> None:
    import grid.signals.sponsor_resolver as sr

    monkeypatch.setattr(sr, "sec_cik_for_ticker", lambda t: "0000000001" if t == "SMLX" else None)
    boom = MagicMock(side_effect=ConnectionError("sec down"))
    p, _ = _puller(http_get=boom)
    assert p.sec_fields("SMLX") == {}
    assert p.sec_fields("NOCIK") == {} and boom.call_count == 1  # no CIK -> no request

    resp = MagicMock(status_code=200)
    resp.json.return_value = _FACTS
    ok = MagicMock(return_value=resp)
    p, _ = _puller(http_get=ok)
    out = p.sec_fields("SMLX")
    assert out["cash"] == 120e6
    assert ok.call_args.args[0] == "https://data.sec.gov/api/xbrl/companyfacts/CIK0000000001.json"
    assert ok.call_args.kwargs["headers"]["User-Agent"] == "GRID Intelligence ops@stepdad.finance"

    resp.status_code = 403
    assert p.sec_fields("SMLX") == {}


def test_enrich_ticker_uses_fmp_then_sec_for_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    p, engine = _puller(api_key="k")
    fmp = MagicMock()
    fmp.pull_profile.return_value = {"companyName": "Smallex", "sector": "Healthcare", "industry": "Biotech", "description": "d", "mktCap": 950e6}
    fmp.pull_quote.return_value = {"marketCap": 900e6, "sharesOutstanding": 50e6}
    fmp.pull_balance_sheet.return_value = [{"cashAndShortTermInvestments": 100e6, "totalDebt": 5e6}]
    fmp.pull_cash_flow.return_value = [{"operatingCashFlow": -30e6}]
    fmp.pull_income_statement.return_value = [{"revenue": 1e6, "netIncome": -20e6}] * 4
    p._fmp = fmp
    monkeypatch.setattr(p, "sec_fields", MagicMock(side_effect=AssertionError("SEC not needed when FMP is complete")))
    row = p.enrich_ticker("SMLX", AS_OF)
    assert row is not None
    prof = row["profile"]
    assert prof["market_cap"] == 900e6 and prof["cash"] == 100e6 and prof["quarterly_burn"] == 30e6
    assert prof["revenue_ttm"] == 4e6 and prof["net_income_ttm"] == -80e6 and prof["cash_runway_months"] == 10.0
    assert prof["enrichment_source"] == "fmp" and p._fmp_calls == 5
    engine.connect.assert_not_called()  # Tiingo not consulted when FMP has the cap

    # FMP degraded (403 -> []) -> SEC fills, Tiingo consulted for cap
    fmp.pull_profile.return_value = None
    fmp.pull_quote.return_value = None
    fmp.pull_balance_sheet.return_value = []
    fmp.pull_cash_flow.return_value = []
    fmp.pull_income_statement.return_value = []
    monkeypatch.setattr(p, "sec_fields", lambda t: sce.parse_sec_companyfacts(_FACTS))
    monkeypatch.setattr(p, "tiingo_market_cap", lambda t, as_of: 1.1e9)
    row = p.enrich_ticker("SMLX", AS_OF)
    prof = row["profile"]
    assert prof["market_cap"] == 1.1e9 and prof["cash"] == 120e6 and prof["enrichment_source"] == "tiingo,sec_xbrl"
    assert row["name"] == "Smallex Bio Inc"


def test_fmp_budget_stops_calls() -> None:
    p, _ = _puller(api_key="k")
    p._fmp = MagicMock()
    p._fmp_calls = sce.FMP_CALL_BUDGET
    out = p.fmp_fields("SMLX")
    assert out["market_cap"] is None and p._fmp.pull_profile.call_count == 0


def test_tiingo_market_cap_reads_pit_bounded_series() -> None:
    p, engine = _puller()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.first.return_value = (2.2e9,)
    assert p.tiingo_market_cap("AAPL", AS_OF) == 2.2e9
    stmt, params = conn.execute.call_args.args
    assert "pull_status = 'SUCCESS'" in str(stmt) and "obs_date <= :as_of" in str(stmt)
    assert params == {"sid": "TIINGO_FUND:AAPL:market_cap", "as_of": AS_OF}
    conn.execute.side_effect = RuntimeError("db down")
    assert p.tiingo_market_cap("AAPL", AS_OF) is None


# ── universe ──────────────────────────────────────────────────────────────


def test_universe_filters_known_large_caps_and_fresh_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    p, _ = _puller()
    monkeypatch.setattr(p, "_distinct", lambda sql, params: {"BIG", "FRESH", "SMLX", "UNKN"} if "trial_signals" in str(sql) else {"OPTX"})
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(p, "known_caps", lambda tickers, as_of: (
        {"BIG": 5e9, "SMLX": 9e8}, {"FRESH": now - timedelta(hours=2), "SMLX": now - timedelta(hours=30)},
    ))
    assert p.universe(AS_OF) == ["OPTX", "SMLX", "UNKN"]
    assert p.universe(AS_OF, force=True) == ["FRESH", "OPTX", "SMLX", "UNKN"]


def test_universe_queries_are_parameterised_and_windowed() -> None:
    p, engine = _puller()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = [("smlx",), ("not a ticker",), (None,)]
    tickers = p.universe(AS_OF, force=True)
    assert tickers == ["SMLX"]
    sqls = [(str(c.args[0]), c.args[1]) for c in conn.execute.call_args_list]
    trial = next(p for s, p in sqls if "trial_signals" in s)
    assert trial["start"].date() == AS_OF - timedelta(days=sce.TRIAL_LOOKBACK_DAYS)
    options = next(p for s, p in sqls if "options_mispricing_scans" in s)
    assert options["start"] == AS_OF - timedelta(days=sce.OPTIONS_LOOKBACK_DAYS)
    calendar = next(p for s, p in sqls if "catalyst_calendar" in s)
    assert calendar == {"shape": r"^[A-Z.\-]{1,6}$"}
    for s, _ in sqls:
        assert "%" not in s or "profile->>" in s  # no interpolation; only jsonb operators


# ── JSONB merge upsert ────────────────────────────────────────────────────


def test_pull_all_upserts_with_jsonb_merge(monkeypatch: pytest.MonkeyPatch) -> None:
    p, engine = _puller()
    monkeypatch.setattr(sce, "ensure_table", lambda e: None)
    now = datetime(2026, 9, 10, 6, tzinfo=timezone.utc)
    rows = {
        "SMLX": sce.shape_enrichment_row("SMLX", {"market_cap": 9e8, "cash": 1e8, "quarterly_burn": 2.5e7,
                                                  "cash_runway_months": 12.0, "enrichment_source": "sec_xbrl", "name": "Smallex"}, now=now),
        "NODATA": None,
    }
    monkeypatch.setattr(p, "enrich_ticker", lambda t, as_of=None: rows[t])
    out = p.pull_all(["smlx", "NODATA"])
    assert out["status"] == "SUCCESS" and out["tickers_attempted"] == 2
    assert out["rows_upserted"] == 1 and out["skipped_no_data"] == 1 and out["sources"] == {"sec_xbrl": 1}
    conn = engine.begin.return_value.__enter__.return_value
    stmt, params = conn.execute.call_args.args
    sql = str(stmt)
    assert "INSERT INTO company_profiles" in sql and "ON CONFLICT (ticker) DO UPDATE" in sql
    assert "COALESCE(company_profiles.profile, '{}'::jsonb) || EXCLUDED.profile" in sql  # merge, not replace
    assert "name = COALESCE(EXCLUDED.name, company_profiles.name)" in sql
    for bind in (":ticker", ":name", ":sector", ":profile", ":last_analyzed"):
        assert bind in sql
    profile = json.loads(params["profile"])
    assert profile["market_cap"] == 9e8 and profile["cash_runway_months"] == 12.0
    assert profile["enriched_at"] == now.isoformat() and profile["enrichment_source"] == "sec_xbrl"
    assert params["ticker"] == "SMLX" and params["name"] == "Smallex"


def test_pull_all_isolates_per_ticker_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    p, engine = _puller()
    monkeypatch.setattr(sce, "ensure_table", lambda e: None)

    def enrich(t: str, as_of: Any = None) -> Any:
        if t == "BAD":
            raise RuntimeError("boom")
        return sce.shape_enrichment_row(t, {"market_cap": 1e8, "enrichment_source": "fmp"})

    monkeypatch.setattr(p, "enrich_ticker", enrich)
    out = p.pull_all(["BAD", "OK"])
    assert out["rows_upserted"] == 1 and out["errors"] == ["BAD: boom"] and out["status"] == "SUCCESS"
    assert p.pull_all([])["tickers_attempted"] == 0


def test_module_pull_all_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    class Boom:
        def __init__(self, *a: Any, **k: Any) -> None:
            raise RuntimeError("ctor")

    monkeypatch.setattr(sce, "SmallCapEnrichmentPuller", Boom)
    assert sce.pull_all(MagicMock())["status"] == "FAILED"
    assert "engine" in inspect.signature(sce.pull_all).parameters  # _FunctionPuller injects by name


# ── Hermes wiring pins ────────────────────────────────────────────────────


def test_hermes_registry_pins() -> None:
    from scripts import hermes_operator as ho

    assert ho._SOURCE_REGISTRY["small_cap_enrichment"] == {
        "mod": "ingestion.altdata.small_cap_enrichment", "fn": "pull_all", "interval_h": 24,
    }
    assert ho._SOURCE_REGISTRY["company_profiles_puller"] == {
        "mod": "ingestion.altdata.company_profiles_puller", "cls": "CompanyProfilesPuller",
        "pull_method": "pull", "interval_h": 24,
    }
    keys = list(ho._SOURCE_EXTRAS)
    assert keys.index("trial_signal") < keys.index("small_cap_enrichment")


# ── derived market cap + SEC sector (FMP v3 deprecated) ───────────────────


@pytest.mark.parametrize(
    "shares,price,expected",
    [
        (50_000_000, 12.5, 625_000_000.0),
        ("50000000", "12.5", 625_000_000.0),
        (None, 12.5, None),
        (50_000_000, None, None),
        (0, 12.5, None),
        (50_000_000, -1.0, None),
        (float("nan"), 12.5, None),
        ("abc", 12.5, None),
    ],
)
def test_derive_market_cap(shares: Any, price: Any, expected: float | None) -> None:
    assert sce.derive_market_cap(shares, price) == expected


@pytest.mark.parametrize(
    "sic,expected",
    [
        (2834, "Healthcare"), ("2836", "Healthcare"), (3841, "Healthcare"), (8071, "Healthcare"),
        (7372, "Technology"), (3674, "Technology"), (1090, "Materials"), (1311, "Energy"),
        (2860, "Materials"), (3711, "Industrials"), (4911, "Utilities"), (6770, "Financials"),
        (9999, None), (None, None), ("", None), ("n/a", None),
    ],
)
def test_sector_from_sic(sic: Any, expected: str | None) -> None:
    assert sce.sector_from_sic(sic) == expected


def test_sec_submissions_maps_sic_to_sector_and_degrades(monkeypatch: pytest.MonkeyPatch) -> None:
    import grid.signals.sponsor_resolver as sr

    monkeypatch.setattr(sr, "sec_cik_for_ticker", lambda t: "0000000001" if t == "SMLX" else None)
    resp = MagicMock(status_code=200)
    resp.json.return_value = {"sic": "2834", "sicDescription": "PHARMACEUTICAL PREPARATIONS", "name": "SMALLEX BIO INC"}
    ok = MagicMock(return_value=resp)
    p, _ = _puller(http_get=ok)
    out = p.sec_submissions("SMLX")
    assert out == {
        "sic": "2834", "sic_description": "PHARMACEUTICAL PREPARATIONS", "sector": "Healthcare",
        "industry": "Pharmaceutical Preparations", "name": "Smallex Bio Inc",
    }
    assert ok.call_args.args[0] == "https://data.sec.gov/submissions/CIK0000000001.json"
    assert ok.call_args.kwargs["headers"]["User-Agent"] == sce.SEC_UA

    assert p.sec_submissions("NOCIK") == {} and ok.call_count == 1   # no CIK -> no request
    resp.status_code = 429
    assert p.sec_submissions("SMLX") == {}
    boom = MagicMock(side_effect=ConnectionError("sec down"))
    p, _ = _puller(http_get=boom)
    assert p.sec_submissions("SMLX") == {}


def test_latest_price_is_pit_bounded_over_yf_and_tiingo_closes() -> None:
    p, engine = _puller()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.first.return_value = (12.5,)
    assert p.latest_price("smlx", AS_OF) == 12.5
    stmt, params = conn.execute.call_args.args
    sql = str(stmt)
    assert "series_id = ANY(:series_ids)" in sql and "obs_date <= :as_of" in sql and "pull_timestamp <= :as_of_ts" in sql
    assert "%" not in sql and "format(" not in sql
    assert params["series_ids"] == ["YF:SMLX:adj_close", "YF:SMLX:close", "TIINGO:SMLX:adj_close", "TIINGO:SMLX:close"]
    assert params["as_of"] == AS_OF and params["as_of_ts"].date() == AS_OF and params["as_of_ts"].tzinfo is not None
    conn.execute.return_value.first.return_value = None
    assert p.latest_price("SMLX", AS_OF) is None
    conn.execute.side_effect = RuntimeError("db down")
    assert p.latest_price("SMLX", AS_OF) is None


def test_enrich_ticker_derives_cap_from_sec_shares_and_sector_from_submissions(monkeypatch: pytest.MonkeyPatch) -> None:
    """No FMP key, no Tiingo cap: cap = SEC shares × PIT close; sector/industry from SEC SIC."""
    p, _ = _puller(api_key="")
    monkeypatch.setattr(p, "tiingo_market_cap", lambda t, as_of: None)
    monkeypatch.setattr(p, "sec_fields", lambda t: sce.parse_sec_companyfacts(_FACTS))
    monkeypatch.setattr(p, "latest_price", lambda t, as_of: 12.5)
    monkeypatch.setattr(p, "sec_submissions", lambda t: {
        "sic": "2834", "sic_description": "PHARMACEUTICAL PREPARATIONS", "sector": "Healthcare",
        "industry": "Pharmaceutical Preparations", "name": "Smallex Bio Inc",
    })
    row = p.enrich_ticker("SMLX", AS_OF)
    assert row is not None
    prof = row["profile"]
    assert prof["market_cap"] == 50_000_000 * 12.5 == 625_000_000.0
    assert prof["shares_outstanding"] == 50_000_000 and prof["cash"] == 120e6
    assert row["sector"] == prof["sector"] == "Healthcare" and prof["industry"] == "Pharmaceutical Preparations"
    assert row["name"] == "Smallex Bio Inc"
    assert prof["enrichment_source"] == "sec_xbrl,derived_shares_x_price,sec_submissions"

    # No price known -> cap stays None (the signal's cap gate must treat it as unknown, not zero).
    monkeypatch.setattr(p, "latest_price", lambda t, as_of: None)
    prof = p.enrich_ticker("SMLX", AS_OF)["profile"]
    assert prof["market_cap"] is None and "derived_shares_x_price" not in prof["enrichment_source"]

    # A reported cap is never overridden by the derived one, and a known sector is kept.
    monkeypatch.setattr(p, "tiingo_market_cap", lambda t, as_of: 1.1e9)
    monkeypatch.setattr(p, "latest_price", MagicMock(side_effect=AssertionError("price not needed when a cap is reported")))
    prof = p.enrich_ticker("SMLX", AS_OF)["profile"]
    assert prof["market_cap"] == 1.1e9 and prof["enrichment_source"] == "tiingo,sec_xbrl,sec_submissions"


def test_enrich_ticker_survives_submissions_miss() -> None:
    p, _ = _puller(api_key="")
    p.tiingo_market_cap = lambda t, as_of: None  # type: ignore[method-assign]
    p.sec_fields = lambda t: sce.parse_sec_companyfacts(_FACTS)  # type: ignore[method-assign]
    p.latest_price = lambda t, as_of: 10.0  # type: ignore[method-assign]
    p.sec_submissions = lambda t: {}  # type: ignore[method-assign]
    row = p.enrich_ticker("SMLX", AS_OF)
    assert row is not None
    assert row["profile"]["market_cap"] == 500_000_000.0 and row["profile"]["sector"] is None
    assert row["profile"]["enrichment_source"] == "sec_xbrl,derived_shares_x_price"
