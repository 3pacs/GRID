"""Unit guards for the E1-V3/V4/V6 fixes (no database; see the *_pg.py proof).

* V3: every SmartScheduler entry the gate flagged has a real entry point --
  ``bls`` is unheld and built with its key as a keyword, ``wiki_history`` is
  unheld, and ``pumpfun`` (dead upstream) is gone from the registry and from
  the full-pipeline crypto step.
* V4: the watchlist live-price cache no longer writes raw_series at all.
* V6: coingecko's ``CG:<id>:usd`` spot series are mapped for the resolver
  exactly for the coins whose ``*_usd_full`` feature has no other feed
  (BTC, ETH and SOL are fed by the yfinance close), and no ``CG:`` target is
  ever fed by a second series.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import ingestion.smart_scheduler as ss
from ingestion import bls, coingecko
from normalization import entity_map as em

REPO = Path(__file__).resolve().parents[1]


def _entry(name: str) -> dict | None:
    return next((e for e in ss.PULLER_REGISTRY if e["name"] == name), None)


def test_bls_entry_is_unheld_and_passes_its_key_by_keyword() -> None:
    entry = _entry("bls")
    assert entry is not None and not entry.get("hold_reason")
    assert entry["method"] == "pull_all" and entry.get("api_key_mode") == "keyword"

    seen: dict = {}

    class Fake:
        def __init__(self, *args, **kwargs) -> None:
            seen.update(args=args, kwargs=kwargs)

    sched = ss.SmartScheduler.__new__(ss.SmartScheduler)
    sched.engine = object()
    sched._build_puller_instance(entry, Fake, {"BLS_API_KEY": "k"})
    assert seen == {"args": (), "kwargs": {"db_engine": sched.engine, "api_key": "k"}}


def test_bls_pull_all_is_one_bounded_window(monkeypatch) -> None:
    puller = bls.BLSPuller.__new__(bls.BLSPuller)
    calls: list[dict] = []
    monkeypatch.setattr(puller, "pull_series", lambda **kw: calls.append(kw) or {"rows_inserted": 0})
    puller.pull_all()
    puller.pull_all(start_year=2020)
    assert calls == [{"start_year": date.today().year - 2}, {"start_year": 2020}]


def test_wiki_history_is_unheld_and_pumpfun_is_not_registered() -> None:
    wiki = _entry("wiki_history")
    assert wiki is not None and not wiki.get("hold_reason") and wiki["method"] == "pull_all"
    assert _entry("pumpfun") is None
    assert not any(e["cls"] == "PumpFunPuller" for e in ss.PULLER_REGISTRY)
    pipeline = (REPO / "scripts" / "run_full_pipeline.py").read_text(encoding="utf-8")
    assert "from ingestion.pumpfun import" not in pipeline


def test_wiki_history_pull_all_without_events_writes_nothing(monkeypatch) -> None:
    from ingestion.wiki_history import WikiHistoryPuller

    puller = WikiHistoryPuller(db_engine=None)
    monkeypatch.setattr(puller, "pull_today", lambda d=None: {"date": "2026-09-30", "wiki_events": []})
    monkeypatch.setattr(puller, "_store", lambda data: (_ for _ in ()).throw(AssertionError("wrote")))
    out = puller.pull_all()
    assert ss._classify_outcome(out)[0] == ss.OUTCOME_FAILED


def test_watchlist_price_cache_no_longer_writes_raw_series() -> None:
    import api.routers.watchlist_helpers as wh

    assert not hasattr(wh, "_cache_price_to_db")
    for rel in ("api/routers/watchlist_helpers.py", "api/routers/watchlist.py",
                "api/routers/price_alerts.py", "api/routers/astrogrid_core.py",
                "astrogrid_api/astrogrid_core.py"):
        source = (REPO / rel).read_text(encoding="utf-8")
        assert "_cache_price_to_db" not in source, rel
        assert "INSERT INTO RAW_SERIES" not in " ".join(source.upper().split()), rel


def test_coingecko_spot_series_map_to_usd_full_except_btc_eth_sol() -> None:
    mapped = {k: v for k, v in em.NEW_MAPPINGS_V2.items() if k.startswith("CG:") and k.endswith(":usd")}
    expected = {
        coingecko.spot_series_id(cg_id): f"{ticker.lower()}_usd_full"
        for ticker, cg_id in coingecko.CRYPTO_MAP.items()
        if ticker not in {"BTC", "ETH", "SOL"}
    }
    assert mapped == expected
    # BTC/ETH/SOL *_usd_full keep their single yfinance daily-close feed.
    assert em.NEW_MAPPINGS_V2["YF:BTC-USD:close"] == "btc_usd_full"
    assert em.NEW_MAPPINGS_V2["YF:ETH-USD:close"] == "eth_usd_full"
    assert em.NEW_MAPPINGS_V2["YF:SOL-USD:close"] == "sol_usd_full"


def test_no_coingecko_target_is_fed_by_a_second_series() -> None:
    """A CG:-fed feature with any other feed is a first-writer-wins race in the resolver."""
    static = {**em.SEED_MAPPINGS, **em.NEW_MAPPINGS_V2}
    cg_targets = {v for k, v in static.items() if k.startswith("CG:") and k.endswith(":usd")}
    assert cg_targets
    shared = sorted((k, v) for k, v in static.items() if v in cg_targets and not k.startswith("CG:"))
    assert shared == [], shared
    # Nor through the YF:<T>-USD:<field> pattern fallback: no yfinance price
    # puller requests the -USD symbol of a CG-fed coin.
    yf_universe = (REPO / "ingestion" / "yfinance_pull.py").read_text(encoding="utf-8")
    for target in sorted(cg_targets):
        symbol = target[: -len("_usd_full")].upper() + "-USD"
        assert f'"{symbol}"' not in yf_universe, (target, symbol)


def test_coingecko_is_a_base_puller_under_its_catalog_name() -> None:
    from ingestion.base import BasePuller

    assert issubclass(coingecko.CoinGeckoPuller, BasePuller)
    assert coingecko.CoinGeckoPuller.SOURCE_NAME == ss.catalog_name_for("coingecko")
    source = (REPO / "ingestion" / "coingecko.py").read_text(encoding="utf-8")
    assert "resolved_series (" not in source and "INSERT INTO resolved_series" not in source
