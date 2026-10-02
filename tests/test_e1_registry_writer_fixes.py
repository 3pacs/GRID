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
from itertools import chain
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
    # Both tables, unmerged: a SEED entry shadowed by a V2 key must still count.
    static = list(chain(em.SEED_MAPPINGS.items(), em.NEW_MAPPINGS_V2.items()))
    cg_targets = {v for k, v in static if k.startswith("CG:") and k.endswith(":usd")}
    assert cg_targets
    shared = sorted((k, v) for k, v in static if v in cg_targets and not k.startswith("CG:"))
    assert shared == [], shared
    # Nor through the YF:<T>-USD:<field> pattern fallback: the yfinance price
    # puller's default universe never requests the -USD symbol of a CG-fed coin.
    from ingestion.yfinance_pull import YF_TICKER_LIST

    universe = {t.upper() for t in YF_TICKER_LIST}
    clashes = sorted(t for t in cg_targets if t[: -len("_usd_full")].upper() + "-USD" in universe)
    assert clashes == [], clashes


def test_coingecko_is_a_base_puller_under_its_catalog_name() -> None:
    from ingestion.base import BasePuller

    assert issubclass(coingecko.CoinGeckoPuller, BasePuller)
    assert coingecko.CoinGeckoPuller.SOURCE_NAME == ss.catalog_name_for("coingecko")
    source = (REPO / "ingestion" / "coingecko.py").read_text(encoding="utf-8")
    assert "resolved_series (" not in source and "INSERT INTO resolved_series" not in source


def test_offshore_store_matches_never_holds_one_transaction_across_many_inserts() -> None:
    """Every transaction writes <= STORE_BATCH_ROWS rows and never opens savepoints."""
    from unittest.mock import MagicMock

    from ingestion.altdata import offshore_leaks as ol

    per_txn: list[int] = []

    class Conn:
        def __init__(self) -> None:
            self.inserts = 0

        def execute(self, stmt, params=None):
            sql = " ".join(str(stmt).split()).upper()
            if sql.startswith("INSERT INTO RAW_SERIES"):
                self.inserts += 1
            res = MagicMock()
            res.fetchall.return_value = []
            return res

        def begin_nested(self):
            raise AssertionError("savepoint per row: each one holds a subtransaction lock")

    class Begin:
        def __enter__(self):
            self.conn = Conn()
            return self.conn

        def __exit__(self, *exc):
            per_txn.append(self.conn.inserts)
            return False

    puller = ol.OffshoreLeaksPuller.__new__(ol.OffshoreLeaksPuller)
    puller.engine = MagicMock()
    puller.engine.begin.side_effect = lambda: Begin()
    puller.source_id = 7
    matches = [{
        "actor_name": f"A{i}", "actor_id": f"a{i}", "officer_name": f"O{i}", "officer_node_id": str(i),
        "officer_jurisdiction": "VGB", "match_type": "partial",
        "connected_entities": [{"entity_name": f"E{i}-{j}", "entity_jurisdiction": "VGB"} for j in range(3)],
    } for i in range(200)]
    out = puller.store_matches(matches)
    assert out["raw_series_inserted"] == 600
    assert max(per_txn) <= ol.STORE_BATCH_ROWS


def test_offshore_failed_batches_are_reported_and_bounded(monkeypatch) -> None:
    from unittest.mock import MagicMock

    from ingestion.altdata import offshore_leaks as ol

    puller = ol.OffshoreLeaksPuller.__new__(ol.OffshoreLeaksPuller)
    puller.engine = MagicMock()
    puller.engine.begin.side_effect = RuntimeError("out of shared memory")
    puller.source_id = 7
    matches = [{
        "actor_name": f"A{i}", "actor_id": f"a{i}", "officer_name": f"O{i}", "officer_node_id": str(i),
        "officer_jurisdiction": "VGB", "match_type": "partial", "connected_entities": [],
    } for i in range(500)]
    out = puller.store_matches(matches)
    assert out["raw_series_inserted"] == 0
    assert out["failed_batches"] == ol.MAX_CONSECUTIVE_BATCH_FAILURES  # stopped, not 10 retries
    assert puller.engine.begin.call_count == ol.MAX_CONSECUTIVE_BATCH_FAILURES

    monkeypatch.setattr(puller, "ensure_data", lambda: True)
    monkeypatch.setattr(puller, "match_actors", lambda: matches)
    pulled = puller.pull()
    assert pulled["status"] == "FAILED"
    import ingestion.smart_scheduler as ss
    assert ss._classify_outcome(pulled)[0] == ss.OUTCOME_FAILED
