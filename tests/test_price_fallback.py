"""ingestion/price_fallback.py is fetch-only: its resolved_series writer is retired (E1-V7, DFa)."""

from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


class _Engine:
    """Records any use; the retired writer must never touch the database."""

    def __init__(self):
        self.used = False

    def begin(self):
        self.used = True
        raise AssertionError("save_to_db opened a transaction")

    connect = begin


def test_save_to_db_refuses_and_writes_nothing():
    from ingestion.price_fallback import RETIRED_REASON, PriceFallbackPuller, PriceFallbackRetired

    engine = _Engine()
    puller = PriceFallbackPuller(db_engine=engine)
    with pytest.raises(PriceFallbackRetired) as exc:
        puller.save_to_db([{"ticker": "SPY", "price": 501.25, "date": "2026-05-27", "source": "stooq"}])
    assert str(exc.value) == RETIRED_REASON
    assert not engine.used


def test_module_holds_no_resolved_series_or_source_catalog_write():
    source = (REPO / "ingestion" / "price_fallback.py").read_text(encoding="utf-8").lower()
    assert "insert into" not in source
    assert ".execute(" not in source


@pytest.mark.parametrize("rel", ["ingestion/scheduler.py", "intelligence/scheduler.py"])
def test_schedulers_no_longer_run_the_fallback_writer(rel):
    source = (REPO / rel).read_text(encoding="utf-8")
    assert "PriceFallbackPuller" not in source
    assert "pfp.save_to_db" not in source
    assert ".do(_price_fallback)" not in source
