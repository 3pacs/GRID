"""Aliases for the series ids the running pullers actually write (T1.1).

``ingestion/altdata/binance_puller.py`` writes ``binance.{SYMBOL}.{field}``
and ``ingestion/altdata/defi_llama_puller.py`` writes
``defillama.chain_tvl.{chain}``; the entity map only knew the colon-form
``BINANCE:BTCUSDT:close`` spellings, so those rows never resolved to a
feature and never reached the scorers. These tests pin that the dotted
spellings resolve to the same canonical features as the colon spellings
and that the merge into ``SEED_MAPPINGS`` happens at ``EntityMap`` init.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from normalization import entity_map as em

DOTTED_TO_COLON = {
    "binance.BTCUSDT.close": "BINANCE:BTCUSDT:close",
    "binance.BTCUSDT.volume": "BINANCE:BTCUSDT:volume",
    "binance.ETHUSDT.close": "BINANCE:ETHUSDT:close",
    "binance.ETHUSDT.volume": "BINANCE:ETHUSDT:volume",
    "binance.SOLUSDT.close": "BINANCE:SOLUSDT:close",
    "binance.SOLUSDT.volume": "BINANCE:SOLUSDT:volume",
}


@pytest.mark.parametrize("dotted,colon", sorted(DOTTED_TO_COLON.items()))
def test_binance_dotted_ids_alias_the_colon_form(dotted: str, colon: str) -> None:
    assert dotted in em.NEW_MAPPINGS_V2, dotted
    assert em.NEW_MAPPINGS_V2[dotted] == em.NEW_MAPPINGS_V2[colon]


def test_defillama_solana_tvl_resolves_to_dex_sol_liquidity() -> None:
    assert em.NEW_MAPPINGS_V2["defillama.chain_tvl.solana"] == "dex_sol_liquidity"


def test_puller_ids_match_what_the_pullers_write() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    binance = (root / "ingestion/altdata/binance_puller.py").read_text(encoding="utf-8")
    llama = (root / "ingestion/altdata/defi_llama_puller.py").read_text(encoding="utf-8")
    # The puller builds `binance.{symbol}.{field}` — the alias keys must use
    # that dotted shape, not the colon shape the map used to assume.
    assert 'f"binance.{' in binance
    assert 'self._series_id("chain_tvl", chain)' in llama
    assert "defillama.chain_tvl.<chain>" in llama


def test_aliases_are_merged_into_seed_mappings_at_init() -> None:
    conn = MagicMock()
    result = MagicMock()
    result.fetchall.return_value = []
    conn.execute.return_value = result
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    engine = MagicMock()
    engine.connect.return_value = conn

    mapper = em.EntityMap(db_engine=engine)

    merged = mapper.get_all_mappings()
    for dotted in DOTTED_TO_COLON:
        assert merged[dotted] == em.NEW_MAPPINGS_V2[dotted]
    assert merged["defillama.chain_tvl.solana"] == "dex_sol_liquidity"
