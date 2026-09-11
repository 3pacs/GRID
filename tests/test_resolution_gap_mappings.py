"""Mappings must point at features that exist, or they resolve to nothing.

EntityMap.get_feature_id() looks the mapped name up in feature_registry and
returns None when it is absent, logging "Mapping exists (X -> Y) but feature
not in registry". Such a mapping is a GHOST: it does not show up in any count
of unmapped series, and it silently drops every row. Migration 0062 fixed one
batch of these for GDELT; 0063 fixes nine more.

These tests pin the mappings added alongside 0063 and the feargreed typo fix,
so a future edit cannot quietly re-point them at a name nothing registers.
"""
from __future__ import annotations

import re
from pathlib import Path

from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS

ALL_MAPPINGS = {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
MIGRATION = Path(__file__).resolve().parents[1] / "migrations" / "0063_register_ghost_mapped_features.sql"


def _names_registered_by_0063() -> set[str]:
    """Feature names 0063 inserts, parsed from the migration itself."""
    sql = MIGRATION.read_text()
    body = sql.split("VALUES", 1)[1]
    return set(re.findall(r"^\s*\('([a-z0-9_]+)',", body, flags=re.MULTILINE))


class TestCryptoSpotMappings:
    def test_btc_and_eth_close_and_adj_close_are_mapped(self):
        # Volume was mapped; price was not, so the daily close went nowhere.
        for sid, target in [
            ("YF:BTC-USD:close", "btc_usd_full"),
            ("YF:BTC-USD:adj_close", "btc_usd_full"),
            ("YF:ETH-USD:close", "eth_usd_full"),
            ("YF:ETH-USD:adj_close", "eth_usd_full"),
        ]:
            assert ALL_MAPPINGS.get(sid) == target, f"{sid} should map to {target}"

    def test_volume_mappings_are_not_disturbed(self):
        assert ALL_MAPPINGS["YF:BTC-USD:volume"] == "btc_total_volume"
        assert ALL_MAPPINGS["YF:ETH-USD:volume"] == "eth_total_volume"

    def test_sol_is_mapped_even_though_no_rows_arrive_yet(self):
        # sol_usd_full is registered (id 2789) but the puller produced 0 rows
        # in the 30 d to 2026-09-11. The mapping is inert, not wrong.
        assert ALL_MAPPINGS["YF:SOL-USD:close"] == "sol_usd_full"


class TestFearGreedTypoFix:
    def test_crypto_value_points_at_the_registered_feature(self):
        # feargreed_crypto_value has no feature_registry row; crypto_fear_greed
        # (id 199) does. The old target dropped ~30 rows/30d.
        assert ALL_MAPPINGS["feargreed.crypto_value"] == "crypto_fear_greed"

    def test_the_other_feargreed_series_are_untouched(self):
        assert ALL_MAPPINGS["feargreed.cnn_value"] == "feargreed_cnn_value"
        assert ALL_MAPPINGS["feargreed.cnn_previous_close"] == "feargreed_cnn_previous_close"


class TestMigration0063CoversItsGhosts:
    def test_migration_registers_every_name_it_claims(self):
        registered = _names_registered_by_0063()
        expected = {
            "eurusd_ecb_daily",
            "shy_full", "ief_full", "emb_full", "jnk_full", "mub_full",
            "smh_close", "icln_close", "lit_close",
        }
        assert registered == expected, f"migration registers {registered}"

    def test_every_registered_name_is_actually_mapped_by_something(self):
        # A registry row for a feature nothing maps onto is dead weight; this
        # is the inverse of the ghost-mapping bug and just as useless.
        targets = set(ALL_MAPPINGS.values())
        for name in _names_registered_by_0063():
            assert name in targets, f"0063 registers {name} but no mapping targets it"

    def test_migration_is_idempotent(self):
        sql = MIGRATION.read_text()
        assert "ON CONFLICT (name) DO NOTHING" in sql

    def test_migration_uses_no_string_interpolation(self):
        sql = MIGRATION.read_text()
        assert "%s" not in sql and "{}" not in sql
