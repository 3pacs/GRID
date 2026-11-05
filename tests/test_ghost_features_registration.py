"""The 45 ghost-mapped features of 2026-11-04 must resolve once registered.

grid-hermes logged 498 "Mapping exists (X -> Y) but feature not in registry"
lines and 55 resolver summaries naming "feature name(s) missing from
feature_registry (rows dropped until registered)" in the 24 h to 2026-11-04
05:28Z. ``migrations/versions/ghost_features_20261104.py`` registers them.
These tests pin the revision to the mappings and to the log, prove the
resolution path over a registry holding exactly the revision's rows, and pin
the once-per-process warning and the resolver summary level.

DB engines are mocked; no network, no database.
"""
from __future__ import annotations

import importlib
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from loguru import logger as log

from normalization import entity_map as entity_map_mod
from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS, EntityMap

ALL_MAPPINGS = {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
REVISION = (
    Path(__file__).resolve().parents[1]
    / "migrations" / "versions" / "ghost_features_20261104.py"
)
ETF = ("SPY", "QQQ", "IWM", "TLT", "HYG", "GLD", "EEM", "XLK", "XLF", "XLE", "XLV")

# series_id -> feature name exactly as the 2026-11-04 journal printed them.
LOGGED_GHOSTS: dict[str, str] = {
    "oi_k_shape_ratio": "oi_k_shape_ratio",
    "COMPUTED:tga_drawdown": "tga_drawdown",
    "COMPUTED:reverse_repo_pct_of_peak": "reverse_repo_pct_of_peak",
    "COMPUTED:fed_net_liquidity_change_1w": "fed_net_liquidity_change_1w",
    "COMPUTED:fed_net_liquidity_change_1m": "fed_net_liquidity_change_1m",
    "COMPUTED:fed_net_liquidity": "fed_net_liquidity",
    "eurozone_hicp_yoy": "eurozone_hicp_yoy",
    "CBOE:VIX9D": "vix9d_spot",
    "CBOE:VIX3M": "vix3m_spot",
    "DGS2": "yc_2y",
    "RRPONTSYD": "overnight_reverse_repo",
    "ecb_bank_lending_yoy": "ecb_bank_lending_yoy",
    **{f"ETF_FLOW:{t}:{s}": f"etf_flow_{t.lower()}_{s}" for t in ETF for s in ("5d", "20d", "accel")},
}

# Owner decision 2026-11-04: these two logged series feed the registered
# names that stopped updating on 2026-04-02, instead of new duplicates.
REMAPPED_TO_REGISTERED: dict[str, str] = {
    "COMPUTED:reverse_repo_pct_of_peak": "rrp_as_pct_of_peak",
    "RRPONTSYD": "reverse_repo_usage",
}


def _revision():
    return importlib.import_module("migrations.versions.ghost_features_20261104")


def _names_inserted() -> list[str]:
    body = REVISION.read_text().split("VALUES", 1)[1]
    return re.findall(r"^\s*\('([a-z0-9_]+)',", body, flags=re.MULTILINE)


def _engine(features: list[tuple[int, str]]) -> MagicMock:
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    conn.execute.return_value.fetchall.return_value = list(features)
    return engine


@pytest.fixture
def warnings_captured():
    records: list[str] = []
    sink_id = log.add(lambda msg: records.append(str(msg)), level="WARNING")
    yield records
    log.remove(sink_id)


@pytest.mark.unit
class TestRevisionCoversTheLog:
    def test_every_logged_mapping_is_still_in_place(self):
        for sid, name in LOGGED_GHOSTS.items():
            expected = REMAPPED_TO_REGISTERED.get(sid, name)
            assert ALL_MAPPINGS.get(sid) == expected, sid

    def test_revision_registers_every_logged_name(self):
        logged = {n for sid, n in LOGGED_GHOSTS.items() if sid not in REMAPPED_TO_REGISTERED}
        assert logged <= set(_names_inserted())
        assert not set(REMAPPED_TO_REGISTERED.values()) & set(_names_inserted())

    def test_revision_adds_only_the_expected_names_beyond_the_log(self):
        # + bls_cpi_u_all / bls_nonfarm_productivity: owner decision 2026-11-04
        # to register the BLS series that were mapped but unregistered.
        assert set(_names_inserted()) - set(LOGGED_GHOSTS.values()) == {
            "implied_correlation_3m", "bls_jolts_openings",
            "bls_cpi_u_all", "bls_nonfarm_productivity",
            "ci_loans_growth", "bank_reserves_at_fed", "brazil_usd_brl", "brazil_fx_reserves",
        }
        assert ALL_MAPPINGS["CBOE:COR3M"] == "implied_correlation_3m"

    def test_jolts_maps_from_the_real_bls_id_and_keeps_the_old_key(self):
        assert ALL_MAPPINGS["JTS000000000000000JOL"] == "bls_jolts_openings"
        assert ALL_MAPPINGS["JTS00000000JOL"] == "bls_jolts_openings"

    def test_no_duplicates_and_exports_match(self):
        names = _names_inserted()
        assert len(names) == len(set(names)) == 51
        assert list(_revision().REGISTERED_FEATURES) == names

    def test_every_registered_name_is_targeted_by_a_mapping(self):
        targets = set(ALL_MAPPINGS.values())
        assert not [n for n in _names_inserted() if n not in targets]

    def test_no_liquidity_family(self):
        # 'liquidity' is what sank ingestion/seed_v2.py's INSERT.
        assert "'liquidity'" not in REVISION.read_text().split("VALUES", 1)[1]

    def test_idempotent_and_parameter_free(self):
        src = REVISION.read_text()
        assert "ON CONFLICT (name) DO NOTHING" in src
        assert "%s" not in src and "{}" not in src

    def test_revision_chain(self):
        mod = _revision()
        assert mod.revision == REVISION.stem and len(mod.revision) <= 32
        assert mod.down_revision == "merge_yf_finra_20261023"

    def test_downgrade_refuses(self):
        with pytest.raises(RuntimeError, match="refused"):
            _revision().downgrade()

    def test_upgrade_executes_insert_and_reports_counts(self, monkeypatch):
        mod = _revision()
        conn = MagicMock()
        conn.execute.return_value.scalar.side_effect = [0, 51]
        fake_op = MagicMock()
        fake_op.get_bind.return_value = conn
        monkeypatch.setattr(mod, "op", fake_op)
        mod.upgrade()
        fake_op.execute.assert_called_once_with(mod.GHOST_FEATURES)
        # Both count reads bind the names; nothing is interpolated.
        for call in conn.execute.call_args_list:
            assert call.args[1] == {"names": list(mod.REGISTERED_FEATURES)}


@pytest.mark.unit
class TestResolutionOverTheSeededRegistry:
    def test_every_logged_series_resolves_once_registered(self):
        # The remap targets already exist on griddb (registered before this
        # revision), so the fake registry carries them too.
        names = _names_inserted() + sorted(set(REMAPPED_TO_REGISTERED.values()))
        registry = [(9000 + i, n) for i, n in enumerate(names)]
        ids = dict((n, i) for i, n in registry)
        em = EntityMap(db_engine=_engine(registry))
        for sid, name in LOGGED_GHOSTS.items():
            assert em.get_feature_id(sid) == ids[REMAPPED_TO_REGISTERED.get(sid, name)], sid
        report = em.missing_feature_report()
        assert report["lookups_missed"] == 0 and report["unregistered_features"] == []

    def test_without_the_rows_they_are_counted_misses(self):
        em = EntityMap(db_engine=_engine([(1, "vix_spot")]))
        assert em.get_feature_id("ETF_FLOW:SPY:5d") is None
        assert "etf_flow_spy_5d" in em.missing_feature_report()["unregistered_features"]


@pytest.mark.unit
class TestMappedMissWarnsOncePerProcess:
    def test_second_instance_does_not_rewarn(self, monkeypatch, warnings_captured):
        monkeypatch.setattr(entity_map_mod, "_WARNED_MAPPED_MISSES", set())
        for _ in range(3):  # the resolver builds a fresh EntityMap every cycle
            em = EntityMap(db_engine=_engine([(1, "vix_spot")]))
            assert em.get_feature_id("CBOE:VIX3M") is None
            # The per-run count is unaffected: the resolver summary still sees it.
            assert em.missing_feature_report()["unregistered_features"] == ["vix3m_spot"]
        lines = [r for r in warnings_captured if "Mapping exists (CBOE:VIX3M -> vix3m_spot)" in r]
        assert len(lines) == 1


@pytest.mark.unit
class TestResolverSummaryLevel:
    """The per-run summary is WARNING only when rows are being dropped."""

    def _levels(self, monkeypatch, report: dict) -> list[str]:
        from datetime import date, datetime, timezone

        from normalization import resolver as resolver_mod
        from tests.test_resolver import FakeRow, _mock_engine

        mock_map = MagicMock()
        mock_map.get_feature_id.return_value = None
        mock_map.missing_feature_report.return_value = report
        monkeypatch.setattr(resolver_mod, "EntityMap", MagicMock(return_value=mock_map))
        pending = [FakeRow(("UNKNOWN", date(2026, 1, 1), 100.0, "s1",
                            datetime(2026, 1, 2, tzinfo=timezone.utc), 1, "Src1"))]
        engine, _ = _mock_engine(pending_rows=pending)
        records: list[tuple[str, str]] = []
        sink = log.add(lambda m: records.append((m.record["level"].name, m.record["message"])),
                       level="DEBUG")
        try:
            resolver_mod.Resolver(db_engine=engine).resolve_pending()
        finally:
            log.remove(sink)
        return [lvl for lvl, msg in records if msg.startswith("Resolution: ")]

    @staticmethod
    def _report(mapped: int, names: list[str]) -> dict:
        return {
            "lookups_missed": 5, "series_ids": 5, "unregistered_features": names,
            "top_series": [["LEGISLATION:x", 1]], "mapped_unregistered_count": mapped,
            "no_mapping_series_ids": 5 - mapped, "by_family": [["LEGISLATION", 5, 5]],
        }

    def test_dropping_rows_is_a_warning(self, monkeypatch):
        assert self._levels(monkeypatch, self._report(1, ["yc_2y"])) == ["WARNING"]

    def test_only_unmapped_ids_is_info(self, monkeypatch):
        assert self._levels(monkeypatch, self._report(0, [])) == ["INFO"]
