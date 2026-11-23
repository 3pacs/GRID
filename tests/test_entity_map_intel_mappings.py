"""The 2026-11-21 intel data-path mappings are inert until a registration lands.

Each mapping added for PR A targets a feature name that (a) no other static
mapping feeds, (b) no file in ``schema.sql`` or ``migrations/`` registers, and
(c) is not a ``_full``/``_close`` price feature. ``EntityMap.get_feature_id``
only resolves registered names, so the resolver drops these rows instead of
writing ``resolved_series``, and nothing in the lookup path inserts a
``feature_registry`` row. Engines are mocked; no network, no database.
"""

from __future__ import annotations

import socket
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from normalization import entity_map as entity_map_module
from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS, EntityMap, price_basis_conflicts

REPO = Path(__file__).resolve().parents[1]

INTEL_MAPPING_KEYS: tuple[str, ...] = (
    "credit_proxy:cat_42_cdx_itraxx:us_hy_ccc_oas",
    "credit_proxy:cat_42_cdx_itraxx:us_hy_bb_oas",
    "credit_proxy:cat_42_cdx_itraxx:us_hy_b_oas",
    "credit_proxy:cat_42_cdx_itraxx:us_bbb_oas",
    "credit_proxy:cat_13_euro_at1:euro_hy_oas",
    "credit_proxy:cat_7_china_hy:em_hy_oas",
    "ofr_fsi_credit", "ofr_fsi_equity_valuation", "ofr_fsi_funding",
    "ofr_fsi_safe_assets", "ofr_fsi_volatility",
    "CFNAI", "IORB", "WRESBAL", "WLCFLPCL", "RPONTSYD", "CPN3M", "DTB3",
    "GACDISA066MSFRBNY", "GACDFSA066MSFRBPHI", "BACTSAMFRBDAL",
    "nowcast.gdpnow",
    "refinery_cracks:crack_321", "refinery_cracks:diesel_spot_gulf",
    "refinery_cracks:gasoline_spot_gulf",
    "freight:scfi_composite",
    "indeed:us:sector:software_development", "indeed:us:sector:retail",
    "pboc_omo_ann:reverse_repo_on_amount_cny_bn",
    "eia.wti_spot", "eia.brent_spot",
    *(f"cftc.{c}.net_speculative" for c in (
        "13874A", "209742", "1170E1", "042601", "044601", "043602", "020601",
        "088691", "084691", "085692", "067651", "023651")),
    *(f"cftc_tff.{c}.lev_money_net" for c in (
        "042601", "044601", "043602", "020601", "13874A", "209742", "1170E1")),
    "options_rates_vol:tlt_iv_30d", "options_rates_vol:ief_iv_30d",
    "options_rates_vol:tlt_iv_term_slope",
)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    calls: list[object] = []

    def _refuse(*args, **kwargs):
        calls.append(args)
        raise AssertionError("network access attempted in an offline test")

    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(socket, "getaddrinfo", _refuse)
    yield
    assert calls == []


def _registration_sources() -> str:
    parts = [(REPO / "schema.sql").read_text(encoding="utf-8")]
    for path in sorted((REPO / "migrations").rglob("*")):
        if path.suffix in (".py", ".sql") and path.is_file():
            parts.append(path.read_text(encoding="utf-8", errors="ignore"))
    return "\n".join(parts)


def test_entity_map_new_mappings_have_zero_price_basis_conflicts() -> None:
    merged = {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
    assert price_basis_conflicts(merged) == {}
    assert merged["credit_proxy:cat_42_cdx_itraxx:us_hy_ccc_oas"] == "us_hy_ccc_oas"
    assert merged["cftc.043602.net_speculative"] == "cftc_ust_10y_net_spec"
    assert merged["cftc_tff.043602.lev_money_net"] == "cftc_tff_ust_10y_lev_net"
    assert merged["options_rates_vol:tlt_iv_30d"] == "rates_vol_tlt_iv_30d"
    assert merged["DTB3"] == "treasury_bill_3m"
    assert merged["GACDISA066MSFRBNY"] == "empire_mfg_index"


def test_every_intel_key_is_mapped_in_v2() -> None:
    missing = [k for k in INTEL_MAPPING_KEYS if k not in NEW_MAPPINGS_V2]
    assert missing == []


def test_intel_targets_are_new_names_with_no_other_feed() -> None:
    intel = set(INTEL_MAPPING_KEYS)
    others = {
        sid: name for sid, name in {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}.items()
        if sid not in intel
    }
    other_targets = set(others.values())
    targets = {NEW_MAPPINGS_V2[k] for k in INTEL_MAPPING_KEYS}
    assert sorted(targets & other_targets) == []
    assert len(targets) == len(INTEL_MAPPING_KEYS)  # one series per new feature
    assert not any(t.endswith(("_full", "_close")) for t in targets)


def test_no_intel_target_is_registered_by_schema_or_migrations() -> None:
    text = _registration_sources()
    registered = sorted(
        t for t in (NEW_MAPPINGS_V2[k] for k in INTEL_MAPPING_KEYS)
        if f"'{t}'" in text or f'"{t}"' in text
    )
    assert registered == []


def test_held_back_series_are_not_mapped() -> None:
    merged = {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
    # NFCI: oracle/prediction_context.py reads chicago_fed_nfci by name.
    assert "NFCI" not in merged
    assert "chicago_fed_nfci" not in set(merged.values())
    held_prefixes = ("treasury_auction:tenor:", "breadth:sp500:", "flows:sp500:")
    assert [k for k in merged if k.startswith(held_prefixes)] == []
    for key in ("eia.cushing_crude_stocks", "eia.us_crude_stocks", "eia.natgas_storage_l48",
                "eia.elec_demand_pjm"):
        assert key not in merged


def test_lookup_of_an_unregistered_target_returns_none_and_writes_nothing(monkeypatch) -> None:
    monkeypatch.setattr(entity_map_module, "_WARNED_MAPPED_MISSES", set())
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    engine.begin.return_value.__enter__.return_value = conn
    statements: list[str] = []

    def _execute(statement, *args, **kwargs):
        statements.append(str(statement))
        result = MagicMock()
        result.fetchall.return_value = [(1, "some_other_feature")]
        return result

    conn.execute.side_effect = _execute
    em = EntityMap(db_engine=engine)
    for key in INTEL_MAPPING_KEYS:
        assert em.get_feature_id(key) is None
    assert not any("INSERT" in s.upper() for s in statements)
    report = em.missing_feature_report()
    assert set(report["unregistered_features"]) >= {NEW_MAPPINGS_V2[k] for k in INTEL_MAPPING_KEYS}
