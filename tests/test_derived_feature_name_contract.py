"""Every feature name the derived-feature builder, the feature lab and the
physics readers ask for must have a writer.

A name with no writer does not raise anywhere: the PIT read comes back
empty and the value is ``None``, or worse, an input that *used* to have a
writer keeps returning its last row as "latest" forever. On 2026-11-11:

* ``features/lab.py`` read ``sp500_close``, ``copper_futures_close`` and
  ``gold_futures_close`` -- never registered (entity_map writes
  ``sp500_full``, ``copper``, ``gold_full``), so sp500_mom_*, the S&P physics
  features and copper/gold were ``None`` on every run.
* ``scripts/compute_derived_features.py`` read ``sp500`` and ``gold``, which
  stopped updating 2026-03-24, and had no scheduled caller -- 17 stored
  derived features froze in March.
* ``api/routers/physics.py`` and ``physics/news_energy.py`` read ``sp500``,
  ``vix`` (stopped 2026-04-02), ``us_treasury_10y`` / ``us_treasury_2y``
  (never registered).

A name is written either by the resolver (a target in
``normalization/entity_map.py``) or by the builder itself (a ``COMPUTATIONS``
output). Membership is a necessary, statically checkable condition; see
``tests/test_momentum_feature_contract.py`` for why registration itself is
guarded on the migration side.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS

ROOT = Path(__file__).resolve().parent.parent

# Stopped updating; a reader must not use them even though entity_map may
# still name them.
DEAD_NAMES = {"sp500", "vix", "gold"}


def _computations() -> list[dict]:
    tree = ast.parse((ROOT / "scripts" / "compute_derived_features.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "COMPUTATIONS":
            return ast.literal_eval(node.value)
    raise AssertionError("COMPUTATIONS not found in compute_derived_features.py")


def _writers() -> set[str]:
    resolver = set(SEED_MAPPINGS.values()) | set(NEW_MAPPINGS_V2.values())
    return resolver | {c["name"] for c in _computations()}


def _literal_list(path: Path, name: str) -> list[str]:
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", "") == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {path}")


def _read_names() -> dict[str, set[str]]:
    lab = (ROOT / "features" / "lab.py").read_text()
    verify = (ROOT / "physics" / "verify.py").read_text()
    builder_inputs = {
        i for c in _computations() for i in c.get("inputs", []) + c.get("resolved_inputs", [])
    }
    return {
        "scripts/compute_derived_features.py inputs": builder_inputs,
        "features/lab.py": set(re.findall(r'_get_pit_series\(\s*"([^"]+)"', lab)),
        "physics/verify.py": set(re.findall(r'_get_latest_value\(\s*"([^"]+)"', verify)),
        "api/routers/physics.py key_features": set(
            _literal_list(ROOT / "api" / "routers" / "physics.py", "key_features")
        ),
        "physics/news_energy.py _MARKET_FEATURES": set(
            _literal_list(ROOT / "physics" / "news_energy.py", "_MARKET_FEATURES")
        ),
    }


@pytest.mark.unit
def test_extractors_find_names() -> None:
    """If a reader is refactored away from these call patterns the guard goes blind."""
    for reader, names in _read_names().items():
        assert names, f"no feature names extracted from {reader}"


@pytest.mark.unit
@pytest.mark.parametrize("reader", sorted(_read_names()))
def test_read_names_have_a_live_writer(reader: str) -> None:
    writers = _writers()
    names = _read_names()[reader]
    missing = sorted(n for n in names if n not in writers)
    dead = sorted(names & DEAD_NAMES)
    assert not missing, (
        f"{reader} reads {missing}, which neither entity_map nor "
        "compute_derived_features writes; the read returns nothing and the "
        "value is silently None."
    )
    assert not dead, (
        f"{reader} reads {dead}, which stopped updating in 2026-03/04; the "
        "PIT read returns the last row as current. Use sp500_full / vix_spot / gold_full."
    )


@pytest.mark.unit
def test_builder_does_not_shadow_other_writers() -> None:
    """Targets a better live source keeps fresh are not recomputed here."""
    owned_elsewhere = {
        "treasury_bill_spread", "rrp_as_pct_of_peak", "sofr_spread_to_ffr",
        "yc_5s30s_spread", "yc_butterfly_2_5_10", "yc_term_premium",
        "trade_volume_yoy", "us_china_trade_balance",
    }
    assert not owned_elsewhere & {c["name"] for c in _computations()}
