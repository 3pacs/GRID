"""The 17 mapped features still dropped after ghost_features_20261104.

A bounded re-resolve dry run on 2026-11-05 listed these names as "missing
from feature_registry (rows dropped until registered)".
``migrations/versions/ghost_features_20261105.py`` registers them, dated from
each series' first SUCCESS raw row. These tests pin the revision to the
entity_map mappings and to the alembic graph, and check the statement's
shape. No database.
"""
from __future__ import annotations

import importlib
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS

REPO = Path(__file__).resolve().parents[1]
REVISION_FILE = REPO / "migrations" / "versions" / "ghost_features_20261105.py"
ALL_MAPPINGS = {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}

# Exactly the names the 2026-11-05 dry run printed (resolver summaries).
DRY_RUN_GHOSTS = {
    "feargreed_cnn_value", "feargreed_cnn_previous_close",
    "aaii_bullish_pct", "aaii_bearish_pct", "aaii_neutral_pct", "aaii_bull_bear_spread",
    "gdelt_recession_tone", "gdelt_fed_tone", "gdelt_trade_conflict_volume",
    "container_index", "us_exports_total", "us_china_bilateral_yoy",
    "fed_treasury_holdings", "central_bank_liq_swaps", "nyfed_soma_total",
    "finra_short_interest_total", "euro_btp_bund_spread",
}

# ('name', 'family', 'desc', 'transformation', v, lag, 'norm', 'policy', 'raw_series_id')
_ROW = re.compile(
    r"\(\s*'(?P<name>[^']+)'\s*,\s*'[^']+'\s*,\s*'(?:[^']|'')*'\s*,\s*'(?:[^']|'')*'\s*,"
    r"\s*\d+\s*,\s*\d+\s*,\s*'[A-Z_]+'\s*,\s*'[A-Z_]+'\s*,\s*'(?P<raw>[^']+)'\s*\)",
)


@pytest.fixture(scope="module")
def rev():
    return importlib.import_module("migrations.versions.ghost_features_20261105")


def _rows(rev) -> dict[str, str]:
    return {m["name"]: m["raw"] for m in _ROW.finditer(rev.GHOST_FEATURES)}


def test_registers_exactly_the_dry_run_ghosts(rev) -> None:
    assert set(rev.REGISTERED_FEATURES) == DRY_RUN_GHOSTS
    assert _rows(rev) == rev.REGISTERED_FEATURES


@pytest.mark.parametrize("name", sorted(DRY_RUN_GHOSTS))
def test_each_row_dates_from_the_series_entity_map_sends_to_it(rev, name: str) -> None:
    raw = rev.REGISTERED_FEATURES[name]
    assert ALL_MAPPINGS.get(raw) == name, f"{raw!r} does not map to {name!r}"


def test_no_name_is_already_inserted_by_another_migration(rev) -> None:
    others = [
        p for p in (REPO / "migrations").rglob("*")
        if p.suffix in {".py", ".sql"} and p != REVISION_FILE
        and re.search(r"INSERT\s+INTO\s+feature_registry", p.read_text(errors="replace"), re.I)
    ]
    for p in others:
        body = p.read_text(errors="replace")
        clash = [n for n in DRY_RUN_GHOSTS if re.search(rf"\(\s*'{n}'\s*,", body)]
        assert not clash, f"{p.name} already inserts {clash}"


def test_revision_is_the_single_child_of_the_vintage_scan(rev) -> None:
    assert rev.revision == "ghost_features_20261105" and len(rev.revision) <= 32
    assert rev.down_revision == "yf_latest_vintage_scan_20261026"
    children = [
        p.name for p in (REPO / "migrations" / "versions").glob("*.py")
        if re.search(r'^down_revision[^=]*=\s*"yf_latest_vintage_scan_20261026"', p.read_text(), re.M)
    ]
    assert children == [REVISION_FILE.name]


def test_statement_dates_from_first_success_row_and_skips_series_without_one(rev) -> None:
    sql = " ".join(rev.GHOST_FEATURES.split())
    assert "CROSS JOIN LATERAL" in sql  # no SUCCESS row -> no candidate row
    assert "r.series_id = c.raw_series_id AND r.pull_status = 'SUCCESS'" in sql
    assert "ORDER BY r.obs_date ASC LIMIT 1" in sql
    assert "first_obs.obs_date, FALSE" in sql  # eligible_from_date, model_eligible
    assert sql.rstrip().endswith("ON CONFLICT (name) DO NOTHING")
    assert text(rev.GHOST_FEATURES).compile().params == {}
    assert "%" not in rev.GHOST_FEATURES


def test_upgrade_bounds_the_statement_and_reports_counts(rev, monkeypatch, caplog) -> None:
    executed: list[str] = []
    conn = MagicMock()
    conn.execute.return_value.scalar.side_effect = [3, 17]
    fake_op = MagicMock()
    fake_op.get_bind.return_value = conn
    fake_op.execute.side_effect = lambda stmt: executed.append(str(stmt))
    monkeypatch.setattr(rev, "op", fake_op)

    with caplog.at_level("WARNING", logger="alembic.runtime.migration"):
        rev.upgrade()

    assert executed == [
        "SET LOCAL statement_timeout = '120s'",
        rev.GHOST_FEATURES,
        "SET LOCAL statement_timeout TO DEFAULT",
    ]
    assert "3 of 17 present before, 17 after (14 inserted" in caplog.text


def test_downgrade_refuses(rev) -> None:
    with pytest.raises(RuntimeError, match="refused"):
        rev.downgrade()
