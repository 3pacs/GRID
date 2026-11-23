"""The scheduled GDELT DOC puller is retired: held, not deleted, and no reader
presents its last values as current.

2026-11-23 on grid-svr: ``smart:gdelt`` had no SUCCESS since 11-04 (25 runs
ended in HTTP 429), and ``api.gdeltproject.org:443`` refused TCP connections
from three hosts at 22:30Z. The actor-tone and tension features stopped on
11-03/06, yet ``/physics/news-momentum`` still answered ``available: true``
from a 90-day window, and 14 of the features were model-eligible with
forward fill.
"""

from __future__ import annotations

import importlib
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text

import ingestion.smart_scheduler as ss
from ingestion.altdata import gdelt
from physics import momentum as pm

ROOT = Path(__file__).resolve().parents[1]
revision = importlib.import_module("migrations.versions.gdelt_doc_retire_20261123")
AS_OF = date(2026, 11, 23)


# ── scheduling ──────────────────────────────────────────────────────────────

def test_registry_entry_is_held_with_the_measured_reason_and_kept() -> None:
    (entry,) = [p for p in ss.PULLER_REGISTRY if p["name"] == "gdelt"]
    reason = entry["hold_reason"]
    assert reason.startswith("Retired 2026-11-23 (owner)")
    assert "429" in reason and "refused TCP" in reason
    assert ss.hold_reason_for("gdelt") == reason
    # Not deleted: the entry still names the module and class that exist.
    assert entry["mod"] == "ingestion.altdata.gdelt" and entry["cls"] == "GDELTPuller"
    assert callable(getattr(gdelt.GDELTPuller, entry["method"]))


def test_grid_scheduler_follows_the_same_hold() -> None:
    source = (ROOT / "ingestion" / "scheduler.py").read_text(encoding="utf-8")
    gate = source.index('hold_reason_for("gdelt")')
    build = source.index('pullers.append(("GDELT", GDELTPuller(db_engine)')
    assert gate < build
    assert source.count("GDELTPuller(db_engine)") == 1     # no ungated second path
    block = source[gate:build]
    assert "if _gdelt_hold:" in block and "else:" in block


def test_hermes_repair_treats_gdelt_as_retired_under_every_log_name() -> None:
    from scripts import hermes_fixers as hf

    held = hf._held_source_names()
    for alias in ("gdelt", "GDELT", "smart:gdelt"):
        assert hf._normalize_source_key(alias) in held, alias
    # No retry route either: the repair map only names runnable writers.
    assert "GDELT" not in hf._CATALOG_TO_REGISTRY


def test_bulk_file_loader_is_not_held() -> None:
    assert not any(p.get("hold_reason") for p in ss.PULLER_REGISTRY if "gdelt_bulk" in p["name"])
    from ingestion.altdata import gdelt_bulk
    assert callable(gdelt_bulk.run_daily)


# ── reader: news momentum ───────────────────────────────────────────────────

def _analyzer(last_obs: date, n: int = 40) -> pm.NewsMomentumAnalyzer:
    ids = {name: i + 1 for i, name in enumerate(pm.GDELT_ACTOR_TONE_FEATURES)}
    days = [last_obs - timedelta(days=k) for k in range(n)][::-1]
    rng = np.random.default_rng(7)
    matrix = pd.DataFrame(
        rng.normal(-1.0, 0.5, size=(n, len(ids))), index=pd.Index(days, name="obs_date"),
        columns=list(ids.values()),
    )
    pit = MagicMock()
    pit.get_feature_matrix.side_effect = lambda feature_ids, **_kw: (
        matrix[feature_ids] if set(feature_ids) <= set(matrix.columns) else pd.DataFrame()
    )
    analyzer = pm.NewsMomentumAnalyzer(MagicMock(), pit)
    analyzer._resolve_feature_ids = lambda names: ids if names == pm.GDELT_ACTOR_TONE_FEATURES else {}
    return analyzer


def test_tone_that_ended_weeks_ago_is_unavailable_and_says_when() -> None:
    result = _analyzer(date(2026, 11, 6)).analyze(AS_OF, lookback_days=90)

    assert result.available is False
    assert result.sentiment_trend == result.direction == result.energy_state == "unavailable"
    assert "2026-11-06" in result.details["note"]
    assert result.details["tone_last_observed"] == "2026-11-06"
    assert result.details["tone_age_days"] == 17
    assert result.details["stale_after_days"] == pm.TONE_STALE_AFTER_DAYS
    assert "trend" not in result.details and "energy" not in result.details
    assert any("stale" in w for w in result.warnings)


@pytest.mark.parametrize(("age_days", "available"), [(0, True), (pm.TONE_STALE_AFTER_DAYS, True),
                                                      (pm.TONE_STALE_AFTER_DAYS + 1, False)])
def test_the_staleness_edge(age_days: int, available: bool) -> None:
    result = _analyzer(AS_OF - timedelta(days=age_days)).analyze(AS_OF, lookback_days=90)

    assert result.available is available
    assert result.details["tone_last_observed"] == (AS_OF - timedelta(days=age_days)).isoformat()


# ── registry migration ──────────────────────────────────────────────────────

def _registry(conn) -> None:
    conn.exec_driver_sql(
        "CREATE TABLE feature_registry (name TEXT UNIQUE, model_eligible BOOLEAN, deprecated_at TIMESTAMP)"
    )
    rows = [(n, True, None) for n in revision.ACTOR_AND_TENSION_FEATURES]
    rows += [(n, False, None) for n in revision.THEME_FEATURES if n != "gdelt_opec_tone"]
    rows += [
        ("gdelt_opec_tone", False, "2026-01-01 00:00:00"),   # already deprecated: left as it is
        ("gdelt_avg_tone", True, None),                      # bulk-file features: untouched
        ("gdelt_conflict_count", False, None),
        ("gdelt_actor_powell_tone_v2", True, None),          # not in the list
        ("sp500_full", True, None),
    ]
    for name, eligible, deprecated in rows:
        conn.execute(text("INSERT INTO feature_registry VALUES (:n, :e, :d)"),
                     {"n": name, "e": eligible, "d": deprecated})


def _rows(conn) -> dict[str, tuple[bool, object]]:
    return {n: (bool(e), d) for n, e, d in
            conn.exec_driver_sql("SELECT name, model_eligible, deprecated_at FROM feature_registry")}


def test_upgrade_retires_exactly_the_doc_features(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        _registry(conn)
        monkeypatch.setattr(revision, "op", Operations(MigrationContext.configure(conn)))
        revision.upgrade()
        revision.upgrade()  # idempotent
        rows = _rows(conn)

    for name in revision.DOC_FEATURES:
        assert rows[name][0] is False and rows[name][1] is not None, name
    assert rows["gdelt_opec_tone"] == (False, "2026-01-01 00:00:00")
    for name in ("gdelt_avg_tone", "gdelt_actor_powell_tone_v2", "sp500_full"):
        assert rows[name] == (True, None), name
    assert rows["gdelt_conflict_count"] == (False, None)


def test_downgrade_restores_the_flags_0062_registered(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        _registry(conn)
        monkeypatch.setattr(revision, "op", Operations(MigrationContext.configure(conn)))
        revision.upgrade()
        revision.downgrade()
        rows = _rows(conn)

    for name in revision.ACTOR_AND_TENSION_FEATURES:
        assert rows[name] == (True, None), name
    for name in revision.THEME_FEATURES:
        assert rows[name] == (False, None), name
    assert rows["gdelt_avg_tone"] == (True, None) and rows["gdelt_conflict_count"] == (False, None)


def test_the_retired_list_is_the_doc_pullers_own_feature_set() -> None:
    written = ({q["feature"] for q in gdelt.GDELT_QUERIES}
               | {a["feature"] for a in gdelt.GDELT_ACTOR_QUERIES}
               | {t["feature"] for t in gdelt.GDELT_TENSION_PAIRS})
    assert set(revision.DOC_FEATURES) == written
    assert set(revision.ACTOR_AND_TENSION_FEATURES) == (
        set(pm.GDELT_ACTOR_TONE_FEATURES) | set(pm.GDELT_TENSION_FEATURES))
    assert len(revision.DOC_FEATURES) == 24


def test_chains_after_the_current_head() -> None:
    assert revision.down_revision == "cboe_equity_pcr_20261123"
    assert len(revision.revision) <= 32
