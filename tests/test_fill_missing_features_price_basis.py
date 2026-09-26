"""The extended Yahoo backfill must not impersonate canonical raw closes."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, text


@pytest.fixture
def extended_module(monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "offline-test-placeholder")
    path = Path(__file__).resolve().parents[1] / "scripts" / "fill_missing_features.py"
    spec = importlib.util.spec_from_file_location("fill_missing_features_price_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "YF_MISSING_TICKERS", {"spy_close": "SPY"})
    return module


@pytest.fixture
def engine():
    db = create_engine("sqlite://")
    with db.begin() as conn:
        conn.execute(text("""
            CREATE TABLE source_catalog (
                id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL,
                base_url TEXT, cost_tier TEXT, latency_class TEXT,
                pit_available BOOLEAN, revision_behavior TEXT,
                trust_score TEXT, priority_rank INTEGER, active BOOLEAN
            )
        """))
        conn.execute(text("""
            CREATE TABLE raw_series (
                series_id TEXT NOT NULL, source_id INTEGER NOT NULL,
                obs_date DATE NOT NULL, value REAL NOT NULL,
                raw_payload TEXT, pull_status TEXT NOT NULL
            )
        """))
        conn.execute(text("INSERT INTO source_catalog (id, name) VALUES (1, 'yfinance')"))
        conn.execute(text("""
            INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status)
            VALUES ('YF:SPY:close', 1, '2026-09-24', 101.0, 'SUCCESS')
        """))
    yield db
    db.dispose()


def _synthetic_download(monkeypatch, close=98.0, volume=1234.0):
    calls = []

    def download(tickers, **kwargs):
        calls.append((tickers, kwargs))
        return pd.DataFrame(
            {"Close": [close], "Volume": [volume]},
            index=pd.to_datetime(["2026-09-24"]),
        )

    monkeypatch.setitem(sys.modules, "yfinance", SimpleNamespace(download=download))
    return calls


def test_adjusted_close_is_separate_from_raw_close_and_has_basis(
    extended_module, engine, monkeypatch,
):
    calls = _synthetic_download(monkeypatch)
    assert extended_module.pull_yfinance_extended(engine) == [
        {"ticker": "SPY", "rows": 2, "status": "OK"},
    ]
    assert calls[0][1]["auto_adjust"] is True

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT sc.name, rs.series_id, rs.obs_date, rs.value, rs.raw_payload
            FROM raw_series rs JOIN source_catalog sc ON sc.id = rs.source_id
            ORDER BY rs.series_id
        """)).fetchall()
        source = conn.execute(text("""
            SELECT pit_available, revision_behavior FROM source_catalog
            WHERE name = 'yfinance_adjusted_extended'
        """)).one()

    assert source == (0, "FREQUENT")
    assert len(rows) == 3
    assert ("yfinance", "YF:SPY:close", "2026-09-24", 101.0, None) in rows
    adjusted = next(row for row in rows if row[1] == "YF_ADJ:SPY:close")
    assert adjusted[0] == "yfinance_adjusted_extended"
    assert date.fromisoformat(adjusted[2]) == date(2026, 9, 24)
    assert adjusted[3] == 98.0
    assert json.loads(adjusted[4]) == {
        "provider": "yfinance", "download_auto_adjust": True,
        "price_basis": "adjusted_close", "known_at_verified": False,
    }
    from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS, _YF_SERIES_RE

    assert adjusted[1] not in SEED_MAPPINGS
    assert adjusted[1] not in NEW_MAPPINGS_V2
    assert _YF_SERIES_RE.fullmatch(adjusted[1]) is None
    volume_row = next(row for row in rows if row[1] == "YF:SPY:volume")
    assert volume_row[0] == "yfinance_adjusted_extended"
    assert json.loads(volume_row[4])["measurement_basis"] == "provider_volume"

    # A second pull does not add duplicate observations under the new identity.
    assert extended_module.pull_yfinance_extended(engine) == [
        {"ticker": "SPY", "rows": 0, "status": "OK"},
    ]


def test_extended_series_rejects_uncontracted_fields(extended_module):
    with pytest.raises(ValueError, match="Unsupported extended yfinance field"):
        extended_module._extended_yf_series("SPY", "Adj Close")


def test_empty_download_writes_no_observations(extended_module, engine, monkeypatch):
    monkeypatch.setitem(
        sys.modules, "yfinance",
        SimpleNamespace(download=lambda *args, **kwargs: pd.DataFrame()),
    )
    assert extended_module.pull_yfinance_extended(engine) == []
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT series_id FROM raw_series")).scalars().all()
    assert rows == ["YF:SPY:close"]
