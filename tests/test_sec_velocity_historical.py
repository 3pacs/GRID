"""pull_historical_velocity calls edgartools' module-level get_filings with
keywords it actually accepts.

The backfill used ``get_filings(form="8-K", date=...)``. Module-level
``edgar.get_filings`` has no ``date`` parameter (only ``filing_date``) in
edgartools 5.62.1 and 6.3.0, so every year raised TypeError, was logged as a
warning, and wrote 0 rows. Offline: get_filings is stubbed, no SEC call.
"""
from __future__ import annotations

import inspect
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ingestion import sec_velocity


def _puller(monkeypatch: pytest.MonkeyPatch) -> sec_velocity.SECVelocityPuller:
    monkeypatch.setattr(sec_velocity, "set_identity", lambda ident: None)
    monkeypatch.setattr(
        sec_velocity.BasePuller, "__init__",
        lambda self, engine: (setattr(self, "source_id", 7), setattr(self, "engine", engine)),
    )
    monkeypatch.setattr(sec_velocity.time, "sleep", lambda s: None)
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    conn.execute.return_value.fetchone.return_value = None  # nothing stored yet
    puller = sec_velocity.SECVelocityPuller(db_engine=engine)
    monkeypatch.setattr(puller, "_ensure_feature_registered", lambda sector: None)
    return puller


def test_historical_backfill_passes_filing_date_range(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def fake_get_filings(**kwargs):
        calls.append(kwargs)
        return [SimpleNamespace(filing_date=date(2020, 3, 4))]

    monkeypatch.setattr(sec_velocity, "get_filings", fake_get_filings)
    puller = _puller(monkeypatch)

    out = puller.pull_historical_velocity(start_year=2020, end_year=2020)

    assert calls == [{"form": "8-K", "filing_date": "2020-01-01:2020-12-31"}]
    assert out["errors"] == []
    assert out["rows_inserted"] == 1


def test_historical_backfill_kwargs_bind_to_installed_edgartools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    edgar = pytest.importorskip("edgar")
    signature = inspect.signature(edgar.get_filings)
    calls: list[dict] = []

    def recording_get_filings(**kwargs):
        signature.bind(**kwargs)  # TypeError on a keyword the real API lacks
        calls.append(kwargs)
        return []

    monkeypatch.setattr(sec_velocity, "get_filings", recording_get_filings)
    out = _puller(monkeypatch).pull_historical_velocity(start_year=2019, end_year=2019)

    assert out["errors"] == [], (edgar.__version__, out["errors"])
    assert len(calls) == 1
    assert "date" not in signature.parameters
