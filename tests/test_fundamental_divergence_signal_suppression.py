"""Offline checks for the financial signal fanout's fail-closed boundary."""
from __future__ import annotations

import builtins
from datetime import date
from unittest.mock import MagicMock

import pytest

from intelligence import fundamental_divergence as fd


@pytest.mark.parametrize("classification", ["long_candidate", "short_candidate", "aligned"])
@pytest.mark.parametrize("generation", [None, "a" * 32])
def test_direct_emission_is_suppressed_even_for_bound_rows(
    monkeypatch, classification, generation,
):
    original_import = builtins.__import__

    def reject_contract_import(name, *args, **kwargs):
        if name == "contracts" or name.startswith("contracts."):
            pytest.fail("Suppressed emission must not discover a contract/default DB engine")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_contract_import)
    row = {
        "ticker": "NVDA",
        "sector": "Technology",
        "divergence": -80.0 if classification == "short_candidate" else 80.0,
        "classification": classification,
    }
    if generation is not None:
        row["sec_generation"] = generation

    assert fd._emit_divergence_signal(row) is None


@pytest.mark.parametrize("write_outcome", [2, 0, "error"])
def test_snapshot_never_calls_emission_helper(monkeypatch, write_outcome):
    as_of = date(2026, 11, 8)
    rows = [
        {
            "ticker": ticker,
            "as_of": as_of,
            "sector": "Technology",
            "fundamental_score": 80.0,
            "price_score": 20.0,
            "divergence": divergence,
            "classification": classification,
            "narrative": "Fixture result",
            "sec_generation": "a" * 32,
        }
        for ticker, divergence, classification in [
            ("NVDA", 60.0, "long_candidate"),
            ("MSFT", -60.0, "short_candidate"),
        ]
    ]
    monkeypatch.setattr(fd, "get_financial_generation", lambda conn: "a" * 32)
    engine = MagicMock()
    emitter = MagicMock()
    writer = MagicMock()
    if write_outcome == "error":
        writer.side_effect = RuntimeError("Fixture write failure")
    else:
        writer.return_value = write_outcome
    monkeypatch.setattr(fd, "compute_divergence", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(fd, "_table_exists", lambda *_args: True)
    monkeypatch.setattr(fd, "_upsert_divergence_chunk", writer)
    monkeypatch.setattr(fd, "_emit_divergence_signal", emitter)

    if write_outcome == 0:
        with pytest.raises(fd.FinancialGenerationRetry):
            fd.snapshot_all(engine, as_of=as_of)
        writer.assert_called_once()
        emitter.assert_not_called()
        return
    summary = fd.snapshot_all(engine, as_of=as_of)

    writer.assert_called_once()
    emitter.assert_not_called()
    assert summary["written"] == (0 if write_outcome == "error" else write_outcome)
    assert summary["rows"] == 2
    assert summary["counts"] == {
        "long_candidate": 1, "short_candidate": 1, "aligned": 0,
    }
