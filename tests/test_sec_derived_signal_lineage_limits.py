"""Executable evidence for the derived-signal redesign gate, not a repair.

These tests intentionally document current provenance loss. Changing them requires
providing a replacement end-to-end lineage contract, not deleting the evidence.
All engines are mocks; no database or provider is consulted.
"""
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from intelligence.adapters.trust_scorer_adapter import TrustScorerAdapter
from intelligence.signal_registry import SignalRegistry


def _convergence(sources):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = [
        ("NVDA", "BUY", source, 0.6) for source in sources
    ]
    now = datetime(2026, 11, 8, tzinfo=timezone.utc)
    return TrustScorerAdapter()._convergence(engine, now, now + timedelta(hours=4))


def test_mixed_convergence_loses_financial_member_identity():
    clean = _convergence(["insider", "congress", "institutional"])
    contaminated = _convergence(["insider", "congress", "fundamental_divergence"])
    assert len(clean) == len(contaminated) == 1
    # Exact identical persisted payloads can have different input ancestry.
    # A timestamp, ticker or source_count cannot recover the lost membership.
    assert asdict(clean[0]) == asdict(contaminated[0])
    assert contaminated[0].metadata == {"convergence_type": "BUY", "source_count": 3}


def test_sector_prefixed_family_is_equally_lost_in_convergence():
    canonical = _convergence(["insider", "congress", "fundamental_divergence"])
    sector = _convergence(["insider", "congress", "fundamental_divergence:Technology"])
    assert asdict(canonical[0]) == asdict(sector[0])


def test_empty_registry_refresh_cannot_withdraw_an_existing_generation():
    engine = MagicMock()
    assert SignalRegistry.register([], engine) == 0
    # No publication marker or withdrawal is written by an empty refresh.
    # Old persisted entries therefore require a separate current-generation gate.
    engine.begin.assert_not_called()
    engine.connect.assert_not_called()


def test_first_slice_inventory_is_explicitly_incomplete_and_preserves_history():
    import json
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads((root / "docs/reference/SEC_DERIVED_SIGNAL_INVENTORY.json").read_text(encoding="utf-8"))
    assert manifest["coverage_complete"] is False
    assert manifest["production_inventory_performed"] is False
    assert manifest["affected_row_counts"] is None
    entries = {entry["id"]: entry for entry in manifest["entries"]}
    assert len(entries) == len(manifest["entries"])
    assert entries["journal_history"]["status"] == "preserved"
    assert entries["trust_convergence_registry"]["status"] == "open"
    assert entries["oracle_prediction"]["status"] == "open"
    for entry in entries.values():
        assert entry["limitation"]
        assert entry["status"] in {"guarded_this_slice", "preserved", "open"}
        for path in entry["paths"]:
            assert (root / path).is_file(), path
