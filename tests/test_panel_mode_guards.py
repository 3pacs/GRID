"""OutcomeWindowGuard (GD6 acceptance 4.4): standing rules R2 / R3 executable, not procedural."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from analysis import panel_mode as pm
from tests.panel_mode_support import (
    Vault, anchor_line, construct, run_spec, synthetic_panel, v8_terminal_witness,
)

IN = (date(2015, 1, 1), date(2015, 3, 1))
AFTER = (date(2026, 7, 1), date(2027, 1, 1))


def sectors_v6_witness(monkeypatch, root) -> pm.SectorsV6Witness:
    vault = Vault(root)
    vault.write(pm.SECTORS_V6_WITNESS_PATH, anchor_line("33" * 32, 2) + b"\n", "sectors-v6 registration")
    monkeypatch.setattr(pm, "SECTORS_V6_PIN", {"head_sha256": "33" * 32, "records": 2})
    return pm.issue_sectors_v6_witness(vault.cache, remote_url=str(vault.remote))


def test_pins_are_unset_on_main():
    """Until v8 is terminal and sectors-v6 is registered (and pinned by review), R2/R3 hold."""
    assert pm.V8_TERMINAL_PIN is None and pm.SECTORS_V6_PIN is None and pm.OWNER_LEDGER_DECISION is None
    with pytest.raises(PermissionError, match="R2 stays in force"):
        pm.issue_v8_terminal_witness(".")
    with pytest.raises(PermissionError, match="R3 stays in force"):
        pm.issue_sectors_v6_witness(".")


def test_denylist_is_e0s_technology_universe():
    deny = pm.load_technology_denylist()
    assert len(deny) > 782 and {"AAPL", "MSFT", "XLK"} <= deny


@pytest.mark.parametrize("sector,tickers,bench", [
    ("Technology", ["ZZZ1"], "XLK"),
    ("Energy", ["XOM", "AAPL"], "XLE"),           # a denylisted ticker in a non-Technology panel
    ("Energy", ["XOM"], "REL:XLK-SPY"),          # an XLK-relative outcome
])
def test_r2_refuses_technology_in_window_without_v8_terminal(sector, tickers, bench):
    with pytest.raises(PermissionError, match="R2"):
        pm.OutcomeWindowGuard().check(sector, tickers, bench, IN)


def test_r3_refuses_non_technology_in_window_without_sectors_v6():
    with pytest.raises(PermissionError, match="R3"):
        pm.OutcomeWindowGuard().check("Energy", ["XOM", "CVX"], "XLE", IN)


@pytest.mark.parametrize("window", [AFTER, (date(2000, 1, 3), date(2007, 11, 2)),
                                    ("2026-07-01", "2026-12-31"),
                                    (datetime(2026, 7, 1, 20, tzinfo=timezone.utc), date(2027, 1, 1))])
def test_out_of_window_passes_without_witnesses(window):
    guard = pm.OutcomeWindowGuard()
    assert guard.check("Technology", ["AAPL"], "XLK", window)["in_quarantine"] is False
    assert guard.check("Energy", ["XOM"], "XLE", window)["in_quarantine"] is False


@pytest.mark.parametrize("window", [(date(2000, 1, 3), date(2007, 11, 3)), (date(2026, 6, 30), date(2026, 8, 1))])
def test_window_edges_touching_the_quarantine_are_refused(window):
    with pytest.raises(PermissionError):
        pm.OutcomeWindowGuard().check("Energy", ["XOM"], "XLE", window)


def test_witnesses_lift_exactly_their_rule(monkeypatch, tmp_path):
    v8 = v8_terminal_witness(monkeypatch, tmp_path / "v8")
    s6 = sectors_v6_witness(monkeypatch, tmp_path / "s6")
    pm.OutcomeWindowGuard(v8_terminal=v8).check("Technology", ["AAPL"], "XLK", IN)
    with pytest.raises(PermissionError, match="R3"):
        pm.OutcomeWindowGuard(v8_terminal=v8).check("Energy", ["XOM"], "XLE", IN)
    pm.OutcomeWindowGuard(sectors_v6=s6).check("Energy", ["XOM"], "XLE", IN)
    with pytest.raises(PermissionError, match="R2"):
        pm.OutcomeWindowGuard(sectors_v6=s6).check("Energy", ["XOM", "AAPL"], "XLE", IN)
    both = pm.OutcomeWindowGuard(v8_terminal=v8, sectors_v6=s6)
    assert both.check("Energy", ["XOM", "AAPL"], "XLE", IN)["technology"] is True


def test_witness_with_another_head_is_refused(monkeypatch, tmp_path):
    vault = Vault(tmp_path / "v")
    vault.write(pm.V8_WITNESS_PATH, anchor_line("44" * 32, 2) + b"\n", "v8 not terminal")
    monkeypatch.setattr(pm, "V8_TERMINAL_PIN", {"head_sha256": "22" * 32, "records": 9, "status": "STOP"})
    with pytest.raises(PermissionError, match="pinned head"):
        pm.issue_v8_terminal_witness(vault.cache, remote_url=str(vault.remote))
    monkeypatch.setattr(pm, "V8_TERMINAL_PIN", {"head_sha256": "44" * 32, "records": 2, "status": "pending"})
    with pytest.raises(PermissionError, match="holdout_result or a STOP"):
        pm.issue_v8_terminal_witness(vault.cache, remote_url=str(vault.remote))


def test_witnesses_cannot_be_forged():
    with pytest.raises(TypeError):
        pm.V8TerminalWitness(object(), tip="x", path="p", head_sha256="0" * 64, records=9, status="STOP")

    class Fake:
        kind = "vs1-v8-terminal"

    with pytest.raises(PermissionError, match="unknown witness"):
        pm.OutcomeWindowGuard(v8_terminal=Fake())


@pytest.mark.parametrize("c,sector,h", [
    (construct("A90", 90), "Energy", 5),
    (construct("A30", 30), "Utilities", 20),
    (construct("form4_buy_density", 90, channels=("form4",), event_filter="P"), "Energy", 20),
])
def test_sectors_v6_confirmatory_trials_always_refused(monkeypatch, tmp_path, c, sector, h):
    with pytest.raises(PermissionError, match="sectors-v6"):
        pm.OutcomeWindowGuard.check_trial(c, sector, h)
    s6 = sectors_v6_witness(monkeypatch, tmp_path / "s6")
    panel = synthetic_panel(f"{c.name}|fwd{h}", "discovery", "2027-01-04", 40, 25, 1)
    with pytest.raises(PermissionError, match="sectors-v6"):
        pm.OutcomeWindowGuard(sectors_v6=s6).check_panel(c, sector, "XLE", panel)


def test_non_confirmatory_neighbours_pass_check_trial():
    pm.OutcomeWindowGuard.check_trial(construct("A90", 90), "Technology", 5)  # v8's own sector: R2, not R3
    pm.OutcomeWindowGuard.check_trial(construct("A90", 90), "Energy", 10)
    pm.OutcomeWindowGuard.check_trial(construct("form4_buy_density", 60, channels=("form4",), event_filter="P"),
                                      "Energy", 5)


def test_check_panel_guards_decision_to_label_end_and_price_lookback():
    guard = pm.OutcomeWindowGuard()
    after = synthetic_panel("X|fwd5", "discovery", "2026-07-06", 40, 25, 1)
    guard.check_panel(construct("X", 10, horizons=(5,), confirmatory=()), "Energy", "XLE", after)
    px = construct("X", 10, horizons=(5,), confirmatory=(), contains_price=True, price_lookback_days=30)
    with pytest.raises(PermissionError, match="R3"):
        guard.check_panel(px, "Energy", "XLE", after)


def test_discovery_refuses_a_relaxed_guard_subclass():
    class Lenient(pm.OutcomeWindowGuard):
        def check(self, *a, **k):
            return {}

    c = construct("X", 10, horizons=(5,), confirmatory=())
    run = run_spec(sectors=("Energy",))
    panel = synthetic_panel("X|fwd5", "discovery", "2012-01-03", 40, 25, 1)
    with pytest.raises(PermissionError, match="itself"):
        pm.discover_panel(c, run, {"Energy": {"X|fwd5": panel}}, inputs={}, guard=Lenient())
    with pytest.raises(PermissionError, match="R3"):
        pm.discover_panel(c, run, {"Energy": {"X|fwd5": panel}}, inputs={}, guard=pm.OutcomeWindowGuard())


def test_registry_and_witness_paths_never_touch_vs1():
    assert pm.witness_path("gd8-mc1") == "05-GRID/Paper-Log/granular/gd8-mc1.anchors.jsonl"
    for bad in ("vs1-sectors", "my-vs1x", "VS1"):
        with pytest.raises((PermissionError, ValueError)):
            pm.check_registry_id(bad)
    for path in ("05-GRID/Paper-Log/vs1/gd8.anchors.jsonl", "05-GRID/Paper-Log/granular/vs1x.anchors.jsonl",
                 "05-GRID/Paper-Log/other/gd8.anchors.jsonl"):
        with pytest.raises(PermissionError):
            pm.check_witness_path(path)
