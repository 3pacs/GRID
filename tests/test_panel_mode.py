"""Panel-mode custody, reproducibility and ledger shape (GD6 acceptance 4.3, 4.5, 4.7, 4.8, 4.9).

A temp registry and a temp git 'vault' stand in for the real ones; prices are
synthetic closes served by a fake ``read_window``. Nothing real is read.
"""

from __future__ import annotations

import dataclasses
from datetime import date, timedelta

import pytest

from analysis import panel_mode as pm
from analysis import panel_prices as pp
from analysis.offline_research_proof import digest
from analysis.research_forward_log import scientific_identity
from tests.panel_mode_support import (
    FLOW_SECTOR,
    FLOW_TICKERS,
    NOW,
    anchor_line,
    construct,
    flow_features,
    flow_setup,
    run_spec,
)


def _register(f):
    return pm.register(f["reg"], NOW, "c0de" * 10, constructs=f["construct"], run=f["run"],
                       prereg_path=f["prereg_path"], repo_root=f["repo_root"])


def _discovery_key(f):
    _register(f)
    pm.freeze_inputs(f["reg"], NOW, f["inputs"])
    pm.open_discovery(f["reg"], NOW, f["inputs"], repo_root=f["repo_root"])
    f["vault"].publish(f["reg"])
    return pm.resume_discovery(f["reg"], f["inputs"], f["vault"].witness(f["reg"].registry_id),
                               repo_root=f["repo_root"])


def _panels(f, key, guard, start: date, as_of: date):
    out = {}
    c = f["construct"]
    for sector, manifest in f["manifests"].items():
        prices = pp.load_panel_prices(None, manifest, start=start, as_of=as_of, key=key, guard=guard)
        feats = flow_features(prices.closes(), pm.universe(manifest))
        out[sector] = {f"{c.name}|fwd{h}": pp.build_label_panel(prices, f["run"], c, feats, h) for h in c.horizons}
    return out


def _discover(f, key, guard, **over):
    panels = _panels(f, key, guard, date(2026, 7, 1), date(2028, 6, 30))
    return pm.discover_panel(over.get("constructs", f["construct"]), over.get("run", f["run"]), panels,
                             inputs=pm.discovery_inputs(key), guard=guard)


def _holdout_key(f, frozen):
    pm.open_holdout(f["reg"], frozen, allow_holdout=True, prereg_sha256=f["prereg"], now=NOW,
                    observed=f["inputs"], repo_root=f["repo_root"])
    f["vault"].publish(f["reg"])
    return pm.resume_holdout(f["reg"], frozen, allow_holdout=True, prereg_sha256=f["prereg"],
                             observed=f["inputs"], witness=f["vault"].witness(f["reg"].registry_id),
                             repo_root=f["repo_root"])


def test_full_one_shot_flow_and_every_second_opening_refused(tmp_path, monkeypatch):
    f = flow_setup(tmp_path, monkeypatch)
    guard = pm.OutcomeWindowGuard()
    key = _discovery_key(f)
    frozen = _discover(f, key, guard)
    assert frozen["payload"]["promotion_allowed"] is False
    pm.seal_discovery(key, NOW, frozen, f["manifests"])
    with pytest.raises(PermissionError, match="one shot"):
        pm.open_discovery(f["reg"], NOW, f["inputs"], repo_root=f["repo_root"])
    with pytest.raises(PermissionError, match="already frozen"):
        pm.resume_discovery(f["reg"], f["inputs"], f["vault"].witness(f["reg"].registry_id), repo_root=f["repo_root"])
    with pytest.raises(PermissionError, match="already frozen"):
        pm.seal_discovery(key, NOW, frozen, f["manifests"])

    # holdout: explicit flag + the frozen hash, then the off-host witness
    kw = dict(prereg_sha256=f["prereg"], now=NOW, observed=f["inputs"], repo_root=f["repo_root"])
    with pytest.raises(PermissionError, match="allow_holdout"):
        pm.open_holdout(f["reg"], frozen, allow_holdout=False, **kw)
    with pytest.raises(PermissionError, match="hash given"):
        pm.open_holdout(f["reg"], frozen, allow_holdout=True, **{**kw, "prereg_sha256": "0" * 64})
    tampered = {"payload": {**frozen["payload"], "state": "X"}, "sha256": frozen["sha256"]}
    with pytest.raises(PermissionError, match="changed"):
        pm.open_holdout(f["reg"], tampered, allow_holdout=True, **kw)
    pm.open_holdout(f["reg"], frozen, allow_holdout=True, **kw)
    with pytest.raises(PermissionError, match="already opened"):
        pm.open_holdout(f["reg"], frozen, allow_holdout=True, **kw)
    hkw = dict(allow_holdout=True, prereg_sha256=f["prereg"], observed=f["inputs"], repo_root=f["repo_root"])
    with pytest.raises(PermissionError, match="covers"):
        pm.resume_holdout(f["reg"], frozen, witness=f["vault"].witness(f["reg"].registry_id), **hkw)
    f["vault"].publish(f["reg"])
    hkey = pm.resume_holdout(f["reg"], frozen, witness=f["vault"].witness(f["reg"].registry_id), **hkw)
    hold = _panels(f, hkey, guard, date(2028, 7, 1), date(2030, 6, 28))
    result = pm.evaluate_panel_holdout(frozen, hold, hkey, guard=guard)
    assert result["promotion_allowed"] is False and result["verdict"]["promotion_allowed"] is False
    assert {c["trial"] for c in result["holdout_checks"]} >= {"gd5_form4_buy_w60|fwd5"}
    pm.seal_holdout(hkey, NOW, result)
    with pytest.raises(PermissionError, match="already recorded"):
        pm.resume_holdout(f["reg"], frozen, witness=f["vault"].witness(f["reg"].registry_id), **hkw)
    kinds = pm.verify(f["reg"], f["vault"].witness(f["reg"].registry_id))["kinds"]
    assert kinds == ["header", "preregistration", "inputs_frozen", "discovery_opened", "prices_read",
                     "discovery_frozen", "holdout_opened", "prices_read", "holdout_result"]


def test_no_price_without_a_discovery_key(tmp_path, monkeypatch):
    f = flow_setup(tmp_path, monkeypatch)
    _register(f)
    pm.freeze_inputs(f["reg"], NOW, f["inputs"])
    with pytest.raises(PermissionError, match="no inputs_frozen|PanelDiscoveryKey"):
        pp.load_panel_prices(None, f["manifest"], start=date(2026, 7, 1), as_of=date(2027, 1, 1),
                             key=object(), guard=pm.OutcomeWindowGuard())
    with pytest.raises(TypeError):
        pm.PanelDiscoveryKey(object(), registry=f["reg"], inputs_frozen_sha256="0" * 64,
                             inputs={"as_of_ts": "2026-09-30T00:00:00+00:00"}, witness_tip="0" * 40)
    pm.open_discovery(f["reg"], NOW, f["inputs"], repo_root=f["repo_root"])
    with pytest.raises(PermissionError, match="off-host"):
        pm.resume_discovery(f["reg"], f["inputs"], None, repo_root=f["repo_root"])
    with pytest.raises(PermissionError, match="off-host"):
        pm.resume_discovery(f["reg"], f["inputs"], object(), repo_root=f["repo_root"])
    # the vault only witnesses the 2-record registration: the opening is not witnessed yet
    vault = f["vault"]
    path = vault.worktree / f["reg"].witness_path
    path.parent.mkdir(parents=True, exist_ok=True)
    first = (f["reg"].log().anchor_path.read_bytes().splitlines()[0])
    path.write_bytes(first + b"\n")
    vault.commit("registration only")
    with pytest.raises(PermissionError, match="covers 2 records"):
        pm.resume_discovery(f["reg"], f["inputs"], vault.witness(f["reg"].registry_id), repo_root=f["repo_root"])
    assert f["reader"].calls == []


def test_split_first_no_discovery_read_at_or_after_the_split(tmp_path, monkeypatch):
    f = flow_setup(tmp_path, monkeypatch)
    key = _discovery_key(f)
    guard = pm.OutcomeWindowGuard()
    with pytest.raises(PermissionError, match="split first"):
        pp.load_panel_prices(None, f["manifest"], start=date(2026, 7, 1), as_of=date(2028, 7, 1),
                             key=key, guard=guard)
    _discover(f, key, guard)
    split = date(2028, 7, 1)
    assert f["reader"].calls and all(c["as_of"] < split for c in f["reader"].calls)
    assert all(c["source"] == "TIINGO" and c["as_of_ts"] == key.as_of_ts for c in f["reader"].calls)


def test_changed_prereg_body_is_refused(tmp_path, monkeypatch):
    from tests.panel_mode_support import write_prereg

    f = flow_setup(tmp_path, monkeypatch)
    _register(f)
    pm.freeze_inputs(f["reg"], NOW, f["inputs"])
    write_prereg(f["repo_root"], body="an edited body\n")
    with pytest.raises(PermissionError, match="pre-registration body"):
        pm.open_discovery(f["reg"], NOW, f["inputs"], repo_root=f["repo_root"])


def test_registration_refusals(tmp_path, monkeypatch):
    f = flow_setup(tmp_path, monkeypatch)
    monkeypatch.setattr(pm, "OWNER_LEDGER_DECISION", None)
    with pytest.raises(PermissionError, match="D-GD6-1"):
        _register(f)
    monkeypatch.setattr(pm, "OWNER_LEDGER_DECISION", "shared")
    with pytest.raises(PermissionError, match="owner chose"):
        _register(f)
    monkeypatch.setattr(pm, "OWNER_LEDGER_DECISION", "separate")
    _register(f)
    with pytest.raises(ValueError, match="already registered"):
        _register(f)


def test_a_fork_is_refused(tmp_path, monkeypatch):
    """A second registration under the same id (another time) never gets the first chain's witness."""
    f = flow_setup(tmp_path, monkeypatch)
    _discovery_key(f)
    witness = f["vault"].witness(f["reg"].registry_id)
    fork = pm.PanelRegistry(tmp_path / "fork", f["reg"].registry_id, f["prereg"])
    pm.register(fork, NOW + timedelta(seconds=1), "c0de" * 10, constructs=f["construct"], run=f["run"],
                prereg_path=f["prereg_path"], repo_root=f["repo_root"])
    pm.freeze_inputs(fork, NOW + timedelta(seconds=1), f["inputs"])
    pm.open_discovery(fork, NOW + timedelta(seconds=1), f["inputs"], repo_root=f["repo_root"])
    with pytest.raises(PermissionError, match="fork|does not witness"):
        pm.resume_discovery(fork, f["inputs"], witness, repo_root=f["repo_root"])


def test_unknown_witness_file_and_older_registry_growth_are_refused(tmp_path, monkeypatch):
    f = flow_setup(tmp_path, monkeypatch, supersedes=("gd6-old",))
    vault = f["vault"]
    old = pm.witness_path("gd6-old")
    first = anchor_line("55" * 32, 2)
    vault.write(old, first + b"\n" + anchor_line("66" * 32, 3, first) + b"\n", "old registry grew")
    with pytest.raises(PermissionError, match="older registry grew"):
        _discovery_key(f)
    f2 = flow_setup(tmp_path / "b", monkeypatch)
    f2["vault"].write("05-GRID/Paper-Log/granular/notes.txt", b"stray\n", "unknown file")
    with pytest.raises(PermissionError, match="unknown files"):
        _discovery_key(f2)


def test_rewritten_witness_history_is_refused(tmp_path, monkeypatch):
    f = flow_setup(tmp_path, monkeypatch)
    _discovery_key(f)
    vault = f["vault"]
    path = vault.worktree / f["reg"].witness_path
    lines = path.read_bytes().splitlines()
    path.write_bytes(b"\n".join(lines[:1]) + b"\n")
    vault.commit("truncate")
    with pytest.raises(PermissionError, match="append-only"):
        vault.witness(f["reg"].registry_id)


def test_reproducible_discovery_and_unhashed_timing(tmp_path, monkeypatch):
    f = flow_setup(tmp_path, monkeypatch)
    key = _discovery_key(f)
    guard = pm.OutcomeWindowGuard()
    one = _discover(f, key, guard)
    two = _discover(f, key, guard)  # a resumed run re-reads: the same receipt is accepted
    assert one["sha256"] == two["sha256"] and digest(one["payload"]) == one["sha256"]
    out = tmp_path / "out"
    pm.write_frozen(out, "discovery-frozen.json", one)
    pm.write_timing(out, {"started": "x", "seconds": 1.5})
    with pytest.raises(FileExistsError):
        pm.write_frozen(out, "discovery-frozen.json", two)
    assert "timing" not in one["payload"]


def test_changed_prices_on_a_resumed_read_are_refused(tmp_path, monkeypatch):
    f = flow_setup(tmp_path, monkeypatch)
    key = _discovery_key(f)
    guard = pm.OutcomeWindowGuard()
    _panels(f, key, guard, date(2026, 7, 1), date(2028, 6, 30))
    f["reader"].closes.iloc[50, 0] *= 1.01
    with pytest.raises(PermissionError, match="prices differ"):
        _panels(f, key, guard, date(2026, 7, 1), date(2028, 6, 30))


def test_entity_split_second_holdout(tmp_path, monkeypatch):
    f = flow_setup(tmp_path, monkeypatch, entity_split_salt="gd6-split")
    guard = pm.OutcomeWindowGuard()
    key = _discovery_key(f)
    frozen = _discover(f, key, guard)
    assert frozen["payload"]["spec"]["entity_split_salt"] == "gd6-split"
    pm.seal_discovery(key, NOW, frozen, f["manifests"])
    hkey = _holdout_key(f, frozen)
    result = pm.evaluate_panel_holdout(frozen, _panels(f, hkey, guard, date(2028, 7, 1), date(2030, 6, 28)), hkey,
                                       guard=guard)
    assert all("entity_split" in c for c in result["holdout_checks"])
    assert result["verdict"]["entity_split_required"] is True
    halves = {pm.entity_half(t, "gd6-split") for t in FLOW_TICKERS}
    assert halves == {0, 1}


def test_family_key_shape_matches_scientific_identity():
    c = construct("gd5_form4_buy_w60", 60, horizons=(5, 20), confirmatory=(5,),
                  feature_class="people_density_form4")
    key = pm.family_key(c, "Consumer Staples", 20)
    assert key == "people_density_form4::SECTOR:Consumer Staples|rel_ret|fwd20"
    ident = scientific_identity(pm.family("Consumer Staples", 20), c.feature)
    assert ident == {"target": "SECTOR:Consumer Staples", "label": "rel_ret", "horizon_sessions": 20,
                     "feature_series": "gd5_form4_buy_w60", "feature_suffix": "W60"}


def test_ledger_options_both_implemented():
    sep = run_spec(option="separate", k=1)
    assert sep.ledger_id == "grid-granular-families" and sep.alpha == pytest.approx(0.05)
    shared = run_spec(option="shared", k=3)
    assert shared.ledger_id == "grid-granular-panel" and shared.alpha == pytest.approx(0.10 / 12)
    with pytest.raises(ValueError, match="k >= 3"):
        run_spec(option="shared", k=2).validate()
    with pytest.raises(ValueError, match="ledger_option"):
        dataclasses.replace(sep, ledger_option="nope").validate()


def test_two_sector_run_with_one_manifest_per_sector(tmp_path, monkeypatch):
    """One Holm over both sectors' trials; per-sector manifests and per-manifest price receipts."""
    f = flow_setup(tmp_path, monkeypatch, sectors=("Energy", "Utilities"))
    guard = pm.OutcomeWindowGuard()
    key = _discovery_key(f)
    frozen = _discover(f, key, guard)
    ledger = frozen["payload"]["ledger"]
    assert {t["sector"] for t in ledger} == {"Energy", "Utilities"} and len(ledger) == 4
    assert frozen["payload"]["entities"]["Utilities"] == pm.universe(f["manifests"]["Utilities"])
    pm.seal_discovery(key, NOW, frozen, f["manifests"])
    hkey = _holdout_key(f, frozen)
    result = pm.evaluate_panel_holdout(frozen, _panels(f, hkey, guard, date(2028, 7, 1), date(2030, 6, 28)), hkey,
                                       guard=guard)
    assert {c["sector"] for c in result["holdout_checks"]} == {"Energy", "Utilities"}


def test_freeze_needs_one_manifest_per_declared_sector(tmp_path, monkeypatch):
    f = flow_setup(tmp_path, monkeypatch, sectors=("Energy", "Utilities"))
    _register(f)
    partial = dataclasses.replace(f["inputs"], price_manifest_sha256s=f["inputs"].price_manifest_sha256s[:1])
    with pytest.raises(ValueError, match="per declared sector"):
        pm.freeze_inputs(f["reg"], NOW, partial)


def test_seal_refuses_a_discovery_other_than_the_registered_design(tmp_path, monkeypatch):
    """After a witnessed price read, a re-run with another seed / direction / subset cannot be sealed."""
    f = flow_setup(tmp_path, monkeypatch)
    guard = pm.OutcomeWindowGuard()
    key = _discovery_key(f)
    reseeded = _discover(f, key, guard, run=dataclasses.replace(f["run"], seed=1))
    with pytest.raises(PermissionError, match="registered constructs and run spec"):
        pm.seal_discovery(key, NOW, reseeded, f["manifests"])
    flipped = _discover(f, key, guard, constructs=dataclasses.replace(f["construct"], direction=-1))
    with pytest.raises(PermissionError, match="registered constructs and run spec"):
        pm.seal_discovery(key, NOW, flipped, f["manifests"])
    honest = _discover(f, key, guard)
    subset = {**honest["payload"], "entities": {FLOW_SECTOR: FLOW_TICKERS[:10]}}
    with pytest.raises(PermissionError, match="full frozen admitted universe"):
        pm.seal_discovery(key, NOW, {"payload": subset, "sha256": digest(subset)}, f["manifests"])
    foreign = {**honest["payload"], "inputs": {**honest["payload"]["inputs"], "as_of_ts": "2026-09-01T00:00:00+00:00"}}
    with pytest.raises(PermissionError, match="inputs_frozen record"):
        pm.seal_discovery(key, NOW, {"payload": foreign, "sha256": digest(foreign)}, f["manifests"])
    pm.seal_discovery(key, NOW, honest, f["manifests"])


def test_reproducible_holdout(tmp_path, monkeypatch):
    f = flow_setup(tmp_path, monkeypatch)
    guard = pm.OutcomeWindowGuard()
    key = _discovery_key(f)
    frozen = _discover(f, key, guard)
    pm.seal_discovery(key, NOW, frozen, f["manifests"])
    hkey = _holdout_key(f, frozen)
    one = pm.evaluate_panel_holdout(frozen, _panels(f, hkey, guard, date(2028, 7, 1), date(2030, 6, 28)), hkey,
                                    guard=guard)
    two = pm.evaluate_panel_holdout(frozen, _panels(f, hkey, guard, date(2028, 7, 1), date(2030, 6, 28)), hkey,
                                    guard=guard)
    assert digest(one) == digest(two)
