"""VS1 supersession across registries (review of #701 at 7eab01ae): replays of attacks A2-A7.

Synthetic data only: hashes are synthetic, the "vault" is a local bare git remote,
no production DB, no price or outcome of any real issuer.

Rules exercised:

* a version pinned as superseded refuses discovery AND holdout (pin only for the
  holdout, so a legitimately opened run survives a later registration);
* a version may open a discovery only if no other VS1 registry witness on the
  pinned ``main`` (every version and the sector registries) covers more than its
  2 registration records, no unknown VS1 witness file exists, and its pinned
  ``earlier`` list is exactly every lower version;
* every census is recorded (discovery_opened, holdout_opened, prices_read) and a
  run during which another registry opened is flagged ``contaminated``.
"""

from __future__ import annotations

import dataclasses
import importlib
import re
from pathlib import Path

import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3
from tests.test_panel_insider_density import NOW
from tests.test_panel_insider_density_v2 import _SEEDS, _discovery_key, _git, _inputs, _observed, _register, _Vault

REPO = Path(__file__).resolve().parent.parent


def _vault(root, **kw):
    return _Vault(root, h=v3, seeds=("vs1-v1", "vs1-v2"), **kw)


@pytest.fixture(autouse=True)
def _v3_not_superseded(monkeypatch):
    """v3 is superseded by v4 (pinned); these replays exercise the Harness through v3 as if it were current."""
    monkeypatch.setattr(v3, "SUPERSEDED_BY", None)


def _open_other(vault, path):
    """Another registry opens: its witness gains an anchor line (records 4)."""
    with open(vault.worktree / path, "ab") as stream:
        stream.write(b'{"head_sha256":"' + b"e" * 64 + b'","prev_anchor_sha256":"x","records":4,'
                     b'"run_at":"2026-10-01T00:00:00+00:00"}\n')
    _git(vault.worktree, "add", "-A")
    _git(vault.worktree, "commit", "-q", "-m", f"opened {path}")
    vault.push()


def _v3_sealed_discovery(tmp_path, vault):
    key, inputs = _discovery_key(tmp_path / "reg", vault)
    frozen = {"payload": {"version": v3.VERSION, "prereg_sha256": v3.PREREG_BODY_SHA256,
                          "state": "DISCOVERY_FROZEN", "inputs": {"inputs_frozen_sha256": key.inputs_frozen_sha256},
                          "calibration": {"state": "CONSISTENT"}, "ledger": []}}
    frozen["sha256"] = v1.digest(frozen["payload"])
    v3.seal_discovery(tmp_path / "reg", NOW, key, frozen)
    return key, inputs, frozen


def _holdout_key(tmp_path, vault, frozen, inputs):
    v3.open_holdout(frozen, allow_holdout=True, prereg_sha256=v3.PREREG_BODY_SHA256, log_dir=tmp_path / "reg",
                    now=NOW, observed=_observed(inputs), witness=vault.witness())
    vault.publish(tmp_path / "reg")
    return v3.resume_holdout(frozen, allow_holdout=True, prereg_sha256=v3.PREREG_BODY_SHA256,
                             log_dir=tmp_path / "reg", observed=_observed(inputs), witness=vault.witness())


# --- fix 1: pinned supersession refuses the holdout too (A5) -----------------------------------------


def _forged_v2_discovery(log_dir):
    """A superseded v2 with a hand-forged discovery_frozen (refused openings bypassed)."""
    inputs = _inputs()
    v2.freeze_inputs(log_dir, NOW, inputs)
    log = v2.registry(log_dir)
    with log.locked():
        records = v2.V2._chain(log)
        fsha = v1._record_sha256(v1._kind(records, "inputs_frozen")[-1])
        frozen = {"payload": {"version": v2.VERSION, "prereg_sha256": v2.PREREG_BODY_SHA256,
                              "state": "DISCOVERY_FROZEN", "inputs": {"inputs_frozen_sha256": fsha}}}
        frozen["sha256"] = v1.digest(frozen["payload"])
        log.append_locked([{"kind": "discovery_opened", "run_at": NOW.isoformat(),
                            "prereg_sha256": v2.PREREG_BODY_SHA256, "inputs_frozen_sha256": fsha},
                           {"kind": "discovery_frozen", "run_at": NOW.isoformat(),
                            "prereg_sha256": v2.PREREG_BODY_SHA256, "inputs_frozen_sha256": fsha,
                            "discovery_sha256": frozen["sha256"]}])
    return frozen, inputs


def test_A5_a_superseded_v2_gets_no_holdout_key_even_with_a_forged_chain(tmp_path):
    vault = _Vault(tmp_path / "vault", h=v2, seeds=("vs1-v1",))
    log_dir = tmp_path / "reg2"
    _register(log_dir, v2)
    vault.publish(log_dir)
    frozen, inputs = _forged_v2_discovery(log_dir)
    assert v2.SUPERSEDED_BY["version"] == "vs1-v4"  # the real pin
    with pytest.raises(PermissionError, match="superseded by vs1-v4"):
        v2.open_holdout(frozen, allow_holdout=True, prereg_sha256=v2.PREREG_BODY_SHA256, log_dir=log_dir,
                        now=NOW, observed=_observed(inputs), witness=vault.witness())
    # even with a holdout_opened forged into the chain, the key is refused on the pin
    log = v2.registry(log_dir)
    with log.locked():
        log.append_locked([{"kind": "holdout_opened", "run_at": NOW.isoformat(),
                            "prereg_sha256": v2.PREREG_BODY_SHA256, "discovery_sha256": frozen["sha256"]}])
    vault.publish(log_dir)
    with pytest.raises(PermissionError, match="superseded by vs1-v4"):
        v2.resume_holdout(frozen, allow_holdout=True, prereg_sha256=v2.PREREG_BODY_SHA256, log_dir=log_dir,
                          observed=_observed(inputs), witness=vault.witness())


def test_A5_the_superseded_v1_holdout_steps_refuse_on_the_pin():
    assert v1.SUPERSEDED_BY["version"] == "vs1-v4"
    with pytest.raises(PermissionError, match="superseded by vs1-v4"):
        v1.open_holdout({}, allow_holdout=True, prereg_sha256=v1.PREREG_BODY_SHA256, log_dir=Path("unused"),
                        now=NOW, observed={})
    with pytest.raises(PermissionError, match="superseded by vs1-v4"):
        v1.resume_holdout({}, allow_holdout=True, prereg_sha256=v1.PREREG_BODY_SHA256, log_dir=Path("unused"),
                          observed={}, witness=None)


def test_the_current_v3_holdout_is_not_refused_by_a_later_registration(tmp_path):
    """Pin only for the holdout: a later version's witness on main stops discovery openings, not v3's holdout."""
    vault = _vault(tmp_path / "vault")
    key, inputs, frozen = _v3_sealed_discovery(tmp_path, vault)
    vault.add_file("05-GRID/Paper-Log/vs1/granular_panel_prereg_v4.anchors.jsonl", b'{"records":2}\n')
    hkey = _holdout_key(tmp_path, vault, frozen, inputs)
    assert hkey.version == "vs1-v3" and hkey.census["files"]["vs1-v4"]


# --- fix 2: every VS1 witness on main (A3, A6, A7) -----------------------------------------------------


V4_PATH = v1.canonical_witness_path("vs1-v4")


def _v4(earlier):
    """A hypothetical v4 on the same Harness, witnessed at its canonical path (synthetic seed line)."""
    pins = dataclasses.replace(v3.V3.pins, version="vs1-v4", number=4, registry_log="v4.jsonl",
                               registry_anchors="v4.anchors.jsonl", registry_lock=".v4.lock",
                               witness_path=V4_PATH, witness_ref="refs/vs1-v4-witness/main", earlier=earlier)
    return v2.Harness(pins)


V3_EARLIER = v2.EarlierVersion(version="vs1-v3", number=3, prereg_sha256=v3.PREREG_BODY_SHA256,
                               registry_head_sha256=v3.REGISTERED_RECORD_SHA256[1], witness_path=v3.WITNESS_PATH,
                               anchor_line=v3.REGISTERED_ANCHOR_LINE)


def test_A3_a_later_version_cannot_open_while_v3_is_opened_whatever_its_earlier_list(tmp_path):
    vault = _vault(tmp_path / "vault")
    _v3_sealed_discovery(tmp_path, vault)  # the v3 witness now covers more than 2 records
    vault.add_file(V4_PATH, v3.REGISTERED_ANCHOR_LINE + b"\n")  # synthetic v4 seed at its canonical path
    for earlier in ((v2.V1_EARLIER, v2.V2_EARLIER, V3_EARLIER), (v2.V1_EARLIER, v2.V2_EARLIER)):
        harness = _v4(earlier)
        witness = harness.check_offhost(vault.cache, remote_url=str(vault.remote))
        with pytest.raises(PermissionError, match="VS1 registry was opened|not every lower version"):
            harness.require_supersession(witness)


def test_an_incomplete_earlier_list_is_refused_even_when_nothing_was_opened(tmp_path):
    vault = _vault(tmp_path / "vault")
    _register(tmp_path / "reg", v3)
    vault.publish(tmp_path / "reg")
    sloppy = v2.Harness(dataclasses.replace(v3.V3.pins, earlier=(v2.V1_EARLIER,)))
    witness = sloppy.check_offhost(vault.cache, remote_url=str(vault.remote))
    with pytest.raises(PermissionError, match="not every lower version"):
        sloppy.require_supersession(witness)


def test_A2_v3_cannot_open_a_discovery_while_another_registry_is_opened(tmp_path):
    for path in (v1.WITNESS_PATH, v2.WITNESS_PATH):
        vault = _vault(tmp_path / f"vault-{Path(path).stem}")
        log_dir = tmp_path / f"reg-{Path(path).stem}"
        _register(log_dir, v3)
        vault.publish(log_dir)
        inputs = _inputs()
        v3.freeze_inputs(log_dir, NOW, inputs)
        _open_other(vault, path)
        with pytest.raises(PermissionError, match="VS1 registry was opened"):
            v3.open_discovery(log_dir, NOW, _observed(inputs), vault.witness())


def test_a_sector_registry_witness_is_known_but_must_stay_at_its_registration(tmp_path):
    sectors = "05-GRID/Paper-Log/vs1/granular_panel_prereg_sectors_v2.anchors.jsonl"
    vault = _vault(tmp_path / "vault")
    vault.add_file(sectors, b'{"records":2}\n')
    key, inputs = _discovery_key(tmp_path / "reg", vault)  # a sector witness at 2 records does not block v3
    assert key.census["files"]["sectors-v2"] == sectors and key.census["unknown"] == []
    vault2 = _vault(tmp_path / "vault2")
    vault2.add_file(sectors, b'{"records":2}\n')
    _register(tmp_path / "reg2", v3)
    vault2.publish(tmp_path / "reg2")
    v3.freeze_inputs(tmp_path / "reg2", NOW, _inputs())
    _open_other(vault2, sectors)
    with pytest.raises(PermissionError, match="VS1 registry was opened"):
        v3.open_discovery(tmp_path / "reg2", NOW, _observed(_inputs()), vault2.witness())


@pytest.mark.parametrize("path", [
    "05-GRID/Paper-Log/vs1-v4/granular_panel_prereg_v4.anchors.jsonl",   # another directory
    "05-GRID/Paper-Log/vs1/granular_panel_prereg_v3b.anchors.jsonl",     # an unknown name
    "05-GRID/Paper-Log/vs1/notes.txt",                                   # anything else in the directory
    "90-Archive/VS1_old/witness.anchors.jsonl",                          # a vs1 witness anywhere
])
def test_A7_an_unknown_vs1_witness_file_refuses_every_opening(tmp_path, path):
    vault = _vault(tmp_path / "vault")
    _register(tmp_path / "reg", v3)
    vault.publish(tmp_path / "reg")
    inputs = _inputs()
    v3.freeze_inputs(tmp_path / "reg", NOW, inputs)
    vault.add_file(path, b"{}\n")
    witness = vault.witness()
    assert path in witness.census["unknown"]
    with pytest.raises(PermissionError, match="unknown VS1 witness files"):
        v3.open_discovery(tmp_path / "reg", NOW, _observed(inputs), witness)


def test_the_witness_directory_readme_is_tolerated(tmp_path):
    vault = _vault(tmp_path / "vault")
    vault.add_file("05-GRID/Paper-Log/vs1/README.md", b"witness files\n")
    key, _ = _discovery_key(tmp_path / "reg", vault)
    assert key.census["unknown"] == []


def test_A6_an_unpinned_v2_is_still_refused_by_the_v3_witness(tmp_path, monkeypatch):
    vault = _Vault(tmp_path / "vault", h=v2, seeds=("vs1-v1",))
    log_dir = tmp_path / "reg2"
    _register(log_dir, v2)
    vault.publish(log_dir)
    inputs = _inputs()
    v2.freeze_inputs(log_dir, NOW, inputs)
    monkeypatch.setattr(v2, "SUPERSEDED_BY", None)
    vault.add_file(v3.WITNESS_PATH, v3.REGISTERED_ANCHOR_LINE + b"\n")
    with pytest.raises(PermissionError, match="later VS1 registry"):
        v2.open_discovery(log_dir, NOW, _observed(inputs), vault.witness())


def test_every_harness_module_pins_every_lower_version_as_earlier():
    """Each analysis/panel_insider_density_v<n>.py lists exactly versions 1..n-1, matching their own pins."""
    pinned = {1: (v1.PREREG_BODY_SHA256, v1.REGISTERED_RECORD_SHA256[1], v1.WITNESS_PATH, v1.REGISTERED_ANCHOR_LINE)}
    found = []
    for path in sorted((REPO / "analysis").glob("panel_insider_density_v*.py"), key=lambda p: int(p.stem[23:])):
        number = int(re.fullmatch(r"panel_insider_density_v(\d+)", path.stem).group(1))
        module = importlib.import_module(f"analysis.{path.stem}")
        harness = getattr(module, f"V{number}")
        assert harness.pins.number == number and harness.pins.version == f"vs1-v{number}"
        assert harness.pins.witness_path == f"05-GRID/Paper-Log/vs1/granular_panel_prereg_v{number}.anchors.jsonl"
        assert sorted(e.number for e in harness.pins.earlier) == list(range(1, number))
        for e in harness.pins.earlier:
            assert (e.prereg_sha256, e.registry_head_sha256, e.witness_path, e.anchor_line) == pinned[e.number]
        pinned[number] = (module.PREREG_BODY_SHA256, module.REGISTERED_RECORD_SHA256[1], module.WITNESS_PATH,
                          module.REGISTERED_ANCHOR_LINE)
        found.append(number)
    assert found == list(range(2, max(found) + 1)) and max(found) >= 3
    # only the newest version is unsuperseded; every older one names a later registered version
    newest = importlib.import_module(f"analysis.panel_insider_density_v{max(found)}")
    assert newest.SUPERSEDED_BY is None
    for number in [1, *found[:-1]]:
        module = v1 if number == 1 else importlib.import_module(f"analysis.panel_insider_density_v{number}")
        assert module.SUPERSEDED_BY and module.SUPERSEDED_BY["registry_head_sha256"] == newest.REGISTERED_RECORD_SHA256[1]


# --- fix 3: every census recorded; contamination flagged (A2) --------------------------------------------


def test_A2_an_opening_of_another_registry_during_v3_is_recorded_and_flags_the_verdict(tmp_path):
    vault = _vault(tmp_path / "vault")
    key, inputs, frozen = _v3_sealed_discovery(tmp_path, vault)
    records = v3.registry(tmp_path / "reg").read_all()
    opened = next(r for r in records if r["kind"] == "discovery_opened")
    assert opened["supersession"]["vs1_witness_census"]["records"] == {"vs1-v1": 2, "vs1-v2": 2, "vs1-v3": 2}
    _open_other(vault, _SEEDS["vs1-v2"][0])  # an old v2 checkout opens later
    with pytest.raises(PermissionError, match="VS1 registry was opened"):
        v3.resume_discovery(tmp_path / "reg", _observed(inputs), vault.witness())
    hkey = _holdout_key(tmp_path, vault, frozen, inputs)  # not refused: pin only
    holdout_opened = [r for r in v3.registry(tmp_path / "reg").read_all() if r["kind"] == "holdout_opened"][0]
    assert holdout_opened["vs1_witness_census"]["records"]["vs1-v2"] == 4
    assert hkey.census["records"]["vs1-v2"] == 4
    found = v3.V3.run_contamination(hkey)
    assert found["contaminated"] and found["detail"]["other_registries_past_registration"] == {"vs1-v2": 4}
    verdict = v3.verdict({"calibration": {"state": "ABSENT"}, "ledger": []}, [], None)
    assert verdict["contaminated"] is False
    flagged = v3.V3.verdict({"calibration": {"state": "ABSENT"}, "ledger": []}, [], None, found)
    assert flagged["contaminated"] is True and flagged["notes"][0].startswith("CONTAMINATED")


def test_every_prices_read_record_carries_the_census(tmp_path):
    import pandas as pd
    from datetime import date

    from tests.test_panel_insider_density import _price_db
    from tests.test_panel_insider_density_v2 import _manifest

    vault = _vault(tmp_path / "vault")
    key, _ = _discovery_key(tmp_path / "reg", vault)
    dates = pd.bdate_range("2019-12-02", "2019-12-31")
    engine = _price_db({"XLK": pd.Series(100.0, index=dates)})
    with engine.connect() as conn:
        v3.load_price_panel(conn, _manifest([]), [], key=key, start=date(2019, 12, 1), as_of=date(2019, 12, 31),
                            window="discovery")
    read = [r for r in v3.registry(tmp_path / "reg").read_all() if r["kind"] == "prices_read"][-1]
    census = read["vs1_witness_census"]
    assert census["records"]["vs1-v1"] == census["records"]["vs1-v2"] == 2
    assert census["records"]["vs1-v3"] >= 4  # its own witness covers discovery_opened
    assert census["unknown"] == [] and census["tip"] == key.witness_tip


# --- review round 2 (R1): the census is keyed by exact, canonical path --------------------------------------


def test_R1_a_leading_zero_witness_path_is_unknown_and_cannot_pose_as_v3(tmp_path):
    vault = _vault(tmp_path / "vault")
    _v3_sealed_discovery(tmp_path, vault)  # the v3 witness now covers more than 2 records
    fake = "05-GRID/Paper-Log/vs1/granular_panel_prereg_v03.anchors.jsonl"
    vault.add_file(fake, v3.REGISTERED_ANCHOR_LINE + b"\n")
    census = v1.vs1_witness_census(vault.remote, "main")
    assert fake in census["unknown"] and census["by_path"][fake]["id"] is None
    assert census["files"]["vs1-v3"] == v3.WITNESS_PATH and fake not in census["files"].values()
    disguised = v2.Harness(dataclasses.replace(
        v3.V3.pins, version="vs1-v4", number=3, registry_log="v4.jsonl", registry_anchors="v4.anchors.jsonl",
        registry_lock=".v4.lock", witness_path=fake, witness_ref="refs/vs1-v4-witness/main"))
    witness = disguised.check_offhost(vault.cache, remote_url=str(vault.remote))
    with pytest.raises(PermissionError, match="unknown VS1 witness files|canonical path"):
        disguised.require_supersession(witness)


def test_R1_a_registry_whose_witness_is_not_its_canonical_path_is_refused(tmp_path):
    vault = _vault(tmp_path / "vault")
    _register(tmp_path / "reg", v3)
    vault.publish(tmp_path / "reg")
    elsewhere = v2.Harness(dataclasses.replace(v3.V3.pins, witness_path=v2.WITNESS_PATH,
                                               registered_anchor_line=v2.REGISTERED_ANCHOR_LINE))
    witness = elsewhere.check_offhost(vault.cache, remote_url=str(vault.remote))
    with pytest.raises(PermissionError, match="canonical path"):
        elsewhere.require_supersession(witness)
    assert v1.canonical_witness_path("vs1-v3") == v3.WITNESS_PATH
    with pytest.raises(ValueError):
        v1.canonical_witness_path("vs1-v03")
