"""Synthetic sectors-v6 custody tests: fail-closed pins, v8 binding and registration timing.

No provider, price, outcome or production registry access.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v2 as s2
from analysis import panel_insider_density_sectors_v3 as s3
from analysis import panel_insider_density_sectors_v4 as s4
from analysis import panel_insider_density_sectors_v6 as s6
from analysis import panel_insider_density_v3 as v3
from analysis import panel_insider_density_v4 as v4
from analysis import panel_insider_density_v5 as v5
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from analysis import panel_insider_density_v8 as v8
from analysis.research_forward_log import canonical
from tests.test_panel_insider_density_v2 import _Vault

NOW = datetime(2026, 10, 3, 0, 0, tzinfo=timezone.utc)
V8_CODE = "b" * 40
V6_STOP_LINE = (
    b'{"head_sha256":"b9d9ab5a3eb82df3d7cd3e5be177cc058b28cb92ea86b7ab124673a43309d284",'
    b'"prev_anchor_sha256":"011693a214618385aa9b80d5f8fc528e166dfd6c478898dd34118c4b95c7bac3",'
    b'"records":3,"run_at":"2026-09-29T23:40:00+00:00"}'
)


def test_fail_closed_until_bound(tmp_path):
    assert s6.PREREG_BODY_SHA256 is None and s6.V8_REGISTRATION_HEAD_SHA256 is None
    assert s6.REGISTERED_RECORD_SHA256 is None and s6.REGISTERED_ANCHOR_LINE is None
    assert s6.REGISTRY_ID == "sectors-v6" and s6.WITNESS_PATH == v1.canonical_witness_path("sectors-v6")
    for call in (s6.check_prereg, lambda: s6.registry(tmp_path), lambda: s6.registration_records(NOW, "a" * 40)):
        with pytest.raises(PermissionError, match="not bound"):
            call()
    with pytest.raises(PermissionError, match="no reviewed sectors-v6 joint-run harness"):
        s6.check_open()


GATE_SPEC = "9" * 64
V8_AT = datetime(2026, 10, 2, 0, 0, tzinfo=timezone.utc)


def _bind_v8(monkeypatch, tmp_path):
    """A synthetic v8 registration (real v8 code, monkeypatched pins) and an s6 bound to it.

    The body is a temp copy with the two placeholders filled, as the real pin PR will do.
    """
    v8_reg = tmp_path / "v8reg"
    log = v8.registry(v8_reg)
    log.append(v8.registration_records(V8_AT, V8_CODE))
    heads = tuple(v1._line_sha256(log))
    anchor = (v8_reg / v8.REGISTRY_ANCHORS).read_bytes().splitlines()[0]
    monkeypatch.setattr(v8, "REGISTERED_RECORD_SHA256", heads)
    monkeypatch.setattr(v8, "REGISTERED_ANCHOR_LINE", anchor)
    body = (s4.REPO / s6.PREREG_PATH).read_text(encoding="utf-8")
    filled = tmp_path / "s6-prereg.md"
    filled.write_bytes(body.replace("@@V8_HEAD@@", heads[1]).replace("@@GATE_SPEC@@", GATE_SPEC)
                       .encode("utf-8"))
    monkeypatch.setattr(s6, "PREREG_PATH", filled)
    monkeypatch.setattr(s6, "V8_REGISTRATION_HEAD_SHA256", heads[1])
    monkeypatch.setattr(s6, "GATE_SPEC_SHA256", GATE_SPEC)
    monkeypatch.setattr(s6, "PREREG_BODY_SHA256", v1.prereg_body_sha256(filled))
    return v8_reg, heads


def _real_vault(root):
    vault = _Vault(root, h=v8, seeds=("vs1-v1", "vs1-v2"))
    for m in (v3, v4, v5, s2, s3, s4):
        vault.add_file(m.WITNESS_PATH, m.REGISTERED_ANCHOR_LINE + b"\n")
    vault.add_file(v6.WITNESS_PATH, v6.REGISTERED_ANCHOR_LINE + b"\n" + V6_STOP_LINE + b"\n")
    vault.add_file(v7.WITNESS_PATH, v7.REGISTERED_ANCHOR_LINE + b"\n" + v8.V7_STOP_ANCHOR_LINE + b"\n")
    return vault


def test_registration_binds_v8_and_only_while_v8_is_at_two_records(monkeypatch, tmp_path):
    v8_reg, heads = _bind_v8(monkeypatch, tmp_path)
    vault = _real_vault(tmp_path / "vault")
    vault.publish(v8_reg)
    witness = vault.witness()  # real v8 census: v6/v7 STOPs, v8 at two records
    assert witness.census["records"]["vs1-v8"] == 2 and "sectors-v6" not in witness.census["records"]
    kw = {"v8_log_dir": v8_reg, "witness": witness}
    with pytest.raises(PermissionError, match="fresh v8 off-host witness"):
        s6.register(tmp_path / "s6reg", NOW, "c" * 40, v8_log_dir=v8_reg, witness=witness.census)
    with pytest.raises(ValueError, match="follow v8's registration"):
        s6.register(tmp_path / "s6reg", datetime(2026, 10, 1, tzinfo=timezone.utc), "c" * 40, **kw)
    preview = s6.register(tmp_path / "s6reg", NOW, "c" * 40, **kw)
    assert preview["dry_run"] is True and not (tmp_path / "s6reg" / s6.REGISTRY_LOG).exists()
    header, prereg = s6.registration_records(NOW, "c" * 40)
    assert prereg["technology_run"]["registration_head_sha256"] == heads[1]
    assert prereg["technology_run"]["prereg_sha256"] == v8.PREREG_BODY_SHA256
    assert prereg["terminal_stops"]["vs1-v7"]["head_sha256"] == v8.V7_STOP_HEAD_SHA256
    assert prereg["supersedes"]["registry_head_sha256"] == s4.REGISTERED_RECORD_SHA256[1]
    assert prereg["discovery"]["start"] == v8.DISCOVERY_START and prereg["promotion_allowed"] is False
    assert prereg["generalization_gate"]["spec_sha256"] == GATE_SPEC
    assert prereg["stage0"]["e0"]["manifest_sha256"] == v8.E0_MANIFEST_SHA256
    out = s6.register(tmp_path / "s6reg", NOW, "c" * 40, dry_run=False, **kw)
    assert out["head_sha256"] == preview["would_register_sha256"][-1]
    assert s6.registry(tmp_path / "s6reg").verify_chain()["records"] == 2
    with pytest.raises(PermissionError, match="already registered"):
        s6.register(tmp_path / "s6reg", NOW, "c" * 40, dry_run=False, **kw)

    # The published sectors-v6 anchor is exactly what v8's opening census accepts.
    s6_anchor = (tmp_path / "s6reg" / s6.REGISTRY_ANCHORS).read_bytes()
    vault.add_file(s6.WITNESS_PATH, s6_anchor)
    after = vault.witness()
    assert after.census["records"]["sectors-v6"] == 2
    assert not v8.contamination([after.census])["contaminated"]

    # Once v8 has grown past its registration (an opening), sectors-v6 registration refuses.
    grown = {**witness.census, "records": {**witness.census["records"], "vs1-v8": 3}}
    with pytest.raises(PermissionError, match="before any v8 opening"):
        s6.check_registration_census(grown, v8_log_dir=v8_reg, witness_repo=witness.repo)
    # A census that already carries a sectors-v6 witness is not the pre-sectors-v6 baseline.
    with pytest.raises(PermissionError, match="exact pre-sectors-v6 baseline"):
        s6.check_registration_census(after.census, v8_log_dir=v8_reg, witness_repo=after.repo)
    # A local v8 chain that grew while the witness still shows two records refuses.
    v8.registry(v8_reg).append([{"kind": "inputs_frozen", "prereg_sha256": v8.PREREG_BODY_SHA256,
                                 "run_at": NOW.isoformat()}])
    with pytest.raises(PermissionError, match="differs from its witness"):
        s6.verify_v8_registration(v8_reg, witness.repo, witness.tip)
    with pytest.raises(PermissionError):
        s6.register(tmp_path / "s6reg2", NOW, "c" * 40, v8_log_dir=v8_reg, witness=witness)
    with pytest.raises(PermissionError):
        s6.register(tmp_path / "s6reg3", NOW, "c" * 40, v8_log_dir=tmp_path / "empty-v8", witness=witness)


def test_registration_refuses_wrong_v8_head_or_changed_body(monkeypatch, tmp_path):
    v8_reg, heads = _bind_v8(monkeypatch, tmp_path)
    monkeypatch.setattr(s6, "V8_REGISTRATION_HEAD_SHA256", "d" * 64)
    with pytest.raises(PermissionError, match="not bound"):
        s6.registration_records(NOW, "c" * 40)
    monkeypatch.setattr(s6, "V8_REGISTRATION_HEAD_SHA256", heads[1])
    monkeypatch.setattr(s6, "PREREG_BODY_SHA256", "e" * 64)
    with pytest.raises(PermissionError, match="differs from its pin"):
        s6.check_prereg()
    monkeypatch.setattr(s6, "PREREG_PATH", s4.REPO / "docs/paper_log/vs1-sectors-v6-preregistration.md")
    with pytest.raises(PermissionError, match="placeholder"):
        s6.check_prereg()  # the committed draft still carries @@ placeholders


def test_cli_execute_requires_the_dry_run_head(monkeypatch, tmp_path, capsys):
    from scripts import run_vs1_sectors_v6_registration as cli

    monkeypatch.setattr(s6, "check_prereg", lambda: "x")
    monkeypatch.setattr(v8, "check_offhost", lambda repo: object())
    calls = []

    def fake_register(log_dir, now, code_sha, *, dry_run=True, **kw):
        calls.append(dry_run)
        return {"dry_run": True, "would_register_sha256": ["a" * 64, "b" * 64]} if dry_run else {"head_sha256": "b"}

    monkeypatch.setattr(s6, "register", fake_register)
    base = ["register", "--log-dir", str(tmp_path / "r"), "--v8-log-dir", str(tmp_path / "v"),
            "--vault-repo", str(tmp_path), "--code-sha", "c" * 40, "--run-at", "2026-10-01T00:00:00Z"]
    cli.main(base)
    assert calls == [True]
    with pytest.raises(SystemExit, match="expected-head"):
        cli.main([*base, "--execute", "--expected-head", "f" * 64])
    assert calls == [True, True]
    for bad in ("2026-10-01T00:00:00", "2026-10-01T00:00:00+02:00", "2999-01-01T00:00:00+00:00", "nope"):
        with pytest.raises(SystemExit):
            cli.main([*base[:-1], bad])


def test_pinned_sectors_v6_anchor_is_enforced_by_v8(monkeypatch, tmp_path):
    line = canonical({"head_sha256": "a" * 64, "prev_anchor_sha256": None, "records": 2, "run_at": "x"})
    monkeypatch.setattr(s6, "REGISTERED_ANCHOR_LINE", line)
    assert v8._load_sectors_v6() is s6
    monkeypatch.setattr(v1, "_git", lambda repo, *a, binary=False: line + b"\n")
    v8._sectors_v6_anchor_at_tip(tmp_path, "t")
    other = line.replace(b"a" * 64, b"f" * 64)
    monkeypatch.setattr(v1, "_git", lambda repo, *a, binary=False: other + b"\n")
    with pytest.raises(PermissionError, match="pinned two-record"):
        v8._sectors_v6_anchor_at_tip(tmp_path, "t")
    assert hashlib.sha256(line).hexdigest() != hashlib.sha256(other).hexdigest()
