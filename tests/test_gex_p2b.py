"""Offline-only P2-B checks on a synthetic Cboe-shaped packet (not market data)."""

import ast
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
import subprocess

import pytest

from scripts.gex_p2b import harness as h

FIXTURES = Path(__file__).parent / "fixtures/gex_p2b"
RAW = FIXTURES / "cboe_synthetic.json"
RECEIPT = FIXTURES / "receipt_synthetic.json"


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("offline harness attempted external IO")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)


@pytest.fixture(scope="module")
def built():
    return h.build(RAW.read_bytes(), RECEIPT.read_bytes())


def results(files):
    return json.loads(files["results.json"])


def doc():
    return json.loads(RAW.read_bytes())


def receipt_for(raw, **changes):
    r = json.loads(RECEIPT.read_bytes())
    r["raw_sha256"] = hashlib.sha256(raw).hexdigest()
    r.update(changes)
    return json.dumps(r).encode()


def encode(d):
    return json.dumps(d).encode()


def statuses(node):
    if isinstance(node, dict):
        for k, v in node.items():
            if k.endswith("status") and isinstance(v, str):
                yield v
            yield from statuses(v)
    elif isinstance(node, list):
        for v in node:
            yield from statuses(v)


def test_normalize_ledger_identity_and_calendar():
    p = h.normalize(RAW.read_bytes(), RECEIPT.read_bytes())
    assert p["valuation_at"] == "2026-09-30T20:00:00+00:00"  # 16:00 EDT
    assert p["prior_close_derived"] == 764.0
    summary = h.exclusion_summary(p)["by_reason"]
    assert set(summary) == {
        "EXPIRED",
        "IV_MISSING_OR_NONPOSITIVE",
        "IV_OUT_OF_RANGE_PERCENT_SUSPECT",
        "NONSTANDARD_SYMBOL",
        "OI_INVALID",
        "ZERO_OI_NO_EXPOSURE",
    }
    assert summary["EXPIRED"]["oi"] == 100.0
    # Invalid OI is unknown mass, not its raw value or zero.
    assert summary["OI_INVALID"] == {"rows": 1, "oi": 0.0, "oi_unknown": 1}
    nov = [r for r in p["rows"] if r["expiry_date"] == "2026-11-27"]
    assert nov and all(r["early_close_candidate"] for r in nov)
    # 16:00 EST is 21:00Z after the DST change; flagged, never shifted to 13:00.
    assert nov[0]["identity"][1] == "2026-11-27T21:00:00+00:00"
    assert all(r["multiplier_basis"] == "osi_standard_root_inferred" for r in p["rows"])
    assert all(r["oi_as_of"] is None for r in p["rows"])


def test_packet_hash_is_order_independent_and_parameter_sensitive():
    d = doc()
    a = h.normalize(encode(d), receipt_for(encode(d)))
    d["data"]["options"].reverse()
    b = h.normalize(encode(d), receipt_for(encode(d)))
    a.pop("packet_id"), b.pop("packet_id")
    assert h.packet_hash(a) == h.packet_hash(b)
    d["data"]["current_price"] = 762.0
    c = h.normalize(encode(d), receipt_for(encode(d)))
    c.pop("packet_id")
    assert h.packet_hash(c) != h.packet_hash(b)


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda d, r: r.update(raw_sha256="0" * 64), "bind"),
        (lambda d, r: r.update(clock="anik_callback"), "clock"),
        (lambda d, r: r.update(receipt_completed_at=None), "receipt_completed_at"),
        (lambda d, r: r.update(receipt_completed_at=12), "receipt_completed_at"),
        (lambda d, r: r.update(receipt_completed_at="2026-10-01T05:59:08"), "naive"),
        (lambda d, r: r.update(receipt_completed_at="yesterday"), "malformed"),
        (
            lambda d, r: r.update(
                request_started_at="2026-09-30T19:00:00Z",
                receipt_completed_at="2026-09-30T19:59:00Z",
            ),
            "precedes valuation",
        ),
        (lambda d, r: d["data"].update(symbol="QQQ"), "SPY"),
        (lambda d, r: d["data"].update(current_price=None), "current_price"),
        (lambda d, r: d["data"].update(last_trade_time=None), "last_trade_time"),
        (lambda d, r: d["data"]["options"].append(dict(d["data"]["options"][1])), "duplicate"),
        (lambda d, r: d["data"].update(options=[]), "empty"),
    ],
)
def test_packet_rejections(mutate, match):
    d = doc()
    r = json.loads(RECEIPT.read_bytes())
    raw = encode(d)
    r["raw_sha256"] = hashlib.sha256(raw).hexdigest()
    mutate(d, r)
    raw = encode(d)
    if match != "bind":
        r["raw_sha256"] = hashlib.sha256(raw).hexdigest()
    with pytest.raises(h.InputRejected, match=match):
        h.normalize(raw, json.dumps(r).encode())


def test_nan_literal_spot_rejected():
    raw = RAW.read_bytes().replace(b'"current_price": 762.5', b'"current_price": NaN')
    with pytest.raises(h.InputRejected, match="current_price"):
        h.normalize(raw, receipt_for(raw))


def test_class_1_pass_with_separate_classes(built):
    r = results(built)
    assert r["class_1_status"] == "PASS_NUMERICAL"
    pc = r["per_contract"]
    assert pc["reference_float_certified"] is True
    for e in pc["engines"].values():
        assert e["status"] == "PASS_NUMERICAL"
        assert e["counts"] == {"PASS_NUMERICAL": pc["contracts"]}
    curves = r["curves"]
    assert len(curves["reference"]["roots"]) >= 1
    for e in curves["engines"].values():
        assert e["curve_status"] == "PASS_NUMERICAL"
        assert e["root_status"] == "PASS_NUMERICAL"
    assert r["walls"]["engines"]["grid_per_strike"]["status"] == "PASS_NUMERICAL"
    assert r["walls"]["engines"]["gamma_watch"]["status"] == "NOT_SUPPORTED"
    manifest = json.loads(built["manifest.json"])
    assert manifest["class_2"] == {
        "eligible_packets": 0,
        "status": "NOT_SUPPORTED",
        "reason": "no free matched vendor input exists",
    }
    assert manifest["class_3"]["pooled_with_class_1"] is False
    vendor = {row["vendor"]: row for row in r["vendor"]["rows"]}
    assert all(row["status"] != "PASS_NUMERICAL" for row in vendor.values())
    assert r["timing_validation"]["status"] == "INDETERMINATE"
    assert set(statuses(r)) <= set(h.STATUSES)


def test_manifest_hashes_every_artifact_and_replay_is_deterministic(built):
    manifest = json.loads(built["manifest.json"])
    assert set(manifest["files"]) == set(built) - {"manifest.json"}
    for name, sha in manifest["files"].items():
        assert hashlib.sha256(built[name]).hexdigest() == sha
    again = h.build(RAW.read_bytes(), RECEIPT.read_bytes())
    assert again == built


def test_native_behavior_preserved_with_frozen_clock(built):
    native = results(built)["native"]
    grid = native["grid"]
    assert grid["spot"] == 764.0 and grid["spot_basis"] == "prior_completed_unadjusted_close"
    assert "adapter" in grid and grid["gex_aggregate"] is not None
    watch = native["gamma_watch"]
    assert watch["computed_at"] == "2026-09-30T20:00:00+00:00"
    assert {v["name"] for v in watch["variants"]} == {"paired OTM IV", "raw IV filtered"}


def test_attribution_isolates_factors(built):
    at = results(built)["attribution"]
    base, f = at["baseline"], at["factors"]
    assert f["q_watch_native_0.012"]["delta_G"] != 0
    assert f["r_grid_native_0.05"]["delta_G"] != 0
    # Changing only the evaluation spot moves G(spot) but never the roots.
    assert f["spot_prior_close_derived"]["roots"] == base["roots"]
    assert f["spot_prior_close_derived"]["delta_G"] != 0
    for combo in at["combined"].values():
        assert "interaction_residual" in combo
    assert at["oi_vintage"]["status"] == "NOT_SUPPORTED"


def test_fault_injection_in_grid_engine_is_detected(monkeypatch):
    from physics.dealer_gamma import DealerGammaEngine

    real = DealerGammaEngine._gex_at_spots_vectorized
    monkeypatch.setattr(
        DealerGammaEngine,
        "_gex_at_spots_vectorized",
        lambda self, *a: real(self, *a) * 1.001,
    )
    r = results(h.build(RAW.read_bytes(), RECEIPT.read_bytes()))
    assert r["curves"]["engines"]["grid_engine_vectorized"]["curve_status"] == "FAIL_NUMERICAL"
    assert r["class_1_status"] == "FAIL_NUMERICAL"


def test_fault_injection_in_shared_primitive_is_detected(monkeypatch):
    real, sha = h.p2a.load_primitive()
    original = real.gamma
    real.gamma = lambda *a, **k: original(*a, **k) * (1 + 1e-6)
    monkeypatch.setattr(h.p2a, "load_primitive", lambda: (real, sha))
    r = results(h.build(RAW.read_bytes(), RECEIPT.read_bytes()))
    e = r["per_contract"]["engines"]["grid_primitive"]
    assert e["contract_status"] == "FAIL_NUMERICAL"
    assert r["class_1_status"] == "FAIL_NUMERICAL"


def test_collector_drift_is_not_supported_not_a_pass_or_failure(monkeypatch):
    monkeypatch.setattr(h, "WATCH_AGGREGATION_FINGERPRINT", "drifted")
    monkeypatch.setattr(h, "WATCH_NATIVE_FINGERPRINT", "drifted")
    r = results(h.build(RAW.read_bytes(), RECEIPT.read_bytes()))
    assert r["curves"]["engines"]["gamma_watch_aggregation"]["status"] == "NOT_SUPPORTED"
    assert r["native"]["gamma_watch"]["status"] == "NOT_SUPPORTED"
    # An incomplete three-way comparison is never reported as a full PASS.
    assert r["class_1_status"] == "NOT_SUPPORTED"
    assert r["curves"]["engines"]["grid_engine_vectorized"]["status"] == "PASS_NUMERICAL"
    assert r["per_contract"]["engines"]["grid_primitive"]["status"] == "PASS_NUMERICAL"


def test_collector_spot_gate_withholds_native_curve():
    d = doc()
    d["data"]["current_price"] = 790.0
    d["data"]["price_change"] = 0.0
    raw = encode(d)
    r = results(h.build(raw, receipt_for(raw)))
    assert r["native"]["gamma_watch"]["status"] == "NOT_SUPPORTED"
    assert "native gate" in r["native"]["gamma_watch"]["reason"]
    assert r["parameters"]["grid"][0] != 735.0  # common grid recentred on spot


def test_cli_create_only_complete_receipts(tmp_path):
    out = tmp_path / "run"
    assert h.main(["--cboe", str(RAW), "--receipt", str(RECEIPT), "--output", str(out)]) == 0
    names = {str(p.relative_to(out)).replace("\\", "/") for p in out.rglob("*") if p.is_file()}
    assert names == {
        "input/cboe_raw.json",
        "input/receipt.json",
        "results.json",
        "packet.json",
        "exclusions.json",
        "REPORT.md",
        "manifest.json",
    }
    assert (out / "input/cboe_raw.json").read_bytes() == RAW.read_bytes()
    before = {n: (out / n).read_bytes() for n in names}
    with pytest.raises(FileExistsError):
        h.main(["--cboe", str(RAW), "--receipt", str(RECEIPT), "--output", str(out)])
    assert {n: (out / n).read_bytes() for n in names} == before


def test_rejected_packet_still_emits_manifest_and_report(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_bytes(b'{"data": {"symbol": "SPY"}}')
    out = tmp_path / "rejected"
    assert h.main(["--cboe", str(bad), "--receipt", str(RECEIPT), "--output", str(out)]) == 1
    manifest = json.loads((out / "manifest.json").read_bytes())
    assert manifest["class_1"] == {"eligible_packets": 0, "status": "INPUT_REJECTED"}
    assert manifest["class_2"]["eligible_packets"] == 0
    r = json.loads((out / "results.json").read_bytes())
    assert r["class_1_status"] == "INPUT_REJECTED" and "bind" in r["reason"]
    assert "INPUT_REJECTED" in (out / "REPORT.md").read_text()


def test_harness_never_imports_frozen_paper_log_or_writers():
    tree = ast.parse(Path(h.__file__).read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
    assert not any(m.startswith(("paper_log", "ingestion", "store", "db", "requests", "urllib")) for m in imported)
    assert not any(m.startswith("collectors") for m in imported)
