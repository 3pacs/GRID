"""Offline-only P2-A checks: synthetic packets, no environment/provider access."""

import copy
from decimal import Decimal, localcontext
import json
import math
from pathlib import Path
import socket
import sqlite3
import subprocess

import pytest

from scripts.gex_p2a import harness as h
from scripts.gex_p2a.reference import Ball, gamma

FIXTURE = Path(__file__).parent / "fixtures/gex_p2a/identical.json"


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("offline harness attempted external IO")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)


def sample():
    return json.loads(FIXTURE.read_bytes())


def test_identical_packets_and_deterministic_replay():
    raw = FIXTURE.read_bytes()
    a, b = h.reconcile(raw), h.reconcile(raw)
    assert h.canonical(a) == h.canonical(b)
    assert a["status"] == "PASS_NUMERICAL"
    assert {e["packet_sha256"] for e in a["engines"]} == {a["packet_sha256"]}
    assert a["vendor_status"] == "NOT_COMPARABLE"
    assert a["grid_production_status"].startswith("NOT_SUPPORTED")
    assert len(a["normalized"]["rows"]) == 4  # zero OI preserved
    for engine in a["engines"]:
        for point in engine["points"]:
            assert Decimal(point["absolute_error"]) <= Decimal(point["error_bound"])


def test_raw_and_normalized_hashes_and_parameter_change():
    p = sample()
    a = h.reconcile(h.canonical(p))
    p["rows"].reverse()
    b = h.reconcile(h.canonical(p))
    assert a["raw_sha256"] != b["raw_sha256"]
    assert a["packet_sha256"] == b["packet_sha256"]
    p["r"] = 0.05
    assert h.reconcile(h.canonical(p))["packet_sha256"] != b["packet_sha256"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("iv", 0),
        ("iv", True),
        ("iv", "20%"),
        ("oi", -1),
        ("oi", 1.5),
        ("strike", 0),
        ("multiplier", 0),
        ("iv_origin", "recovered"),
    ],
)
def test_bad_rows_fail_closed(field, value):
    p = sample()
    p["rows"][0][field] = value
    with pytest.raises(ValueError):
        h.packet(h.canonical(p))


def test_duplicate_clock_expiry_and_empty_cases():
    p = sample()
    p["rows"].append(copy.deepcopy(p["rows"][0]))
    with pytest.raises(ValueError, match="duplicate"):
        h.packet(h.canonical(p))
    p = sample()
    p["availability_basis"] = "anik_callback"
    with pytest.raises(ValueError, match="ANIK"):
        h.packet(h.canonical(p))
    p = sample()
    p["available_at"] = "2026-09-28T15:01:00Z"
    with pytest.raises(ValueError, match="unavailable"):
        h.packet(h.canonical(p))
    p = sample()
    p["valuation_at"] = "2026-09-28T15:00:00"
    with pytest.raises(ValueError, match="naive"):
        h.packet(h.canonical(p))
    p = sample()
    for i, row in enumerate(p["rows"]):
        row["expiry"] = p["valuation_at"]
        row["strike"] += i
    r = h.reconcile(h.canonical(p))
    assert r["status"] == "INPUT_REJECTED"
    assert len(r["normalized"]["exclusions"]) == 4


def test_t_floor_is_policy_difference_not_numeric_pass():
    p = sample()
    p["rows"] = p["rows"][:1]
    p["rows"][0]["expiry"] = "2026-09-28T15:00:01Z"
    r = h.reconcile(h.canonical(p))
    assert r["status"] == "NOT_SUPPORTED"
    assert all(x["status"] == "NOT_SUPPORTED" for x in r["engines"][0]["points"])
    assert all(x["status"] == "PASS_NUMERICAL" for x in r["engines"][1]["points"])


def test_cancellation_bound_uses_gross_not_net():
    p = sample()
    p["rows"] = p["rows"][:2]
    p["rows"][1].update(iv=p["rows"][0]["iv"], oi=p["rows"][0]["oi"])
    r = h.reconcile(h.canonical(p))
    assert r["status"] == "PASS_NUMERICAL"
    for engine in r["engines"]:
        for point in engine["points"]:
            assert point["sign"] == "INDETERMINATE"
            assert Decimal(point["gross_gex"]) > 0
            assert Decimal(point["error_bound"]) > 0


def test_fault_injection_is_not_hidden_by_bound(monkeypatch):
    real, sha = h.load_primitive()
    original = real.gamma
    real.gamma = lambda *a, **k: original(*a, **k) * 1.001
    monkeypatch.setattr(h, "load_primitive", lambda: (real, sha))
    assert h.reconcile(FIXTURE.read_bytes())["status"] == "FAIL_NUMERICAL"


def test_wrong_formula_center_cannot_enlarge_tolerance(monkeypatch):
    real, sha = h.watch_kernel()

    # Corrupt both the floating expression and the bound's high-precision center.
    def wrong(*args, **kwargs):
        return real(*args, **kwargs) + 0.001

    monkeypatch.setattr(h, "watch_kernel", lambda: (wrong, sha))
    assert h.reconcile(FIXTURE.read_bytes())["status"] == "FAIL_NUMERICAL"


def test_timezone_equivalence_and_metadata_identity():
    p = sample()
    a = h.packet(h.canonical(p))[2]
    p["valuation_at"] = "2026-09-28T08:00:00-07:00"
    assert h.packet(h.canonical(p))[2] == a
    p["rows"][0]["oi_as_of"] = "2026-09-25"
    assert h.packet(h.canonical(p))[2] != a
    p["rows"][0]["oi_as_of"] = "2026-10-01"
    with pytest.raises(ValueError, match="future OI"):
        h.packet(h.canonical(p))
    with pytest.raises(ValueError, match="duplicate JSON"):
        h.packet(b'{"schema":1,"schema":2}')


@pytest.mark.parametrize(
    "r,q,iv,oi,mult",
    [(0, 0, 0.02, 1, 10), (-0.01, 0.03, 0.8, 1e6, 100), (0.04, 0.012, 0.2, 0, 100)],
)
def test_parameter_matrix(r, q, iv, oi, mult):
    p = sample()
    p.update(r=r, q=q)
    for row in p["rows"]:
        row.update(iv=iv, oi=oi, multiplier=mult)
    assert h.reconcile(h.canonical(p))["status"] == "PASS_NUMERICAL"


def test_reference_gamma_from_independent_delta_convergence():
    # Independent CDF delta derivative; decreasing central-difference error.
    s, k, t, r, q, iv = 100.0, 103.0, 0.3, 0.04, 0.012, 0.25
    ref = float(gamma(s, k, t, r, q, iv))

    def delta(spot):
        d = (math.log(spot / k) + (r - q + iv * iv / 2) * t) / (iv * math.sqrt(t))
        return math.exp(-q * t) * (1 + math.erf(d / math.sqrt(2))) / 2

    errors = [
        abs((delta(s + step) - delta(s - step)) / (2 * step) - ref)
        for step in (1.0, 0.5, 0.25, 0.125)
    ]
    assert all(b < a for a, b in zip(errors, errors[1:]))
    # Convergence is diagnostic, not an empirically fitted pass tolerance.


def test_ball_propagates_cancellation_and_division_singularity():
    with localcontext() as ctx:
        ctx.prec = 80
        b = (Ball(1) + Ball(1e-16)) - 1
        assert b.e > 0 and b.lo <= Decimal.from_float((1 + 1e-16) - 1) <= b.hi
        with pytest.raises(ValueError, match="zero denominator"):
            Ball(1) / Ball(0, 1)


def test_cli_capture_create_only_and_rejection(tmp_path):
    out = tmp_path / "run"
    assert h.main([str(FIXTURE), "--output", str(out)]) == 0
    assert (out / "input.json").read_bytes() == FIXTURE.read_bytes()
    before = (out / "result.json").read_bytes()
    with pytest.raises(FileExistsError):
        h.main([str(FIXTURE), "--output", str(out)])
    assert (out / "result.json").read_bytes() == before
    bad = tmp_path / "bad.json"
    bad.write_text('{"schema":"wrong"}')
    assert h.main([str(bad), "--output", str(tmp_path / "rejected")]) == 1
    assert (
        json.loads((tmp_path / "rejected/result.json").read_bytes())["status"]
        == "INPUT_REJECTED"
    )
