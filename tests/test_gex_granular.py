"""Synthetic regression tests for offline granular exposure; no market claims."""

import copy
from datetime import datetime
import json
import math
from pathlib import Path
import socket

import pytest

from scripts.gex_p2a import granular as g
from scripts.gex_p2a.harness import canonical

FIXTURE = Path(__file__).parent / "fixtures/gex_p2a/granular.json"


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("offline benchmark attempted network I/O")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)


@pytest.fixture
def packet():
    return json.loads(FIXTURE.read_bytes())


def build(p):
    return g.build(canonical(p))


def point(result, name="oi_sign_baseline", i=0):
    return next(sc for sc in result["scenarios"] if sc["name"] == name)["spots"][i]


def test_fixture_reconciles_all_levels(packet):
    result = build(packet)
    assert result["schema"] == "gex-granular-v1"
    assert result["status"] == "PASS_NUMERICAL"
    assert result["coverage_status"] == "PARTIAL_DECLARED_COVERAGE"
    assert result["inventory_status"] == "HYPOTHETICAL_UNOBSERVED"
    assert result["coverage"]["admitted_count"] == 4
    assert result["coverage"]["expected_count"] == 7
    reasons = {e["reason"] for e in result["coverage"]["exclusions"]}
    assert reasons == {"EXPIRED", "FUTURE_QUOTE_RECEIPT", "MISSING_CONTRACT"}
    for sc in result["scenarios"]:
        for pt in sc["spots"]:
            a = pt["aggregates"]
            for key, total in a["total"].items():
                assert math.fsum(b[key] for b in a["by_expiry"]) == pytest.approx(total)
                assert math.fsum(
                    b[key] for b in a["by_strike_within_expiry"]
                ) == pytest.approx(total)
            assert a["total"]["signed_net"] == pytest.approx(
                a["total"]["call_signed"] + a["total"]["put_signed"]
            )


def test_independent_atm_gamma_and_dollar_scaling(packet):
    packet["r"] = packet["q"] = 0.0
    packet["spots"] = [765.0]
    packet["rows"][0]["iv"] = 0.2
    result = build(packet)
    row = packet["rows"][0]
    c = next(
        c for c in point(result)["contracts"] if c["contract_id"] == row["contract_id"]
    )
    expiry = datetime.fromisoformat(row["expiry"].replace("Z", "+00:00"))
    now = datetime.fromisoformat(packet["valuation_at"].replace("Z", "+00:00"))
    t = (expiry - now).total_seconds() / (365 * 86400)
    # Independent closed-form at-the-money expression; no native Greek import.
    d1 = 0.5 * 0.2 * math.sqrt(t)
    gamma = math.exp(-0.5 * d1 * d1) / (
        math.sqrt(2 * math.pi) * 765 * 0.2 * math.sqrt(t)
    )
    expected = gamma * row["oi"] * row["multiplier"] * 765**2 * 0.01
    assert c["gamma"] == pytest.approx(gamma, rel=1e-13)
    assert c["oi_gross_usd_per_1pct"] == pytest.approx(expected, rel=1e-13)
    assert result["units"] == "USD_per_1pct_underlying_move"
    # Legacy GRID scale converts at each spot; no billion scaling in payload.
    legacy = gamma * row["oi"] * row["multiplier"] * 765
    assert expected == pytest.approx(legacy * 765 * 0.01)


def test_fraction_inventory_gross_is_separate(packet):
    ids = packet["granular"]["expected_contract_ids"]
    packet["granular"]["scenarios"] = [
        {
            "name": "half",
            "kind": "hypothetical_signed_inventory",
            "fractions": dict.fromkeys(ids, -0.5),
        }
    ]
    r = build(packet)
    base = point(r)["aggregates"]["total"]
    half = point(r, "half")["aggregates"]["total"]
    assert half["oi_gross"] == base["oi_gross"]
    assert half["inventory_gross"] == base["oi_gross"] * 0.5
    assert half["signed_net"] == -base["oi_gross"] * 0.5
    assert base["signed_net"] == pytest.approx(
        base["call_oi_gross"] - base["put_oi_gross"]
    )


def test_zero_oi_vs_missing(packet):
    zero = next(row for row in packet["rows"] if row["oi"] == 0)
    r = build(packet)
    c = next(
        c for c in point(r)["contracts"] if c["contract_id"] == zero["contract_id"]
    )
    assert c["oi_gross_usd_per_1pct"] == 0
    packet["rows"] = []
    r = build(packet)
    assert r["status"] == "INSUFFICIENT_DATA"
    assert r["scenarios"] == []
    assert r["coverage"]["admitted_count"] == 0


def test_expiry_utc_bucket_and_unknown_clocks(packet):
    r = build(packet)
    buckets = point(r)["aggregates"]["by_expiry"]
    assert [b["is_0dte"] for b in buckets] == [True, False]
    c = point(r)["contracts"][0]
    assert c["unknown"]["oi_as_of"] is True
    assert c["clocks"]["oi"]["source_at"] is None


@pytest.mark.parametrize("field", ["quote", "greek", "oi"])
def test_future_receipt_excludes_each_field(packet, field):
    row = packet["rows"][0]
    row["clocks"][field]["received_at"] = "2026-09-28T15:00:01Z"
    r = build(packet)
    assert any(
        e["contract_id"] == row["contract_id"]
        and e["reason"] == f"FUTURE_{field.upper()}_RECEIPT"
        for e in r["coverage"]["exclusions"]
    )


@pytest.mark.parametrize("field", ["quote", "greek", "oi"])
def test_packet_receipt_bounds_later_valuation(packet, field):
    packet["valuation_at"] = "2026-09-28T16:00:00Z"
    row = packet["rows"][0]
    row["clocks"][field]["received_at"] = "2026-09-28T15:30:00Z"
    r = build(packet)
    assert not any(
        c["contract_id"] == row["contract_id"] for c in point(r)["contracts"]
    )


def test_distinct_nearby_strikes_and_spots(packet):
    packet["rows"][2]["expiry"] = packet["rows"][0]["expiry"]
    packet["rows"][2]["strike"] = packet["rows"][0]["strike"] + 0.0000005
    packet["spots"] = [765, 765.0000001]
    r = build(packet)
    for pt in r["scenarios"][0]["spots"]:
        a = pt["aggregates"]
        assert math.fsum(
            b["oi_gross"] for b in a["by_strike_within_expiry"]
        ) == pytest.approx(a["total"]["oi_gross"], rel=1e-14)
    for i, s in enumerate(packet["spots"]):
        q = copy.deepcopy(packet)
        q["spots"] = [s]
        assert point(r, i=i)["contracts"] == point(build(q))["contracts"]


@pytest.mark.parametrize("change", ["iv", "oi", "fraction", "clock"])
def test_full_parameter_binding(packet, change):
    before = build(packet)["canonical_parameters_sha256"]
    row = packet["rows"][0]
    if change == "iv":
        row["iv"] += 0.1
    elif change == "oi":
        row["oi"] += 1
    elif change == "clock":
        row["clocks"]["quote"]["source_at"] = None
    else:
        packet["granular"]["scenarios"][0]["fractions"][row["contract_id"]] = 0.25
    assert build(packet)["canonical_parameters_sha256"] != before


def test_excluded_identity_duplicate_rejected(packet):
    row = copy.deepcopy(packet["rows"][4])
    row["contract_id"] = "duplicate-expired"
    packet["rows"].append(row)
    packet["granular"]["expected_contract_ids"].append(row["contract_id"])
    for sc in packet["granular"]["scenarios"]:
        sc["fractions"][row["contract_id"]] = 0
    assert build(packet)["status"] == "INPUT_REJECTED"


@pytest.mark.parametrize("value", [123, [], "naive", "2026-09-28T14:00:00"])
def test_malformed_expired_clock_rejected(packet, value):
    packet["rows"][4]["clocks"]["quote"]["source_at"] = value
    assert build(packet)["status"] == "INPUT_REJECTED"


@pytest.mark.parametrize("value", ["", 123, None])
def test_clock_source_label_required(packet, value):
    packet["rows"][0]["clocks"]["quote"]["source"] = value
    assert build(packet)["status"] == "INPUT_REJECTED"


def test_explicit_unknown_vintage_required(packet):
    del packet["rows"][0]["oi_as_of"]
    assert build(packet)["status"] == "INPUT_REJECTED"


@pytest.mark.parametrize("raw", [b"[]", b"null", b"{", b"42"])
def test_direct_api_rejects_bad_json(raw):
    assert g.build(raw)["status"] == "INPUT_REJECTED"


def test_input_unchanged_and_semantic_order(packet):
    raw = canonical(packet)
    before = copy.deepcopy(packet)
    a = g.build(raw)
    assert packet == before and raw == canonical(before)
    packet["rows"].reverse()
    packet["granular"]["expected_contract_ids"].reverse()
    b = build(packet)
    assert a["canonical_parameters_sha256"] == b["canonical_parameters_sha256"]
    assert a["scenarios"] == b["scenarios"]


def test_cli_rejection_and_retry_evidence(tmp_path):
    source = tmp_path / "bad.json"
    source.write_bytes(b"[]")
    output = tmp_path / "receipt"
    assert g.main([str(source), "--output", str(output)]) == 1
    before = {p.name: p.read_bytes() for p in output.iterdir()}
    assert json.loads(before["result.json"])["status"] == "INPUT_REJECTED"
    with pytest.raises(FileExistsError):
        g.main([str(source), "--output", str(output)])
    assert before == {p.name: p.read_bytes() for p in output.iterdir()}
