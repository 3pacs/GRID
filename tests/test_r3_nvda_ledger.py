"""Tests for the R3 NVIDIA commitment ledger (#1085): validation, dedup, scenario math, freeze pin."""

from __future__ import annotations

import copy
import hashlib
import math

import pytest

from analysis import r3_nvda_ledger as L


def rec(**over):
    base = {
        "id": "r1",
        "record_kind": "commitment",
        "stage": "purchase_commitment",
        "direction": "demand",
        "issuer": "Example Corp",
        "counterparty": "NVIDIA Corporation",
        "source": {
            "url": "https://www.sec.gov/Archives/edgar/data/1/x.htm",
            "doc_type": "10-Q",
            "accession": "0000000001-26-000001",
            "section": "Note 9",
            "quote": "we committed to purchase $10.0 billion of systems",
            "published_at": "2026-08-27T16:05:00Z",
            "retrieved_at": "2026-11-12T22:00:00Z",
            "retrieved_sha256": "a" * 64,
        },
        "fiscal_period": "Q2 FY2027",
        "amount": {"value": 10.0, "currency": "USD", "units": "billions", "basis": "counterparty_commitment_to_nvidia"},
        "timing": {"start": None, "end": None, "text": None},
        "conditions": [],
        "contradictions": [],
        "circularity": "none",
        "dedup_group": None,
        "counted": True,
        "firmness": "firm",
        "unknowns": [],
        "confidence": "primary_verbatim",
    }
    for k, v in over.items():
        if k in ("source", "amount") and isinstance(v, dict):
            merged = dict(base[k])
            merged.update(v)
            base[k] = merged
        else:
            base[k] = v
    return base


def fact(rid, value, units="billions", currency="USD", **over):
    return rec(
        id=rid,
        record_kind="financial_fact",
        stage="financial_fact",
        direction="nvidia_financials",
        issuer="NVIDIA Corporation",
        counterparty=None,
        amount={"value": value, "currency": currency, "units": units, "basis": "balance_sheet"},
        counted=False,
        **over,
    )


# ----------------------------------------------------------------- validation


def test_good_record_validates():
    assert L.validate_record(rec()) == []


@pytest.mark.parametrize(
    "over, needle",
    [
        ({"stage": "rumour"}, "stage="),
        ({"confidence": "vibes"}, "confidence="),
        ({"amount": {"value": float("nan")}}, "finite"),
        ({"amount": {"value": float("inf")}}, "finite"),
        ({"amount": {"basis": "made_up"}}, "basis="),
        ({"amount": {"units": "trillions"}}, "units="),
        ({"source": {"quote": ""}}, "verbatim quote"),
        ({"source": {"url": "http://example.com"}}, "https"),
        ({"source": {"retrieved_sha256": "abc"}}, "hex"),
        ({"counted": "yes"}, "true/false"),
        ({"confidence": "secondary_only"}, "only a primary record"),
        ({"amount": {"value": None}}, "only a primary record"),
    ],
)
def test_bad_records_are_rejected(over, needle):
    errs = L.validate_record(rec(**over))
    assert any(needle in e for e in errs), errs


def test_missing_field_rejected():
    r = rec()
    del r["circularity"]
    assert "missing fields" in L.validate_record(r)[0]


def test_record_published_after_cutoff_is_lookahead():
    errs = L.validate_record(rec(source={"published_at": "2026-11-17T21:20:00Z", "retrieved_at": "2026-11-17T22:00:00Z"}))
    assert any("freeze cutoff" in e for e in errs)
    assert L.validate_record(rec(source={"published_at": "2026-11-17T20:59:59Z", "retrieved_at": "2026-11-17T21:00:00Z"})) == []


def test_date_only_published_at_is_accepted():
    assert L.validate_record(rec(source={"published_at": "2026-08-27"})) == []


def test_unvalued_secondary_negative_finding_is_allowed_uncounted():
    r = rec(confidence="secondary_only", amount={"value": None}, counted=False, source={"quote": ""})
    assert L.validate_record(r) == []


def test_duplicate_ids_and_double_counted_group():
    a = rec(id="a", dedup_group="g1")
    b = rec(id="b", dedup_group="g1")
    errs = L.validate_ledger([a, b, copy.deepcopy(a)])
    assert any("duplicate id" in e for e in errs)
    assert any("dedup_group g1" in e for e in errs)


# ----------------------------------------------------------------- stack


def test_stack_counts_one_per_group_and_keeps_context_out():
    records = [
        rec(id="lo1", dedup_group="openai", counted=True, amount={"value": 10.0}),
        rec(id="lo2", dedup_group="openai", counted=False, amount={"value": 10.0}),
        rec(id="capex", counted=True, amount={"value": 90.0, "basis": "counterparty_capex"}),
        rec(id="proj", counted=True, amount={"value": 500.0, "basis": "whole_project"}),
        rec(id="mm", counted=True, amount={"value": 6300.0, "units": "millions"}, circularity="capacity_backstop"),
        fact("rev", 46.7),
    ]
    assert L.validate_ledger(records) == []
    out = L.commitment_stack(records)
    assert out["stack_usd_bn"] == {"purchase_commitment": {"counterparty_commitment_to_nvidia": pytest.approx(16.3)}}
    why = {e["id"]: e["why"] for e in out["excluded"]}
    assert "not counted" in why["lo2"]
    assert "counterparty_capex" in why["capex"]
    assert "whole_project" in why["proj"]
    assert "rev" not in why  # financial facts are not commitments
    assert out["circular_ids"] == ["mm"]
    assert out["counted_by_circularity_usd_bn"] == {"none": pytest.approx(10.0), "capacity_backstop": pytest.approx(6.3)}
    assert out["counted_total_usd_bn"] == pytest.approx(16.3)


def test_stack_skips_non_usd_and_gw():
    records = [
        rec(id="gw", amount={"value": 10.0, "units": "GW", "currency": None}),
        rec(id="eur", amount={"value": 5.0, "currency": "EUR"}),
    ]
    out = L.commitment_stack(records)
    assert out["stack_usd_bn"] == {}
    assert {e["id"] for e in out["excluded"]} == {"gw", "eur"}


# ----------------------------------------------------------------- scenario math


def test_value_per_share_golden():
    res = L.value_per_share(
        base_quarter_revenue_bn=40.0,
        qoq_growth=[0.10, 0.10, 0.0, -0.10],
        fcf_margin=0.5,
        fcf_multiple=20.0,
        net_cash_bn=50.0,
        diluted_shares_bn=25.0,
    )
    quarters = [44.0, 48.4, 48.4, 43.56]
    assert res["quarters_bn"] == pytest.approx(quarters)
    assert res["fwd4q_revenue_bn"] == pytest.approx(sum(quarters))
    assert res["equity_value_bn"] == pytest.approx(sum(quarters) * 0.5 * 20 + 50)
    assert res["value_per_share"] == pytest.approx((sum(quarters) * 10 + 50) / 25)


def test_value_per_share_rejects_bad_inputs():
    with pytest.raises(ValueError):
        L.value_per_share(base_quarter_revenue_bn=1, qoq_growth=[0.1], fcf_margin=0.5, fcf_multiple=1, net_cash_bn=0, diluted_shares_bn=1)
    with pytest.raises(ValueError):
        L.value_per_share(base_quarter_revenue_bn=1, qoq_growth=[0] * 4, fcf_margin=0.5, fcf_multiple=1, net_cash_bn=0, diluted_shares_bn=0)


def test_implied_growth_round_trips():
    kw = dict(base_quarter_revenue_bn=40.0, fcf_margin=0.45, fcf_multiple=25.0, net_cash_bn=40.0, diluted_shares_bn=24.5)
    target = L.value_per_share(qoq_growth=[0.05] * 4, **kw)["value_per_share"]
    g = L.implied_flat_growth(price=target, **kw)
    assert g == pytest.approx(0.05, abs=1e-9)
    assert L.implied_flat_growth(price=1e9, **kw) is None


def _scenario_fixture():
    records = [
        fact("shares", 24.5),
        fact("cash", 56.0),
        fact("debt", 8.5),
        fact("rev", 46.7),
        fact("fcfm", 45.0, units="percent"),
    ]
    spec = {
        "basis": {
            "price": {"assumption": 168.89, "why": "close used for the freeze", "as_of": "2026-11-10", "source": "Tiingo EOD (unqualified)"},
            "diluted_shares": {"observed": "shares"},
            "cash_and_securities": {"observed": "cash"},
            "total_debt": {"observed": "debt"},
            "base_quarter_revenue": {"observed": "rev"},
        },
        "scenarios": {
            "base": {
                "qoq_growth": [{"assumption": 0.05, "why": "x"}] * 4,
                "fcf_margin": {"observed": "fcfm"},
                "fcf_multiple": {"assumption": 25.0, "why": "x"},
            }
        },
    }
    return records, spec


def test_run_scenarios_ok_and_origins():
    records, spec = _scenario_fixture()
    out = L.run_scenarios(records, spec)
    assert out["basis"]["status"] == "OK"
    assert out["basis"]["net_cash_bn"] == pytest.approx(47.5)
    assert out["basis"]["origins"]["diluted_shares"] == "observed:shares"
    base = out["scenarios"]["base"]
    assert base["status"] == "OK"
    expect = L.value_per_share(base_quarter_revenue_bn=46.7, qoq_growth=[0.05] * 4, fcf_margin=0.45, fcf_multiple=25.0, net_cash_bn=47.5, diluted_shares_bn=24.5)
    assert base["value_per_share"] == pytest.approx(expect["value_per_share"])
    assert base["vs_price_pct"] == pytest.approx((expect["value_per_share"] / 168.89 - 1) * 100)
    assert base["implied_flat_qoq_growth_at_price"] is not None
    assert base["label"] == "conditional calculation, not a forecast"


def test_missing_observed_input_makes_scenario_unavailable_not_filled():
    records, spec = _scenario_fixture()
    records = [r for r in records if r["id"] != "fcfm"]
    out = L.run_scenarios(records, spec)
    assert out["scenarios"]["base"]["status"] == "UNAVAILABLE"
    assert "fcfm" in out["scenarios"]["base"]["why"]


def test_missing_basis_makes_everything_unavailable():
    records, spec = _scenario_fixture()
    records = [r for r in records if r["id"] != "shares"]
    out = L.run_scenarios(records, spec)
    assert out["basis"]["status"] == "UNAVAILABLE"
    assert out["scenarios"]["base"]["status"] == "UNAVAILABLE"


def test_secondary_only_input_is_unavailable():
    records, spec = _scenario_fixture()
    for r in records:
        if r["id"] == "rev":
            r["confidence"] = "secondary_only"
    assert L.run_scenarios(records, spec)["basis"]["status"] == "UNAVAILABLE"


def test_assumption_without_reason_or_undated_price_is_an_error():
    records, spec = _scenario_fixture()
    bad = copy.deepcopy(spec)
    bad["scenarios"]["base"]["fcf_multiple"] = {"assumption": 25.0}
    with pytest.raises(ValueError, match="why"):
        L.run_scenarios(records, bad)
    bad = copy.deepcopy(spec)
    del bad["basis"]["price"]["as_of"]
    with pytest.raises(ValueError, match="as_of"):
        L.run_scenarios(records, bad)


def test_assumption_must_be_finite():
    records, spec = _scenario_fixture()
    spec["scenarios"]["base"]["qoq_growth"] = [{"assumption": math.nan, "why": "x"}] * 4
    with pytest.raises(ValueError, match="finite"):
        L.run_scenarios(records, spec)


# ----------------------------------------------------------------- the real ledger


def test_canonical_hash_is_order_and_key_stable():
    a = rec()
    b = dict(reversed(list(rec().items())))
    assert L.ledger_sha256([a]) == L.ledger_sha256([b])
    assert L.ledger_sha256([a]) != L.ledger_sha256([rec(id="other")])


def test_committed_v1_ledger_validates_against_its_build_time_and_pin():
    path = L.REPO / L.LEDGER_PATH
    records = L.load_ledger(path)
    assert L.LEDGER_BUILT_AT is not None
    assert L.validate_ledger(records, built_at=L.LEDGER_BUILT_AT) == []
    assert path.read_bytes() == L.canonical_bytes(records), "ledger file must be in canonical form"
    assert L.FROZEN_LEDGER_SHA256 is not None, "R3 v1 is frozen; the ledger pin must stay set"
    assert L.ledger_sha256(records) == L.FROZEN_LEDGER_SHA256


def test_frozen_v1_and_v0_artifacts_match_their_pins():
    for pins in (L.FROZEN_FILES, L.V0_FROZEN_FILES):
        assert pins
        for rel, want in pins.items():
            got = hashlib.sha256((L.REPO / rel).read_bytes()).hexdigest()
            assert got == want, f"{rel} changed after its freeze; publish a new version instead"


def test_v1_stack_split_and_every_record_has_a_review_field():
    records = L.load_ledger(L.REPO / L.LEDGER_PATH)
    assert len(records) == 453
    assert all("review" in r for r in records)
    stack = L.commitment_stack(records)
    assert stack["counted_total_usd_bn"] == pytest.approx(542.4)
    assert stack["counted_by_firmness_usd_bn"] == pytest.approx(
        {"firm": 118.0, "cancellable": 279.0, "contingent": 133.5, "post_balance": 11.9})
    assert stack["counted_by_circularity_usd_bn"]["capacity_backstop"] == pytest.approx(36.0)


def test_v0_ledger_fails_the_v1_build_time_rule():
    """The reason v1 exists: v0 claims retrievals after its own write time."""
    v0 = L.load_ledger(L.REPO / L.V0_LEDGER_PATH)
    errs = L.validate_ledger(v0, built_at="2026-11-13T01:12:40Z")
    assert any("after the ledger build time" in e for e in errs)


# ----------------------------------------------------------------- v1 validator rules


def test_date_only_publication_on_cutoff_day_is_lookahead():
    errs = L.validate_record(rec(source={"published_at": "2026-11-17", "retrieved_at": "2026-11-17T22:00:00Z"}))
    assert any("freeze cutoff" in e for e in errs)
    assert L.validate_record(rec(source={"published_at": "2026-11-16", "retrieved_at": "2026-11-16T12:00:00Z"})) == []


def test_retrieval_after_build_or_before_publication_is_refused():
    errs = L.validate_record(rec(source={"retrieved_at": "2026-11-13T04:00:00Z"}), built_at="2026-11-13T03:00:00Z")
    assert any("after the ledger build time" in e for e in errs)
    errs = L.validate_record(rec(source={"published_at": "2026-08-27T16:05:00Z", "retrieved_at": "2026-08-01T00:00:00Z"}))
    assert any("before published_at" in e for e in errs)


def test_null_times_only_on_uncounted_secondary_records():
    sec = rec(confidence="secondary_only", counted=False, amount={"value": None},
              source={"published_at": None, "retrieved_at": None, "quote": ""})
    assert L.validate_record(sec) == []
    prim = rec(source={"published_at": None})
    assert any("may be null only" in e for e in L.validate_record(prim))
    prim = rec(source={"retrieved_at": None})
    assert any("may be null only" in e for e in L.validate_record(prim))


def test_hash_must_be_hex_counted_values_non_negative_and_firmness_required():
    assert any("hex" in e for e in L.validate_record(rec(source={"retrieved_sha256": "z" * 64})))
    assert any("negative" in e for e in L.validate_record(rec(amount={"value": -1.0})))
    r = rec()
    del r["firmness"]
    assert any("firmness" in e for e in L.validate_record(r))
    assert any("firmness" in e for e in L.validate_record(rec(firmness="maybe")))


def test_total_and_component_cannot_both_count():
    comp = rec(id="c1")
    tot = rec(id="t1", total_of=["c1"])
    errs = L.validate_ledger([comp, tot])
    assert any("both counted" in e for e in errs)
    tot["counted"] = False
    tot.pop("firmness")
    assert L.validate_ledger([comp, tot]) == []
    assert any("unknown record" in e for e in L.validate_ledger([rec(id="t2", counted=False, total_of=["nope"])]))


def test_price_and_multiple_can_never_be_ledger_ids():
    records, spec = _scenario_fixture()
    bad = copy.deepcopy(spec)
    bad["basis"]["price"] = {"observed": "shares", "as_of": "2026-11-10", "source": "x"}
    with pytest.raises(ValueError, match="never an observed"):
        L.run_scenarios(records, bad)
    bad = copy.deepcopy(spec)
    bad["scenarios"]["base"]["fcf_multiple"] = {"observed": "fcfm"}
    with pytest.raises(ValueError, match="always an assumption"):
        L.run_scenarios(records, bad)


def test_sensitivity_prints_implied_working_capital():
    records, spec = _scenario_fixture()
    spec["scenarios"]["base"]["reference_fcf_margin"] = {"assumption": 0.55, "why": "x"}
    out = L.run_scenarios(records, spec)["scenarios"]["base"]
    assert out["implied_extra_working_capital_bn"] == pytest.approx((0.55 - 0.45) * out["fwd4q_revenue_bn"])
    assert out["implied_extra_dso_days_if_all_receivables"] == pytest.approx(0.10 * 364.0)
