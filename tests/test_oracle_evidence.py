"""Oracle evidence counts expiry clusters and exploratory relative returns.

Read-only audit 2026-11-08: options_flow's 351 scored calls fall on 2 expiry
dates; its +2.64% raw mean was the market's drift on those two windows.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from oracle import evidence as ev
from oracle.evidence import Outcome, build_evidence

T0 = datetime(2026, 4, 1, tzinfo=timezone.utc)


def _o(model: str, pnl: float, expiry: date, direction: str = "CALL", basis: str = "official", verdict: str = "hit") -> Outcome:
    return Outcome(model, pnl, verdict, direction, expiry, T0, basis)


def _model(rows, name):
    return next(m for m in rows["models"] if m["name"] == name)


def test_many_calls_on_few_expiries_get_no_verdict() -> None:
    e1, e2 = date(2026, 4, 17), date(2026, 5, 15)
    rows = [_o("options_flow", 3.0, e) for e in (e1, e2) for _ in range(175)]
    rows += [_o("other", 1.0, e) for e in (e1, e2) for _ in range(175)]
    out = build_evidence(rows)
    m = _model(out, "options_flow")
    assert (m["outcomes"], m["windows"]) == (350, 2)
    assert m["raw_mean_pct"] == 3.0
    assert m["excess_mean_pct"] == 1.0          # 3.0 minus the same-expiry CALL cohort mean of 2.0
    assert m["lower_bound_pct"] is None
    assert m["verdict"] == ev.VERDICT_INSUFFICIENT
    assert out["summary"]["models_tested"] == 0


def test_one_direction_drift_is_not_edge() -> None:
    expiries = [date(2026, 1, 2) + timedelta(days=7 * i) for i in range(40)]
    rows = []
    for i, e in enumerate(expiries):
        drift = 2.0 if i % 2 else -1.0             # every PUT gains or loses with the market
        rows += [_o("put_bot", drift, e, "PUT") for _ in range(5)]
        rows += [_o("crowd", drift, e, "PUT") for _ in range(5)]
    m = _model(build_evidence(rows, seed=1), "put_bot")
    assert m["windows"] == 40
    assert m["raw_mean_pct"] == 0.5
    assert m["excess_mean_pct"] == 0.0
    assert m["verdict"] == ev.VERDICT_NO_RELATIVE_ADVANTAGE


def test_consistent_excess_is_relative_advantage_not_financial_edge() -> None:
    expiries = [date(2026, 1, 2) + timedelta(days=7 * i) for i in range(40)]
    rows = []
    for e in expiries:
        rows += [_o("good", 2.0, e) for _ in range(3)]
        rows += [_o("crowd", -1.0, e) for _ in range(3)]
    out = build_evidence(rows, seed=2)
    assert _model(out, "good")["verdict"] == ev.VERDICT_RELATIVE_ADVANTAGE
    assert _model(out, "crowd")["verdict"] == ev.VERDICT_NO_RELATIVE_ADVANTAGE
    assert out["summary"]["models_with_relative_advantage"] == 1
    assert out["summary"]["models_with_edge"] == 0
    assert out["financial_edge_demonstrated"] is False


def test_hindsight_only_losing_models_cannot_demonstrate_financial_edge() -> None:
    rows = []
    for i in range(30):
        expiry = date(2026, 1, 2) + timedelta(days=i)
        rows += [_o("less_bad", -1.0, expiry, basis="hindsight", verdict="miss"),
                 _o("worse", -3.0, expiry, basis="hindsight", verdict="miss")]
    out = build_evidence(rows)
    less_bad = _model(out, "less_bad")
    assert less_bad["raw_mean_pct"] == -1.0
    assert less_bad["hit_rate"] == 0.0
    assert less_bad["official"] == 0
    assert less_bad["verdict"] == ev.VERDICT_RELATIVE_ADVANTAGE
    assert out["evidence_class"] == "exploratory_relative_comparison"
    assert out["financial_edge_demonstrated"] is False
    assert out["summary"]["models_with_edge"] == 0
    assert "independent_window" not in out["method"]


def test_implausible_outcomes_and_small_models_are_counted_not_listed() -> None:
    e = date(2026, 6, 19)
    rows = [_o("big", 1.0, e) for _ in range(10)] + [_o("big", 7435.1, e)]
    rows += [_o("tiny", 1.0, e) for _ in range(3)]
    rows += [_o("big", 0.5, e, basis="oracle_rescore_hindsight_v1")]
    out = build_evidence(rows)
    sm = out["summary"]
    assert sm["excluded_implausible"] == 1
    assert sm["models_hidden_small"] == 1
    assert [m["name"] for m in out["models"]] == ["big"]
    assert (sm["official_outcomes"], sm["corrected_outcomes"]) == (13, 1)
    assert _model(out, "big")["official"] == 10


def test_job_and_route_are_wired() -> None:
    from intelligence import api_precompute as ap

    assert ap.JOB_BY_KEY["oracle.evidence"].name == "oracle_evidence"
    from api.routers import oracle as oracle_router

    assert "/api/v1/oracle/evidence" in {getattr(r, "path", "") for r in oracle_router.router.routes}
