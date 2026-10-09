from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import pytest
from evaluation.continuous_forecasts import (
    Bar,
    ContractError,
    CostScenario,
    Forecast,
    VenueContract,
    evaluate,
    preflight,
)


def fixture():
    origin = datetime(2026, 3, 7, 12, tzinfo=timezone.utc)
    c = VenueContract(
        "SYNTHETIC",
        "BTC/USDT",
        "perpetual",
        "raw_trade_close",
        3600,
        "24/7",
        "synthetic:contract:v1",
        True,
    )

    def bar(i):
        t = origin + timedelta(hours=i)
        return Bar(
            t,
            t + timedelta(seconds=5),
            100 + i,
            c.venue,
            c.instrument,
            c.basis,
            f"synthetic:bar:{i}",
        )

    h = tuple(bar(i) for i in range(-24, 0))
    o = tuple(bar(i) for i in range(1, 73))
    p = tuple(b.price for b in o)
    f = Forecast(
        origin,
        origin - timedelta(minutes=1),
        origin - timedelta(minutes=2),
        "synthetic:model:v1",
        "synthetic:input:v1",
        tuple(b.closed_at for b in o),
        p,
        tuple(v - 2 for v in p),
        p,
        tuple(v + 2 for v in p),
    )
    end = origin + timedelta(hours=72)
    cost = CostScenario(
        5, 3, 12, origin, end, end + timedelta(seconds=5), "synthetic:cost:v1", True
    )
    return c, f, h, o, cost, end + timedelta(seconds=5), bar(0)


def run(c, f, h, o, cost, end, entry):
    return evaluate(c, f, h, o, cost, as_of=end, entry_bar=entry)


def test_weekend_dst_delayed_availability_metrics():
    r = run(*fixture())
    assert r["n_bars"] == 72
    assert r["metrics"]["model"]["mae_quote"] == 0
    assert r["metrics"]["baselines"]["linear_drift"]["mae_quote"] == 0
    assert r["metrics"]["baselines"]["persistence"]["mae_quote"] == 37.5
    assert r["metrics"]["pinball_quote"]["0.1"] == pytest.approx(0.2)
    assert r["metrics"]["interval_80_coverage"] == 1
    assert r["metrics"]["scenario_net_long_return_pct"] == pytest.approx(71.72)
    assert r["provenance"]["synthetic"] is True
    assert r["preflight"]["activation_authorized"] is False
    json.dumps(r, allow_nan=False)


def test_immature():
    c, f, h, _, _, end, _ = fixture()
    r = evaluate(c, f, h, (), None, as_of=end - timedelta(seconds=6))
    assert r["metrics"] is None and r["as_of"] is None and not r["available"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("calendar", "NYSE"),
        ("basis", "adj_close"),
        ("venue", ""),
        ("evidence_ref", ""),
        ("cadence_seconds", 4000),
        ("cadence_seconds", 0),
        ("cadence_seconds", True),
        ("synthetic", None),
    ],
)
def test_contract(field, value):
    c, f, h, *_ = fixture()
    with pytest.raises(ContractError):
        preflight(replace(c, **{field: value}), f, h)


@pytest.mark.parametrize("field", ["issued_at", "input_as_of"])
def test_leakage(field):
    c, f, h, *_ = fixture()
    with pytest.raises(ContractError):
        preflight(c, replace(f, **{field: f.origin + timedelta(seconds=1)}), h)


@pytest.mark.parametrize(
    "field,value",
    [
        ("price", float("nan")),
        ("price", 0),
        ("basis", "mark_close"),
        ("venue", "OTHER"),
        ("instrument", "ETH/USDT"),
        ("source_ref", ""),
    ],
)
def test_bar_fields(field, value):
    c, f, h, *_ = fixture()
    with pytest.raises(ContractError):
        preflight(c, f, (replace(h[0], **{field: value}),) + h[1:])


@pytest.mark.parametrize(
    "mode", ["missing", "duplicate", "late", "early", "naive", "stale", "future"]
)
def test_history(mode):
    c, f, h, *_ = fixture()
    h = list(h)
    if mode == "missing":
        h.pop(3)
    if mode == "duplicate":
        h.insert(3, h[3])
    if mode == "late":
        h[-1] = replace(h[-1], available_at=f.origin)
    if mode == "early":
        h[-1] = replace(h[-1], available_at=h[-1].closed_at - timedelta(seconds=1))
    if mode == "naive":
        h[-1] = replace(h[-1], closed_at=h[-1].closed_at.replace(tzinfo=None))
    if mode == "stale":
        h = h[:-1]
    if mode == "future":
        h.append(replace(h[-1], closed_at=f.origin, available_at=f.origin))
    with pytest.raises(ContractError):
        preflight(c, f, h)


@pytest.mark.parametrize("mode", ["missing", "late", "extra", "shifted"])
def test_outcomes(mode):
    c, f, h, o, cost, end, entry = fixture()
    o = list(o)
    if mode == "missing":
        o.pop(10)
    if mode == "late":
        o[-1] = replace(o[-1], available_at=end + timedelta(seconds=1))
    if mode == "extra":
        o.insert(0, entry)
    if mode == "shifted":
        o = [replace(b, closed_at=b.closed_at + timedelta(seconds=1)) for b in o]
    with pytest.raises(ContractError):
        run(c, f, h, o, cost, end, entry)


@pytest.mark.parametrize(
    "field,value",
    [
        ("point", (100,) * 128),
        ("q10", (100,) * 71),
        ("q10", (1000,) * 72),
        ("point", (-1,) * 72),
    ],
)
def test_forecast_arrays(field, value):
    c, f, h, *_ = fixture()
    with pytest.raises(ContractError):
        preflight(c, replace(f, **{field: value}), h)


def test_wrong_target_grid_and_naive_origin():
    c, f, h, *_ = fixture()
    for new in [
        replace(f, origin=f.origin.replace(tzinfo=None)),
        replace(f, targets=tuple(t + timedelta(minutes=1) for t in f.targets)),
    ]:
        with pytest.raises(ContractError):
            preflight(c, new, h)


@pytest.mark.parametrize(
    "field,value",
    [
        ("fee_bps_per_side", -1),
        ("slippage_bps_per_side", float("nan")),
        ("funding_bps_long", float("inf")),
        ("evidence_ref", ""),
        ("estimated", None),
    ],
)
def test_cost_fields(field, value):
    c, f, h, o, cost, end, entry = fixture()
    with pytest.raises(ContractError):
        run(c, f, h, o, replace(cost, **{field: value}), end, entry)


def test_cost_coverage_and_availability():
    c, f, h, o, cost, end, entry = fixture()
    for new in [
        replace(cost, funding_start=f.origin + timedelta(hours=1)),
        replace(cost, available_at=end + timedelta(seconds=1)),
        replace(cost, available_at=f.origin),
    ]:
        with pytest.raises(ContractError):
            run(c, f, h, o, new, end, entry)


def test_spot_funding_and_perpetual_credit():
    c, f, h, o, cost, end, entry = fixture()
    with pytest.raises(ContractError):
        run(replace(c, market="spot"), f, h, o, cost, end, entry)
    assert run(
        replace(c, market="spot"),
        f,
        h,
        o,
        replace(cost, funding_bps_long=0),
        end,
        entry,
    )["metrics"]["scenario_net_long_return_pct"] == pytest.approx(71.84)
    assert run(c, f, h, o, replace(cost, funding_bps_long=-12), end, entry)["metrics"][
        "scenario_net_long_return_pct"
    ] == pytest.approx(71.96)


def test_entry_exact_and_required():
    c, f, h, o, cost, end, entry = fixture()
    for new in [None, h[-1]]:
        with pytest.raises(ContractError):
            run(c, f, h, o, cost, end, new)


def test_extreme_finite_inputs_fail_closed():
    c, f, h, o, cost, end, entry = fixture()
    with pytest.raises((ContractError, OverflowError)):
        run(c, f, h, o, cost, end, replace(entry, price=1e-320))


def test_half_hour_requires_144_steps():
    c, f, h, *_ = fixture()
    with pytest.raises(ContractError):
        preflight(replace(c, cadence_seconds=1800), f, h)


def test_complete_half_hour_forecast_has_exact_144_targets():
    c, f, _, _, cost, cutoff, entry = fixture()
    c = replace(c, cadence_seconds=1800)

    def bar(i):
        t = f.origin + timedelta(minutes=30 * i)
        return replace(
            entry,
            closed_at=t,
            available_at=t + timedelta(seconds=5),
            price=100 + i / 2,
            source_ref=f"synthetic:half-hour:{i}",
        )

    history = tuple(bar(i) for i in range(-48, 0))
    outcomes = tuple(bar(i) for i in range(1, 145))
    point = tuple(b.price for b in outcomes)
    f = replace(
        f,
        targets=tuple(b.closed_at for b in outcomes),
        point=point,
        q10=tuple(p - 2 for p in point),
        q50=point,
        q90=tuple(p + 2 for p in point),
    )
    result = run(c, f, history, outcomes, cost, cutoff, entry)
    assert result["n_bars"] == 144
    assert result["metrics"]["baselines"]["linear_drift"]["mae_quote"] == 0
    assert (
        result["preflight"]["target_end"]
        == (f.origin + timedelta(hours=72)).isoformat()
    )
