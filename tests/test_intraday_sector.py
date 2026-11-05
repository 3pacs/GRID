"""Synthetic admission/control tests, not evidence of trading profitability."""

from copy import deepcopy
import pytest
from datetime import datetime, timedelta
from scripts.intraday_sector import participation


def interval_age_packet(age):
    p = packet()
    end = datetime.fromisoformat(NOW.replace("Z", "+00:00")) - timedelta(seconds=age)
    start = end - timedelta(seconds=60)
    p.update(interval_start=start.isoformat(), interval_end=end.isoformat(),
             weights_effective_at=start.isoformat(), weights_available_at=start.isoformat())
    for constituent in p["constituents"].values():
        constituent.update(interval_start=start.isoformat(), interval_end=end.isoformat(),
                           source_at=NOW, available_at=NOW)
    return p


@pytest.mark.parametrize("age,limit,expected", [
    (0, 0, "VALID_DIAGNOSTIC"),
    (0.000001, 0, "UNAVAILABLE"),
    (59.999999, 60, "VALID_DIAGNOSTIC"),
    (60, 60, "VALID_DIAGNOSTIC"),
    (60.000001, 60, "UNAVAILABLE"),
    (86410, 60, "UNAVAILABLE"),
    (-1, 60, "UNAVAILABLE"),
])
def test_measured_interval_freshness_boundary(age, limit, expected):
    result = participation(interval_age_packet(age), NOW, max_age_seconds=limit)
    assert result["status"] == expected
    assert result["directional_edge"] == "UNVALIDATED"
    if expected == "UNAVAILABLE":
        assert "weighted_return" not in result
        if age > limit:
            assert result["reason"] == "stale return interval"


def test_fresh_reports_cannot_repair_old_interval_but_fresh_measurements_can():
    old = interval_age_packet(86410)
    assert participation(old, NOW)["reason"] == "stale return interval"
    fresh = interval_age_packet(10)
    partial = deepcopy(old)
    partial["interval_start"], partial["interval_end"] = fresh["interval_start"], fresh["interval_end"]
    assert participation(partial, NOW)["reason"] == "mismatched return intervals"
    recovered = participation(fresh, NOW)
    assert recovered["status"] == "VALID_DIAGNOSTIC"
    assert recovered["weighted_return"] == pytest.approx(0.002)
    assert recovered["interval_end"] == fresh["interval_end"]
    assert recovered["available_at"] == datetime.fromisoformat(NOW.replace("Z", "+00:00")).isoformat()
    assert recovered["directional_edge"] == "UNVALIDATED"


@pytest.mark.parametrize("defect", ["future_weights", "future_source", "future_receipt", "missing_member", "unknown_sector"])
def test_interval_recovery_keeps_existing_lineage_and_completeness_guards(defect):
    p = interval_age_packet(10)
    if defect == "future_weights":
        p["weights_available_at"] = "2026-10-02T14:01:11Z"
    elif defect == "future_source":
        p["constituents"]["A"]["source_at"] = "2026-10-02T14:01:11Z"
    elif defect == "future_receipt":
        p["constituents"]["A"]["available_at"] = "2026-10-02T14:01:11Z"
    elif defect == "missing_member":
        del p["constituents"]["C"]
    else:
        p["constituents"]["C"]["sector"] = ""
    bad = participation(p, NOW)
    assert bad["status"] == "UNAVAILABLE" and "weighted_return" not in bad
    assert participation(interval_age_packet(10), NOW)["status"] == "VALID_DIAGNOSTIC"

START = "2026-10-02T14:00:00Z"
END = "2026-10-02T14:01:00Z"
NOW = "2026-10-02T14:01:10Z"


def packet():
    return {
        "weights": {"A": 0.6, "B": 0.3, "C": 0.1},
        "weights_source_id": "synthetic-control",
        "universe_id": "synthetic-three-member",
        "weights_effective_at": START,
        "weights_available_at": START,
        "interval_start": START,
        "interval_end": END,
        "constituents": {
            symbol: {
                "return": value,
                "sector": sector,
                "source_at": END,
                "source_id": "synthetic-control",
                "available_at": "2026-10-02T14:01:02Z",
                "interval_start": START,
                "interval_end": END,
            }
            for symbol, value, sector in [
                ("A", 0.01, "tech"),
                ("B", -0.01, "finance"),
                ("C", -0.01, "tech"),
            ]
        },
    }


@pytest.mark.parametrize("limit", [float("nan"), float("inf"), -1, True])
def test_invalid_freshness_limits(limit):
    assert (
        participation(packet(), NOW, max_age_seconds=limit)["status"] == "UNAVAILABLE"
    )
    assert (
        participation(packet(), NOW, max_skew_seconds=limit)["status"] == "UNAVAILABLE"
    )


def test_positive_index_with_negative_breadth():
    result = participation(packet(), NOW)
    assert result["status"] == "VALID_DIAGNOSTIC"
    assert result["weighted_return"] == pytest.approx(0.002)
    assert result["advancing_fraction"] == pytest.approx(1 / 3)
    assert result["advancing_weight"] == 0.6
    assert result["contribution_hhi"] == pytest.approx(0.46)
    assert result["largest_absolute_contribution_share"] == pytest.approx(0.6)
    assert result["sector_contributions"] == pytest.approx(
        {"tech": 0.005, "finance": -0.003}
    )
    assert result["directional_edge"] == "UNVALIDATED"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda p: p["constituents"].pop("C"),
        lambda p: p["weights"].update(C=0.2),
        lambda p: p.update(weights_available_at=END),
        lambda p: p.update(weights_effective_at=END),
        lambda p: p["constituents"]["A"].update(return_value=None, **{"return": None}),
        lambda p: p["constituents"]["A"].update(**{"return": float("nan")}),
        lambda p: p["constituents"]["A"].update(available_at="2026-10-02T14:01:11Z"),
        lambda p: p["constituents"]["A"].update(available_at="2026-10-02T14:01:09Z"),
        lambda p: p["constituents"]["A"].update(
            source_at="2026-10-02T14:01:09Z", available_at="2026-10-02T14:01:09Z"
        ),
        lambda p: p["constituents"]["A"].update(interval_start=END),
        lambda p: p["constituents"]["A"].update(sector=""),
        lambda p: p["weights"].update(A=True),
        lambda p: p.update(weights_source_id=""),
    ],
)
def test_rejects_invalid_or_unavailable_inputs(mutation):
    data = deepcopy(packet())
    mutation(data)
    assert participation(data, NOW)["status"] == "UNAVAILABLE"


def test_stale_and_naive_clock_rejected():
    assert participation(packet(), "2026-10-02T14:03:00Z")["status"] == "UNAVAILABLE"
    assert participation(packet(), "2026-10-02T14:01:10")["status"] == "UNAVAILABLE"


def test_flat_is_not_neutral_edge_or_concentration():
    data = packet()
    for row in data["constituents"].values():
        row["return"] = 0
    result = participation(data, NOW)
    assert result["weighted_return"] == 0
    assert result["contribution_hhi"] is None
    assert result["largest_absolute_contribution_share"] is None
    assert result["directional_edge"] == "UNVALIDATED"
