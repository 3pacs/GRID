"""Synthetic admission/control tests, not evidence of trading profitability."""
from copy import deepcopy
import pytest
from scripts.intraday_sector import participation

START = "2026-10-02T14:00:00Z"
END = "2026-10-02T14:01:00Z"
NOW = "2026-10-02T14:01:10Z"


def packet():
    return {
        "weights": {"A": .6, "B": .3, "C": .1},
        "weights_source_id": "synthetic-control", "universe_id": "synthetic-three-member",
        "weights_effective_at": START, "weights_available_at": START,
        "interval_start": START, "interval_end": END,
        "constituents": {
            symbol: {"return": value, "sector": sector, "source_at": END, "source_id": "synthetic-control",
                     "available_at": "2026-10-02T14:01:02Z",
                     "interval_start": START, "interval_end": END}
            for symbol, value, sector in [("A", .01, "tech"), ("B", -.01, "finance"), ("C", -.01, "tech")]
        },
    }


def test_positive_index_with_negative_breadth():
    result = participation(packet(), NOW)
    assert result["status"] == "VALID_DIAGNOSTIC"
    assert result["weighted_return"] == pytest.approx(.002)
    assert result["advancing_fraction"] == pytest.approx(1/3)
    assert result["advancing_weight"] == .6
    assert result["contribution_hhi"] == pytest.approx(.46)
    assert result["largest_absolute_contribution_share"] == pytest.approx(.6)
    assert result["sector_contributions"] == pytest.approx({"tech": .005, "finance": -.003})
    assert result["directional_edge"] == "UNVALIDATED"


@pytest.mark.parametrize("mutation", [
    lambda p: p["constituents"].pop("C"),
    lambda p: p["weights"].update(C=.2),
    lambda p: p.update(weights_available_at=END),
    lambda p: p.update(weights_effective_at=END),
    lambda p: p["constituents"]["A"].update(return_value=None, **{"return": None}),
    lambda p: p["constituents"]["A"].update(**{"return": float("nan")}),
    lambda p: p["constituents"]["A"].update(available_at="2026-10-02T14:01:11Z"),
    lambda p: p["constituents"]["A"].update(available_at="2026-10-02T14:01:09Z"),
    lambda p: p["constituents"]["A"].update(source_at="2026-10-02T14:01:09Z", available_at="2026-10-02T14:01:09Z"),
    lambda p: p["constituents"]["A"].update(interval_start=END),
    lambda p: p["constituents"]["A"].update(sector=""),
    lambda p: p["weights"].update(A=True),
    lambda p: p.update(weights_source_id=""),
])
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
