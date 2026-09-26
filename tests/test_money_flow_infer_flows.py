"""``analysis.money_flow._infer_flows`` must not fabricate a period change.

The Fed -> equities edge used to report
``"change": _safe_pct_change(abs_vol, abs_vol * 0.9)`` which is
``(v - 0.9v) / 0.9v = +0.111111`` for every request, and both Fed edges
were stamped ``"confidence": "confirmed"`` although the 50% / 30% split of
the net-liquidity change into equity and bond channels is an assumption.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from analysis.money_flow import _infer_flows, _safe_pct_change


def _layers(net_liquidity_change_1m: float) -> list[dict]:
    return [
        {
            "id": "central_banks",
            "label": "Central Banks",
            "nodes": [
                {
                    "id": "fed",
                    "label": "Federal Reserve",
                    "metrics": {"net_liquidity_change_1m": net_liquidity_change_1m},
                },
            ],
        },
    ]


def test_fed_edges_have_no_fabricated_change_and_are_estimated():
    flows = _infer_flows(_layers(120.0), MagicMock(), date(2026, 9, 17))
    fed = {f["to"]: f for f in flows if f["from"] == "fed"}

    assert set(fed) == {"equities", "bonds"}
    for edge in fed.values():
        assert edge["change"] is None
        assert edge["confidence"] == "estimated"
        assert "assumed" in edge["basis"]

    assert fed["equities"]["volume"] == 60.0
    assert fed["equities"]["direction"] == "inflow"
    assert fed["bonds"]["volume"] == 36.0
    assert fed["bonds"]["direction"] == "outflow"


def test_the_retired_constant_really_was_a_constant():
    """Document why the field was removed: the old formula never varied."""
    for v in (1.0, 50.0, 1e9):
        assert _safe_pct_change(v, v * 0.9) == 0.111111
