"""Tests for intelligence/chain_contagion.py.

The simulator is pure in its math: DB access is factored into small
functions that can be stubbed. We build a FakeConn that serves canned
``supply_chain_edges`` / ``supply_chain_nodes`` / ``capital_flows`` rows and
drive the BFS deterministically.

Coverage:
    1. Single-hop price_increase shock (happy path, margin+rev scale)
    2. Two-hop propagation with attenuation
    3. Attenuation strictly reduces incoming shock per hop
    4. Cycle safety — A<->B loop terminates
    5. Revenue scaling uses latest capital_flows entry
    6. Narrative includes shock label, worst tier-1 victim, worst ticker
    7. supply_disruption uses alternative count to scale down impact
    8. Unknown shock_type raises ValueError
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from intelligence.chain_contagion import (
    DEFAULT_PASS_THROUGH,
    HOP_ATTENUATION,
    VALID_SHOCK_TYPES,
    _alt_availability,
    _edge_impact,
    simulate_contagion,
)


# ── Fake DB plumbing ─────────────────────────────────────────────────────────

@dataclass
class _Row:
    """Emulates a SQLAlchemy Row supporting tuple indexing."""

    values: tuple

    def __getitem__(self, idx):
        return self.values[idx]

    def __iter__(self):
        return iter(self.values)


class FakeConn:
    """Minimal ``engine.connect()`` context manager + execute() stub.

    Accepts a dict of edges keyed by upstream_id (list of edge tuples) and
    per-actor revenues / labels, then matches SQL prefix to dispatch.
    """

    def __init__(
        self,
        edges_by_upstream: dict[str, list[tuple]],
        revenues: dict[str, float] | None = None,
        labels: dict[str, str] | None = None,
        alt_counts: dict[tuple[str, str | None], int] | None = None,
    ) -> None:
        self._edges = edges_by_upstream
        self._revenues = revenues or {}
        self._labels = labels or {}
        self._alt_counts = alt_counts or {}

    # context manager protocol
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def connect(self):
        return self

    def execute(self, stmt, params=None):
        sql = str(stmt).strip().lower()
        merged: dict[str, Any] = {}
        # Pull bound params from the TextClause if present.
        try:
            compiled = stmt.compile()
            for k, v in (compiled.params or {}).items():
                if v is not None:
                    merged[k] = v
        except Exception:
            pass
        if params:
            merged.update(params)
        params = merged

        # to_regclass checks
        if "to_regclass" in sql:
            name = params.get("n", "")
            present = name in {
                "supply_chain_edges",
                "supply_chain_nodes",
                "capital_flows",
            }
            return _Result([_Row((name if present else None,))])

        # downstream edge fetch
        if "from supply_chain_edges where upstream_id" in sql:
            node = params.get("n")
            rows = [
                _Row(e) for e in self._edges.get(node, [])
            ]
            return _Result(rows)

        # alt-count query
        if "count(distinct upstream_id)" in sql:
            d = params.get("d")
            it = params.get("it")
            c = self._alt_counts.get((d, it), 1)
            return _Result([_Row((c,))])

        # label lookup
        if "from supply_chain_nodes where id" in sql:
            ids = params.get("ids", [])
            rows = [_Row((i, self._labels.get(i, i))) for i in ids if i in self._labels]
            return _Result(rows)

        # revenue lookup
        if "from capital_flows" in sql:
            ids = params.get("ids", [])
            rows = [
                _Row((i, self._revenues[i])) for i in ids if i in self._revenues
            ]
            return _Result(rows)

        return _Result([])


class _Result:
    def __init__(self, rows: list[_Row]) -> None:
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._rows[0][0] if self._rows else None


class FakeEngine:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn

    def connect(self):
        return self._conn


# Edge tuple layout (7 cols) matches _EDGE_COLS order.
def _edge(
    up: str,
    down: str,
    *,
    rel: str = "raw_material",
    input_type: str | None = None,
    annual_usd: float | None = None,
    pct: float | None = None,
    choke: float | None = None,
) -> tuple:
    return (up, down, rel, input_type, annual_usd, pct, choke)


# ── Pure math tests ──────────────────────────────────────────────────────────

def test_alt_availability_bounds():
    assert _alt_availability(0) == 0.0
    assert _alt_availability(1) == 0.0
    assert _alt_availability(2) == pytest.approx(0.5)
    assert _alt_availability(10) == pytest.approx(0.9)


def test_edge_impact_is_negative_for_price_increase():
    hit = _edge_impact(
        incoming_shock=0.30,
        shock_type="price_increase",
        pct_cogs=0.10,
        pass_through=0.70,
        alt_count=1,
    )
    # 0.30 * 0.10 * 0.70 = 0.021 → -0.021
    assert hit == pytest.approx(-0.021)


def test_edge_impact_uses_default_pct_when_missing():
    hit = _edge_impact(
        incoming_shock=0.30,
        shock_type="price_increase",
        pct_cogs=None,
        pass_through=0.70,
        alt_count=1,
    )
    # default pct_cogs = 0.05 → 0.30*0.05*0.70 = 0.0105
    assert hit == pytest.approx(-0.0105)


def test_edge_impact_disruption_respects_alternatives():
    solo = _edge_impact(
        0.30, "supply_disruption", pct_cogs=0.20, pass_through=0.7, alt_count=1
    )
    with_alts = _edge_impact(
        0.30, "supply_disruption", pct_cogs=0.20, pass_through=0.7, alt_count=4
    )
    assert abs(with_alts) < abs(solo)


def test_edge_impact_pct_over_one_gets_normalized():
    # legacy seed rows use 0-100 instead of 0-1
    hit = _edge_impact(0.10, "price_increase", 50.0, pass_through=1.0, alt_count=1)
    # 0.10 * 0.50 * 1.0 = 0.05
    assert hit == pytest.approx(-0.05)


# ── Integration tests via FakeEngine ─────────────────────────────────────────

def test_simple_one_hop_price_shock_scales_revenue():
    edges = {
        "cocoa_beans": [
            _edge("cocoa_beans", "hsy", input_type="cocoa", pct=0.20),
        ],
    }
    conn = FakeConn(
        edges_by_upstream=edges,
        revenues={"hsy": 10_000_000_000.0},
        labels={"cocoa_beans": "Cocoa Beans", "hsy": "The Hershey Company"},
    )
    engine = FakeEngine(conn)

    result = simulate_contagion(
        engine, "cocoa_beans", "price_increase", 0.30, max_depth=2
    )

    assert result["shock"]["node_id"] == "cocoa_beans"
    assert result["summary"]["total_actors_affected"] == 1
    assert result["summary"]["worst_case_tier1"] == "hsy"
    assert result["summary"]["worst_case_ticker"] == "hsy"

    hsy = result["ranked_impact"][0]
    # margin hit = 0.30 * 0.20 * 0.70 = 0.042 (negative)
    assert hsy["margin_impact_pct"] == pytest.approx(-0.042)
    # revenue at risk = 0.042 * 10B = 420M
    assert hsy["revenue_at_risk_usd"] == pytest.approx(420_000_000)
    assert hsy["path"] == ["cocoa_beans", "hsy"]


def test_two_hop_propagation_and_attenuation():
    edges = {
        "cocoa_beans": [_edge("cocoa_beans", "barry_callebaut", pct=0.50)],
        "barry_callebaut": [_edge("barry_callebaut", "hsy", pct=0.40)],
    }
    conn = FakeConn(edges_by_upstream=edges)
    engine = FakeEngine(conn)

    result = simulate_contagion(engine, "cocoa_beans", "price_increase", 0.30, max_depth=3)

    ids = {a["id"]: a for a in result["ranked_impact"]}
    assert "barry_callebaut" in ids
    assert "hsy" in ids

    bc = ids["barry_callebaut"]
    hsy = ids["hsy"]
    assert bc["tier"] == 1
    assert hsy["tier"] == 2

    # Tier 1: 0.30 * 0.50 * 0.70 = 0.105 absolute
    assert bc["margin_impact_pct"] == pytest.approx(-0.105)
    # Tier 2 incoming shock = 0.105 * HOP_ATTENUATION
    expected_hsy = -(0.105 * HOP_ATTENUATION) * 0.40 * DEFAULT_PASS_THROUGH
    assert hsy["margin_impact_pct"] == pytest.approx(expected_hsy)
    # Magnitude must strictly attenuate over the hop
    assert abs(hsy["margin_impact_pct"]) < abs(bc["margin_impact_pct"])
    # Path must be fully qualified
    assert hsy["path"] == ["cocoa_beans", "barry_callebaut", "hsy"]


def test_cycle_safety_terminates():
    # A -> B -> A deliberately creates a cycle
    edges = {
        "a": [_edge("a", "b", pct=0.20)],
        "b": [_edge("b", "a", pct=0.20)],
    }
    conn = FakeConn(edges_by_upstream=edges)
    engine = FakeEngine(conn)

    result = simulate_contagion(engine, "a", "price_increase", 0.30, max_depth=5)

    # B gets hit once; A is the seed and skipped as self-loop target.
    ids = [a["id"] for a in result["ranked_impact"]]
    assert "b" in ids
    assert "a" not in ids


def test_revenue_scaling_picks_latest_entry():
    edges = {"gpu_x": [_edge("gpu_x", "nvda", pct=0.10)]}
    conn = FakeConn(
        edges_by_upstream=edges,
        revenues={"nvda": 200_000_000_000.0},
    )
    engine = FakeEngine(conn)

    result = simulate_contagion(engine, "gpu_x", "price_increase", 0.50, max_depth=2)
    nvda = result["ranked_impact"][0]
    # margin = 0.50 * 0.10 * 0.70 = 0.035 → 0.035 * 200B = 7B rev risk
    assert nvda["revenue_at_risk_usd"] == pytest.approx(7_000_000_000)
    assert result["summary"]["total_revenue_at_risk_usd"] == pytest.approx(7_000_000_000)


def test_narrative_names_shock_and_victims():
    edges = {
        "cocoa_beans": [_edge("cocoa_beans", "barry_callebaut", pct=0.50)],
        "barry_callebaut": [_edge("barry_callebaut", "hsy", pct=0.40)],
    }
    conn = FakeConn(
        edges_by_upstream=edges,
        labels={
            "cocoa_beans": "Cocoa Beans",
            "barry_callebaut": "Barry Callebaut",
            "hsy": "Hershey",
        },
        revenues={"hsy": 10_000_000_000.0, "barry_callebaut": 8_000_000_000.0},
    )
    engine = FakeEngine(conn)

    result = simulate_contagion(engine, "cocoa_beans", "price_increase", 0.30, max_depth=3)
    narrative = result["narrative"]
    assert "Cocoa Beans" in narrative
    assert "Barry Callebaut" in narrative
    # Percentage formatting present
    assert "%" in narrative
    assert "30%" in narrative
    assert "revenue at risk" in narrative.lower()


def test_disruption_respects_alternatives_via_alt_count():
    # Two parallel scenarios: same edge shape, different alt counts.
    edges_one = {"asml": [_edge("asml", "tsmc", input_type="euv", pct=0.20)]}
    edges_many = {"asml": [_edge("asml", "tsmc", input_type="euv", pct=0.20)]}

    conn_solo = FakeConn(
        edges_by_upstream=edges_one,
        alt_counts={("tsmc", "euv"): 1},  # no alternatives
    )
    conn_multi = FakeConn(
        edges_by_upstream=edges_many,
        alt_counts={("tsmc", "euv"): 5},
    )

    solo = simulate_contagion(
        FakeEngine(conn_solo), "asml", "supply_disruption", 0.30, max_depth=2
    )
    multi = simulate_contagion(
        FakeEngine(conn_multi), "asml", "supply_disruption", 0.30, max_depth=2
    )
    assert abs(solo["ranked_impact"][0]["margin_impact_pct"]) > abs(
        multi["ranked_impact"][0]["margin_impact_pct"]
    )


def test_invalid_shock_type_raises():
    conn = FakeConn(edges_by_upstream={})
    engine = FakeEngine(conn)
    with pytest.raises(ValueError):
        simulate_contagion(engine, "cocoa_beans", "explosion", 0.30, max_depth=2)


def test_empty_shock_node_raises():
    conn = FakeConn(edges_by_upstream={})
    engine = FakeEngine(conn)
    with pytest.raises(ValueError):
        simulate_contagion(engine, "", "price_increase", 0.30, max_depth=2)


def test_valid_shock_types_constant():
    assert "price_increase" in VALID_SHOCK_TYPES
    assert "supply_disruption" in VALID_SHOCK_TYPES


def test_no_edges_returns_empty_ranking():
    conn = FakeConn(edges_by_upstream={})
    engine = FakeEngine(conn)
    result = simulate_contagion(engine, "orphan_node", "price_increase", 0.30)
    assert result["ranked_impact"] == []
    assert result["summary"]["total_actors_affected"] == 0
    assert "No downstream exposure" in result["narrative"]
