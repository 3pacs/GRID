"""Tests for chain-contagion v2 upgrades.

Covers the four v2 sub-features:

1. Empirical pass-through priors — ``_empirical_pass_through`` resolves
   ``supply_shock_attributions`` correlations (clamped to [0.1, 1.0]), and
   ``simulate_contagion`` uses them per-edge, tracking usage stats in
   ``provenance.pass_through_empirical`` / ``pass_through_default``. Falls
   back to the flat default when attributions are absent.
2. Substitution / mitigation suggestions — every ranked victim carries a
   ``mitigation`` list populated from ``supply_chokepoints.find_alternatives``
   (deduped, top-3 by chokepoint_score ascending).
3. Preset scenario catalog — ``SCENARIO_CATALOG`` module-level constant
   exposed via ``GET /api/v1/contagion/scenarios``. Immutable shape verified.
4. Sector contagion matrix — ``GET /api/v1/sectors/{name}/contagion-matrix``
   assembles a ticker × scenario grid and caches the result for 1h.

All DB access is faked with a small in-memory connection harness so the
tests run without a live PostgreSQL.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any
from unittest.mock import patch

import pytest

from api.routers.contagion import (
    SCENARIO_CATALOG,
    _matrix_cache,
    _scenario_sim_cache,
    _severity_bucket,
    get_contagion_matrix,
    get_scenarios,
)
from intelligence.chain_contagion import (
    EMPIRICAL_PASS_THROUGH_MAX,
    EMPIRICAL_PASS_THROUGH_MIN,
    _empirical_pass_through,
    simulate_contagion,
)


# ── Fake DB plumbing ─────────────────────────────────────────────────────────


@dataclass
class _Row:
    values: tuple

    def __getitem__(self, idx):
        return self.values[idx]

    def __iter__(self):
        return iter(self.values)


class _Result:
    def __init__(self, rows: list[_Row]) -> None:
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._rows[0][0] if self._rows else None


class FakeConnV2:
    """FakeConn that speaks the additional v2 SQL dialects.

    Extends the v1 test harness with:
      - ``supply_shock_attributions`` table presence + lagged_correlation rows
      - ``find_alternatives`` JOIN SELECT (via ``supply_chokepoints``)
      - chokepoint_score aggregation MIN(...) query used by ``_mitigation_for``
    """

    def __init__(
        self,
        edges_by_upstream: dict[str, list[tuple]],
        *,
        revenues: dict[str, float] | None = None,
        labels: dict[str, str] | None = None,
        alt_counts: dict[tuple[str, str | None], int] | None = None,
        attributions: dict[tuple[str, str], float] | None = None,
        # alternatives map: (downstream_id, input_type) -> list of
        # {upstream_id, annual_usd, country, chokepoint_score}
        alternatives: dict[
            tuple[str, str | None], list[dict[str, Any]]
        ]
        | None = None,
        present_tables: set[str] | None = None,
    ) -> None:
        self._edges = edges_by_upstream
        self._revenues = revenues or {}
        self._labels = labels or {}
        self._alt_counts = alt_counts or {}
        self._attributions = attributions or {}
        self._alternatives = alternatives or {}
        self._present = present_tables or {
            "supply_chain_edges",
            "supply_chain_nodes",
            "capital_flows",
            "supply_shock_attributions",
        }

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def connect(self):
        return self

    def execute(self, stmt, params=None):
        sql = str(stmt).strip().lower()
        merged: dict[str, Any] = {}
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

        if "to_regclass" in sql:
            name = params.get("n", "")
            return _Result([_Row((name if name in self._present else None,))])

        # Empirical pass-through lookup
        if "from supply_shock_attributions" in sql:
            up = params.get("up")
            dn = params.get("dn")
            corr = self._attributions.get((up, dn))
            if corr is None:
                return _Result([])
            return _Result([_Row((corr,))])

        # find_alternatives JOIN SELECT (supply_chokepoints._FIND_ALTERNATIVES_SQL)
        if (
            "from supply_chain_edges e" in sql
            and "left join supply_chain_nodes n" in sql
        ):
            dn = params.get("downstream_id")
            it = params.get("input_type")
            rows = self._alternatives.get((dn, it), [])
            return _Result(
                [
                    _Row(
                        (
                            r["upstream_id"],
                            r.get("annual_usd"),
                            r.get("country"),
                        )
                    )
                    for r in rows
                ]
            )

        # chokepoint_score MIN aggregation used by _mitigation_for
        if (
            "min(chokepoint_score)" in sql
            and "from supply_chain_edges" in sql
        ):
            dn = params.get("d")
            ids = set(params.get("ids", []) or [])
            it = params.get("it")
            rows = self._alternatives.get((dn, it), [])
            out_rows: list[_Row] = []
            for r in rows:
                uid = r["upstream_id"].lower()
                if uid not in ids:
                    continue
                out_rows.append(_Row((uid, r.get("chokepoint_score"))))
            return _Result(out_rows)

        # Downstream edge fetch
        if "from supply_chain_edges where upstream_id" in sql:
            node = params.get("n")
            return _Result([_Row(e) for e in self._edges.get(node, [])])

        # alt-count query
        if "count(distinct upstream_id)" in sql:
            c = self._alt_counts.get(
                (params.get("d"), params.get("it")), 1
            )
            return _Result([_Row((c,))])

        # Label lookup
        if "from supply_chain_nodes where id" in sql:
            ids = params.get("ids", []) or []
            return _Result(
                [
                    _Row((i, self._labels.get(i, i)))
                    for i in ids
                    if i in self._labels
                ]
            )

        # Revenue lookup
        if "from capital_flows" in sql:
            ids = params.get("ids", []) or []
            return _Result(
                [
                    _Row((i, self._revenues[i]))
                    for i in ids
                    if i in self._revenues
                ]
            )

        return _Result([])


class FakeEngineV2:
    def __init__(self, conn: FakeConnV2) -> None:
        self._conn = conn

    def connect(self):
        return self._conn


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


# ─── Sub-feature 1: empirical pass-through ──────────────────────────────────


def test_empirical_pass_through_uses_attribution_correlation():
    """Observed correlation overrides the flat default for known pairs."""
    conn = FakeConnV2(
        edges_by_upstream={
            "cocoa_beans": [
                _edge("cocoa_beans", "hsy", input_type="cocoa", pct=0.20)
            ],
        },
        attributions={("cocoa_beans", "hsy"): -0.82},
    )
    value = _empirical_pass_through(conn, "cocoa_beans", "hsy")
    # abs(-0.82) within [0.1, 1.0] → 0.82
    assert value == pytest.approx(0.82)
    assert EMPIRICAL_PASS_THROUGH_MIN <= value <= EMPIRICAL_PASS_THROUGH_MAX


def test_empirical_pass_through_clamps_low_correlation():
    conn = FakeConnV2(
        edges_by_upstream={},
        attributions={("up_x", "dn_y"): 0.02},
    )
    # below floor → clamped to 0.1
    assert _empirical_pass_through(conn, "up_x", "dn_y") == pytest.approx(
        EMPIRICAL_PASS_THROUGH_MIN
    )


def test_empirical_pass_through_returns_none_on_missing_pair():
    conn = FakeConnV2(edges_by_upstream={}, attributions={})
    assert _empirical_pass_through(conn, "foo", "bar") is None


def test_empirical_pass_through_returns_none_when_table_absent():
    conn = FakeConnV2(
        edges_by_upstream={},
        attributions={("cocoa_beans", "hsy"): -0.82},
        present_tables={"supply_chain_edges", "supply_chain_nodes"},
    )
    assert _empirical_pass_through(conn, "cocoa_beans", "hsy") is None


def test_simulate_uses_empirical_prior_and_reports_stats():
    """End-to-end: edge with an attribution pulls its margin impact toward
    the empirical prior, not the default 0.7."""
    edges = {
        "cocoa_beans": [
            _edge("cocoa_beans", "hsy", input_type="cocoa", pct=0.20)
        ],
    }
    conn = FakeConnV2(
        edges_by_upstream=edges,
        attributions={("cocoa_beans", "hsy"): 0.40},
    )
    engine = FakeEngineV2(conn)
    result = simulate_contagion(
        engine, "cocoa_beans", "price_increase", 0.30, max_depth=2
    )
    hsy = result["ranked_impact"][0]
    # Empirical pass-through = 0.40 (vs default 0.70)
    # margin = 0.30 * 0.20 * 0.40 = 0.024 → -0.024
    assert hsy["margin_impact_pct"] == pytest.approx(-0.024)
    assert result["provenance"]["pass_through_empirical"] == 1
    assert result["provenance"]["pass_through_default"] == 0
    assert result["provenance"]["algorithm_version"] == "2.0"


def test_simulate_falls_back_to_default_when_attribution_missing():
    """No attribution rows → uses DEFAULT_PASS_THROUGH (0.70) verbatim."""
    edges = {
        "cocoa_beans": [_edge("cocoa_beans", "hsy", pct=0.20)],
    }
    conn = FakeConnV2(edges_by_upstream=edges, attributions={})
    engine = FakeEngineV2(conn)
    result = simulate_contagion(
        engine, "cocoa_beans", "price_increase", 0.30, max_depth=2
    )
    hsy = result["ranked_impact"][0]
    # default 0.70 → 0.30 * 0.20 * 0.70 = 0.042
    assert hsy["margin_impact_pct"] == pytest.approx(-0.042)
    assert result["provenance"]["pass_through_default"] == 1
    assert result["provenance"]["pass_through_empirical"] == 0


# ─── Sub-feature 2: mitigation suggestions ──────────────────────────────────


def test_mitigation_field_populated_with_top3_alternatives():
    """Ranked impact entries should carry a deduped top-3 mitigation list,
    ordered lowest chokepoint_score first (most substitutable)."""
    edges = {
        "cocoa_beans": [
            _edge("cocoa_beans", "hsy", input_type="cocoa", pct=0.20)
        ],
    }
    alternatives = {
        ("hsy", "cocoa"): [
            # focal — must be filtered out
            {"upstream_id": "cocoa_beans", "chokepoint_score": 0.92},
            # three alt suppliers, unsorted
            {"upstream_id": "ghana_co_op", "chokepoint_score": 0.55},
            {"upstream_id": "ecuador_cocoa", "chokepoint_score": 0.30},
            {"upstream_id": "brazil_cocoa", "chokepoint_score": 0.45},
            # duplicate should be deduped
            {"upstream_id": "ecuador_cocoa", "chokepoint_score": 0.30},
            # fourth alt should be dropped (top-3 cap)
            {"upstream_id": "peru_cocoa", "chokepoint_score": 0.75},
        ]
    }
    conn = FakeConnV2(
        edges_by_upstream=edges,
        alternatives=alternatives,
        labels={
            "ecuador_cocoa": "Ecuador Cocoa Co",
            "brazil_cocoa": "Brazil Cocoa SA",
            "ghana_co_op": "Ghana Cocoa Board",
        },
    )
    engine = FakeEngineV2(conn)
    result = simulate_contagion(
        engine, "cocoa_beans", "price_increase", 0.30, max_depth=2
    )
    hsy = result["ranked_impact"][0]
    assert "mitigation" in hsy
    mit = hsy["mitigation"]
    assert len(mit) == 3
    ids = [m["id"] for m in mit]
    assert "cocoa_beans" not in ids  # focal is filtered
    assert "peru_cocoa" not in ids  # top-3 capped
    # Ordered by ascending chokepoint_score
    assert ids[0] == "ecuador_cocoa"  # 0.30
    assert ids[1] == "brazil_cocoa"  # 0.45
    assert ids[2] == "ghana_co_op"  # 0.55
    # Name resolved via supply_chain_nodes join
    assert mit[0]["name"] == "Ecuador Cocoa Co"
    assert result["provenance"]["mitigations_resolved"] == 1


def test_mitigation_empty_when_no_alternatives():
    """No alt rows → empty mitigation list, simulator does not raise."""
    edges = {"x": [_edge("x", "y", pct=0.10)]}
    conn = FakeConnV2(edges_by_upstream=edges, alternatives={})
    engine = FakeEngineV2(conn)
    result = simulate_contagion(
        engine, "x", "price_increase", 0.20, max_depth=2
    )
    y = result["ranked_impact"][0]
    assert y["mitigation"] == []


# ─── Sub-feature 3: scenario catalog ────────────────────────────────────────


def test_scenario_catalog_shape_and_ids():
    expected_ids = {
        "cocoa_crisis",
        "taiwan_crisis",
        "fed_hike_100bp",
        "opec_cut",
        "usd_up_10",
        "glencore_halt",
        "euv_down",
        "neon_shortage",
    }
    assert {s["id"] for s in SCENARIO_CATALOG} == expected_ids
    for scenario in SCENARIO_CATALOG:
        assert set(scenario).issuperset(
            {"id", "label", "description", "shock", "expected_victims_preview"}
        )
        shock = scenario["shock"]
        assert set(shock) == {"shock_node", "shock_type", "magnitude"}
        assert shock["shock_type"] in {"price_increase", "supply_disruption"}
        assert isinstance(scenario["expected_victims_preview"], list)


def test_get_scenarios_endpoint_returns_catalog_copy():
    scns = asyncio.run(get_scenarios(_token="test"))
    assert len(scns) == len(SCENARIO_CATALOG)
    assert {s["id"] for s in scns} == {s["id"] for s in SCENARIO_CATALOG}
    # Mutating the response must not mutate the module constant
    scns[0]["label"] = "mutated"
    assert SCENARIO_CATALOG[0]["label"] != "mutated"


# ─── Sub-feature 4: sector contagion matrix ─────────────────────────────────


def _fake_engine_for_scenarios() -> FakeEngineV2:
    """Build a FakeEngineV2 that produces a visible hit for HSY on the
    cocoa_crisis scenario, so the matrix has at least one nonzero cell.
    """
    edges = {
        "cocoa_beans": [
            _edge("cocoa_beans", "hsy", input_type="cocoa", pct=0.20)
        ],
    }
    return FakeEngineV2(
        FakeConnV2(
            edges_by_upstream=edges,
            revenues={"hsy": 10_000_000_000.0},
            labels={"hsy": "Hershey", "cocoa_beans": "Cocoa"},
        )
    )


def test_contagion_matrix_shape_and_cache(monkeypatch):
    """The matrix endpoint returns a ticker × scenario grid, caches the
    assembled payload, and reuses a cached per-scenario simulation so we
    don't run N cold sims per matrix.
    """
    _matrix_cache.clear()
    _scenario_sim_cache.clear()

    from api.routers import contagion as contagion_router

    fake_engine = _fake_engine_for_scenarios()
    monkeypatch.setattr(
        contagion_router, "get_db_engine", lambda: fake_engine
    )
    # Trim the sector to a small deterministic list of tickers.
    monkeypatch.setattr(
        contagion_router,
        "_sector_tickers",
        lambda name: ["hsy", "mdlz", "ko"] if name == "Consumer Staples" else [],
    )

    result = asyncio.run(
        get_contagion_matrix("Consumer Staples", _token="test")
    )

    assert result["sector"] == "Consumer Staples"
    assert result["tickers"] == ["hsy", "mdlz", "ko"]
    assert result["scenarios"] == [s["id"] for s in SCENARIO_CATALOG]
    # ticker count × scenario count = total cells
    assert len(result["cells"]) == 3 * len(SCENARIO_CATALOG)
    assert all(
        set(c.keys()) == {"ticker", "scenario", "margin_impact_pct", "severity"}
        for c in result["cells"]
    )

    # HSY/cocoa_crisis should be the one non-zero hit we wired up
    hsy_cocoa = next(
        c
        for c in result["cells"]
        if c["ticker"] == "hsy" and c["scenario"] == "cocoa_crisis"
    )
    assert hsy_cocoa["margin_impact_pct"] < 0.0
    assert hsy_cocoa["severity"] in {"low", "medium", "high"}


def test_contagion_matrix_cache_hit_path(monkeypatch):
    """Second call for the same sector returns the cached payload without
    re-running simulations (the scenario sim cache is the single source of
    truth for underlying results)."""
    _matrix_cache.clear()
    _scenario_sim_cache.clear()

    from api.routers import contagion as contagion_router

    calls = {"n": 0}

    def _counting_engine_factory() -> FakeEngineV2:
        return _fake_engine_for_scenarios()

    def _tracked_engine():
        calls["n"] += 1
        return _counting_engine_factory()

    monkeypatch.setattr(contagion_router, "get_db_engine", _tracked_engine)
    monkeypatch.setattr(
        contagion_router,
        "_sector_tickers",
        lambda name: ["hsy"] if name == "Consumer Staples" else [],
    )

    first = asyncio.run(
        get_contagion_matrix("Consumer Staples", _token="test")
    )
    engine_calls_after_first = calls["n"]
    second = asyncio.run(
        get_contagion_matrix("Consumer Staples", _token="test")
    )
    # Matrix cache short-circuits — no further engine lookups required.
    assert calls["n"] == engine_calls_after_first
    assert first["tickers"] == second["tickers"]
    assert len(first["cells"]) == len(second["cells"])
    # Cached payload is flagged so the client can tell them apart.
    assert second.get("cached") is True


def test_contagion_matrix_unknown_sector_returns_empty(monkeypatch):
    _matrix_cache.clear()
    _scenario_sim_cache.clear()

    from api.routers import contagion as contagion_router

    monkeypatch.setattr(
        contagion_router,
        "get_db_engine",
        lambda: _fake_engine_for_scenarios(),
    )
    monkeypatch.setattr(contagion_router, "_sector_tickers", lambda name: [])

    result = asyncio.run(get_contagion_matrix("Nonexistent", _token="test"))
    assert result["tickers"] == []
    assert result["cells"] == []
    assert "error" in result


def test_severity_bucket_thresholds():
    assert _severity_bucket(-0.05) == "high"
    assert _severity_bucket(-0.02) == "medium"
    assert _severity_bucket(-0.005) == "low"
    assert _severity_bucket(0.0) == "none"
