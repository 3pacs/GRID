"""Tests for ``api.routers.conviction`` — the conviction API surface.

All 5 downstream confidence-stack modules are monkeypatched so no live
database, oracle, or LLM call is made. Each test builds a minimal
FastAPI app with just the conviction router mounted and overrides the
``get_db_engine`` + ``require_auth`` dependencies via
``app.dependency_overrides`` so tests never touch a real engine or
auth backend.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.routers import conviction as conviction_module
from api.routers.conviction import _to_serializable, router as conviction_router


# ── Test doubles (plain frozen dataclasses with to_dict) ─────────────────


@dataclass(frozen=True)
class FakeProvenance:
    ticker: str = "NVDA"
    direction: str = "bullish"
    verdict: str = "high"
    aggregate_conviction: float = 0.87

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "direction": self.direction,
            "verdict": self.verdict,
            "aggregate_conviction": self.aggregate_conviction,
        }


@dataclass(frozen=True)
class FakeStress:
    robustness_label: str = "robust"
    robustness_score: float = 0.82

    def to_dict(self) -> dict[str, Any]:
        return {
            "robustness_label": self.robustness_label,
            "robustness_score": self.robustness_score,
        }


@dataclass(frozen=True)
class FakeDecisionResponse:
    ticker: str = "NVDA"
    generated_at: str = "2026-04-13T00:00:00+00:00"
    horizon_days: int = 7
    prediction: Any = None
    red_team_report: Any = None
    provenance_report: Any = None
    pattern_report: Any = None
    stress_report: Any = None
    trade_ticket: Any = None
    unified_verdict: str = "high"
    verdict_reasons: list[str] = field(default_factory=lambda: ["all aligned"])
    stage_errors: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        def _ser(x: Any) -> Any:
            if x is None:
                return None
            return x.to_dict() if hasattr(x, "to_dict") else x

        return {
            "ticker": self.ticker,
            "generated_at": self.generated_at,
            "horizon_days": self.horizon_days,
            "provenance_report": _ser(self.provenance_report),
            "stress_report": _ser(self.stress_report),
            "unified_verdict": self.unified_verdict,
            "verdict_reasons": list(self.verdict_reasons),
            "stage_errors": dict(self.stage_errors),
        }


@dataclass(frozen=True)
class FakeNarrativeReport:
    ticker: str = "NVDA"
    headline: str = "LONG NVDA — HIGH conviction"
    thesis: str = "NVDA bullish thesis body..."
    source: str = "template"
    word_count: int = 42
    generated_at: str = "2026-04-13T00:00:00+00:00"

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "headline": self.headline,
            "thesis": self.thesis,
            "source": self.source,
            "word_count": self.word_count,
            "generated_at": self.generated_at,
        }


@dataclass(frozen=True)
class FakeUniverseRankingReport:
    universe_name: str = "SP500"
    tickers_attempted: int = 100
    tickers_succeeded: int = 95
    top_k: list[dict[str, Any]] = field(default_factory=list)
    all_rankings: list[dict[str, Any]] = field(default_factory=list)
    sector_distributions: list[dict[str, Any]] = field(default_factory=list)
    concentration_alerts: list[str] = field(default_factory=list)
    regime_signature: str = "mixed"
    narrative: str = "Universe SP500: 95/100 scored..."
    generated_at: str = "2026-04-13T00:00:00+00:00"

    def to_dict(self) -> dict[str, Any]:
        return {
            "universe_name": self.universe_name,
            "tickers_attempted": self.tickers_attempted,
            "tickers_succeeded": self.tickers_succeeded,
            "top_k": list(self.top_k),
            "all_rankings": list(self.all_rankings),
            "sector_distributions": list(self.sector_distributions),
            "concentration_alerts": list(self.concentration_alerts),
            "regime_signature": self.regime_signature,
            "narrative": self.narrative,
            "generated_at": self.generated_at,
        }


@dataclass(frozen=True)
class FakePairLeg:
    ticker: str
    direction: str
    kelly_size_pct: float = 0.04
    kelly_size_dollars: float = 4000.0
    entry_price: float = 100.0
    stop_price: float = 95.0
    target_price: float = 110.0
    conviction: float = 0.8
    robustness_label: str = "robust"
    robustness_score: float = 0.85
    signal_summary: str = "leg summary"
    sector: str = "tech"

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "direction": self.direction,
            "kelly_size_pct": self.kelly_size_pct,
            "kelly_size_dollars": self.kelly_size_dollars,
            "entry_price": self.entry_price,
            "stop_price": self.stop_price,
            "target_price": self.target_price,
            "conviction": self.conviction,
            "robustness_label": self.robustness_label,
            "robustness_score": self.robustness_score,
            "signal_summary": self.signal_summary,
            "sector": self.sector,
        }


@dataclass(frozen=True)
class FakePairTradeTicket:
    pair_name: str = "LONG TSM / SHORT NVDA"
    long_leg: Any = None
    short_leg: Any = None
    pair_conviction_score: float = 0.76
    spread_sharpness: float = 0.55
    net_exposure_usd: float = 200.0
    gross_exposure_usd: float = 8200.0
    thesis: str = "SPREAD: LONG TSM / SHORT NVDA ..."
    invalidation: str = "exit rule"
    causation_chain: str = "chain"
    generated_at: str = "2026-04-13T00:00:00+00:00"
    verdict: str = "medium"

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_name": self.pair_name,
            "long_leg": self.long_leg.to_dict() if self.long_leg else None,
            "short_leg": self.short_leg.to_dict() if self.short_leg else None,
            "pair_conviction_score": self.pair_conviction_score,
            "spread_sharpness": self.spread_sharpness,
            "net_exposure_usd": self.net_exposure_usd,
            "gross_exposure_usd": self.gross_exposure_usd,
            "thesis": self.thesis,
            "invalidation": self.invalidation,
            "causation_chain": self.causation_chain,
            "generated_at": self.generated_at,
            "verdict": self.verdict,
        }


@dataclass(frozen=True)
class FakeSignalHealthReport:
    generated_at: str = "2026-04-13T00:00:00+00:00"
    total_series: int = 16
    by_status: dict[str, int] = field(
        default_factory=lambda: {"green": 12, "yellow": 3, "orange": 1, "red": 0}
    )
    by_namespace: dict[str, dict[str, int]] = field(default_factory=dict)
    unhealthy: list[dict[str, Any]] = field(default_factory=list)
    summary: str = "12 green, 3 yellow, 1 orange, 0 red across 16 series"

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "total_series": self.total_series,
            "by_status": dict(self.by_status),
            "by_namespace": {k: dict(v) for k, v in self.by_namespace.items()},
            "unhealthy": list(self.unhealthy),
            "summary": self.summary,
        }


# ── Fixtures ──────────────────────────────────────────────────────────────


def _build_app() -> FastAPI:
    """Build a minimal FastAPI app with just the conviction router mounted.

    ``get_db_engine`` is overridden to return a MagicMock so no real engine
    is ever touched. ``require_auth`` is overridden to a no-op returning a
    dummy token string.
    """
    app = FastAPI()
    app.include_router(conviction_router)

    app.dependency_overrides[get_db_engine] = lambda: MagicMock(name="FakeEngine")
    app.dependency_overrides[require_auth] = lambda: "test-token"
    return app


@pytest.fixture
def client() -> TestClient:
    return TestClient(_build_app())


@pytest.fixture
def no_auth_client() -> TestClient:
    """Client where ``require_auth`` raises 401 so we can test unauth path."""
    app = FastAPI()
    app.include_router(conviction_router)
    app.dependency_overrides[get_db_engine] = lambda: MagicMock(name="FakeEngine")

    def _unauth() -> str:
        raise HTTPException(status_code=401, detail="not authenticated")

    app.dependency_overrides[require_auth] = _unauth
    return TestClient(app)


# ── Downstream monkeypatch helpers ───────────────────────────────────────


def _ok_decision_response() -> FakeDecisionResponse:
    return FakeDecisionResponse(
        provenance_report=FakeProvenance(),
        stress_report=FakeStress(),
    )


def _ok_narrative() -> FakeNarrativeReport:
    return FakeNarrativeReport()


def _ok_universe() -> FakeUniverseRankingReport:
    return FakeUniverseRankingReport(
        top_k=[{"ticker": "NVDA", "composite_score": 1.1}],
    )


def _ok_pair_ticket() -> FakePairTradeTicket:
    return FakePairTradeTicket(
        long_leg=FakePairLeg(ticker="TSM", direction="long"),
        short_leg=FakePairLeg(ticker="NVDA", direction="short"),
    )


def _ok_health() -> FakeSignalHealthReport:
    return FakeSignalHealthReport()


# ── Tests — /ticker/{ticker} ──────────────────────────────────────────────


def test_ticker_happy_path(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    """/ticker/NVDA returns 200 with decision + narrative fields."""
    monkeypatch.setattr(
        conviction_module, "should_i_trade", lambda *a, **k: _ok_decision_response()
    )
    monkeypatch.setattr(
        conviction_module, "narrate_trade", lambda *a, **k: _ok_narrative()
    )

    resp = client.get("/api/v1/conviction/ticker/NVDA")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ticker"] == "NVDA"
    assert body["unified_verdict"] == "high"
    assert "narrative" in body
    assert body["narrative"]["headline"].startswith("LONG NVDA")
    assert body["narrative"]["source"] == "template"


def test_ticker_uppercased(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    """Lowercase 'nvda' is uppercased to 'NVDA' before stack call."""
    seen: dict[str, Any] = {}

    def fake_should_i_trade(engine: Any, ticker: str, **kwargs: Any) -> FakeDecisionResponse:
        seen["ticker"] = ticker
        return FakeDecisionResponse(ticker=ticker, provenance_report=FakeProvenance(ticker=ticker), stress_report=FakeStress())

    monkeypatch.setattr(conviction_module, "should_i_trade", fake_should_i_trade)
    monkeypatch.setattr(conviction_module, "narrate_trade", lambda *a, **k: _ok_narrative())

    resp = client.get("/api/v1/conviction/ticker/nvda")
    assert resp.status_code == 200
    assert seen["ticker"] == "NVDA"
    assert resp.json()["ticker"] == "NVDA"


def test_ticker_downstream_error(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """should_i_trade raising surfaces as a structured 500."""

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("db down")

    monkeypatch.setattr(conviction_module, "should_i_trade", boom)
    monkeypatch.setattr(conviction_module, "narrate_trade", lambda *a, **k: _ok_narrative())

    resp = client.get("/api/v1/conviction/ticker/NVDA")
    assert resp.status_code == 500
    detail = resp.json()["detail"]
    assert detail["stage"] == "should_i_trade"
    assert "db down" in detail["error"]
    assert detail["error_type"] == "RuntimeError"


def test_ticker_default_account_size(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """/ticker/NVDA with no query params uses account_size_usd=100_000 default."""
    seen: dict[str, Any] = {}

    def fake_should_i_trade(engine: Any, ticker: str, **kwargs: Any) -> FakeDecisionResponse:
        seen.update(kwargs)
        return _ok_decision_response()

    monkeypatch.setattr(conviction_module, "should_i_trade", fake_should_i_trade)
    monkeypatch.setattr(conviction_module, "narrate_trade", lambda *a, **k: _ok_narrative())

    resp = client.get("/api/v1/conviction/ticker/NVDA")
    assert resp.status_code == 200
    assert seen["account_size_usd"] == 100_000.0
    assert seen["horizon_days"] == 7
    assert seen["instrument"] == "equity"


# ── Tests — /top ─────────────────────────────────────────────────────────


def test_top_happy_path(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    """/top?universe=SP500&k=10 returns 200 with universe ranking report."""
    monkeypatch.setattr(
        conviction_module, "rank_universe", lambda *a, **k: _ok_universe()
    )

    resp = client.get("/api/v1/conviction/top?universe=SP500&k=10")
    assert resp.status_code == 200
    body = resp.json()
    assert body["universe_name"] == "SP500"
    assert body["tickers_attempted"] == 100
    assert body["regime_signature"] == "mixed"
    assert isinstance(body["top_k"], list)


def test_top_invalid_universe(client: TestClient) -> None:
    """Unknown universe label → 422 with structured detail."""
    resp = client.get("/api/v1/conviction/top?universe=INVALID&k=10")
    assert resp.status_code == 422


def test_top_parallel_false_passed_through(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """parallel=false is forwarded as a bool to rank_universe."""
    seen: dict[str, Any] = {}

    def fake_rank(engine: Any, universe: Any, **kwargs: Any) -> FakeUniverseRankingReport:
        seen["universe"] = universe
        seen.update(kwargs)
        return FakeUniverseRankingReport(universe_name=universe)

    monkeypatch.setattr(conviction_module, "rank_universe", fake_rank)

    resp = client.get("/api/v1/conviction/top?universe=NASDAQ100&k=5&parallel=false")
    assert resp.status_code == 200
    assert seen["universe"] == "NASDAQ100"
    assert seen["parallel"] is False
    assert seen["top_k"] == 5
    assert resp.json()["universe_name"] == "NASDAQ100"


def test_top_large_k_ok(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    """k=100 is within bounds and flows through."""
    monkeypatch.setattr(
        conviction_module, "rank_universe", lambda *a, **k: _ok_universe()
    )
    resp = client.get("/api/v1/conviction/top?universe=SP500&k=100")
    assert resp.status_code == 200


def test_top_negative_k_validation_error(client: TestClient) -> None:
    """k=-1 is below the ge=1 bound → 422."""
    resp = client.get("/api/v1/conviction/top?universe=SP500&k=-1")
    assert resp.status_code == 422


# ── Tests — /pair/{long}/{short} ─────────────────────────────────────────


def test_pair_happy_path(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    """/pair/TSM/NVDA returns 200 with a serialized pair ticket."""
    monkeypatch.setattr(
        conviction_module, "generate_pair_ticket", lambda *a, **k: _ok_pair_ticket()
    )

    resp = client.get("/api/v1/conviction/pair/TSM/NVDA")
    assert resp.status_code == 200
    body = resp.json()
    assert body["pair_name"] == "LONG TSM / SHORT NVDA"
    assert body["verdict"] == "medium"
    assert body["long_leg"]["ticker"] == "TSM"
    assert body["short_leg"]["ticker"] == "NVDA"


def test_pair_rejection(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    """generate_pair_ticket returning None → 200 with structured reason."""
    monkeypatch.setattr(
        conviction_module, "generate_pair_ticket", lambda *a, **k: None
    )

    resp = client.get("/api/v1/conviction/pair/TSM/NVDA")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ticket"] is None
    assert "rejected" in body["reason"].lower()
    assert body["long_ticker"] == "TSM"
    assert body["short_ticker"] == "NVDA"


# ── Tests — /pair/candidates ─────────────────────────────────────────────


def test_pair_candidates_happy_path(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """/pair/candidates returns a list of serialized tickets."""
    monkeypatch.setattr(
        conviction_module,
        "scan_candidate_pairs",
        lambda *a, **k: [_ok_pair_ticket(), _ok_pair_ticket()],
    )

    resp = client.get("/api/v1/conviction/pair/candidates")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 2
    assert len(body["tickets"]) == 2
    assert body["tickets"][0]["pair_name"] == "LONG TSM / SHORT NVDA"
    assert body["candidates_scanned"] >= 1


# ── Tests — /health ──────────────────────────────────────────────────────


def test_health_happy_path(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    """/health returns 200 with signal-health report."""
    monkeypatch.setattr(
        conviction_module, "audit_all_series", lambda *a, **k: _ok_health()
    )

    resp = client.get("/api/v1/conviction/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_series"] == 16
    assert body["by_status"]["green"] == 12
    assert "summary" in body


# ── Tests — /narrative/{ticker} ──────────────────────────────────────────


def test_narrative_happy_path(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """/narrative/NVDA returns ONLY the narrative payload."""
    monkeypatch.setattr(
        conviction_module, "should_i_trade", lambda *a, **k: _ok_decision_response()
    )
    monkeypatch.setattr(
        conviction_module, "narrate_trade", lambda *a, **k: _ok_narrative()
    )

    resp = client.get("/api/v1/conviction/narrative/NVDA")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ticker"] == "NVDA"
    assert "headline" in body
    assert "thesis" in body
    # The decision-level fields should NOT be present on the narrative endpoint.
    assert "unified_verdict" not in body
    assert "stage_errors" not in body


def test_narrative_should_i_trade_fails(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    """/narrative/NVDA surfaces should_i_trade errors as structured 500."""

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("bad ticker")

    monkeypatch.setattr(conviction_module, "should_i_trade", boom)

    resp = client.get("/api/v1/conviction/narrative/NVDA")
    assert resp.status_code == 500
    detail = resp.json()["detail"]
    assert detail["stage"] == "should_i_trade"
    assert detail["error_type"] == "ValueError"


# ── Tests — auth ─────────────────────────────────────────────────────────


def test_missing_auth_returns_401(
    monkeypatch: pytest.MonkeyPatch, no_auth_client: TestClient
) -> None:
    """When require_auth raises 401 the endpoint surfaces 401 (not 500)."""
    monkeypatch.setattr(
        conviction_module, "audit_all_series", lambda *a, **k: _ok_health()
    )
    resp = no_auth_client.get("/api/v1/conviction/health")
    assert resp.status_code == 401


# ── Tests — _to_serializable helper ───────────────────────────────────────


def test_to_serializable_set_and_datetime() -> None:
    """_to_serializable converts sets to lists and datetimes to ISO strings."""
    now = datetime(2026, 4, 13, 12, 0, 0, tzinfo=timezone.utc)
    payload = {
        "tags": {"alpha", "beta"},
        "timestamp": now,
        "nested": {"inner_set": frozenset({1, 2, 3})},
        "list_of_objs": [now, {"x": 1}],
        "number": 3.14,
        "flag": True,
        "none": None,
    }

    out = _to_serializable(payload)

    assert isinstance(out["tags"], list)
    assert sorted(out["tags"]) == ["alpha", "beta"]
    assert out["timestamp"] == now.isoformat()
    assert isinstance(out["nested"]["inner_set"], list)
    assert sorted(out["nested"]["inner_set"]) == [1, 2, 3]
    assert out["list_of_objs"][0] == now.isoformat()
    assert out["list_of_objs"][1] == {"x": 1}
    assert out["number"] == 3.14
    assert out["flag"] is True
    assert out["none"] is None
