"""
GRID API — Ollama integration endpoints.

Provides REST API access to market briefings, LLM reasoning,
and Ollama status:
  GET  /api/v1/ollama/status          — Check Ollama availability
  POST /api/v1/ollama/briefing        — Generate a market briefing
  GET  /api/v1/ollama/briefing/latest — Get latest briefing by type
  GET  /api/v1/ollama/briefings       — List saved briefings
  POST /api/v1/ollama/ask             — Free-form question to reasoner
  POST /api/v1/ollama/explain         — Explain feature relationship
  POST /api/v1/ollama/hypotheses      — Generate hypothesis candidates
  POST /api/v1/ollama/regime-analysis — Analyze regime transition
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from pydantic import BaseModel

from api.auth import require_auth

router = APIRouter(
    prefix="/api/v1/ollama",
    tags=["ollama"],
    dependencies=[Depends(require_auth)],
)

_BRIEFING_DIR = Path(__file__).parent.parent.parent / "outputs" / "market_briefings"


class BriefingRequest(BaseModel):
    briefing_type: str = "hourly"  # hourly | daily | weekly


class AskRequest(BaseModel):
    question: str
    context: str = ""


class ExplainRequest(BaseModel):
    feature_a: str
    feature_b: str
    observed_pattern: str


class HypothesisRequest(BaseModel):
    pattern_description: str
    n_candidates: int = 3


class RegimeAnalysisRequest(BaseModel):
    from_regime: str
    to_regime: str
    feature_changes: dict[str, dict[str, float]] = {}


def _get_client():
    from ollama.client import get_client
    return get_client()


def _get_briefing_engine():
    from ollama.market_briefing import MarketBriefingEngine
    try:
        from db import get_engine
        engine = get_engine()
    except Exception:
        engine = None
    return MarketBriefingEngine(db_engine=engine)


def _get_reasoner():
    from ollama.reasoner import OllamaReasoner
    return OllamaReasoner()


@router.get("/status")
async def ollama_status() -> dict[str, Any]:
    """Check Ollama availability and model info."""
    try:
        client = _get_client()
        return {
            "available": client.is_available,
            "model": client.model,
            "embed_model": client.embed_model,
            "base_url": client.base_url,
        }
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@router.post("/briefing")
async def generate_briefing(req: BriefingRequest) -> dict[str, Any]:
    """Generate a market briefing (hourly/daily/weekly)."""
    if req.briefing_type not in ("hourly", "daily", "weekly"):
        raise HTTPException(status_code=400, detail="Type must be hourly, daily, or weekly")

    engine = _get_briefing_engine()
    result = engine.generate_briefing(briefing_type=req.briefing_type, save=True)
    return {
        "content": result["content"],
        "type": result["type"],
        "timestamp": result["timestamp"],
    }


@router.get("/briefing/latest")
async def get_latest_briefing(
    briefing_type: str = Query(default="hourly"),
) -> dict[str, Any]:
    """Get the most recent saved briefing of the given type."""
    engine = _get_briefing_engine()
    content = engine.get_latest_briefing(briefing_type=briefing_type)
    if content is None:
        return {"content": None, "message": f"No {briefing_type} briefings found"}
    return {"content": content, "type": briefing_type}


@router.get("/briefings")
async def list_briefings(
    briefing_type: str = Query(default=""),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    """List saved briefing files."""
    _BRIEFING_DIR.mkdir(parents=True, exist_ok=True)

    pattern = f"{briefing_type}*.md" if briefing_type else "*.md"
    files = sorted(_BRIEFING_DIR.glob(pattern), reverse=True)[:limit]

    briefings = []
    for f in files:
        name = f.stem
        parts = name.split("_", 1)
        btype = parts[0] if parts else "unknown"
        briefings.append({
            "filename": f.name,
            "type": btype,
            "size_bytes": f.stat().st_size,
            "modified": f.stat().st_mtime,
        })

    return {"briefings": briefings, "total": len(briefings)}


@router.get("/briefings/{filename}")
async def read_briefing(filename: str) -> dict[str, Any]:
    """Read a specific saved briefing file."""
    filepath = _BRIEFING_DIR / filename
    if not filepath.exists() or not filepath.is_file():
        raise HTTPException(status_code=404, detail="Briefing not found")
    content = filepath.read_text(encoding="utf-8")
    return {"filename": filename, "content": content}


@router.post("/ask")
async def ask_ollama(req: AskRequest) -> dict[str, Any]:
    """Ask a free-form question with optional context."""
    client = _get_client()
    if not client.is_available:
        raise HTTPException(status_code=503, detail="Ollama not available")

    messages = []
    if req.context:
        messages.append({"role": "system", "content": req.context})
    messages.append({"role": "user", "content": req.question})

    response = client.chat(
        messages,
        temperature=0.3,
        num_predict=2000,
        system_knowledge=["01_grid_overview", "06_market_analysis_framework"],
    )

    return {"response": response, "question": req.question}


@router.post("/explain")
async def explain_relationship(req: ExplainRequest) -> dict[str, Any]:
    """Explain the economic mechanism behind a feature relationship."""
    reasoner = _get_reasoner()
    result = reasoner.explain_relationship(
        req.feature_a, req.feature_b, req.observed_pattern
    )
    if result is None:
        raise HTTPException(status_code=503, detail="Ollama not available")
    return {"explanation": result}


@router.post("/hypotheses")
async def generate_hypotheses(req: HypothesisRequest) -> dict[str, Any]:
    """Generate falsifiable hypothesis candidates from a pattern."""
    reasoner = _get_reasoner()
    result = reasoner.generate_hypothesis_candidates(
        req.pattern_description, req.n_candidates
    )
    if result is None:
        raise HTTPException(status_code=503, detail="Ollama not available")
    return {"hypotheses": result}


@router.post("/regime-analysis")
async def analyze_regime(req: RegimeAnalysisRequest) -> dict[str, Any]:
    """Analyze a regime transition with economic context."""
    reasoner = _get_reasoner()
    result = reasoner.analyze_regime_transition(
        req.from_regime, req.to_regime, req.feature_changes
    )
    if result is None:
        raise HTTPException(status_code=503, detail="Ollama not available")
    return {"analysis": result}
