"""LLM second-opinion review of active hypotheses.

The optional **cloud-analyst layer** on top of the deterministic
``intelligence.hypothesis_engine.score_due_active_hypotheses`` batch (which runs
every 30 min from hermes_operator and is free/Bayesian). This module reviews the
top-N highest-conviction *active* hypotheses with ``HermesAgent.score_hypothesis``
(the GPT-5.5 / reasoning analyst) and records the verdicts as an **advisory**
artifact. It never touches the deterministic score or the ``discovered_hypotheses``
table — it's a parallel second opinion, not a replacement.

DORMANT BY DEFAULT. Runs only when ``HERMES_HYPO_LLM_ENABLED=true``.

COST: one analyst call per hypothesis (~9s on the Codex subscription, or real $
on the API-key lane). Bounded by ``HERMES_HYPO_LLM_LIMIT`` and, on the openai
backend, by ``HERMES_DAILY_SPEND_CAP_USD``. Run it on the **API-key lane** for
unattended/scheduled use — see ``intelligence/hermes/BACKEND_ROUTING.md``.

Wiring into the 6:30 Hermes cycle (the daemon owner adds this; this module stays
standalone so it doesn't collide with active hermes_operator work):

    # 7x. LLM hypothesis review — daily ~06:30 UTC
    try:
        now_utc = datetime.now(timezone.utc)
        last = getattr(state, "_last_llm_hypo_date", None)
        if now_utc.hour == 6 and last != now_utc.date():
            from scripts.hermes_llm_hypothesis_review import llm_review_active_hypotheses
            if not dry_run:
                llm_review_active_hypotheses(engine)
                state._last_llm_hypo_date = now_utc.date()
    except Exception as exc:
        log.warning("LLM hypothesis review failed: {e}", e=str(exc))

CLI: ``python -m scripts.hermes_llm_hypothesis_review`` (honors the same gate).
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text


def _flag(name: str, default: Any) -> Any:
    """Read a GRID setting (settings-or-env), mirroring the hermes config."""
    try:
        from config import settings
        if hasattr(settings, name):
            return getattr(settings, name)
    except Exception:
        pass
    raw = os.getenv(name)
    if raw is None:
        return default
    if isinstance(default, bool):
        return raw.strip().lower() in ("1", "true", "yes", "on")
    if isinstance(default, int):
        try:
            return int(raw)
        except ValueError:
            return default
    return raw


_SELECT_TOP_ACTIVE = text("""
    SELECT id, thesis, pattern_type, evidence, invalidation, confidence
    FROM discovered_hypotheses
    WHERE status = 'active'
    ORDER BY confidence DESC, created_at DESC
    LIMIT :limit
""")


def _build_context(row: Any) -> str:
    """Compact, PIT-safe context for the analyst (no future data)."""
    _, _thesis, ptype, evidence, invalidation, confidence = row
    parts = [f"pattern_type: {ptype}", f"prior_confidence: {confidence}"]
    if invalidation:
        parts.append(f"declared_invalidation: {invalidation}")
    if evidence:
        try:
            ev = evidence if isinstance(evidence, list) else json.loads(evidence)
            parts.append(f"evidence_count: {len(ev)}")
        except Exception:
            pass
    return "\n".join(parts)


def llm_review_active_hypotheses(
    engine: Any,
    *,
    limit: int | None = None,
    enabled: bool | None = None,
) -> dict:
    """Run the LLM analyst over the top-N active hypotheses (advisory only).

    Returns a counts dict. No-op (``reviewed=0``) unless enabled. Never raises
    into the caller and never writes to ``discovered_hypotheses``.
    """
    if enabled is None:
        enabled = bool(_flag("HERMES_HYPO_LLM_ENABLED", False))
    if not enabled:
        log.debug("LLM hypothesis review disabled (HERMES_HYPO_LLM_ENABLED=false)")
        return {"enabled": False, "reviewed": 0}

    if limit is None:
        limit = int(_flag("HERMES_HYPO_LLM_LIMIT", 10))
    limit = max(1, int(limit))

    try:
        with engine.connect() as conn:
            rows = conn.execute(_SELECT_TOP_ACTIVE, {"limit": limit}).fetchall()
    except Exception as exc:
        log.warning("LLM hypothesis review: select failed: {e}", e=str(exc))
        return {"enabled": True, "reviewed": 0, "errors": 1}

    if not rows:
        log.info("LLM hypothesis review: no active hypotheses")
        return {"enabled": True, "reviewed": 0}

    from intelligence.hermes import HermesAgent

    agent = HermesAgent()  # one config load + provider for the whole batch
    start = time.monotonic()
    verdicts: list[dict] = []
    counts: dict = {
        "enabled": True, "reviewed": 0, "errors": 0,
        "total_cost_usd": 0.0, "by_source": {},
    }

    for row in rows:
        hid, thesis = row[0], row[1]
        try:
            verdict = agent.score_hypothesis(thesis, context=_build_context(row))
        except Exception as exc:
            counts["errors"] += 1
            log.warning("LLM hypothesis review: {h} raised: {e}", h=str(hid)[:12], e=str(exc))
            continue
        verdict["hypothesis_id"] = hid
        verdicts.append(verdict)
        counts["reviewed"] += 1
        counts["total_cost_usd"] = round(
            counts["total_cost_usd"] + float(verdict.get("cost_usd") or 0.0), 6
        )
        src = verdict.get("source", "unknown")
        counts["by_source"][src] = counts["by_source"].get(src, 0) + 1

    report_path = _write_report(verdicts, counts)
    counts["report_path"] = str(report_path) if report_path else None
    log.info(
        "LLM hypothesis review — reviewed={r} errors={e} cost=${c:.4f} {ms:.0f}ms by_source={s}",
        r=counts["reviewed"], e=counts["errors"], c=counts["total_cost_usd"],
        ms=(time.monotonic() - start) * 1000, s=counts["by_source"],
    )
    return counts


def _write_report(verdicts: list[dict], counts: dict) -> Path | None:
    """Write the advisory report under outputs/hermes/ (gitignored)."""
    try:
        out_dir = Path(_flag("HERMES_HYPO_LLM_OUTDIR", "outputs/hermes"))
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = out_dir / f"hypothesis_llm_review_{stamp}.json"
        path.write_text(json.dumps({"summary": counts, "verdicts": verdicts}, indent=2, default=str))
        return path
    except Exception as exc:
        log.warning("LLM hypothesis review: report write failed: {e}", e=str(exc))
        return None


def main() -> int:
    from db import get_engine

    result = llm_review_active_hypotheses(get_engine())
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
