"""
Timestamped markdown logger for all LLM outputs and insights.

Every LLM-generated response — reasoner explanations, hypothesis
candidates, backtest critiques, regime analysis, and agent
deliberations — is logged to a timestamped .md file under
``outputs/llm_insights/``.  This creates a searchable archive for
long-term pattern recognition and retrospective analysis.

Usage::

    from outputs.llm_logger import log_insight

    log_insight(
        category="explanation",
        title="Yield curve vs HY spread correlation breakdown",
        content=llm_response_text,
        metadata={"feature_a": "yield_curve_2s10s", "feature_b": "hy_spread"},
    )
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger as log

_INSIGHTS_DIR = Path(__file__).parent / "llm_insights"
_INSIGHTS_DIR.mkdir(parents=True, exist_ok=True)

# Valid insight categories — each gets its own prefix for easy filtering
CATEGORIES = {
    "explanation",       # Economic mechanism explanations
    "hypothesis",        # Generated hypothesis candidates
    "critique",          # Backtest critiques
    "regime_analysis",   # Regime transition analysis
    "agent_deliberation",  # Full TradingAgents deliberation
    "briefing",          # Market briefings (also saved separately)
    "ad_hoc",            # Ad-hoc Ollama/Hyperspace queries
}


def log_insight(
    category: str,
    title: str,
    content: str | None,
    metadata: dict[str, Any] | None = None,
    provider: str = "unknown",
) -> Path | None:
    """Write an LLM output to a timestamped markdown file.

    Parameters:
        category: One of CATEGORIES (explanation, hypothesis, etc.).
        title: Short human-readable title for the insight.
        content: The LLM-generated text.  If None, logs a skip and returns.
        metadata: Optional dict of structured context (features, tickers,
            regime states, metrics, etc.).
        provider: LLM provider name (ollama, hyperspace, openai, etc.).

    Returns:
        Path to the written file, or None if content was empty/None.
    """
    if content is None:
        log.debug("LLM insight skipped (no content) — category={c}", c=category)
        return None

    if category not in CATEGORIES:
        log.warning("Unknown insight category '{c}' — using 'ad_hoc'", c=category)
        category = "ad_hoc"

    ts = datetime.now()
    ts_str = ts.strftime("%Y%m%d_%H%M%S")
    filename = f"{category}_{ts_str}.md"
    filepath = _INSIGHTS_DIR / filename

    lines = [
        f"# {title}",
        "",
        f"**Category:** {category}  ",
        f"**Provider:** {provider}  ",
        f"**Timestamp:** {ts.isoformat()}  ",
    ]

    if metadata:
        lines.append("")
        lines.append("## Metadata")
        lines.append("```json")
        lines.append(json.dumps(metadata, indent=2, default=str))
        lines.append("```")

    lines.append("")
    lines.append("## Content")
    lines.append("")
    lines.append(content)
    lines.append("")
    lines.append("---")
    lines.append(f"*Logged by GRID LLM Logger at {ts.isoformat()}*")

    filepath.write_text("\n".join(lines), encoding="utf-8")
    log.debug("LLM insight logged — {f}", f=filename)

    # Send newsletter for noteworthy insights
    _NEWSLETTER_CATEGORIES = {"regime_analysis", "hypothesis", "critique", "100x_opportunity"}
    if category in _NEWSLETTER_CATEGORIES:
        try:
            from alerts.email import send_insight as _send_insight
            _send_insight(category, title, content, metadata)
        except Exception:
            pass

    return filepath


def log_agent_deliberation(
    ticker: str,
    regime_state: str,
    confidence: float,
    parsed: dict[str, Any],
    provider: str,
    model: str,
    duration: float,
) -> Path | None:
    """Log a full TradingAgents deliberation to a timestamped md file.

    Parameters:
        ticker: Stock ticker analysed.
        regime_state: Detected regime at time of run.
        confidence: Regime confidence (0-1).
        parsed: Parsed agent decision dict with analyst_reports,
            bull_bear_debate, risk_assessment, final_decision, etc.
        provider: LLM provider.
        model: LLM model name.
        duration: Run duration in seconds.

    Returns:
        Path to the written file.
    """
    ts = datetime.now()
    ts_str = ts.strftime("%Y%m%d_%H%M%S")
    filename = f"agent_deliberation_{ts_str}.md"
    filepath = _INSIGHTS_DIR / filename

    lines = [
        f"# Agent Deliberation: {ticker}",
        "",
        f"**Timestamp:** {ts.isoformat()}  ",
        f"**Ticker:** {ticker}  ",
        f"**Regime:** {regime_state} (confidence: {confidence:.1%})  ",
        f"**Final Decision:** {parsed.get('final_decision', 'UNKNOWN')}  ",
        f"**Provider:** {provider} / {model}  ",
        f"**Duration:** {duration:.1f}s  ",
        "",
        "## Decision Reasoning",
        "",
        parsed.get("decision_reasoning", "(none)"),
        "",
    ]

    # Analyst reports
    reports = parsed.get("analyst_reports")
    if reports and reports != {"note": "simulated — package not available"}:
        lines.append("## Analyst Reports")
        lines.append("")
        if isinstance(reports, dict):
            for role, report in reports.items():
                lines.append(f"### {role}")
                lines.append("")
                lines.append(str(report) if not isinstance(report, str) else report)
                lines.append("")
        else:
            lines.append(json.dumps(reports, indent=2, default=str))
            lines.append("")

    # Bull/Bear debate
    debate = parsed.get("bull_bear_debate")
    if debate and debate != {"note": "simulated"}:
        lines.append("## Bull/Bear Debate")
        lines.append("")
        if isinstance(debate, dict):
            for side, argument in debate.items():
                lines.append(f"### {side}")
                lines.append("")
                lines.append(str(argument) if not isinstance(argument, str) else argument)
                lines.append("")
        else:
            lines.append(str(debate))
            lines.append("")

    # Risk assessment
    risk = parsed.get("risk_assessment")
    if risk and risk != {"note": "simulated"}:
        lines.append("## Risk Assessment")
        lines.append("")
        if isinstance(risk, dict):
            lines.append(json.dumps(risk, indent=2, default=str))
        else:
            lines.append(str(risk))
        lines.append("")

    lines.append("---")
    lines.append(f"*Logged by GRID LLM Logger at {ts.isoformat()}*")

    filepath.write_text("\n".join(lines), encoding="utf-8")
    log.debug("Agent deliberation logged — {f}", f=filename)
    return filepath


def get_recent_insights(
    category: str | None = None,
    days: int = 7,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """List recent insight files with basic metadata.

    Parameters:
        category: Filter by category prefix (optional).
        days: How many days back to look.
        limit: Maximum number of files to return.

    Returns:
        List of dicts with filename, category, timestamp, path.
    """
    from datetime import timedelta

    cutoff = datetime.now() - timedelta(days=days)
    pattern = f"{category}_*.md" if category else "*.md"
    files = sorted(_INSIGHTS_DIR.glob(pattern), reverse=True)

    results: list[dict[str, Any]] = []
    for f in files[:limit]:
        # Extract timestamp from filename: category_YYYYMMDD_HHMMSS.md
        parts = f.stem.rsplit("_", 2)
        if len(parts) >= 3:
            try:
                file_ts = datetime.strptime(f"{parts[-2]}_{parts[-1]}", "%Y%m%d_%H%M%S")
                if file_ts < cutoff:
                    continue
                results.append({
                    "filename": f.name,
                    "category": "_".join(parts[:-2]),
                    "timestamp": file_ts.isoformat(),
                    "path": str(f),
                    "size_bytes": f.stat().st_size,
                })
            except ValueError:
                continue

    return results
