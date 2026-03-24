"""
Periodic scanner for accumulated LLM insights.

Reads all timestamped LLM insight files, groups them by theme and
time period, and generates a review markdown file summarising
recurring patterns, evolving hypotheses, and notable regime changes.

Can run as a scheduled task (daily or weekly) or on demand.

Usage::

    from outputs.insight_scanner import run_insight_review

    # Generate a weekly review of all accumulated insights
    review_path = run_insight_review(days=7)

    # Or from CLI:
    # python -m outputs.insight_scanner --days 7
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from loguru import logger as log

_INSIGHTS_DIR = Path(__file__).parent / "llm_insights"
_REVIEWS_DIR = Path(__file__).parent / "insight_reviews"
_REVIEWS_DIR.mkdir(parents=True, exist_ok=True)


def _parse_insight_file(filepath: Path) -> dict[str, Any] | None:
    """Parse a single insight markdown file into structured data."""
    try:
        text = filepath.read_text(encoding="utf-8")
    except Exception:
        return None

    result: dict[str, Any] = {"path": str(filepath), "filename": filepath.name}

    # Extract title
    title_match = re.search(r"^# (.+)$", text, re.MULTILINE)
    result["title"] = title_match.group(1) if title_match else filepath.stem

    # Extract category
    cat_match = re.search(r"\*\*Category:\*\*\s*(\S+)", text)
    result["category"] = cat_match.group(1) if cat_match else "unknown"

    # Extract provider
    prov_match = re.search(r"\*\*Provider:\*\*\s*(\S+)", text)
    result["provider"] = prov_match.group(1) if prov_match else "unknown"

    # Extract timestamp
    ts_match = re.search(r"\*\*Timestamp:\*\*\s*(.+?)  ", text)
    if ts_match:
        try:
            result["timestamp"] = datetime.fromisoformat(ts_match.group(1).strip())
        except ValueError:
            result["timestamp"] = None
    else:
        result["timestamp"] = None

    # Extract content section
    content_match = re.search(r"## Content\n\n(.+?)(?:\n---|\Z)", text, re.DOTALL)
    result["content"] = content_match.group(1).strip() if content_match else ""

    # Extract metadata JSON
    meta_match = re.search(r"```json\n(.+?)```", text, re.DOTALL)
    if meta_match:
        import json
        try:
            result["metadata"] = json.loads(meta_match.group(1))
        except json.JSONDecodeError:
            result["metadata"] = {}
    else:
        result["metadata"] = {}

    return result


def _extract_key_terms(content: str) -> list[str]:
    """Extract significant terms from content for theme detection."""
    # Financial/economic terms to track
    important_patterns = [
        r"yield\s+curve", r"credit\s+spread", r"inflation",
        r"recession", r"tightening", r"easing", r"regime",
        r"volatility", r"correlation", r"momentum",
        r"mean[\s-]reversion", r"risk[\s-]on", r"risk[\s-]off",
        r"overfitting", r"lookahead", r"survivorship",
        r"VIX", r"Fed", r"ECB", r"BOJ",
        r"bull", r"bear", r"neutral", r"BUY", r"SELL", r"HOLD",
    ]
    terms: list[str] = []
    lower = content.lower()
    for p in important_patterns:
        if re.search(p, lower, re.IGNORECASE):
            terms.append(re.sub(r"\\s[+-]?", " ", p.replace(r"\s+", " ")))
    return terms


def run_insight_review(days: int = 7) -> Path | None:
    """Generate a review of accumulated LLM insights.

    Reads all insight files from the last ``days`` days, groups them
    by category and theme, and writes a review markdown file to
    ``outputs/insight_reviews/``.

    Parameters:
        days: Number of days to look back.

    Returns:
        Path to the generated review file, or None if no insights found.
    """
    cutoff = datetime.now() - timedelta(days=days)
    all_files = sorted(_INSIGHTS_DIR.glob("*.md"))

    insights: list[dict[str, Any]] = []
    for f in all_files:
        parsed = _parse_insight_file(f)
        if parsed and parsed.get("timestamp") and parsed["timestamp"] >= cutoff:
            insights.append(parsed)

    if not insights:
        log.info("No insights found in the last {d} days — skipping review", d=days)
        return None

    log.info("Scanning {n} insights from the last {d} days", n=len(insights), d=days)

    # Group by category
    by_category: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for i in insights:
        by_category[i["category"]].append(i)

    # Collect all key terms for theme analysis
    all_terms: list[str] = []
    for i in insights:
        all_terms.extend(_extract_key_terms(i.get("content", "")))
    term_counts = Counter(all_terms).most_common(15)

    # Track regime transitions
    regime_insights = by_category.get("regime_analysis", [])
    regime_transitions: list[str] = []
    for ri in regime_insights:
        meta = ri.get("metadata", {})
        if meta.get("from_regime") and meta.get("to_regime"):
            regime_transitions.append(
                f"- {meta['from_regime']} -> {meta['to_regime']} "
                f"({ri.get('timestamp', '').isoformat() if ri.get('timestamp') else 'unknown'})"
            )

    # Track agent decisions
    agent_insights = by_category.get("agent_deliberation", [])
    decision_counts: Counter[str] = Counter()
    for ai in agent_insights:
        content = ai.get("content", "")
        for decision in ("BUY", "SELL", "HOLD"):
            if decision in content:
                decision_counts[decision] += 1
                break

    # Track hypothesis themes
    hypothesis_insights = by_category.get("hypothesis", [])

    # Build review document
    ts = datetime.now()
    ts_str = ts.strftime("%Y%m%d_%H%M%S")
    review_filename = f"review_{ts_str}.md"
    review_path = _REVIEWS_DIR / review_filename

    lines = [
        f"# GRID Insight Review — {ts.strftime('%Y-%m-%d')}",
        "",
        f"**Period:** Last {days} days (since {cutoff.strftime('%Y-%m-%d')})  ",
        f"**Total insights:** {len(insights)}  ",
        f"**Generated:** {ts.isoformat()}  ",
        "",
        "## Summary by Category",
        "",
        "| Category | Count |",
        "|----------|-------|",
    ]

    for cat in sorted(by_category):
        lines.append(f"| {cat} | {len(by_category[cat])} |")

    lines.extend(["", "## Dominant Themes", ""])
    if term_counts:
        for term, count in term_counts:
            lines.append(f"- **{term}** — mentioned in {count} insight(s)")
    else:
        lines.append("No dominant themes detected.")

    # Regime transitions
    lines.extend(["", "## Regime Transitions", ""])
    if regime_transitions:
        lines.extend(regime_transitions)
    else:
        lines.append("No regime transitions detected in this period.")

    # Agent decision tally
    lines.extend(["", "## Agent Decision Distribution", ""])
    if decision_counts:
        for decision, count in decision_counts.most_common():
            lines.append(f"- **{decision}**: {count} run(s)")
    else:
        lines.append("No agent runs in this period.")

    # Hypothesis evolution
    lines.extend(["", "## Hypothesis Activity", ""])
    if hypothesis_insights:
        lines.append(f"{len(hypothesis_insights)} hypothesis generation(s) in this period:")
        lines.append("")
        for hi in hypothesis_insights[-5:]:  # Last 5
            lines.append(f"### {hi['title']}")
            lines.append("")
            content = hi.get("content", "")
            # Show first 500 chars
            lines.append(content[:500] + ("..." if len(content) > 500 else ""))
            lines.append("")
    else:
        lines.append("No hypothesis generation in this period.")

    # Critique summary
    critique_insights = by_category.get("critique", [])
    lines.extend(["", "## Backtest Critique Summary", ""])
    if critique_insights:
        lines.append(f"{len(critique_insights)} backtest critique(s) in this period:")
        lines.append("")
        for ci in critique_insights[-3:]:
            lines.append(f"- **{ci['title']}**")
            meta = ci.get("metadata", {})
            if meta.get("metric_name"):
                lines.append(
                    f"  - {meta['metric_name']}: {meta.get('metric_value', '?')} "
                    f"vs baseline {meta.get('baseline_value', '?')}"
                )
    else:
        lines.append("No backtest critiques in this period.")

    # Recent explanations
    explanation_insights = by_category.get("explanation", [])
    lines.extend(["", "## Mechanism Explanations", ""])
    if explanation_insights:
        lines.append(f"{len(explanation_insights)} explanation(s) in this period:")
        lines.append("")
        for ei in explanation_insights[-5:]:
            meta = ei.get("metadata", {})
            lines.append(
                f"- **{meta.get('feature_a', '?')} x {meta.get('feature_b', '?')}**: "
                f"{ei.get('content', '')[:150]}..."
            )
    else:
        lines.append("No mechanism explanations in this period.")

    # Longer-term patterns (if enough data)
    lines.extend(["", "## Longer-Term Patterns", ""])
    if len(insights) >= 10:
        lines.append("Sufficient data for pattern analysis:")
        lines.append("")

        # Check for recurring themes
        if term_counts and term_counts[0][1] >= 3:
            lines.append(
                f"- **Persistent theme**: '{term_counts[0][0]}' appears in "
                f"{term_counts[0][1]}/{len(insights)} insights — worth monitoring"
            )

        # Check for regime stability
        if len(regime_transitions) == 0 and agent_insights:
            lines.append("- **Regime stability**: No transitions detected despite active analysis")
        elif len(regime_transitions) >= 3:
            lines.append(
                f"- **Regime instability**: {len(regime_transitions)} transitions in {days} days "
                "— elevated uncertainty"
            )

        # Decision consensus
        if decision_counts:
            dominant = decision_counts.most_common(1)[0]
            total = sum(decision_counts.values())
            pct = dominant[1] / total * 100
            if pct >= 70:
                lines.append(
                    f"- **Strong consensus**: {pct:.0f}% of agent runs recommend {dominant[0]}"
                )
            elif pct <= 40:
                lines.append("- **No consensus**: Agent decisions are split — high uncertainty regime")
    else:
        lines.append(f"Only {len(insights)} insights — need 10+ for pattern analysis.")

    lines.extend([
        "",
        "---",
        f"*Generated by GRID Insight Scanner at {ts.isoformat()}*",
    ])

    review_path.write_text("\n".join(lines), encoding="utf-8")
    log.info("Insight review generated — {f}", f=review_filename)

    # Send weekly review newsletter (only for 7+ day reviews)
    if days >= 7:
        try:
            from alerts.email import send_weekly_review
            send_weekly_review("\n".join(lines))
        except Exception:
            pass

    return review_path


def schedule_reviews() -> None:
    """Register daily and weekly insight reviews with the ingestion scheduler.

    Call this from the API startup to enable automatic reviews.
    """
    import threading
    import time as _time

    def _daily_review_loop() -> None:
        while True:
            _time.sleep(86400)  # 24 hours
            try:
                run_insight_review(days=1)
            except Exception as exc:
                log.warning("Daily insight review failed: {e}", e=str(exc))

    def _weekly_review_loop() -> None:
        while True:
            _time.sleep(604800)  # 7 days
            try:
                run_insight_review(days=7)
            except Exception as exc:
                log.warning("Weekly insight review failed: {e}", e=str(exc))

    threading.Thread(target=_daily_review_loop, daemon=True, name="insight-daily").start()
    threading.Thread(target=_weekly_review_loop, daemon=True, name="insight-weekly").start()
    log.info("Insight scanner scheduled — daily + weekly reviews")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="GRID Insight Scanner")
    parser.add_argument("--days", type=int, default=7, help="Days to look back")
    args = parser.parse_args()

    result = run_insight_review(days=args.days)
    if result:
        print(f"Review written to: {result}")
    else:
        print("No insights found for the given period.")
