"""
GRID Oracle Report — prediction digest with scorecard.

Sends the Oracle's latest predictions with:
  - Model leaderboard (which models are winning)
  - Top predictions with signal/anti-signal breakdown
  - Recent scorecard (hit/miss/partial from last cycle)
  - Capital flow context
  - Self-improvement notes (what changed in weights)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log

from alerts.email import _render_html, _send_in_thread, _section_text, _section_kpi


def send_oracle_report(cycle_result: dict[str, Any]) -> None:
    """Send the Oracle Report email after a prediction cycle."""
    sections = []

    # ── Scorecard ───────────────────────────────────────────────────────
    scoring = cycle_result.get("scoring", {})
    if scoring.get("total", 0) > 0:
        h, m, p = scoring.get("hits", 0), scoring.get("misses", 0), scoring.get("partials", 0)
        total = h + m + p
        hit_rate = (h + p * 0.5) / total * 100 if total > 0 else 0

        sections.append({
            "title": "Scorecard — Recent Results",
            "body": f'''
            <div style="display:flex;justify-content:space-around;text-align:center;padding:12px 0;">
                <div>
                    <div style="font-size:32px;font-weight:800;color:#22C55E;">{h}</div>
                    <div style="font-size:10px;color:#5A7A96;">HITS</div>
                </div>
                <div>
                    <div style="font-size:32px;font-weight:800;color:#F59E0B;">{p}</div>
                    <div style="font-size:10px;color:#5A7A96;">PARTIAL</div>
                </div>
                <div>
                    <div style="font-size:32px;font-weight:800;color:#EF4444;">{m}</div>
                    <div style="font-size:10px;color:#5A7A96;">MISS</div>
                </div>
                <div>
                    <div style="font-size:32px;font-weight:800;color:#4fc3f7;">{hit_rate:.0f}%</div>
                    <div style="font-size:10px;color:#5A7A96;">ADJ RATE</div>
                </div>
            </div>''',
            "accent": "green" if hit_rate > 55 else "red" if hit_rate < 45 else "amber",
        })

    # ── Model Leaderboard ───────────────────────────────────────────────
    leaderboard = cycle_result.get("leaderboard", [])
    if leaderboard:
        rows = ""
        for i, m in enumerate(leaderboard):
            medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f"#{i+1}"
            weight_color = "#22C55E" if m["weight"] > 1.2 else "#EF4444" if m["weight"] < 0.5 else "#C8D8E8"
            rows += f'''<tr>
                <td>{medal}</td>
                <td style="font-weight:700;">{m["name"].replace("_"," ").title()}</td>
                <td style="color:{weight_color};font-weight:700;">{m["weight"]:.2f}x</td>
                <td>{m["total"]}</td>
                <td style="color:#22C55E;">{m["hit_rate"]:.0%}</td>
                <td>{m["pnl"]:+.1f}%</td>
            </tr>'''
        sections.append({
            "title": "Model Tournament",
            "body": f'''<table class="data-table">
                <thead><tr><th></th><th>Model</th><th>Weight</th><th>Predictions</th><th>Hit Rate</th><th>P/L</th></tr></thead>
                <tbody>{rows}</tbody>
            </table>''',
        })

    # ── Weight Evolution ────────────────────────────────────────────────
    evolution = cycle_result.get("evolution", {})
    changes = evolution.get("changes", {})
    if changes:
        change_html = ""
        for model, change in changes.items():
            arrow = "↑" if change["new"] > change["old"] else "↓"
            color = "#22C55E" if change["new"] > change["old"] else "#EF4444"
            change_html += (
                f'<div style="padding:4px 0;border-bottom:1px solid #0D1520;">'
                f'<span style="color:{color};font-weight:700;">{arrow}</span> '
                f'<strong>{model.replace("_"," ").title()}</strong>: '
                f'{change["old"]:.2f} → {change["new"]:.2f} '
                f'(hit rate: {change["hit_rate"]:.0%})</div>'
            )
        sections.append({
            "title": "Self-Improvement — Weight Changes",
            "body": change_html,
            "accent": "purple",
        })

    # ── Top Predictions ─────────────────────────────────────────────────
    preds = cycle_result.get("top_predictions", [])
    for pred in preds[:8]:
        direction = pred.get("direction", "?")
        ticker = pred.get("ticker", "?")
        conf = pred.get("confidence", 0)
        target = pred.get("target_price", 0)
        current = pred.get("current_price", 0)
        expiry = pred.get("expiry", "?")
        model = pred.get("model_name", "?").replace("_", " ").title()
        expected = pred.get("expected_move_pct", 0)

        # Signal summary
        signals = pred.get("signals", [])
        bull_signals = [s for s in signals if s.get("direction") == "bullish"]
        bear_signals = [s for s in signals if s.get("direction") == "bearish"]

        signal_html = ""
        for s in signals[:5]:
            color = "#22C55E" if s["direction"] == "bullish" else "#EF4444" if s["direction"] == "bearish" else "#5A7A96"
            signal_html += (
                f'<span style="display:inline-block;padding:2px 6px;border-radius:4px;'
                f'font-size:10px;margin:2px;background:{color}22;color:{color};">'
                f'{s["name"]} z={s["z_score"]:.1f}</span>'
            )

        # Anti-signals
        anti_html = ""
        anti_signals = pred.get("anti_signals", [])
        if anti_signals:
            anti_html = '<div style="margin-top:6px;"><span style="color:#EF4444;font-size:10px;font-weight:700;">ANTI-SIGNALS:</span> '
            for a in anti_signals[:3]:
                anti_html += (
                    f'<span style="display:inline-block;padding:2px 6px;border-radius:4px;'
                    f'font-size:10px;margin:2px;background:#EF444422;color:#EF4444;">'
                    f'{a["name"]} z={a["z_score"]:.1f}</span>'
                )
            anti_html += '</div>'

        # Flow context
        flow = pred.get("flow_context", {})
        flow_html = ""
        if flow:
            regime = flow.get("regime", "?")
            sector = flow.get("sector", "?")
            posture = flow.get("posture", "?")
            flow_html = (
                f'<div style="margin-top:4px;font-size:10px;color:#5A7A96;">'
                f'Regime: {regime} | Sector: {sector} | Posture: {posture}</div>'
            )

        badge_color = "#22C55E" if direction == "CALL" else "#EF4444"
        conf_color = "#22C55E" if conf > 0.6 else "#F59E0B" if conf > 0.3 else "#5A7A96"

        sections.append({
            "title": f"{ticker} — {direction}",
            "body": f'''
            <div style="display:flex;justify-content:space-between;align-items:center;">
                <div>
                    <span style="display:inline-block;padding:4px 12px;border-radius:20px;font-size:12px;
                        font-weight:700;background:{badge_color}22;color:{badge_color};">{ticker} {direction}</span>
                    <span style="margin-left:8px;font-size:11px;color:#5A7A96;">via {model}</span>
                </div>
                <div style="text-align:right;">
                    <div style="font-size:20px;font-weight:800;color:{conf_color};">{conf:.0%}</div>
                    <div style="font-size:10px;color:#5A7A96;">CONFIDENCE</div>
                </div>
            </div>
            <div style="display:flex;justify-content:space-around;text-align:center;padding:8px 0;margin:8px 0;
                border-top:1px solid #1A2A3A;border-bottom:1px solid #1A2A3A;">
                <div><div style="font-size:10px;color:#5A7A96;">ENTRY</div><div style="font-weight:700;color:#E8F0F8;">${current:.2f}</div></div>
                <div><div style="font-size:10px;color:#5A7A96;">TARGET</div><div style="font-weight:700;color:{badge_color};">${target:.2f}</div></div>
                <div><div style="font-size:10px;color:#5A7A96;">EXPECTED</div><div style="font-weight:700;color:{badge_color};">{expected:+.1f}%</div></div>
                <div><div style="font-size:10px;color:#5A7A96;">EXPIRY</div><div style="font-weight:700;color:#E8F0F8;">{expiry}</div></div>
            </div>
            <div style="margin:4px 0;">
                <span style="font-size:10px;color:#5A7A96;font-weight:700;">SIGNALS ({len(bull_signals)}↑ {len(bear_signals)}↓):</span><br>
                {signal_html}
            </div>
            {anti_html}
            {flow_html}
            ''',
            "accent": "green" if direction == "CALL" else "red",
        })

    # ── Send ────────────────────────────────────────────────────────────
    n_preds = cycle_result.get("new_predictions", 0)
    subject = f"GRID Oracle — {n_preds} Predictions | Cycle Report"

    html = _render_html(subject, sections,
                        footer_note="Predictions are generated by the self-improving Oracle engine. "
                                    "Scored after expiry. Model weights evolve with track record.")
    plain = "\n\n".join(f"[{s['title']}]\n{s.get('body', '')}" for s in sections)

    _send_in_thread(subject, html, plain)
    log.info("Oracle Report sent — {n} predictions", n=n_preds)
