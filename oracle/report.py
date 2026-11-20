"""Oracle cycle email: concise activity summary, then explicit diagnostic detail.

Legacy model counters and signed underlying moves are not a reconciled track
record. No portfolio/option return can be inferred from their sum.
"""
from __future__ import annotations

import math
import re
from datetime import date, datetime, timezone
from html import escape, unescape
from typing import Any

from loguru import logger as log

from alerts.email import _render_html, _send_in_thread


_FOOTER = (
    "Research predictions, not executed trades. Prices are recorded observations, not live quotes. "
    "Historical model counters are unreconciled; no portfolio or option return is reported."
)


def _count(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _number(value: Any, fmt: str, *, prefix: str = "", suffix: str = "") -> str:
    if value is None or isinstance(value, bool):
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return "n/a"
    return f"{prefix}{number:{fmt}}{suffix}" if math.isfinite(number) else "n/a"


def _timestamp(value: Any) -> str:
    try:
        at = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return "unknown"
    if at.tzinfo is None or at.utcoffset() is None:
        return f"{at:%Y-%m-%d %H:%M:%S} (timezone unknown)"
    return f"{at.astimezone(timezone.utc):%Y-%m-%d %H:%M:%S} UTC"


def _plain(body: str) -> str:
    text = re.sub(r"</(?:tr|div|p|li|h[1-6])>|<br\s*/?>", "\n", body)
    text = re.sub(r"</t[dh]>\s*<t[dh][^>]*>", " | ", text)
    text = unescape(re.sub(r"<[^>]+>", "", text))
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


def _scoring_summary(scoring: dict[str, Any]) -> str:
    total = _count(scoring.get("total"))
    outcomes = [_count(scoring.get(key)) for key in ("hits", "partials", "misses")]
    if total is None or any(n is None for n in outcomes):
        return "Scoring unavailable; no result rate can be established."
    hits, partials, misses = outcomes
    if sum(outcomes) != total:
        return "Scoring counts disagree; results need reconciliation."
    return f"{total:,} scored this cycle: {hits:,} hits / {partials:,} partial / {misses:,} misses."



def _signal_age(signal: dict[str, Any]) -> str:
    """Distinguish exact event age, observation DATE and ingestion availability."""
    p = signal.get("provenance") or {}
    if p.get("event_time_basis") == "signal_date_date_only":
        return (
            "exact source age unknown; observation date: "
            f"{escape(str(p.get('signal_date') or 'unknown'))} (DATE only); "
            f"calendar date age: {_number(p.get('date_age_days'), '.0f', suffix='d')}; "
            f"observation-date age lower bound (UTC calendar): {_number(p.get('freshness_hours_lower_bound'), '.1f', suffix='h')}; "
            f"ingested: {escape(_timestamp(p.get('available_at')))}"
        )
    return f"source age at generation: {_number(signal.get('freshness_hours'), '.1f', suffix='h')}"

def _prediction_section(pred: dict[str, Any], today: date) -> dict[str, Any]:
    ticker = escape(str(pred.get("ticker") or "unknown"))
    direction = escape(str(pred.get("direction") or "unknown"))
    model = escape(str(pred.get("model_name") or "unknown").replace("_", " ").title())
    expiry = pred.get("expiry")
    try:
        expiry_date = date.fromisoformat(str(expiry))
        if expiry_date < today:
            status = "Expired; not an active call"
        elif expiry_date == today:
            status = "Expires today; intraday expiry status unknown"
        else:
            status = "Future expiry; current validity not rechecked"
    except (TypeError, ValueError):
        status = "Expiry unknown; current validity unknown"
    body = (
        f"<p><strong>{status}</strong><br>Model: {model}<br>"
        f"Prediction recorded: {escape(_timestamp(pred.get('timestamp')))}<br>"
        "Underlying reference price below is not a live quote. "
        "CALL/PUT is the predicted direction, not a priced option trade.</p>"
        '<table class="data-table"><tr><th>Reference price</th><th>Target</th>'
        '<th>Expected underlying move</th><th>Expiry</th><th>Model confidence score</th></tr><tr>'
        f"<td>{_number(pred.get('current_price'), '.2f', prefix='$')}</td>"
        f"<td>{_number(pred.get('target_price'), '.2f', prefix='$')}</td>"
        f"<td>{_number(pred.get('expected_move_pct'), '+.1f', suffix='%')}</td>"
        f"<td>{escape(str(expiry or 'unknown'))}</td>"
        f"<td>{_number(pred.get('confidence'), '.0%')}</td></tr></table>"
        '<p>Confidence is a model score, not an established probability of profit.</p>'
    )
    signal_payload = pred.get("signals") or []
    signals = signal_payload.get("items", []) if isinstance(signal_payload, dict) else signal_payload
    antis = pred.get("anti_signals") or []
    bull = sum(s.get("direction") == "bullish" for s in signals)
    bear = sum(s.get("direction") == "bearish" for s in signals)
    body += f"<p><strong>Evidence: {len(signals)} signals ({bull} bullish / {bear} bearish), {len(antis)} anti-signals.</strong></p>"
    if signals:
        body += "<ul>" + "".join(
            f"<li>{escape(str(s.get('name', 'unknown')))}: "
            f"{escape(str(s.get('direction', 'unknown')))}, z={_number(s.get('z_score'), '.1f')}; "
            f"{_signal_age(s)}</li>"
            for s in signals
        ) + "</ul>"
    actor_audit = (signal_payload.get("actor_evidence") if isinstance(signal_payload, dict)
                   else (pred.get("flow_context") or {}).get("actor_evidence"))
    if isinstance(actor_audit, dict):
        if not actor_audit.get("complete"):
            body += (
                "<p>Actor evidence unavailable; no actor votes used: "
                f"{escape(str(actor_audit.get('reason') or 'unknown'))}.</p>"
            )
        else:
            candidate_count = _count(actor_audit.get("observation_count"))
            sample = actor_audit.get("observations")
            sample_count = len(sample) if isinstance(sample, list) else 0
            count_label = str(candidate_count) if candidate_count is not None else "unknown"
            detail_status = "complete" if actor_audit.get("observations_complete") is True else "sampled/truncated"
            body += (
                f"<p>Actor evidence: {len(actor_audit.get('direct_vote_row_ids', []))} direct votes "
                f"from {count_label} candidate observations. "
                "Repeated actor/channel observations are not independent confirmations; "
                "latest-day conflicting or unknown directions are withheld. "
                "Actor observations have DATE precision; exact event age is unknown. "
                f"Stored candidate detail: {sample_count}/{count_label} ({detail_status}); "
                "used support references are complete.</p>"
            )
    if antis:
        body += "<p><strong>Anti-signals</strong></p><ul>" + "".join(
            f"<li>{escape(str(a.get('name', 'unknown')))}: z={_number(a.get('z_score'), '.1f')}; "
            f"{escape(str(a.get('contradiction') or 'reason unavailable'))}</li>"
            for a in antis
        ) + "</ul>"
    flow = pred.get("flow_context") or {}
    if flow:
        body += "<p>Recorded flow context: " + " | ".join(
            f"{label}: {escape(str(flow.get(key) or 'unknown'))}"
            for key, label in (("regime", "Regime"), ("sector", "Sector"), ("posture", "Posture"))
        ) + "</p>"
    return {"title": f"{ticker} - {direction}", "body": body}


def build_oracle_report(cycle_result: dict[str, Any], *, now: datetime | None = None) -> tuple[str, str, str]:
    """Build without sending or querying live data; useful for offline previews."""
    now = now or datetime.now(timezone.utc)
    votes = _count(cycle_result.get("new_predictions"))
    inserted = _count(cycle_result.get("distinct_positions_written"))
    updated = _count(cycle_result.get("positions_updated"))
    writes = _count(cycle_result.get("upsert_statements"))
    forward_record = "new_abstentions" in cycle_result
    abstentions = _count(cycle_result.get("new_abstentions"))
    unchanged = _count(cycle_result.get("unchanged_forecasts"))
    # Update operations can touch the same row repeatedly. Incomplete RETURNING
    # counts cannot establish a distinct-row total; never substitute model votes.
    if forward_record:
        if (None in (inserted, abstentions, unchanged, writes) or updated != 0
                or inserted + abstentions + unchanged != writes):
            inserted = updated = None
    elif writes is not None and (inserted is None or updated is None or inserted + updated != writes):
        inserted = updated = None
    headline = f"{inserted:,} New prediction rows" if inserted is not None else "New rows unknown"
    vote_label = f"{votes:,} model votes" if votes is not None else "model votes unknown"
    if forward_record:
        vote_label = f"{votes:,} accepted forecasts" if votes is not None else "accepted count unknown"
    admission_note = (
        f"<p>{abstentions:,} new abstention records; {unchanged:,} prior forecasts preserved unchanged. "
        "Abstentions are excluded from new forecast counts and top calls.</p>"
        if forward_record and abstentions is not None and unchanged is not None else ""
    )
    subject = f"GRID Oracle - {headline} | {vote_label}"
    scoring = cycle_result.get("scoring") or {}
    summary = _scoring_summary(scoring)
    unscorable = _count(scoring.get("unscorable_entry_price"))
    held_counts = [_count(scoring.get(k)) for k in ("held_legacy_entry_price", "held_unmarked_null_entry_price")]
    held = sum(held_counts) if all(n is not None for n in held_counts) else None
    exceptions = (
        f"{unscorable:,} unscorable" if unscorable is not None else "unscorable count unknown"
    ) + "; " + (f"{held:,} held" if held is not None else "held count unknown")
    preds = cycle_result.get("top_predictions") or []
    changes = (cycle_result.get("evolution") or {}).get("changes") or {}
    sections = [{
        "title": "At a glance",
        "body": (
            f'<div style="font-size:32px;font-weight:800;color:#4fc3f7">{headline}</div>'
            f"<p>{vote_label}; "
            + (f"{updated:,} update operations" if updated is not None else "update count unknown")
            + ". Updates are not distinct positions or executed trades.</p>"
            + admission_note +
            f"<p><strong>{summary}</strong><br>{exceptions}. "
            "These counts describe this scoring pass, not the entire pending backlog.</p>"
            f"<p>{len(preds)} supplied prediction details below; {len(changes)} model weight changes. "
            "Historical model counters remain unreconciled; do not use them as an investment track record.</p>"
            f"<p>Cycle started: {escape(_timestamp(cycle_result.get('cycle_started_at')))}<br>"
            f"Cycle completed: {escape(_timestamp(cycle_result.get('cycle_completed_at')))}<br>"
            f"Report generated: {_timestamp(now)}<br>Report time does not establish source freshness.</p>"
        ),
    }]
    sections.append({
        "title": "Scoring detail - this cycle only",
        "body": f"<p>{summary}</p><p>{exceptions}. Unscorable rows lack usable entry data. "
                "Held rows retain the historical-write/provenance hold; they are not scored as misses.</p>"
                "<p>Hits/partials/misses are the engine's underlying-price outcome labels, "
                "not portfolio or option returns. Historical reconciliation is still required.</p>",
    })
    leaderboard = cycle_result.get("leaderboard") or []
    if leaderboard:
        rows = "".join(
            "<tr>"
            f"<td>{escape(str(model.get('name', 'unknown')).replace('_', ' ').title())}</td>"
            f"<td>{_number(model.get('weight'), '.2f', suffix='x')}</td>"
            f"<td>{_number(model.get('total'), ',.0f')}</td>"
            f"<td>{_number(model.get('hits'), ',.0f')} / {_number(model.get('partials'), ',.0f')} / {_number(model.get('misses'), ',.0f')}</td>"
            f"<td>{_number(model.get('pnl'), '+,.1f', suffix=' pp')}</td></tr>"
            for model in leaderboard
        )
        sections.append({
            "title": "Model diagnostics - historical, unreconciled",
            "body": (
                "<p>Stored lifetime counters, ordered by current model weight, not investment performance. "
                "Recorded totals can disagree with outcome counts; no hit-rate claim is made from them.</p>"
                '<table class="data-table"><thead><tr><th>Model</th><th>Weight</th>'
                '<th>Recorded count</th><th>Hit / partial / miss</th><th>Signed move sum (pp)</th>'
                f"</tr></thead><tbody>{rows}</tbody></table>"
                "<p>pp = percentage points: the sum of direction-signed underlying price changes across predictions. "
                "This adds overlapping predictions without capital allocation, compounding, option premiums or costs. "
                "It is not portfolio or option returns and is not a validated track record.</p>"
            ),
            "accent": "amber",
        })
    if changes:
        body = "<ul>" + "".join(
            f"<li>{escape(str(name).replace('_', ' ').title())}: "
            f"{_number(change.get('old'), '.2f')} to {_number(change.get('new'), '.2f')} "
            f"(recorded hit-rate input: {_number(change.get('hit_rate'), '.0%')})</li>"
            for name, change in changes.items()
        ) + "</ul><p>Weight changes are model settings, not evidence of improved performance.</p>"
        sections.append({"title": "Model weight changes", "body": body})
    if preds:
        sections.append({"title": "Prediction details", "body":
            f"<p>Showing all {len(preds)} predictions supplied to this report (the cycle supplies a ranked sample, "
            "not the complete journal). Multiple models can describe the same ticker and expiry.</p>"})
        sections.extend(_prediction_section(pred, now.astimezone(timezone.utc).date()) for pred in preds)
    plain = "\n\n".join(f"[{_plain(s['title'])}]\n{_plain(s['body'])}" for s in sections) + "\n\n" + _FOOTER
    return subject, _render_html(subject, sections, footer_note=_FOOTER), plain


def send_oracle_report(cycle_result: dict[str, Any]) -> None:
    """Queue the Oracle report after a prediction cycle, without claiming delivery."""
    subject, html, plain = build_oracle_report(cycle_result)
    # Retain the existing reference verification path. The HTML contains no
    # external source links; the plain-text annotation is checked before send.
    try:
        from config import settings
        if settings.REF_CHECK_ENABLED:
            import asyncio
            from verification.ref_extractor import extract_refs
            from verification.url_health import check_urls
            from verification.annotator import annotate_output

            refs = extract_refs(plain)
            if refs:
                loop = asyncio.new_event_loop()
                try:
                    url_results = loop.run_until_complete(check_urls(
                        [r.url for r in refs],
                        max_concurrent=settings.REF_CHECK_MAX_CONCURRENT,
                        timeout_s=settings.REF_CHECK_TIMEOUT_S,
                        wayback_enabled=settings.REF_CHECK_WAYBACK_ENABLED,
                    ))
                finally:
                    loop.close()
                annotated = annotate_output(plain, url_results)
                plain = annotated.cleaned_text
                log.info("Ref guard on Oracle report: {rem} removed, {rep} replaced",
                         rem=annotated.removed_count, rep=annotated.replaced_count)
    except Exception as exc:
        log.debug("Reference check skipped for Oracle report: {e}", e=str(exc))
    _send_in_thread(subject, html, plain)
    log.info("Oracle report queued: {subject}", subject=subject)
