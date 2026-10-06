"""
GRID Hourly Market Briefing Engine.

Generates comprehensive market condition reports using Ollama with
GRID's knowledge base. Pulls latest market data, constructs a
structured prompt with real data context, and produces an AI-powered
market briefing every hour.

Can run as a standalone scheduler or be called from the API.
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import schedule
from loguru import logger as log
from outputs.path_utils import ensure_output_dir

# Output directory for saved briefings
_BRIEFING_DIR = Path(__file__).parent.parent / "outputs" / "market_briefings"


class MarketBriefingEngine:
    """Generates AI-powered market condition briefings using Ollama.

    Pulls latest data from GRID's data stores and feeds it as context
    to the Ollama model along with GRID's knowledge documents for
    deeply informed market analysis.

    Attributes:
        ollama_client: OllamaClient instance.
        db_engine: Optional SQLAlchemy engine for live data access.
    """

    def __init__(
        self,
        ollama_client: Any = None,
        db_engine: Any = None,
    ) -> None:
        self.ollama = ollama_client
        self.engine = db_engine

        if self.ollama is None:
            from ollama.client import get_client
            self.ollama = get_client()

        self.output_dir = ensure_output_dir(_BRIEFING_DIR)
        log.info("MarketBriefingEngine initialised")

    # ------------------------------------------------------------------
    # Data gathering
    # ------------------------------------------------------------------
    def _gather_market_snapshot(self) -> dict[str, Any]:
        """Gather latest available market data for the briefing.

        Returns:
            dict: Structured market data snapshot.
        """
        snapshot: dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "date": date.today().isoformat(),
            "equities": {},
            "equity_volume": {},
            "rates": {},
            "credit": {},
            "volatility": {},
            "commodities": {},
            "fx": {},
            "macro": {},
        }

        if self.engine is None:
            return snapshot

        try:
            from sqlalchemy import text

            with self.engine.connect() as conn:
                # Get latest values from raw_series for key tickers
                key_series = {
                    "equities": [
                        "YF:^GSPC:close", "YF:^DJI:close", "YF:^IXIC:close",
                        "YF:^RUT:close",
                    ],
                    "equity_volume": [
                        "YF:^GSPC:volume",
                    ],
                    "rates": [
                        "FRED:DFF", "FRED:T10Y2Y", "FRED:T10Y3M",
                        "FRED:DGS10", "FRED:DGS2",
                    ],
                    "credit": [
                        "YF:HYG:close", "YF:LQD:close", "YF:JNK:close",
                        "YF:EMB:close",
                    ],
                    "volatility": [
                        "YF:^VIX:close", "YF:^VIX3M:close", "YF:^VIX9D:close",
                    ],
                    "commodities": [
                        "YF:GC=F:close", "YF:SI=F:close", "YF:CL=F:close",
                        "YF:HG=F:close", "YF:GLD:close",
                    ],
                    "fx": [
                        "YF:UUP:close", "YF:FXE:close", "YF:FXY:close",
                        "YF:EEM:close",
                    ],
                }

                for category, series_ids in key_series.items():
                    for sid in series_ids:
                        row = conn.execute(
                            text(
                                "SELECT value, obs_date FROM raw_series "
                                "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                                "ORDER BY obs_date DESC LIMIT 1"
                            ),
                            {"sid": sid},
                        ).fetchone()
                        if row:
                            # Extract a clean label
                            parts = sid.split(":")
                            label = parts[1] if len(parts) > 1 else sid
                            snapshot[category][label] = {
                                "value": round(row[0], 4),
                                "date": str(row[1]),
                            }

                # Get latest feature values from resolved_series.
                #
                # This used to be a single query with a correlated
                # `obs_date = (SELECT MAX(obs_date) FROM resolved_series
                # WHERE feature_id = rs.feature_id)` subquery, which
                # re-scans every historical vintage of resolved_series once
                # per outer row and reliably tripped the DB's
                # statement_timeout (hourly QueryCanceled failures).
                #
                # A plain `DISTINCT ON (feature_id) ... ORDER BY feature_id,
                # obs_date DESC, release_date DESC` (the store/pit.py idiom)
                # was tried first but forces Postgres to sort every matching
                # row across all vintages/features before deduping — with
                # ~13.5M matching rows here that spilled to an external disk
                # sort and took ~48s (measured read-only on grid-svr,
                # 2026-09-27), i.e. still over budget.
                #
                # This LATERAL "top-1-per-feature" rewrite instead does one
                # bounded, per-feature index probe: for each model-eligible
                # feature_id (parameterized via ANY(:fids), same idiom as
                # PITStore.get_pit), take the single row with the greatest
                # obs_date, tiebroken by release_date DESC for deterministic
                # same-obs_date multi-vintage rows — per data-integrity.md's
                # release_date convention. Same meaning as before: the
                # latest available value per model-eligible feature, as of
                # now. Hits idx_resolved_series_pit_latest (feature_id,
                # obs_date, vintage_date DESC) INCLUDE (value, release_date)
                # WHERE release_date IS NOT NULL via an Index Only Scan
                # Backward + Incremental Sort + Limit 1 per feature —
                # measured ~62ms end-to-end read-only on grid-svr vs. a
                # cost-12.4M plan (and reliable timeout) for the original.
                feature_ids = [
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT id FROM feature_registry "
                            "WHERE model_eligible = TRUE"
                        )
                    ).fetchall()
                ]

                feature_rows: list[Any] = []
                if feature_ids:
                    feature_rows = conn.execute(
                        text(
                            "SELECT fr.name, latest.value, latest.obs_date "
                            "FROM feature_registry fr "
                            "JOIN LATERAL ("
                            "  SELECT rs.value, rs.obs_date "
                            "  FROM resolved_series rs "
                            "  WHERE rs.feature_id = fr.id "
                            "  AND rs.release_date IS NOT NULL "
                            "  ORDER BY rs.obs_date DESC, rs.release_date DESC "
                            "  LIMIT 1"
                            ") latest ON TRUE "
                            "WHERE fr.id = ANY(:fids) "
                            "ORDER BY fr.name"
                        ),
                        {"fids": feature_ids},
                    ).fetchall()

                snapshot["features"] = {
                    row[0]: {"value": round(row[1], 4), "date": str(row[2])}
                    for row in feature_rows
                }

                # SPY put/call ratio — explicit lookup so the briefing writer
                # has a real number to cite. On 2026-09-23 the LLM published
                # "P/C ratio of 8.96" though this module had no put/call
                # input anywhere; number_grounding.py now catches an invented
                # figure like that, but the better fix is to also give the
                # model the real one to cite instead. See _fetch_spy_put_call.
                snapshot["options"] = {"spy_put_call": self._fetch_spy_put_call(conn)}

                # Get latest regime inference if available
                regime_row = conn.execute(
                    text(
                        "SELECT inferred_state, state_confidence, "
                        "transition_probability, contradiction_flags, "
                        "grid_recommendation, decision_timestamp "
                        "FROM decision_journal "
                        "ORDER BY decision_timestamp DESC LIMIT 1"
                    )
                ).fetchone()

                if regime_row:
                    snapshot["latest_regime"] = {
                        "state": regime_row[0],
                        "confidence": round(regime_row[1], 4),
                        "transition_prob": round(regime_row[2], 4),
                        "contradictions": regime_row[3],
                        "recommendation": regime_row[4],
                        "timestamp": str(regime_row[5]),
                    }

                # Convergence signals from trust scorer
                try:
                    from intelligence.trust_scorer import detect_convergence
                    convergence = detect_convergence(self.engine)
                    if convergence:
                        snapshot["convergence"] = [
                            {
                                "ticker": e.get("ticker"),
                                "direction": e.get("signal_type"),
                                "sources": e.get("source_count"),
                                "confidence": round(e.get("combined_confidence", 0), 3),
                                "source_types": [s["source_type"] for s in e.get("sources", [])],
                            }
                            for e in convergence[:5]
                        ]
                except Exception:
                    pass

                # High-trust signal summary (top signals from last 7 days)
                try:
                    trust_rows = conn.execute(text(
                        "SELECT source_type, source_id, ticker, signal_type, trust_score "
                        "FROM signal_sources "
                        "WHERE signal_date >= CURRENT_DATE - 7 "
                        "AND trust_score >= 0.65 "
                        "AND outcome IN ('PENDING', 'CORRECT') "
                        "ORDER BY trust_score DESC LIMIT 10"
                    )).fetchall()
                    if trust_rows:
                        snapshot["high_trust_signals"] = [
                            {
                                "type": r[0], "source": r[1], "ticker": r[2],
                                "direction": r[3], "trust": round(r[4], 3),
                            }
                            for r in trust_rows
                        ]
                except Exception:
                    pass

        except Exception as exc:
            log.warning("Could not gather market snapshot: {err}", err=str(exc))

        return snapshot

    def _fetch_spy_put_call(self, conn: Any) -> dict[str, Any] | None:
        """Fetch the latest SPY put/call ratio (open interest, all expiries).

        Tries the resolved feature ``spy_pcr`` first — the PIT-correct
        resolved-series value that ``ingestion/options.py`` writes via the
        ``OPT:SPY:pcr`` source (see ``normalization/entity_map.py``) — then
        falls back to reading ``options_daily_signals.put_call_ratio``
        directly, which is the same figure at its origin table. Returns
        ``None`` rather than a fabricated number if neither has data, per
        ``docs/reference/AVAILABILITY_CONTRACT.md``.

        A stored value of exactly 0 is treated as unavailable, not as a
        real reading: ``spy_pcr`` has repeated ``0`` rows (confirmed via a
        read-only production query — 2026-09-17 and at least 14 other dates
        in its history), which is put OI of 0 against real call OI, not a
        plausible SPY put/call ratio. ``value <= 0`` can only be a bad
        write for a ratio of two positive open-interest counts; treating it
        as "no data" and falling back (or reporting unavailable) is the
        same honesty rule this whole module exists to enforce, applied to
        its own new input.

        Parameters:
            conn: Open SQLAlchemy connection, reused from the caller's
                ``with self.engine.connect() as conn:`` block.

        Returns:
            dict with ``value``, ``date`` (as_of, as text), and ``source``,
            or None if unavailable.
        """
        try:
            from sqlalchemy import text

            row = conn.execute(
                text(
                    "SELECT rs.value, rs.obs_date "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE fr.name = :name "
                    "ORDER BY rs.obs_date DESC LIMIT 1"
                ),
                {"name": "spy_pcr"},
            ).fetchone()
            if row and row[0] is not None and float(row[0]) > 0:
                return {
                    "value": round(float(row[0]), 4),
                    "date": str(row[1]),
                    "source": "spy_pcr",
                }

            row = conn.execute(
                text(
                    "SELECT put_call_ratio, signal_date "
                    "FROM options_daily_signals "
                    "WHERE ticker = :ticker AND put_call_ratio IS NOT NULL "
                    "ORDER BY signal_date DESC LIMIT 1"
                ),
                {"ticker": "SPY"},
            ).fetchone()
            if row and row[0] is not None and float(row[0]) > 0:
                return {
                    "value": round(float(row[0]), 4),
                    "date": str(row[1]),
                    "source": "options_daily_signals",
                }
        except Exception as exc:
            log.warning("Could not fetch SPY put/call ratio: {e}", e=str(exc))

        return None

    def _build_data_context(self, snapshot: dict[str, Any]) -> str:
        """Convert market snapshot to a readable text block for the LLM.

        Parameters:
            snapshot: Market data snapshot dict.

        Returns:
            str: Formatted data context.
        """
        from ollama.number_grounding import publication_context_facts
        facts = publication_context_facts(snapshot)
        lines: list[str] = []
        lines.append(f"## Market Data Snapshot — {snapshot['timestamp']}")
        lines.append("")

        for category in ["equities", "equity_volume", "rates", "credit", "volatility", "commodities", "fx"]:
            data = snapshot.get(category, {})
            if data:
                lines.append(f"### {category.upper()}")
                for label, info in data.items():
                    tenor = '^' + label.lstrip('^').upper()
                    if category == "volatility" and tenor in facts['vix_observation_dates'] and facts['vix_observation_dates'][tenor] is None:
                        lines.append(f"- {label}: unavailable at snapshot cutoff {facts['snapshot_cutoff_date']} "
                                     f"(source observation date: {(info or {}).get('date')})")
                        continue
                    lines.append(f"- {label}: {info['value']} (as of {info['date']})")
                lines.append("")

        lines.append("### PUBLICATION CONTEXT LIMITS")
        lines.append(f"- VIX eligibility cutoff: {facts['snapshot_cutoff_date']} "
                     "(recorded snapshot calendar date; intraday availability unverified).")
        lines.append(f"- VIX tenor observation dates: {json.dumps(facts['vix_observation_dates'])}")
        lines.append(
            "- VIX observation dates match; daily tenor levels may be compared. "
            "Simultaneous intraday quotes and release/ingest times are unverified."
            if facts["vix_dates_compatible"] else
            "- VIX observation dates differ or are unavailable. Report each dated level; "
            "a full three-tenor contemporaneous term-structure comparison is unavailable."
        )
        lines.append("- Regime comparator: unavailable; only the latest journal record is supplied. "
                     "Trend, deterioration and improvement are unmeasured.")
        lines.append("- Classifier confidence is the reported state-assignment score; calibration is unverified. "
                     "Lower confidence does not establish stability or a new regime.")
        lines.append("- Thesis conviction is absolute aggregate model-score magnitude (0..100; stored as 0..1); "
                     "sentiment is a separate weighted "
                     "directional score. Neither measures market-wide certainty or calibrated outcome probability.")
        lines.append("- Aggregate PCR does not identify dealer side, inventory or causal exposure. "
                     "No validated positioning policy or action threshold is supplied.")
        lines.append("")

        # Feature values — select most informative via orthogonality
        features = snapshot.get("features", {})
        if features:
            try:
                from analysis.prompt_optimizer import select_prompt_features, format_features_for_prompt

                # Build feature dicts with z-scores (use value as proxy if z not available)
                feat_list = [
                    {"name": name, "z": info["value"], "value": info["value"]}
                    for name, info in features.items()
                    if info.get("value") is not None
                ]
                selected = select_prompt_features(feat_list, max_count=20, corr_threshold=0.7)
                lines.append(f"### GRID FEATURES ({len(features)} total, {len(selected)} selected by orthogonality)")
                lines.append(format_features_for_prompt(selected, include_value=True))
                lines.append("")
            except Exception:
                # Fallback to simple truncation
                lines.append(f"### GRID FEATURES ({len(features)} total, showing top 20)")
                shown = 0
                for name, info in features.items():
                    if shown >= 20:
                        break
                    lines.append(f"- {name}: {info['value']} (as of {info['date']})")
                    shown += 1
                lines.append("")

        # SPY put/call ratio — explicit, clearly-labeled section so the
        # model has a real number to cite instead of inventing one (see
        # the 2026-09-23 "P/C ratio of 8.96" incident — this section did
        # not exist and no put/call input reached the prompt at all).
        pcr = snapshot.get("options", {}).get("spy_put_call")
        lines.append("### OPTIONS — SPY PUT/CALL RATIO")
        if pcr:
            lines.append(
                "Definition: SPY put/call ratio, open interest, all "
                "expiries (put open interest / call open interest)."
            )
            lines.append("Interpretation: relative contracts only; buyer/seller initiation and hedge intent are unmeasured.")
            lines.append(
                f"- Value: {pcr['value']} (as of {pcr['date']}, "
                f"source: {pcr['source']})"
            )
        else:
            lines.append(
                "- Value: unavailable (no spy_pcr / options_daily_signals data)"
            )
        lines.append("")

        # Latest regime
        regime = snapshot.get("latest_regime")
        if regime:
            lines.append("### LATEST REGIME INFERENCE")
            lines.append(f"- State: {regime['state']}")
            lines.append(f"- Reported classifier confidence (calibration unverified): {regime['confidence']}")
            lines.append(f"- Reported transition score (calibration unverified): {regime['transition_prob']}")
            lines.append(f"- Journal recommendation (unvalidated policy): {regime['recommendation']}")
            lines.append(f"- Timestamp: {regime['timestamp']}")
            if regime.get("contradictions"):
                lines.append(f"- Contradictions: {json.dumps(regime['contradictions'])}")
            lines.append("")

        # Convergence signals
        convergence = snapshot.get("convergence")
        if convergence:
            lines.append("### CONVERGENCE SIGNALS (3+ independent sources agreeing)")
            for evt in convergence:
                sources = ", ".join(evt.get("source_types", []))
                lines.append(
                    f"- {evt['ticker']}: {evt['direction']} — "
                    f"{evt['sources']} sources ({sources}), "
                    f"reported combined confidence (calibration unverified) {evt['confidence']}"
                )
                from analysis.market_universe import search_company
                matches = [c for c in search_company(evt['ticker']) if c['ticker'] == evt['ticker']]
                if matches:
                    company = matches[0]
                    lines.append(f"  Company classification: {company['name']} — {company['sector']} / {company['industry']}")
            lines.append("")

        # High-trust signals
        trust_signals = snapshot.get("high_trust_signals")
        if trust_signals:
            lines.append("### HIGH-TRUST SIGNALS (trust_score >= 0.65, last 7 days)")
            for sig in trust_signals:
                lines.append(
                    f"- [{sig['type']}] {sig['source']}: {sig['direction']} {sig['ticker']} "
                    f"(trust={sig['trust']})"
                )
            lines.append("")

        # Intelligence context: hypotheses, postmortems, company profiles
        try:
            from intelligence.context_provider import build_full_context
            intel_context = build_full_context(self.engine, max_hypotheses=8, max_postmortems=3, max_companies=3)
            if intel_context:
                lines.append("")
                lines.append(intel_context)
        except Exception as exc:
            log.debug("Market briefing: intelligence context injection failed: {e}", e=str(exc))

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Briefing generation
    # ------------------------------------------------------------------
    def generate_briefing(
        self,
        briefing_type: str = "hourly",
        save: bool = True,
    ) -> dict[str, Any]:
        """Generate a market conditions briefing.

        Parameters:
            briefing_type: One of 'hourly', 'daily', 'weekly'.
            save: Whether to save the briefing to disk.

        Returns:
            dict: Briefing result with keys 'content', 'snapshot',
                  'timestamp', 'type'.
        """
        log.info("Generating {t} market briefing", t=briefing_type)

        # Gather live data
        snapshot = self._gather_market_snapshot()
        data_context = self._build_data_context(snapshot)

        # Add historical context from wiki history
        try:
            from ingestion.wiki_history import WikiHistoryPuller
            wp = WikiHistoryPuller()
            wiki = wp.pull_today()
            if wiki.get("on_this_day_summary"):
                data_context += f"\n\n### HISTORICAL CONTEXT\n{wiki['on_this_day_summary']}\n"
                # Add financially relevant events
                financial_events = [e for e in wiki.get("wiki_events", [])
                    if any(k in (e.get("text", "")).lower()
                           for k in ["bank", "stock", "crash", "recession", "fed", "gold", "oil", "war", "trade"])]
                if financial_events:
                    data_context += "Key financial history for this date:\n"
                    for e in financial_events[:3]:
                        data_context += f"- {e.get('year', '?')}: {e.get('text', '')[:120]}\n"
        except Exception:
            pass

        # Add social sentiment context
        try:
            from ingestion.social_sentiment import SocialSentimentPuller
            sp = SocialSentimentPuller()
            sentiment = sp.pull_all()
            if sentiment.get("ticker_sentiment"):
                data_context += "\n\n### SOCIAL SENTIMENT (Reddit + Bluesky)\n"
                data_context += sentiment.get("summary", "") + "\n"
                for tk, sc in sorted(
                    sentiment["ticker_sentiment"].items(),
                    key=lambda x: x[1]["mentions"], reverse=True
                )[:8]:
                    data_context += f"- {tk}: {sc['sentiment']} ({sc['mentions']} mentions, bull ratio {sc['bull_ratio']})\n"
            if sentiment.get("trends"):
                data_context += "\n### GOOGLE TRENDS (7-day)\n"
                for kw, t in sentiment["trends"].items():
                    data_context += f"- {kw}: {t['trend']} (current {t['current']}, avg {t['avg_7d']}, peak {t['peak']})\n"
        except Exception:
            pass

        # ── Deterministic sentiment scoring ──
        # The LLM interprets — it NEVER computes direction or sentiment.
        sentiment = None
        try:
            from intelligence.sentiment_scorer import compute_sentiment, log_prediction
            _eng = self.engine
            if _eng is None:
                from db import get_engine
                _eng = get_engine()
            sentiment = compute_sentiment(_eng)
            log_prediction(_eng, sentiment)
            data_context += "\n\n### COMPUTED SENTIMENT (deterministic — DO NOT override)\n"
            data_context += f"- Score: {sentiment.score:+.2f} ({sentiment.label})\n"
            data_context += f"- Context: {sentiment.context}\n"
            for comp in sentiment.components:
                data_context += f"- {comp.name}: score={comp.score:+.2f}, weight={comp.weight:.0%} — {comp.detail}\n"
            data_context += (
                "\nIMPORTANT: The sentiment score above is computed from data. "
                "Your job is to EXPLAIN why the score is what it is and what it means — "
                "NOT to compute your own score. Do not say 'bearish' if the score is bullish. "
                "Start your briefing with the computed score and label.\n"
            )
        except Exception as exc:
            log.warning("Sentiment scoring unavailable: {e}", e=exc)

        # Build the prompt
        system_prompt = self._get_system_prompt(briefing_type)
        user_prompt = self._get_user_prompt(briefing_type, data_context)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        content = self.ollama.chat(
            messages=messages,
            temperature=0.4,
            num_predict=800,
        )

        if content is None:
            content = self._generate_fallback_briefing(snapshot)
            log.warning("LLM unavailable — using fallback briefing")

        from ollama.number_grounding import annotate_publication_context, check_publication_claims

        content, context_guard = annotate_publication_context(content, snapshot)
        snapshot["publication_context_guard"] = context_guard

        claim_guard = check_publication_claims(content)
        snapshot["publication_claim_guard"] = claim_guard
        if not claim_guard["passed"]:
            # Replace the whole candidate instead of leaving its causal story
            # intact with a numeric footnote. Only future generations change.
            summary = self._generate_fallback_briefing(snapshot)
            summary, fallback_context_guard = annotate_publication_context(summary, snapshot)
            context_guard["fallback"] = fallback_context_guard
            summary_guard = check_publication_claims(summary)
            claim_guard["fallback"] = summary_guard
            if not summary_guard["passed"]:
                raise ValueError("Data summary withheld: unsupported source interpretation.")
            content = (
                "**AI narrative withheld: unsupported source interpretation. Data summary only.**\n\n"
                + summary
            )

        # ── Number-grounding gate ──
        # Runs on every generated briefing before it is written: numeric
        # claims (decimals, percents, $ amounts, ratios) must be backed by
        # a number actually present in `data_context` — the same text
        # handed to the LLM above. Catches invented figures with no basis
        # at all (the 2026-09-23 "P/C ratio of 8.96" incident) that a
        # prompt instruction alone did not stop. See ollama/number_grounding.py.
        from ollama.number_grounding import check_numbers_grounded

        grounding = check_numbers_grounded(content, data_context)
        content = grounding.annotated_text
        grounding_stats = grounding.to_dict()
        snapshot["grounding"] = grounding_stats

        result = {
            "content": content,
            "snapshot": snapshot,
            "timestamp": datetime.now().isoformat(),
            "type": briefing_type,
            "sentiment": sentiment.to_dict() if sentiment else None,
            "grounding": grounding_stats,
            "publication_claim_guard": claim_guard,
            "publication_context_guard": context_guard,
        }

        if save:
            self._save_briefing(result)
            self._persist_to_db(result)

        log.info(
            "{t} briefing generated — {n} chars, sentiment={s}, "
            "grounding={u}/{gt} unverified",
            t=briefing_type,
            n=len(content),
            s=sentiment.label if sentiment else "N/A",
            u=len(grounding.ungrounded),
            gt=grounding.total_numbers,
        )
        return result

    def _get_system_prompt(self, briefing_type: str) -> str:
        """Build the system prompt for the briefing.

        Parameters:
            briefing_type: Type of briefing.

        Returns:
            str: System prompt.
        """
        base = (
            "You are GRID's market analyst AI. You write briefings for a solo "
            "systematic trader who needs to know WHAT IS HAPPENING, WHY IT MATTERS, "
            "and WHAT TO DO ABOUT IT. Never list raw numbers — interpret every data "
            "point with evidence. Describe VIX as implied volatility. Historical "
            "frequency or return predictions require supplied empirical evidence. "
            "Be direct. Describe source-supported observations and what to monitor. "
            "Start with the single most important thing happening right now. "
            "Separate LEVERS (actor actions that open/close liquidity valves) from "
            "CONDITIONS (environment that amplifies). Lead each section with the "
            "lever, then the condition. If you cannot name the actor, the valve, "
            "and the flow direction, do not make the call."
        )
        from ollama.number_grounding import PUBLICATION_EVIDENCE_RULES
        base += " " + PUBLICATION_EVIDENCE_RULES

        if briefing_type == "hourly":
            return (
                f"{base}\n\n"
                "HOURLY BRIEFING FORMAT — 300 words max:\n\n"
                "## What's Happening Now\n"
                "One paragraph: the single most important market development right now and why it matters.\n\n"
                "## Regime Check\n"
                "One line: latest dated regime state and reported classifier confidence. "
                "A trend is unavailable without a dated comparator.\n\n"
                "## Contradictions\n"
                "Any signals that disagree with each other. If none, say 'Signals aligned.'\n\n"
                "## Action\n"
                "One sentence: what evidence or observation dates to watch. No positioning instructions."
            )
        elif briefing_type == "daily":
            return (
                f"{base}\n\n"
                "DAILY BRIEFING FORMAT — 500 words max:\n\n"
                "## Bottom Line\n"
                "Two sentences: What happened today and what it means for positioning.\n\n"
                "## Regime\n"
                "Report the latest dated state and classifier confidence. "
                "Do not compare with yesterday without a supplied dated comparator.\n\n"
                "## What Changed\n"
                "Only describe changes backed by dated prior and current observations. "
                "For each: what moved, by how much, and what it implies.\n\n"
                "## Risks\n"
                "What could go wrong from here. Name specific scenarios.\n\n"
                "## Opportunities\n"
                "Describe observations to investigate; use supplied company classifications.\n\n"
                "## Tomorrow\n"
                "What to watch for tomorrow. Scheduled data releases, key levels, catalysts."
            )
        else:  # weekly
            return (
                f"{base}\n\n"
                "WEEKLY BRIEFING FORMAT — 800 words max:\n\n"
                "## The Week in One Sentence\n"
                "Capture the week's story arc.\n\n"
                "## Regime Evolution\n"
                "Report the dated regime record; disclose missing weekly history.\n\n"
                "## Winners and Losers\n"
                "Which sectors/assets outperformed and underperformed. Name specific "
                "tickers and percentage moves. Explain WHY (flows, earnings, macro).\n\n"
                "## Macro Signals\n"
                "Key economic data that landed this week and what it means for "
                "the rate path, growth outlook, and sector rotation.\n\n"
                "## International\n"
                "Anything outside the US that matters: China, Europe, Japan, EM.\n\n"
                "## Three Scenarios for Next Week\n"
                "Qualitative hypotheses only; no probabilities or trade triggers without empirical support.\n\n"
                "## Playbook\n"
                "Evidence to investigate next week; no unsupported positioning instructions."
            )

    def _get_user_prompt(self, briefing_type: str, data_context: str) -> str:
        """Build the user prompt with embedded data.

        Parameters:
            briefing_type: Type of briefing.
            data_context: Formatted market data context.

        Returns:
            str: User prompt.
        """
        now = datetime.now()
        return (
            f"Generate the {briefing_type} GRID market conditions briefing.\n\n"
            f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
            f"Day of week: {now.strftime('%A')}\n"
            f"Market hours context: "
            f"{'US markets are open' if 9 <= now.hour <= 16 and now.weekday() < 5 else 'US markets are closed'}\n\n"
            f"{data_context}\n\n"
            f"Analyze these conditions using the GRID framework. Follow the "
            f"market analysis framework structure. Be specific about feature "
            f"values and what they indicate. Flag any contradictions between "
            f"signal families. Provide a clear regime classification with "
            f"confidence level."
        )

    def _generate_fallback_briefing(self, snapshot: dict[str, Any]) -> str:
        """Generate a basic data-driven briefing when Ollama is unavailable.

        Parameters:
            snapshot: Market data snapshot.

        Returns:
            str: Fallback briefing text.
        """
        from ollama.number_grounding import publication_context_facts
        facts = publication_context_facts(snapshot)
        lines = [
            f"# GRID Market Briefing — {snapshot['timestamp']}",
            "",
            "**Note: AI analysis unavailable. Data summary only.**",
            "",
        ]

        for category in ["equities", "equity_volume", "rates", "credit", "volatility", "commodities", "fx"]:
            data = snapshot.get(category, {})
            if data:
                lines.append(f"## {category.replace('_', ' ').title()}")
                for label, info in data.items():
                    tenor = '^' + label.lstrip('^').upper()
                    if category == "volatility" and tenor in facts['vix_observation_dates'] and facts['vix_observation_dates'][tenor] is None:
                        lines.append(f"- {label}: unavailable at snapshot cutoff {facts['snapshot_cutoff_date']}")
                        continue
                    lines.append(f"- **{label}**: {info['value']} ({info['date']})")
                lines.append("")

        regime = snapshot.get("latest_regime")
        if regime:
            lines.append("## Latest Regime")
            state_display = " ".join(str(regime['state']).splitlines())
            lines.append(f"- Reported state: **{state_display}**")
            lines.append(f"- Reported model confidence (calibration unverified): {regime['confidence']}")
            lines.append(f"- Journal record timestamp: {regime['timestamp']}")
            lines.append("- Prior regime comparator unavailable; trend unmeasured.")
            # Preserve the original recommendation in the audit snapshot,
            # not in the deterministic publication's factual data summary.
            lines.append("")

        lines.append("---")
        lines.append("*Data summary; AI narrative unavailable.*")

        return "\n".join(lines)

    def _persist_to_db(self, result: dict[str, Any]) -> int | None:
        """Persist briefing + sentiment to the database for API serving.

        Creates the market_briefings table if needed. Returns briefing ID.
        """
        eng = self.engine
        if eng is None:
            try:
                from db import get_engine
                eng = get_engine()
            except Exception:
                return None

        try:
            from sqlalchemy import text as _text
            with eng.connect() as conn:
                conn.execute(_text("""
                    CREATE TABLE IF NOT EXISTS market_briefings (
                        id              SERIAL PRIMARY KEY,
                        briefing_type   TEXT NOT NULL,
                        briefing_date   DATE NOT NULL,
                        content         TEXT NOT NULL,
                        sentiment_score REAL,
                        sentiment_label TEXT,
                        sentiment_data  JSONB,
                        snapshot_data   JSONB,
                        created_at      TIMESTAMPTZ DEFAULT NOW()
                    );
                    CREATE INDEX IF NOT EXISTS idx_market_briefings_date
                        ON market_briefings (briefing_date DESC);
                    CREATE INDEX IF NOT EXISTS idx_market_briefings_type
                        ON market_briefings (briefing_type, briefing_date DESC);
                """))

                sentiment = result.get("sentiment")
                row = conn.execute(_text(
                    "INSERT INTO market_briefings "
                    "(briefing_type, briefing_date, content, "
                    " sentiment_score, sentiment_label, sentiment_data, snapshot_data) "
                    "VALUES (:btype, CURRENT_DATE, :content, "
                    " :score, :label, :sdata, :snap) "
                    "RETURNING id"
                ), {
                    "btype": result["type"],
                    "content": result["content"],
                    "score": sentiment["score"] if sentiment else None,
                    "label": sentiment["label"] if sentiment else None,
                    "sdata": json.dumps(sentiment) if sentiment else None,
                    "snap": json.dumps(result["snapshot"], default=str),
                }).fetchone()
                conn.commit()
                bid = row[0] if row else None
                log.info("Briefing persisted to DB id={id}", id=bid)
                return bid
        except Exception as e:
            log.warning("Failed to persist briefing to DB: {e}", e=e)
            return None

    def _save_briefing(self, result: dict[str, Any]) -> None:
        """Save a briefing to disk as a markdown file.

        Also writes a ``<stem>.grounding.json`` sidecar with the
        number-grounding stats for this briefing (see
        ``ollama/number_grounding.py``), matching how the rest of this
        engine's output is written — one file per generated briefing.

        Parameters:
            result: Briefing result dict.
        """
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{result['type']}_{ts}.md"
        filepath = self.output_dir / filename

        with filepath.open("w", encoding="utf-8") as f:
            f.write(result["content"])
            f.write(f"\n\n---\n*Generated: {result['timestamp']}*\n")

        grounding = result.get("grounding")
        if grounding is not None:
            sidecar_path = filepath.with_name(f"{filepath.stem}.grounding.json")
            try:
                with sidecar_path.open("w", encoding="utf-8") as f:
                    json.dump(grounding, f, indent=2, default=str)
            except Exception as exc:
                log.warning(
                    "Could not write grounding sidecar {p}: {e}",
                    p=sidecar_path, e=str(exc),
                )

    @staticmethod
    def cleanup_old_briefings(max_age_days: int = 90) -> int:
        """Delete briefing files older than max_age_days.

        Returns the number of files deleted.
        """

        cutoff = datetime.now() - timedelta(days=max_age_days)
        deleted = 0
        output_dir = ensure_output_dir(_BRIEFING_DIR)
        for f in output_dir.glob("*.md"):
            parts = f.stem.rsplit("_", 2)
            if len(parts) >= 3:
                try:
                    file_ts = datetime.strptime(
                        f"{parts[-2]}_{parts[-1]}", "%Y%m%d_%H%M%S"
                    )
                    if file_ts < cutoff:
                        f.unlink()
                        deleted += 1
                        sidecar = f.with_name(f"{f.stem}.grounding.json")
                        sidecar.unlink(missing_ok=True)
                except ValueError:
                    continue
        return deleted

    # ------------------------------------------------------------------
    # Latest briefing retrieval
    # ------------------------------------------------------------------
    def get_latest_briefing(self, briefing_type: str = "hourly") -> str | None:
        """Read the most recent briefing file of the given type.

        Parameters:
            briefing_type: Type prefix to filter by.

        Returns:
            str: Briefing content, or None if not found.
        """
        pattern = f"{briefing_type}_*.md"
        files = sorted(self.output_dir.glob(pattern), reverse=True)
        if not files:
            return None
        return files[0].read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def start_hourly_briefings(db_engine: Any = None) -> None:
    """Start the hourly market briefing scheduler.

    Generates an hourly briefing during US market hours (8 AM - 5 PM ET,
    Monday-Friday) and a daily summary at 5:30 PM ET on weekdays.

    Parameters:
        db_engine: Optional SQLAlchemy engine for live data.
    """
    engine_instance = MarketBriefingEngine(db_engine=db_engine)

    log.info("Starting hourly market briefing scheduler")

    # Hourly briefings every hour on the hour, market hours
    for hour in range(8, 18):
        time_str = f"{hour:02d}:00"
        schedule.every().monday.at(time_str).do(
            engine_instance.generate_briefing, briefing_type="hourly"
        )
        schedule.every().tuesday.at(time_str).do(
            engine_instance.generate_briefing, briefing_type="hourly"
        )
        schedule.every().wednesday.at(time_str).do(
            engine_instance.generate_briefing, briefing_type="hourly"
        )
        schedule.every().thursday.at(time_str).do(
            engine_instance.generate_briefing, briefing_type="hourly"
        )
        schedule.every().friday.at(time_str).do(
            engine_instance.generate_briefing, briefing_type="hourly"
        )

    # Daily summary at 5:30 PM ET on weekdays
    schedule.every().monday.at("17:30").do(
        engine_instance.generate_briefing, briefing_type="daily"
    )
    schedule.every().tuesday.at("17:30").do(
        engine_instance.generate_briefing, briefing_type="daily"
    )
    schedule.every().wednesday.at("17:30").do(
        engine_instance.generate_briefing, briefing_type="daily"
    )
    schedule.every().thursday.at("17:30").do(
        engine_instance.generate_briefing, briefing_type="daily"
    )
    schedule.every().friday.at("17:30").do(
        engine_instance.generate_briefing, briefing_type="daily"
    )

    # Weekly summary Sunday evening
    schedule.every().sunday.at("18:00").do(
        engine_instance.generate_briefing, briefing_type="weekly"
    )

    # Generate an initial briefing immediately
    log.info("Generating initial briefing...")
    engine_instance.generate_briefing(briefing_type="hourly")

    log.info(
        "Briefing scheduler configured — hourly (8AM-5PM M-F), "
        "daily (5:30PM M-F), weekly (6PM Sun)"
    )

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        log.info("Briefing scheduler stopped (KeyboardInterrupt)")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        # Generate a single briefing and print it
        briefing_type = sys.argv[2] if len(sys.argv) > 2 else "hourly"
        engine = MarketBriefingEngine()
        result = engine.generate_briefing(briefing_type=briefing_type)
        print(result["content"])
    elif len(sys.argv) > 1 and sys.argv[1] == "--daemon":
        # Run as a background scheduler
        try:
            from db import get_engine
            db_eng = get_engine()
        except Exception:
            db_eng = None
            log.warning("No database — briefings will use limited data")
        start_hourly_briefings(db_engine=db_eng)
    else:
        print("Usage:")
        print("  python -m ollama.market_briefing --once [hourly|daily|weekly]")
        print("  python -m ollama.market_briefing --daemon")
