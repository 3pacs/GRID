"""
GRID — Composite Valuation Model.

Ties together:
  1. Intrinsic value engine (balance sheet + earnings valuations)
  2. Milestone tracker (company goals, guidance, rumors)
  3. Derivatives support gauge (short float + GEX + options)

Produces:
  - A unified timeline showing valuation vs price vs derivatives support
  - A structured prompt for Claude Max deep analysis
  - Date-stamped prediction logging for tracking accuracy

The core question this answers:
  "Given what this company is worth on paper, what they say they're going
   to do, and how derivatives are positioned — is the price justified?"
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from valuation.intrinsic import IntrinsicValueEngine, ValuationResult
from valuation.milestones import MilestoneTracker
from valuation.derivatives_support import DerivativesSupportEngine, DerivativesSupportResult


@dataclass
class CompositeValuation:
    """Full valuation picture combining all three components."""

    ticker: str
    analysis_date: date
    valuation: ValuationResult | None = None
    derivatives: DerivativesSupportResult | None = None
    milestones: list[dict[str, Any]] = field(default_factory=list)
    scorecard: dict[str, Any] = field(default_factory=dict)

    # Probability-weighted milestone impact on intrinsic value
    milestone_value_adjustment: float = 0.0

    # Adjusted intrinsic value (base + milestone expectations)
    adjusted_intrinsic_low: float | None = None
    adjusted_intrinsic_mid: float | None = None
    adjusted_intrinsic_high: float | None = None

    # The verdict
    price_vs_value: str = "UNKNOWN"  # UNDERVALUED / FAIR / OVERVALUED
    derivatives_alignment: str = "UNKNOWN"  # SUPPORTING / NEUTRAL / CONFLICTING
    overall_assessment: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "analysis_date": str(self.analysis_date),
            "valuation": self.valuation.to_dict() if self.valuation else None,
            "derivatives": self.derivatives.to_dict() if self.derivatives else None,
            "milestones": self.milestones,
            "scorecard": self.scorecard,
            "milestone_value_adjustment": self.milestone_value_adjustment,
            "adjusted_intrinsic_low": self.adjusted_intrinsic_low,
            "adjusted_intrinsic_mid": self.adjusted_intrinsic_mid,
            "adjusted_intrinsic_high": self.adjusted_intrinsic_high,
            "price_vs_value": self.price_vs_value,
            "derivatives_alignment": self.derivatives_alignment,
            "overall_assessment": self.overall_assessment,
        }


class CompositeValuationEngine:
    """Orchestrates the full valuation pipeline.

    1. Compute intrinsic values from balance sheet data
    2. Gather milestones and compute probability-weighted adjustments
    3. Analyze derivatives positioning
    4. Produce composite assessment
    5. Generate Claude Max prompt
    6. Store and log everything with timestamps
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.intrinsic = IntrinsicValueEngine(db_engine)
        self.milestones = MilestoneTracker(db_engine)
        self.derivatives = DerivativesSupportEngine(db_engine)

    def analyze(self, ticker: str, as_of: date | None = None) -> CompositeValuation:
        """Run the full composite valuation pipeline."""
        if as_of is None:
            as_of = date.today()

        ticker = ticker.upper()
        composite = CompositeValuation(ticker=ticker, analysis_date=as_of)

        # 1. Intrinsic valuation from balance sheet
        composite.valuation = self.intrinsic.valuate(ticker, as_of)

        # 2. Milestones + scorecard
        composite.milestones = self.milestones.get_timeline(ticker)
        composite.scorecard = self.milestones.get_scorecard(ticker)
        composite.milestone_value_adjustment = self.milestones.probability_weighted_impact(ticker)

        # 3. Adjusted intrinsic values (base + milestone expectations)
        if composite.valuation:
            adj = composite.milestone_value_adjustment
            if composite.valuation.intrinsic_low is not None:
                composite.adjusted_intrinsic_low = composite.valuation.intrinsic_low + adj
            if composite.valuation.intrinsic_mid is not None:
                composite.adjusted_intrinsic_mid = composite.valuation.intrinsic_mid + adj
            if composite.valuation.intrinsic_high is not None:
                composite.adjusted_intrinsic_high = composite.valuation.intrinsic_high + adj

        # 4. Derivatives support
        intrinsic_for_deriv = composite.adjusted_intrinsic_mid
        composite.derivatives = self.derivatives.analyze(
            ticker, as_of, intrinsic_mid=intrinsic_for_deriv,
        )

        # 5. Compute verdict
        self._assess(composite)

        return composite

    def analyze_and_store(self, ticker: str, as_of: date | None = None) -> CompositeValuation:
        """Run analysis and persist all components."""
        composite = self.analyze(ticker, as_of)

        # Store valuation
        if composite.valuation:
            self.intrinsic._store(composite.valuation)

        # Store derivatives
        if composite.derivatives:
            self.derivatives._store(composite.derivatives)

        return composite

    def generate_prompt(self, ticker: str, as_of: date | None = None) -> dict[str, Any]:
        """Generate a Claude Max prompt with all quantified data.

        Returns:
            {
                "prompt": str,       -- The full prompt to paste into Claude Max
                "data": dict,        -- The raw data for reference
                "analysis_id": str,  -- Unique ID for tracking predictions
            }
        """
        composite = self.analyze(ticker, as_of)
        analysis_id = f"{ticker}_{composite.analysis_date}_{datetime.utcnow().strftime('%H%M%S')}"

        prompt = self._build_prompt(composite, analysis_id)

        return {
            "prompt": prompt,
            "data": composite.to_dict(),
            "analysis_id": analysis_id,
            "analysis_date": str(composite.analysis_date),
            "ticker": ticker.upper(),
        }

    def log_response(
        self,
        analysis_id: str,
        ticker: str,
        response_text: str,
        predictions: list[dict[str, Any]] | None = None,
    ) -> int:
        """Log a Claude Max response with date stamp and extracted predictions.

        Args:
            analysis_id: The analysis ID from generate_prompt()
            ticker: Stock ticker
            response_text: The full Claude Max response text
            predictions: Optionally, extracted predictions in structured form:
                [{"metric": "price", "target": 150, "timeframe": "6mo", "direction": "up", "confidence": 0.7}]

        Returns:
            The ID of the stored analysis log entry.
        """
        with self.engine.begin() as conn:
            row = conn.execute(
                text("""
                    INSERT INTO valuation_analysis_log (
                        analysis_id, ticker, analysis_date, response_text,
                        predictions, created_at
                    ) VALUES (
                        :aid, :ticker, CURRENT_DATE, :response,
                        :predictions::jsonb, NOW()
                    )
                    RETURNING id
                """),
                {
                    "aid": analysis_id,
                    "ticker": ticker.upper(),
                    "response": response_text,
                    "predictions": json.dumps(predictions or []),
                },
            ).fetchone()

            log_id = row[0]

        log.info("Logged analysis response #{id} for {t} (analysis_id={aid})",
                 id=log_id, t=ticker, aid=analysis_id)
        return log_id

    def get_prediction_history(self, ticker: str, limit: int = 50) -> list[dict[str, Any]]:
        """Get all past analysis predictions for a ticker, for accuracy tracking."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT id, analysis_id, analysis_date, predictions,
                           accuracy_score, created_at
                    FROM valuation_analysis_log
                    WHERE ticker = :ticker
                    ORDER BY analysis_date DESC
                    LIMIT :lim
                """),
                {"ticker": ticker.upper(), "lim": limit},
            ).fetchall()

        return [
            {
                "id": r[0], "analysis_id": r[1], "analysis_date": str(r[2]),
                "predictions": r[3], "accuracy_score": r[4],
                "created_at": str(r[5]),
            }
            for r in rows
        ]

    def _assess(self, c: CompositeValuation) -> None:
        """Compute the overall assessment verdict."""
        # Price vs value
        if c.valuation and c.valuation.market_price and c.adjusted_intrinsic_mid:
            price = c.valuation.market_price
            mid = c.adjusted_intrinsic_mid
            ratio = price / mid if mid > 0 else 1.0

            if ratio < 0.80:
                c.price_vs_value = "SIGNIFICANTLY_UNDERVALUED"
            elif ratio < 0.95:
                c.price_vs_value = "UNDERVALUED"
            elif ratio <= 1.10:
                c.price_vs_value = "FAIR_VALUE"
            elif ratio <= 1.25:
                c.price_vs_value = "OVERVALUED"
            else:
                c.price_vs_value = "SIGNIFICANTLY_OVERVALUED"

        # Derivatives alignment
        if c.derivatives:
            ds = c.derivatives.derivatives_support_score
            if c.price_vs_value in ("UNDERVALUED", "SIGNIFICANTLY_UNDERVALUED"):
                # Price is cheap — are derivatives supporting a rebound?
                if ds >= 60:
                    c.derivatives_alignment = "SUPPORTING"  # Derivatives agree, could bounce
                elif ds <= 40:
                    c.derivatives_alignment = "CONFLICTING"  # Cheap but derivatives pressuring
                else:
                    c.derivatives_alignment = "NEUTRAL"
            elif c.price_vs_value in ("OVERVALUED", "SIGNIFICANTLY_OVERVALUED"):
                # Price is rich — are derivatives keeping it propped up?
                if ds >= 60:
                    c.derivatives_alignment = "PROPPING"  # Derivatives holding up overvalued price
                elif ds <= 40:
                    c.derivatives_alignment = "CONFIRMING_WEAKNESS"  # Overvalued + derivatives weak
                else:
                    c.derivatives_alignment = "NEUTRAL"
            else:
                c.derivatives_alignment = "NEUTRAL"

        # Overall assessment narrative
        parts = []
        if c.valuation and c.adjusted_intrinsic_mid:
            parts.append(
                f"{c.ticker} adjusted intrinsic value: ${c.adjusted_intrinsic_mid:.2f} "
                f"(range ${c.adjusted_intrinsic_low or 0:.2f}-${c.adjusted_intrinsic_high or 0:.2f})."
            )
            if c.valuation.market_price:
                parts.append(f"Market price: ${c.valuation.market_price:.2f}. Assessment: {c.price_vs_value}.")

        if c.scorecard.get("execution_score") is not None:
            parts.append(
                f"Execution score: {c.scorecard['execution_score']:.0f}/100 "
                f"({c.scorecard.get('assessment', 'N/A')})."
            )

        if c.derivatives:
            parts.append(
                f"Derivatives: {c.derivatives.support_regime} "
                f"(score {c.derivatives.derivatives_support_score:.0f}/100). "
                f"Alignment: {c.derivatives_alignment}."
            )

        c.overall_assessment = " ".join(parts)

    def _build_prompt(self, c: CompositeValuation, analysis_id: str) -> str:
        """Build the Claude Max prompt with all quantified data."""
        sections = []

        # Header
        sections.append(f"""# Valuation & Derivatives Analysis: {c.ticker}
**Analysis Date:** {c.analysis_date}
**Analysis ID:** {analysis_id}

You are a senior equity analyst. Below is quantified data from GRID's valuation model for {c.ticker}. Analyze this data and provide:

1. **Fair value assessment** — Is the current price justified by the balance sheet?
2. **Milestone execution scoring** — Is management delivering on their stated plans?
3. **Derivatives positioning read** — Are derivatives supporting or pressuring the price?
4. **Forward predictions** — Date-stamped predictions with specific price targets and timeframes.
5. **Risk factors** — What could invalidate the thesis?

Be specific. Use numbers. Every prediction must have a target value, timeframe, and confidence level (0-1).
""")

        # Valuation section
        if c.valuation:
            v = c.valuation
            sections.append(f"""## Balance Sheet Intrinsic Values

| Method | Per-Share Value | Notes |
|--------|----------------|-------|
| Book Value | ${v.book_value_ps or 0:.2f} | Total equity / shares |
| Tangible Book | ${v.tangible_book_ps or 0:.2f} | Equity minus intangibles & goodwill |
| Net Current Assets (NCAV) | ${v.ncav_ps or 0:.2f} | Graham's liquidation floor |
| Net Cash | ${v.net_cash_ps or 0:.2f} | Cash minus all debt |
| Liquidation Value | ${v.liquidation_ps or 0:.2f} | Conservative asset haircuts |
| Earnings Power Value | ${v.epv_ps or 0:.2f} | Normalized earnings / 10% CoC |
| Owner Earnings | ${v.owner_earnings_ps or 0:.2f} | Buffett: NI + D&A - maint capex |
| DCF (10yr, 3% growth) | ${v.dcf_ps or 0:.2f} | Free cash flow discounted at 10% |

**Composite Range:** ${v.intrinsic_low or 0:.2f} (conservative) — ${v.intrinsic_mid or 0:.2f} (median) — ${v.intrinsic_high or 0:.2f} (optimistic)
**Current Price:** ${v.market_price or 0:.2f}
**Margin of Safety:** {(v.margin_of_safety or 0) * 100:.1f}%

**Key Ratios:** P/E={v.pe_ratio or 'N/A'}, P/B={v.pb_ratio or 'N/A'}, P/S={v.ps_ratio or 'N/A'}, EV/EBITDA={v.ev_ebitda or 'N/A'}
""")

            if v.input_payload:
                sections.append(f"""### Raw Financial Inputs (most recent filing)
```json
{json.dumps(v.input_payload, indent=2, default=str)}
```
""")

        # Milestones section
        if c.milestones:
            sections.append("## Company Milestones & Goals\n")
            for m in c.milestones:
                status_icon = {
                    "ACHIEVED": "[ACHIEVED]", "AHEAD": "[AHEAD]",
                    "ON_TRACK": "[ON TRACK]", "PENDING": "[PENDING]",
                    "BEHIND": "[BEHIND]", "MISSED": "[MISSED]",
                    "CANCELLED": "[CANCELLED]", "SUPERSEDED": "[SUPERSEDED]",
                }.get(m["status"], f"[{m['status']}]")

                target_str = ""
                if m.get("target_value") is not None:
                    target_str = f" Target: {m['target_value']}{m.get('target_unit', '')}"

                actual_str = ""
                if m.get("actual_value") is not None:
                    actual_str = f" Actual: {m['actual_value']}{m.get('target_unit', '')}"
                    if m.get("achievement_pct") is not None:
                        actual_str += f" ({m['achievement_pct']:.0f}% of target)"

                prob_str = f" [Probability: {m['probability']:.0%}]" if m.get("probability") else ""

                sections.append(
                    f"- **{m['date']}** {status_icon} {m['type']}: "
                    f"{m['description']}{target_str}{actual_str}{prob_str}"
                )

            sections.append("")  # blank line

            if c.scorecard.get("execution_score") is not None:
                sc = c.scorecard
                sections.append(f"""### Execution Scorecard
- Total milestones: {sc['total_milestones']}
- Achieved: {sc['achieved']} | Ahead: {sc['ahead_of_schedule']} | Missed/Behind: {sc['missed_or_behind']} | Pending: {sc['pending']}
- Average achievement: {sc.get('avg_achievement_pct', 'N/A')}%
- **Execution Score: {sc['execution_score']:.0f}/100 ({sc['assessment']})**
""")

        if c.milestone_value_adjustment != 0:
            sections.append(
                f"**Probability-weighted milestone value adjustment:** "
                f"${c.milestone_value_adjustment:+.2f}/share"
            )
            sections.append(
                f"**Adjusted intrinsic value:** ${c.adjusted_intrinsic_low or 0:.2f} — "
                f"${c.adjusted_intrinsic_mid or 0:.2f} — ${c.adjusted_intrinsic_high or 0:.2f}"
            )
            sections.append("")

        # Derivatives section
        if c.derivatives:
            d = c.derivatives
            sections.append(f"""## Derivatives & Short Positioning

### Short Interest
- Short float: {d.short.short_float_pct or 'N/A'}%
- Shares short: {d.short.short_interest or 'N/A'}
- Days to cover: {d.short.days_to_cover or 'N/A'}
- Short change (period): {d.short.short_change_pct or 'N/A'}%
- Borrow rate: {d.short.borrow_rate or 'N/A'}%

### Dealer Gamma Exposure
- GEX Regime: **{d.dealer.gex_regime or 'N/A'}**
- GEX Aggregate: {d.dealer.gex_aggregate or 'N/A'}
- Gamma Flip Point: ${d.dealer.gamma_flip or 0:.2f}
- Gamma Wall (resistance): ${d.dealer.gamma_wall or 0:.2f}
- Put Wall (support): ${d.dealer.put_wall or 0:.2f}
- Call Wall (ceiling): ${d.dealer.call_wall or 0:.2f}
- Vanna Exposure: {d.dealer.vanna_exposure or 'N/A'}
- Charm Exposure: {d.dealer.charm_exposure or 'N/A'}

### Options Sentiment
- Put/Call Ratio: {d.options.put_call_ratio or 'N/A'}
- IV Skew: {d.options.iv_skew or 'N/A'}
- IV Percentile: {d.options.iv_percentile or 'N/A'}
- Max Pain: ${d.options.max_pain or 0:.2f}
- Distance from Max Pain: {d.options.max_pain_dist_pct or 'N/A'}%

### Composite Scores
| Component | Score (0-100) |
|-----------|--------------|
| Short Pressure | {d.short_pressure_score:.0f} |
| Gamma Support | {d.gamma_support_score:.0f} |
| Options Sentiment | {d.options_sentiment_score:.0f} |
| **Total Derivatives Support** | **{d.derivatives_support_score:.0f}** |

**Support Regime:** {d.support_regime}
**Narrative:** {d.narrative}
""")

        # Assessment
        sections.append(f"""## Current Assessment

- **Price vs Value:** {c.price_vs_value}
- **Derivatives Alignment:** {c.derivatives_alignment}
- **Overall:** {c.overall_assessment}
""")

        # Instructions for predictions
        sections.append(f"""## Required Output Format

Provide your analysis, then end with structured predictions in this exact format:

```json
{{
  "analysis_id": "{analysis_id}",
  "ticker": "{c.ticker}",
  "analysis_date": "{c.analysis_date}",
  "fair_value_estimate": <your_estimate>,
  "predictions": [
    {{
      "metric": "price",
      "target": <target_value>,
      "direction": "up|down|flat",
      "timeframe": "1w|1m|3m|6m|1y",
      "confidence": <0.0-1.0>,
      "rationale": "<brief>"
    }},
    {{
      "metric": "earnings_beat|revenue_beat|milestone_achieved",
      "target": <expected_value>,
      "timeframe": "<next_earnings_date_or_period>",
      "confidence": <0.0-1.0>,
      "rationale": "<brief>"
    }}
  ],
  "risk_factors": ["<risk1>", "<risk2>"],
  "derivatives_read": "<one_sentence_summary>",
  "execution_grade": "A|B|C|D|F"
}}
```

Be honest. If the data is insufficient for a prediction, say so. Prefer fewer high-confidence predictions over many low-confidence ones.
""")

        return "\n".join(sections)
