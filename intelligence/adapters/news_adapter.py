"""GRID Signal Adapter — News Intel. Sentiment momentum + volume signals per ticker."""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.adapters.base import BaseAdapter, clamp, sid
from intelligence.signal_registry import RegisteredSignal, SignalType

_SRC = "news_intel"
_REFRESH = 6.0


class NewsAdapter(BaseAdapter):
    SOURCE_MODULE = _SRC
    REFRESH_HOURS = _REFRESH
    LOG_NAME = "news_adapter"

    def _build_signals(self, engine: Engine, now: datetime) -> list[RegisteredSignal]:
        vu = now + timedelta(hours=6)
        signals: list[RegisteredSignal] = []
        lb = now - timedelta(days=3)
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT unnest(tickers) as ticker, COUNT(*) as vol,
                       AVG(CASE WHEN sentiment='BULLISH' THEN 1 WHEN sentiment='BEARISH' THEN -1 ELSE 0 END) as avg_dir,
                       AVG(confidence) as avg_strength
                FROM news_articles
                WHERE published_at >= :lb AND tickers IS NOT NULL AND array_length(tickers,1) > 0
                GROUP BY unnest(tickers) HAVING COUNT(*) >= 3
                ORDER BY COUNT(*) DESC LIMIT 100
            """), {"lb": lb}).fetchall()

        for ticker, vol, avg_dir, avg_str in rows:
            signals.append(RegisteredSignal(
                signal_id=sid(_SRC, "volume", ticker, str(now.date())),
                source_module=_SRC, signal_type=SignalType.MAGNITUDE,
                ticker=ticker, direction="neutral", value=float(vol),
                z_score=None, confidence=clamp(min(float(vol) / 20, 1.0)),
                valid_from=now, valid_until=vu, freshness_hours=0.0,
                metadata={"article_count": int(vol), "window_days": 3},
                provenance=f"news_articles:volume:{ticker}",
            ))
            # 2026-04-28: sentiment polarity is NOT a directional bet.
            # Postmortem: news_intel signals showed up in 14+ wrong predictions
            # with zero positive-EV evidence. Bullish news in a bear market gets
            # sold; bearish news in a rally gets bought. Causality between news
            # tone and next-day price is weak and regime-dependent. Publish as
            # NEUTRAL — the avg_dir value is preserved in the payload so
            # trace_evolver can learn nonlinear context, but it stops voting
            # directionally on its own.
            if abs(float(avg_dir or 0)) > 0.2:
                signals.append(RegisteredSignal(
                    signal_id=sid(_SRC, "sentiment", ticker, str(now.date())),
                    source_module=_SRC, signal_type=SignalType.MAGNITUDE,
                    ticker=ticker, direction="neutral",
                    value=round(float(avg_dir), 4),
                    z_score=round(float(avg_dir) * 2, 2),
                    confidence=clamp(float(avg_str or 0.5)),
                    valid_from=now, valid_until=vu, freshness_hours=0.0,
                    metadata={"avg_direction": round(float(avg_dir), 4), "volume": int(vol)},
                    provenance=f"news_articles:sentiment:{ticker}",
                ))

        # News impact catalysts (if any) — swallow failures, table may not exist yet.
        try:
            with engine.connect() as conn:
                catalysts = conn.execute(text("""
                    SELECT ticker, catalyst_type, direction, confidence
                    FROM news_impact_catalysts
                    WHERE created_at >= :lb ORDER BY confidence DESC LIMIT 50
                """), {"lb": now - timedelta(days=2)}).fetchall()
            for ticker, cat_type, direction, conf in catalysts:
                if not ticker or not direction:
                    continue
                signals.append(RegisteredSignal(
                    signal_id=sid(_SRC, "catalyst", ticker, cat_type or "", str(now.date())),
                    source_module=_SRC, signal_type=SignalType.NARRATIVE,
                    ticker=ticker, direction=direction.lower() if direction else "neutral",
                    value=1.0, z_score=None, confidence=clamp(float(conf or 0.5)),
                    valid_from=now, valid_until=vu, freshness_hours=0.0,
                    metadata={"catalyst_type": cat_type},
                    provenance=f"news_impact:catalyst:{ticker}",
                ))
        except Exception:
            pass  # Table may not have data yet
        return signals
