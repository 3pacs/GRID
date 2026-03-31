"""GRID Signal Adapter — News Intel. Sentiment momentum + volume signals per ticker."""

from __future__ import annotations
import hashlib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from intelligence.signal_registry import RegisteredSignal, SignalType

_SRC = "news_intel"
_REFRESH = 6.0

def _sid(*p): return hashlib.sha1(":".join(p).encode()).hexdigest()[:16]
def _now(): return datetime.now(timezone.utc)
def _clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))

class NewsAdapter:
    @property
    def source_module(self): return _SRC
    @property
    def refresh_interval_hours(self): return _REFRESH

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        now = _now()
        vu = now + timedelta(hours=6)
        signals = []
        try:
            # Aggregate sentiment per ticker from recent news
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
                # Volume signal
                signals.append(RegisteredSignal(
                    signal_id=_sid(_SRC, "volume", ticker, str(now.date())),
                    source_module=_SRC, signal_type=SignalType.MAGNITUDE,
                    ticker=ticker, direction="neutral", value=float(vol),
                    z_score=None, confidence=_clamp(min(float(vol) / 20, 1.0)),
                    valid_from=now, valid_until=vu, freshness_hours=0.0,
                    metadata={"article_count": int(vol), "window_days": 3},
                    provenance=f"news_articles:volume:{ticker}",
                ))
                # Directional signal
                if abs(float(avg_dir or 0)) > 0.2:
                    d = "bullish" if float(avg_dir) > 0 else "bearish"
                    signals.append(RegisteredSignal(
                        signal_id=_sid(_SRC, "sentiment", ticker, str(now.date())),
                        source_module=_SRC, signal_type=SignalType.DIRECTIONAL,
                        ticker=ticker, direction=d, value=round(float(avg_dir), 4),
                        z_score=round(float(avg_dir) * 2, 2),
                        confidence=_clamp(float(avg_str or 0.5)),
                        valid_from=now, valid_until=vu, freshness_hours=0.0,
                        metadata={"avg_direction": round(float(avg_dir), 4), "volume": int(vol)},
                        provenance=f"news_articles:sentiment:{ticker}",
                    ))

            # News impact catalysts (if any)
            try:
                with engine.connect() as conn:
                    catalysts = conn.execute(text("""
                        SELECT ticker, catalyst_type, direction, confidence
                        FROM news_impact_catalysts
                        WHERE created_at >= :lb ORDER BY confidence DESC LIMIT 50
                    """), {"lb": now - timedelta(days=2)}).fetchall()
                for ticker, cat_type, direction, conf in catalysts:
                    if not ticker or not direction: continue
                    signals.append(RegisteredSignal(
                        signal_id=_sid(_SRC, "catalyst", ticker, cat_type or "", str(now.date())),
                        source_module=_SRC, signal_type=SignalType.NARRATIVE,
                        ticker=ticker, direction=direction.lower() if direction else "neutral",
                        value=1.0, z_score=None, confidence=_clamp(float(conf or 0.5)),
                        valid_from=now, valid_until=vu, freshness_hours=0.0,
                        metadata={"catalyst_type": cat_type},
                        provenance=f"news_impact:catalyst:{ticker}",
                    ))
            except Exception:
                pass  # Table may not have data yet
        except Exception as e:
            log.error("news_adapter: {e}", e=e)
        log.info("news_adapter: {n} signals", n=len(signals))
        return signals
