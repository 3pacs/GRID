#!/usr/bin/env python3
"""Baseline Prediction Comparison Engine.

Proves GRID intelligence adds alpha over raw LLM predictions.

How it works:
    1. Pick a set of tickers/assets
    2. Generate predictions from EACH model with TWO prompts:
       a) NAKED: "What will {ticker} do in the next 7 days?" (no GRID data)
       b) AUGMENTED: Same question + GRID intelligence context (signals, actors,
          dealer gamma, regime, trust scores, cross-reference data)
    3. Score both against actual outcomes using identical criteria
    4. Compare: if GRID-augmented consistently beats naked, the intelligence
       has measurable alpha

Models tested:
    - Qwen 32B (local, free — our workhorse)
    - GPT-4o (via API if key available)
    - Claude Sonnet (via API if key available)
    - Gemini Pro (via API if key available)
    - Llama 3 (local if available)
    - Mistral (local if available)

The table `baseline_predictions` stores ALL predictions with:
    - model_name, prompt_type (naked/augmented), ticker, direction,
      confidence, reasoning, entry_price, created_at
    - Scored later by score_baseline_predictions.py against actual prices
"""
import os
import sys
import json
import time
import hashlib
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env file if it exists
_env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

from db import get_engine
from sqlalchemy import text
from loguru import logger as log

engine = get_engine()

# ── Table Setup ──────────────────────────────────────────────────────

def ensure_tables():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS baseline_predictions (
                id              BIGSERIAL PRIMARY KEY,
                prediction_id   TEXT NOT NULL UNIQUE,
                model_name      TEXT NOT NULL,
                prompt_type     TEXT NOT NULL,
                ticker          TEXT NOT NULL,
                direction       TEXT NOT NULL,
                confidence      DOUBLE PRECISION DEFAULT 0.5,
                target_price    DOUBLE PRECISION,
                stop_price      DOUBLE PRECISION,
                timeframe_days  INTEGER DEFAULT 7,
                reasoning       TEXT DEFAULT '',
                lever           TEXT DEFAULT '',
                condition       TEXT DEFAULT '',
                entry_price     DOUBLE PRECISION,
                exit_price      DOUBLE PRECISION,
                verdict         TEXT DEFAULT 'pending',
                pnl_pct         DOUBLE PRECISION,
                scored_at       TIMESTAMPTZ,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_baseline_model
                ON baseline_predictions(model_name, prompt_type, verdict);
            CREATE INDEX IF NOT EXISTS idx_baseline_ticker
                ON baseline_predictions(ticker, created_at);
        """))
    log.info("baseline_predictions table ready")


# ── Price Fetching ───────────────────────────────────────────────────

def get_crypto_price(symbol: str) -> float | None:
    """Get live price from Crypto.com Exchange API.

    The bulk tickers endpoint uses short field names:
        i=instrument, a=last_price, b=best_bid, k=best_ask, h=high, l=low
    Instruments are {COIN}_USD (spot) or {COIN}USD-PERP (futures).
    """
    targets = [f"{symbol.upper()}_USD", f"{symbol.upper()}_USDT"]
    try:
        resp = requests.get(
            "https://api.crypto.com/exchange/v1/public/get-tickers",
            timeout=15,
        )
        all_tickers = resp.json().get("result", {}).get("data", [])
        for t in all_tickers:
            if t.get("i") in targets:
                price = t.get("a") or t.get("b")
                if price:
                    return float(price)
        return None
    except Exception:
        return None


def get_stock_price(symbol: str) -> float | None:
    """Get stock price from Yahoo Finance (free, no API key)."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=10)
        data = resp.json()
        price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
        return float(price)
    except Exception:
        return None


def get_price(ticker: str) -> float | None:
    """Try crypto first, then stock."""
    p = get_crypto_price(ticker)
    if p and p > 0:
        return p
    return get_stock_price(ticker)


# ── GRID Intelligence Context ───────────────────────────────────────

def get_grid_context(ticker: str) -> str:
    """Pull all GRID intelligence for a ticker to augment predictions.

    Reuses a single connection for all sub-queries instead of opening a new
    connection per query (was 6 separate engine.connect() calls implicitly).
    """
    context_parts = []

    with engine.connect() as conn:
        # Active signals
        try:
            signals = conn.execute(text("""
                SELECT signal_type, direction, confidence, source, created_at
                FROM signals WHERE ticker = :t
                AND created_at > NOW() - INTERVAL '7 days'
                ORDER BY created_at DESC LIMIT 10
            """), {"t": ticker}).fetchall()
            if signals:
                context_parts.append("RECENT SIGNALS:")
                for s in signals:
                    context_parts.append(
                        f"  - {s[0]} {s[1]} (conf={s[2]:.0%}) from {s[3]} @ {s[4]}"
                    )
        except Exception as exc:
            log.debug("BaselinePred: signals query failed for {t}: {e}", t=ticker, e=str(exc))

        # Dealer gamma / options data
        try:
            gamma = conn.execute(text("""
                SELECT category, ticker, data
                FROM analytical_snapshots
                WHERE ticker = :t AND category LIKE '%gamma%'
                ORDER BY created_at DESC LIMIT 1
            """), {"t": ticker}).fetchall()
            if gamma:
                context_parts.append(f"DEALER GAMMA: {gamma[0][2][:200] if gamma[0][2] else 'N/A'}")
        except Exception as exc:
            log.debug("BaselinePred: dealer gamma query failed for {t}: {e}", t=ticker, e=str(exc))

        # Regime
        try:
            regime = conn.execute(text("""
                SELECT label, confidence
                FROM regime_history
                ORDER BY detected_at DESC LIMIT 1
            """)).fetchone()
            if regime:
                context_parts.append(f"CURRENT REGIME: {regime[0]} (conf={regime[1]:.0%})")
        except Exception as exc:
            log.debug("BaselinePred: regime query failed: {e}", e=str(exc))

        # Recent predictions for this ticker
        try:
            preds = conn.execute(text("""
                SELECT call, setup, verdict, pnl_pct
                FROM oracle_predictions
                WHERE ticker = :t
                ORDER BY created_at DESC LIMIT 5
            """), {"t": ticker}).fetchall()
            if preds:
                context_parts.append("RECENT ORACLE PREDICTIONS:")
                for p in preds:
                    v = p[2] or "pending"
                    pnl = f" ({p[3]:+.1%})" if p[3] else ""
                    context_parts.append(f"  - {p[0][:80]} → {v}{pnl}")
        except Exception as exc:
            log.debug("BaselinePred: predictions history query failed for {t}: {e}", t=ticker, e=str(exc))

        # Actor/insider signals
        try:
            insider = conn.execute(text("""
                SELECT category, data
                FROM analytical_snapshots
                WHERE ticker = :t AND category IN ('insider_filing', 'congressional_trade', 'dark_pool')
                AND created_at > NOW() - INTERVAL '30 days'
                ORDER BY created_at DESC LIMIT 5
            """), {"t": ticker}).fetchall()
            if insider:
                context_parts.append("INSIDER/INSTITUTIONAL ACTIVITY:")
                for i in insider:
                    context_parts.append(f"  - [{i[0]}] {str(i[1])[:150]}")
        except Exception as exc:
            log.debug("BaselinePred: insider activity query failed for {t}: {e}", t=ticker, e=str(exc))

        # Trust-scored social signals
        try:
            social = conn.execute(text("""
                SELECT source, direction, confidence
                FROM signals WHERE ticker = :t AND signal_type = 'social'
                AND created_at > NOW() - INTERVAL '7 days'
                ORDER BY confidence DESC LIMIT 5
            """), {"t": ticker}).fetchall()
            if social:
                context_parts.append("SOCIAL SIGNALS (trust-scored):")
                for s in social:
                    context_parts.append(f"  - {s[0]}: {s[1]} (trust={s[2]:.0%})")
        except Exception as exc:
            log.debug("BaselinePred: social signals query failed for {t}: {e}", t=ticker, e=str(exc))

    if not context_parts:
        return "(No GRID intelligence available for this ticker)"

    return "\n".join(context_parts)


# ── Prompt Templates ─────────────────────────────────────────────────

NAKED_PROMPT = """You are a financial analyst. Based on your general knowledge:

What will {ticker} do in the next {days} days?

Respond in this EXACT format:
DIRECTION: [bullish/bearish/neutral]
CONFIDENCE: [0.0-1.0]
TARGET: [price target]
STOP: [stop loss price]
TIMEFRAME: {days} days
LEVER: [what specific event/action/actor is causing this move - name the valve]
CONDITION: [what environmental factor amplifies/dampens the lever]
REASONING: [2-3 sentences explaining your thesis]

Current price: ${price}
"""

AUGMENTED_PROMPT = """You are a financial analyst with access to GRID intelligence data.

Based on the following GRID intelligence AND your general knowledge:

{context}

What will {ticker} do in the next {days} days?

Respond in this EXACT format:
DIRECTION: [bullish/bearish/neutral]
CONFIDENCE: [0.0-1.0]
TARGET: [price target]
STOP: [stop loss price]
TIMEFRAME: {days} days
LEVER: [what specific event/action/actor is causing this move - name the valve]
CONDITION: [what environmental factor amplifies/dampens the lever]
REASONING: [2-3 sentences explaining your thesis, referencing the GRID data]

Current price: ${price}
"""


# ── LLM Backends ─────────────────────────────────────────────────────

def query_ollama(prompt: str, model: str = "qwen2.5:7b") -> str | None:
    """Query Ollama for local models."""
    try:
        resp = requests.post(
            "http://localhost:11434/api/generate",
            json={"model": model, "prompt": prompt, "stream": False,
                  "options": {"temperature": 0.3, "num_predict": 400}},
            timeout=180,
        )
        return resp.json().get("response", "").strip()
    except Exception:
        return None


def query_llamacpp(prompt: str) -> str | None:
    """Query llama.cpp server (Qwen 32B)."""
    from config import settings
    try:
        resp = requests.post(
            f"{settings.LLAMACPP_BASE_URL}/completion",
            json={"prompt": prompt, "n_predict": 400, "temperature": 0.3, "stop": ["\n\n\n"]},
            timeout=180,
        )
        return resp.json().get("content", "").strip()
    except Exception:
        return None


def query_openai(prompt: str, model: str = "gpt-4o") -> str | None:
    """Query OpenAI API (requires OPENAI_API_KEY)."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={"model": model, "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.3, "max_tokens": 400},
            timeout=30,
        )
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception:
        return None


def query_anthropic(prompt: str, model: str = "claude-sonnet-4-20250514") -> str | None:
    """Query Anthropic API (requires ANTHROPIC_API_KEY)."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": model, "max_tokens": 400,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=30,
        )
        return resp.json()["content"][0]["text"].strip()
    except Exception:
        return None


def query_gemini(prompt: str) -> str | None:
    """Query Google Gemini API (requires GEMINI_API_KEY)."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return None
    try:
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}",
            json={"contents": [{"parts": [{"text": prompt}]}],
                  "generationConfig": {"temperature": 0.3, "maxOutputTokens": 400}},
            timeout=30,
        )
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        return None


def query_groq(prompt: str, model: str = "llama-3.3-70b-versatile") -> str | None:
    """Query Groq API — free tier, blazing fast inference."""
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return None
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={"model": model, "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.3, "max_tokens": 400},
            timeout=30,
        )
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception:
        return None


def query_openrouter(prompt: str, model: str = "meta-llama/llama-3.1-70b-instruct") -> str | None:
    """Query OpenRouter — aggregated access to many models."""
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        return None
    try:
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={"model": model, "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.3, "max_tokens": 400},
            timeout=30,
        )
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception:
        return None


# Model registry: name -> (query_fn, requires_api_key)
# Order matters — llamacpp first since it has VRAM allocated already
MODELS = {
    # Local models (free, no API key)
    "qwen-32b-llamacpp": (query_llamacpp, False),
    # Cloud models (require API keys — test all that have keys)
    "groq-llama70b": (lambda p: query_groq(p, "llama-3.3-70b-versatile"), True),
    "groq-qwen32b": (lambda p: query_groq(p, "qwen/qwen3-32b"), True),
    "groq-llama8b": (lambda p: query_groq(p, "llama-3.1-8b-instant"), True),
    "gemini-2.5-flash": (query_gemini, True),
    "openrouter-llama70b": (lambda p: query_openrouter(p, "meta-llama/llama-3.1-70b-instruct"), True),
    "openrouter-mistral": (lambda p: query_openrouter(p, "mistralai/mistral-7b-instruct"), True),
    # OpenAI — gpt-4o is expensive, keep disabled. Mini is cheap ($0.15/1M tokens).
    # "gpt-4o": (lambda p: query_openai(p, "gpt-4o"), True),
    "gpt-4o-mini": (lambda p: query_openai(p, "gpt-4o-mini"), True),
    # Claude — uses your subscription via subagents instead, not API.
    # "claude-sonnet": (query_anthropic, True),
    # Ollama models — try last since GPU may be full from llamacpp
    "qwen2.5-7b": (lambda p: query_ollama(p, "qwen2.5:7b"), False),
    "llama3.1-8b": (lambda p: query_ollama(p, "llama3.1:8b"), False),
    "llama3.2-3b": (lambda p: query_ollama(p, "llama3.2:latest"), False),
}


# ── Parse LLM Response ──────────────────────────────────────────────

def parse_prediction(text: str) -> dict:
    """Parse structured prediction from LLM response."""
    result = {
        "direction": "neutral",
        "confidence": 0.5,
        "target": None,
        "stop": None,
        "lever": "",
        "condition": "",
        "reasoning": "",
    }

    for line in text.split("\n"):
        line = line.strip()
        upper = line.upper()

        if upper.startswith("DIRECTION:"):
            val = line.split(":", 1)[1].strip().lower()
            if "bull" in val:
                result["direction"] = "bullish"
            elif "bear" in val:
                result["direction"] = "bearish"
            else:
                result["direction"] = "neutral"

        elif upper.startswith("CONFIDENCE:"):
            try:
                val = line.split(":", 1)[1].strip().replace("%", "")
                c = float(val)
                result["confidence"] = c if c <= 1.0 else c / 100.0
            except ValueError:
                pass

        elif upper.startswith("TARGET:"):
            try:
                val = line.split(":", 1)[1].strip().replace("$", "").replace(",", "")
                result["target"] = float(val)
            except ValueError:
                pass

        elif upper.startswith("STOP:"):
            try:
                val = line.split(":", 1)[1].strip().replace("$", "").replace(",", "")
                result["stop"] = float(val)
            except ValueError:
                pass

        elif upper.startswith("LEVER:"):
            result["lever"] = line.split(":", 1)[1].strip()

        elif upper.startswith("CONDITION:"):
            result["condition"] = line.split(":", 1)[1].strip()

        elif upper.startswith("REASONING:"):
            result["reasoning"] = line.split(":", 1)[1].strip()

    # If no structured reasoning, use the whole response
    if not result["reasoning"]:
        result["reasoning"] = text[:500]

    return result


# ── Main Prediction Loop ────────────────────────────────────────────

TICKERS = ["BTC", "ETH", "SOL"]  # Start with 24/7 crypto for fast scoring
TIMEFRAME_DAYS = 3  # Short timeframe for fast feedback


def generate_predictions():
    """Generate naked + augmented predictions from all available models."""
    ensure_tables()

    # Get current prices
    prices = {}
    for ticker in TICKERS:
        p = get_price(ticker)
        if p:
            prices[ticker] = p
            print(f"  {ticker}: ${p:,.2f}")

    if not prices:
        print("Failed to fetch prices")
        return

    # Test which models are available
    available_models = {}
    for name, (query_fn, needs_key) in MODELS.items():
        if needs_key:
            # Check if API key exists
            key_map = {
                "gpt-4o": "OPENAI_API_KEY", "gpt-4o-mini": "OPENAI_API_KEY",
                "claude-sonnet": "ANTHROPIC_API_KEY",
                "gemini-2.5-flash": "GEMINI_API_KEY",
                "groq-llama70b": "GROQ_API_KEY",
                "groq-qwen32b": "GROQ_API_KEY",
                "groq-llama8b": "GROQ_API_KEY",
                "openrouter-llama70b": "OPENROUTER_API_KEY",
                "openrouter-mistral": "OPENROUTER_API_KEY",
            }
            key_name = key_map.get(name, "")
            if key_name and os.getenv(key_name):
                available_models[name] = query_fn
                print(f"  Model {name}: available (API key found)")
            else:
                print(f"  Model {name}: skipped (no {key_name})")
        else:
            # Test local model with a ping
            result = query_fn("Say 'ok'")
            if result:
                available_models[name] = query_fn
                print(f"  Model {name}: available (local)")
            else:
                print(f"  Model {name}: skipped (not responding)")

    if not available_models:
        print("No models available!")
        return

    print(f"\n{len(available_models)} models × {len(prices)} tickers × 2 prompts "
          f"= {len(available_models) * len(prices) * 2} predictions\n")

    # ── Pre-batch existence check ────────────────────────────────────────
    # Build all candidate prediction IDs upfront and fetch existing ones in
    # a single query instead of one SELECT per candidate (was N round-trips).
    today_str = str(datetime.now(timezone.utc).date())
    all_pred_ids = {
        hashlib.sha256(
            f"{model_name}:{prompt_type}:{ticker}:{today_str}".encode()
        ).hexdigest()[:16]
        for model_name in available_models
        for ticker in prices
        for prompt_type in ["naked", "augmented"]
    }
    with engine.connect() as conn:
        existing_rows = conn.execute(
            text("SELECT prediction_id FROM baseline_predictions WHERE prediction_id = ANY(:ids)"),
            {"ids": list(all_pred_ids)},
        ).fetchall()
    already_done: set[str] = {r[0] for r in existing_rows}

    # ── Build work items ─────────────────────────────────────────────────
    # Pre-fetch all grid contexts (one per ticker) before spawning threads.
    grid_contexts = {ticker: get_grid_context(ticker) for ticker in prices}

    work_items = []  # list of (model_name, query_fn, ticker, price, prompt_type, prompt, pred_id)
    for ticker, price in prices.items():
        grid_context = grid_contexts[ticker]
        for model_name, query_fn in available_models.items():
            for prompt_type in ["naked", "augmented"]:
                pred_id = hashlib.sha256(
                    f"{model_name}:{prompt_type}:{ticker}:{today_str}".encode()
                ).hexdigest()[:16]

                if pred_id in already_done:
                    print(f"  [{model_name}:{prompt_type}:{ticker}] already exists, skipping")
                    continue

                if prompt_type == "naked":
                    prompt = NAKED_PROMPT.format(
                        ticker=ticker, days=TIMEFRAME_DAYS, price=f"{price:,.2f}"
                    )
                else:
                    prompt = AUGMENTED_PROMPT.format(
                        ticker=ticker, days=TIMEFRAME_DAYS, price=f"{price:,.2f}",
                        context=grid_context,
                    )
                work_items.append((model_name, query_fn, ticker, price, prompt_type, prompt, pred_id))

    def _run_one(item):
        """Query one model and return (pred_id, model_name, prompt_type, ticker,
        price, pred_dict) or None on failure."""
        model_name, query_fn, ticker, price, prompt_type, prompt, pred_id = item
        t_start = time.time()
        response = query_fn(prompt)
        elapsed = time.time() - t_start
        if not response:
            print(f"  [{model_name}:{prompt_type}:{ticker}] FAILED ({elapsed:.1f}s)")
            return None
        pred = parse_prediction(response)
        print(f"  [{model_name}:{prompt_type}:{ticker}] {pred['direction']} "
              f"conf={pred['confidence']:.0%} ({elapsed:.1f}s)")
        return (pred_id, model_name, prompt_type, ticker, price, pred)

    # ── Parallel execution ───────────────────────────────────────────────
    # Local models (llamacpp / ollama) must serialize since they share GPU.
    # Cloud API models (groq, openai, gemini, openrouter) are I/O-bound and
    # can run concurrently. We use a modest thread pool — cloud APIs have
    # rate limits and we don't want to hammer them.
    local_models = {"qwen-32b-llamacpp", "qwen2.5-7b", "llama3.1-8b", "llama3.2-3b"}
    local_items = [w for w in work_items if w[0] in local_models]
    cloud_items = [w for w in work_items if w[0] not in local_models]

    results = []

    # Local: run sequentially to avoid GPU contention
    for item in local_items:
        r = _run_one(item)
        if r:
            results.append(r)

    # Cloud: run up to 5 in parallel (respect rate limits)
    if cloud_items:
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(_run_one, item) for item in cloud_items]
            for fut in as_completed(futures):
                try:
                    r = fut.result()
                    if r:
                        results.append(r)
                except Exception as exc:
                    print(f"  [parallel] task raised: {exc}")

    # ── Batch store results ──────────────────────────────────────────────
    total = 0
    for pred_id, model_name, prompt_type, ticker, price, pred in results:
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO baseline_predictions
                        (prediction_id, model_name, prompt_type, ticker,
                         direction, confidence, target_price, stop_price,
                         timeframe_days, reasoning, lever, condition,
                         entry_price)
                    VALUES (:pid, :model, :ptype, :ticker,
                            :dir, :conf, :target, :stop,
                            :days, :reason, :lever, :cond,
                            :entry)
                """), {
                    "pid": pred_id,
                    "model": model_name,
                    "ptype": prompt_type,
                    "ticker": ticker,
                    "dir": pred["direction"],
                    "conf": pred["confidence"],
                    "target": pred["target"],
                    "stop": pred["stop"],
                    "days": TIMEFRAME_DAYS,
                    "reason": pred["reasoning"],
                    "lever": pred["lever"],
                    "cond": pred["condition"],
                    "entry": price,
                })
            total += 1
        except Exception as exc:
            print(f"  [store] failed for {pred_id}: {exc}")

    print(f"\nGenerated {total} baseline predictions")


def show_comparison():
    """Show current baseline comparison results."""
    with engine.connect() as conn:
        # Overall comparison
        rows = conn.execute(text("""
            SELECT
                model_name,
                prompt_type,
                COUNT(*) as total,
                SUM(CASE WHEN verdict = 'hit' THEN 1 ELSE 0 END) as hits,
                SUM(CASE WHEN verdict = 'miss' THEN 1 ELSE 0 END) as misses,
                SUM(CASE WHEN verdict = 'partial' THEN 1 ELSE 0 END) as partials,
                SUM(CASE WHEN verdict = 'pending' THEN 1 ELSE 0 END) as pending,
                AVG(CASE WHEN verdict != 'pending' THEN pnl_pct END) as avg_pnl,
                AVG(confidence) as avg_conf
            FROM baseline_predictions
            GROUP BY model_name, prompt_type
            ORDER BY model_name, prompt_type
        """)).fetchall()

    if not rows:
        print("No baseline predictions yet. Run: python scripts/baseline_predictions.py generate")
        return

    print(f"\n{'='*80}")
    print(f"  GRID BASELINE COMPARISON — Does Intelligence Beat Raw LLM?")
    print(f"{'='*80}")
    print(f"\n  {'Model':<20} {'Type':<12} {'Total':>6} {'Hit':>5} {'Miss':>5} "
          f"{'Part':>5} {'Pend':>5} {'Acc':>7} {'AvgPnL':>8} {'Conf':>6}")
    print(f"  {'-'*20} {'-'*12} {'-'*6} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*7} {'-'*8} {'-'*6}")

    for row in rows:
        scored = (row[3] or 0) + (row[4] or 0) + (row[5] or 0)
        acc = ((row[3] or 0) + (row[5] or 0) * 0.5) / scored if scored > 0 else 0
        pnl = f"{row[7]:+.1%}" if row[7] else "N/A"
        print(f"  {row[0]:<20} {row[1]:<12} {row[2]:>6} {row[3] or 0:>5} "
              f"{row[4] or 0:>5} {row[5] or 0:>5} {row[6] or 0:>5} "
              f"{acc:>6.0%} {pnl:>8} {row[8] or 0:>5.0%}")

    # Summary: naked vs augmented
    print(f"\n  {'='*60}")
    print(f"  NAKED vs AUGMENTED (all models combined)")
    print(f"  {'='*60}")

    with engine.connect() as conn:
        summary = conn.execute(text("""
            SELECT
                prompt_type,
                COUNT(*) FILTER (WHERE verdict != 'pending') as scored,
                AVG(CASE WHEN verdict = 'hit' THEN 1.0
                         WHEN verdict = 'partial' THEN 0.5
                         ELSE 0.0 END)
                    FILTER (WHERE verdict != 'pending') as accuracy,
                AVG(pnl_pct) FILTER (WHERE verdict != 'pending') as avg_pnl
            FROM baseline_predictions
            GROUP BY prompt_type
        """)).fetchall()

    for row in summary:
        acc = f"{row[2]:.0%}" if row[2] else "N/A"
        pnl = f"{row[3]:+.2%}" if row[3] else "N/A"
        print(f"  {row[0]:<12} scored={row[1]:<5} accuracy={acc:<8} avg_pnl={pnl}")

    naked_acc = next((r[2] for r in summary if r[0] == "naked"), None)
    aug_acc = next((r[2] for r in summary if r[0] == "augmented"), None)
    if naked_acc and aug_acc:
        delta = aug_acc - naked_acc
        print(f"\n  GRID ALPHA: {delta:+.1%} accuracy improvement over naked LLM")
        if delta > 0:
            print(f"  → Intelligence adds value. Worth paying for.")
        elif delta == 0:
            print(f"  → No difference yet. Need more data or better intelligence.")
        else:
            print(f"  → Naked LLM is beating us. Intelligence needs work.")


def score_predictions():
    """Score pending baseline predictions against current prices."""
    with engine.connect() as conn:
        pending = conn.execute(text("""
            SELECT id, ticker, direction, entry_price, timeframe_days, created_at
            FROM baseline_predictions
            WHERE verdict = 'pending'
              AND created_at < NOW() - (timeframe_days || ' days')::INTERVAL
        """)).fetchall()

    if not pending:
        print("No predictions ready to score (need to wait for timeframe to expire)")
        return

    scored = 0
    for row in pending:
        pid, ticker, direction, entry_price, days, created_at = row
        current = get_price(ticker)
        if not current or not entry_price:
            continue

        pct = (current - entry_price) / entry_price

        if direction == "bullish":
            verdict = "hit" if pct > 0.01 else ("miss" if pct < -0.02 else "partial")
        elif direction == "bearish":
            verdict = "hit" if pct < -0.01 else ("miss" if pct > 0.02 else "partial")
        else:
            verdict = "hit" if abs(pct) < 0.02 else "miss"

        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE baseline_predictions
                SET verdict = :v, pnl_pct = :pnl, exit_price = :exit, scored_at = NOW()
                WHERE id = :id
            """), {"v": verdict, "pnl": pct, "exit": current, "id": pid})

        print(f"  {ticker} {direction} entry=${entry_price:,.0f} now=${current:,.0f} "
              f"({pct:+.1%}) → {verdict}")
        scored += 1

    print(f"\nScored {scored} predictions")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="GRID Baseline Prediction Comparison")
    parser.add_argument("command", choices=["generate", "score", "compare"],
                        help="generate=create predictions, score=check outcomes, compare=show results")
    args = parser.parse_args()

    if args.command == "generate":
        generate_predictions()
    elif args.command == "score":
        score_predictions()
    elif args.command == "compare":
        show_comparison()
