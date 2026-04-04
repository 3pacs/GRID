# Task: Vol-Regime Energy Sector Signal — LLM-Reasoned

## Philosophy

This is NOT a traditional quant model. The quantitative data (prices, vol, macro)
provides **context and regime detection**. The LLM's job is to **reason about
what the data means** — interpret signals, weigh conflicting indicators, and
produce a conviction-based BUY/NO_BUY call.

The LLM advantage: you can read a credit spread widening alongside rising oil
and reason "energy companies with strong balance sheets benefit from rising oil
even in tightening credit" — something no sklearn model can do.

## Objective

Build a **volatility-regime-gated energy sector signal** for EOG Resources.

The strategy has two layers:
1. **Regime Layer**: Classify the current vol/credit environment
2. **Signal Layer**: Within each regime, reason about whether EOG is a BUY

## Data Access

GRID's PostgreSQL database is available via the `grid_bridge` module:

```python
from grid_bridge import GridBridge
bridge = GridBridge()
```

### Methods
- `bridge.get_eog_prices()` → DataFrame [obs_date, close]
- `bridge.get_features(names)` → DataFrame (obs_date index, one column per feature)
- `bridge.get_available_features()` → list of (name, family, description, row_count)

### Key Data for Vol-Regime Energy Strategy

**Regime Indicators (use these to classify market environment):**
- `vix_spot` — VIX index (18,303 rows)
- `ofr_financial_stress` — OFR financial stress (13,155 rows)
- `hyg_full` / `lqd_full` — Credit spread proxy (14k/15k rows)
- `tlt_full` — Rate expectations (25k rows)

**Energy Sector Data:**
- `eog_full` — EOG price (13,672 rows)
- `xle_full` — Energy sector ETF (26,976 rows)
- `cl_close` — Crude oil (12,856 rows)
- `uso_full` — Oil ETF (17,184 rows)
- `dvn_full` — Devon Energy peer (15,798 rows)

**Macro Context:**
- `gld_full` — Gold/risk proxy (22,563 rows)

## Requirements

### 1. Write `/app/signal_generator.py` that:

**Step A — Regime Classification:**
- Compute VIX regime: LOW (<15), NORMAL (15-25), HIGH (25-35), CRISIS (>35)
- Compute credit regime from HYG/LQD ratio z-score
- Combine into composite regime: RISK_ON, NEUTRAL, RISK_OFF, CRISIS

**Step B — Feature Context Assembly:**
For each trading day, assemble a context snapshot:
- Oil price momentum (21d, 63d)
- EOG relative strength vs XLE sector
- Credit spread direction (tightening/widening)
- VIX term structure (if available) or level
- EOG price vs 50d/200d moving averages

**Step C — Signal Generation:**
For each day in the walk-forward test window:
- Classify the regime
- Assess the feature context
- Apply regime-gated logic:
  - In RISK_ON: BUY when oil momentum is positive AND EOG is below 63d mean
  - In NEUTRAL: BUY when credit is tightening AND oil is stable/rising
  - In RISK_OFF: BUY only on extreme oversold (z-score < -2)
  - In CRISIS: NO_BUY (capital preservation)
- Set confidence based on how many indicators align

**Step D — Walk-Forward Output:**
- Train window: 504+ trading days
- Test window: 63 trading days, rolling
- Output predictions.csv: obs_date, signal, confidence, predicted_return
- Minimum 4 walk-forward windows

### 2. Output Format

```csv
obs_date,signal,confidence,predicted_return
2010-01-04,BUY,0.72,0.045
2010-01-05,NO_BUY,0.35,-0.012
```

### 3. Scoring (what the test evaluates)
- **Sharpe ratio** of BUY-gated returns (40% weight)
- **Hit rate** of BUY signals (25% weight)
- **Max drawdown** during BUY periods (15% weight)
- **Information coefficient** — correlation of confidence with returns (10%)
- **Parsimony** — fewer features is better (10%)

## Constraints
- All features lagged by 1+ days (no look-ahead)
- Walk-forward only (no future data in training)
- Max 30 features
- Handle missing data (forward-fill then drop)

## Step E — Supply Chain News Reasoning (THE EDGE)

This is what separates this system from every other quant model. The LLM
must **read news headlines and reason about supply chain implications** for
the energy sector.

### Available News Methods
- `bridge.get_energy_news_context(days=7)` → energy-specific headlines
  (oil, OPEC, pipeline, shale, Permian, sanctions, shipping, freight)
- `bridge.get_news_headlines(hours=72, ticker="EOG")` → EOG-specific news
- `bridge.get_supply_chain_data()` → shipping rates, container indices,
  manufacturing, trade balance
- `bridge.get_gdelt_tone()` → global event tone and conflict data

### Supply Chain Reasoning Chain (example)

The LLM should build reasoning chains like:
1. "OPEC announces production cut" → tighter global supply → oil price support
2. "Permian Basin pipeline capacity expanding" → EOG can ship more → volume upside
3. "Houthi attacks on Red Sea shipping" → freight rates spike → energy supply
   disruption → short-term bullish energy but long-term demand destruction risk
4. "Russia sanctions tightened" → less Russian oil on market → Brent premium
   → US shale producers (like EOG) benefit as marginal supplier
5. "China demand slowing" → crude demand down → bearish energy despite supply

### How to Use News in the Signal

For each walk-forward test window, if news data is available:
1. Pull recent energy news via `get_energy_news_context()`
2. Have the LLM classify each headline's supply chain implication:
   - SUPPLY_TIGHTENING, SUPPLY_EXPANDING, DEMAND_GROWING, DEMAND_SHRINKING,
     GEOPOLITICAL_RISK, INFRASTRUCTURE, REGULATORY, NEUTRAL
3. Aggregate: net supply/demand balance from news
4. Combine with the vol-regime quant signal:
   - If quant says BUY and news confirms supply tightening → HIGH confidence
   - If quant says BUY but news says demand destruction → LOWER confidence
   - If quant says NO_BUY but massive supply shock in news → override to BUY

The news reasoning layer should be additive — it improves signal quality
when news is available, but the strategy still works on quant-only data
for historical periods where news isn't in the DB.

## Key Insight

The vol regime gate prevents you from being long energy during crises.
The supply chain news reasoning tells you *why* to be long within a
favorable regime. Together: regime filter (avoid losses) + news reasoning
(pick entries) = alpha.

This is a single-sector, regime-aware, LLM-reasoned strategy — not a
stock-picking model and not a dumb quant screen.
