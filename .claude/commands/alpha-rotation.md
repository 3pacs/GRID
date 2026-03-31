# Alpha Rotation

Run the Adaptive Rotation Strategy and report current target allocation.

## Instructions

1. **Run rotation:**
   ```python
   from sqlalchemy import create_engine
   from config import settings
   from alpha_research.strategies.adaptive_rotation import run_rotation

   engine = create_engine(settings.DB_URL)
   result = run_rotation(engine)
   ```

2. **Report:**
   - Current regime (risk-on / neutral / risk-off)
   - Whether fast risk-off is active
   - Active groups and their Information Ratios
   - Target portfolio weights
   - Cash allocation
   - Any stopped tickers

3. **Regime logic:**
   - 26-week SPY trend + VIX z-score → regime label
   - Fast risk-off: 3-day SPY drawdown < -3% OR VIX z-score > 3.0 → 50% cash for 10 days
   - risk-on: max 2 groups, 0% cash floor
   - neutral: max 2 groups, 20% cash floor
   - risk-off: max 1 group, 50% cash floor

4. **Asset Groups:**
   - Growth Tech: AAPL, MSFT, NVDA, META, AMZN, GOOGL, TSLA
   - Real Assets: XOM, CVX, GLD, SLV, XLE
   - Defensive: TLT, XLU, XLV, XLB, XLI

5. **Risk controls:**
   - 5% absolute stop-loss from entry
   - 10% trailing stop from peak
   - 20-day cooldown after stop

## Source
FinRL-X (AI4Finance-Foundation) — only live-verified system: Sharpe 1.96, 5 months paper trading on Alpaca
