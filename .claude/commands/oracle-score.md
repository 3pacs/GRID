# Oracle Score

Score Oracle predictions against actual outcomes and generate calibration report.

## Instructions

1. **Check pending predictions:**
   ```python
   from db import get_engine
   from sqlalchemy import text
   engine = get_engine()
   with engine.connect() as conn:
       r = conn.execute(text(
           "SELECT COUNT(*), MIN(expiry_date), MAX(expiry_date) "
           "FROM oracle_predictions WHERE outcome IS NULL AND expiry_date <= CURRENT_DATE"
       )).fetchone()
       print(f"Scoreable: {r[0]}, range: {r[1]} → {r[2]}")
   ```

2. **Run scoring:**
   ```python
   from oracle.engine import OracleEngine
   oracle = OracleEngine(engine)
   scored = oracle.score_expired()
   ```

3. **Generate calibration report:**
   ```python
   from oracle.calibration import CalibrationReport
   report = CalibrationReport(engine)
   metrics = report.compute()
   # Brier score, ECE, reliability diagram
   ```

4. **Interpret results:**
   - Brier score < 0.25 = good calibration
   - ECE < 0.10 = well-calibrated probabilities
   - Per-model accuracy determines weight evolution
   - Models below 40% accuracy get weight reduced

## Key dates
- Scoring starts: April 5, 2026
- 10,893 predictions pending
- 5 competing models, all at weight=1.0 (pre-scoring)

## Source files
- `oracle/engine.py` — 5-model ensemble, signal weighting
- `oracle/calibration.py` — Brier score, ECE, reliability
- `oracle/report.py` — email digest after each cycle
