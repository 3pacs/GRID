# GRID Algorithm Brief — For Codex / ChatGPT / o1

You are writing Python modules for GRID, a trading intelligence system.
The code must be production-grade, PIT-correct, and follow existing patterns.

## Architecture Rules
- **PIT Correctness:** Every data query uses an `as_of` timestamp. No future data.
- **SQL:** Always use `sqlalchemy.text()` with bind params. NEVER f-strings.
- **NaN:** Follow the module's existing NaN strategy (see examples below).
- **Logging:** `from loguru import logger as log` — use `log.info()`, `log.warning()`, etc.
- **Type hints:** Required on all functions.
- **No new deps** unless absolutely necessary.

## Database Access Pattern
```python
from sqlalchemy import text
from sqlalchemy.engine import Engine

def query_something(engine: Engine, as_of: str) -> pd.DataFrame:
    """Always use as_of for PIT correctness."""
    with engine.connect() as conn:
        df = pd.read_sql(
            text(
                "SELECT feature_id, obs_date, value "
                "FROM resolved_series "
                "WHERE obs_date <= :as_of "
                "ORDER BY obs_date"
            ),
            conn,
            params={"as_of": as_of},
        )
    return df
```

## Feature Engineering Pattern (features/lab.py)
```python
import numpy as np
import pandas as pd

def z_score_rolling(series: pd.Series, window: int = 63) -> pd.Series:
    """Rolling z-score — no lookahead."""
    mu = series.rolling(window, min_periods=max(1, window // 2)).mean()
    sigma = series.rolling(window, min_periods=max(1, window // 2)).std()
    return (series - mu) / sigma.replace(0, np.nan)
```

## Ingestion Pattern (ingestion/*.py)
```python
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

class MyPuller:
    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self._source_id = self._resolve_source_id("MY_SOURCE")

    def _resolve_source_id(self, name: str) -> int:
        with self.engine.begin() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :n"),
                {"n": name},
            ).fetchone()
            if row:
                return row[0]
            conn.execute(
                text("INSERT INTO source_catalog (name, provider) VALUES (:n, :p)"),
                {"n": name, "p": "my_provider"},
            )
            return conn.execute(text("SELECT lastval()")).fetchone()[0]

    def _row_exists(self, conn, series_id: str, obs_date: str) -> bool:
        row = conn.execute(
            text(
                "SELECT 1 FROM raw_series "
                "WHERE source_id = :sid AND series_id = :ser AND obs_date = :d"
            ),
            {"sid": self._source_id, "ser": series_id, "d": obs_date},
        ).fetchone()
        return row is not None
```

## Discovery/Clustering Pattern
```python
from sklearn.mixture import GaussianMixture
import numpy as np

# NaN handling in discovery: ffill().dropna()
features = df[feature_cols].ffill().dropna()

# Fit with BIC selection
best_bic, best_k = np.inf, 2
for k in range(2, max_k + 1):
    gm = GaussianMixture(n_components=k, random_state=42)
    gm.fit(features)
    bic = gm.bic(features)
    if bic < best_bic:
        best_bic, best_k = bic, k
```

## Your Task
<!-- PASTE YOUR SPECIFIC REQUEST HERE -->
<!-- Example: "Write a function that computes regime transition probabilities -->
<!-- from a sequence of regime labels using a Markov chain approach" -->

## Output Format
Return a single .py file (or function) with:
1. Module docstring explaining what it does
2. All imports at the top
3. Type hints on every function
4. No external API calls (I'll provide the data)
5. List any new pip dependencies needed (prefer stdlib/numpy/scipy/sklearn)
