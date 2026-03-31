# GRID Architecture Fixes — Implementation Guide

This document provides step-by-step fixes for the top 5 architectural risks identified in ARCHITECTURE_REVIEW.md.

---

## FIX 1: Database Connection Pooling (CRITICAL)
**Time:** 15 minutes | **Effort:** Minimal | **Impact:** Prevents production deadlocks

### Current State
```python
# api/dependencies.py currently does:
from db import get_engine
engine = get_engine()  # Uses SQLAlchemy defaults (pool_size=5, no overflow)
```

### Root Cause
- Default pool_size=5 is insufficient for 34 API routers + WebSocket handler
- No overflow handling when all connections are busy
- Each systemd worker process gets its own pool (doesn't help, multiplies problem)

### Fix

Create or modify `grid/db.py` to explicitly configure the pool:

```python
# db.py

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from config import settings

def get_engine():
    """Create a database engine with explicit connection pool configuration.

    For a production system with 30-50 concurrent users:
    - pool_size=20: Base connections per worker process
    - max_overflow=10: Additional connections allowed when all are busy
    - pool_pre_ping=True: Verify connection is still alive before use
    - pool_recycle=3600: Recycle connections older than 1 hour (handles DB timeouts)
    """
    return create_engine(
        settings.DB_URL,
        poolclass=QueuePool,
        pool_size=20,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
        # Optional: echo=settings.LOG_LEVEL == "DEBUG",  # Log all SQL in DEBUG mode
        connect_args={
            "connect_timeout": 5,
            # For PostgreSQL performance tuning:
            "options": "-c work_mem=256MB -c shared_buffers=256MB"
        }
    )

# Keep the singleton pattern but now it uses a healthy pool
_engine = None

def get_db_engine():
    global _engine
    if _engine is None:
        _engine = get_engine()
    return _engine
```

### Verification

After deploying:

```bash
# Monitor active connections
psql -h localhost -d grid -c "SELECT count(*) FROM pg_stat_activity;"

# Should see 20-30 connections (not stuck at 5)

# Run a simple load test
python -m pytest tests/test_api.py -v -n 4  # 4 parallel workers

# Under load, verify no "QueuePool timeout" errors in logs
```

### Expected Result
- Backtesting can run 20+ concurrent validation workers
- API can handle 100+ concurrent requests without queuing
- WebSocket broadcasts don't block database access

---

## FIX 2: Add Indexes to Journal and Resolved Series (HIGH)
**Time:** 10 minutes | **Effort:** Minimal | **Impact:** 10-100x speedup for queries

### Current Queries Doing Full Table Scans

```python
# 1. governance/registry.py likely does:
SELECT * FROM decision_journal WHERE model_version_id = ?

# 2. Outcome tracking UI does:
SELECT * FROM decision_journal WHERE outcome_recorded_at > ? ORDER BY outcome_recorded_at DESC LIMIT 100

# 3. Conflict reporting does:
SELECT * FROM resolved_series WHERE feature_id = ? AND obs_date = ? AND conflict_flag = TRUE
```

### Fix: Add Indexes in Migration

Create migration file:
```bash
touch grid/migrations/versions/XXX_add_critical_indexes.py
```

Contents:
```python
"""Add indexes for journal and resolved_series performance.

Revision ID: 001_critical_indexes
"""

from alembic import op
import sqlalchemy as sa

def upgrade():
    """Add missing indexes."""
    # Index for journal queries by model version
    op.create_index(
        'ix_decision_journal_model_version_id',
        'decision_journal',
        ['model_version_id'],
        unique=False,
    )

    # Index for outcome statistics (most recent decisions first)
    op.create_index(
        'ix_decision_journal_outcome_recorded_at',
        'decision_journal',
        ['outcome_recorded_at'],
        unique=False,
    )

    # Index for conflict reporting
    op.create_index(
        'ix_resolved_series_conflict_lookup',
        'resolved_series',
        ['feature_id', 'obs_date', 'conflict_flag'],
        unique=False,
    )

def downgrade():
    """Remove indexes if migration is reverted."""
    op.drop_index('ix_decision_journal_model_version_id')
    op.drop_index('ix_decision_journal_outcome_recorded_at')
    op.drop_index('ix_resolved_series_conflict_lookup')
```

Apply:
```bash
cd grid
alembic upgrade head
```

### Verification

```sql
-- Before migration (SLOW)
EXPLAIN ANALYZE SELECT * FROM decision_journal WHERE model_version_id = 5;
-- Seq Scan on decision_journal  (cost=0.00..12345.00)

-- After migration (FAST)
EXPLAIN ANALYZE SELECT * FROM decision_journal WHERE model_version_id = 5;
-- Bitmap Index Scan on ix_decision_journal_model_version_id  (cost=0.00..45.00)
```

---

## FIX 3: Remove N+1 Query in models.py (HIGH)
**Time:** 30 minutes | **Effort:** Moderate | **Impact:** 100x speedup for model comparison

### Current Problematic Code

```python
# api/routers/models.py:91-98 (BEFORE)
def get_model_details(model_id: int):
    model = db.query(Model).filter(Model.id == model_id).first()
    if not model:
        raise HTTPException(status_code=404)

    # THIS IS N+1: makes separate query for each validation result
    validation_results = []
    for version in model.versions:
        results = db.query(ValidationResult).filter(
            ValidationResult.model_version_id == version.id
        ).all()  # ← Makes separate query per version
        validation_results.extend(results)

    return {
        "model": model,
        "validations": validation_results  # ← Could be 100+ queries
    }
```

### Fix: Use SQLAlchemy Eager Loading

```python
# api/routers/models.py:91-98 (AFTER)
from sqlalchemy.orm import selectinload

def get_model_details(model_id: int):
    # Use selectinload to fetch all validations in a SINGLE query
    model = db.query(Model).options(
        selectinload(Model.versions).selectinload(ModelVersion.validation_results)
    ).filter(Model.id == model_id).first()

    if not model:
        raise HTTPException(status_code=404)

    # Validations are already loaded, no additional queries
    validation_results = [
        result
        for version in model.versions
        for result in version.validation_results
    ]

    return {
        "model": model,
        "validations": validation_results
    }
```

### Alternative: Batch Query

If eager loading isn't possible:

```python
def get_model_details(model_id: int):
    model = db.query(Model).filter(Model.id == model_id).first()
    if not model:
        raise HTTPException(status_code=404)

    # Batch query instead of loop
    version_ids = [v.id for v in model.versions]
    validation_results = db.query(ValidationResult).filter(
        ValidationResult.model_version_id.in_(version_ids)
    ).all()  # ← Single query for all versions

    return {
        "model": model,
        "validations": validation_results
    }
```

### Verification

Add query counting to tests:

```python
import pytest
from sqlalchemy import event

def test_model_details_single_query(db_session):
    query_count = 0

    def count_queries(conn, cursor, statement, parameters, context, executemany):
        nonlocal query_count
        query_count += 1

    event.listen(db_session.connection(), "before_cursor_execute", count_queries)

    result = get_model_details(model_id=1)

    # Should be exactly 1 query, not 1 + N versions queries
    assert query_count == 1, f"Expected 1 query, got {query_count}"
```

---

## FIX 4: Add Tests for Zero-Coverage Modules (HIGH)
**Time:** 4-6 hours per module | **Effort:** Moderate | **Impact:** Prevents silent bugs

### Test Template for resolver.py

Create `grid/tests/test_resolver_gaps.py`:

```python
import pytest
from normalization.resolver import resolve_conflict, ThresholdExceeded

class TestResolverEdgeCases:
    """Test edge cases in resolver.py that cause bugs at scale."""

    def test_no_lookahead_bias_in_resolution(self, db_session):
        """Verify resolution doesn't use future release dates."""
        # Insert data with mismatched timestamps
        row1 = ResolvedSeries(
            feature_id=1,
            obs_date=date(2025, 1, 1),
            release_date=date(2025, 1, 2),  # Released next day
            value=100.0,
        )
        row2 = ResolvedSeries(
            feature_id=1,
            obs_date=date(2025, 1, 1),
            release_date=date(2025, 1, 15),  # Released 2 weeks later
            value=102.0,
        )
        db_session.add_all([row1, row2])
        db_session.commit()

        # Decision made on Jan 10
        as_of = date(2025, 1, 10)

        # Resolver should ONLY use row1 (released Jan 2)
        # row2 doesn't exist yet as of Jan 10
        resolved = resolve_conflict([row1, row2], as_of)
        assert resolved["value"] == 100.0
        assert resolved["release_date"] == date(2025, 1, 2)

    def test_division_by_zero_handling(self):
        """Verify 0.5% threshold doesn't crash when baseline is 0."""
        # Some features naturally go to zero (e.g., commodity prices, volatility)
        baseline = 0
        new_value = 100

        # Should not raise ZeroDivisionError
        result = resolve_conflict(
            baseline=baseline,
            new_value=new_value,
            threshold=0.005
        )
        assert result is not None

    def test_volatility_threshold_needs_tuning(self):
        """Document known false positive: VIX swings exceed 0.5%."""
        # VIX on 2025-01-15: 12.5
        # VIX on 2025-01-16: 18.3
        # Change: 46% (far exceeds 0.5% threshold)

        baseline_vix = 12.5
        new_vix = 18.3

        # Current code flags this as conflict (wrong!)
        # Should have feature-specific threshold (e.g., 30% for VIX)
        change_pct = abs(new_vix - baseline_vix) / baseline_vix
        assert change_pct > 0.005, "VIX change should exceed 0.5% (known issue)"
```

### Test Template for gates.py

Create `grid/tests/test_gates_validation.py`:

```python
import pytest
from validation.gates import GateChecker

class TestGateLogic:
    """Verify walk-forward temporal boundaries are enforced."""

    def test_gate_prevents_lookahead_models(self, db_session):
        """Models trained on future data should fail gates."""
        # Create a model that was trained using data from after its decision date
        bad_model = Model(
            name="bad_model_with_lookahead",
            training_cutoff=date(2025, 1, 15),
            decision_date=date(2025, 1, 10),  # Trained on data after decision
        )

        gate_checker = GateChecker(db_engine)
        passed = gate_checker.check(bad_model)
        assert not passed, "Gate should reject models trained on future data"

    def test_gate_passes_correct_temporal_order(self, db_session):
        """Models with proper temporal ordering should pass gates."""
        good_model = Model(
            name="good_model",
            training_cutoff=date(2025, 1, 10),
            decision_date=date(2025, 1, 15),  # Decision after training (correct)
        )

        gate_checker = GateChecker(db_engine)
        passed = gate_checker.check(good_model)
        assert passed, "Gate should pass properly ordered models"

    def test_gate_min_sample_size(self, db_session):
        """Models trained on insufficient data should fail gates."""
        # Create model trained on only 10 observations (too few)
        insufficient_model = Model(
            name="insufficient_data_model",
            training_samples=10,  # Need minimum 250 for statistical significance
        )

        gate_checker = GateChecker(db_engine, min_samples=250)
        passed = gate_checker.check(insufficient_model)
        assert not passed, "Gate should reject models with insufficient training data"
```

### Running Tests

```bash
cd grid

# Run new tests
pytest tests/test_resolver_gaps.py -v
pytest tests/test_gates_validation.py -v

# Check coverage
pytest --cov=normalization --cov=validation tests/test_resolver_gaps.py tests/test_gates_validation.py
```

---

## FIX 5: Establish Consistent NaN Handling (HIGH)
**Time:** 2 hours | **Effort:** Moderate | **Impact:** Prevents silent data loss

### Current Inconsistency

```python
# discovery/orthogonality.py:156
df = df.ffill(limit=5)  # Forward-fill max 5 periods

# discovery/clustering.py:114
df = df.ffill().dropna()  # Forward-fill all, then drop

# features/lab.py ← varies by transformation (undocumented)
```

### Standard Pattern

Create `grid/utils/nan_handling.py`:

```python
"""Standard NaN handling for GRID system.

All modules MUST use this approach to ensure consistent behavior:
1. Forward-fill missing values up to 5 periods (reasonable for macro data)
2. Drop remaining NaNs only at feature computation time
3. Log where values are interpolated for audit trail
"""

import pandas as pd
from loguru import logger as log
from typing import Optional

def fill_missing_values(
    df: pd.DataFrame,
    max_fill_periods: int = 5,
    column: Optional[str] = None,
) -> pd.DataFrame:
    """Forward-fill missing values with a limit, logging what was filled.

    Parameters:
        df: DataFrame with potential NaN values
        max_fill_periods: Maximum consecutive periods to forward-fill (default 5)
        column: Specific column to fill (if None, fills all)

    Returns:
        DataFrame with forward-filled values, original NaNs documented

    Example:
        >>> df = pd.DataFrame({'price': [100, NaN, NaN, NaN, NaN, NaN, 102]})
        >>> filled = fill_missing_values(df, max_fill_periods=5)
        # Logs: "Filled 4 NaN values in price (periods 2-5, gap >5 detected at 6)"
        >>> filled.iloc[1:5] == 100  # periods 2-5 filled with last valid (100)
        >>> filled.iloc[6].isna()  # period 6 remains NaN (exceeds limit)
    """
    if column is not None:
        df = df.copy()
        original_nans = df[column].isna().sum()
        df[column] = df[column].ffill(limit=max_fill_periods)
        filled_count = original_nans - df[column].isna().sum()

        if filled_count > 0:
            log.info(
                "Filled {n} NaN values in {col} (limit={limit})",
                n=filled_count,
                col=column,
                limit=max_fill_periods,
            )
        return df

    else:
        df = df.copy()
        original_nans = df.isna().sum()
        df = df.ffill(limit=max_fill_periods)
        filled_counts = original_nans - df.isna().sum()

        for col in df.columns:
            if filled_counts[col] > 0:
                log.info(
                    "Filled {n} NaN values in {col} (limit={limit})",
                    n=filled_counts[col],
                    col=col,
                    limit=max_fill_periods,
                )
        return df


def drop_remaining_nans(
    df: pd.DataFrame,
    subset: Optional[list[str]] = None,
    context: str = "",
) -> pd.DataFrame:
    """Drop rows with NaN values, logging how many were dropped.

    Only call this at feature computation time, NOT during data ingestion.

    Parameters:
        df: DataFrame with potential remaining NaN values
        subset: Specific columns to check (if None, checks all)
        context: Descriptive context for logging (e.g., "feature_lab:momentum")

    Returns:
        DataFrame with NaN rows removed
    """
    original_len = len(df)
    df = df.dropna(subset=subset)
    dropped_count = original_len - len(df)

    if dropped_count > 0:
        log.warning(
            "Dropped {n} rows with NaN ({pct:.1f}% of data) [{context}]",
            n=dropped_count,
            pct=100 * dropped_count / original_len,
            context=context or "unspecified",
        )

    return df
```

### Update All Modules

**discovery/orthogonality.py** (before):
```python
features_df = features_df.ffill(limit=5)
features_df = features_df.dropna()
```

**discovery/orthogonality.py** (after):
```python
from utils.nan_handling import fill_missing_values, drop_remaining_nans

features_df = fill_missing_values(features_df, max_fill_periods=5)
features_df = drop_remaining_nans(features_df, context="orthogonality:regime_detection")
```

**discovery/clustering.py** (before):
```python
df = df.ffill().dropna()
```

**discovery/clustering.py** (after):
```python
from utils.nan_handling import fill_missing_values, drop_remaining_nans

df = fill_missing_values(df, max_fill_periods=5)
df = drop_remaining_nans(df, context="clustering:transition_matrix")
```

### Test the Standard

```python
# tests/test_nan_handling.py
from utils.nan_handling import fill_missing_values, drop_remaining_nans

def test_consistent_fill_across_modules():
    """Verify all modules use same fill limit."""
    df = pd.DataFrame({'x': [1, np.nan, np.nan, np.nan, np.nan, np.nan, 2]})

    # All modules should fill exactly 5 periods
    filled = fill_missing_values(df, max_fill_periods=5)

    assert filled.iloc[1:6, 0].isna().sum() == 1  # period 6 (6th NaN) remains
    assert not filled.iloc[5, 0].isna()  # period 5 (5th NaN) was filled
```

---

## FIX 6: Extract actor_network.py (MEDIUM - Lower Priority)
**Time:** 3-5 days | **Effort:** Substantial | **Impact:** Maintainability, testability

### Current Structure
```
intelligence/actor_network.py (7002 lines)
├── Actor model definition
├── Relationship queries
├── Panama Papers import
├── Wealth flow calculation
└── ICIJ bulk loader
```

### Target Structure
```
intelligence/actors/
├── __init__.py
├── core.py (400 lines) — Actor, ActorRelationship, WealthFlow models
├── lookups.py (800 lines) — Query interface
├── importer.py (1200 lines) — Panama Papers, bulk import
├── discovery.py (1500 lines) — 3-degree BFS, board interlocks, batch Form 4
└── serializer.py (300 lines) — JSON/dict conversion for API
```

### Example Extraction

**Before: intelligence/actor_network.py**
```python
class Actor:
    def __init__(self, name: str, confidence: float):
        self.name = name
        self.confidence = confidence

    def wealth_flows(self):
        # 200 lines of complex logic
        ...

    def Panama_papers_exposure(self):
        # 150 lines
        ...

    def board_interlocks(self):
        # 200 lines
        ...
```

**After: intelligence/actors/core.py**
```python
from dataclasses import dataclass

@dataclass
class Actor:
    """Core actor model — minimal, focused."""
    name: str
    confidence: float
    # Only store data, no complex methods
```

**After: intelligence/actors/lookups.py**
```python
from intelligence.actors.core import Actor

class ActorLookup:
    """Query interface for actors."""

    def wealth_flows(self, actor: Actor):
        """Separate module for wealth flow queries."""
        from intelligence.actors.flows import calculate_wealth_flows
        return calculate_wealth_flows(actor)
```

This is more complex, so prioritize Fixes 1-5 first.

---

## Implementation Checklist

- [ ] **FIX 1 (CRITICAL):** Database connection pooling
  - [ ] Update `grid/db.py` with explicit pool config
  - [ ] Test with concurrent load
  - [ ] Deploy to staging
  - [ ] Monitor connection count in production

- [ ] **FIX 2 (HIGH):** Add missing indexes
  - [ ] Create migration file
  - [ ] Apply to staging database
  - [ ] Verify EXPLAIN ANALYZE shows index usage
  - [ ] Deploy to production

- [ ] **FIX 3 (HIGH):** Fix N+1 query in models.py
  - [ ] Identify all N+1 patterns with query counter
  - [ ] Refactor to use eager loading or batch queries
  - [ ] Add regression tests
  - [ ] Verify query performance

- [ ] **FIX 4 (HIGH):** Add tests for zero-coverage modules
  - [ ] Start with resolver.py (4 hours)
  - [ ] Continue with gates.py (4 hours)
  - [ ] Add to CI/CD pipeline
  - [ ] Require 80%+ coverage for merges

- [ ] **FIX 5 (HIGH):** NaN handling standardization
  - [ ] Create `utils/nan_handling.py`
  - [ ] Update discovery/orthogonality.py
  - [ ] Update discovery/clustering.py
  - [ ] Add tests verifying consistent behavior
  - [ ] Update style guide

---

## Timeline & Resource Allocation

**Week 1 (Critical Fixes):**
- Mon: Implement Fix 1 + Fix 2 (database health)
- Tue-Wed: Implement Fix 3 (N+1 queries)
- Thu-Fri: Implement Fix 5 (NaN handling)

**Week 2-3 (High Priority Tests):**
- Daily: Add tests from Fix 4 (resolver, gates, features, inference)

**Week 4-6 (Medium Priority):**
- Fix 6: Extract actor_network.py (if resources available)
- Refactor large API routers (intelligence.py, astrogrid.py)

---

**For questions, refer to ARCHITECTURE_REVIEW.md for detailed analysis.**
