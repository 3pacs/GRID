# Performance Rules

These rules apply when modifying database queries, API endpoints, or computation-heavy modules.

## Database

- Avoid N+1 query patterns — use JOINs or batch queries:
  - `api/routers/models.py:91-98` fetches validation results without JOIN (#27)
  - `discovery/orthogonality.py:75-80` does feature lookups in loops (#27)
- Missing indexes that affect query performance (#16):
  - `decision_journal(model_version_id)` — heavily queried
  - `decision_journal(outcome_recorded_at)` — outcome statistics
  - `resolved_series(feature_id, obs_date) WHERE conflict_flag = TRUE` — conflict reporting
- No explicit connection pool configuration — default SQLAlchemy pool may be insufficient for production (#29)

## Computation

- `discovery/clustering.py:292-313` has O(n^2) nested loop for transition matrices — will be slow for >10K observations (#28)
- Feature computation in `features/lab.py` should be vectorized with pandas/numpy — avoid Python loops over rows

## API

- List endpoints must have proper pagination (offset + limit + total count)
- Use async handlers where appropriate for I/O-bound operations
- Keep route handlers thin — heavy computation belongs in domain modules
