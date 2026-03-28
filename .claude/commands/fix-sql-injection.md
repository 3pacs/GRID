# Fix SQL Injection

Find and fix SQL injection vulnerabilities in the codebase.

## Instructions

1. Search for these dangerous patterns across all `.py` files:
   - `f"` or `f'` followed by SQL keywords (SELECT, INSERT, UPDATE, DELETE, WHERE, INTERVAL)
   - `.format(` near SQL query strings
   - String concatenation (`+`) building SQL
2. Known locations (from ATTENTION.md #1):
   - `api/routers/regime.py:85-93` — `.format()` for INTERVAL with user `days` param
   - `journal/log.py:241` — string interpolation in interval clause
3. For each finding:
   - Replace with SQLAlchemy `text()` and `.bindparams()`
   - Verify the fix doesn't change query behavior
   - Add a test case covering the parameterized query
4. Run `cd grid && python -m pytest tests/ -v` to confirm nothing breaks
