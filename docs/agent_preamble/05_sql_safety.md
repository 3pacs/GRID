### 5. SQL safety

Parameterized queries only. No f-string SQL for user-provided values. Use `text(...)` + `.bindparams(...)`. Regression guard at `tests/test_no_sql_fstrings.py` blocks regressions.
