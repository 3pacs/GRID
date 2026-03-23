# GRID Utility Task Brief — For Copilot / GPT-4o

You are writing straightforward utility code for GRID. These are well-defined
tasks with clear inputs and outputs — no ambiguity, no architecture decisions.

## Rules
- Python 3.11, type hints required
- Use `from loguru import logger as log` for logging
- SQL must use `sqlalchemy.text()` with `:param` bind syntax. NEVER f-strings.
- No new pip dependencies
- Keep it simple — no over-engineering

## Common Tasks for Copilot

### 1. Data format converters
```python
# Example: convert FRED date format to ISO
def fred_date_to_iso(fred_date: str) -> str:
    """Convert FRED date 'YYYY-MM-DD' to ISO format."""
    ...
```

### 2. Validation helpers
```python
# Example: validate a FRED series ID
def is_valid_series_id(series_id: str) -> bool:
    """Check if series_id matches FRED naming conventions."""
    ...
```

### 3. Batch SQL generation
```python
# Example: generate INSERT statements for seed data
# MUST use parameterized queries — output text() + params, not raw SQL strings
```

### 4. Test stubs
```python
# Example: generate test cases for a puller class
import pytest
from unittest.mock import MagicMock, patch

class TestMyPuller:
    def test_pull_inserts_rows(self):
        ...
    def test_pull_failure_logs_not_raises(self):
        ...
```

### 5. Data transformation one-liners
```python
# Example: pivot a long-form DataFrame to wide
# Example: resample daily data to weekly with specific aggregation
# Example: compute rolling correlation between two series
```

## Your Task
<!-- PASTE YOUR SPECIFIC TASK HERE -->
<!-- Keep it focused — one function, one conversion, one test class -->
<!-- Example: "Write a function that takes a pandas DataFrame with columns -->
<!-- ['date', 'series_id', 'value'] and returns a pivot table with -->
<!-- dates as index and series_ids as columns, forward-filling NaN -->
<!-- with a limit of 5 rows." -->

## Output Format
Return just the code. No explanation needed. Include:
1. Imports at top
2. The function/class
3. A brief docstring
4. Type hints
