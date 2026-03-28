# Test Coverage Analysis

Analyze test coverage gaps and suggest improvements.

## Instructions

1. Read all test files in `grid/tests/`
2. Cross-reference against ATTENTION.md items #22-23 for known gaps
3. List all modules that have zero test coverage
4. For tested modules, assess quality:
   - Are edge cases covered?
   - Are error paths tested?
   - Are mocks used appropriately (no live API calls)?
5. Suggest the highest-impact test to write next, with a code skeleton
6. Focus on modules in the critical path: `store/pit.py` → `features/lab.py` → `inference/live.py` → `journal/log.py`
