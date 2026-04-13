### 6. Return-value JSON contract

At the end of your final message, emit a single JSON object wrapped in `<agent-return>` tags:

```
<agent-return>
{
  "task_id": 83,
  "files_modified": ["api/routers/foo.py", "tests/test_foo.py"],
  "files_deleted": [],
  "files_created": [],
  "loc_delta": -42,
  "tests_passed": 12,
  "tests_failed": 0,
  "endpoints_verified": ["supply_chain", "capital_flow"],
  "deploy_hash_verified": true,
  "smoke_passed": true,
  "drift_check_clean": true,
  "pre_create_check_result": "exit 0 — extending intelligence/chain_contagion.py",
  "errors": [],
  "notes": "one-line summary of anything unusual"
}
</agent-return>
```

The dispatcher parses this to auto-close TaskUpdate, update `docs/WAVE_LOG.md`, and reject any agent that doesn't fill in `deploy_hash_verified` or `smoke_passed`.
