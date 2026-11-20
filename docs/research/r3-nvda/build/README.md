# R3 ledger build steps (owner)

How `ledger-v0.jsonl` was produced:
1. Worker JSON Lines, made by separate research agents per `RECORD_SCHEMA.md`:
   - `sec-records.jsonl` (NVIDIA's own SEC filings);
   - `cp-records.jsonl` (counterparty and announcement documents);
   - `health-*-records.jsonl` (counterparty health; rubric in `../health-rubric-v0.md`).
2. Independent critic passes re-fetched every cited document and wrote a verdict per record (`critic-sec.jsonl`, `critic-cp.jsonl`).
3. `corrections.py` and `cp_corrections.py` apply each critic correction and omission. Each record keeps its verdict and finding in `review`.
4. `merge.py` merges everything, assigns `dedup_group` / `counted` / `count_reason` (each dollar counted once), and writes canonical JSON Lines.

**What is not in the repo.** The worker and critic files, and the raw downloaded documents, stay in the owner's local scratch area. These are third-party documents; each record carries its URL and the sha256 of the raw bytes instead.

**To check.** Run `python -m analysis.r3_nvda_ledger`. It validates the ledger and recomputes the stack and scenarios. `tests/test_r3_nvda_ledger.py` checks the frozen hashes.
