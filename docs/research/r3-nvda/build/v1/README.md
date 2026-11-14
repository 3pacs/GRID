# R3 ledger v1 build (owner)

v1 is built the same way as v0 (see `../README.md`), with these changes:
- `corrections.py` and `cp_corrections.py` record the **real** download time of the bytes that were hashed, never a typed-in time. They also add the Q2 tax-timing records.
- `merge_v1.py` takes `owner-records-v1.jsonl`, which has the real 8-K fetch times and the 11-17 report-time record. When a publication time is unknown it stays null instead of being filled. The merge also applies the v1 circularity reclassifications, sets the firmness of each counted line, and declares `total_of` links.

The build ran at `LEDGER_BUILT_AT` (2026-11-13T03:06:53Z). The validator refuses any record whose retrieval is later than that time.
