# R3 ledger v2 build (owner)

`merge_v2.py` starts from the v1 ledger and applies the fixes from the #1150 re-review:
- **Retrieval times.** Each one is the earliest real download time (file mtime) of exactly the bytes its `retrieved_sha256` covers. A record with no hash keeps no retrieval time.
- **Supply line.** Its firmness becomes `partly_cancellable`. The "firm" AI-cloud and third-party-lease lines are noted as maxima.
- **Owner 8-K records.** They are relabelled NOT_CHECKED, with their true provenance.
- **New records.** Two are added: the 10-Q risk factor "we may be unable to reduce these commitments", and SoftBank's slide-17 sentence that the $3.7B loss is "completely unrelated to PORTS-Pike".

Usage: `python merge_v2.py <scratch-dir> ledger-v1.jsonl ledger-v2.jsonl`. The build ran at `LEDGER_BUILT_AT` (2026-11-13T04:33:42Z).
