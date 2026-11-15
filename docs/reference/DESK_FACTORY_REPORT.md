# Factory report to desk evidence adapter

Bounded continuation of #1164. This is an offline library, not the daily brief,
factory runner, slot manager, or a source of validated findings.

## Input contract

`analysis.desk_factory_report.factory_report_cards` accepts immutable native
factory report bytes and an independently obtained expected **file-byte** SHA256.
Do not substitute the factory ledger's canonical-object digest unless its byte
serialization is proven identical. Do not calculate the expectation from the
untrusted bytes at the call site and call that independent verification.

The caller provides a receipt identity, report observation time, actual report
availability time, as-of clock and explicit freshness policy. Unknown clocks
remain None and yield withheld cards. Never use file mtime, the final historical
session, or current ingestion time as a substitute for report availability.
Report-clock eligibility does not establish freshness of underlying market data.

The adapter checks exact development state, non-promotion, unique JSON keys,
finite ranking metrics, trial counts, identifiers and selection consistency.
Report bytes are limited to 2 MB. It emits only bounded approved fields, not
source narrative. Frozen discovery and ledger integrity remain **unverified**
by this adapter; matching the report hash alone does not verify either.

## Output behavior

- A summary always exists, including no testable trials and no selected modules.
- Negative development metrics and untestable counts remain visible.
- Historical fires are `development_fires`; forward samples remain zero.
- Selected modules remain development candidates, not proven strategies.
- Native reports can limit ranking to the top 25. Selected IDs omitted by that
  truncation remain explicit; no metric or evidence card is invented for them.
- No count threshold, p-value, selected flag or report headline promotes anything.
- Input dictionaries/bytes are not mutated. No data paths, ledgers, network,
  schedulers, files, or live slots are read or written.

## Check command for Claude

From this branch in the ordinary GRID checkout:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_factory_report.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_evidence.py' -v
```

21 synthetic tests passed on cloud Python 3.12 using identical source loaded
in memory (no filesystem writes). The 17 existing helper tests passed on Precision
Python 3.14 before this change. Ruff is unavailable on the cloud host; CI and
Claude's checkout checks remain required. No production report or holdout used.

## Remaining integration

Full factory10 remains open. A caller must supply independently verified report
and receipt custody, actual clocks, source-data qualifications and live-slot
status. Sleuth and thesis adapters, complete S2/GRID fixture integration,
rendering and the scheduled daily brief remain separate work. No new daily review
writer is introduced; GRID's existing review pipeline stays untouched.

Claude verifies the exact final kit; Anik alone applies any GRID release.

## Review hardening

Oversized numeric metrics and decoder recursion failures normalize to ValueError;
no raw OverflowError/RecursionError escapes these boundaries. A selected ID outside
the truncated ranking is only a producer claim: the report has no row proving it
exists or was tested. Output explicitly states selected IDs have not been checked
against frozen discovery. A downstream caller must verify them before any selection
or activation. The independently obtained expected file hash remains a caller
contract, not something this pure adapter can prove.
