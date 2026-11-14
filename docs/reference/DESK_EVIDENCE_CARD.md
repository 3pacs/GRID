# Desk evidence card: bounded offline contract (#1164)

`analysis.desk_evidence.evidence_card` is a pure adapter target, not a daily job
or a replacement for factory7. It performs no reads, writes, promotions or
calculations of strategy returns. Synthetic inputs only have been exercised.

A caller supplies item/receipt identities, evidence class, observation and actual
availability clocks, counts, and a declared source-specific freshness tolerance.
Unknown clocks, future availability, reversed clocks and stale observations remain
withheld. A recent ingestion timestamp cannot refresh an old observation.

- Forward candidates remain `unproven, n of required`; reaching the count does not
  validate them or promote a slot.
- Development results remain development-only, including zero-selection results.
- Sleuth leads require a declared test and remain leads, not findings.
- `clock_eligible` only means these clocks pass this policy. It does not validate
  source correctness, independence, hypothesis provenance, P&L or trading suitability.
- Receipt identifiers are references supplied by callers, not verified custody.

Next integration work: adapt actual factory slot state, Sleuth tests and thesis log,
using their existing receipts; exercise S2 output and a GRID fixture. Only then add
one daily brief writer with duplicate-day handling. Do not derive a live verdict
from this module alone or claim full factory10 acceptance. No production code calls
this module yet. Rendering must escape all user/source text.

Check: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_desk_evidence -v`
