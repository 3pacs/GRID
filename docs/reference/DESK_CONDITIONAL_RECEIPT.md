# Receipt-bound conditional cards

Pure entry point: `analysis.desk_conditional_receipt.receipt_bound_conditional_cards`.
Stacked on #1221. Consumes the existing canonical factory_run_result line and
the corresponding report bytes. The expected result-record SHA must come from
an independently verified ledger/anchor, not hashing whatever receipt arrives.
No files, ledgers, services, providers or research data are read or written.

Validation first reuses receipt_bound_factory_cards: canonical result bytes,
pinned hash, report object digest, run/frozen identities, full trial identities
and all ranked metrics. Only then are the SAME immutable report bytes adapted
into conditional cards. All losing/winning cuts survive. The stronger base
receipt provenance is retained, including selected trials outside the ranking.

Clock source is the result record's recorded_at plus explicit receipt availability.
These are not file creation or market-event clocks. No caller observation override,
mtime substitution or current-time fallback. Missing/stale/future evidence remains
withheld on both base and conditional cards.

This verifies NO ledger chain, frozen discovery, point-in-time sector labels,
execution costs, confirmation or forward record. Confirmation/forward remain
not_supplied, promotion_allowed remains false. #1161/#1162 producer contract
and outcome joins stay separate. This is not runtime wiring or proven alpha.

## Exact synthetic check
```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_conditional_receipt.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_conditional_cards.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_factory_receipt.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_factory_report.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_evidence.py' -v
```
Expected 9+19+18+26+17 =89. Claude verifies exact kit; Anik applies.
Stacked PR needs main retarget and substantive CI after #1221 lands. A skipped
review on a feature-base PR is not CI qualification.
