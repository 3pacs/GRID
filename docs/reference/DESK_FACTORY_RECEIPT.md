# Receipt-bound factory desk caller

Continuation of #1164 after #1208. Pure offline join, not a production reader or
daily writer. Uses the existing factory result record; creates no new receipt.

## Existing producer seam verified from source

`analysis.module_factory.run` writes report.json using `write_once` (pretty JSON),
then records a `factory_run_result` through the existing factory ledger. That
record includes all trials, counts, frozen_sha256, report_sha256 and recorded_at.

**The ledger's report_sha256 is a canonical-object digest, not the file-byte
digest.** It uses json.dumps(sort_keys=True, allow_nan=False), with ordinary
separators. Ledger lines instead use compact separators. The producer's actual
report file uses indent=2. Treating these three serializations as identical
rejects authentic reports or encourages self-authenticating hash checks.

## Caller

`receipt_bound_factory_cards(report_raw, result_raw,
expected_result_sha256=..., receipt_available_at=..., as_of=..., max_age=...)`

- result_raw is the exact canonical ledger line, excluding its newline.
- expected_result_sha256 must come from existing independently verified ledger/
  anchor custody. Never derive the expected pin from the untrusted input.
- The caller validates the receipt bytes/pin first, then checks the report's
  canonical-object digest against the receipt and binds run/frozen identities.
- Only after that independent binding does it derive the report file-byte hash
  for #1208. Reformatting a report does not change its canonical-object identity.
- Complete receipt trial IDs/counts and every displayed metric are reconciled.
  Selected IDs outside the truncated report ranking must exist and be selected
  tested trials in the pinned receipt. A phantom ID is refused.
- Result recorded_at is the observation clock of the **result record**. It is
  not described as report-file creation time or market-data freshness.
- Receipt availability is explicitly supplied from existing custody. Never use
  mtime or current time by default. Missing clocks withhold; malformed clocks
  reject; stale/future/reversed clocks retain #1208's conservative behavior.
- Neither the ledger chain nor frozen discovery is verified here. Their output
  flags remain false. Matching the result receipt is not a validated strategy.

Trust-anchor selection and availability provenance remain external obligations,
not magically established by a hash. No caller can claim trusted provenance if it
has only the report and a locally fabricated matching receipt. This slice makes
the existing trust boundary explicit and testable; it does not invent custody.

## Checks

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_factory_receipt.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_factory_report.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_evidence.py' -v
```

18 new synthetic tests passed in memory on Python 3.12. Cases include canonical
versus file digest mismatch, changed report/receipt, phantom selected IDs,
truncated ranking, negative/no-testable results, clocks and no input mutation.
No actual ledger, dataset, provider, factory run or holdout was read.
CI and independent Claude verification remain required. Anik alone applies.

## Not completed by this slice

No filesystem/CLI ingestion or production wiring; no live-slot, Sleuth or thesis
feed; no scheduler; no replacement daily review writer. An existing owner must
provide the verified receipt pin and its availability evidence before a real
artifact is consumed. Full factory10 remains open.

The complete tested-trial receipt is checked for safe counts and finite metrics,
including tested trials omitted from the displayed ranking. Boolean counts,
oversized integers and invalid p-values cannot enter through that omitted subset.

## Independent review follow-up

The integrity field now identifies the actual check: canonical report object
matches a pinned result receipt, not merely a same-byte hash comparison.

Concrete pin source: open the existing factory ledger together with its matching
anchor and verify it. For the latest result, use its verified ledger head. For an
older result, use the next record's prev_sha256 only after verifying that record's
membership in the full anchored chain. An unverified next record is not an anchor.
Do not read a result hash from the untrusted receipt itself and call it custody.

Additional synthetic producer seam check:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_factory_receipt_producer.py' -v
```

This generates temporary planted/noise bars using existing factory fixtures,
runs the real producer, reopens its synthetic ledger with its anchor, and feeds
the emitted report/result to the caller. Only private temporary calendar copies
are created; their exact pinned bytes are unchanged and checkout permissions are
not modified. No actual research ledger or holdout is used.

Author follow-up verification: all 18 caller tests plus the producer seam passed
(19 tests total, 2.94 seconds) on Precision Python 3.11.15 using the existing
factory source and generated temporary artifacts only. The original independent
review also passed 18+22+17 tests at the preceding head on Python 3.14.4; a changed
head still requires renewed verification.
