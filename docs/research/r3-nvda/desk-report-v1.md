# Offline NVDA desk report v1

Additive #1085 integration of the accepted November 14 research kit. This joins
the frozen 455-record ledger, historical cash reconciliation and shared desk
evidence labels. It is not a forecast, valuation or live desk writer.

```sh
python -m analysis.r3_nvda_ledger \
  --ledger docs/research/r3-nvda/ledger-v2.jsonl \
  --desk-cash docs/research/r3-nvda/cash-reconciliation-companion-v1.json \
  --desk-as-of 2026-11-14T09:30:00Z --desk-max-age-days 120
```

The 120-day freshness limit is an explicit demonstration setting, not an approved
live policy. Display each period's evidence quality beside its totals: withheld
figures remain historical accounting context, not usable current evidence.

Both artifact hashes are checked. Every exported cash fact uses receipt-aware
availability, never earlier than the integration bundle floor
`2026-11-14T09:27:06.330970Z`. Earlier cutoffs are refused. Raw issuer verification
is inherited from the earlier cash-bridge review; this command does not fetch or
independently recheck issuer HTML.

Recognition is UNAVAILABLE for all seven stage-tagged candidates. Missing inputs:
NVIDIA-attributable dollars per delivered lot; fiscal recognition allocation;
qualified incremental cash assumptions; and consistent FCFF/FCFE, dilution and
valuation-date price/share/net-cash basis. No guessed sale, valuation, probability
or trade signal is emitted. Legacy scenarios and the frozen Nov18 comparison are
unchanged. The report does not invoke the recognition model.

## Checks

```sh
PYTHONDONTWRITEBYTECODE=1 python -m pytest -q --noconftest -p no:cacheprovider \
  tests/test_r3_nvda_ledger.py tests/test_r3_nvda_cash_source.py \
  tests/test_nvda_desk_integration.py tests/test_desk_evidence.py
```

111 focused/regression tests passed on Python 3.14 after applying the exact kit to
main `c19920e9`. Ruff and whitespace checks passed; the pinned report reproduced
exactly. Full repository CI remains separate. No production, provider, schedule,
holdout or release action follows from this report.
