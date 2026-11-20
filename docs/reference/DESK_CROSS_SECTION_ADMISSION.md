# Cross-section development report admission

Bounded #1164 seam for Claude's #1217 producer. The existing pure adapter now
admits exactly INTRADAY_DEV_RANKING_ONLY and CROSS_SECTION_DEV_RANKING_ONLY.
It never rewrites a cross-section report as intraday. source_state preserves the
distinction; target_unit remains the producer's bounded explicit unit.

All prior hash/clock/identity/metric/count/selection checks remain. Negative
results survive; historical nights/fires never become forward observations.
Selection is development-only, never confirmation, validation or promotion.

This is ranking_summary_only. The fixed-name unadapted_report_fields list
identifies cards, coverage, method and max_t when present. Their contents are
not copied into evidence cards. In particular conditional tables, gross_bp,
turnover, share_of_nights and gates are NOT displayed or verified here. No
inference of net dollars as realized P&L: consumers must retain target_unit.
This is NOT the full card-as-product milestone or review of the producer's
statistics/sector assignments. No new producer run, real ledger or holdout read.

Check in a pinned checkout:
```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_factory_report.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_evidence.py' -v
```
Expected 26 adapter + 17 helper tests. Four added synthetic checks cover explicit
cross-section state/units, non-promotion, negative/missing-clock retention,
unadapted field disclosure and rejection of unknown/validated states.

#1217 may replace its synthetic test's state-rewriting workaround after this
adapter is available; its author owns that change. This branch edits no producer
files. Claude verifies; Anik alone applies. No runtime or schedule activation.
