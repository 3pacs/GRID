# Synthesis evidence failure boundary

This source-only correction extends existing decision gateway and freshness code.
It does not introduce a parallel intake, synthesis or evaluation architecture.
It complements #1083 and #1090; validation protocol work remains under #960.

## Corrected behavior

- `llm_red_team` returns an empty report with zero risk when invocation or parsing
  fails. The gateway now requires counterarguments before treating zero as a
  completed check. Gateway calls opt into strict raw score validation before
  coercion: missing/boolean/string/nonfinite/out-of-range scores reject the whole
  response, and grades are recomputed rather than taken from model overrides.
  Other red-team consumers retain their default tolerant behavior.
- Missing, malformed, nonfinite or out-of-range red-team/stress evidence cannot
  produce a positive gateway verdict. Boolean/string values are not coerced into
  numeric evidence. Oversized integer risk cannot crash numeric validation.
- Any failed orchestration stage blocks ticket generation. A genuinely absent
  optional pattern estimate remains allowed when the required checks passed, but
  is described as unavailable rather than as agreement. A failed pattern stage is
  distinct from that legitimate absence.
- Existing sufficient-data verdicts and downgrade rules are unchanged. A valid
  zero-risk red-team response with counterarguments remains valid.
- Future observation dates are `INVALID`, not `FRESH`.

These checks establish completeness/shape at this boundary, not source truth,
calibrated probability, point-in-time correctness or profitable trading.

## Future-date consumer limitation

`check_freshness` uses latest observation date and is advisory. The current
callers in `company_analyzer`, `forensics`, `causation_scoring` and `thesis_tracker`
log the status; this patch does not certify end-to-end exclusion of bad inputs.
Observation time is not publication, retrieval, vintage availability or session
finality. Required-source admission must be implemented and qualified at each
consumer under its existing ownership before claiming a hardened whole pipeline.

## Qualification and rollout

Tests use mocked dependencies or injected local fake clients, never live models,
providers or production databases. The relevant suites include gateway, real
red-team fallback behavior, provenance, stress, pattern library and trade tickets.
The existing high-on-pattern-exception regression now deliberately expects
`no_trade`; valid-path tests remain.

No schema, scheduler, historical state, provider configuration, scoring protocol,
publication hold or production runtime changes. No merge, deploy or trade is
included. Repository CI and runtime acceptance are separate gates. No win rate or
correctness guarantee follows from this code change.
