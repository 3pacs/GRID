# Factory 3: pure input eligibility prerequisite

`analysis.desk_input_qualification` is a local, unwired helper. It performs no reads,
feature computation, registration, scoring, promotion or production writes.

The caller supplies exact decision/capture clocks and a source-evidence identity.
Modeled lag or end-of-day context cannot be substituted for actual availability.
Tape eligibility requires completed intervals, interval (not cumulative) volume,
finite valid bid/ask and explicit freshness policy. Zero volume is valid; missing
volume is not zero. Quotes are observations, not guaranteed executable fills.
Calendar eligibility requires versioned source-specific complete coverage of the
requested window. An empty covered calendar does not establish a flow-only day:
unscheduled news and other sources remain unknown. Future event dates are allowed
when the schedule snapshot was already available.

Policies are horizon-specific. This helper does not require one-minute precision
for weekly or longer-horizon option research. Entry/exit option quotes, instrument
identity, contract actions, surface coverage and costs need their own admission.

Remaining integration work: source adapters, immutable custody validation, actual
symbol/session alignment, duplicate/window coverage, named feature contracts,
run-file policy validation, and research/live parity. Caller-provided metadata is
not authenticated by this helper. Dealer rows without retained capture lineage
remain unqualified. Do not infer that an eligible source means a qualified thesis.

Verification (synthetic, no databases/providers/data files):

    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_desk_input_qualification -v
    ruff check analysis/desk_input_qualification.py tests/test_desk_input_qualification.py --no-cache

Claude independently reviews this Codex-built slice. Anik applies releases.
