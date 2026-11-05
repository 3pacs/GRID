# Time-of-day activity candidate

Offline research only. `time_of_day.normalize_activity` compares a completed
exchange-session bucket with the median of the same bucket in earlier sessions.
The default uses 300-second buckets and requires 20 prior sessions, capped at 60.
The ratio is activity context, not a direction, probability, or trade instruction.
Candidate metrics include volume, trade count and realized volatility, provided
their units and calculation methods stay consistent within one source ID.

Input contract is documented in the function. Calendar opens/closes must come
from a verified exchange calendar; UTC hour matching is deliberately avoided.
Receipt timestamps must be first trusted collector/server receipts, never local
ANIK callback clocks. Buckets must be complete and available before decision time.
The current session never enters the baseline. Duplicate buckets, inadequate
history, source mixing, incomplete current buckets and zero/nonfinite baselines
return explicit unavailable results. Missing days are not imputed as zero.

Synthetic tests verify mechanics, including DST offsets, shortened sessions,
lookahead exclusion and history count/window controls. These fixtures cannot
establish a valuable edge. Subsequent research must test incremental benefit over
price-only baselines with preregistered chronological splits, multiple-candidate
controls, executable SPYU quotes and cost sensitivity. This module adds no data
collector, runtime activation, model fitting or deployment.

Focused check: `python -m pytest tests/test_intraday_time_of_day.py -q`.
