"""godview.availability_basis — observed vs. inferred vs. unknown, shared across pillars.

Operator direction (2026-09-18): a published release schedule alone does
not establish historical availability for revised or backfilled records.
A pillar's own release-date rule (e.g. CFTC's Tuesday->Friday+3d, or the
Fed's H.4.1 Wednesday->Thursday+1d) says WHEN a report is *supposed* to
have published; ``availability_basis`` says WHETHER a pillar can actually
back that up with an observed acquisition near that time, or is only
inferring it from the schedule.

Originally written for the CFTC positioning pillar
(``godview/cftc_pillar.py``, which still re-exports these names for
backward compatibility); factored out here so the Fed liquidity and
commodity warehouse pillars can reuse the exact same rule instead of
re-picking their own tolerance or note text. See
docs/reference/GODVIEW_PILLAR_CONTRACT.md section 10 for the full CFTC
write-up (the rule itself is pillar-agnostic).
"""

from __future__ import annotations

from datetime import date, datetime

AVAILABILITY_BASIS_OBSERVED = "observed_acquisition"
AVAILABILITY_BASIS_INFERRED = "inferred_schedule"
AVAILABILITY_BASIS_UNKNOWN = "unknown"
AVAILABILITY_BASIS_VALUES = frozenset(
    {AVAILABILITY_BASIS_OBSERVED, AVAILABILITY_BASIS_INFERRED, AVAILABILITY_BASIS_UNKNOWN}
)

#: How many days after release_date a puller run may land and still count as
#: "we observed the real acquisition," not "we are inferring from schedule."
#: Covers a puller running over a weekend/holiday shift after the scheduled
#: release. Wider than this and the row is treated the same as a historical
#: backfill: the schedule, not an observation, is doing the work.
AVAILABILITY_BASIS_TOLERANCE_DAYS = 3

INFERRED_BASIS_NOTE = "availability inferred from schedule; record revised/backfilled"
UNKNOWN_BASIS_NOTE = "acquisition observed before the scheduled release; basis unclear"


def classify_availability_basis(
    release_date: date | None,
    available_at: datetime | date | None,
    *,
    distinct_pull_count: int = 1,
    tolerance_days: int = AVAILABILITY_BASIS_TOLERANCE_DAYS,
) -> tuple[str, str | None]:
    """Classify how we know this row's (or field's) data was available, and why.

    Returns ``(availability_basis, note)``. ``note`` is ``None`` exactly when
    ``availability_basis == AVAILABILITY_BASIS_OBSERVED`` -- an observed
    value needs no caveat.

    * ``distinct_pull_count`` is how many DISTINCT ``pull_timestamp`` values
      ``raw_series`` has ever recorded for this observation, across all of
      its raw components -- more than one means the report was re-pulled
      (revised, or a delayed second run), and the value a materializer keeps
      is never labelled ``observed_acquisition`` "for the original release"
      even if the winning (latest) pull happens to look timely.
    * Otherwise: ``available_at`` within ``tolerance_days`` on-or-after
      ``release_date`` (allowing 1 day of slack early, for timezone/clock
      noise) is ``observed_acquisition``; later than that is
      ``inferred_schedule`` (a backfill: the schedule, not an observation,
      places it); an acquisition implausibly far before the schedule says
      the report could have existed is ``unknown``.
    * ``release_date is None`` (no schedule rule could be applied -- e.g. a
      quarantined non-Tuesday CFTC report) or ``available_at is None`` ->
      always ``unknown``: with no schedule-derived release_date to compare
      against, "observed near schedule" and "inferred from schedule" are
      both meaningless.
    """
    if release_date is None or available_at is None:
        return AVAILABILITY_BASIS_UNKNOWN, None

    available_date = available_at.date() if isinstance(available_at, datetime) else available_at
    age_days = (available_date - release_date).days

    if distinct_pull_count > 1:
        return AVAILABILITY_BASIS_INFERRED, INFERRED_BASIS_NOTE
    if age_days > tolerance_days:
        return AVAILABILITY_BASIS_INFERRED, INFERRED_BASIS_NOTE
    if age_days < -1:
        return AVAILABILITY_BASIS_UNKNOWN, UNKNOWN_BASIS_NOTE
    return AVAILABILITY_BASIS_OBSERVED, None
