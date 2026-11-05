# Historical diary reference review

The existing authenticated `GET /api/v1/intelligence/diary` supports an explicit
review of the October 14 and October 15, 2026 publications. The normal response
is unchanged. Opt in with all three query parameters:

```text
?date=2026-10-15&reference_overlay=true&reference_as_of=2026-10-17T03:50:03.199022Z
```

The response keeps the complete stored original publication under `original`,
including its narrative, market moves and original generation timestamp.
`reference` is a separate review object. Its S&P 500 reference close/prior/change
is 7718.94 / 7805.45 / -86.51 for October 14, and
7744.06 / 7718.94 / +25.12 for October 15. These are later reference calculations,
not revisions of the original publications or new statements about other assets.
October 13 is a rounded benchmark agreement (7805.45), without a correction row.

Admission is limited to repair `yf_close_reference_correction_20261017_v1` and
the exact native full-record SHA256 snapshots of correction IDs 1958031151–1958031154,
all four `CORRECTION:SP500:close`, source 1 (FRED), `QUARANTINED`. It also requires
the five original raw-row snapshots and the exact original diary 14 or 15
snapshot. Native PostgreSQL hashes bind every payload field, including source,
session/basis, evidence receipt, original snapshots and publication lineage.
An extra same-repair member, or any additional correction-series row in the
five-date October 9/12/13/14/15 slice, refuses the overlay regardless of its
repair ID, source or status. Unrelated foreign repairs on other dates are outside
this review. Missing, partial, duplicate, changed or conflicting records produce
`reference.status=unavailable`; no corrected move is synthesized.

`reference_as_of` must include a UTC offset. Before the original publication,
neither original nor reference is returned. After publication but before both
the public-evidence observation (2026-10-17T03:34:01Z) and actual database append
(2026-10-17T03:50:03.199022Z), only the verified original is available. The append
boundary is inclusive. This cutoff describes availability of this stored review
evidence; it does not establish first public benchmark publication or backdate
today's knowledge. Original input-row receipts were absent: diary linkage is
reconstructed date/source/value/arithmetic consistency. No correction notice
publication is asserted (`correction_notice_published_at=null`).

The helper issues one bounded PostgreSQL SELECT with CTEs/UNION ALL, giving one
statement-level MVCC snapshot for all three record groups (maximum 2+6+5 rows).
It changes no connection state, initializes no schema and invokes no writer or
generation function. UTC/Etc/UTC session serialization is required to match the
pinned native hashes; other timezones and read/hash failures refuse availability.
The snapshot describes that statement, not a lock or guarantee against later
database changes. Existing authentication remains on the diary route.

This is an opt-in API review response. It does not change a published narrative,
raw/resolved readers, features, scores, correction statuses, diary generation,
the diary list/search routes or any browser rendering. Reader/scoring admission
remains held. It is not a blanket repair of the 388-row batch or Nasdaq/VIX.

Prepared regressions in `tests/test_historical_close_review_overlay.py` are
UNRUN. They use in-memory fake result rows, pinned native correction/original
bodies and publication projections. Stubbed server hashes model the database
boundary; these tests do not prove PostgreSQL SHA/JSON serialization or real
query execution. A later allocated offline validation must run these cases and
the unchanged diary tests. Any PostgreSQL integration validation requires a
separately authorized disposable fixture and exact full-record serialization;
no production DB or provider run is part of this patch.
