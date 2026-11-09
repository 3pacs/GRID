# Company research evidence-quality contract

This is an offline, caller-driven evidence gate, not an investment-rating engine,
financial truth oracle, autonomous collector, or proof of complete source coverage.

## Safe path

1. Capture genuine source bytes with ingestion.company_documents.ingest_document.
2. Give evaluate_archived_company_research a packet with company_id,
   required_source_kinds, snapshot_ids and claims, plus archive_root, an explicit
   timezone-aware as_of, and max_age_days keyed by source category.
3. The archive wrapper calls read_document to verify raw bytes, extracted text,
   metadata and snapshot identity before evaluating the packet.
4. Consume issues and coverage. Any issue sets eligible_for_synthesis=false.
   This flag means only that declared structural checks passed when true; it
   never means the assertions are correct or every needed source was captured.

The lower-level evaluate_company_research accepts documents already read through
read_document. It checks text identity but cannot independently verify raw files.
Do not use unverified hand-built manifests as a production evidence path.

## Claim shape

Every claim needs id, kind (fact or inference), text, and evidence references
containing snapshot_id plus a verbatim quote in the extracted source text.
Inference additionally needs reasoning. Quotation presence proves traceability,
not entailment: extraction/review must still determine whether the quote supports
the claim and whether the supplied publication date is authoritative.

Numeric facts add metric, value, unit (including currency), positive scale,
period_type (instant or duration), period_end, fiscal_year, fiscal_period, scope
and basis. Duration facts also require period_start. Fiscal calendars are not
inferred from calendar quarters. Currency conversion and XBRL dimensions must
be resolved upstream, not guessed. Comparable same-metric facts with different
scaled values are flagged; distinct periods, currencies, scope or GAAP basis
are not silently compared. Restatements retain both observations until an
explicit review resolves their relation; newest is not automatically the winner.

Commitment claims may carry a canonical commitment_id. Repeated IDs are flagged
and never summed. This catches only explicitly linked repetition, not semantic
identity across different IDs. No aggregate obligations total is generated.

## Coverage and time

required_source_kinds is a declared research plan, not a global completeness
score. Intake categories currently include sec_filing, issuer, counterparty and
user_supplied. A useful company-specific plan also needs finer source slots:
annual/interim filings, footnotes, earnings release, prepared remarks and Q&A,
segment and geographic data, debt/lease/purchase obligations, cash-flow quality,
customer/supplier concentration, dilution, governance, material events and peer
or counterparty evidence. These semantic slots are not yet automated here.

A category is available only with a qualifying snapshot, unusable when supplied
snapshots fail checks, and missing when none is supplied. Missing is not zero,
absence of evidence is not a negative fact, and unsupported PDF/encoding/text
remains unavailable rather than being silently counted as parsed.

Publication and retrieval must both predate the research cutoff. Publication age
is checked against explicit caller policy; a fresh download does not refresh an
old report. Unknown publication dates block PIT use. Report/fiscal dates remain
on the metric and never substitute for publication time. latest_source_confirmed
and truth_assessed are always false: no live enumeration, authenticity validation,
semantic entailment check or full-filing completeness audit was performed.

## Verification and limits

Run python -m unittest discover -s tests -p test_company_research_quality.py.
No provider, model, DB, production or paid calls are required. This module does
not release held derived signals or alter existing SEC numeric qualification.
The current gate needs upstream source selection, extraction and canonical
commitment linkage. Review disagreements rather than clearing issues by deleting
evidence. Production orchestration and policy ownership are separate work.
