# SEC source-backed financial-period qualification

Captured 2026-11-07 with four unauthenticated official SEC GET requests. Request
identification used the repository's existing GRID Intelligence contact. No API
keys, provider entitlements, database connections, ingestion runs or services.

The three *.companyfacts.json.gz files store ORIGINAL response body bytes from current
Companyfacts endpoints. They are NOT immutable historical point-in-time snapshots
and may contain later comparatives or amendments. Each provenance JSON records
URL, UTC retrieval timestamp, byte count and SHA-256. The separate NVDA accession
HTML is the official filing document, preserved without rewriting. All four
source files are stored with deterministic gzip (mtime=0). Decompression restores
the exact original response bytes; provenance SHA-256 and byte counts refer to
those original bytes, while stored_bytes records the compressed size. Tests
decompress before verifying original hashes. No JSON or HTML normalization occurred. Its inline
XBRL values and dimensionless date contexts are independently tested against the
Companyfacts regression. Test assertions are not substitutes for the raw bytes.

The project registry exposed the owner root and cloud projects, not additional
local checkout roots. No relevant cached fixture was found in the checkout's
tests/fixtures or data trees. A broader Codex filename-only scan was stopped
without results because it was taking too long; this was not an exhaustive
machine-wide cache search.

## Verified cases

- NVDA July 26, 2026: actual us-gaap:Revenues, accession 0001045810-26-000075.
  Quarter April 27-July 26 is USD 96,221,000,000. January 26-July 26 is the
  six-month USD 177,837,000,000 context and is rejected, never divided/inferred.
  The historical higher-priority RevenueFromContractWithCustomerExcludingAssessedTax
  concept is present in the snapshot but is NOT the 2026 revenue concept.
- Apple: September 25, 2022-September 30, 2023 is a real 371-day fiscal year,
  USD 383,285,000,000. September 25-December 31, 2022 is a real 98-day quarter,
  USD 117,154,000,000. Both are accepted with the correct duration type.
- TSMC: ifrs-full revenue includes TWD and USD presentations. The current
  mapping selects the higher-priority TWD concept for 2024 annual revenue,
  TWD 2,894,307,700,000. With no supplied TWD rate it remains explicitly estimated
  under existing approximate FX conversion. This does not validate FX accuracy.
  This sample produces only annual rows; it does not establish IFRS quarterly
  coverage, despite containing interim facts that must be rejected.

## Offline checks and limitations

11 tests passed through run_sec_financial_period_checks.py with outbound sockets
and PostgreSQL blocked, plugin autoload disabled and repository conftest excluded.
Tests verify byte hashes, NVDA raw filing XBRL, actual full-payload parser output,
noncalendar and IFRS cases, full-snapshot reverse-array order invariance, economic
key uniqueness and only supported output durations. No synthetic facts were
inserted into these source files.

qualification.json records counts from this parser revision: NVDA 587 rows,
Apple 599, TSMC 64. Simple-mapping input counts are before deduplication, include
all configured tag/taxonomy candidates, and exclude special net-debt processing.
Rejected-duration counts alone are NOT rejection reasons: Apple also includes
unsupported filing forms. Conservative missing/conflicting debt contexts cause
omissions. These counts are qualification observations, not financial completeness
or rollout acceptance criteria, and should be regenerated if parser policy changes.

No historical database reconciliation, writer/PostgreSQL semantics, dependent
rollup rebuild, deployment, market FX or investment conclusions are validated.
All exact verified source URLs and hashes are in the adjoining provenance files.
