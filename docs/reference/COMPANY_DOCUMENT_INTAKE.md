# Offline company document intake

This slice archives already-downloaded SEC filings, issuer/counterparty documents,
and user-supplied source material for **any explicitly identified company**. It
complements #1050's numeric SEC snapshots and #1051's AV transcript lineage; it
neither replaces those collectors nor changes their financial projections.

## Run

Create a UTF-8 JSON metadata file with exactly these fields:

```json
{
  "company_id": "your-stable-issuer-id",
  "source_kind": "user_supplied",
  "source_url": "https://example.com/original-source",
  "title": "Replace with actual document title",
  "published_at": null,
  "retrieved_at": "2026-11-09T00:00:00Z",
  "media_type": "text/plain"
}
```

The example is a template, not evidence. Use actual retrieval time. Record a
publication timestamp only when supported by source evidence; date-only metadata
must remain unknown rather than inventing a midnight time. Source kinds are
`sec_filing`, `issuer`, `counterparty`, `user_supplied`. SEC locators must use
sec.gov. HTTPS locators identify sources; no URL is fetched or authenticated.
Private user documents can use their existing authorized HTTPS source locator;
no upload or sharing occurs. Do not include credentials in metadata or sources.

```sh
python scripts/ingest_company_document.py --metadata metadata.json \
  --document filing.html --archive /private/research/company-documents
```

Keep the archive outside git and in an access-controlled, trusted local directory.
The tool makes no network, database or model calls. Maximum input is 16 MiB, and
metadata is bounded to 64 KiB. It reads input bytes unchanged and publishes one
complete content-addressed JSON object using atomic no-replace hardlink creation.
An identical retry reuses the object; any metadata or byte revision gets its own
snapshot ID. No history is overwritten. The filesystem must support hardlinks.

## Consumer contract

Use `ingestion.company_documents.read_document(root, snapshot_id)` before consuming
an envelope. It recomputes the raw-byte digest, parsed text, text digest, normalized
metadata and complete snapshot ID. A hash detects corruption/substitution relative
to the requested ID, not lying publishers or an attacker replacing your whole index.

`build_document(metadata, bytes)` is the pure construction seam;
`ingest_document(root, metadata, bytes)` stores it. The JSON schema is
`company-document-v1`; parser version is `company-text-v1`. A parser change must
use a new version and a reader capable of preserving old versions, not reinterpret
an old snapshot in place. There is deliberately no mutable latest pointer.

Raw bytes (base64), content SHA256, byte length, text, text SHA256, publication and
retrieval dates all travel together. UTF-8 plain text and basic HTML are parsed.
HTML scripts/styles are excluded from the derived navigation text, and cell/block
boundaries are separated. This is **not** a table, XBRL or financial fact parser.
Use the original source to verify quotations, units and table context. Unsupported
PDF/binary or non-UTF-8 data are still archived and explicitly unparsed, never
silently decoded or represented as complete evidence. Empty text is also explicit.

`availability(doc, as_of=..., max_age_days=...)` returns `available`, `stale`,
`not_yet_observed`, `unknown_publication`, `not_yet_published`, or the parse failure.
Both retrieval and publication must be at/before the as-of cutoff. Downloading an
old filing today does not create an observation from last year. Freshness is a
caller-specified age policy, **not** proof that all latest filings were collected.

## Remaining pipes and qualification

This is a local intake primitive, not a live end-to-end research service. Needed:
SEC submissions discovery and complete filing/exhibit acquisition; actual published
metadata verification; issuer/counterparty source discovery; PDF extraction with
page locators; XBRL/table unit and fiscal-period handling; entity resolution;
coverage inventory and refresh jobs; source-bound claim/commitment extraction;
conflict/dedup review; explicit actual-event dates; independent source validation.
Do not turn an available document into a verified assertion, revenue prediction,
complete-company badge or trade signal. Claims and coverage require separate gates.
No paid providers, production writes, migration, scheduler, deployment or repair is
included. Fixtures are labeled synthetic; no invented company facts are shipped.

## Tests

```sh
python -m pytest tests/test_company_documents.py -q
python -m ruff check ingestion/company_documents.py scripts/ingest_company_document.py tests/test_company_documents.py
```

Tests block socket connections and cover revisions, round-trip integrity, concurrent
idempotence, corruption, bounds, dates, parsing failures and temporal availability.
