# R3 NVIDIA commitment ledger — record schema (one JSON object per line)

Write records as JSON Lines (one object per line, UTF-8). Every field is required; use `null` for unknown, never a guess.

```json
{
  "id": "short-kebab-id, unique, e.g. nvda-10q-q2fy27-purchase-obligations",
  "record_kind": "commitment | financial_fact",
  "stage": "announced_intention | capacity_plan | purchase_commitment | order_backlog | delivery | recognized_revenue | cash_receipt | cancellation | nvidia_investment | nvidia_obligation | financing | financial_fact",
  "direction": "demand | supply | infrastructure | financing | nvidia_financials",
  "issuer": "who published the document (e.g. NVIDIA Corporation, OpenAI, CoreWeave Inc.)",
  "counterparty": "the other party, or null",
  "source": {
    "url": "exact URL of the primary document (SEC Archives URL for filings, issuer newsroom URL for press releases)",
    "doc_type": "10-K | 10-Q | 8-K | 8-K EX-99.1 | 6-K | S-1 | press_release | earnings_call_transcript | blog | other",
    "accession": "SEC accession number like 0001045810-26-000123, or null",
    "section": "note/item/page/heading where the quote is",
    "quote": "EXACT verbatim text from the document, max ~60 words, that supports the number",
    "published_at": "ISO date/time the document became public (SEC acceptance datetime if available, else filing date, else release date)",
    "retrieved_at": "ISO UTC time you fetched it",
    "retrieved_sha256": "sha256 of the raw bytes you downloaded, or null if you only had a rendered/processed view"
  },
  "fiscal_period": "e.g. Q2 FY2027 (ended 2026-07-26), or null",
  "amount": {
    "value": 12.3,
    "currency": "USD",
    "units": "billions | millions | GW | units | percent | null",
    "basis": "nvidia_revenue | nvidia_obligation | nvidia_investment | counterparty_capex | whole_project | counterparty_commitment_to_nvidia | balance_sheet | cash_flow | unknown"
  },
  "timing": {"start": "ISO date or null", "end": "ISO date or null", "text": "timing language as stated, or null"},
  "conditions": ["verbatim or close-paraphrase conditions/contingencies (e.g. 'non-binding LOI', 'subject to definitive agreement', 'progressive per GW deployed')"],
  "contradictions": ["any conflicting statement found elsewhere, with its URL"],
  "circularity": "none | nvidia_investment_in_customer | vendor_financing | capacity_backstop | lease_guarantee | unknown",
  "dedup_hint": "free text naming other records that describe the same dollars, or null",
  "unknowns": ["what the document does NOT say that matters (e.g. 'split between NVIDIA hardware and other capex not stated')"],
  "confidence": "primary_verbatim | primary_paraphrase | secondary_only"
}
```

Rules:
- Primary sources only for numbers: SEC EDGAR filings (www.sec.gov/Archives/...), issuer/counterparty official newsrooms or IR pages. News articles may be used ONLY to find a primary source, and a record whose only source is news gets `confidence: secondary_only`.
- `quote` must be copied exactly. If you cannot get exact text, set confidence `primary_paraphrase` and say so in `unknowns`.
- Do not convert units, do not sum, do not annualize. Record the number as stated.
- Whole-project spend (e.g. "a $500B data-center program") is `whole_project`, never `nvidia_revenue`.
- Record negative findings too: if a widely reported number is NOT in the primary source, add a record with `amount.value: null` and say so in `unknowns`.
