# PageIndex Integration Plan

**Date:** 2026-04-09
**Status:** Proposal
**Repo:** [VectifyAI/PageIndex](https://github.com/VectifyAI/PageIndex)

## Problem Statement

GRID's current RAG stack (pgvector + sentence-transformers / [[Ollama]] nomic-embed-text) excels at fast similarity search over pre-chunked text but has two blind spots:

1. **No PDF ingestion** — SEC filings (10-K, 10-Q, [[Institutional Flows|13F]]), [[FOIA]] cables, earnings transcripts, and diplomatic documents arrive as PDFs. We have no way to parse and reason over them natively.
2. **Structural reasoning** — Vector similarity treats all chunks as flat bags of meaning. For structured financial documents, "What was the change in operating margin between Q3 and Q4?" requires navigating a document hierarchy, not matching embedding distance.

PageIndex solves both: it builds a hierarchical tree from document structure (TOC, headings, sections), then uses LLM reasoning — not vector similarity — to navigate to the right pages. It achieved **98.7% accuracy on FinanceBench**, a financial document QA benchmark.

## Architecture: Hybrid Pipeline

PageIndex does **not** replace the existing RAG. It becomes a **structured document pre-processor** that feeds into the existing pgvector pipeline.

```
                                  +------------------+
                                  |  GRID pgvector   |
                                  |  RAG Pipeline    |
                                  |  (fast queries)  |
                                  +--------^---------+
                                           |
                              structured sections + metadata
                                           |
+-----------+     +------------------+     +------------------+
|  PDF/Doc  | --> |  PageIndex       | --> |  Section Store   |
|  Sources  |     |  Tree Builder    |     |  (Postgres JSONB)|
+-----------+     +------------------+     +------------------+
                         |                          |
                    tree index JSON           on-demand LLM
                    (cached per doc)          tree traversal
                                             for complex queries
```

### Two retrieval paths

| Path | When | Latency | Cost |
|------|------|---------|------|
| **Fast path** (existing) | Simple keyword/semantic queries across all intelligence data | <100ms | ~0 (local embeddings) |
| **Deep path** (PageIndex) | Complex, multi-hop questions over specific documents | 2-10s | LLM API tokens |

The router decides which path based on query complexity — simple queries go fast path, document-specific reasoning queries go deep path.

## Integration Phases

### Phase 1: PDF Ingestion Pipeline

**Goal:** Parse PDFs into structured markdown, build PageIndex trees, cache them.

**New module:** `rag/pageindex_bridge.py`

```python
# Core interface
class PageIndexBridge:
    """Bridge between GRID's document sources and PageIndex tree builder."""

    async def index_pdf(self, pdf_path: Path, source_type: str, source_id: str) -> TreeIndex
    async def index_markdown(self, content: str, source_type: str, source_id: str) -> TreeIndex
    def get_cached_index(self, source_id: str) -> TreeIndex | None
```

**Dependencies to add:**
```
pageindex>=0.1.0       # VectifyAI/PageIndex
litellm>=1.0.0         # Multi-LLM provider (PageIndex dependency)
pymupdf>=1.24.0        # PDF text extraction (fast, accurate)
```

**Storage:** Tree index JSON stored in new `document_trees` table:
```sql
CREATE TABLE document_trees (
    id          BIGSERIAL PRIMARY KEY,
    source_type TEXT NOT NULL,          -- filing, cable, transcript, report
    source_id   TEXT NOT NULL UNIQUE,
    tree_json   JSONB NOT NULL,         -- PageIndex hierarchical tree
    page_count  INTEGER,
    metadata    JSONB DEFAULT '{}',
    indexed_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_doctrees_source ON document_trees (source_type, source_id);
```

**Tasks:**
- [ ] Install PageIndex + pymupdf dependencies
- [ ] Create `rag/pageindex_bridge.py` with async index/query methods
- [ ] Create `document_trees` migration
- [ ] Wire LLM calls through `llm/router.py` (use REASON tier for tree building, LOCAL for simple extraction)
- [ ] Add tree index caching (skip re-indexing if PDF hash unchanged)

### Phase 2: Document Source Connectors

**Goal:** Connect PageIndex to GRID's existing document sources.

**Priority document types (by intelligence value):**

| Source | Type | Volume | Current State |
|--------|------|--------|---------------|
| SEC 10-K/10-Q filings | PDF | ~50/quarter (tracked tickers) | `edgartools` fetches but no PDF parsing |
| SEC 13F holdings | PDF/HTML | ~200/quarter | `institutional_flows.py` parses some fields |
| FOIA diplomatic cables | PDF | ~100 in corpus | `foia_cables.py` exists, text-only |
| Earnings transcripts | PDF/text | ~50/quarter | `edgar_transcripts.py` fetches text |
| Fed speeches/minutes | PDF | ~8/month | `fed_speeches.py` fetches text |
| FARA registrations | PDF | ~500/year | `fara.py` fetches structured data |

**New module:** `ingestion/document_indexer.py`

```python
async def index_sec_filing(ticker: str, filing_type: str, accession: str) -> int:
    """Fetch SEC filing PDF, build PageIndex tree, extract sections into RAG."""

async def index_foia_cable(cable_id: str, pdf_path: Path) -> int:
    """Index a declassified cable with full structural awareness."""

async def index_earnings_transcript(ticker: str, quarter: str) -> int:
    """Index earnings call transcript for structured Q&A retrieval."""
```

**Tasks:**
- [ ] Extend `edgar_transcripts.py` to fetch PDF versions when available
- [ ] Create `ingestion/document_indexer.py` orchestrator
- [ ] Add [[Hermes Scheduler|Hermes scheduler]] jobs for document indexing (daily, after market close)
- [ ] Backfill existing corpus (~500 documents)
- [ ] Feed extracted sections into existing `index_document()` for pgvector fast path

### Phase 3: Hybrid Query Router

**Goal:** Automatically route queries to the right retrieval path.

**New module:** `rag/query_router.py`

```python
class QueryRouter:
    """Routes queries to fast (pgvector) or deep (PageIndex) retrieval."""

    def classify_query(self, query: str) -> Literal["fast", "deep", "both"]:
        """Classify query complexity.

        fast: "What is Apple's current P/E ratio?"
        deep: "Compare Apple's R&D spending growth in 10-K FY2024 vs FY2025"
        both: "Which tracked companies increased capex?" (fast scan + deep verify)
        """

    async def retrieve(self, query: str, source_filter: dict | None = None) -> list[RetrievalResult]:
        """Unified retrieval interface — routes internally."""
```

**Routing heuristics:**
- References specific documents/filings -> deep path
- Cross-section comparison queries -> deep path
- Page/section references -> deep path
- Broad corpus search -> fast path
- Actor/entity lookup -> fast path
- Ambiguous -> fast path first, escalate to deep if low confidence

**Tasks:**
- [ ] Create `rag/query_router.py` with classification logic
- [ ] Add query complexity classifier (rule-based first, LLM-assisted later)
- [ ] Unified `RetrievalResult` dataclass that works for both paths
- [ ] Update `intelligence/rag.py` `RAGIndexer.search()` to use router
- [ ] Add retrieval path to API response metadata (for transparency)

### Phase 4: Intelligence Layer Integration

**Goal:** Wire PageIndex into GRID's intelligence modules that would benefit most.

**High-value integrations:**

1. **[[Cross Reference|Cross-Reference]] [[Cross Reference|lie detector]]** (`intelligence/cross_reference.py`)
   - Deep-query Fed minutes and [[BLS]] methodology PDFs to check if government stats align with the underlying methodology described in source documents
   - Example: "Does the CPI methodology PDF describe the substitution adjustment that explains this divergence?"

2. **[[Actor Network]] enrichment** (`intelligence/actor_network.py`)
   - Extract board memberships, compensation, and ownership from 10-K/proxy PDFs
   - Currently hardcoded 495 actors — PageIndex enables dynamic extraction from filings

3. **[[Postmortem]] analysis** (`intelligence/postmortem.py`)
   - When a trade fails, pull the specific sections from relevant filings that were available at decision time ([[PIT Store|PIT-correct]] document retrieval)
   - "What did the 10-Q say about [[Supply Chain|supply chain]] risk in the quarter before the miss?"

4. **[[Causation]] tracing** (`intelligence/causation.py`)
   - Link market moves to specific disclosures found in documents
   - "Trace the 8% drop to the specific risk factor disclosed in the 10-K"

**Tasks:**
- [ ] Add `pageindex_query()` helper to `intelligence/rag.py`
- [ ] Integrate deep retrieval into [[Cross Reference|cross-reference]] verification flow
- [ ] Add filing-aware actor extraction to `actor_network.py`
- [ ] Wire [[PIT Store|PIT-correct]] document retrieval into [[Postmortem|postmortem]] (use `release_date` filtering)
- [ ] Add document-backed evidence to [[Causation|causation]] chains

### Phase 5: API + Frontend

**Goal:** Expose document intelligence through the API and PWA.

**API endpoints:**

```
POST /api/documents/index          # Trigger indexing for a document
GET  /api/documents/{source_id}    # Get document tree structure
POST /api/documents/query          # Deep query a specific document
GET  /api/documents/search         # Search across indexed documents
```

**Frontend integration points:**
- **WhyView** (`pwa/src/WhyView.jsx`) — Add "Source Documents" panel showing PageIndex-retrieved evidence with page numbers
- **Timeline** (`pwa/src/Timeline.jsx`) — Link events to source document sections
- **[[Intel Dashboard View|IntelDashboard]]** — Document search with tree navigation
- New **DocumentViewer** component — renders document tree with expandable sections and highlighted evidence

**Tasks:**
- [ ] Add `api/routers/documents.py` with CRUD + query endpoints
- [ ] Add document search to existing `/api/intelligence/query` endpoint
- [ ] Build `DocumentViewer.jsx` component with tree navigation
- [ ] Integrate document evidence into WhyView and Timeline
- [ ] Add document indexing status to health endpoint

## LLM Routing Strategy

PageIndex needs LLM calls for tree building and reasoning. Map to GRID's existing 3-tier system:

| PageIndex operation | GRID LLM tier | Model | Rationale |
|---------------------|---------------|-------|-----------|
| TOC extraction | LOCAL | Nemotron-Cascade 30B | Structured extraction, runs frequently |
| Section summarization | LOCAL | Nemotron-Cascade 30B | Batch operation, cost-sensitive |
| Tree verification/fix | REASON | Nemotron-Super 120B | Needs reasoning for correction cycles |
| Deep query traversal | REASON | Nemotron-Super 120B | Core reasoning task |
| Complex financial QA | ORACLE | Claude (OpenRouter) | Highest accuracy needed for trading decisions |

Configure via `llm/router.py` — PageIndex uses LiteLLM internally, so we wrap GRID's router as a LiteLLM custom provider.

## PIT Correctness

Document retrieval MUST respect [[PIT Store|point-in-time]] constraints:

- `document_trees.indexed_at` tracks when we indexed the document
- `metadata.filing_date` / `metadata.release_date` tracks when the document became public
- All intelligence queries that use PageIndex results must pass `as_of` timestamp
- `assert_no_lookahead()` applies to document-sourced evidence — a 10-K filed on 2026-02-15 cannot be used in a backtest decision dated 2026-02-14
- Backfill indexing must preserve original `release_date`, not use current timestamp

## Cost & Performance Budget

| Metric | Target | Notes |
|--------|--------|-------|
| Tree build (per document) | <30s, ~$0.02 | One-time cost, cached |
| Deep query | <5s, ~$0.005 | Per query, uses LOCAL/REASON tier |
| Storage (per document) | ~50KB tree JSON | Negligible vs pgvector embeddings |
| Daily indexing budget | <$1.00 | ~50 new documents/day |
| Backfill (500 docs) | ~$10, ~4 hours | One-time, parallelizable |

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| PageIndex is early-stage (v0.x) | Breaking API changes | Pin version, wrap in bridge module, minimal coupling |
| LLM cost overrun on deep queries | Budget blow-out | Rate limit deep path, cache tree traversals, prefer LOCAL tier |
| PDF parsing failures (scanned docs) | Missing data | Fallback to OCR (pymupdf has built-in), log failures for manual review |
| Latency on deep path too high for real-time | UX degradation | Async queries with streaming, pre-compute for tracked tickers |
| Tree quality varies by document structure | Bad retrieval | Validate tree completeness, fall back to flat chunking for poorly-structured docs |

## Success Criteria

1. **PDF ingestion works** — Can index a 10-K PDF and retrieve specific sections by query
2. **Accuracy improvement** — Financial document QA accuracy >90% (vs current ~70% with flat chunking)
3. **Latency acceptable** — Deep path <5s p95, fast path unchanged
4. **PIT-correct** — All document retrievals respect temporal boundaries
5. **Cost-controlled** — <$1/day for ongoing indexing, <$0.01/deep query

## Dependencies

```
# New in requirements.txt
pageindex>=0.1.0
litellm>=1.0.0
pymupdf>=1.24.0
```

No conflicts with existing dependencies. LiteLLM's `openai` dependency overlaps with our existing `openai>=1.12.0` — compatible.

## Implementation Priority

```
Phase 1 (Week 1)     — PDF pipeline + tree storage     [foundation]
Phase 2 (Week 2)     — SEC filing connectors            [data flow]
Phase 3 (Week 2-3)   — Query router                     [usability]
Phase 4 (Week 3-4)   — Intelligence integration         [value delivery]
Phase 5 (Week 4+)    — API + Frontend                   [user-facing]
```

Phase 1-2 deliver standalone value (searchable PDFs). Phase 3-5 multiply existing intelligence capabilities.
