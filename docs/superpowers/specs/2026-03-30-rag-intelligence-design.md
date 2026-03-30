# GRID RAG Intelligence Layer — Design Spec

**Date:** 2026-03-30
**Status:** Approved for implementation
**Author:** Claude Opus 4.6 + Anik

## Problem

GRID has 13GB+ of downloaded datasets, 1.6M actors, 810K ICIJ entities, 7,500+ analytical snapshots, and growing. No LLM can hold this in context. We need retrieval-augmented generation so any query ("Who has offshore connections to Pfizer board members?") searches the full intelligence corpus and synthesizes an answer with citations and confidence labels.

## Architecture

```
User query
    ↓
Embed query (sentence-transformers or Qwen)
    ↓
pgvector similarity search (top-K chunks)
    + metadata filters (confidence, category, date range)
    ↓
Rerank by relevance + trust score
    ↓
LLM (Qwen / GPT / Claude via MCP) + retrieved context
    ↓
Structured answer with citations + confidence labels
```

## Approach: pgvector in PostgreSQL

**Why pgvector:**
- Zero new infrastructure — PostgreSQL already running
- Embeddings live alongside data — hybrid SQL + vector queries in one statement
- HNSW index for fast ANN search (sub-100ms on millions of vectors)
- Filter by confidence, category, date, actor in same query
- 24GB RAM handles millions of 384-dim vectors fine

**Not chosen:**
- ChromaDB/Qdrant/Weaviate — another service, more RAM, overkill at current scale
- FAISS — no persistence, doesn't integrate with SQL filters

## Components

### 1. Schema Extension

```sql
CREATE EXTENSION IF NOT EXISTS vector;

-- Embeddings table (stores vectors for any content type)
CREATE TABLE intelligence_embeddings (
    id          BIGSERIAL PRIMARY KEY,
    source_type TEXT NOT NULL,        -- 'snapshot', 'actor', 'icij', 'prediction', 'news', 'filing'
    source_id   TEXT NOT NULL,        -- reference to source record
    chunk_text  TEXT NOT NULL,        -- the text that was embedded
    embedding   vector(384) NOT NULL, -- 384-dim from all-MiniLM-L6-v2
    metadata    JSONB DEFAULT '{}',   -- category, ticker, confidence, date, etc.
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_embeddings_vector ON intelligence_embeddings
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

CREATE INDEX idx_embeddings_source ON intelligence_embeddings (source_type, source_id);
CREATE INDEX idx_embeddings_metadata ON intelligence_embeddings USING gin (metadata);
```

### 2. Embedding Model

**Primary:** `all-MiniLM-L6-v2` via sentence-transformers (384 dimensions)
- 80MB model, runs on CPU, ~1000 embeddings/sec
- Already referenced in `subnet/semantic_scorer.py`
- Install: `pip install sentence-transformers`

**Fallback:** TF-IDF vectors (already built in semantic_scorer.py)
- Lower quality but zero dependencies
- Use when sentence-transformers unavailable

**Future:** Qwen embeddings via llamacpp
- Higher quality, GPU-accelerated
- But competes with inference for VRAM

### 3. Indexer (`intelligence/rag_indexer.py`)

Processes each data source into chunks and embeddings:

```python
class RAGIndexer:
    def __init__(self, engine, model='all-MiniLM-L6-v2'):
        self.engine = engine
        self.model = SentenceTransformer(model)

    def index_snapshots(self, batch_size=100):
        """Index analytical_snapshots — completed research results."""
        # Chunk: each snapshot payload is one chunk
        # Metadata: category, subcategory, created_at, task_id

    def index_actors(self, batch_size=500):
        """Index actors — name + title + category + connections."""
        # Chunk: "{name}: {title}. Category: {category}. Tier: {tier}."
        # Metadata: tier, category, influence_score

    def index_icij(self, batch_size=1000):
        """Index ICIJ entities from CSV files."""
        # Chunk: "{entity_name} ({jurisdiction}). Officers: {officers}. Connected to: {connections}."
        # Metadata: jurisdiction, entity_type, leak_source

    def index_predictions(self):
        """Index oracle predictions with outcomes."""
        # Chunk: "{ticker} {direction} conf={confidence}. Model: {model}. Verdict: {verdict}."
        # Metadata: ticker, model, verdict, pnl

    def index_news(self):
        """Index Reuters financial news articles."""
        # Chunk: headline + first 500 chars of body
        # Metadata: date, tickers mentioned

    def index_datasets(self):
        """Index downloaded bulk datasets (congressional trades, sanctions, etc.)."""
        # Parse CSV/JSON, create chunks per record

    def index_all(self):
        """Full reindex — run nightly or on demand."""
```

**Chunking strategy:**
- Analytical snapshots: entire payload as one chunk (already research-sized)
- Actors: name + title + category as one chunk
- ICIJ entities: entity + officers + jurisdiction as one chunk
- News articles: headline + first 500 chars
- SEC filings: section-level chunks (risk factors, MD&A, etc.)
- Max chunk size: 512 tokens (matches MiniLM context window)

### 4. Retriever (`intelligence/rag_retriever.py`)

```python
class RAGRetriever:
    def __init__(self, engine, model='all-MiniLM-L6-v2'):
        self.engine = engine
        self.model = SentenceTransformer(model)

    def search(
        self,
        query: str,
        top_k: int = 10,
        source_types: list[str] | None = None,
        min_confidence: float | None = None,
        date_after: date | None = None,
        ticker: str | None = None,
    ) -> list[dict]:
        """Hybrid vector + metadata search."""
        query_embedding = self.model.encode(query)

        # Build SQL with pgvector similarity + metadata filters
        # SELECT *, 1 - (embedding <=> :query_vec) AS similarity
        # FROM intelligence_embeddings
        # WHERE source_type = ANY(:types)
        #   AND metadata->>'confidence' >= :min_conf
        #   AND created_at > :date_after
        # ORDER BY embedding <=> :query_vec
        # LIMIT :top_k

    def search_and_rerank(self, query, top_k=10, **filters):
        """Search then rerank by trust score * similarity."""
        results = self.search(query, top_k=top_k * 3, **filters)
        # Rerank: score = similarity * trust_weight
        # trust_weight from confidence label:
        #   confirmed=1.0, derived=0.8, estimated=0.6, rumored=0.4, inferred=0.3
```

### 5. RAG Endpoint (`/api/v1/intel/ask`)

```python
@router.post("/ask")
async def intel_ask(
    query: str,
    sources: list[str] | None = None,  # filter by source type
    ticker: str | None = None,
    max_context: int = 5,
) -> dict:
    """Natural language query against all GRID intelligence.

    Retrieves relevant chunks, builds context, sends to LLM,
    returns structured answer with citations and confidence.
    """
    # 1. Retrieve top-K relevant chunks
    # 2. Build context string with citations
    # 3. Send to Qwen (or route to user's preferred LLM)
    # 4. Parse response, attach citations
    # 5. Return { answer, citations: [{source, text, confidence}], query_time_ms }
```

**Tier:** PRO ($200/month) — this is the premium endpoint.

### 6. MCP Integration

Add `grid_ask(query)` tool to `mcp_server.py`:
- Same as `/api/v1/intel/ask` but via MCP protocol
- Any LLM with GRID MCP server can query the intelligence corpus

### 7. Indexing Schedule

| Data Source | Frequency | Est. Chunks | Priority |
|-------------|-----------|-------------|----------|
| analytical_snapshots | Every 30 min | 7,500+ | High |
| actors | Daily | 1.6M (top 50K by tier) | High |
| ICIJ entities | Once (bulk load) | 810K | High |
| oracle_predictions | Every hour | 1,200+ | Medium |
| OpenSanctions | Daily | ~500K | Medium |
| Congressional trades | Daily | ~5K | Medium |
| Reuters news | Once (bulk load) | 106K | Medium |
| SEC filings | Weekly | Varies | Lower |
| Downloaded datasets | Once per dataset | Varies | Lower |

**Total estimated vectors:** ~600K initially (indexing top-tier actors + all snapshots + ICIJ + predictions). Grows to 2-3M as more datasets are indexed.

**Storage:** 384 dims × 4 bytes × 600K vectors = ~920MB. With HNSW index overhead: ~2GB. Fits easily in RAM.

## Data Flow

```
1. New data arrives (Hermes pulls, drainer processes, datasets downloaded)
        ↓
2. RAG Indexer runs (scheduled or triggered)
        ↓
3. Text chunked → embedded → stored in intelligence_embeddings
        ↓
4. User/LLM queries via /intel/ask or MCP grid_ask()
        ↓
5. Query embedded → pgvector ANN search → metadata filter → rerank by trust
        ↓
6. Top-K chunks assembled as context → LLM generates answer
        ↓
7. Response with citations, confidence labels, source provenance
```

## Success Criteria

1. Sub-500ms query response time for top-10 retrieval
2. Relevant chunks appear in top-5 for domain-specific queries
3. `/intel/ask` returns answers with verifiable citations
4. Trust-weighted reranking prefers confirmed sources over rumored
5. Indexer processes 1000+ chunks/second on CPU

## Implementation Order

1. `CREATE EXTENSION vector` + schema migration
2. `intelligence/rag_indexer.py` — indexer for snapshots + actors
3. `intelligence/rag_retriever.py` — search + rerank
4. `/api/v1/intel/ask` endpoint
5. `grid_ask()` MCP tool
6. Index ICIJ bulk CSV
7. Index downloaded datasets
8. Wire indexer into Hermes scheduler (periodic reindex)

## Dependencies

- `pip install pgvector sentence-transformers` on server
- PostgreSQL 15 with `CREATE EXTENSION vector` (requires superuser once)
- ~2GB additional DB storage for vectors + index
- CPU for embedding generation (GPU optional but not required)

## Non-Goals (for now)

- Multi-modal RAG (images, PDFs) — text only
- Fine-tuned embedding model — use off-the-shelf MiniLM
- Streaming responses — batch first, stream later
- User-specific context — all users see same intelligence (tier-gated)
