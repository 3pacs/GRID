# Obsidian Bridge — Bidirectional Knowledge Layer

**Date:** 2026-04-04
**Status:** Approved
**Scope:** Connect Obsidian vault to GRID as a shared human+agent working memory

---

## Problem

Obsidian is write-only. The bookmark triage pipeline dumps markdown into the vault, but GRID's agents, API, and frontend never read from it. The vault has curated intel across 5 domains (Pipeline, Tools, Alpha, Intel, GRID notes) that's invisible to the system. Human edits in Obsidian have no effect on GRID behavior.

## Solution

Bidirectional sync between Obsidian vault and Postgres. Obsidian is the human interface, Postgres is the agent interface. An active agent loop runs every Hermes cycle to react, enrich, create, and act on vault content.

---

## 1. Data Model

### Table: `obsidian_notes`

| Column | Type | Purpose |
|--------|------|---------|
| `id` | SERIAL PK | Internal ref |
| `vault_path` | TEXT UNIQUE | Relative path in vault (e.g., `02-Tools/Firecrawl.md`) |
| `domain` | TEXT | `pipeline`, `tools`, `alpha`, `intel`, `grid`, `dashboard` |
| `status` | TEXT | `inbox`, `evaluating`, `approved`, `rejected`, `active`, `archived` |
| `title` | TEXT | Note title |
| `content_hash` | TEXT | SHA-256 of file content (detect changes) |
| `frontmatter` | JSONB | Parsed YAML frontmatter |
| `body` | TEXT | Full markdown body |
| `body_tsvector` | TSVECTOR | Postgres full-text search index |
| `agent_flags` | JSONB | Agent metadata: `{"needs_human_review": true, "priority": "high", ...}` |
| `synced_at` | TIMESTAMPTZ | Last sync from vault |
| `modified_at` | TIMESTAMPTZ | File mtime from disk |
| `created_at` | TIMESTAMPTZ | First seen |

### Table: `obsidian_actions` (immutable audit log)

| Column | Type | Purpose |
|--------|------|---------|
| `id` | SERIAL PK | |
| `note_id` | FK -> obsidian_notes | Which note |
| `actor` | TEXT | `hermes`, `claude`, `user`, `triage_pipeline` |
| `action` | TEXT | `created`, `updated`, `status_changed`, `flagged`, `acted_on` |
| `detail` | JSONB | What changed and why |
| `created_at` | TIMESTAMPTZ | When |

### Indexes

```sql
CREATE INDEX idx_obsidian_notes_domain ON obsidian_notes(domain);
CREATE INDEX idx_obsidian_notes_status ON obsidian_notes(status);
CREATE INDEX idx_obsidian_notes_fts ON obsidian_notes USING gin(body_tsvector);
CREATE INDEX idx_obsidian_notes_agent_flags ON obsidian_notes USING gin(agent_flags);
CREATE INDEX idx_obsidian_actions_note_id ON obsidian_actions(note_id);
CREATE INDEX idx_obsidian_actions_created ON obsidian_actions(created_at DESC);
```

### Frontmatter Contract

Every vault note gets standardized YAML frontmatter:

```yaml
---
title: Firecrawl
domain: tools
status: approved
tags: [scraping, intelligence-pipeline]
confidence: confirmed
priority: medium
last_agent: hermes
last_synced: 2026-04-05T12:00:00Z
---
```

Notes without frontmatter get it auto-generated from path and content on first sync.

---

## 2. Sync Engine

**Module:** `ingestion/altdata/obsidian_sync.py`

Runs every Hermes cycle (5 min). Two-way sync.

### Vault -> Postgres (inbound)

1. Walk `~/Documents/Obsidian Vault/` recursively, skip `.obsidian/`
2. For each `.md` file: compute SHA-256, compare to `content_hash` in DB
3. New file -> INSERT, parse frontmatter + body, assign domain from path prefix
4. Changed file -> UPDATE body, frontmatter, content_hash, synced_at
5. Deleted file -> mark `archived` (never hard-delete from DB)
6. Log every change to `obsidian_actions`

### Domain mapping from path

| Path prefix | Domain |
|-------------|--------|
| `00-DASHBOARD` | `dashboard` |
| `01-Pipeline/` | `pipeline` |
| `02-Tools/` | `tools` |
| `03-Alpha/` | `alpha` |
| `04-Intel/` | `intel` |
| `05-GRID/` | `grid` |

### Postgres -> Vault (outbound)

1. Query `obsidian_notes` where `agent_flags->>'pending_write' = 'true'`
2. For each pending write:
   - New note -> write markdown file to correct domain folder, clear flag
   - Updated note -> overwrite file content, clear flag
   - Status change -> update frontmatter in file
3. Regenerate `00-DASHBOARD.md` if anything changed

### Conflict Resolution

File mtime wins. If both sides changed since last sync, vault version takes precedence (human overrides agent). Agent change logged as `conflict_deferred` in actions table.

---

## 3. Agent Interface

**Module:** `intelligence/obsidian_agent.py`

Active decision-maker, not CRUD. Runs as Hermes cycle step.

### Cycle Steps

```
1. SYNC        — vault <-> postgres (detect changes from either side)
2. REACT       — process changes (new notes, status changes, human edits)
3. ENRICH      — cross-reference new content against GRID intelligence
4. PRIORITIZE  — rank what needs human attention, flag it
5. ACT         — execute downstream effects of approved items
6. CREATE      — proactively write new notes from GRID discoveries
7. LEARN       — update preferences from approval/rejection patterns
```

### REACT: Processing Changes

- New note in pipeline/inbox -> run multi-LLM triage (reuse existing bookmark triage logic)
- Status changed to `approved`:
  - Tool -> log to compute stack candidates, create integration task in Hermes backlog
  - Alpha -> generate trade ticket using template, create baseline prediction entry in `oracle_predictions`
  - Intel -> enrich relevant actors in `actors` table, update trust scores
- Status changed to `rejected` -> update learning preferences
- Human edited a note -> re-parse, update cross-references

### ENRICH: Cross-Reference

For every new or changed note:
- Extract entities (tickers, people, companies, events) from body text
- Check against `actors` table — link known actors
- Check against `signal_data` — find correlated signals
- Check against `oracle_predictions` — find related predictions
- Append `## Cross-References` section to note body with findings

### PRIORITIZE: Dashboard Surfacing

| Priority | Criteria |
|----------|----------|
| URGENT | Items needing human decision that block downstream actions |
| HIGH | Cross-reference hits (new connection found) |
| MEDIUM | Agent-created notes for review |
| LOW | Status updates, learning log |

### CREATE: Proactive Note Generation

The agent creates notes when GRID systems produce actionable findings:
- Anomaly in dark pool flow -> Alpha note
- Trust score downgrade -> Intel note
- Hypothesis passes backtest -> Alpha note with pre-filled trade ticket
- Regime change detected -> Intel note with historical analogs
- Postmortem on failed prediction -> Intel note with lessons

### LEARN: Preference Tracking

- Track approval/rejection rate by domain, tag, source, relevance score
- Adjust triage thresholds based on patterns (e.g., user rejects tools under relevance 6 -> raise threshold)
- Boost triage scores for Twitter authors whose content gets consistently approved
- Store learned preferences in `05-GRID/agent-preferences.md`

---

## 4. Integration Points

### Hermes Operator

- New cycle step: `_run_obsidian_sync()` after health check, before pipeline work
- Register obsidian as a source in `_SOURCE_REGISTRY`
- Agent logic runs within Hermes cycle, not a separate daemon

### Bookmark Pipeline Rewire

- `ingestion/altdata/bookmarks.py` writes to Postgres `obsidian_notes` instead of directly to vault files
- Sync engine handles Postgres -> vault file generation
- SQLite bookmark DB remains as scraper cache, triage results move to Postgres

### MCP Server Tools

Add to `mcp_server.py`:

| Tool | Purpose |
|------|---------|
| `grid_vault_search(query, domain?, status?)` | FTS across all vault notes |
| `grid_vault_read(path)` | Read a specific note |
| `grid_vault_write(path, content, domain)` | Create or update a note |
| `grid_vault_flag(path, priority, reason)` | Flag for human review |
| `grid_vault_act(path, action)` | Trigger downstream action |

### Intelligence Layer

- `intelligence/actor_discovery.py` — agent finds new actor in note -> auto-link to actor network
- `intelligence/thesis_tracker.py` — approved alpha notes become tracked theses
- `intelligence/trust_scorer.py` — track which vault sources lead to good trades

### API Router

New `api/routers/vault.py`:

```
GET    /api/v1/vault/notes              — list notes (filter by domain, status, priority)
GET    /api/v1/vault/notes/:id          — read single note
POST   /api/v1/vault/notes              — create note
PUT    /api/v1/vault/notes/:id          — update note
PATCH  /api/v1/vault/notes/:id/status   — change status (approve/reject/archive)
GET    /api/v1/vault/search?q=          — full-text search
GET    /api/v1/vault/actions            — audit log
GET    /api/v1/vault/dashboard          — prioritized items for review
POST   /api/v1/vault/sync              — trigger manual sync
```

### Frontend View

New `pwa/src/views/Vault.jsx`:
- Browse notes by domain (tabs: Pipeline, Tools, Alpha, Intel, GRID)
- Filter by status (inbox/evaluating/approved/rejected/active)
- Approve/reject buttons with one click
- Agent activity feed (what it did and why)
- Cross-reference panel (connections found)
- Priority badges on items needing attention
- Search bar with FTS

---

## 5. Dashboard Regeneration

`00-DASHBOARD.md` is rebuilt every sync cycle:

```markdown
# GRID Intelligence Vault

## Needs Your Review
- [URGENT] Tool: TurboQuant — official code released, 3 LLMs recommend approve
- [HIGH] Alpha: NVDA dark pool anomaly — 3x avg volume, cross-refs 2 actor flows

## Recent Agent Actions
- Created: Alpha/nvda-dark-pool-anomaly.md (dark pool scanner trigger)
- Enriched: Tools/Firecrawl.md (added 3 cross-references to scraping signals)
- Acted: Alpha/fed-rate-thesis.md approved -> trade ticket created, prediction logged

## Pipeline Stats
| Domain | Inbox | Evaluating | Approved | Rejected | Active |
|--------|-------|------------|----------|----------|--------|
| Tools  | 1     | 1          | 3        | 0        | 3      |
| Alpha  | 2     | 0          | 1        | 0        | 1      |
| Intel  | 0     | 0          | 4        | 1        | 3      |

## Learning Log
- Raised tool relevance threshold from 5 -> 6 (3 consecutive rejections under 6)
- Boosted @rryssf_ triage score (2 approvals in a row)
```

---

## 6. File Inventory

| File | Purpose | Est. LoC |
|------|---------|----------|
| `schema_obsidian.sql` | Table definitions + indexes | 60 |
| `ingestion/altdata/obsidian_sync.py` | Bidirectional sync engine | 350 |
| `intelligence/obsidian_agent.py` | Active agent loop (react/enrich/prioritize/act/create/learn) | 600 |
| `api/routers/vault.py` | REST API endpoints | 250 |
| `mcp_server.py` (additions) | 5 new MCP tools | 150 |
| `pwa/src/views/Vault.jsx` | Frontend view | 400 |
| `ingestion/altdata/bookmarks.py` (modifications) | Rewire to write Postgres instead of vault | ~50 changed |
| `scripts/hermes_operator.py` (modifications) | Add obsidian sync cycle step | ~30 changed |
| `tests/test_obsidian_sync.py` | Sync engine tests | 200 |
| `tests/test_obsidian_agent.py` | Agent logic tests | 250 |

**Total estimated: ~2,340 new/changed lines**

---

## 7. Constraints

- Default triage uses Groq free tier + local llama.cpp (Hermes z4)
- Paid APIs (Gemini, OpenAI) allowed for high-severity items: cross-reference validation, trade ticket generation, when local LLMs give incoherent answers
- Escalation pattern: local first -> if incoherent, retry with paid -> log cost + reason
- Vault path: `~/Documents/Obsidian Vault/` (configurable via `OBSIDIAN_VAULT_PATH` in config.py, replaces `BOOKMARKS_OBSIDIAN_PATH`)
- Human edits always win in sync conflicts
- All actions logged to `obsidian_actions` (immutable audit trail)
- Notes are never hard-deleted from Postgres (archived only)
- Confidence labels on all agent-generated content (confirmed/derived/estimated/rumored/inferred)
