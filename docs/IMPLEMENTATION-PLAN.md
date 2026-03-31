# GRID Implementation Plan — Remaining High-Impact Items

**Created**: 2026-03-30 (via ECC planner)
**Total Effort**: ~38.5 hours across 4-5 focused sessions
**Status**: PLANNING → CONFIRMED → EXECUTING

---

## Sequencing

| # | Item | Est. Hours | Dependencies | Priority |
|---|------|-----------|--------------|----------|
| 1 | GRID MCP Server | 5-6.5h | None | **HIGHEST** — unblocks everything |
| 2 | Data Freshness Monitoring | 4.5h | Item 1 (uses MCP) | HIGH |
| 3 | Zero-Coverage Module Tests | 23.5h | None (parallel w/ Item 1) | HIGH |
| 4 | Alpha Leaderboard Skill | 5.5h | Items 1 + 3 | MEDIUM |
| 5 | Production Config Validation Hook | 1h | None | MEDIUM |
| 6 | Run /update-codemaps | 0.5h | None | LOW |

---

## Item 1: GRID MCP Server (HIGHEST PRIORITY)

**Purpose**: Wrap grid.stepdad.finance API so Claude queries live trust scores, actor networks, predictions, and data freshness directly.

### Files to Create
- `mcp_server_http.py` — FastMCP server (HTTP client to REST API)
- `api/routers/mcp_export.py` — Internal export endpoints
- `tests/test_mcp_export.py` — Export endpoint tests

### Files to Modify
- `api/main.py` — Add mcp_export router
- `.claude/mcp.json` — MCP server config

### MCP Tools
| Tool | Purpose |
|------|---------|
| `grid_trust_score(actor, window)` | Trust score + provenance |
| `grid_actor_profile(name_or_id)` | Full actor dossier |
| `grid_actor_network(actor, depth)` | Network graph structure |
| `grid_predictions(symbol, lookback)` | Recent predictions |
| `grid_prediction_accuracy(metric_type)` | Accuracy by source/actor/lever |
| `grid_data_freshness()` | Source staleness report |
| `grid_signal_sources(symbol)` | Active signal sources |
| `grid_lever_activity(actor, timeframe)` | Recent actor actions |

### Phases
1. **Export Endpoints** (2-3h) — Build /v1/mcp/* routes
2. **MCP Server** (2h) — FastMCP wrappers + caching
3. **Integration Testing** (1.5h) — JWT flow, caching, degradation

### Risks
- API rate limiting → backoff
- JWT lifecycle → cache + refresh
- Network latency → 5s timeout
- Cold startup → degraded response

---

## Item 2: Scheduled Data Freshness Monitoring

**Purpose**: Daily check of 37+ source freshness, NaN accumulation, API key availability.

### Files to Create
- `data_freshness_monitor.py` — Monitor logic
- `tests/test_data_freshness_monitor.py` — Tests
- Scheduled task config (Claude Code)

### Files to Modify
- `schema.sql` — Add data_source_health table
- `alerts/email.py` — Add freshness alert template

### Database Schema
```sql
CREATE TABLE data_source_health (
    id SERIAL PRIMARY KEY,
    source_id INT NOT NULL REFERENCES source_catalog(id),
    check_timestamp TIMESTAMPTZ NOT NULL,
    days_old NUMERIC(5,2),
    nan_percentage NUMERIC(5,2),
    status VARCHAR(20), -- healthy/stale/critical/error
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_dsh_check ON data_source_health(source_id, check_timestamp DESC);
```

### Phases
1. **Monitor Logic** (2h) — Query each source, compute staleness
2. **DB + Alerting** (1.5h) — Schema, storage, email integration
3. **Scheduled Task** (1h) — Daily @ 08:00 UTC

---

## Item 3: Zero-Coverage Module Tests

**Target**: 8 critical modules → 80%+ coverage each

| Module | Test File | Key Scenarios |
|--------|-----------|---------------|
| `normalization/resolver.py` | `test_resolver.py` (EXPAND) | Div-by-zero, NaN, source priority |
| `normalization/entity_map.py` | `test_entity_map.py` (NEW) | Fuzzy match, case insensitive, unmapped |
| `features/lab.py` | `test_feature_lab.py` (EXPAND) | zscore, rolling_slope, NaN propagation |
| `discovery/orthogonality.py` | `test_orthogonality.py` (EXPAND) | PCA, singular matrix, zero-variance |
| `discovery/clustering.py` | `test_clustering.py` (EXPAND) | Regime discovery, O(n²) transition matrix |
| `validation/gates.py` | `test_gates.py` (EXPAND) | CANDIDATE→SHADOW→STAGING→PRODUCTION flow |
| `governance/registry.py` | `test_registry.py` (EXPAND) | State transitions, invalid rejected |
| `inference/live.py` | `test_live_inference.py` (EXPAND) | No lookahead, confidence bounds |

### Phases
1. **Test Infrastructure** (1.5h) — Extend conftest.py fixtures
2. **Per-Module Tests** (~2.5h each, ~20h total) — TDD: RED → GREEN → REFACTOR
3. **Integration + Regression** (2h) — Full suite, coverage report

---

## Item 4: Alpha Leaderboard Skill

**Purpose**: Rank prediction accuracy by source, actor, and lever type.

### Files to Create
- `inference/alpha_leaderboard.py` — Accuracy computation
- `tests/test_alpha_leaderboard.py` — Tests
- `.claude/skills/alpha-leaderboard/SKILL.md` — Skill definition
- `api/routers/leaderboard.py` — API endpoints
- `pwa/src/views/LeaderboardView.jsx` — Frontend

### Metrics
- Win rate (% correct direction)
- Win/loss ratio (avg win / avg loss)
- Hit rate by signal source, actor, lever type
- Confidence intervals for small samples
- Filterable by timeframe (7d/30d/90d/1y/all)

### Phases
1. **Backend Logic** (2.5h) — Metrics + caching + API
2. **Skill Definition** (1h) — SKILL.md + integration
3. **Frontend** (2h) — Leaderboard component + filtering

---

## Item 5: Production Config Validation Hook

**Purpose**: PreToolUse hook that warns if JWT secret or DB password are defaults.

### Implementation
- Check env vars on session start
- Flag `GRID_JWT_SECRET == "dev-secret-change-me"`
- Flag `GRID_DATABASE_PASSWORD == "changeme"`
- Advisory warning in agent context

**Est**: 1 hour, single file: `.claude/hooks/grid-config-guard.js`

---

## Item 6: Run /update-codemaps

**Purpose**: Generate token-lean architecture docs in `docs/CODEMAPS/`.

**Est**: 30 min, run command → review → commit

---

## Success Criteria

- [ ] Claude can query live GRID data via MCP (trust scores, actors, predictions)
- [ ] Daily data freshness alerts running
- [ ] All 8 critical modules at 80%+ test coverage
- [ ] Alpha leaderboard visible in PWA
- [ ] Production config validation active
- [ ] Architecture docs generated and current

---

*Start with Item 1 (MCP Server) + Item 3 (Tests) in parallel. These are independent and highest ROI.*
