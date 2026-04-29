---
source: /Users/anikdang/grid_obsidian/Architecture/Planning-Docs.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
---
tags: [docs, planning, architecture, roadmap]
created: 2026-04-04
---

# Planning & Documentation Index

All planning documents, architecture docs, and audit reports.

Related: [[Config-Map]], [[Frontend-Views]], [[Database-Schema]], [[Module-Sizes]]

---

## CLAUDE.md — Architecture Rules

The root `CLAUDE.md` is the authoritative guide for Claude Code sessions. Key rules:

1. **PIT Correctness is non-negotiable** — every data query MUST use `store/pit.py`
2. **Never use f-strings for SQL** — parameterized queries only
3. **Immutable Journal** — `journal/log.py` entries never updated/deleted
4. **Multi-source conflicts** go through `normalization/resolver.py`
5. **Model Governance**: CANDIDATE -> SHADOW -> STAGING -> PRODUCTION
6. **Prediction Causation Standard**: Levers (causes) vs Conditions (amplifiers)

### Gotchas
- `DISTINCT ON` in `store/pit.py` is PostgreSQL-specific
- `assert_no_lookahead()` raises ValueError but does NOT rollback
- `_resolve_source_id()` auto-creates source_catalog entries silently
- `pd.to_numeric(errors="coerce")` silently converts bad data to NaN
- Two scheduler files exist — `scheduler.py` is authoritative

---

## docs/ Directory (37 files)

### Core Architecture
| File | Purpose |
|------|---------|
| `architecture.md` | Full system architecture document (31,815 bytes) |
| `api-reference.md` | API endpoint reference |
| `deployment.md` | Deployment procedures |
| `development.md` | Development guide |
| `server-config.md` | Server configuration |
| `SERVER-SERVICES.md` | Systemd service reference |
| `MODULE_CATALOG.md` | Full module catalog (29,815 bytes) |
| `SHARED-READ-CONTRACT.md` | Multi-agent read contract |
| `IMPLEMENTATION-PLAN.md` | Current implementation plan |

### AstroGrid Documentation (15 files)
| File | Purpose |
|------|---------|
| `astrogrid-project.md` | Full AstroGrid project spec |
| `astrogrid-project-brief.md` | Executive brief |
| `astrogrid-5-day-execution-plan.md` | Sprint plan |
| `astrogrid-agent-handoff-week.md` | Agent handoff guide |
| `astrogrid-build.md` | Build instructions |
| `astrogrid-celestial-registry.md` | Celestial signal registry |
| `astrogrid-cosmology.md` | Cosmological theory mapping |
| `astrogrid-engines.md` | Engine architecture |
| `astrogrid-next-agent.md` | Next agent instructions |
| `astrogrid-oracle-stance.md` | Oracle positioning |
| `astrogrid-schema.md` | Database schema |
| `astrogrid-seer.md` | Seer prediction model |
| `astrogrid-strategy.md` | Trading strategy |
| `astrogrid-tasks.md` | Task backlog |
| `astrogrid-visualization.md` | Visualization spec |
| `astrogrid-world.md` | World model |

### Evaluation & Integration
| File | Purpose |
|------|---------|
| `eval-viz-libraries.md` | Visualization library evaluation |
| `viz-integration-instructions.md` | Viz library integration guide |
| `review-notes.md` | Code review notes |
| `plan.md` | High-level plan |

### Audits (14 files in docs/audits/)
| File | Purpose |
|------|---------|
| `ARCHITECTURE_REVIEW.md` | Architecture review |
| `ARCHITECTURE_EXECUTIVE_SUMMARY.md` | Executive summary |
| `ARCHITECTURE_FIXES.md` | Fix recommendations |
| `ARCHITECTURE_INDEX.md` | Review index |
| `BUILD_HEALTH.md` | Build health report |
| `CODE_REVIEW.md` | Code quality review |
| `CONSOLIDATED_AGENT_REPORT.md` | Multi-agent report |
| `DATABASE_REVIEW.md` | Database review |
| `DOC_AUDIT.md` | Documentation audit |
| `GRID-INFRA-AUDIT.md` | Infrastructure audit |
| `PERFORMANCE_AUDIT.md` | Performance audit |
| `PYTHON_REVIEW.md` | Python code review |
| `REFACTORING_INDEX.md` | Refactoring recommendations |
| `REFACTOR_REPORT.md` / `REFACTOR_SUMMARY.txt` | Refactor results |
| `SECURITY_AUDIT.md` | Security audit |

### Planning (17 files in docs/planning/)
| File | Purpose |
|------|---------|
| `ROADMAP.md` | **Master roadmap** — 4-week tactical + 4-quarter strategic |
| `MASTER-PLAN.md` | Master development plan |
| `GSD-PLAN.md` | "Get Shit Done" execution plan |
| `GSD-OPTIONS-EDGE.md` | Options edge GSD plan |
| `PROJECT.md` | Project overview |
| `REQUIREMENTS.md` | System requirements |
| `STATE.md` | Current system state |
| `TODO-NEXT.md` | Next action items |
| `NEXT-SESSION.md` | Next session prep |
| `AGENT-INSTRUCTIONS.md` | Agent behavior instructions |
| `NEXT-AGENT-INSTRUCTIONS.md` | Next agent handoff |
| `ASTROGRID-PLAN.md` | AstroGrid plan |
| `DERIVATIVESGRID-PLAN.md` | Derivatives grid plan |
| `SIGNAL-CONNECTIVITY-PLAN.md` | Signal connectivity |
| `VIEW-ARCHITECTURE.md` | Frontend view architecture |
| `config.json` | Planning config |

### Codebase Documentation (7 files in docs/planning/codebase/)
| File | Purpose |
|------|---------|
| `ARCHITECTURE.md` | Detailed architecture |
| `CONCERNS.md` | Known concerns |
| `CONVENTIONS.md` | Coding conventions |
| `INTEGRATIONS.md` | External integrations |
| `STACK.md` | Technology stack |
| `STRUCTURE.md` | Directory structure |
| `TESTING.md` | Testing strategy |

### Superpowers
| File | Purpose |
|------|---------|
| `superpowers/specs/2026-03-30-rag-intelligence-design.md` | RAG intelligence design spec |

---

## .claude/plan/ Files

| File | Purpose |
|------|---------|
| `quanta-alpha-integration.md` | QuantaAlpha evolutionary factor mining integration plan |

---

## Other Root Documentation

| File | Purpose |
|------|---------|
| `ATTENTION.md` | 64-item audit of known issues |
| `README.md` | Project readme |
| `DATA_SOURCES_CATALOG.md` | All data sources catalog |
| `DEV-NOTES-DATA-INTEGRITY.md` | Data integrity developer notes |
| `FIRST_DAY_REPORT.md` | New developer onboarding |
| `HOSTING.md` | Hosting configuration |
