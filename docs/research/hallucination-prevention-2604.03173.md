# Hallucination Prevention Research Notes

**Source:** [Detecting and Correcting Reference Hallucinations in Commercial LLMs and Deep Research Agents](https://arxiv.org/abs/2604.03173)
**Authors:** Delip Rao, Eric Wong, Chris Callison-Burch (UPenn)
**Date:** April 3, 2026

## Problem

LLMs and deep research agents supply citation URLs to support claims, but 3-13% of those URLs are **hallucinated** (never existed — no Wayback Machine record) and 5-18% are non-resolving overall. Deep research agents hallucinate URLs at higher rates than search-augmented LLMs despite generating more citations per query.

## Key Findings

- **Domain effects are pronounced**: non-resolving rates range from 5.4% (Business) to 11.4% (Theology)
- Per-model variance is even larger than per-domain variance
- Deep research agents trade quantity for quality — more citations, more hallucinations

## Detection: urlhealth

An 83-line Python tool (pip-installable + agentskills.io skill) that classifies URLs:

| Category | Meaning |
|----------|---------|
| LIVE | HTTP 200 — URL resolves |
| DEAD | HTTP 404 + Wayback snapshot exists — stale but real |
| LIKELY_HALLUCINATED | HTTP 404 + no archive — never existed |
| UNKNOWN | Other status codes / connection failures |

Uses HTTP HEAD requests with rate limiting (negligible server load).

## Correction: Agentic Self-Correction

When urlhealth is given to the LLM as a callable tool:
- Reduces non-resolving citations by **6-79x** to under 1%
- Effectiveness depends on the model's **tool-use competence**
- Models that are better at tool use benefit more from self-correction

## Actionable Lessons for GRID

### 1. Never Trust LLM-Generated References
Any URL, citation, or data source reference produced by an LLM must be verified programmatically before use. This applies to Oracle engine reports, LLM-generated briefings, and any research agent output.

### 2. HEAD-Before-GET Verification Pattern
Cheap HTTP HEAD request before fetching full content. Classify results by status code + Wayback Machine lookup to distinguish stale from fabricated.

### 3. Wayback Machine as Ground Truth
If a URL has no Internet Archive record, it likely never existed. This is the key discriminator between "stale" (real but moved/deleted) and "hallucinated" (fabricated).

### 4. Tool-Use > Prompting for Accuracy
Giving the model a verification tool (like urlhealth) works far better than prompt engineering alone ("please be accurate"). This reinforces GRID's pattern of tool-augmented agents over pure LLM generation.

### 5. Post-Generation Verification Loop
All LLM-generated content with citations should pass through a verification stage before being persisted or shown to users. This maps to GRID's existing pattern of `assert_no_lookahead()` — a post-generation gate.

### 6. Domain-Aware Confidence Calibration
Some knowledge domains have inherently higher hallucination rates. GRID's Oracle calibration system should account for domain-specific reliability when scoring predictions that cite external sources.

### 7. Stale vs. Hallucinated Distinction
A dead link to a real page (archived) is recoverable via Wayback Machine. A fabricated URL is not. This distinction matters for automated correction — stale links can be redirected to archived versions, hallucinated ones must be replaced or removed.

## Relevance to GRID Modules

| Module | Application |
|--------|-------------|
| `oracle/engine.py` | Verify any external references in prediction rationale |
| `oracle/report.py` | Validate URLs in email digests before sending |
| `outputs/scanner.py` | Gate LLM insights that cite external sources |
| `intelligence/trust_scorer.py` | Downweight sources that produce hallucinated refs |
| `hyperspace/`, `llamacpp/` | Add urlhealth as available tool for local LLM agents |
| `agents/` | Integrate verification into TradingAgents agentic loops |
