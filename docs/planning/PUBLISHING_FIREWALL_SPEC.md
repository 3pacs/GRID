# Publishing Firewall — Full Engineering Spec

## Saved from user's detailed specification (2026-04-05)

See `~/.claude/skills/publishing-firewall/SKILL.md` for the operational guide.

## Key Decisions

- 9 services: Draft → Claim Extract → Retrieval → Verification → Sanity → Contradiction → Rewrite → Risk → Gate
- Fail-closed: if anything important cannot be verified, it does not publish
- Atomic claims: one fact per row, verified individually
- Separate writer and verifier (never same model)
- Deterministic sanity engine (no LLM for math/date/unit checks)
- 8 non-negotiable code rules (contradicted = block, no unsupported quotes, etc.)
- Domain-specific policies: news, social, finance, economics
- Full audit trail: every claim, source, verdict, rewrite logged

## Non-Negotiable Code Rules

1. No material claim without a verdict
2. No contradicted material claim may publish
3. No failed critical numeric check may publish
4. No unsupported quote may publish
5. No stale material claim may publish in news/finance/economics
6. No "latest/current/today" without validated timestamp
7. No single-source weakly sourced breaking-news publish
8. Rewrites may only narrow claims, never add unsupported detail

## Risk Score Formula

```
risk =
  0.30 * unsupported_material_claim_rate +
  0.15 * stale_claim_rate +
  0.15 * contradiction_rate +
  0.10 * numeric_failure_rate +
  0.10 * ambiguity_rate +
  0.10 * low_authority_dependency +
  0.05 * rewrite_burden +
  0.05 * time_sensitivity_weight
```

Thresholds: 0-0.15 publish, 0.16-0.35 review, >0.35 reject. Hard rules override score.

## DB Schema: 10 tables

content_requests, drafts, claims, sources, verifications, evidence_spans,
sanity_checks, contradictions, risk_scores, audit_log

## Phase 1 (ship first)
- Claim extraction + verification + numeric/date checks + hard blocking
- Wire into chatbot and RSS feed

## Full spec in user's message — reference the skill for implementation details.
