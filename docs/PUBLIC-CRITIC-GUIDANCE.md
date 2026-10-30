# Daily public-critic harness guidance v2 — October23, 2026

Owner: existing task10 daily-review consumer. This supplements the existing
read-only review workflow; it does not modify the browser harness, original
quote extractor, publication validator, scheduler, scorer or frozen evaluations.
Those existing validators remain appropriate to their original packet contracts.
The gap addressed here is a reusable offline guard for reviewer-authored checks
of public-source criticism, separate from semantic adjudication.

Include these checks in every newly prepared critic prompt; record this guidance
version/hash in its preparation receipt. Never rewrite a previously frozen prompt.

- Quote a literal span from the named passage. Cite an identified retained source,
  its exact supporting excerpt and its URL. A valid URL/quote is not proof that
  the source entails an interpretation; inspect that separately.
- Relative ETF/country/sector performance does not identify component contribution
  or causality. Without an identified mechanism and evidence, label the proposal
  a hypothesis or reject it. No automatic causal validation from correlations.
- Keep reported NAV percentages, computed rounded NAV-level changes, market-price
  changes and total returns distinct. State instrument, unit, both base values,
  dates and retained-versus-implied status. Never invent a discrepancy cause.
- State the actual single-session horizon and basis of each question. Do not infer
  a multiday streak from one contrast or substitute another benchmark. Preserve
  basis-specific outcomes when they disagree. A new composite falsifier requires
  a prospective protocol; it is not an editorial repair.
- Recompute counts from the complete pinned eligible universe and dated primitive
  values. Missing primitives in the model packet are an input limitation, not proof
  the count is false. A later supplied baseline is new evidence, not old context.
  Count-universe member IDs are instrument IDs in this bounded contract: both
  observations in each member's pair must name that same instrument. A context
  digest pins supplied bytes but cannot establish internal member-to-pair identity;
  reject reused or swapped cross-member pairs even when the numerator looks right.
- Missing GRID original publications, known-at lineage or matched outcomes remain
  explicit unknowns/unscorable. Quality review never authorizes performance scoring.
- Acknowledge demonstrated errors without fabricating mistakes for correct caveats.
  Challenge unsupported reviewer requests using exact evidence; no forced agreement.
- Enforce JSON/schema/quote/citation/length checks separately from substantive
  adjudication. Preserve overlength or rejected responses; no automatic retry.
- Prioritize early material catalysts broadly; countries/sectors are examples, not
  mandatory recurring sections. No hindsight prediction, priced-in assumption,
  calibrated odds or profitability claim without eligible evidence.

## Offline deterministic check record

`scripts/verify_public_critic_review.py` consumes a separately pinned context and
a reviewer-authored structured check record. Context contains exact source texts,
URLs/digests, original passages, dated typed observations, a pinned one-session
calendar pair, and complete count universe/pairs. Review contains literal claim
quotes/citations, inference/disposition labels, method-specific comparisons/counts
and unscorable status. Supply the canonical context SHA256 independently of the
review; do not trust a digest selected by the model or recompute it to accept a
changed context. All source values and horizon/session labels require independent
custody verification before that pin is admitted.

```text
python3 -B scripts/verify_public_critic_review.py --context CONTEXT.json --review REVIEW.json --expected-context-sha256 PIN
python3 -B -m unittest discover -s tests -p test_public_critic_review.py -v
```

This small guard supports computed NAV-price-level or market-close-price returns
and complete-universe positive counts, not issuer-reported/total-return calculators
or general outcome scoring. It rejects structured supported causal attributions,
mixed methods/windows, mismatched quote/citation/digest, arithmetic/count errors,
and performance-scoring attempts. It cannot discover hidden causal prose, ensure
an exhaustive inventory, authenticate a primary publisher or prove citation
entailment. A PASS always leaves semantic review REQUIRED. Do not claim arbitrary
natural-language causal detection or whole-pipeline readiness.

All regression examples are invented; example.invalid is not a real source.
Synthetic tests demonstrate these local guards only, not better model performance.
No runtime integration, provider pilot, underlying model training, live alert,
new writer, scheduler or production adoption is claimed. Future fresh model chats
receive these checks in their new prompts; do not rely on persistent model memory.
