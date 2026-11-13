# R3 (#1085): NVIDIA commitment-to-valuation ledger — synthesis v0

**Status: FROZEN 2026-11-13, before NVIDIA's Q3 FY2027 report (scheduled 2026-11-17 at about 1:20 p.m. PT).** Ledger sha256 `484f39994d26e9e2cf4185b686edbf58f79b8ed09258d84f34146b91dab9e23c`. The scenario, health and rubric hashes are pinned in `analysis/r3_nvda_ledger.py` (`FROZEN_FILES`). Any change after this is a new version. Research artifact only: no trade, recommendation or predictive-performance claim. Records must have become public before 2026-11-17 21:00Z.

| | |
|---|---|
| Ledger | `ledger-v0.jsonl`: 450 records.<br>• 221 commitment and financial records: 2 owner, 140 from NVIDIA's SEC filings, 79 from counterparty documents. Of these, 176 were confirmed by an independent re-fetch, 20 corrected, 19 added and 4 unverifiable.<br>• 229 counterparty-health facts. | Each record has a URL, a verbatim quote, a publication time and, where it could be fetched, the sha256 of the raw file. |
| Calculations | `scenarios-v0.json`. Run with `python -m analysis.r3_nvda_ledger`; tests are in `tests/test_r3_nvda_ledger.py`. |
| Price basis | NVDA close of $166.19 on 2026-11-12, read from griddb `YF:NVDA:close`. This price is **unqualified**: it has not passed the R1 price checks. |
| Share and cash basis | Q2 FY2027 (ended 2026-07-26): 24,285M diluted shares; $56.6B cash and marketable securities; $33.4B total debt. Net cash is $23.2B. |
| Independent check | Two separate agents re-fetched every source and checked each record (`critic-*.jsonl`). This is weaker than the separate human or agent reviewer that #1085 asks for; see "Limits". |

## 1. What was public before the report

**Leadership.**
- On 2026-11-11 the Board accepted Jensen Huang's resignation as President, CEO and director, effective immediately. The 8-K is accession 0001045810-26-000082, accepted 2026-11-12 13:52Z.
- CFO Colette Kress is interim CEO and remains CFO. Harvey C. Jones is Chair.
- No reason, severance or guidance change is stated.
- The Board "expect[s] ... customer commitments ... to continue without interruption". That is the Board's expectation, not a counterparty's confirmation.
- No primary statement from OpenAI, Microsoft, Oracle, CoreWeave, Meta, Anthropic, xAI, Nebius, Nscale or TSMC about NVIDIA commitments was found for 11-11 or 11-12 (`leadership-change-counterparty-reaction-none-found`).

**The benchmark for 11-17.**
- Q3 FY2027 revenue outlook: $108.0B ±2%, assuming no Data Center compute revenue from China. Gross margin outlook: 74.0% ±50 bp.
- NVIDIA beat its own guide in the prior two quarters:

  | Quarter | Actual | Guide | Beat |
  |---|---|---|---|
  | Q2 FY2027 | $96.2B | $91.0B | +5.7% |
  | Q1 FY2027 | $81.6B | $78.0B | +4.6% |

## 2. Commitment stack, each dollar counted once (USD bn)

**Counting rule.** Only NVIDIA's own latest point-in-time disclosures count.
- Earlier-date balances, restatements and totals of components are kept in the ledger but not counted.
- Named investments and named capacity buy-backs are not counted either. NVIDIA reports these only in aggregate, so counting them by name would double count.
- Hyperscaler capex, whole-programme spend and cloud contracts between two non-NVIDIA parties are context only. They are never NVIDIA revenue.

| Counted line (balance at 2026-07-26 unless noted) | USD bn | Circularity |
|---|---|---|
| Supply and capacity commitments ("primarily memory and manufacturing facilities") | 279.0 | none |
| Data-center leases not commenced | 25.0 | none |
| Capital-expenditure commitments | 8.0 | none |
| Hugging Face acquisition (agreed 2026-09-03, after the balance date) | 11.9 | none |
| Multi-year cloud service agreements (NVIDIA buying cloud capacity) | 29.0 | capacity backstop |
| AI cloud agreements (NVIDIA sells to AI clouds and buys capacity back, typically for 6 years) | 36.0 | capacity backstop |
| Data-center leases signed to reassign to third parties | 20.0 | capacity backstop |
| Equity-investment commitments, unfunded | 25.0 | investment in customers |
| PORTS-Pike guarantees, signed 2026-08-17. A cap that takes effect lease by lease from FY2029; NVIDIA pays only if OpenAI defaults. | 105.0 | lease guarantee |
| Earlier AI-cloud lease guarantees | 3.5 | lease guarantee |
| **Total** | **542.4** | |

- **Circular share.** $218.5B of the $542.4B (40%) loops back to NVIDIA's own customers: capacity buy-backs, guarantees of customers' leases, or equity into customers. On top of that, NVIDIA held $99B of funded equity investments at Q2.
- **Contingent share.** $108.5B is guarantees. These become cash outflows only on a customer default. On 2026-07-26 only $3.5B was in force; the $105B cap was signed in August and starts phasing in from FY2029.
- **Funded stakes named in NVIDIA's own 13F/13G filings.** As of 2026-06-30: Intel $30.0B, SpaceX $21.0B, CoreWeave $4.7B (11.5%), plus Nebius at 9.3% (13G, 2026-07-13). The 13F total is $63.4B. NVIDIA bought $42.4B of equity securities in H1 FY2027 alone.
- **What is not there.** No primary source gives a dollar hardware backlog or an order book that commits customers to NVIDIA. The only demand-side figures are:
  - the OpenAI letter of intent (10 GW; NVIDIA "intends to invest up to $100B progressively"). No definitive agreement was found in any primary source.
  - OpenAI's ~4.25 GW at PORTS-Pike from 2028.
  - NVIDIA's own "could represent" estimate of $150–200B of revenue per infrastructure generation at that site. This is recorded as an intention, not backlog.
  - Customer cloud contracts with neoclouds, all running on NVIDIA hardware. Examples: Nscale with Microsoft (up to $43.8B, of which $3.22B was terminated) and with Anthropic (up to $44.6B); CoreWeave with Meta (up to $21B) and with OpenAI (up to $6.5B). These are the neoclouds' revenue, not NVIDIA's.
- **Cancellations on record.** OpenAI withdrew from Stargate UK in April 2026 (Nscale S-1/A). Nscale's $3.22B Microsoft statement of work was terminated after a power allocation was withdrawn. A reported Abilene expansion cancellation is secondary-only.

## 3. Conversion: commitment → delivery → revenue → cash

- **Supply is front-loaded.** Of the $279B, $92B falls in the rest of FY2027 (two quarters), $87B in FY2028, $88B in FY2029 and $12B after that. The 10-Q says the commitments are "primarily memory and manufacturing facilities"; the CFO says the *increase* was "primarily related to the procurement of memory". They "may be cancelable, rescheduled, or adjustable ... prior to placing firm orders", so they are not all firm.
  - $46B per quarter is 1.9× Q2's cost of revenue of $24.1B (Q2 revenue $96.2B at 75.0% gross margin).
  - As a calculation, not a forecast: if all of that were cost of revenue at a 75% margin, it would support about $184B of revenue per quarter, against a $108B guide.
  - So either revenue ramps well above the guide (Vera Rubin), or inventory and prepayments build. Inventory went $21.4B → $25.8B → $31.6B across FY26-end, Q1 and Q2.
- **Cash conversion fell.** Free cash flow went from $48.6B in Q1 (59% of revenue) to $21.3B in Q2 (22%).
  - Receivables were $63.1B in Q2. Days sales outstanding were 60, up from 45.
  - NVIDIA says it extended payment terms of 90 days to one year to investment-grade customers.
  - That is vendor financing in substance, though not in name.
  - H1 FY2027 cash flow shows the same squeeze: receivables −$24.6B, inventories −$10.2B, prepaid and other assets −$6.5B.

## 4. Valuation sensitivity (conditional, not forecasts)

Enterprise value at the basis is $4,013B: a market capitalisation of $4,036B less $23.2B net cash. On the base revenue path, forward four quarters revenue is $464B, so EV is 8.6× forward revenue.

| FCF margin on forward revenue | FCF multiple the price implies |
|---|---|
| 22% (Q2 FY27) | 39× |
| 30% | 29× |
| 40% | 22× |
| 52% (Q4 FY26 to Q1 FY27 range) | 17× |

Value per share on the base revenue path:

| FCF margin \ multiple | 20× | 28× | 35× |
|---|---|---|---|
| 22% | $85 | $119 | $148 |
| 30% | $116 | $162 | $202 |
| 40% | $154 | $215 | $269 |
| 52% | $200 | $279 | $349 |

Scenarios (`scenarios-v0.json`, every assumption with its reason):

| | Q3 vs guide | Forward 4Q revenue | FCF margin | Multiple | Value per share | vs $166.19 |
|---|---|---|---|---|---|---|
| Bear | at −2% | $403B | 22% | 20× | $74 | −56% |
| Base | at midpoint | $464B | 40% | 28× | $215 | +29% |
| Bull | +5% beat | $516B | 52% | 35× | $387 | +133% |

**Reading.** Revenue assumptions move the value less than cash conversion and the multiple do. The ledger's sharpest open question is how much of NVIDIA's revenue becomes free cash flow. That depends on extended payment terms, $279B of supply prepayments and commitments, and $85B of capacity it has agreed to buy back. Whether revenue beats $108B matters less.

## 5. What the 11-17 report will be compared against

These are frozen pre-report observables, not predictions:

| Observable | Pre-report value (record id) |
|---|---|
| Q3 revenue | guide $108.0B ±2% (`nvda-pr-q3fy27-outlook-no-china`) |
| Q3 gross margin | guide 74.0% ±50 bp (`nvda-pr-q3fy27-outlook-gm`) |
| Free cash flow and its share of revenue | Q2 $21.3B, 22% (`nvda-pr-q2fy27-fcf`) |
| Receivables and DSO | $63.1B, 60 days (`nvda-10q-q2fy27-accounts-receivable`, `nvda-cfo-q2fy27-dso`) |
| Inventory | $31.6B (`nvda-10q-q2fy27-inventory`) |
| Supply commitments | $279B (`nvda-10q-q2fy27-supply-commitments`) |
| Cloud plus AI-cloud buy-backs | $29B + $36B |
| Guarantees | $108.5B cap including the August SB Energy guarantees; $3.5B in force at 2026-07-26 (`nvda-10q-q2fy27-guarantee-total`) |
| Equity investments, funded and committed | $99B / $25B |
| Largest direct customer share | 16% (`nvda-10q-q2fy27-customer-concentration`) |
| Any cancellation, or a counterparty statement on the CEO change | none found as of 11-12 |

## 6. Counterparty health: does the base case stand?

**Method.** The rubric and decision rule (`health-rubric-v0.md`, sha256 `5e11a86e…`) were written at 2026-11-13 00:51:54Z. The first health download was at 00:53:01Z, so the rule came before the data and has not been edited since. Grades and evidence ids are in `health-v0.json`.

| Counterparty | Grade | Deciding fact |
|---|---|---|
| CoreWeave | RED (literal) / AMBER (lenient) | Cash $4.1B against $2.5B a month of capex-inclusive burn. $47.5B of debt. Interest is 0.45× adjusted operating income. $1.9B of contracts "became cancellable". Material weaknesses. |
| Nebius | RED (literal) / AMBER (lenient) | Cash $7.4B against $2.5B a month of burn. 2026 revenue guide cut. Microsoft sites slipped to 2Q27. One customer holds 75% of receivables. |
| Nscale | RED (any reading) | Going-concern language in the S-1/A. New financing required for 2027. The Anthropic $44.6B deal has no committed financing. |
| OpenAI | AMBER floor; liquidity not supported by evidence | No primary cash, burn or debt data. At least $394.5B of compute contracts with a stated dollar amount, against a $122B round. Withdrew from Stargate UK and Norway. Owes Microsoft $7.2B, up from $6.0B. |
| Oracle | AMBER | Q1 FCF −$5.4B, or −$16.8B excluding an $11.4B prepayment. Positive FCF only from 2H FY28. Needs about $20B more capital. Expects "vendor financing" (vendor not named). |
| SoftBank | AMBER | LTV 15.9% against a 25% ceiling. $3.73B loss on power-equipment advances tied in part to PORTS-Pike. PORTS-Pike to be project-financed. |
| Amazon | AMBER (literal) | Q3 FCF −$33.5B. Debt up to $180.9B. Supplier-financed loan facility of up to about $80B, plus up-to-one-year payment terms. |
| Meta | AMBER (literal: layoffs) | Q3 FCF −$2.1B. Seeking more external capital. $41.5B of guarantees to data-center ventures. |
| Intel | AMBER (impairment) | NVIDIA's exposure is mark-to-market only. |
| Microsoft, Alphabet | GREEN | Microsoft FCF +$18.6B per quarter. Alphabet holds $265.1B of cash (though its FCF is negative two quarters running). |
| Anthropic, SpaceX (incl. xAI) | GREEN | Anthropic is self-funding with $131.9B of cash and short-term investments. SpaceX has 27 months of runway and no maturities before 2031. |

**Decision: DOWNGRADED.** The result is the same under both readings of the rubric:
- **Literal reading:** three neoclouds are RED and two hyperscalers are AMBER.
- **Lenient reading:** growth capex is not counted as burn, Meta's layoff is ignored, and Amazon is judged on trailing burn. Nscale is still RED on its going-concern language.

**Which assumption fails:**
1. **Customers keep buying for four quarters: supported, with named risks.** Capex is being funded, and debt markets were open to Oracle, Meta, Alphabet and Amazon in October and November. The risks are neocloud funding and substitution: Alphabet now sells TPU systems, Anthropic runs on four chip families, and AMD has granted warrants to OpenAI and Meta.
2. **FCF margin recovers to 40%: NOT SUPPORTED.** NVIDIA's buyers are cash-negative and fund capex with debt and supplier credit. NVIDIA already extended payment terms, and its receivables rose $24.6B in H1. 22–30% is the evidence-backed range.
3. **No material cash call in four quarters: mostly supported.** The PORTS-Pike guarantees start in FY2029. The CoreWeave buy-back exposure is unknown in size.

**Health-adjusted base** (`base_health_adjusted`; same revenue path and multiple, FCF margin 26%): forward FCF $120.7B, **$140 per share, −16% against $166.19**. This is a conditional calculation, not a forecast. On the base revenue path the market price corresponds to a FCF margin of about 30% at 28×.

**Limits.** The health facts were checked by script against raw downloads but had no separate critic pass. The vendors behind Amazon's and Oracle's supplier financing are not named, so NVIDIA's share of them is unknown.

## 7. Limits

- **Reviewer independence.** The independent check was done by separate agents of the same model family, not by Codex or a human. #1085 asks for an independent primary-source critic, and this is a weaker form of that.
- **openai.com was unreachable** to automated fetches (HTTP 403). The reported $30B OpenAI stake and the 2026 OpenAI–NVIDIA GW split are therefore secondary-only. They are not counted and not used in any calculation.
- **Unqualified price.** The price is griddb `YF:NVDA:close`, which has not passed the R1 price checks. Fundamentals come straight from the filings, not from GRID tables.
- **Assumptions.** Scenario growth paths, FCF margins and multiples are stated assumptions. None is estimated from data.
- **Equity stakes.** The $99B of equity stakes and the $108.5B of guarantees are left out of net cash. They are only discussed as context, never added.
