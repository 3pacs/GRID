# R3 (#1085): NVIDIA commitment-to-valuation ledger — synthesis v2

**Status: v2, FROZEN 2026-11-13 before NVIDIA's Q3 FY2027 report (scheduled 2026-11-17 at about 1:20 p.m. PT, record `nvda-8k-20261112-ex991-q3-report-time`).** v2 supersedes v1 after a re-review on #1150 (narrow REQUEST_CHANGES); v1 had superseded v0 after the first review. §8 and §9 list every change. v0 and v1 stay in the repo unchanged, pinned in `V0_FROZEN_FILES` and `V1_FROZEN_FILES`. The v2 hashes are pinned in `analysis/r3_nvda_ledger.py`; any change after this is v3. Research artifact only: no trade, recommendation or predictive-performance claim. Records must have become public before 2026-11-17 21:00Z. No record may claim a retrieval after the build time (`LEDGER_BUILT_AT`). Each retrieval time is the earliest real download of exactly the hashed bytes.

| | |
|---|---|
| Ledger | `ledger-v2.jsonl`: 455 records.<br>• **200 critic-checked:** a separate critic agent re-fetched the cited document and checked the record. 176 confirmed, 20 corrected, 4 unverifiable.<br>• **255 not critic-checked:**<br>&nbsp;&nbsp;– 230 counterparty-health facts, which the health workers script-checked against their own downloads;<br>&nbsp;&nbsp;– 21 owner-added records, taken from the critics' downloads;<br>&nbsp;&nbsp;– 2 leadership 8-K records, whose quotes and hashes the #1150 review matched at source;<br>&nbsp;&nbsp;– the report-time record and the risk-factor record.<br>• The #1150 reviews re-downloaded the cited documents. 428 of the 450 v0 records matched hash and quote, and 13 more documents were re-checked for v1. They did not re-derive every health grade.<br>• **Gaps:** 8 records have no hash, 9 are paraphrases, 9 are secondary-only, 2 have an empty quote, and 3 have no publication time. 8 have no retrieval time; that is every record without a hash, since no bytes were kept. |
| Calculations | `scenarios-v2.json`. Run with `python -m analysis.r3_nvda_ledger`; tests are in `tests/test_r3_nvda_ledger.py`. |
| Price basis | NVDA close of $166.19 on 2026-11-12, read from griddb `YF:NVDA:close`. This price is **unqualified**: it has not passed the R1 price checks. |
| Share and cash basis | Q2 FY2027 (ended 2026-07-26): 24,285M diluted shares; $56.6B cash and marketable securities; $33.4B total debt. Net cash is $23.2B. |
| Independent check | Two critic agents covered the commitment and financial records, as counted above. A separate review covered the whole PR (#1150). All are agents of the same model family, which is weaker than a human or other-agent reviewer; see Limits. |

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

| Counted line (balance at 2026-07-26 unless noted) | USD bn | Firmness | Circularity (NVIDIA's own wording) |
|---|---|---|---|
| Supply and capacity commitments ("primarily memory and manufacturing facilities"). The 10-Q says "in certain instances, these agreements may be cancelable, rescheduled, or adjustable for our business needs prior to placing firm orders. Changes to these agreements may result in additional costs". Its risk factors add: "we may be unable to reduce these commitments". | 279.0 | **partly cancellable** (share not disclosed) | none |
| Data-center leases not commenced | 25.0 | firm | none |
| Capital-expenditure commitments | 8.0 | firm | none |
| Cloud service agreements: "cloud infrastructure to support our research and development of our open models ... and our autonomous vehicle software" (vendors not named) | 29.0 | firm | unknown, R&D compute, **not** a buy-back |
| AI cloud agreements: AI clouds buy NVIDIA infrastructure and NVIDIA commits to buy cloud services back | 36.0 | firm (maximum; decreases as capacity is used by third parties) | **capacity buy-back** |
| Data-center leases signed for third parties (about 15 years, commencing FY2028–29, expected to be reassigned) | 20.0 | firm (maximum) | credit support (vendor financing) |
| Equity-investment commitments to "AI model makers, infrastructure financiers, and other private companies, subject to certain contingencies" | 25.0 | contingent | unknown (not all customers) |
| PORTS-Pike guarantees, signed 2026-08-17: a cap taking effect lease by lease from FY2029, payable only on OpenAI's default, with OpenAI indemnifying NVIDIA | 105.0 | contingent | lease guarantee |
| Earlier AI-cloud lease guarantees (in force at 2026-07-26) | 3.5 | contingent | lease guarantee |
| Hugging Face acquisition (agreed 2026-09-03, after the balance date; closing conditional) | 11.9 | post-balance | none |

**By firmness:** firm $118.0B, two lines of it maxima; supply commitments $279.0B, cancellable only in certain instances with the share not disclosed; contingent $133.5B; post-balance $11.9B. The $542.4B sum mixes these four kinds and has no single meaning. Nothing is double counted: the ten lines are distinct rows of NVIDIA's own tables plus two later events.

**How much is circular depends on the definition.** v0's headline "40% circular" is withdrawn.

| Definition | Circular / total | Share |
|---|---|---|
| Buy-back proper (AI cloud agreements) | 36 / 542.4 | 7% |
| Excluding both the not-in-force $105B cap and the $29B of R&D cloud services | 84.5 / 437.4 | 19% |
| Excluding the not-in-force $105B cap only | 113.5 / 437.4 | 26% |
| v1/v2 tags (buy-back + credit-support leases + lease guarantees), cap included | 164.5 / 542.4 | 30% |
| v1/v2 tags, excluding the not-in-force $105B cap | 59.5 / 437.4 | 14% |
| v0's definition: every line tagged in v0, cap included | 218.5 / 542.4 | 40% |

The 19% and 26% rows count the $25B equity line as circular, which v1/v2 tag `unknown`.

The denominator is dominated by $279B of supplier commitments, so each ratio moves with memory procurement as much as with circularity. NVIDIA held a further $99B of funded equity investments at Q2.

- **Funded stakes named in NVIDIA's own 13F/13G filings.** As of 2026-06-30: Intel $30.0B, SpaceX $21.0B, CoreWeave $4.7B (11.5%). Nebius was 9.3% per a 13G dated 2026-07-13. The 13F total is $63.4B. NVIDIA bought $42.4B of equity securities in H1 FY2027.
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
  - Three outcomes are possible: revenue ramps well above the guide (Vera Rubin); inventory and prepayments build; or NVIDIA cancels, reschedules or adjusts some agreements, possibly at extra cost. Inventory went $21.4B → $25.8B → $31.6B across FY26-end, Q1 and Q2.
- **Cash conversion: one weak quarter; whether it is a trend is not established.**

  | Period | FCF / revenue |
  |---|---|
  | Q4 FY2026 | 51% |
  | Q1 FY2027 | 59% |
  | Q2 FY2027 | 22% |
  | **H1 FY2027** | **39.3%** |
  | **Trailing three quarters** | **42.6%** |

  - **What depressed Q2.** Days sales outstanding stepped from 45 to 60, about $16B on one quarter's revenue. That is a change of level, which need not repeat.
  - **Tax timing.** NVIDIA "made two federal income tax payments in the second quarter ... as compared with no estimated tax payments in the first quarter". The CFO attributes the sequential fall to "working capital adjustments and cash taxes". So Q1 was flattered and Q2 depressed by timing.
  - **Extended terms.** NVIDIA gives 90-day-to-one-year payment terms to "investment-grade customer purchases". Receivables were $63.1B at Q2. In substance that is customer financing.
  - **H1 FY2027 cash flow:** receivables −$24.6B, inventories −$10.2B, prepaid and other assets −$6.5B.

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

Scenarios (`scenarios-v1.json`, every assumption with its reason):

| | Q3 vs guide | Forward 4Q revenue | FCF margin | Multiple | Value per share | vs $166.19 |
|---|---|---|---|---|---|---|
| Bear | at −2% | $403B | 22% | 20× | $74 | −56% |
| Base | at midpoint | $464B | 40% | 28× | $215 | +29% |
| Bull | +5% beat | $516B | 52% | 35× | $387 | +133% |
| Sensitivity: Q2 cash conversion persists | at midpoint | $464B | 22% | 28× | $119 | −29% |

The sensitivity is **not a health result**. Holding 22% for four quarters needs about $80B more working capital than the H1 FY2027 pace, and about $151B in total: H1 operating cash flow before working capital was 57.1% of revenue and capex 2.5%, against $27B of working capital absorbed in H1. If the extra $80B sat in receivables, they would rise by about 63 days of forward revenue, to roughly 106–112 days depending on the revenue basis. The 28× multiple is applied to FCF that a transient working-capital build has depressed, so this row also capitalises that build as if it were permanent.

**Reading.** Within the ranges chosen here, the revenue assumptions move the value less than cash conversion and the multiple do. That is a property of these ranges, not a finding. The ledger's sharpest open question is how much of NVIDIA's revenue becomes free cash flow. That depends on extended payment terms, $279B of supply commitments (cancellable only in certain instances) and $36B of capacity buy-back agreements. Whether revenue beats $108B matters less.

## 5. What the 11-17 report will be compared against

These are frozen pre-report observables, not predictions:

| Observable | Pre-report value (record id) |
|---|---|
| Q3 revenue | guide $108.0B ±2% (`nvda-pr-q3fy27-outlook-no-china`) |
| Q3 gross margin | guide 74.0% ±50 bp (`nvda-pr-q3fy27-outlook-gm`) |
| Free cash flow and its share of revenue | Q2 $21.3B, 22%; H1 FY27 39.3% (`nvda-pr-q2fy27-fcf`, `nvda-pr-q1fy27-fcf`); Q2 tax timing (`nvda-10q-q2fy27-federal-tax-payments-timing`) |
| Receivables and DSO | $63.1B, 60 days (`nvda-10q-q2fy27-accounts-receivable`, `nvda-cfo-q2fy27-dso`) |
| Inventory | $31.6B (`nvda-10q-q2fy27-inventory`) |
| Supply commitments (cancellable in certain instances; share not disclosed) | $279B (`nvda-10q-q2fy27-supply-commitments`) |
| AI-cloud buy-back agreements; R&D cloud services | $36B; $29B |
| Guarantees | $108.5B cap including the August SB Energy guarantees; $3.5B in force at 2026-07-26 (`nvda-10q-q2fy27-guarantee-total`) |
| Equity investments, funded and committed | $99B / $25B |
| Largest direct customer share | 16% (`nvda-10q-q2fy27-customer-concentration`) |
| Any cancellation, or a counterparty statement on the CEO change | none found as of 11-12 |

## 6. Counterparty health: does the base case stand?

**Method.** The rubric and decision rule (`health-rubric-v0.md`, sha256 `5e11a86e…`) were written at 2026-11-13 00:51:54Z, before the first health download at 00:53:01Z, and are unchanged in v1. Its literal "one distress signal" clause is not tightened after seeing the results; the lenient reading is shown alongside. Grades and evidence ids are in `health-v2.json`.

| Counterparty | Grade | Deciding fact |
|---|---|---|
| CoreWeave | RED (literal) / AMBER (lenient) | Cash $4.1B against $2.5B a month of capex-inclusive burn. Debt $47.5B. **Adjusted operating income ($379M) is 0.45× interest expense ($846M).** $1.9B of contracts "became cancellable". Material weaknesses. Its 10-Q states one-year liquidity sufficiency, and there is no going-concern language. |
| Nebius | RED (literal) / AMBER (lenient) | Cash $7.4B against $2.5B a month of burn. 2026 revenue guide cut. Microsoft sites slipped to 2Q27. One customer holds 75% of receivables. |
| Nscale | RED (any reading) | The S-1/A says reliance on uncommitted financing "initially raised substantial doubt" about going concern, and that a capex-deferral plan "would ... alleviate the substantial doubt". New financing is needed for 2027. The Anthropic $44.6B deal has no committed financing. |
| OpenAI | **UNKNOWN** | No primary cash, burn, revenue or debt data, so its part of the base case is not supported by evidence either way. Context only: it withdrew from Stargate UK and Norway, and it owes Microsoft $7.2B, up from $6.0B. Contract totals are not compared with one funding round. |
| Oracle | AMBER | Q1 FCF −$5.4B, or −$16.8B excluding an $11.4B prepayment. Positive FCF only from 2H FY28. Needs about $20B more capital. Expects "vendor financing", vendor not named. |
| SoftBank | AMBER | LTV 15.9% against a 25% ceiling. $3.73B loss on power-equipment advances; SoftBank says this loss is **"completely unrelated to PORTS-Pike"** (slide 17, `health-softbank-sb7-loss-unrelated-to-portspike`). Its financial report places the equipment in AI data-center projects "including the PORTS-Pike Technology Campus", and $5.6B of the remaining advances are to be recouped from that project's power plant. PORTS-Pike is to be project-financed. |
| Amazon | AMBER (literal) | Q3 FCF −$33.5B. Debt $180.9B. Its facility of up to about $80B is a **secured loan** (five-year borrowings booked as long-term debt), not receivable terms. |
| Meta | AMBER (literal: layoffs) | Q3 FCF −$2.1B; +$11.0B over nine months. Seeking more external capital. $41.5B of guarantees to data-center ventures. |
| Intel | AMBER (literal: impairment) | NVIDIA's exposure is mark-to-market only. |
| Microsoft, Alphabet | GREEN | Microsoft FCF +$18.6B a quarter. Alphabet holds $265.1B of cash and securities. |
| Anthropic, SpaceX (incl. xAI) | GREEN | Anthropic is self-funding, with $131.9B of cash and short-term investments. SpaceX has 27 months of runway at the Q3 burn (about 23 at the nine-month burn) and no material principal payments under its notes until July 15, 2031 ($2.7B of short-term debt and finance leases). |

**Decision: DOWNGRADED by the rubric's any-RED rule, size not established.**
- **Under both readings.** Literal: CoreWeave, Nebius and Nscale are RED. Lenient: only Nscale is.
- **The one RED that survives every reading is small.** Nscale's four-quarter cash exposure to NVIDIA is about $0.4B of lease guarantee not covered by escrow, plus a $1.2B services agreement in which NVIDIA is the payer, plus about $2.8B of equity at cost, of which $1.0B was due to close around 2026-11-16, after this freeze. NVIDIA revenue at risk through Nscale's purchases is not quantified.
- **The rubric's AMBER dollar test (about $46B of FCF) was not computed.** The vendors behind Oracle's vendor financing are not named, and Amazon's facility is a secured loan.
- **OpenAI's part is UNKNOWN,** so it is neither STANDS nor DOWNGRADED.

**Assumptions:**
1. **Customers keep buying for four quarters: supported, with named risks.** Debt markets were open to Oracle, Meta, Alphabet and Amazon in October and November. The risks are neocloud funding and substitution: Alphabet sells TPU systems, Anthropic runs on four chip families, and AMD has granted warrants to OpenAI and Meta.
2. **FCF margin about 40%: not established either way.** The observed H1 FY2027 level is 39.3%, and the trailing three quarters 42.6%. Q2's 22% reflects a DSO step and tax timing (§3). The extended terms went to investment-grade customers, so the RED neoclouds are not shown to be behind the receivables. v0's "22–30% is the evidence-backed range" is withdrawn.
3. **No material cash call in four quarters: mostly supported.** The PORTS-Pike guarantees start in FY2029. The size of the CoreWeave buy-back exposure is unknown; $1.9B of CoreWeave contracts "became cancellable", which could raise the unsold capacity NVIDIA must buy.

**v0's "health-adjusted base" of $140 per share is withdrawn.** It implied an unstated forecast that receivables would keep building. §4 shows a plain sensitivity in its place, with its implied working capital.

## 7. Limits

- **Reviewer independence.** Critic agents and the #1150 review are all agents of the same model family, not Codex or a human. #1085 asks for an independent primary-source critic, and this is a weaker form of that. 255 of 455 records had no critic check (see the header).
- **Scenarios do not use the stack.** They use neither the commitment stack, gross margin nor delivery timing. #1085's "link commitment to delivery, revenue, gross margin, working capital, FCF" is met in §3 prose only.
- **Coverage gaps.**
  - The server OEM/ODM channel is not researched. Five direct customers hold 70% of receivables (22/14/13/11/10%), and their identity decides whose health matters.
  - Call transcripts and keynote demand statements were not searched.
  - Several investees and memory suppliers are thin or absent.
- **Diluted shares** are the Q2 weighted average (24,285M), before later buybacks.
- **openai.com was unreachable** to automated fetches (HTTP 403). The reported $30B OpenAI stake and the 2026 OpenAI–NVIDIA GW split are therefore secondary-only. They are not counted and not used in any calculation.
- **Unqualified price.** The price is griddb `YF:NVDA:close`, which has not passed the R1 price checks. Fundamentals come straight from the filings, not from GRID tables.
- **Assumptions.** Scenario growth paths, FCF margins and multiples are stated assumptions. None is estimated from data.
- **Equity stakes.** The $99B of equity stakes and the $108.5B of guarantees are left out of net cash. They are only discussed as context, never added.

## 8. Changes from v0 (independent review on #1150, REQUEST_CHANGES)

| Finding | v1 change |
|---|---|
| B1: invented timestamps | 21 retrieval times replaced with the real download times; the owner 8-K times corrected (00:22Z); no publication-time fallback, so the field is null when unknown. The validator now refuses any retrieval after `LEDGER_BUILT_AT`. |
| B2: "$85B buy-backs" / "40% circular" | $29B reclassified as R&D cloud services, $20B as credit-support leases, $25B equity as not-all-customers. Firm / cancellable / contingent / post-balance split, and a definitions table instead of one headline. |
| B3: FCF verdict and the $140 figure | A2 is now "not established either way"; H1 and trailing-three-quarter margins shown; tax-timing records added; the $140 scenario replaced by a sensitivity printing its implied working capital; decision restated as "any-RED rule, size not established". |
| B4: factual errors | CoreWeave coverage direction fixed; SoftBank's "completely unrelated to PORTS-Pike" quoted; OpenAI graded UNKNOWN; Nscale's "alleviate" wording added; Amazon's facility described as a secured loan. |
| B5: overstated independence | Exact counts of checked and unchecked records in the header, PR and limits. |
| N1, N2, N4, N5: validator gaps | Date-only publication = end of day; hex hash check; no negative counted values; firmness required on counted records; totals declare `total_of` and cannot be counted alongside their components; price and multiple can never be ledger ids. |
| N13: report time unbacked | `nvda-8k-20261112-ex991-q3-report-time` added. |

## 9. Changes from v1 (re-review on #1150, narrow REQUEST_CHANGES)

| Finding | v2 change |
|---|---|
| NB1: $279B labelled "cancellable" | Firmness is `partly_cancellable`. The quote now keeps "in certain instances" and "may result in additional costs". The risk factor "we may be unable to reduce these commitments" has its own record. Every sentence is reworded. |
| NB2: health file claimed a correction it had not made | OpenAI's SB7 signal is removed from `distress_signals`. `grade_rule` and `reasoning` are rewritten to the UNKNOWN rule, and the old fields are kept as `*_v0`. |
| NB3: two records mislabelled as critic-confirmed | Relabelled NOT_CHECKED with the true provenance. The counts are now 200 / 255. |
| Retrieval times | Each one is the earliest real download of exactly the hashed bytes; 201 records re-derived. Five hash-less secondary records with a typed time are now null. |
| Working-capital figures | Both the extra ($80B) and the total ($151B) are given; days are 106–112 on one basis. |
| Circular-share table | Rows using v1/v2's own tags (30%; 14% without the cap). |
| SoftBank "unrelated" | Own record citing slide 17, with the tension against the financial report noted. |
| Wording | Nscale equity is at cost, with $1.0B not closed; SpaceX's maturities are stated exactly; "firm (maximum)"; "could", not "would"; three v0 sentences softened; §3 heading softened. |
| Validator | Date-only retrieval on the build day is read as end of day; counted records need a hash; a total must equal the sum of its declared components. |
