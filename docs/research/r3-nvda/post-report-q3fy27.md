# R3 NVDA: Q3 FY2027 report vs the frozen pre-report ledger

Written 2026-11-18. This is research only. It does not change `ledger-v2.jsonl` or
`synthesis-v2.md`, which stay frozen. It holds no trade view, and the market reaction is
out of scope.

The frozen reference is `synthesis-v2.md` §5 and `ledger-v2.jsonl` on origin/main
`95e85349`. Record ids are given in brackets.

## Summary

- **Revenue came in above the whole guide range.** Reported $115.8B ($115,844M). The guide
  was $108.0B ±2%, a range of $105.8B–$110.2B. That is +$7.8B (+7.3%) over the midpoint and
  +$5.7B over the top. The guide assumed no China Data Center compute revenue. The CFO
  commentary says $2.4B of Data Center Hopper products shipped to China in the quarter.
  Even without that $2.4B, revenue would still be above the top of the range. The filings do
  not say whether that $2.4B counts as "compute revenue" in the sense the guide used.
- **Gross margin came in above the guide range.** GAAP 74.8% and non-GAAP 74.9%, against a
  guide of 74.0% ±50 bp (73.5%–74.5%). That is +80 / +90 bp over the midpoint. Margin fell
  slightly from Q2's 75.0%, which NVIDIA puts down to "the initial Rubin ramp".
- **Cash conversion recovered, but receivables kept growing.** This was the ledger's main
  open question.
  - Q3 free cash flow was $43.8B, or 37.8% of revenue (derived). Q2 was $21.3B, or 22%.
    The nine-month figure is 38.7% (derived), against 39.3% for H1.
  - Part of the jump from Q2 is tax timing, which the ledger flagged: NVIDIA says the
    sequential rise in operating cash flow was "also impacted by the timing of federal cash
    tax payments".
  - Receivables went up again, from $63.1B to $81.0B (+$18.0B). DSO went from 60 to 64 days.
    NVIDIA names two causes: Rubin shipment linearity, and "extended payment terms on large,
    multi-quarter agreements with certain investment-grade customers".
  - Inventory went from $31.6B to $39.7B (+$8.1B).
- **Supply commitments, buy-backs and guarantees all moved.**
  - Supply commitments rose from $279B to **$390B** (+$111B). NVIDIA says this is mainly
    multi-year memory procurement.
  - The AI-cloud agreements now go by "Neocloud agreements", at **$32B** (was $36B).
  - Cloud service agreements are **$28B** (was $29B).
  - The maximum guarantee exposure is **$118.0B** (was a $108.5B cap). The increase is a new
    $9.5B line of "Power and systems credit support guarantees". The $3.5B neocloud line and
    the $105B SB Energy line are unchanged.
  - NVIDIA says it has made no payments under its guarantees. The commentary does not give
    an "in force" amount, which needs the 10-Q.
  - Equity-investment commitments still outstanding are $20B (was $25B). The funded
    cumulative total is not reported.
- **The Q4 FY27 outlook includes an extra week.**
  - Revenue $130.0B ±3%, again with no China Data Center compute assumed.
  - GAAP and non-GAAP gross margin 71.0%–72.0%. That is about 300 bp lower than this
    quarter's guide midpoint.
  - Opex about $11.0B GAAP and $10.7B non-GAAP.
- **On the CEO transition, NVIDIA said little.** Colette Kress signs as "Interim Chief
  Executive Officer, Executive Vice President and Chief Financial Officer". The
  forward-looking-statement language now covers "the search for a new chief executive
  officer" and "the ongoing investigation by an independent committee of NVIDIA's board".
  The release and the CFO commentary say nothing about any cancellation, and nothing about a
  counterparty's reaction to the CEO change. No 8-K has been filed since 2026-11-12T13:52:41Z
  other than this earnings 8-K; the only other filing in that window is a 13F-HR on 11-16.
- **The Q3 10-Q has not been filed yet.** The CFO commentary says "We plan to file our 10-Q in
  the coming weeks."

## Sources

All sources come from one filing.

- **Filing:** NVIDIA 8-K, accession `0001045810-26-000088`.
- **Accepted:** 2026-11-17T21:20:51Z.
- **Items:** 2.02 and 9.01.
- **Retrieved:** 2026-11-18 from SEC EDGAR, using the submissions feed
  `https://data.sec.gov/submissions/CIK0001045810.json`.
- **Base URL:** `https://www.sec.gov/Archives/edgar/data/1045810/000104581026000088/`.

| Document | File | sha256 of raw bytes |
|---|---|---|
| 8-K cover | `nvda-20261117.htm` | `0ab482bed1940475cacc97a30ae8246f6c9f86b94f08b01afb93bbdfb5400e51` |
| EX-99.1 press release (PR) | `q3fy27pr.htm` | `699e2bf368f8a57fd056d7edb7d9d97f8abf4e57ec086fe988fd4933a169ef35` |
| EX-99.2 CFO commentary (CFO) | `q3fy27cfocommentary.htm` | `1d1e1fc9c2500058535956bacecb8718cb305c565b97b5716c86e8506572b75a` |
| EX-99.3 supplemental financial information (SUPP) | `supplementalfinancialinfor.htm` | `f19a674ae26d2d8f941ca8cb9aecc195a5db9ca70ee4a1df179207301e865f99` |
| Filing index | `0001045810-26-000088-index.html` | `94968a98b3fdc1797b9aefded04aad176415c95d935cc5338e18df7e6b101ec6` |

Other filings since the 11-12 8-K (`0001045810-26-000082`, Items 5.02 and 9.01):

- One 13F-HR, `0001045810-26-000084`, accepted 2026-11-16T21:41:59Z.
- No other 8-K.
- No 10-Q.

Some table rows say "derived". Those values are arithmetic on the quoted figures; no source
states them. Table quotes use the HTML-to-text form of each document, so table cells appear
separated by `|`.

## Comparison table

| Observable | Frozen pre-report value (record id) | Reported value | Quote with source | Delta | Note |
|---|---|---|---|---|---|
| Q3 revenue | guide $108.0B ±2% (`nvda-pr-q3fy27-outlook-no-china`) | $115,844M | PR: "reported revenue for the third quarter ended October 25, 2026, of $115.8 billion, up 20% from the previous quarter"; income statement "Revenue \| $ \| 115,844" | +$7.8B vs midpoint (+7.3%); +$5.7B over the top of the range ($110.16B) | Above the range |
| China DC shipments (context for the no-China guide) | guide assumed no China DC compute (`nvda-pr-q3fy27-outlook-no-china`) | $2.4B Hopper shipments | CFO: "Shipments of Data Center Hopper products to China during the quarter were $2.4 billion." | n/a | Ex-$2.4B revenue ≈ $113.4B (derived), still above the range. The filings do not say whether these shipments are "compute revenue" in the guide's sense |
| Q3 gross margin | guide 74.0% ±50 bp (`nvda-pr-q3fy27-outlook-gm`) | GAAP 74.8%, non-GAAP 74.9% | PR: "For the quarter, GAAP and non-GAAP gross margins were 74.8% and 74.9%, respectively." | +80 bp / +90 bp vs midpoint; +30 / +40 bp over the top | Above the range. CFO: "decreased slightly sequentially, reflecting the initial Rubin ramp" |
| Q3 free cash flow | Q2 $21.3B (`nvda-pr-q2fy27-fcf`) | $43,777M | PR reconciliation: "Free cash flow \| \| $ \| 43,777 \| \| \| $ \| 21,341" (Q3 FY27, Q2 FY27) | +$22.4B q/q | Q3 OCF $48,253M vs Q2 $24,077M |
| FCF / revenue, quarter | Q2 22% (`nvda-pr-q2fy27-fcf`) | 37.8% (derived: 43,777 / 115,844) | as above, plus revenue 115,844 | +15.6 pp | Close to the H1 level again |
| FCF / revenue, year to date | H1 FY27 39.3% (`nvda-pr-q1fy27-fcf` + `nvda-pr-q2fy27-fcf`) | 9M FY27 38.7% (derived: 113,672 / 293,681) | PR reconciliation, nine months: "Free cash flow ... 113,672"; "Revenue ... 293,681" | −0.6 pp | |
| Tax timing in operating cash flow | Q2 federal tax timing (`nvda-10q-q2fy27-federal-tax-payments-timing`) | Named as a driver | CFO: "the sequential increase was also impacted by the timing of federal cash tax payments." | n/a | Part of the Q3 cash rebound is timing. The amount is not given; needs the 10-Q |
| Accounts receivable | $63.1B (`nvda-10q-q2fy27-accounts-receivable`, 63,059M) | $81,033M | CFO: "Accounts receivable was $81.0 billion with 64 days sales outstanding (DSO), up from 60 days sequentially"; balance sheet "Accounts receivable, net \| 81,033" | +$17.97B q/q | Cash-flow line "Accounts receivable \| (18,052)" for Q3 |
| DSO | 60 days (`nvda-cfo-q2fy27-dso`) | 64 days | CFO, same sentence: "driven by shipment linearity from our Rubin launch and extended payment terms on large, multi-quarter agreements with certain investment-grade customers." | +4 days | Extended payment terms are now named as a cause (compare `nvda-10q-q2fy27-extended-payment-terms`) |
| Receivables concentration | n/a in §5 (Q2: `nvda-10q-q2fy27-ar-concentration`) | 18% and 17% | SUPP: "Two direct customers accounted for 18% and 17% of our accounts receivable balance as of October 25, 2026." | n/a | Context only |
| Inventory | $31.6B (`nvda-10q-q2fy27-inventory`, 31,575M) | $39,724M | CFO: "Inventory was $39.7 billion, up from $31.6 billion sequentially, to support our Rubin product ramp." | +$8.1B | SUPP: raw materials $15,802M, WIP $17,767M, finished goods $6,155M; Q3 provisions $680M |
| Supply commitments | $279B (`nvda-10q-q2fy27-supply-commitments`) | $390B | CFO: "Our supply commitments increased from $279 billion last quarter to $390 billion, primarily related to the multi-year procurement of memory supply." | +$111B (+40%) | By year: rest of FY27 $69B, FY28 $203B, FY29 $92B, FY30 $10B, FY31 $8B, FY32+ $8B |
| Supply commitments: cancellability | cancellable only "in certain instances", share not disclosed (`nvda-10q-q2fy27-supply-commitments-text`) | not reported yet (needs 10-Q) | n/a | n/a | The CFO commentary does not say whether these commitments can be cancelled |
| AI-cloud buy-back agreements (now "Neocloud agreements") | $36B (`nvda-10q-q2fy27-ai-cloud-agreements`) | $32B | CFO: "Neocloud agreements (1) \| $ \| — \| ... \| 32"; "(1)Formerly referred to as "AI cloud agreements"" | −$4B | Renamed. NVIDIA says commitments "take effect only if the neoclouds secure financing ... and the related compute capacity is deployed, made operational, and accepted by us" |
| R&D cloud service agreements | $29B (`nvda-10q-q2fy27-cloud-service-agreements`) | $28B | CFO table: "Cloud service agreements \| 2 \| ... \| 28" | −$1B | CFO: cloud service plus DC lease commitments together are "$102 billion, an increase from last quarter's $74 billion". DC leases not yet commenced are $74B |
| Guarantees: maximum | $108.5B cap (`nvda-10q-q2fy27-guarantee-total`) | $118.0B | CFO: "Land, power, and shell guarantees for neoclouds \| $ \| 3.5"; "SB Energy Corp. credit support guarantees \| 105.0"; "Power and systems credit support guarantees \| 9.5"; "Total \| $ \| 118.0" | +$9.5B | The new line is "Power and systems credit support guarantees". Its counterparties are not named. CFO: "To date, we have not made any payments related to our guarantees." |
| Guarantees: in force | $3.5B at 2026-07-26 (`nvda-10q-q2fy27-guarantee-total`) | not reported yet (needs 10-Q) | n/a | n/a | The CFO table shows "maximum gross exposure" only. For SB Energy: "The first is expected in fiscal 2029." |
| Equity investments funded | $99B (`nvda-10q-q2fy27-equity-investments-99b`) | not reported yet (needs 10-Q) | n/a | n/a | SUPP rollforward: non-marketable equity "Net additions \| 15,431" in Q3, ending balance $61,650M. That is a balance-sheet figure, not the cumulative funded total, so it is not comparable |
| Equity investments committed | $25B (`nvda-10q-q2fy27-equity-investment-commitments`) | $20B | CFO table: "Equity investments \| 5 \| \| \| 11 \| \| \| 2 \| \| \| 2 \| ... \| 20" | −$5B | A smaller outstanding commitment is consistent with funding during the quarter. The split is not given |
| Largest direct customer share | 16% (`nvda-10q-q2fy27-customer-concentration`) | 14% | SUPP: "For the third quarter of fiscal year 2027, three direct customers represented 14%, 11%, and 11% of total revenue" | −2 pp | Three customers at ≥10% this quarter. Nine months: 13%, 12%, 12%, 11% |
| CEO change: cancellation or counterparty statement | none found as of 11-12 (`nvda-8k-20261112-ceo-resignation`, `nvda-8k-20261112-ex991-commitments-continue`) | none in these documents | PR: Kress quoted as "interim CEO"; forward-looking statements cover "NVIDIA's leadership transition and the search for a new chief executive officer" and "the ongoing investigation by an independent committee of NVIDIA's board of directors" | no change | No new 8-K since 11-12 other than this one |
| Q4 FY27 outlook (new) | n/a | revenue $130.0B ±3%; GM 71.0%–72.0%; opex ≈$11.0B GAAP / $10.7B non-GAAP | PR: "NVIDIA's outlook for the fourth quarter of fiscal 2027, which includes an additional week"; "Revenue is expected to be $130.0 billion, plus or minus 3%. NVIDIA is not assuming any Data Center compute revenue from China in its outlook."; "GAAP and non-GAAP gross margins are expected to be between 71.0% and 72.0%." | GM guide midpoint −250 bp vs Q3 guide; −340 bp vs reported Q3 non-GAAP | Q4 has 14 weeks. The commentary gives no reason for the lower margin guide |
| Share repurchase authorization (context) | Q2 buyback record (`nvda-pr-q2fy27-buyback`) | +$150B authorization; $227.3B remaining | PR: "approved an increase of $150 billion to the company's share repurchase authorization, without expiration"; CFO: "$227.3 billion of share repurchase authorization remaining which we plan to utilize through fiscal 2028." | n/a | Q3 capital returned: $28.1B |

## What waits for the 10-Q

These items are not in the earnings release, the CFO commentary or the supplemental
information:

- whether the supply commitments can be cancelled, and how much;
- the amount of guarantees in force at quarter end;
- the cumulative equity investments funded;
- the dollar amount of the federal tax timing effect;
- the extended-payment-terms disclosure;
- receivables aging;
- who the counterparties to the new $9.5B power-and-systems guarantees are.
