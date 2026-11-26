# D2-H1 inspection-ledger reconciliation (G5, free step 1) — DRAFT FOR REVIEW

Status: draft, 2026-11-26. Owner: Claude. Reviewer: Codex (not yet reviewed). Authorised by the operator on 2026-11-25
("approve G4 and G5 free parts only"). This step reads records and fetches nothing, spends nothing and opens no protected
reservation. Machine-readable source: `d2-h1-ledger-reconciliation-v0.json`.

Contract section 6.2 requires, before any pre-2022 fetch, a reconciliation of the full inspection ledger to confirm that no
SPY option-premium family, entry time or contract scope has been tested on 2018-01-02 .. 2022-11-11, and strikes a cell that
was.

## What was found

1. **No SPY option premium before 2022-11-14 has ever been fetched.** The four OPRA fetchers in the research lane (edge2,
   edge4, edge6 XSP, edge6 SPXW) start SPY and XSP at 2022-11-14. Only SPXW starts earlier, at 2016-11-15.
2. **Section 4 of the contract is incomplete.** It omits nine records that exist in the lane: edge6-SPX, edge9, edge10,
   edge11, edge12, eod-flows, the playbook evidence cuts, flow-sign and the factory development data. Five of them have
   windows or families that matter here (below). The other four are after the target window or are different families.
3. **One judged look is on the target window's sessions with the same strategy:** the edge6 follow-up that ran the 15:00
   short iron fly on SPXW over 2016-11-15 .. 2022-11-11 (verdict class FAIL; 14:00 and 14:30 entries reported only, no
   verdict). It is a different underlying, but it is the same structure on the same days, and its own record concludes the
   premium "is in SPY options only". That conclusion is part of why D2-H1 exists.
4. **Three SPY price-path reads cover the target window** without reading option premium: edge9's holdout (2019-01-02 ..
   2022-11-11), the eod-flows exploratory read (2018-05-02 ..) and the playbook evidence cuts (2018-05-01 ..). D2-H1's
   payoff proxy depends on the SPY path to 15:59, so these are disclosed in the selection ledger.
5. **Protected reservations are untouched:** edge3's daily-SPY calendar holdout (2013 .. 2026-11-10) and the insider
   reservations are not opened by any step here.

## The decision this needs (not made here)

Section 6.2: "if reconciliation finds an overlap, that cell is struck before the fetch." Whether the SPXW fly counts as the
same family for that rule is the owner's and reviewer's call, and it changes what a fetch is worth:

- **If it counts,** both cells (14:00 and 15:00) are struck, D2-H1 is dropped, and no spend is needed.
- **If it does not count,** the cells proceed as replications on a window that is untouched for SPY option premium but not
  for the family or the SPY price path; the result is reported with these overlaps beside it.

My reading is that the second is defensible only with the disclosure above and a plain statement that D2-H1 is not an
independent test. I have not decided it, and the fetch manifest and quote are held until it is decided.

## Not read in this pass

gamma-vol (optflow), exits (trail-arm), burnin-v2, overnight-shadow, nvda-watch, gex-trader, desk-state, bounce-shadow,
fly-shadow. File names show registration on or after 2026-11-06 or forward shadows; their windows are unconfirmed. They are
listed in the JSON, not silently omitted. The reviewer should confirm or I will read them next.

## Limits

The GEX research repository is private, so this file copies only ids, windows and verdict classes. A row marked as not
overlapping was read for its window, not re-run.
