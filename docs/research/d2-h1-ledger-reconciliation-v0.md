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
2. **Section 4 of the contract is incomplete.** It omits seventeen records that exist in the lane (the JSON lists each).
   Four bear on the target window: edge6-SPX, edge9, eod-flows and the playbook evidence cuts (below). Edge10, edge11 and
   edge12 share dates but are different families and instruments, and gamma-vol part 2 is a judged daily index-gamma
   series look over 2020-01-02 .. 2026-11-17, also a different family (below). The rest are later than the window or
   forward-only.
3. **One judged look is on the target window's sessions with the same strategy:** the edge6 follow-up that ran the 15:00
   short iron fly on SPXW over 2016-11-15 .. 2022-11-11 (verdict class FAIL; 14:00 and 14:30 entries reported only, no
   verdict). It is a different underlying, but it is the same structure on the same days, and its own record concludes the
   premium "is in SPY options only". That conclusion is part of why D2-H1 exists.
4. **Three SPY price-path reads cover the target window** without reading option premium: edge9's holdout (2019-01-02 ..
   2022-11-11), the eod-flows exploratory read (2018-05-02 ..) and the playbook evidence cuts (2018-05-01 ..). D2-H1's
   payoff proxy depends on the SPY path to 15:59, so these are disclosed in the selection ledger.
5. **Protected reservations are untouched:** edge3's daily-SPY calendar holdout (2013 .. 2026-11-10) and the insider
   reservations are not opened by any step here.

## The decision this needs

Section 6.2: "if reconciliation finds an overlap, that cell is struck before the fetch."

- **Reviewer (Codex, review 6068505242 at `0e109fad`):** the SPXW fly is the same selection family. Strike the 15:00 cell,
  and keep the 14:00 cell off any fetch manifest too, because a reported-only look is still a look. Any replication must be
  proposed separately, authorised separately, and labelled non-independent, never "untouched".
- **Owner (Claude):** agrees. The reasoning is that a different ticker and settlement make a different estimand, not an
  independent selection: the SPXW result is the stated reason the SPY-only premise exists.
- **Not yet done:** the owner decision belongs in a versioned contract revision (the signed body is pinned, so this is a new
  version, not an edit); an unsigned draft exists (#1470), no signed or effective revision has been adopted, and the operator has been asked. This file records the position and does not make that revision.
- **Consequence:** if both cells are struck, D2-H1 is dropped, G5 needs no spend, the fetch manifest and the Databento quote
  are not built, and the E2 options cost class (G4) has no hypothesis to serve in v1.

## Not read

None remain. The last item, gamma-vol (optflow), was read from its branch on 2026-11-26: its option data is the free
trailing year only (from 2025-11-19), so it holds no SPY option premium before 2022-11-14. Its second look is a judged
regression of a long daily index-gamma series against volatility, with a fit through 2019-12-31 and a check from 2020-01-02
to 2026-11-17. That is a daily-series look over most of the target window's dates, not an option-premium look, so it is
disclosed beside edge3 and edge9 and does not change the same-family finding.

## Limits

The GEX research repository is private, so this file copies only ids, windows and verdict classes. A row marked as not
overlapping was read for its window, not re-run.
