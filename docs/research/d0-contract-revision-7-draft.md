# D0 contract revision 7 (D2-H1 struck) — APPLIED at signing, 2026-11-26 (history)

> This file is the draft record. Revision 7 is now signed and pinned in the contract itself; the text below describes what the draft did and is kept as history.

Drafted 2026-11-26 by the research owner (Claude) on the operator's "Yes, drop D2-H1 and draft it". Machine-readable form:
`d0-contract-revision-7-draft.json`. The signed revision 6 body stays pinned and unedited; this draft is a set of
anchored replacements that apply mechanically to it. Signing and pinning need the reviewer's ACCEPT and the operator's
separate go, as for revision 6.

## Why

The G5 ledger reconciliation found the 15:00 short iron fly (14:00 and 14:30 reported) was already judged on SPXW over
2016-11-15 .. 2022-11-11. The reviewer (review 6068505242) and the owner agree that is the same selection family, so
section 6.2 strikes both D2-H1 cells before any fetch. No D2 outcome had been read.

## What changes in the contract

- **Header, title and §3.1:** every signature display now says revision 6 was signed and revision 7 is an unsigned draft; §3.1 no longer calls the pre-2022 window a candidate replication window.
- **§6.2:** D2-H1a and D2-H1b are struck and kept as history. The pre-2022 window paragraph records the outcome.
- **§6.5:** the GO rule required D2-H1 PASS, so it is rewritten rather than left dangling. **The fly structure cannot reach
  GO in this version.** A D2-H2 PASS alone is HOLD (`FORWARD_PASS_UNREPLICATED`); a D2-H2 FAIL is still RETIRE. A GO needs a
  separately authorised, labelled non-independent replication registered in a new version before anything is fetched.
- **§6.4 / §6.6:** the H1 tests are void; the selection disclosure adds the SPXW look and the SPY price-path reads; no OPRA
  spend is authorised or needed.
- **§4 / §9:** the ledger gains the omitted lane records; G5 is closed; G4 is no longer blocking (D2-H2 and D2-H3 keep their
  own costs) but stays open for any later E2 scoring of options.
- **§10.6:** revision log (the existing §10.5 is the revision 6 owner signature and is kept).

The old D2-H1 text is kept, not deleted, but only inside marked history blocks (`D2-H1-HISTORY-BEGIN` / `END`) or
struck table rows. A test renders the revision 7 body, removes the history and the revision log, and fails if any remaining
live line names H1a, H1b or D2-H1 without saying it is struck, or if the GO, HOLD, precedence and RETIRE rules differ from
revision 7's (D2-H2 FAIL is RETIRE; D2-H2 PASS or UNDERPOWERED is HOLD; GO is not reachable; D2-H3 decides only itself). That
check is lexical: it finds a surviving live H1 rule, not a mistaken one, so the reviewer's reading still matters.

D2-H2, D2-H3 and every D1 rule are unchanged. This is a real tightening, not a relaxation: removing the replication would
otherwise make GO easier, so GO is made unreachable instead.

## What this draft does not do

It does not sign, re-pin, flip `VERIFIER_ACCEPTED`, or touch the admission code. At signing the body is edited, the pin
constant and the coverage manifest are updated, D2-H1a and D2-H1b are removed from the verifier's frozen definitions (so an
ADMITTED row for either is refused), and the tests are updated (listed in the JSON under `at_signing_not_in_this_draft`).
