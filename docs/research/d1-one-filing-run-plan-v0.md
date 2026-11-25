# D1 one-filing SEC observation — run plan v0 (plan only, nothing fetched)

Status: **plan, blocked**. This file authorises nothing. A fetch needs all three preconditions in section 1 and
Anik's separate word in the owner session. The collector, its unit, timer, wrapper and env stay untouched.

Owner: the Claude D-track session (SEC collector owner). Follows the proposal
`d1-collector-shadow-observation-proposal-v0.md`, review 6051894778 (docs ACCEPT, operational gates open) and the
independent Dell-delta review (PR review 6052702790 on #1424, ACCEPT_WITH_NOTES for offline code only).

## 1. Preconditions (all three must hold; today none is fully met)

| # | Precondition | State |
|---|---|---|
| P1 | **Reviewed code that can take one new filing.** The Dell delta is pinned to one cached Dell capture (submissions hash, document hash, claim span, accession), issues no request and refuses any other filing, so it cannot serve this plan. A parameterised version is a different delta: the author (Codex) writes it, an independent reviewer reads the exact head. | **Not met.** No such code exists. |
| P2 | **The Dell delta's real-bytes evidence is reproducible.** Four tests that touch the real Dell bytes skip unless fixtures are supplied, and the fixtures are not in the vault. The reviewer asked Codex for them (lane entry `GRID-D1-DELL-DELTA-FIXTURE-REQUEST-20261125`). Until then "robust and functional" is unproven for the real-bytes path. | **Open**, waits on Codex. |
| P3 | **An eligible filing exists** (section 2). | **Open**, depends on the market. |

The review's notes F1 (rehearsal marker is a label, not enforced) and F4 (filing-date identity check compares the
EDGAR filing date with the UTC date of the acceptance timestamp, wrong for filings accepted after 17:30 ET) must be
fixed or explicitly accepted in the parameterised code before P1 counts as met.

## 2. Eligibility and the frozen record (written before any request)

Freeze these in a file hashed and posted to the lane before the first request. Changing any field voids the plan.

- **Accession and issuer:** one 8-K already present in `earnings_events` from a normal 06:00Z run, `items`
  containing 2.02, issuer on the watchlist, no earlier observation of the same accession. The row is read once,
  read-only.
- **Scope:** the primary document and its EX-99.1 only.
- **Cutoff:** the latest time an observation is still useful for a D1 card, written as an absolute UTC time.
  A filing already past its cutoff is ineligible. It is not fetched to "see what happens".
- **Request budget:** at most 3 SEC requests in total (the filing index, the primary document, the exhibit),
  sequential, none retried, at least 1 second apart. The client identity is the collector's existing configured
  SEC identity. No new contact detail is invented or published.
- **Size bound:** abort if either document exceeds 5 MB or the response host or redirect differs from the SEC
  archive host.

## 3. What is run (after the preconditions and Anik's word)

1. Read the one row. Record `t_row_read`.
2. Fetch within the budget. Record `t_retrieved` per request.
3. Write the bytes to a throw-away scratch archive. Record `t_written`. Read them back and compare hashes.
   Record `t_readback`.
4. Run #1424's `diagnose_sec_exhibit_evidence` with a recording consumer **inside an isolated fixture
   injection only**. The production code path never sees the receipt.
5. Export the receipt. Check that the unchanged production diagnostic refuses it **as exported**, and that the
   marker (section 1, F1) makes a relabelled copy fail too. If the marker is not implemented, report that the
   refusal is label-only.
6. Delete the scratch archive.

Clock rule: `accepted ≤ public ≤ retrieved ≤ written ≤ readback ≤ cutoff`, using real times. The 06:00Z run time
is the collector's ingest clock, not a retrieval clock. A clock that would need backdating ends the run as
`ineligible`. It is never repaired.

**Stop rules:** any non-200, unexpected redirect, hash mismatch on readback, exceeded budget or size bound, or
clock out of order ends the run. No retry. Record status only.

## 4. What is published

Only: accession, hashes of the two documents and the claim, the clocks above, request count, status and the
diagnostic's refusal or qualification reason. Not the document text, not the recording consumer's input, not
account, key or path detail. No database write, no hold change, no ranking. **No price or outcome read: no
instrument and no dates are touched.**

## 5. What this plan does not do

- It does not approve the Dell delta for production, make any receipt primary, or change the earnings-release hold.
- It does not cover Item 2.02 classification by the producer or authenticated receipts (open limits of #1424).
- It does not choose the filing. The frozen record in section 2 does, once P1 and P2 hold.

## 6. Decision path

1. Codex publishes the Dell fixtures (P2) and writes the parameterised code with the marker fix (P1).
2. An independent reviewer (not this owner, and not the author) accepts the exact head.
3. An eligible filing appears (P3). The owner freezes section 2 and posts its hash.
4. The owner asks Anik in the owner session for the fetch. Anik's yes is for that one accession and budget.
