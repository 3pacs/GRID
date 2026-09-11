# 05 — Insider events: carry the Form 4 transaction code so motivation is not "Unknown"

Branch: `claude/handoff-05-insider-form4-code`. Lane: code + one ops-exec backfill.

## Evidence

Lever map rerun 2026-09-10: 4,743 insider events in 30 days, 3,147 narrate
"Unknown motivation". `intelligence/lever_pullers.py::assess_motivation` can
only separate planned 10b5-1 sells, option exercises and grants from
discretionary trades if the row says which it is. The Form 4 puller
(`ingestion/altdata/insider_filings.py`) parses `transactionCode` (line ~390)
and maps it through `_TXN_CODES` to BUY/SELL, then `_emit_signal` writes
`signal_value = {insider_title, shares, price, value, is_derivative,
is_unusual_size}` — the code itself, the 10b5-1 footnote flag and the
ownership nature are dropped.

## Design

1. Puller: keep `transaction_code` (P, S, A, M, F, G, …), `is_10b5_1`
   (Form 4 `<aff10b5One>` element or the footnote text "10b5-1"), and
   `direct_or_indirect` in the trade dict and in `signal_value`. Do not change
   `signal_type` semantics (BUY/SELL/UNUSUAL_*) or the conflict key.
2. `assess_motivation` (lever_pullers): for `source_type='insider'`
   - `is_10b5_1` true → `routine` ("10b5-1 plan sale")
   - code `M`/`A`/`F`/`G` → `routine` (exercise / grant / tax withholding / gift)
   - code `P` with `value >= 100000` or officer title containing CEO/CFO/Chief/Director → `likely_informed`
   - code `S` without a plan flag and `value >= 250000` → `likely_informed` (discretionary sale)
   - else `routine`.
   `_motivation_narrative` prints the code and the plan flag.
3. Backfill script `scripts/backfill_form4_codes.py`: re-parse the stored Form 4
   documents (the puller keeps `raw_payload` or the accession number — read it)
   for the last 120 days and `UPDATE signal_sources SET signal_value = signal_value || :patch`
   (jsonb merge, parameterized) only for rows lacking `transaction_code`.
   Idempotent; `--dry-run` first; bounded by `signal_date`.
4. Tests: puller parsing (fixture XML with `aff10b5One`), motivation rules,
   narrative text, backfill SQL shape (fake conn).
5. After merge: run the backfill detached on grid-svr, then
   `identify_lever_pullers` + `get_active_lever_events(days=30)` and report the
   new motivation mix (target: "Unknown motivation" well under 20 % of insider events).

## Guardrails

Never write to `decision_journal`. Keep `signal_sources` unique key intact.
`trust_scorer._extract_price` reads `signal_value["price"]` — keep `price` as
the trade price per share, do not repurpose it.
