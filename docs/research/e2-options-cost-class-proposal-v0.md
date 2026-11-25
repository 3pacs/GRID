# E2 options cost class — version proposal v0 (proposal only)

Status: **proposal**. Nothing in this file bumps `evals.e2.VERSION`, edits `evals/e2/cost_model.json`, writes a
ledger or changes the running `grid-e2-scoreboard` job. A version bump needs Anik's separate approval and
Codex's review of the exact code (`evals/e2/README.md`, "Versioned").

Owner: the Claude D-track session (E2 scoreboard owner, named 2026-11-25). Answers Codex's handoff
`GRID-D2-G4-H1-COST-PREVIEW-20261125`.

## Why

D0 rev 6 §6.3 (owner-signed draft, #1142, not yet on main) says: "no options cost class exists in
`evals/e2/cost_model.json`. D2 must add `us_equity_option_0dte` to the cost model (versioned) before D3 can
score through E2." The D2-H1 fee is fixed in D0 §6.2: $0.0003 per share per leg, four entry legs only, never
at the synthetic exit.

## What E2 v1 cannot represent

The v1 scorer (`evals/e2/scoring.py`, blob `cf66ae36`) computes

    net_return = side * (exit_price / entry_price - 1) - 2 * per_side_bps / 1e4

and refuses any price that is not positive and finite (`scoring.py:75`). For the D2-H1 iron butterfly:

1. **Cash, not a return.** The result is dollars per share, `credit - payoff - 4 * fee`. There is no
   positive notional to divide by: the entry is a credit, and per D0 §6.2 the risk is `5 - credit`.
2. **Zero exit is normal.** The proxy exit is intrinsic value at the 15:59 close. When the close lands on the
   short strike the payoff is 0, which v1 rejects.
3. **Nonpositive credit must stay representable.** A fill at bids and asks can give a credit of 0 or less.
   That is a valid (losing) row, not an error.
4. **Wrong cost shape.** v1 charges bp of notional on both sides. D0 charges a fixed cash fee on the four
   entry legs only. The spread is already paid because entry fills are at the bid and ask, so there is no
   half-spread or slippage term. The ETF classes must not be reused (Codex's handoff says the same).

## Proposed v2 rule (exact)

New cost model `e2-costs-v2`. Every v1 class is copied byte-for-byte, plus one class:

```json
"us_equity_option_0dte": {
  "units": "USD per share per leg, charged on entry legs only",
  "fee_per_share_per_leg": 0.0003,
  "legs_charged": "entry",
  "half_spread": 0,
  "slippage": 0,
  "basis": "D0 rev 6 §6.2/§6.3, bound to GEX edge6 (protocol cb7b213, vrp_edge6.py at 35fa182: FEE = 0.0003). Fills at bid/ask (MODELED_BBO); full fill at the quoted price; the T+1-minute sensitivity is reported, not judged."
}
```

Metric for this class, as a new `net_cash_per_share` metric (`METRIC_KIND` sum) beside `net_return`:

    net_cash_per_share = round(credit - payoff - n_entry_legs * fee_per_share_per_leg, 4)

- `credit` = short call bid + short put bid - long call ask - long put ask, at the mark (§6.3 four-leg rule).
- `payoff` = the intrinsic value of the fly at S, which can be 0. `risk_per_share = 5 - credit` is recorded
  beside the net value, never divided into it.
- **edge6 parity:** edge6 computes `round(credit - straddle_pay + wing_pay - 4 * FEE, 4)` in float per row
  (`vrp_edge6.py:62`). v2 must use the same expression order and rounding per row, and a test must
  reproduce edge6's per-row value on synthetic rows to the 4th decimal, including credit ≤ 0 and payoff 0.
- Per contract this is ×100: $0.12 of fees per fly, matching Codex's preview ($0.0012 per share).
- **Denominators unchanged:** a session that fails §6.3 is `EXCLUDED_INPUT`. It counts in the exclusion
  count, never as a zero-P&L trade, and the candidate and exclusion counts must equal the D2 producer's.

## Effect on past scores

- **v1 is untouched.** v2 writes its own `e2_scoreboard_e2-v2.jsonl`, anchors and `MANIFEST.sha256` entry, and
  `RELEASED_MANIFESTS` gains one line. The v1 ledger and its vault anchors are never edited.
- **Nothing past changes meaning.** No E2 record today uses an options class: S10 is `not_tradable` and
  GEX-levels v1 uses an ETF class. A required test: v2 run over v1's inputs gives byte-identical metrics for every
  existing stream.
- **No D2-H1 score exists yet.** The H1 inputs are not fetched, so v2 only enables future D3 scoring. It rescores
  no history.
- **No activation.** Shipping v2 code does not change the running timer. Pointing the job at v2, or running v2
  next to v1, is a separate owner step with its own approval, and only when D3 needs it.

## Open before a version PR

1. Codex's prototype (implementation sha256 `de646281…6e4ff8`, 12 synthetic tests) is not reachable from
   the vault or this repo. Retain it at a pinned vault path so the v2 code can be checked against it for parity.
2. Whether D2-H2 (fly-shadow/1, registered fe8318a with its own fees) ever goes through E2. This proposal
   leaves H2 and H3 untouched, as D0 and Codex both require.
3. Anik's approval of the version bump, and Codex as the independent reviewer of the exact v2 head.
