# OPEX Waterfall Playbook

> The short-term tape is written by ~$2T of structured-product hedging flows,
> because that is where dealers are forced to transact predictably. The rest
> of the $14T alternative-asset pool (crypto, metals, hedge funds) does not
> set the short-term tape because nobody in those pools is mechanically
> forced to buy or sell at a specific time for a specific reason.
>
> The entire discipline below exists to keep us on the right side of the
> waterfall when dealer positioning flips from rubber band to slingshot.

## Core Principle

**Be the dealer's friend, not his counterparty, when he is forced to transact.**

The waterfall only hurts people who are on the wrong side of a *forced* flow.
Dealers are not predicting anything — they are mechanically rebalancing. If
you know what they have to do and when, you can stand beside them instead of
in front of them.

## The Two Regimes

### Long gamma (rubber band)
- Dealers are forced to **sell strength** and **buy weakness**
- Realized vol is suppressed, ranges are tight, dips get bought
- Price is magnetized toward strike clusters — this is **pinning**
- **Strategy:** sell vol, fade extremes, carry tail hedges cheaply

### Short gamma (slingshot)
- Dealers are forced to **buy strength** and **sell weakness**
- Realized vol expands, moves accelerate, trends persist intraday
- Price gaps in whichever direction it starts moving
- **Strategy:** buy vol, follow the trend, do NOT fade

The single most expensive mistake is selling vol in a short-gamma regime
"because the premium looks fat." The premium is fat because the slingshot
is loaded.

## The Five Forced-Flow Conditions (Watched Every Morning)

`intelligence/forced_flow_monitor.py` evaluates these daily. Two or more
tripped simultaneously fires `alerts/waterfall_watch.py`.

| # | Condition | Why it matters |
|---|---|---|
| 1 | `gamma_flip_proximity` — SPY spot within 1.5% of flip | Regime can transition on a small move |
| 2 | `short_gamma_regime` — aggregate GEX < 0 | Dealer positioning is already slingshot mode |
| 3 | `put_wall_proximity` — spot within 2% of SPY put wall and below it | Key support with slingshot cliff if broken |
| 4 | `high_impact_catalyst_within_5d` — quarterly OPEX / JHEQX / FOMC / autocall obs | Forced flow schedule collision |
| 5 | `compound_regime_catalyst` — short gamma AND catalyst within 3 days | The actual waterfall setup |

## The Four Key Dates

### Quarterly OPEX (third Friday of Mar/Jun/Sep/Dec)
- 3–5× the open interest of a monthly OPEX
- Pinning forces dissolve at 4pm close
- A new rubber band gets installed at new strike levels for the next quarter

### JHEQX + buffer ETF roll (last business day of Mar/Jun/Sep/Dec)
- JPMorgan Hedged Equity Fund (~$20B) always does the same trade:
    - Buys SPX put spread ~5% OTM
    - Sells SPX call ~3–5% OTM
    - Same quarterly expiry forward
- The **sold call strike** becomes a natural ceiling for the next quarter
- The **bought put strike** becomes a softer floor
- Innovator / First Trust / Global X buffer suites roll around the same window
  for another ~$80–100B of forced collar flow

### FOMC statement days
- Vol crush immediately after the release
- Regime can flip on the statement itself
- **Never hold a gamma-regime trade through the 2pm statement**

### Autocall observation dates (quarterly, aligned with OPEX)
- SX5E / KOSPI / NKY autocall observation clusters
- Knock-in barriers typically 60–65% of initial
- If spot within 10% of a barrier cluster → inverted regime: short vol is
  unsafe, buy downside protection instead

## The 2026 Quarterly Calendar

| Event | Date |
|---|---|
| Q1 Monthly OPEX | Jan 16, 2026 |
| Feb Monthly OPEX | Feb 20, 2026 |
| **Q1 Quarterly OPEX** | **Mar 20, 2026** |
| JHEQX Q1 roll | Mar 31, 2026 |
| FOMC | Jan 28, Mar 18, Apr 29 |
| Apr Monthly OPEX | Apr 17, 2026 |
| May Monthly OPEX | May 15, 2026 |
| **Q2 Quarterly OPEX** | **Jun 19, 2026** |
| JHEQX Q2 roll | Jun 30, 2026 |
| FOMC | Jun 17, Jul 29 |
| Jul Monthly OPEX | Jul 17, 2026 |
| Aug Monthly OPEX | Aug 21, 2026 |
| **Q3 Quarterly OPEX** | **Sep 18, 2026** |
| JHEQX Q3 roll | Sep 30, 2026 |
| FOMC | Sep 16, Oct 28 |
| Oct Monthly OPEX | Oct 16, 2026 |
| Nov Monthly OPEX | Nov 20, 2026 |
| **Q4 Quarterly OPEX** | **Dec 18, 2026** |
| JHEQX Q4 roll | Dec 31, 2026 |
| FOMC | Dec 9, 2026 |

FOMC dates are sourced from the Federal Reserve 2026 meeting calendar and
maintained in `intelligence.forced_flow_monitor.FOMC_DATES_2026`.

## The Daily Routine

Every trading morning, Hermes runs `run_forced_flow_cycle` which does:

1. **Regime check** — reads `physics/dealer_gamma.get_market_gex_summary`
2. **Calendar check** — enumerates forced-flow events in the next 10 trading days
3. **Threshold scan** — evaluates the five forced-flow conditions
4. **Waterfall score** — counts tripped conditions
5. **Posture generation** — emits LEVER / CONDITION / THESIS / INVALIDATION
6. **Alert** — if score ≥ 2, fires `alerts/waterfall_watch.send_waterfall_alert`
7. **Persist** — stores the briefing in `forced_flow_briefings` table

Pre-market humans should review this briefing before placing any new trades.
The briefing is the answer to the daily question: *who is going to be forced
to transact in the next 10 days, in what direction, triggered by what?*

## Pre-Trade Checklist

Before sizing any new position, confirm:

- [ ] What regime are we in? (long gamma / short gamma / unknown)
- [ ] Where is the gamma flip, and how close is spot?
- [ ] What's within 5 trading days? (OPEX, FOMC, JHEQX roll, autocall obs)
- [ ] Any forced-flow threshold tripped? (check `forced_flow_briefings`)
- [ ] Any invalidation triggers from prior positions still active?

## In-Trade Rules

1. **Size for the slingshot, not the rubber band.** Run Kelly on the
   short-gamma loss distribution, not the long-gamma one.
2. **No short vol within 3 trading days of a high-impact catalyst.**
3. **Always carry cheap tail hedges during pinning regimes.** Pay for them
   with smaller pinning-trade harvests.
4. **Exit on pre-committed invalidation** without renegotiating.

## Waterfall Invalidation Template

Every short-vol or pinning trade must carry a written invalidation at entry:

```
INVALIDATION:
  - SPY closes below gamma flip level for 2 consecutive sessions, OR
  - VIX term structure inverts (1M > 3M), OR
  - Realized 5d vol exceeds implied 5d vol by > 20%, OR
  - Waterfall score reaches 3+ of 5 tripped conditions

ACTION ON TRIGGER:
  - Exit full position
  - Flip to long vol via tail hedges
  - No negotiation, no "one more day"
```

## What This Does Not Cover

- Single-name earnings vol — different mechanics, different timing
- Commodity-specific forced flows (roll calendars, inventory reports)
- Crypto perpetual funding cascades — orthogonal signal domain
- Macro regime shifts beyond the 10-day lookahead — that is the Oracle
  engine's job, not this playbook's

The playbook's scope is deliberately narrow: **the short-term tape around
dealer-hedging forced flows, and only that.**

## References

- Cem Karsan — SqueezeMetrics and dealer gamma framework
- Kris Sidial — tail vol, long-volatility strategy
- JP Morgan Hedged Equity Fund prospectus — JHEQX roll mechanics
- SpotGamma / Menthor Q — daily gamma flip publication
- Nomura Charlie McElligott — 0DTE and systematic flow notes
