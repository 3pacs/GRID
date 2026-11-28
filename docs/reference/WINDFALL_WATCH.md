# Near-term windfall watch

From the GRID root, run:

```sh
python -m scripts.windfall_watch --grid-calendar --reddit stocks --reddit options
python -m scripts.windfall_watch snapshot.json --days 10
python -m scripts.windfall_watch snapshot.json --as-of 2026-11-23T15:00:00Z
```

The first command gathers leads from the GRID earnings and clinical calendars
and the configured Reddit OAuth client. Either source can run alone. Reddit
discovery checks the latest 50 posts per requested subreddit for posts from the
past three days with an explicit cashtag and a catalyst keyword. It returns
links for review. Validate the ticker, specific claim, catalyst date, and
primary evidence before adding a lead to a snapshot. Reddit setup follows
[Reddit's Data API requirements](https://support.reddithelp.com/hc/en-us/articles/16160319875092-Reddit-Data-API-Wiki).
GRID calendar results are discovery leads. The SEC's
[EDGAR submissions API](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
is one source for checking public filings.

## Snapshot format

Example with fictional prices and evidence. Replace the URLs and figures with
timestamped source material. Use full snapshots for each new lead revision.

```json
{
  "leads": [{
    "ticker": "EXMP",
    "event_id": "2026-q3-results",
    "event": "Earnings and guidance",
    "event_date": "2026-11-24",
    "observed_at": "2026-11-23T14:40:00Z",
    "evidence": [{
      "kind": "primary",
      "finding": "confirms_event",
      "url": "https://example.com/investors/calendar",
      "observed_at": "2026-11-23T14:35:00Z"
    }],
    "delivery_checks": [{
      "factor": "capacity_and_funding",
      "status": "supporting",
      "side": "long",
      "note": "Existing capacity covers disclosed production commitments.",
      "url": "https://example.com/investors/filing",
      "observed_at": "2026-11-23T14:36:00Z"
    }, {
      "factor": "customer_concentration",
      "status": "supporting",
      "side": "short",
      "note": "Largest customer accounts for a material share of sales.",
      "url": "https://example.com/investors/filing",
      "observed_at": "2026-11-23T14:36:00Z"
    }, {
      "factor": "expectations_already_priced_in",
      "status": "unknown",
      "side": "long",
      "decision_critical": true,
      "note": "Need a dated consensus range to assess the surprise hurdle.",
      "observed_at": "2026-11-23T14:37:00Z"
    }],
    "long": {
      "observed_at": "2026-11-23T14:40:00Z",
      "trigger": 50, "invalidation": 49, "target": 54,
      "thesis": "Guidance can clear consensus given available capacity.",
      "antithesis": "Strength fades against peers or consensus already assumes the upside."
    },
    "short": {
      "observed_at": "2026-11-23T14:40:00Z",
      "trigger": 47, "invalidation": 48, "target": 43,
      "thesis": "Concentration makes a customer delay material to guidance.",
      "antithesis": "Diversified bookings absorb the delay and the stock reclaims 48."
    },
    "paid_research": {
      "observed_at": "2026-11-23T14:39:00Z",
      "factor": "expectations_already_priced_in",
      "source": "Dated consensus vendor snapshot",
      "cost_usd": 25, "budget_usd": 40,
      "free_sources_exhausted": true,
      "decision_change": "Drop the long thesis if published consensus already implies the bull case."
    }
  }],
  "tape": [{
    "ticker": "EXMP",
    "observed_at": "2026-11-23T14:59:00Z",
    "price": 50.10,
    "relative_volume": 1.8,
    "spread_bps": 12,
    "sector_relative_return": 0.012,
    "average_daily_dollar_volume": 18000000,
    "regular_session": true,
    "halted": false,
    "short_available": true
  }]
}
```

Add Reddit permalinks as timestamped evidence with `kind: "rumor"`. Use
`primary` with `confirms_event` or `contradicts_event` for a dated issuer,
regulator or similarly authoritative source. Evaluate delivery through factors
such as funding, capacity, approvals, customer commitments, dilution,
execution history and expectations. A check is `supporting`, `negative`,
`blocking`, or `unknown`. Label the applicable side `long`, `short`, or
`both`. Include a source URL for resolved checks. Set `cancelled: true` on
the latest lead revision when the event is withdrawn.

## Signal and purchase gates

- Confirm the event and supply sourced supporting evidence for the applicable
  side. A primary contradiction, a blocking factor, or a decision-critical
  unknown keeps the affected setup on watch.
- Use current regular-session tape at or after the plan timestamp. The five-minute
  freshness limit, 30 bps maximum spread, $10M minimum average daily dollar
  volume, 1.5× same-time relative volume, and positive direction-adjusted
  sector-relative session return are initial research thresholds. Short
  availability must come from a current broker or locate feed. An omitted
  short availability or halt status blocks the corresponding setup.
- Require at least 2R gross both at the trigger and at the current price.
  Triggered setups rank first by current gross reward-to-risk. Remaining
  candidates prioritize confirmed events and nearer dates. Status is long,
  short or no trade. Treat each output as a snapshot, since live prices,
  borrow and execution costs can change.
- Only flag paid research for human review when it answers a decision-critical
  unknown about a confirmed catalyst, free sources are exhausted, a proposed
  result changes a decision, the cost fits the budget, and there is no known
  blocker. The scanner never submits purchases or orders.

Tape snapshots currently come from the caller. The CLI does not pull live
quotes, calculate same-time volume baselines, or price options. Gross R excludes
fees, slippage and borrow costs. Thresholds are heuristics and need
point-in-time walk-forward validation before they can be treated as an edge.
