# Provisional signal-evaluation price contract (sig-eval-3-dryrun)

This is an isolated, manual, read-only evaluator. It has no schema migration,
persistence path, imported production call site, provider call, or scheduler
hook. Its result is an exploratory cohort, not an accepted performance metric.

The accessor can select `raw_series` rows with an exact
`YF:{instrument}:close` identifier, `source_catalog.name = yfinance`,
`pull_status = SUCCESS`, observation date no later than the requested day,
and pull timestamp no later than that day's end in America/New_York. The
series convention and explicit `auto_adjust=False` current ingestion call
come from `ingestion/yfinance_pull.py`; the fields and status come from
`schema.sql`. Historical rows do not carry the ingestion code version or
an adjustment flag. Therefore a verified deployment/ingestion cutover must
be supplied before the accessor reads any price. This branch has no such
evidence; the manual CLI supplies no cutover and refuses ambiguous history.
Unit tests inject a hypothetical cutover solely to exercise the real query.
Only a row passing that guard can be recorded as `raw_close`.
It does not infer basis from `feature_registry` or `resolved_series`: the
entity map can send both `close` and `adj_close` into the same feature, while
resolved rows omit raw `series_id`.

The evaluator requires an exact-date entry and exact-date exit bar: a latest
bar before the entry target predates signal availability, and a bar before
the exit target cannot stand in for the nominal horizon. On weekends or
holidays this conservatively yields an ineligible result; it does not infer
the next tradable session. It assumes a calendar-day horizon and an after-16:00
New York timestamp becoming eligible on the following **calendar** date.
These are versioned dry-run assumptions, not approved exchange-calendar or
execution policy. The signal entry date is at least its `signal_date` and
the market date of its timezone-aware `known_at`, or, if absent, `created_at`.
`created_at` is an ingestion-time proxy; it does not prove publication or
public availability. Congressional transaction dates are refused unless an
explicit publication timestamp is supplied. The current CLI has no such
column and will therefore classify those rows as `known_at_unverified`.

The CLI accepts explicit source and date bounds and a maximum of 1000 rows.
It checks the live column type through `information_schema`: native DATE is
compared against DATE bounds; TIMESTAMPTZ is compared against UTC instants
derived from New York midnight boundaries. It then normalizes native Python
values and rejects naive timestamps. This avoids claiming a blanket SQL
`::date` cast works for both schemas. PostgreSQL behavior has **not** been
tested against a real database in this branch.

An immature horizon remains `UNRESOLVED` without fetching an exit. A stale
entry or stale/same exit is `INELIGIBLE`, never `NO_MOVE`. Dead-band and cost
parameters must be finite and within declared bounds. The input record has
no origin tag and all outcomes remain `unknown` because no row-level origin
proof is read.
A later provider
pull cannot retroactively establish a bar was known on an earlier day.
Historical bars first pulled after the cutoff will be absent by design;
without a verified raw-basis cutover, the cohort cannot have priced outcomes.
Real exchange sessions, disclosures,
corporate actions, splits, timezone/session alignment, and source lineage
still require validation before any production interpretation.

## Multi-valued dates are refused, not resolved

`raw_series`'s only uniqueness constraint is on `(series_id, source_id,
obs_date, pull_timestamp)`. A second writer under the same `series_id` and
`source_id` — for example an adjusted-close puller sharing the `yfinance`
source row — can insert a second, differently-valued row for the same
`obs_date` at a later `pull_timestamp`. Historical `YF:{ticker}:close` rows
are confirmed contaminated this way (a pre-`#656` `fill_missing_features.py`
run wrote adjusted closes under the same series/source identity; the vast
majority of `YF:SPY:close` dates hold more than one distinct value). The
accessor computes the distinct-value count for the selected date in the same
query that selects the price and refuses the date outright
(`ambiguous_raw_close_multiple_values`) whenever more than one distinct value
exists, rather than accepting "the latest pull wins." A single
`verified_raw_close_since` cutover cannot close this gap by itself — it only
proves the canonical puller was live by that date, not that nothing else
wrote to the same series afterward.

## Instrument-class gate

The accessor refuses any ticker matching a 24/7 crypto pattern
(`^[A-Z0-9]{2,10}-USD$`, e.g. `BTC-USD`, `ETH-USD`, `SOL-USD`, `TAO-USD` —
exactly the tickers `fill_missing_features.py` pulls into this same
`YF:{ticker}:close` shape) before any query. This evaluator's exact-date bar
matching and after-16:00-America/New_York rollover are an equity/ETF
NYSE-session policy; a continuously-traded instrument does not have that
session structure, so it is refused rather than silently mispriced.

## `known_at` for congressional disclosures

`scripts/evaluate_signals.py::to_signal_record` now reads the congressional
`disclosure_date` out of `signal_value` JSONB (captured by
`ingestion/altdata/congressional.py`) and uses it as `known_at`, anchored to
the end of that calendar day (`time.max` in `America/New_York`) so the
existing after-16:00 rollover applies conservatively rather than assuming an
earlier intraday disclosure time. Every other `source_type`, and any
congressional row missing or with an unparseable `disclosure_date`, has no
verified publication field wired here yet: `known_at` stays `None` and
`evaluate_signal()` falls back to `created_at`, with the specific reason
recorded on `SignalRecord.metadata["known_at_source"]` — the fallback is
explicit and labelled, not a silent assumption that ingestion time is always
a safe proxy for publication time.
