# GRID email alert inventory

## Scope and delivery boundary

This inventory covers repository-defined outbound email templates. A source scan
found two Python SMTP transports: `alerts/email.py` and `scripts/notify.py`.
The draft changes add a short, source-specific recap and a prominent count,
metric, or recorded status before full detail in every template listed below.
They do not change schedules, recipients, enablement flags, triggering thresholds,
SMTP configuration, or historical scoring. They do not establish which templates
are deployed or prove mailbox delivery. No production email was sent to validate
these changes.

## Shared transport and core templates

`alerts/email.py` owns the shared renderer and `_send`, `_send_sync`, and
`_send_in_thread` entry points. Custom report senders below retain their own
summaries rather than receiving a duplicate generic one. HTML-only sections get
readable plain text; explicit plain text and code blocks retain their source.

| Template | Headline and recap | Main callers / scope |
| --- | --- | --- |
| `send_alert` | Alert title and extractive first-line status/numbers, with complete detail | Health alerter, fleet heartbeat, Robinhood operational notifications |
| `alert_on_failure` | Failed source and complete failure context | Ingestion scheduler and realtime flusher |
| `alert_on_failure_with_fix` | Failed source, diagnosis and repair availability; full commands/error below | Ingestion and self-healing alerts |
| `alert_on_regime_change` | Recorded regime transition and supplied confidence | Contract alert handler and pipeline |
| `alert_on_100x_opportunity` | Supplied composite score, direction, heuristic confidence and thesis; not a return claim | Options opportunity events |
| `send_insight` | Insight title and short extractive summary | LLM logger / generated insights |
| `send_agent_report` | Agent status and substantive report summary | Agent runner |
| `send_weekly_review` | Review title and extractive findings | Insight scanner |
| `daily_digest` | Collected decision/source counts, freshness-gated regime and short summaries of available research | Alert scheduler / research digest |
| `alert_on_transition_leaders` | Leader count and leading names; full supplied candidates retained | Training and pipeline reports |
| `alert_on_discovery_insight` | Discovery title and extractive findings | Associations API and pipeline |
| `send_test_email` | Explicit synthetic test status | Manual configuration check |

## Specialized reports using the shared transport

| Template | Headline and recap | Important scope / freshness caveats |
| --- | --- | --- |
| `oracle/report.py` | New prediction rows, model votes and update operations distinguished | Historical signed price-move sums use percentage points, not P/L or portfolio returns. Incomplete counts remain unknown. Cycle time and expiry status are shown. Historical model records remain unreconciled. |
| `scripts/daily_digest.py` | Exact historical issue-record count in a bounded 24-hour window, severity and operational recap | Full totals are queried before the 100-record detail sample; shown sample is labeled. Historical records are not current open incidents. Saved Long Plays age and expired catalysts are explicit. |
| `alerts/hundredx_digest.py` | Selected/eligible/evaluated option counts, cap and leading candidate | Quote/source dates, hypothetical historical results and bounded samples remain distinct from executable prices or realized returns. |
| `alerts/supply_chain_alerts.py` | Detector finding count, structural risk and largest supplied changes | Findings can overlap and are not unique incidents. Missing/failed detector coverage is not all-clear. Source/baseline timestamps are shown when available; acquisition announcements are not closed transactions. Preview and send use the same sections. |
| `alerts/waterfall_watch.py` | Tripped conditions over checks with usable inputs | Missing inputs are separate from checked-clear. Overlapping compound checks are not a probability. All supplied calendar events are retained with dated/unconfirmed status and snapshot age. |

## Legacy transport and formatters

`scripts/notify.py` retains its existing SMTP selection and notification gates.

- `send_insight_email`: generic notifications, including chat capability-gap/build
  requests and the CLI test, gain an extractive recap above the complete body.
  Plain and HTML MIME parts are both supplied. Already summarized internal
  formatters opt out of a duplicate recap.
- `format_backtest_summary` / `notify_on_pass`: simulated Sharpe, recorded verdict,
  baseline and supplied era coverage lead the full hypothesis, metrics, features
  and era detail. Fractional return/drawdown values render as percentages and are
  explicitly simulation outputs, not a live investment track record.
- `format_daily_digest`: bounded 24-hour historical journal, recorded-PASSED
  hypothesis and drift-record counts precede full detail. Unavailable collectors
  stay unknown instead of becoming zero. Source timestamps are preserved.

## Existing dormant notification paths, deliberately not activated

These pre-existing wiring issues are outside a presentation-only change. Fixing
them would activate notifications, so they require a separate delivery review.

1. `scripts/autoresearch.py` passes a raw attempt to `notify_on_pass`, but that
   function's existing gate requires the full run result's `passed` flag. The raw
   attempt lacks the flag. Rendering now supports either shape, but the gate is
   unchanged and covered by a regression test.
2. `trading/circuit_breaker.py` imports `alerts.email.send_failure_alert`, which is
   not defined. Its exception handling suppresses the attempted alert. No alias
   or new send was introduced here.

## Verification strategy

Tests use synthetic payloads, mocked SMTP, and SQLite where SQL behavior matters.
Outbound sockets are blocked in the combined local verification harness. Checks
cover unavailable/non-finite values, sample-versus-total labels, preserved long
text, escaping, exact repair-command plain text, stale saved boards, past dates,
and unchanged notification gating. Synthetic local HTML previews supplement
source tests; they do not prove production data quality, email-client rendering,
or delivery. CI results and exact local test counts are recorded in the PR.

