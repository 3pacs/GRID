# Offline sector participation controls

`scripts.intraday_sector.participation(packet, decision_at)` accepts ISO-8601
timezone-aware timestamps, fractional returns, a full strictly positive index
weight map summing to one, and matching constituent rows. The synthetic fixture
in `tests/test_intraday_sector.py` documents the packet shape. `weights_source_id`
and `universe_id` identify the PIT weight and membership artifact; their recorded
receipt must precede the interval start. Each constituent carries `source_id`,
source event time and trusted collector `available_at`. Device callback clocks
must not be passed as collector receipts.

The module computes weighted return contributions, advancing/declining member
fractions, advancing index weight, sector contributions, and concentration of
absolute contributions (HHI and largest share). A positive index return alongside
negative breadth is a diagnostic divergence, not evidence of subsequent decline.
HHI is contribution concentration, not constituent weight concentration. Flat
returns produce unknown concentration because the denominator is zero.

Missing members, incomplete weights, unknown sectors, stale clocks, future
receipts, nonfinite data and mixed return intervals yield `UNAVAILABLE` without
zero substitution. Default 60-second freshness and 5-second synchronization
limits are admission controls, not optimized trading thresholds.

This lane ran synthetic arithmetic and failure controls only. No PIT production
membership/weight archive or fully synchronized constituent return dataset was
admitted. ETF sector prices are not a substitute for index constituent weights.
Incremental predictive value and executable SPYU after-cost outcomes require
the controller's common evaluation protocol and an admitted unseen sample.
Frozen evaluations, runtime feeds, deployment, paid APIs and trading are untouched.
