# Offline volatility repricing lane

`scripts/intraday_lab/volatility_repricing.py` accepts intraday-lab packets with
matched `OPTION` observations. It is a research diagnostic, not a live feed,
predictor, dealer-position estimate, or profitable-edge claim.

Every snapshot requires source identity, event and trusted first-receipt clocks,
availability, synchronized spot/IV/quote inputs, provider IV basis, valid bid/ask
with size, and exact contract identity (underlying, expiry epoch, right, strike,
multiplier). Latest event must be within 10 seconds by default; pair history is
bounded to 300 seconds. The clock guard rejects future data and unmonitored ANIK
callback clocks. Invalid observations are reported unavailable, never neutral.

Measurements retain IV change in volatility points, time remaining, moneyness
change, bid/ask midpoint change, model residual and the sum of quote half-spreads.
A fixed-order European continuous-yield counterfactual separates spot, time,
IV and carry components. Nonlinearity makes the decomposition order-dependent.
For American SPY options this is a sensitivity proxy, not exact attribution;
exercise/dividend effects and provider-IV methodology can appear in residuals.
Midpoints are not executable prices, and IV increase is not a bullish vote.

Synthetic tests check constant-IV spot/time changes, IV changes on calls and
puts, mismatches, future and untrusted receipts, stale data, crossed quotes,
missing IV and size, unsynchronized snapshots, overflow, residual preservation,
and identity/receipt custody. They establish calculation behavior only.

No market sample with synchronized option quotes/provider IV and trusted
available-at lineage has been admitted by this lane. Existing lab public capture
normalizes SPY/GEX, not a full matched options stream. Before outcome evaluation,
the controller must admit such packets and freeze a separate preregistration for
feature construction and IV-provider comparability; do not modify frozen v1 logs.
