# Private Gamma Board: intraday scope

The GRID page is the shared comparison screen for SPX, SPY, QQQ and supported standard US stock/ETF symbols. It is separate from the dedicated SPY collector. A symbol that has price bars but no qualifying options chain may still have an intraday price chart. An index has no share volume. Options open interest is a daily input; GEX is a model, not an observed dealer position.

## Price history versus GEX history

Minute OHLC charts can be rebuilt from dated underlying bars. Each bar belongs to its exchange session; missing minutes remain gaps. The current call and put wall snapshots may serve as *current references*, not as evidence of where a wall stood earlier in the session.

To extend the dedicated SPY strike-by-time display to the other symbols:
1. Record one snapshot per tracked symbol per regular-session minute from a server-side collector, even when no browser is open. Persist the observation time, quote time, source, OI provenance, scope and version, per-strike signed GEX and contemporaneous walls. Do not recalculate old samples using the latest chain.
2. Distinguish the same-day PM expiry book from the broader 30-day book. Skip AM-settled expiring series. Compare one SPY session to the existing collector using the same scope; explain methodology/coverage differences before comparing numeric levels. Start the SPX and QQQ historical displays at their first recorded observation.
3. Add a time axis to the strike heat map and overlay the recorded walls on the minute price path. Preserve missing intervals and stale inputs. A custom ticker can have on-demand price history immediately; full-session GEX history requires explicit collection of that ticker.

## Futures context and evaluation

ES tracks S&P 500 futures; NQ tracks Nasdaq-100 futures. Display their own contract identifiers, timestamps and volume units. Avoid mixing futures prices with cash-index strike levels without a contemporaneous basis. If comparing overnight participation, normalize to the same clock and session and account for rolls, holidays and abbreviated sessions. Near expiry, a shift of activity to the next contract can masquerade as a signal.

Before giving futures inputs forecast weight, preregister a small comparison on the same future forecasts, with identical forecast times, horizons and missing-data rules. A candidate pair of features is location within the prior overnight range and volume relative to matched prior sessions. Keep the baseline and candidate outputs immutable before the outcome. Compare an appropriate loss against the baseline (e.g., Brier score for direction probabilities); report coverage and uncertainty. No forecasting or trading weight is assigned by the board.
