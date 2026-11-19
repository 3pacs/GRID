# Private Gamma Board: intraday scope

The GRID page is the shared comparison screen for SPX, SPY, QQQ and supported standard US stock/ETF symbols. It is separate from the dedicated SPY collector. A symbol that has price bars but no qualifying options chain may still have an intraday price chart. An index has no share volume. Options open interest is a daily input; GEX is a model, not an observed dealer position.

## Price history versus GEX history

Minute OHLC charts can be rebuilt from dated underlying bars. Each bar belongs to its exchange session; missing minutes remain gaps. The current call and put wall snapshots may serve as *current references*, not as evidence of where a wall stood earlier in the session.

The strike-by-time chart uses saved observations from a single expiry. Each column shows the exposure recorded at that minute, with the observed spot price overlaid. Missing observations remain gaps; the chart does not reconstruct the morning from an afternoon options chain. The separate strike-by-expiry snapshot can cover several expirations and is not directly comparable.

## Futures context and evaluation

ES tracks S&P 500 futures; NQ tracks Nasdaq-100 futures. Display their own contract identifiers, timestamps and volume units. Avoid mixing futures prices with cash-index strike levels without a contemporaneous basis. If comparing overnight participation, normalize to the same clock and session and account for rolls, holidays and abbreviated sessions. Near expiry, a shift of activity to the next contract can masquerade as a signal.

Before giving futures inputs forecast weight, preregister a small comparison on the same future forecasts, with identical forecast times, horizons and missing-data rules. A candidate pair of features is location within the prior overnight range and volume relative to matched prior sessions. Keep the baseline and candidate outputs immutable before the outcome. Compare an appropriate loss against the baseline (e.g., Brier score for direction probabilities); report coverage and uncertainty. No forecasting or trading weight is assigned by the board.

## Implemented strike-by-time recorder

The primary view now plots recorded strike GEX against cash-session time. SPX, SPY and QQQ record a single same-day PM expiry; up to three optional plain-letter symbols can be explicitly recorded using the nearest qualifying expiry, held to one expiry for the session. Adding a card alone does not enable background recording. The first qualifying observation per minute is immutable. At the regular open, a pinned symbol missing a qualifying first observation can get one more attempt in the same minute. A later minute cannot be used to fill the open. At close, or with stale inputs, the recorder does not synthesize a frame. SPX uses Massive daily contract OI with current Schwab gamma; the other private panels use Schwab. The original, differently sourced SPY tape is readable as a complete session fallback; sources are not spliced together. Historical GRID observations for each symbol begin when capture is running and a qualifying chain arrives.

Deployment: install deploy/systemd/grid-gamma-time.service.template as grid-gamma-time.service and enable/start the unit. It runs as the existing grid service user, observes the exchange calendar (including early closes) and re-enters the current deployed release when it changes. Default state is /var/lib/grid-gamma-time (0700); GRID_GAMMA_TIME_DB can override the frames path. Stop/disable the unit to stop new observations; existing recordings remain. For verification use the authenticated time-map endpoint, systemctl status and the recorder's per-symbol outcome lines. A healthy idle service outside cash hours does not prove an in-session observation was captured. Monitor disk space and back up this append-only journal; do not mutate it to simulate an earlier session.
