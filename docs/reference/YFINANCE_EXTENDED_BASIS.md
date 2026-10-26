# Extended yfinance backfill: price-basis boundary

`scripts/fill_missing_features.py` downloads five-year history with
`auto_adjust=True`. Its `Close` is therefore an adjusted series. Future runs
write that value as `YF_ADJ:{ticker}:close` under the distinct
`yfinance_adjusted_extended` source. `raw_payload` records
`price_basis=adjusted_close`, `download_auto_adjust=true`, and
`known_at_verified=false`. The source catalog also records `pit_available=false`.
The entity map does not promote `YF_ADJ:` into a raw-close feature.

The download's `Volume` keeps its existing `YF:{ticker}:volume` series mapping,
but uses the same distinct source. Its payload identifies provider volume from
the adjusted download; it makes no claim about a historical publication time.
The canonical `ingestion/yfinance_pull.py` continues to request
`auto_adjust=False` for its raw `YF:{ticker}:close` series.

This is a **prospective writer separation**, not a historical cleanup or a
verified raw-close cutover. Older `YF:{ticker}:close` rows under the `yfinance`
source may still mix raw and adjusted values without row-level basis markers.
Neither `pull_timestamp` nor a code commit proves original publication time.
Any point-in-time evaluator must keep those rows unavailable until an exact
cohort-specific basis and availability contract is independently proven.
Tickers whose only close came from this backfill may now remain unfilled in
raw-close features; a raw-basis puller must supply them before evaluation.
