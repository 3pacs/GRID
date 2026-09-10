# 02 — Replace the dead FRED series in buyback_execution and credit_index_proxies

Branch: `claude/handoff-02-fred-dead-series`. Lane: code + one gemini-task/ops-exec lookup.

## Evidence

errors.jsonl (Hermes tree, daily): `400 Client Error` from
`https://api.stlouisfed.org/fred/series/observations` for

| Module | Label | Dead id |
|---|---|---|
| `ingestion/altdata/buyback_execution.py` | `net_repurchases` | `NCBBCCB1Q027S` |
| `ingestion/altdata/buyback_execution.py` | `capex` | `NCBEIAQ027S` |
| `ingestion/altdata/credit_index_proxies.py` | `cat_7_china_hy/em_hy_oas` | `BAMLEMHYCRPIOAS` |
| `ingestion/altdata/credit_index_proxies.py` | `cat_7_china_hy/em_hy_yield` | `BAMLEMIBHYCRPIEY` |
| `ingestion/altdata/credit_index_proxies.py` | `cat_13_euro_at1/euro_ig_oas` | `BAMLEMRACRPIEMEAOAS` |

FRED returns 400 for an id that does not exist. The other ids in both modules
(`CPATAX`, `BOGZ1FU104122005Q`, `BAMLEMCBPIOAS`, `BAMLHE00EHYIOAS`, `BAMLHE00EHYIEY`,
`BAMLC0A4CBBB`, `BAMLH0A1HYBB`, `BAMLH0A2HYB`, `BAMLH0A3HYC`) load fine.
The `buybacks:execution_ratio` composite needs `net_repurchases` and
`profits_after_tax`, so the buyback proxy has been dead data since the id broke.

## Steps

1. Look the replacements up with the server's key (the sandbox proxy blocks
   api.stlouisfed.org). Use gemini-task or ops-exec with `source_env: true`;
   never print `$FRED_API_KEY`. Check candidates with
   `GET /fred/series?series_id=<id>` and search with
   `GET /fred/series/search?search_text=...&order_by=popularity`:
   - net equity repurchases / net issuance of equities, nonfinancial corporate
     business (Z.1 Flow of Funds, quarterly, millions SAAR). Search
     "nonfinancial corporate business net issuance equity" and
     "nonfinancial corporate equity repurchases". Prefer the `BOGZ1F…105…Q` family
     that the existing `BOGZ1FU104122005Q` comes from.
   - capital expenditures, nonfinancial corporate business (Z.1, quarterly).
   - ICE BofA High Yield Emerging Markets Corporate Plus Index OAS and Effective
     Yield — candidates `BAMLEMHBHYCRPIOAS` / `BAMLEMHBHYCRPIEY`.
   - ICE BofA EMEA Emerging Markets Corporate Plus Index OAS — candidate
     `BAMLEMRECRPIEMEAOAS`.
   Record id, title, frequency and `observation_end` for each pick.
2. Update the id maps (`BUYBACK_SERIES`, `PROXY_SERIES`) and the module
   docstrings; keep the GRID labels so `raw_series.series_id` values do not
   change. Update `tests/test_buyback_execution.py` and
   `tests/test_credit_index_proxies.py` mock keys to the new ids.
3. Make a 400 from FRED a `log.warning` with the id and the phrase
   "series does not exist — update the id map", not `log.error`, in both
   modules' per-series fetch loops (a dead id is configuration, not a bug).
4. Run the two test files plus `tests/test_no_sql_fstrings.py`; open the PR;
   merge when green; then trigger one pull of each module on grid-svr via
   ops-exec (`PYTHONPATH=. python3 -c "from db import get_engine; from ingestion.altdata.buyback_execution import BuybackExecutionPuller as P; print(P(get_engine()).pull_all())"` — read the module for the actual entry point first) and confirm rows landed:
   `SELECT series_id, max(obs_date), count(*) FROM raw_series WHERE series_id LIKE 'buybacks:%' AND obs_date > current_date - 400 GROUP BY 1`.

## Done when

No FRED 400s in the next 24 h audit, `buybacks:execution_ratio` has fresh rows,
and the three credit proxy series populate for the current month.
