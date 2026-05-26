# SEC Tools Rebuild — handoff note

> **Status:** in progress. First increment shipped (PR #260). Remaining work is **blocked
> on live SEC access** and should be done where `data.sec.gov` is reachable (grid-svr),
> not in a web sandbox (which gets HTTP 403 from SEC).

## Why we're doing this

GRID's SEC ingestion is a **split-brain**: `ingestion/edgar.py` and
`ingestion/sec_velocity.py` already use the `edgartools` library, but every
`ingestion/altdata/sec_*.py` module hand-rolls raw HTTP against `data.sec.gov` /
`efts.sec.gov` and parses XML/JSON by hand. `edgartools` (MIT, actively maintained,
already a pinned dependency: `edgartools>=4.6.3`, upstream is 5.x) exposes a much richer
layer than the hand-rolled parsers: typed Form 3/4/5 ownership objects, `ThirteenF`
holdings, standardized XBRL `EntityFacts` (with TTM, ratios, concept standardization),
and EFTS full-text search.

**Directive (from the owner):** don't cram edgartools into the old raw-extraction shapes.
Where its data is richer/cleaner, **rebuild the tool around the better data**. The
existing SEC output isn't producing anything worth preserving, so do not be precious about
matching every legacy measurement — a proxy that is as good or better with better data is
the goal. This is a standing GRID principle: *better data, better tools than we had
yesterday.*

## Done

- **PR #260** — `ingestion/altdata/sec_13f_live.py`: ripped the `index.json` directory
  walk + multi-candidate XML probing in `fetch_infotable` over to edgartools
  (`find(accession).obj().infotable`), with the raw-HTTP path kept as a
  graceful-degradation fallback. Version-tolerant column converter (works on 4.x lowercase
  and 5.x capitalized columns). All GRID business logic (CUSIP map, aggregation, upsert,
  `find_latest_13f`) unchanged. 13 mocked tests in `tests/test_sec_13f_live.py`.
  - **Server validation still needed:** run one live filer pull on grid-svr and confirm
    the edgartools path returns parsed positions (it falls back to raw XML if not, so this
    is a "is the happy path actually firing" check, not a correctness risk).

## Remaining modules — rebuild plan

All of the below should be **written and validated on grid-svr** (SEC reachable). Each is
its own PR. Reuse the patterns proven in PR #260: `set_identity()` once per process,
version-tolerant column access, graceful fallback where a live path feeds trading.

| Module | Current approach | edgartools rebuild | Risk / notes |
|---|---|---|---|
| `insider_filings.py` (Form 4) | raw EFTS search + manual Form-4 XML parse | `Form4` ownership objects: `get_transaction_activities()` / `to_dataframe()` | **Map on the raw transaction `code` (P→BUY, S→SELL) to preserve GRID's open-market-only semantics** — do NOT use edgartools' broader "purchase/sale" buckets, which lump awards/grants/exercises. Keep the 429 cooldown, cluster-buy detection, unusual-size signal. Feeds live signals → validate against real filings before merge. |
| `sec_xbrl_financials.py` (41KB) | raw companyfacts JSON parse | `Company.get_facts()` → `EntityFacts` | EntityFacts **standardizes concepts** (`get_revenue`, `get_net_income`, TTM, ratios). This is the biggest "better data" win, but the value set will differ from the raw us-gaap tags. For a faithful-but-safe path, `edgar.entity.entity_facts.download_company_facts_from_sec(cik)` returns the **raw companyfacts dict** unchanged — use it if you want to keep the existing extraction while dropping the hand-rolled HTTP. Prefer the rebuild on standardized facts per the directive. |
| `sec_xbrl_shares.py` | builds on financials parser | same `EntityFacts` (shares outstanding, public float) | Fold into the financials rebuild — `EntityFacts.shares_outstanding` / `public_float`. |
| `sec_edgar_company.py` (9KB) | companyfacts via httpx | `Company(ticker).get_facts()` + edgartools CIK resolution | Smallest/cleanest. Good first rebuild to set the EntityFacts pattern for the two above. |
| `edgar_transcripts.py` | raw EFTS full-text | `edgar.EFTSSearch` / full-text search | Rip the fetch layer; keep transcript post-processing. |
| `sec_item_1c_cyber.py` (27KB) | requests + BeautifulSoup, Item 1C | **keep / thin-wrap** | Item 1C cybersecurity is niche; edgartools may not isolate it cleanly. Lowest priority. |

## Key API facts (edgartools 5.31.5, confirmed offline)

- `ThirteenF.infotable` → DataFrame, columns `Issuer/Class/Cusip/Value/SharesPrnAmount/Type`
  (5.x). 4.x emits lowercase — access columns case-insensitively.
- `Form4` → `get_transaction_activities()` (typed `TransactionActivity`: `code`,
  `price_per_share`, `value`, `transaction_type`, `security_type`), `to_dataframe()`,
  `market_trades`, `insider_name`, `shares_traded`.
- `EntityFacts.to_dataframe()` columns: `concept`, `label`, `value`, `numeric_value`,
  `unit`, `period_type`, `period_start`, `period_end`, `fiscal_year`, `fiscal_period`.
  Accessors: `get_revenue/get_net_income/get_total_assets/get_shareholders_equity`,
  `get_ttm_revenue`, `shares_outstanding`, `public_float`, `calculate_ratios`.
- Raw escape hatch: `edgar.entity.entity_facts.download_company_facts_from_sec(cik: int)`
  returns the raw `{"facts": {"us-gaap": {...}}}` dict (raises `NoCompanyFactsFound`).
- Identity is mandatory: call `edgar.set_identity(...)` once (env `SEC_USER_AGENT`).

## Version caveat

The pin is `edgartools>=4.6.3` but upstream is 5.31.5. **`ingestion/edgar.py` reads
lowercase `holdings["value"]`, which breaks under 5.x** (5.x emits `Value`). The server
runs 4.6.3 today where it works. If/when the pin moves to 5.x, audit `ingestion/edgar.py`
and `ingestion/sec_velocity.py` for the column-casing change. New rebuilds should be
version-tolerant from the start (see PR #260's converter).
