# Dune saved queries for the GRID `DunePuller`

Three Ethereum-only SQL queries to paste into [dune.com](https://dune.com) and
save. After saving, copy the numeric query id from the URL
(`https://dune.com/queries/<ID>/...`) into `.env`:

```
DUNE_QUERY_SMART_MONEY=<id1>
DUNE_QUERY_CEX_FLOW=<id2>
DUNE_QUERY_NARRATIVE_HEAT=<id3>
```

Then `sudo systemctl restart grid-hermes` and re-run
`python3 -m ingestion.altdata.dune_puller` to verify.

> **DuneSQL dialect note.** All three queries use the modern `dex.trades`,
> `tokens.erc20`, `prices.usd`, `erc20_ethereum.evt_Transfer`, and
> `labels.cex_ethereum` spellbook tables. They are Ethereum-only by design;
> extending to Solana/Base is a follow-up (the `blockchain = 'ethereum'`
> predicate becomes a parameter).

> **Honesty check on "realized PnL".** A true realized PnL needs FIFO/avg-cost
> matching of buys to sells. Query 1 below uses **net cash flow on DEX trades**
> (`sold_usd − bought_usd`) as a tractable proxy. It under-counts realized PnL
> for partially-exited wallets but cleanly separates winners from losers at the
> top of the leaderboard, which is what the puller consumes.

---

## 1) Smart money — top wallets by realized PnL on a token

**Save name:** `GRID — smart money leaderboard (token_address, days)`
**Env var:** `DUNE_QUERY_SMART_MONEY`

> Uses the token **contract address** (not symbol) because there are 10+ tokens
> with the symbol `PEPE` on Ethereum and `tokens.erc20` will match the wrong
> one. Canonical PEPE: `0x6982508145454ce325ddbe47a25d4ec3d2311933`.

```sql
-- Parameters: token_address (text, default 0x6982508145454ce325ddbe47a25d4ec3d2311933),
--             days (number, default 30)
WITH params AS (
  SELECT LOWER('{{token_address}}') AS addr,
         CAST('{{days}}' AS INTEGER) AS lookback_days
),
target_token AS (
  SELECT contract_address, symbol, decimals
    FROM tokens.erc20
   WHERE blockchain = 'ethereum'
     AND LOWER(CAST(contract_address AS VARCHAR)) = (SELECT addr FROM params)
   LIMIT 1
),
trades AS (
  SELECT
    t.taker AS wallet,
    CASE WHEN t.token_bought_address = (SELECT contract_address FROM target_token)
         THEN t.amount_usd ELSE 0 END AS bought_usd,
    CASE WHEN t.token_sold_address   = (SELECT contract_address FROM target_token)
         THEN t.amount_usd ELSE 0 END AS sold_usd,
    CASE WHEN t.token_bought_address = (SELECT contract_address FROM target_token)
         THEN t.token_bought_amount ELSE 0 END AS bought_amount,
    CASE WHEN t.token_sold_address   = (SELECT contract_address FROM target_token)
         THEN t.token_sold_amount   ELSE 0 END AS sold_amount
  FROM dex.trades t
  WHERE t.blockchain = 'ethereum'
    AND t.block_time >= NOW() - (SELECT lookback_days FROM params) * INTERVAL '1' DAY
    AND (t.token_bought_address = (SELECT contract_address FROM target_token)
         OR t.token_sold_address = (SELECT contract_address FROM target_token))
),
agg AS (
  SELECT
    CAST(wallet AS VARCHAR) AS wallet,
    SUM(bought_usd)    AS total_bought_usd,
    SUM(sold_usd)      AS total_sold_usd,
    SUM(bought_amount) AS total_bought_amount,
    SUM(sold_amount)   AS total_sold_amount
  FROM trades
  WHERE wallet IS NOT NULL
  GROUP BY 1
)
SELECT
  wallet,
  (SELECT symbol FROM target_token) AS token,
  (total_sold_usd - total_bought_usd) AS realized_pnl_usd,
  (total_bought_amount > total_sold_amount) AS still_holding,
  GREATEST(0.0, total_bought_amount - total_sold_amount)
    * CASE WHEN total_bought_amount > 0
           THEN total_bought_usd / total_bought_amount
           ELSE 0 END AS balance_usd
FROM agg
WHERE total_bought_usd + total_sold_usd > 100  -- noise filter
ORDER BY realized_pnl_usd DESC
LIMIT 50;
```

Output columns the puller expects: `wallet`, `token`, `realized_pnl_usd`,
`still_holding`, `balance_usd`. Top-20 is taken in code.

**Verified working 2026-04-19:** query id `7341448` (default params) returned
20 wallets, ingested as `dune.smart_money.pepe` (value=20).

---

## 2) CEX flow balance — accumulation vs distribution

**Save name:** `GRID — CEX net flow (token, days)`
**Env var:** `DUNE_QUERY_CEX_FLOW`

Positive `net_usd` means coins are leaving CEXes (accumulation). Negative
means coins are moving onto CEXes (distribution).

```sql
-- Parameters: token_symbol (text, default PEPE), days (number, default 14)
WITH params AS (
  SELECT UPPER('{{token_symbol}}') AS sym,
         CAST('{{days}}' AS INTEGER) AS lookback_days
),
target_token AS (
  SELECT contract_address, symbol, decimals
    FROM tokens.erc20
   WHERE blockchain = 'ethereum'
     AND UPPER(symbol) = (SELECT sym FROM params)
   LIMIT 1
),
latest_price AS (
  SELECT AVG(price) AS price_usd
    FROM prices.usd
   WHERE blockchain = 'ethereum'
     AND contract_address = (SELECT contract_address FROM target_token)
     AND minute >= NOW() - INTERVAL '1' DAY
),
cex AS (
  SELECT LOWER(CAST(address AS VARCHAR)) AS address
    FROM labels.cex_ethereum
),
flows AS (
  SELECT
    SUM(CASE WHEN LOWER(CAST(t."from" AS VARCHAR)) IN (SELECT address FROM cex)
              AND LOWER(CAST(t."to"   AS VARCHAR)) NOT IN (SELECT address FROM cex)
             THEN CAST(t.value AS DOUBLE) / POWER(10, (SELECT decimals FROM target_token))
             ELSE 0 END) AS outflow_amount,
    SUM(CASE WHEN LOWER(CAST(t."to"   AS VARCHAR)) IN (SELECT address FROM cex)
              AND LOWER(CAST(t."from" AS VARCHAR)) NOT IN (SELECT address FROM cex)
             THEN CAST(t.value AS DOUBLE) / POWER(10, (SELECT decimals FROM target_token))
             ELSE 0 END) AS inflow_amount,
    COUNT(DISTINCT
      CASE WHEN LOWER(CAST(t."from" AS VARCHAR)) IN (SELECT address FROM cex)
             OR LOWER(CAST(t."to"   AS VARCHAR)) IN (SELECT address FROM cex)
           THEN COALESCE(
             CASE WHEN LOWER(CAST(t."from" AS VARCHAR)) IN (SELECT address FROM cex)
                  THEN CAST(t."from" AS VARCHAR) END,
             CAST(t."to" AS VARCHAR)
           ) END
    ) AS exchange_count
  FROM erc20_ethereum.evt_Transfer t
  WHERE t.contract_address = (SELECT contract_address FROM target_token)
    AND t.evt_block_time >= NOW() - (SELECT lookback_days FROM params) * INTERVAL '1' DAY
)
SELECT
  (SELECT sym FROM params) AS token,
  inflow_amount  * (SELECT price_usd FROM latest_price) AS inflow_usd,
  outflow_amount * (SELECT price_usd FROM latest_price) AS outflow_usd,
  (outflow_amount - inflow_amount) * (SELECT price_usd FROM latest_price) AS net_usd,
  exchange_count
FROM flows;
```

Output columns: `token`, `inflow_usd`, `outflow_usd`, `net_usd`,
`exchange_count`.

---

## 3) Narrative heat — w/w new-holder growth

**Save name:** `GRID — narrative heat (no params)`
**Env var:** `DUNE_QUERY_NARRATIVE_HEAT`

Top tokens by week-over-week growth in unique new holders. Slow-ish (1–2
min on Dune); cached results are then free for the puller to read.

```sql
WITH this_week AS (
  SELECT contract_address,
         LOWER(CAST("to" AS VARCHAR)) AS holder
    FROM erc20_ethereum.evt_Transfer
   WHERE evt_block_time >= NOW() - INTERVAL '7' DAY
     AND CAST(value AS DOUBLE) > 0
   GROUP BY 1, 2
),
prior_week AS (
  SELECT contract_address,
         LOWER(CAST("to" AS VARCHAR)) AS holder
    FROM erc20_ethereum.evt_Transfer
   WHERE evt_block_time >= NOW() - INTERVAL '14' DAY
     AND evt_block_time <  NOW() - INTERVAL '7'  DAY
     AND CAST(value AS DOUBLE) > 0
   GROUP BY 1, 2
),
new_holders AS (
  SELECT tw.contract_address,
         COUNT(DISTINCT tw.holder) AS new_holders
    FROM this_week tw
    LEFT JOIN prior_week pw
      ON pw.contract_address = tw.contract_address
     AND pw.holder = tw.holder
   WHERE pw.holder IS NULL
   GROUP BY 1
),
prior_count AS (
  SELECT contract_address,
         COUNT(DISTINCT holder) AS prior_holders
    FROM prior_week
   GROUP BY 1
)
SELECT
  COALESCE(t.symbol,
           '0x' || SUBSTRING(CAST(nh.contract_address AS VARCHAR), 3, 8)) AS token,
  nh.new_holders,
  COALESCE(pc.prior_holders, 0) AS prior_holders,
  CASE WHEN COALESCE(pc.prior_holders, 0) > 0
       THEN CAST(nh.new_holders AS DOUBLE) / pc.prior_holders
       ELSE 1.0 END AS pct_change
FROM new_holders nh
LEFT JOIN prior_count pc ON pc.contract_address = nh.contract_address
LEFT JOIN tokens.erc20 t
       ON LOWER(CAST(t.contract_address AS VARCHAR))
        = LOWER(CAST(nh.contract_address AS VARCHAR))
      AND t.blockchain = 'ethereum'
WHERE nh.new_holders >= 100  -- noise filter
ORDER BY pct_change DESC
LIMIT 50;
```

Output columns: `token`, `new_holders`, `prior_holders`, `pct_change`.
Puller takes the top 10 in code.

---

## After you save the queries

```bash
# On grid-svr
cd /data/grid_v4/grid_repo
git pull origin main                          # picks up this doc
nano .env                                     # set DUNE_QUERY_* numeric ids
sudo systemctl restart grid-hermes
python3 -m ingestion.altdata.dune_puller      # expect SUCCESS, not SKIPPED

# Verify rows landed
psql -d griddb -c "SELECT series_id, obs_date, value
                     FROM raw_series
                    WHERE series_id LIKE 'dune.%'
                    ORDER BY pull_timestamp DESC
                    LIMIT 10;"
```

Once you see `dune.smart_money.*`, `dune.cex_flow.*`, and
`dune.narrative_heat` rows, the three intelligence functions
(`smart_money_leaderboard`, `cex_flow_balance`, `narrative_heat` in
`intelligence/dune_smart_money.py`) will return populated dicts.
