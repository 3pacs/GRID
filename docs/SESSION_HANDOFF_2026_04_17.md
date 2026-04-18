# Session Handoff - 2026-04-17

**Original handoff doc commit:** `7f756c2d`.
**Runtime-fix baseline before this doc:** `33a94dee`.
**Redbox node integration merged from other agent:** `3d7c6520` via merge commit `40c9719c`.
**Scope:** Puller/scheduler hardening, hourly catch-up stability, production data-flow cleanup.
**Result:** Production API is healthy, scheduler/Hermes are active, and fresh timestamped puller error scans were clean after the final deploy.

---

## Start Here

The user is trying to get GRID ready for a demo and is highly sensitive to scraper/puller errors. Do not treat noisy logs as acceptable. If a source is unavailable, invalid, rate-limited, or malformed, it should be a clean `SKIPPED`/`NO_DATA`/`PARTIAL` result with a warning at most, not an `ERROR`, traceback, fake failed row, or scheduler cooldown loop.

Use this repo path locally:

```bash
cd /Users/anikdang/dev/GRID
```

Production services run on `grid@grid-svr`. The deploy helper must be used for runtime files because GRID runs from two server trees:

```bash
python3 scripts/deploy.py --snapshot path/to/file.py
ssh grid@grid-svr 'sudo systemctl restart grid-hermes grid-scheduler grid-api'
```

For API-only files, use `--restart --smoke` when practical.

---

## What Shipped In This Session

### Ingestion and scheduler hardening

- `ingestion/smart_scheduler.py`
  - Made API-key constructor behavior explicit via `api_key_mode`.
  - Fixed Tiingo, Tiingo news, Tiingo fundamentals, KOSIS-style keyword args, and env-key-only pullers.
  - Corrected QuiverQuant registry path to `ingestion.altdata.quiverquant`.
  - Added bounded GDELT fast-lane kwargs so it cannot consume the whole scheduler tick.

- `ingestion/altdata/quiverquant.py`
  - Added `QuiverQuantPuller` adapter class so the scheduler registry resolves a real class.

- `ingestion/altdata/gdelt.py`
  - Bounded the scheduler path: limited theme queries, actor/tension/signal sections optional.
  - Switched the thematic endpoint to the DOC API, normalized mode casing, short timeout.
  - Public upstream 429/4xx/5xx responses now clean-skip instead of warning/error spam.

- `ingestion/fred.py`
  - Removed invalid breadth IDs `ADVFN` and `DECFN` from the default FRED pull list.
  - FRED HTTP/retry wrapper rejection paths now soft-skip.
  - Malformed dataframe layouts now soft-skip instead of writing fake failed rows.

- `ingestion/altdata/fed_liquidity.py`
  - In-batch duplicate dates are deduped before insert.
  - Malformed frames skip cleanly.
  - Generic source-level failures were downgraded to `SKIPPED` with warning.

- `ingestion/yfinance_pull.py`
  - Suppressed third-party `ERROR:yfinance` library logger pollution.
  - Invalid ticker strings skip before network: `N/A`, `NYSE:...`, slash tickers, empty/null values.
  - Class-share tickers normalize from `BRK.B` style to Yahoo `BRK-B`.
  - yfinance exceptions now return `SKIPPED` instead of `FAILED`.

### Earlier same-day hardening already pushed

- Added hourly GRID catch-up script and cron entry.
- Added separate hourly AstroGrid catch-up script and cron entry.
- Kept AstroGrid separate but running on top of the GRID API.
- Hardened FRED permanent 400/404 handling.
- Hardened solar Kp, Fed liquidity parsing, Finviz insider URL, prediction odds scan cap, signal convergence parsing.
- Restored missing oracle ensemble/model-registry/horizon behavior.
- Added pull lifecycle contract handler.
- Updated system health so external `grid-scheduler`/`grid-hermes` service status backs ingestion/thread checks.

### Redbox quick LLM node from parallel agent

Merged from `origin/main` commit `3d7c6520`:

- New opt-in LLM provider: `llamacpp_quick`.
- Redbox Tailscale endpoint: `http://100.126.129.45:8080`.
- Model label: `qwen3-14b`.
- Config flags live in `config.py`:
  - `LLAMACPP_QUICK_BASE_URL`
  - `LLAMACPP_QUICK_ENABLED`
  - `LLAMACPP_QUICK_TIMEOUT_SECONDS`
  - `LLAMACPP_QUICK_CHAT_MODEL`
- Router factory lives in `llm/router.py::_create_llamacpp_quick_client`.
- First opt-in callsites:
  - `ingestion/altdata/news_scraper.py` sentiment/summarization.
  - `scripts/ux_auditor.py` UX summary generation.
- Test file: `tests/test_llamacpp_quick.py`.

Important: `LLAMACPP_QUICK_ENABLED` defaults to `False`, so redbox does not enter the global fallback chain by accident. Callers must explicitly ask for `get_llm(provider="llamacpp_quick")`.

### gridz4 LLM/compute node

Verified after the redbox merge:

- Hostname: `gridz4`.
- Tailscale address from `grid-svr`: `100.68.9.27 gridz4.tail1e8407.ts.net`.
- GPUs:
  - `NVIDIA GeForce GTX 1080`, 8192 MiB.
  - `Quadro P1000`, 4096 MiB.
- Active services:
  - `grid-compute.service`
  - `grid-taskrunner.service`
  - `ollama.service`
  - `z4-llama.service`
- llama.cpp health from `grid-svr`: `curl http://gridz4:8080/health` returns `{"status":"ok"}`.
- Current z4 llama model from `/props`:
  - `Qwen3.5-9B-Claude-Opus-Reasoning-v2.Q4_K_M.gguf`
  - Context: `8192`
  - Slots: `1`
  - Reasoning format: `none`

Important: gridz4 is separate from redbox. Redbox is `100.126.129.45:8080`; gridz4 is `gridz4:8080` / `100.68.9.27`.

---

## Production State At Handoff

Latest pushed runtime-fix commit before this handoff doc:

```text
33a94dee quiet yfinance invalid tickers
```

Both refs matched at the end of the runtime work:

```text
HEAD == origin/contracts-phase-1 == origin/main
```

Both refs matched again after this handoff doc was committed:

```text
HEAD == origin/contracts-phase-1 == origin/main == 7f756c2d
```

Public health:

```json
{"status":"ok","degraded_reasons":[]}
```

Active services checked:

```text
grid-hermes    active
grid-scheduler active
grid-api       active
```

Final fresh timestamped scans after the yfinance deploy used:

```bash
grep "^2026-04-17 14:08" /data/grid/logs/hermes-operator.log \
  | grep -E "ERROR|Traceback|failed after|TIMEOUT|SmartScheduler: .* failed|Exception|FRED pull failed|FedLiquidity .* pull failed|yfinance pull failed" || true

journalctl -u grid-scheduler --since "2026-04-17 14:08:00 UTC" --no-pager \
  | grep -E "ERROR|Traceback|failed|TIMEOUT|Exception|FRED pull failed|FedLiquidity .* pull failed|yfinance pull failed" || true
```

Both returned no matching fresh errors.

Important caveat: old log files still contain many pre-fix errors. Always scan by timestamp after the relevant deploy/restart.

---

## Validation Already Run

Full backend suite after final yfinance patch:

```text
5094 passed, 83 skipped, 1 xfailed in 197.10s
```

Targeted tests added/updated:

```bash
.venv/bin/pytest tests/test_smart_scheduler.py tests/test_intelligence_sources.py::TestGDELTEnhanced -q
.venv/bin/pytest tests/test_ingestion.py::TestFREDPuller tests/test_ingestion.py::TestAltDataPullers tests/test_ingestion.py::TestYFinancePuller -q
```

Live production sanity checks performed:

- Tiingo, Tiingo fundamentals, Tiingo news, and QuiverQuant instantiate through `SmartScheduler._build_puller_instance`.
- GDELT bounded pull returns `SUCCESS` and clean-skips 429.
- FRED `ADVFN` returns `SKIPPED` and is not in default pull list.
- FRED `H8B1023NCBCMG` and `TOTRESNS` manual pulls returned `SUCCESS`.
- FedLiquidity `RRPONTSYD`, `H8B1023NCBCMG`, and `TOTRESNS` returned `SUCCESS`.
- yfinance junk tickers `NYSE:FLG` and `N/A` returned `SKIPPED` before network.

---

## Next-Agent TODO List

### P0 - Keep pullers painless

1. Run a fresh timestamped puller scan after the next scheduler/Hermes cycle.

   ```bash
   ssh grid@grid-svr 'date -u'
   ssh grid@grid-svr 'grep "^2026-04-17 14:" /data/grid/logs/hermes-operator.log | grep -E "ERROR|Traceback|failed after|TIMEOUT|SmartScheduler: .* failed|Exception|pull failed|ERROR:yfinance" || true'
   ssh grid@grid-svr 'journalctl -u grid-scheduler --since "30 minutes ago" --no-pager | grep -E "ERROR|Traceback|failed|TIMEOUT|Exception|pull failed|ERROR:yfinance" || true'
   ```

2. If new puller errors appear, fix the puller. Do not just widen the grep.

3. Audit yfinance dynamic ticker sources. The static `YF_TICKER_LIST` is clean; the huge old `ERROR:yfinance` spam came from dynamically discovered actor/company tickers that included bad symbols. Find those producers and sanitize before they call yfinance.

   Likely search starts:

   ```bash
   rg -n "pull_ticker\\(|YFinancePuller|ticker_list|SECTOR_MAP|actor.*ticker|ticker" ingestion intelligence analysis api -g '*.py'
   ```

4. Consider moving yfinance invalid ticker normalization into a shared ticker utility if other modules use Yahoo symbols directly.

### P0 - Verify hourly jobs and backlog

1. Confirm cron remains installed:

   ```bash
   ssh grid@grid-svr 'crontab -l | grep -E "GRID-CRON-hourly-catchup|GRID-CRON-astrogrid-hourly-catchup"'
   ```

2. Confirm the spider timer is hourly:

   ```bash
   ssh grid@grid-svr 'systemctl list-timers grid-spider.timer --no-pager'
   ssh grid@grid-svr 'systemctl cat grid-spider.timer'
   ```

3. Recheck backlog tables:

   - `spider_queue` should stay near empty.
   - `baseline_predictions` unscored should stay zero or drain hourly.
   - `contagion_simulations` mature unscored rows should drain as windows mature.
   - `oracle` pending rows may exist if their expiry is still in the future; that is not a backlog.

### P1 - Frontend intelligence surfacing

The user still wants the app to surface intelligence in ingestible bites. The next frontend pass should prioritize:

1. Canvas expand/detail behavior.
   - Verify every expand and detail path still works after the previous canvas route fixes.
   - Add Playwright coverage for expand/detail if absent.

2. Unique findings / "why this matters" surface.
   - Need a page/card/stream that shows high-signal non-obvious findings from convergence, cross-reference, actor graph, and backtests.
   - Avoid dumping tables. Show small digestible cards with evidence links and confidence.

3. Chart containment.
   - The user complained about runaway charts loading the whole page.
   - Audit chart components for fixed responsive bounds and loading/error states.

4. Dead buttons and links.
   - Re-run full link/module clickthrough.
   - For any big broken flow, leave a planning note with repro and owner.
   - For small dead buttons, fix immediately.

### P1 - Backtests and long-horizon plays

The user does not need daily narrative briefings as much as hardened swing/long-term analysis.

1. Keep contagion scoring long horizon:
   - Existing windows are `7, 14, 30, 60, 90, 180`.
   - Do not collapse back to short-term/day-trading behavior.

2. Build or improve the "plays" surface:
   - Show longer-term trade theses.
   - Include source evidence, regime, catalyst window, invalidation, and backtest history.

3. Confirm hourly catch-up keeps:
   - oracle trade scoring
   - baseline prediction scoring
   - contagion backtest scoring
   - contagion feedback
   - dashboard cache warm

### P1 - Data quality / source health

1. Build a source health dashboard view or API that separates:
   - `SUCCESS`
   - `NO_DATA`
   - `SKIPPED` expected upstream/unavailable
   - true `FAILED`

2. Track consecutive failures by source without treating unavailable/paywalled/deprecated series as failures.

3. Add a "bad ticker quarantine" table or cache:
   - symbol
   - reason
   - first seen
   - last seen
   - producer module
   - suggested normalized ticker

4. Stop retrying known bad symbols every cycle.

### P2 - Known non-puller issue

`llm_available` was false in final public health before the redbox merge, but `degraded_reasons` was empty. This did not block the ingestion work. Redbox is now available as an opt-in quick node, but it is not part of the default fallback chain unless enabled/configured. Next agent should decide whether:

- this is acceptable because LLM is optional, or
- health should degrade when the LLM provider is unavailable.

Check:

```bash
curl -sS https://grid.stepdad.finance/api/v1/system/health
ssh grid@grid-svr 'systemctl is-active grid-llamacpp-oracle grid-llamacpp || true'
ssh grid@grid-svr 'curl -sS http://100.126.129.45:8080/health || true'
ssh grid@grid-svr 'journalctl -u grid-llamacpp-oracle --since "2 hours ago" --no-pager | tail -n 120'
```

### P2 - Clean old logs / observability

Old Hermes logs contain thousands of fixed historical errors. Add a cleaner operational report so agents do not confuse stale errors with fresh regressions:

- `scripts/check_recent_puller_errors.sh --since "30 minutes ago"`
- Output grouped by source and fresh timestamp.
- Ignore known benign yfinance third-party lines only if the source result is `SKIPPED` or `PARTIAL`.

---

## Commands For The Next Agent

Local:

```bash
cd /Users/anikdang/dev/GRID
git status --short
git fetch origin main contracts-phase-1
git rev-parse HEAD origin/contracts-phase-1 origin/main
.venv/bin/pytest -q
```

Production:

```bash
ssh grid@grid-svr 'systemctl is-active grid-api grid-hermes grid-scheduler grid-spider.timer grid-backlinker grid-realtime'
curl -sS https://grid.stepdad.finance/api/v1/system/health
ssh grid@grid-svr 'journalctl -u grid-scheduler --since "30 minutes ago" --no-pager | grep -E "ERROR|Traceback|failed|TIMEOUT|Exception|pull failed|ERROR:yfinance" || true'
ssh grid@grid-svr 'tail -n 300 /data/grid/logs/hermes-operator.log | grep -E "ERROR|Traceback|failed after|TIMEOUT|SmartScheduler: .* failed|Exception|pull failed|ERROR:yfinance" || true'
```

Deploy:

```bash
python3 scripts/deploy.py --snapshot path/to/file.py
ssh grid@grid-svr 'sudo systemctl restart grid-hermes grid-scheduler'
```

GitHub:

```bash
git push origin contracts-phase-1
git fetch origin main
git push origin contracts-phase-1:main
```

---

## Current Bias For Next Work

Do not start with new features. Start with one clean scheduler cycle and one clean Hermes cycle, then move to frontend intelligence surfacing. The demo risk is not a missing novelty feature; it is broken buttons, runaway charts, unreadable intelligence, and scary puller logs.
