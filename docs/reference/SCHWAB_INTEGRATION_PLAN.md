# Schwab / thinkorswim feed — end-to-end integration plan (source-only, 2026-10-31)

**Status:** source-only preparation for controller pickup `760bf1f3` (sole GRID controller
01a18502). Nothing here is live: no provider call has been made from GRID, no credential exists
on grid-svr, the private endpoint ships **off** (`SCHWAB_PRIVATE_QUOTES_ENABLED=false`), and
nothing Schwab-sourced may be published. The target the user named (wired by Sunday night,
2026-11-01 Pacific) is a target, not an authorization; every live step below is an owner's
decision that the author cannot make.

Pins: PR #931 reader at `29f636a2` (OPEN, unmerged); accepted private quote-path candidate r4
`b7af6579`; r1 `77b88654` on main `4ef90a14` (reviewed: REQUEST CHANGES); r2 on main `25fe9fd9`, branch
`claude/schwab-integration-r2-20261031` (local only, never pushed).

## 1. Slices, what exists, what is newly wired, who owns it

| # | Slice | Existing (reviewed) | Newly wired in this candidate | Owner of the slice | Owner of the live decision |
|---|---|---|---|---|---|
| 1 | **Reader** | `ingestion/schwab_market.py` (#931 + r4 boundary): OAuth refresh, private token file, `/quotes`, `/chains`, exact-integer and finite-float parsing | nothing | this author (PR #931) | GRID Root merges; Anik authorizes the release |
| 2 | **Quote path** | `ingestion/schwab_quote_path.py` (r4): classification `fresh/stale/delayed/missing/invalid/unavailable`, provenance, identity, fail-closed inputs, declared-audience label | nothing | this author; retained independent source reviewer | — |
| 3 | **Materialization** | pattern: `store/availability.py` contract | `ingestion/schwab_private_store.py`: process-local bounded store, identity + `as_of` + `vintage` carried, read-time ageing (`fresh`→`stale`, never promoted), `ProvenanceError` on loss of provenance, exact ints | this author | durable persistence (`raw_series` / table) is **not** built: needs Anik's redistribution/persistence sign-off (`persist_allowed=False`) |
| 4 | **Private API** | `api/auth.py` JWT role gate (`require_role("admin")`), optional router table in `api/main.py` | `api/routers/schwab_private.py`: `/api/v1/schwab/private/{status,quotes}`, admin-only, default-off flag `SCHWAB_PRIVATE_QUOTES_ENABLED` (config + `.env.example`); receipt time taken after the source operation and freshness re-evaluated at response time (r2); `Cache-Control: no-store, private` on handler responses (200/503/500; the framework's 401/403/422 carry no quote data) | this author | operator flips the flag in a reviewed change; Root deploys |
| 5 | **Private UI** | `pwa/src/views/TenYearPortfolio.jsx` "API setup" card (already Schwab-aware), `pwa/src/api.js` fetch wrapper, `authSession` role | `pwa/src/components/SchwabPrivateQuote.jsx` (admin-only render; LIVE only when `live && fresh` and not aged past `stale_after_seconds` on a monotonic local clock, re-evaluated each second with cleanup and no promotion (r2); as-of, vintage and evaluation time shown; counts use the server's `*_exact` strings), `api.getSchwabPrivateQuotes/Status` (excluded from the GET cache) | this author (no separate PWA owner on record) | Anik decides whether the admin view is enough or a different private surface is wanted |
| 6 | **Options / GEX seam** | `docs/reference/SCHWAB_GEX_CHAIN_SEAM.md`; GEX read contract in `collectors/gamma_watch/broker.py` | `ingestion/schwab_chain_seam.py`: pure `schwab_chain_to_rtd_records` with its own identity (`spy-chain-schwab-v1`/`GRID`), so the GEX identity gate rejects it; requested-symbol identity for chain, universe and underlying with explicit aliases only, and ambiguous duplicates invalidated (r2) | GEX owner (Claude session `local_d0cd874c`, gex lane; Codex custodians for the RTD collector on ANIK) | the six owner decisions in the seam note §4; no GEX file is touched here |
| 7 | **Scheduling / Hermes** | `ingestion/scheduler.py` pattern | nothing | Hermes owner | not requested; not built |
| 8 | **Credentials / runtime** | login helper `~/dev/grid-schwab/schwab_auth.py` on precision5520; token file 0600 | nothing | Anik (operator) | see §3 |

Boundary statement for slice 6: the author does not compete with the GEX owner. The seam
produces an envelope the owner *could* accept; accepting it (identity gate, timestamp
semantics, transport, universe roll, redistribution, catalogue entries) is theirs.

## 2. Data flow when every gate is open (none is today)

```
admin JWT ──> GET /api/v1/schwab/private/quotes?symbols=SPY
             │ require_role("admin")            (existing)      401/403
             │ SCHWAB_PRIVATE_QUOTES_ENABLED    (new, default off) 503
             │ is_configured() + SchwabMarketClient()  (existing reader; credentials stay in #931 code)
             │ read_private_quotes()            (r4: classify, provenance, identity)
             │ PrivateQuoteStore.put()          (new: materialize; ProvenanceError => 500, nothing stored)
             └ response = snapshot + view       (new: as_of/vintage/identity/status; live only if fresh)
PWA (admin) ─> SchwabPrivateQuote renders the view; nothing cached, nothing published
GEX         ─> (not connected) schwab_chain_to_rtd_records() would feed a snapshot the owner may accept
```

## 3. Unresolved prerequisites (explicit)

1. **Provider schema / entitlements.** Live `/quotes` semantics for SPY under this app
   (`realtime` flag, `quoteTime` in epoch ms, `symbol` echo) are documented, not verified from
   GRID; the only live read on record was a SPY quote on 2026-10-29 from the login helper on
   precision5520. `/chains` field names, units, `isDelayed`, gamma sign/scale: unverified.
2. **Credentials.** `credentials.env` + `tokens.json` exist only on precision5520 (0600). Nothing
   on grid-svr. The refresh token expires 2026-11-05; the weekly re-login is interactive and
   the helper lives on precision5520 only.
3. **Target runtime.** grid-api on grid-svr runs the release tree (`/data/grid_v4/grid_release`)
   from `origin/main`; this candidate is on neither. Python floor qualification for the reader
   (`Self`, `O_NOFOLLOW`) on the service interpreter is not re-verified here.
4. **Login lock.** The reader's `_TokenFileLock` and the login helper's own locking were never
   exercised against each other; a concurrent refresh on grid-svr and re-login on
   precision5520 would be two writers of two different token files, which is safe only
   because they are different files.
5. **Release prerequisites.** #931 unmerged (Root is the sole merger). This candidate has no PR,
   no hosted CI run (not dispatched by instruction), no push. Local checks: 597 Python tests
   in the Schwab suites + 30 API/auth/read-guard + 28 env-example tests pass; PWA 89 files /
   610 tests, `tsc --noEmit` and `npm run build` pass with Node 22.22.1 (one patch below the
   lockfile's engine floor 22.22.2; installed with `--engine-strict=false`, CI uses its own Node).
6. **Reviewer limit carried forward.** The outer-key alias limitation from r4 stands: an
   alias-keyed outer `/quotes` response maps to `missing`. Nothing in this plan or its receipts
   claims to prove how measured data would be disclosed on a live surface; that is D5/D6.

## 4. Minimum decisions required for live acceptance (in order)

| # | Decision | Decider | What it unlocks |
|---|---|---|---|
| D1 | Authorize release of #931 and merge it | Anik → Root | reader on main |
| D2 | Open a PR from this candidate and merge it | Anik (say-so) → Root | store, private API, UI hook, seam on main |
| D3 | Place `credentials.env` + `tokens.json` for the `grid` user on grid-svr (0600) and own the weekly re-login | Anik | `is_configured()` true on the target |
| D4 | Set `SCHWAB_PRIVATE_QUOTES_ENABLED=true` in the service env and restart grid-api | operator, reviewed change; Root deploys | endpoint answers 200 |
| D5 | First admin read of `/quotes?symbols=SPY` and inspection of `quote_status`/`freshness` | Anik (admin JWT) | verifies schema assumptions in §3.1; stale/delayed outcomes are reported as such, never as live |
| D6 | Redistribution / persistence sign-off | Anik | only then may any Schwab number leave the admin surface or be stored durably |
| D7 | GEX seam decisions 1–6 (identity gate, timestamps, transport, universe, redistribution, catalogue) | GEX owner | a Schwab chain leg; not built until decided |

D1–D5 are the smallest set that puts a private, admin-only, clearly-labelled quote in front of
Anik by the Sunday target. D6 and D7 are not on that path and are deliberately left closed.

## 5. What this candidate never does

No provider call at import or test time (every test uses synthetic bodies through the reviewed
parser), no credential or token read outside the reader's own code path, no account/position/
order endpoint, no persistence, no scheduler step, no public projection, no GEX-tree edit, no
push, no CI dispatch.
