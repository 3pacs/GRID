# M1 local SPY Options / Gamma Watch evidence

Prepared for issue #959 on main `5b16185b397cef60ef8a1827ec1fed9e268eecbd`
(#964). Tests/evidence lane only; no product/persona code changes. The finite
Muse J1 recovery and granular UI successor slices were already accepted; their
existing owners are retained (see [ownership reconciliation](MUSE-OWNERSHIP.json)).
No M0 capture, production probe, collector,
service, notification, release, or publication is performed by this harness.

## Reproduce

Install locked dependencies with `npm ci` in `pwa/` and `tests/browser/`.
Python 3 and a system Chrome/Chromium/Edge are required. No database is used.
From the repository root:

```sh
node --test tests/browser/spy_contract.test.mjs
python -m unittest discover -s tests/browser/fixture_api -p test_preload_coverage.py
node tests/browser/run_evidence.mjs --tree-label m1-spy --roles admin --journeys spy-options --scenarios healthy,partial,empty,unavailable,error --out /path/to/local/evidence
```

The default PWA is this checkout's `pwa/`. Override it with `--pwa-dir`.
Use `--browser-executable /path/to/browser` or `GRID_BROWSER_EXECUTABLE` for
nonstandard browser installs; normal Windows, Linux and macOS locations are
discovered without downloads. Local servers bind loopback ports 8001/5174;
occupied ports cause refusal. The runner launches Vite directly through Node
and closes its browser/server children afterward. It does not read a real
credential: it submits the fixture login form with a fixed synthetic password.

## Journey and assertions

Initial load is the existing 10Y route. Open **More views**, click the actual
**Options** drawer row, inspect empty **Signals**, select **Dealer Flow**
(default SPY), then select **Gamma Watch**. No direct hash substitution is
used for the Options navigation or tab changes. No external Gamma Watch link
or recommendation refresh button is clicked.

| Scenario | Expected evidence |
| --- | --- |
| healthy | Modeled SPY GEX with explicit synthetic provenance; measured zero versus missing max pain; public price and delayed levels with distinct source/receipt clocks; frozen historical research and hypothetical rebalance kept separate |
| partial | Stale SPY GEX withheld; fresh transport still withholds stale provider price/levels; frozen research remains separately labeled |
| empty | Explicit missing SPY capture; available transport with no public values/research points; values stay unavailable |
| unavailable | HTTP 503 collector-unavailable envelope; explicit no-cached-substitute message |
| error | HTTP 502 transport failure; collector status remains unknown |

Every scenario first captures **Connecting to Gamma Watch** while its fixture
request is deliberately delayed 1.5 seconds. Final screenshots are saved at
1280x900 and 390x844; intermediate captures use desktop size. Assertions test
rendered text and actual navigation, not just component mocks. The Node tests
run the Python fixture through the real `gammaWatchPublicAdapter.js`; they
test accepted values, source staleness, display expiry, zero/null, clocks,
HTTP failure distinctions, browser override and manifest binding.

`fixture_api/spy.py` follows `api/routers/gamma_watch.py`'s bridge envelope,
`pwa/src/components/gammaWatchPublicAdapter.js`'s public schema and
`pwa/src/__tests__/gammaWatchPublicContract.test.jsx`. SPY dealer fixtures use
the shape consumed by `GEXProfile.jsx`/`GEXProvenance.jsx`. Vanna/charm and
historical flow are deliberately unavailable/empty; this is no coverage
claim for them. The fixed SPY dealer receipt dates are synthetic labels,
not a fresh quote. Gamma fixture source/receipt times are relative to the
local server clock, with an injectable fixed clock for contract tests.

## Receipt binding and limits

Each run has a fresh timestamped directory. `summary.json` includes harness
and PWA commits, dirty paths, per-file SHA256 manifests before/after, actual
served Vite-module hashes, relevant synthetic API response bodies/statuses,
assertion results, final URL, blocked external requests and console/network
logs. Nonzero exit means a runner/assertion/readiness failure, changed source,
missing Gamma transport receipt, unexpected bootstrap/SPY API failure, or empty journey selection. A completed
capture is evidence to review, not an independent reviewer verdict.

The service worker is bypassed so API requests and payloads are observable;
offline caching/PWA lifecycle are **not** qualified. All page requests outside
the two local server origins are aborted (including the Google Fonts import).
Chrome's own background networking is not a tested product surface.

### Preload reconciliation (2026-11-06)

All 14 earlier 404 paths are real source-defined GET routes. They originate in
`OPERATOR_PRELOAD_API_PATHS` (`pwa/src/app.jsx:120`), called by
`warmVisualizations` through `api.preload` after authentication. The standalone
fixture server had no handlers for them. [The inventory](fixture_api/preload_coverage.json)
binds every path to its handler, optional facade prefix, and main router registry.
Tests inspect those actual source definitions without importing the backend,
and check that every current preload is either modeled or explicitly omitted.
This is source registration evidence, not a claim that optional routers loaded
successfully in any running deployment.

These unvisited surfaces remain **outside the SPY fixture contract**. The server
now returns explicit HTTP **501**, with `fixture_preload_unavailable`, the path,
source handler and reason. It does not fabricate successful data or silently
cache an empty market snapshot. 501 is a harness-only coverage marker, not the
status/schema promised by the real product route. Existing real fixture routes
take precedence. Unknown paths and unsupported methods still return 404.

Every failure body/status remains captured in `summary.json` and the network
log; console errors are retained. Classification accepts only the exact
allowlisted 501 marker and the scenario's intended Gamma 503/502 response.
Unknown/malformed markers or any other bootstrap/SPY API failure fail the M1
network gate. Other legacy journeys retain their separate, unqualified status.
Thus a passing SPY journey still does **not** claim a fully green app network
or coverage of the 14 omitted product surfaces.

The ten September evidence directories are historical supporting runs with
different source compositions. Their screenshots do not qualify this source.
The obsolete `#/godview` path is replaced by `#/god-view`; the current
`/api/v1/godview/latest` fixture deliberately supplies unavailable pillars.

Current-owner availability, the original failing deployed URL, any required Muse
source-adoption mapping, source coverage/exclusions/OI vintage on the SPY inventory,
actual served release/bundle identity, operator-run production evidence and
independent acceptance remain controller/owner gates in #959. The existing
[production runbook](PRODUCTION-PROBE-RUNBOOK.md) is unchanged and still applies.
The existing [PR2 acceptance receipt](https://github.com/3pacs/muse/pull/2#issuecomment-6005299300)
and MAIN Amendment 2 supersede the old PR3 status shorthand: do not reopen the
completed finite J1/UI slices or assign a replacement hosted frontend owner.
