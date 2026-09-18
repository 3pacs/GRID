# Production probe runbook — `probe_production.mjs`

**This tool is prepared, never run.** It was written and `node --check`
syntax-validated only. No agent session runs this against a real origin —
it is executed only after an approved release, and only by the operator
(Anik), by hand, from a machine with real production credentials.

## What this proves / does not prove

**Proves:** the deployed pages rendered and what they displayed — text,
screenshots, HTTP status, and timing — at the moment the probe ran.

**Does not prove:**
- That any displayed number is correct. This tool captures; it never
  asserts. It does not know what a "right" value looks like.
- That the underlying data pipeline, ingestion, or scoring is healthy —
  only that the API answered (or didn't) for the fixed endpoint list below.
- That behavior is stable. A second run five minutes later could differ —
  this is one point-in-time snapshot, not a monitor.
- Anything about load, concurrency, or security posture. One session at
  ≤1 request/s proves nothing about either.

Every `summary.json` this tool writes has an `operator_checklist` block per
journey with `null` values — **the operator fills those in by eye** against
the saved screenshot/text (honest unavailable states? no invented numbers?
data ages shown?). The script does not and cannot fill them in itself.

## Non-negotiable safety properties (enforced in code, not just by convention)

| Property | How it's enforced |
|---|---|
| Refuses to run without release approval | `--i-have-release-approval <release-sha>` is required and validated as a 7-40 char hex string before any browser/network/file activity. No default, no "skip this check" flag. |
| Refuses to run without an explicit target | `--origin https://<host>` is required; rejects non-`https://` and rejects anything that looks local (`localhost`/`127.0.0.1`/`0.0.0.0`/`::1`) — that's what `run_evidence.mjs` is for. |
| Read-only except the one login | Chrome DevTools Protocol request interception (`installRequestGuard`) **aborts** any request to the target origin that is not `GET`, except exactly `POST <origin>/api/v1/auth/login`. This runs live in the browser process, not as a lint rule. |
| Never injects a token | The script only ever obtains a token by watching the real login response after submitting the real form (`loginThroughRealForm` + the `response` listener in `runRole`). There is no code path that constructs or sets a token directly. |
| Password never printed or written | Read from `GRID_PROBE_PASSWORD` (and `GRID_PROBE_CONTRIBUTOR_PASSWORD` or the env var named by `--contributor-password-env`) or a masked interactive prompt (`promptPasswordHidden`, which suppresses echo of anything typed after the prompt string). Never appears in any `console.log`, any written file, or any screenshot (it's typed into the field, not left rendered — the login screenshot only ever shows whatever the page itself masks). |
| Rate limited | `makeApiRateGate(1000)` — a single shared token-bucket gate serializes all requests to the origin's `/api/` paths to at most one per second, across every page and every journey in the run. Static assets are not throttled (throttling images/CSS would make the page load take forever for no safety benefit). |
| No crawling | Only `journeysFor()` (hash routes) and `DIRECT_ENDPOINTS` (raw GETs) are ever requested — both are fixed, hard-coded lists in the script. There is no link-following, no sitemap walk, no recursive discovery. |
| Secrets redacted before they touch disk | Every JSON response body is passed through `redact()` before being written to `network.jsonl` or `direct_endpoints.json`: any key matching `/token|password|secret/i`, anywhere in the (possibly nested) object, becomes the literal string `"[REDACTED]"`. The `Authorization` header itself is never logged. |

## Prerequisites

1. **A release has actually been approved**, and you have its SHA. If you
   don't have this in hand, stop — do not invent one to get past the
   refusal gate.
2. `tests/browser/` has its dependencies installed:
   ```bash
   cd tests/browser
   npm ci
   ```
3. System Chrome or Edge installed at the default path this script checks
   (`C:/Program Files/Google/Chrome/Application/chrome.exe`, falling back
   to Edge). Edit `CHROME_PATH`/`EDGE_PATH` at the top of the script if
   installed elsewhere — this tool does not download a browser.
4. A real admin/operator account username + password on the target
   deployment, and (optional) a real contributor account if you want that
   role's journeys covered too. **A contributor account is optional** —
   the script runs the admin-only journeys and the fixed direct-endpoint
   list regardless; pass `--contributor-username` only if you have a
   second, lower-privilege account to probe with.
5. A real ticker to use for the ticker-investigation journeys
   (`--ticker AAPL` or similar) — production has no synthetic `TEST1`.
   Omitting it is allowed; those two journeys are recorded as skipped with
   a reason instead of guessed at.

## Running it

```bash
cd tests/browser

# Set the password where your shell history won't keep it, e.g. read into
# an env var interactively rather than typing it as a literal on the
# command line:
read -s GRID_PROBE_PASSWORD
export GRID_PROBE_PASSWORD

node probe_production.mjs \
  --i-have-release-approval <the-approved-release-sha> \
  --origin https://grid.stepdad.finance \
  --username operator \
  --ticker AAPL
```

With an optional contributor account:

```bash
export GRID_PROBE_CONTRIBUTOR_PASSWORD='...'   # or use --contributor-password-env to name a different var
node probe_production.mjs \
  --i-have-release-approval <sha> \
  --origin https://grid.stepdad.finance \
  --username operator \
  --contributor-username dad \
  --ticker AAPL
```

If you omit `GRID_PROBE_PASSWORD`, the script prompts interactively with
input echo suppressed (masked) — only when stdin is a real TTY; otherwise
it refuses rather than hang.

## What gets written, and where

```
tests/browser/evidence/production/<release-sha>/run-<UTC-timestamp>/
  PROBE_SUMMARY.json          # origin, release SHA, per-role summary refs, proves/does-not-prove
  blocked_requests.jsonl      # any request the guard aborted (should normally be empty)
  direct_endpoints.json       # status/timing/redacted-body for the 7 fixed endpoints
  admin/
    console.jsonl             # console errors/warnings
    network.jsonl             # every /api/ response: method, url, status, redacted body
    summary.json              # per-journey {crashed, operator_checklist:{...}}
    home.png / home.mobile.png / home.text.txt
    ticker-lookup.png / ...
    watchlist-analysis.png / ...
    portfolio.png / ...
    discovery.png / ...
    pipeline-health.png / ...
    operator.png / ...
    ten-year.png / ...
  contributor/                 # only if --contributor-username was given
    ... same file shapes, fewer journeys (home, ticker-lookup, ten-year)
```

Every screenshot is full-page, captured twice per journey: 1280×900
(desktop) and 390×844 (mobile), matching `run_evidence.mjs`'s convention.

## After it runs

1. Open each `summary.json` and fill in `operator_checklist` per journey by
   eye, against the matching screenshot/text file:
   - `honest_unavailable_states`: did an unavailable widget say so, rather
     than showing a blank or a fabricated number?
   - `no_invented_numbers`: no percentage/score appeared where you'd expect
     `null`/unknown given what you know about the deployment's data state?
   - `data_ages_shown`: where a reading has an age/staleness, is it shown?
   - `notes`: anything else worth recording in plain language.
2. Check `direct_endpoints.json` — a 404 on `/api/v1/snapshots/research/latest`
   is expected until W4c ships; record it, it is not a failure of this probe.
3. Check `blocked_requests.jsonl` is empty. If it isn't, something the
   pages did tried to make a non-GET call to the origin outside the login
   flow — stop and investigate before treating the rest of the run as
   trustworthy.
4. This evidence is a point-in-time snapshot for a specific release SHA.
   It does not need to be re-run for every later deploy of the *same*
   release, but it proves nothing about a *different* release.

## What NOT to do with this tool

- Do not run it against any origin you have not been explicitly told is
  the approved production deployment.
- Do not run it without a real release SHA you can point to.
- Do not add a flag that skips the approval/origin checks "for testing" —
  test against a local deployment with `run_evidence.mjs` instead, which
  has no production guardrails to route around because it never needs any.
- Do not commit real credentials, real screenshots, or real evidence
  output from a production run into this repository. This runbook and the
  script are the deliverable; a real run's output is operational evidence,
  not source code, and belongs wherever the operator's other production
  evidence lives — not in `tests/browser/evidence/` in git history.
