#!/usr/bin/env node
/**
 * GRID production probe — READ-ONLY, NEVER RUN WITHOUT RELEASE APPROVAL.
 *
 * ============================================================================
 * THIS SCRIPT HAS NEVER BEEN EXECUTED. It was authored, prepared, and
 * `node --check`-syntax-validated only. It is meant to run against a real
 * deployed GRID origin, after a real release, by the operator — never by an
 * agent session. See tests/browser/PRODUCTION-PROBE-RUNBOOK.md before ever
 * running this.
 * ============================================================================
 *
 * What it does: logs in through the real login form/endpoint, visits a
 * FIXED list of journeys + endpoints, and saves screenshots/text/console/
 * network evidence to disk — exactly like tests/browser/run_evidence.mjs,
 * but against a real origin instead of the local fixture+dev-server pair,
 * and with production-specific guardrails (below) that run_evidence.mjs
 * does not need.
 *
 * What it refuses to do, and how:
 *   1. Refuses to start at all unless BOTH
 *        --i-have-release-approval <release-sha>
 *        --origin https://<host>
 *      are passed. Missing either -> print the reason and exit(1) before
 *      opening a browser, making a request, or writing anything.
 *   2. Refuses a non-`https://` origin (a `http://` or `localhost` origin
 *      is almost certainly a mistake — point run_evidence.mjs at that
 *      instead) and refuses an origin that looks like the local dev
 *      harness (`localhost`, `127.0.0.1`).
 *   3. Never injects a token. The ONLY non-GET request this script is
 *      allowed to make is `POST <origin>/api/v1/auth/login`. Enforced live,
 *      not just by convention: Chrome DevTools Protocol request
 *      interception aborts any other non-GET request before it leaves the
 *      browser (see `installRequestGuard`).
 *   4. Never prints or writes the login password. Read from
 *      `GRID_PROBE_PASSWORD` env, or an interactive masked prompt
 *      (`promptPasswordHidden`) if the env var is unset. Never logged,
 *      never included in any evidence file, never in a screenshot (the
 *      password field is typed, not left visible — see `loginThroughRealForm`).
 *   5. Rate-limited to <= 1 request/s against the target origin's `/api/`
 *      paths (`installRequestGuard`'s token-bucket gate). Static assets
 *      (JS/CSS/images) are not throttled — only API calls, which is the
 *      part capable of putting load on the backend.
 *   6. No crawling. Only the fixed `JOURNEYS` list (page visits) and the
 *      fixed `DIRECT_ENDPOINTS` list (raw GETs) are ever requested — no
 *      following of links, no discovery of new routes.
 *   7. Redacts before writing to disk: any JSON response body field whose
 *      key matches /token|password|secret/i (recursively) is replaced with
 *      "[REDACTED]" before it is ever written to a `*.json`/`*.jsonl` file
 *      (`redact`). The `Authorization` request header is never logged.
 *
 * This script CAPTURES; it does not ASSERT. It does not decide whether a
 * number is honest, whether a widget's empty state is correct, or whether
 * data looks fresh — it records HTTP status, timing, screenshots, and page
 * text, and leaves a per-journey checklist for the operator to fill by eye.
 * See "What this proves / does not prove" below and in the runbook.
 *
 * What this proves: the deployed pages render, what they say, and how the
 * deployed API responded, at the moment this ran.
 * What this does NOT prove: that any displayed number is correct, that the
 * underlying data pipeline is healthy, that behavior will be the same a
 * minute later, or anything about load, concurrency, or security posture.
 *
 * Usage (see the runbook for the full walkthrough):
 *   cd tests/browser
 *   npm ci
 *   GRID_PROBE_PASSWORD='...' node probe_production.mjs \
 *     --i-have-release-approval <release-sha> \
 *     --origin https://grid.stepdad.finance \
 *     --username operator [--contributor-username dad --contributor-password-env GRID_PROBE_CONTRIBUTOR_PASSWORD] \
 *     --ticker AAPL
 *
 * A contributor account is OPTIONAL — pass --contributor-username (and set
 * its password env var) only if a second, lower-privilege account exists to
 * probe with; the admin-only journeys and DIRECT_ENDPOINTS always run.
 */

import { mkdirSync, writeFileSync, appendFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import readline from 'node:readline';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ── CLI args ──────────────────────────────────────────────────────────

function parseArgs(argv) {
    const args = {
        origin: null,
        'i-have-release-approval': null,
        username: 'operator',
        'contributor-username': null,
        'contributor-password-env': null,
        ticker: null,
        out: null,
    };
    for (let i = 0; i < argv.length; i++) {
        const a = argv[i];
        if (a.startsWith('--')) {
            const key = a.slice(2);
            const val = argv[i + 1];
            args[key] = val;
            i++;
        }
    }
    return args;
}

const args = parseArgs(process.argv.slice(2));

// ── Refusal gate #1: approval + origin are both mandatory ───────────────
// This block runs before ANYTHING else — no browser, no network, no file
// writes above this line.

const RELEASE_SHA = args['i-have-release-approval'];
const ORIGIN_RAW = args.origin;

function refuse(reason) {
    console.error(`\nREFUSING TO RUN: ${reason}\n`);
    console.error('Required: --i-have-release-approval <release-sha> AND --origin https://<host>');
    console.error('See tests/browser/PRODUCTION-PROBE-RUNBOOK.md before running this script.');
    process.exit(1);
}

if (!RELEASE_SHA) {
    refuse('--i-have-release-approval <release-sha> was not passed. This script assumes a release ' +
        'has NOT been approved unless a human explicitly names the approved SHA on the command line.');
}
if (!/^[0-9a-fA-F]{7,40}$/.test(RELEASE_SHA)) {
    refuse(`--i-have-release-approval value "${RELEASE_SHA}" does not look like a git SHA (7-40 hex chars).`);
}
if (!ORIGIN_RAW) {
    refuse('--origin <https://host> was not passed. This script will not guess a target.');
}

let ORIGIN;
try {
    ORIGIN = new URL(ORIGIN_RAW);
} catch {
    refuse(`--origin "${ORIGIN_RAW}" is not a valid URL.`);
}
if (ORIGIN.protocol !== 'https:') {
    refuse(`--origin must be https:// (got "${ORIGIN.protocol}"). A plaintext origin has no place ` +
        'being probed with real credentials.');
}
const LOOKS_LOCAL = /^(localhost|127\.\d+\.\d+\.\d+|0\.0\.0\.0|::1)$/i.test(ORIGIN.hostname);
if (LOOKS_LOCAL) {
    refuse(`--origin "${ORIGIN_RAW}" looks like a local address, not a production deployment. ` +
        'Use tests/browser/run_evidence.mjs for local/dev harness runs instead.');
}
const ORIGIN_STR = ORIGIN.origin;

console.log(`Release approval acknowledged for SHA ${RELEASE_SHA}. Target origin: ${ORIGIN_STR}`);
console.log('This is a READ-ONLY probe (one login POST, everything else GET). Proceeding.');

// ── Password: env var or masked interactive prompt — never printed/written ──

function promptPasswordHidden(promptText) {
    return new Promise((resolve) => {
        const rl = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: true });
        const originalWrite = rl._writeToOutput.bind(rl);
        rl._writeToOutput = (str) => {
            // Echo the prompt text itself, but never the characters typed
            // after it (readline calls this on every keystroke).
            if (str.startsWith(promptText)) originalWrite(promptText);
        };
        rl.question(promptText, (answer) => {
            rl._writeToOutput = originalWrite;
            rl.close();
            process.stdout.write('\n');
            resolve(answer);
        });
    });
}

async function resolvePassword(envVar, label) {
    const fromEnv = process.env[envVar];
    if (fromEnv) return fromEnv;
    if (!process.stdin.isTTY) {
        refuse(`${envVar} is not set and stdin is not an interactive TTY to prompt for the ${label} password.`);
    }
    return promptPasswordHidden(`${label} password (not echoed, not logged): `);
}

// ── Redaction — applied to every JSON body before it touches disk ──────

const SECRET_KEY_RE = /token|password|secret/i;

function redact(value) {
    if (Array.isArray(value)) return value.map(redact);
    if (value && typeof value === 'object') {
        const out = {};
        for (const [k, v] of Object.entries(value)) {
            out[k] = SECRET_KEY_RE.test(k) ? '[REDACTED]' : redact(v);
        }
        return out;
    }
    return value;
}

// ── Browser executable — same convention as run_evidence.mjs ───────────

const CHROME_PATH = 'C:/Program Files/Google/Chrome/Application/chrome.exe';
const EDGE_PATH = 'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe';

function resolveBrowserExecutable() {
    if (existsSync(CHROME_PATH)) return CHROME_PATH;
    if (existsSync(EDGE_PATH)) return EDGE_PATH;
    throw new Error(`Neither Chrome nor Edge found at the expected paths. Edit CHROME_PATH/EDGE_PATH if installed elsewhere.`);
}

// ── Fixed journey + endpoint lists — NO crawling beyond these ──────────
// Mirrors the five first-release journeys from tests/browser/README.md.
// `ticker` is a placeholder the operator must supply for a real deployment
// (production has no "TEST1"); journeys that need one are skipped with a
// recorded reason if --ticker was not passed.

function journeysFor(ticker) {
    const tickerJourneys = ticker
        ? [
            { name: 'ticker-lookup', hash: `#/ticker-lookup?ticker=${encodeURIComponent(ticker)}`, roles: ['admin', 'contributor'] },
            { name: 'watchlist-analysis', hash: `#/watchlist/${encodeURIComponent(ticker)}`, roles: ['admin'] },
        ]
        : [
            { name: 'ticker-lookup', hash: '#/ticker-lookup', skipped_reason: 'no --ticker given', roles: ['admin', 'contributor'] },
            { name: 'watchlist-analysis', hash: null, skipped_reason: 'no --ticker given', roles: ['admin'] },
        ];
    return [
        // (a) home / market overview
        { name: 'home', hash: '#/home', roles: ['admin', 'contributor'] },
        // (b) ticker investigation
        ...tickerJourneys,
        // (c) watchlist / portfolio
        { name: 'portfolio', hash: '#/portfolio', roles: ['admin'] },
        // (d) research status
        { name: 'discovery', hash: '#/discovery', roles: ['admin'] },
        { name: 'pipeline-health', hash: '#/pipeline-health', roles: ['admin'] },
        // (e) data health
        { name: 'operator', hash: '#/operator', roles: ['admin'] },
        // Dad-mode-reachable journey, run under contributor too when available
        { name: 'ten-year', hash: '#/ten-year', roles: ['admin', 'contributor'] },
    ];
}

// Raw endpoint GETs named explicitly by the lead — recorded regardless of
// status; a 404 on the research endpoint is expected until W4c ships.
const DIRECT_ENDPOINTS = [
    { path: '/api/v1/system/health' },
    { path: '/api/v1/system/freshness' },
    { path: '/api/v1/system/pipeline-health' },
    { path: '/api/v1/snapshots/research/latest', note: 'may 404 until W4c ships — record, do not fail' },
    { path: '/api/v1/regime/current' },
    { path: '/api/v1/watchlist/' },
    { path: '/api/v1/ten-year-portfolio/weekly' },
];

// ── Rate limiter + non-GET guard, installed on every page ──────────────
// Single shared gate across all pages/requests in this run: <=1 request/s
// to ORIGIN's /api/ paths, and hard-abort on any non-GET request that
// isn't the one allowed login POST.

function makeApiRateGate(minIntervalMs = 1000) {
    let nextAllowedAt = 0;
    let queue = Promise.resolve();
    return function gate() {
        let release;
        const wait = new Promise((r) => { release = r; });
        queue = queue.then(async () => {
            const now = Date.now();
            const waitMs = Math.max(0, nextAllowedAt - now);
            if (waitMs > 0) await new Promise((r) => setTimeout(r, waitMs));
            nextAllowedAt = Date.now() + minIntervalMs;
            release();
        });
        return wait;
    };
}

const apiGate = makeApiRateGate(1000);

async function installRequestGuard(page, violationsLog) {
    await page.setRequestInterception(true);
    page.on('request', async (request) => {
        const url = request.url();
        const method = request.method();
        const isOurOrigin = url.startsWith(ORIGIN_STR);
        const isLoginPost = method === 'POST' && url === `${ORIGIN_STR}/api/v1/auth/login`;

        if (isOurOrigin && method !== 'GET' && !isLoginPost) {
            appendFileSync(violationsLog, JSON.stringify({
                ts: new Date().toISOString(), blocked_method: method, url,
            }) + '\n');
            await request.abort('aborted').catch(() => {});
            return;
        }

        if (isOurOrigin && url.includes('/api/')) {
            await apiGate();
        }
        await request.continue().catch(() => {});
    });
}

// ── Login through the real form — never inject a token ──────────────────

async function loginThroughRealForm(page, password) {
    await page.waitForSelector('input[placeholder="Password"]', { timeout: 20000 });
    await page.type('input[placeholder="Password"]', password);
    await page.click('button[type="submit"]');
    await page.waitForFunction(
        () => !document.querySelector('input[placeholder="Password"]'),
        { timeout: 20000 }
    ).catch(() => {
        // Recorded honestly by the caller via a screenshot/text dump —
        // don't throw, and never log the password either way.
    });
}

function detectCrash(bodyText) {
    return /\bError\b[\s\S]{0,200}\bRetry\b/.test(bodyText);
}

// ── Direct endpoint probing (no page navigation, just fetch) ───────────

async function probeDirectEndpoints(token, outDir) {
    const results = [];
    for (const ep of DIRECT_ENDPOINTS) {
        await apiGate();
        const url = `${ORIGIN_STR}${ep.path}`;
        const start = Date.now();
        let status = null;
        let bodySnippet = null;
        let error = null;
        try {
            const res = await fetch(url, {
                method: 'GET',
                headers: token ? { Authorization: `Bearer ${token}` } : {},
            });
            status = res.status;
            try {
                const json = await res.json();
                bodySnippet = redact(json);
            } catch {
                bodySnippet = null; // non-JSON or empty body — fine, still recorded by status/timing
            }
        } catch (err) {
            error = String(err && err.message || err);
        }
        results.push({
            path: ep.path,
            note: ep.note || null,
            status,
            timing_ms: Date.now() - start,
            error,
            body_redacted: bodySnippet,
        });
    }
    writeFileSync(path.join(outDir, 'direct_endpoints.json'), JSON.stringify(results, null, 2));
    return results;
}

// ── Per-role journey run ────────────────────────────────────────────────

async function runRole(browser, role, password, username, ticker, outRoot, networkGuardLog) {
    const outDir = path.join(outRoot, role);
    mkdirSync(outDir, { recursive: true });
    const consoleLogPath = path.join(outDir, 'console.jsonl');
    const networkLogPath = path.join(outDir, 'network.jsonl');

    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 900 });
    await installRequestGuard(page, networkGuardLog);

    let capturedToken = null;

    page.on('console', (msg) => {
        const type = msg.type();
        if (type === 'error' || type === 'warning') {
            appendFileSync(consoleLogPath, JSON.stringify({
                ts: new Date().toISOString(), role, type, text: msg.text(),
            }) + '\n');
        }
    });
    page.on('response', async (res) => {
        const url = res.url();
        if (!url.startsWith(ORIGIN_STR) || !url.includes('/api/')) return;
        let bodyRedacted = null;
        try {
            const json = await res.json();
            bodyRedacted = redact(json);
        } catch {
            bodyRedacted = null;
        }
        appendFileSync(networkLogPath, JSON.stringify({
            ts: new Date().toISOString(), role,
            method: res.request().method(), url, status: res.status(),
            body_redacted: bodyRedacted,
        }) + '\n');
        // Pull the token out of the login response for the direct-endpoint
        // probe below, without ever writing it anywhere — read from the
        // live response object, not from the redacted copy we just logged.
        if (url === `${ORIGIN_STR}/api/v1/auth/login`) {
            try {
                const raw = await res.json();
                if (raw && typeof raw.token === 'string') capturedToken = raw.token;
            } catch { /* already consumed or non-JSON; login screenshot/text still records the outcome */ }
        }
    });

    await page.goto(`${ORIGIN_STR}/#/login`, { waitUntil: 'networkidle2' });
    await page.evaluate(() => { try { localStorage.clear(); sessionStorage.clear(); } catch {} });
    await page.reload({ waitUntil: 'networkidle2' });
    // Username field only shows in user-login mode; master-password mode
    // (the default tab) has no username input — this probe assumes a real
    // user account per role, so switch to that tab if present.
    const hasUsernameToggle = await page.evaluate(() => {
        const tabs = Array.from(document.querySelectorAll('button'));
        const tab = tabs.find((b) => /user|account/i.test(b.textContent || ''));
        if (tab) { tab.click(); return true; }
        return false;
    });
    if (hasUsernameToggle) {
        const usernameInput = await page.$('input[placeholder="Username"]');
        if (usernameInput) await usernameInput.type(username);
    }
    await loginThroughRealForm(page, password);

    const journeys = journeysFor(ticker).filter((j) => j.roles.includes(role));
    const summary = {
        role, username, ticker: ticker || null,
        started_at: new Date().toISOString(),
        journeys: {},
    };

    for (const journey of journeys) {
        if (!journey.hash) {
            summary.journeys[journey.name] = { skipped_reason: journey.skipped_reason };
            continue;
        }
        try {
            await apiGate(); // pace navigation itself too, not just XHR/fetch
            await page.evaluate((hash) => { window.location.hash = hash; }, journey.hash);
            await page.waitForNetworkIdle({ idleTime: 800, timeout: 8000 }).catch(() => {});

            await page.setViewport({ width: 1280, height: 900 });
            await page.screenshot({ path: path.join(outDir, `${journey.name}.png`), fullPage: true });
            await page.setViewport({ width: 390, height: 844 });
            await page.screenshot({ path: path.join(outDir, `${journey.name}.mobile.png`), fullPage: true });
            await page.setViewport({ width: 1280, height: 900 });

            const bodyText = await page.evaluate(() => document.body.innerText);
            writeFileSync(path.join(outDir, `${journey.name}.text.txt`), bodyText);

            summary.journeys[journey.name] = {
                crashed: detectCrash(bodyText),
                // The operator fills these in by eye against the saved
                // screenshot/text — this script only captures, never asserts:
                operator_checklist: {
                    honest_unavailable_states: null, // true/false/'n/a'
                    no_invented_numbers: null,
                    data_ages_shown: null,
                    notes: '',
                },
            };
        } catch (err) {
            summary.journeys[journey.name] = { runner_error: String(err && err.message || err) };
        }
    }

    summary.finished_at = new Date().toISOString();
    writeFileSync(path.join(outDir, 'summary.json'), JSON.stringify(summary, null, 2));
    await page.close();
    return { summary, token: capturedToken };
}

// ── Entry point ───────────────────────────────────────────────────────

async function main() {
    let puppeteer;
    try {
        puppeteer = await import('puppeteer-core');
    } catch (err) {
        console.error('puppeteer-core is not installed. Run `npm ci` in tests/browser/ first.\n' + err);
        process.exit(1);
    }

    const executablePath = resolveBrowserExecutable();
    const runStamp = new Date().toISOString().replace(/[:.]/g, '-');
    const outRoot = path.resolve(
        args.out || path.join(__dirname, 'evidence', 'production', RELEASE_SHA, `run-${runStamp}`)
    );
    mkdirSync(outRoot, { recursive: true });
    const networkGuardLog = path.join(outRoot, 'blocked_requests.jsonl');

    const adminPassword = await resolvePassword('GRID_PROBE_PASSWORD', 'admin/operator');

    const browser = await puppeteer.default.launch({
        executablePath,
        headless: true,
        userDataDir: undefined, // let puppeteer create a fresh temp profile; never reused across runs
        args: ['--no-first-run', '--disable-extensions'],
    });

    const runResults = { origin: ORIGIN_STR, release_sha: RELEASE_SHA, started_at: new Date().toISOString(), roles: {} };
    try {
        const adminRun = await runRole(browser, 'admin', adminPassword, args.username, args.ticker, outRoot, networkGuardLog);
        runResults.roles.admin = adminRun.summary;

        // Direct endpoint checks run once, under the admin token (falls back
        // to unauthenticated if login didn't yield one — recorded honestly
        // either way via each entry's `status`).
        const directResults = await probeDirectEndpoints(adminRun.token, outRoot);
        runResults.direct_endpoints = directResults;

        if (args['contributor-username']) {
            const contribPasswordEnv = args['contributor-password-env'] || 'GRID_PROBE_CONTRIBUTOR_PASSWORD';
            const contribPassword = await resolvePassword(contribPasswordEnv, 'contributor');
            const contribRun = await runRole(browser, 'contributor', contribPassword, args['contributor-username'], args.ticker, outRoot, networkGuardLog);
            runResults.roles.contributor = contribRun.summary;
        } else {
            runResults.roles.contributor = { skipped_reason: 'no --contributor-username given (optional)' };
        }
    } finally {
        await browser.close().catch(() => {});
    }

    runResults.finished_at = new Date().toISOString();
    runResults.proves = 'The deployed pages rendered and what they displayed, at the moment this ran.';
    runResults.does_not_prove = 'That any displayed number is correct, that the data pipeline is healthy, ' +
        'that behavior is stable over time, or anything about load, concurrency, or security posture.';
    writeFileSync(path.join(outRoot, 'PROBE_SUMMARY.json'), JSON.stringify(runResults, null, 2));

    console.log(`\nDone. Evidence written under ${outRoot}`);
    console.log('Fill in each summary.json\'s operator_checklist by eye before treating this as a signed-off run.');
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
