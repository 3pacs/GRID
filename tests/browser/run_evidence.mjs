#!/usr/bin/env node
/**
 * Evidence runner for the GRID browser-acceptance harness.
 *
 * Drives system Chrome (or Edge) via puppeteer-core against the fixture
 * server + a PWA dev server, and saves screenshots/text/console/network
 * logs as files under `--out`, because the browser tool available to the
 * lead cannot save images.
 *
 * IMPORTANT — not run by the harness build itself:
 *   This script was written to spec but has NOT been executed in this
 *   session. The task that built this harness (tests/browser/fixture_api/*)
 *   was explicitly told not to install or drive a browser itself, because
 *   only one browser is available on this machine and it is driven live by
 *   the lead — the same constraint applies to launching Chrome/Edge via
 *   puppeteer-core, so this script was authored but never run, `npm ci`
 *   was never run in this directory, and no tests/browser/evidence/ output
 *   was generated or committed by that session. Run it yourself:
 *
 *     cd tests/browser
 *     npm ci
 *     node run_evidence.mjs --tree-label <sha-or-label> [options]
 *
 * Usage:
 *   node run_evidence.mjs --tree-label <sha-or-label> \
 *     [--pwa-dir <path>] [--scenarios healthy,partial,empty] \
 *     [--roles admin,contributor] [--journeys home,ticker-lookup,...] \
 *     [--out tests/browser/evidence/<tree-label>/]
 *
 * --pwa-dir defaults to this checkout's own pwa/ directory.
 *
 * Browser: set CHROME_PATH to a Chrome/Chromium/Edge executable, or let the
 * runner auto-detect a system install (Linux: google-chrome[-stable],
 * chromium[-browser]; macOS: /Applications; Windows: Program Files). No
 * browser is ever downloaded.
 *
 * Python: set PYTHON to the interpreter for the fixture server; defaults to
 * `python3` on Linux/macOS and `python` on Windows.
 *
 * --journeys filters to journey ids already used as keys in summary.json
 * (e.g. `home`, `ticker-lookup`, `catalyst-timeline-ACME`) — useful for
 * re-verifying one journey (e.g. after a fix) without a full re-run.
 *
 * Ports: fixture server 8001, PWA dev server 5174 — deliberately not
 * 8000/5173, which are the lead's own running servers.
 */

import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, appendFileSync, existsSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '..', '..'); // this harness worktree root

// ── CLI args ──────────────────────────────────────────────────────────

function parseArgs(argv) {
    const args = {
        'pwa-dir': path.join(REPO_ROOT, 'pwa'),
        scenarios: 'healthy,partial,empty',
        roles: 'admin,contributor',
        journeys: null, // null = all journeys for the role (default)
        out: null,
        'tree-label': null,
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
    if (!args['tree-label']) {
        console.error('--tree-label is required (written into every output file).');
        process.exit(1);
    }
    if (!args.out) {
        args.out = path.join(__dirname, 'evidence', args['tree-label']);
    }
    return args;
}

const args = parseArgs(process.argv.slice(2));
const PWA_DIR = path.resolve(args['pwa-dir']);
const TREE_LABEL = args['tree-label'];
const SCENARIOS = args.scenarios.split(',').map((s) => s.trim()).filter(Boolean);
const ROLES = args.roles.split(',').map((s) => s.trim()).filter(Boolean);
const JOURNEY_FILTER = args.journeys
    ? new Set(args.journeys.split(',').map((s) => s.trim()).filter(Boolean))
    : null;
// Every attempt gets its own run directory so a failed or superseded attempt
// can never contaminate a later verdict (old 502 rows in an appended
// network.jsonl once looked like view behaviour).
const RUN_STAMP = new Date().toISOString().replace(/[:.]/g, '-');
const OUT_ROOT = path.join(path.resolve(args.out), `run-${RUN_STAMP}`);

const FIXTURE_PORT = 8001;
const PWA_PORT = 5174;
const FIXTURE_BASE = `http://127.0.0.1:${FIXTURE_PORT}`;
const PWA_BASE = `http://localhost:${PWA_PORT}`;

// ── Browser executable ──────────────────────────────────────────────
// No browser download: point puppeteer-core at the system install.

const BROWSER_CANDIDATES = {
    linux: [
        '/usr/bin/google-chrome',
        '/usr/bin/google-chrome-stable',
        '/opt/google/chrome/chrome',
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
        '/snap/bin/chromium',
        '/usr/bin/microsoft-edge',
    ],
    darwin: [
        '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
        '/Applications/Chromium.app/Contents/MacOS/Chromium',
        '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
    ],
    win32: [
        'C:/Program Files/Google/Chrome/Application/chrome.exe',
        'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe',
        'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
        'C:/Program Files/Microsoft/Edge/Application/msedge.exe',
    ],
};

function resolveBrowserExecutable(env = process.env, platform = process.platform, exists = existsSync) {
    const override = env.CHROME_PATH;
    if (override) {
        if (exists(override)) return override;
        throw new Error(`CHROME_PATH=${override} does not exist.`);
    }
    const candidates = BROWSER_CANDIDATES[platform] || BROWSER_CANDIDATES.linux;
    const found = candidates.find((p) => exists(p));
    if (found) return found;
    throw new Error(
        `No Chrome/Chromium/Edge found (checked: ${candidates.join(', ')}). ` +
        'This runner does not download a browser — install one or set CHROME_PATH.'
    );
}

const PYTHON = process.env.PYTHON || (process.platform === 'win32' ? 'python' : 'python3');

// ── Journeys per role ─────────────────────────────────────────────────
// Hashes confirmed against pwa/src/routing.js / routes.js / app.jsx:
//   'watchlist' + segment -> view 'watchlist-analysis' (the #/watchlist/TEST1 shortcut)
//   DAD_VIEWS (contributor / "dad mode") = {'home', 'ten-year', 'ticker-lookup'}
//   — catalyst-timeline is NOT a dad view, so visiting it as contributor is
//   expected to either redirect or crash; record whichever happens, don't
//   "fix" the attempt.

const JOURNEYS = {
    admin: [
        { name: 'home', hash: '#/home', clickText: 'How are my stocks doing?', ready: /Your read|Start over|Here's (how|what)/ },
        { name: 'ticker-lookup', hash: '#/ticker-lookup', ready: /GOLD VERDICT/ },
        { name: 'watchlist-analysis', hash: '#/watchlist/TEST1', ready: /INSIDER EDGE|AI OVERVIEW/ },
        { name: 'portfolio', hash: '#/portfolio', ready: /Portfolio Value/ },
        { name: 'operator', hash: '#/operator', ready: /HERMES STATUS/ },
        { name: 'discovery', hash: '#/discovery', ready: /HYPOTHESES|RESEARCH RUN|No research run recorded|Research status unavailable/ },
        { name: 'pipeline-health', hash: '#/pipeline-health', ready: /PIPELINE HEALTH|Pipeline health unavailable/ },
        { name: 'ten-year', hash: '#/ten-year', ready: /10-Year|TOP CHART|No eligible/ },
        { name: 'snapshots', hash: '#/snapshots', ready: /LATEST SNAPSHOT|No snapshots|Snapshots unavailable|unexpected response/ },
        // Main's route id is 'god-view' -> views/GodView.jsx (routes.js),
        // which reads GET /api/v1/godview/latest. The old '#/godview' route
        // (W6 draft GodViewPillars.jsx) does not exist on main.
        {
            name: 'god-view', hash: '#/god-view', ready: /Point in time|God view unavailable/,
            checks: {
                healthy: { expect: [/FED NET LIQUIDITY/, /CFTC POSITIONING/, /DEALER GAMMA \(SPY, MODELED\)/, /no data/] },
                partial: { expect: [/STALE/] },
                empty: { expect: [/schema not migrated/i, /UNAVAILABLE/] },
            },
        },
        // Money Flow -> Dashboard tab: components/flows/FreshnessIndicator.jsx.
        // A confirmed node reads OBSERVED and a missing confidence UNKNOWN —
        // never the retired "LIVE" / "EST" labels.
        {
            name: 'money-flow-dashboard', hash: '#/money-flow', clickText: 'Dashboard',
            ready: /junction_point_readings/,
            checks: {
                healthy: {
                    expect: [/OBSERVED/, /UNKNOWN/, /ESTIMATED/, /newest obs 2026-09-15/],
                    forbid: [/\bEST\b/],
                },
                partial: { expect: [/STALE/, /newest obs 2026-09-09 \(6d old, threshold 3d\)/], forbid: [/\bEST\b/] },
                empty: { expect: [/UNAVAILABLE/, /No junction points found/], forbid: [/\bEST\b/] },
            },
        },
        // Options -> Gamma Watch tab (components/GammaWatch.jsx). Delayed
        // provider levels, frozen research, hypothetical rebalancing and the
        // RTD receipt must stay separately labelled; the public schema
        // (healthy) carries no RTD price, the legacy frame (partial) does.
        {
            name: 'options-gamma-watch', hash: '#/options', clickText: 'Gamma Watch',
            ready: /Gamma Watch · experimental public context|Collector unavailable|Request failed|Request timed out/,
            checks: {
                healthy: {
                    expect: [
                        /GRID connection recent/,
                        /ZeroGEX delayed context · unverified-delayed/,
                        /Delayed levels: put wall 590 · call wall 605/,
                        /Frozen historical research · not current GEX/,
                        /frozen-research/,
                        /Rebalancing sensitivity · hypothetical demand/,
                        /modeled-history-unqualified/,
                        /Private RTD, current GEX, journal and contracts unavailable on the public schema/,
                    ],
                    forbid: [/Legacy RTD receipt-only price/, /Display withheld/],
                },
                partial: {
                    expect: [
                        /GRID connection recent/,
                        /ZeroGEX delayed context · unverified-delayed/,
                        /Legacy RTD receipt-only price 600\.5/,
                        /Rebalancing sensitivity · hypothetical demand/,
                        /Frozen historical research · not current GEX/,
                    ],
                    forbid: [/Display withheld/],
                },
                empty: {
                    expect: [/Collector unavailable; no cached substitute\./],
                    forbid: [/Delayed levels/, /RTD receipt-only price/],
                },
            },
        },
        // Operator (admin) can reach catalyst-timeline normally, unlike the
        // contributor gate below — added so the earlier owner-fix on this
        // view can be re-verified from the admin side too.
        { name: 'catalyst-timeline-ACME', hash: '#/catalyst-timeline?ticker=ACME', ready: /Catalyst|CATALYST|Enter a ticker|Error/ },
    ],
    contributor: [
        { name: 'home', hash: '#/home', clickText: 'How are my stocks doing?', ready: /Your read|Start over|Here's (how|what)/ },
        { name: 'ticker-lookup', hash: '#/ticker-lookup', ready: /GOLD VERDICT/ },
        { name: 'ten-year', hash: '#/ten-year', ready: /10-Year|TOP CHART|No eligible/ },
        { name: 'catalyst-timeline-ACME', hash: '#/catalyst-timeline?ticker=ACME', allowCrash: true, ready: /Catalyst|CATALYST|Enter a ticker|Error|Tap a question/ },
    ],
};

// ── Process helpers ──────────────────────────────────────────────────

function killTree(child) {
    if (!child || child.killed || child.exitCode !== null) return;
    try {
        if (process.platform === 'win32') {
            spawnSync('taskkill', ['/pid', String(child.pid), '/T', '/F'], { stdio: 'ignore' });
        } else {
            // Children are spawned detached (own process group), so the
            // negative pid reaps `npm run dev` *and* the vite grandchild that
            // actually holds the port; killing npm alone leaves vite on 5174
            // and the next pair's assertPortsFree refuses to start.
            try { process.kill(-child.pid, 'SIGKILL'); } catch { child.kill('SIGKILL'); }
        }
    } catch {
        // best-effort — a leaked dev server is a smaller problem than a
        // thrown error stopping the rest of the run
    }
}

async function waitForHttp(url, { timeoutMs = 30000, intervalMs = 500 } = {}) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        try {
            const res = await fetch(url, { method: 'GET' });
            if (res.status < 500) return true; // any non-5xx means the server answered
        } catch {
            // not up yet
        }
        await new Promise((r) => setTimeout(r, intervalMs));
    }
    return false;
}

function gitRevParse(cwd) {
    try {
        const res = spawnSync('git', ['rev-parse', 'HEAD'], { cwd, encoding: 'utf-8' });
        return res.status === 0 ? res.stdout.trim() : null;
    } catch {
        return null;
    }
}

// ── Server lifecycle for one (role, scenario) pair ──────────────────

/**
 * Refuse to start a pair while its ports are still held (e.g. a fixture or
 * Vite process from a previous invocation that has not finished dying).
 * A pair that starts on a half-dead port produces 502s from the proxy that
 * look like view failures; fail loudly instead and record it.
 */
async function portBusy(port) {
    const net = await import('node:net');
    return new Promise((resolve) => {
        const srv = net.createServer();
        srv.once('error', () => resolve(true));
        srv.once('listening', () => srv.close(() => resolve(false)));
        srv.listen(port, '127.0.0.1');
    });
}

async function assertPortsFree(ports) {
    for (const port of ports) {
        if (await portBusy(port)) throw new Error(`port ${port} is still in use; refusing to start this pair (stale server from a previous run?)`);
    }
}

/** After killTree, wait (bounded) for the OS to release this pair's ports. */
async function waitPortsReleased(ports, timeoutMs = 15000) {
    const deadline = Date.now() + timeoutMs;
    for (const port of ports) {
        while (await portBusy(port)) {
            if (Date.now() > deadline) return;
            await new Promise((r) => setTimeout(r, 250));
        }
    }
}

function startFixtureServer(scenario, role) {
    const serverPath = path.join(__dirname, 'fixture_api', 'server.py');
    const child = spawn(
        PYTHON,
        [serverPath, '--port', String(FIXTURE_PORT), '--scenario', scenario],
        {
            cwd: REPO_ROOT,
            env: { ...process.env, FIXTURE_ROLE: role },
            stdio: ['ignore', 'pipe', 'pipe'],
            detached: process.platform !== 'win32',
        }
    );
    return child;
}

function startPwaDevServer() {
    const npmCmd = process.platform === 'win32' ? 'npm.cmd' : 'npm';
    const child = spawn(
        npmCmd,
        ['run', 'dev', '--', '--port', String(PWA_PORT), '--strictPort'],
        {
            cwd: PWA_DIR,
            env: {
                ...process.env,
                GRID_API_PROXY_TARGET: FIXTURE_BASE,
                GRID_WS_PROXY_TARGET: `ws://127.0.0.1:${FIXTURE_PORT}`,
            },
            stdio: ['ignore', 'pipe', 'pipe'],
            // Node >= 20.12 refuses to spawn a .cmd shim without a shell (EINVAL);
            // stopChild's taskkill /T reaps the whole shell tree on Windows.
            shell: process.platform === 'win32',
            detached: process.platform !== 'win32',
        }
    );
    return child;
}

// ── Puppeteer helpers ─────────────────────────────────────────────────

async function loginThroughRealForm(page) {
    // Login.jsx: password input has placeholder "Password", submit button
    // is type=submit with text AUTHENTICATE (master-password mode is the
    // default tab). The fixture server's /api/v1/auth/login ignores the
    // password value entirely (see README "Auth: the legitimate local dev
    // path") — any non-empty string logs in.
    await page.waitForSelector('input[placeholder="Password"]', { timeout: 15000 });
    await page.type('input[placeholder="Password"]', 'fixture-dev-password');
    await page.click('button[type="submit"]');
    // Wait for the app shell to replace the login card.
    await page.waitForFunction(
        () => !document.querySelector('input[placeholder="Password"]'),
        { timeout: 15000 }
    ).catch(() => {
        // If login didn't visibly complete, subsequent navigation/screenshots
        // will show that honestly — don't throw here.
    });
}

async function clickByVisibleText(page, text) {
    const clicked = await page.evaluate((needle) => {
        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
        let node;
        while ((node = walker.nextNode())) {
            if (node.children.length === 0 && node.textContent && node.textContent.trim() === needle) {
                node.click();
                return true;
            }
        }
        return false;
    }, text);
    return clicked;
}

/**
 * Wait for the page's network to settle before capturing anything. Panels
 * that fetch their own data independently (see README "Home compose")
 * can still be mid-flight right after a hash change or click; this is
 * called again immediately before each screenshot/text dump, not just
 * once after navigation, per the h-vs-g rerun finding that most text-dump
 * diffs were unsettled panels / nav-chrome timing / clock strings rather
 * than a real source difference.
 */
async function waitSettled(page) {
    await page.waitForNetworkIdle({ idleTime: 800, timeout: 8000 }).catch(() => {
        // Some views (e.g. a chart with a polling widget) never truly go
        // idle — capture anyway rather than hang the whole run on one view.
    });
}

/**
 * Bounded, explicit UI-ready wait: poll document.body.innerText for the
 * journey's `ready` regex (a piece of copy that only the settled view
 * renders). Network-idle alone is not enough — SSE streams or polling
 * widgets may never go idle. Returns 'ok', 'timeout', or 'none' (journey
 * has no ready condition). A 'timeout' is recorded in summary.json and
 * marks that journey's captures as INCOMPLETE evidence, never accepted.
 */
async function waitForReady(page, journey, timeoutMs = 10000) {
    if (!journey.ready) return { ready: 'none', ready_wait_ms: 0 };
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        const text = await page.evaluate(() => document.body.innerText).catch(() => '');
        if (journey.ready.test(text)) return { ready: 'ok', ready_wait_ms: Date.now() - start };
        await new Promise((r) => setTimeout(r, 250));
    }
    return { ready: 'timeout', ready_wait_ms: Date.now() - start };
}

/**
 * Scenario-specific label assertions: every `expect` regex must match the
 * settled body text and no `forbid` regex may. Recorded per journey in
 * summary.json as `label_checks`; a failure is evidence, not a runner error.
 */
function evaluateChecks(journey, scenario, bodyText) {
    const spec = journey.checks && journey.checks[scenario];
    if (!spec) return null;
    const missing = (spec.expect || []).filter((re) => !re.test(bodyText)).map(String);
    const forbiddenPresent = (spec.forbid || []).filter((re) => re.test(bodyText)).map(String);
    return { pass: missing.length === 0 && forbiddenPresent.length === 0, missing, forbidden_present: forbiddenPresent };
}

function detectCrash(bodyText) {
    // ViewErrorBoundary.jsx renders "<ViewName> Error" as an <h3>, with
    // "An unexpected error occurred" (or the real error message) below it,
    // and a "Retry" button. Match on the combination rather than a single
    // generic word, since "error" alone appears in legitimate copy too.
    return /\bError\b[\s\S]{0,200}\bRetry\b/.test(bodyText);
}

// ── Main per-(role, scenario) run ────────────────────────────────────

async function runOne(browser, role, scenario, outDir, fixtureChild = { exitCode: null }) {
    mkdirSync(outDir, { recursive: true });
    const consoleLogPath = path.join(outDir, 'console.jsonl');
    const networkLogPath = path.join(outDir, 'network.jsonl');
    const summary = {
        tree_label: TREE_LABEL,
        harness_commit: gitRevParse(REPO_ROOT),
        pwa_dir: PWA_DIR,
        pwa_commit: gitRevParse(PWA_DIR),
        role,
        scenario,
        started_at: new Date().toISOString(),
        journeys: {},
    };

    const userDataDir = mkdtempSync(path.join(tmpdir(), `grid-evidence-${role}-${scenario}-`));
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 900 });

    page.on('console', (msg) => {
        const type = msg.type();
        if (type === 'error' || type === 'warning') {
            appendFileSync(consoleLogPath, JSON.stringify({
                ts: new Date().toISOString(), role, scenario, type, text: msg.text(),
            }) + '\n');
        }
    });
    page.on('response', (res) => {
        const url = res.url();
        if (url.includes('/api/')) {
            appendFileSync(networkLogPath, JSON.stringify({
                ts: new Date().toISOString(), role, scenario,
                method: res.request().method(), url, status: res.status(),
            }) + '\n');
        }
    });

    await page.goto(`${PWA_BASE}/#/login`, { waitUntil: 'networkidle2' });
    await page.evaluate(() => { localStorage.clear(); sessionStorage.clear(); });
    await page.reload({ waitUntil: 'networkidle2' });
    await loginThroughRealForm(page);

    const journeyList = (JOURNEYS[role] || []).filter(
        (j) => !JOURNEY_FILTER || JOURNEY_FILTER.has(j.name)
    );
    for (const journey of journeyList) {
        const journeyErrorCountBefore = countJsonlLines(consoleLogPath, (l) => l.type === 'error');
        const apiCallsBefore = readJsonl(networkLogPath).length;

        try {
            if (fixtureChild.exitCode !== null) {
                throw new Error(`fixture server exited (code ${fixtureChild.exitCode}) before journey ${journey.name}; evidence INCOMPLETE`);
            }
            await page.evaluate((hash) => { window.location.hash = hash; }, journey.hash);
            await waitSettled(page);
            if (journey.clickText) {
                await clickByVisibleText(page, journey.clickText);
                await waitSettled(page);
            }
            const readiness = await waitForReady(page, journey);

            // Desktop screenshot
            await page.setViewport({ width: 1280, height: 900 });
            await waitSettled(page);
            await page.screenshot({ path: path.join(outDir, `${journey.name}.png`), fullPage: true });

            // Mobile screenshot
            await page.setViewport({ width: 390, height: 844 });
            await waitSettled(page);
            await page.screenshot({ path: path.join(outDir, `${journey.name}.mobile.png`), fullPage: true });
            await page.setViewport({ width: 1280, height: 900 });

            await waitSettled(page);
            const bodyText = await page.evaluate(() => document.body.innerText);
            writeFileSync(path.join(outDir, `${journey.name}.text.txt`), bodyText);

            const errorsAfter = countJsonlLines(consoleLogPath, (l) => l.type === 'error');
            const apiAfter = readJsonl(networkLogPath);
            const nonTwoXx = apiAfter.slice(apiCallsBefore).filter((r) => r.status >= 300).map((r) => r.url);

            summary.journeys[journey.name] = {
                crashed: detectCrash(bodyText),
                console_errors: errorsAfter - journeyErrorCountBefore,
                api_non_2xx: nonTwoXx,
                allow_crash: !!journey.allowCrash,
                ready: readiness.ready,
                ready_wait_ms: readiness.ready_wait_ms,
                label_checks: evaluateChecks(journey, scenario, bodyText),
                evidence: readiness.ready === 'timeout' ? 'INCOMPLETE (ready condition not met within 10 s)' : 'complete',
            };
        } catch (err) {
            summary.journeys[journey.name] = {
                crashed: null,
                console_errors: null,
                api_non_2xx: null,
                runner_error: String(err && err.message || err),
            };
        }
    }

    await page.close();
    summary.finished_at = new Date().toISOString();
    writeFileSync(path.join(outDir, 'summary.json'), JSON.stringify(summary, null, 2));
    return summary;
}

function readJsonl(p) {
    if (!existsSync(p)) return [];
    return readFileSync(p, 'utf-8')
        .split('\n')
        .filter(Boolean)
        .map((l) => { try { return JSON.parse(l); } catch { return null; } })
        .filter(Boolean);
}
function countJsonlLines(p, predicate) {
    return readJsonl(p).filter(predicate).length;
}

// ── Entry point ───────────────────────────────────────────────────────

async function main() {
    let puppeteer;
    try {
        puppeteer = await import('puppeteer-core');
    } catch (err) {
        console.error(
            'puppeteer-core is not installed. Run `npm ci` in tests/browser/ first.\n' + err
        );
        process.exit(1);
    }

    const executablePath = resolveBrowserExecutable();
    mkdirSync(OUT_ROOT, { recursive: true });

    const results = [];
    for (const role of ROLES) {
        for (const scenario of SCENARIOS) {
            console.log(`\n=== role=${role} scenario=${scenario} ===`);
            await assertPortsFree([FIXTURE_PORT, PWA_PORT]);
            const fixtureChild = startFixtureServer(scenario, role);
            const pwaChild = startPwaDevServer();
            let browserInstance = null;
            try {
                const fixtureUp = await waitForHttp(`${FIXTURE_BASE}/api/v1/auth/verify`, { timeoutMs: 15000 });
                if (!fixtureUp) throw new Error('fixture server did not come up on 8001');
                const pwaUp = await waitForHttp(PWA_BASE, { timeoutMs: 60000 });
                if (!pwaUp) throw new Error('pwa dev server did not come up on 5174');

                browserInstance = await puppeteer.default.launch({
                    executablePath,
                    headless: true,
                    userDataDir: mkdtempSync(path.join(tmpdir(), 'grid-evidence-profile-')),
                    args: ['--no-first-run', '--disable-extensions'],
                });

                const outDir = path.join(OUT_ROOT, `${role}-${scenario}`);
                const summary = await runOne(browserInstance, role, scenario, outDir, fixtureChild);
                results.push(summary);
            } catch (err) {
                console.error(`role=${role} scenario=${scenario} failed: ${err}`);
                results.push({ role, scenario, error: String(err && err.message || err) });
            } finally {
                if (browserInstance) await browserInstance.close().catch(() => {});
                killTree(fixtureChild);
                killTree(pwaChild);
                await waitPortsReleased([FIXTURE_PORT, PWA_PORT]);
            }
        }
    }

    writeFileSync(path.join(OUT_ROOT, 'run_index.json'), JSON.stringify({
        tree_label: TREE_LABEL,
        pwa_dir: PWA_DIR,
        generated_at: new Date().toISOString(),
        results,
    }, null, 2));

    console.log(`\nDone. Evidence written under ${OUT_ROOT}`);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
