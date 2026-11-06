#!/usr/bin/env node
/**
 * Evidence runner for the GRID browser-acceptance harness.
 *
 * Drives system Chrome (or Edge) via puppeteer-core against the fixture
 * server + a PWA dev server, and saves screenshots/text/console/network
 * logs as files under `--out`, because the browser tool available to the
 * lead cannot save images.
 *
 * Local synthetic source acceptance only; not production/deploy acceptance.
 * See M1-SPY-JOURNEY.md for the current scope and legacy receipt limitations.
 *
 *     cd tests/browser
 *     npm ci
 *     node run_evidence.mjs --tree-label <sha-or-label> [options]
 *
 * Usage:
 *   node run_evidence.mjs --tree-label <sha-or-label> \
 *     [--pwa-dir <path>] [--browser-executable <path>] \
 *     [--scenarios healthy,partial,empty,unavailable,error] \
 *     [--roles admin,contributor] [--journeys home,ticker-lookup,...] \
 *     [--out tests/browser/evidence/<tree-label>/]
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
import { resolveBrowserExecutable, sourceIdentity, classifyApiFailures } from './harness_config.mjs';
import { runSpyJourney } from './spy_journey.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '..', '..'); // this harness worktree root
const PRELOAD_COVERAGE = JSON.parse(readFileSync(path.join(__dirname, 'fixture_api', 'preload_coverage.json'), 'utf8'));
const OMITTED_PRELOAD_PATHS = new Set(PRELOAD_COVERAGE.map(row => row.path));

// ── CLI args ──────────────────────────────────────────────────────────

function parseArgs(argv) {
    const args = {
        'pwa-dir': path.join(REPO_ROOT, 'pwa'),
        'browser-executable': null,
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
const PWA_BASE = `http://127.0.0.1:${PWA_PORT}`;

// ── Browser executable ──────────────────────────────────────────────
// No browser download: point puppeteer-core at the system install.

// ── Journeys per role ─────────────────────────────────────────────────
// Hashes confirmed against pwa/src/routing.js / routes.js / app.jsx:
//   'watchlist' + segment -> view 'watchlist-analysis' (the #/watchlist/TEST1 shortcut)
//   DAD_VIEWS (contributor / "dad mode") = {'home', 'ten-year', 'ticker-lookup'}
//   — catalyst-timeline is NOT a dad view, so visiting it as contributor is
//   expected to either redirect or crash; record whichever happens, don't
//   "fix" the attempt.

const JOURNEYS = {
    admin: [
        { name: 'spy-options', hash: '#/ten-year', ready: /10-Year|TOP CHART|No eligible/, run: runSpyJourney },
        { name: 'home', hash: '#/home', clickText: 'How are my stocks doing?', ready: /Your read|Start over|Here's (how|what)/ },
        { name: 'ticker-lookup', hash: '#/ticker-lookup', ready: /GOLD VERDICT/ },
        { name: 'watchlist-analysis', hash: '#/watchlist/TEST1', ready: /INSIDER EDGE|AI OVERVIEW/ },
        { name: 'portfolio', hash: '#/portfolio', ready: /Portfolio Value/ },
        { name: 'operator', hash: '#/operator', ready: /HERMES STATUS/ },
        { name: 'discovery', hash: '#/discovery', ready: /HYPOTHESES|RESEARCH RUN|No research run recorded|Research status unavailable/ },
        { name: 'pipeline-health', hash: '#/pipeline-health', ready: /PIPELINE HEALTH|Pipeline health unavailable/ },
        { name: 'ten-year', hash: '#/ten-year', ready: /10-Year|TOP CHART|No eligible/ },
        { name: 'snapshots', hash: '#/snapshots', ready: /LATEST SNAPSHOT|No snapshots|Snapshots unavailable|unexpected response/ },
        { name: 'god-view', hash: '#/god-view', ready: /Fed net liquidity|UNAVAILABLE|God View/ },
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
            child.kill('SIGKILL');
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
async function assertPortsFree(ports) {
    const net = await import('node:net');
    for (const port of ports) {
        const busy = await new Promise((resolve) => {
            const srv = net.createServer();
            srv.once('error', () => resolve(true));
            srv.once('listening', () => srv.close(() => resolve(false)));
            srv.listen(port, '127.0.0.1');
        });
        if (busy) throw new Error(`port ${port} is still in use; refusing to start this pair (stale server from a previous run?)`);
    }
}

function startFixtureServer(scenario, role) {
    const serverPath = path.join(__dirname, 'fixture_api', 'server.py');
    const child = spawn(
        'python',
        [serverPath, '--port', String(FIXTURE_PORT), '--scenario', scenario],
        {
            cwd: REPO_ROOT,
            env: { ...process.env, FIXTURE_ROLE: role },
            stdio: ['ignore', 'pipe', 'pipe'],
        }
    );
    return child;
}

function startPwaDevServer() {
    const child = spawn(
        process.execPath,
        [path.join(PWA_DIR, 'node_modules', 'vite', 'bin', 'vite.js'), '--host', '127.0.0.1', '--port', String(PWA_PORT), '--strictPort'],
        {
            cwd: PWA_DIR,
            env: {
                ...process.env,
                GRID_API_PROXY_TARGET: FIXTURE_BASE,
                GRID_WS_PROXY_TARGET: `ws://127.0.0.1:${FIXTURE_PORT}`,
            },
            stdio: ['ignore', 'pipe', 'pipe'],
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
        mode: 'local synthetic fixtures / Vite development modules; no deployment identity',
        source_before: sourceIdentity(REPO_ROOT, ['tests/browser']),
        pwa_source_before: sourceIdentity(PWA_DIR, ['src']),
        journeys: {},
    };

    const userDataDir = mkdtempSync(path.join(tmpdir(), `grid-evidence-${role}-${scenario}-`));
    const page = await browser.newPage();
    // Capture actual responses rather than service-worker-owned fetches. This
    // is a source journey, not a service-worker/offline-cache qualification.
    await page.setBypassServiceWorker(true);
    await page.setViewport({ width: 1280, height: 900 });
    // Fail closed on external requests, including links/providers accidentally
    // introduced by a view. Only the two runner-owned loopback servers are allowed.
    const blockedRequests = [];
    let activeJourney = 'bootstrap';
    const requestJourneys = new WeakMap();
    await page.setRequestInterception(true);
    page.on('request', request => {
        requestJourneys.set(request, activeJourney);
        const url = new URL(request.url());
        if (['data:', 'blob:', 'about:'].includes(url.protocol) ||
            [PWA_BASE, FIXTURE_BASE].includes(url.origin)) request.continue();
        else { blockedRequests.push(request.url()); request.abort(); }
    });
    const moduleResponses = [];
    const fixtureResponses = [];

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
        if (url.startsWith(PWA_BASE) && /\/(src|@vite)\//.test(url)) {
            moduleResponses.push(res.buffer().then(bytes => {
                return import('node:crypto').then(({ createHash }) => ({ url, status: res.status(), sha256: createHash('sha256').update(bytes).digest('hex') }));
            }).catch(() => ({ url, error: 'body unavailable' })));
        }
        if (url.includes('/api/')) {
            if (res.status() >= 300 || /\/gamma-watch\/state|\/derivatives\/(?:gex|vanna-charm|flow-timeline)\/SPY/.test(url)) {
                const journey = requestJourneys.get(res.request()) || 'bootstrap';
                fixtureResponses.push(res.json().then(body => ({ url, status: res.status(), journey, body })).catch(() => ({ url, status: res.status(), journey, error: 'body unavailable' })));
            }
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
        activeJourney = journey.name;
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
            if (readiness.ready === 'timeout' && journey.run) throw new Error('Initial view not ready');
            const capture = async name => {
                await page.screenshot({ path: path.join(outDir, `${journey.name}.${name}.png`), fullPage: true });
                writeFileSync(path.join(outDir, `${journey.name}.${name}.text.txt`), await page.evaluate(() => document.body.innerText));
            };
            const checks = journey.run ? await journey.run(page, scenario, capture) : [];

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
                checks,
                final_url: page.url(),
                ready: readiness.ready,
                ready_wait_ms: readiness.ready_wait_ms,
                evidence: readiness.ready === 'timeout' ? 'INCOMPLETE (ready condition not met within 10 s)' : 'complete',
            };
        } catch (err) {
            summary.journeys[journey.name] = {
                crashed: null,
                console_errors: null,
                api_non_2xx: null,
                runner_error: String(err && err.message || err),
                checks: err.checks || [],
            };
            await page.screenshot({ path: path.join(outDir, `${journey.name}.failure.png`), fullPage: true }).catch(() => {});
            writeFileSync(path.join(outDir, `${journey.name}.failure.text.txt`), await page.evaluate(() => document.body.innerText).catch(() => ''));
        }
    }

    summary.served_modules = await Promise.all(moduleResponses);
    summary.fixture_responses = await Promise.all(fixtureResponses);
    summary.api_failures = classifyApiFailures(summary.fixture_responses, scenario, OMITTED_PRELOAD_PATHS);
    summary.unexpected_api_failures = summary.api_failures.filter(r => r.classification === 'unexpected_api_failure');
    summary.preload_fixture_coverage = PRELOAD_COVERAGE;
    summary.spy_network_gate_passed = !summary.journeys['spy-options'] ||
        !summary.unexpected_api_failures.some(r => ['bootstrap', 'spy-options'].includes(r.journey));
    summary.transport_evidence_complete = !summary.journeys['spy-options'] ||
        summary.fixture_responses.some(r => r.url.endsWith('/gamma-watch/state') && r.body);
    summary.blocked_requests = blockedRequests;
    summary.source_after = sourceIdentity(REPO_ROOT, ['tests/browser']);
    summary.pwa_source_after = sourceIdentity(PWA_DIR, ['src']);
    summary.source_unchanged = summary.source_before.sha256 === summary.source_after.sha256 && summary.pwa_source_before.sha256 === summary.pwa_source_after.sha256;
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

    const executablePath = resolveBrowserExecutable(args['browser-executable']);
    for (const role of ROLES) {
        if (!JOURNEYS[role]) throw new Error(`Unknown role: ${role}`);
        if (JOURNEY_FILTER && !JOURNEYS[role].some(j => JOURNEY_FILTER.has(j.name))) throw new Error(`No requested journey for ${role}`);
    }
    mkdirSync(OUT_ROOT, { recursive: true });

    const results = [];
    for (const role of ROLES) {
        for (const scenario of SCENARIOS) {
            console.log(`\n=== role=${role} scenario=${scenario} ===`);
            await assertPortsFree([FIXTURE_PORT, PWA_PORT]);
            const fixtureChild = startFixtureServer(scenario, role);
            const pwaChild = startPwaDevServer();
            const serverLogDir = path.join(OUT_ROOT, `${role}-${scenario}`);
            mkdirSync(serverLogDir, { recursive: true });
            for (const [name, child] of [['fixture', fixtureChild], ['vite', pwaChild]]) {
                for (const stream of ['stdout', 'stderr']) child[stream].on('data', chunk => appendFileSync(path.join(serverLogDir, `${name}.log`), chunk));
                child.on('error', error => appendFileSync(path.join(serverLogDir, `${name}.log`), String(error)));
            }
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
    if (results.some(r => r.error || !r.source_unchanged || !r.transport_evidence_complete || !r.spy_network_gate_passed || !Object.keys(r.journeys || {}).length || Object.values(r.journeys || {}).some(j => j.runner_error || j.crashed || j.ready === 'timeout' || j.checks?.some(c => !c.passed)))) {
        process.exitCode = 1;
    }
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
