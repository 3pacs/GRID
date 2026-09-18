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
 *     [--roles admin,contributor] [--out tests/browser/evidence/<tree-label>/]
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
        'pwa-dir': 'C:/Users/owner/dev/GRID-fable-wt-composed-g/pwa',
        scenarios: 'healthy,partial,empty',
        roles: 'admin,contributor',
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
const PWA_DIR = args['pwa-dir'];
const TREE_LABEL = args['tree-label'];
const SCENARIOS = args.scenarios.split(',').map((s) => s.trim()).filter(Boolean);
const ROLES = args.roles.split(',').map((s) => s.trim()).filter(Boolean);
const OUT_ROOT = path.resolve(args.out);

const FIXTURE_PORT = 8001;
const PWA_PORT = 5174;
const FIXTURE_BASE = `http://127.0.0.1:${FIXTURE_PORT}`;
const PWA_BASE = `http://localhost:${PWA_PORT}`;

// ── Browser executable ──────────────────────────────────────────────
// No browser download: point puppeteer-core at the system install.

const CHROME_PATH = 'C:/Program Files/Google/Chrome/Application/chrome.exe';
const EDGE_PATH = 'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe';

function resolveBrowserExecutable() {
    if (existsSync(CHROME_PATH)) return CHROME_PATH;
    if (existsSync(EDGE_PATH)) return EDGE_PATH;
    throw new Error(
        `Neither Chrome (${CHROME_PATH}) nor Edge (${EDGE_PATH}) found. ` +
        'This runner does not download a browser — install one of these or edit the paths.'
    );
}

// ── Journeys per role ─────────────────────────────────────────────────
// Hashes confirmed against pwa/src/routing.js / routes.js / app.jsx:
//   'watchlist' + segment -> view 'watchlist-analysis' (the #/watchlist/TEST1 shortcut)
//   DAD_VIEWS (contributor / "dad mode") = {'home', 'ten-year', 'ticker-lookup'}
//   — catalyst-timeline is NOT a dad view, so visiting it as contributor is
//   expected to either redirect or crash; record whichever happens, don't
//   "fix" the attempt.

const JOURNEYS = {
    admin: [
        { name: 'home', hash: '#/home', clickText: 'How are my stocks doing?' },
        { name: 'ticker-lookup', hash: '#/ticker-lookup' },
        { name: 'watchlist-analysis', hash: '#/watchlist/TEST1' },
        { name: 'portfolio', hash: '#/portfolio' },
        { name: 'operator', hash: '#/operator' },
        { name: 'discovery', hash: '#/discovery' },
        { name: 'pipeline-health', hash: '#/pipeline-health' },
        { name: 'ten-year', hash: '#/ten-year' },
    ],
    contributor: [
        { name: 'home', hash: '#/home', clickText: 'How are my stocks doing?' },
        { name: 'ticker-lookup', hash: '#/ticker-lookup' },
        { name: 'ten-year', hash: '#/ten-year' },
        { name: 'catalyst-timeline-ACME', hash: '#/catalyst-timeline?ticker=ACME', allowCrash: true },
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

function detectCrash(bodyText) {
    // ViewErrorBoundary.jsx renders "<ViewName> Error" as an <h3>, with
    // "An unexpected error occurred" (or the real error message) below it,
    // and a "Retry" button. Match on the combination rather than a single
    // generic word, since "error" alone appears in legitimate copy too.
    return /\bError\b[\s\S]{0,200}\bRetry\b/.test(bodyText);
}

// ── Main per-(role, scenario) run ────────────────────────────────────

async function runOne(browser, role, scenario, outDir) {
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

    for (const journey of JOURNEYS[role] || []) {
        const journeyErrorCountBefore = countJsonlLines(consoleLogPath, (l) => l.type === 'error');
        const apiCallsBefore = readJsonl(networkLogPath).length;

        try {
            await page.evaluate((hash) => { window.location.hash = hash; }, journey.hash);
            await page.waitForNetworkIdle({ idleTime: 750, timeout: 10000 }).catch(() => {});
            if (journey.clickText) {
                await clickByVisibleText(page, journey.clickText);
                await page.waitForNetworkIdle({ idleTime: 750, timeout: 10000 }).catch(() => {});
            }

            // Desktop screenshot
            await page.setViewport({ width: 1280, height: 900 });
            await page.screenshot({ path: path.join(outDir, `${journey.name}.png`), fullPage: true });

            // Mobile screenshot
            await page.setViewport({ width: 390, height: 844 });
            await page.screenshot({ path: path.join(outDir, `${journey.name}.mobile.png`), fullPage: true });
            await page.setViewport({ width: 1280, height: 900 });

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
                const summary = await runOne(browserInstance, role, scenario, outDir);
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
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
