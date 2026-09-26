#!/usr/bin/env node
/**
 * dbproof browser wrapper -- drives the real API (uvicorn on :8010) through
 * the integration worktree's PWA dev server (vite on :5175, proxying to
 * :8010) using system Chrome via puppeteer-core.
 *
 * Reuses the shape of tests/browser/run_evidence.mjs's helpers
 * (resolveBrowserExecutable, loginThroughRealForm, waitSettled,
 * waitForReady, detectCrash, JOURNEYS.admin) from
 * C:/Users/owner/dev/GRID-fable-wt-browser-acceptance/tests/browser/run_evidence.mjs
 * -- that file is plain top-level functions (no `export`), so it cannot be
 * imported cross-file without editing it; this wrapper reimplements the
 * same logic rather than modifying that other worktree's source.
 *
 * Master password is read from process.env.DBPROOF_MASTER_PW (sourced by
 * the caller from the local, never-printed secrets file) and is never
 * logged, screenshotted deliberately, or written to any output file here.
 */
import puppeteer from 'file:///C:/Users/owner/dev/GRID-fable-wt-browser-acceptance/tests/browser/node_modules/puppeteer-core/lib/esm/puppeteer/puppeteer-core.js';
import { mkdirSync, writeFileSync, appendFileSync, existsSync } from 'node:fs';
import path from 'node:path';

const OUT = 'C:/Users/owner/AppData/Local/Temp/claude/C--Users-owner/97c0cc34-7976-464f-bc96-cae538a74a3d/scratchpad/dbproof/evidence-real-api';
mkdirSync(OUT, { recursive: true });

const BASE = 'http://localhost:5175';
const MASTER_PW = process.env.DBPROOF_MASTER_PW;
if (!MASTER_PW) {
    console.error('DBPROOF_MASTER_PW not set in environment -- refusing to continue.');
    process.exit(1);
}

const CHROME_PATH = 'C:/Program Files/Google/Chrome/Application/chrome.exe';

function detectCrash(bodyText) {
    return /\bError\b[\s\S]{0,200}\bRetry\b/.test(bodyText);
}

async function waitSettled(page) {
    await page.waitForNetworkIdle({ idleTime: 800, timeout: 8000 }).catch(() => {});
}

async function waitForReady(page, ready, timeoutMs = 10000) {
    if (!ready) return { ready: 'none', ready_wait_ms: 0 };
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        const text = await page.evaluate(() => document.body.innerText).catch(() => '');
        if (ready.test(text)) return { ready: 'ok', ready_wait_ms: Date.now() - start };
        await new Promise((r) => setTimeout(r, 250));
    }
    return { ready: 'timeout', ready_wait_ms: Date.now() - start };
}

async function loginThroughRealForm(page, password) {
    await page.waitForSelector('input[placeholder="Password"]', { timeout: 15000 });
    await page.type('input[placeholder="Password"]', password);
    await page.click('button[type="submit"]');
    await page.waitForFunction(
        () => !document.querySelector('input[placeholder="Password"]'),
        { timeout: 15000 }
    ).catch(() => {});
}

const JOURNEYS = [
    { name: 'home', hash: '#/home', ready: /Your read|Start over|Here's (how|what)/ },
    { name: 'ticker-lookup', hash: '#/ticker-lookup', ready: /GOLD VERDICT/ },
    { name: 'watchlist-analysis', hash: '#/watchlist/TEST1', ready: /INSIDER EDGE|AI OVERVIEW/ },
    { name: 'godview', hash: '#/godview', ready: /God View . Institutional Pillars|UNAVAILABLE/ },
];

async function main() {
    const browser = await puppeteer.launch({
        executablePath: CHROME_PATH,
        headless: true,
        args: ['--no-sandbox', '--disable-dev-shm-usage'],
    });
    const summary = { base: BASE, journeys: {} };
    try {
        const page = await browser.newPage();
        await page.setViewport({ width: 1280, height: 900 });

        const consoleLines = [];
        const networkLines = [];
        page.on('console', (msg) => consoleLines.push(JSON.stringify({ t: Date.now(), type: msg.type(), text: msg.text() })));
        page.on('response', (res) => {
            try {
                networkLines.push(JSON.stringify({ t: Date.now(), url: res.url(), status: res.status() }));
            } catch {}
        });

        await page.goto(`${BASE}/#/home`, { waitUntil: 'domcontentloaded', timeout: 30000 });
        await loginThroughRealForm(page, MASTER_PW);
        await waitSettled(page);

        for (const j of JOURNEYS) {
            await page.evaluate((hash) => { window.location.hash = hash; }, j.hash);
            await new Promise((r) => setTimeout(r, 400));
            await waitSettled(page);
            const readyResult = await waitForReady(page, j.ready);
            await waitSettled(page);
            const bodyText = await page.evaluate(() => document.body.innerText).catch(() => '');
            const crashed = detectCrash(bodyText);
            await page.screenshot({ path: path.join(OUT, `${j.name}.png`) });
            writeFileSync(path.join(OUT, `${j.name}.txt`), bodyText, 'utf-8');
            summary.journeys[j.name] = { ...readyResult, crashed, text_len: bodyText.length };
        }

        writeFileSync(path.join(OUT, 'console.jsonl'), consoleLines.join('\n'), 'utf-8');
        writeFileSync(path.join(OUT, 'network.jsonl'), networkLines.join('\n'), 'utf-8');
        writeFileSync(path.join(OUT, 'summary.json'), JSON.stringify(summary, null, 2), 'utf-8');
        console.log('BROWSER_WRAPPER_OK');
        console.log(JSON.stringify(summary, null, 2));
    } finally {
        await browser.close();
    }
}

main().catch((e) => {
    console.error('BROWSER_WRAPPER_FAILED', e && e.stack || e);
    process.exit(1);
});
