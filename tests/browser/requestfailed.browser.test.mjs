import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
test('M1-REVIEW-1: failed Signals connection rejects otherwise passing SPY browser journey', { timeout: 120000 }, async t => {
    const out = process.env.GRID_GATE_TEST_OUT || mkdtempSync(path.join(tmpdir(), 'grid-m1-requestfailed-'));
    const result = await new Promise((resolve, reject) => {
        const child = spawn(process.execPath, [path.join(here, 'fixture_api/abort_signals.mjs'),
            '--tree-label', 'M1-REVIEW-1-connection-failed', '--roles', 'admin', '--scenarios', 'healthy',
            '--journeys', 'spy-options', '--out', out], { cwd: path.resolve(here, '../..') });
        let stdout = '', stderr = '';
        child.stdout.on('data', b => { stdout += b; });
        child.stderr.on('data', b => { stderr += b; });
        child.on('error', reject);
        child.on('exit', code => resolve({ code, stdout, stderr }));
    });
    const run = readdirSync(out).filter(name => name.startsWith('run-')).sort().at(-1);
    const receipt = path.join(out, run, 'admin-healthy/summary.json');
    t.diagnostic(`Negative-control receipt: ${receipt}`);
    const summary = JSON.parse(readFileSync(receipt, 'utf8'));
    // The UI's empty fallback still satisfies its original assertions. The
    // transport gate, rather than an incidental crash/assertion, must reject it.
    assert.equal(summary.journeys['spy-options'].checks.length, 9);
    assert.ok(summary.journeys['spy-options'].checks.every(c => c.passed));
    assert.equal(summary.source_unchanged, true);
    assert.equal(summary.spy_network_gate_passed, false);
    assert.ok(summary.unexpected_request_failures.some(r =>
        new URL(r.url).pathname === '/api/v1/options/signals' &&
        r.error_text === 'net::ERR_CONNECTION_FAILED' && ['bootstrap', 'spy-options'].includes(r.journey)));
    assert.equal(summary.api_failures.filter(r => r.classification === 'fixture_preload_unavailable').length, 14);
    assert.equal(result.code, 1, result.stderr || result.stdout);
});
