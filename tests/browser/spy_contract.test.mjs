import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { adaptGammaWatch } from '../../pwa/src/components/gammaWatchPublicAdapter.js';
import { resolveBrowserExecutable, sourceIdentity } from './harness_config.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const now = 1793908800;
const fixtures = spawnSync(process.env.PYTHON || 'python', ['-c',
    'import json,spy; print(json.dumps({s:spy.gamma_state(s,1793908800) for s in ["healthy","partial","empty","unavailable","error"]}))'],
    { cwd: path.join(here, 'fixture_api'), encoding: 'utf8' });
assert.equal(fixtures.status, 0, fixtures.stderr);
const cases = JSON.parse(fixtures.stdout);

test('the HTTP fixtures satisfy the real public adapter, preserving zero/null and time kinds', () => {
    const [envelope, status] = cases.healthy;
    assert.equal(status, 200);
    const v = adaptGammaWatch(envelope, now * 1000);
    assert.equal(v.valid, true);
    assert.equal(v.price.price, 600);
    assert.equal(v.delayed.levels.gamma_flip, 0);
    assert.equal(v.delayed.levels.max_pain, null);
    assert.equal(v.research.status, 'frozen-research');
    assert.equal(v.rebalance.rows[0].equity_return, 0);
    assert.equal(v.rebalance.rows[0].bond_return, null);
    assert.notEqual(v.price.sourceTime, v.price.receiptTime);
    assert.notEqual(v.research.chainAsOf, v.research.computedAt);
});
test('fresh bridge clock does not refresh stale provider inputs', () => {
    const v = adaptGammaWatch(cases.partial[0], now * 1000);
    assert.equal(v.valid, true);
    assert.equal(v.price.status, 'stale');
    assert.equal(v.price.price, null);
    assert.equal(v.delayed.status, 'stale');
    assert.equal(v.delayed.levels.put_wall, null);
    assert.equal(v.research.status, 'frozen-research');
});
test('empty, collector unavailable, and transport error stay separate', () => {
    const v = adaptGammaWatch(cases.empty[0], now * 1000);
    assert.equal(v.valid, true);
    assert.equal(v.price.price, null);
    assert.equal(v.research.grid.length, 0);
    assert.equal(cases.unavailable[1], 503);
    assert.equal(cases.unavailable[0].status, 'unavailable');
    assert.equal(cases.error[1], 502);
    assert.equal(cases.error[0].status, undefined);
});
test('fixture values stop rendering after the real adapter display clock expires', () => {
    const v = adaptGammaWatch(cases.healthy[0], (now + 16) * 1000);
    assert.equal(v.valid, false);
    assert.equal(v.price.price, null);
    assert.equal(v.delayed.levels.put_wall, null);
});
test('portable browser override fails closed and source manifest includes fixture bytes', () => {
    const dir = mkdtempSync(path.join(tmpdir(), 'grid-m1-config-'));
    try {
        const executable = path.join(dir, 'browser');
        writeFileSync(executable, 'fixture');
        assert.equal(resolveBrowserExecutable(executable), executable);
        assert.equal(resolveBrowserExecutable(null, { GRID_BROWSER_EXECUTABLE: executable }, 'linux'), executable);
        assert.throws(() => resolveBrowserExecutable(path.join(dir, 'missing')), /not found/);
        const root = path.resolve(here, '../..');
        const identity = sourceIdentity(root, ['tests/browser/fixture_api']);
        assert.match(identity.commit, /^[a-f0-9]{40}$/);
        assert.match(identity.files['tests/browser/fixture_api/spy.py'], /^[a-f0-9]{64}$/);
    } finally { rmSync(dir, { recursive: true }); }
});
