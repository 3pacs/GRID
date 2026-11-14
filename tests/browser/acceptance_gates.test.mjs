import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { classifyRequestFailures, hasCompleteResultMatrix } from './harness_config.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, '../..');
test('CLI refuses empty or unsupported scenario/role matrices before writing evidence', () => {
    const root = mkdtempSync(path.join(tmpdir(), 'grid-m1-cli-gate-'));
    const cases = [
        ['--scenarios', ','], ['--scenarios', ''], ['--scenarios', 'healthy,typo'],
        ['--roles', ','], ['--roles', ''], ['--roles', 'admin,typo'],
    ];
    for (const [flag, value] of cases) {
        const out = path.join(root, `${flag.slice(2)}-${cases.findIndex(c => c[0] === flag && c[1] === value)}`);
        const result = spawnSync(process.execPath, [path.join(here, 'run_evidence.mjs'),
            '--tree-label', 'invalid-matrix', '--roles', 'admin', '--journeys', 'spy-options',
            '--out', out, flag, value], { cwd: repo, encoding: 'utf8', timeout: 10000 });
        assert.equal(result.error, undefined, `${flag}=${value}: ${result.error}`);
        assert.notEqual(result.status, 0, `${flag}=${value}: zero-test matrix passed`);
        assert.match(result.stderr, /--(?:roles|scenarios)/);
        assert.equal(existsSync(out), false, 'invalid input must fail before output/server startup');
    }
});

test('result matrix cannot accept zero, missing, duplicate or unsupported pairs', () => {
    const roles = ['admin'], scenarios = ['healthy', 'partial'];
    assert.equal(hasCompleteResultMatrix([], roles, scenarios), false);
    assert.equal(hasCompleteResultMatrix([], [], []), false);
    assert.equal(hasCompleteResultMatrix([{ role: 'admin', scenario: 'healthy' }], roles, scenarios), false);
    assert.equal(hasCompleteResultMatrix([{ role: 'admin', scenario: 'healthy' }, { role: 'admin', scenario: 'healthy' }], roles, scenarios), false);
    assert.equal(hasCompleteResultMatrix([{ role: 'admin', scenario: 'healthy' }, { role: 'admin', scenario: 'typo' }], roles, scenarios), false);
    assert.equal(hasCompleteResultMatrix(scenarios.map(scenario => ({ role: 'admin', scenario })), roles, scenarios), true);
});

test('request failure exceptions are limited to explicit teardown or Gamma unmount ERR_ABORTED', () => {
    const signal = { url: 'http://127.0.0.1:5174/api/v1/options/signals?limit=50',
        method: 'GET', journey: 'spy-options', active_journey: 'spy-options', error_text: 'net::ERR_CONNECTION_FAILED', teardown: false };
    const classify = r => classifyRequestFailures([r])[0].classification;
    assert.equal(classify(signal), 'unexpected_request_failure');
    assert.equal(classify({ ...signal, teardown: true }), 'unexpected_request_failure');
    assert.equal(classify({ ...signal, url: 'http://127.0.0.1:5174/api/v1/signals' }), 'unexpected_request_failure');
    assert.equal(classify({ ...signal, error_text: 'net::ERR_ABORTED' }), 'unexpected_request_failure');
    assert.equal(classify({ ...signal, error_text: 'net::ERR_ABORTED', teardown: true }), 'expected_browser_teardown_abort');
    const gamma = { ...signal, url: 'http://127.0.0.1:5174/api/v1/gamma-watch/state', error_text: 'net::ERR_ABORTED' };
    assert.equal(classify(gamma), 'unexpected_request_failure');
    assert.equal(classify({ ...gamma, active_journey: 'home' }), 'expected_gamma_unmount_abort');
    assert.equal(classify({ ...gamma, active_journey: undefined }), 'unexpected_request_failure');
    assert.equal(classify({ ...gamma, method: 'POST', active_journey: 'home' }), 'unexpected_request_failure');
    assert.equal(classify({ ...signal, error_text: 'net::ERR_ABORTED', active_journey: 'home' }), 'unexpected_request_failure');
});
