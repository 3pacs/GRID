import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import path from 'node:path';

export function resolveBrowserExecutable(explicit, env = process.env, platform = process.platform) {
    const override = explicit || env.GRID_BROWSER_EXECUTABLE;
    if (override) {
        if (!existsSync(override)) throw new Error(`Browser executable not found: ${override}`);
        return override;
    }
    const candidates = platform === 'win32' ? [
        path.join(env.PROGRAMFILES || 'C:/Program Files', 'Google/Chrome/Application/chrome.exe'),
        path.join(env['PROGRAMFILES(X86)'] || 'C:/Program Files (x86)', 'Microsoft/Edge/Application/msedge.exe'),
        path.join(env.LOCALAPPDATA || '', 'Google/Chrome/Application/chrome.exe'),
    ] : platform === 'darwin' ? [
        '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
        '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
    ] : ['/usr/bin/google-chrome', '/usr/bin/google-chrome-stable', '/usr/bin/chromium', '/usr/bin/chromium-browser'];
    const found = candidates.find(p => existsSync(p));
    if (!found) throw new Error('No browser found. Set --browser-executable or GRID_BROWSER_EXECUTABLE; no browser is downloaded.');
    return found;
}

export function sourceIdentity(root, dirs) {
    const git = (...args) => {
        const r = spawnSync('git', args, { cwd: root, encoding: 'utf8' });
        if (r.status !== 0) throw new Error(`Cannot bind source identity: git ${args.join(' ')}`);
        return r.stdout.trim();
    };
    const files = {};
    function scan(rel) {
        for (const entry of readdirSync(path.join(root, rel), { withFileTypes: true })) {
            if (['node_modules', 'evidence', '__pycache__'].includes(entry.name)) continue;
            const name = path.posix.join(rel.replaceAll('\\', '/'), entry.name);
            if (entry.isDirectory()) scan(name);
            else if (entry.isFile()) files[name] = createHash('sha256').update(readFileSync(path.join(root, name))).digest('hex');
        }
    }
    dirs.forEach(scan);
    const sorted = Object.fromEntries(Object.entries(files).sort(([a], [b]) => a.localeCompare(b)));
    return { commit: git('rev-parse', 'HEAD'), dirty: git('status', '--porcelain', '--', ...dirs),
        sha256: createHash('sha256').update(JSON.stringify(sorted)).digest('hex'), files: sorted };
}

// Classification is deliberately narrow: an unknown path/status/marker fails
// the SPY network gate rather than disappearing behind a blanket error ignore.
export function classifyApiFailures(responses, scenario, omittedPaths) {
    return responses.filter(r => r.status >= 300).map(r => {
        const pathname = new URL(r.url).pathname;
        let classification = 'unexpected_api_failure';
        if (r.status === 501 && omittedPaths.has(pathname) && r.body?.error === true &&
            r.body?.code === 'fixture_preload_unavailable' && r.body?.path === pathname &&
            r.body?.fixture_scope === 'outside-spy-journey') classification = 'fixture_preload_unavailable';
        else if (pathname === '/api/v1/gamma-watch/state' && scenario === 'unavailable' &&
            r.status === 503 && r.body?.status === 'unavailable' && r.body?.data === null) classification = 'expected_gamma_collector_unavailable';
        else if (pathname === '/api/v1/gamma-watch/state' && scenario === 'error' &&
            r.status === 502 && r.body?.error === 'synthetic upstream transport failure') classification = 'expected_gamma_transport_error';
        return { ...r, classification };
    });
}
