/**
 * Step scripts for probe_production.mjs journeys that need more than one hash
 * navigation. Capture-only: steps click in-page tab buttons and save what the
 * page shows; they never assert, click Refresh, follow external links, or
 * trigger any non-GET request (probe_production.mjs's request guard aborts
 * those regardless).
 */

// Capture-only steps for the spy-options journey. Each step is attempted in
// order; a missing button or a slow view is recorded as a step note, never an
// assertion (the operator judges the captures by eye).
export async function spyOptionsSteps(page, capture, gate = async () => {}) {
    const notes = [];
    const clickTab = async (label) => {
        await gate();
        const found = await page.evaluate((needle) => {
            const b = [...document.querySelectorAll('button')].find((x) => x.innerText.trim() === needle);
            if (!b) return false;
            b.click();
            return true;
        }, label);
        if (!found) throw new Error(`tab button absent: ${label}`);
    };
    const waitGone = (needle, ms) => page
        .waitForFunction((n) => !document.body.innerText.includes(n), { timeout: ms }, needle)
        .catch(() => notes.push(`still showing "${needle}" after ${ms / 1000}s`));
    try {
        await clickTab('Dealer Flow');
        await page.waitForNetworkIdle({ idleTime: 800, timeout: 10000 }).catch(() => {});
        await waitGone('Loading GEX profile', 15000);
        await capture('spy-dealer-flow');
    } catch (err) {
        notes.push(`dealer flow: ${String(err && err.message || err)}`);
    }
    try {
        await clickTab('Gamma Watch');
        await page.waitForNetworkIdle({ idleTime: 800, timeout: 10000 }).catch(() => {});
        await waitGone('Connecting to Gamma Watch', 15000);
        await capture('gamma-result');
    } catch (err) {
        notes.push(`gamma watch: ${String(err && err.message || err)}`);
    }
    return notes;
}

