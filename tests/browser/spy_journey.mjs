// Exercises the actual app navigation/buttons. Never follows the external
// full-Gamma-Watch link, activates collectors, or refreshes recommendations.
export async function runSpyJourney(page, scenario, capture) {
    const checks = [];
    const body = () => page.evaluate(() => document.body.innerText);
    async function check(name, predicate) {
        const text = await body();
        const passed = predicate(text);
        checks.push({ name, passed });
        if (!passed) throw new Error(`M1 assertion failed: ${name}`);
    }
    async function clickButton(text) {
        const found = await page.evaluate(needle => {
            const b = [...document.querySelectorAll('button')].find(b => b.innerText.trim() === needle);
            if (!b) return false;
            b.click(); return true;
        }, text);
        if (!found) throw new Error(`Real navigation button absent: ${text}`);
    }
    const waitText = text => page.waitForFunction(needle => document.body.innerText.includes(needle), { timeout: 12000 }, text);
    try {
        await page.click('button[aria-label="More views"]');
        await page.waitForFunction(() => [...document.querySelectorAll('div')].some(b => b.children.length === 0 && b.innerText.trim() === 'Options'));
        const navigated = await page.evaluate(() => {
            // NavBar's drawer uses a clickable div around icon + label/desc.
            const b = [...document.querySelectorAll('div')].find(b => b.children.length === 0 && b.innerText.trim() === 'Options');
            if (!b) return false;
            b.parentElement.parentElement.click(); return true;
        });
        if (!navigated) throw new Error('Options drawer item absent');
        await waitText('Options & Watchlist');
        await page.waitForFunction(() => location.hash === '#/options');
        await waitText('No options signals available');
        await check('real drawer navigation reaches Options and renders empty signals', t => t.includes('Options & Watchlist'));
        await capture('options-empty');
        await clickButton('Dealer Flow');
        await waitText('SPY');
        await page.waitForFunction(() => !document.body.innerText.includes('Loading GEX profile'));
        await check('SPY dealer flow resolves without crashing', t => t.includes('SPY') && !t.includes('An unexpected error'));
        if (scenario === 'healthy') await check('modeled SPY provenance is visible', t => t.includes('MODELED GEX PROFILE') && t.includes('synthetic-fixture') && t.includes('Underlying quote times do not establish option-chain or open-interest freshness.'));
        else if (scenario === 'partial') await check('stale dealer profile is withheld', t => t.includes('Stale GEX profile unavailable'));
        else await check('missing completed capture is explicit', t => t.includes('synthetic: no completed SPY options-chain capture'));
        await capture('spy-dealer-flow');
        await clickButton('Gamma Watch');
        await waitText('Connecting to Gamma Watch');
        await capture('gamma-loading');
        checks.push({ name: 'Gamma Watch exposes loading before response', passed: true });
        await page.waitForFunction(() => !document.body.innerText.includes('Connecting to Gamma Watch'), { timeout: 12000 });
        if (scenario === 'unavailable') {
            await check('collector failure is explicit without cached substitute', t => t.includes('Collector unavailable; no cached substitute.') && !t.includes('SPY 600'));
        } else if (scenario === 'error') {
            await check('transport error keeps collector status unknown', t => t.includes('Request failed; collector status unknown.') && !t.includes('SPY 600'));
        } else {
            await check('connection clock and provider clocks remain distinct', t => t.includes('GRID connection recent') && t.includes('Provider as-of') && t.includes('site receipt'));
            await check('experimental assumptions and private capability exclusions are visible', t => t.includes('Dealer inventory is assumed, not observed.') && t.includes('Private RTD, current GEX, journal and contracts unavailable'));
            if (scenario === 'healthy') {
                await check('known public and delayed values render with source status', t => t.includes('SPY 600') && t.includes('unverified-public-latency') && t.includes('put wall 590') && t.includes('unverified-delayed'));
                await check('measured zero and missing value differ', t => t.includes('flip 0') && t.includes('max pain Unavailable'));
                await check('frozen research and hypothetical demand are separate', t => t.includes('frozen-research') && t.includes('hypothetical demand') && t.includes('SPY 0%, AGG Unavailable'));
            } else if (scenario === 'partial') {
                await check('stale provider values are withheld despite recent transport', t => t.includes('SPY Unavailable') && t.includes('stale') && t.includes('put wall Unavailable') && !t.includes('SPY 600'));
            } else if (scenario === 'empty') {
                await check('empty source values remain unavailable', t => t.includes('SPY Unavailable') && t.includes('put wall Unavailable') && t.includes('0 points') && !t.includes('SPY 600'));
            }
        }
        await capture('gamma-result');
        return checks;
    } catch (error) {
        error.checks = checks;
        throw error;
    }
}
