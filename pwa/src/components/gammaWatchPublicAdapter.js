// Pure public display projection. Never forward provider diagnostics or private blocks.
const obj = x => x && typeof x === 'object' && !Array.isArray(x) ? x : {};
const list = (x, max) => Array.isArray(x) ? x.slice(0, max) : [];
const finite = x => typeof x === 'number' && Number.isFinite(x);
const num = x => finite(x) ? x : null;
const positive = x => finite(x) && x > 0 ? x : null;
const text = x => typeof x === 'string' ? x.slice(0, 160) : '';
const iso = x => {
    const parts = typeof x === 'string' && /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{3}|\d{6}))?(Z|([+-])(\d{2}):(\d{2}))$/.exec(x);
    if (!parts) return null;
    const [year, month, date, hour, minute, second] = parts.slice(1, 7).map(Number);
    const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
    const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    const offsetHour = Number(parts[10] || 0), offsetMinute = Number(parts[11] || 0);
    if (month < 1 || month > 12 || date < 1 || date > days[month - 1] || hour > 23 || minute > 59 || second > 59 || offsetHour > 23 || offsetMinute > 59) return null;
    const parsed = Date.parse(x);
    if (!finite(parsed)) return null;
    const offset = (parts[9] === '-' ? -1 : 1) * (offsetHour * 60 + offsetMinute);
    const local = new Date(parsed + offset * 60000);
    // Date.parse may normalize impossible dates; require the local components
    // to survive the explicit offset round trip without changing the source day.
    if (local.getUTCFullYear() !== year || local.getUTCMonth() + 1 !== month || local.getUTCDate() !== date || local.getUTCHours() !== hour || local.getUTCMinutes() !== minute || local.getUTCSeconds() !== second) return null;
    return parsed / 1000;
};
const day = x => typeof x === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(x) && finite(Date.parse(x)) && new Date(x).toISOString().slice(0, 10) === x ? x : null;
const within = (x, now, age) => finite(x) && x >= 0 && x <= now && now - x <= age;
const ordered = (source, receipt, now) => finite(source) && finite(receipt) && source >= 0 && source <= receipt && receipt <= now;
const status = x => ['failed', 'clock-invalid', 'stale', 'unavailable'].includes(x) ? x : 'unavailable';
const levels = ['call_wall', 'put_wall', 'gamma_flip', 'max_pain'];
const sectorSymbols = ['SPY', 'QQQ', 'IWM', 'XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLY', 'XLP', 'XLU', 'XLB', 'XLRE', 'XLC', 'SMH'];
const dates = ['operationDate', 'maturityDate', 'settlementDate', 'record_date', 'auctionDate', 'issueDate'];
const amounts = ['totalAmtAccepted', 'totalAmtSubmitted', 'open_today_bal', 'offeringAmount', 'highYield', 'bidToCoverRatio'];
const publicNumber = x => num(typeof x === 'string' && /^-?\d+(?:\.\d+)?$/.test(x) && x.length <= 128 ? Number(x) : x);
const captureClock = x => iso(x) ?? (typeof x === 'string' && /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}\+00$/.test(x) ? iso(x.slice(0, 10) + 'T' + x.slice(11) + ':00') : null);

/** @typedef {{status:string, sourceTime:number|null, receiptTime:number|null, price:number|null}} PriceContext */
/** @typedef {{valid:boolean, schema:string, servedAt:number|null, price:PriceContext, delayed:object, research:object, sectors:object, official:Array<object>, rebalance:object, live:object}} PublicView */

// Known unversioned collector shape only. Explicit unknown versions never fall back.
function legacyFrame(raw) {
    const q = obj(raw.quote), g = obj(raw.gex), m = obj(raw.model);
    const context = (value, source, kind, error, age) => ({
        status: error ? 'failed' : value.source === source ? kind : 'unavailable',
        source_time: iso(value.as_of), receipt_time: iso(value.received_at),
        values_available: !error && value.source === source, max_context_age_seconds: age,
        current_interpretation: false, source,
    });
    const official = obj(raw.structural_official), reb = obj(raw.structural_rebalance), sectors = obj(raw.sectors);
    return {
        schema: 'legacy', symbol: 'SPY', evaluation_clock: 'ok', served_at: iso(raw.served_at),
        public_price: { ...context(q, 'Yahoo chart · public, latency not guaranteed', 'unverified-public-latency', raw.quote_error, 120), price: q.price },
        delayed_gex: { ...context(g, 'ZeroGEX · delayed ~15 minutes', 'unverified-delayed', raw.gex_error, 1800), levels: g, spot: g.spot, provider_market_phase: g.market_phase },
        research_model: { ...m, status: !raw.model_error && m.live_inputs !== true ? 'frozen-research' : 'unavailable', chain_as_of: captureClock(m.chain_as_of), computed_at: iso(m.computed_at), current_interpretation: false },
        sectors: { status: !raw.sectors_error ? 'delayed-unqualified' : 'unavailable', computed_at: iso(sectors.computed_at), rows: list(sectors.rows, 15).map(obj).map(r => ({ ...r, status: !r.error && r.source === 'Cboe delayed options snapshot; not executable quotes' ? 'delayed-unqualified' : 'unavailable', received_at: iso(r.received_at) })) },
        structural: {
            official: Object.entries(obj(official.sources)).slice(0, 16).map(([name, value]) => { const v = obj(value); return { ...v, name, status: !raw.structural_official_error && v.status === 'available' ? 'published-history-unqualified' : 'unavailable', received_at: iso(v.received_at) }; }),
            rebalance: { ...reb, status: !raw.structural_rebalance_error && reb.status === 'modeled_daily_proxy' ? 'modeled-history-unqualified' : 'unavailable', computed_at: iso(reb.computed_at) },
        },
    };
}

/** Project the authenticated bridge envelope into an allowlisted public view.
 * Numeric clocks are epoch seconds; legacy ISO clocks are parsed only in legacyFrame.
 * Receipt, source, capture and computation clocks are never substituted for one another.
 * @returns {PublicView}
 */
export function adaptGammaWatch(envelope, nowMs) {
    const input = obj(obj(envelope).data), now = nowMs / 1000;
    const legacy = !Object.hasOwn(input, 'schema') && typeof input.served_at === 'string' && (Object.hasOwn(input, 'quote') || Object.hasOwn(input, 'gex'));
    const frame = legacy ? legacyFrame(input) : input;
    const known = legacy || (frame.schema === 'gex-screen/2' && frame.symbol === 'SPY');
    const valid = envelope?.status === 'available' && known && frame.evaluation_clock === 'ok' && within(frame.served_at, now, legacy ? 20 : 15);
    function context(raw, kind, age, source) {
        const c = obj(raw), clockOk = ordered(c.source_time, c.receipt_time, now);
        const capValid = finite(c.max_context_age_seconds) && c.max_context_age_seconds >= 0;
        const effectiveAge = capValid ? Math.min(age, c.max_context_age_seconds) : null;
        const fresh = clockOk && capValid && within(c.source_time, now, effectiveAge) && within(c.receipt_time, now, effectiveAge);
        const ok = valid && c.source === source && c.status === kind && c.values_available === true && c.current_interpretation === false && fresh;
        return { ok, status: ok ? kind : !valid ? 'unavailable' : c.status === 'failed' ? 'failed' : !clockOk ? 'clock-invalid' : capValid && !fresh ? 'stale' : status(c.status), sourceTime: num(c.source_time), receiptTime: num(c.receipt_time) };
    }
    const p = obj(frame.public_price), g = obj(frame.delayed_gex);
    const priceContext = context(p, 'unverified-public-latency', 120, 'Yahoo chart · public, latency not guaranteed'), delayedContext = context(g, 'unverified-delayed', 1800, 'ZeroGEX · delayed ~15 minutes');
    const m = obj(frame.research_model), grid = list(m.grid, 1001), variants = list(m.variants, 7);
    const modelOk = valid && m.status === 'frozen-research' && m.current_interpretation === false && ordered(m.chain_as_of, m.computed_at, now) && grid.length >= 2 && grid.length <= 1000 && grid.every((x, i) => positive(x) !== null && (i === 0 || x > grid[i - 1])) && variants.length > 0 && variants.length <= 6 && variants.every(v => ['paired OTM IV', 'raw IV filtered'].includes(v?.name) && [0, .2, .4].includes(v.iv_shock) && Array.isArray(v.values) && v.values.length === grid.length && v.values.every(finite));
    const sector = obj(frame.sectors), sectorOk = valid && sector.status === 'delayed-unqualified' && within(sector.computed_at, now, 600);
    const structural = obj(frame.structural), reb = obj(structural.rebalance);
    const rebOk = valid && reb.status === 'modeled-history-unqualified' && within(reb.computed_at, now, Infinity);
    // Legacy live receipts retain their dedicated table without exposing broker/model blocks.
    const live = legacy ? obj(input.structural_live) : {};
    const liveOk = valid && legacy && !input.structural_live_error && within(iso(live.computed_at), now, 20);
    const legacyQuote = legacy ? obj(input.quote) : {};
    const legacyRtd = valid && legacyQuote.is_rtd === true && !input.quote_error && within(iso(legacyQuote.as_of), now, 20);
    return {
        valid, schema: known ? frame.schema : 'unsupported', servedAt: num(frame.served_at),
        price: { ...priceContext, price: priceContext.ok ? positive(p.price) : null, source: 'Yahoo public context · latency not guaranteed' },
        delayed: { ...delayedContext, levels: Object.fromEntries(levels.map(k => [k, delayedContext.ok ? num(obj(g.levels)[k]) : null])), spot: delayedContext.ok ? positive(g.spot) : null, phase: ['pre', 'regular', 'post', 'closed'].includes(g.provider_market_phase) ? g.provider_market_phase : null, receiptLag: ordered(g.source_time, g.receipt_time, now) ? g.receipt_time - g.source_time : null },
        research: { status: modelOk ? 'frozen-research' : 'unavailable', chainAsOf: num(m.chain_as_of), computedAt: num(m.computed_at), grid: modelOk ? grid : [], variants: modelOk ? variants.map(v => ({ name: v.name, iv_shock: v.iv_shock, values: [...v.values] })) : [] },
        sectors: { status: sectorOk ? 'delayed-unqualified' : 'unavailable', computedAt: num(sector.computed_at), rows: list(sector.rows, 15).map(obj).filter(r => sectorSymbols.includes(r.symbol)).map(r => {
            const ok = sectorOk && r.status === 'delayed-unqualified' && within(r.received_at, now, 600);
            return { symbol: r.symbol, status: ok ? 'delayed-unqualified' : 'unavailable', receivedAt: num(r.received_at), session_date: day(r.session_date), nearest_expiry: day(r.nearest_expiry), ...Object.fromEntries(['price', 'change_pct', 'today_volume', 'expiry_volume', 'atm_spread_pct', 'atm_two_sided', 'atm_contracts'].map(k => [k, ok ? num(r[k]) : null])) };
        }) },
        official: list(structural.official, 16).map(obj).filter(group => ['fed_treasury', 'repo', 'tga'].includes(group.name) || (typeof group.name === 'string' && group.name.startsWith('auction_') && day(group.name.slice(8)))).map(group => {
            const ok = valid && group.status === 'published-history-unqualified' && within(group.received_at, now, Infinity);
            return { name: group.name, status: ok ? group.status : 'unavailable', receivedAt: num(group.received_at), rows: ok ? list(group.rows, 16).map(obj).map(r => ({ ...Object.fromEntries(dates.map(k => [k, day(r[k])])), ...Object.fromEntries(amounts.map(k => [k, publicNumber(r[k])])) })) : [] };
        }),
        rebalance: { status: rebOk ? reb.status : 'unavailable', computedAt: num(reb.computed_at), rows: rebOk ? list(reb.rows, 2).map(obj).filter(r => ['month', 'quarter'].includes(r.period)).map(r => ({ period: r.period, baseline: day(r.baseline), as_of_date: day(r.as_of_date), equity_return: num(r.equity_return), bond_return: num(r.bond_return), scenarios: list(r.scenarios, 3).map(obj).map(s => ({ equity_weight: num(s.equity_weight), equity_purchase_per_initial_billion: num(s.equity_purchase_per_initial_billion) })) })) : [] },
        live: { status: liveOk ? 'receipt-only' : 'unavailable', computedAt: iso(live.computed_at), rows: list(live.rows, 100).map(obj).map(r => ({ symbol: text(r.symbol), value: liveOk && r.direction_usable === true && within(iso(r.callback_at), now, 20) ? num(r.value) : null, callbackAt: iso(r.callback_at), status: liveOk && r.direction_usable === true && within(iso(r.callback_at), now, 20) ? 'receipt-only' : 'unavailable' })) },
        rtd: { price: legacyRtd ? positive(legacyQuote.price) : null, receiptTime: legacyRtd ? iso(legacyQuote.as_of) : null },
        privateStatus: 'Private RTD, current GEX, journal and contracts unavailable on the public schema. Legacy receipt-only observations, when supplied, do not certify exchange event time.',
    };
}
