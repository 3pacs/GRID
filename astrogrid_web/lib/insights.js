// Public evidence cards: every value retains its source time and research status.
export function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (c) => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
}

export function edgeMarkup(report, live) {
    if (!report || report.version !== 'astrogrid-spot-flow-edge-v1') {
        return '<section class="panel" aria-label="Predictive edge"><h2>Predictive edge</h2><p>Evaluation unavailable. No demonstrated edge.</p></section>';
    }
    const test = report.test;
    const quality = Object.values(report.data_quality);
    const rows = quality.reduce((n, q) => n + q.rows, 0);
    const missing = quality.reduce((n, q) => n + q.missing_bars, 0);
    const duplicates = quality.reduce((n, q) => n + q.duplicates, 0);
    const bp = (v) => Number.isFinite(v) ? `${v.toFixed(2)} bp` : 'unavailable';
    const ci = (v) => Array.isArray(v) ? `${bp(v[0])} to ${bp(v[1])}` : 'unavailable';
    const names = {flow:'Taker-flow rule', cash:'Cash / no trade', always_long:'Always long',
        always_short:'Always short (paper)', price_momentum:'Previous 5m price momentum',
        random_expected:'Random direction (expected)', reverse_flow:'Reverse flow (diagnostic)',
        long_only_flow:'Buy on bullish flow; otherwise cash'};
    const verdicts = {NO_POSITIVE_NET_RETURN:'No predictive edge after costs',
        NO_DEMONSTRATED_EDGE:'No demonstrated advantage over simple baselines',
        INSUFFICIENT_SAMPLE:'Insufficient historical sample',
        HISTORICAL_SUPPORT_ONLY:'Historical support; prospective confirmation required'};
    return `<section class="panel edge-panel" aria-label="Predictive edge">
        <div class="split-header"><h2>Predictive edge</h2><span>frozen-rule evaluation</span></div>
        <h3>${escapeHtml(verdicts[report.verdict] || 'Evaluation unavailable')}</h3>
        <p>Historical replay: ${test.sample.events.toLocaleString()} nonoverlapping events across ${test.sample.utc_dates} UTC days, August–October 2026. BTC ${test.sample.per_asset.BTC.toLocaleString()} · ETH ${test.sample.per_asset.ETH.toLocaleString()} · SOL ${test.sample.per_asset.SOL.toLocaleString()}. Historical replay is separate from live forecasts.</p>
        <p>Mean gross: <strong>${bp(test.flow_gross_mean_bps)}</strong>; after fees only: <strong>${bp(test.flow_fees_only_mean_bps)}</strong>; after fees and slippage: <strong>${bp(test.strategies.flow.mean_net_bps)}</strong> per event (95% interval ${ci(test.strategies.flow.mean_net_95ci_bps)}).</p>
        <p>Cost scenario: 0.10% fee + 0.02% slippage per side. Enter at the next completed 5m close; exit at the original forecast outcome. 1 bp = 0.01%. Fixed-notional event returns; no compounding.</p>
        <div style="overflow-x:auto"><table><caption>Matched test events · mean return after costs</caption><thead><tr><th scope="col">Rule / baseline</th><th scope="col">Net per event</th><th scope="col">95% interval</th></tr></thead><tbody>
        ${Object.entries(test.strategies).map(([name, s]) => `<tr><th scope="row">${escapeHtml(names[name] || name)}</th><td>${bp(s.mean_net_bps)}</td><td>${ci(s.mean_net_95ci_bps)}</td></tr>`).join('')}
        </tbody></table></div>
        <p>Flow advantage over random direction: ${bp(test.paired_advantages.random_expected.mean_advantage_bps)} (paired 95% interval ${ci(test.paired_advantages.random_expected.advantage_95ci_bps)}). An interval spanning zero does not establish directional skill.</p>
        <details><summary>Trading costs, sample and methodology</summary>
            <p>One year of checksum-verified Binance global spot archives, November 2025–October 2026; ${rows.toLocaleString()} validated bars, ${missing} missing bars and ${duplicates} duplicate timestamps. November–July is a separate reference period; August–October is the temporal test. No parameter tuning. Archives are current vintages, without historical receipt or fill evidence.</p>
            <p>Qualification: 100,000 USDT turnover, 100 trades, 10% absolute taker imbalance. Overlapping exposure removed separately for each asset. Seven-day moving-block bootstrap samples all assets together; 1,999 replicates. Positive historical support requires simultaneous improvement over cash and four matched baselines. Original 0.10% target hit rate is ${(test.forecast_close_basis.hit_rate * 100).toFixed(2)}%; target hits are not profitable fills.</p>
            <p>Costs are scenarios using current regular-user spot fees, not historical or account-specific rates. Paper shorts exclude borrowing, funding and availability. The long-only/cash restriction is shown separately; no live trades were executed.</p>
            <table><caption>Flow cost sensitivity · same test events</caption><thead><tr><th>Roundtrip allowance</th><th>Mean net</th></tr></thead><tbody>${test.cost_sensitivity.map((s) => `<tr><td>${bp(s.roundtrip_bps)}</td><td>${bp(s.flow_mean_net_bps)}</td></tr>`).join('')}</tbody></table>
            <p>${Object.entries(report.per_month).map(([m, r]) => `${escapeHtml(m)}: ${r.sample.events.toLocaleString()} events, ${bp(r.strategies.flow.mean_net_bps)}`).join(' · ')}</p>
            <p>Per asset (descriptive): ${Object.entries(report.per_asset).map(([s, r]) => `${escapeHtml(s)} ${bp(r.strategies.flow.mean_net_bps)}`).join(' · ')}. Reference period: ${report.reference.sample.events.toLocaleString()} events, ${bp(report.reference.strategies.flow.mean_net_bps)} after the same costs.</p>
        </details>
        <details><summary>Sources and reproducible results</summary>
            <p>Analysis generated ${escapeHtml(report.generated_at)}. <a href="./research/spot-flow-edge-v1.json">Full results, per-asset comparisons and archive SHA256 checksums</a> · <a href="https://github.com/3pacs/GRID/blob/main/${encodeURI(report.protocol_path)}">Frozen protocol</a></p>
            ${report.sources.map((s) => `<p><a href="${escapeHtml(s.url)}">${escapeHtml(s.title)}</a></p>`).join('')}
        </details>
        <h3>Live prospective cohort — separate evidence</h3>
        <p>${live?.scored || 0} scored forecasts across ${live?.utc_dates || 0} UTC dates. ${live?.scored ? `Signal-close gross mean ${bp(live.mean_gross_bps)}; same cost scenario ${bp(live.mean_scenario_net_bps)}.` : ''} Overlapping calls and signal-close prices make this a diagnostic, not independent trading performance. Insufficient independent prospective evidence.</p>
        <p>Current evidence does not justify trading or promoting this rule. The live rule and original forecast history remain unchanged.</p>
    </section>`;
}

export function insightsMarkup(payload, error = '') {
    const items = payload?.items || [];
    const performance = payload?.performance;
    const pct = (v) => Number.isFinite(v) ? `${(v * 100).toFixed(2)}%` : 'unavailable';
    const time = (v) => v ? new Date(v).toLocaleString() : 'unknown';
    const cohorts = Object.values(payload?.performance_by_contract || {});
    const flowReasons = {measured_taker_imbalance:'Measured flow qualifies', balanced_taker_flow:'Imbalance below 10%',
        insufficient_quote_turnover:'Turnover below 100,000 USDT', insufficient_trades:'Fewer than 100 trades',
        stale_spot_flow:'Flow measurement is more than 10 minutes old',
        no_verified_spot_flow_receipt:'No recorded flow measurement', invalid_spot_flow_receipt:'Flow measurement failed source checks'};
    const cards = items.map((item) => {
        const evidence = item.evidence || {};
        const forecast = evidence.forecast;
        return `<article class="engine-card insight-card" data-insight-symbol="${escapeHtml(evidence.symbol)}">
            <div class="engine-head"><strong>${escapeHtml(evidence.symbol)}</strong>
                <span>${forecast ? escapeHtml(item.status) : item.stale ? 'STALE' : 'OBSERVATION'}</span></div>
            <h3>${escapeHtml(item.call)}</h3>
            <p>${escapeHtml(!forecast && evidence.kind === 'observation' && evidence.available
                ? 'Measured market conditions. Qualified directional forecasts appear as separate research contracts.'
                : item.setup)}</p>
            <div class="seer-support">Observed ${escapeHtml(time(evidence.as_of))} · Binance ${escapeHtml(evidence.entry?.instrument || evidence.symbol + 'USDT')} · ${evidence.entry?.source === 'binance_spot_rest' ? 'global spot REST · ' : 'observed trade stream · '}USDT</div>
            ${forecast ? `<div class="seer-support">${escapeHtml(forecast.label || '24h actor-flow research')} · declared threshold ${pct(forecast.threshold ?? .01)}</div>`
                : `<div class="seer-support">24h candle coverage: ${pct(evidence.coverage)} · ${evidence.change_24h == null ? '24h change unavailable' : 'measured 24h change'}</div>`}
            ${forecast ? `<p>Target ${Number(forecast.target_price).toFixed(2)} USDT by ${escapeHtml(time(forecast.due_at))}.
                Miss if the declared ${pct(forecast.threshold ?? .01)} directional move is absent at that outcome. First recorded completed 5m close at or after due, within 15m.</p>
                <div>Result: ${escapeHtml(item.status)}${item.score ? ` · signed return ${pct(item.score.signed_return)}` : ' · awaiting the future outcome'}</div>`
                : '<div>Measured observation; no win/loss grade. See separate research contracts.</div>'}
            <div class="seer-support">${escapeHtml(evidence.validation)} · ${item.prediction_id ? `<a href="/api/v1/astrogrid/predictions/${encodeURIComponent(item.prediction_id)}">Evidence record</a>` : ''}</div>
        </article>`;
    }).join('');
    return `${edgeMarkup(payload?.edge_assessment, payload?.prospective_cost_diagnostic)}<section class="panel insights-panel" aria-label="Market evidence">
        <div class="split-header"><h2>Market evidence</h2><span>source-backed research</span></div>
        ${error ? `<p role="alert">${escapeHtml(error)}</p>` : ''}
        ${cards || `<p>${payload ? 'No fresh market evidence has been published yet.' : 'Loading market evidence…'}</p>`}
        ${cohorts.length ? cohorts.map((p) => `<p class="seer-support">${escapeHtml(p.label)}: ${p.scored || 0} prospective forecasts scored${p.scored ? ` · ${pct(p.accuracy)} hit rate` : ' · no outcomes yet'} · fees excluded</p>`).join('') : `<p class="seer-support">${performance?.scored
            ? `${performance.scored} prospective forecasts scored · ${pct(performance.accuracy)} hit rate · fees excluded`
            : 'No validated forecast track record yet. Legacy templates are excluded.'}</p>`}
        ${payload?.flow_diagnostics ? `<details><summary>30m flow qualification</summary>${Object.entries(payload.flow_diagnostics).map(([s, q]) => `<p>${escapeHtml(s)}: ${escapeHtml(flowReasons[q.reason] || q.reason)}${Number.isFinite(q.imbalance) ? ` · signed taker imbalance ${pct(q.imbalance)} · ${Number(q.quote_volume).toLocaleString()} USDT turnover · ${Number(q.trade_count).toLocaleString()} trades · observed ${escapeHtml(time(q.observed_at))}` : ''}</p>`).join('')}<p>Latest completed 5m bar: at least 100,000 USDT turnover, 100 trades and 10% absolute taker imbalance. Anonymous exchange cohorts; identities unknown.</p></details>` : ''}
        <p class="seer-support">Research outcomes do not establish trading edge. Legacy templates are excluded.</p>
        <p class="seer-support">Celestial readings below are symbolic context; their market edge has not been demonstrated.</p>
    </section>`;
}
