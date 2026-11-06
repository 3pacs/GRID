// Public evidence cards: every value retains its source time and research status.
export function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (c) => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
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
    return `<section class="panel insights-panel" aria-label="Market evidence">
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
