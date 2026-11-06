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
    const cards = items.map((item) => {
        const evidence = item.evidence || {};
        const forecast = evidence.forecast;
        return `<article class="engine-card insight-card" data-insight-symbol="${escapeHtml(evidence.symbol)}">
            <div class="engine-head"><strong>${escapeHtml(evidence.symbol)}</strong>
                <span>${item.stale ? 'STALE' : forecast ? escapeHtml(item.status) : 'OBSERVATION'}</span></div>
            <h3>${escapeHtml(item.call)}</h3>
            <p>${escapeHtml(item.setup)}</p>
            <div class="seer-support">Observed ${escapeHtml(time(evidence.as_of))} · Binance ${escapeHtml(evidence.entry?.instrument || evidence.symbol + 'USDT')} · USDT</div>
            <div class="seer-support">24h candle coverage: ${pct(evidence.coverage)} · ${evidence.change_24h == null ? '24h change unavailable' : 'measured 24h change'}</div>
            ${forecast ? `<p>Target ${Number(forecast.target_price).toFixed(2)} USDT by ${escapeHtml(time(forecast.due_at))}.
                Miss if the declared 1% directional move is absent at that outcome.</p>
                <div>Result: ${escapeHtml(item.status)}${item.score ? ` · signed return ${pct(item.score.signed_return)}` : ' · awaiting the future outcome'}</div>`
                : '<div>No directional forecast. An observation receives no win/loss grade.</div>'}
            <div class="seer-support">${escapeHtml(evidence.validation)} · ${item.prediction_id ? `<a href="/api/v1/astrogrid/predictions/${encodeURIComponent(item.prediction_id)}">Evidence record</a>` : ''}</div>
        </article>`;
    }).join('');
    return `<section class="panel insights-panel" aria-label="Market evidence">
        <div class="split-header"><h2>Market evidence</h2><span>source-backed research</span></div>
        ${error ? `<p role="alert">${escapeHtml(error)}</p>` : ''}
        ${cards || `<p>${payload ? 'No fresh market evidence has been published yet.' : 'Loading market evidence…'}</p>`}
        <p class="seer-support">${performance?.scored
            ? `${performance.scored} prospective forecasts scored · ${pct(performance.accuracy)} hit rate · fees excluded`
            : 'No validated forecast track record yet. Legacy templates are excluded.'}</p>
        <p class="seer-support">Celestial readings below are symbolic context; their market edge has not been demonstrated.</p>
    </section>`;
}
