function readHashPath(hash = '') {
    if (hash.startsWith('#/')) return hash.slice(2);
    if (hash.startsWith('#')) return hash.slice(1);
    return hash;
}

function safeDecode(value) {
    if (!value) return value;
    try {
        return decodeURIComponent(value);
    } catch {
        return value;
    }
}

function firstQueryValue(search, names) {
    const params = new URLSearchParams(search || '');
    for (const name of names) {
        const value = params.get(name);
        if (value) return value;
    }
    return null;
}

function readOriginView(search) {
    return safeDecode(firstQueryValue(search, ['from']));
}

function appendOrigin(params, from) {
    if (from) {
        params.set('from', from);
    }
    return params;
}

function withOrigin(route, originView) {
    if (!originView) return route;
    return { ...route, originView };
}

const CANVAS_LENSES = new Set(['graph', 'supply', 'capital']);

export function parseHashRoute(hash = '') {
    const raw = readHashPath(hash) || 'surfacer';
    const [path = 'surfacer', search = ''] = raw.split('?');
    const segments = path.split('/').filter(Boolean).map(safeDecode);
    const route = segments[0] || 'surfacer';
    const originView = readOriginView(search);

    if (route === 'login') {
        return { view: 'login' };
    }

    if (route === 'journal' && segments[1]) {
        return withOrigin({
            view: 'journal-entry',
            entryId: Number.parseInt(segments[1], 10),
        }, originView);
    }

    if (route === 'watchlist' && segments[1]) {
        return withOrigin({
            view: 'watchlist-analysis',
            selectedTicker: segments[1],
        }, originView);
    }

    if (route === 'watchlist-analysis' || route === 'ticker') {
        const selectedTicker = segments[1] || firstQueryValue(search, ['ticker', 'symbol']);
        return withOrigin({
            view: 'watchlist-analysis',
            selectedTicker: safeDecode(selectedTicker),
        }, originView);
    }

    if (route === 'sector-dive' && segments[1]) {
        return withOrigin({
            view: 'sector-dive',
            selectedSector: segments[1],
        }, originView);
    }

    if (route === 'signals') {
        return {
            view: 'signals',
            focusFeature: safeDecode(firstQueryValue(search, ['feature', 'q'])),
        };
    }

    if (route === 'discovery') {
        return {
            view: 'discovery',
            focusHypothesis: safeDecode(firstQueryValue(search, ['hypothesis', 'q'])),
        };
    }

    if (route === 'actor-network') {
        return {
            view: 'actor-network',
            focusActor: safeDecode(firstQueryValue(search, ['actor', 'q'])),
        };
    }

    if (route === 'system') {
        return {
            view: 'system',
            focusSource: safeDecode(firstQueryValue(search, ['source', 'q'])),
        };
    }

    if (route === 'intel' && segments[1] === 'submit') {
        return { view: 'intel-submit' };
    }

    if (route === 'intel-submit') {
        return { view: 'intel-submit' };
    }

    if (route === 'intel-mod') {
        return { view: 'intel-mod' };
    }

    if (route === 'canvas') {
        return withOrigin({
            view: 'canvas',
            actorId: segments[1] || null,
            lens: CANVAS_LENSES.has(segments[2]) ? segments[2] : 'graph',
            boardId: safeDecode(firstQueryValue(search, ['board'])),
        }, originView);
    }

    if (originView) {
        return { view: route, originView };
    }

    return { view: route };
}

export function buildRouteHash(view, id) {
    const params = new URLSearchParams();
    const from = typeof id === 'object' && id !== null ? id.from : null;

    if (view === 'journal-entry' && id) {
        const entryId = typeof id === 'object' ? id.id ?? id.entryId : id;
        appendOrigin(params, from);
        const suffix = params.toString();
        return `#/journal/${encodeURIComponent(entryId)}${suffix ? `?${suffix}` : ''}`;
    }

    if ((view === 'watchlist-analysis' || view === 'ticker') && id) {
        const ticker = typeof id === 'object' ? id.id ?? id.symbol ?? id.ticker : id;
        appendOrigin(params, from);
        const suffix = params.toString();
        return `#/watchlist/${encodeURIComponent(ticker)}${suffix ? `?${suffix}` : ''}`;
    }

    if (view === 'sector-dive' && id) {
        const sector = typeof id === 'object' ? id.id ?? id.sector ?? id.name : id;
        appendOrigin(params, from);
        const suffix = params.toString();
        return `#/sector-dive/${encodeURIComponent(sector)}${suffix ? `?${suffix}` : ''}`;
    }

    if (view === 'signals' && id) {
        return `#/signals?feature=${encodeURIComponent(id)}`;
    }

    if (view === 'discovery' && id) {
        return `#/discovery?hypothesis=${encodeURIComponent(id)}`;
    }

    if (view === 'actor-network' && id) {
        return `#/actor-network?actor=${encodeURIComponent(id)}`;
    }

    if (view === 'system' && id) {
        return `#/system?source=${encodeURIComponent(id)}`;
    }

    if (view === 'intel-submit') {
        return '#/intel/submit';
    }

    if (view === 'intelligence-search') {
        appendOrigin(params, from);
        const suffix = params.toString();
        return `#/intelligence-search${suffix ? `?${suffix}` : ''}`;
    }

    if (view === 'canvas') {
        const actorId = typeof id === 'object' && id !== null ? id.actorId ?? id.id : id;
        const lens = typeof id === 'object' && id !== null ? id.lens : null;
        const boardId = typeof id === 'object' && id !== null ? id.board : null;
        const parts = ['canvas'];
        if (actorId) parts.push(encodeURIComponent(actorId));
        if (lens && lens !== 'graph') parts.push(encodeURIComponent(lens));
        if (boardId) params.set('board', boardId);
        appendOrigin(params, from);
        const suffix = params.toString();
        return `#/${parts.join('/')}${suffix ? `?${suffix}` : ''}`;
    }

    return `#/${view}`;
}
