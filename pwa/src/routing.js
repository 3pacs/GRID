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

export function parseHashRoute(hash = '') {
    const raw = readHashPath(hash) || 'canvas';
    const [path = 'canvas', search = ''] = raw.split('?');
    const segments = path.split('/').filter(Boolean).map(safeDecode);
    const route = segments[0] || 'canvas';

    if (route === 'login') {
        return { view: 'login' };
    }

    if (route === 'journal' && segments[1]) {
        return { view: 'journal-entry', entryId: Number.parseInt(segments[1], 10) };
    }

    if (route === 'watchlist' && segments[1]) {
        return { view: 'watchlist-analysis', selectedTicker: segments[1] };
    }

    if (route === 'watchlist-analysis' || route === 'ticker') {
        const selectedTicker = segments[1] || firstQueryValue(search, ['ticker', 'symbol']);
        return { view: 'watchlist-analysis', selectedTicker: safeDecode(selectedTicker) };
    }

    if (route === 'sector-dive' && segments[1]) {
        return { view: 'sector-dive', selectedSector: segments[1] };
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
        return { view: 'canvas' };
    }

    return { view: route };
}

export function buildRouteHash(view, id) {
    if (view === 'journal-entry' && id) {
        return `#/journal/${encodeURIComponent(id)}`;
    }

    if ((view === 'watchlist-analysis' || view === 'ticker') && id) {
        const ticker = typeof id === 'object' ? id.symbol || id.ticker : id;
        return `#/watchlist/${encodeURIComponent(ticker)}`;
    }

    if (view === 'sector-dive' && id) {
        return `#/sector-dive/${encodeURIComponent(id)}`;
    }

    if (view === 'intel-submit') {
        return '#/intel/submit';
    }

    return `#/${view}`;
}
