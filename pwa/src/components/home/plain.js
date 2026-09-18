// Plain-English helpers for the stepdad.finance home page.
// Everything here exists to make the page legible and unambiguous for a
// non-technical 73-year-old: company names instead of tickers, calm words
// instead of trader jargon, reassuring error copy instead of system leaks.

const TICKER_NAMES = {
    AAPL: 'Apple', MSFT: 'Microsoft', AMZN: 'Amazon', GOOGL: 'Google', GOOG: 'Google',
    META: 'Meta', NVDA: 'Nvidia', TSLA: 'Tesla', AMD: 'AMD', AVGO: 'Broadcom',
    NFLX: 'Netflix', INTC: 'Intel', MU: 'Micron', DELL: 'Dell', ANET: 'Arista',
    ASML: 'ASML', HPE: 'HP Enterprise', JPM: 'JPMorgan', V: 'Visa', MA: 'Mastercard',
    BRK: 'Berkshire', 'BRK-B': 'Berkshire', LLY: 'Eli Lilly', COST: 'Costco',
    HD: 'Home Depot', CAT: 'Caterpillar', GE: 'GE', BHP: 'BHP', FCX: 'Freeport',
    NUE: 'Nucor', CCJ: 'Cameco', CEG: 'Constellation', ETN: 'Eaton',
    // funds / commodities
    QQQ: 'Nasdaq tech', SPY: 'S&P 500', GLD: 'Gold', SLV: 'Silver',
    'BTC-USD': 'Bitcoin', 'ETH-USD': 'Ethereum', TLT: 'Long bonds', VIX: 'Fear index',
};

/** "Apple (AAPL)" — plain name first, symbol second. Falls back to the symbol. */
export function tickerLabel(symbol) {
    const s = String(symbol || '').toUpperCase();
    const name = TICKER_NAMES[s] || TICKER_NAMES[s.split('-')[0]];
    return name ? `${name} (${s})` : s;
}

/** Just the friendly name if we know it, else the symbol. */
export function tickerName(symbol) {
    const s = String(symbol || '').toUpperCase();
    return TICKER_NAMES[s] || TICKER_NAMES[s.split('-')[0]] || s;
}

/** Trader sentiment → calm plain words. Returns {label, tone}. */
export function plainSentiment(raw) {
    const v = String(raw || '').toLowerCase();
    if (/bull|positive|up|risk.?on|green|inflow/.test(v)) return { label: 'Looking up', tone: 'up' };
    if (/bear|negative|down|risk.?off|red|outflow/.test(v)) return { label: 'Looking shaky', tone: 'down' };
    return { label: 'Calm / mixed', tone: 'flat' };
}

/**
 * Market regime string (+ optional confidence 0-1) → a full plain sentence,
 * tone, and whether there is actually a reading to show.
 *
 * `UNCALIBRATED` (api/routers/regime.py's sentinel for "no decision_journal
 * row exists yet") and any null/missing/empty state are not a regime
 * reading at all — they must say so plainly, with no numeric confidence,
 * rather than being cleaned up into "The market read is "UNCALIBRATED"."
 * as if it were a real (if unfamiliar) label.
 *
 * A genuine reading's confidence can legitimately be 0.0 (a real regime the
 * model is simply not confident about) — that must still render as a real
 * reading with 0%, so this checks `typeof === 'number'`, never truthiness.
 */
export function plainRegime(raw, confidence) {
    const v = String(raw || '').toLowerCase();
    const confidencePct = (typeof confidence === 'number' && Number.isFinite(confidence))
        ? Math.round(confidence * 100)
        : null;

    if (!raw || v === 'uncalibrated') {
        return {
            sentence: "The market read isn't ready yet — no regime reading has been made.",
            tone: 'flat',
            available: false,
            confidencePct: null,
        };
    }
    if (/risk.?off|bear|stress|defensive|fear/.test(v))
        return { sentence: 'Investors are nervous and playing it safe.', tone: 'down', available: true, confidencePct };
    if (/risk.?on|bull|greed|expansion|growth/.test(v))
        return { sentence: 'Investors are feeling confident.', tone: 'up', available: true, confidencePct };
    if (/neutral|mixed|transition|chop/.test(v))
        return { sentence: 'The market is calm and mixed — no strong direction.', tone: 'flat', available: true, confidencePct };
    // unknown regime label: show it cleaned, neutral tone
    return { sentence: `The market read is "${String(raw).replace(/_/g, ' ')}".`, tone: 'flat', available: true, confidencePct };
}

/** Turn whatever an error throw gives us into one calm sentence. Never leak. */
export function warmError(kind) {
    switch (kind) {
        case 'price': return "That price isn’t updating right now (the market may be closed).";
        case 'verdict': return "I couldn’t get an answer just now. Please try again in a moment.";
        default: return "Something went wrong — please try again.";
    }
}
