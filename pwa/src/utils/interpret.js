/**
 * interpret.js — Shared interpretation engine for GRID.
 *
 * Every number in the system gets context. Z-scores get plain-English
 * descriptions. Features get "why this matters" explanations. Options
 * metrics get positioning implications.
 */

// ── Z-Score Interpretation ──────────────────────────────────────────

export function interpretZScore(z, featureName = '') {
    if (z == null) return '';
    const abs = Math.abs(z);
    const dir = z > 0 ? 'above' : 'below';
    const dirAction = z > 0 ? 'elevated' : 'depressed';

    if (abs > 3) return `Extreme (${abs.toFixed(1)}σ ${dir} mean) — occurs <0.3% of the time. Likely regime-relevant.`;
    if (abs > 2.5) return `Very rare (${abs.toFixed(1)}σ) — outside normal range 99% of the time.`;
    if (abs > 2) return `Unusual (${abs.toFixed(1)}σ) — ${dirAction}, seen ~5% of the time historically.`;
    if (abs > 1.5) return `Notable (${abs.toFixed(1)}σ) — moderately ${dirAction}, worth monitoring.`;
    if (abs > 1) return `Slightly ${dirAction} (${abs.toFixed(1)}σ) — within one standard deviation of normal.`;
    return `Normal range (${abs.toFixed(1)}σ) — no signal.`;
}

// ── Feature Interpretation ──────────────────────────────────────────

const FEATURE_CONTEXT = {
    // Rates
    'treasury_10y': { what: '10-Year Treasury Yield', why: 'Rising yields signal tighter conditions, pressure on growth stocks and real estate. Falling yields signal flight to safety.' },
    'treasury_2y': { what: '2-Year Treasury Yield', why: 'Reflects Fed rate expectations. Sharp moves signal market repricing of monetary policy.' },
    'yield_curve_10y2y': { what: 'Yield Curve (10Y-2Y)', why: 'Inversion (negative) historically precedes recessions by 6-18 months. Steepening signals recovery expectations.' },
    'sofr': { what: 'SOFR Rate', why: 'Overnight funding rate. Spikes signal bank stress or liquidity crunch.' },
    'mortgage_30y': { what: '30-Year Mortgage Rate', why: 'Directly impacts housing demand and consumer spending power.' },
    'fed_balance_sheet': { what: 'Fed Balance Sheet', why: 'QE expands it (risk-on), QT contracts it (tighter liquidity). Direction matters more than level.' },
    'breakeven_10y': { what: '10Y Breakeven Inflation', why: 'Market-implied inflation expectation. Rising = inflationary pressure, falling = deflation risk.' },

    // Credit
    'hy_spread': { what: 'High Yield Spread', why: 'Widening = credit stress, risk aversion. Tightening = risk appetite, easy money.' },
    'ted_spread': { what: 'TED Spread', why: 'Interbank lending stress. Spikes preceded 2008 crisis. Normally <0.5%.' },
    'ofr_financial_stress': { what: 'OFR Financial Stress Index', why: 'Composite of credit, equity, funding, and safe haven stress. Above 0 = elevated stress.' },

    // Vol
    'vix': { what: 'VIX (Fear Index)', why: 'Below 15 = complacency, 15-25 = normal, 25-35 = fear, above 35 = panic. Mean-reverts strongly.' },
    'vixcls': { what: 'VIX Close', why: 'Same as VIX. Elevated levels historically precede either sharp selloffs or volatility crushes within 2 weeks.' },

    // Macro
    'cpi': { what: 'Consumer Price Index', why: 'Inflation gauge. Above target = hawkish Fed, below = dovish. Drives rate expectations.' },
    'unemployment': { what: 'Unemployment Rate', why: 'Lagging indicator. Rising unemployment confirms recession, falling confirms expansion.' },
    'nonfarm_payrolls': { what: 'Nonfarm Payrolls', why: 'Leading employment indicator. Strong = economy expanding, weak = slowing.' },
    'm2_money_supply': { what: 'M2 Money Supply', why: 'Broad money. Contraction historically precedes deflationary pressure and asset weakness.' },
    'industrial_production': { what: 'Industrial Production', why: 'Real economy output. Declining = manufacturing recession signal.' },
    'initial_claims': { what: 'Initial Jobless Claims', why: 'Weekly leading indicator. Rising claims = labor market weakening. Watch for trend, not single prints.' },
    'leading_index': { what: 'Leading Economic Index', why: '10-component composite. 3+ months of decline historically signals recession within 6 months.' },

    // Equity / ETFs
    'spy_full': { what: 'S&P 500 (SPY)', why: 'Broad market benchmark. Everything is measured relative to this.' },
    'xlk_full': { what: 'Technology Sector (XLK)', why: 'Growth/momentum proxy. Outperforms in GROWTH regime, underperforms in FRAGILE/CRISIS.' },
    'xle_full': { what: 'Energy Sector (XLE)', why: 'Commodity-linked. Outperforms during inflation and supply shocks.' },
    'xlf_full': { what: 'Financials (XLF)', why: 'Rate-sensitive. Benefits from steeper yield curve and economic expansion.' },
    'xlv_full': { what: 'Healthcare (XLV)', why: 'Defensive sector. Outperforms in FRAGILE/CRISIS regimes.' },

    // Crypto
    'btc_usd_full': { what: 'Bitcoin', why: 'Digital gold narrative + risk-on asset. Correlates with liquidity conditions and tech sentiment.' },
    'eth_usd_full': { what: 'Ethereum', why: 'Smart contract platform. Higher beta than BTC. DeFi TVL is a usage indicator.' },
};

export function interpretFeature(name, z = null) {
    const ctx = FEATURE_CONTEXT[name];
    if (!ctx) {
        // Generate generic interpretation from name
        const clean = name.replace(/_/g, ' ').replace(/full$/, '').trim();
        const zPart = z != null ? ` Currently ${interpretZScore(z)}` : '';
        return `${clean}${zPart}`;
    }
    const zPart = z != null ? ` ${interpretZScore(z)}` : '';
    return `${ctx.why}${zPart}`;
}

export function getFeatureLabel(name) {
    return FEATURE_CONTEXT[name]?.what || name.replace(/_/g, ' ').replace(/full$/, '').trim();
}

// ── Options Interpretation ──────────────────────────────────────────

export function interpretPCR(pcr) {
    if (pcr == null) return '';
    if (pcr > 1.5) return `Extreme bearish positioning (P/C ${pcr.toFixed(2)}) — heavy put buying, either hedging or directional bets on downside.`;
    if (pcr > 1.2) return `Bearish tilt (P/C ${pcr.toFixed(2)}) — more puts than calls being traded. Could signal fear or smart money hedging.`;
    if (pcr > 0.9) return `Neutral options flow (P/C ${pcr.toFixed(2)}) — balanced put/call activity.`;
    if (pcr > 0.7) return `Bullish tilt (P/C ${pcr.toFixed(2)}) — more calls than puts. Directional optimism or upside speculation.`;
    return `Extreme bullish positioning (P/C ${pcr.toFixed(2)}) — heavy call buying. Could signal greed or a squeeze setup.`;
}

export function interpretIV(iv) {
    if (iv == null) return '';
    const pct = typeof iv === 'number' && iv < 1 ? iv * 100 : iv;
    if (pct > 80) return `Very high IV (${pct.toFixed(0)}%) — options are expensive. Favor selling premium or spreads.`;
    if (pct > 50) return `Elevated IV (${pct.toFixed(0)}%) — above-average uncertainty priced in. Straddles are expensive.`;
    if (pct > 30) return `Normal IV (${pct.toFixed(0)}%) — average implied volatility.`;
    if (pct > 15) return `Low IV (${pct.toFixed(0)}%) — options are cheap. Good environment for buying protection.`;
    return `Very low IV (${pct.toFixed(0)}%) — historically cheap options. Consider buying gamma.`;
}

export function interpretMaxPain(maxPain, currentPrice) {
    if (maxPain == null || currentPrice == null) return '';
    const diff = ((currentPrice - maxPain) / maxPain) * 100;
    if (Math.abs(diff) < 2) return `Price near max pain ($${maxPain.toFixed(0)}) — pinning likely into expiry.`;
    if (diff > 5) return `Trading ${diff.toFixed(1)}% above max pain ($${maxPain.toFixed(0)}) — dealers short gamma, may amplify moves.`;
    if (diff < -5) return `Trading ${Math.abs(diff).toFixed(1)}% below max pain ($${maxPain.toFixed(0)}) — gravitational pull upward into expiry.`;
    return `Near max pain ($${maxPain.toFixed(0)}), ${diff > 0 ? 'slightly above' : 'slightly below'}.`;
}

// ── Correlation Interpretation ──────────────────────────────────────

export function interpretCorrelation(corr, featureA, featureB) {
    if (corr == null) return '';
    const abs = Math.abs(corr);
    const dir = corr > 0 ? 'move together' : 'move opposite';

    if (abs > 0.8) return `Very strong ${corr > 0 ? 'positive' : 'negative'} correlation (${corr.toFixed(2)}) — ${featureA || 'A'} and ${featureB || 'B'} ${dir} almost lockstep. Breaking this would signal a structural change.`;
    if (abs > 0.5) return `Moderate correlation (${corr.toFixed(2)}) — these ${dir} but with meaningful independence.`;
    if (abs > 0.3) return `Weak correlation (${corr.toFixed(2)}) — some relationship but largely independent.`;
    return `Near zero correlation (${corr.toFixed(2)}) — these are independent signals. Both are informative.`;
}

// ── Regime Driver Interpretation ────────────────────────────────────

export function interpretDriver(feature, magnitude, direction) {
    const label = getFeatureLabel(feature);
    const ctx = FEATURE_CONTEXT[feature];
    const dirWord = direction === 'up' ? 'elevated' : 'depressed';
    const impact = Math.abs(magnitude);

    let severity = '';
    if (impact > 2.5) severity = 'This is a dominant driver — ';
    else if (impact > 1.5) severity = 'Significant influence — ';
    else severity = 'Contributing factor — ';

    const explanation = ctx ? ctx.why : '';
    return `${severity}${label} is ${dirWord} at ${impact.toFixed(1)}σ. ${explanation}`;
}
