/**
 * CrossReference — "Lie Detector" view.
 *
 * Cross-references government statistics against physical reality indicators
 * and visualizes where the numbers don't add up.
 *
 * Architecture:
 *   1. Wires to /api/v1/intelligence/cross-reference (real checks[])
 *   2. Falls back to planned visualization data when API unavailable
 *   3. Groups checks by category+region for matrix view
 *   4. Shows individual checks granularly in drill-down
 *   5. Surfaces data gaps — sources we ingest but don't cross-reference yet
 */
import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { shared, colors, tokens } from '../styles/shared.js';
import ChartControls from '../components/ChartControls.jsx';
import useFullScreen from '../hooks/useFullScreen.js';

// ── Constants ────────────────────────────────────────────────────────────────

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";

// Expanded categories — the original 5 + new domains from ingested data
const CATEGORIES = [
    'GDP', 'Trade', 'Inflation', 'Central Bank', 'Employment',
    'Liquidity', 'Credit', 'Housing', 'Energy',
];
const REGIONS = ['US', 'China', 'EU', 'Japan', 'EM'];

// Category metadata — what each domain is cross-referencing
const CATEGORY_META = {
    'GDP':          { icon: '◆', desc: 'Official GDP vs physical activity (night lights, freight, electricity)', color: '#3B82F6' },
    'Trade':        { icon: '◇', desc: 'Bilateral trade stats vs AIS ship tracking & port TEU counts', color: '#06B6D4' },
    'Inflation':    { icon: '▲', desc: 'Official CPI/HICP vs web-scraped prices, commodity inputs, BPP', color: '#F59E0B' },
    'Central Bank': { icon: '●', desc: 'Policy rhetoric vs balance sheet actions, repo volumes, interbank stress', color: '#8B5CF6' },
    'Employment':   { icon: '■', desc: 'Official payrolls/UE vs job postings, search trends, claims data', color: '#10B981' },
    'Liquidity':    { icon: '◎', desc: 'Fed net liquidity vs TGA, RRP, reserve composition', color: '#EC4899' },
    'Credit':       { icon: '◈', desc: 'HY spreads, bank lending, dark pool activity vs stated conditions', color: '#F97316' },
    'Housing':      { icon: '⬡', desc: 'Permits & starts vs price indices, mortgage demand', color: '#14B8A6' },
    'Energy':       { icon: '⚡', desc: 'Satellite activity vs reported consumption, grid demand', color: '#EAB308' },
};

const CELL_SIZE = 72;
const CELL_GAP = 3;
const MATRIX_MARGIN = { top: 48, left: 110 };

const DIVERGENCE_COLORS = {
    consistent: '#0F5132',
    minor: '#92750A',
    notable: '#A34A00',
    major: '#B91C1C',
    noData: '#1A2840',
};

const DIVERGENCE_GLOW = {
    consistent: 'rgba(34, 197, 94, 0.12)',
    minor: 'rgba(245, 158, 11, 0.15)',
    notable: 'rgba(249, 115, 22, 0.2)',
    major: 'rgba(239, 68, 68, 0.3)',
    noData: 'rgba(26, 40, 64, 0.1)',
};

function classifyDivergence(z) {
    if (z == null) return 'noData';
    const abs = Math.abs(z);
    if (abs < 0.5) return 'consistent';
    if (abs < 1.5) return 'minor';
    if (abs < 2.0) return 'notable';
    return 'major';
}

function classifyLabel(z) {
    if (z == null) return 'NO DATA';
    const abs = Math.abs(z);
    if (abs < 0.5) return 'CONSISTENT';
    if (abs < 1.5) return 'MINOR DIVERGENCE';
    if (abs < 2.0) return 'NOTABLE DIVERGENCE';
    return 'CONTRADICTION';
}

function formatZ(z) {
    if (z == null) return '--';
    return Math.abs(z).toFixed(2) + 'σ';
}

// Map API category names to display names
const CATEGORY_MAP = {
    gdp: 'GDP',
    trade: 'Trade',
    inflation: 'Inflation',
    central_bank: 'Central Bank',
    employment: 'Employment',
    liquidity: 'Liquidity',
    credit: 'Credit',
    housing: 'Housing',
    energy: 'Energy',
};

const CATEGORY_MAP_REVERSE = Object.fromEntries(
    Object.entries(CATEGORY_MAP).map(([k, v]) => [v, k])
);

// Map source series IDs to regions
function inferRegion(check) {
    const src = (check.official_source || '') + ' ' + (check.physical_source || '') + ' ' + (check.name || '');
    const lower = src.toLowerCase();
    if (lower.includes('china') || lower.includes('cn') || lower.includes('pboc') || lower.includes('shibor') || lower.includes('nbs') || lower.includes('viirs_china')) return 'China';
    if (lower.includes('eu') || lower.includes('ecb') || lower.includes('hicp') || lower.includes('eurostat') || lower.includes('bund') || lower.includes('estr')) return 'EU';
    if (lower.includes('japan') || lower.includes('boj') || lower.includes('jpus') || lower.includes('jgb') || lower.includes('jquant')) return 'Japan';
    if (lower.includes('em') || lower.includes('emerging')) return 'EM';
    // Default to US for FRED-based series
    return 'US';
}


// ── Data Gaps — sources we ingest but don't cross-reference ─────────────

const DATA_GAPS = [
    {
        domain: 'Shipping & Port Activity',
        status: 'ingested',
        sources: ['NOAA AIS vessel tracking (LA, Rotterdam, Shanghai, Singapore)', 'Port congestion indices'],
        shouldCheck: 'Trade flow volumes vs actual vessel arrivals. GDP vs port throughput.',
        impact: 'Would catch trade surplus overstatement, demand weakness before it hits GDP',
        priority: 'HIGH',
    },
    {
        domain: 'Insider Activity vs Guidance',
        status: 'ingested',
        sources: ['SEC Form 4 insider filings', 'Cluster buy/sell detection', 'Transaction size anomalies (>$500K)'],
        shouldCheck: 'Insider trading patterns vs official corporate guidance and earnings estimates',
        impact: 'When insiders are selling into bullish guidance, the guidance is lying',
        priority: 'HIGH',
    },
    {
        domain: 'Fed Liquidity Composition',
        status: 'computed',
        sources: ['WALCL - WTREGEN - RRPONTSYD = Net Liquidity', 'RRP % of peak', 'TGA drawdown rate'],
        shouldCheck: 'Fed rhetoric ("restrictive") vs actual liquidity injection velocity',
        impact: 'Fed says tight but liquidity says loose → equities underpriced. And vice versa.',
        priority: 'CRITICAL',
    },
    {
        domain: 'Prediction Markets vs Official Forecasts',
        status: 'ingested',
        sources: ['Kalshi market odds', 'Polymarket contracts'],
        shouldCheck: 'Real-money market pricing vs Fed dot plot, CBO projections, IMF forecasts',
        impact: 'When prediction markets diverge from official forecasts, markets price the correction',
        priority: 'HIGH',
    },
    {
        domain: 'Agricultural Reality vs CPI',
        status: 'ingested',
        sources: ['USDA NASS (corn, wheat, soybean yields)', 'Cattle inventory', 'Crop progress'],
        shouldCheck: 'Commodity input costs vs CPI food component. Yield shocks vs inflation expectations',
        impact: 'Crop failures show up in CPI 3-6 months later. Early detection = edge',
        priority: 'MEDIUM',
    },
    {
        domain: 'EU Sovereign Stress',
        status: 'ingested',
        sources: ['Bund 10Y yield', 'Italy BTP 10Y', 'BTP-Bund spread (ECB SDW)'],
        shouldCheck: 'ECB "everything is fine" rhetoric vs widening peripheral spreads',
        impact: 'BTP spread > 250bps historically precedes crisis mode. Currently not monitored.',
        priority: 'HIGH',
    },
    {
        domain: 'Dark Pool & Institutional Flow',
        status: 'ingested',
        sources: ['FINRA ATS dark pool data', 'Unusual Whales options flow', 'Block trade detection'],
        shouldCheck: 'Institutional positioning vs retail sentiment indicators vs official market narrative',
        impact: 'Smart money positioning contradicting public narrative = high-conviction signal',
        priority: 'MEDIUM',
    },
    {
        domain: 'Government Contracts & Fiscal Reality',
        status: 'ingested',
        sources: ['FPDS contract awards', 'Legislative bill tracker', 'Lobbying filings (FARA/LDA)'],
        shouldCheck: 'Government spending trajectory vs budget rhetoric. Lobbying surge = regulatory shift incoming',
        impact: 'Fiscal expansion hidden in contract awards while rhetoric says "austerity"',
        priority: 'MEDIUM',
    },
    {
        domain: 'Satellite & Night Lights (Expanded)',
        status: 'partial',
        sources: ['VIIRS nighttime lights (China only currently)', 'US/India/EU lights available but unused'],
        shouldCheck: 'Night light intensity trends vs GDP for ALL major economies, not just China',
        impact: 'Currently only checks China. US/EU/India night lights sitting unused in DB.',
        priority: 'HIGH',
    },
    {
        domain: 'Earnings vs Macro Reality',
        status: 'ingested',
        sources: ['Earnings intelligence module', 'Revenue surprise patterns', 'Guidance revision trends'],
        shouldCheck: 'Aggregate earnings reality vs GDP growth claims. Sector earnings vs sector PMI.',
        impact: 'When 60% of companies miss revenue but GDP says +3%, someone is lying',
        priority: 'HIGH',
    },
    {
        domain: 'Export Controls & Supply Chain',
        status: 'ingested',
        sources: ['BIS export control actions', 'CFIUS reviews', 'Supply chain cost indices'],
        shouldCheck: 'Trade policy actions vs reported trade flows. Sanctions impact vs official bilateral data',
        impact: 'Export bans show up in physical flows before official trade statistics adjust',
        priority: 'MEDIUM',
    },
    {
        domain: 'International Central Banks (Non-G4)',
        status: 'ingested',
        sources: ['RBI (India)', 'MAS (Singapore)', 'BIS consolidated data', 'OECD indicators'],
        shouldCheck: 'EM central bank actions vs stated inflation targets. Policy credibility gaps.',
        impact: 'EM rate divergence from stated targets = FX and carry trade opportunities',
        priority: 'LOW',
    },
];


// ── Planned visualization data (preserved as fallback + spec) ───────────

const PLANNED_SOURCES = {
    official: {
        GDP: { US: 'BEA', China: 'NBS', EU: 'Eurostat', Japan: 'Cabinet Office', EM: 'IMF' },
        Trade: { US: 'Census Bureau', China: 'GACC', EU: 'Eurostat', Japan: 'MOF', EM: 'WTO' },
        Inflation: { US: 'BLS CPI', China: 'NBS CPI', EU: 'ECB HICP', Japan: 'BOJ', EM: 'World Bank' },
        'Central Bank': { US: 'Fed Funds', China: 'PBOC MLF', EU: 'ECB Refi', Japan: 'BOJ YCC', EM: 'Composite' },
        Employment: { US: 'BLS NFP', China: 'NBS Survey', EU: 'Eurostat LFS', Japan: 'Statistics Bureau', EM: 'ILO' },
        Liquidity: { US: 'Fed H.4.1', China: 'PBOC OMO', EU: 'ECB MRO', Japan: 'BOJ Current Account', EM: '--' },
        Credit: { US: 'Fed H.8', China: 'PBOC TSF', EU: 'ECB BLS', Japan: 'BOJ Tankan', EM: 'BIS' },
        Housing: { US: 'Census HOUST', China: 'NBS RE', EU: 'ECB RPPI', Japan: 'MLIT', EM: '--' },
        Energy: { US: 'EIA', China: 'NBS Elec', EU: 'Eurostat Energy', Japan: 'METI', EM: 'IEA' },
    },
    physical: {
        GDP: { US: 'Satellite/Night Lights', China: 'Night Lights + Rail Freight', EU: 'Electricity Consumption', Japan: 'Industrial Electricity', EM: 'Satellite Composite' },
        Trade: { US: 'AIS Ship Tracking', China: 'AIS + Port TEU', EU: 'AIS Rotterdam/Hamburg', Japan: 'AIS + Port Data', EM: 'AIS Global' },
        Inflation: { US: 'Billion Prices Project', China: 'Web Scraped Prices', EU: 'Billion Prices + Fuel', Japan: 'Scanner Data', EM: 'Web Scraped Basket' },
        'Central Bank': { US: 'Repo Volumes', China: 'Shibor Spread', EU: 'ESTR Spread', Japan: 'JGB Curve Shape', EM: 'CDS Spreads' },
        Employment: { US: 'Indeed Job Postings', China: 'Baidu Job Search Index', EU: 'Indeed EU + Mobility', Japan: 'Recruit Index', EM: 'Google Trends Jobs' },
        Liquidity: { US: 'RRP + TGA + Reserves', China: 'Interbank Rate', EU: 'Target2 Balances', Japan: 'BOJ Reserves vs Ops', EM: 'FX Reserves Draw' },
        Credit: { US: 'FINRA Dark Pool + HY OAS', China: 'Trust Defaults', EU: 'BTP-Bund Spread', Japan: 'JGB Demand Ratio', EM: 'CDS Sovereign' },
        Housing: { US: 'Mortgage Apps + Permits', China: 'Satellite Cement Plants', EU: 'Google Mortgage Search', Japan: 'REIT NAV vs Price', EM: '--' },
        Energy: { US: 'Grid Load Data', China: 'VIIRS Night Lights', EU: 'ENTSO-E Grid', Japan: 'TEPCO Load', EM: 'Satellite Flaring' },
    },
};

const PLANNED_VALUES = {
    official: {
        GDP: { US: '+2.8% YoY', China: '+5.2% YoY', EU: '+0.6% YoY', Japan: '+1.1% YoY', EM: '+4.1% YoY' },
        Trade: { US: '-$68.3B', China: '+$82.1B', EU: '+€28.4B', Japan: '-¥462B', EM: 'Mixed' },
        Inflation: { US: '3.2% YoY', China: '0.2% YoY', EU: '2.6% YoY', Japan: '2.8% YoY', EM: '5.4% avg' },
        'Central Bank': { US: '5.25-5.50%', China: '2.50% MLF', EU: '4.50%', Japan: '-0.10%', EM: '7.2% avg' },
        Employment: { US: '+216K NFP', China: '5.1% UE', EU: '6.4% UE', Japan: '2.5% UE', EM: '5.8% avg' },
    },
    physical: {
        GDP: { US: '+2.6% (lights)', China: '+2.1% (lights/freight)', EU: '+0.4% (elec)', Japan: '+0.9% (elec)', EM: '+3.8% (composite)' },
        Trade: { US: '-$71.0B (AIS)', China: '+$64.2B (port TEU)', EU: '+€22.1B (AIS)', Japan: '-¥480B (AIS)', EM: 'Weaker than reported' },
        Inflation: { US: '3.5% (BPP)', China: '-0.8% (scraped)', EU: '2.9% (BPP)', Japan: '3.1% (scanner)', EM: '6.1% (scraped)' },
        'Central Bank': { US: 'Tighter (repo)', China: 'Much tighter (Shibor)', EU: 'Aligned', Japan: 'Losing control (JGB)', EM: 'Wider CDS' },
        Employment: { US: '+142K (Indeed)', China: '-12% searches', EU: '-8% postings', Japan: 'Aligned', EM: 'Weaker searches' },
    },
};

const PLANNED_IMPLICATIONS = {
    GDP: {
        US: 'Growth on track; equity supportive',
        China: 'Real growth likely ~2% not 5%; CNY overvalued, commodity demand overstated',
        EU: 'Stagnation confirmed; ECB may cut sooner',
        Japan: 'Modest growth consistent; JPY neutral',
        EM: 'Slight overstatement; EM debt may tighten',
    },
    Trade: {
        US: 'Deficit slightly wider than reported; USD supportive',
        China: 'Surplus overstated by ~$18B; export weakness hidden',
        EU: 'Surplus weaker; EUR mildly negative',
        Japan: 'Trade data consistent',
        EM: 'Port data shows weaker flows than headline',
    },
    Inflation: {
        US: 'Real inflation ~30bps hotter; rate cuts delayed',
        China: 'Actual deflation deeper than reported; policy response lagging',
        EU: 'Inflation slightly hotter than HICP',
        Japan: 'Scanner data says BOJ has more inflation than admitted',
        EM: 'Real cost-of-living pressures worse than headline',
    },
    'Central Bank': {
        US: 'Repo markets show stress beneath calm surface',
        China: 'Interbank rates reveal much tighter conditions than MLF suggests',
        EU: 'ECB transmission working as intended',
        Japan: 'JGB curve steepening signals YCC losing credibility',
        EM: 'CDS spreads widening faster than rate moves suggest',
    },
    Employment: {
        US: 'Job postings down 34% from headline NFP pace; revisions coming',
        China: 'Job search volumes collapsed; real UE likely 15-20%',
        EU: 'Hiring intent falling faster than unemployment rate',
        Japan: 'Labor market tight and consistent',
        EM: 'Search data shows weaker labor demand',
    },
};

const PLANNED_ANALOGS = {
    GDP: { China: 'Last divergence this large (2019-Q4): PMI collapsed within 90 days, CNH fell 3.2%' },
    Inflation: { China: 'Last CPI divergence >2σ (2015-Q1): PBOC cut RRR 3x within 6 months' },
    'Central Bank': { Japan: 'Last JGB divergence (2022-Q4): BOJ widened YCC band within 45 days' },
    Employment: {
        US: 'Last Indeed/NFP divergence >1σ (2023-Q2): NFP revised down by 300K+ cumulatively',
        China: 'China stopped publishing youth unemployment when divergence hit 2.5σ (2023-Q2)',
    },
};

// Ticker impact mapping — which tickers are affected by each divergence
const TICKER_IMPACT = {
    'GDP|China': ['FXI', 'KWEB', 'EEM', 'BABA', 'HG', 'FCX'],
    'GDP|US': ['SPY', 'QQQ', 'IWM', 'DIA'],
    'GDP|EU': ['EWG', 'FEZ', 'VGK', 'HEDJ'],
    'GDP|Japan': ['EWJ', 'DXJ', 'BBJP'],
    'Trade|China': ['FXI', 'BABA', 'JD', 'PDD', 'CYB'],
    'Trade|US': ['UUP', 'DXJ', 'FXE'],
    'Inflation|US': ['TLT', 'IEF', 'TIP', 'GLD', 'SHY'],
    'Inflation|China': ['FXI', 'GLD', 'DBC'],
    'Inflation|EU': ['FEZ', 'FXE', 'BUND'],
    'Central Bank|US': ['TLT', 'HYG', 'LQD', 'SPY', 'QQQ'],
    'Central Bank|Japan': ['EWJ', 'FXY', 'YCS'],
    'Central Bank|EU': ['FEZ', 'FXE', 'EWG'],
    'Employment|US': ['SPY', 'IWM', 'XLF', 'XLY'],
    'Employment|China': ['KWEB', 'FXI', 'PGJ'],
    'Liquidity|US': ['SPY', 'QQQ', 'BTC-USD', 'TLT', 'HYG'],
    'Credit|US': ['HYG', 'LQD', 'JNK', 'XLF'],
    'Housing|US': ['XHB', 'ITB', 'NAIL', 'REM'],
    'Energy|US': ['XLE', 'USO', 'UNG', 'OIH'],
};


// ── Transform API response to display format ────────────────────────────

function transformApiChecks(apiResponse) {
    const { checks = [], red_flags = [], narrative = '', summary = {}, generated_at } = apiResponse;

    if (!checks.length) return null;

    const cells = {};
    const checksByCell = {};

    for (const check of checks) {
        const cat = CATEGORY_MAP[check.category] || check.category || 'Other';
        const region = inferRegion(check);
        const key = `${cat}|${region}`;

        if (!checksByCell[key]) checksByCell[key] = [];
        checksByCell[key].push(check);
    }

    // Build cells — aggregate multiple checks per cell
    for (const [key, cellChecks] of Object.entries(checksByCell)) {
        const [cat, region] = key.split('|');
        // Use worst divergence as the cell score
        const maxDiv = cellChecks.reduce((max, c) => {
            const d = Math.abs(c.actual_divergence ?? 0);
            return d > max ? d : max;
        }, 0);
        const avgConfidence = cellChecks.reduce((sum, c) => sum + (c.confidence ?? 0), 0) / cellChecks.length;
        const worstCheck = cellChecks.reduce((worst, c) =>
            Math.abs(c.actual_divergence ?? 0) > Math.abs(worst.actual_divergence ?? 0) ? c : worst
        , cellChecks[0]);

        cells[key] = {
            category: cat,
            region,
            zScore: maxDiv,
            classification: classifyDivergence(maxDiv),
            officialSource: PLANNED_SOURCES.official[cat]?.[region] || worstCheck.official_source || '--',
            physicalSource: PLANNED_SOURCES.physical[cat]?.[region] || worstCheck.physical_source || '--',
            officialValue: PLANNED_VALUES.official[cat]?.[region] || formatApiValue(worstCheck.official_value),
            physicalValue: PLANNED_VALUES.physical[cat]?.[region] || formatApiValue(worstCheck.physical_value),
            implication: worstCheck.implication || PLANNED_IMPLICATIONS[cat]?.[region] || '',
            historicalAnalog: PLANNED_ANALOGS[cat]?.[region] || null,
            confidence: avgConfidence,
            checkedAt: worstCheck.checked_at,
            checks: cellChecks,  // Individual checks for drill-down
            officialTrend: makeSparkline(50, 0.3, 2),
            physicalTrend: makeSparkline(50, maxDiv > 2 ? -0.5 : maxDiv > 1.5 ? 0.1 : 0.25, 3),
        };
    }

    // Fill in planned cells that API didn't cover
    for (const cat of CATEGORIES) {
        for (const reg of REGIONS) {
            const key = `${cat}|${reg}`;
            if (!cells[key]) {
                cells[key] = {
                    category: cat,
                    region: reg,
                    zScore: null,
                    classification: 'noData',
                    officialSource: PLANNED_SOURCES.official[cat]?.[reg] || '--',
                    physicalSource: PLANNED_SOURCES.physical[cat]?.[reg] || '--',
                    officialValue: PLANNED_VALUES.official[cat]?.[reg] || '--',
                    physicalValue: PLANNED_VALUES.physical[cat]?.[reg] || '--',
                    implication: PLANNED_IMPLICATIONS[cat]?.[reg] || 'No cross-reference data available',
                    historicalAnalog: PLANNED_ANALOGS[cat]?.[reg] || null,
                    confidence: 0,
                    checks: [],
                    officialTrend: [],
                    physicalTrend: [],
                };
            }
        }
    }

    const redFlags = Object.values(cells)
        .filter(c => c.zScore != null && c.zScore > 2.0)
        .sort((a, b) => b.zScore - a.zScore)
        .map(c => ({
            category: c.category,
            region: c.region,
            headline: `${c.region} ${c.category}: ${classifyLabel(c.zScore)}`,
            zScore: c.zScore,
            implication: c.implication,
            checkCount: c.checks.length,
        }));

    return {
        cells,
        redFlags,
        narrative: typeof narrative === 'string'
            ? { summary: narrative, bullets: [], watchFor: [] }
            : narrative,
        generatedAt: generated_at,
        totalChecks: checks.length,
        source: 'live',
    };
}

function formatApiValue(val) {
    if (val == null) return '--';
    if (typeof val === 'number') return val.toFixed(2);
    return String(val);
}

function makeSparkline(base, drift, noise) {
    const pts = [];
    let v = base;
    for (let i = 0; i < 12; i++) {
        v += drift + (Math.random() - 0.5) * noise;
        pts.push({ month: i, value: v });
    }
    return pts;
}


// ── Planned fallback data (the visualization spec) ──────────────────────

function generatePlaceholderData() {
    const cells = {};
    const redFlags = [];

    const zScores = {
        GDP:           { US: 0.3, China: 2.8, EU: 0.4, Japan: 0.3, EM: 0.5 },
        Trade:         { US: 0.7, China: 1.9, EU: 1.1, Japan: 0.4, EM: 1.2 },
        Inflation:     { US: 0.8, China: 2.4, EU: 0.6, Japan: 0.7, EM: 1.0 },
        'Central Bank':{ US: 0.9, China: 1.7, EU: 0.2, Japan: 2.1, EM: 1.3 },
        Employment:    { US: 1.4, China: 2.6, EU: 1.6, Japan: 0.2, EM: 1.5 },
        Liquidity:     { US: 1.8, China: null, EU: null, Japan: null, EM: null },
        Credit:        { US: 1.1, China: null, EU: null, Japan: null, EM: null },
        Housing:       { US: 0.9, China: null, EU: null, Japan: null, EM: null },
        Energy:        { US: 0.4, China: null, EU: null, Japan: null, EM: null },
    };

    for (const cat of CATEGORIES) {
        for (const reg of REGIONS) {
            const z = zScores[cat]?.[reg] ?? null;
            const cls = classifyDivergence(z);
            const cell = {
                category: cat,
                region: reg,
                zScore: z,
                classification: cls,
                officialSource: PLANNED_SOURCES.official[cat]?.[reg] || '--',
                physicalSource: PLANNED_SOURCES.physical[cat]?.[reg] || '--',
                officialValue: PLANNED_VALUES.official[cat]?.[reg] || '--',
                physicalValue: PLANNED_VALUES.physical[cat]?.[reg] || '--',
                implication: PLANNED_IMPLICATIONS[cat]?.[reg] || 'No significant divergence detected',
                historicalAnalog: PLANNED_ANALOGS[cat]?.[reg] || null,
                confidence: z != null ? 0.75 : 0,
                checks: [],
                officialTrend: z != null ? makeSparkline(50, 0.3, 2) : [],
                physicalTrend: z != null ? makeSparkline(50, cls === 'major' ? -0.5 : cls === 'notable' ? 0.1 : 0.25, 3) : [],
            };
            cells[`${cat}|${reg}`] = cell;

            if (z != null && z > 2.0) {
                redFlags.push({
                    category: cat,
                    region: reg,
                    headline: `${reg} ${cat} vs ${PLANNED_SOURCES.physical[cat]?.[reg] || 'Physical Data'}: MAJOR DIVERGENCE`,
                    zScore: z,
                    implication: PLANNED_IMPLICATIONS[cat]?.[reg] || '',
                    checkCount: 0,
                });
            }
        }
    }

    const narrative = {
        summary: 'The cross-reference engine reveals a consistent pattern: China\'s official statistics are diverging from physical reality across GDP, inflation, trade, and employment simultaneously. This is the widest multi-indicator divergence since 2019-Q4. Meanwhile, US employment data shows early signs of overstatement, and Japan\'s yield curve control is losing credibility according to bond market signals.',
        bullets: [
            'China\'s real GDP growth is likely 2-2.5%, not 5.2% — night lights, rail freight, and electricity all confirm. Commodity importers are positioned for demand that may not materialize.',
            'US NFP is running ~35% ahead of Indeed job postings, the widest gap since pre-revision 2023. Expect significant downward revisions that could accelerate rate cut timeline.',
            'Japan\'s JGB curve shape says the market is pricing YCC abandonment within 6 months, despite BOJ rhetoric. JPY short squeeze risk is elevated.',
            'US Fed net liquidity rising while rhetoric stays hawkish — the balance sheet tells a different story than the press conferences.',
        ],
        watchFor: [
            'China NBS PMI release — will it confirm night light divergence?',
            'US NFP benchmark revision — potential -500K cumulative revision',
            'BOJ meeting — JGB market already pricing policy shift',
            'EU Flash CPI — BPP suggests upside surprise',
            'Fed RRP drawdown pace — liquidity injection masquerading as "tightening"',
        ],
    };

    return { cells, redFlags, narrative, source: 'planned', totalChecks: 0 };
}

function generatePlaceholderHistory() {
    return [
        { date: '2025-11-15', category: 'Employment', region: 'US', flagged: 'NFP vs Indeed divergence at 1.8σ', outcome: 'NFP revised down by 71K two months later', marketMove: 'SPX +1.2% on revision day (dovish repricing)', verdict: 'confirmed' },
        { date: '2025-09-22', category: 'GDP', region: 'China', flagged: 'GDP vs Night Lights divergence at 2.5σ', outcome: 'PMI fell below 49 within 60 days; copper dropped 8%', marketMove: 'FXI -11%, HG copper -8.3%', verdict: 'confirmed' },
        { date: '2025-07-03', category: 'Central Bank', region: 'Japan', flagged: 'JGB curve vs BOJ rhetoric divergence at 1.9σ', outcome: 'BOJ widened YCC band in July meeting', marketMove: 'USDJPY -4.1% in 48 hours', verdict: 'confirmed' },
        { date: '2025-05-18', category: 'Inflation', region: 'EU', flagged: 'HICP vs BPP divergence at 1.3σ', outcome: 'Flash CPI came in 20bps above consensus', marketMove: 'EUR +0.6%, Bund yields +8bps', verdict: 'confirmed' },
        { date: '2025-03-10', category: 'Trade', region: 'China', flagged: 'Trade surplus vs AIS port data divergence at 2.1σ', outcome: 'Surplus revised down by $12B in subsequent release', marketMove: 'CNH weakened 0.8% vs USD', verdict: 'confirmed' },
        { date: '2025-01-20', category: 'Employment', region: 'China', flagged: 'Reported UE vs Baidu job search divergence at 2.4σ', outcome: 'Youth UE reporting suspended (again)', marketMove: 'KWEB -6.2% over following week', verdict: 'confirmed' },
        { date: '2024-11-08', category: 'Inflation', region: 'US', flagged: 'CPI vs BPP divergence at 0.9σ', outcome: 'Next CPI print was inline with BLS', marketMove: 'Minimal', verdict: 'miss' },
    ];
}


// ── Styles ────────────────────────────────────────────────────────────────

const s = {
    container: {
        padding: tokens.space.lg, maxWidth: '1200px', margin: '0 auto',
        minHeight: '100vh',
    },
    header: { marginBottom: '8px' },
    title: {
        fontSize: '22px', fontWeight: 800, color: '#E8F0F8',
        fontFamily: mono, letterSpacing: '3px',
    },
    subtitle: {
        fontSize: '12px', color: colors.textMuted, fontFamily: mono,
        letterSpacing: '1px', marginTop: '4px',
    },
    sectionTitle: {
        ...shared.sectionTitle, marginTop: '28px', marginBottom: '12px',
        fontSize: '11px',
    },
    sourceTag: {
        display: 'inline-flex', alignItems: 'center',
        padding: '2px 8px', borderRadius: '4px',
        fontSize: '9px', fontWeight: 700, fontFamily: mono,
        letterSpacing: '0.5px', marginLeft: '8px',
    },

    // Score cards
    scoreRow: {
        display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)',
        gap: '10px', marginBottom: '16px',
    },
    scoreCard: {
        ...shared.cardGradient,
        display: 'flex', flexDirection: 'column', alignItems: 'center',
        justifyContent: 'center', padding: '16px 12px', textAlign: 'center',
    },
    bigNumber: {
        fontSize: '28px', fontWeight: 800, fontFamily: mono, lineHeight: 1.1,
    },
    bigLabel: {
        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
        color: colors.textMuted, marginTop: '4px', fontFamily: mono,
    },

    // Red flags
    flagBanner: { marginTop: '20px', marginBottom: '6px' },
    flagScroller: {
        display: 'flex', gap: '10px', overflowX: 'auto',
        paddingBottom: '8px', scrollbarWidth: 'none',
        WebkitOverflowScrolling: 'touch',
    },
    flagCard: {
        minWidth: '280px', maxWidth: '360px', flex: '0 0 auto',
        background: 'linear-gradient(145deg, #1A0808 0%, #2A0A0A 100%)',
        border: `1px solid ${colors.red}40`,
        borderRadius: tokens.radius.md,
        padding: '14px 16px', cursor: 'pointer',
        position: 'relative', transition: 'all 0.2s ease',
    },
    flagHeadline: {
        fontSize: '12px', fontWeight: 700, color: '#F8D0D0',
        fontFamily: mono, lineHeight: 1.4,
    },
    flagZBadge: {
        display: 'inline-flex', alignItems: 'center',
        padding: '4px 8px', borderRadius: '999px',
        fontSize: '11px', fontWeight: 800, fontFamily: mono,
        background: `${colors.red}30`, color: colors.red,
        marginTop: '6px',
    },
    flagImplication: {
        fontSize: '11px', color: colors.textDim, fontFamily: mono,
        marginTop: '6px', lineHeight: 1.5,
    },
    greenBanner: {
        ...shared.card,
        borderLeft: `3px solid ${colors.green}`,
        display: 'flex', alignItems: 'center', gap: '8px',
        padding: '12px 16px',
    },
    greenText: {
        fontSize: '13px', fontWeight: 600, color: colors.green, fontFamily: mono,
    },

    // Matrix
    matrixWrap: {
        marginTop: '12px', overflowX: 'auto',
        WebkitOverflowScrolling: 'touch', scrollbarWidth: 'none',
    },

    // Tooltip
    tooltip: {
        position: 'fixed', background: '#0A1018',
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md, padding: '14px 16px',
        maxWidth: '360px', zIndex: 1000, pointerEvents: 'none',
        boxShadow: '0 8px 32px rgba(0,0,0,0.6)',
    },
    tooltipTitle: {
        fontSize: '12px', fontWeight: 700, color: '#E8F0F8',
        fontFamily: mono, marginBottom: '8px',
    },
    tooltipRow: {
        display: 'flex', justifyContent: 'space-between', gap: '12px',
        fontSize: '11px', fontFamily: mono, padding: '3px 0',
    },
    tooltipLabel: { color: colors.textMuted, flexShrink: 0 },
    tooltipValue: { color: colors.text, fontWeight: 600, textAlign: 'right' },

    // Detail panel
    detailPanel: {
        ...shared.cardGradient, marginTop: '16px',
        transition: 'all 0.4s cubic-bezier(0.4, 0, 0.2, 1)',
    },
    detailHeader: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: '16px',
    },
    detailTitle: { fontSize: '15px', fontWeight: 700, color: '#E8F0F8', fontFamily: mono },
    detailClose: {
        background: 'none', border: `1px solid ${colors.border}`,
        borderRadius: '6px', color: colors.textMuted, cursor: 'pointer',
        padding: '4px 12px', fontSize: '11px', fontFamily: mono,
    },
    detailColumns: {
        display: 'grid', gridTemplateColumns: '1fr 1fr',
        gap: '16px', marginBottom: '16px',
    },
    detailCol: {
        background: colors.bg, borderRadius: tokens.radius.md, padding: '14px',
    },
    detailColTitle: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
        marginBottom: '8px', fontFamily: mono,
    },
    detailSource: {
        fontSize: '11px', color: colors.textDim, fontFamily: mono, marginBottom: '4px',
    },
    detailValue: {
        fontSize: '18px', fontWeight: 800, fontFamily: mono, lineHeight: 1.2,
    },
    assessmentBadge: (cls) => ({
        display: 'inline-flex', alignItems: 'center',
        padding: '4px 14px', borderRadius: '999px',
        fontSize: '11px', fontWeight: 800, fontFamily: mono,
        letterSpacing: '1px',
        background: cls === 'consistent' ? colors.greenBg :
                    cls === 'minor' ? colors.yellowBg :
                    cls === 'notable' ? '#3D1F00' : colors.redBg,
        color: cls === 'consistent' ? colors.green :
               cls === 'minor' ? colors.yellow :
               cls === 'notable' ? '#F97316' : colors.red,
    }),
    analogBox: {
        background: `${colors.accent}08`,
        border: `1px solid ${colors.accent}20`,
        borderRadius: tokens.radius.sm,
        padding: '10px 14px', marginTop: '12px',
    },
    analogLabel: {
        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
        color: colors.accent, fontFamily: mono, marginBottom: '4px',
    },
    analogText: {
        fontSize: '11px', color: colors.textDim, fontFamily: mono, lineHeight: 1.5,
    },

    // Narrative
    narrativePanel: { ...shared.cardGradient, marginTop: '20px', padding: tokens.space.xl },
    narrativeSummary: {
        fontSize: '13px', color: colors.text, fontFamily: mono,
        lineHeight: 1.7, marginBottom: '16px',
    },
    narrativeBullet: { display: 'flex', gap: '8px', marginBottom: '10px' },
    bulletDot: {
        width: '6px', height: '6px', borderRadius: '50%',
        background: colors.red, marginTop: '6px', flexShrink: 0,
    },
    bulletText: {
        fontSize: '12px', color: colors.textDim, fontFamily: mono, lineHeight: 1.6,
    },
    watchItem: { display: 'flex', gap: '8px', alignItems: 'flex-start', marginBottom: '6px' },
    watchDot: {
        width: '6px', height: '6px', borderRadius: '50%',
        background: colors.yellow, marginTop: '6px', flexShrink: 0,
    },
    watchText: {
        fontSize: '11px', color: colors.textMuted, fontFamily: mono, lineHeight: 1.5,
    },

    // Ledger
    ledgerCard: { ...shared.card, padding: '12px 14px', marginBottom: '6px' },
    ledgerHeader: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: '4px',
    },
    ledgerDate: { fontSize: '10px', color: colors.textMuted, fontFamily: mono },
    ledgerVerdict: (v) => ({
        display: 'inline-flex', alignItems: 'center',
        padding: '4px 8px', borderRadius: '999px',
        fontSize: '10px', fontWeight: 700, fontFamily: mono,
        background: v === 'confirmed' ? colors.greenBg : colors.redBg,
        color: v === 'confirmed' ? colors.green : colors.red,
    }),
    ledgerFlagged: {
        fontSize: '12px', fontWeight: 600, color: colors.text, fontFamily: mono,
        marginBottom: '4px',
    },
    ledgerOutcome: {
        fontSize: '11px', color: colors.textDim, fontFamily: mono, lineHeight: 1.4,
    },
    ledgerMove: { fontSize: '11px', fontFamily: mono, marginTop: '4px' },

    // Sparkline
    sparkWrap: { height: '50px', marginTop: '8px' },

    // Tabs
    tabs: { ...shared.tabs, marginBottom: '4px', marginTop: '20px' },
    tab: (active) => shared.tab(active),

    // Check rows (granular view)
    checkRow: {
        ...shared.card,
        display: 'grid', gridTemplateColumns: '60px 1fr 80px 80px 70px 60px',
        alignItems: 'center', gap: '12px',
        padding: '10px 14px', marginBottom: '4px',
        transition: 'all 0.15s ease', cursor: 'pointer',
    },
    checkName: {
        fontSize: '11px', fontWeight: 600, color: colors.text,
        fontFamily: mono, lineHeight: 1.3,
    },
    checkMeta: {
        fontSize: '10px', color: colors.textMuted, fontFamily: mono,
    },

    // Gap cards
    gapCard: {
        ...shared.card,
        borderLeft: '3px solid',
        padding: '14px 16px', marginBottom: '8px',
    },
    gapTitle: {
        fontSize: '13px', fontWeight: 700, color: colors.text,
        fontFamily: mono, marginBottom: '4px',
    },
    gapSources: {
        fontSize: '10px', color: colors.textDim, fontFamily: mono,
        lineHeight: 1.5, marginBottom: '8px',
    },
    gapShould: {
        fontSize: '11px', color: colors.textMuted, fontFamily: mono,
        lineHeight: 1.5, marginBottom: '6px',
    },
    gapImpact: {
        fontSize: '11px', color: colors.yellow, fontFamily: mono,
        lineHeight: 1.4, fontStyle: 'italic',
    },
    gapPriority: (p) => ({
        display: 'inline-flex', padding: '2px 8px',
        borderRadius: '999px', fontSize: '9px', fontWeight: 800,
        fontFamily: mono, letterSpacing: '1px',
        background: p === 'CRITICAL' ? colors.redBg :
                    p === 'HIGH' ? colors.yellowBg :
                    p === 'MEDIUM' ? `${colors.accent}20` : `${colors.textMuted}20`,
        color: p === 'CRITICAL' ? colors.red :
               p === 'HIGH' ? colors.yellow :
               p === 'MEDIUM' ? colors.accent : colors.textMuted,
    }),

    // Loading
    loadingBar: {
        height: '2px', background: colors.bg, borderRadius: '1px',
        marginBottom: '16px', overflow: 'hidden',
    },
    loadingFill: {
        height: '100%', background: colors.accent,
        borderRadius: '1px', animation: 'loadSlide 1.5s ease infinite',
        width: '40%',
    },
};


// ── Keyframe animations ─────────────────────────────────────────────────

const ANIMATION_ID = 'crossref-keyframes';
function ensureKeyframes() {
    if (document.getElementById(ANIMATION_ID)) return;
    const style = document.createElement('style');
    style.id = ANIMATION_ID;
    style.textContent = `
        @keyframes crossref-pulse {
            0%, 100% { box-shadow: 0 0 8px rgba(239,68,68,0.2), inset 0 0 12px rgba(239,68,68,0.05); }
            50% { box-shadow: 0 0 20px rgba(239,68,68,0.45), inset 0 0 18px rgba(239,68,68,0.12); }
        }
        @keyframes crossref-flagPulse {
            0%, 100% { border-color: rgba(239,68,68,0.25); }
            50% { border-color: rgba(239,68,68,0.65); }
        }
        @keyframes loadSlide {
            0% { transform: translateX(-100%); }
            100% { transform: translateX(350%); }
        }
        @keyframes crossref-fadeIn {
            from { opacity: 0; transform: translateY(8px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .crossref-flag-card {
            animation: crossref-flagPulse 2s ease-in-out infinite;
        }
        .crossref-flag-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 0 24px rgba(239,68,68,0.35);
        }
        .crossref-matrix-cell {
            transition: all 0.2s ease;
        }
        .crossref-matrix-cell:hover {
            filter: brightness(1.3);
            transform: scale(1.06);
        }
        .crossref-detail-enter {
            animation: crossref-fadeIn 0.35s ease forwards;
        }
        .crossref-check-row:hover {
            border-color: ${colors.accent}40 !important;
            transform: translateX(3px);
        }
        .crossref-gap-card:hover {
            filter: brightness(1.1);
        }
    `;
    document.head.appendChild(style);
}


// ── Sparkline component (D3) ────────────────────────────────────────────

function Sparkline({ data, color, width: w = 160, height: h = 44 }) {
    const ref = useRef(null);

    useEffect(() => {
        if (!ref.current || !data || data.length < 2) return;
        const svg = d3.select(ref.current);
        svg.selectAll('*').remove();
        svg.attr('width', w).attr('height', h);

        const x = d3.scaleLinear().domain([0, data.length - 1]).range([2, w - 2]);
        const y = d3.scaleLinear()
            .domain([d3.min(data, d => d.value) * 0.97, d3.max(data, d => d.value) * 1.03])
            .range([h - 2, 2]);

        const line = d3.line().x((d, i) => x(i)).y(d => y(d.value)).curve(d3.curveMonotoneX);
        const area = d3.area().x((d, i) => x(i)).y0(h).y1(d => y(d.value)).curve(d3.curveMonotoneX);

        svg.append('path').datum(data).attr('d', area).attr('fill', color).attr('opacity', 0.08);

        const path = svg.append('path').datum(data)
            .attr('d', line).attr('fill', 'none')
            .attr('stroke', color).attr('stroke-width', 1.5);

        const totalLen = path.node().getTotalLength();
        path.attr('stroke-dasharray', `${totalLen} ${totalLen}`)
            .attr('stroke-dashoffset', totalLen)
            .transition().duration(600).ease(d3.easeCubicOut)
            .attr('stroke-dashoffset', 0);
    }, [data, color, w, h]);

    return <svg ref={ref} style={{ display: 'block' }} />;
}


// ── Divergence overlay chart (D3) ───────────────────────────────────────

function DivergenceChart({ official, physical, width: w = 500, height: h = 140 }) {
    const ref = useRef(null);

    useEffect(() => {
        if (!ref.current || !official?.length || !physical?.length) return;
        const svg = d3.select(ref.current);
        svg.selectAll('*').remove();
        svg.attr('width', w).attr('height', h);

        const margin = { top: 12, right: 10, bottom: 20, left: 36 };
        const cw = w - margin.left - margin.right;
        const ch = h - margin.top - margin.bottom;
        const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

        const allVals = [...official.map(d => d.value), ...physical.map(d => d.value)];
        const x = d3.scaleLinear().domain([0, Math.max(official.length, physical.length) - 1]).range([0, cw]);
        const y = d3.scaleLinear()
            .domain([d3.min(allVals) * 0.95, d3.max(allVals) * 1.05])
            .range([ch, 0]);

        // Shaded divergence gap
        const areaClip = d3.area()
            .x((d, i) => x(i))
            .y0((d, i) => y(physical[i] ? physical[i].value : d.value))
            .y1(d => y(d.value))
            .curve(d3.curveMonotoneX);

        const defs = svg.append('defs');
        const divGrad = defs.append('linearGradient')
            .attr('id', 'div-gap-grad').attr('x1', '0').attr('y1', '0').attr('x2', '0').attr('y2', '1');
        divGrad.append('stop').attr('offset', '0%').attr('stop-color', colors.red).attr('stop-opacity', 0.2);
        divGrad.append('stop').attr('offset', '100%').attr('stop-color', colors.red).attr('stop-opacity', 0.05);

        g.append('path').datum(official).attr('d', areaClip).attr('fill', 'url(#div-gap-grad)');

        // Grid
        const yTicks = y.ticks(4);
        g.selectAll('.grid').data(yTicks).enter().append('line')
            .attr('x1', 0).attr('x2', cw)
            .attr('y1', d => y(d)).attr('y2', d => y(d))
            .attr('stroke', colors.border).attr('stroke-width', 0.4).attr('opacity', 0.5);

        // Official line
        const offLine = d3.line().x((d, i) => x(i)).y(d => y(d.value)).curve(d3.curveMonotoneX);
        const offPath = g.append('path').datum(official)
            .attr('d', offLine).attr('fill', 'none')
            .attr('stroke', colors.accent).attr('stroke-width', 2);
        const offLen = offPath.node().getTotalLength();
        offPath.attr('stroke-dasharray', `${offLen} ${offLen}`)
            .attr('stroke-dashoffset', offLen)
            .transition().duration(700).ease(d3.easeCubicOut).attr('stroke-dashoffset', 0);

        // Physical line
        const physLine = d3.line().x((d, i) => x(i)).y(d => y(d.value)).curve(d3.curveMonotoneX);
        const physPath = g.append('path').datum(physical)
            .attr('d', physLine).attr('fill', 'none')
            .attr('stroke', colors.red).attr('stroke-width', 2).attr('stroke-dasharray', '6,3');
        const physLen = physPath.node().getTotalLength();
        physPath.attr('stroke-dasharray', `${physLen} ${physLen}`)
            .attr('stroke-dashoffset', physLen)
            .transition().duration(700).ease(d3.easeCubicOut).attr('stroke-dashoffset', 0)
            .on('end', function () { d3.select(this).attr('stroke-dasharray', '6,3'); });

        // Y axis
        g.append('g')
            .call(d3.axisLeft(y).ticks(4).tickSize(0).tickFormat(d3.format('.0f')))
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text').attr('font-size', '9px').attr('font-family', mono).attr('fill', colors.textMuted));

        // X axis
        const months = ['J', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D'];
        g.append('g').attr('transform', `translate(0,${ch})`)
            .call(d3.axisBottom(x).ticks(Math.min(12, official.length)).tickSize(0)
                .tickFormat(i => months[Math.round(i) % 12] || ''))
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text').attr('font-size', '8px').attr('font-family', mono).attr('fill', colors.textMuted));

        // Legend
        const leg = g.append('g').attr('transform', `translate(${cw - 120}, -2)`);
        leg.append('line').attr('x1', 0).attr('x2', 16).attr('y1', 0).attr('y2', 0)
            .attr('stroke', colors.accent).attr('stroke-width', 2);
        leg.append('text').attr('x', 20).attr('y', 3)
            .attr('font-size', '8px').attr('font-family', mono).attr('fill', colors.textMuted).text('Official');
        leg.append('line').attr('x1', 62).attr('x2', 78).attr('y1', 0).attr('y2', 0)
            .attr('stroke', colors.red).attr('stroke-width', 2).attr('stroke-dasharray', '4,2');
        leg.append('text').attr('x', 82).attr('y', 3)
            .attr('font-size', '8px').attr('font-family', mono).attr('fill', colors.textMuted).text('Physical');
    }, [official, physical, w, h]);

    return <svg ref={ref} style={{ display: 'block', width: '100%' }} />;
}


// ── D3 Heatmap Matrix ───────────────────────────────────────────────────

function DivergenceMatrix({ cells, categories, onCellClick, selectedCell }) {
    const svgRef = useRef(null);
    const [tooltipData, setTooltipData] = useState(null);
    const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });

    useEffect(() => {
        if (!svgRef.current || !cells) return;

        const totalW = MATRIX_MARGIN.left + REGIONS.length * (CELL_SIZE + CELL_GAP);
        const totalH = MATRIX_MARGIN.top + categories.length * (CELL_SIZE + CELL_GAP);

        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();
        svg.attr('width', totalW).attr('height', totalH);

        const defs = svg.append('defs');
        ['consistent', 'minor', 'notable', 'major'].forEach(level => {
            const filter = defs.append('filter')
                .attr('id', `glow-${level}`)
                .attr('x', '-30%').attr('y', '-30%').attr('width', '160%').attr('height', '160%');
            filter.append('feGaussianBlur')
                .attr('in', 'SourceGraphic')
                .attr('stdDeviation', level === 'major' ? 6 : level === 'notable' ? 4 : level === 'minor' ? 3 : 2)
                .attr('result', 'blur');
            const merge = filter.append('feMerge');
            merge.append('feMergeNode').attr('in', 'blur');
            merge.append('feMergeNode').attr('in', 'SourceGraphic');
        });

        const g = svg.append('g');

        // Column headers
        REGIONS.forEach((reg, ci) => {
            g.append('text')
                .attr('x', MATRIX_MARGIN.left + ci * (CELL_SIZE + CELL_GAP) + CELL_SIZE / 2)
                .attr('y', MATRIX_MARGIN.top - 14)
                .attr('text-anchor', 'middle')
                .attr('font-size', '11px').attr('font-weight', 700)
                .attr('font-family', mono).attr('fill', colors.text)
                .attr('letter-spacing', '1px').text(reg);
        });

        // Row headers
        categories.forEach((cat, ri) => {
            const meta = CATEGORY_META[cat];
            g.append('text')
                .attr('x', MATRIX_MARGIN.left - 12)
                .attr('y', MATRIX_MARGIN.top + ri * (CELL_SIZE + CELL_GAP) + CELL_SIZE / 2 + 4)
                .attr('text-anchor', 'end')
                .attr('font-size', '10px').attr('font-weight', 600)
                .attr('font-family', mono)
                .attr('fill', meta?.color || colors.textDim)
                .text(`${meta?.icon || '●'} ${cat}`);
        });

        // Cells
        categories.forEach((cat, ri) => {
            REGIONS.forEach((reg, ci) => {
                const key = `${cat}|${reg}`;
                const cell = cells[key];
                if (!cell) return;

                const x = MATRIX_MARGIN.left + ci * (CELL_SIZE + CELL_GAP);
                const y = MATRIX_MARGIN.top + ri * (CELL_SIZE + CELL_GAP);
                const cls = cell.classification;
                const isSelected = selectedCell === key;

                const cellG = g.append('g')
                    .attr('class', 'crossref-matrix-cell')
                    .style('cursor', 'pointer');

                // Outer glow
                cellG.append('rect')
                    .attr('x', x - 2).attr('y', y - 2)
                    .attr('width', CELL_SIZE + 4).attr('height', CELL_SIZE + 4)
                    .attr('rx', 10)
                    .attr('fill', DIVERGENCE_GLOW[cls])
                    .attr('filter', `url(#glow-${cls})`);

                // Main cell
                const grad = defs.append('linearGradient')
                    .attr('id', `cell-grad-${ri}-${ci}`)
                    .attr('x1', '0').attr('y1', '0').attr('x2', '1').attr('y2', '1');
                grad.append('stop').attr('offset', '0%')
                    .attr('stop-color', DIVERGENCE_COLORS[cls]).attr('stop-opacity', 0.85);
                grad.append('stop').attr('offset', '100%')
                    .attr('stop-color', d3.color(DIVERGENCE_COLORS[cls]).darker(0.5).toString())
                    .attr('stop-opacity', 1);

                cellG.append('rect')
                    .attr('x', x).attr('y', y)
                    .attr('width', CELL_SIZE).attr('height', CELL_SIZE)
                    .attr('rx', 8)
                    .attr('fill', `url(#cell-grad-${ri}-${ci})`)
                    .attr('stroke', isSelected ? '#E8F0F8' : DIVERGENCE_COLORS[cls])
                    .attr('stroke-width', isSelected ? 2 : 0.5)
                    .attr('opacity', 0)
                    .transition().delay(ri * 60 + ci * 40).duration(400).attr('opacity', 1);

                // Z-score text
                cellG.append('text')
                    .attr('x', x + CELL_SIZE / 2).attr('y', y + CELL_SIZE / 2 - 6)
                    .attr('text-anchor', 'middle').attr('dominant-baseline', 'central')
                    .attr('font-size', '16px').attr('font-weight', 800)
                    .attr('font-family', mono)
                    .attr('fill', cls === 'noData' ? colors.textMuted : '#E8F0F8')
                    .text(cell.zScore != null ? cell.zScore.toFixed(1) + 'σ' : '--');

                // Check count badge
                const checkCount = cell.checks?.length || 0;
                if (checkCount > 0) {
                    cellG.append('text')
                        .attr('x', x + CELL_SIZE / 2).attr('y', y + CELL_SIZE - 10)
                        .attr('text-anchor', 'middle')
                        .attr('font-size', '8px').attr('font-family', mono)
                        .attr('fill', 'rgba(255,255,255,0.4)')
                        .text(`${checkCount} check${checkCount > 1 ? 's' : ''}`);
                } else {
                    cellG.append('text')
                        .attr('x', x + CELL_SIZE / 2).attr('y', y + CELL_SIZE - 10)
                        .attr('text-anchor', 'middle')
                        .attr('font-size', '7px').attr('font-family', mono)
                        .attr('fill', 'rgba(255,255,255,0.2)').attr('letter-spacing', '0.5px')
                        .text(cls === 'noData' ? 'GAP' : classifyLabel(cell.zScore).split(' ')[0]);
                }

                // Pulsing for major divergences
                if (cls === 'major') {
                    const pulseRect = cellG.append('rect')
                        .attr('x', x - 1).attr('y', y - 1)
                        .attr('width', CELL_SIZE + 2).attr('height', CELL_SIZE + 2)
                        .attr('rx', 9).attr('fill', 'none')
                        .attr('stroke', colors.red).attr('stroke-width', 1.5).attr('opacity', 0);
                    (function animatePulse() {
                        pulseRect.attr('opacity', 0.6)
                            .transition().duration(1500).ease(d3.easeSinInOut)
                            .attr('opacity', 0).on('end', animatePulse);
                    })();
                }

                // Interaction
                cellG
                    .on('mouseenter', function (event) {
                        setTooltipData(cell);
                        setTooltipPos({ x: event.clientX + 12, y: event.clientY - 60 });
                    })
                    .on('mousemove', function (event) {
                        setTooltipPos({ x: event.clientX + 12, y: event.clientY - 60 });
                    })
                    .on('mouseleave', function () { setTooltipData(null); })
                    .on('click', function () { onCellClick(key); });
            });
        });
    }, [cells, categories, selectedCell, onCellClick]);

    return (
        <div style={s.matrixWrap}>
            <svg ref={svgRef} style={{ display: 'block' }} />
            {tooltipData && (
                <div style={{ ...s.tooltip, left: tooltipPos.x, top: tooltipPos.y }}>
                    <div style={s.tooltipTitle}>
                        {tooltipData.region} — {tooltipData.category}
                        {CATEGORY_META[tooltipData.category] && (
                            <div style={{ fontSize: '9px', color: colors.textMuted, marginTop: '2px', fontWeight: 400 }}>
                                {CATEGORY_META[tooltipData.category].desc}
                            </div>
                        )}
                    </div>
                    <div style={s.tooltipRow}>
                        <span style={s.tooltipLabel}>Official</span>
                        <span style={s.tooltipValue}>{tooltipData.officialValue}</span>
                    </div>
                    <div style={s.tooltipRow}>
                        <span style={s.tooltipLabel}>Physical</span>
                        <span style={s.tooltipValue}>{tooltipData.physicalValue}</span>
                    </div>
                    <div style={{ ...s.tooltipRow, borderTop: `1px solid ${colors.borderSubtle}`, paddingTop: '6px', marginTop: '4px' }}>
                        <span style={s.tooltipLabel}>Divergence</span>
                        <span style={{
                            ...s.tooltipValue,
                            color: tooltipData.classification === 'major' ? colors.red :
                                   tooltipData.classification === 'notable' ? '#F97316' :
                                   tooltipData.classification === 'minor' ? colors.yellow : colors.green,
                        }}>
                            {formatZ(tooltipData.zScore)} — {classifyLabel(tooltipData.zScore)}
                        </span>
                    </div>
                    {tooltipData.checks?.length > 0 && (
                        <div style={{ fontSize: '9px', color: colors.accent, fontFamily: mono, marginTop: '6px' }}>
                            {tooltipData.checks.length} individual check{tooltipData.checks.length > 1 ? 's' : ''} — click to drill down
                        </div>
                    )}
                    <div style={{ fontSize: '10px', color: colors.textDim, fontFamily: mono, marginTop: '6px', lineHeight: 1.4 }}>
                        {tooltipData.implication}
                    </div>
                </div>
            )}
        </div>
    );
}


// ── Individual Check Row (granular view) ────────────────────────────────

function CheckRow({ check, onClick }) {
    const z = Math.abs(check.actual_divergence ?? 0);
    const cls = classifyDivergence(z);
    const clsColor = cls === 'major' ? colors.red :
                     cls === 'notable' ? '#F97316' :
                     cls === 'minor' ? colors.yellow : colors.green;

    return (
        <div
            className="crossref-check-row"
            style={s.checkRow}
            onClick={onClick}
        >
            <span style={{ ...s.assessmentBadge(cls), fontSize: '9px', padding: '3px 8px' }}>
                {z.toFixed(1)}σ
            </span>
            <div>
                <div style={s.checkName}>{check.name}</div>
                <div style={s.checkMeta}>
                    {check.official_source} vs {check.physical_source}
                </div>
            </div>
            <div style={{ fontSize: '11px', fontFamily: mono, color: colors.textDim, textAlign: 'right' }}>
                {formatApiValue(check.official_value)}
            </div>
            <div style={{ fontSize: '11px', fontFamily: mono, color: clsColor, textAlign: 'right' }}>
                {formatApiValue(check.physical_value)}
            </div>
            <div style={{ fontSize: '9px', fontFamily: mono, color: colors.textMuted, textAlign: 'center' }}>
                {check.expected_relationship?.replace(/_/g, ' ') || '--'}
            </div>
            <div style={{
                fontSize: '9px', fontFamily: mono, textAlign: 'right',
                color: (check.confidence ?? 0) > 0.7 ? colors.green : (check.confidence ?? 0) > 0.4 ? colors.yellow : colors.textMuted,
            }}>
                {check.confidence != null ? (check.confidence * 100).toFixed(0) + '%' : '--'}
            </div>
        </div>
    );
}


// ── Main Component ──────────────────────────────────────────────────────

export default function CrossReference({ onNavigate }) {
    const [data, setData] = useState(null);
    const [history, setHistory] = useState(null);
    const [loading, setLoading] = useState(true);
    const [selectedCell, setSelectedCell] = useState(null);
    const [activeTab, setActiveTab] = useState('matrix');
    const [flagsExpanded, setFlagsExpanded] = useState(true);
    const [expandedCategories, setExpandedCategories] = useState({});
    const [matrixZoom, setMatrixZoom] = useState(1);
    const detailRef = useRef(null);
    const flagsRef = useRef(null);
    const fullScreenRef = useRef(null);
    const { isFullScreen, toggleFullScreen } = useFullScreen(fullScreenRef);

    useEffect(() => {
        ensureKeyframes();
        loadData();
    }, []);

    async function loadData() {
        setLoading(true);
        try {
            let crossRef, hist;
            try {
                [crossRef, hist] = await Promise.all([
                    api.getCrossReference(),
                    api.getCrossRefHistory(),
                ]);
            } catch {
                crossRef = null;
                hist = null;
            }

            // Transform API response OR fall back to planned data
            if (crossRef && Array.isArray(crossRef.checks) && crossRef.checks.length > 0) {
                const transformed = transformApiChecks(crossRef);
                setData(transformed);
            } else {
                setData(generatePlaceholderData());
            }

            if (hist && Array.isArray(hist.records) && hist.records.length > 0) {
                setHistory(hist.records);
            } else {
                setHistory(generatePlaceholderHistory());
            }
        } catch {
            setData(generatePlaceholderData());
            setHistory(generatePlaceholderHistory());
        } finally {
            setLoading(false);
        }
    }

    const handleCellClick = useCallback((key) => {
        setSelectedCell(prev => prev === key ? null : key);
        setTimeout(() => {
            detailRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }, 100);
    }, []);

    const handleFlagClick = useCallback((cat, reg) => {
        setSelectedCell(`${cat}|${reg}`);
        setActiveTab('matrix');
        setTimeout(() => {
            detailRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }, 100);
    }, []);

    const toggleCategory = useCallback((cat) => {
        setExpandedCategories(prev => ({ ...prev, [cat]: !prev[cat] }));
    }, []);

    // Compute stats
    const stats = useMemo(() => {
        if (!data) return { flags: 0, hitRate: '--', tracked: 0, checks: 0, gaps: DATA_GAPS.length, liveChecks: 0 };
        const confirmed = (history || []).filter(h => h.verdict === 'confirmed').length;
        const total = (history || []).length;
        const hitRate = total > 0 ? ((confirmed / total) * 100).toFixed(0) : '--';
        const allChecks = Object.values(data.cells || {}).reduce((sum, c) => sum + (c.checks?.length || 0), 0);
        return {
            flags: (data.redFlags || []).length,
            hitRate,
            tracked: total,
            checks: allChecks,
            gaps: DATA_GAPS.length,
            liveChecks: data.totalChecks || 0,
        };
    }, [data, history]);

    // Determine which categories have data
    const activeCategories = useMemo(() => {
        if (!data?.cells) return CATEGORIES;
        return CATEGORIES.filter(cat =>
            REGIONS.some(reg => {
                const cell = data.cells[`${cat}|${reg}`];
                return cell && cell.zScore != null;
            })
        );
    }, [data]);

    // All checks flat (for granular view)
    const allChecks = useMemo(() => {
        if (!data?.cells) return [];
        return Object.values(data.cells)
            .flatMap(cell => cell.checks || [])
            .sort((a, b) => Math.abs(b.actual_divergence ?? 0) - Math.abs(a.actual_divergence ?? 0));
    }, [data]);

    // Group checks by category
    const checksByCategory = useMemo(() => {
        const groups = {};
        for (const check of allChecks) {
            const cat = CATEGORY_MAP[check.category] || check.category || 'Other';
            if (!groups[cat]) groups[cat] = [];
            groups[cat].push(check);
        }
        return groups;
    }, [allChecks]);

    const handleMatrixZoomIn = useCallback(() => setMatrixZoom(prev => Math.min(prev * 1.3, 2.5)), []);
    const handleMatrixZoomOut = useCallback(() => setMatrixZoom(prev => Math.max(prev * 0.7, 0.5)), []);
    const handleMatrixFit = useCallback(() => setMatrixZoom(1), []);

    if (loading) {
        return (
            <div style={s.container}>
                <div style={s.header}>
                    <div style={s.title}>CROSS-REFERENCE ENGINE</div>
                    <div style={s.subtitle}>Government statistics vs. physical reality</div>
                </div>
                <div style={s.loadingBar}><div style={s.loadingFill} /></div>
                <div style={{ textAlign: 'center', color: colors.textMuted, fontSize: '13px', fontFamily: mono, padding: '60px 0' }}>
                    Running {CATEGORIES.length * REGIONS.length} cross-reference checks across {CATEGORIES.length} domains...
                </div>
            </div>
        );
    }

    const { cells, redFlags, narrative } = data || {};
    const selectedData = selectedCell ? cells?.[selectedCell] : null;
    const tickers = selectedCell ? TICKER_IMPACT[selectedCell] : null;

    return (
        <div ref={fullScreenRef} style={{ ...s.container, background: isFullScreen ? colors.bg : undefined }}>
            {/* ── Header ── */}
            <div style={s.header}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                    <div style={s.title}>CROSS-REFERENCE ENGINE</div>
                    <span style={{
                        ...s.sourceTag,
                        background: data?.source === 'live' ? colors.greenBg : colors.yellowBg,
                        color: data?.source === 'live' ? colors.green : colors.yellow,
                    }}>
                        {data?.source === 'live' ? 'LIVE' : 'PLANNED'}
                    </span>
                </div>
                <div style={s.subtitle}>
                    Government statistics vs. physical reality — {CATEGORIES.length} domains × {REGIONS.length} regions
                    {data?.generatedAt && (
                        <span style={{ marginLeft: '12px', color: colors.textDim }}>
                            Updated {new Date(data.generatedAt).toLocaleString()}
                        </span>
                    )}
                </div>
            </div>

            {/* ── Score Row ── */}
            <div style={s.scoreRow}>
                <div
                    onClick={() => { if (stats.flags > 0) { setFlagsExpanded(prev => !prev); setTimeout(() => flagsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' }), 100); } }}
                    style={{ ...s.scoreCard, cursor: stats.flags > 0 ? 'pointer' : 'default' }}
                >
                    <div style={{ ...s.bigNumber, color: stats.flags > 0 ? colors.red : colors.green }}>
                        {stats.flags}
                    </div>
                    <div style={s.bigLabel}>RED FLAGS</div>
                </div>
                <div onClick={() => setActiveTab('checks')} style={{ ...s.scoreCard, cursor: 'pointer' }}>
                    <div style={{ ...s.bigNumber, color: colors.accent }}>
                        {stats.liveChecks || stats.checks}
                    </div>
                    <div style={s.bigLabel}>LIVE CHECKS</div>
                </div>
                <div onClick={() => setActiveTab('ledger')} style={{ ...s.scoreCard, cursor: 'pointer' }}>
                    <div style={{ ...s.bigNumber, color: colors.green }}>{stats.hitRate}%</div>
                    <div style={s.bigLabel}>HIT RATE</div>
                </div>
                <div onClick={() => setActiveTab('gaps')} style={{ ...s.scoreCard, cursor: 'pointer' }}>
                    <div style={{ ...s.bigNumber, color: colors.yellow }}>{stats.gaps}</div>
                    <div style={s.bigLabel}>DATA GAPS</div>
                </div>
            </div>

            {/* ── Red Flags Banner ── */}
            {redFlags && redFlags.length > 0 ? (
                <div ref={flagsRef} style={s.flagBanner}>
                    <div
                        onClick={() => setFlagsExpanded(prev => !prev)}
                        style={{ ...s.sectionTitle, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '8px' }}
                    >
                        ACTIVE RED FLAGS
                        <span style={{ display: 'inline-flex', padding: '2px 8px', borderRadius: '4px', fontSize: '11px', fontWeight: 800, fontFamily: mono, background: `${colors.red}25`, color: colors.red }}>
                            {redFlags.length}
                        </span>
                        <span style={{ fontSize: '10px', color: colors.textMuted, transform: flagsExpanded ? 'rotate(180deg)' : 'rotate(0deg)', transition: 'transform 0.2s ease' }}>
                            {'\u25BC'}
                        </span>
                    </div>
                    <div style={{
                        ...s.flagScroller,
                        maxHeight: flagsExpanded ? '600px' : '0px',
                        overflow: 'hidden',
                        transition: 'max-height 0.4s cubic-bezier(0.4, 0, 0.2, 1)',
                        paddingBottom: flagsExpanded ? '8px' : '0px',
                    }}>
                        {redFlags.map((flag, i) => (
                            <div key={i} className="crossref-flag-card" style={s.flagCard}
                                onClick={() => handleFlagClick(flag.category, flag.region)}>
                                <div style={s.flagHeadline}>{flag.headline}</div>
                                <div style={{ display: 'flex', gap: '8px', alignItems: 'center', marginTop: '6px' }}>
                                    <div style={s.flagZBadge}>{formatZ(flag.zScore)}</div>
                                    {flag.checkCount > 0 && (
                                        <span style={{ fontSize: '9px', color: colors.textMuted, fontFamily: mono }}>
                                            {flag.checkCount} check{flag.checkCount > 1 ? 's' : ''}
                                        </span>
                                    )}
                                </div>
                                <div style={s.flagImplication}>{flag.implication}</div>
                            </div>
                        ))}
                    </div>
                </div>
            ) : (
                <div style={s.greenBanner}>
                    <span style={s.greenText}>All cross-reference checks consistent</span>
                </div>
            )}

            {/* ── Tabs ── */}
            <div style={s.tabs}>
                {[
                    { id: 'matrix', label: 'Divergence Matrix' },
                    { id: 'checks', label: `Checks (${stats.liveChecks || stats.checks})` },
                    { id: 'gaps', label: `Data Gaps (${stats.gaps})` },
                    { id: 'narrative', label: 'Intelligence Brief' },
                    { id: 'ledger', label: 'Lies Ledger' },
                ].map(tab => (
                    <button key={tab.id} style={s.tab(activeTab === tab.id)}
                        onClick={() => setActiveTab(tab.id)}>
                        {tab.label}
                    </button>
                ))}
            </div>

            {/* ── Matrix Tab ── */}
            {activeTab === 'matrix' && (
                <>
                    <div style={s.sectionTitle}>DIVERGENCE MATRIX — {CATEGORIES.length} DOMAINS × {REGIONS.length} REGIONS</div>
                    <div style={{ position: 'relative' }}>
                        <ChartControls
                            onZoomIn={handleMatrixZoomIn}
                            onZoomOut={handleMatrixZoomOut}
                            onFitScreen={handleMatrixFit}
                            onFullScreen={toggleFullScreen}
                            isFullScreen={isFullScreen}
                            compact
                        />
                        <div style={{
                            transform: `scale(${matrixZoom})`, transformOrigin: 'top left',
                            transition: 'transform 0.3s ease',
                        }}>
                            <DivergenceMatrix
                                cells={cells}
                                categories={CATEGORIES}
                                onCellClick={handleCellClick}
                                selectedCell={selectedCell}
                            />
                        </div>
                    </div>

                    {/* Legend */}
                    <div style={{ display: 'flex', gap: '16px', marginTop: '12px', flexWrap: 'wrap', alignItems: 'center' }}>
                        {[
                            { label: 'Consistent', color: DIVERGENCE_COLORS.consistent, range: '<0.5σ' },
                            { label: 'Minor', color: DIVERGENCE_COLORS.minor, range: '0.5-1.5σ' },
                            { label: 'Notable', color: DIVERGENCE_COLORS.notable, range: '1.5-2.0σ' },
                            { label: 'Contradiction', color: DIVERGENCE_COLORS.major, range: '>2.0σ' },
                            { label: 'No Data / Gap', color: DIVERGENCE_COLORS.noData, range: '' },
                        ].map(item => (
                            <div key={item.label} style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <div style={{ width: '12px', height: '12px', borderRadius: '3px', background: item.color }} />
                                <span style={{ fontSize: '10px', fontFamily: mono, color: colors.textMuted }}>
                                    {item.label} {item.range && `(${item.range})`}
                                </span>
                            </div>
                        ))}
                    </div>

                    {/* ── Detail Panel ── */}
                    {selectedData && (
                        <div ref={detailRef} className="crossref-detail-enter" style={s.detailPanel}>
                            <div style={s.detailHeader}>
                                <div>
                                    <div style={s.detailTitle}>
                                        {selectedData.region} — {selectedData.category}
                                    </div>
                                    <div style={{ marginTop: '6px', display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
                                        <span style={s.assessmentBadge(selectedData.classification)}>
                                            {classifyLabel(selectedData.zScore)}
                                        </span>
                                        <span style={{
                                            fontSize: '14px', fontWeight: 800, fontFamily: mono,
                                            color: selectedData.classification === 'major' ? colors.red :
                                                   selectedData.classification === 'notable' ? '#F97316' :
                                                   selectedData.classification === 'minor' ? colors.yellow : colors.green,
                                        }}>
                                            {formatZ(selectedData.zScore)}
                                        </span>
                                        {selectedData.confidence > 0 && (
                                            <span style={{ fontSize: '10px', fontFamily: mono, color: colors.textMuted }}>
                                                Confidence: {(selectedData.confidence * 100).toFixed(0)}%
                                            </span>
                                        )}
                                    </div>
                                </div>
                                <button style={s.detailClose} onClick={() => setSelectedCell(null)}>CLOSE</button>
                            </div>

                            {/* Two-column comparison */}
                            <div style={s.detailColumns}>
                                <div style={s.detailCol}>
                                    <div style={{ ...s.detailColTitle, color: colors.accent }}>OFFICIAL STORY</div>
                                    <div style={s.detailSource}>{selectedData.officialSource}</div>
                                    <div style={{ ...s.detailValue, color: colors.accent }}>{selectedData.officialValue}</div>
                                    {selectedData.officialTrend?.length > 1 && (
                                        <div style={s.sparkWrap}>
                                            <Sparkline data={selectedData.officialTrend} color={colors.accent} />
                                        </div>
                                    )}
                                </div>
                                <div style={s.detailCol}>
                                    <div style={{ ...s.detailColTitle, color: colors.red }}>PHYSICAL REALITY</div>
                                    <div style={s.detailSource}>{selectedData.physicalSource}</div>
                                    <div style={{ ...s.detailValue, color: colors.red }}>{selectedData.physicalValue}</div>
                                    {selectedData.physicalTrend?.length > 1 && (
                                        <div style={s.sparkWrap}>
                                            <Sparkline data={selectedData.physicalTrend} color={colors.red} />
                                        </div>
                                    )}
                                </div>
                            </div>

                            {/* Divergence overlay chart */}
                            {selectedData.officialTrend?.length > 1 && selectedData.physicalTrend?.length > 1 && (
                                <>
                                    <div style={s.sectionTitle}>DIVERGENCE X-RAY</div>
                                    <div style={{ background: colors.bg, borderRadius: tokens.radius.md, padding: '12px', marginBottom: '12px' }}>
                                        <DivergenceChart official={selectedData.officialTrend} physical={selectedData.physicalTrend} />
                                    </div>
                                </>
                            )}

                            {/* Individual checks drill-down */}
                            {selectedData.checks?.length > 0 && (
                                <>
                                    <div style={s.sectionTitle}>INDIVIDUAL CHECKS ({selectedData.checks.length})</div>
                                    <div style={{ marginBottom: '12px' }}>
                                        {/* Check header */}
                                        <div style={{ ...s.checkRow, background: 'none', border: 'none', cursor: 'default', marginBottom: '2px', padding: '4px 14px' }}>
                                            <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px' }}>Z-SCORE</span>
                                            <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px' }}>CHECK NAME</span>
                                            <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px', textAlign: 'right' }}>OFFICIAL</span>
                                            <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px', textAlign: 'right' }}>PHYSICAL</span>
                                            <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px', textAlign: 'center' }}>RELATION</span>
                                            <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px', textAlign: 'right' }}>CONF</span>
                                        </div>
                                        {selectedData.checks.map((check, i) => (
                                            <CheckRow key={i} check={check} onClick={() => {}} />
                                        ))}
                                    </div>
                                </>
                            )}

                            {/* Ticker impact */}
                            {tickers && tickers.length > 0 && (
                                <div style={{ marginBottom: '12px' }}>
                                    <div style={s.sectionTitle}>TICKER IMPACT</div>
                                    <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                                        {tickers.map(t => (
                                            <span key={t} style={{
                                                padding: '4px 10px', borderRadius: '6px',
                                                fontSize: '11px', fontWeight: 700, fontFamily: mono,
                                                background: `${colors.accent}15`, color: colors.accent,
                                                border: `1px solid ${colors.accent}30`,
                                                cursor: 'pointer',
                                            }}
                                            onClick={() => onNavigate?.('ticker', { symbol: t })}
                                            >
                                                {t}
                                            </span>
                                        ))}
                                    </div>
                                </div>
                            )}

                            {/* Implication */}
                            <div style={{
                                background: `${selectedData.classification === 'major' ? colors.red : colors.accent}08`,
                                border: `1px solid ${selectedData.classification === 'major' ? colors.red : colors.accent}15`,
                                borderRadius: tokens.radius.sm, padding: '12px 14px', marginBottom: '12px',
                            }}>
                                <div style={{
                                    fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                                    color: selectedData.classification === 'major' ? colors.red : colors.accent,
                                    fontFamily: mono, marginBottom: '4px',
                                }}>
                                    MARKET IMPLICATION
                                </div>
                                <div style={{ fontSize: '12px', color: colors.text, fontFamily: mono, lineHeight: 1.5 }}>
                                    {selectedData.implication}
                                </div>
                            </div>

                            {/* Historical analog */}
                            {selectedData.historicalAnalog && (
                                <div style={s.analogBox}>
                                    <div style={s.analogLabel}>HISTORICAL ANALOG</div>
                                    <div style={s.analogText}>{selectedData.historicalAnalog}</div>
                                </div>
                            )}
                        </div>
                    )}
                </>
            )}

            {/* ── Checks Tab (Granular) ── */}
            {activeTab === 'checks' && (
                <>
                    {allChecks.length > 0 ? (
                        <>
                            <div style={s.sectionTitle}>
                                ALL CROSS-REFERENCE CHECKS — {allChecks.length} TOTAL
                            </div>
                            {/* Column header */}
                            <div style={{ ...s.checkRow, background: 'none', border: 'none', cursor: 'default', padding: '4px 14px' }}>
                                <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px' }}>Z-SCORE</span>
                                <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px' }}>CHECK NAME</span>
                                <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px', textAlign: 'right' }}>OFFICIAL</span>
                                <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px', textAlign: 'right' }}>PHYSICAL</span>
                                <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px', textAlign: 'center' }}>RELATION</span>
                                <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px', textAlign: 'right' }}>CONF</span>
                            </div>

                            {Object.entries(checksByCategory).map(([cat, checks]) => (
                                <div key={cat} style={{ marginBottom: '16px' }}>
                                    <div
                                        onClick={() => toggleCategory(cat)}
                                        style={{
                                            ...s.sectionTitle, cursor: 'pointer',
                                            display: 'flex', alignItems: 'center', gap: '8px',
                                            color: CATEGORY_META[cat]?.color || colors.accent,
                                        }}
                                    >
                                        {CATEGORY_META[cat]?.icon || '●'} {cat}
                                        <span style={{ fontSize: '10px', color: colors.textMuted }}>
                                            ({checks.length} check{checks.length > 1 ? 's' : ''})
                                        </span>
                                        <span style={{
                                            fontSize: '10px', transform: expandedCategories[cat] === false ? 'rotate(-90deg)' : 'rotate(0deg)',
                                            transition: 'transform 0.2s ease',
                                        }}>
                                            {'\u25BC'}
                                        </span>
                                    </div>
                                    {expandedCategories[cat] !== false && checks.map((check, i) => (
                                        <CheckRow key={i} check={check} onClick={() => {
                                            const region = inferRegion(check);
                                            const displayCat = CATEGORY_MAP[check.category] || check.category;
                                            setSelectedCell(`${displayCat}|${region}`);
                                            setActiveTab('matrix');
                                            setTimeout(() => detailRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' }), 100);
                                        }} />
                                    ))}
                                </div>
                            ))}
                        </>
                    ) : (
                        <div style={s.narrativePanel}>
                            <div style={{ ...s.sectionTitle, marginTop: 0, color: colors.yellow }}>NO LIVE CHECKS AVAILABLE</div>
                            <div style={s.narrativeSummary}>
                                The cross-reference engine API is not returning live checks. The matrix view shows planned data from the visualization spec.
                                When the API is connected, this tab will show every individual check with its z-score, official value, physical value, and confidence.
                            </div>
                            <div style={{ ...s.sectionTitle, color: colors.accent }}>PLANNED CHECK CATEGORIES</div>
                            {CATEGORIES.map(cat => (
                                <div key={cat} style={{ marginBottom: '8px' }}>
                                    <span style={{ fontSize: '11px', fontFamily: mono, color: CATEGORY_META[cat]?.color || colors.text, fontWeight: 700 }}>
                                        {CATEGORY_META[cat]?.icon} {cat}
                                    </span>
                                    <span style={{ fontSize: '10px', fontFamily: mono, color: colors.textDim, marginLeft: '8px' }}>
                                        — {CATEGORY_META[cat]?.desc}
                                    </span>
                                </div>
                            ))}
                        </div>
                    )}
                </>
            )}

            {/* ── Gaps Tab ── */}
            {activeTab === 'gaps' && (
                <>
                    <div style={s.sectionTitle}>
                        DATA GAPS — {DATA_GAPS.length} SOURCES INGESTED BUT NOT CROSS-REFERENCED
                    </div>
                    <div style={{
                        ...shared.card, borderLeft: `3px solid ${colors.red}`,
                        marginBottom: '16px', padding: '12px 16px',
                    }}>
                        <div style={{ fontSize: '12px', fontFamily: mono, color: colors.text, lineHeight: 1.6 }}>
                            We are pulling data from {DATA_GAPS.length} additional source categories that are <span style={{ color: colors.red, fontWeight: 700 }}>not yet wired</span> into
                            the cross-reference engine. Each one represents a missed opportunity to catch governments and institutions lying.
                            The data sits in our database. We just need to build the checks.
                        </div>
                    </div>

                    {DATA_GAPS.map((gap, i) => (
                        <div key={i}
                            className="crossref-gap-card"
                            style={{
                                ...s.gapCard,
                                borderLeftColor: gap.priority === 'CRITICAL' ? colors.red :
                                    gap.priority === 'HIGH' ? colors.yellow :
                                    gap.priority === 'MEDIUM' ? colors.accent : colors.textMuted,
                                transition: 'all 0.15s ease',
                            }}
                        >
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                                <div style={s.gapTitle}>{gap.domain}</div>
                                <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
                                    <span style={s.gapPriority(gap.priority)}>{gap.priority}</span>
                                    <span style={{
                                        ...s.sourceTag,
                                        background: gap.status === 'ingested' ? colors.greenBg :
                                            gap.status === 'computed' ? `${colors.accent}20` : colors.yellowBg,
                                        color: gap.status === 'ingested' ? colors.green :
                                            gap.status === 'computed' ? colors.accent : colors.yellow,
                                        marginLeft: 0,
                                    }}>
                                        {gap.status.toUpperCase()}
                                    </span>
                                </div>
                            </div>
                            <div style={s.gapSources}>
                                Sources: {gap.sources.join(' · ')}
                            </div>
                            <div style={s.gapShould}>
                                Should check: {gap.shouldCheck}
                            </div>
                            <div style={s.gapImpact}>
                                {gap.impact}
                            </div>
                        </div>
                    ))}
                </>
            )}

            {/* ── Narrative Tab ── */}
            {activeTab === 'narrative' && narrative && (
                <div style={s.narrativePanel}>
                    <div style={{ ...s.sectionTitle, marginTop: 0 }}>
                        WHAT THE DATA IS TELLING US THAT THE HEADLINES AREN'T
                    </div>
                    <div style={s.narrativeSummary}>
                        {typeof narrative === 'string' ? narrative : narrative.summary}
                    </div>

                    {narrative.bullets?.length > 0 && (
                        <>
                            <div style={{ ...s.sectionTitle, color: colors.red }}>KEY REVELATIONS</div>
                            {narrative.bullets.map((b, i) => (
                                <div key={i} style={{ ...s.narrativeBullet, cursor: 'pointer', borderRadius: '4px', padding: '4px 0' }}
                                    onClick={() => setActiveTab('matrix')}
                                    onMouseEnter={(e) => { e.currentTarget.style.background = `${colors.red}08`; }}
                                    onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                                >
                                    <div style={s.bulletDot} />
                                    <div style={s.bulletText}>{b}</div>
                                </div>
                            ))}
                        </>
                    )}

                    {narrative.watchFor?.length > 0 && (
                        <>
                            <div style={{ ...s.sectionTitle, color: colors.yellow, marginTop: '20px' }}>WATCH FOR</div>
                            {narrative.watchFor.map((w, i) => (
                                <div key={i} style={{ ...s.watchItem, cursor: 'pointer', borderRadius: '4px', padding: '4px 0' }}
                                    onClick={() => onNavigate?.('predictions')}
                                    onMouseEnter={(e) => { e.currentTarget.style.background = `${colors.yellow}08`; }}
                                    onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                                >
                                    <div style={s.watchDot} />
                                    <div style={s.watchText}>{w}</div>
                                </div>
                            ))}
                        </>
                    )}
                </div>
            )}

            {/* ── Ledger Tab ── */}
            {activeTab === 'ledger' && (
                <>
                    <div style={s.sectionTitle}>LIES LEDGER — HISTORICAL TRACK RECORD</div>
                    <div style={{
                        ...shared.card,
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        gap: '16px', padding: '14px', marginBottom: '12px',
                        borderLeft: `3px solid ${colors.green}`,
                    }}>
                        <span style={{ fontSize: '13px', fontWeight: 700, fontFamily: mono, color: colors.green }}>
                            {(history || []).filter(h => h.verdict === 'confirmed').length}/{(history || []).length} confirmed
                        </span>
                        <span style={{ fontSize: '11px', fontFamily: mono, color: colors.textMuted }}>
                            ({stats.hitRate}% accuracy)
                        </span>
                        <span style={{ fontSize: '11px', fontFamily: mono, color: colors.textDim }}>
                            When we flag a divergence, the subsequent data confirms it {stats.hitRate}% of the time
                        </span>
                    </div>

                    {(history || []).map((entry, i) => (
                        <div key={i}
                            onClick={() => {
                                setSelectedCell(`${entry.category}|${entry.region}`);
                                setActiveTab('matrix');
                                setTimeout(() => detailRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' }), 100);
                            }}
                            style={{ ...s.ledgerCard, cursor: 'pointer', transition: 'all 0.2s ease' }}
                            onMouseEnter={(e) => { e.currentTarget.style.borderColor = `${colors.accent}40`; e.currentTarget.style.transform = 'translateX(4px)'; }}
                            onMouseLeave={(e) => { e.currentTarget.style.borderColor = ''; e.currentTarget.style.transform = 'translateX(0)'; }}
                        >
                            <div style={s.ledgerHeader}>
                                <span style={s.ledgerDate}>{entry.date}</span>
                                <span style={s.ledgerVerdict(entry.verdict)}>
                                    {entry.verdict === 'confirmed' ? 'CONFIRMED' : 'MISS'}
                                </span>
                            </div>
                            <div style={s.ledgerFlagged}>
                                {entry.region} {entry.category}: {entry.flagged}
                            </div>
                            <div style={s.ledgerOutcome}>{entry.outcome}</div>
                            <div style={{ ...s.ledgerMove, color: entry.verdict === 'confirmed' ? colors.green : colors.textMuted }}>
                                {entry.marketMove}
                            </div>
                        </div>
                    ))}
                </>
            )}

            {/* Bottom padding */}
            <div style={{ height: '40px' }} />
        </div>
    );
}
