/**
 * CrossReference — "Lie Detector" view.
 *
 * Cross-references government statistics against physical reality indicators
 * and visualizes where the numbers don't add up.
 *
 * D3 heatmap matrix + divergence detail overlays + narrative panel + history ledger.
 */
import React, { useState, useEffect, useRef, useCallback } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { shared, colors, tokens } from '../styles/shared.js';

// ── Constants ────────────────────────────────────────────────────────────────

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";

const CATEGORIES = ['GDP', 'Trade', 'Inflation', 'Central Bank', 'Employment'];
const REGIONS = ['US', 'China', 'EU', 'Japan', 'EM'];

const CELL_SIZE = 88;
const CELL_GAP = 4;
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
    if (z < 0.5) return 'consistent';
    if (z < 1.5) return 'minor';
    if (z < 2.0) return 'notable';
    return 'major';
}

function classifyLabel(z) {
    if (z == null) return 'NO DATA';
    if (z < 0.5) return 'CONSISTENT';
    if (z < 1.5) return 'MINOR DIVERGENCE';
    if (z < 2.0) return 'NOTABLE DIVERGENCE';
    return 'CONTRADICTION';
}

function formatZ(z) {
    if (z == null) return '--';
    return z.toFixed(2) + 'σ';
}

// ── Placeholder data generator (used until API is wired) ─────────────────

function generatePlaceholderData() {
    const cells = {};
    const redFlags = [];

    const officialSources = {
        GDP: { US: 'BEA', China: 'NBS', EU: 'Eurostat', Japan: 'Cabinet Office', EM: 'IMF' },
        Trade: { US: 'Census Bureau', China: 'GACC', EU: 'Eurostat', Japan: 'MOF', EM: 'WTO' },
        Inflation: { US: 'BLS CPI', China: 'NBS CPI', EU: 'ECB HICP', Japan: 'BOJ', EM: 'World Bank' },
        'Central Bank': { US: 'Fed Funds', China: 'PBOC MLF', EU: 'ECB Refi', Japan: 'BOJ YCC', EM: 'Composite' },
        Employment: { US: 'BLS NFP', China: 'NBS Survey', EU: 'Eurostat LFS', Japan: 'Statistics Bureau', EM: 'ILO' },
    };

    const physicalSources = {
        GDP: { US: 'Satellite/Night Lights', China: 'Night Lights + Rail Freight', EU: 'Electricity Consumption', Japan: 'Industrial Electricity', EM: 'Satellite Composite' },
        Trade: { US: 'AIS Ship Tracking', China: 'AIS + Port TEU', EU: 'AIS Rotterdam/Hamburg', Japan: 'AIS + Port Data', EM: 'AIS Global' },
        Inflation: { US: 'Billion Prices Project', China: 'Web Scraped Prices', EU: 'Billion Prices + Fuel', Japan: 'Scanner Data', EM: 'Web Scraped Basket' },
        'Central Bank': { US: 'Repo Volumes', China: 'Shibor Spread', EU: 'ESTR Spread', Japan: 'JGB Curve Shape', EM: 'CDS Spreads' },
        Employment: { US: 'Indeed Job Postings', China: 'Baidu Job Search Index', EU: 'Indeed EU + Mobility', Japan: 'Recruit Index', EM: 'Google Trends Jobs' },
    };

    const officialValues = {
        GDP: { US: '+2.8% YoY', China: '+5.2% YoY', EU: '+0.6% YoY', Japan: '+1.1% YoY', EM: '+4.1% YoY' },
        Trade: { US: '-$68.3B', China: '+$82.1B', EU: '+€28.4B', Japan: '-¥462B', EM: 'Mixed' },
        Inflation: { US: '3.2% YoY', China: '0.2% YoY', EU: '2.6% YoY', Japan: '2.8% YoY', EM: '5.4% avg' },
        'Central Bank': { US: '5.25-5.50%', China: '2.50% MLF', EU: '4.50%', Japan: '-0.10%', EM: '7.2% avg' },
        Employment: { US: '+216K NFP', China: '5.1% UE', EU: '6.4% UE', Japan: '2.5% UE', EM: '5.8% avg' },
    };

    const physicalValues = {
        GDP: { US: '+2.6% (lights)', China: '+2.1% (lights/freight)', EU: '+0.4% (elec)', Japan: '+0.9% (elec)', EM: '+3.8% (composite)' },
        Trade: { US: '-$71.0B (AIS)', China: '+$64.2B (port TEU)', EU: '+€22.1B (AIS)', Japan: '-¥480B (AIS)', EM: 'Weaker than reported' },
        Inflation: { US: '3.5% (BPP)', China: '-0.8% (scraped)', EU: '2.9% (BPP)', Japan: '3.1% (scanner)', EM: '6.1% (scraped)' },
        'Central Bank': { US: 'Tighter (repo)', China: 'Much tighter (Shibor)', EU: 'Aligned', Japan: 'Losing control (JGB)', EM: 'Wider CDS' },
        Employment: { US: '+142K (Indeed)', China: '-12% searches', EU: '-8% postings', Japan: 'Aligned', EM: 'Weaker searches' },
    };

    // Z-scores designed to tell a story
    const zScores = {
        GDP:           { US: 0.3, China: 2.8, EU: 0.4, Japan: 0.3, EM: 0.5 },
        Trade:         { US: 0.7, China: 1.9, EU: 1.1, Japan: 0.4, EM: 1.2 },
        Inflation:     { US: 0.8, China: 2.4, EU: 0.6, Japan: 0.7, EM: 1.0 },
        'Central Bank':{ US: 0.9, China: 1.7, EU: 0.2, Japan: 2.1, EM: 1.3 },
        Employment:    { US: 1.4, China: 2.6, EU: 1.6, Japan: 0.2, EM: 1.5 },
    };

    const implications = {
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

    const historicalAnalogs = {
        GDP: {
            China: 'Last divergence this large (2019-Q4): PMI collapsed within 90 days, CNH fell 3.2%',
        },
        Inflation: {
            China: 'Last CPI divergence >2σ (2015-Q1): PBOC cut RRR 3x within 6 months',
        },
        'Central Bank': {
            Japan: 'Last JGB divergence (2022-Q4): BOJ widened YCC band within 45 days',
        },
        Employment: {
            US: 'Last Indeed/NFP divergence >1σ (2023-Q2): NFP revised down by 300K+ cumulatively',
            China: 'China stopped publishing youth unemployment when divergence hit 2.5σ (2023-Q2)',
        },
    };

    // Sparkline-ish time series (12 months of fake data)
    function makeSparkline(base, drift, noise) {
        const pts = [];
        let v = base;
        for (let i = 0; i < 12; i++) {
            v += drift + (Math.random() - 0.5) * noise;
            pts.push({ month: i, value: v });
        }
        return pts;
    }

    for (const cat of CATEGORIES) {
        for (const reg of REGIONS) {
            const z = zScores[cat]?.[reg] ?? null;
            const cls = classifyDivergence(z);
            const cell = {
                category: cat,
                region: reg,
                zScore: z,
                classification: cls,
                officialSource: officialSources[cat]?.[reg] || 'Government',
                physicalSource: physicalSources[cat]?.[reg] || 'Alternative Data',
                officialValue: officialValues[cat]?.[reg] || '--',
                physicalValue: physicalValues[cat]?.[reg] || '--',
                implication: implications[cat]?.[reg] || 'No significant divergence detected',
                historicalAnalog: historicalAnalogs[cat]?.[reg] || null,
                officialTrend: makeSparkline(50, 0.3, 2),
                physicalTrend: makeSparkline(50, cls === 'major' ? -0.5 : cls === 'notable' ? 0.1 : 0.25, 3),
            };
            cells[`${cat}|${reg}`] = cell;

            if (z > 2.0) {
                redFlags.push({
                    category: cat,
                    region: reg,
                    headline: `${reg} ${cat} vs ${physicalSources[cat]?.[reg] || 'Physical Data'}: MAJOR DIVERGENCE`,
                    zScore: z,
                    implication: implications[cat]?.[reg],
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
        ],
        watchFor: [
            'China NBS PMI release (Apr 1) — will it confirm night light divergence?',
            'US NFP benchmark revision (Feb → released Apr) — potential -500K cumulative revision',
            'BOJ April meeting — JGB market already pricing policy shift',
            'EU Flash CPI (Apr 3) — BPP suggests upside surprise',
        ],
    };

    return { cells, redFlags, narrative };
}

function generatePlaceholderHistory() {
    return [
        {
            date: '2025-11-15',
            category: 'Employment',
            region: 'US',
            flagged: 'NFP vs Indeed divergence at 1.8σ',
            outcome: 'NFP revised down by 71K two months later',
            marketMove: 'SPX +1.2% on revision day (dovish repricing)',
            verdict: 'confirmed',
        },
        {
            date: '2025-09-22',
            category: 'GDP',
            region: 'China',
            flagged: 'GDP vs Night Lights divergence at 2.5σ',
            outcome: 'PMI fell below 49 within 60 days; copper dropped 8%',
            marketMove: 'FXI -11%, HG copper -8.3%',
            verdict: 'confirmed',
        },
        {
            date: '2025-07-03',
            category: 'Central Bank',
            region: 'Japan',
            flagged: 'JGB curve vs BOJ rhetoric divergence at 1.9σ',
            outcome: 'BOJ widened YCC band in July meeting',
            marketMove: 'USDJPY -4.1% in 48 hours',
            verdict: 'confirmed',
        },
        {
            date: '2025-05-18',
            category: 'Inflation',
            region: 'EU',
            flagged: 'HICP vs BPP divergence at 1.3σ',
            outcome: 'Flash CPI came in 20bps above consensus',
            marketMove: 'EUR +0.6%, Bund yields +8bps',
            verdict: 'confirmed',
        },
        {
            date: '2025-03-10',
            category: 'Trade',
            region: 'China',
            flagged: 'Trade surplus vs AIS port data divergence at 2.1σ',
            outcome: 'Surplus revised down by $12B in subsequent release',
            marketMove: 'CNH weakened 0.8% vs USD',
            verdict: 'confirmed',
        },
        {
            date: '2025-01-20',
            category: 'Employment',
            region: 'China',
            flagged: 'Reported UE vs Baidu job search divergence at 2.4σ',
            outcome: 'Youth UE reporting suspended (again)',
            marketMove: 'KWEB -6.2% over following week',
            verdict: 'confirmed',
        },
        {
            date: '2024-11-08',
            category: 'Inflation',
            region: 'US',
            flagged: 'CPI vs BPP divergence at 0.9σ',
            outcome: 'Next CPI print was inline with BLS',
            marketMove: 'Minimal',
            verdict: 'miss',
        },
    ];
}

// ── Styles ────────────────────────────────────────────────────────────────

const s = {
    container: {
        padding: tokens.space.lg, maxWidth: '1100px', margin: '0 auto',
        minHeight: '100vh',
    },
    header: {
        marginBottom: '8px',
    },
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

    // Red flags banner
    flagBanner: {
        marginTop: '20px', marginBottom: '6px',
    },
    flagScroller: {
        display: 'flex', gap: '10px', overflowX: 'auto',
        paddingBottom: '8px', scrollbarWidth: 'none',
        WebkitOverflowScrolling: 'touch',
    },
    flagCard: {
        minWidth: '300px', maxWidth: '380px', flex: '0 0 auto',
        background: 'linear-gradient(145deg, #1A0808 0%, #2A0A0A 100%)',
        border: `1px solid ${colors.red}40`,
        borderRadius: tokens.radius.md,
        padding: '14px 16px',
        cursor: 'pointer',
        position: 'relative',
        overflow: 'hidden',
        transition: 'all 0.2s ease',
    },
    flagHeadline: {
        fontSize: '12px', fontWeight: 700, color: '#F8D0D0',
        fontFamily: mono, lineHeight: 1.4,
    },
    flagZBadge: {
        display: 'inline-flex', alignItems: 'center',
        padding: '2px 8px', borderRadius: '4px',
        fontSize: '11px', fontWeight: 800, fontFamily: mono,
        background: `${colors.red}30`, color: colors.red,
        marginTop: '6px',
    },
    flagImplication: {
        fontSize: '11px', color: colors.textDim, fontFamily: mono,
        marginTop: '6px', lineHeight: 1.4,
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
        marginTop: '12px',
        overflowX: 'auto',
        WebkitOverflowScrolling: 'touch',
        scrollbarWidth: 'none',
    },

    // Tooltip
    tooltip: {
        position: 'fixed',
        background: '#0A1018',
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md,
        padding: '14px 16px',
        maxWidth: '340px',
        zIndex: 1000,
        pointerEvents: 'none',
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
    tooltipLabel: { color: colors.textMuted },
    tooltipValue: { color: colors.text, fontWeight: 600, textAlign: 'right' },

    // Detail panel
    detailPanel: {
        ...shared.cardGradient,
        marginTop: '16px',
        overflow: 'hidden',
        transition: 'all 0.4s cubic-bezier(0.4, 0, 0.2, 1)',
    },
    detailHeader: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: '16px',
    },
    detailTitle: {
        fontSize: '15px', fontWeight: 700, color: '#E8F0F8', fontFamily: mono,
    },
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
        background: colors.bg, borderRadius: tokens.radius.md,
        padding: '14px',
    },
    detailColTitle: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
        marginBottom: '8px', fontFamily: mono,
    },
    detailSource: {
        fontSize: '11px', color: colors.textDim, fontFamily: mono,
        marginBottom: '4px',
    },
    detailValue: {
        fontSize: '18px', fontWeight: 800, fontFamily: mono,
        lineHeight: 1.2,
    },
    assessmentBadge: (cls) => ({
        display: 'inline-flex', alignItems: 'center',
        padding: '4px 14px', borderRadius: '6px',
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
    narrativePanel: {
        ...shared.cardGradient,
        marginTop: '20px',
        padding: tokens.space.xl,
    },
    narrativeSummary: {
        fontSize: '13px', color: colors.text, fontFamily: mono,
        lineHeight: 1.7, marginBottom: '16px',
    },
    narrativeBullet: {
        display: 'flex', gap: '8px', marginBottom: '10px',
    },
    bulletDot: {
        width: '6px', height: '6px', borderRadius: '50%',
        background: colors.red, marginTop: '6px', flexShrink: 0,
    },
    bulletText: {
        fontSize: '12px', color: colors.textDim, fontFamily: mono, lineHeight: 1.6,
    },
    watchItem: {
        display: 'flex', gap: '8px', alignItems: 'flex-start',
        marginBottom: '6px',
    },
    watchDot: {
        width: '6px', height: '6px', borderRadius: '50%',
        background: colors.yellow, marginTop: '6px', flexShrink: 0,
    },
    watchText: {
        fontSize: '11px', color: colors.textMuted, fontFamily: mono, lineHeight: 1.5,
    },

    // Ledger
    ledgerCard: {
        ...shared.card,
        padding: '12px 14px', marginBottom: '6px',
    },
    ledgerHeader: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: '4px',
    },
    ledgerDate: {
        fontSize: '10px', color: colors.textMuted, fontFamily: mono,
    },
    ledgerVerdict: (v) => ({
        display: 'inline-flex', alignItems: 'center',
        padding: '2px 8px', borderRadius: '4px',
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
    ledgerMove: {
        fontSize: '11px', fontFamily: mono, marginTop: '4px',
    },

    // Score
    scoreRow: {
        display: 'grid', gridTemplateColumns: '1fr 1fr 1fr',
        gap: '10px', marginBottom: '16px',
    },
    scoreCard: {
        ...shared.cardGradient,
        display: 'flex', flexDirection: 'column', alignItems: 'center',
        justifyContent: 'center', padding: '16px 12px', textAlign: 'center',
    },
    bigNumber: {
        fontSize: '32px', fontWeight: 800, fontFamily: mono, lineHeight: 1.1,
    },
    bigLabel: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
        color: colors.textMuted, marginTop: '4px', fontFamily: mono,
    },

    // Tabs
    tabs: { ...shared.tabs, marginBottom: '4px', marginTop: '20px' },
    tab: (active) => shared.tab(active),

    // Sparkline container
    sparkWrap: {
        height: '50px', marginTop: '8px',
    },

    // Loading
    loadingBar: {
        height: '2px', background: colors.bg, borderRadius: '1px',
        marginBottom: '16px', overflow: 'hidden',
    },
    loadingFill: {
        height: '100%', background: colors.accent,
        borderRadius: '1px',
        animation: 'loadSlide 1.5s ease infinite',
        width: '40%',
    },
};

// ── Keyframe animations (injected once) ──────────────────────────────────

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
    `;
    document.head.appendChild(style);
}


// ── Sparkline component (D3) ─────────────────────────────────────────────

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

        const line = d3.line()
            .x((d, i) => x(i))
            .y(d => y(d.value))
            .curve(d3.curveMonotoneX);

        const area = d3.area()
            .x((d, i) => x(i))
            .y0(h)
            .y1(d => y(d.value))
            .curve(d3.curveMonotoneX);

        svg.append('path')
            .datum(data)
            .attr('d', area)
            .attr('fill', color)
            .attr('opacity', 0.08);

        const path = svg.append('path')
            .datum(data)
            .attr('d', line)
            .attr('fill', 'none')
            .attr('stroke', color)
            .attr('stroke-width', 1.5);

        // Animate draw
        const totalLen = path.node().getTotalLength();
        path.attr('stroke-dasharray', `${totalLen} ${totalLen}`)
            .attr('stroke-dashoffset', totalLen)
            .transition().duration(600).ease(d3.easeCubicOut)
            .attr('stroke-dashoffset', 0);
    }, [data, color, w, h]);

    return <svg ref={ref} style={{ display: 'block' }} />;
}


// ── Divergence overlay chart (D3) ────────────────────────────────────────

function DivergenceChart({ official, physical, width: w = 500, height: h = 140 }) {
    const ref = useRef(null);

    useEffect(() => {
        if (!ref.current || !official || !physical) return;
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

        // Defs for glow
        const defs = svg.append('defs');
        const divGrad = defs.append('linearGradient')
            .attr('id', 'div-gap-grad')
            .attr('x1', '0').attr('y1', '0').attr('x2', '0').attr('y2', '1');
        divGrad.append('stop').attr('offset', '0%').attr('stop-color', colors.red).attr('stop-opacity', 0.2);
        divGrad.append('stop').attr('offset', '100%').attr('stop-color', colors.red).attr('stop-opacity', 0.05);

        g.append('path')
            .datum(official)
            .attr('d', areaClip)
            .attr('fill', 'url(#div-gap-grad)');

        // Grid
        const yTicks = y.ticks(4);
        g.selectAll('.grid')
            .data(yTicks).enter()
            .append('line')
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
            .transition().duration(700).ease(d3.easeCubicOut)
            .attr('stroke-dashoffset', 0);

        // Physical line
        const physLine = d3.line().x((d, i) => x(i)).y(d => y(d.value)).curve(d3.curveMonotoneX);
        const physPath = g.append('path').datum(physical)
            .attr('d', physLine).attr('fill', 'none')
            .attr('stroke', colors.red).attr('stroke-width', 2)
            .attr('stroke-dasharray', '6,3');

        const physLen = physPath.node().getTotalLength();
        physPath.attr('stroke-dasharray', `${physLen} ${physLen}`)
            .attr('stroke-dashoffset', physLen)
            .transition().duration(700).ease(d3.easeCubicOut)
            .attr('stroke-dashoffset', 0)
            .on('end', function() {
                d3.select(this).attr('stroke-dasharray', '6,3');
            });

        // Y axis
        g.append('g')
            .call(d3.axisLeft(y).ticks(4).tickSize(0).tickFormat(d3.format('.0f')))
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text')
                .attr('font-size', '9px').attr('font-family', mono).attr('fill', colors.textMuted));

        // X axis
        const months = ['J', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D'];
        g.append('g')
            .attr('transform', `translate(0,${ch})`)
            .call(d3.axisBottom(x).ticks(Math.min(12, official.length)).tickSize(0)
                .tickFormat(i => months[Math.round(i) % 12] || ''))
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text')
                .attr('font-size', '8px').attr('font-family', mono).attr('fill', colors.textMuted));

        // Legend
        const leg = g.append('g').attr('transform', `translate(${cw - 120}, -2)`);
        leg.append('line').attr('x1', 0).attr('x2', 16).attr('y1', 0).attr('y2', 0)
            .attr('stroke', colors.accent).attr('stroke-width', 2);
        leg.append('text').attr('x', 20).attr('y', 3)
            .attr('font-size', '8px').attr('font-family', mono).attr('fill', colors.textMuted)
            .text('Official');
        leg.append('line').attr('x1', 62).attr('x2', 78).attr('y1', 0).attr('y2', 0)
            .attr('stroke', colors.red).attr('stroke-width', 2).attr('stroke-dasharray', '4,2');
        leg.append('text').attr('x', 82).attr('y', 3)
            .attr('font-size', '8px').attr('font-family', mono).attr('fill', colors.textMuted)
            .text('Physical');

    }, [official, physical, w, h]);

    return <svg ref={ref} style={{ display: 'block', width: '100%' }} />;
}


// ── D3 Heatmap Matrix ────────────────────────────────────────────────────

function DivergenceMatrix({ cells, onCellClick, selectedCell }) {
    const containerRef = useRef(null);
    const svgRef = useRef(null);
    const [tooltipData, setTooltipData] = useState(null);
    const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });

    useEffect(() => {
        if (!svgRef.current || !cells) return;

        const totalW = MATRIX_MARGIN.left + REGIONS.length * (CELL_SIZE + CELL_GAP);
        const totalH = MATRIX_MARGIN.top + CATEGORIES.length * (CELL_SIZE + CELL_GAP);

        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();
        svg.attr('width', totalW).attr('height', totalH);

        const defs = svg.append('defs');

        // Glow filters for each divergence level
        ['consistent', 'minor', 'notable', 'major'].forEach(level => {
            const filter = defs.append('filter')
                .attr('id', `glow-${level}`)
                .attr('x', '-30%').attr('y', '-30%')
                .attr('width', '160%').attr('height', '160%');
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
                .attr('font-size', '11px')
                .attr('font-weight', 700)
                .attr('font-family', mono)
                .attr('fill', colors.text)
                .attr('letter-spacing', '1px')
                .text(reg);
        });

        // Row headers
        CATEGORIES.forEach((cat, ri) => {
            g.append('text')
                .attr('x', MATRIX_MARGIN.left - 12)
                .attr('y', MATRIX_MARGIN.top + ri * (CELL_SIZE + CELL_GAP) + CELL_SIZE / 2 + 4)
                .attr('text-anchor', 'end')
                .attr('font-size', '10px')
                .attr('font-weight', 600)
                .attr('font-family', mono)
                .attr('fill', colors.textDim)
                .text(cat);
        });

        // Cells
        CATEGORIES.forEach((cat, ri) => {
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

                // Outer glow rect
                cellG.append('rect')
                    .attr('x', x - 2).attr('y', y - 2)
                    .attr('width', CELL_SIZE + 4).attr('height', CELL_SIZE + 4)
                    .attr('rx', 10)
                    .attr('fill', DIVERGENCE_GLOW[cls])
                    .attr('filter', `url(#glow-${cls})`);

                // Main cell rect with gradient
                const grad = defs.append('linearGradient')
                    .attr('id', `cell-grad-${ri}-${ci}`)
                    .attr('x1', '0').attr('y1', '0').attr('x2', '1').attr('y2', '1');
                grad.append('stop').attr('offset', '0%')
                    .attr('stop-color', DIVERGENCE_COLORS[cls])
                    .attr('stop-opacity', 0.85);
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
                    .transition()
                    .delay(ri * 60 + ci * 40)
                    .duration(400)
                    .attr('opacity', 1);

                // Inner shadow (inset effect)
                cellG.append('rect')
                    .attr('x', x + 2).attr('y', y + 2)
                    .attr('width', CELL_SIZE - 4).attr('height', CELL_SIZE - 4)
                    .attr('rx', 6)
                    .attr('fill', 'none')
                    .attr('stroke', 'rgba(255,255,255,0.06)')
                    .attr('stroke-width', 0.5);

                // Z-score text (large)
                cellG.append('text')
                    .attr('x', x + CELL_SIZE / 2)
                    .attr('y', y + CELL_SIZE / 2 - 2)
                    .attr('text-anchor', 'middle')
                    .attr('dominant-baseline', 'central')
                    .attr('font-size', '18px')
                    .attr('font-weight', 800)
                    .attr('font-family', mono)
                    .attr('fill', cls === 'noData' ? colors.textMuted : '#E8F0F8')
                    .text(cell.zScore != null ? cell.zScore.toFixed(1) + 'σ' : '--');

                // Category abbreviation (subtle)
                cellG.append('text')
                    .attr('x', x + CELL_SIZE / 2)
                    .attr('y', y + CELL_SIZE - 12)
                    .attr('text-anchor', 'middle')
                    .attr('font-size', '8px')
                    .attr('font-family', mono)
                    .attr('fill', 'rgba(255,255,255,0.3)')
                    .attr('letter-spacing', '0.5px')
                    .text(classifyLabel(cell.zScore).split(' ')[0]);

                // Pulsing animation for major divergences
                if (cls === 'major') {
                    const pulseRect = cellG.append('rect')
                        .attr('x', x - 1).attr('y', y - 1)
                        .attr('width', CELL_SIZE + 2).attr('height', CELL_SIZE + 2)
                        .attr('rx', 9)
                        .attr('fill', 'none')
                        .attr('stroke', colors.red)
                        .attr('stroke-width', 1.5)
                        .attr('opacity', 0);

                    (function animatePulse() {
                        pulseRect
                            .attr('opacity', 0.6)
                            .transition().duration(1500).ease(d3.easeSinInOut)
                            .attr('opacity', 0)
                            .on('end', animatePulse);
                    })();
                }

                // Interaction
                cellG
                    .on('mouseenter', function (event) {
                        setTooltipData(cell);
                        const rect = svgRef.current.getBoundingClientRect();
                        setTooltipPos({
                            x: event.clientX + 12,
                            y: event.clientY - 60,
                        });
                    })
                    .on('mousemove', function (event) {
                        setTooltipPos({
                            x: event.clientX + 12,
                            y: event.clientY - 60,
                        });
                    })
                    .on('mouseleave', function () {
                        setTooltipData(null);
                    })
                    .on('click', function () {
                        onCellClick(key);
                    });
            });
        });

    }, [cells, selectedCell, onCellClick]);

    return (
        <div ref={containerRef} style={s.matrixWrap}>
            <svg ref={svgRef} style={{ display: 'block' }} />
            {tooltipData && (
                <div style={{
                    ...s.tooltip,
                    left: tooltipPos.x,
                    top: tooltipPos.y,
                }}>
                    <div style={s.tooltipTitle}>
                        {tooltipData.region} {tooltipData.category}
                    </div>
                    <div style={s.tooltipRow}>
                        <span style={s.tooltipLabel}>Official ({tooltipData.officialSource})</span>
                        <span style={s.tooltipValue}>{tooltipData.officialValue}</span>
                    </div>
                    <div style={s.tooltipRow}>
                        <span style={s.tooltipLabel}>Physical ({tooltipData.physicalSource})</span>
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
                    <div style={{
                        fontSize: '10px', color: colors.textDim, fontFamily: mono,
                        marginTop: '8px', lineHeight: 1.4,
                        borderTop: `1px solid ${colors.borderSubtle}`, paddingTop: '6px',
                    }}>
                        {tooltipData.implication}
                    </div>
                </div>
            )}
        </div>
    );
}


// ── Main Component ───────────────────────────────────────────────────────

export default function CrossReference() {
    const [data, setData] = useState(null);
    const [history, setHistory] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [selectedCell, setSelectedCell] = useState(null);
    const [activeTab, setActiveTab] = useState('matrix');
    const detailRef = useRef(null);

    useEffect(() => {
        ensureKeyframes();
        loadData();
    }, []);

    async function loadData() {
        setLoading(true);
        try {
            // Try API first, fall back to placeholder
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

            if (crossRef && crossRef.cells) {
                setData(crossRef);
            } else {
                setData(generatePlaceholderData());
            }

            if (hist && Array.isArray(hist.entries)) {
                setHistory(hist.entries);
            } else {
                setHistory(generatePlaceholderHistory());
            }
        } catch (err) {
            setError(err.message);
            setData(generatePlaceholderData());
            setHistory(generatePlaceholderHistory());
        } finally {
            setLoading(false);
        }
    }

    const handleCellClick = useCallback((key) => {
        setSelectedCell(prev => prev === key ? null : key);
        // Scroll to detail after render
        setTimeout(() => {
            detailRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }, 100);
    }, []);

    const handleFlagClick = useCallback((cat, reg) => {
        const key = `${cat}|${reg}`;
        setSelectedCell(key);
        setActiveTab('matrix');
        setTimeout(() => {
            detailRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }, 100);
    }, []);

    if (loading) {
        return (
            <div style={s.container}>
                <div style={s.header}>
                    <div style={s.title}>CROSS-REFERENCE ENGINE</div>
                    <div style={s.subtitle}>Government statistics vs. physical reality</div>
                </div>
                <div style={s.loadingBar}>
                    <div style={s.loadingFill} />
                </div>
                <div style={{ textAlign: 'center', color: colors.textMuted, fontSize: '13px', fontFamily: mono, padding: '60px 0' }}>
                    Scanning divergences across 25 indicator pairs...
                </div>
            </div>
        );
    }

    const { cells, redFlags, narrative } = data || {};
    const selectedData = selectedCell ? cells?.[selectedCell] : null;

    // Ledger stats
    const confirmed = (history || []).filter(h => h.verdict === 'confirmed').length;
    const total = (history || []).length;
    const hitRate = total > 0 ? ((confirmed / total) * 100).toFixed(0) : '--';
    const activeFlags = (redFlags || []).length;

    return (
        <div style={s.container}>
            {/* ── Header ── */}
            <div style={s.header}>
                <div style={s.title}>CROSS-REFERENCE ENGINE</div>
                <div style={s.subtitle}>Government statistics vs. physical reality</div>
            </div>

            {/* ── Score Row ── */}
            <div style={s.scoreRow}>
                <div style={s.scoreCard}>
                    <div style={{ ...s.bigNumber, color: activeFlags > 0 ? colors.red : colors.green }}>
                        {activeFlags}
                    </div>
                    <div style={s.bigLabel}>RED FLAGS</div>
                </div>
                <div style={s.scoreCard}>
                    <div style={{ ...s.bigNumber, color: colors.green }}>{hitRate}%</div>
                    <div style={s.bigLabel}>HIT RATE</div>
                </div>
                <div style={s.scoreCard}>
                    <div style={{ ...s.bigNumber, color: colors.text }}>{total}</div>
                    <div style={s.bigLabel}>FLAGS TRACKED</div>
                </div>
            </div>

            {/* ── Red Flags Banner ── */}
            {redFlags && redFlags.length > 0 ? (
                <div style={s.flagBanner}>
                    <div style={s.sectionTitle}>ACTIVE RED FLAGS</div>
                    <div style={s.flagScroller}>
                        {redFlags.map((flag, i) => (
                            <div
                                key={i}
                                className="crossref-flag-card"
                                style={s.flagCard}
                                onClick={() => handleFlagClick(flag.category, flag.region)}
                            >
                                <div style={s.flagHeadline}>{flag.headline}</div>
                                <div style={s.flagZBadge}>{formatZ(flag.zScore)}</div>
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
                {['matrix', 'narrative', 'ledger'].map(tab => (
                    <button
                        key={tab}
                        style={s.tab(activeTab === tab)}
                        onClick={() => setActiveTab(tab)}
                    >
                        {tab === 'matrix' ? 'Divergence Matrix' :
                         tab === 'narrative' ? 'Intelligence Brief' :
                         'Lies Ledger'}
                    </button>
                ))}
            </div>

            {/* ── Matrix Tab ── */}
            {activeTab === 'matrix' && (
                <>
                    <div style={s.sectionTitle}>DIVERGENCE MATRIX</div>
                    <DivergenceMatrix
                        cells={cells}
                        onCellClick={handleCellClick}
                        selectedCell={selectedCell}
                    />

                    {/* Legend */}
                    <div style={{
                        display: 'flex', gap: '16px', marginTop: '12px', flexWrap: 'wrap',
                        alignItems: 'center',
                    }}>
                        {[
                            { label: 'Consistent', color: DIVERGENCE_COLORS.consistent, range: '<0.5σ' },
                            { label: 'Minor', color: DIVERGENCE_COLORS.minor, range: '0.5-1.5σ' },
                            { label: 'Notable', color: DIVERGENCE_COLORS.notable, range: '1.5-2.0σ' },
                            { label: 'Contradiction', color: DIVERGENCE_COLORS.major, range: '>2.0σ' },
                            { label: 'No Data', color: DIVERGENCE_COLORS.noData, range: '' },
                        ].map(item => (
                            <div key={item.label} style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <div style={{
                                    width: '12px', height: '12px', borderRadius: '3px',
                                    background: item.color,
                                }} />
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
                                    <div style={{ marginTop: '6px' }}>
                                        <span style={s.assessmentBadge(selectedData.classification)}>
                                            {classifyLabel(selectedData.zScore)}
                                        </span>
                                        <span style={{
                                            fontSize: '14px', fontWeight: 800, fontFamily: mono,
                                            marginLeft: '10px',
                                            color: selectedData.classification === 'major' ? colors.red :
                                                   selectedData.classification === 'notable' ? '#F97316' :
                                                   selectedData.classification === 'minor' ? colors.yellow : colors.green,
                                        }}>
                                            {formatZ(selectedData.zScore)}
                                        </span>
                                    </div>
                                </div>
                                <button style={s.detailClose} onClick={() => setSelectedCell(null)}>
                                    CLOSE
                                </button>
                            </div>

                            {/* Two-column comparison */}
                            <div style={s.detailColumns}>
                                <div style={s.detailCol}>
                                    <div style={{ ...s.detailColTitle, color: colors.accent }}>
                                        OFFICIAL STORY
                                    </div>
                                    <div style={s.detailSource}>{selectedData.officialSource}</div>
                                    <div style={{ ...s.detailValue, color: colors.accent }}>
                                        {selectedData.officialValue}
                                    </div>
                                    <div style={s.sparkWrap}>
                                        <Sparkline data={selectedData.officialTrend} color={colors.accent} />
                                    </div>
                                </div>
                                <div style={s.detailCol}>
                                    <div style={{ ...s.detailColTitle, color: colors.red }}>
                                        PHYSICAL REALITY
                                    </div>
                                    <div style={s.detailSource}>{selectedData.physicalSource}</div>
                                    <div style={{ ...s.detailValue, color: colors.red }}>
                                        {selectedData.physicalValue}
                                    </div>
                                    <div style={s.sparkWrap}>
                                        <Sparkline data={selectedData.physicalTrend} color={colors.red} />
                                    </div>
                                </div>
                            </div>

                            {/* Divergence overlay chart */}
                            <div style={s.sectionTitle}>DIVERGENCE X-RAY</div>
                            <div style={{
                                background: colors.bg, borderRadius: tokens.radius.md,
                                padding: '12px', marginBottom: '12px',
                            }}>
                                <DivergenceChart
                                    official={selectedData.officialTrend}
                                    physical={selectedData.physicalTrend}
                                />
                            </div>

                            {/* Implication */}
                            <div style={{
                                background: `${selectedData.classification === 'major' ? colors.red : colors.accent}08`,
                                border: `1px solid ${selectedData.classification === 'major' ? colors.red : colors.accent}15`,
                                borderRadius: tokens.radius.sm,
                                padding: '12px 14px', marginBottom: '12px',
                            }}>
                                <div style={{
                                    fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                                    color: selectedData.classification === 'major' ? colors.red : colors.accent,
                                    fontFamily: mono, marginBottom: '4px',
                                }}>
                                    MARKET IMPLICATION
                                </div>
                                <div style={{
                                    fontSize: '12px', color: colors.text, fontFamily: mono, lineHeight: 1.5,
                                }}>
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

            {/* ── Narrative Tab ── */}
            {activeTab === 'narrative' && narrative && (
                <div style={s.narrativePanel}>
                    <div style={{ ...s.sectionTitle, marginTop: 0 }}>
                        WHAT THE DATA IS TELLING US THAT THE HEADLINES AREN'T
                    </div>
                    <div style={s.narrativeSummary}>{narrative.summary}</div>

                    <div style={{ ...s.sectionTitle, color: colors.red }}>KEY REVELATIONS</div>
                    {narrative.bullets.map((b, i) => (
                        <div key={i} style={s.narrativeBullet}>
                            <div style={s.bulletDot} />
                            <div style={s.bulletText}>{b}</div>
                        </div>
                    ))}

                    <div style={{ ...s.sectionTitle, color: colors.yellow, marginTop: '20px' }}>
                        WATCH FOR
                    </div>
                    {narrative.watchFor.map((w, i) => (
                        <div key={i} style={s.watchItem}>
                            <div style={s.watchDot} />
                            <div style={s.watchText}>{w}</div>
                        </div>
                    ))}
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
                            {confirmed}/{total} confirmed
                        </span>
                        <span style={{ fontSize: '11px', fontFamily: mono, color: colors.textMuted }}>
                            ({hitRate}% accuracy)
                        </span>
                        <span style={{ fontSize: '11px', fontFamily: mono, color: colors.textDim }}>
                            When we flag a divergence, the subsequent data confirms it {hitRate}% of the time
                        </span>
                    </div>

                    {(history || []).map((entry, i) => (
                        <div key={i} style={s.ledgerCard}>
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
                            <div style={{
                                ...s.ledgerMove,
                                color: entry.verdict === 'confirmed' ? colors.green : colors.textMuted,
                            }}>
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
