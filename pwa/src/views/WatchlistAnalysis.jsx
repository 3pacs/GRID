/**
 * Watchlist Ticker Analysis — premium full-page deep dive on a single ticker.
 *
 * Shows AI overview, capital flow path, price chart, options signals,
 * related features, regime context, and TradingView webhook history.
 */
import React, { useEffect, useState, useRef, useCallback, useMemo } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { shared, colors, tokens } from '../styles/shared.js';
import { formatTime, formatDate } from '../utils/formatTime.js';
import { useDevice } from '../hooks/useDevice.js';
import PriceChart from '../components/PriceChart.jsx';
import GEXProfile from '../components/GEXProfile.jsx';
import VannaCharmViz from '../components/VannaCharmViz.jsx';
import FlowTimeline from '../components/FlowTimeline.jsx';
import { TickerRecommendations } from './Options.jsx';

/* ═══════════════════════════════════════════════════════════════════
   Shared sub-components
   ═══════════════════════════════════════════════════════════════════ */

function StatCard({ label, value, sub, color }) {
    return (
        <div style={shared.metric}>
            <div style={{ ...shared.metricValue, fontSize: '15px', color: color || '#E8F0F8' }}>{value}</div>
            <div style={shared.metricLabel}>{label}</div>
            {sub && <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '2px' }}>{sub}</div>}
        </div>
    );
}

function SignalDot({ value }) {
    if (value == null) return null;
    const color = value > 0 ? colors.green : value < 0 ? colors.red : colors.yellow;
    const label = value > 0 ? 'BUY' : value < 0 ? 'SELL' : 'ALERT';
    return (
        <span style={{
            fontSize: '10px', fontWeight: 700, padding: '2px 8px',
            borderRadius: '4px', background: `${color}20`, color,
            fontFamily: "'JetBrains Mono', monospace",
        }}>{label}</span>
    );
}

/* ═══════════════════════════════════════════════════════════════════
   Skeleton loader for AI overview
   ═══════════════════════════════════════════════════════════════════ */

function OverviewSkeleton() {
    const barStyle = (width) => ({
        height: '12px',
        width,
        background: `linear-gradient(90deg, ${colors.border} 25%, ${colors.borderSubtle} 50%, ${colors.border} 75%)`,
        backgroundSize: '200% 100%',
        animation: 'shimmer 1.5s infinite',
        borderRadius: '4px',
        marginBottom: '8px',
    });
    return (
        <div style={{ ...shared.cardGradient, position: 'relative', overflow: 'hidden' }}>
            <style>{`@keyframes shimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }`}</style>
            <div style={{ display: 'flex', gap: '8px', alignItems: 'center', marginBottom: '12px' }}>
                <div style={{ ...barStyle('60px'), height: '22px', marginBottom: 0 }} />
                <div style={{ ...barStyle('80px'), height: '22px', marginBottom: 0 }} />
            </div>
            <div style={barStyle('100%')} />
            <div style={barStyle('90%')} />
            <div style={barStyle('75%')} />
            <div style={{ display: 'flex', gap: '6px', marginTop: '12px' }}>
                <div style={{ ...barStyle('50px'), height: '24px', borderRadius: '12px', marginBottom: 0 }} />
                <div style={{ ...barStyle('60px'), height: '24px', borderRadius: '12px', marginBottom: 0 }} />
            </div>
        </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════
   AI Overview card
   ═══════════════════════════════════════════════════════════════════ */

function CollapsibleSection({ title, body, defaultExpanded = true }) {
    const [expanded, setExpanded] = useState(defaultExpanded);

    return (
        <div style={{
            background: colors.bg,
            borderRadius: tokens.radius.sm,
            border: `1px solid ${colors.borderSubtle}`,
            marginBottom: '6px',
            overflow: 'hidden',
        }}>
            <button
                onClick={() => setExpanded(e => !e)}
                style={{
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    width: '100%', padding: '12px',
                    background: 'transparent', border: 'none', cursor: 'pointer',
                    color: colors.text, fontFamily: "'JetBrains Mono', monospace",
                    fontSize: '12px', fontWeight: 600, letterSpacing: '0.5px',
                    minHeight: tokens.minTouch,
                }}
            >
                <span>{title}</span>
                <span style={{
                    fontSize: '10px', color: colors.textMuted,
                    transition: `transform ${tokens.transition.fast}`,
                    transform: expanded ? 'rotate(180deg)' : 'rotate(0deg)',
                }}>
                    &#9660;
                </span>
            </button>
            {expanded && (
                <div style={{
                    padding: '0 12px 10px 12px',
                    fontSize: '13px', lineHeight: '1.65', color: colors.textDim,
                    fontFamily: colors.sans,
                }}>
                    {body}
                </div>
            )}
        </div>
    );
}

function AIOverviewCard({ overview }) {
    if (!overview) return null;

    const sentimentColors = {
        bullish: { bg: `${colors.green}15`, border: `${colors.green}40`, text: colors.green },
        bearish: { bg: `${colors.red}15`, border: `${colors.red}40`, text: colors.red },
        neutral: { bg: `${colors.textMuted}15`, border: `${colors.textMuted}40`, text: colors.textMuted },
    };
    const sc = sentimentColors[overview.sentiment] || sentimentColors.neutral;

    const sections = overview.sections || [];
    const hasSections = sections.length > 0;

    return (
        <div style={{
            ...shared.cardGradient,
            borderLeft: `3px solid ${sc.text}`,
            position: 'relative',
        }}>
            {/* Header row: AI Generated label + sentiment */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={{
                        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                        color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                    }}>AI OVERVIEW</span>
                    <span style={{
                        fontSize: '9px', fontWeight: 500, letterSpacing: '0.5px',
                        color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace",
                        background: `${colors.textMuted}10`,
                        padding: '2px 6px', borderRadius: tokens.radius.sm,
                    }}>
                        AI Generated{overview.generated_at ? ` \u00b7 ${formatTime(overview.generated_at)}` : ''}
                    </span>
                </div>
                <span style={{
                    fontSize: '11px', fontWeight: 700, padding: '3px 10px',
                    borderRadius: tokens.radius.pill, background: sc.bg,
                    border: `1px solid ${sc.border}`, color: sc.text,
                    fontFamily: "'JetBrains Mono', monospace",
                    textTransform: 'uppercase', letterSpacing: '0.5px',
                }}>
                    {overview.sentiment}
                </span>
            </div>

            {/* Bottom line — prominent */}
            {overview.bottom_line && (
                <div style={{
                    fontSize: '15px', fontWeight: 700, lineHeight: '1.5',
                    color: '#E8F0F8', fontFamily: colors.sans,
                    marginBottom: hasSections ? '14px' : '0',
                    padding: '8px 0',
                    borderBottom: hasSections ? `1px solid ${colors.borderSubtle}` : 'none',
                }}>
                    {overview.bottom_line}
                </div>
            )}

            {/* Structured sections (collapsible) */}
            {hasSections ? (
                <div style={{ marginBottom: '2px' }}>
                    {sections.map((section, i) => (
                        <CollapsibleSection
                            key={i}
                            title={section.title}
                            body={section.body}
                            defaultExpanded={true}
                        />
                    ))}
                </div>
            ) : (
                /* Legacy fallback: plain overview text */
                <div style={{
                    fontSize: '14px', lineHeight: '1.65', color: colors.text,
                    fontFamily: colors.sans,
                }}>
                    {overview.overview}
                </div>
            )}

            {/* Key levels pills */}
            {overview.key_levels && overview.key_levels.length > 0 && (
                <div style={{ display: 'flex', gap: '6px', marginTop: '14px', flexWrap: 'wrap' }}>
                    {overview.key_levels.map((level, i) => (
                        <span key={i} style={{
                            fontSize: '11px', padding: '4px 10px',
                            borderRadius: tokens.radius.pill,
                            background: colors.bg, border: `1px solid ${colors.border}`,
                            color: colors.textDim, fontFamily: "'JetBrains Mono', monospace",
                        }}>
                            {level.label}: ${typeof level.value === 'number' ? level.value.toLocaleString(undefined, { maximumFractionDigits: 0 }) : level.value}
                        </span>
                    ))}
                </div>
            )}
        </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════
   Insider Edge panel — aggregated intelligence signals
   ═══════════════════════════════════════════════════════════════════ */

const SIGNAL_ICONS = {
    congressional: '\u{1F3DB}',  // bank
    insider: '\u{1F464}',        // person
    dark_pool: '\u{1F30A}',      // wave
    whale_flow: '\u{1F433}',     // whale
    prediction_markets: '\u{1F52E}', // crystal ball
    smart_money: '\u{1F9E0}',    // brain
    lever_pullers: '\u{1F3AF}',  // target
};

function TrustBar({ score, width = 48 }) {
    const pct = Math.max(0, Math.min(1, score || 0));
    const barColor = pct >= 0.7 ? colors.green : pct >= 0.5 ? colors.yellow : colors.red;
    return (
        <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
            <div style={{
                width: `${width}px`, height: '4px', borderRadius: '2px',
                background: colors.borderSubtle, overflow: 'hidden',
            }}>
                <div style={{
                    width: `${pct * 100}%`, height: '100%',
                    background: barColor, borderRadius: '2px',
                    transition: 'width 0.3s ease',
                }} />
            </div>
            <span style={{ fontSize: '9px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace" }}>
                {(pct * 100).toFixed(0)}
            </span>
        </div>
    );
}

function SignalCard({ icon, label, actor, action, date, trustScore, direction }) {
    const dirColor = direction === 'bullish' || action === 'BUY' || action === 'CALL' || action === 'INCREASED'
        ? colors.green
        : direction === 'bearish' || action === 'SELL' || action === 'PUT' || action === 'DECREASED'
            ? colors.red : colors.textDim;
    return (
        <div style={{
            background: colors.bg, border: `1px solid ${colors.borderSubtle}`,
            borderLeft: `3px solid ${dirColor}`, borderRadius: tokens.radius.sm,
            padding: '8px 10px', display: 'flex', flexDirection: 'column', gap: '4px',
            minWidth: 0,
        }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span style={{ fontSize: '10px', color: colors.textMuted }}>{icon} {label}</span>
                <span style={{
                    fontSize: '9px', fontWeight: 700, padding: '4px 8px',
                    borderRadius: '999px', background: `${dirColor}18`, color: dirColor,
                    fontFamily: "'JetBrains Mono', monospace",
                    whiteSpace: 'nowrap', flexShrink: 0,
                }}>{action}</span>
            </div>
            <div title={actor} style={{ fontSize: '12px', fontWeight: 600, color: colors.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', lineHeight: '1.3' }}>
                {actor}
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span style={{ fontSize: '9px', color: colors.textMuted }}>{date ? formatDate(date) : ''}</span>
                {trustScore != null && <TrustBar score={trustScore} />}
            </div>
        </div>
    );
}

function InsiderEdgePanel({ edgeData, loading }) {
    const [expanded, setExpanded] = useState(true);

    if (loading) return <OverviewSkeleton />;
    if (!edgeData) return null;

    const { congressional, insider, dark_pool, whale_flow, prediction_markets,
            smart_money, lever_pullers, leads, convergence, edge_summary } = edgeData;

    const hasSignals = (congressional?.length || insider?.length || dark_pool ||
        whale_flow?.length || prediction_markets?.length || smart_money?.length ||
        lever_pullers?.length);

    if (!hasSignals && !leads?.length) return null;

    const dirColor = convergence?.direction === 'bullish'
        ? colors.green : convergence?.direction === 'bearish'
            ? colors.red : colors.textMuted;

    return (
        <div style={{
            ...shared.cardGradient,
            borderLeft: `3px solid ${dirColor}`,
            marginTop: '12px',
        }}>
            {/* Header */}
            <button
                onClick={() => setExpanded(e => !e)}
                style={{
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    width: '100%', padding: '0 0 10px 0',
                    background: 'transparent', border: 'none', cursor: 'pointer',
                    color: colors.text, fontFamily: "'JetBrains Mono', monospace",
                }}
            >
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={{ fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px', color: colors.accent }}>
                        INSIDER EDGE
                    </span>
                    {convergence && convergence.source_count > 0 && (
                        <span style={{
                            fontSize: '9px', fontWeight: 700, padding: '2px 8px',
                            borderRadius: tokens.radius.pill,
                            background: `${dirColor}18`, color: dirColor,
                            border: `1px solid ${dirColor}40`,
                        }}>
                            {convergence.source_count} sources {convergence.direction}
                            {convergence.confidence ? ` \u00b7 ${(convergence.confidence * 100).toFixed(0)}%` : ''}
                        </span>
                    )}
                </div>
                <span style={{
                    fontSize: '10px', color: colors.textMuted,
                    transition: `transform ${tokens.transition.fast}`,
                    transform: expanded ? 'rotate(180deg)' : 'rotate(0deg)',
                }}>&#9660;</span>
            </button>

            {expanded && (
                <div>
                    {/* Edge Summary */}
                    {edge_summary && (
                        <div style={{
                            fontSize: '13px', lineHeight: '1.6', color: colors.text,
                            fontFamily: colors.sans, marginBottom: '12px',
                            padding: '10px', background: `${dirColor}08`,
                            borderRadius: tokens.radius.sm,
                            border: `1px solid ${dirColor}20`,
                        }}>
                            {edge_summary}
                        </div>
                    )}

                    {/* Signal Cards Grid */}
                    <div style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))',
                        gap: '8px', marginBottom: leads?.length ? '12px' : '0',
                    }}>
                        {congressional?.map((c, i) => (
                            <SignalCard key={`cong-${i}`}
                                icon={SIGNAL_ICONS.congressional} label="Congress"
                                actor={c.member} action={c.action} date={c.date}
                                trustScore={c.trust_score} direction={c.action === 'BUY' ? 'bullish' : 'bearish'}
                            />
                        ))}
                        {insider?.map((ins, i) => (
                            <SignalCard key={`ins-${i}`}
                                icon={SIGNAL_ICONS.insider} label={ins.title || 'Insider'}
                                actor={ins.name} action={ins.action} date={ins.date}
                                trustScore={null}
                                direction={ins.action === 'BUY' ? 'bullish' : 'bearish'}
                            />
                        ))}
                        {dark_pool && (
                            <SignalCard
                                icon={SIGNAL_ICONS.dark_pool} label="Dark Pool"
                                actor={`${dark_pool.volume_vs_avg?.toFixed(1)}x avg volume`}
                                action={dark_pool.signal?.toUpperCase() || 'NEUTRAL'}
                                date={dark_pool.date}
                                trustScore={null}
                                direction={dark_pool.signal === 'accumulation' ? 'bullish' : 'bearish'}
                            />
                        )}
                        {whale_flow?.map((w, i) => (
                            <SignalCard key={`whale-${i}`}
                                icon={SIGNAL_ICONS.whale_flow} label="Whale Flow"
                                actor={w.strike ? `$${w.strike} ${w.expiry || ''}` : 'Options flow'}
                                action={w.direction} date={w.date}
                                trustScore={null}
                                direction={w.direction === 'CALL' || w.direction === 'BUY' ? 'bullish' : 'bearish'}
                            />
                        ))}
                        {prediction_markets?.map((p, i) => (
                            <SignalCard key={`pred-${i}`}
                                icon={SIGNAL_ICONS.prediction_markets} label="Prediction Mkt"
                                actor={p.market}
                                action={p.probability >= 0.6 ? 'LIKELY' : p.probability <= 0.4 ? 'UNLIKELY' : 'TOSS-UP'}
                                date={null}
                                trustScore={p.probability}
                                direction={p.change_24h > 0 ? 'bullish' : p.change_24h < 0 ? 'bearish' : 'neutral'}
                            />
                        ))}
                        {smart_money?.map((s, i) => (
                            <SignalCard key={`smart-${i}`}
                                icon={SIGNAL_ICONS.smart_money} label={s.source || 'Social'}
                                actor={s.user} action={s.direction} date={null}
                                trustScore={s.trust_score}
                                direction={s.direction === 'BUY' ? 'bullish' : 'bearish'}
                            />
                        ))}
                        {lever_pullers?.map((lp, i) => (
                            <SignalCard key={`lp-${i}`}
                                icon={SIGNAL_ICONS.lever_pullers} label="Lever Puller"
                                actor={lp.name}
                                action={lp.action}
                                date={null}
                                trustScore={null}
                                direction={lp.action === 'INCREASED' || lp.action === 'BUY' ? 'bullish'
                                    : lp.action === 'DECREASED' || lp.action === 'SELL' ? 'bearish' : 'neutral'}
                            />
                        ))}
                    </div>

                    {/* Active Leads */}
                    {leads?.length > 0 && (
                        <div>
                            <div style={{ fontSize: '9px', fontWeight: 700, letterSpacing: '1px', color: colors.textMuted, marginBottom: '6px' }}>
                                ACTIVE LEADS
                            </div>
                            {leads.map((lead, i) => {
                                const statusColor = lead.status === 'investigating' ? colors.yellow
                                    : lead.status === 'confirmed' ? colors.green
                                        : lead.status === 'dismissed' ? colors.textMuted : colors.accent;
                                return (
                                    <div key={i} style={{
                                        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                        padding: '6px 0',
                                        borderBottom: i < leads.length - 1 ? `1px solid ${colors.borderSubtle}` : 'none',
                                    }}>
                                        <span style={{ fontSize: '12px', color: colors.text, fontFamily: colors.sans }}>
                                            {lead.question}
                                        </span>
                                        <span style={{
                                            fontSize: '9px', fontWeight: 700, padding: '2px 8px',
                                            borderRadius: '3px', background: `${statusColor}18`,
                                            color: statusColor, flexShrink: 0, marginLeft: '8px',
                                            fontFamily: "'JetBrains Mono', monospace",
                                            textTransform: 'uppercase',
                                        }}>
                                            {lead.status}
                                        </span>
                                    </div>
                                );
                            })}
                        </div>
                    )}

                    {/* Trust Indicator */}
                    {convergence && convergence.source_count > 0 && (
                        <div style={{
                            display: 'flex', alignItems: 'center', gap: '12px',
                            marginTop: '10px', padding: '8px 10px',
                            background: colors.bg, borderRadius: tokens.radius.sm,
                            border: `1px solid ${colors.borderSubtle}`,
                        }}>
                            <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace" }}>
                                TRUST
                            </span>
                            <div style={{ flex: 1 }}>
                                <TrustBar score={convergence.confidence} width={120} />
                            </div>
                            <span style={{ fontSize: '10px', color: dirColor, fontFamily: "'JetBrains Mono', monospace", fontWeight: 600 }}>
                                {convergence.source_count} independent source{convergence.source_count !== 1 ? 's' : ''}
                            </span>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════
   Options Intelligence card
   ═══════════════════════════════════════════════════════════════════ */

function OptionsIntel({ opts }) {
    const chartRef = useRef(null);

    const spot = opts.spot_price;
    const maxPain = opts.max_pain;
    const pcr = opts.put_call_ratio;
    const ivAtm = opts.iv_atm;
    const ivSkew = opts.iv_skew;
    const totalOi = opts.total_oi;
    const oiConcentration = opts.oi_concentration; // may contain strike levels

    // Determine bullish/bearish from spot vs max pain
    const isBullish = spot != null && maxPain != null && spot > maxPain;
    const regionColor = isBullish ? colors.green : colors.red;

    // Parse key strikes from oi_concentration if available (expects comma-sep or array)
    const keyStrikes = useMemo(() => {
        if (!oiConcentration) return [];
        if (Array.isArray(oiConcentration)) return oiConcentration.filter(v => typeof v === 'number');
        if (typeof oiConcentration === 'string') {
            return oiConcentration.split(',').map(s => parseFloat(s.trim())).filter(v => !isNaN(v));
        }
        if (typeof oiConcentration === 'number') return [oiConcentration];
        return [];
    }, [oiConcentration]);

    // ── Price positioning chart (D3) ──
    useEffect(() => {
        if (!chartRef.current || spot == null || maxPain == null) return;

        const svg = d3.select(chartRef.current);
        svg.selectAll('*').remove();

        const width = 560;
        const height = 200;
        const margin = { top: 24, right: 30, bottom: 32, left: 30 };
        const plotW = width - margin.left - margin.right;
        const plotH = height - margin.top - margin.bottom;

        // Determine price domain
        const allPrices = [spot, maxPain, ...keyStrikes];
        const pMin = d3.min(allPrices);
        const pMax = d3.max(allPrices);
        const pad = (pMax - pMin) * 0.25 || pMax * 0.05;
        const domainLo = Math.max(0, pMin - pad);
        const domainHi = pMax + pad;

        const x = d3.scaleLinear().domain([domainLo, domainHi]).range([0, plotW]);

        const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

        // Subtle grid lines
        const ticks = x.ticks(6);
        g.selectAll('.grid')
            .data(ticks)
            .enter().append('line')
            .attr('x1', d => x(d)).attr('x2', d => x(d))
            .attr('y1', 0).attr('y2', plotH)
            .attr('stroke', colors.borderSubtle).attr('stroke-width', 0.5);

        // Shaded region between spot and max pain
        const xLo = x(Math.min(spot, maxPain));
        const xHi = x(Math.max(spot, maxPain));
        g.append('rect')
            .attr('x', xLo).attr('y', 8)
            .attr('width', Math.max(0, xHi - xLo)).attr('height', plotH - 16)
            .attr('fill', regionColor).attr('opacity', 0.10)
            .attr('rx', 4);

        // Label the gravitational pull zone
        if (Math.abs(xHi - xLo) > 30) {
            g.append('text')
                .attr('x', (xLo + xHi) / 2).attr('y', plotH / 2 + 3)
                .attr('text-anchor', 'middle')
                .attr('font-size', '9').attr('font-family', "'JetBrains Mono', monospace")
                .attr('fill', regionColor).attr('opacity', 0.6)
                .text(isBullish ? 'bullish pull' : 'bearish pull');
        }

        // Max pain dashed line
        g.append('line')
            .attr('x1', x(maxPain)).attr('x2', x(maxPain))
            .attr('y1', 0).attr('y2', plotH)
            .attr('stroke', colors.yellow).attr('stroke-width', 1.5)
            .attr('stroke-dasharray', '6,4');

        g.append('text')
            .attr('x', x(maxPain)).attr('y', -6)
            .attr('text-anchor', 'middle')
            .attr('font-size', '9').attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.yellow)
            .text(`Max Pain $${maxPain.toLocaleString(undefined, { maximumFractionDigits: 0 })}`);

        // Key strike levels (if available)
        keyStrikes.forEach(strike => {
            if (strike === maxPain || strike === spot) return;
            g.append('line')
                .attr('x1', x(strike)).attr('x2', x(strike))
                .attr('y1', 4).attr('y2', plotH - 4)
                .attr('stroke', colors.textMuted).attr('stroke-width', 0.8)
                .attr('stroke-dasharray', '3,3');
            g.append('text')
                .attr('x', x(strike)).attr('y', plotH + 14)
                .attr('text-anchor', 'middle')
                .attr('font-size', '8').attr('font-family', "'JetBrains Mono', monospace")
                .attr('fill', colors.textMuted)
                .text(`$${strike.toLocaleString(undefined, { maximumFractionDigits: 0 })}`);
        });

        // Spot price bright dot
        g.append('circle')
            .attr('cx', x(spot)).attr('cy', plotH / 2)
            .attr('r', 6).attr('fill', colors.accent)
            .attr('stroke', '#fff').attr('stroke-width', 1.5);

        // Spot glow
        g.append('circle')
            .attr('cx', x(spot)).attr('cy', plotH / 2)
            .attr('r', 14).attr('fill', colors.accent).attr('opacity', 0.12);

        g.append('text')
            .attr('x', x(spot)).attr('y', plotH / 2 - 16)
            .attr('text-anchor', 'middle')
            .attr('font-size', '10').attr('font-weight', '700')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', '#E8F0F8')
            .text(`Spot $${spot.toLocaleString(undefined, { maximumFractionDigits: 0 })}`);

        // X-axis labels
        g.selectAll('.tick-label')
            .data(ticks)
            .enter().append('text')
            .attr('x', d => x(d)).attr('y', plotH + 20)
            .attr('text-anchor', 'middle')
            .attr('font-size', '8').attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.textMuted)
            .text(d => `$${d.toLocaleString(undefined, { maximumFractionDigits: 0 })}`);

        // Bottom axis line
        g.append('line')
            .attr('x1', 0).attr('x2', plotW)
            .attr('y1', plotH).attr('y2', plotH)
            .attr('stroke', colors.border).attr('stroke-width', 0.5);

    }, [spot, maxPain, keyStrikes, isBullish, regionColor]);

    // ── Metrics helpers ──
    const pcrColor = pcr != null ? (pcr < 0.7 ? colors.green : pcr > 1.3 ? colors.red : colors.text) : colors.text;
    const pcrBarWidth = pcr != null ? Math.min(100, (pcr / 2) * 100) : 0;

    const ivPct = ivAtm != null ? ivAtm * 100 : null;
    const ivLabel = ivPct != null
        ? (ivPct < 15 ? 'Low' : ivPct < 30 ? 'Normal' : ivPct < 50 ? 'Elevated' : 'Extreme')
        : '--';
    const ivColor = ivPct != null
        ? (ivPct < 15 ? colors.green : ivPct < 30 ? colors.text : ivPct < 50 ? colors.yellow : colors.red)
        : colors.textMuted;
    const ivGaugeAngle = ivPct != null ? Math.min(180, (ivPct / 80) * 180) : 0;

    const skewText = ivSkew != null
        ? (ivSkew > 1.3 ? 'Put demand high' : ivSkew < 0.9 ? 'Complacent' : 'Normal')
        : '--';

    const oiFmt = totalOi != null
        ? (totalOi >= 1e6 ? `${(totalOi / 1e6).toFixed(1)}M` : `${(totalOi / 1e3).toFixed(0)}K`)
        : '--';

    // ── Interpretation ──
    const interpretation = useMemo(() => {
        const signals = [];
        let bias = 0;

        if (pcr != null) {
            if (pcr < 0.7) { signals.push(`low P/C ${pcr.toFixed(2)}`); bias += 1; }
            else if (pcr > 1.3) { signals.push(`high P/C ${pcr.toFixed(2)}`); bias -= 1; }
        }
        if (ivPct != null) {
            if (ivPct > 50) { signals.push(`extreme IV ${ivPct.toFixed(0)}%`); bias -= 1; }
            else if (ivPct > 30) { signals.push(`elevated IV ${ivPct.toFixed(0)}%`); }
            else if (ivPct < 15) { signals.push(`low IV ${ivPct.toFixed(0)}%`); bias += 1; }
        }
        if (spot != null && maxPain != null) {
            const dist = ((spot - maxPain) / maxPain * 100);
            if (Math.abs(dist) > 2) {
                signals.push(`spot ${dist > 0 ? 'above' : 'below'} max pain by ${Math.abs(dist).toFixed(1)}%`);
                bias += dist > 0 ? 1 : -1;
            }
        }

        const sentiment = bias > 0 ? 'bullish' : bias < 0 ? 'bearish' : 'neutral';
        const reason = signals.length > 0 ? signals.join(', ') : 'mixed signals';
        return `Options market is ${sentiment}: ${reason}.`;
    }, [pcr, ivPct, spot, maxPain]);

    return (
        <div style={{ ...shared.card, padding: '12px 16px' }}>
            <div style={{
                fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
                color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                marginBottom: '10px',
            }}>
                OPTIONS INTEL · {opts.date}
            </div>

            {/* Price positioning chart */}
            {spot != null && maxPain != null && (
                <div style={{ marginBottom: '14px', overflowX: 'auto' }}>
                    <svg
                        ref={chartRef}
                        viewBox="0 0 560 200"
                        width="100%"
                        height={200}
                        style={{ display: 'block', maxWidth: '100%' }}
                    />
                </div>
            )}

            {/* Metrics row */}
            <div style={{
                display: 'flex', flexWrap: 'wrap', gap: '12px',
                marginBottom: '12px',
            }}>
                {/* P/C Ratio with bar */}
                <div style={{
                    flex: '1 1 120px', background: colors.bg,
                    borderRadius: tokens.radius.md, padding: '10px 12px',
                }}>
                    <div style={{ fontSize: '10px', color: colors.textMuted, marginBottom: '4px', fontFamily: "'JetBrains Mono', monospace" }}>
                        Put/Call Ratio
                    </div>
                    <div style={{ fontSize: '16px', fontWeight: 700, color: pcrColor, fontFamily: "'JetBrains Mono', monospace" }}>
                        {pcr != null ? pcr.toFixed(2) : '--'}
                    </div>
                    <div style={{
                        marginTop: '6px', height: '4px', borderRadius: '2px',
                        background: colors.border, overflow: 'hidden',
                    }}>
                        <div style={{
                            height: '100%', width: `${pcrBarWidth}%`,
                            borderRadius: '2px', background: pcrColor,
                            transition: `width ${tokens.transition.normal}`,
                        }} />
                    </div>
                </div>

                {/* IV ATM gauge */}
                <div style={{
                    flex: '1 1 120px', background: colors.bg,
                    borderRadius: tokens.radius.md, padding: '10px 12px',
                }}>
                    <div style={{ fontSize: '10px', color: colors.textMuted, marginBottom: '4px', fontFamily: "'JetBrains Mono', monospace" }}>
                        IV ATM
                    </div>
                    <div style={{ display: 'flex', alignItems: 'baseline', gap: '6px' }}>
                        <span style={{ fontSize: '16px', fontWeight: 700, color: ivColor, fontFamily: "'JetBrains Mono', monospace" }}>
                            {ivPct != null ? `${ivPct.toFixed(1)}%` : '--'}
                        </span>
                        <span style={{ fontSize: '10px', color: ivColor, fontFamily: "'JetBrains Mono', monospace" }}>
                            {ivLabel}
                        </span>
                    </div>
                    {/* Mini gauge arc */}
                    <svg viewBox="0 0 60 32" width="60" height="32" style={{ display: 'block', marginTop: '4px' }}>
                        <path
                            d="M 5 28 A 25 25 0 0 1 55 28"
                            fill="none" stroke={colors.border} strokeWidth="3" strokeLinecap="round"
                        />
                        <path
                            d="M 5 28 A 25 25 0 0 1 55 28"
                            fill="none" stroke={ivColor} strokeWidth="3" strokeLinecap="round"
                            strokeDasharray={`${(ivGaugeAngle / 180) * 78.5} 78.5`}
                        />
                        {/* Needle tick */}
                        {ivPct != null && (() => {
                            const angle = Math.PI - (ivGaugeAngle / 180) * Math.PI;
                            const nx = 30 + 20 * Math.cos(angle);
                            const ny = 28 - 20 * Math.sin(angle);
                            return <circle cx={nx} cy={ny} r="2.5" fill={ivColor} />;
                        })()}
                    </svg>
                </div>

                {/* IV Skew */}
                <div style={{
                    flex: '1 1 120px', background: colors.bg,
                    borderRadius: tokens.radius.md, padding: '10px 12px',
                }}>
                    <div style={{ fontSize: '10px', color: colors.textMuted, marginBottom: '4px', fontFamily: "'JetBrains Mono', monospace" }}>
                        IV Skew
                    </div>
                    <div style={{ fontSize: '16px', fontWeight: 700, color: '#E8F0F8', fontFamily: "'JetBrains Mono', monospace" }}>
                        {ivSkew != null ? ivSkew.toFixed(2) : '--'}
                    </div>
                    <div style={{ fontSize: '10px', color: colors.textDim, marginTop: '4px', fontFamily: "'JetBrains Mono', monospace" }}>
                        {skewText}
                    </div>
                </div>

                {/* Total OI */}
                <div style={{
                    flex: '1 1 90px', background: colors.bg,
                    borderRadius: tokens.radius.md, padding: '10px 12px',
                }}>
                    <div style={{ fontSize: '10px', color: colors.textMuted, marginBottom: '4px', fontFamily: "'JetBrains Mono', monospace" }}>
                        Total OI
                    </div>
                    <div style={{ fontSize: '16px', fontWeight: 700, color: '#E8F0F8', fontFamily: "'JetBrains Mono', monospace" }}>
                        {oiFmt}
                    </div>
                </div>
            </div>

            {/* One-line interpretation */}
            <div style={{
                fontSize: '12px', lineHeight: '1.5', color: colors.textDim,
                padding: '8px 10px', background: `${regionColor}08`,
                border: `1px solid ${regionColor}20`,
                borderRadius: tokens.radius.sm,
                fontFamily: "'JetBrains Mono', monospace",
            }}>
                {interpretation}
            </div>
        </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════
   Capital Flow mini-chart (inline SVG)
   ═══════════════════════════════════════════════════════════════════ */

const FLOW_LEVEL_COLORS = {
    market: '#1A6EBF',
    sector: '#8B5CF6',
    subsector: '#06B6D4',
    ticker: '#F59E0B',
};

function CapitalFlowPath({ sectorPath, ticker }) {
    const svgRef = useRef(null);
    const barRef = useRef(null);

    if (!sectorPath || !sectorPath.sector) return null;

    const nodes = [
        { id: 'market', label: 'Market', level: 'market' },
        { id: 'sector', label: sectorPath.sector, level: 'sector' },
        { id: 'subsector', label: sectorPath.subsector, level: 'subsector' },
        { id: 'ticker', label: ticker, level: 'ticker' },
    ];

    const influence = sectorPath.influence || 0;
    const subWeight = sectorPath.subsector_weight || 0;
    const actorWeight = sectorPath.actor_weight || 0;
    const peers = sectorPath.peers || [];

    // Link widths proportional to weight, min 2px, max 16px
    const links = [
        { from: 0, to: 1, weight: 1.0, label: '' },
        { from: 1, to: 2, weight: subWeight, label: `${(subWeight * 100).toFixed(0)}%` },
        { from: 2, to: 3, weight: actorWeight, label: `${(actorWeight * 100).toFixed(0)}%` },
    ];

    const width = 600;
    const height = 80;
    const nodeW = 100;
    const nodeH = 32;
    const padX = 30;
    const spacing = (width - 2 * padX - nodeW) / (nodes.length - 1);

    // Build bar chart data: current ticker + peers
    const barData = useMemo(() => {
        const items = [
            { ticker: ticker, name: sectorPath.actor_name || ticker, weight: actorWeight, isSelf: true },
            ...peers.map(p => ({ ticker: p.ticker, name: p.name, weight: p.weight, isSelf: false })),
        ];
        return items.sort((a, b) => b.weight - a.weight);
    }, [ticker, actorWeight, peers, sectorPath.actor_name]);

    // D3 peer comparison bar chart
    useEffect(() => {
        if (!barRef.current || barData.length < 2) return;

        const svg = d3.select(barRef.current);
        svg.selectAll('*').remove();

        const margin = { top: 4, right: 50, bottom: 4, left: 56 };
        const barW = 600;
        const barH = barData.length * 24 + margin.top + margin.bottom;
        svg.attr('viewBox', `0 0 ${barW} ${barH}`).attr('height', barH);

        const innerW = barW - margin.left - margin.right;
        const innerH = barH - margin.top - margin.bottom;
        const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

        const x = d3.scaleLinear()
            .domain([0, d3.max(barData, d => d.weight) || 1])
            .range([0, innerW]);

        const y = d3.scaleBand()
            .domain(barData.map(d => d.ticker))
            .range([0, innerH])
            .padding(0.25);

        // Bars
        g.selectAll('rect.bar')
            .data(barData)
            .join('rect')
            .attr('class', 'bar')
            .attr('x', 0)
            .attr('y', d => y(d.ticker))
            .attr('width', d => Math.max(2, x(d.weight)))
            .attr('height', y.bandwidth())
            .attr('rx', 3)
            .attr('fill', d => d.isSelf ? FLOW_LEVEL_COLORS.ticker : `${colors.textMuted}40`)
            .attr('opacity', d => d.isSelf ? 0.9 : 0.6);

        // Ticker labels (left axis)
        g.selectAll('text.label')
            .data(barData)
            .join('text')
            .attr('class', 'label')
            .attr('x', -6)
            .attr('y', d => y(d.ticker) + y.bandwidth() / 2)
            .attr('dy', '0.35em')
            .attr('text-anchor', 'end')
            .attr('font-size', '9px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', d => d.isSelf ? FLOW_LEVEL_COLORS.ticker : colors.textDim)
            .attr('font-weight', d => d.isSelf ? 700 : 400)
            .text(d => d.ticker);

        // Weight percentage labels (right of bar)
        g.selectAll('text.value')
            .data(barData)
            .join('text')
            .attr('class', 'value')
            .attr('x', d => Math.max(2, x(d.weight)) + 6)
            .attr('y', d => y(d.ticker) + y.bandwidth() / 2)
            .attr('dy', '0.35em')
            .attr('text-anchor', 'start')
            .attr('font-size', '9px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', d => d.isSelf ? FLOW_LEVEL_COLORS.ticker : colors.textMuted)
            .text(d => `${(d.weight * 100).toFixed(0)}%`);

    }, [barData]);

    return (
        <div style={{
            ...shared.card,
            padding: '12px 16px',
            overflow: 'hidden',
        }}>
            <div style={{
                fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
                color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                marginBottom: '8px',
            }}>
                CAPITAL FLOW PATH
                {influence > 0 && (
                    <span style={{
                        marginLeft: '10px', fontWeight: 400, color: colors.textMuted,
                        letterSpacing: '0px', fontSize: '10px',
                    }}>
                        Net influence: {(influence * 100).toFixed(1)}%
                    </span>
                )}
            </div>
            <svg
                ref={svgRef}
                viewBox={`0 0 ${width} ${height}`}
                width="100%"
                height={height}
                style={{ display: 'block' }}
            >
                <defs>
                    {/* Gradient for links */}
                    {links.map((link, i) => {
                        const fromColor = FLOW_LEVEL_COLORS[nodes[link.from].level];
                        const toColor = FLOW_LEVEL_COLORS[nodes[link.to].level];
                        return (
                            <linearGradient key={`lg-${i}`} id={`flow-grad-${i}`} x1="0" y1="0" x2="1" y2="0">
                                <stop offset="0%" stopColor={fromColor} stopOpacity="0.6" />
                                <stop offset="100%" stopColor={toColor} stopOpacity="0.6" />
                            </linearGradient>
                        );
                    })}
                </defs>

                {/* Links (curved paths) */}
                {links.map((link, i) => {
                    const x1 = padX + link.from * spacing + nodeW;
                    const x2 = padX + link.to * spacing;
                    const y = height / 2;
                    const linkWidth = Math.max(2, Math.min(16, link.weight * 20));
                    const cx1 = x1 + (x2 - x1) * 0.4;
                    const cx2 = x1 + (x2 - x1) * 0.6;

                    return (
                        <g key={`link-${i}`}>
                            <path
                                d={`M ${x1} ${y} C ${cx1} ${y}, ${cx2} ${y}, ${x2} ${y}`}
                                fill="none"
                                stroke={`url(#flow-grad-${i})`}
                                strokeWidth={linkWidth}
                                strokeLinecap="round"
                            />
                            {link.label && (
                                <text
                                    x={(x1 + x2) / 2}
                                    y={y - linkWidth / 2 - 4}
                                    textAnchor="middle"
                                    fontSize="8"
                                    fontFamily="'JetBrains Mono', monospace"
                                    fill={colors.textMuted}
                                >
                                    {link.label}
                                </text>
                            )}
                        </g>
                    );
                })}

                {/* Nodes */}
                {nodes.map((node, i) => {
                    const x = padX + i * spacing;
                    const y = (height - nodeH) / 2;
                    const levelColor = FLOW_LEVEL_COLORS[node.level];
                    const isTarget = node.level === 'ticker';

                    return (
                        <g key={node.id}>
                            <rect
                                x={x}
                                y={y}
                                width={nodeW}
                                height={nodeH}
                                rx="6"
                                fill={isTarget ? `${levelColor}30` : `${levelColor}15`}
                                stroke={levelColor}
                                strokeWidth={isTarget ? 1.5 : 0.5}
                            />
                            <text
                                x={x + nodeW / 2}
                                y={y + nodeH / 2}
                                textAnchor="middle"
                                dominantBaseline="central"
                                fontSize={isTarget ? '11' : '10'}
                                fontWeight={isTarget ? '700' : '500'}
                                fontFamily="'JetBrains Mono', monospace"
                                fill={isTarget ? levelColor : colors.text}
                            >
                                {node.label.length > 14 ? node.label.substring(0, 12) + '..' : node.label}
                            </text>
                        </g>
                    );
                })}
            </svg>

            {/* Peer comparison bar chart */}
            {barData.length >= 2 && (
                <div style={{ marginTop: '10px' }}>
                    <div style={{
                        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                        color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace",
                        marginBottom: '6px',
                    }}>
                        SUBSECTOR PEER WEIGHTS
                    </div>
                    <svg
                        ref={barRef}
                        width="100%"
                        style={{ display: 'block' }}
                    />
                </div>
            )}

            {/* Subsector influence label */}
            {influence > 0 && (
                <div style={{
                    marginTop: '8px', fontSize: '11px', color: colors.textDim,
                    fontFamily: "'JetBrains Mono', monospace",
                    padding: '6px 10px',
                    background: `${FLOW_LEVEL_COLORS.ticker}10`,
                    borderRadius: tokens.radius.sm,
                    border: `1px solid ${FLOW_LEVEL_COLORS.ticker}20`,
                }}>
                    This ticker represents <span style={{ color: FLOW_LEVEL_COLORS.ticker, fontWeight: 700 }}>
                        {(influence * 100).toFixed(1)}%
                    </span> of subsector influence
                </div>
            )}
        </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════
   Main component
   ═══════════════════════════════════════════════════════════════════ */

export default function WatchlistAnalysis({ ticker, onBack, enrichedData }) {
    const { isMobile } = useDevice();
    const [data, setData] = useState(null);
    const [dataLoading, setDataLoading] = useState(true);
    const [error, setError] = useState(null);
    const [overview, setOverview] = useState(null);
    const [overviewLoading, setOverviewLoading] = useState(true);
    const [period, setPeriod] = useState('3M');
    const [priceLoading, setPriceLoading] = useState(false);
    const [gexData, setGexData] = useState(null);
    const [gexLoading, setGexLoading] = useState(true);
    const [vannaCharmData, setVannaCharmData] = useState(null);
    const [flowTimelineData, setFlowTimelineData] = useState(null);
    const [secondaryLoading, setSecondaryLoading] = useState(true);
    const [edgeData, setEdgeData] = useState(null);
    const [edgeLoading, setEdgeLoading] = useState(true);

    useEffect(() => {
        if (!ticker) return;

        // Reset state for new ticker
        setData(null);
        setDataLoading(true);
        setError(null);
        setOverview(null);
        setOverviewLoading(true);
        setGexData(null);
        setGexLoading(true);
        setVannaCharmData(null);
        setFlowTimelineData(null);
        setSecondaryLoading(true);
        setEdgeData(null);
        setEdgeLoading(true);

        // Phase 1: Fetch core analysis data (fastest — often cached)
        api.getTickerAnalysis(ticker, period).then(result => {
            setData(result);
            setDataLoading(false);
        }).catch(err => {
            setError(err.message || 'Failed to load');
            setDataLoading(false);
        });

        // Phase 2: Fetch AI overview (may be slow due to LLM)
        api.getTickerOverview(ticker).then(result => {
            setOverview(result);
            setOverviewLoading(false);
        }).catch(() => {
            setOverviewLoading(false);
        });

        // Phase 2b: Fetch insider edge intelligence
        api.getTickerEdge(ticker).then(result => {
            setEdgeData(result);
            setEdgeLoading(false);
        }).catch(() => {
            setEdgeLoading(false);
        });

        // Phase 3: Fetch GEX, vanna-charm, flow timeline in parallel
        Promise.allSettled([
            api.getGEXProfile(ticker),
            api.getVannaCharm(ticker),
            api.getFlowTimeline(ticker, 90),
        ]).then(([gexResult, vcResult, ftResult]) => {
            if (gexResult.status === 'fulfilled' && !gexResult.value?.error) {
                setGexData(gexResult.value);
            }
            setGexLoading(false);
            if (vcResult.status === 'fulfilled' && !vcResult.value?.error) {
                setVannaCharmData(vcResult.value);
            }
            if (ftResult.status === 'fulfilled' && !ftResult.value?.error) {
                setFlowTimelineData(ftResult.value);
            }
            setSecondaryLoading(false);
        });
    }, [ticker]);

    const handlePeriodChange = useCallback(async (newPeriod) => {
        if (newPeriod === period) return;
        setPeriod(newPeriod);
        setPriceLoading(true);
        try {
            const refreshed = await api.getTickerAnalysis(ticker, newPeriod);
            setData(prev => ({
                ...prev,
                price_history: refreshed.price_history,
                price_source: refreshed.price_source,
                period: refreshed.period,
            }));
        } catch (err) {
            // Keep existing data on failure
        }
        setPriceLoading(false);
    }, [ticker, period]);

    // Use enrichedData for instant display while analysis loads
    const enrichedPrice = enrichedData?.price;
    const enrichedPct1d = enrichedData?.pct_1d;
    const enrichedName = enrichedData?.display_name;
    const enrichedAssetType = enrichedData?.asset_type;
    const enrichedSector = enrichedData?.sector;
    const enrichedRegimeState = enrichedData?.regime?.state;

    // Show error only if core data failed AND we have no enriched fallback
    if (error && !data && !dataLoading && !enrichedData) {
        return (
            <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
                <button onClick={onBack} style={{ ...shared.buttonSmall, background: colors.card, marginBottom: '16px' }}>Back</button>
                <div style={shared.error}>{error || 'No data'}</div>
            </div>
        );
    }

    const item = data?.watchlist_item || enrichedData;
    const prices = data?.price_history || [];
    const opts = (data?.options || [])[0]; // latest
    const regime = data?.regime;
    const related = data?.related_features || [];
    const tvSignals = data?.tradingview_signals || [];

    // Use enriched price as fallback if analysis data hasn't loaded yet
    const lastPrice = prices.length ? prices[prices.length - 1].value : (enrichedPrice || null);
    const prevPrice = prices.length > 1 ? prices[prices.length - 2].value : null;
    const change = prices.length > 1 && lastPrice && prevPrice ? ((lastPrice - prevPrice) / prevPrice * 100)
        : (enrichedPct1d != null ? enrichedPct1d * 100 : null);
    return (
        <div style={{
            ...shared.container,
            padding: isMobile ? '0 8px 16px 8px' : shared.container.padding,
            paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)',
            overflowX: 'hidden',
            fontSize: isMobile ? '13px' : '14px',
        }}>
            {/* Header — sticky on mobile */}
            <div style={{
                display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '16px',
                ...(isMobile ? {
                    position: 'sticky', top: 0, zIndex: 10,
                    background: colors.bg, padding: '8px 0',
                    borderBottom: `1px solid ${colors.borderSubtle}`,
                } : {}),
            }}>
                <button onClick={onBack} style={{
                    background: colors.card, border: `1px solid ${colors.border}`,
                    borderRadius: '8px', color: colors.textDim, padding: '8px 14px',
                    fontSize: '13px', cursor: 'pointer', minHeight: '44px',
                }}>Back</button>
                <div style={{ flex: 1 }}>
                    <div style={{ display: 'flex', alignItems: 'baseline', gap: '10px' }}>
                        <span style={{ fontSize: '22px', fontWeight: 700, color: '#E8F0F8', fontFamily: "'JetBrains Mono', monospace" }}>
                            {ticker}
                        </span>
                        {lastPrice != null && (
                            <span style={{ fontSize: '16px', fontWeight: 600, color: colors.text, fontFamily: "'JetBrains Mono', monospace" }}>
                                ${typeof lastPrice === 'number' ? lastPrice.toLocaleString(undefined, { maximumFractionDigits: 2 }) : lastPrice}
                            </span>
                        )}
                        {change != null && (
                            <span style={{
                                fontSize: '12px', fontWeight: 600,
                                color: change >= 0 ? colors.green : colors.red,
                                fontFamily: "'JetBrains Mono', monospace",
                            }}>
                                {change >= 0 ? '+' : ''}{change.toFixed(2)}%
                            </span>
                        )}
                    </div>
                    <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '2px' }}>
                        {item?.display_name || enrichedName || ticker} · {(item?.asset_type || enrichedAssetType || 'stock').toUpperCase()}
                        {(regime || enrichedRegimeState) && <> · Regime: <span style={{ color: colors.accent }}>{regime?.state || enrichedRegimeState}</span></>}
                        {enrichedSector && !regime && <> · {enrichedSector}</>}
                    </div>
                </div>
            </div>

            {/* ═══ AI OVERVIEW ═══ */}
            {overviewLoading ? (
                <OverviewSkeleton />
            ) : overview ? (
                <AIOverviewCard overview={overview} />
            ) : null}

            {/* ═══ CAPITAL FLOW PATH ═══ */}
            {overview?.sector_path && (
                <div style={{ marginTop: '12px' }}>
                    <CapitalFlowPath sectorPath={overview.sector_path} ticker={ticker} />
                </div>
            )}

            {/* ═══ INSIDER EDGE ═══ */}
            <InsiderEdgePanel edgeData={edgeData} loading={edgeLoading} />

            {/* ═══ DATA GRID ═══ */}
            <div style={{
                display: 'grid',
                gridTemplateColumns: isMobile ? '1fr' : 'repeat(auto-fit, minmax(280px, 1fr))',
                gap: isMobile ? '8px' : '12px',
                marginTop: isMobile ? '8px' : '16px',
            }}>
                {/* Price Chart — show loading skeleton until analysis data arrives */}
                <div style={{ gridColumn: '1 / -1', position: 'relative' }}>
                    {(priceLoading || dataLoading) && (
                        <div style={{
                            position: 'absolute', top: 0, left: 0, right: 0, bottom: 0,
                            background: `${colors.bg}80`, zIndex: 2,
                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                            borderRadius: tokens.radius.md,
                        }}>
                            <span style={{ fontSize: '11px', color: colors.textMuted }}>
                                {dataLoading ? 'Loading chart data...' : 'Loading...'}
                            </span>
                        </div>
                    )}
                    <PriceChart
                        data={prices}
                        ticker={ticker}
                        period={period}
                        onPeriodChange={handlePeriodChange}
                        keyLevels={overview?.key_levels}
                        regime={regime}
                    />
                </div>

                {/* Options Intelligence — show when available */}
                {opts && (
                    <div style={{ gridColumn: '1 / -1' }}>
                        {isMobile ? (
                            <CollapsibleSection title="OPTIONS INTELLIGENCE" defaultExpanded={false}
                                body={<OptionsIntel opts={opts} />} />
                        ) : (
                            <OptionsIntel opts={opts} />
                        )}
                    </div>
                )}

                {/* Dealer GEX Profile — show skeleton while loading */}
                {gexData ? (
                    <div style={{ gridColumn: '1 / -1' }}>
                        {isMobile ? (
                            <CollapsibleSection title="DEALER GEX PROFILE" defaultExpanded={false}
                                body={<GEXProfile ticker={ticker} gexData={gexData} spotPrice={gexData.spot} />} />
                        ) : (
                            <GEXProfile ticker={ticker} gexData={gexData} spotPrice={gexData.spot} />
                        )}
                    </div>
                ) : gexLoading ? (
                    <div style={{ gridColumn: '1 / -1' }}>
                        <OverviewSkeleton />
                    </div>
                ) : null}

                {/* Vanna / Charm Compass */}
                {vannaCharmData && (
                    <div style={{ gridColumn: '1 / -1' }}>
                        {isMobile ? (
                            <CollapsibleSection title="VANNA / CHARM" defaultExpanded={false}
                                body={<VannaCharmViz ticker={ticker} vannaCharmData={vannaCharmData} />} />
                        ) : (
                            <VannaCharmViz ticker={ticker} vannaCharmData={vannaCharmData} />
                        )}
                    </div>
                )}

                {/* Flow Timeline */}
                {flowTimelineData ? (
                    <div style={{ gridColumn: '1 / -1' }}>
                        {isMobile ? (
                            <CollapsibleSection title="FLOW TIMELINE" defaultExpanded={false}
                                body={<FlowTimeline ticker={ticker} timelineData={flowTimelineData} />} />
                        ) : (
                            <FlowTimeline ticker={ticker} timelineData={flowTimelineData} />
                        )}
                    </div>
                ) : secondaryLoading ? (
                    <div style={{ gridColumn: '1 / -1' }}>
                        <OverviewSkeleton />
                    </div>
                ) : null}

                {/* Trade Recommendations for this ticker */}
                <div style={{ gridColumn: '1 / -1' }}>
                    {isMobile ? (
                        <CollapsibleSection title="TRADE RECOMMENDATIONS" defaultExpanded={false}
                            body={<TickerRecommendations ticker={ticker} />} />
                    ) : (
                        <TickerRecommendations ticker={ticker} />
                    )}
                </div>

                {/* Regime Context */}
                {regime && (
                    <div style={{ ...shared.card, padding: '12px' }}>
                        <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px', marginBottom: '8px' }}>
                            REGIME CONTEXT
                        </div>
                        <div style={{ ...shared.metricGrid }}>
                            <StatCard label="Regime" value={regime.state}
                                color={regime.state === 'GROWTH' ? colors.green : regime.state === 'CRISIS' ? colors.red : regime.state === 'FRAGILE' ? colors.yellow : colors.text}
                            />
                            <StatCard label="Confidence" value={regime.confidence != null ? `${(regime.confidence * 100).toFixed(0)}%` : '--'} />
                            <StatCard label="Posture" value={regime.posture || '--'} />
                        </div>
                    </div>
                )}
            </div>

            {/* Related Features */}
            {related.length > 0 && (
                <div style={{ marginTop: isMobile ? '8px' : '16px' }}>
                    {isMobile ? (
                        <CollapsibleSection title={`RELATED FEATURES \u00b7 ${related.length}`} defaultExpanded={false} body={
                            <div>
                                {related.map((f, i) => {
                                    const signalColor = f.signal === 'bullish' ? colors.green : f.signal === 'bearish' ? colors.red : colors.textDim;
                                    return (
                                        <div key={f.name} style={{
                                            display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start',
                                            padding: '8px 0',
                                            borderBottom: i < related.length - 1 ? `1px solid ${colors.borderSubtle}` : 'none',
                                        }}>
                                            <div style={{ flex: 1, minWidth: 0 }}>
                                                <div title={f.display_name || f.name} style={{ fontSize: '12px', color: colors.text, fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{f.display_name || f.name}</div>
                                                <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '1px' }}>{f.family} · {f.obs_date}</div>
                                                {f.interpretation && <div style={{ fontSize: '10px', color: signalColor, marginTop: '3px', lineHeight: '1.5', wordBreak: 'break-word' }}>{f.interpretation}</div>}
                                            </div>
                                            <div style={{ fontSize: '13px', fontWeight: 600, color: signalColor, fontFamily: "'JetBrains Mono', monospace", textAlign: 'right', flexShrink: 0, marginLeft: '12px', whiteSpace: 'nowrap', overflow: 'visible' }}>
                                                {f.value != null ? (typeof f.value === 'number' && Math.abs(f.value) > 100 ? f.value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : f.value.toFixed(4)) : '--'}
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        } />
                    ) : (
                        <>
                            <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px', marginBottom: '6px' }}>
                                RELATED FEATURES · {related.length}
                            </div>
                            <div style={shared.card}>
                                {related.map((f, i) => {
                                    const signalColor = f.signal === 'bullish' ? colors.green : f.signal === 'bearish' ? colors.red : colors.textDim;
                                    return (
                                        <div key={f.name} style={{
                                            display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start',
                                            padding: '8px 0',
                                            borderBottom: i < related.length - 1 ? `1px solid ${colors.borderSubtle}` : 'none',
                                        }}>
                                            <div style={{ flex: 1, minWidth: 0 }}>
                                                <div title={f.display_name || f.name} style={{ fontSize: '12px', color: colors.text, fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{f.display_name || f.name}</div>
                                                <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '1px' }}>{f.family} · {f.obs_date}</div>
                                                {f.interpretation ? (
                                                    <div style={{ fontSize: '10px', color: signalColor, marginTop: '3px', lineHeight: '1.5', wordBreak: 'break-word' }}>{f.interpretation}</div>
                                                ) : (
                                                    <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '3px', fontStyle: 'italic' }}>raw data</div>
                                                )}
                                            </div>
                                            <div style={{ fontSize: '13px', fontWeight: 600, color: signalColor, fontFamily: "'JetBrains Mono', monospace", textAlign: 'right', flexShrink: 0, marginLeft: '12px', whiteSpace: 'nowrap', overflow: 'visible' }}>
                                                {f.value != null ? (typeof f.value === 'number' && Math.abs(f.value) > 100 ? f.value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : f.value.toFixed(4)) : '--'}
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        </>
                    )}
                </div>
            )}

            {/* TradingView Signals */}
            {tvSignals.length > 0 && (
                <div style={{ marginTop: '16px' }}>
                    <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px', marginBottom: '6px' }}>
                        TRADINGVIEW ALERTS · {tvSignals.length}
                    </div>
                    <div style={shared.card}>
                        {tvSignals.map((s, i) => (
                            <div key={i} style={{
                                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                padding: '8px 0',
                                borderBottom: i < tvSignals.length - 1 ? `1px solid ${colors.borderSubtle}` : 'none',
                            }}>
                                <div>
                                    <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                                        <SignalDot value={s.signal_value} />
                                        <span style={{ fontSize: '12px', color: colors.text }}>
                                            {s.strategy || s.action || 'alert'}
                                        </span>
                                    </div>
                                    {s.message && <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '2px' }}>{s.message}</div>}
                                </div>
                                <div style={{ textAlign: 'right' }}>
                                    {s.price != null && <div style={{ fontSize: '12px', fontFamily: "'JetBrains Mono', monospace", color: colors.text }}>${s.price}</div>}
                                    <div style={{ fontSize: '10px', color: colors.textMuted }}>
                                        {s.timestamp ? formatDate(s.timestamp) : ''}
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* Notes */}
            {item?.notes && (
                <div style={{ marginTop: '16px' }}>
                    <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px', marginBottom: '6px' }}>NOTES</div>
                    <div style={{ ...shared.card, fontSize: '12px', color: colors.textDim, lineHeight: '1.5' }}>{item.notes}</div>
                </div>
            )}

            <div style={{ height: '80px' }} />
        </div>
    );
}
