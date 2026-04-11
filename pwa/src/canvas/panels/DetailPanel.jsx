/**
 * DetailPanel -- Right-side intelligence panel for selected canvas nodes.
 * Slides in from right, 360px wide, full height.
 * Shows rich intelligence detail varying by node type: actor, ticker, signal, event.
 */
import React, { useState, useCallback } from 'react';
import { colors, tokens, shared, glassMorphism } from '../../styles/shared.js';

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

const TIER_COLORS = {
    sovereign: '#FFD700',
    regional: '#3B82F6',
    institutional: '#8B5CF6',
    individual: '#06B6D4',
    unknown: colors.textMuted,
};

const DIRECTION_COLORS = {
    bullish: colors.green,
    buy: colors.green,
    long: colors.green,
    bearish: colors.red,
    sell: colors.red,
    short: colors.red,
    neutral: colors.textMuted,
};

const CONFIDENCE_LABELS = {
    confirmed: { color: colors.green, label: 'CONFIRMED' },
    derived: { color: '#3B82F6', label: 'DERIVED' },
    estimated: { color: colors.yellow, label: 'ESTIMATED' },
    rumored: { color: '#F97316', label: 'RUMORED' },
    inferred: { color: '#8B5CF6', label: 'INFERRED' },
};

/* ── Styles ──────────────────────────────────────────────────── */

const S = {
    overlay: {
        position: 'absolute',
        top: 0, right: 0, bottom: 0,
        width: '360px',
        background: colors.card,
        borderLeft: `1px solid ${colors.border}`,
        zIndex: 100,
        display: 'flex',
        flexDirection: 'column',
        transition: 'transform 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
        boxShadow: '-4px 0 24px rgba(0,0,0,0.4)',
        overflow: 'hidden',
    },
    header: {
        ...glassMorphism,
        padding: '16px',
        borderBottom: `1px solid ${colors.border}`,
        flexShrink: 0,
        position: 'relative',
    },
    headerRow: {
        display: 'flex',
        alignItems: 'flex-start',
        justifyContent: 'space-between',
        gap: '8px',
    },
    headerInfo: {
        flex: 1,
        minWidth: 0,
    },
    nodeName: {
        fontSize: '18px',
        fontWeight: 700,
        color: '#E8F0F8',
        fontFamily: SANS,
        marginBottom: '4px',
        wordBreak: 'break-word',
    },
    nodeTitle: {
        fontSize: '12px',
        color: colors.textDim,
        fontFamily: SANS,
        lineHeight: 1.4,
    },
    closeBtn: {
        background: 'none',
        border: 'none',
        color: colors.textMuted,
        cursor: 'pointer',
        fontSize: '18px',
        padding: '2px 6px',
        lineHeight: 1,
        flexShrink: 0,
        borderRadius: tokens.radius.sm,
        transition: `background ${tokens.transition.fast}`,
    },
    body: {
        flex: 1,
        overflowY: 'auto',
        padding: '12px 16px',
        display: 'flex',
        flexDirection: 'column',
        gap: '16px',
        WebkitOverflowScrolling: 'touch',
    },
    section: {
        display: 'flex',
        flexDirection: 'column',
        gap: '6px',
    },
    sectionTitle: {
        fontSize: '10px',
        fontWeight: 700,
        letterSpacing: '1.5px',
        color: colors.accent,
        fontFamily: MONO,
        textTransform: 'uppercase',
    },
    card: {
        background: colors.bg,
        border: `1px solid ${colors.borderSubtle}`,
        borderRadius: tokens.radius.sm,
        padding: '10px 12px',
    },
    row: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '6px 0',
        borderBottom: `1px solid ${colors.borderSubtle}`,
    },
    rowLast: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '6px 0',
    },
    label: {
        fontSize: '12px',
        color: colors.textMuted,
        fontFamily: SANS,
    },
    value: {
        fontSize: '13px',
        color: colors.text,
        fontFamily: MONO,
        fontWeight: 600,
    },
    footer: {
        display: 'flex',
        gap: '6px',
        padding: '12px 16px',
        borderTop: `1px solid ${colors.border}`,
        flexShrink: 0,
        flexWrap: 'wrap',
    },
    actionBtn: {
        flex: '1 1 auto',
        minWidth: '70px',
        background: colors.bg,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.sm,
        padding: '8px 10px',
        fontSize: '11px',
        fontWeight: 600,
        color: colors.textDim,
        cursor: 'pointer',
        fontFamily: SANS,
        textAlign: 'center',
        transition: `all ${tokens.transition.fast}`,
    },
    actionBtnPrimary: {
        background: colors.accent,
        border: `1px solid ${colors.accent}`,
        color: '#fff',
    },
    badge: (color) => ({
        display: 'inline-flex',
        alignItems: 'center',
        padding: '2px 8px',
        borderRadius: tokens.radius.sm,
        fontSize: '10px',
        fontWeight: 700,
        fontFamily: MONO,
        background: `${color}22`,
        color: color,
        letterSpacing: '0.5px',
        textTransform: 'uppercase',
    }),
    directionBadge: (dir) => {
        const c = DIRECTION_COLORS[dir?.toLowerCase()] || colors.textMuted;
        return {
            display: 'inline-flex',
            alignItems: 'center',
            padding: '2px 8px',
            borderRadius: tokens.radius.sm,
            fontSize: '10px',
            fontWeight: 700,
            fontFamily: MONO,
            background: `${c}22`,
            color: c,
            letterSpacing: '0.5px',
            textTransform: 'uppercase',
        };
    },
    gauge: {
        width: '100%',
        height: '6px',
        background: colors.border,
        borderRadius: '3px',
        overflow: 'hidden',
        marginTop: '4px',
    },
    gaugeFill: (pct, color) => ({
        width: `${Math.max(0, Math.min(100, pct))}%`,
        height: '100%',
        background: color,
        borderRadius: '3px',
        transition: `width ${tokens.transition.normal}`,
    }),
    strengthBar: {
        flex: 1,
        height: '4px',
        background: colors.border,
        borderRadius: '2px',
        overflow: 'hidden',
        maxWidth: '80px',
    },
    strengthFill: (pct) => ({
        width: `${pct}%`,
        height: '100%',
        background: colors.accent,
        borderRadius: '2px',
    }),
    signalItem: {
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        padding: '6px 8px',
        borderRadius: tokens.radius.sm,
        background: colors.bg,
        border: `1px solid ${colors.borderSubtle}`,
        fontSize: '12px',
        fontFamily: SANS,
    },
    priceRow: {
        display: 'flex',
        alignItems: 'baseline',
        gap: '10px',
    },
    priceLarge: {
        fontSize: '24px',
        fontWeight: 700,
        color: '#E8F0F8',
        fontFamily: MONO,
    },
    priceChange: (positive) => ({
        fontSize: '13px',
        fontWeight: 600,
        fontFamily: MONO,
        color: positive ? colors.green : colors.red,
    }),
    description: {
        fontSize: '13px',
        color: colors.textDim,
        fontFamily: SANS,
        lineHeight: 1.6,
    },
    countdown: {
        fontSize: '12px',
        fontFamily: MONO,
        color: colors.yellow,
        marginTop: '2px',
    },
    emptyText: {
        fontSize: '12px',
        color: colors.textMuted,
        fontFamily: SANS,
        fontStyle: 'italic',
        padding: '4px 0',
    },
};

/* ── Helpers ──────────────────────────────────────────────────── */

function trustColor(score) {
    if (score >= 0.8) return colors.green;
    if (score >= 0.6) return colors.yellow;
    return colors.red;
}

function formatMoney(amount) {
    if (!amount && amount !== 0) return '--';
    const abs = Math.abs(amount);
    if (abs >= 1e9) return `$${(amount / 1e9).toFixed(1)}B`;
    if (abs >= 1e6) return `$${(amount / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `$${(amount / 1e3).toFixed(0)}K`;
    return `$${amount.toFixed(0)}`;
}

function timeAgo(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr);
    const now = new Date();
    const diff = now - d;
    const days = Math.floor(diff / 86400000);
    if (days === 0) return 'today';
    if (days === 1) return '1 day ago';
    if (days < 30) return `${days} days ago`;
    if (days < 365) return `${Math.floor(days / 30)}mo ago`;
    return `${Math.floor(days / 365)}y ago`;
}

function countdown(dateStr) {
    if (!dateStr) return null;
    const d = new Date(dateStr);
    const now = new Date();
    const diff = d - now;
    if (diff <= 0) return null;
    const days = Math.floor(diff / 86400000);
    if (days === 0) return 'today';
    if (days === 1) return 'tomorrow';
    return `in ${days} days`;
}

function ConfidenceLabel({ level }) {
    const info = CONFIDENCE_LABELS[level] || { color: colors.textMuted, label: level || 'UNKNOWN' };
    return <span style={S.badge(info.color)}>{info.label}</span>;
}

/* ── Section Components ──────────────────────────────────────── */

function TrustGauge({ score }) {
    const pct = (score || 0) * 100;
    const color = trustColor(score);
    return (
        <div style={S.card}>
            <div style={S.row}>
                <span style={S.label}>Trust Score</span>
                <span style={{ ...S.value, color }}>{pct.toFixed(0)}%</span>
            </div>
            <div style={S.gauge}>
                <div style={S.gaugeFill(pct, color)} />
            </div>
        </div>
    );
}

function ActorDetail({ node }) {
    const data = node.data || {};
    const recentActions = data.recent_actions || data.recentActions || [];
    const wealthFlows = data.wealth_flows || data.wealthFlows || [];
    const connections = (data.connections || []).slice(0, 10);
    const boardSeats = data.board_seats || data.boardSeats || [];
    const positions = data.known_positions || data.knownPositions || [];
    const motivationModel = data.motivation_model || data.motivationModel || null;
    const influenceRank = data.influence_rank || data.influenceRank || null;
    const totalActors = data.total_actors || data.totalActors || null;

    return (
        <>
            <TrustGauge score={data.trust_score || data.trustScore || 0} />

            {influenceRank && (
                <div style={S.card}>
                    <div style={S.rowLast}>
                        <span style={S.label}>Influence Rank</span>
                        <span style={S.value}>
                            #{influenceRank}{totalActors ? ` of ${totalActors}` : ''}
                        </span>
                    </div>
                </div>
            )}

            {/* Recent Actions */}
            <div style={S.section}>
                <div style={S.sectionTitle}>RECENT ACTIONS</div>
                {recentActions.length === 0 && <div style={S.emptyText}>No recent actions</div>}
                {recentActions.slice(0, 8).map((a, i) => (
                    <div key={i} style={S.signalItem}>
                        <span style={S.directionBadge(a.direction)}>{a.direction || '?'}</span>
                        <span style={{ fontSize: '12px', color: colors.text, fontFamily: MONO, fontWeight: 600 }}>
                            {a.ticker || a.target || '--'}
                        </span>
                        <span style={{ fontSize: '11px', color: colors.textMuted, fontFamily: SANS, flex: 1, textAlign: 'right' }}>
                            {a.type || ''}
                        </span>
                        <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: MONO }}>
                            {timeAgo(a.date)}
                        </span>
                    </div>
                ))}
            </div>

            {/* Wealth Flows */}
            <div style={S.section}>
                <div style={S.sectionTitle}>WEALTH FLOWS</div>
                {wealthFlows.length === 0 && <div style={S.emptyText}>No wealth flow data</div>}
                {wealthFlows.slice(0, 6).map((f, i) => (
                    <div key={i} style={S.card}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <span style={{ fontSize: '12px', color: colors.text, fontFamily: SANS }}>
                                {f.direction === 'in' ? '\u2192' : '\u2190'} {f.counterparty || f.description || '--'}
                            </span>
                            <span style={{
                                fontSize: '13px', fontFamily: MONO, fontWeight: 600,
                                color: f.direction === 'in' ? colors.green : colors.red,
                            }}>
                                {formatMoney(f.amount)}
                            </span>
                        </div>
                        {f.confidence && (
                            <div style={{ marginTop: '4px' }}>
                                <ConfidenceLabel level={f.confidence} />
                            </div>
                        )}
                    </div>
                ))}
            </div>

            {/* Connections */}
            <div style={S.section}>
                <div style={S.sectionTitle}>CONNECTIONS</div>
                {connections.length === 0 && <div style={S.emptyText}>No connections loaded</div>}
                {connections.map((c, i) => (
                    <div key={i} style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '4px 0' }}>
                        <span style={{ fontSize: '12px', color: colors.text, fontFamily: SANS, flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {c.name || c.target || '--'}
                        </span>
                        <div style={S.strengthBar}>
                            <div style={S.strengthFill((c.strength || c.weight || 0) * 100)} />
                        </div>
                        <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: MONO, width: '28px', textAlign: 'right' }}>
                            {((c.strength || c.weight || 0) * 100).toFixed(0)}
                        </span>
                    </div>
                ))}
            </div>

            {/* Board Seats */}
            {boardSeats.length > 0 && (
                <div style={S.section}>
                    <div style={S.sectionTitle}>BOARD SEATS</div>
                    {boardSeats.map((b, i) => (
                        <div key={i} style={{ fontSize: '12px', color: colors.text, fontFamily: SANS, padding: '3px 0' }}>
                            {typeof b === 'string' ? b : b.company || b.name || '--'}
                        </div>
                    ))}
                </div>
            )}

            {/* Known Positions */}
            {positions.length > 0 && (
                <div style={S.section}>
                    <div style={S.sectionTitle}>KNOWN POSITIONS</div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                        {positions.map((p, i) => (
                            <span key={i} style={S.directionBadge(p.direction || p.side)}>
                                {p.ticker || p.symbol || '--'} {p.direction || p.side || ''}
                            </span>
                        ))}
                    </div>
                </div>
            )}

            {/* Motivation */}
            {motivationModel && (
                <div style={S.section}>
                    <div style={S.sectionTitle}>MOTIVATION MODEL</div>
                    <span style={S.badge(colors.accent)}>
                        {typeof motivationModel === 'string' ? motivationModel : motivationModel.tag || 'classified'}
                    </span>
                </div>
            )}
        </>
    );
}

function TickerDetail({ node }) {
    const data = node.data || {};
    const price = data.price || data.current_price || null;
    const change = data.change || data.pct_change || data.pct_1d || null;
    const sector = data.sector || null;
    const relatedActors = data.related_actors || data.relatedActors || [];
    const recentSignals = data.recent_signals || data.recentSignals || [];
    const options = data.options || {};
    const catalysts = data.catalysts || data.upcoming_events || [];

    return (
        <>
            {/* Price */}
            {price != null && (
                <div style={S.card}>
                    <div style={S.priceRow}>
                        <span style={S.priceLarge}>${typeof price === 'number' ? price.toFixed(2) : price}</span>
                        {change != null && (
                            <span style={S.priceChange(change >= 0)}>
                                {change >= 0 ? '+' : ''}{typeof change === 'number' ? change.toFixed(2) : change}%
                            </span>
                        )}
                    </div>
                    {sector && (
                        <div style={{ marginTop: '6px' }}>
                            <span style={S.badge(colors.accent)}>{sector}</span>
                        </div>
                    )}
                </div>
            )}

            {/* Related Actors */}
            <div style={S.section}>
                <div style={S.sectionTitle}>RELATED ACTORS</div>
                {relatedActors.length === 0 && <div style={S.emptyText}>No related actors</div>}
                {relatedActors.slice(0, 8).map((a, i) => (
                    <div key={i} style={S.signalItem}>
                        <span style={{ fontSize: '12px', color: colors.text, fontFamily: SANS, flex: 1 }}>
                            {a.name || a.actor || '--'}
                        </span>
                        {a.signal_type && (
                            <span style={S.badge(colors.accent)}>{a.signal_type}</span>
                        )}
                    </div>
                ))}
            </div>

            {/* Recent Signals */}
            <div style={S.section}>
                <div style={S.sectionTitle}>RECENT SIGNALS</div>
                {recentSignals.length === 0 && <div style={S.emptyText}>No recent signals</div>}
                {recentSignals
                    .sort((a, b) => (b.confidence || 0) - (a.confidence || 0))
                    .slice(0, 8)
                    .map((s, i) => (
                        <div key={i} style={S.signalItem}>
                            <span style={S.directionBadge(s.direction)}>{s.direction || '?'}</span>
                            <span style={{ fontSize: '12px', color: colors.text, fontFamily: SANS, flex: 1 }}>
                                {s.source || s.type || '--'}
                            </span>
                            <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: MONO }}>
                                {s.confidence != null ? `${(s.confidence * 100).toFixed(0)}%` : ''}
                            </span>
                        </div>
                    ))}
            </div>

            {/* Options */}
            {(options.gamma != null || options.iv_percentile != null || options.put_call_ratio != null) && (
                <div style={S.section}>
                    <div style={S.sectionTitle}>OPTIONS</div>
                    <div style={S.card}>
                        {options.gamma != null && (
                            <div style={S.row}>
                                <span style={S.label}>GEX</span>
                                <span style={S.value}>{typeof options.gamma === 'number' ? options.gamma.toFixed(2) : options.gamma}</span>
                            </div>
                        )}
                        {options.iv_percentile != null && (
                            <div style={S.row}>
                                <span style={S.label}>IV Percentile</span>
                                <span style={S.value}>{typeof options.iv_percentile === 'number' ? `${options.iv_percentile.toFixed(0)}%` : options.iv_percentile}</span>
                            </div>
                        )}
                        {options.put_call_ratio != null && (
                            <div style={S.rowLast}>
                                <span style={S.label}>Put/Call Ratio</span>
                                <span style={S.value}>{typeof options.put_call_ratio === 'number' ? options.put_call_ratio.toFixed(2) : options.put_call_ratio}</span>
                            </div>
                        )}
                    </div>
                </div>
            )}

            {/* Catalysts */}
            <div style={S.section}>
                <div style={S.sectionTitle}>CATALYSTS</div>
                {catalysts.length === 0 && <div style={S.emptyText}>No upcoming catalysts</div>}
                {catalysts.slice(0, 6).map((c, i) => {
                    const cd = countdown(c.date);
                    return (
                        <div key={i} style={S.card}>
                            <div style={{ fontSize: '12px', color: colors.text, fontFamily: SANS, fontWeight: 600 }}>
                                {c.name || c.event || c.title || '--'}
                            </div>
                            <div style={{ display: 'flex', gap: '8px', marginTop: '4px', alignItems: 'center' }}>
                                <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: MONO }}>
                                    {c.date || ''}
                                </span>
                                {cd && <span style={S.countdown}>{cd}</span>}
                                {c.category && <span style={S.badge(colors.accent)}>{c.category}</span>}
                            </div>
                        </div>
                    );
                })}
            </div>
        </>
    );
}

function SignalDetail({ node }) {
    const data = node.data || {};
    const confidence = data.confidence || 0;
    const sourceType = data.source_type || data.sourceType || data.source || '';
    const direction = data.direction || '';
    const actor = data.actor || data.actor_name || '';
    const date = data.date || data.signal_date || '';
    const description = data.description || data.text || '';
    const hitRate = data.source_accuracy || data.sourceAccuracy || data.hit_rate || null;

    return (
        <>
            {/* Confidence */}
            <div style={S.card}>
                <div style={S.row}>
                    <span style={S.label}>Confidence</span>
                    <span style={{ ...S.value, color: trustColor(confidence) }}>
                        {(confidence * 100).toFixed(0)}%
                    </span>
                </div>
                <div style={S.gauge}>
                    <div style={S.gaugeFill(confidence * 100, trustColor(confidence))} />
                </div>
            </div>

            {/* Meta */}
            <div style={S.card}>
                {actor && (
                    <div style={S.row}>
                        <span style={S.label}>Actor</span>
                        <span style={S.value}>{actor}</span>
                    </div>
                )}
                <div style={S.row}>
                    <span style={S.label}>Date</span>
                    <span style={S.value}>{date || '--'}</span>
                </div>
                <div style={S.rowLast}>
                    <span style={S.label}>Age</span>
                    <span style={{ ...S.value, color: colors.textDim }}>{timeAgo(date) || '--'}</span>
                </div>
            </div>

            {/* Description */}
            {description && (
                <div style={S.section}>
                    <div style={S.sectionTitle}>DESCRIPTION</div>
                    <div style={S.description}>{description}</div>
                </div>
            )}

            {/* Source Accuracy */}
            {hitRate != null && (
                <div style={S.section}>
                    <div style={S.sectionTitle}>SOURCE ACCURACY</div>
                    <div style={S.card}>
                        <div style={S.row}>
                            <span style={S.label}>Historical Hit Rate</span>
                            <span style={{ ...S.value, color: trustColor(hitRate) }}>
                                {(hitRate * 100).toFixed(0)}%
                            </span>
                        </div>
                        <div style={S.gauge}>
                            <div style={S.gaugeFill(hitRate * 100, trustColor(hitRate))} />
                        </div>
                    </div>
                </div>
            )}
        </>
    );
}

function EventDetail({ node }) {
    const data = node.data || {};
    const category = data.category || data.type || '';
    const eventName = data.name || data.title || data.event || '';
    const date = data.date || data.event_date || '';
    const description = data.description || data.text || '';
    const relatedActors = data.related_actors || data.relatedActors || [];
    const impact = data.impact || data.impact_assessment || null;
    const cd = countdown(date);

    return (
        <>
            {/* Date + Countdown */}
            <div style={S.card}>
                <div style={S.row}>
                    <span style={S.label}>Date</span>
                    <span style={S.value}>{date || '--'}</span>
                </div>
                {cd && (
                    <div style={S.rowLast}>
                        <span style={S.label}>Countdown</span>
                        <span style={S.countdown}>{cd}</span>
                    </div>
                )}
                {!cd && date && (
                    <div style={S.rowLast}>
                        <span style={S.label}>Age</span>
                        <span style={{ ...S.value, color: colors.textDim }}>{timeAgo(date)}</span>
                    </div>
                )}
            </div>

            {/* Description */}
            {description && (
                <div style={S.section}>
                    <div style={S.sectionTitle}>DESCRIPTION</div>
                    <div style={S.description}>{description}</div>
                </div>
            )}

            {/* Related Actors */}
            <div style={S.section}>
                <div style={S.sectionTitle}>RELATED ACTORS</div>
                {relatedActors.length === 0 && <div style={S.emptyText}>No related actors</div>}
                {relatedActors.slice(0, 8).map((a, i) => (
                    <div key={i} style={S.signalItem}>
                        <span style={{ fontSize: '12px', color: colors.text, fontFamily: SANS }}>
                            {typeof a === 'string' ? a : a.name || a.actor || '--'}
                        </span>
                    </div>
                ))}
            </div>

            {/* Impact Assessment */}
            {impact && (
                <div style={S.section}>
                    <div style={S.sectionTitle}>IMPACT ASSESSMENT</div>
                    <div style={S.card}>
                        {typeof impact === 'string' ? (
                            <div style={S.description}>{impact}</div>
                        ) : (
                            <>
                                {impact.severity && (
                                    <div style={S.row}>
                                        <span style={S.label}>Severity</span>
                                        <span style={S.badge(
                                            impact.severity === 'high' ? colors.red :
                                            impact.severity === 'medium' ? colors.yellow : colors.accent
                                        )}>
                                            {impact.severity}
                                        </span>
                                    </div>
                                )}
                                {impact.description && (
                                    <div style={{ ...S.description, marginTop: '6px' }}>{impact.description}</div>
                                )}
                            </>
                        )}
                    </div>
                </div>
            )}
        </>
    );
}

/* ── Main Component ──────────────────────────────────────────── */

export default function DetailPanel({ node, onClose, onExpand, onInvestigate, onHide, onPin }) {
    if (!node) return null;

    const nodeType = node.type || node.nodeType || 'actor';
    const nodeName = node.label || node.name || node.id || 'Unknown';
    const nodeTitle = node.title || node.subtitle || '';
    const tier = node.tier || node.data?.tier || '';
    const tierColor = TIER_COLORS[tier] || TIER_COLORS.unknown;
    const sourceType = node.data?.source_type || node.data?.sourceType || nodeType;
    const direction = node.data?.direction || '';
    const category = node.data?.category || node.data?.type || '';

    const handleBtnHover = useCallback((e, enter) => {
        if (enter) {
            e.currentTarget.style.borderColor = colors.accent;
            e.currentTarget.style.color = colors.text;
        } else {
            e.currentTarget.style.borderColor = colors.border;
            e.currentTarget.style.color = colors.textDim;
        }
    }, []);

    return (
        <div
            style={S.overlay}
            onClick={(e) => e.stopPropagation()}
        >
            {/* Header */}
            <div style={S.header}>
                <div style={S.headerRow}>
                    <div style={S.headerInfo}>
                        <div style={{ display: 'flex', gap: '6px', marginBottom: '6px', flexWrap: 'wrap' }}>
                            {/* Type badge */}
                            <span style={S.badge(
                                nodeType === 'actor' ? tierColor :
                                nodeType === 'ticker' ? colors.accent :
                                nodeType === 'signal' ? '#8B5CF6' :
                                nodeType === 'event' ? colors.yellow :
                                colors.textMuted
                            )}>
                                {nodeType === 'actor' ? (tier || 'actor') : nodeType}
                            </span>
                            {/* Direction badge for signals */}
                            {direction && <span style={S.directionBadge(direction)}>{direction}</span>}
                            {/* Source type badge for signals */}
                            {nodeType === 'signal' && sourceType && sourceType !== 'signal' && (
                                <span style={S.badge(colors.textMuted)}>{sourceType}</span>
                            )}
                            {/* Category badge for events */}
                            {nodeType === 'event' && category && (
                                <span style={S.badge(colors.yellow)}>{category}</span>
                            )}
                        </div>
                        <div style={S.nodeName}>{nodeName}</div>
                        {nodeTitle && <div style={S.nodeTitle}>{nodeTitle}</div>}
                    </div>
                    <button
                        style={S.closeBtn}
                        onClick={onClose}
                        onMouseEnter={(e) => { e.currentTarget.style.background = colors.cardHover; }}
                        onMouseLeave={(e) => { e.currentTarget.style.background = 'none'; }}
                        title="Close"
                    >
                        &times;
                    </button>
                </div>
            </div>

            {/* Body */}
            <div style={S.body}>
                {nodeType === 'actor' && <ActorDetail node={node} />}
                {nodeType === 'ticker' && <TickerDetail node={node} />}
                {nodeType === 'signal' && <SignalDetail node={node} />}
                {nodeType === 'event' && <EventDetail node={node} />}
            </div>

            {/* Footer Actions */}
            <div style={S.footer}>
                <button
                    style={{ ...S.actionBtn, ...S.actionBtnPrimary }}
                    onClick={() => onExpand?.(node)}
                    title="Add connected nodes to canvas"
                >
                    Expand
                </button>
                <button
                    style={S.actionBtn}
                    onClick={() => onInvestigate?.(node)}
                    onMouseEnter={(e) => handleBtnHover(e, true)}
                    onMouseLeave={(e) => handleBtnHover(e, false)}
                    title="Open investigation thread"
                >
                    Investigate
                </button>
                <button
                    style={S.actionBtn}
                    onClick={() => onHide?.(node)}
                    onMouseEnter={(e) => handleBtnHover(e, true)}
                    onMouseLeave={(e) => handleBtnHover(e, false)}
                    title="Remove from canvas"
                >
                    Hide
                </button>
                <button
                    style={S.actionBtn}
                    onClick={() => onPin?.(node)}
                    onMouseEnter={(e) => handleBtnHover(e, true)}
                    onMouseLeave={(e) => handleBtnHover(e, false)}
                    title="Lock position"
                >
                    Pin
                </button>
            </div>
        </div>
    );
}
