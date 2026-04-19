import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { AlertTriangle, ArrowRight, Brain, RefreshCw, Search, ShieldAlert, Target } from 'lucide-react';
import { api } from '../api.js';
import { routes } from '../routes.js';
import { colors, shared, tokens } from '../styles/shared.js';
import { useDevice } from '../hooks/useDevice.js';

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";
const STATUS_ORDER = ['active', 'arming', 'watch', 'background'];

const STATUS_META = {
    active: { label: 'Act Now', color: colors.green, background: colors.greenBg },
    arming: { label: 'Getting Close', color: colors.yellow, background: colors.yellowBg },
    watch: { label: 'On Watch', color: colors.accent, background: `${colors.accent}22` },
    background: { label: 'Background', color: colors.textMuted, background: colors.bg },
};

const QUALITY_META = {
    tight: { label: 'Tight', color: colors.green, background: colors.greenBg },
    mixed: { label: 'Mixed', color: colors.yellow, background: colors.yellowBg },
    lagging: { label: 'Lagging', color: colors.red, background: colors.redBg },
};

const BOARD_STATUS_META = {
    confirmed: { label: 'Confirmed', color: colors.green, background: colors.greenBg },
    contained: { label: 'Held', color: colors.green, background: colors.greenBg },
    due: { label: 'Due', color: colors.yellow, background: colors.yellowBg },
    late: { label: 'Late', color: colors.red, background: colors.redBg },
    missing: { label: 'No Print', color: colors.red, background: colors.redBg },
    narrow: { label: 'Thin', color: colors.accent, background: `${colors.accent}22` },
    unframed: { label: 'Open', color: colors.textMuted, background: colors.bg },
};

function formatStamp(value) {
    if (!value) return 'n/a';
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return value;
    return parsed.toLocaleString([], {
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
    });
}

function formatDay(value) {
    if (!value || value === 'n/a') return 'TBD';
    const raw = String(value).includes('T') ? String(value) : `${value}T12:00:00Z`;
    const parsed = new Date(raw);
    if (Number.isNaN(parsed.getTime())) return value;
    return parsed.toLocaleDateString([], {
        month: 'short',
        day: 'numeric',
    });
}

function StatCard({ label, value, tone = colors.accent }) {
    return (
        <div style={{
            border: `1px solid ${colors.border}`,
            borderRadius: '8px',
            background: colors.card,
            padding: '12px',
            minHeight: '74px',
        }}>
            <div style={{
                color: colors.textMuted,
                fontSize: '10px',
                fontWeight: 800,
                fontFamily: mono,
                textTransform: 'uppercase',
                letterSpacing: 0,
            }}>
                {label}
            </div>
            <div style={{
                marginTop: '8px',
                color: tone,
                fontSize: '24px',
                fontWeight: 900,
                fontFamily: mono,
                lineHeight: 1.1,
                overflowWrap: 'anywhere',
            }}>
                {value}
            </div>
        </div>
    );
}

function MiniMetric({ label, value, tone = '#E8F0F8' }) {
    return (
        <div style={{
            border: `1px solid ${colors.borderSubtle}`,
            borderRadius: '8px',
            padding: '10px',
            background: colors.bg,
            minWidth: 0,
        }}>
            <div style={{
                color: colors.textMuted,
                fontSize: '10px',
                fontWeight: 800,
                fontFamily: mono,
                textTransform: 'uppercase',
                overflowWrap: 'anywhere',
            }}>
                {label}
            </div>
            <div style={{
                marginTop: '6px',
                color: tone,
                fontSize: '14px',
                fontWeight: 800,
                lineHeight: 1.35,
                overflowWrap: 'anywhere',
            }}>
                {value}
            </div>
        </div>
    );
}

function FilterButton({ active, label, onClick }) {
    return (
        <button
            onClick={onClick}
            style={{
                minHeight: '36px',
                padding: '8px 12px',
                borderRadius: '8px',
                border: `1px solid ${active ? colors.accent : colors.border}`,
                background: active ? `${colors.accent}22` : colors.card,
                color: active ? '#E8F0F8' : colors.textDim,
                fontFamily: mono,
                fontSize: '11px',
                fontWeight: 800,
                letterSpacing: 0,
                cursor: 'pointer',
            }}
        >
            {label}
        </button>
    );
}

function DotList({ items, color = colors.textDim }) {
    if (!items?.length) return null;
    return (
        <ul style={{
            margin: 0,
            paddingLeft: '16px',
            color,
            display: 'grid',
            gap: '6px',
        }}>
            {items.map((item) => (
                <li key={item} style={{
                    fontSize: '12px',
                    lineHeight: 1.55,
                    overflowWrap: 'anywhere',
                }}>
                    {item}
                </li>
            ))}
        </ul>
    );
}

function DecisionWindow({ window, isMobile }) {
    const statusMeta = BOARD_STATUS_META[window?.status] || BOARD_STATUS_META.unframed;
    const points = [
        { label: 'Last Proof', value: formatDay(window?.last_signal_date), color: colors.accent },
        { label: 'Need More By', value: formatDay(window?.confirm_by_date), color: colors.yellow },
        { label: 'Wrong If Quiet', value: formatDay(window?.negate_by_date), color: colors.red },
    ];

    return (
        <div style={{ display: 'grid', gap: '10px', minWidth: 0 }}>
            <div style={{
                display: 'grid',
                gridTemplateColumns: 'minmax(0, 1fr) 24px minmax(0, 1fr) 24px minmax(0, 1fr)',
                rowGap: '8px',
                alignItems: 'center',
            }}>
                {points.map((point, index) => (
                    <React.Fragment key={point.label}>
                        <div style={{
                            display: 'grid',
                            justifyItems: 'center',
                            textAlign: 'center',
                            gap: '8px',
                            minWidth: 0,
                        }}>
                            <div style={{
                                width: '14px',
                                height: '14px',
                                borderRadius: '999px',
                                background: point.value === 'TBD' ? colors.bg : point.color,
                                border: `2px solid ${point.value === 'TBD' ? colors.borderSubtle : point.color}`,
                                boxSizing: 'border-box',
                            }} />
                            <div style={{ minWidth: 0 }}>
                                <div style={{
                                    color: colors.textMuted,
                                    fontSize: '10px',
                                    fontFamily: mono,
                                    fontWeight: 800,
                                    textTransform: 'uppercase',
                                    overflowWrap: 'anywhere',
                                }}>
                                    {point.label}
                                </div>
                                <div style={{
                                    marginTop: '4px',
                                    color: '#E8F0F8',
                                    fontSize: isMobile ? '12px' : '13px',
                                    fontWeight: 700,
                                    overflowWrap: 'anywhere',
                                }}>
                                    {point.value}
                                </div>
                            </div>
                        </div>
                        {index < points.length - 1 ? (
                            <div style={{
                                height: '2px',
                                background: colors.borderSubtle,
                                borderRadius: '999px',
                            }} />
                        ) : null}
                    </React.Fragment>
                ))}
            </div>

            <div style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: '8px',
                alignItems: 'center',
            }}>
                <span style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    minHeight: '24px',
                    padding: '3px 8px',
                    borderRadius: '6px',
                    background: statusMeta.background,
                    color: statusMeta.color,
                    fontSize: '10px',
                    fontWeight: 800,
                    fontFamily: mono,
                    textTransform: 'uppercase',
                }}>
                    {statusMeta.label}
                </span>
                {window?.cadence_days ? (
                    <span style={{
                        color: colors.textMuted,
                        fontSize: '11px',
                        fontFamily: mono,
                        overflowWrap: 'anywhere',
                    }}>
                        cadence ~{window.cadence_days}d
                    </span>
                ) : null}
            </div>

            <div style={{
                color: colors.textDim,
                fontSize: '12px',
                lineHeight: 1.6,
                overflowWrap: 'anywhere',
            }}>
                {window?.status_note}
            </div>
        </div>
    );
}

function DriverStack({ steps }) {
    if (!steps?.length) return null;
    return (
        <div style={{ display: 'grid', gap: '8px', minWidth: 0 }}>
            {steps.map((step, index) => (
                <div key={`${step.label}-${index}`} style={{ display: 'grid', gap: '4px', minWidth: 0 }}>
                    <div style={{
                        color: colors.textMuted,
                        fontSize: '10px',
                        fontWeight: 800,
                        fontFamily: mono,
                        textTransform: 'uppercase',
                        overflowWrap: 'anywhere',
                    }}>
                        {step.label}
                    </div>
                    <div style={{
                        color: '#E8F0F8',
                        fontSize: '13px',
                        lineHeight: 1.55,
                        overflowWrap: 'anywhere',
                    }}>
                        {step.value}
                    </div>
                    {index < steps.length - 1 ? (
                        <div style={{
                            width: '100%',
                            height: '1px',
                            background: colors.borderSubtle,
                            marginTop: '4px',
                        }} />
                    ) : null}
                </div>
            ))}
        </div>
    );
}

function ConfirmationBoard({ rows, isMobile }) {
    if (!rows?.length) return null;
    return (
        <div style={{ display: 'grid', gap: '8px', minWidth: 0 }}>
            {rows.map((row) => {
                const meta = BOARD_STATUS_META[row.status] || BOARD_STATUS_META.unframed;
                return (
                    <div
                        key={`${row.label}-${row.detail}`}
                        style={{
                            display: 'grid',
                            gridTemplateColumns: isMobile ? '1fr' : 'minmax(120px, 160px) minmax(92px, 110px) minmax(0, 1fr)',
                            gap: '8px',
                            alignItems: 'start',
                            border: `1px solid ${colors.borderSubtle}`,
                            borderRadius: '8px',
                            background: colors.bg,
                            padding: '10px',
                            minWidth: 0,
                        }}
                    >
                        <div style={{
                            color: '#E8F0F8',
                            fontSize: '12px',
                            fontWeight: 700,
                            overflowWrap: 'anywhere',
                        }}>
                            {row.label}
                        </div>
                        <div>
                            <span style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                minHeight: '22px',
                                padding: '2px 8px',
                                borderRadius: '6px',
                                background: meta.background,
                                color: meta.color,
                                fontSize: '10px',
                                fontWeight: 800,
                                fontFamily: mono,
                                textTransform: 'uppercase',
                            }}>
                                {meta.label}
                            </span>
                        </div>
                        <div style={{
                            color: colors.textDim,
                            fontSize: '12px',
                            lineHeight: 1.55,
                            overflowWrap: 'anywhere',
                        }}>
                            {row.detail}
                        </div>
                    </div>
                );
            })}
        </div>
    );
}

function OpportunityCard({ item, routeLabel, isMobile, onNavigate }) {
    const statusMeta = STATUS_META[item.status] || STATUS_META.watch;
    const qualityMeta = QUALITY_META[item.quality_label] || QUALITY_META.mixed;
    const targetLabel = routeLabel || 'Next clue chain';

    return (
        <article style={{
            border: `1px solid ${colors.border}`,
            borderRadius: '8px',
            background: colors.card,
            padding: isMobile ? '12px' : '16px',
            display: 'grid',
            gap: '14px',
            minWidth: 0,
        }}>
            <div style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'flex-start',
                gap: '12px',
                flexWrap: 'wrap',
            }}>
                <div style={{ minWidth: 0, flex: '1 1 320px' }}>
                    <div style={{
                        display: 'flex',
                        flexWrap: 'wrap',
                        gap: '6px',
                        marginBottom: '8px',
                    }}>
                        <span style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            minHeight: '24px',
                            padding: '3px 8px',
                            borderRadius: '6px',
                            background: statusMeta.background,
                            color: statusMeta.color,
                            fontSize: '10px',
                            fontWeight: 800,
                            fontFamily: mono,
                            letterSpacing: 0,
                            textTransform: 'uppercase',
                        }}>
                            {statusMeta.label}
                        </span>
                        <span style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            minHeight: '24px',
                            padding: '3px 8px',
                            borderRadius: '6px',
                            background: colors.bg,
                            color: colors.textDim,
                            fontSize: '10px',
                            fontWeight: 800,
                            fontFamily: mono,
                            letterSpacing: 0,
                            textTransform: 'uppercase',
                        }}>
                            {item.category}
                        </span>
                        {item.sector_focus ? (
                            <span style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                minHeight: '24px',
                                padding: '3px 8px',
                                borderRadius: '6px',
                                background: colors.cardHover,
                                color: '#E8F0F8',
                                fontSize: '10px',
                                fontWeight: 800,
                                fontFamily: mono,
                                letterSpacing: 0,
                                textTransform: 'uppercase',
                            }}>
                                {item.sector_focus}
                            </span>
                        ) : null}
                        {typeof item.confidence === 'number' ? (
                            <span style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                minHeight: '24px',
                                padding: '3px 8px',
                                borderRadius: '6px',
                                background: `${statusMeta.color}22`,
                                color: statusMeta.color,
                                fontSize: '10px',
                                fontWeight: 800,
                                fontFamily: mono,
                                letterSpacing: 0,
                                textTransform: 'uppercase',
                            }}>
                                CONF {item.confidence}%
                            </span>
                        ) : null}
                        {item.quality_label ? (
                            <span style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                minHeight: '24px',
                                padding: '3px 8px',
                                borderRadius: '6px',
                                background: qualityMeta.background,
                                color: qualityMeta.color,
                                fontSize: '10px',
                                fontWeight: 800,
                                fontFamily: mono,
                                letterSpacing: 0,
                                textTransform: 'uppercase',
                            }}>
                                {qualityMeta.label}
                            </span>
                        ) : null}
                    </div>
                    <h2 style={{
                        margin: 0,
                        color: '#E8F0F8',
                        fontSize: isMobile ? '20px' : '24px',
                        lineHeight: 1.15,
                        fontWeight: 800,
                        overflowWrap: 'anywhere',
                    }}>
                        {item.title}
                    </h2>
                    <p style={{
                        margin: '8px 0 0',
                        color: colors.textDim,
                        fontSize: '13px',
                        lineHeight: 1.6,
                        overflowWrap: 'anywhere',
                    }}>
                        {item.summary}
                    </p>
                </div>

                <div style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(2, minmax(88px, 1fr))',
                    gap: '8px',
                    width: isMobile ? '100%' : '220px',
                    flexShrink: 0,
                }}>
                    <div style={{
                        border: `1px solid ${colors.borderSubtle}`,
                        borderRadius: '8px',
                        padding: '10px',
                        background: colors.bg,
                    }}>
                        <div style={{ color: colors.textMuted, fontSize: '10px', fontFamily: mono, fontWeight: 800 }}>
                            SCORE
                        </div>
                        <div style={{ marginTop: '6px', color: '#E8F0F8', fontSize: '24px', fontFamily: mono, fontWeight: 900 }}>
                            {item.score}
                        </div>
                    </div>
                    <div style={{
                        border: `1px solid ${colors.borderSubtle}`,
                        borderRadius: '8px',
                        padding: '10px',
                        background: colors.bg,
                    }}>
                        <div style={{ color: colors.textMuted, fontSize: '10px', fontFamily: mono, fontWeight: 800 }}>
                            EDGE
                        </div>
                        <div style={{ marginTop: '6px', color: statusMeta.color, fontSize: '24px', fontFamily: mono, fontWeight: 900 }}>
                            {Math.round(item.expected_edge_pct)}%
                        </div>
                    </div>
                </div>
            </div>

            <div style={{
                borderTop: `1px solid ${colors.borderSubtle}`,
                paddingTop: '14px',
                display: 'grid',
                gridTemplateColumns: isMobile ? '1fr' : 'repeat(2, minmax(0, 1fr))',
                gap: '14px',
                minWidth: 0,
            }}>
                <div style={{ minWidth: 0 }}>
                    <div style={{ ...shared.sectionTitle, marginBottom: '8px' }}>Decision Window</div>
                    <DecisionWindow window={item.decision_window} isMobile={isMobile} />
                </div>
                <div style={{ minWidth: 0 }}>
                    <div style={{ ...shared.sectionTitle, marginBottom: '8px' }}>What Is In Play</div>
                    <DriverStack steps={item.driver_stack} />
                </div>
            </div>

            <div style={{
                borderTop: `1px solid ${colors.borderSubtle}`,
                paddingTop: '14px',
                display: 'grid',
                gridTemplateColumns: isMobile ? 'repeat(2, minmax(0, 1fr))' : 'repeat(3, minmax(0, 1fr))',
                gap: '10px',
                minWidth: 0,
            }}>
                <MiniMetric label="Names Carrying It" value={`${item.stakes?.breadth_count ?? 0} names`} tone={colors.accent} />
                <MiniMetric label="Proof Types" value={`${item.stakes?.source_family_count ?? 0}`} tone={'#E8F0F8'} />
                <MiniMetric label="At Stake" value={item.stakes?.capital_signal || 'n/a'} tone={statusMeta.color} />
            </div>

            <div style={{
                borderTop: `1px solid ${colors.borderSubtle}`,
                paddingTop: '14px',
                display: 'grid',
                gap: '10px',
                minWidth: 0,
            }}>
                <div style={{ ...shared.sectionTitle }}>Confirmation / Negation</div>
                <ConfirmationBoard rows={item.confirmation_board} isMobile={isMobile} />
            </div>

            {(item.lagging_factors?.length || item.upgrade_trigger) ? (
                <div style={{
                    borderTop: `1px solid ${colors.borderSubtle}`,
                    paddingTop: '14px',
                    display: 'grid',
                    gridTemplateColumns: isMobile ? '1fr' : 'repeat(2, minmax(0, 1fr))',
                    gap: '14px',
                    minWidth: 0,
                }}>
                    <div style={{ minWidth: 0 }}>
                        <div style={{ ...shared.sectionTitle, marginBottom: '8px' }}>What Is Dragging</div>
                        {item.lagging_factors?.length ? (
                            <DotList items={item.lagging_factors} color={colors.textDim} />
                        ) : (
                            <div style={{ color: colors.textDim, fontSize: '12px', lineHeight: 1.6 }}>
                                Nothing obvious is dragging this setup right now.
                            </div>
                        )}
                    </div>
                    <div style={{ minWidth: 0 }}>
                        <div style={{ ...shared.sectionTitle, marginBottom: '8px' }}>What Upgrades It</div>
                        <div style={{
                            color: '#E8F0F8',
                            fontSize: '13px',
                            lineHeight: 1.6,
                            overflowWrap: 'anywhere',
                        }}>
                            {item.upgrade_trigger}
                        </div>
                    </div>
                </div>
            ) : null}

            <div style={{
                borderTop: `1px solid ${colors.borderSubtle}`,
                paddingTop: '14px',
                display: 'grid',
                gridTemplateColumns: isMobile ? '1fr' : 'repeat(2, minmax(0, 1fr))',
                gap: '14px',
                minWidth: 0,
            }}>
                <div style={{ minWidth: 0 }}>
                    <div style={{ ...shared.sectionTitle, marginBottom: '8px' }}>What Has To Be True</div>
                    <div style={{
                        color: '#E8F0F8',
                        fontSize: '13px',
                        lineHeight: 1.6,
                        overflowWrap: 'anywhere',
                    }}>
                        {item.mispricing_test}
                    </div>
                </div>
                <div style={{ minWidth: 0 }}>
                    <div style={{ ...shared.sectionTitle, marginBottom: '8px' }}>What Would Prove It</div>
                    <div style={{
                        color: colors.textDim,
                        fontSize: '13px',
                        lineHeight: 1.6,
                        overflowWrap: 'anywhere',
                    }}>
                        {item.proof_needed}
                    </div>
                </div>
                <div style={{ minWidth: 0 }}>
                    <div style={{ ...shared.sectionTitle, marginBottom: '8px' }}>What To Hunt</div>
                    <DotList items={item.clues} />
                </div>
                <div style={{ minWidth: 0 }}>
                    <div style={{ ...shared.sectionTitle, marginBottom: '8px' }}>Kill It If</div>
                    <div style={{
                        color: '#FCA5A5',
                        fontSize: '13px',
                        lineHeight: 1.6,
                        overflowWrap: 'anywhere',
                    }}>
                        {item.kill_switch}
                    </div>
                </div>
            </div>

            <div style={{
                borderTop: `1px solid ${colors.borderSubtle}`,
                paddingTop: '14px',
                display: 'grid',
                gridTemplateColumns: isMobile ? '1fr' : 'minmax(0, 1.25fr) minmax(0, 1fr)',
                gap: '14px',
                minWidth: 0,
            }}>
                <div style={{ minWidth: 0 }}>
                    <div style={{ ...shared.sectionTitle, marginBottom: '8px' }}>Live Clues</div>
                    <DotList items={item.evidence} color={colors.textDim} />
                </div>
                <div style={{ minWidth: 0 }}>
                    <div style={{ ...shared.sectionTitle, marginBottom: '8px' }}>Targets</div>
                    <div style={{
                        display: 'flex',
                        flexWrap: 'wrap',
                        gap: '6px',
                        marginBottom: item.targets?.length ? '12px' : 0,
                    }}>
                        {(item.targets || []).map((target) => (
                            <span
                                key={target}
                                style={{
                                    display: 'inline-flex',
                                    alignItems: 'center',
                                    minHeight: '24px',
                                    padding: '4px 8px',
                                    borderRadius: '6px',
                                    background: colors.bg,
                                    border: `1px solid ${colors.borderSubtle}`,
                                    color: '#E8F0F8',
                                    fontSize: '11px',
                                    fontFamily: mono,
                                    fontWeight: 700,
                                    overflowWrap: 'anywhere',
                                }}
                            >
                                {target}
                            </span>
                        ))}
                    </div>
                    <div style={{ ...shared.sectionTitle, marginBottom: '8px' }}>Trade Plan</div>
                    <div style={{
                        display: 'grid',
                        gap: '8px',
                        color: colors.textDim,
                        fontSize: '12px',
                        lineHeight: 1.55,
                        overflowWrap: 'anywhere',
                    }}>
                        <div><span style={{ color: '#E8F0F8', fontWeight: 700 }}>Enter:</span> {item.entry_rule}</div>
                        <div><span style={{ color: '#E8F0F8', fontWeight: 700 }}>Exit:</span> {item.exit_rule}</div>
                        <div><span style={{ color: '#E8F0F8', fontWeight: 700 }}>Why now:</span> {item.why_now}</div>
                    </div>
                </div>
            </div>

            <div style={{
                borderTop: `1px solid ${colors.borderSubtle}`,
                paddingTop: '12px',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                gap: '10px',
                flexWrap: 'wrap',
            }}>
                <div style={{
                    color: colors.textMuted,
                    fontSize: '11px',
                    fontFamily: mono,
                    lineHeight: 1.5,
                    overflowWrap: 'anywhere',
                }}>
                    <div>{item.data_mode.toUpperCase()} | {item.horizon} | {(item.source_tags?.length ? item.source_tags.join(' | ') : 'Public data')}</div>
                    {item.supporting_source_types?.length ? (
                        <div style={{ marginTop: '4px' }}>
                            Sources: {item.supporting_source_types.join(' | ')}
                        </div>
                    ) : null}
                </div>
                <button
                    onClick={() => item.route_hint && onNavigate?.(item.route_hint)}
                    disabled={!item.route_hint}
                    style={{
                        minHeight: '38px',
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: '8px',
                        borderRadius: '8px',
                        border: `1px solid ${item.route_hint ? colors.accent : colors.border}`,
                        background: item.route_hint ? `${colors.accent}22` : colors.bg,
                        color: item.route_hint ? '#E8F0F8' : colors.textMuted,
                        padding: '8px 12px',
                        fontSize: '12px',
                        fontWeight: 800,
                        fontFamily: mono,
                        cursor: item.route_hint ? 'pointer' : 'default',
                    }}
                >
                    Open {targetLabel}
                    <ArrowRight size={14} />
                </button>
            </div>
        </article>
    );
}

export default function EdgeScanner({ onNavigate }) {
    const { isMobile } = useDevice();
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');
    const [filter, setFilter] = useState('all');

    const routeLabels = useMemo(
        () => new Map(routes.map((route) => [route.id, route.label])),
        []
    );

    const loadEdges = useCallback(async () => {
        setLoading(true);
        try {
            const payload = await api.getMarketEdges(10);
            if (payload?.error && !payload?.opportunities?.length) {
                setError(payload.error || payload.message || 'Failed to load edge scan.');
            } else {
                setError(payload?.error || '');
            }
            setData(payload);
        } catch (err) {
            setError(err.message || 'Failed to load edge scan.');
            setData(null);
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        loadEdges();
    }, [loadEdges]);

    const opportunities = data?.opportunities || [];
    const coverageGaps = data?.coverage_gaps || [];
    const summary = data?.summary || {};
    const filtered = filter === 'all'
        ? opportunities
        : opportunities.filter((item) => item.status === filter);

    const sections = STATUS_ORDER
        .map((status) => ({
            status,
            items: filtered.filter((item) => item.status === status),
        }))
        .filter((section) => section.items.length > 0);

    return (
        <div style={{
            width: '100%',
            minHeight: '100vh',
            boxSizing: 'border-box',
            padding: isMobile ? '12px 12px calc(84px + env(safe-area-inset-bottom, 0px))' : '18px 18px 32px',
            background: colors.bg,
            color: colors.text,
            overflowX: 'hidden',
        }}>
            <div style={{
                maxWidth: '1180px',
                margin: '0 auto',
                display: 'grid',
                gap: '14px',
            }}>
                <header style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'flex-start',
                    gap: '12px',
                    flexWrap: 'wrap',
                }}>
                    <div style={{ minWidth: 0 }}>
                        <div style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '8px',
                            color: colors.accentLight || colors.accent,
                            fontSize: '11px',
                            fontWeight: 800,
                            fontFamily: mono,
                            textTransform: 'uppercase',
                            letterSpacing: 0,
                            marginBottom: '8px',
                        }}>
                            <Search size={14} />
                            Edge Scanner
                        </div>
                        <h1 style={{
                            margin: 0,
                            color: '#E8F0F8',
                            fontSize: isMobile ? '28px' : '34px',
                            lineHeight: 1.05,
                            fontWeight: 900,
                            overflowWrap: 'anywhere',
                        }}>
                            Durable edges from public clue chains
                        </h1>
                        <p style={{
                            margin: '10px 0 0',
                            maxWidth: '880px',
                            color: colors.textDim,
                            fontSize: '14px',
                            lineHeight: 1.6,
                            overflowWrap: 'anywhere',
                        }}>
                            Start with the names already showing proof. If the proof is weak, it stays on watch. If the kill switch trips, it is dead.
                        </p>
                    </div>

                    <button
                        onClick={loadEdges}
                        disabled={loading}
                        style={{
                            minHeight: '40px',
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '8px',
                            padding: '8px 12px',
                            borderRadius: '8px',
                            border: `1px solid ${colors.border}`,
                            background: colors.card,
                            color: '#E8F0F8',
                            fontSize: '12px',
                            fontWeight: 800,
                            fontFamily: mono,
                            cursor: loading ? 'wait' : 'pointer',
                            flexShrink: 0,
                        }}
                    >
                        <RefreshCw size={14} style={{ animation: loading ? 'spin 1s linear infinite' : 'none' }} />
                        {loading ? 'Refreshing' : 'Refresh'}
                    </button>
                </header>

                <div style={{
                    border: `1px solid ${colors.border}`,
                    borderRadius: '8px',
                    background: colors.card,
                    padding: isMobile ? '12px' : '14px 16px',
                    display: 'grid',
                    gap: '10px',
                }}>
                    <div style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px',
                        color: '#E8F0F8',
                        fontSize: '13px',
                        fontWeight: 700,
                        overflowWrap: 'anywhere',
                    }}>
                        <Target size={15} color={colors.accent} />
                        Work the ones with proof first. Let the rest earn the right to matter.
                    </div>
                    <div style={{
                        color: colors.textDim,
                        fontSize: '12px',
                        lineHeight: 1.6,
                        overflowWrap: 'anywhere',
                    }}>
                        {summary?.disclaimer || 'This map ranks live structural levers from public data only.'}
                    </div>
                    <div style={{
                        color: colors.textMuted,
                        fontSize: '11px',
                        fontFamily: mono,
                        overflowWrap: 'anywhere',
                    }}>
                        Updated {formatStamp(data?.generated_at)} | As of {data?.as_of || 'n/a'}
                    </div>
                </div>

                {error && (
                    <div style={{
                        border: `1px solid ${colors.red}55`,
                        borderRadius: '8px',
                        background: colors.redBg,
                        padding: '12px 14px',
                        display: 'flex',
                        gap: '10px',
                        alignItems: 'flex-start',
                    }}>
                        <AlertTriangle size={16} color={colors.red} style={{ flexShrink: 0, marginTop: '1px' }} />
                        <div style={{
                            color: '#FECACA',
                            fontSize: '12px',
                            lineHeight: 1.6,
                            overflowWrap: 'anywhere',
                        }}>
                            {error}
                        </div>
                    </div>
                )}

                <div style={{
                    display: 'grid',
                    gridTemplateColumns: `repeat(auto-fit, minmax(${isMobile ? 130 : 150}px, 1fr))`,
                    gap: '8px',
                }}>
                    <StatCard label="Active" value={summary.active_count ?? 0} tone={colors.green} />
                    <StatCard label="Arming" value={summary.arming_count ?? 0} tone={colors.yellow} />
                    <StatCard label="Live" value={summary.live_count ?? 0} tone={colors.accent} />
                    <StatCard label="Gaps" value={summary.coverage_gap_count ?? coverageGaps.length} tone={colors.red} />
                    <StatCard label="Avg Edge" value={`${Math.round(summary.avg_expected_edge_pct || 0)}%`} tone={'#E8F0F8'} />
                </div>

                <div style={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    gap: '8px',
                }}>
                    <FilterButton active={filter === 'all'} label="All" onClick={() => setFilter('all')} />
                    {STATUS_ORDER.map((status) => (
                        <FilterButton
                            key={status}
                            active={filter === status}
                            label={STATUS_META[status].label}
                            onClick={() => setFilter(status)}
                        />
                    ))}
                </div>

                {loading && !opportunities.length ? (
                    <div style={{
                        border: `1px solid ${colors.border}`,
                        borderRadius: '8px',
                        background: colors.card,
                        padding: '20px 16px',
                        color: colors.textMuted,
                        fontFamily: mono,
                        fontSize: '12px',
                    }}>
                        Scanning public clues...
                    </div>
                ) : null}

                {!loading && !filtered.length ? (
                    <div style={{
                        border: `1px solid ${colors.border}`,
                        borderRadius: '8px',
                        background: colors.card,
                        padding: '20px 16px',
                        display: 'grid',
                        gap: '8px',
                        color: colors.textDim,
                    }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#E8F0F8', fontWeight: 700 }}>
                            <ShieldAlert size={16} color={colors.yellow} />
                            Nothing matched that filter
                        </div>
                        <div style={{ fontSize: '13px', lineHeight: 1.6 }}>
                            No live signal clusters passed that screen. Hidden buckets stay hidden until real evidence lands.
                        </div>
                    </div>
                ) : null}

                {!loading && coverageGaps.length ? (
                    <section style={{ display: 'grid', gap: '10px' }}>
                        <div style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '8px',
                            color: colors.red,
                            fontSize: '12px',
                            fontWeight: 800,
                            fontFamily: mono,
                            textTransform: 'uppercase',
                            letterSpacing: 0,
                        }}>
                            <Search size={15} />
                            Missing Real Coverage
                        </div>
                        <div style={{
                            border: `1px solid ${colors.border}`,
                            borderRadius: '8px',
                            background: colors.card,
                            padding: isMobile ? '12px' : '14px',
                            display: 'grid',
                            gap: '10px',
                        }}>
                            <div style={{
                                color: colors.textDim,
                                fontSize: '13px',
                                lineHeight: 1.6,
                                overflowWrap: 'anywhere',
                            }}>
                                These sectors are withheld until qualifying live signals show up. No synthetic placeholders.
                            </div>
                            <div style={{ display: 'grid', gap: '8px' }}>
                                {coverageGaps.map((gap) => (
                                    <div
                                        key={gap.id}
                                        style={{
                                            border: `1px solid ${colors.borderSubtle}`,
                                            borderRadius: '8px',
                                            background: colors.bg,
                                            padding: '10px',
                                            display: 'grid',
                                            gap: '6px',
                                        }}
                                    >
                                        <div style={{ color: '#E8F0F8', fontSize: '13px', fontWeight: 700, overflowWrap: 'anywhere' }}>
                                            {gap.title}
                                        </div>
                                        <div style={{ color: colors.textMuted, fontSize: '11px', fontFamily: mono, overflowWrap: 'anywhere' }}>
                                            {gap.sector_focus} | Need {(gap.missing_primary_sources || []).join(' | ')}
                                        </div>
                                        <div style={{ color: colors.textDim, fontSize: '12px', lineHeight: 1.55, overflowWrap: 'anywhere' }}>
                                            {gap.reason}
                                        </div>
                                        {gap.targets?.length ? (
                                            <div style={{
                                                display: 'flex',
                                                flexWrap: 'wrap',
                                                gap: '6px',
                                            }}>
                                                {gap.targets.map((target) => (
                                                    <span
                                                        key={`${gap.id}-${target}`}
                                                        style={{
                                                            display: 'inline-flex',
                                                            alignItems: 'center',
                                                            minHeight: '24px',
                                                            padding: '4px 8px',
                                                            borderRadius: '6px',
                                                            background: colors.card,
                                                            border: `1px solid ${colors.borderSubtle}`,
                                                            color: '#E8F0F8',
                                                            fontSize: '11px',
                                                            fontFamily: mono,
                                                            fontWeight: 700,
                                                        }}
                                                    >
                                                        {target}
                                                    </span>
                                                ))}
                                            </div>
                                        ) : null}
                                    </div>
                                ))}
                            </div>
                        </div>
                    </section>
                ) : null}

                {sections.map((section) => (
                    <section key={section.status} style={{ display: 'grid', gap: '10px' }}>
                        <div style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '8px',
                            color: STATUS_META[section.status].color,
                            fontSize: '12px',
                            fontWeight: 800,
                            fontFamily: mono,
                            textTransform: 'uppercase',
                            letterSpacing: 0,
                        }}>
                            <Brain size={15} />
                            {STATUS_META[section.status].label}
                        </div>
                        {section.items.map((item) => (
                            <OpportunityCard
                                key={item.id}
                                item={item}
                                routeLabel={routeLabels.get(item.route_hint)}
                                isMobile={isMobile}
                                onNavigate={onNavigate}
                            />
                        ))}
                    </section>
                ))}
            </div>

            <style>{`
                @keyframes spin {
                    from { transform: rotate(0deg); }
                    to { transform: rotate(360deg); }
                }
            `}</style>
        </div>
    );
}
