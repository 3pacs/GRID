import React, { useState, useEffect, useMemo } from 'react';
import { api } from '../api.js';
import { shared, colors, tokens } from '../styles/shared.js';
import { formatRelative, formatFullDateTime } from '../utils/formatTime.js';

const MONO = "'JetBrains Mono', 'IBM Plex Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

const TABS = [
    { key: 'deep_dives', label: 'Deep Dives' },
    { key: 'audio', label: 'Audio Briefings' },
    { key: 'postmortems', label: 'Post-Mortems' },
    { key: 'diary', label: 'Market Diary' },
    { key: 'theses', label: 'Thesis History' },
];

const DIR_COLOR = {
    BULLISH: colors.green, bullish: colors.green,
    BEARISH: colors.red, bearish: colors.red,
    NEUTRAL: colors.textMuted, neutral: colors.textMuted,
};

function Badge({ text, color }) {
    return (
        <span style={{
            fontFamily: MONO, fontSize: '10px', fontWeight: 700,
            padding: '2px 8px', borderRadius: '4px',
            background: `${color || colors.accent}15`,
            color: color || colors.accent,
            border: `1px solid ${color || colors.accent}30`,
            letterSpacing: '0.5px',
        }}>{text}</span>
    );
}

function ExpandableCard({ title, subtitle, badges, children, defaultOpen = false }) {
    const [open, setOpen] = useState(defaultOpen);
    return (
        <div style={{
            background: colors.gradientCard, border: `1px solid ${colors.border}`,
            borderRadius: tokens.radius.md, overflow: 'hidden', marginBottom: '8px',
        }}>
            <div onClick={() => setOpen(!open)} style={{
                padding: '12px 16px', cursor: 'pointer', display: 'flex',
                justifyContent: 'space-between', alignItems: 'center',
                transition: 'background 0.15s',
            }}
                onMouseEnter={e => { e.currentTarget.style.background = `${colors.accent}08`; }}
                onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flex: 1, minWidth: 0 }}>
                    <span style={{ fontFamily: MONO, fontSize: '10px', color: colors.textDim,
                        transform: open ? 'rotate(90deg)' : 'none', transition: 'transform 0.15s' }}>
                        {'\u25B6'}
                    </span>
                    <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ fontFamily: SANS, fontSize: '13px', fontWeight: 600,
                            color: colors.text, overflow: 'hidden', textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap' }}>{title}</div>
                        {subtitle && <div style={{ fontFamily: MONO, fontSize: '10px',
                            color: colors.textDim, marginTop: '2px' }}>{subtitle}</div>}
                    </div>
                    <div style={{ display: 'flex', gap: '6px', flexShrink: 0 }}>
                        {badges}
                    </div>
                </div>
            </div>
            {open && (
                <div style={{ padding: '0 16px 14px', borderTop: `1px solid ${colors.border}40` }}>
                    {children}
                </div>
            )}
        </div>
    );
}

function DeepDiveList({ dives }) {
    if (!dives?.length) return <Empty msg="No deep dives yet. They auto-generate with each thesis." />;
    return dives.map(d => (
        <ExpandableCard
            key={d.id}
            title={`Deep Dive #${d.id} — ${d.thesis_direction || 'N/A'}`}
            subtitle={`${formatRelative(new Date(d.generated_at))} via ${d.model_used} (${d.provider_used})`}
            badges={<>
                <Badge text={d.thesis_direction || '?'} color={DIR_COLOR[d.thesis_direction]} />
                <Badge text={`${d.duration_ms}ms`} color={colors.textMuted} />
            </>}
        >
            <div style={{ marginTop: '10px' }}>
                {d.key_insights?.length > 0 && (
                    <Section label="KEY INSIGHTS" color={colors.accent}>
                        {d.key_insights.map((item, i) => <BulletItem key={i} text={item} />)}
                    </Section>
                )}
                {d.contrarian_signals?.length > 0 && (
                    <Section label="CONTRARIAN SIGNALS" color={colors.yellow}>
                        {d.contrarian_signals.map((item, i) => <BulletItem key={i} text={item} />)}
                    </Section>
                )}
                {d.risk_blind_spots?.length > 0 && (
                    <Section label="RISK BLIND SPOTS" color={colors.red}>
                        {d.risk_blind_spots.map((item, i) => <BulletItem key={i} text={item} />)}
                    </Section>
                )}
                {d.follow_up_questions?.length > 0 && (
                    <Section label="FOLLOW-UP QUESTIONS" color={colors.textMuted}>
                        {d.follow_up_questions.map((item, i) => <BulletItem key={i} text={item} />)}
                    </Section>
                )}
                <details style={{ marginTop: '10px' }}>
                    <summary style={{ fontFamily: MONO, fontSize: '10px', color: colors.textDim,
                        cursor: 'pointer', letterSpacing: '1px' }}>FULL ANALYSIS</summary>
                    <div style={{
                        marginTop: '8px', padding: '12px', background: colors.bg,
                        borderRadius: '6px', fontSize: '12px', lineHeight: 1.7,
                        color: colors.textMuted, whiteSpace: 'pre-wrap', maxHeight: '400px',
                        overflowY: 'auto', fontFamily: SANS,
                    }}>{d.analysis}</div>
                </details>
            </div>
        </ExpandableCard>
    ));
}

function AudioList({ briefings, onPlay }) {
    if (!briefings?.length) return <Empty msg="No audio briefings recorded yet." />;
    return briefings.map((b, i) => (
        <div key={i} style={{
            display: 'flex', alignItems: 'center', gap: '12px',
            padding: '10px 14px', borderRadius: '8px', marginBottom: '6px',
            background: colors.gradientCard, border: `1px solid ${colors.border}`,
        }}>
            <button onClick={() => onPlay(b.filename)} style={{
                width: '32px', height: '32px', borderRadius: '50%',
                background: `${colors.accent}20`, border: `1px solid ${colors.accent}50`,
                color: colors.accent, fontSize: '12px', cursor: 'pointer',
                display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
            }}>{'\u25B6'}</button>
            <div style={{ flex: 1 }}>
                <div style={{ fontFamily: MONO, fontSize: '12px', fontWeight: 600, color: colors.text }}>
                    {b.briefing_date}
                </div>
                <div style={{ fontFamily: MONO, fontSize: '10px', color: colors.textDim }}>
                    {(b.size_bytes / 1024).toFixed(0)} KB
                    {b.has_script && <span style={{ marginLeft: '8px', color: colors.green }}>transcript saved</span>}
                </div>
            </div>
            <div style={{ fontFamily: MONO, fontSize: '10px', color: colors.textDim }}>
                {formatRelative(new Date(b.generated_at))}
            </div>
        </div>
    ));
}

function PostmortemList({ postmortems }) {
    if (!postmortems?.length) return <Empty msg="No post-mortems yet. They auto-generate when theses are scored wrong." />;
    return postmortems.map((pm, i) => (
        <ExpandableCard
            key={i}
            title={`${pm.thesis_direction || '?'} thesis was ${pm.actual_direction || '?'}`}
            subtitle={`Root cause: ${pm.root_cause || 'unknown'}`}
            badges={<Badge text={pm.root_cause || '?'} color={colors.yellow} />}
        >
            <div style={{ marginTop: '8px' }}>
                {pm.what_we_missed && (
                    <Section label="WHAT WE MISSED" color={colors.red}>
                        <div style={{ fontSize: '12px', color: colors.textMuted, lineHeight: 1.6 }}>
                            {pm.what_we_missed}
                        </div>
                    </Section>
                )}
                {pm.lesson && (
                    <Section label="LESSON" color={colors.green}>
                        <div style={{ fontSize: '12px', color: colors.textMuted, lineHeight: 1.6 }}>
                            {pm.lesson}
                        </div>
                    </Section>
                )}
                {pm.models_that_were_right?.length > 0 && (
                    <div style={{ marginTop: '6px', fontSize: '11px' }}>
                        <span style={{ color: colors.green, fontFamily: MONO }}>RIGHT: </span>
                        <span style={{ color: colors.textMuted }}>{pm.models_that_were_right.join(', ')}</span>
                    </div>
                )}
                {pm.models_that_were_wrong?.length > 0 && (
                    <div style={{ fontSize: '11px' }}>
                        <span style={{ color: colors.red, fontFamily: MONO }}>WRONG: </span>
                        <span style={{ color: colors.textMuted }}>{pm.models_that_were_wrong.join(', ')}</span>
                    </div>
                )}
            </div>
        </ExpandableCard>
    ));
}

function DiaryList({ entries }) {
    if (!entries?.length) return <Empty msg="No diary entries yet." />;
    return entries.map((e, i) => (
        <ExpandableCard
            key={i}
            title={e.date || e.entry_date || `Entry ${i + 1}`}
            subtitle={e.verdict || e.summary?.slice(0, 80)}
            badges={e.market_return != null && (
                <Badge
                    text={`${e.market_return >= 0 ? '+' : ''}${(e.market_return * 100).toFixed(1)}%`}
                    color={e.market_return >= 0 ? colors.green : colors.red}
                />
            )}
        >
            <div style={{
                marginTop: '8px', fontSize: '12px', lineHeight: 1.7,
                color: colors.textMuted, whiteSpace: 'pre-wrap',
            }}>{e.content || e.summary || 'No content available'}</div>
        </ExpandableCard>
    ));
}

function ThesisList({ snapshots }) {
    if (!snapshots?.length) return <Empty msg="No thesis snapshots yet." />;
    return (
        <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontFamily: MONO, fontSize: '11px' }}>
                <thead>
                    <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                        {['Date', 'Direction', 'Conviction', 'Outcome', 'SPY Move'].map(h => (
                            <th key={h} style={{ padding: '6px 10px', textAlign: 'left',
                                color: colors.textDim, fontWeight: 600, letterSpacing: '1px', fontSize: '9px' }}>{h}</th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {snapshots.map((s, i) => {
                        const dir = s.overall_direction || s.direction || '?';
                        const outcome = s.outcome || '-';
                        const outcomeColor = outcome === 'correct' ? colors.green
                            : outcome === 'wrong' ? colors.red
                            : outcome === 'partial' ? colors.yellow : colors.textDim;
                        return (
                            <tr key={i} style={{ borderBottom: `1px solid ${colors.border}30` }}>
                                <td style={{ padding: '6px 10px', color: colors.textMuted }}>
                                    {s.timestamp ? formatRelative(new Date(s.timestamp)) : '-'}
                                </td>
                                <td style={{ padding: '6px 10px', color: DIR_COLOR[dir] || colors.textMuted, fontWeight: 600 }}>
                                    {dir}
                                </td>
                                <td style={{ padding: '6px 10px', color: colors.text }}>
                                    {s.conviction != null ? `${(s.conviction * 100).toFixed(0)}%` : '-'}
                                </td>
                                <td style={{ padding: '6px 10px', color: outcomeColor, fontWeight: 600 }}>
                                    {outcome}
                                </td>
                                <td style={{ padding: '6px 10px', color: s.actual_market_move >= 0 ? colors.green : colors.red }}>
                                    {s.actual_market_move != null ? `${(s.actual_market_move * 100).toFixed(2)}%` : '-'}
                                </td>
                            </tr>
                        );
                    })}
                </tbody>
            </table>
        </div>
    );
}

function Section({ label, color, children }) {
    return (
        <div style={{ marginBottom: '8px' }}>
            <div style={{ fontFamily: MONO, fontSize: '9px', fontWeight: 700,
                letterSpacing: '1.5px', color: color || colors.textMuted,
                marginBottom: '4px' }}>{label}</div>
            {children}
        </div>
    );
}

function BulletItem({ text }) {
    return (
        <div style={{ display: 'flex', gap: '6px', marginBottom: '3px', fontSize: '12px',
            lineHeight: 1.5, color: colors.textMuted }}>
            <span style={{ color: colors.textDim, flexShrink: 0 }}>{'\u2022'}</span>
            <span>{text}</span>
        </div>
    );
}

function Empty({ msg }) {
    return (
        <div style={{ padding: '30px', textAlign: 'center', fontFamily: SANS,
            fontSize: '13px', color: colors.textDim, fontStyle: 'italic' }}>{msg}</div>
    );
}


export default function Archive() {
    const [activeTab, setActiveTab] = useState('deep_dives');
    const [archive, setArchive] = useState(null);
    const [loading, setLoading] = useState(true);
    const [generating, setGenerating] = useState(false);
    const [playerUrl, setPlayerUrl] = useState(null);
    const [days, setDays] = useState(90);

    useEffect(() => {
        setLoading(true);
        api.getResearchArchive(days).then(r => {
            setArchive(r);
            setLoading(false);
        }).catch(() => setLoading(false));
    }, [days]);

    const triggerDeepDive = async () => {
        setGenerating(true);
        try {
            await api.triggerDeepDive();
        } catch { /* ignore */ }
        setGenerating(false);
    };

    const playAudio = (filename) => {
        setPlayerUrl(api.getFlowBriefingAudioUrl(filename));
    };

    const counts = useMemo(() => ({
        deep_dives: archive?.deep_dive_count || 0,
        audio: archive?.audio_count || 0,
        postmortems: archive?.postmortem_count || 0,
        diary: archive?.diary_count || 0,
        theses: archive?.thesis_count || 0,
    }), [archive]);

    const total = Object.values(counts).reduce((a, b) => a + b, 0);

    return (
        <div style={shared.container}>
            {/* Header */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
                <div>
                    <div style={shared.header}>Research Archive</div>
                    <div style={{ fontFamily: MONO, fontSize: '11px', color: colors.textDim }}>
                        {total} reports archived {'\u2014'} nothing is ever deleted
                    </div>
                </div>
                <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
                    <select value={days} onChange={e => setDays(Number(e.target.value))} style={{
                        fontFamily: MONO, fontSize: '10px', padding: '4px 8px',
                        background: colors.bg, color: colors.text, border: `1px solid ${colors.border}`,
                        borderRadius: '4px',
                    }}>
                        <option value={30}>30d</option>
                        <option value={90}>90d</option>
                        <option value={365}>1yr</option>
                        <option value={3650}>All</option>
                    </select>
                    <button onClick={triggerDeepDive} disabled={generating} style={{
                        fontFamily: MONO, fontSize: '10px', fontWeight: 600, padding: '4px 12px',
                        borderRadius: '4px', background: generating ? 'transparent' : `${colors.accent}15`,
                        border: `1px solid ${generating ? colors.border : colors.accent}40`,
                        color: generating ? colors.textDim : colors.accent,
                        cursor: generating ? 'wait' : 'pointer',
                    }}>{generating ? 'Running...' : 'Run Deep Dive'}</button>
                </div>
            </div>

            {/* Audio Player (sticky) */}
            {playerUrl && (
                <div style={{
                    background: colors.gradientCard, border: `1px solid ${colors.accent}30`,
                    borderRadius: tokens.radius.md, padding: '10px 14px', marginBottom: '12px',
                    display: 'flex', alignItems: 'center', gap: '10px',
                }}>
                    <span style={{ fontFamily: MONO, fontSize: '10px', color: colors.accent,
                        letterSpacing: '1px', flexShrink: 0 }}>NOW PLAYING</span>
                    <audio src={playerUrl} controls autoPlay
                        style={{ flex: 1, height: '32px', filter: 'invert(1) hue-rotate(180deg)', opacity: 0.7 }} />
                    <button onClick={() => setPlayerUrl(null)} style={{
                        fontFamily: MONO, fontSize: '12px', background: 'transparent',
                        border: 'none', color: colors.textDim, cursor: 'pointer',
                    }}>{'\u2715'}</button>
                </div>
            )}

            {/* Tabs */}
            <div style={{
                display: 'flex', gap: '2px', marginBottom: '16px',
                background: colors.bg, borderRadius: '8px', padding: '3px',
                border: `1px solid ${colors.border}`,
            }}>
                {TABS.map(t => (
                    <button key={t.key} onClick={() => setActiveTab(t.key)} style={{
                        fontFamily: MONO, fontSize: '10px', fontWeight: activeTab === t.key ? 700 : 500,
                        padding: '6px 12px', borderRadius: '6px', border: 'none', cursor: 'pointer',
                        background: activeTab === t.key ? `${colors.accent}15` : 'transparent',
                        color: activeTab === t.key ? colors.accent : colors.textMuted,
                        transition: 'all 0.15s',
                    }}>
                        {t.label}
                        {counts[t.key] > 0 && (
                            <span style={{ marginLeft: '4px', fontSize: '9px', opacity: 0.7 }}>
                                ({counts[t.key]})
                            </span>
                        )}
                    </button>
                ))}
            </div>

            {/* Content */}
            {loading ? (
                <div style={{ textAlign: 'center', padding: '40px', fontFamily: MONO,
                    fontSize: '12px', color: colors.textDim }}>Loading archive...</div>
            ) : (
                <div>
                    {activeTab === 'deep_dives' && <DeepDiveList dives={archive?.deep_dives} />}
                    {activeTab === 'audio' && <AudioList briefings={archive?.audio_briefings} onPlay={playAudio} />}
                    {activeTab === 'postmortems' && <PostmortemList postmortems={archive?.postmortems} />}
                    {activeTab === 'diary' && <DiaryList entries={archive?.diary_entries} />}
                    {activeTab === 'theses' && <ThesisList snapshots={archive?.thesis_snapshots} />}
                </div>
            )}
        </div>
    );
}
