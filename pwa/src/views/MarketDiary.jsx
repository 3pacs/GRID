import React, { useState, useEffect, useMemo, useCallback } from 'react';
import DOMPurify from 'dompurify';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';
import ViewHelp from '../components/ViewHelp.jsx';
import { formatMonthYear, formatLongDate, formatDateTime } from '../utils/formatTime.js';

/* ── Markdown renderer ──────────────────────────────────────── */

/**
 * Sanitize HTML produced by the markdown renderer before injecting via
 * dangerouslySetInnerHTML. Uses DOMPurify which handles the full range of
 * XSS vectors (script tags, event handlers, javascript: URIs, SVG payloads,
 * data: URIs, etc.) that a hand-rolled regex cannot reliably cover.
 *
 * Diary content is LLM-generated (untrusted) and must always pass through
 * this function before being set as innerHTML.
 */
function sanitizeHtml(html) {
    return DOMPurify.sanitize(html, {
        // Allow the inline styles the markdown renderer produces but nothing else.
        ALLOWED_ATTR: ['style'],
        // No external resources — prevents data-exfiltration via img/iframe src.
        FORBID_TAGS: ['script', 'iframe', 'object', 'embed', 'form', 'input'],
    });
}

function escapeHtml(text) {
    return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function inlineFormat(text) {
    let r = escapeHtml(text);
    r = r.replace(/`([^`]+)`/g, '<code style="background:#080C10;padding:1px 5px;border-radius:3px;font-size:12px;color:#C8D8E8;font-family:IBM Plex Mono,monospace">$1</code>');
    r = r.replace(/\*\*([^*]+)\*\*/g, '<strong style="color:#C8D8E8;font-weight:600">$1</strong>');
    r = r.replace(/\*([^*]+)\*/g, '<em>$1</em>');
    return r;
}

function parseMarkdown(text) {
    if (!text) return '';
    const lines = text.split('\n');
    const html = [];
    let inCode = false, inList = false, listType = null;

    for (const line of lines) {
        if (line.trim().startsWith('```')) {
            if (inCode) { html.push('</code></pre>'); inCode = false; }
            else {
                if (inList) { html.push(listType === 'ol' ? '</ol>' : '</ul>'); inList = false; }
                inCode = true;
                html.push('<pre style="background:#080C10;border:1px solid #1A2840;border-radius:6px;padding:12px;overflow-x:auto;font-size:12px;color:#8AA0B8"><code>');
            }
            continue;
        }
        if (inCode) { html.push(escapeHtml(line) + '\n'); continue; }

        const isUl = /^(\s*)([-*])\s+(.+)/.test(line);
        const isOl = /^(\s*)\d+\.\s+(.+)/.test(line);
        if (inList && !isUl && !isOl) { html.push(listType === 'ol' ? '</ol>' : '</ul>'); inList = false; listType = null; }

        if (/^#{1,4}\s+/.test(line)) {
            const level = line.match(/^(#+)/)[1].length;
            const content = line.replace(/^#+\s+/, '');
            const sizes = { 1: '18px', 2: '16px', 3: '14px', 4: '13px' };
            html.push(`<div style="font-size:${sizes[level]};font-weight:600;color:#E8F0F8;margin:${level <= 2 ? '20px 0 10px' : '14px 0 6px'};font-family:IBM Plex Sans,sans-serif">${inlineFormat(content)}</div>`);
            continue;
        }
        if (isUl) {
            const m = line.match(/^(\s*)([-*])\s+(.+)/);
            if (!inList || listType !== 'ul') { if (inList) html.push('</ul>'); html.push('<ul style="margin:6px 0;padding-left:20px;color:#8AA0B8;font-size:13px;line-height:1.8">'); inList = true; listType = 'ul'; }
            html.push(`<li>${inlineFormat(m[3])}</li>`); continue;
        }
        if (isOl) {
            const m = line.match(/^(\s*)\d+\.\s+(.+)/);
            if (!inList || listType !== 'ol') { if (inList) html.push('</ol>'); html.push('<ol style="margin:6px 0;padding-left:20px;color:#8AA0B8;font-size:13px;line-height:1.8">'); inList = true; listType = 'ol'; }
            html.push(`<li>${inlineFormat(m[2])}</li>`); continue;
        }
        if (/^\|/.test(line.trim())) {
            // Table row
            if (/^\|[\s-:|]+\|$/.test(line.trim())) continue; // separator
            const cells = line.trim().split('|').filter(c => c.trim() !== '');
            const isHeader = html.length > 0 && !html[html.length - 1].includes('<td');
            const tag = isHeader ? 'th' : 'td';
            const cellStyle = tag === 'th'
                ? 'padding:6px 10px;text-align:left;font-size:11px;font-weight:600;color:#8AA0B8;border-bottom:1px solid #1A2840;font-family:IBM Plex Mono,monospace'
                : 'padding:6px 10px;font-size:12px;color:#C8D8E8;border-bottom:1px solid #111B2A;font-family:IBM Plex Mono,monospace';
            html.push(`<tr>${cells.map(c => `<${tag} style="${cellStyle}">${inlineFormat(c.trim())}</${tag}>`).join('')}</tr>`);
            continue;
        }
        if (/^---+$/.test(line.trim())) { html.push('<hr style="border:none;border-top:1px solid #1A2840;margin:12px 0" />'); continue; }
        if (line.trim() === '') { html.push('<div style="height:8px"></div>'); continue; }
        html.push(`<p style="margin:4px 0;color:#8AA0B8;font-size:13px;line-height:1.7">${inlineFormat(line)}</p>`);
    }
    if (inList) html.push(listType === 'ol' ? '</ol>' : '</ul>');
    if (inCode) html.push('</code></pre>');
    return html.join('\n');
}

/* ── Verdict badge ─────────────────────────────────────────── */

const verdictConfig = {
    correct: { label: 'CORRECT', color: colors.green, bg: colors.greenBg },
    wrong:   { label: 'WRONG',   color: colors.red,   bg: colors.redBg },
    partial: { label: 'PARTIAL', color: colors.yellow, bg: colors.yellowBg },
    unknown: { label: 'N/A',     color: colors.textMuted, bg: '#1A2840' },
};

function VerdictBadge({ verdict }) {
    const cfg = verdictConfig[verdict] || verdictConfig.unknown;
    return (
        <span style={{
            display: 'inline-block',
            padding: '2px 8px',
            borderRadius: '4px',
            fontSize: '10px',
            fontWeight: 700,
            letterSpacing: '0.5px',
            fontFamily: "'IBM Plex Mono', monospace",
            color: cfg.color,
            background: cfg.bg,
            border: `1px solid ${cfg.color}30`,
        }}>
            {cfg.label}
        </span>
    );
}

/* ── Calendar mini-selector ────────────────────────────────── */

function CalendarGrid({ entries, selectedDate, onSelect }) {
    // Build a map of date -> entry for quick lookup
    const entryMap = useMemo(() => {
        const m = {};
        (entries || []).forEach(e => { m[e.date] = e; });
        return m;
    }, [entries]);

    const [viewMonth, setViewMonth] = useState(() => {
        if (selectedDate) return new Date(selectedDate + 'T00:00:00');
        return new Date();
    });

    const year = viewMonth.getFullYear();
    const month = viewMonth.getMonth();
    const firstDay = new Date(year, month, 1).getDay();
    const daysInMonth = new Date(year, month + 1, 0).getDate();

    const dayLabels = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'];
    const cells = [];
    for (let i = 0; i < firstDay; i++) cells.push(null);
    for (let d = 1; d <= daysInMonth; d++) cells.push(d);

    const prevMonth = () => setViewMonth(new Date(year, month - 1, 1));
    const nextMonth = () => setViewMonth(new Date(year, month + 1, 1));
    const monthLabel = formatMonthYear(viewMonth);

    return (
        <div style={{ background: colors.card, borderRadius: '8px', border: `1px solid ${colors.border}`, padding: '12px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
                <button onClick={prevMonth} style={calNavBtn}>&lt;</button>
                <span style={{ fontSize: '13px', fontWeight: 600, color: colors.text, fontFamily: "'IBM Plex Sans', sans-serif" }}>{monthLabel}</span>
                <button onClick={nextMonth} style={calNavBtn}>&gt;</button>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: '2px' }}>
                {dayLabels.map(d => (
                    <div key={d} style={{ textAlign: 'center', fontSize: '10px', color: colors.textMuted, fontFamily: "'IBM Plex Mono', monospace", padding: '4px 0' }}>
                        {d}
                    </div>
                ))}
                {cells.map((day, i) => {
                    if (!day) return <div key={`empty-${i}`} />;
                    const dateStr = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
                    const entry = entryMap[dateStr];
                    const isSelected = dateStr === selectedDate;
                    const hasEntry = !!entry;
                    const dotColor = entry ? (verdictConfig[entry.verdict] || verdictConfig.unknown).color : null;

                    return (
                        <button
                            key={dateStr}
                            onClick={() => hasEntry && onSelect(dateStr)}
                            style={{
                                background: isSelected ? `${colors.accent}20` : 'none',
                                border: isSelected ? `1px solid ${colors.accent}` : '1px solid transparent',
                                borderRadius: '4px',
                                padding: '4px 0',
                                cursor: hasEntry ? 'pointer' : 'default',
                                opacity: hasEntry ? 1 : 0.3,
                                display: 'flex',
                                flexDirection: 'column',
                                alignItems: 'center',
                                gap: '2px',
                                minHeight: '32px',
                            }}
                        >
                            <span style={{ fontSize: '12px', color: isSelected ? colors.accent : colors.text, fontFamily: "'IBM Plex Mono', monospace" }}>
                                {day}
                            </span>
                            {dotColor && (
                                <div style={{ width: '5px', height: '5px', borderRadius: '50%', background: dotColor }} />
                            )}
                        </button>
                    );
                })}
            </div>
            <div style={{ display: 'flex', gap: '12px', marginTop: '10px', justifyContent: 'center' }}>
                {Object.entries(verdictConfig).filter(([k]) => k !== 'unknown').map(([k, v]) => (
                    <div key={k} style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                        <div style={{ width: '6px', height: '6px', borderRadius: '50%', background: v.color }} />
                        <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'IBM Plex Mono', monospace" }}>{v.label}</span>
                    </div>
                ))}
            </div>
        </div>
    );
}

const calNavBtn = {
    background: 'none',
    border: `1px solid ${colors.border}`,
    borderRadius: '4px',
    color: colors.textDim,
    cursor: 'pointer',
    padding: '4px 10px',
    fontSize: '14px',
    fontFamily: "'IBM Plex Mono', monospace",
};

/* ── Styles ────────────────────────────────────────────────── */

const s = {
    container: {
        maxWidth: '1200px',
        margin: '0 auto',
        padding: '20px 16px',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    header: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        flexWrap: 'wrap',
        gap: '12px',
        marginBottom: '20px',
    },
    title: {
        fontSize: '20px',
        fontWeight: 700,
        color: colors.text,
        fontFamily: "'IBM Plex Mono', monospace",
        letterSpacing: '1px',
    },
    searchRow: {
        display: 'flex',
        gap: '8px',
        alignItems: 'center',
    },
    searchInput: {
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: '6px',
        padding: '8px 12px',
        color: colors.text,
        fontSize: '13px',
        fontFamily: "'IBM Plex Sans', sans-serif",
        outline: 'none',
        width: '200px',
    },
    searchBtn: {
        background: colors.accent,
        border: 'none',
        borderRadius: '6px',
        padding: '8px 14px',
        color: '#fff',
        fontSize: '12px',
        fontWeight: 600,
        cursor: 'pointer',
        fontFamily: "'IBM Plex Mono', monospace",
    },
    layout: {
        display: 'grid',
        gridTemplateColumns: '260px 1fr',
        gap: '20px',
    },
    layoutMobile: {
        display: 'flex',
        flexDirection: 'column',
        gap: '16px',
    },
    sidebar: {},
    entryCard: {
        background: colors.card,
        borderRadius: '10px',
        border: `1px solid ${colors.border}`,
        padding: '24px',
        minHeight: '400px',
    },
    entryHeader: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '16px',
        paddingBottom: '12px',
        borderBottom: `1px solid ${colors.border}`,
    },
    entryDate: {
        fontSize: '16px',
        fontWeight: 600,
        color: colors.text,
        fontFamily: "'IBM Plex Mono', monospace",
    },
    statRow: {
        display: 'flex',
        gap: '16px',
        alignItems: 'center',
        flexWrap: 'wrap',
    },
    statItem: {
        display: 'flex',
        alignItems: 'center',
        gap: '6px',
        fontSize: '12px',
        fontFamily: "'IBM Plex Mono', monospace",
    },
    entryBody: {
        lineHeight: '1.7',
    },
    listItem: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '8px 10px',
        borderRadius: '6px',
        cursor: 'pointer',
        transition: 'background 0.15s',
        borderLeft: '3px solid transparent',
    },
    empty: {
        textAlign: 'center',
        padding: '60px 20px',
        color: colors.textMuted,
        fontSize: '14px',
    },
    generateBtn: {
        background: 'none',
        border: `1px solid ${colors.accent}`,
        borderRadius: '6px',
        padding: '6px 14px',
        color: colors.accent,
        fontSize: '11px',
        fontWeight: 600,
        cursor: 'pointer',
        fontFamily: "'IBM Plex Mono', monospace",
    },
};

/* ── Main Component ────────────────────────────────────────── */

export default function MarketDiary() {
    const [entries, setEntries] = useState([]);
    const [selectedDate, setSelectedDate] = useState(null);
    const [currentEntry, setCurrentEntry] = useState(null);
    const [loading, setLoading] = useState(true);
    const [entryLoading, setEntryLoading] = useState(false);
    const [searchTerm, setSearchTerm] = useState('');
    const [searchResults, setSearchResults] = useState(null);
    const [generating, setGenerating] = useState(false);
    const [isMobile, setIsMobile] = useState(window.innerWidth < 768);

    useEffect(() => {
        const h = () => setIsMobile(window.innerWidth < 768);
        window.addEventListener('resize', h);
        return () => window.removeEventListener('resize', h);
    }, []);

    // Load entry list
    useEffect(() => {
        setLoading(true);
        api._fetch('/api/v1/intelligence/diary/list?limit=365')
            .then(data => {
                setEntries(data.entries || []);
                // Auto-select the most recent entry
                if (data.entries?.length > 0 && !selectedDate) {
                    setSelectedDate(data.entries[0].date);
                }
            })
            .catch(() => setEntries([]))
            .finally(() => setLoading(false));
    }, []);

    // Load selected entry
    useEffect(() => {
        if (!selectedDate) return;
        setEntryLoading(true);
        api._fetch(`/api/v1/intelligence/diary?date=${selectedDate}`)
            .then(data => {
                if (!data.error) setCurrentEntry(data);
                else setCurrentEntry(null);
            })
            .catch(() => setCurrentEntry(null))
            .finally(() => setEntryLoading(false));
    }, [selectedDate]);

    const handleSearch = useCallback(() => {
        if (!searchTerm.trim()) { setSearchResults(null); return; }
        api._fetch(`/api/v1/intelligence/diary/search?q=${encodeURIComponent(searchTerm)}`)
            .then(data => setSearchResults(data.results || []))
            .catch(() => setSearchResults([]));
    }, [searchTerm]);

    const handleGenerate = useCallback(() => {
        setGenerating(true);
        api._fetch('/api/v1/intelligence/diary/generate', { method: 'POST' })
            .then(data => {
                if (data.date) {
                    setSelectedDate(data.date);
                    // Refresh entry list
                    api._fetch('/api/v1/intelligence/diary/list?limit=365')
                        .then(d => setEntries(d.entries || []));
                }
            })
            .finally(() => setGenerating(false));
    }, []);

    const handleKeyDown = (e) => {
        if (e.key === 'Enter') handleSearch();
    };

    const accuracy = currentEntry?.thesis_accuracy || {};
    const moves = currentEntry?.market_moves || {};

    const renderedContent = useMemo(
        () => currentEntry?.content ? sanitizeHtml(parseMarkdown(currentEntry.content)) : '',
        [currentEntry?.content]
    );

    // Search result entries for sidebar
    const displayEntries = searchResults !== null ? searchResults : entries;

    const helpContent = [
        'Automated daily market diary generated after US market close (10 PM UTC).',
        'Each entry includes LLM narrative, thesis accuracy scoring, and data appendix.',
        'Calendar dots show thesis accuracy: green = correct, red = wrong, yellow = partial.',
        'Use search to find entries by keyword across all diary content.',
        'Click "Generate Today" to manually trigger the current day\'s diary.',
    ];

    if (loading) {
        return (
            <div style={s.container}>
                <div style={s.empty}>Loading market diary...</div>
            </div>
        );
    }

    const sidebar = (
        <div style={s.sidebar}>
            <CalendarGrid
                entries={entries}
                selectedDate={selectedDate}
                onSelect={setSelectedDate}
            />
            {searchResults !== null && (
                <div style={{ marginTop: '12px' }}>
                    <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: "'IBM Plex Mono', monospace", marginBottom: '6px' }}>
                        {searchResults.length} result{searchResults.length !== 1 ? 's' : ''} for "{searchTerm}"
                    </div>
                    {searchResults.map(r => (
                        <div
                            key={r.date}
                            onClick={() => { setSelectedDate(r.date); setSearchResults(null); }}
                            style={{
                                ...s.listItem,
                                background: r.date === selectedDate ? `${colors.accent}10` : 'none',
                                borderLeftColor: r.date === selectedDate ? colors.accent : 'transparent',
                            }}
                        >
                            <span style={{ fontSize: '12px', color: colors.text, fontFamily: "'IBM Plex Mono', monospace" }}>{r.date}</span>
                            <VerdictBadge verdict={r.verdict} />
                        </div>
                    ))}
                </div>
            )}
        </div>
    );

    const mainContent = (
        <div style={s.entryCard}>
            {entryLoading ? (
                <div style={s.empty}>Loading entry...</div>
            ) : currentEntry ? (
                <>
                    <div style={s.entryHeader}>
                        <div>
                            <div style={s.entryDate}>
                                {formatLongDate(currentEntry.date + 'T00:00:00')}
                            </div>
                            <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '4px', fontFamily: "'IBM Plex Mono', monospace" }}>
                                Generated {currentEntry.generated_at ? formatDateTime(currentEntry.generated_at) : 'N/A'}
                            </div>
                        </div>
                        <div style={s.statRow}>
                            <div style={s.statItem}>
                                <span style={{ color: colors.textMuted }}>Thesis:</span>
                                <VerdictBadge verdict={accuracy.verdict} />
                            </div>
                            {accuracy.sp500_return_pct !== undefined && accuracy.sp500_return_pct !== null && (
                                <div style={s.statItem}>
                                    <span style={{ color: colors.textMuted }}>S&P:</span>
                                    <span style={{
                                        color: accuracy.sp500_return_pct >= 0 ? colors.green : colors.red,
                                        fontWeight: 600,
                                    }}>
                                        {accuracy.sp500_return_pct >= 0 ? '+' : ''}{accuracy.sp500_return_pct}%
                                    </span>
                                </div>
                            )}
                            {accuracy.morning_thesis && (
                                <div style={s.statItem}>
                                    <span style={{ color: colors.textMuted }}>Call:</span>
                                    <span style={{ color: colors.text }}>{accuracy.morning_thesis}</span>
                                </div>
                            )}
                        </div>
                    </div>
                    <div
                        style={s.entryBody}
                        dangerouslySetInnerHTML={{ __html: renderedContent }}
                    />
                    {/* Wrap tables */}
                    <style>{`
                        .market-diary-entry table {
                            width: 100%;
                            border-collapse: collapse;
                            margin: 10px 0;
                        }
                    `}</style>
                </>
            ) : (
                <div style={s.empty}>
                    {selectedDate
                        ? `No diary entry for ${selectedDate}`
                        : 'Select a date from the calendar to view the diary entry.'
                    }
                </div>
            )}
        </div>
    );

    return (
        <div style={s.container}>
            <div style={s.header}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                    <h1 style={s.title}>MARKET DIARY</h1>
                    <ViewHelp items={helpContent} />
                </div>
                <div style={s.searchRow}>
                    <input
                        style={s.searchInput}
                        placeholder="Search entries..."
                        value={searchTerm}
                        onChange={e => setSearchTerm(e.target.value)}
                        onKeyDown={handleKeyDown}
                    />
                    <button style={s.searchBtn} onClick={handleSearch}>SEARCH</button>
                    {searchResults !== null && (
                        <button
                            style={{ ...s.searchBtn, background: colors.border }}
                            onClick={() => { setSearchResults(null); setSearchTerm(''); }}
                        >CLEAR</button>
                    )}
                    <button
                        style={s.generateBtn}
                        onClick={handleGenerate}
                        disabled={generating}
                    >
                        {generating ? 'GENERATING...' : 'GENERATE TODAY'}
                    </button>
                </div>
            </div>
            <div style={isMobile ? s.layoutMobile : s.layout}>
                {sidebar}
                {mainContent}
            </div>
        </div>
    );
}
