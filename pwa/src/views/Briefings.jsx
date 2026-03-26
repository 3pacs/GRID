import React, { useState, useEffect, useMemo } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';
import ViewHelp from '../components/ViewHelp.jsx';

/**
 * Lightweight markdown-to-HTML renderer.
 * Handles: headers (##), bold (**), italic (*), unordered lists (- / *),
 * ordered lists (1.), inline code (`), code blocks (```), and line breaks.
 * No external dependencies required.
 */
function parseMarkdown(text) {
    if (!text) return '';

    const lines = text.split('\n');
    const html = [];
    let inCodeBlock = false;
    let inList = false;
    let listType = null; // 'ul' or 'ol'

    for (let i = 0; i < lines.length; i++) {
        let line = lines[i];

        // Code blocks
        if (line.trim().startsWith('```')) {
            if (inCodeBlock) {
                html.push('</code></pre>');
                inCodeBlock = false;
            } else {
                if (inList) { html.push(listType === 'ol' ? '</ol>' : '</ul>'); inList = false; }
                inCodeBlock = true;
                html.push('<pre style="background:#080C10;border:1px solid #1A2840;border-radius:6px;padding:12px;overflow-x:auto;font-size:12px;color:#8AA0B8"><code>');
            }
            continue;
        }
        if (inCodeBlock) {
            html.push(escapeHtml(line) + '\n');
            continue;
        }

        // Close list if current line is not a list item
        const isUnorderedItem = /^(\s*)([-*])\s+(.+)/.test(line);
        const isOrderedItem = /^(\s*)\d+\.\s+(.+)/.test(line);
        if (inList && !isUnorderedItem && !isOrderedItem) {
            html.push(listType === 'ol' ? '</ol>' : '</ul>');
            inList = false;
            listType = null;
        }

        // Headers
        if (/^#{1,4}\s+/.test(line)) {
            const level = line.match(/^(#+)/)[1].length;
            const content = line.replace(/^#+\s+/, '');
            const sizes = { 1: '18px', 2: '16px', 3: '14px', 4: '13px' };
            const margins = { 1: '20px 0 10px', 2: '16px 0 8px', 3: '14px 0 6px', 4: '12px 0 4px' };
            html.push(`<div style="font-size:${sizes[level]};font-weight:600;color:#E8F0F8;margin:${margins[level]};font-family:IBM Plex Sans,sans-serif">${inlineFormat(content)}</div>`);
            continue;
        }

        // Unordered list items
        if (isUnorderedItem) {
            const match = line.match(/^(\s*)([-*])\s+(.+)/);
            const content = match[3];
            if (!inList || listType !== 'ul') {
                if (inList) html.push(listType === 'ol' ? '</ol>' : '</ul>');
                html.push('<ul style="margin:6px 0;padding-left:20px;color:#8AA0B8;font-size:13px;line-height:1.8">');
                inList = true;
                listType = 'ul';
            }
            html.push(`<li>${inlineFormat(content)}</li>`);
            continue;
        }

        // Ordered list items
        if (isOrderedItem) {
            const match = line.match(/^(\s*)\d+\.\s+(.+)/);
            const content = match[2];
            if (!inList || listType !== 'ol') {
                if (inList) html.push(listType === 'ol' ? '</ol>' : '</ul>');
                html.push('<ol style="margin:6px 0;padding-left:20px;color:#8AA0B8;font-size:13px;line-height:1.8">');
                inList = true;
                listType = 'ol';
            }
            html.push(`<li>${inlineFormat(content)}</li>`);
            continue;
        }

        // Horizontal rule
        if (/^---+$/.test(line.trim())) {
            html.push(`<hr style="border:none;border-top:1px solid #1A2840;margin:12px 0" />`);
            continue;
        }

        // Empty line
        if (line.trim() === '') {
            html.push('<div style="height:8px"></div>');
            continue;
        }

        // Regular paragraph
        html.push(`<p style="margin:4px 0;color:#8AA0B8;font-size:13px;line-height:1.7">${inlineFormat(line)}</p>`);
    }

    if (inList) html.push(listType === 'ol' ? '</ol>' : '</ul>');
    if (inCodeBlock) html.push('</code></pre>');

    return html.join('\n');
}

function escapeHtml(text) {
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

/** Apply inline formatting: bold, italic, inline code. */
function inlineFormat(text) {
    let result = escapeHtml(text);
    // Inline code
    result = result.replace(/`([^`]+)`/g, '<code style="background:#080C10;padding:1px 5px;border-radius:3px;font-size:12px;color:#C8D8E8;font-family:IBM Plex Mono,monospace">$1</code>');
    // Bold
    result = result.replace(/\*\*([^*]+)\*\*/g, '<strong style="color:#C8D8E8;font-weight:600">$1</strong>');
    // Italic
    result = result.replace(/\*([^*]+)\*/g, '<em>$1</em>');
    return result;
}

/**
 * Try to detect structured sections in a briefing.
 * LLM briefings often contain headers like Summary, Key Signals, Risks, Outlook.
 */
const SECTION_LABELS = [
    { key: 'summary', patterns: ['summary', 'overview', 'executive summary', 'tldr', 'tl;dr'] },
    { key: 'signals', patterns: ['key signals', 'signals', 'indicators', 'key indicators', 'key metrics', 'data points'] },
    { key: 'risks', patterns: ['risks', 'risk factors', 'warnings', 'concerns', 'downside', 'caution'] },
    { key: 'outlook', patterns: ['outlook', 'forecast', 'forward look', 'expectations', 'conclusion', 'recommendation'] },
];

const SECTION_ICONS = {
    summary: { icon: 'S', color: colors.accent },
    signals: { icon: 'K', color: colors.green },
    risks: { icon: 'R', color: colors.red },
    outlook: { icon: 'O', color: colors.yellow },
    other: { icon: '#', color: colors.textMuted },
};

function detectSections(content) {
    if (!content) return null;

    const lines = content.split('\n');
    const sections = [];
    let currentSection = { key: 'intro', title: null, lines: [] };

    for (const line of lines) {
        // Check if line is a header (markdown ## or just ALL CAPS label followed by colon)
        const headerMatch = line.match(/^#{1,3}\s+(.+)/);
        const labelMatch = line.match(/^([A-Z][A-Za-z\s/]+):\s*$/);

        if (headerMatch || labelMatch) {
            const title = (headerMatch ? headerMatch[1] : labelMatch[1]).trim();
            const titleLower = title.toLowerCase();

            // Save previous section
            if (currentSection.lines.length > 0 || currentSection.title) {
                sections.push(currentSection);
            }

            // Detect section type
            let sectionKey = 'other';
            for (const s of SECTION_LABELS) {
                if (s.patterns.some(p => titleLower.includes(p))) {
                    sectionKey = s.key;
                    break;
                }
            }

            currentSection = { key: sectionKey, title, lines: [] };
        } else {
            currentSection.lines.push(line);
        }
    }

    if (currentSection.lines.length > 0 || currentSection.title) {
        sections.push(currentSection);
    }

    // If only one section with no title, don't bother with section rendering
    if (sections.length <= 1 && !sections[0]?.title) {
        return null;
    }

    return sections;
}

function formatTimestamp(ts) {
    if (!ts) return null;
    const d = new Date(ts);
    if (isNaN(d.getTime())) return null;

    const now = new Date();
    const diffMs = now - d;
    const diffMin = Math.floor(diffMs / 60000);
    const diffHr = Math.floor(diffMs / 3600000);

    let relative;
    if (diffMin < 1) relative = 'just now';
    else if (diffMin < 60) relative = `${diffMin}m ago`;
    else if (diffHr < 24) relative = `${diffHr}h ago`;
    else relative = `${Math.floor(diffHr / 24)}d ago`;

    const formatted = d.toLocaleDateString('en-US', {
        weekday: 'short', month: 'short', day: 'numeric',
        hour: '2-digit', minute: '2-digit', hour12: false,
    });

    return { formatted, relative };
}

/** Rendered markdown content block. */
function RenderedContent({ content }) {
    const html = useMemo(() => parseMarkdown(content), [content]);
    return (
        <div
            style={localStyles.renderedContent}
            dangerouslySetInnerHTML={{ __html: html }}
        />
    );
}

/** Section block with icon badge and collapsible content. */
function SectionBlock({ section }) {
    const meta = SECTION_ICONS[section.key] || SECTION_ICONS.other;
    const content = section.lines.join('\n').trim();

    return (
        <div style={localStyles.section}>
            <div style={localStyles.sectionHeader}>
                <span style={{
                    ...shared.badge(meta.color),
                    width: '22px', height: '22px', display: 'inline-flex',
                    alignItems: 'center', justifyContent: 'center',
                    borderRadius: '6px', fontSize: '11px', fontWeight: 700,
                    flexShrink: 0,
                }}>
                    {meta.icon}
                </span>
                <span style={localStyles.sectionTitle}>
                    {section.title}
                </span>
            </div>
            {content && <RenderedContent content={content} />}
        </div>
    );
}

export default function Briefings() {
    const [ollamaStatus, setOllamaStatus] = useState(null);
    const [activeTab, setActiveTab] = useState('hourly');
    const [briefing, setBriefing] = useState(null);
    const [briefingList, setBriefingList] = useState([]);
    const [generating, setGenerating] = useState(false);
    const [question, setQuestion] = useState('');
    const [askResult, setAskResult] = useState(null);
    const [asking, setAsking] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        api.getOllamaStatus().then(setOllamaStatus).catch(() => {});
        loadBriefing('hourly');
        loadBriefingList();
    }, []);

    const loadBriefing = async (type) => {
        try {
            const result = await api.getLatestBriefing(type);
            setBriefing(result);
        } catch { setBriefing(null); }
    };

    const loadBriefingList = async () => {
        try {
            const result = await api.listBriefings('', 30);
            setBriefingList(result.briefings || []);
        } catch (e) { console.warn('[GRID] Briefings:', e.message); }
    };

    const generate = async (type) => {
        setGenerating(true);
        setError(null);
        try {
            const result = await api.generateBriefing(type);
            setBriefing(result);
            loadBriefingList();
        } catch (e) {
            setError(e.message || 'Generation failed');
        }
        setGenerating(false);
    };

    const switchTab = (type) => {
        setActiveTab(type);
        loadBriefing(type);
    };

    const readSaved = async (filename) => {
        try {
            const result = await api.readBriefing(filename);
            setBriefing({ content: result.content, type: filename.split('_')[0] });
        } catch (e) { console.warn('[GRID] Briefings:', e.message); }
    };

    const askQuestion = async () => {
        if (!question.trim()) return;
        setAsking(true);
        try {
            const result = await api.askOllama(question);
            setAskResult(result);
        } catch (e) {
            setAskResult({ response: `Error: ${e.message}` });
        }
        setAsking(false);
    };

    const available = ollamaStatus?.available;
    const ts = formatTimestamp(briefing?.timestamp);
    const sections = useMemo(
        () => briefing?.content ? detectSections(briefing.content) : null,
        [briefing?.content]
    );

    return (
        <div style={shared.container}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={shared.header}>Market Briefings</div>
                <ViewHelp id="briefings" />
            </div>

            {/* Ollama Status */}
            <div style={shared.card}>
                <div style={shared.row}>
                    <div>
                        <span style={shared.label}>Ollama</span>
                        <span style={shared.value}>{ollamaStatus?.model || 'unknown'}</span>
                    </div>
                    <span style={shared.badge(available ? '#1A7A4A' : '#8B1F1F')}>
                        {available ? 'ONLINE' : 'OFFLINE'}
                    </span>
                </div>
            </div>

            {/* Generate Briefings */}
            <div style={shared.card}>
                <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap', alignItems: 'center' }}>
                    <span style={{ ...shared.label, marginBottom: 0 }}>Generate:</span>
                    {['hourly', 'daily', 'weekly'].map(type => (
                        <button
                            key={type}
                            style={{
                                ...shared.buttonSmall,
                                ...(generating ? shared.buttonDisabled : {}),
                            }}
                            onClick={() => generate(type)}
                            disabled={generating || !available}
                        >
                            {generating ? 'Generating...' : type.charAt(0).toUpperCase() + type.slice(1)}
                        </button>
                    ))}
                </div>
                {error && <div style={shared.error}>{error}</div>}
            </div>

            {/* Briefing Tabs */}
            <div style={shared.tabs}>
                {['hourly', 'daily', 'weekly'].map(type => (
                    <button
                        key={type}
                        style={shared.tab(activeTab === type)}
                        onClick={() => switchTab(type)}
                    >
                        {type.charAt(0).toUpperCase() + type.slice(1)}
                    </button>
                ))}
            </div>

            {/* Current Briefing */}
            {briefing?.content ? (
                <div style={shared.card}>
                    {/* Briefing header with type badge and timestamp */}
                    <div style={localStyles.briefingHeader}>
                        <span style={shared.badge('#1A2840')}>
                            {briefing.type?.toUpperCase()}
                        </span>
                        {ts && (
                            <div style={localStyles.timestampWrap}>
                                <span style={localStyles.timestampRelative}>{ts.relative}</span>
                                <span style={localStyles.timestampFull}>{ts.formatted}</span>
                            </div>
                        )}
                    </div>

                    {/* Structured sections if detected, otherwise rendered markdown */}
                    {sections ? (
                        <div style={localStyles.sectionsWrap}>
                            {sections.map((sec, i) => (
                                sec.title
                                    ? <SectionBlock key={i} section={sec} />
                                    : <RenderedContent key={i} content={sec.lines.join('\n')} />
                            ))}
                        </div>
                    ) : (
                        <RenderedContent content={briefing.content} />
                    )}
                </div>
            ) : (
                <div style={{
                    ...shared.card, textAlign: 'center', color: colors.textMuted,
                    padding: '40px', fontSize: '13px',
                }}>
                    No {activeTab} briefing available. Generate one above.
                </div>
            )}

            {/* Saved Briefings Archive */}
            <div style={shared.sectionTitle}>Archive</div>
            <div style={shared.card}>
                {briefingList.length === 0 ? (
                    <div style={{ color: colors.textMuted, fontSize: '13px' }}>No saved briefings</div>
                ) : (
                    briefingList.map((b, i) => (
                        <div
                            key={i}
                            style={{ ...shared.row, cursor: 'pointer' }}
                            onClick={() => readSaved(b.filename)}
                        >
                            <div>
                                <span style={shared.badge(
                                    b.type === 'weekly' ? '#5A3A00'
                                        : b.type === 'daily' ? '#1A6EBF'
                                        : '#1A2840'
                                )}>
                                    {b.type?.toUpperCase()}
                                </span>
                                <span style={{ fontSize: '12px', color: colors.textMuted, marginLeft: '10px' }}>
                                    {b.filename}
                                </span>
                            </div>
                            <span style={{ fontSize: '11px', color: colors.textMuted }}>
                                {(b.size_bytes / 1024).toFixed(1)}kb
                            </span>
                        </div>
                    ))
                )}
            </div>

            {/* Ask Ollama */}
            <div style={shared.sectionTitle}>Ask GRID Analyst</div>
            <div style={shared.card}>
                <textarea
                    style={shared.textarea}
                    value={question}
                    onChange={(e) => setQuestion(e.target.value)}
                    placeholder="Ask about market conditions, regime state, feature relationships..."
                />
                <div style={{ marginTop: '8px' }}>
                    <button
                        style={{
                            ...shared.buttonSmall,
                            ...(asking || !available ? shared.buttonDisabled : {}),
                        }}
                        onClick={askQuestion}
                        disabled={asking || !available}
                    >
                        {asking ? 'Thinking...' : 'Ask'}
                    </button>
                </div>
                {askResult?.response && (
                    <div style={localStyles.askResultWrap}>
                        <div style={localStyles.askResultLabel}>GRID Analyst</div>
                        <RenderedContent content={askResult.response} />
                    </div>
                )}
            </div>
        </div>
    );
}

const localStyles = {
    renderedContent: {
        background: colors.bg,
        borderRadius: '8px',
        padding: '16px',
        fontSize: '13px',
        color: colors.textDim,
        lineHeight: '1.7',
        maxHeight: '500px',
        overflowY: 'auto',
        fontFamily: colors.sans,
    },
    briefingHeader: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '12px',
    },
    timestampWrap: {
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'flex-end',
        gap: '2px',
    },
    timestampRelative: {
        fontSize: '12px',
        color: colors.text,
        fontWeight: 600,
        fontFamily: colors.mono,
    },
    timestampFull: {
        fontSize: '11px',
        color: colors.textMuted,
        fontFamily: colors.mono,
    },
    sectionsWrap: {
        display: 'flex',
        flexDirection: 'column',
        gap: '12px',
    },
    section: {
        background: colors.bg,
        borderRadius: '8px',
        padding: '14px',
        border: `1px solid ${colors.border}`,
    },
    sectionHeader: {
        display: 'flex',
        alignItems: 'center',
        gap: '10px',
        marginBottom: '10px',
    },
    sectionTitle: {
        fontSize: '14px',
        fontWeight: 600,
        color: '#E8F0F8',
        fontFamily: colors.sans,
    },
    askResultWrap: {
        marginTop: '12px',
        borderTop: `1px solid ${colors.border}`,
        paddingTop: '12px',
    },
    askResultLabel: {
        fontSize: '11px',
        fontWeight: 600,
        color: colors.accent,
        marginBottom: '8px',
        letterSpacing: '1px',
        textTransform: 'uppercase',
        fontFamily: colors.mono,
    },
};
