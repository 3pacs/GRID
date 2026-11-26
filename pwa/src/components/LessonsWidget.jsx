/**
 * LessonsWidget — async-fetched post-mortem lessons learned.
 *
 * Background: on 2026-05-16 the cold dashboard payload dropped the LLM
 * `generate_lessons_learned` synthesis (saved 8.5s on cold load). The lessons
 * are now exposed via the async `/api/v1/postmortem-lessons`
 * endpoint (task #63, 3-tier cached). This widget fetches them after the
 * main dashboard payload arrives, shows a loading state, and exposes a
 * Refresh button that forces regeneration (~30s on cold).
 *
 * Graceful degradation: if the endpoint is unavailable (e.g. backend
 * deployment lag, 404, 401), the widget renders an "unavailable" state
 * instead of crashing the dashboard.
 */
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { RefreshCw, AlertTriangle, BookOpen } from 'lucide-react';
import { api } from '../api.js';
import { colors, tokens } from '../styles/shared.js';

const MONO = "'JetBrains Mono', 'IBM Plex Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

const DEFAULT_N = 5;
const DEFAULT_DAYS = 30;

function timeAgo(ts) {
    if (!ts) return '';
    const diff = Date.now() - new Date(ts).getTime();
    if (isNaN(diff)) return '';
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return 'just now';
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    return `${Math.floor(hrs / 24)}d ago`;
}

/**
 * Normalize the endpoint payload into `{items, report}`.
 * The backend sends `lessons` as one of: an array of strings (older rows), a
 * string (legacy text blob), or an object `{text, count, label}` (the shape the
 * endpoint writes and returns today, with `count` the number of post-mortems
 * summarized; `{lessons: [...]}` inside the object is also tolerated).
 * `items` are separate lessons to list; `report` is a multi-line text report
 * (ruler lines, "-- Section --" headers, indented rows) that is shown whole, as
 * written: splitting it into bullets and capping the list at `n` showed only
 * its header lines. An object with `count: 0` is an empty window, not a lesson.
 */
function normalizeLessons(payload) {
    const none = { items: [], report: null };
    if (!payload || payload.error) return none;
    const raw = payload.lessons;
    if (Array.isArray(raw)) return { items: raw.filter(Boolean), report: null };
    if (typeof raw === 'string' && raw.trim()) return { items: [], report: raw.trimEnd() };
    if (raw && typeof raw === 'object') {
        if (Array.isArray(raw.lessons)) return { items: raw.lessons.filter(Boolean), report: null };
        if (raw.count === 0) return none;
        if (typeof raw.text === 'string' && raw.text.trim()) return { items: [], report: raw.text.trimEnd() };
    }
    return none;
}

export default function LessonsWidget({ n = DEFAULT_N, days = DEFAULT_DAYS, isMobile = false }) {
    const [lessons, setLessons] = useState([]);
    const [report, setReport] = useState(null);
    const [generatedAt, setGeneratedAt] = useState(null);
    const [label, setLabel] = useState(null);
    const [note, setNote] = useState(null);
    const [loading, setLoading] = useState(false);
    const [refreshing, setRefreshing] = useState(false);
    const [error, setError] = useState(null);
    const mountedRef = useRef(true);

    const fetchLessons = useCallback(async (force = false) => {
        if (force) {
            setRefreshing(true);
        } else {
            setLoading(true);
        }
        setError(null);
        try {
            const result = await api.getPostmortemLessons(n, days, force);
            if (!mountedRef.current) return;
            if (result?.error) {
                // Show a friendly message instead of crashing.
                setError(result.message || `Lessons unavailable (status ${result.status || '?'})`);
            } else {
                const parsed = normalizeLessons(result);
                setLessons(parsed.items);
                setReport(parsed.report);
                setGeneratedAt(result?.generated_at || null);
                setLabel(result?.label || null);
                setNote(result?.note || null);
            }
        } catch (err) {
            if (mountedRef.current) {
                setError(err?.message || 'Lessons unavailable');
            }
        } finally {
            if (mountedRef.current) {
                setLoading(false);
                setRefreshing(false);
            }
        }
    }, [n, days]);

    useEffect(() => {
        mountedRef.current = true;
        fetchLessons(false);
        return () => { mountedRef.current = false; };
    }, [fetchLessons]);

    const pad = isMobile ? '14px' : '18px';
    const card = {
        background: colors.gradientCard,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md,
        padding: pad,
    };

    const handleRefresh = () => {
        if (refreshing || loading) return;
        fetchLessons(true);
    };

    const showEmpty = !loading && !refreshing && !error && lessons.length === 0 && !report;
    const isBusy = loading || refreshing;
    const labelText = refreshing ? 'Regenerating... (~30s)' : 'Loading lessons...';

    return (
        <div style={card} data-testid="lessons-widget">
            {/* Header */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                marginBottom: '12px', gap: '8px', flexWrap: 'wrap',
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <BookOpen size={12} color={colors.textMuted} />
                    <span style={{
                        fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                        letterSpacing: '1.5px', color: colors.textMuted,
                    }}>LESSONS LEARNED</span>
                    {generatedAt && (
                        <span style={{ fontFamily: MONO, fontSize: '10px', color: colors.textDim }}
                            title={label || undefined}>
                            {timeAgo(generatedAt)}
                        </span>
                    )}
                </div>
                <button
                    onClick={handleRefresh}
                    disabled={isBusy}
                    aria-label="Refresh lessons"
                    data-testid="lessons-refresh"
                    style={{
                        display: 'inline-flex', alignItems: 'center', gap: '5px',
                        fontFamily: MONO, fontSize: '10px', fontWeight: 600,
                        padding: '3px 10px', borderRadius: '4px',
                        background: isBusy ? 'transparent' : `${colors.accent}15`,
                        border: `1px solid ${isBusy ? colors.border : colors.accent}40`,
                        color: isBusy ? colors.textDim : colors.accent,
                        cursor: isBusy ? 'wait' : 'pointer',
                    }}
                >
                    <RefreshCw
                        size={10}
                        style={refreshing ? { animation: 'spin 1s linear infinite' } : undefined}
                    />
                    {refreshing ? 'Regenerating' : 'Refresh'}
                </button>
            </div>

            {label && !error && !isBusy && (
                <div style={{ fontFamily: MONO, fontSize: '10px', color: colors.textDim,
                    marginBottom: '8px', fontStyle: 'italic' }}>
                    {label}
                </div>
            )}

            {/* Body */}
            {error ? (
                <div
                    role="status"
                    aria-live="polite"
                    style={{
                        display: 'flex', alignItems: 'flex-start', gap: '8px',
                        padding: '8px 10px', borderRadius: tokens.radius.sm,
                        border: `1px solid ${colors.border}`,
                        background: 'transparent', color: colors.textDim,
                        fontFamily: SANS, fontSize: '12px', lineHeight: 1.5,
                    }}
                >
                    <AlertTriangle size={13} color={colors.yellow} style={{ flexShrink: 0, marginTop: '2px' }} />
                    <span style={{ minWidth: 0, flex: 1 }}>
                        Lessons unavailable. {error}
                    </span>
                </div>
            ) : isBusy ? (
                <div style={{
                    fontFamily: SANS, fontSize: '12px', color: colors.textDim, fontStyle: 'italic',
                    padding: '8px 0',
                }}>
                    {labelText}
                </div>
            ) : showEmpty ? (
                <div style={{
                    fontFamily: SANS, fontSize: '12px', color: colors.textDim, fontStyle: 'italic',
                    padding: '4px 0',
                }}>
                    No post-mortem lessons in the last {days} days.
                </div>
            ) : report ? (
                <pre data-testid="lessons-report" style={{
                    margin: 0, padding: '8px 10px', borderRadius: tokens.radius.sm,
                    background: `${colors.accent}06`, borderLeft: `2px solid ${colors.accent}50`,
                    fontFamily: MONO, fontSize: '11px', lineHeight: 1.5, color: colors.text,
                    whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', maxHeight: '360px', overflowY: 'auto',
                }}>{report}</pre>
            ) : (
                <ul style={{
                    margin: 0, padding: 0, listStyle: 'none',
                    display: 'flex', flexDirection: 'column', gap: '8px',
                }}>
                    {lessons.slice(0, n).map((lesson, i) => (
                        <li
                            key={i}
                            style={{
                                display: 'flex', alignItems: 'flex-start', gap: '8px',
                                padding: '6px 8px', borderRadius: '4px',
                                background: `${colors.accent}06`,
                                borderLeft: `2px solid ${colors.accent}50`,
                            }}
                        >
                            <span style={{
                                fontFamily: MONO, fontSize: '10px', color: colors.accent,
                                flexShrink: 0, marginTop: '1px',
                            }}>
                                {String(i + 1).padStart(2, '0')}
                            </span>
                            <span style={{
                                fontFamily: SANS, fontSize: '13px', lineHeight: 1.5,
                                color: colors.text, minWidth: 0, flex: 1,
                            }}>
                                {lesson}
                            </span>
                        </li>
                    ))}
                </ul>
            )}
            <style>{`@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }`}</style>
        </div>
    );
}
