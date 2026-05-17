/**
 * LessonsWidget — async-fetched post-mortem lessons learned.
 *
 * Background: on 2026-05-16 the cold dashboard payload dropped the LLM
 * `generate_lessons_learned` synthesis (saved 8.5s on cold load). The lessons
 * are now exposed via the async `/api/v1/intelligence/postmortem-lessons`
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
 * Normalize the endpoint payload into an array of lesson strings.
 * Backend may return `{lessons: [...]}` (preferred) or `{lessons: "string"}`
 * (legacy LLM blob) — we tolerate both.
 */
function normalizeLessons(payload) {
    if (!payload || payload.error) return [];
    const raw = payload.lessons;
    if (Array.isArray(raw)) return raw.filter(Boolean);
    if (typeof raw === 'string' && raw.trim()) {
        // Split LLM blob into bullets — keep behaviour permissive.
        return raw
            .split(/\n+/)
            .map(line => line.replace(/^\s*[-*•]\s*/, '').trim())
            .filter(Boolean);
    }
    return [];
}

export default function LessonsWidget({ n = DEFAULT_N, days = DEFAULT_DAYS, isMobile = false }) {
    const [lessons, setLessons] = useState([]);
    const [generatedAt, setGeneratedAt] = useState(null);
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
                setLessons(normalizeLessons(result));
                setGeneratedAt(result?.generated_at || null);
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

    const showEmpty = !loading && !refreshing && !error && lessons.length === 0;
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
                        <span style={{ fontFamily: MONO, fontSize: '10px', color: colors.textDim }}>
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
