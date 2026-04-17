import React, { useState } from 'react';
import { Search } from 'lucide-react';
import IntelligenceSearch from '../components/IntelligenceSearch.jsx';
import { colors, tokens } from '../styles/shared.js';

const styles = {
    page: {
        minHeight: 'calc(100vh - 64px)',
        position: 'relative',
        background: colors.bg,
        color: colors.text,
        overflow: 'hidden',
    },
    aside: {
        marginLeft: 320,
        padding: '28px',
        maxWidth: 720,
    },
    eyebrow: {
        display: 'flex',
        alignItems: 'center',
        gap: 8,
        color: colors.accent,
        fontSize: 11,
        fontFamily: "'IBM Plex Mono', monospace",
        letterSpacing: '1px',
        fontWeight: 700,
        textTransform: 'uppercase',
        marginBottom: 12,
    },
    title: {
        margin: 0,
        fontSize: 30,
        lineHeight: 1.15,
        color: colors.text,
    },
    body: {
        marginTop: 12,
        color: colors.textMuted,
        fontSize: 14,
        lineHeight: 1.6,
    },
    staged: {
        marginTop: 24,
        padding: 14,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.sm,
        background: colors.card,
    },
    stagedTitle: {
        color: colors.textDim,
        fontFamily: "'IBM Plex Mono', monospace",
        fontSize: 10,
        letterSpacing: '1px',
        fontWeight: 700,
        marginBottom: 10,
    },
    stagedItem: {
        padding: '8px 0',
        borderTop: `1px solid ${colors.border}`,
        color: colors.text,
        fontSize: 13,
    },
};

export default function IntelligenceSearchView({ onNavigate }) {
    const [staged, setStaged] = useState([]);

    return (
        <div style={styles.page}>
            <IntelligenceSearch
                onClose={() => onNavigate?.('canvas')}
                onAddToCanvas={(node) => setStaged(prev => [node, ...prev].slice(0, 8))}
            />
            <div style={styles.aside}>
                <div style={styles.eyebrow}>
                    <Search size={14} />
                    Intel Search
                </div>
                <h1 style={styles.title}>Search actors, signals, hypotheses, and snapshots.</h1>
                <div style={styles.body}>
                    Add promising results to a working set here, then open Canvas when you are ready to map the connections.
                </div>

                {staged.length > 0 && (
                    <div style={styles.staged}>
                        <div style={styles.stagedTitle}>WORKING SET</div>
                        {staged.map((node, idx) => (
                            <div key={`${node.type}-${node.id}-${idx}`} style={styles.stagedItem}>
                                {node.label}
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}
