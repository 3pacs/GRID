import React from 'react';
import { RotateCcw, Sparkles } from 'lucide-react';
import { tokens } from '../styles/tokens.js';

const styles = {
    card: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: tokens.spacing.md,
        padding: '14px 16px',
        borderRadius: tokens.radius.md,
        border: `1px solid ${tokens.cardBorder}`,
        background: 'linear-gradient(135deg, rgba(124, 58, 237, 0.22) 0%, rgba(10, 18, 35, 0.8) 100%)',
        marginBottom: tokens.spacing.md,
    },
    left: {
        display: 'flex',
        alignItems: 'center',
        gap: tokens.spacing.md,
    },
    title: {
        fontSize: '12px',
        color: tokens.textBright,
        fontWeight: 700,
        letterSpacing: '1.2px',
        textTransform: 'uppercase',
        fontFamily: tokens.fontMono,
    },
    subtitle: {
        fontSize: '13px',
        color: tokens.text,
        marginTop: '4px',
    },
    status: {
        fontSize: '12px',
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
    },
};

export default function RetrogradeBanner({ retrogrades = [], summary = 'All tracked bodies direct' }) {
    const active = retrogrades.length > 0;

    return (
        <div style={styles.card}>
            <div style={styles.left}>
                {active ? <RotateCcw size={18} color={tokens.red} /> : <Sparkles size={18} color={tokens.accent} />}
                <div>
                    <div style={styles.title}>{active ? 'Retrograde Watch' : 'Orbital Flow'}</div>
                    <div style={styles.subtitle}>
                        {active ? retrogrades.join(', ') : summary}
                    </div>
                </div>
            </div>
            <div style={styles.status}>
                {active ? `${retrogrades.length} active` : 'Direct'}
            </div>
        </div>
    );
}
