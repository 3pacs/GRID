import React from 'react';
import { shared, colors, tokens } from '../styles/shared.js';

export default function RiskView() {
    return (
        <div style={shared.container}>
            <div style={shared.header}>Risk</div>
            <div style={{
                ...shared.cardGradient,
                display: 'flex', flexDirection: 'column',
                alignItems: 'center', justifyContent: 'center',
                minHeight: '400px', gap: tokens.space.lg,
            }}>
                <div style={{
                    fontSize: '64px', lineHeight: 1,
                    filter: 'grayscale(0.3)',
                }}>
                    ⚠️
                </div>
                <div style={{
                    fontSize: tokens.fontSize.xl, fontWeight: 600,
                    color: '#E8F0F8', fontFamily: colors.sans,
                }}>
                    Risk Treemap
                </div>
                <div style={{
                    fontSize: tokens.fontSize.md, color: colors.textMuted,
                    textAlign: 'center', maxWidth: '400px', lineHeight: 1.6,
                    fontFamily: colors.sans,
                }}>
                    Hierarchical treemap of portfolio and market risk exposures,
                    concentration metrics, and tail-risk indicators. Coming soon.
                </div>
            </div>
        </div>
    );
}
