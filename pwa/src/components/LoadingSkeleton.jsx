import React from 'react';
import { colors, tokens } from '../styles/shared.js';

const shimmerKeyframes = `
@keyframes gridShimmer {
    0% { background-position: -200% 0; }
    100% { background-position: 200% 0; }
}
`;

// Inject keyframes once
let injected = false;
function injectKeyframes() {
    if (injected || typeof document === 'undefined') return;
    const style = document.createElement('style');
    style.textContent = shimmerKeyframes;
    document.head.appendChild(style);
    injected = true;
}

const baseStyle = {
    background: `linear-gradient(90deg, ${colors.card} 25%, #1A2332 50%, ${colors.card} 75%)`,
    backgroundSize: '200% 100%',
    animation: 'gridShimmer 1.5s ease-in-out infinite',
    borderRadius: tokens.radius.sm,
};

/**
 * LoadingSkeleton - shimmer placeholder for loading states.
 *
 * Props:
 *   height   - line height (default '14px')
 *   width    - line width (default '100%')
 *   count    - number of skeleton lines (default 1)
 *   variant  - 'text' | 'card' | 'chart' (preset shapes)
 *   style    - additional style overrides
 */
export default function LoadingSkeleton({
    height = '14px',
    width = '100%',
    count = 1,
    variant,
    style: extraStyle,
}) {
    injectKeyframes();

    if (variant === 'card') {
        return (
            <div style={{ marginBottom: tokens.space.sm, ...extraStyle }}>
                <div style={{ ...baseStyle, height: '120px', width: '100%', borderRadius: tokens.radius.md, marginBottom: tokens.space.sm }} />
                <div style={{ ...baseStyle, height: '14px', width: '60%', marginBottom: tokens.space.xs }} />
                <div style={{ ...baseStyle, height: '12px', width: '40%' }} />
            </div>
        );
    }

    if (variant === 'chart') {
        return (
            <div style={{
                background: colors.card,
                border: `1px solid ${colors.border}`,
                borderRadius: tokens.radius.md,
                padding: tokens.space.lg,
                ...extraStyle,
            }}>
                <div style={{ ...baseStyle, height: '16px', width: '30%', marginBottom: tokens.space.lg }} />
                <div style={{ ...baseStyle, height: '200px', width: '100%', borderRadius: tokens.radius.md }} />
            </div>
        );
    }

    // Default: text lines
    const lines = [];
    for (let i = 0; i < count; i++) {
        const isLast = i === count - 1 && count > 1;
        lines.push(
            <div
                key={i}
                style={{
                    ...baseStyle,
                    height,
                    width: isLast ? '60%' : width,
                    marginBottom: i < count - 1 ? tokens.space.sm : 0,
                    ...extraStyle,
                }}
            />
        );
    }

    return <>{lines}</>;
}
