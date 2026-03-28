import React from 'react';
import { colors, tokens, shared } from '../styles/shared.js';

/**
 * EmptyState - shown when data hasn't loaded or doesn't exist.
 *
 * Props:
 *   icon       - emoji or text icon (default '/')
 *   message    - main message (required)
 *   detail     - optional sub-text
 *   actionLabel - button text (if provided, shows action button)
 *   onAction   - callback for action button
 *   style      - additional style overrides
 */
export default function EmptyState({
    icon = '/',
    message = 'No data available',
    detail,
    actionLabel,
    onAction,
    style: extraStyle,
}) {
    return (
        <div style={{
            padding: '48px 20px',
            textAlign: 'center',
            color: colors.textMuted,
            ...extraStyle,
        }}>
            <div style={{
                fontSize: '36px',
                marginBottom: tokens.space.lg,
                opacity: 0.3,
                fontFamily: "'JetBrains Mono', monospace",
                userSelect: 'none',
            }}>
                {icon}
            </div>
            <div style={{
                fontSize: tokens.fontSize.lg,
                fontWeight: 600,
                color: colors.textDim,
                marginBottom: tokens.space.sm,
                fontFamily: colors.sans,
            }}>
                {message}
            </div>
            {detail && (
                <div style={{
                    fontSize: tokens.fontSize.md,
                    color: colors.textMuted,
                    maxWidth: '360px',
                    margin: '0 auto',
                    lineHeight: '1.5',
                    marginBottom: tokens.space.lg,
                }}>
                    {detail}
                </div>
            )}
            {actionLabel && onAction && (
                <button onClick={onAction} style={shared.buttonSmall}>
                    {actionLabel}
                </button>
            )}
        </div>
    );
}
