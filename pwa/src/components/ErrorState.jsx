/**
 * ErrorState — shared error-with-retry UI for view data fetches.
 *
 * Pairs with useAsyncData: render this when `error` is set, wired to
 * `refetch` so the user can retry the same fetcher.
 */

import React from 'react';
import { AlertTriangle } from 'lucide-react';
import { colors, tokens, shared } from '../styles/shared.js';

/**
 * Props:
 *   error    - Error object or string describing the failure
 *   onRetry  - callback invoked when the retry button is clicked
 *   title    - optional heading shown above the error message
 */
export default function ErrorState({ error, onRetry, title }) {
    const message = error?.message || (typeof error === 'string' ? error : null) || 'Failed to load data';

    return (
        <div style={{
            ...shared.card,
            display: 'flex', alignItems: 'flex-start', gap: '10px',
            borderColor: `${colors.red}40`, background: `${colors.red}10`,
        }}>
            <AlertTriangle size={16} color={colors.red} style={{ flexShrink: 0, marginTop: '2px' }} />
            <div style={{ flex: 1, minWidth: 0 }}>
                {title && (
                    <div style={{
                        fontSize: '13px', fontWeight: 700, color: colors.red,
                        marginBottom: '4px',
                    }}>
                        {title}
                    </div>
                )}
                <div style={{ fontSize: '13px', color: colors.red, lineHeight: '1.4' }}>
                    {message}
                </div>
                {onRetry && (
                    <button
                        onClick={onRetry}
                        style={{ ...shared.buttonSmall, marginTop: tokens.space.sm }}
                    >
                        Retry
                    </button>
                )}
            </div>
        </div>
    );
}
