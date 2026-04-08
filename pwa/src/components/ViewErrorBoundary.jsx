/**
 * ViewErrorBoundary — per-view error isolation with retry and context.
 *
 * Wraps each view independently so a crash in one view doesn't take down
 * the entire app. Shows the view name, error details, and a retry button
 * that re-mounts the component.
 */

import React from 'react';
import { colors, tokens, shared } from '../styles/shared.js';

const MONO = "'JetBrains Mono', monospace";

export default class ViewErrorBoundary extends React.Component {
    constructor(props) {
        super(props);
        this.state = { hasError: false, error: null, errorInfo: null };
    }

    static getDerivedStateFromError(error) {
        return { hasError: true, error };
    }

    componentDidCatch(error, errorInfo) {
        this.setState({ errorInfo });
        console.error(
            `[ViewErrorBoundary] ${this.props.viewName || 'unknown'} crashed:`,
            error,
            errorInfo?.componentStack,
        );
    }

    render() {
        if (this.state.hasError) {
            const viewName = this.props.viewName || 'View';
            return (
                <div style={{
                    padding: '60px 20px', textAlign: 'center',
                    maxWidth: '500px', margin: '0 auto',
                }}>
                    <div style={{
                        fontSize: '40px', marginBottom: '16px',
                        filter: 'grayscale(1)',
                    }}>
                        {'\u26A0'}
                    </div>
                    <h3 style={{
                        color: colors.red,
                        fontFamily: MONO,
                        fontSize: tokens.fontSize.xl,
                        marginBottom: '8px',
                    }}>
                        {viewName} Error
                    </h3>
                    <p style={{
                        fontSize: '13px',
                        color: colors.textMuted,
                        fontFamily: MONO,
                        marginBottom: '24px',
                        lineHeight: '1.6',
                        wordBreak: 'break-word',
                    }}>
                        {this.state.error?.message || 'An unexpected error occurred'}
                    </p>
                    <div style={{ display: 'flex', gap: '12px', justifyContent: 'center' }}>
                        <button
                            onClick={() => this.setState({ hasError: false, error: null, errorInfo: null })}
                            style={{
                                ...shared.button,
                                fontSize: '13px',
                                padding: '10px 24px',
                            }}
                        >
                            Retry
                        </button>
                        {this.props.onNavigateHome && (
                            <button
                                onClick={this.props.onNavigateHome}
                                style={{
                                    ...shared.button,
                                    fontSize: '13px',
                                    padding: '10px 24px',
                                    background: 'transparent',
                                    border: `1px solid ${colors.border}`,
                                }}
                            >
                                Go Home
                            </button>
                        )}
                    </div>
                </div>
            );
        }

        return this.props.children;
    }
}
