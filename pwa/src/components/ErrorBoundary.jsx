import React from 'react';
import { colors, tokens, shared } from '../styles/shared.js';

export default class ErrorBoundary extends React.Component {
    constructor(props) {
        super(props);
        this.state = { hasError: false, error: null };
    }

    static getDerivedStateFromError(error) {
        return { hasError: true, error };
    }

    componentDidCatch(error, info) {
        console.error('View crashed:', error, info?.componentStack);
    }

    render() {
        if (this.state.hasError) {
            return (
                <div style={{
                    padding: '40px 20px', textAlign: 'center',
                    color: colors.textMuted,
                }}>
                    <h3 style={{
                        color: colors.red,
                        fontFamily: "'JetBrains Mono', monospace",
                        fontSize: tokens.fontSize.xl,
                        marginBottom: tokens.space.sm,
                    }}>
                        View Error
                    </h3>
                    <p style={{
                        fontSize: tokens.fontSize.md,
                        color: colors.textMuted,
                        maxWidth: '400px',
                        margin: '0 auto',
                        marginBottom: tokens.space.xl,
                        lineHeight: '1.5',
                    }}>
                        {this.state.error?.message || 'An unexpected error occurred'}
                    </p>
                    <button
                        onClick={() => this.setState({ hasError: false, error: null })}
                        style={shared.button}
                    >
                        Retry
                    </button>
                </div>
            );
        }

        return this.props.children;
    }
}
