import React from 'react';

export default class ErrorBoundary extends React.Component {
    constructor(props) {
        super(props);
        this.state = { hasError: false, error: null };
    }

    static getDerivedStateFromError(error) {
        return { hasError: true, error };
    }

    componentDidCatch(error, info) {
        console.error('[GRID] Component error:', error, info?.componentStack);
    }

    render() {
        if (this.state.hasError) {
            return (
                <div style={{
                    padding: '32px 20px', textAlign: 'center',
                    color: '#C8D8E8', fontFamily: "'IBM Plex Sans', sans-serif",
                }}>
                    <div style={{
                        fontSize: '48px', marginBottom: '16px', opacity: 0.3,
                    }}>!</div>
                    <div style={{
                        fontSize: '16px', fontWeight: 600, marginBottom: '8px',
                    }}>Something went wrong</div>
                    <div style={{
                        fontSize: '13px', color: '#5A7080', marginBottom: '20px',
                        maxWidth: '400px', margin: '0 auto 20px',
                    }}>
                        {this.state.error?.message || 'An unexpected error occurred'}
                    </div>
                    <button
                        onClick={() => this.setState({ hasError: false, error: null })}
                        style={{
                            background: '#1A6EBF', color: '#fff', border: 'none',
                            borderRadius: '8px', padding: '10px 24px', fontSize: '14px',
                            fontWeight: 600, cursor: 'pointer',
                            fontFamily: "'IBM Plex Sans', sans-serif",
                        }}
                    >
                        Try Again
                    </button>
                </div>
            );
        }

        return this.props.children;
    }
}
