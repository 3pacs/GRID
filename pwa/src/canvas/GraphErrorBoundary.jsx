/**
 * GraphErrorBoundary — isolates the Sigma (WebGL) graph from the rest of the
 * Canvas page.
 *
 * Sigma v4 needs WebGL 2 and throws ("Sigma: WebGL 2 is not supported…") when
 * it cannot get a context. Without this boundary that error unmounts the whole
 * Canvas view; with it, only the graph area shows a fallback and the lenses,
 * panels and toolbar keep working. A class component because hooks cannot
 * catch render or effect errors.
 */

import React from 'react';
import { colors, shared } from '../styles/shared.js';

const SANS = colors.sans || "'IBM Plex Sans', sans-serif";
const MONO = colors.mono || "'IBM Plex Mono', monospace";

export function isWebGLError(error) {
    return /webgl/i.test(String(error?.message || ''));
}

export default class GraphErrorBoundary extends React.Component {
    constructor(props) {
        super(props);
        this.state = { error: null };
    }

    static getDerivedStateFromError(error) {
        return { error };
    }

    componentDidCatch(error, errorInfo) {
        console.warn('[GraphErrorBoundary] graph view failed:', error, errorInfo?.componentStack);
    }

    render() {
        const { error } = this.state;
        if (!error) return this.props.children;

        const webgl = isWebGLError(error);
        return (
            <div role="alert" style={{
                position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column',
                alignItems: 'center', justifyContent: 'center', gap: '12px', padding: '24px',
                textAlign: 'center', fontFamily: SANS,
            }}>
                <div style={{ fontSize: '16px', fontWeight: 700, color: colors.text }}>
                    {webgl ? 'The graph view needs WebGL 2' : 'The graph view failed to start'}
                </div>
                <div style={{ fontSize: '13px', color: colors.textDim, maxWidth: 420, lineHeight: 1.5 }}>
                    {webgl
                        ? 'This browser or device does not provide WebGL 2, so the network graph cannot be drawn. The other lenses and panels still work.'
                        : 'The other lenses and panels still work.'}
                </div>
                <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: MONO, wordBreak: 'break-word', maxWidth: 520 }}>
                    {String(error?.message || error)}
                </div>
                <button
                    type="button"
                    style={{ ...shared.button, fontSize: '13px', padding: '8px 20px' }}
                    onClick={() => this.setState({ error: null })}
                >
                    Retry
                </button>
            </div>
        );
    }
}
