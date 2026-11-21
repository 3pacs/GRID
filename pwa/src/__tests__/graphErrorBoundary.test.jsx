/**
 * GraphErrorBoundary — keeps a Sigma failure (e.g. no WebGL 2) inside the
 * graph area instead of unmounting the whole Canvas view.
 */
import React, { useEffect } from 'react';
import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';

import GraphErrorBoundary, { isWebGLError } from '../canvas/GraphErrorBoundary.jsx';

const WEBGL_MSG = 'Sigma: WebGL 2 is not supported by your browser. Please use a modern browser (Chrome 56+, Firefox 51+, Safari 15+, Edge 79+).';

function ThrowsInRender({ message }) {
    throw new Error(message);
}

// SigmaContainer constructs Sigma inside useEffect, so that is where it throws.
function ThrowsInEffect({ message }) {
    useEffect(() => { throw new Error(message); }, [message]);
    return <div>graph</div>;
}

afterEach(() => vi.restoreAllMocks());

function quietReact() {
    // React reports caught errors through console.error; keep test output clean.
    vi.spyOn(console, 'error').mockImplementation(() => {});
    return vi.spyOn(console, 'warn').mockImplementation(() => {});
}

describe('GraphErrorBoundary', () => {
    it('renders children when nothing throws', () => {
        render(<GraphErrorBoundary><div>graph ok</div></GraphErrorBoundary>);
        expect(screen.getByText('graph ok')).toBeInTheDocument();
        expect(screen.queryByRole('alert')).toBeNull();
    });

    it('shows the WebGL 2 fallback when sigma throws during render, without an uncaught error', () => {
        const warn = quietReact();
        expect(() => render(
            <div>
                <div>lens toolbar</div>
                <GraphErrorBoundary><ThrowsInRender message={WEBGL_MSG} /></GraphErrorBoundary>
            </div>,
        )).not.toThrow();
        expect(screen.getByRole('alert')).toHaveTextContent('The graph view needs WebGL 2');
        expect(screen.getByText(WEBGL_MSG)).toBeInTheDocument();
        expect(screen.getByText('lens toolbar')).toBeInTheDocument();
        expect(warn).toHaveBeenCalled();
    });

    it('catches the error sigma throws from an effect (SigmaContainer construction)', () => {
        quietReact();
        expect(() => render(<GraphErrorBoundary><ThrowsInEffect message={WEBGL_MSG} /></GraphErrorBoundary>)).not.toThrow();
        expect(screen.getByRole('alert')).toHaveTextContent('The graph view needs WebGL 2');
    });

    it('uses a generic message for non-WebGL failures and can retry', () => {
        quietReact();
        let fail = true;
        function Flaky() {
            if (fail) throw new Error('boom');
            return <div>graph back</div>;
        }
        render(<GraphErrorBoundary><Flaky /></GraphErrorBoundary>);
        expect(screen.getByRole('alert')).toHaveTextContent('The graph view failed to start');
        fail = false;
        fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
        expect(screen.getByText('graph back')).toBeInTheDocument();
    });

    it('recognises WebGL errors by message', () => {
        expect(isWebGLError(new Error(WEBGL_MSG))).toBe(true);
        expect(isWebGLError(new Error('boom'))).toBe(false);
        expect(isWebGLError(null)).toBe(false);
    });
});
