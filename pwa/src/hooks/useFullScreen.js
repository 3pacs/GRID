/**
 * useFullScreen -- Hook to toggle full-screen mode on any container ref.
 *
 * Uses the native Fullscreen API where available, with a CSS-based
 * fallback (fixed position, fills viewport, hides nav).
 *
 * Returns { isFullScreen, toggleFullScreen, exitFullScreen }
 */
import { useState, useCallback, useEffect } from 'react';

export default function useFullScreen(containerRef) {
    const [isFullScreen, setIsFullScreen] = useState(false);

    // Sync state if user exits fullscreen via Escape key / browser UI
    useEffect(() => {
        function onFSChange() {
            const fsEl =
                document.fullscreenElement ||
                document.webkitFullscreenElement ||
                document.mozFullScreenElement ||
                document.msFullscreenElement;
            if (!fsEl) {
                setIsFullScreen(false);
                // Remove fallback styles if they were applied
                if (containerRef?.current) {
                    containerRef.current.style.removeProperty('position');
                    containerRef.current.style.removeProperty('top');
                    containerRef.current.style.removeProperty('left');
                    containerRef.current.style.removeProperty('width');
                    containerRef.current.style.removeProperty('height');
                    containerRef.current.style.removeProperty('z-index');
                    containerRef.current.style.removeProperty('background');
                    containerRef.current.style.removeProperty('overflow');
                }
            }
        }

        document.addEventListener('fullscreenchange', onFSChange);
        document.addEventListener('webkitfullscreenchange', onFSChange);
        return () => {
            document.removeEventListener('fullscreenchange', onFSChange);
            document.removeEventListener('webkitfullscreenchange', onFSChange);
        };
    }, [containerRef]);

    const enterFullScreen = useCallback(() => {
        const el = containerRef?.current;
        if (!el) return;

        // Try native Fullscreen API first
        const requestFS =
            el.requestFullscreen ||
            el.webkitRequestFullscreen ||
            el.mozRequestFullScreen ||
            el.msRequestFullscreen;

        if (requestFS) {
            requestFS.call(el).catch(() => {
                // Fallback: CSS-based full screen
                applyFallback(el);
            });
        } else {
            // No API support: CSS fallback
            applyFallback(el);
        }
        setIsFullScreen(true);
    }, [containerRef]);

    const exitFullScreen = useCallback(() => {
        const exitFS =
            document.exitFullscreen ||
            document.webkitExitFullscreen ||
            document.mozCancelFullScreen ||
            document.msExitFullscreen;

        if (exitFS && (document.fullscreenElement || document.webkitFullscreenElement)) {
            exitFS.call(document).catch(() => {});
        }

        // Always remove fallback styles
        if (containerRef?.current) {
            containerRef.current.style.removeProperty('position');
            containerRef.current.style.removeProperty('top');
            containerRef.current.style.removeProperty('left');
            containerRef.current.style.removeProperty('width');
            containerRef.current.style.removeProperty('height');
            containerRef.current.style.removeProperty('z-index');
            containerRef.current.style.removeProperty('background');
            containerRef.current.style.removeProperty('overflow');
        }
        setIsFullScreen(false);
    }, [containerRef]);

    const toggleFullScreen = useCallback(() => {
        if (isFullScreen) {
            exitFullScreen();
        } else {
            enterFullScreen();
        }
    }, [isFullScreen, enterFullScreen, exitFullScreen]);

    return { isFullScreen, toggleFullScreen, exitFullScreen };
}

function applyFallback(el) {
    el.style.position = 'fixed';
    el.style.top = '0';
    el.style.left = '0';
    el.style.width = '100vw';
    el.style.height = '100vh';
    el.style.zIndex = '9999';
    el.style.background = '#080C10';
    el.style.overflow = 'auto';
}
