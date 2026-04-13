/**
 * useKeyboardShortcuts -- Canvas keyboard handler.
 * Binds global keyboard shortcuts for canvas operations:
 *   f/F  - Fit to viewport
 *   e/E  - Expand selected node
 *   l/L  - Toggle labels
 *   c/C  - Toggle community cluster hulls
 *   Esc  - Deselect / close panels
 *   Del  - Hide selected nodes
 *   Bksp - Hide selected nodes
 *   1-8  - Toggle layers
 */
import { useEffect, useCallback } from 'react';

const LAYER_KEYS = ['1', '2', '3', '4', '5', '6', '7', '8'];
const LAYER_ORDER = [
    'financial', 'insider', 'political', 'news',
    'options', 'macro', 'offshore', 'predictions',
];

export function useKeyboardShortcuts({
    sigmaRef,
    selectedNode,
    onFitViewport,
    onExpandSelected,
    onToggleLabels,
    onDeselect,
    onClosePanel,
    onHideSelected,
    onToggleLayer,
    onToggleCommunities,
    onSetLens,
}) {
    const handler = useCallback((e) => {
        // Don't fire when typing in input fields
        const tag = e.target.tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
        if (e.target.contentEditable === 'true') return;

        // Don't fire with modifier keys (except shift for letter shortcuts)
        if (e.ctrlKey || e.metaKey || e.altKey) return;

        switch (e.key) {
            case 'g':
            case 'G': {
                if (onSetLens) {
                    e.preventDefault();
                    onSetLens('graph');
                }
                break;
            }
            case 's':
            case 'S': {
                if (onSetLens) {
                    e.preventDefault();
                    onSetLens('supply');
                }
                break;
            }
            case 'f':
            case 'F': {
                e.preventDefault();
                // When lens switcher is active, F → capital flow lens.
                // Otherwise fall back to Fit Viewport.
                if (onSetLens) {
                    onSetLens('capital');
                } else if (onFitViewport) {
                    onFitViewport();
                } else if (sigmaRef?.current) {
                    const sigma = sigmaRef.current;
                    const camera = sigma.getCamera();
                    camera.animatedReset({ duration: 300 });
                }
                break;
            }

            case 'e':
            case 'E': {
                e.preventDefault();
                if (selectedNode && onExpandSelected) {
                    onExpandSelected(selectedNode);
                }
                break;
            }

            case 'l':
            case 'L': {
                e.preventDefault();
                onToggleLabels?.();
                break;
            }

            case 'Escape': {
                e.preventDefault();
                // Close panel first, then deselect
                if (onClosePanel) {
                    onClosePanel();
                } else if (onDeselect) {
                    onDeselect();
                }
                break;
            }

            case 'Delete':
            case 'Backspace': {
                e.preventDefault();
                if (selectedNode && onHideSelected) {
                    onHideSelected(selectedNode);
                }
                break;
            }

            case 'c':
            case 'C': {
                e.preventDefault();
                onToggleCommunities?.();
                break;
            }

            default: {
                // Number keys 1-8 toggle layers
                const layerIdx = LAYER_KEYS.indexOf(e.key);
                if (layerIdx !== -1 && onToggleLayer) {
                    e.preventDefault();
                    onToggleLayer(LAYER_ORDER[layerIdx]);
                }
                break;
            }
        }
    }, [
        sigmaRef, selectedNode, onFitViewport, onExpandSelected,
        onToggleLabels, onDeselect, onClosePanel, onHideSelected,
        onToggleLayer, onToggleCommunities, onSetLens,
    ]);

    useEffect(() => {
        window.addEventListener('keydown', handler);
        return () => window.removeEventListener('keydown', handler);
    }, [handler]);
}

export default useKeyboardShortcuts;
