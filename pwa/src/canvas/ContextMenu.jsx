/**
 * ContextMenu -- Right-click context menu for canvas nodes.
 * Positioned absolutely at mouse coordinates with glassmorphism styling.
 * Menu items vary by node type: actor, ticker, signal, event.
 */
import React, { useEffect, useRef, useCallback } from 'react';
import {
    GitBranch, Network, DollarSign, TrendingUp, Shield,
    Pin, EyeOff, UserCheck, Landmark, BarChart3, Users,
    Database, Layers, CheckCircle, FileText,
} from 'lucide-react';
import { colors, tokens, glassMorphism } from '../styles/shared.js';

const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

/* ── Menu definitions by node type ───────────────────────────── */

const MENUS = {
    actor: [
        { id: 'expand-1', label: 'Expand Connections', icon: GitBranch, action: 'expand', depth: 1 },
        { id: 'expand-3', label: 'Expand Deep', icon: Network, action: 'expandDeep', depth: 3 },
        { id: 'wealth', label: 'Show Wealth Flows', icon: DollarSign, action: 'showWealthFlows' },
        { id: 'trading', label: 'Show Trading History', icon: TrendingUp, action: 'showTradingHistory' },
        { id: 'trust', label: 'View Trust Breakdown', icon: Shield, action: 'showTrustBreakdown' },
        { id: 'div1', type: 'divider' },
        { id: 'pin', label: 'Pin Position', icon: Pin, action: 'pin' },
        { id: 'hide', label: 'Hide from Canvas', icon: EyeOff, action: 'hide' },
    ],
    ticker: [
        { id: 'insider', label: 'Show Insider Activity', icon: UserCheck, action: 'showInsiderActivity' },
        { id: 'congress', label: 'Show Congressional Trades', icon: Landmark, action: 'showCongressionalTrades' },
        { id: 'options', label: 'Show Options Data', icon: BarChart3, action: 'showOptionsData' },
        { id: 'actors', label: 'Connect Related Actors', icon: Users, action: 'connectRelatedActors' },
        { id: 'div1', type: 'divider' },
        { id: 'pin', label: 'Pin Position', icon: Pin, action: 'pin' },
        { id: 'hide', label: 'Hide from Canvas', icon: EyeOff, action: 'hide' },
    ],
    signal: [
        { id: 'source', label: 'View Source Data', icon: Database, action: 'viewSourceData' },
        { id: 'related', label: 'Show Related Signals', icon: Layers, action: 'showRelatedSignals' },
        { id: 'investigated', label: 'Mark Investigated', icon: CheckCircle, action: 'markInvestigated' },
        { id: 'div1', type: 'divider' },
        { id: 'hide', label: 'Hide from Canvas', icon: EyeOff, action: 'hide' },
    ],
    event: [
        { id: 'actors', label: 'Show Related Actors', icon: Users, action: 'showRelatedActors' },
        { id: 'details', label: 'View Full Details', icon: FileText, action: 'viewFullDetails' },
        { id: 'div1', type: 'divider' },
        { id: 'hide', label: 'Hide from Canvas', icon: EyeOff, action: 'hide' },
    ],
};

/* ── Styles ──────────────────────────────────────────────────── */

const S = {
    menu: {
        position: 'absolute',
        zIndex: 200,
        ...glassMorphism,
        background: 'rgba(13, 21, 32, 0.92)',
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md,
        padding: '4px 0',
        minWidth: '220px',
        boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
    },
    item: {
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        height: '36px',
        padding: '0 12px',
        fontSize: '13px',
        fontFamily: SANS,
        color: colors.textDim,
        cursor: 'pointer',
        background: 'transparent',
        border: 'none',
        width: '100%',
        textAlign: 'left',
        transition: `background ${tokens.transition.fast}`,
    },
    itemDisabled: {
        opacity: 0.4,
        cursor: 'not-allowed',
    },
    divider: {
        height: '1px',
        background: colors.borderSubtle,
        margin: '4px 8px',
    },
    icon: {
        width: '16px',
        height: '16px',
        flexShrink: 0,
        strokeWidth: 1.5,
    },
};

/* ── Component ───────────────────────────────────────────────── */

export default function ContextMenu({ x, y, node, onAction, onClose }) {
    const menuRef = useRef(null);

    // Close on outside click
    useEffect(() => {
        const handleClick = (e) => {
            if (menuRef.current && !menuRef.current.contains(e.target)) {
                onClose?.();
            }
        };
        const handleEsc = (e) => {
            if (e.key === 'Escape') onClose?.();
        };
        // Use setTimeout to avoid the same click that opened the menu from closing it
        const timer = setTimeout(() => {
            document.addEventListener('mousedown', handleClick);
            document.addEventListener('keydown', handleEsc);
        }, 0);
        return () => {
            clearTimeout(timer);
            document.removeEventListener('mousedown', handleClick);
            document.removeEventListener('keydown', handleEsc);
        };
    }, [onClose]);

    // Reposition if menu would overflow viewport
    useEffect(() => {
        if (!menuRef.current) return;
        const rect = menuRef.current.getBoundingClientRect();
        const parent = menuRef.current.parentElement?.getBoundingClientRect();
        if (!parent) return;

        let adjustedX = x;
        let adjustedY = y;

        if (x + rect.width > parent.width) {
            adjustedX = Math.max(0, parent.width - rect.width - 8);
        }
        if (y + rect.height > parent.height) {
            adjustedY = Math.max(0, parent.height - rect.height - 8);
        }

        if (adjustedX !== x || adjustedY !== y) {
            menuRef.current.style.left = `${adjustedX}px`;
            menuRef.current.style.top = `${adjustedY}px`;
        }
    }, [x, y]);

    if (!node) return null;

    const nodeType = node.type || node.nodeType || 'actor';
    const items = MENUS[nodeType] || MENUS.actor;

    const handleItemClick = useCallback((item) => {
        if (item.disabled) return;
        onAction?.(item.action, node, item);
        onClose?.();
    }, [node, onAction, onClose]);

    return (
        <div
            ref={menuRef}
            style={{
                ...S.menu,
                left: `${x}px`,
                top: `${y}px`,
            }}
            onContextMenu={(e) => e.preventDefault()}
        >
            {items.map((item) => {
                if (item.type === 'divider') {
                    return <div key={item.id} style={S.divider} />;
                }

                const IconComponent = item.icon;

                return (
                    <button
                        key={item.id}
                        style={{
                            ...S.item,
                            ...(item.disabled ? S.itemDisabled : {}),
                        }}
                        onClick={() => handleItemClick(item)}
                        onMouseEnter={(e) => {
                            if (!item.disabled) {
                                e.currentTarget.style.background = colors.cardHover;
                                e.currentTarget.style.color = colors.text;
                            }
                        }}
                        onMouseLeave={(e) => {
                            e.currentTarget.style.background = 'transparent';
                            e.currentTarget.style.color = colors.textDim;
                        }}
                        disabled={item.disabled}
                    >
                        {IconComponent && <IconComponent style={S.icon} />}
                        <span>{item.label}</span>
                    </button>
                );
            })}
        </div>
    );
}
