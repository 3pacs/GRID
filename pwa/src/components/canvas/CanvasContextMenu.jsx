import React, { useEffect, useRef } from 'react';
import { Network, Trash2, Palette, Waypoints, Zap, Brain, BarChart3, Clock, Target } from 'lucide-react';

const menuStyle = {
    position: 'fixed',
    zIndex: 100,
    background: '#0D1117',
    border: '1px solid #1E2A3A',
    borderRadius: 8,
    boxShadow: '0 8px 24px rgba(0,0,0,0.6)',
    minWidth: 180,
    padding: '4px 0',
    fontFamily: "'IBM Plex Sans', sans-serif",
};

const itemStyle = {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    padding: '8px 14px',
    fontSize: 12,
    color: '#C8D8E8',
    cursor: 'pointer',
    border: 'none',
    background: 'transparent',
    width: '100%',
    textAlign: 'left',
    fontFamily: 'inherit',
};

const disabledItemStyle = {
    ...itemStyle,
    color: '#3A4A5A',
    cursor: 'not-allowed',
};

const separatorStyle = {
    height: 1,
    background: '#1E2A3A',
    margin: '4px 0',
};

function CanvasContextMenu({
    x,
    y,
    node,
    edge,
    selectedNodes,
    explaining,
    onClose,
    onExpand,
    onRemove,
    onChangeColor,
    onSuggestConnections,
    onExplainConnection,
    onAddChart,
    onAddTimeline,
    onCreatePrediction,
}) {
    const ref = useRef(null);

    // Close on click outside or Escape
    useEffect(() => {
        const handleClick = (e) => {
            if (ref.current && !ref.current.contains(e.target)) {
                onClose();
            }
        };
        const handleKey = (e) => {
            if (e.key === 'Escape') onClose();
        };
        document.addEventListener('mousedown', handleClick);
        document.addEventListener('keydown', handleKey);
        return () => {
            document.removeEventListener('mousedown', handleClick);
            document.removeEventListener('keydown', handleKey);
        };
    }, [onClose]);

    const isExpandable = node?.type === 'actor' || node?.type === 'company';

    // Determine if "Explain Connection" should appear:
    // - When right-clicking an edge
    // - When exactly 2 nodes are selected
    const hasTwoSelected = (selectedNodes || []).length === 2;
    const canExplain = !!edge || hasTwoSelected;

    // Clamp menu position to viewport
    const clampedX = Math.min(x, window.innerWidth - 200);
    const clampedY = Math.min(y, window.innerHeight - 300);

    return (
        <div
            ref={ref}
            style={{ ...menuStyle, left: clampedX, top: clampedY }}
            onContextMenu={(e) => e.preventDefault()}
        >
            {/* Only show node-specific items when a node is targeted */}
            {node && (
                <>
                    <button
                        style={isExpandable ? itemStyle : disabledItemStyle}
                        onClick={() => {
                            if (isExpandable) {
                                onExpand(node);
                                onClose();
                            }
                        }}
                        onMouseEnter={(e) => {
                            if (isExpandable) e.currentTarget.style.background = '#161B22';
                        }}
                        onMouseLeave={(e) => {
                            e.currentTarget.style.background = 'transparent';
                        }}
                    >
                        <Network size={14} />
                        Expand Network
                    </button>

                    <button
                        style={itemStyle}
                        onClick={() => {
                            onSuggestConnections();
                            onClose();
                        }}
                        onMouseEnter={(e) => { e.currentTarget.style.background = '#161B22'; }}
                        onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                    >
                        <Zap size={14} />
                        Suggest Connections
                    </button>

                    {isExpandable && onAddChart && (
                        <button
                            style={{ ...itemStyle, color: '#06B6D4' }}
                            onClick={() => {
                                onAddChart(node);
                                onClose();
                            }}
                            onMouseEnter={(e) => { e.currentTarget.style.background = '#161B22'; }}
                            onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                        >
                            <BarChart3 size={14} />
                            Add Price Chart
                        </button>
                    )}

                    {isExpandable && onAddTimeline && (
                        <button
                            style={{ ...itemStyle, color: '#F97316' }}
                            onClick={() => {
                                onAddTimeline(node);
                                onClose();
                            }}
                            onMouseEnter={(e) => { e.currentTarget.style.background = '#161B22'; }}
                            onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                        >
                            <Clock size={14} />
                            Add Event Timeline
                        </button>
                    )}
                </>
            )}

            {/* Explain Connection — visible when edge clicked or 2 nodes selected */}
            {canExplain && (
                <button
                    style={explaining ? disabledItemStyle : { ...itemStyle, color: '#A78BFA' }}
                    onClick={() => {
                        if (explaining) return;
                        if (edge && onExplainConnection) {
                            onExplainConnection(edge.source, edge.target);
                        } else if (hasTwoSelected && onExplainConnection) {
                            onExplainConnection(selectedNodes[0].id, selectedNodes[1].id);
                        }
                        onClose();
                    }}
                    onMouseEnter={(e) => {
                        if (!explaining) e.currentTarget.style.background = '#161B22';
                    }}
                    onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                >
                    <Brain size={14} />
                    {explaining ? 'Explaining...' : 'Explain Connection'}
                </button>
            )}

            {/* Create Prediction — visible when 1+ nodes selected */}
            {(selectedNodes || []).length >= 1 && onCreatePrediction && (
                <button
                    style={{ ...itemStyle, color: '#10B981' }}
                    onClick={() => {
                        onCreatePrediction();
                        onClose();
                    }}
                    onMouseEnter={(e) => { e.currentTarget.style.background = '#161B22'; }}
                    onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                >
                    <Target size={14} />
                    Create Prediction from Selection
                </button>
            )}

            {node && (
                <>
                    <div style={separatorStyle} />

                    <button
                        style={itemStyle}
                        onClick={() => {
                            onChangeColor(node);
                            onClose();
                        }}
                        onMouseEnter={(e) => { e.currentTarget.style.background = '#161B22'; }}
                        onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                    >
                        <Palette size={14} />
                        Change Color
                    </button>

                    <button
                        style={{ ...itemStyle, color: '#EF4444' }}
                        onClick={() => {
                            onRemove(node);
                            onClose();
                        }}
                        onMouseEnter={(e) => { e.currentTarget.style.background = '#161B22'; }}
                        onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                    >
                        <Trash2 size={14} />
                        Remove
                    </button>
                </>
            )}

            {/* Edge-only context: just explain + remove */}
            {edge && !node && (
                <>
                    <div style={separatorStyle} />
                    <button
                        style={{ ...itemStyle, color: '#EF4444' }}
                        onClick={() => {
                            onRemove(edge);
                            onClose();
                        }}
                        onMouseEnter={(e) => { e.currentTarget.style.background = '#161B22'; }}
                        onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                    >
                        <Trash2 size={14} />
                        Remove Edge
                    </button>
                </>
            )}
        </div>
    );
}

export default React.memo(CanvasContextMenu);
