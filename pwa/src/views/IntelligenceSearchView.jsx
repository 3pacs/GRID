import React, { useEffect, useState } from 'react';
import { Search, ExternalLink } from 'lucide-react';
import IntelligenceSearch from '../components/IntelligenceSearch.jsx';
import { api } from '../api.js';
import { colors, tokens } from '../styles/shared.js';

const styles = {
    page: {
        minHeight: 'calc(100vh - 64px)',
        position: 'relative',
        background: colors.bg,
        color: colors.text,
        overflow: 'hidden',
    },
    aside: {
        marginLeft: 320,
        padding: '28px',
        maxWidth: 720,
    },
    eyebrow: {
        display: 'flex',
        alignItems: 'center',
        gap: 8,
        color: colors.accent,
        fontSize: 11,
        fontFamily: "'IBM Plex Mono', monospace",
        letterSpacing: '1px',
        fontWeight: 700,
        textTransform: 'uppercase',
        marginBottom: 12,
    },
    title: {
        margin: 0,
        fontSize: 30,
        lineHeight: 1.15,
        color: colors.text,
    },
    body: {
        marginTop: 12,
        color: colors.textMuted,
        fontSize: 14,
        lineHeight: 1.6,
    },
    controls: {
        marginTop: 20,
        display: 'flex',
        gap: 8,
        alignItems: 'center',
        flexWrap: 'wrap',
    },
    select: {
        minWidth: 220,
        background: colors.card,
        color: colors.text,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.sm,
        padding: '9px 10px',
        fontSize: 13,
    },
    button: {
        display: 'inline-flex',
        alignItems: 'center',
        gap: 6,
        background: colors.accent,
        color: '#fff',
        border: 'none',
        borderRadius: tokens.radius.sm,
        padding: '9px 12px',
        fontWeight: 700,
        cursor: 'pointer',
    },
    status: {
        marginTop: 12,
        color: colors.textDim,
        fontSize: 12,
        minHeight: 18,
    },
    staged: {
        marginTop: 24,
        padding: 14,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.sm,
        background: colors.card,
    },
    stagedTitle: {
        color: colors.textDim,
        fontFamily: "'IBM Plex Mono', monospace",
        fontSize: 10,
        letterSpacing: '1px',
        fontWeight: 700,
        marginBottom: 10,
    },
    stagedItem: {
        padding: '8px 0',
        borderTop: `1px solid ${colors.border}`,
        color: colors.text,
        fontSize: 13,
    },
};

function nodeTypeForSearchResult(node) {
    return node.type === 'snapshot' ? 'note' : (node.type || 'note');
}

function toGraphNode(node, index) {
    const nodeType = nodeTypeForSearchResult(node);
    const nodeId = `intel:${node.type || nodeType}:${node.id || node.label || Date.now()}`;
    return {
        key: nodeId,
        id: nodeId,
        label: node.label || nodeId,
        x: 120 + (index % 4) * 140,
        y: 120 + Math.floor(index / 4) * 100,
        type: nodeType,
        nodeType,
        attributes: {
            nodeType,
            label: node.label || nodeId,
            data: node.data || {},
            source: 'intelligence-search',
        },
    };
}

function normalizeGraphState(board) {
    const graphState = board?.graph_state || board?.graph || {};
    return {
        nodes: Array.isArray(graphState.nodes) ? [...graphState.nodes] : [],
        edges: Array.isArray(graphState.edges) ? [...graphState.edges] : [],
    };
}

export default function IntelligenceSearchView({ onNavigate, originView }) {
    const [boards, setBoards] = useState([]);
    const [selectedBoardId, setSelectedBoardId] = useState('');
    const [added, setAdded] = useState([]);
    const [status, setStatus] = useState('');
    const [saving, setSaving] = useState(false);
    const [isStacked, setIsStacked] = useState(
        typeof window !== 'undefined' ? window.innerWidth < 960 : false
    );

    useEffect(() => {
        const syncLayout = () => setIsStacked(window.innerWidth < 960);
        window.addEventListener('resize', syncLayout);
        return () => window.removeEventListener('resize', syncLayout);
    }, []);

    useEffect(() => {
        let cancelled = false;
        async function loadBoards() {
            try {
                const data = await api.listBoards();
                const items = Array.isArray(data) ? data : [];
                if (!cancelled) {
                    setBoards(items);
                    setSelectedBoardId(items[0]?.id || '');
                }
            } catch {
                if (!cancelled) setBoards([]);
            }
        }
        loadBoards();
        return () => { cancelled = true; };
    }, []);

    const ensureBoardId = async () => {
        if (selectedBoardId) return selectedBoardId;
        const created = await api.createBoard(`Intel Search ${new Date().toISOString().slice(0, 10)}`);
        const boardId = created?.id;
        if (!boardId) throw new Error('Board creation failed');
        setBoards(prev => [created, ...prev]);
        setSelectedBoardId(boardId);
        return boardId;
    };

    const addToBoard = async (node) => {
        setSaving(true);
        setStatus('');
        try {
            const boardId = await ensureBoardId();
            const board = await api.getBoard(boardId);
            const graphState = normalizeGraphState(board);
            const graphNode = toGraphNode(node, graphState.nodes.length);
            const key = graphNode.key || graphNode.id;
            const existingIndex = graphState.nodes.findIndex(n => (n.key || n.id) === key);
            if (existingIndex >= 0) {
                graphState.nodes[existingIndex] = { ...graphState.nodes[existingIndex], ...graphNode };
            } else {
                graphState.nodes.push(graphNode);
            }
            await api.saveBoard(boardId, {
                graph_state: graphState,
                filters: board?.filters || {},
            });
            setAdded(prev => [graphNode, ...prev].slice(0, 8));
            setStatus(`Added "${graphNode.label}" to ${board?.name || 'Canvas board'}.`);
        } catch (err) {
            setStatus(err?.message || 'Failed to add result to Canvas.');
        } finally {
            setSaving(false);
        }
    };

    const openSelectedBoard = () => {
        if (selectedBoardId) {
            window.location.hash = `#/canvas?board=${encodeURIComponent(selectedBoardId)}`;
        } else {
            onNavigate?.('canvas');
        }
    };

    const closeSearch = () => {
        if (originView) {
            onNavigate?.(originView);
            return;
        }
        if (typeof window !== 'undefined' && window.history.length > 1) {
            window.history.back();
            return;
        }
        onNavigate?.('surfacer');
    };

    const pageStyle = isStacked
        ? {
            ...styles.page,
            display: 'flex',
            flexDirection: 'column',
            overflowY: 'auto',
        }
        : styles.page;
    const asideStyle = isStacked
        ? {
            ...styles.aside,
            marginLeft: 0,
            maxWidth: '100%',
            padding: '20px 16px calc(96px + env(safe-area-inset-bottom, 0px))',
        }
        : styles.aside;
    const titleStyle = isStacked
        ? { ...styles.title, fontSize: 24 }
        : styles.title;
    const controlsStyle = isStacked
        ? { ...styles.controls, flexDirection: 'column', alignItems: 'stretch' }
        : styles.controls;
    const selectStyle = isStacked
        ? { ...styles.select, width: '100%', minWidth: 0 }
        : styles.select;
    const buttonStyle = isStacked
        ? { ...styles.button, width: '100%', justifyContent: 'center' }
        : styles.button;

    return (
        <div style={pageStyle}>
            <IntelligenceSearch
                stacked={isStacked}
                onClose={closeSearch}
                onAddToCanvas={addToBoard}
            />
            <div style={asideStyle}>
                <div style={styles.eyebrow}>
                    <Search size={14} />
                    Intel Search
                </div>
                <h1 style={titleStyle}>Search actors, signals, hypotheses, and snapshots.</h1>
                <div style={styles.body}>
                    Add promising results to a Canvas board, then open the board to map the connections.
                </div>

                <div style={controlsStyle}>
                    <select
                        style={selectStyle}
                        value={selectedBoardId}
                        onChange={(e) => setSelectedBoardId(e.target.value)}
                        disabled={saving}
                    >
                        {boards.length === 0 && <option value="">New Canvas board</option>}
                        {boards.map(board => (
                            <option key={board.id} value={board.id}>
                                {board.name || 'Untitled board'}
                            </option>
                        ))}
                    </select>
                    <button type="button" style={buttonStyle} onClick={openSelectedBoard}>
                        <ExternalLink size={14} />
                        Open Canvas
                    </button>
                </div>
                <div style={styles.status}>{saving ? 'Saving to Canvas...' : status}</div>

                {added.length > 0 && (
                    <div style={styles.staged}>
                        <div style={styles.stagedTitle}>RECENTLY ADDED</div>
                        {added.map((node, idx) => (
                            <div key={`${node.key}-${idx}`} style={styles.stagedItem}>
                                {node.label}
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}
