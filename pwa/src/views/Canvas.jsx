import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import {
    ReactFlow,
    Background,
    Controls,
    MiniMap,
    addEdge,
    useNodesState,
    useEdgesState,
    Panel,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { api } from '../api.js';
import useCanvasStore from '../stores/canvasStore.js';
import ActorNode from '../components/canvas/ActorNode.jsx';
import CompanyNode from '../components/canvas/CompanyNode.jsx';
import HypothesisNode from '../components/canvas/HypothesisNode.jsx';
import SignalNode from '../components/canvas/SignalNode.jsx';
import NoteNode from '../components/canvas/NoteNode.jsx';
import EvidenceNode from '../components/canvas/EvidenceNode.jsx';
import ChartNode from '../components/canvas/ChartNode.jsx';
import TimelineNode from '../components/canvas/TimelineNode.jsx';
import { NODE_COLORS } from '../components/canvas/nodeStyles.js';
import { Plus, Save, Trash2, StickyNote, ChevronDown, Network, Zap, Search as SearchIcon, BarChart3, Clock } from 'lucide-react';
import CanvasContextMenu from '../components/canvas/CanvasContextMenu.jsx';
import IntelligenceSearch from '../components/IntelligenceSearch.jsx';

const nodeTypes = {
    actor: ActorNode,
    company: CompanyNode,
    hypothesis: HypothesisNode,
    signal: SignalNode,
    note: NoteNode,
    evidence: EvidenceNode,
    chart: ChartNode,
    timeline: TimelineNode,
};

const defaultEdgeOptions = {
    type: 'smoothstep',
    style: { stroke: '#3B82F6', strokeWidth: 1.5 },
    animated: false,
};

const minimapStyle = {
    background: '#0D1117',
    border: '1px solid #1E2A3A',
    borderRadius: 6,
};

const btnBase = {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 5,
    padding: '5px 10px',
    fontSize: 12,
    fontFamily: "'IBM Plex Sans', sans-serif",
    color: '#C8D8E8',
    background: '#161B22',
    border: '1px solid #1E2A3A',
    borderRadius: 6,
    cursor: 'pointer',
};

function Canvas() {
    const {
        boards, currentBoardId, loading,
        setBoards, setCurrentBoardId, setLoading,
    } = useCanvasStore();

    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);
    const [dirty, setDirty] = useState(false);
    const [pickerOpen, setPickerOpen] = useState(false);
    const [expanding, setExpanding] = useState(false);
    const [explaining, setExplaining] = useState(false);
    const [contextMenu, setContextMenu] = useState(null); // { x, y, node, edge }
    const [searchOpen, setSearchOpen] = useState(false);
    const autoSaveTimer = useRef(null);
    const pickerRef = useRef(null);

    const currentBoard = useMemo(
        () => boards.find((b) => (b.board_id || b.id) === currentBoardId),
        [boards, currentBoardId],
    );

    // Close picker on outside click
    useEffect(() => {
        if (!pickerOpen) return;
        const handler = (e) => {
            if (pickerRef.current && !pickerRef.current.contains(e.target)) {
                setPickerOpen(false);
            }
        };
        document.addEventListener('mousedown', handler);
        return () => document.removeEventListener('mousedown', handler);
    }, [pickerOpen]);

    // Load boards on mount
    useEffect(() => {
        (async () => {
            try {
                const res = await api.getCanvasBoards();
                const list = res.items || res.boards || res || [];
                setBoards(list);
                if (list.length > 0 && !currentBoardId) {
                    const first = list[0];
                    setCurrentBoardId(first.board_id || first.id);
                }
            } catch {
                setBoards([]);
            }
        })();
    }, []);

    // Load board graph when currentBoardId changes
    useEffect(() => {
        if (!currentBoardId) return;
        (async () => {
            setLoading(true);
            try {
                const res = await api.getCanvasBoard(currentBoardId);
                const graph = res.graph || res || {};
                const rfNodes = (graph.nodes || []).map((n) => ({
                    id: String(n.node_id),
                    type: n.node_type,
                    position: { x: n.position_x ?? 0, y: n.position_y ?? 0 },
                    data: {
                        label: n.label,
                        entityId: n.entity_id,
                        ...(n.data || {}),
                    },
                }));
                const rfEdges = (graph.edges || []).map((e) => ({
                    id: String(e.edge_id),
                    source: String(e.source_node_id),
                    target: String(e.target_node_id),
                    type: e.edge_type || 'smoothstep',
                    label: e.label || '',
                }));
                setNodes(rfNodes);
                setEdges(rfEdges);
                setDirty(false);
            } catch {
                setNodes([]);
                setEdges([]);
            } finally {
                setLoading(false);
            }
        })();
    }, [currentBoardId]);

    // Auto-save: 3s after last change
    useEffect(() => {
        if (!dirty || !currentBoardId) return;
        if (autoSaveTimer.current) clearTimeout(autoSaveTimer.current);
        autoSaveTimer.current = setTimeout(() => {
            saveGraph();
        }, 3000);
        return () => {
            if (autoSaveTimer.current) clearTimeout(autoSaveTimer.current);
        };
    }, [dirty, nodes, edges, currentBoardId]);

    // Ctrl+S to save
    useEffect(() => {
        const handler = (e) => {
            if ((e.metaKey || e.ctrlKey) && e.key === 's') {
                e.preventDefault();
                if (currentBoardId) saveGraph();
            }
        };
        window.addEventListener('keydown', handler);
        return () => window.removeEventListener('keydown', handler);
    }, [currentBoardId, nodes, edges]);

    // Delete key to remove selected node
    useEffect(() => {
        const handler = (e) => {
            if (e.key === 'Delete' || e.key === 'Backspace') {
                // Don't intercept if user is typing in an input
                if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
                const selected = nodes.find((n) => n.selected);
                if (selected) {
                    setNodes((prev) => prev.filter((n) => n.id !== selected.id));
                    setEdges((prev) => prev.filter((e2) => e2.source !== selected.id && e2.target !== selected.id));
                    setDirty(true);
                }
                const selectedEdge = edges.find((e2) => e2.selected);
                if (selectedEdge) {
                    setEdges((prev) => prev.filter((e2) => e2.id !== selectedEdge.id));
                    setDirty(true);
                }
            }
        };
        window.addEventListener('keydown', handler);
        return () => window.removeEventListener('keydown', handler);
    }, [nodes, edges]);

    const saveGraph = useCallback(async () => {
        if (!currentBoardId) return;
        const dbNodes = nodes.map((n) => ({
            node_id: n.id,
            node_type: n.type || 'note',
            position_x: n.position?.x ?? 0,
            position_y: n.position?.y ?? 0,
            label: n.data?.label || '',
            entity_id: n.data?.entityId || null,
            data: { ...n.data },
        }));
        const dbEdges = edges.map((e) => ({
            edge_id: e.id,
            source_node_id: e.source,
            target_node_id: e.target,
            edge_type: e.type || 'smoothstep',
            label: e.label || '',
        }));
        try {
            await api.saveCanvasGraph(currentBoardId, { nodes: dbNodes, edges: dbEdges });
            setDirty(false);
        } catch (err) {
            console.error('Canvas save failed:', err);
        }
    }, [currentBoardId, nodes, edges]);

    const onConnect = useCallback((params) => {
        setEdges((prev) => addEdge({ ...params, type: 'smoothstep' }, prev));
        setDirty(true);
    }, []);

    const onNodesChangeWrapped = useCallback((changes) => {
        onNodesChange(changes);
        const hasPositionChange = changes.some((c) => c.type === 'position' && c.dragging === false);
        if (hasPositionChange) setDirty(true);
    }, [onNodesChange]);

    const handleNewBoard = async () => {
        const name = prompt('Board name:');
        if (!name) return;
        try {
            const res = await api.createCanvasBoard(name);
            const id = res.board_id || res.id;
            const fresh = await api.getCanvasBoards();
            const list = fresh.boards || fresh || [];
            setBoards(list);
            setCurrentBoardId(id);
        } catch (err) {
            console.error('Failed to create board:', err);
        }
    };

    const handleDeleteBoard = async () => {
        if (!currentBoardId) return;
        const boardName = currentBoard?.name || 'this board';
        if (!confirm(`Delete "${boardName}"?`)) return;
        try {
            await api.deleteCanvasBoard(currentBoardId);
            const fresh = await api.getCanvasBoards();
            const list = fresh.boards || fresh || [];
            setBoards(list);
            if (list.length > 0) {
                setCurrentBoardId(list[0].board_id || list[0].id);
            } else {
                setCurrentBoardId(null);
                setNodes([]);
                setEdges([]);
            }
        } catch (err) {
            console.error('Failed to delete board:', err);
        }
    };

    const handleAddNote = () => {
        const id = `note-${Date.now()}`;
        const newNode = {
            id,
            type: 'note',
            position: { x: 200 + Math.random() * 200, y: 200 + Math.random() * 200 },
            data: {
                label: '',
                onLabelChange: (text) => {
                    setNodes((prev) =>
                        prev.map((n) =>
                            n.id === id ? { ...n, data: { ...n.data, label: text } } : n
                        )
                    );
                    setDirty(true);
                },
            },
        };
        setNodes((prev) => [...prev, newNode]);
        setDirty(true);
    };

    const handleAddFromSearch = useCallback(({ type, id, label, data }) => {
        const nodeId = `${type}-search-${id}-${Date.now()}`;
        const newNode = {
            id: nodeId,
            type: type || 'note',
            position: { x: 350 + Math.random() * 300, y: 100 + Math.random() * 300 },
            data: {
                label: label || `${type} ${id}`,
                entityId: id,
                ...data,
            },
        };
        setNodes((prev) => [...prev, newNode]);
        setDirty(true);
    }, [setNodes]);

    const handleExpandNode = useCallback(async (node) => {
        if (!currentBoardId || !node) return;
        setExpanding(true);
        try {
            const res = await api.expandCanvasNode(currentBoardId, node.id);
            const newNodes = (res.new_nodes || []).map((n) => ({
                id: String(n.id),
                type: n.node_type || 'actor',
                position: { x: n.position_x ?? 0, y: n.position_y ?? 0 },
                data: {
                    label: n.label,
                    entityId: typeof n.data === 'string' ? JSON.parse(n.data)?.entityId : n.data?.entityId,
                    category: typeof n.data === 'string' ? JSON.parse(n.data)?.category : n.data?.category,
                    trust_score: typeof n.data === 'string' ? JSON.parse(n.data)?.trust_score : n.data?.trust_score,
                },
            }));
            const newEdges = (res.new_edges || []).map((e) => ({
                id: String(e.id),
                source: String(e.source_node_id),
                target: String(e.target_node_id),
                type: e.edge_type || 'smoothstep',
                label: e.label || '',
            }));
            if (newNodes.length > 0) {
                setNodes((prev) => [...prev, ...newNodes]);
                setEdges((prev) => [...prev, ...newEdges]);
                setDirty(true);
            }
        } catch (err) {
            console.error('Expand failed:', err);
        } finally {
            setExpanding(false);
        }
    }, [currentBoardId, setNodes, setEdges]);

    const handleSuggestConnections = useCallback(async () => {
        if (!currentBoardId) return;
        try {
            const res = await api.suggestCanvasConnections(currentBoardId);
            const suggestions = res.suggestions || [];
            if (suggestions.length === 0) return;
            const newEdges = suggestions.map((s) => ({
                id: s.edge_id,
                source: String(s.source_node_id),
                target: String(s.target_node_id),
                type: 'smoothstep',
                label: s.relationship || '',
                style: { stroke: '#F59E0B', strokeWidth: 1.5, strokeDasharray: '5,5' },
            }));
            setEdges((prev) => [...prev, ...newEdges]);
            setDirty(true);
        } catch (err) {
            console.error('Suggest connections failed:', err);
        }
    }, [currentBoardId, setEdges]);

    const handleExplainConnection = useCallback(async (sourceId, targetId) => {
        if (!currentBoardId || !sourceId || !targetId) return;
        setExplaining(true);
        try {
            const res = await api.explainCanvasConnection(currentBoardId, sourceId, targetId);

            // Find source and target node positions to place the note midway
            const sourceNode = nodes.find((n) => n.id === sourceId);
            const targetNode = nodes.find((n) => n.id === targetId);
            const midX = ((sourceNode?.position?.x ?? 0) + (targetNode?.position?.x ?? 0)) / 2;
            const midY = ((sourceNode?.position?.y ?? 0) + (targetNode?.position?.y ?? 0)) / 2 + 120;

            // Build the explanation text
            const confidenceTag = `[${(res.confidence || 'estimated').toUpperCase()}]`;
            const factsText = (res.key_facts || []).map((f) => `  - ${f}`).join('\n');
            const leverText = res.lever ? `\nLever: ${res.lever}` : '';
            const noteLabel = [
                `${confidenceTag} ${res.source_label || ''} <-> ${res.target_label || ''}`,
                '',
                res.explanation || '',
                '',
                factsText ? `Key facts:\n${factsText}` : '',
                leverText,
            ].filter(Boolean).join('\n');

            const noteId = `explain-${Date.now()}`;
            const noteNode = {
                id: noteId,
                type: 'note',
                position: { x: midX, y: midY },
                data: {
                    label: noteLabel,
                    confidence: res.confidence,
                    onLabelChange: (text) => {
                        setNodes((prev) =>
                            prev.map((n) =>
                                n.id === noteId ? { ...n, data: { ...n.data, label: text } } : n
                            )
                        );
                        setDirty(true);
                    },
                },
            };

            // Add the note node and two edges connecting it to source and target
            const edgeToSource = {
                id: `explain-edge-s-${Date.now()}`,
                source: sourceId,
                target: noteId,
                type: 'smoothstep',
                style: { stroke: '#A78BFA', strokeWidth: 1, strokeDasharray: '4,4' },
                animated: true,
            };
            const edgeToTarget = {
                id: `explain-edge-t-${Date.now()}`,
                source: targetId,
                target: noteId,
                type: 'smoothstep',
                style: { stroke: '#A78BFA', strokeWidth: 1, strokeDasharray: '4,4' },
                animated: true,
            };

            setNodes((prev) => [...prev, noteNode]);
            setEdges((prev) => [...prev, edgeToSource, edgeToTarget]);
            setDirty(true);
        } catch (err) {
            console.error('Explain connection failed:', err);
        } finally {
            setExplaining(false);
        }
    }, [currentBoardId, nodes, setNodes, setEdges]);

    const handleExpandSelected = useCallback(() => {
        const selected = nodes.find((n) => n.selected && (n.type === 'actor' || n.type === 'company'));
        if (selected) handleExpandNode(selected);
    }, [nodes, handleExpandNode]);

    const handleRemoveNode = useCallback((nodeOrEdge) => {
        if (!nodeOrEdge) return;
        // If it has source/target properties, treat it as an edge
        if (nodeOrEdge.source && nodeOrEdge.target) {
            setEdges((prev) => prev.filter((e) => e.id !== nodeOrEdge.id));
        } else {
            setNodes((prev) => prev.filter((n) => n.id !== nodeOrEdge.id));
            setEdges((prev) => prev.filter((e) => e.source !== nodeOrEdge.id && e.target !== nodeOrEdge.id));
        }
        setDirty(true);
    }, [setNodes, setEdges]);

    const handleChangeColor = useCallback((node) => {
        if (!node) return;
        const colors = ['#8B5CF6', '#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#EC4899', '#6366F1'];
        const current = node.data?.color || NODE_COLORS[node.type] || '#6B7280';
        const idx = colors.indexOf(current);
        const next = colors[(idx + 1) % colors.length];
        setNodes((prev) =>
            prev.map((n) =>
                n.id === node.id
                    ? { ...n, data: { ...n.data, color: next } }
                    : n
            )
        );
        setDirty(true);
    }, [setNodes]);

    const handleAddChart = useCallback(async (node) => {
        if (!node) return;
        const ticker = node.data?.ticker || node.data?.label || '';
        if (!ticker) return;
        const chartId = `chart-${ticker}-${Date.now()}`;
        const pos = node.position || { x: 0, y: 0 };
        const chartNode = {
            id: chartId,
            type: 'chart',
            position: { x: pos.x + 280, y: pos.y },
            data: { label: `${ticker} Price`, ticker, prices: [] },
        };
        const edgeId = `edge-${node.id}-${chartId}`;
        const chartEdge = {
            id: edgeId,
            source: node.id,
            target: chartId,
            type: 'smoothstep',
            label: 'price',
            style: { stroke: NODE_COLORS.chart, strokeWidth: 1.5 },
        };
        setNodes((prev) => [...prev, chartNode]);
        setEdges((prev) => [...prev, chartEdge]);
        setDirty(true);
        try {
            const prices = await api.getCanvasChartPrices(ticker);
            setNodes((prev) =>
                prev.map((n) =>
                    n.id === chartId ? { ...n, data: { ...n.data, prices } } : n
                )
            );
        } catch (err) {
            console.error('Failed to fetch chart prices:', err);
        }
    }, [setNodes, setEdges]);

    const handleAddTimeline = useCallback(async (node) => {
        if (!node) return;
        const ticker = node.data?.ticker || node.data?.label || '';
        if (!ticker) return;
        const tlId = `timeline-${ticker}-${Date.now()}`;
        const pos = node.position || { x: 0, y: 0 };
        const tlNode = {
            id: tlId,
            type: 'timeline',
            position: { x: pos.x + 280, y: pos.y + 120 },
            data: { label: `${ticker} Events`, ticker, events: [] },
        };
        const edgeId = `edge-${node.id}-${tlId}`;
        const tlEdge = {
            id: edgeId,
            source: node.id,
            target: tlId,
            type: 'smoothstep',
            label: 'events',
            style: { stroke: NODE_COLORS.timeline, strokeWidth: 1.5 },
        };
        setNodes((prev) => [...prev, tlNode]);
        setEdges((prev) => [...prev, tlEdge]);
        setDirty(true);
        try {
            const events = await api.getCanvasTimelineEvents(ticker);
            setNodes((prev) =>
                prev.map((n) =>
                    n.id === tlId ? { ...n, data: { ...n.data, events } } : n
                )
            );
        } catch (err) {
            console.error('Failed to fetch timeline events:', err);
        }
    }, [setNodes, setEdges]);

    const onNodeContextMenu = useCallback((event, node) => {
        event.preventDefault();
        setContextMenu({ x: event.clientX, y: event.clientY, node, edge: null });
    }, []);

    const onEdgeContextMenu = useCallback((event, edge) => {
        event.preventDefault();
        setContextMenu({ x: event.clientX, y: event.clientY, node: null, edge });
    }, []);

    const onPaneClick = useCallback(() => {
        setContextMenu(null);
    }, []);

    // Track selected nodes for context menu (explain requires 2 selected)
    const selectedNodes = useMemo(
        () => nodes.filter((n) => n.selected),
        [nodes],
    );

    const selectBoard = (boardId) => {
        setCurrentBoardId(boardId);
        setPickerOpen(false);
    };

    const miniMapNodeColor = (node) => NODE_COLORS[node.type] || '#6B7280';

    if (!currentBoardId && boards.length === 0 && !loading) {
        return (
            <div style={{
                height: 'calc(100vh - 60px)',
                background: '#080C10',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                justifyContent: 'center',
                color: '#5A7080',
                fontFamily: "'IBM Plex Sans', sans-serif",
                gap: 12,
            }}>
                <div style={{ fontSize: 16 }}>No investigation boards yet.</div>
                <button onClick={handleNewBoard} style={{ ...btnBase, background: '#3B82F6', border: 'none', color: '#fff' }}>
                    <Plus size={14} /> Create Board
                </button>
            </div>
        );
    }

    return (
        <div style={{ height: 'calc(100vh - 60px)', background: '#080C10' }}>
            <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChangeWrapped}
                onEdgesChange={onEdgesChange}
                onConnect={onConnect}
                onNodeContextMenu={onNodeContextMenu}
                onEdgeContextMenu={onEdgeContextMenu}
                onPaneClick={onPaneClick}
                nodeTypes={nodeTypes}
                defaultEdgeOptions={defaultEdgeOptions}
                fitView
                proOptions={{ hideAttribution: true }}
                style={{ background: '#080C10' }}
            >
                <Background color="#1E2A3A" gap={20} size={1} />
                <Controls
                    style={{ background: '#0D1117', border: '1px solid #1E2A3A', borderRadius: 6 }}
                />
                <MiniMap
                    style={minimapStyle}
                    nodeColor={miniMapNodeColor}
                    maskColor="rgba(0,0,0,0.6)"
                />
                <Panel position="top-left" style={{ display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap' }}>
                    {/* Board picker */}
                    <div ref={pickerRef} style={{ position: 'relative' }}>
                        <button
                            onClick={() => setPickerOpen(!pickerOpen)}
                            style={{ ...btnBase, minWidth: 140, justifyContent: 'space-between' }}
                        >
                            <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 120 }}>
                                {currentBoard?.name || 'Select board'}
                                {dirty ? ' *' : ''}
                            </span>
                            <ChevronDown size={12} />
                        </button>
                        {pickerOpen && (
                            <div style={{
                                position: 'absolute',
                                top: '100%',
                                left: 0,
                                marginTop: 4,
                                background: '#0D1117',
                                border: '1px solid #1E2A3A',
                                borderRadius: 6,
                                minWidth: 180,
                                zIndex: 50,
                                boxShadow: '0 8px 24px rgba(0,0,0,0.5)',
                                maxHeight: 300,
                                overflowY: 'auto',
                            }}>
                                {boards.map((b) => {
                                    const bid = b.board_id || b.id;
                                    return (
                                        <div
                                            key={bid}
                                            onClick={() => selectBoard(bid)}
                                            style={{
                                                padding: '6px 12px',
                                                fontSize: 12,
                                                color: bid === currentBoardId ? '#3B82F6' : '#C8D8E8',
                                                cursor: 'pointer',
                                                fontFamily: "'IBM Plex Sans', sans-serif",
                                                borderBottom: '1px solid #1E2A3A',
                                                fontWeight: bid === currentBoardId ? 600 : 400,
                                            }}
                                            onMouseEnter={(e) => { e.currentTarget.style.background = '#161B22'; }}
                                            onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                                        >
                                            {b.name}
                                        </div>
                                    );
                                })}
                            </div>
                        )}
                    </div>

                    <button onClick={handleNewBoard} style={btnBase} title="New board">
                        <Plus size={14} /> New
                    </button>
                    <button onClick={saveGraph} style={btnBase} title="Save (Ctrl+S)">
                        <Save size={14} /> Save
                    </button>
                    <button onClick={handleAddNote} style={btnBase} title="Add note">
                        <StickyNote size={14} /> Note
                    </button>
                    <button
                        onClick={handleExpandSelected}
                        style={{ ...btnBase, color: '#8B5CF6', borderColor: '#8B5CF6' }}
                        title="Expand selected actor node"
                        disabled={expanding}
                    >
                        <Network size={14} /> {expanding ? 'Expanding...' : 'Expand'}
                    </button>
                    <button
                        onClick={handleSuggestConnections}
                        style={{ ...btnBase, color: '#F59E0B', borderColor: '#F59E0B' }}
                        title="Suggest connections between actors"
                    >
                        <Zap size={14} /> Suggest
                    </button>
                    <button
                        onClick={() => setSearchOpen((v) => !v)}
                        style={{
                            ...btnBase,
                            color: searchOpen ? '#fff' : '#3B82F6',
                            borderColor: '#3B82F6',
                            background: searchOpen ? '#3B82F6' : '#161B22',
                        }}
                        title="Intelligence search"
                    >
                        <SearchIcon size={14} /> Search
                    </button>
                    <button
                        onClick={handleDeleteBoard}
                        style={{ ...btnBase, color: '#EF4444', borderColor: '#EF4444' }}
                        title="Delete board"
                    >
                        <Trash2 size={14} />
                    </button>
                </Panel>

                {loading && (
                    <Panel position="top-center">
                        <div style={{
                            padding: '6px 16px',
                            background: '#0D1117',
                            border: '1px solid #1E2A3A',
                            borderRadius: 6,
                            fontSize: 12,
                            color: '#5A7080',
                            fontFamily: "'IBM Plex Sans', sans-serif",
                        }}>
                            Loading...
                        </div>
                    </Panel>
                )}

                {explaining && (
                    <Panel position="bottom-center">
                        <div style={{
                            padding: '6px 16px',
                            background: '#1E1040',
                            border: '1px solid #7C3AED',
                            borderRadius: 6,
                            fontSize: 12,
                            color: '#A78BFA',
                            fontFamily: "'IBM Plex Sans', sans-serif",
                            display: 'flex',
                            alignItems: 'center',
                            gap: 8,
                        }}>
                            <span style={{
                                width: 8,
                                height: 8,
                                borderRadius: '50%',
                                background: '#A78BFA',
                                display: 'inline-block',
                                animation: 'pulse 1.5s ease-in-out infinite',
                            }} />
                            LLM analyzing connection...
                        </div>
                    </Panel>
                )}
            </ReactFlow>

            {contextMenu && (
                <CanvasContextMenu
                    x={contextMenu.x}
                    y={contextMenu.y}
                    node={contextMenu.node}
                    edge={contextMenu.edge}
                    selectedNodes={selectedNodes}
                    explaining={explaining}
                    onClose={() => setContextMenu(null)}
                    onExpand={handleExpandNode}
                    onRemove={handleRemoveNode}
                    onChangeColor={handleChangeColor}
                    onSuggestConnections={handleSuggestConnections}
                    onExplainConnection={handleExplainConnection}
                    onAddChart={handleAddChart}
                    onAddTimeline={handleAddTimeline}
                />
            )}

            {searchOpen && (
                <IntelligenceSearch
                    onClose={() => setSearchOpen(false)}
                    onAddToCanvas={handleAddFromSearch}
                />
            )}
        </div>
    );
}

export default Canvas;
