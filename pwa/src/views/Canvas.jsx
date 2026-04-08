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
import { NODE_COLORS } from '../components/canvas/nodeStyles.js';
import { Plus, Save, Trash2, StickyNote, ChevronDown } from 'lucide-react';

const nodeTypes = {
    actor: ActorNode,
    company: CompanyNode,
    hypothesis: HypothesisNode,
    signal: SignalNode,
    note: NoteNode,
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
                const list = res.boards || res || [];
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
            </ReactFlow>
        </div>
    );
}

export default Canvas;
