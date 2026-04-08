import React, { useState, useEffect, useRef } from 'react';
import { Grid3X3 } from 'lucide-react';
import { api } from '../api.js';

/**
 * Reusable "Send to Canvas" button.
 * Props: type (actor|company|hypothesis|signal|note), entityId, label, data
 */
function SendToCanvas({ type, entityId, label, data }) {
    const [open, setOpen] = useState(false);
    const [boards, setBoards] = useState([]);
    const [sent, setSent] = useState(false);
    const [loading, setLoading] = useState(false);
    const ref = useRef(null);

    useEffect(() => {
        if (!open) return;
        const handleClickOutside = (e) => {
            if (ref.current && !ref.current.contains(e.target)) {
                setOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, [open]);

    const fetchBoards = async () => {
        try {
            const res = await api.getCanvasBoards();
            setBoards(res.boards || res || []);
        } catch {
            setBoards([]);
        }
    };

    const handleToggle = async () => {
        if (sent) return;
        if (!open) {
            await fetchBoards();
        }
        setOpen(!open);
    };

    const sendToBoard = async (boardId) => {
        setLoading(true);
        try {
            await api.addCanvasNode(boardId, {
                node_type: type,
                entity_id: entityId || null,
                label: label || type,
                position_x: 100 + Math.random() * 400,
                position_y: 100 + Math.random() * 300,
                data: data || {},
            });
            setOpen(false);
            setSent(true);
            setTimeout(() => setSent(false), 1200);
        } catch (err) {
            console.error('Failed to send node to canvas:', err);
        } finally {
            setLoading(false);
        }
    };

    const createAndSend = async () => {
        const name = prompt('Board name:');
        if (!name) return;
        setLoading(true);
        try {
            const res = await api.createCanvasBoard(name);
            const boardId = res.board_id || res.id;
            await sendToBoard(boardId);
        } catch (err) {
            console.error('Failed to create board:', err);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div ref={ref} style={{ position: 'relative', display: 'inline-block' }}>
            <button
                onClick={handleToggle}
                disabled={loading}
                style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 4,
                    padding: '3px 8px',
                    fontSize: 11,
                    fontFamily: "'IBM Plex Sans', sans-serif",
                    color: sent ? '#10B981' : '#5A7080',
                    background: 'transparent',
                    border: '1px solid #1E2A3A',
                    borderRadius: 4,
                    cursor: loading ? 'wait' : 'pointer',
                    transition: 'color 0.2s',
                }}
            >
                <Grid3X3 size={12} />
                {sent ? 'Sent!' : 'Canvas'}
            </button>
            {open && (
                <div style={{
                    position: 'absolute',
                    top: '100%',
                    left: 0,
                    marginTop: 4,
                    background: '#0D1117',
                    border: '1px solid #1E2A3A',
                    borderRadius: 6,
                    minWidth: 160,
                    zIndex: 999,
                    boxShadow: '0 8px 24px rgba(0,0,0,0.5)',
                }}>
                    {boards.map((b) => (
                        <div
                            key={b.board_id || b.id}
                            onClick={() => sendToBoard(b.board_id || b.id)}
                            style={{
                                padding: '6px 12px',
                                fontSize: 12,
                                color: '#C8D8E8',
                                cursor: 'pointer',
                                fontFamily: "'IBM Plex Sans', sans-serif",
                                borderBottom: '1px solid #1E2A3A',
                            }}
                            onMouseEnter={(e) => { e.currentTarget.style.background = '#161B22'; }}
                            onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                        >
                            {b.name}
                        </div>
                    ))}
                    <div
                        onClick={createAndSend}
                        style={{
                            padding: '6px 12px',
                            fontSize: 12,
                            color: '#3B82F6',
                            cursor: 'pointer',
                            fontFamily: "'IBM Plex Sans', sans-serif",
                        }}
                        onMouseEnter={(e) => { e.currentTarget.style.background = '#161B22'; }}
                        onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                    >
                        + New board...
                    </div>
                </div>
            )}
        </div>
    );
}

export default React.memo(SendToCanvas);
