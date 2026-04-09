import React, { useState, useRef } from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, glowNodeStyle, labelStyle, handleStyle } from './nodeStyles.js';

const color = NODE_COLORS.note;

function NoteNode({ data, selected }) {
    const [editing, setEditing] = useState(false);
    const [text, setText] = useState(data.label || '');
    const textareaRef = useRef(null);

    const handleDoubleClick = () => {
        setEditing(true);
        setTimeout(() => textareaRef.current?.focus(), 0);
    };

    const handleBlur = () => {
        setEditing(false);
        if (data.onLabelChange) data.onLabelChange(text);
    };

    return (
        <div
            style={{ ...glowNodeStyle('note', selected), minWidth: 180, maxWidth: 280 }}
            onDoubleClick={handleDoubleClick}
        >
            <Handle type="target" position={Position.Left} style={handleStyle(color)} />
            {editing ? (
                <textarea
                    ref={textareaRef}
                    value={text}
                    onChange={(e) => setText(e.target.value)}
                    onBlur={handleBlur}
                    onKeyDown={(e) => { if (e.key === 'Escape') handleBlur(); }}
                    style={{
                        width: '100%',
                        minHeight: 60,
                        background: '#161B22',
                        color: '#C8D8E8',
                        border: '1px solid #3B82F6',
                        borderRadius: 4,
                        fontFamily: "'IBM Plex Sans', sans-serif",
                        fontSize: 12,
                        padding: 4,
                        resize: 'vertical',
                        outline: 'none',
                    }}
                />
            ) : (
                <div style={{ ...labelStyle, whiteSpace: 'pre-wrap', wordWrap: 'break-word', cursor: 'text' }}>
                    {text || 'Double-click to edit...'}
                </div>
            )}
            <Handle type="source" position={Position.Right} style={handleStyle(color)} />
        </div>
    );
}

export default React.memo(NoteNode);
