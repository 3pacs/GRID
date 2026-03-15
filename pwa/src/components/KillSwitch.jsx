import React, { useState } from 'react';

export default function KillSwitch({ modelId, modelName, onKill }) {
    const [showConfirm, setShowConfirm] = useState(false);
    const [confirmText, setConfirmText] = useState('');

    const canConfirm = confirmText === modelName;

    if (!showConfirm) {
        return (
            <button
                onClick={() => setShowConfirm(true)}
                style={{
                    width: '100%', padding: '12px', borderRadius: '8px',
                    border: '1px solid #8B1F1F', background: '#8B1F1F22',
                    color: '#8B1F1F', fontFamily: "'JetBrains Mono', monospace",
                    fontSize: '14px', fontWeight: 600, cursor: 'pointer',
                    minHeight: '44px',
                }}
            >
                KILL MODEL
            </button>
        );
    }

    return (
        <div style={{
            background: '#0D1520', borderRadius: '12px', padding: '16px',
            border: '1px solid #8B1F1F44',
        }}>
            <div style={{
                fontSize: '14px', color: '#C8D8E8', marginBottom: '12px',
                fontFamily: "'IBM Plex Sans', sans-serif", lineHeight: 1.5,
            }}>
                This will retire <strong>{modelName}</strong> permanently.
                Type the model name to confirm.
            </div>
            <input
                type="text"
                value={confirmText}
                onChange={e => setConfirmText(e.target.value)}
                placeholder={modelName}
                style={{
                    width: '100%', padding: '10px 12px', borderRadius: '6px',
                    border: '1px solid #1A2840', background: '#080C10',
                    color: '#C8D8E8', fontFamily: "'JetBrains Mono', monospace",
                    fontSize: '14px', marginBottom: '12px', outline: 'none',
                }}
            />
            <div style={{ display: 'flex', gap: '12px' }}>
                <button
                    onClick={() => { setShowConfirm(false); setConfirmText(''); }}
                    style={{
                        flex: 1, padding: '10px', borderRadius: '8px',
                        border: '1px solid #1A2840', background: 'transparent',
                        color: '#5A7080', cursor: 'pointer', minHeight: '44px',
                        fontFamily: "'JetBrains Mono', monospace", fontSize: '13px',
                    }}
                >
                    CANCEL
                </button>
                <button
                    disabled={!canConfirm}
                    onClick={() => { onKill(modelId); setShowConfirm(false); setConfirmText(''); }}
                    style={{
                        flex: 1, padding: '10px', borderRadius: '8px',
                        border: 'none', background: canConfirm ? '#8B1F1F' : '#333',
                        color: canConfirm ? '#fff' : '#666', cursor: canConfirm ? 'pointer' : 'not-allowed',
                        fontFamily: "'JetBrains Mono', monospace", fontSize: '13px',
                        fontWeight: 600, minHeight: '44px',
                    }}
                >
                    CONFIRM KILL
                </button>
            </div>
        </div>
    );
}
