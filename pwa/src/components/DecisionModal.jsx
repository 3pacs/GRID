import React from 'react';

const styles = {
    overlay: {
        position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
        background: 'rgba(0,0,0,0.7)', zIndex: 200,
        display: 'flex', alignItems: 'flex-end', justifyContent: 'center',
    },
    modal: {
        background: '#0D1520', borderRadius: '16px 16px 0 0',
        padding: '24px', width: '100%', maxWidth: '500px',
        border: '1px solid #1A2840', borderBottom: 'none',
        paddingBottom: 'calc(24px + env(safe-area-inset-bottom, 0px))',
    },
    title: {
        fontFamily: "'JetBrains Mono', monospace",
        fontSize: '16px', fontWeight: 600, color: '#C8D8E8',
        marginBottom: '16px',
    },
    body: {
        fontSize: '14px', color: '#5A7080', lineHeight: 1.6,
        marginBottom: '20px', fontFamily: "'IBM Plex Sans', sans-serif",
    },
    btnRow: {
        display: 'flex', gap: '12px',
    },
    btnConfirm: {
        flex: 1, padding: '12px', borderRadius: '8px', border: 'none',
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        fontWeight: 600, cursor: 'pointer', minHeight: '44px',
    },
    btnCancel: {
        flex: 1, padding: '12px', borderRadius: '8px',
        border: '1px solid #1A2840', background: 'transparent',
        color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
        fontSize: '14px', cursor: 'pointer', minHeight: '44px',
    },
};

export default function DecisionModal({ title, body, confirmLabel = 'CONFIRM', confirmColor = '#1A6EBF', onConfirm, onCancel }) {
    return (
        <div style={styles.overlay} onClick={onCancel}>
            <div style={styles.modal} onClick={e => e.stopPropagation()}>
                <div style={styles.title}>{title}</div>
                <div style={styles.body}>{body}</div>
                <div style={styles.btnRow}>
                    <button style={styles.btnCancel} onClick={onCancel}>CANCEL</button>
                    <button style={{ ...styles.btnConfirm, background: confirmColor, color: '#fff' }}
                        onClick={onConfirm}>
                        {confirmLabel}
                    </button>
                </div>
            </div>
        </div>
    );
}
