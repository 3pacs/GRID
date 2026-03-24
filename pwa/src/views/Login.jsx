import React, { useState, useEffect, useRef } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';

const styles = {
    container: {
        display: 'flex', flexDirection: 'column', alignItems: 'center',
        justifyContent: 'center', minHeight: '100vh', padding: '24px',
        background: `
            linear-gradient(rgba(8,12,16,0.95), rgba(8,12,16,0.95)),
            repeating-linear-gradient(0deg, transparent, transparent 40px, #1A284010 40px, #1A284010 41px),
            repeating-linear-gradient(90deg, transparent, transparent 40px, #1A284010 40px, #1A284010 41px)
        `,
    },
    card: {
        width: '100%', maxWidth: '360px', padding: '40px 24px',
        background: '#0D1520', borderRadius: '16px', border: '1px solid #1A2840',
    },
    wordmark: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '36px',
        fontWeight: 700, color: '#1A6EBF', textAlign: 'center',
        letterSpacing: '8px', marginBottom: '40px',
    },
    inputWrap: {
        position: 'relative', marginBottom: '16px',
    },
    input: {
        width: '100%', padding: '14px 44px 14px 16px', borderRadius: '8px',
        border: '1px solid #1A2840', background: '#080C10', color: '#C8D8E8',
        fontFamily: "'IBM Plex Sans', sans-serif", fontSize: '16px', outline: 'none',
    },
    toggle: {
        position: 'absolute', right: '12px', top: '50%', transform: 'translateY(-50%)',
        background: 'none', border: 'none', color: '#5A7080', cursor: 'pointer',
        fontSize: '13px', fontFamily: "'IBM Plex Sans', sans-serif",
        minWidth: '44px', minHeight: '44px', display: 'flex', alignItems: 'center',
        justifyContent: 'center',
    },
    button: {
        width: '100%', padding: '14px', borderRadius: '8px', border: 'none',
        background: '#1A6EBF', color: '#fff', fontFamily: "'JetBrains Mono', monospace",
        fontSize: '14px', fontWeight: 600, letterSpacing: '2px', cursor: 'pointer',
        minHeight: '44px',
    },
    error: {
        color: '#8B1F1F', fontSize: '13px', textAlign: 'center', marginTop: '12px',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    modeToggle: {
        textAlign: 'center', marginTop: '16px', fontSize: '12px',
        color: '#5A7080', cursor: 'pointer',
    },
};

export default function Login() {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [showPassword, setShowPassword] = useState(false);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [useUserLogin, setUseUserLogin] = useState(false);
    const inputRef = useRef(null);
    const setAuth = useStore(s => s.setAuth);

    useEffect(() => {
        inputRef.current?.focus();
    }, [useUserLogin]);

    const handleSubmit = async (e) => {
        e?.preventDefault();
        if (!password || loading) return;
        setLoading(true);
        setError('');
        try {
            const data = useUserLogin
                ? await api.login(password, username)
                : await api.login(password);
            setAuth(data.token, data.role, data.username);
        } catch (err) {
            setError(err.message || 'Authentication failed');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div style={styles.container}>
            <form style={styles.card} onSubmit={handleSubmit}>
                <div style={styles.wordmark}>GRID</div>
                {useUserLogin && (
                    <div style={styles.inputWrap}>
                        <input
                            ref={inputRef}
                            type="text"
                            value={username}
                            onChange={e => setUsername(e.target.value)}
                            placeholder="Username"
                            style={styles.input}
                            autoComplete="username"
                        />
                    </div>
                )}
                <div style={styles.inputWrap}>
                    <input
                        ref={useUserLogin ? null : inputRef}
                        type={showPassword ? 'text' : 'password'}
                        value={password}
                        onChange={e => setPassword(e.target.value)}
                        placeholder="Password"
                        style={styles.input}
                        autoComplete="current-password"
                    />
                    <button
                        type="button"
                        onClick={() => setShowPassword(!showPassword)}
                        style={styles.toggle}
                    >
                        {showPassword ? 'HIDE' : 'SHOW'}
                    </button>
                </div>
                <button type="submit" disabled={loading} style={{
                    ...styles.button,
                    opacity: loading ? 0.6 : 1,
                }}>
                    {loading ? '...' : 'AUTHENTICATE'}
                </button>
                {error && <div style={styles.error}>{error}</div>}
                <div
                    style={styles.modeToggle}
                    onClick={() => { setUseUserLogin(!useUserLogin); setError(''); }}
                >
                    {useUserLogin ? 'Use master password instead' : 'Log in with username'}
                </div>
            </form>
        </div>
    );
}
