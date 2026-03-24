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
        letterSpacing: '8px', marginBottom: '32px',
    },
    tabRow: {
        display: 'flex', marginBottom: '24px', borderRadius: '8px',
        border: '1px solid #1A2840', overflow: 'hidden',
    },
    tab: {
        flex: 1, padding: '10px', border: 'none', cursor: 'pointer',
        fontFamily: "'JetBrains Mono', monospace", fontSize: '12px',
        fontWeight: 600, letterSpacing: '1px', textAlign: 'center',
        transition: 'background 0.2s, color 0.2s',
    },
    tabActive: {
        background: '#1A6EBF', color: '#fff',
    },
    tabInactive: {
        background: '#080C10', color: '#5A7080',
    },
    inputWrap: {
        position: 'relative', marginBottom: '16px',
    },
    input: {
        width: '100%', padding: '14px 44px 14px 16px', borderRadius: '8px',
        border: '1px solid #1A2840', background: '#080C10', color: '#C8D8E8',
        fontFamily: "'IBM Plex Sans', sans-serif", fontSize: '16px', outline: 'none',
        boxSizing: 'border-box',
    },
    inputFocused: {
        borderColor: '#1A6EBF',
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
        color: '#D44', fontSize: '13px', textAlign: 'center', marginTop: '12px',
        fontFamily: "'IBM Plex Sans', sans-serif", padding: '8px',
        background: '#8B1F1F20', borderRadius: '6px',
    },
    success: {
        color: '#4A9', fontSize: '13px', textAlign: 'center', marginTop: '12px',
        fontFamily: "'IBM Plex Sans', sans-serif", padding: '8px',
        background: '#1A7A4A20', borderRadius: '6px',
    },
    modeToggle: {
        textAlign: 'center', marginTop: '16px', fontSize: '12px',
        color: '#5A7080', cursor: 'pointer',
    },
    hint: {
        fontSize: '11px', color: '#4A6070', marginTop: '4px', marginBottom: '12px',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
};

export default function Login() {
    const [mode, setMode] = useState('login'); // 'login' | 'signup'
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [confirmPassword, setConfirmPassword] = useState('');
    const [showPassword, setShowPassword] = useState(false);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [useUserLogin, setUseUserLogin] = useState(false);
    const usernameRef = useRef(null);
    const passwordRef = useRef(null);
    const setAuth = useStore(s => s.setAuth);

    useEffect(() => {
        if (mode === 'signup' || useUserLogin) {
            usernameRef.current?.focus();
        } else {
            passwordRef.current?.focus();
        }
    }, [mode, useUserLogin]);

    const switchMode = (newMode) => {
        setMode(newMode);
        setError('');
        setPassword('');
        setConfirmPassword('');
        if (newMode === 'login') {
            setUseUserLogin(false);
        }
    };

    const handleSubmit = async (e) => {
        e?.preventDefault();
        if (loading) return;

        if (mode === 'signup') {
            if (!username || !password) return;
            if (username.length < 3 || username.length > 50) {
                setError('Username must be 3-50 characters');
                return;
            }
            if (password.length < 8) {
                setError('Password must be at least 8 characters');
                return;
            }
            if (password !== confirmPassword) {
                setError('Passwords do not match');
                return;
            }
        } else {
            if (!password) return;
            if (useUserLogin && !username) return;
        }

        setLoading(true);
        setError('');

        try {
            let data;
            if (mode === 'signup') {
                data = await api.register(username, password);
            } else if (useUserLogin) {
                data = await api.login(password, username);
            } else {
                data = await api.login(password);
            }
            setAuth(data.token, data.role, data.username);
        } catch (err) {
            setError(err.message || (mode === 'signup' ? 'Registration failed' : 'Authentication failed'));
        } finally {
            setLoading(false);
        }
    };

    const isSignup = mode === 'signup';

    return (
        <div style={styles.container}>
            <form style={styles.card} onSubmit={handleSubmit}>
                <div style={styles.wordmark}>GRID</div>

                <div style={styles.tabRow}>
                    <button
                        type="button"
                        onClick={() => switchMode('login')}
                        style={{
                            ...styles.tab,
                            ...(mode === 'login' ? styles.tabActive : styles.tabInactive),
                        }}
                    >
                        SIGN IN
                    </button>
                    <button
                        type="button"
                        onClick={() => switchMode('signup')}
                        style={{
                            ...styles.tab,
                            ...(mode === 'signup' ? styles.tabActive : styles.tabInactive),
                        }}
                    >
                        CREATE ACCOUNT
                    </button>
                </div>

                {(isSignup || useUserLogin) && (
                    <div style={styles.inputWrap}>
                        <input
                            ref={usernameRef}
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
                        ref={passwordRef}
                        type={showPassword ? 'text' : 'password'}
                        value={password}
                        onChange={e => setPassword(e.target.value)}
                        placeholder="Password"
                        style={styles.input}
                        autoComplete={isSignup ? 'new-password' : 'current-password'}
                    />
                    <button
                        type="button"
                        onClick={() => setShowPassword(!showPassword)}
                        style={styles.toggle}
                    >
                        {showPassword ? 'HIDE' : 'SHOW'}
                    </button>
                </div>

                {isSignup && (
                    <>
                        <div style={styles.inputWrap}>
                            <input
                                type={showPassword ? 'text' : 'password'}
                                value={confirmPassword}
                                onChange={e => setConfirmPassword(e.target.value)}
                                placeholder="Confirm password"
                                style={styles.input}
                                autoComplete="new-password"
                            />
                        </div>
                        <div style={styles.hint}>
                            Username: 3-50 characters. Password: 8+ characters.
                        </div>
                    </>
                )}

                <button type="submit" disabled={loading} style={{
                    ...styles.button,
                    opacity: loading ? 0.6 : 1,
                }}>
                    {loading ? '...' : (isSignup ? 'CREATE ACCOUNT' : 'AUTHENTICATE')}
                </button>

                {error && <div style={styles.error}>{error}</div>}

                {!isSignup && (
                    <div
                        style={styles.modeToggle}
                        onClick={() => { setUseUserLogin(!useUserLogin); setError(''); }}
                    >
                        {useUserLogin ? 'Use master password instead' : 'Log in with username'}
                    </div>
                )}
            </form>
        </div>
    );
}
