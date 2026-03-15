import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import SignalCard from '../components/SignalCard.jsx';

const styles = {
    container: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    title: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        color: '#5A7080', letterSpacing: '2px', marginBottom: '16px',
    },
    grid: {
        display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))',
        gap: '10px',
    },
    empty: {
        color: '#5A7080', textAlign: 'center', padding: '40px',
        fontFamily: "'IBM Plex Sans', sans-serif", fontSize: '14px',
    },
};

export default function Signals() {
    const [signals, setSignals] = useState(null);
    const [snapshot, setSnapshot] = useState([]);

    useEffect(() => {
        api.getCurrent().then(setSignals).catch(() => {});
        api.getStatus().then(d => {
            // Feature snapshot
        }).catch(() => {});
    }, []);

    return (
        <div style={styles.container}>
            <div style={styles.title}>LIVE SIGNALS</div>
            {signals?.top_drivers?.length > 0 ? (
                <div style={styles.grid}>
                    {signals.top_drivers.map((d, i) => (
                        <SignalCard
                            key={i}
                            name={d.feature}
                            value={d.magnitude}
                            direction={d.direction}
                            magnitude={d.magnitude}
                        />
                    ))}
                </div>
            ) : (
                <div style={styles.empty}>
                    {signals?.state === 'UNCALIBRATED'
                        ? 'No production model — signals unavailable'
                        : 'No active signals'}
                </div>
            )}
        </div>
    );
}
