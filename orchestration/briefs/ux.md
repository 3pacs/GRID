# GRID UX Brief — For Gemini / Google AI

You are designing React components for GRID, a trading intelligence dashboard.

## Design System
- **Background:** #080C10 (near-black)
- **Text primary:** #C8D8E8 (cool light gray)
- **Text secondary:** #6A7A8A
- **Accent green:** #00E676 (bullish/positive)
- **Accent red:** #FF1744 (bearish/negative)
- **Accent blue:** #448AFF (neutral/info)
- **Accent amber:** #FFAB00 (warning/caution)
- **Card background:** #0D1117
- **Card border:** #1E2A3A
- **Font:** 'IBM Plex Sans', system stack
- **Radius:** 8px cards, 4px buttons
- **Spacing:** 8px base grid

## Stack
- React 18 functional components
- Zustand for state (import useStore from '../store.js')
- Lucide React for icons (import { IconName } from 'lucide-react')
- Inline styles (no CSS files, no Tailwind, no styled-components)
- API calls via `import { api } from '../api.js'` — returns { get, post, put }

## Existing Component Pattern
```jsx
import React, { useState, useEffect } from 'react';
import useStore from '../store.js';
import { api } from '../api.js';
import { Activity } from 'lucide-react';

const styles = {
    container: {
        padding: 16,
        maxWidth: 1200,
        margin: '0 auto',
    },
    card: {
        background: '#0D1117',
        border: '1px solid #1E2A3A',
        borderRadius: 8,
        padding: 16,
        marginBottom: 12,
    },
    title: {
        fontSize: 18,
        fontWeight: 600,
        color: '#C8D8E8',
        marginBottom: 12,
        display: 'flex',
        alignItems: 'center',
        gap: 8,
    },
};

export default function MyComponent() {
    const token = useStore(s => s.token);
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        if (!token) return;
        api.get('/api/v1/endpoint', token)
            .then(setData)
            .catch(() => {})
            .finally(() => setLoading(false));
    }, [token]);

    if (loading) return <div style={styles.container}>Loading...</div>;

    return (
        <div style={styles.container}>
            <div style={styles.card}>
                <div style={styles.title}>
                    <Activity size={18} />
                    Component Title
                </div>
                {/* content */}
            </div>
        </div>
    );
}
```

## Existing Views (for reference)
- Dashboard.jsx — main overview with regime state, signals, journal stats
- Regime.jsx — regime history chart, transition matrix, drivers
- Signals.jsx — signal cards with confidence meters
- Journal.jsx — decision log with filters
- Discovery.jsx — feature orthogonality, clustering visualization
- Briefings.jsx — LLM-generated market briefings
- Agents.jsx — TradingAgents multi-agent debate view
- Physics.jsx — physical economy indicators (VIIRS, patents, shipping)

## Your Task
<!-- PASTE YOUR SPECIFIC REQUEST HERE -->
<!-- Example: "Design a new Alerts view that shows data pull failures, -->
<!-- stale data warnings, and regime transition notifications" -->

## Output Format
Return a single .jsx file with:
1. A styles object at the top
2. A single default export functional component
3. All API endpoints it expects (I'll wire them up)
4. Any new Zustand store fields it needs (I'll add them)
