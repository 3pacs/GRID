import { useEffect, useState } from 'react';
import api from '../api.js';

export default function useAstrogridSnapshot(selectedDate, enabled = true) {
    const [snapshot, setSnapshot] = useState(null);
    const [status, setStatus] = useState(enabled ? 'loading' : 'idle');
    const [error, setError] = useState(null);

    useEffect(() => {
        let cancelled = false;

        if (!enabled) {
            setSnapshot(null);
            setStatus('idle');
            setError(null);
            return () => {
                cancelled = true;
            };
        }

        setStatus('loading');
        setError(null);
        api.getSnapshot(selectedDate)
            .then((payload) => {
                if (cancelled) return;
                setSnapshot(payload);
                setStatus('live');
            })
            .catch((nextError) => {
                if (cancelled) return;
                setSnapshot(null);
                setStatus('error');
                setError(nextError.message);
            });

        return () => {
            cancelled = true;
        };
    }, [enabled, selectedDate]);

    return { snapshot, status, error };
}
