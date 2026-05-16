import '@testing-library/jest-dom';

function createStorageShim() {
    const state = new Map();

    return {
        getItem(key) {
            const normalized = String(key);
            return state.has(normalized) ? state.get(normalized) : null;
        },
        setItem(key, value) {
            state.set(String(key), String(value));
        },
        removeItem(key) {
            state.delete(String(key));
        },
        clear() {
            state.clear();
        },
    };
}

function hasWebStorageShape(storage) {
    return storage
        && typeof storage.getItem === 'function'
        && typeof storage.setItem === 'function'
        && typeof storage.removeItem === 'function'
        && typeof storage.clear === 'function';
}

if (!hasWebStorageShape(globalThis.localStorage)) {
    const localStorageShim = createStorageShim();

    Object.defineProperty(globalThis, 'localStorage', {
        value: localStorageShim,
        configurable: true,
    });

    if (typeof window !== 'undefined') {
        Object.defineProperty(window, 'localStorage', {
            value: localStorageShim,
            configurable: true,
        });
    }
}
