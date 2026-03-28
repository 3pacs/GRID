const CACHE_NAME = 'grid-v1';
const PRECACHE_URLS = [
    '/',
    '/manifest.json',
];

// IndexedDB helpers for offline journal queue
const IDB_NAME = 'grid-offline';
const IDB_VERSION = 1;
const JOURNAL_STORE = 'journal-queue';

function openDB() {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(IDB_NAME, IDB_VERSION);
        req.onupgradeneeded = () => {
            const db = req.result;
            if (!db.objectStoreNames.contains(JOURNAL_STORE)) {
                db.createObjectStore(JOURNAL_STORE, { keyPath: 'id', autoIncrement: true });
            }
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}

function addToQueue(entry) {
    return openDB().then(db => new Promise((resolve, reject) => {
        const tx = db.transaction(JOURNAL_STORE, 'readwrite');
        tx.objectStore(JOURNAL_STORE).add({ ...entry, queuedAt: Date.now() });
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    }));
}

function getAllQueued() {
    return openDB().then(db => new Promise((resolve, reject) => {
        const tx = db.transaction(JOURNAL_STORE, 'readonly');
        const req = tx.objectStore(JOURNAL_STORE).getAll();
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    }));
}

function removeFromQueue(id) {
    return openDB().then(db => new Promise((resolve, reject) => {
        const tx = db.transaction(JOURNAL_STORE, 'readwrite');
        tx.objectStore(JOURNAL_STORE).delete(id);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    }));
}

// Install: pre-cache shell
self.addEventListener('install', event => {
    event.waitUntil(
        caches.open(CACHE_NAME)
            .then(cache => cache.addAll(PRECACHE_URLS))
            .then(() => self.skipWaiting())
    );
});

// Activate: clean old caches
self.addEventListener('activate', event => {
    event.waitUntil(
        caches.keys().then(keys =>
            Promise.all(
                keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
            )
        ).then(() => self.clients.claim())
    );
});

// Fetch: network-first for API, cache-first for static
self.addEventListener('fetch', event => {
    const url = new URL(event.request.url);

    // Never cache API requests
    if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/ws')) {
        // Intercept journal POST when offline — queue for background sync
        if (url.pathname === '/api/v1/journal' && event.request.method === 'POST') {
            event.respondWith(
                fetch(event.request.clone()).catch(async () => {
                    const body = await event.request.json();
                    await addToQueue(body);
                    // Register for background sync if supported
                    if (self.registration.sync) {
                        await self.registration.sync.register('journal-sync');
                    }
                    return new Response(
                        JSON.stringify({ queued: true, message: 'Saved offline — will sync when reconnected' }),
                        { status: 202, headers: { 'Content-Type': 'application/json' } }
                    );
                })
            );
            return;
        }

        event.respondWith(
            fetch(event.request).catch(() =>
                new Response(JSON.stringify({ error: 'offline' }), {
                    headers: { 'Content-Type': 'application/json' }
                })
            )
        );
        return;
    }

    // For navigation (SPA): return cached shell
    if (event.request.mode === 'navigate') {
        event.respondWith(
            fetch(event.request)
                .then(response => {
                    const clone = response.clone();
                    caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
                    return response;
                })
                .catch(() => caches.match('/'))
        );
        return;
    }

    // Static assets: cache-first
    event.respondWith(
        caches.match(event.request).then(cached => {
            if (cached) return cached;
            return fetch(event.request).then(response => {
                if (response.ok) {
                    const clone = response.clone();
                    caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
                }
                return response;
            });
        })
    );
});

// Background sync for journal entries
self.addEventListener('sync', event => {
    if (event.tag === 'journal-sync') {
        event.waitUntil(syncJournalEntries());
    }
});

async function syncJournalEntries() {
    const entries = await getAllQueued();
    if (!entries.length) return;

    console.log(`Journal sync: ${entries.length} queued entries`);

    for (const entry of entries) {
        try {
            const { id, queuedAt, ...payload } = entry;
            const response = await fetch('/api/v1/journal', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            if (response.ok) {
                await removeFromQueue(id);
                console.log(`Journal sync: entry ${id} synced`);
            } else if (response.status >= 400 && response.status < 500) {
                // Client error — remove from queue, won't succeed on retry
                await removeFromQueue(id);
                console.warn(`Journal sync: entry ${id} rejected (${response.status}), removed`);
            }
            // 5xx errors — leave in queue for next sync attempt
        } catch (err) {
            console.warn(`Journal sync: entry ${entry.id} failed, will retry`, err);
            break; // Stop processing — likely still offline
        }
    }

    // Notify all clients that sync completed
    const clients = await self.clients.matchAll();
    for (const client of clients) {
        client.postMessage({ type: 'journal-synced', remaining: (await getAllQueued()).length });
    }
}

// Push notifications
self.addEventListener('push', (event) => {
    let title = 'GRID Intelligence';
    let options = {
        body: 'New notification',
        icon: '/icons/icon-192.png',
        badge: '/icons/icon-76.png',
    };

    if (event.data) {
        try {
            const data = event.data.json();
            title = data.title || title;
            options = {
                body: data.body || options.body,
                icon: data.icon || '/icons/icon-192.png',
                badge: data.badge || '/icons/icon-76.png',
                tag: data.tag || undefined,
                data: { url: data.url || '/' },
                vibrate: [100, 50, 100],
                actions: data.actions || [],
                requireInteraction: data.requireInteraction || false,
            };
        } catch {
            // Fallback for plain text payloads
            options.body = event.data.text();
        }
    }

    event.waitUntil(
        self.registration.showNotification(title, options)
    );
});

// Notification click — open the app at the specified URL
self.addEventListener('notificationclick', (event) => {
    event.notification.close();

    const targetUrl = event.notification.data?.url || '/';

    event.waitUntil(
        self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientList) => {
            // Focus an existing window if one is open
            for (const client of clientList) {
                if (client.url.includes(self.location.origin) && 'focus' in client) {
                    client.navigate(targetUrl);
                    return client.focus();
                }
            }
            // Otherwise open a new window
            return self.clients.openWindow(targetUrl);
        })
    );
});
