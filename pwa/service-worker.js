const CACHE_NAME = 'grid-v1';
const PRECACHE_URLS = [
    '/',
    '/manifest.json',
];

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
    // Retrieve queued entries from IndexedDB and POST them
    // This is a placeholder — actual IndexedDB logic lives in the app
    console.log('Journal sync triggered');
}

// Push notifications (future use)
self.addEventListener('push', event => {
    const data = event.data ? event.data.text() : 'GRID notification';
    event.waitUntil(
        self.registration.showNotification('GRID', {
            body: data,
            icon: '/icons/icon-192.png',
            badge: '/icons/icon-76.png',
        })
    );
});
