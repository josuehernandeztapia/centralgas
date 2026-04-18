// Central Gas Dashboard — Service Worker (PWA)
// HU-DASH-X.4: Minimal offline-first cache for the dashboard shell

const CACHE_NAME = 'central-gas-v1';
const SHELL_URLS = [
  '/dashboard',
  '/static/manifest.json',
];

// External CDN assets to cache for offline shell
const CDN_URLS = [
  'https://fonts.googleapis.com/css2?family=Rajdhani:wght@300;400;600;700&family=DM+Sans:wght@300;400;500;600;700&display=swap',
  'https://cdn.tailwindcss.com',
  'https://unpkg.com/react@18.2.0/umd/react.production.min.js',
  'https://unpkg.com/react-dom@18.2.0/umd/react-dom.production.min.js',
  'https://unpkg.com/prop-types@15.8.1/prop-types.min.js',
  'https://unpkg.com/recharts@2.12.7/umd/Recharts.js',
  'https://unpkg.com/@babel/standalone/babel.min.js',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      // Cache shell URLs (these are same-origin, should always succeed)
      return cache.addAll(SHELL_URLS).catch(() => {});
    })
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // API calls: network-first (never serve stale data for /api, /stats, etc.)
  if (url.pathname.startsWith('/api') || url.pathname === '/stats' ||
      url.pathname === '/transactions' || url.pathname === '/health') {
    event.respondWith(fetch(event.request));
    return;
  }

  // Everything else: cache-first, fallback to network
  event.respondWith(
    caches.match(event.request).then((cached) => {
      if (cached) return cached;
      return fetch(event.request).then((response) => {
        // Cache successful GET responses
        if (response.ok && event.request.method === 'GET') {
          const clone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
        }
        return response;
      });
    }).catch(() => {
      // Offline fallback for navigation
      if (event.request.mode === 'navigate') {
        return caches.match('/dashboard');
      }
    })
  );
});
