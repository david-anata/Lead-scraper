"""Installable employee web-app entrypoints.

The service worker never caches authenticated HR pages or employee data. It
only provides the branded icon and a generic offline explanation.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from sales_support_agent.services.auth_deps import require_tool


router = APIRouter()
_hr_guard = require_tool("hr.access")


@router.get("/app", include_in_schema=False)
async def employee_app_entry(_user: dict = Depends(_hr_guard)):
    return RedirectResponse("/admin/hr", status_code=303)


@router.get("/app.webmanifest", include_in_schema=False)
async def employee_app_manifest():
    return JSONResponse(
        {
            "id": "/app",
            "name": "Anata Employee",
            "short_name": "Anata",
            "description": "Secure employee time, onboarding, PTO, policies, and pay records.",
            "start_url": "/app",
            "scope": "/",
            "display": "standalone",
            "background_color": "#f9f7f3",
            "theme_color": "#2b3644",
            "icons": [
                {
                    "src": "/brand-static/agent-favicon.png",
                    "sizes": "563x563",
                    "type": "image/png",
                    "purpose": "any",
                },
                {
                    "src": "/brand-static/agent-favicon.png",
                    "sizes": "563x563",
                    "type": "image/png",
                    "purpose": "maskable",
                },
            ],
            "shortcuts": [
                {
                    "name": "Clock in or out",
                    "short_name": "Time",
                    "url": "/admin/hr/time",
                    "icons": [{"src": "/brand-static/agent-favicon.png", "sizes": "563x563"}],
                },
                {
                    "name": "Finish my information",
                    "short_name": "My information",
                    "url": "/admin/hr/onboarding",
                    "icons": [{"src": "/brand-static/agent-favicon.png", "sizes": "563x563"}],
                },
                {
                    "name": "View pay statements",
                    "short_name": "Pay",
                    "url": "/admin/hr/pay-statements",
                    "icons": [{"src": "/brand-static/agent-favicon.png", "sizes": "563x563"}],
                },
            ],
        },
        headers={"Cache-Control": "public, max-age=3600"},
        media_type="application/manifest+json",
    )


@router.get("/app/offline", include_in_schema=False)
async def employee_app_offline():
    return HTMLResponse(
        """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#2b3644"><title>Anata is offline</title>
<style>
body{margin:0;min-height:100vh;display:grid;place-items:center;background:#f9f7f3;
color:#2b3644;font-family:Inter,Segoe UI,sans-serif;padding:24px;box-sizing:border-box}
main{width:min(100%,480px);background:#fff;border:1px solid #dfe3e6;border-radius:20px;
padding:28px;box-sizing:border-box}img{width:52px;height:52px;border-radius:13px}
h1{font:800 25px/1.2 Montserrat,Inter,sans-serif;margin:18px 0 8px}
p{color:#5d6977;line-height:1.6;margin:0 0 18px}button{min-height:46px;border:0;border-radius:10px;
padding:0 18px;background:#2b3644;color:#fff;font-weight:700;cursor:pointer}
</style></head><body><main>
<img src="/brand-static/agent-favicon.png" alt="Agent icon">
<h1>You’re offline.</h1>
<p>Reconnect before opening time, W-4, PTO, or pay information. Anata does not store private HR records on this device for offline use.</p>
<button type="button" onclick="location.reload()">Try again</button>
</main></body></html>""",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/service-worker.js", include_in_schema=False)
async def employee_app_service_worker():
    script = r"""
const CACHE_NAME = 'anata-employee-shell-v1';
const SAFE_ASSETS = ['/app/offline', '/brand-static/agent-favicon.png'];
self.addEventListener('install', event => {
  event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.addAll(SAFE_ASSETS)));
  self.skipWaiting();
});
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(key => key !== CACHE_NAME).map(key => caches.delete(key))
    ))
  );
  self.clients.claim();
});
self.addEventListener('fetch', event => {
  if (event.request.method !== 'GET') return;
  const url = new URL(event.request.url);
  if (event.request.mode === 'navigate') {
    event.respondWith(fetch(event.request, {cache: 'no-store'})
      .catch(() => caches.match('/app/offline')));
    return;
  }
  if (url.origin === self.location.origin &&
      url.pathname === '/brand-static/agent-favicon.png') {
    event.respondWith(caches.match(event.request)
      .then(cached => cached || fetch(event.request)));
  }
});
"""
    return Response(
        script,
        media_type="application/javascript",
        headers={
            "Cache-Control": "no-cache",
            "Service-Worker-Allowed": "/",
        },
    )
