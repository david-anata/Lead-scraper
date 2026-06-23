"""On-request background trigger for the HubSpot sales sync.

Mirrors the dashboard sync pattern (``_start_remote_dashboard_sync`` in
``main.py``): a single-worker ``ThreadPoolExecutor`` on ``app.state``, a lock to
avoid concurrent runs, and a status dict the page/poll endpoint can read. Never
blocks the request — the page renders from the mirror tables and the sync
refreshes them in the background.
"""

from __future__ import annotations

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from sales_support_agent.integrations.hubspot import HubSpotClient
from sales_support_agent.models.database import kv_set_json, session_scope
from sales_support_agent.services.hubspot_sync.service import (
    SYNC_STATE_KEY,
    sync_hubspot_sales,
)

logger = logging.getLogger(__name__)


def _ensure_state(app) -> None:
    if getattr(app.state, "hubspot_sync_executor", None) is None:
        app.state.hubspot_sync_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="hubspot-sync"
        )
    if getattr(app.state, "hubspot_sync_lock", None) is None:
        app.state.hubspot_sync_lock = Lock()
    if getattr(app.state, "hubspot_sync_future", None) is None:
        app.state.hubspot_sync_future = None
    if getattr(app.state, "hubspot_sync_last_result", None) is None:
        app.state.hubspot_sync_last_result = {}


def _run_sync(app) -> dict[str, Any]:
    settings = app.state.settings
    client = HubSpotClient(settings)
    try:
        with session_scope(app.state.session_factory) as session:
            result = sync_hubspot_sales(session, client, settings)
        payload = result.as_dict()
    except Exception as exc:  # noqa: BLE001
        logger.exception("[hubspot_sync] background run failed")
        payload = {
            "ok": False,
            "errors": [str(exc)],
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
    app.state.hubspot_sync_last_result = payload
    # Persist outside the sync transaction (kv helpers use their own engine).
    try:
        kv_set_json(SYNC_STATE_KEY, payload)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[hubspot_sync] failed to persist sync state: %s", exc)
    return payload


def start_hubspot_sync(app, *, force: bool = False) -> dict[str, Any]:
    """Kick off a background sync if one isn't already running. Returns status."""
    _ensure_state(app)
    if not HubSpotClient(app.state.settings).is_configured:
        return {"status": "unconfigured", "running": False,
                "message": "HUBSPOT_API_TOKEN is not set."}
    with app.state.hubspot_sync_lock:
        current: Future | None = app.state.hubspot_sync_future
        if isinstance(current, Future) and not current.done():
            return {"status": "running", "running": True,
                    "message": "HubSpot sync already in progress."}
        app.state.hubspot_sync_future = app.state.hubspot_sync_executor.submit(_run_sync, app)
        app.state.hubspot_sync_started_at = datetime.now(timezone.utc).isoformat()
        return {"status": "started", "running": True, "message": "HubSpot sync started."}


def hubspot_sync_status(app) -> dict[str, Any]:
    _ensure_state(app)
    current: Future | None = app.state.hubspot_sync_future
    running = isinstance(current, Future) and not current.done()
    last = dict(getattr(app.state, "hubspot_sync_last_result", {}) or {})
    last["running"] = running
    last.setdefault("status", "running" if running else "idle")
    return last
