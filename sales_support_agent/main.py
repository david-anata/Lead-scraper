"""FastAPI entrypoint for the ClickUp sales support agent."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import logging
import os
from threading import Lock

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from sales_support_agent.api.auth_router import router as auth_router
from sales_support_agent.api.cashflow_router import router as cashflow_router
from sales_support_agent.api.router import router
from sales_support_agent.config import load_settings
from sales_support_agent.models.database import create_session_factory, init_cashflow_db, init_database


def create_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    session_factory = create_session_factory(settings.sales_agent_db_url)
    init_database(session_factory)
    init_cashflow_db(settings.sales_agent_db_url)

    app = FastAPI(title="Sales Support Agent")
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    os.makedirs(static_dir, exist_ok=True)
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    app.state.settings = settings
    # Also expose as agent_settings so auth_deps._get_auth_settings() finds it
    # via the preferred code path (agent_settings → admin_dashboard_settings → settings).
    app.state.agent_settings = settings
    app.state.session_factory = session_factory
    app.state.dashboard_sync_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dashboard-sync")
    app.state.dashboard_sync_lock = Lock()
    app.state.dashboard_sync_future = None
    app.state.dashboard_sync_last_started_at = None
    app.state.dashboard_sync_last_completed_at = None
    app.state.dashboard_sync_last_error = ""
    app.include_router(router)
    app.include_router(auth_router)
    app.include_router(cashflow_router)
    return app


app = create_app()
