"""FastAPI entrypoint for the ClickUp sales support agent."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import logging
from threading import Lock

from fastapi import FastAPI

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
    app.state.settings = settings
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
