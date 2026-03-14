"""FastAPI entrypoint for the ClickUp sales support agent."""

from __future__ import annotations

import logging

from fastapi import FastAPI

from sales_support_agent.api.router import router
from sales_support_agent.config import load_settings
from sales_support_agent.models.database import create_session_factory, init_database


def create_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    session_factory = create_session_factory(settings.sales_agent_db_url)
    init_database(session_factory)

    app = FastAPI(title="Sales Support Agent")
    app.state.settings = settings
    app.state.session_factory = session_factory
    app.include_router(router)
    return app


app = create_app()
