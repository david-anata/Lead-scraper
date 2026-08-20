"""FastAPI entrypoint for the ClickUp sales support agent."""

from __future__ import annotations

from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
import logging
import os
from threading import Lock
from time import perf_counter

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from sales_support_agent.api.advertising_router import public_router as advertising_public_router, router as advertising_router
from sales_support_agent.api.auth_router import router as auth_router
from sales_support_agent.api.assets_router import router as assets_router
from sales_support_agent.api.access_router import router as access_router, _settings_router
from sales_support_agent.api.hr_router import router as hr_router
from sales_support_agent.api.employee_app_router import router as employee_app_router
from sales_support_agent.api.hr_jobs_router import router as hr_jobs_router
from sales_support_agent.api.marketing_router import router as marketing_router
from sales_support_agent.api.leads_router import router as leads_router
from sales_support_agent.api.website_ops_jobs_router import (
    install_embedded_website_ops_scheduler,
    router as website_ops_jobs_router,
)
from sales_support_agent.api.content_router import router as content_router
from sales_support_agent.api.outbound_jobs import router as outbound_jobs_router
from sales_support_agent.api.vercel_cron_router import router as vercel_cron_router
from sales_support_agent.api.sales_jobs_router import router as sales_jobs_router
from sales_support_agent.api.sales_router import router as sales_router
from sales_support_agent.api.brand_analysis_router import (
    public_router as brand_analysis_public_router,
    router as brand_analysis_router,
)
from sales_support_agent.api.building_routes import include_building_routers
from sales_support_agent.api.cashflow_router import plaid_webhook_router, router as cashflow_router
from sales_support_agent.api.qbo_auth_router import router as qbo_auth_router
from sales_support_agent.api.fulfillment_deck_router import (
    admin_router as fulfillment_deck_admin_router,
    public_router as fulfillment_deck_public_router,
)
from sales_support_agent.api.fulfillment_public_router import router as fulfillment_public_router
from sales_support_agent.api.router import router
from sales_support_agent.config import load_settings
from sales_support_agent.models.database import (
    backfill_building_inquiry_assignments,
    create_session_factory,
    init_database,
)


logger = logging.getLogger("agent.lifecycle")


def _log_sales_snapshot_prewarm(future) -> None:
    """Report background snapshot readiness without delaying app startup."""
    try:
        snapshot = future.result()
    except Exception:  # noqa: BLE001 - prewarming must never block the service
        logger.exception("lifecycle milestone=sales_snapshot_prewarm_failed")
        return
    logger.info(
        "lifecycle milestone=sales_snapshot_ready generated_at=%s",
        snapshot.get("generatedAt", "unknown"),
    )


def _startup_database_prep_enabled() -> bool:
    """Keep local SQLite convenient without running DDL in Vercel requests."""

    default = "false" if os.getenv("VERCEL") else "true"
    return os.getenv("AGENT_PREPARE_DATABASE_ON_STARTUP", default).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _prepare_database(settings, session_factory) -> None:
    """Compatibility path for local/test startup; production uses pre-deploy."""

    init_database(session_factory)
    configured_building_owner = (
        settings.building_default_lead_owner
        or (
            settings.rbac_superadmin_emails[0]
            if settings.rbac_superadmin_emails
            else "building-operator"
        )
    )
    backfill_building_inquiry_assignments(
        session_factory,
        default_owner=configured_building_owner,
        response_sla_hours=settings.building_response_sla_hours,
    )
    try:
        from sales_support_agent.services.access import store as access_store

        access_store.seed_superadmins(
            getattr(settings, "rbac_superadmin_emails", ())
        )
    except Exception:  # noqa: BLE001 — local seeding must never block startup
        logger.exception("lifecycle milestone=superadmin_seed_failed")


def create_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO)
    process_started = perf_counter()
    commit = (
        os.getenv("VERCEL_GIT_COMMIT_SHA", "").strip()
        or os.getenv("RENDER_GIT_COMMIT", "").strip()
        or "local"
    )
    logger.info("lifecycle milestone=process_started commit=%s", commit)
    settings = load_settings()
    session_factory = create_session_factory(settings.sales_agent_db_url)
    logger.info(
        "lifecycle milestone=database_configured commit=%s elapsed_ms=%.1f",
        commit,
        (perf_counter() - process_started) * 1000,
    )
    if _startup_database_prep_enabled():
        _prepare_database(settings, session_factory)
        logger.info(
            "lifecycle milestone=schema_ready source=startup commit=%s elapsed_ms=%.1f",
            commit,
            (perf_counter() - process_started) * 1000,
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        from sales_support_agent.services.website_ops_storage import (
            database_mirror_enabled,
            synchronize_website_ops_cache,
        )

        if database_mirror_enabled() and not os.getenv("VERCEL"):
            storage_stats = synchronize_website_ops_cache(
                settings,
                session_factory.kw["bind"],
            )
            logger.info(
                "lifecycle milestone=website_ops_storage_ready files=%s bytes=%s",
                storage_stats["files"],
                storage_stats["bytes"],
            )
        if not os.getenv("VERCEL"):
            from sales_support_agent.services.fulfillment_report_storage import (
                synchronize_fulfillment_reports,
            )
            fulfillment_stats = synchronize_fulfillment_reports(
                session_factory.kw["bind"],
                settings.fulfillment_cs_reports_dir,
            )
            logger.info(
                "lifecycle milestone=fulfillment_report_storage_ready files=%s bytes=%s",
                fulfillment_stats["files"],
                fulfillment_stats["bytes"],
            )
        install_embedded_website_ops_scheduler(app)
        app.state.ready = True
        logger.info(
            "lifecycle milestone=app_ready commit=%s elapsed_ms=%.1f",
            commit,
            (perf_counter() - process_started) * 1000,
        )
        if os.getenv("RENDER", "").strip().lower() in {"1", "true", "yes"}:
            from sales_support_agent.services.sales.operator_dashboard import (
                get_operator_snapshot,
            )

            sales_snapshot_future = app.state.dashboard_sync_executor.submit(
                get_operator_snapshot,
                settings,
                session_factory=session_factory,
            )
            sales_snapshot_future.add_done_callback(_log_sales_snapshot_prewarm)
        try:
            yield
        finally:
            app.state.ready = False
            logger.info("lifecycle milestone=shutdown_started commit=%s", commit)
            for name in (
                "website_ops_scheduler_stop",
                "outbound_scheduler_stop",
            ):
                stop_event = getattr(app.state, name, None)
                if stop_event is not None:
                    stop_event.set()
            app.state.dashboard_sync_executor.shutdown(
                wait=False,
                cancel_futures=True,
            )
            engine = session_factory.kw.get("bind")
            if engine is not None:
                engine.dispose()
            logger.info("lifecycle milestone=shutdown_complete commit=%s", commit)

    app = FastAPI(title="Sales Support Agent", lifespan=lifespan)

    @app.middleware("http")
    async def finance_read_performance(request, call_next):
        """Time Finance pages and invalidate cached reads after every write."""
        started = perf_counter()
        response = await call_next(request)
        path = request.url.path
        if path.startswith("/admin/finances"):
            elapsed_ms = (perf_counter() - started) * 1000
            response.headers["Server-Timing"] = f"finance;dur={elapsed_ms:.1f}"
            logger.info(
                "finance_page method=%s path=%s status=%s duration_ms=%.1f",
                request.method,
                path,
                response.status_code,
                elapsed_ms,
            )
            if request.method not in {"GET", "HEAD", "OPTIONS"}:
                from sales_support_agent.api.cashflow_router import (
                    clear_finance_brief_cache,
                )

                clear_finance_brief_cache(request.app)
        return response
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    brand_static_dir = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "shared",
        "anata_brand",
        "assets",
    )
    os.makedirs(static_dir, exist_ok=True)
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    app.mount(
        "/brand-static",
        StaticFiles(directory=brand_static_dir),
        name="brand-static",
    )
    app.state.settings = settings
    # Also expose as agent_settings so auth_deps._get_auth_settings() finds it
    # via the preferred code path (agent_settings → admin_dashboard_settings → settings).
    app.state.agent_settings = settings
    app.state.session_factory = session_factory
    app.state.ready = False
    app.state.render_git_commit = commit
    app.state.dashboard_sync_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dashboard-sync")
    app.state.dashboard_sync_lock = Lock()
    app.state.dashboard_sync_future = None
    app.state.dashboard_sync_last_started_at = None
    app.state.dashboard_sync_last_completed_at = None
    app.state.dashboard_sync_last_error = ""
    app.include_router(assets_router)
    app.include_router(router)
    app.include_router(auth_router)
    app.include_router(cashflow_router)
    app.include_router(plaid_webhook_router)
    # OAuth is public by Intuit requirement, while the Finance settings page remains access-controlled.
    app.include_router(qbo_auth_router, prefix="/admin/finances/qbo")
    app.include_router(advertising_router)
    app.include_router(advertising_public_router)
    app.include_router(brand_analysis_router)
    app.include_router(brand_analysis_public_router)
    app.include_router(fulfillment_deck_admin_router)
    app.include_router(fulfillment_deck_public_router)
    app.include_router(fulfillment_public_router)
    app.include_router(access_router)
    app.include_router(_settings_router)
    app.include_router(hr_router)
    app.include_router(employee_app_router)
    app.include_router(hr_jobs_router)
    app.include_router(sales_jobs_router)
    app.include_router(sales_router)
    app.include_router(marketing_router)
    app.include_router(leads_router)
    app.include_router(website_ops_jobs_router)
    app.include_router(content_router)
    app.include_router(outbound_jobs_router)
    app.include_router(vercel_cron_router)
    include_building_routers(app)
    from sales_support_agent.api.outbound_router import router as outbound_router
    app.include_router(outbound_router)

    # RBAC: per-tool authorization gate + friendly 403 handler.
    from sales_support_agent.services.access.middleware import install_access_middleware
    from sales_support_agent.services.auth_deps import ToolForbidden, render_forbidden_response
    from sales_support_agent.services.performance import install_performance_middleware
    from sales_support_agent.services.website_ops_storage import (
        WebsiteOpsStorageMiddleware,
    )
    from sales_support_agent.services.fulfillment_report_storage import (
        FulfillmentReportStorageMiddleware,
    )

    install_performance_middleware(app, session_factory.kw.get("bind"))
    app.add_middleware(WebsiteOpsStorageMiddleware)
    app.add_middleware(FulfillmentReportStorageMiddleware)
    install_access_middleware(app)
    app.add_exception_handler(ToolForbidden, render_forbidden_response)
    return app


app = create_app()
