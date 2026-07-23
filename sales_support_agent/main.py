"""FastAPI entrypoint for the ClickUp sales support agent."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import logging
import os
from threading import Lock

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from sales_support_agent.api.advertising_router import public_router as advertising_public_router, router as advertising_router
from sales_support_agent.api.auth_router import router as auth_router
from sales_support_agent.api.access_router import router as access_router, _settings_router
from sales_support_agent.api.hr_router import router as hr_router
from sales_support_agent.api.hr_jobs_router import router as hr_jobs_router
from sales_support_agent.api.marketing_router import router as marketing_router
from sales_support_agent.api.sales_jobs_router import router as sales_jobs_router
from sales_support_agent.api.sales_router import router as sales_router
from sales_support_agent.api.brand_analysis_router import (
    public_router as brand_analysis_public_router,
    router as brand_analysis_router,
)
from sales_support_agent.api.building_router import (
    internal_router as building_internal_router,
    public_router as building_public_router,
)
from sales_support_agent.api.building_crm_router import (
    admin_router as building_crm_admin_router,
    internal_router as building_crm_internal_router,
    public_router as building_crm_public_router,
)
from sales_support_agent.api.building_booking_router import router as building_booking_router
from sales_support_agent.api.building_billing_router import (
    internal_router as building_billing_internal_router,
    webhook_router as building_stripe_webhook_router,
)
from sales_support_agent.api.building_admin_operations_router import (
    router as building_admin_operations_router,
)
from sales_support_agent.api.building_calendar_router import (
    router as building_calendar_router,
)
from sales_support_agent.api.building_checklist_router import (
    router as building_checklist_router,
)
from sales_support_agent.api.building_adjustment_router import (
    router as building_adjustment_router,
)
from sales_support_agent.api.building_service_request_router import (
    router as building_service_request_router,
)
from sales_support_agent.api.building_email_webhook_router import (
    router as building_resend_webhook_router,
)
from sales_support_agent.api.building_privacy_router import (
    admin_router as building_privacy_admin_router,
    internal_router as building_privacy_internal_router,
)
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
    init_cashflow_db,
    init_database,
)


def create_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    session_factory = create_session_factory(settings.sales_agent_db_url)
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
    init_cashflow_db(settings.sales_agent_db_url)

    # RBAC: seed the never-lockable super-admin(s).
    try:
        from sales_support_agent.services.access import store as access_store
        access_store.seed_superadmins(getattr(settings, "rbac_superadmin_emails", ()))
    except Exception:  # noqa: BLE001 — seeding must never block startup
        logging.getLogger(__name__).exception("Failed to seed RBAC super-admins")

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
    app.include_router(hr_jobs_router)
    app.include_router(sales_jobs_router)
    app.include_router(sales_router)
    app.include_router(marketing_router)
    app.include_router(building_public_router)
    app.include_router(building_internal_router)
    app.include_router(building_crm_public_router)
    app.include_router(building_crm_internal_router)
    app.include_router(building_crm_admin_router)
    app.include_router(building_booking_router)
    app.include_router(building_billing_internal_router)
    app.include_router(building_stripe_webhook_router)
    app.include_router(building_admin_operations_router)
    app.include_router(building_calendar_router)
    app.include_router(building_checklist_router)
    app.include_router(building_adjustment_router)
    app.include_router(building_service_request_router)
    app.include_router(building_resend_webhook_router)
    app.include_router(building_privacy_internal_router)
    app.include_router(building_privacy_admin_router)

    # RBAC: per-tool authorization gate + friendly 403 handler.
    from sales_support_agent.services.access.middleware import install_access_middleware
    from sales_support_agent.services.auth_deps import ToolForbidden, render_forbidden_response
    install_access_middleware(app)
    app.add_exception_handler(ToolForbidden, render_forbidden_response)
    return app


app = create_app()
