"""Register the complete Building operating-system surface on an Agent app."""

from __future__ import annotations

from fastapi import FastAPI

from sales_support_agent.api.building_adjustment_router import (
    router as building_adjustment_router,
)
from sales_support_agent.api.building_admin_operations_router import (
    router as building_admin_operations_router,
)
from sales_support_agent.api.building_agreement_readiness_router import (
    admin_router as building_agreement_readiness_admin_router,
    internal_router as building_agreement_readiness_internal_router,
)
from sales_support_agent.api.building_billing_router import (
    internal_router as building_billing_internal_router,
    webhook_router as building_stripe_webhook_router,
)
from sales_support_agent.api.building_booking_router import (
    public_router as building_booking_public_router,
    router as building_booking_router,
)
from sales_support_agent.api.building_booking_workspace_router import (
    router as building_booking_workspace_router,
)
from sales_support_agent.api.building_calendar_router import (
    router as building_calendar_router,
)
from sales_support_agent.api.building_checklist_router import (
    router as building_checklist_router,
)
from sales_support_agent.api.building_contract_router import (
    router as building_contract_router,
)
from sales_support_agent.api.building_content_router import (
    admin_router as building_content_admin_router,
    internal_router as building_content_internal_router,
    public_router as building_content_public_router,
)
from sales_support_agent.api.building_crm_router import (
    admin_router as building_crm_admin_router,
    internal_router as building_crm_internal_router,
    public_router as building_crm_public_router,
)
from sales_support_agent.api.building_email_webhook_router import (
    router as building_resend_webhook_router,
)
from sales_support_agent.api.building_privacy_router import (
    admin_router as building_privacy_admin_router,
    internal_router as building_privacy_internal_router,
)
from sales_support_agent.api.building_router import (
    internal_router as building_internal_router,
    public_router as building_public_router,
)
from sales_support_agent.api.building_service_request_router import (
    router as building_service_request_router,
)
from sales_support_agent.api.building_signature_readiness_router import (
    router as building_signature_readiness_router,
)


BUILDING_ROUTERS = (
    building_public_router,
    building_internal_router,
    building_crm_public_router,
    building_crm_internal_router,
    building_crm_admin_router,
    building_booking_router,
    building_booking_public_router,
    building_booking_workspace_router,
    building_agreement_readiness_internal_router,
    building_agreement_readiness_admin_router,
    building_contract_router,
    building_signature_readiness_router,
    building_billing_internal_router,
    building_stripe_webhook_router,
    building_admin_operations_router,
    building_calendar_router,
    building_checklist_router,
    building_adjustment_router,
    building_service_request_router,
    building_resend_webhook_router,
    building_privacy_internal_router,
    building_privacy_admin_router,
    building_content_public_router,
    building_content_internal_router,
    building_content_admin_router,
)


def include_building_routers(app: FastAPI) -> None:
    """Mount every Building route exactly once on one FastAPI entrypoint."""

    for router in BUILDING_ROUTERS:
        app.include_router(router)
