"""Morning prep job: ClickUp prep comment for each sales call today.

Off unless SALES_MORNING_PREP_ENABLED is true. `?dry_run=1` (or the flag off)
returns the comments without posting. Not scheduled in vercel.json yet.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Request

from sales_support_agent.api.outbound_jobs import _require_scheduler_auth
from sales_support_agent.api.sales_call_prep_job import _clickup, _gmail_clients
from sales_support_agent.models.database import session_scope
from sales_support_agent.services.sales.call_script.bookings import load_bookings
from sales_support_agent.services.sales.call_script.morning_prep import enabled, run_morning_prep
from sales_support_agent.services.sales.call_script.prep import gather_context

router = APIRouter(prefix="/api/jobs/sales-morning-prep", tags=["sales-morning-prep"])


@router.api_route("/run", methods=["GET", "POST"])
def run_sales_morning_prep(request: Request, dry_run: bool = False) -> dict:
    _require_scheduler_auth(request)
    if not enabled() and not dry_run:
        return {"status": "disabled", "message": "SALES_MORNING_PREP_ENABLED is off."}
    settings = request.app.state.settings
    now = datetime.now(timezone.utc)
    gmail_clients = _gmail_clients(settings)
    loaded = load_bookings(settings, now, gmail_clients=gmail_clients)
    clickup = _clickup(settings)
    with session_scope(request.app.state.session_factory) as session:
        return run_morning_prep(
            now=now,
            bookings=loaded.bookings,
            clickup=clickup,
            dry_run=dry_run,
            context_for=lambda b: gather_context(
                session, clickup, b, gmail=gmail_clients[0] if gmail_clients else None
            ),
        )
