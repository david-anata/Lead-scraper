"""Outbound pages for the sales-support-agent app (agent.anatainc.com).

The outbound scoreboard and the StoreLeads brand-list download. The engine lives
in the repo-root modules (outbound_pipeline, outbound_scoreboard); this router
just exposes them on the agent.anatainc.com app, gated by that app's own login.
Read-only and dry-run: nothing sends, nothing pushes.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from sales_support_agent.services.auth_deps import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(tags=["outbound"])


def _require_login(request: Request):
    return get_current_user(request) is not None


@router.get("/admin/outbound/scoreboard", response_class=HTMLResponse)
def outbound_scoreboard(request: Request) -> Response:
    if not _require_login(request):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})
    import outbound_scoreboard as _sb

    board = _sb.get_scoreboard(_sb.load_instantly_key())
    return HTMLResponse(_sb.render_scoreboard_html(board))


@router.get("/admin/api/outbound/brands.csv", response_class=Response)
def outbound_brands_csv(request: Request, max_new: int = 100) -> Response:
    """Pull ICP-matched brands from StoreLeads and return them as a CSV to import
    into Clay. Sends nothing. Dedup state is not yet shared with this service, so
    a brand may appear across runs until that is wired; the CSV is a preview only.
    """
    if not _require_login(request):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    import outbound_pipeline as _op

    api_key, _clay = _op.load_config_from_env()
    if not api_key:
        return JSONResponse(status_code=400, content={"detail": "STORELEADS_API_KEY is not set on this service."})

    try:
        result = _op.run_storeleads_to_clay(
            api_key=api_key,
            clay_webhook_url="",  # dry-run: build the list, push nothing
            processed_domains=set(),
            max_new=max(1, min(int(max_new or 100), 500)),
            dry_run=True,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[outbound] StoreLeads CSV build failed")
        return JSONResponse(status_code=502, content={"detail": f"StoreLeads fetch failed: {exc}"})

    return Response(
        content=_op.leads_to_csv(result.leads),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="anata_clay_brands.csv"'},
    )
