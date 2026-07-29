"""Building contract workspace routes.

The workspace is a presentation and routing layer over the existing
agreement-readiness internal API. Every mutation delegates to the same guarded
function the internal API uses, so typed confirmations, preconditions,
idempotency, and audit evidence are unchanged. No route here creates a document,
signature request, invoice, payment object, or booking confirmation.
"""

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import urlencode
from uuid import uuid4

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import ValidationError

from sales_support_agent.api.building_agreement_readiness_router import (
    AgreementPackageInput,
    ReadinessTransitionInput,
    prepare_agreement_package,
    transition_agreement_package,
    transition_payment_readiness,
)
from sales_support_agent.models.database import session_scope
from sales_support_agent.services.admin_nav import render_agent_nav
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_contracts import (
    filter_contract_rows,
    has_approved_template,
    load_contract_detail,
    load_contract_rows,
    load_preparation_options,
)
from sales_support_agent.services.building_contracts_page import (
    CONTRACTS_URL,
    render_contract_detail,
    render_contract_index,
)
from sales_support_agent.services.building_security import (
    csrf_token,
    require_building_form_security,
)
from sales_support_agent.services.ui_shell import render_transition_document


router = APIRouter(prefix="/admin/building/contracts", tags=["building-contracts"])
FORM_DEPS = [Depends(require_building_form_security)]


def _actor(user: dict[str, Any]) -> str:
    return str(user.get("email") or "building-operator")


def _internal_key(request: Request) -> str:
    key = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    if not key:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    return key


def _may(user: dict[str, Any], permission: str) -> bool:
    """Mirror ``require_tool`` exactly so the page never renders an action that
    the guarded route would reject."""

    if user.get("is_superadmin"):
        return True
    return permission in set(user.get("permissions") or set())


def _redirect(target: str, *, notice: str = "", error: str = "") -> RedirectResponse:
    query = urlencode({"notice": notice} if notice else {"error": error})
    return RedirectResponse(f"{target}?{query}" if query else target, status_code=303)


def _index_redirect(*, notice: str = "", error: str = "") -> RedirectResponse:
    return _redirect(CONTRACTS_URL, notice=notice, error=error)


def _detail_redirect(
    agreement_id: str, *, notice: str = "", error: str = ""
) -> RedirectResponse:
    return _redirect(
        f"{CONTRACTS_URL}/{agreement_id}", notice=notice, error=error
    )


@router.get("", response_class=HTMLResponse)
def contract_index(
    request: Request,
    q: str = "",
    state: str = "",
    type: str = "",
    notice: str = "",
    error: str = "",
    user: dict = Depends(require_tool("building.manage")),
) -> HTMLResponse:
    """List every contract with the evidence an operator needs to judge it."""

    with session_scope(request.app.state.session_factory) as session:
        rows = load_contract_rows(session)
        template_approved = has_approved_template(session)
        can_prepare = _may(user, "building.agreements.prepare")
        options = (
            load_preparation_options(session)
            if can_prepare and template_approved
            else {"reservations": [], "quotes": [], "templates": []}
        )
    visible = filter_contract_rows(
        rows, search=q, state=state, contract_type=type
    )
    return HTMLResponse(render_contract_index(
        navigation=render_agent_nav("building_contracts", user=user),
        rows=visible,
        total_count=len(rows),
        search=q,
        state=state,
        contract_type=type,
        options=options,
        template_approved=template_approved,
        can_prepare=can_prepare,
        csrf_token=csrf_token(user),
        suggested_idempotency_key=f"contract-{uuid4().hex[:16]}",
        notice=notice,
        error=error,
    ))


@router.post("/packages", dependencies=FORM_DEPS)
def prepare_contract(
    request: Request,
    reservation_id: str = Form(...),
    quote_id: str = Form(...),
    template_id: str = Form(...),
    idempotency_key: str = Form(...),
    agreement_version: int = Form(1),
    payment_version: int = Form(1),
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    """Freeze an immutable evidence package. Creates no provider object."""

    try:
        result = prepare_agreement_package(
            AgreementPackageInput(
                reservation_id=reservation_id.strip(),
                quote_id=quote_id.strip(),
                template_id=template_id.strip(),
                agreement_version=agreement_version,
                payment_version=payment_version,
                actor=_actor(user),
            ),
            request,
            idempotency_key.strip(),
            _internal_key(request),
        )
    except (HTTPException, ValidationError, ValueError) as exc:
        return _index_redirect(error=str(getattr(exc, "detail", exc)))
    agreement_id = str((result.get("agreement") or {}).get("id") or "")
    if not agreement_id:
        return _index_redirect(error="Preparation returned no agreement record.")
    return _detail_redirect(
        agreement_id,
        notice="Contract package and payment request prepared for review; nothing was sent.",
    )


@router.post("/{agreement_id}/transition", dependencies=FORM_DEPS)
def transition_contract(
    agreement_id: str,
    request: Request,
    target_status: Literal["in_review", "approved"] = Form(...),
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.agreements.approve")),
) -> RedirectResponse:
    """Move agreement readiness. Approval authorizes a future handoff only."""

    try:
        transition_agreement_package(
            agreement_id,
            ReadinessTransitionInput(
                target_status=target_status,
                confirmation=confirmation.strip(),
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )
    except (HTTPException, ValidationError, ValueError) as exc:
        return _detail_redirect(
            agreement_id, error=str(getattr(exc, "detail", exc))
        )
    return _detail_redirect(
        agreement_id,
        notice=f"Agreement readiness moved to {target_status.replace('_', ' ')}.",
    )


@router.post("/{agreement_id}/payments/transition", dependencies=FORM_DEPS)
def transition_contract_payment(
    agreement_id: str,
    request: Request,
    target_status: Literal["in_review", "approved"] = Form(...),
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.payments.prepare")),
) -> RedirectResponse:
    """Move payment-request readiness. Creates no invoice, link, or charge."""

    with session_scope(request.app.state.session_factory) as session:
        detail = load_contract_detail(session, agreement_id)
        payment_id = str(((detail or {}).get("payment") or {}).get("id") or "")
    if not payment_id:
        return _detail_redirect(
            agreement_id, error="This contract has no payment-request record."
        )
    try:
        transition_payment_readiness(
            payment_id,
            ReadinessTransitionInput(
                target_status=target_status,
                confirmation=confirmation.strip(),
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )
    except (HTTPException, ValidationError, ValueError) as exc:
        return _detail_redirect(
            agreement_id, error=str(getattr(exc, "detail", exc))
        )
    return _detail_redirect(
        agreement_id,
        notice=f"Payment readiness moved to {target_status.replace('_', ' ')}.",
    )


@router.get("/{agreement_id}", response_class=HTMLResponse)
def contract_detail(
    agreement_id: str,
    request: Request,
    notice: str = "",
    error: str = "",
    user: dict = Depends(require_tool("building.manage")),
) -> HTMLResponse:
    """One contract: reconciled state, frozen terms, linked records, and audit."""

    with session_scope(request.app.state.session_factory) as session:
        contract = load_contract_detail(session, agreement_id)
    if contract is None:
        return HTMLResponse(
            render_transition_document(
                title="Contract not found · Anata Agent",
                body=(
                    '<header class="app-page-header"><div><h1>Contract not found</h1>'
                    "<p>This contract does not exist, or it was prepared under a "
                    "different identifier.</p></div></header>"
                    f'<p><a class="admin-btn" href="{CONTRACTS_URL}">All contracts</a></p>'
                ),
            ),
            status_code=404,
        )
    return HTMLResponse(render_contract_detail(
        navigation=render_agent_nav("building_contracts", user=user),
        contract=contract,
        can_approve=_may(user, "building.agreements.approve"),
        can_prepare_payment=_may(user, "building.payments.prepare"),
        can_manage=_may(user, "building.manage"),
        csrf_token=csrf_token(user),
        notice=notice,
        error=error,
    ))
