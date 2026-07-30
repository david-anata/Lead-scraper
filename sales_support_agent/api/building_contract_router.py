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

from sqlalchemy import select

from sales_support_agent.api.building_agreement_readiness_router import (
    AgreementPackageInput,
    AgreementTemplateInput,
    ReadinessTransitionInput,
    ReviewActionInput,
    prepare_agreement_package,
    transition_agreement_package,
    transition_agreement_template,
    transition_payment_readiness,
    upsert_agreement_template,
)
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAgreementTemplate,
    BuildingAuditEvent,
)
from sales_support_agent.services.admin_nav import render_agent_nav
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_contract_templates import (
    CONTRACT_TYPES,
    format_merge_value,
    next_version,
    render_document_html,
    render_document_text,
    template_payload,
    unresolved_fields,
)
from sales_support_agent.services.building_contract_templates_page import (
    TEMPLATES_URL,
    render_contract_document,
    render_template_editor,
    render_template_index,
)
from sales_support_agent.services.building_contracts import (
    filter_contract_rows,
    has_approved_template,
    load_contract_detail,
    load_contract_rows,
    load_preparation_options,
    load_preview_merge_values,
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


def _templates_redirect(*, notice: str = "", error: str = "") -> RedirectResponse:
    return _redirect(TEMPLATES_URL, notice=notice, error=error)


def _first_message(exc: Exception) -> str:
    """Surface one actionable sentence instead of a pydantic error dump."""

    if isinstance(exc, ValidationError):
        errors = exc.errors()
        message = str(errors[0].get("msg", "")) if errors else ""
        return message.removeprefix("Value error, ") or "Review the template values."
    return str(getattr(exc, "detail", exc))


def _template_identity(request: Request, template_id: str) -> tuple[str, int]:
    """Return the immutable (template_key, version) pair for an existing draft.

    The editor never lets an operator retype these; they are established when
    the version is created.
    """

    with session_scope(request.app.state.session_factory) as session:
        row = session.execute(
            select(
                BuildingAgreementTemplate.template_key,
                BuildingAgreementTemplate.version,
            ).where(BuildingAgreementTemplate.id == template_id)
        ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Template not found.")
    return str(row[0]), int(row[1])


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


@router.get("/templates", response_class=HTMLResponse)
def template_index(
    request: Request,
    notice: str = "",
    error: str = "",
    user: dict = Depends(require_tool("building.manage")),
) -> HTMLResponse:
    """The template registry: what exists and what is approved."""

    with session_scope(request.app.state.session_factory) as session:
        rows = session.execute(
            select(BuildingAgreementTemplate).order_by(
                BuildingAgreementTemplate.template_key,
                BuildingAgreementTemplate.version.desc(),
            )
        ).scalars().all()
        templates = [template_payload(row) for row in rows]
    return HTMLResponse(render_template_index(
        navigation=render_agent_nav("building_contracts", user=user),
        templates=templates,
        can_author=_may(user, "building.agreements.prepare"),
        csrf_token=csrf_token(user),
        notice=notice,
        error=error,
    ))


@router.post("/templates", dependencies=FORM_DEPS)
def create_template_draft(
    request: Request,
    template_key: str = Form(...),
    name: str = Form(...),
    contract_type: str = Form("event"),
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    """Create an empty editable draft at the next unused version."""

    key = template_key.strip()
    if contract_type not in CONTRACT_TYPES:
        return _templates_redirect(error=f"Unsupported contract type: {contract_type}.")
    with session_scope(request.app.state.session_factory) as session:
        versions = session.execute(
            select(BuildingAgreementTemplate.version).where(
                BuildingAgreementTemplate.template_key == key
            )
        ).scalars().all()
        version = next_version(versions)
        template_id = f"{key}-v{version}"
        if session.get(BuildingAgreementTemplate, template_id) is not None:
            return _templates_redirect(
                error="That template identifier already exists."
            )
        session.add(BuildingAgreementTemplate(
            id=template_id,
            template_key=key,
            version=version,
            name=name.strip(),
            status="draft",
            contract_type=contract_type,
            created_by=_actor(user),
        ))
        session.add(BuildingAuditEvent(
            entity_type="agreement_template",
            entity_id=template_id,
            action="agreement_template_created",
            actor=_actor(user),
            after_json={
                "template_key": key,
                "version": version,
                "contract_type": contract_type,
                "provider_write": False,
            },
        ))
    return _redirect(
        f"{TEMPLATES_URL}/{template_id}",
        notice=f"Draft version {version} created. Nothing is approved yet.",
    )


@router.post("/templates/{template_id}", dependencies=FORM_DEPS)
async def save_template_draft(
    template_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    """Save authored contract text. Validates every merge token before writing."""

    form = await request.form()
    clauses: list[dict[str, str]] = []
    index = 0
    while True:
        title = form.get(f"clause_title_{index}")
        body = form.get(f"clause_body_{index}")
        if title is None and body is None:
            break
        clauses.append({"title": str(title or ""), "body": str(body or "")})
        index += 1
    template_key, version = _template_identity(request, template_id)
    try:
        upsert_agreement_template(
            template_id,
            AgreementTemplateInput(
                id=template_id,
                template_key=template_key,
                version=version,
                name=str(form.get("name") or "").strip(),
                contract_type=str(form.get("contract_type") or "event").strip(),
                template_reference=str(form.get("template_reference") or "").strip(),
                body_markdown=str(form.get("body_markdown") or ""),
                clauses=clauses,
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )
    except (HTTPException, ValidationError, ValueError) as exc:
        return _redirect(
            f"{TEMPLATES_URL}/{template_id}",
            error=_first_message(exc),
        )
    return _redirect(
        f"{TEMPLATES_URL}/{template_id}",
        notice="Template draft saved and validated. It is not approved.",
    )


@router.post("/templates/{template_id}/transition", dependencies=FORM_DEPS)
def transition_template(
    template_id: str,
    request: Request,
    target_status: Literal["in_review", "approved", "retired"] = Form(...),
    confirmation: str = Form(...),
    evidence: str = Form(""),
    user: dict = Depends(require_tool("building.agreements.approve")),
) -> RedirectResponse:
    """Move a template version. Approved versions become immutable."""

    try:
        transition_agreement_template(
            template_id,
            ReviewActionInput(
                target_status=target_status,
                confirmation=confirmation.strip(),
                evidence=evidence.strip(),
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )
    except (HTTPException, ValidationError, ValueError) as exc:
        return _redirect(f"{TEMPLATES_URL}/{template_id}", error=_first_message(exc))
    return _redirect(
        f"{TEMPLATES_URL}/{template_id}",
        notice=f"Template moved to {target_status.replace('_', ' ')}.",
    )


@router.post("/templates/{template_id}/new-version", dependencies=FORM_DEPS)
def start_next_template_version(
    template_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    """Copy an immutable version into a fresh editable draft."""

    with session_scope(request.app.state.session_factory) as session:
        source = session.get(BuildingAgreementTemplate, template_id)
        if source is None:
            return _templates_redirect(error="That template version does not exist.")
        versions = session.execute(
            select(BuildingAgreementTemplate.version).where(
                BuildingAgreementTemplate.template_key == source.template_key
            )
        ).scalars().all()
        version = next_version(versions)
        new_id = f"{source.template_key}-v{version}"
        if session.get(BuildingAgreementTemplate, new_id) is not None:
            return _templates_redirect(error="That next version already exists.")
        session.add(BuildingAgreementTemplate(
            id=new_id,
            template_key=source.template_key,
            version=version,
            name=source.name,
            status="draft",
            contract_type=source.contract_type or "event",
            template_reference=source.template_reference,
            body_markdown=source.body_markdown,
            clauses_json=list(source.clauses_json or []),
            merge_fields_json=list(source.merge_fields_json or []),
            created_by=_actor(user),
        ))
        session.add(BuildingAuditEvent(
            entity_type="agreement_template",
            entity_id=new_id,
            action="agreement_template_version_started",
            actor=_actor(user),
            before_json={"source_template_id": template_id},
            after_json={"version": version, "provider_write": False},
        ))
    return _redirect(
        f"{TEMPLATES_URL}/{new_id}",
        notice=f"Version {version} started as an editable draft.",
    )


@router.get("/templates/{template_id}", response_class=HTMLResponse)
def template_editor(
    template_id: str,
    request: Request,
    preview: str = "",
    notice: str = "",
    error: str = "",
    user: dict = Depends(require_tool("building.manage")),
) -> HTMLResponse:
    """Author, preview, and move one template version."""

    preview_html = ""
    preview_values: list[tuple[str, str]] = []
    preview_error = ""
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingAgreementTemplate, template_id)
        if row is None:
            return HTMLResponse(
                render_transition_document(
                    title="Template not found · Anata Agent",
                    body=(
                        '<header class="app-page-header"><div>'
                        "<h1>Template not found</h1><p>This template version does "
                        "not exist.</p></div></header>"
                        f'<p><a class="admin-btn" href="{TEMPLATES_URL}">All templates</a></p>'
                    ),
                ),
                status_code=404,
            )
        template = template_payload(row)
        options = load_preparation_options(session)["reservations"]
        if preview:
            merge_values = load_preview_merge_values(session, preview)
            if merge_values is None:
                preview_error = (
                    "That booking has no active contact, space, and quote to "
                    "preview against."
                )
            else:
                text = render_document_text(
                    name=template["name"],
                    body_markdown=template["body_markdown"],
                    clauses=template["clauses"],
                    merge_values=merge_values,
                )
                preview_html = render_document_html(text)
                preview_values = [
                    (field, format_merge_value(field, merge_values.get(field)))
                    for field in template["merge_fields"]
                ]
                if unresolved_fields(text):
                    preview_error = (
                        "This booking cannot supply every merge field. A package "
                        "prepared against it would be refused."
                    )
    return HTMLResponse(render_template_editor(
        navigation=render_agent_nav("building_contracts", user=user),
        template=template,
        preview_options=options,
        preview_reservation_id=preview,
        preview_html=preview_html,
        preview_values=preview_values,
        preview_error=preview_error,
        can_author=_may(user, "building.agreements.prepare"),
        can_approve=_may(user, "building.agreements.approve"),
        csrf_token=csrf_token(user),
        notice=notice,
        error=error,
    ))


@router.get("/{agreement_id}/document", response_class=HTMLResponse)
def contract_document(
    agreement_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> HTMLResponse:
    """Render the frozen contract text for reading and printing.

    Approved packages only. This creates no provider object and sends nothing;
    the operator prints or saves the page from the browser.
    """

    with session_scope(request.app.state.session_factory) as session:
        contract = load_contract_detail(session, agreement_id)
    if contract is None:
        raise HTTPException(status_code=404, detail="Contract not found.")
    document = dict((contract.get("snapshot") or {}).get("document") or {})
    if not document.get("text"):
        raise HTTPException(
            status_code=409,
            detail=(
                "This contract has no frozen document. It was prepared against a "
                "template that holds its text outside Agent."
            ),
        )
    if contract["preparation_status"] != "approved":
        raise HTTPException(
            status_code=409,
            detail="The contract document is available once the package is approved.",
        )
    return HTMLResponse(render_contract_document(
        contract=contract,
        document_html=render_document_html(str(document["text"])),
        checksum=str(document.get("checksum") or ""),
    ))


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
        can_prepare_signature=_may(user, "building.agreements.prepare"),
        can_prepare_payment=_may(user, "building.payments.prepare"),
        can_manage=_may(user, "building.manage"),
        csrf_token=csrf_token(user),
        notice=notice,
        error=error,
    ))
