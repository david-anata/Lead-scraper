"""Staff-facing guided workspace for one Building reservation."""

from __future__ import annotations

import html
from datetime import datetime
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from sales_support_agent.services.ui_shell import render_operator_document


MOUNTAIN = ZoneInfo("America/Denver")


def _esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _when(value: datetime | None) -> str:
    if value is None:
        return "Not set"
    local = value.astimezone(MOUNTAIN)
    return local.strftime("%b %d, %Y · %I:%M %p MT")


def _status(value: str) -> str:
    normalized = str(value or "not_started").replace("_", " ")
    modifier = (
        "confirmed"
        if normalized in {"confirmed", "signed", "paid", "approved", "accepted", "synced", "completed"}
        else "review"
        if normalized in {"soft hold", "draft", "in review", "sent", "pending", "prepared"}
        else "blocked"
        if normalized in {"error", "expired", "cancelled", "voided", "declined"}
        else "neutral"
    )
    return f'<span class="app-status app-status--{modifier}">{_esc(normalized.title())}</span>'


def _phase(
    *,
    number: int,
    label: str,
    state: str,
    summary: str,
) -> str:
    state_label = {
        "done": "Complete",
        "current": "Current step",
        "waiting": "Waiting",
        "blocked": "Blocked",
    }.get(state, state.title())
    return f"""<li class="booking-phase booking-phase--{_esc(state)}">
      <span class="booking-phase__number">{number}</span>
      <span><strong>{_esc(label)}</strong><small>{_esc(summary)}</small></span>
      <span class="booking-phase__state">{_esc(state_label)}</span>
    </li>"""


def _build_phases(data: dict[str, Any]) -> tuple[list[dict[str, str]], dict[str, str]]:
    reservation = data["reservation"]
    proposal = data.get("proposal")
    agreement = data.get("agreement")
    payment = data.get("payment")
    status = str(reservation.get("status") or "inquiry")
    proposal_status = str((proposal or {}).get("status") or "not_started")
    agreement_preparation = str(
        (agreement or {}).get("preparation_status") or "not_started"
    )
    agreement_signed = str(reservation.get("agreement_status") or "") == "signed"
    deposit_status = str(reservation.get("deposit_status") or "not_started")
    is_confirmed = status == "confirmed"

    request_done = bool(reservation.get("inquiry_id") or reservation.get("contact_id"))
    date_done = status in {
        "soft_hold",
        "quote_sent",
        "agreement_pending",
        "confirmed",
        "completed",
    }
    quote_done = proposal_status == "accepted"
    agreement_done = agreement_signed
    payment_done = deposit_status in {"paid", "waived"} or not reservation.get(
        "deposit_required", True
    )

    flags = [
        request_done,
        date_done,
        quote_done,
        agreement_done,
        payment_done,
        is_confirmed,
    ]
    current_index = next((index for index, done in enumerate(flags) if not done), 6)
    labels = [
        ("Customer request", "Contact and request are linked."),
        ("Date and temporary hold", "The complete access window is conflict-checked."),
        ("Quote", "Customer has accepted the frozen quote version."),
        ("Agreement", "Signature evidence is verified."),
        ("Required payment", "Cleared payment evidence is recorded."),
        ("Booking confirmation", "Agent is authoritative for the confirmed booking."),
        ("Event operations", "Calendar and event-day work are tracked."),
    ]
    phases: list[dict[str, str]] = []
    for index, (label, summary) in enumerate(labels):
        if status in {"cancelled", "expired"} and index >= current_index:
            phase_state = "blocked"
        elif index < current_index or (index == 6 and is_confirmed):
            phase_state = "done"
        elif index == current_index:
            phase_state = "current"
        else:
            phase_state = "waiting"
        phases.append({
            "label": label,
            "summary": summary,
            "state": phase_state,
        })

    if status in {"cancelled", "expired"}:
        next_action = {
            "title": f"This booking is {status}.",
            "body": "Inventory is not held. Review the audit trail before starting a new request.",
            "href": "/admin/building/bookings",
            "label": "Return to bookings",
        }
    elif not request_done:
        next_action = {
            "title": "Link the responsible customer.",
            "body": "A booking cannot progress safely without an active contact and request evidence.",
            "href": "/admin/building/sales#incoming-inquiries",
            "label": "Open inquiries",
        }
    elif not date_done:
        next_action = {
            "title": "Review the requested date.",
            "body": "Confirm the setup-through-teardown window and create a conflict-checked temporary hold.",
            "href": "/admin/building/bookings#review-event-date",
            "label": "Review date and hold",
        }
    elif not quote_done:
        next_action = {
            "title": "Finish the customer quote.",
            "body": (
                f"The latest quote is {proposal_status.replace('_', ' ')}. "
                "Review, approve, send through the approved channel, and record acceptance."
            ),
            "href": "/admin/building/bookings#bookings-and-holds",
            "label": "Manage quote",
        }
    elif not agreement_done:
        next_action = {
            "title": "Prepare or finish the agreement.",
            "body": (
                f"Agreement preparation is {agreement_preparation.replace('_', ' ')}. "
                "Use the governed contract workspace; do not claim signature without provider evidence."
            ),
            "href": f"/admin/building/contracts?q={quote(str(reservation.get('id') or ''))}",
            "label": "Open contracts",
        }
    elif not payment_done:
        next_action = {
            "title": "Record cleared payment evidence.",
            "body": "A prepared payment request is not a payment. Confirm only provider-cleared or approved manual evidence.",
            "href": "/admin/building/billing",
            "label": "Open billing",
        }
    elif not is_confirmed:
        next_action = {
            "title": "Run the final confirmation gate.",
            "body": "Recheck conflicts, signed agreement evidence, and cleared required payment before confirming.",
            "href": "/admin/building/bookings#bookings-and-holds",
            "label": "Confirm booking",
        }
    else:
        next_action = {
            "title": "Complete event operations.",
            "body": "Assign checklist work and monitor the calendar projection. Agent remains the booking source of truth.",
            "href": "/admin/building/operations",
            "label": "Open operations",
        }
    return phases, next_action


def render_booking_workspace(
    *,
    navigation: str,
    data: dict[str, Any],
    csrf_token: str,
    notice: str = "",
    error: str = "",
) -> str:
    """Render one customer-named, plain-language booking workspace."""

    reservation = data["reservation"]
    contact = data.get("contact") or {}
    inquiry = data.get("inquiry") or {}
    proposal = data.get("proposal") or {}
    agreement = data.get("agreement") or {}
    payment = data.get("payment") or {}
    calendar = data.get("calendar") or {}
    checklist = data.get("checklist") or {}
    customer_name = (
        contact.get("full_name")
        or inquiry.get("name")
        or contact.get("email")
        or inquiry.get("email")
        or "Unlinked customer"
    )
    space_name = data.get("space_name") or "Building space"
    phases, next_action = _build_phases(data)
    phase_html = "".join(
        _phase(
            number=index,
            label=item["label"],
            state=item["state"],
            summary=item["summary"],
        )
        for index, item in enumerate(phases, start=1)
    )
    messages = ""
    if notice:
        messages += f'<div class="app-alert app-alert--notice"><p>{_esc(notice)}</p></div>'
    if error:
        messages += f'<div class="app-alert app-alert--error"><p>{_esc(error)}</p></div>'

    guest_window = ""
    if reservation.get("guest_starts_at") or reservation.get("guest_ends_at"):
        guest_window = (
            f"<dt>Guest event</dt><dd>{_esc(_when(reservation.get('guest_starts_at')))}"
            f" – {_esc(_when(reservation.get('guest_ends_at')))}</dd>"
        )
    hold_copy = (
        _when(reservation.get("hold_expires_at"))
        if reservation.get("hold_expires_at")
        else "No active hold expiry"
    )
    contract_href = (
        f"/admin/building/contracts/{_esc(agreement.get('id'))}"
        if agreement.get("id")
        else f"/admin/building/contracts?q={quote(str(reservation.get('id') or ''))}"
    )
    detail_rows = f"""
      <dt>Customer</dt><dd><strong>{_esc(customer_name)}</strong><span>{_esc(contact.get("email") or inquiry.get("email") or "No email linked")}</span></dd>
      <dt>Venue</dt><dd>{_esc(space_name)}</dd>
      <dt>Full access window</dt><dd>{_esc(_when(reservation.get("starts_at")))} – {_esc(_when(reservation.get("ends_at")))}</dd>
      {guest_window}
      <dt>Attendance</dt><dd>{_esc(reservation.get("attendance") or "Not set")}</dd>
      <dt>Assigned owner</dt><dd>{_esc(reservation.get("assigned_owner") or "Unassigned")}</dd>
      <dt>Temporary hold</dt><dd>{_esc(hold_copy)}</dd>
    """
    evidence_rows = f"""
      <div><span>Quote</span>{_status(proposal.get("status") or "not_started")}<small>{f"Version {proposal.get('version')} · {proposal.get('currency', 'USD')} {int(proposal.get('amount_cents') or 0) / 100:,.2f}" if proposal else "No frozen quote yet"}</small></div>
      <div><span>Agreement</span>{_status(agreement.get("preparation_status") or reservation.get("agreement_status") or "not_started")}<small>{"Signature verified" if reservation.get("agreement_status") == "signed" else "No signature success is claimed"}</small></div>
      <div><span>Payment</span>{_status(reservation.get("deposit_status") or payment.get("status") or "not_started")}<small>{"Cleared evidence recorded" if reservation.get("deposit_status") == "paid" else "Readiness is not payment"}</small></div>
      <div><span>Calendar</span>{_status(calendar.get("status") or "not_started")}<small>{"Projected to the configured calendar" if calendar.get("status") == "synced" else "Agent remains authoritative"}</small></div>
      <div><span>Operations</span>{_status(checklist.get("status") or "not_started")}<small>{_esc(checklist.get("summary") or "No event-day checklist yet")}</small></div>
    """
    body = f"""
      <header class="app-page-header booking-header">
        <div>
          <a class="booking-back" href="/admin/building/bookings">← All bookings</a>
          <p class="app-eyebrow">Building booking</p>
          <h1>{_esc(customer_name)} · {_esc(space_name)}</h1>
          <p>One guided record for the customer, date, quote, agreement, payment, confirmation, and event operations.</p>
        </div>
        {_status(reservation.get("status") or "inquiry")}
      </header>
      {messages}
      <section class="booking-next" aria-labelledby="booking-next-title">
        <div><p class="app-eyebrow">Do this next</p><h2 id="booking-next-title">{_esc(next_action["title"])}</h2><p>{_esc(next_action["body"])}</p></div>
        <a class="booking-button booking-button--primary" href="{_esc(next_action["href"])}">{_esc(next_action["label"])} →</a>
      </section>
      <section class="booking-layout">
        <div class="booking-workspace booking-journey">
          <div class="booking-workspace__header"><div><h2>Booking journey</h2><p>Completed evidence stays distinct from work that is only prepared or waiting.</p></div></div>
          <ol>{phase_html}</ol>
        </div>
        <aside class="booking-workspace booking-summary">
          <div class="booking-workspace__header"><div><h2>Booking summary</h2><p>Mountain time</p></div></div>
          <dl>{detail_rows}</dl>
        </aside>
      </section>
      <section class="booking-workspace">
        <div class="booking-workspace__header"><div><h2>Authoritative evidence</h2><p>Prepared, sent, signed, paid, and confirmed are never treated as the same state.</p></div></div>
        <div class="booking-evidence">{evidence_rows}</div>
      </section>
      <section class="booking-actions" aria-labelledby="booking-actions-title">
        <div><h2 id="booking-actions-title">Common staff actions</h2><p>Use the governed workspace for each action; every write keeps its existing permission and audit checks.</p></div>
        <div>
          <a class="booking-button booking-button--secondary" href="/admin/building/bookings#bookings-and-holds">Manage quote or status</a>
          <a class="booking-button booking-button--secondary" href="{contract_href}">Open contract</a>
          <a class="booking-button booking-button--secondary" href="/admin/building/billing">Open billing</a>
        </div>
        {(
          f'''<form method="post" action="/admin/building/reservations/{_esc(reservation.get("id"))}/customer-status-access">
            <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
            <label>Customer status link expires in <input type="number" name="expires_in_days" min="1" max="90" value="30" required> days</label>
            <button class="booking-button booking-button--secondary" type="submit">Prepare status link</button>
            <small>Creates a copyable link only. Nothing is sent.</small>
          </form>'''
          if reservation.get("kind") == "event"
          else ""
        )}
      </section>
      <details class="booking-technical">
        <summary>Technical record details</summary>
        <p>Reservation reference: <code>{_esc(reservation.get("id"))}</code></p>
        <p>Source: {_esc(reservation.get("source") or "Agent")} · {_esc(reservation.get("source_reference") or "No external reference")}</p>
      </details>
    """
    styles = """
    <style>
      .building-booking-workspace .app-eyebrow{margin:0;color:var(--agent-ink-muted);font:700 .75rem/1.3 "Montserrat",sans-serif;letter-spacing:.06em;text-transform:uppercase}
      .building-booking-workspace .app-page{display:grid;gap:20px;padding-block:28px 64px}
      .booking-header{align-items:flex-start;margin-bottom:0}.booking-back{display:inline-flex;margin-bottom:18px;color:var(--agent-blue-strong);font-weight:700}
      .booking-next{display:flex;align-items:center;justify-content:space-between;gap:24px;padding:24px 26px;border:1px solid rgba(94,159,196,.35);border-radius:var(--agent-radius-panel);background:linear-gradient(135deg,#fff,#f1f8fb)}
      .booking-next h2{margin:3px 0 7px}.booking-next p{margin:0;max-width:760px;color:var(--agent-ink-muted)}
      .booking-button{display:inline-flex;align-items:center;justify-content:center;min-height:42px;padding:9px 14px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);font:700 .8rem/1.2 "Montserrat",sans-serif;text-decoration:none;cursor:pointer}.booking-button--primary{border-color:var(--agent-blue-strong);background:var(--agent-blue-strong);color:#fff}.booking-button--secondary{background:var(--agent-surface);color:var(--agent-ink)}
      .booking-layout{display:grid;grid-template-columns:minmax(0,1.55fr) minmax(300px,.75fr);gap:20px}
      .booking-workspace{min-width:0;overflow:hidden;border:1px solid var(--agent-border);border-radius:var(--agent-radius-panel);background:var(--agent-surface)}.booking-workspace__header{display:flex;align-items:flex-start;justify-content:space-between;gap:18px;padding:18px 20px}.booking-workspace__header h2{margin:0}.booking-workspace__header p{margin:5px 0 0;color:var(--agent-ink-muted)}
      .booking-journey ol{margin:0;padding:0;list-style:none}.booking-phase{display:grid;grid-template-columns:34px minmax(0,1fr) auto;gap:14px;align-items:center;padding:15px 20px;border-top:1px solid var(--agent-border)}
      .booking-phase__number{display:grid;place-items:center;width:30px;height:30px;border-radius:50%;background:var(--agent-surface-soft);font-weight:800}
      .booking-phase strong,.booking-phase small{display:block}.booking-phase small{margin-top:3px;color:var(--agent-ink-muted);line-height:1.4}.booking-phase__state{font-size:12px;font-weight:800;color:var(--agent-ink-muted)}
      .booking-phase--done .booking-phase__number{background:#e4f4f1;color:#11665f}.booking-phase--current{background:#f3f8fb}.booking-phase--current .booking-phase__number{background:#397a9d;color:#fff}.booking-phase--current .booking-phase__state{color:#397a9d}
      .booking-phase--waiting{opacity:.7}.booking-phase--blocked .booking-phase__number{background:#fff0ed;color:#8b2f23}
      .booking-summary dl{margin:0}.booking-summary dt,.booking-summary dd{margin:0;padding:12px 18px;border-top:1px solid var(--agent-border)}.booking-summary dt{padding-bottom:0;color:var(--agent-ink-muted);font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.06em}.booking-summary dd{padding-top:4px}.booking-summary dd span{display:block;margin-top:3px;color:var(--agent-ink-muted);font-size:12px}
      .booking-evidence{display:grid;grid-template-columns:repeat(5,minmax(0,1fr))}.booking-evidence>div{min-width:0;padding:18px;border-top:1px solid var(--agent-border);border-right:1px solid var(--agent-border)}.booking-evidence>div:last-child{border-right:0}.booking-evidence>div>span,.booking-evidence small{display:block}.booking-evidence>div>span{margin-bottom:8px;color:var(--agent-ink-muted);font-size:11px;font-weight:800;text-transform:uppercase}.booking-evidence small{margin-top:8px;color:var(--agent-ink-muted);line-height:1.4}
      .booking-actions{display:grid;grid-template-columns:minmax(220px,.7fr) minmax(0,1.3fr);gap:20px;padding:22px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-panel);background:var(--agent-surface)}.booking-actions h2{margin:0}.booking-actions p{margin:6px 0 0;color:var(--agent-ink-muted)}.booking-actions>div:nth-child(2){display:flex;justify-content:flex-end;align-items:start;flex-wrap:wrap;gap:8px}.booking-actions form{grid-column:1/-1;display:flex;align-items:center;flex-wrap:wrap;gap:10px;padding-top:16px;border-top:1px solid var(--agent-border)}.booking-actions input{width:76px}.booking-actions small{color:var(--agent-ink-muted)}
      .booking-technical{padding:14px 18px;border:1px dashed var(--agent-border);border-radius:var(--agent-radius-control)}.booking-technical summary{cursor:pointer;font-weight:700;color:var(--agent-ink-muted)}
      @media(max-width:900px){.booking-layout{grid-template-columns:1fr}.booking-evidence{grid-template-columns:1fr 1fr}.booking-evidence>div{border-bottom:1px solid var(--app-border)}.booking-actions{grid-template-columns:1fr}.booking-actions>div:nth-child(2){justify-content:flex-start}}
      @media(max-width:600px){.booking-next{align-items:stretch;flex-direction:column}.booking-phase{grid-template-columns:30px minmax(0,1fr)}.booking-phase__state{grid-column:2}.booking-evidence{grid-template-columns:1fr}.booking-evidence>div{border-right:0}.booking-actions form{align-items:stretch;flex-direction:column}.booking-actions input{width:100%}}
    </style>
    """
    return render_operator_document(
        title=f"{customer_name} booking | Anata Agent",
        navigation=navigation,
        body=body,
        page_class="building-booking-workspace",
        extra_head=styles,
    )
