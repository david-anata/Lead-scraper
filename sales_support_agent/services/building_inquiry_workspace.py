"""Plain-language staff workspace for one Building inquiry."""

from __future__ import annotations

import html
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from sales_support_agent.services.ui_shell import render_operator_document
from sales_support_agent.services.building_lead_intake import (
    event_qualification_missing,
)
from sales_support_agent.services.building_event_journey import (
    resolve_event_next_action,
)


MOUNTAIN = ZoneInfo("America/Denver")

DETAIL_LABELS = {
    "alternateDate": "Second-choice date",
    "backupDate2": "Third-choice date",
    "dateFlexibility": "Date flexibility",
    "eventType": "Event type",
    "guestStartTime": "Guest start time",
    "guestEndTime": "Guest end time",
    "accessStartTime": "Setup or vendor access begins",
    "accessEndTime": "Teardown and final access ends",
    "groupSize": "Estimated attendance",
    "attendance": "Estimated attendance",
    "budgetRange": "Budget range",
    "catering": "Food and catering",
    "alcohol": "Alcohol plans",
    "alcoholPlan": "Alcohol service being considered",
    "avNeeds": "AV and sound",
    "accessibilityNeeds": "Accessibility needs",
    "vendorPlan": "Food and outside vendors",
    "tourInterest": "Tour interest",
    "organization": "Organization",
    "setupRequired": "Setup support needed",
    "notes": "Event needs and notes",
    "requestedOfferingReference": "Requested offering",
    "landingPage": "Landing page",
    "utmCampaign": "Campaign",
}

INTERVIEW_SECTIONS = (
    ("Event and timing", "Confirm the outcome, date choices, and complete access window.", (
        ("event_purpose", "Event purpose"), ("event_format", "Event format"),
        ("candidate_dates", "Candidate dates"), ("guest_schedule", "Guest schedule"),
        ("access_schedule", "Full access, setup, and teardown window"),
        ("attendance", "Expected and maximum attendance"),
    )),
    ("People and decision", "Know who decides, signs, pays, and when.", (
        ("decision_maker", "Decision maker"), ("authorized_signer", "Authorized signer"),
        ("billing_contact", "Billing contact"), ("decision_timeline", "Decision timeline"),
    )),
    ("Room and production", "Plan the physical room and technical experience.", (
        ("room_layout", "Room layout"), ("furniture", "Furniture needs"),
        ("av_and_sound", "AV and sound"), ("internet_and_power", "Internet and power"),
    )),
    ("Food, access, and vendors", "Capture regulated services and guest access needs.", (
        ("catering", "Catering"), ("alcohol", "Alcohol"),
        ("vendors_and_load_in", "Vendors and load-in"),
        ("parking_and_transportation", "Parking and transportation"),
        ("accessibility", "Accessibility"), ("security_and_staffing", "Security and staffing"),
        ("insurance", "Insurance"),
    )),
    ("Finish and follow-up", "Close open risks and leave one dated next step.", (
        ("decor_and_signage", "Decor and signage"), ("cleanup_and_waste", "Cleanup and waste"),
        ("marketing_and_media", "Marketing and media"), ("special_requests", "Special requests"),
        ("known_risks", "Known risks"), ("agreed_next_step", "Agreed next step"),
        ("operator_notes", "Internal operator notes"),
    )),
)
INTERVIEW_FIELDS = tuple(
    field for _, _, fields in INTERVIEW_SECTIONS for field in fields
)
#: The only answers that gate qualification (event_qualification_missing).
#: Twenty-eight questions rendered as one wall made these six hard to find,
#: so they lead the form and the remaining twenty-two collapse behind them.
REQUIRED_INTERVIEW_KEYS = (
    "event_purpose",
    "event_format",
    "candidate_dates",
    "guest_schedule",
    "attendance",
    "agreed_next_step",
)
REQUIRED_INTERVIEW_FIELDS = tuple(
    field for field in INTERVIEW_FIELDS if field[0] in REQUIRED_INTERVIEW_KEYS
)
OPTIONAL_INTERVIEW_SECTIONS = tuple(
    (title, description, tuple(
        field for field in fields if field[0] not in REQUIRED_INTERVIEW_KEYS
    ))
    for title, description, fields in INTERVIEW_SECTIONS
)


def _esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _when(value: datetime | None) -> str:
    if value is None:
        return "Not recorded"
    local = value if value.tzinfo else value.replace(tzinfo=ZoneInfo("UTC"))
    return local.astimezone(MOUNTAIN).strftime("%b %d, %Y · %I:%M %p MT")


def is_test_inquiry(*, name: str, email: str, source: str) -> bool:
    """Identify deterministic internal QA records without hiding prospects."""

    normalized_email = str(email or "").strip().casefold()
    local, _, domain = normalized_email.partition("@")
    normalized_name = str(name or "").strip().casefold()
    normalized_source = str(source or "").strip().casefold()
    if normalized_source in {"production_qa", "qa"}:
        return True
    if domain == "anatainc.com" and "+" in local:
        return True
    markers = (" qa", "qa ", "test —", "test -", "production qa")
    return domain == "anatainc.com" and any(
        marker in f" {normalized_name} " for marker in markers
    )


#: Activity entries shown before the rest folds away.
ACTIVITY_VISIBLE = 3

#: How each calendar state reads to an operator, and the tone it carries.
_DAY_STATES = {
    "open": ("Open", "open"),
    "pending": ("Held", "pending"),
    "booked": ("Booked", "booked"),
    "external": ("Busy", "external"),
    "heads_up": ("Next to a full day", "headsup"),
}


def _calendar_section(
    data: dict[str, Any],
    *,
    interview: dict[str, Any],
    csrf_token: str,
    hold_control: str,
    blockers: list[str],
) -> str:
    """Render the month calendar and, once a day is chosen, its time picker.

    Choosing a date is a link, not a script, so the page works the same way the
    rest of Agent does and a chosen date survives a refresh.
    """

    view = dict(data.get("calendar") or {})
    inquiry_id = _esc(data.get("id"))
    if not view:
        # Rendering nothing here would leave an operator with no way to take a
        # date and no reason given. Say what is missing instead.
        return (
            '<section class="lead-panel" id="date-review">'
            '<div class="lead-panel__head"><div><h2>Pick the date</h2>'
            '<p>No event offering is set up yet, so there is nothing to book '
            'against. Add one under Building settings, then this calendar '
            'appears.</p></div></div></section>'
        )
    selected = str(data.get("selected_date") or "")
    base = f"/admin/building/inquiries/{inquiry_id}"

    def month_link(value: Any, label: str, arrow: str) -> str:
        target = f"{base}?month={_esc(value)}"
        if selected:
            target += f"&date={_esc(selected)}"
        return (
            f'<a class="lead-cal__nav" href="{target}#date-review" '
            f'aria-label="{_esc(label)}">{arrow}</a>'
        )

    headers = "".join(
        f"<th scope=\"col\"><abbr title=\"{day}\">{day[:3]}</abbr></th>"
        for day in (
            "Monday", "Tuesday", "Wednesday", "Thursday",
            "Friday", "Saturday", "Sunday",
        )
    )
    cells: list[str] = []
    for cell in view.get("cells", []):
        label, tone = _DAY_STATES.get(str(cell.get("state")), ("Open", "open"))
        classes = [f"lead-cal__day lead-cal__day--{tone}"]
        if not cell.get("in_month"):
            classes.append("is-outside")
        if cell.get("is_today"):
            classes.append("is-today")
        if cell.get("is_past"):
            classes.append("is-past")
        if cell.get("iso") == selected:
            classes.append("is-selected")
        if cell.get("requested"):
            classes.append("is-requested")
        inner = f'<span class="lead-cal__num">{_esc(cell.get("day"))}</span>'
        note = str(cell.get("note") or "")
        if cell.get("requested"):
            note = f"Asked for · {note}" if note else "Asked for"
        if note:
            inner += f'<span class="lead-cal__note">{_esc(note)}</span>'
        described = f"{cell.get('iso')} · {note or label}"
        if cell.get("selectable"):
            body = (
                f'<a href="{base}?month={_esc(view.get("month"))}'
                f'&date={_esc(cell.get("iso"))}#date-review" '
                f'title="{_esc(described)}">{inner}</a>'
            )
        else:
            body = f'<span title="{_esc(described)}">{inner}</span>'
        cells.append(f'<td class="{" ".join(classes)}">{body}</td>')
    weeks = "".join(
        "<tr>" + "".join(cells[index:index + 7]) + "</tr>"
        for index in range(0, len(cells), 7)
    )
    legend = "".join(
        f'<span class="lead-cal__key lead-cal__key--{tone}">{_esc(label)}</span>'
        for label, tone in (
            ("Open", "open"), ("Held", "pending"), ("Booked", "booked"),
            ("Busy elsewhere", "external"), ("Next to a full day", "headsup"),
            ("Asked for", "requested"),
        )
    )
    warning = ""
    if str(view.get("calendar_status")) != "connected":
        warning = (
            '<p class="lead-cal__warning">The Anata Events calendar could not be '
            'read, so only Agent\'s own holds are shown here. An open day is not '
            'confirmed until that calendar is reachable.</p>'
        )
    picker = _confirm_contract_panel(
        data, interview=interview, csrf_token=csrf_token, blockers=blockers
    )
    if not picker:
        picker = _time_picker(
            data,
            interview=interview,
            csrf_token=csrf_token,
            hold_control=hold_control,
        )
    return f'''<section class="lead-panel" id="date-review"><div class="lead-panel__head"><div><h2>Pick the date</h2><p>Open days are clear on both Agent and the Anata Events calendar. Choose a day, then the hours.</p></div></div>
          <div class="lead-cal">
            <div class="lead-cal__bar">{month_link(view.get("previous"), "Previous month", "←")}<strong>{_esc(view.get("label"))}</strong>{month_link(view.get("next"), "Next month", "→")}</div>
            <table class="lead-cal__grid" aria-label="Arena availability for {_esc(view.get("label"))}"><thead><tr>{headers}</tr></thead><tbody>{weeks}</tbody></table>
            <div class="lead-cal__legend">{legend}</div>{warning}
          </div>{picker}
        </section>'''


def _contract_link(data: dict[str, Any], *, csrf_token: str) -> str:
    """Offer to create the contract, or link to the one this lead already has."""

    agreement = dict(data.get("agreement") or {})
    if agreement:
        state = str(agreement.get("status") or "").replace("_", " ")
        signing = (
            f' · <a href="{_esc(agreement.get("document_url"))}" target="_blank" '
            'rel="noopener">Open the signing Doc</a>'
            if agreement.get("document_url")
            else ""
        )
        return (
            '<div class="lead-price__contract">'
            f'<a class="lead-button lead-button--primary" '
            f'href="/admin/building/contracts/{_esc(agreement.get("id"))}">'
            f'Open the contract</a>'
            f'<span>Version {_esc(agreement.get("version"))} · {_esc(state)}{signing}</span>'
            "</div>"
        )
    return (
        '<form class="lead-price__contract" method="post" '
        f'action="/admin/building/inquiries/{_esc(data.get("id"))}/contract">'
        f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
        '<button class="lead-button" type="submit">Create the contract</button>'
        "<span>Writes this pricing into the booking and prepares the contract. "
        "Nothing is sent.</span>"
        "</form>"
    )


def _confirm_contract_panel(
    data: dict[str, Any],
    *,
    interview: dict[str, Any],
    csrf_token: str,
    blockers: list[str],
) -> str:
    """Ask before a contract takes a date, instead of refusing to make one.

    Shows the exact window and any clash, so agreeing is a decision about a
    named date rather than a click through a warning.
    """

    confirm = dict(data.get("confirm_contract") or {})
    if not confirm:
        return ""
    action = f"/admin/building/inquiries/{_esc(data.get('id'))}/contract"
    if blockers:
        # Offering a button the hold would refuse just moves the dead end one
        # click later.
        return (
            '<div class="lead-cal__confirm"><h3>A few answers first</h3>'
            '<p>Taking a date needs these from the interview above: '
            f'{_esc(", ".join(blockers))}. Answer them and create the contract '
            'again.</p></div>'
        )
    if not confirm.get("ready"):
        return (
            '<div class="lead-cal__confirm"><h3>Which date is this contract for?</h3>'
            f'<p>{_esc(confirm.get("message"))}</p></div>'
        )
    clash = str(confirm.get("clash") or "")
    warning = (
        '<p class="lead-cal__clash"><strong>That date is already taken '
        f'({_esc(clash)}).</strong> Going ahead books two things at once, on '
        'your authority, and it is recorded against the booking.</p>'
        if clash else ""
    )
    label = "Double-book it and create the contract" if clash else "Yes, hold it and create the contract"
    return f'''<div class="lead-cal__confirm">
            <h3>Hold {_esc(confirm.get("label"))} and create the contract?</h3>
            <p>In the building <strong>{_esc(confirm.get("setup"))}</strong> to
            <strong>{_esc(confirm.get("teardown"))}</strong>, guests
            {_esc(confirm.get("guests"))}. The date is held for seven days and the
            pricing on this page is frozen into the contract. Nothing is sent.</p>{warning}
            <form method="post" action="{action}">
              <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
              <input type="hidden" name="confirm_hold" value="yes">
              <input type="hidden" name="event_date" value="{_esc(confirm.get("date"))}">
              <input type="hidden" name="guest_start_time" value="{_esc(confirm.get("guest_start"))}">
              <input type="hidden" name="guest_end_time" value="{_esc(confirm.get("guest_end"))}">
              <input type="hidden" name="attendance" value="{_esc(_attendance_guess(interview))}">
              {'<input type="hidden" name="override_conflicts" value="yes">' if clash else ''}
              <div class="lead-interview__save">
                <button class="lead-button {'lead-button--danger' if clash else 'lead-button--primary'}" type="submit">{_esc(label)}</button>
                <a class="lead-button" href="/admin/building/inquiries/{_esc(data.get('id'))}#date-review">Pick a different date</a>
              </div>
            </form>
          </div>'''


def _time_picker(
    data: dict[str, Any],
    *,
    interview: dict[str, Any],
    csrf_token: str,
    hold_control: str,
) -> str:
    """The hours for a chosen day, with setup and teardown already worked out."""

    selected = str(data.get("selected_date") or "")
    if not selected:
        return (
            '<p class="lead-cal__prompt">Choose an open day above to set the '
            'hours and take the date.</p>'
        )
    options = list(data.get("hour_options") or [])
    guest_start = str(data.get("guest_start") or "")
    guest_end = str(data.get("guest_end") or "")
    # On a day being deliberately double-booked, every hour is already taken.
    # Disabling them would block the very submission the warning is inviting.
    occupied_day = bool(data.get("selected_occupied"))

    def select(name: str, chosen: str, label: str) -> str:
        items = "".join(
            '<option value="{value}"{sel}{dis}>{text}</option>'.format(
                value=_esc(option.get("value")),
                sel=" selected" if option.get("value") == chosen else "",
                dis=" disabled" if option.get("taken") and not occupied_day else "",
                text=_esc(
                    f"{option.get('label')} — taken"
                    if option.get("taken")
                    else option.get("label")
                ),
            )
            for option in options
        )
        return f"<label>{_esc(label)}<select name=\"{name}\" required>{items}</select></label>"

    window = dict(data.get("preview_window") or {})
    summary = ""
    if window:
        summary = (
            '<p class="lead-cal__summary">In the building '
            f'<strong>{_esc(window.get("setup"))}</strong> to '
            f'<strong>{_esc(window.get("teardown"))}</strong>, '
            f'guests {_esc(window.get("guests"))}. '
            'Setup and teardown are three hours either side.</p>'
        )
    # An occupied day can still be taken. It says so plainly, carries the
    # override, and the button stops pretending this is an ordinary hold.
    occupied = bool(data.get("selected_occupied"))
    override_field = ""
    control = hold_control
    if occupied:
        override_field = '<input type="hidden" name="override_conflicts" value="yes">'
        summary += (
            '<p class="lead-cal__clash"><strong>This date is already taken '
            f'({_esc(data.get("selected_note") or "occupied")}).</strong> '
            'Taking it books two things at once, on your authority, and it is '
            'recorded against the booking.</p>'
        )
        if not str(hold_control).count("disabled"):
            control = (
                '<div class="lead-interview__save">'
                '<button class="lead-button lead-button--danger" type="submit">'
                'Double-book this date</button>'
                '<span>Recorded as your decision. Nothing is sent.</span></div>'
            )
    return f'''<form class="lead-availability lead-availability--hold" method="post" action="/admin/building/inquiries/{_esc(data.get('id'))}/hold-date">
            <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
            <input type="hidden" name="event_date" value="{_esc(selected)}">{override_field}
            <h3>{_esc(data.get("selected_label") or selected)}</h3>
            <div class="lead-availability__times">
              {select("guest_start_time", guest_start, "Guests arrive")}
              {select("guest_end_time", guest_end, "Guests leave")}
              <label>Attendance<input type="number" name="attendance" min="1" value="{_esc(_attendance_guess(interview))}"></label>
            </div>{summary}
            {control}
          </form>'''


def _status(value: str) -> str:
    normalized = str(value or "new").replace("_", " ")
    tone = (
        "confirmed"
        if normalized in {"qualified", "closed won", "delivered", "sent"}
        else "blocked"
        if normalized in {"closed lost", "failed", "bounced", "complained"}
        else "review"
        if normalized in {"new", "responded", "queued", "unknown"}
        else "neutral"
    )
    return f'<span class="app-status app-status--{tone}">{_esc(normalized.title())}</span>'


def _next_action(data: dict[str, Any]) -> dict[str, str]:
    stage = str((data.get("lifecycle") or {}).get("stage") or "new")
    inquiry_id = str(data.get("id") or "")
    if stage == "new":
        return {
            "title": "Respond to this inquiry.",
            "body": "Contact the prospect, record the meaningful response, and keep the event request attached to this record.",
            "kind": "respond",
            "href": "",
            "label": "Record response",
        }
    if data.get("kind") == "event" and stage == "responded":
        missing = event_qualification_missing(
            dict(data.get("event_interview") or {}),
            dict(data.get("details") or {}),
        )
        if not missing:
            return {
                "title": "Qualify this event request.",
                "body": "The minimum discovery record is complete. Qualification carries the request into date review without promising availability.",
                "kind": "qualify",
                "href": "",
                "label": "Qualify for date review",
            }
        return {
            "title": "Finish the minimum event interview.",
            "body": "Still needed before qualification: " + ", ".join(missing) + ".",
            "kind": "link",
            "href": "#event-interview",
            "label": "Continue interview",
        }
    if stage == "qualified" and not data.get("reservation_id"):
        return {
            "title": "Review the requested dates.",
            "body": "Compare the complete access window, then prepare a temporary hold only if the date is conflict-free.",
            "kind": "link",
            "href": "#date-review",
            "label": "Review date",
        }
    if data.get("reservation_id"):
        resolved = resolve_event_next_action(data)
        return {
            "title": str(resolved.get("title") or "Continue this event."),
            "body": str(resolved.get("body") or "Review the current evidence below."),
            "kind": "link",
            "href": str(resolved.get("href") or "#event-summary"),
            "label": str(resolved.get("label") or "Continue"),
        }
    return {
        "title": "This inquiry is complete.",
        "body": "Review the activity below before reopening or starting a new request.",
        "kind": "link",
        "href": "/admin/building/sales",
        "label": "Return to Sales",
    }


def _attendance_guess(interview: dict[str, Any]) -> str:
    """Pull a number out of a free-text attendance answer, if there is one."""

    for token in str(interview.get("attendance") or "").replace(",", " ").split():
        if token.isdigit():
            return token
    return ""


def _deposit_label(plan: dict[str, Any]) -> str:
    kind = str(plan.get("deposit_type") or "none")
    if kind == "fixed":
        return f"{plan.get('currency', 'USD')} {int(plan.get('deposit_amount_cents') or 0) / 100:,.2f}"
    if kind == "percent":
        return f"{int(plan.get('deposit_percent_bps') or 0) / 100:g}%"
    return "Full amount"


def _money_value(cents: Any, currency: str = "USD") -> str:
    return f"{str(currency or 'USD').upper()} {int(cents or 0) / 100:,.2f}"


def _journey_sections(data: dict[str, Any], *, csrf_token: str) -> str:
    """Render later-stage evidence and actions on the canonical inquiry page."""

    journey = dict(data.get("journey") or {})
    reservation = dict(journey.get("reservation") or {})
    if not reservation:
        return ""
    inquiry_id = str(data.get("id") or "")
    reservation_id = str(reservation.get("id") or "")
    return_to = f"/admin/building/inquiries/{inquiry_id}"
    contract = dict(journey.get("contract") or {})
    billing = dict(journey.get("billing") or {})
    calendar = dict(journey.get("calendar") or {})
    deposit = dict(journey.get("deposit_evidence") or {})

    summary = f'''<section class="lead-panel" id="event-summary">
      <div class="lead-panel__head"><div><h2>Customer and event</h2><p>The same authoritative booking remains attached to this inquiry.</p></div>{_status(str(reservation.get("status") or "not started"))}</div>
      <dl class="lead-details">
        <dt>Venue</dt><dd>{_esc(reservation.get("space_name") or "Not linked")}</dd>
        <dt>Full access</dt><dd>{_esc(_when(reservation.get("starts_at")))} – {_esc(_when(reservation.get("ends_at")))}</dd>
        <dt>Guest event</dt><dd>{_esc(_when(reservation.get("guest_starts_at")))} – {_esc(_when(reservation.get("guest_ends_at")))}</dd>
        <dt>Attendance</dt><dd>{_esc(reservation.get("attendance") or "Not set")}</dd>
        <dt>Owner</dt><dd>{_esc(reservation.get("assigned_owner") or data.get("assigned_owner") or "Unassigned")}</dd>
        <dt>Temporary hold</dt><dd>{_esc(_when(reservation.get("hold_expires_at"))) if reservation.get("hold_expires_at") else "No active expiry"}</dd>
      </dl>
    </section>'''

    agreement_section = '<section class="lead-panel" id="agreement"><div class="lead-panel__head"><div><h2>Agreement and signature</h2><p>No signature or delivery is claimed without recorded evidence.</p></div></div>'
    if not contract:
        agreement_section += '<div class="lead-journey-empty"><strong>No agreement yet.</strong><span>Review saved pricing above, then create the contract. Nothing is sent.</span></div>'
    else:
        comparison = dict(contract.get("template_comparison") or {})
        signature = dict(contract.get("signature") or {})
        differences = list(comparison.get("differences") or [])
        difference_html = (
            '<ul class="lead-journey-list">' + ''.join(f'<li>{_esc(item)}</li>' for item in differences) + '</ul>'
            if differences else '<p class="lead-journey-ok">Frozen package matches the approved template.</p>'
        )
        ready = contract.get("preparation_status") == "approved" and bool(contract.get("document_url"))
        agreement_section += f'''<dl class="lead-details">
          <dt>Package</dt><dd>Version {_esc(contract.get("version"))} · {_status(str(contract.get("preparation_status") or "not started"))}</dd>
          <dt>Frozen value</dt><dd>{_esc(_money_value(contract.get("amount_cents"), contract.get("currency") or "USD"))}</dd>
          <dt>Template</dt><dd>{_esc((contract.get("template") or {}).get("name") or "Not linked")}</dd>
          <dt>Signing</dt><dd>{_status(str(reservation.get("agreement_status") or "not started"))}</dd>
          <dt>Signer</dt><dd>{_esc(signature.get("signer_email") or data.get("email"))}</dd>
        </dl><div class="lead-journey-content"><h3>Template check</h3>{difference_html}'''
        if not ready:
            agreement_section += f'''<form method="post" action="/admin/building/contracts/{_esc(contract.get("id"))}/ready-to-send">
              <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="return_to" value="{_esc(return_to)}#agreement">
              <button class="lead-button lead-button--primary" type="submit">Approve and create the signing copy</button>
              <span class="lead-action-note">Approves the frozen package and creates a Google Doc. Nothing is emailed.</span>
            </form>'''
        else:
            agreement_section += f'''<p><a class="lead-button lead-button--primary" href="{_esc(contract.get("document_url"))}" target="_blank" rel="noopener">Open signing Doc</a></p>
            <details class="lead-inline-action"><summary>Record delivery or signed evidence</summary>
              <form method="post" action="/admin/building/reservations/{_esc(reservation_id)}/agreements">
                <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="return_to" value="{_esc(return_to)}#agreement">
                <input type="hidden" name="version" value="{_esc(contract.get("version") or 1)}"><input type="hidden" name="provider" value="google_docs">
                <label>Evidence state<select name="status"><option value="sent">Sent for signature</option><option value="signed">Signed</option><option value="voided">Voided</option></select></label>
                <label>Google document or request reference<input name="provider_reference" required></label>
                <label>Document URL<input type="url" name="document_url" value="{_esc(contract.get("document_url"))}" required></label>
                <label>E-sign certificate reference<input name="esign_certificate_reference"></label>
                <label>Signed PDF SHA-256<input name="signed_document_checksum" minlength="64" maxlength="64"></label>
                <button class="lead-button" type="submit">Record provider evidence</button>
              </form>
            </details>'''
        agreement_section += f'<p><a href="/admin/building/contracts/{_esc(contract.get("id"))}">Advanced contract record</a></p></div>'
    agreement_section += '</section>'

    schedules = list(billing.get("schedules") or [])
    invoices = list(billing.get("invoices") or [])
    invoice_by_schedule = {str(row.get("billing_schedule_id") or ""): row for row in invoices}
    schedule_rows = ''
    for row in schedules:
        schedule_id = str(row.get("id") or "")
        action = '—'
        if row.get("status") == "draft":
            action = f'''<form method="post" action="/admin/building/billing/schedules/{_esc(schedule_id)}/approve"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="return_to" value="{_esc(return_to)}#billing"><button class="lead-button" type="submit">Approve</button></form>'''
        elif row.get("status") == "approved" and schedule_id not in invoice_by_schedule:
            action = f'''<form method="post" action="/admin/building/billing/schedules/{_esc(schedule_id)}/invoice"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="return_to" value="{_esc(return_to)}#billing"><input type="hidden" name="confirmation" value="INVOICE {_esc(schedule_id)}"><button class="lead-button" type="submit">Create QuickBooks draft</button></form>'''
        schedule_rows += f'<tr><td>{_esc(str(row.get("component") or "charge").replace("_", " ").title())}</td><td>{_esc(_money_value(row.get("amount_cents"), row.get("currency") or "USD"))}</td><td>{_status(str(row.get("status") or "draft"))}</td><td>{action}</td></tr>'
    if not schedule_rows:
        schedule_rows = '<tr><td colspan="4">No billing drafts prepared.</td></tr>'
    invoice_rows = ''
    for row in invoices:
        provider_link = f'<a href="{_esc(row.get("url"))}" target="_blank" rel="noopener">Open QuickBooks</a>' if row.get("url") else 'Not created'
        sync = f'''<form method="post" action="/admin/building/billing/invoices/{_esc(row.get("id"))}/sync-qbo"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="return_to" value="{_esc(return_to)}#billing"><button class="lead-button" type="submit">Refresh evidence</button></form>''' if row.get("qbo_invoice_id") else '—'
        invoice_rows += f'<tr><td>{_esc(row.get("qbo_invoice_id") or "Agent draft")}</td><td>{_status(str(row.get("status") or "draft"))}</td><td>{_esc(_money_value(row.get("amount_due_cents"), row.get("currency") or "USD"))}</td><td>{_esc(_money_value(row.get("amount_paid_cents"), row.get("currency") or "USD"))}</td><td>{provider_link} {sync}</td></tr>'
    if not invoice_rows:
        invoice_rows = '<tr><td colspan="5">No QuickBooks invoice created.</td></tr>'
    can_prepare = bool(
        reservation.get("agreement_status") == "signed"
        and list(journey.get("quotes") or [])
        and (journey.get("quotes") or [])[0].get("status") == "accepted"
        and (contract.get("payment") or {}).get("status") == "approved"
        and not schedules
    )
    prepare = f'''<form class="lead-primary-form" method="post" action="/admin/building/bookings/{_esc(reservation_id)}/billing/prepare"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="return_to" value="{_esc(return_to)}#billing"><button class="lead-button lead-button--primary" type="submit">Prepare exact billing drafts</button><span>Creates no QuickBooks object and sends nothing.</span></form>''' if can_prepare else ''
    account = dict(billing.get("account") or {})
    billing_section = f'''<section class="lead-panel" id="billing"><div class="lead-panel__head"><div><h2>QuickBooks invoice and payment</h2><p>Prepared, invoiced, and paid remain separate evidence states.</p></div>{_status(str(reservation.get("deposit_status") or "not started"))}</div>
      <dl class="lead-details"><dt>Billing customer</dt><dd>{_esc(account.get("account_name") or "Created with billing drafts")}</dd><dt>QuickBooks customer</dt><dd>{_esc(account.get("qbo_customer_id") or "Not created")}</dd><dt>Payment evidence</dt><dd>{_esc(deposit.get("provider_reference") or "Not verified")} · {_status(str(deposit.get("status") or "not started"))}</dd></dl>{prepare}
      <div class="lead-table"><table><thead><tr><th>Charge</th><th>Amount</th><th>State</th><th>Action</th></tr></thead><tbody>{schedule_rows}</tbody></table></div>
      <div class="lead-table"><table><thead><tr><th>QuickBooks</th><th>State</th><th>Due</th><th>Paid</th><th>Evidence</th></tr></thead><tbody>{invoice_rows}</tbody></table></div>
    </section>'''

    can_confirm = bool(
        reservation.get("status") in {"contract_pending", "deposit_due"}
        and reservation.get("agreement_status") == "signed"
        and (not reservation.get("deposit_required") or reservation.get("deposit_status") == "paid")
    )
    confirmation_action = ''
    if can_confirm:
        confirmation_action = f'''<form class="lead-primary-form" method="post" action="/admin/building/reservations/{_esc(reservation_id)}/transition"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="return_to" value="{_esc(return_to)}#confirmation"><input type="hidden" name="target_status" value="confirmed"><input type="hidden" name="reason" value="Final customer, agreement, payment, inventory, and Anata Events calendar gates reviewed from the customer record."><button class="lead-button lead-button--primary" type="submit">Confirm booking</button><span>Rechecks every authoritative gate before changing inventory.</span></form>'''
    elif reservation.get("status") == "confirmed" and calendar.get("status") == "synced":
        confirmation_action = f'''<form class="lead-primary-form" method="post" action="/admin/building/reservations/{_esc(reservation_id)}/transition"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="return_to" value="{_esc(return_to)}#operations"><input type="hidden" name="target_status" value="pre_event"><input type="hidden" name="reason" value="Confirmed booking and dedicated calendar projection verified; begin event operations."><button class="lead-button lead-button--primary" type="submit">Begin event operations</button></form>'''
    calendar_action = ''
    if reservation.get("status") in {"confirmed", "pre_event", "cancelled", "expired"} and calendar.get("status") in {"pending", "error", "claimed"}:
        deleting = reservation.get("status") in {"cancelled", "expired"}
        calendar_action = f'''<form class="lead-primary-form" method="post" action="/admin/building/inquiries/{_esc(inquiry_id)}/calendar-sync"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="confirmation" value="SYNC {_esc(reservation_id)}"><button class="lead-button" type="submit">{'Clear this date from Anata Events' if deleting else 'Retry this calendar update'}</button><span>{'Deletes only this cancelled event from the dedicated Anata Events calendar.' if deleting else 'Writes only this confirmed event to the dedicated Anata Events calendar.'}</span></form>'''
    confirmation_section = f'''<section class="lead-panel" id="confirmation"><div class="lead-panel__head"><div><h2>Confirmation and calendar</h2><p>Agent confirms only after agreement, payment, inventory, and calendar checks pass.</p></div>{_status(str(reservation.get("status") or "not started"))}</div>
      <dl class="lead-details"><dt>Agreement</dt><dd>{_status(str(reservation.get("agreement_status") or "not started"))}</dd><dt>Required payment</dt><dd>{_status(str(reservation.get("deposit_status") or "not started"))}</dd><dt>Anata Events projection</dt><dd>{_status(str(calendar.get("status") or "not started"))}<br>{_esc(calendar.get("last_error") or calendar.get("provider_event_id") or "No provider evidence yet")}</dd></dl>{confirmation_action}{calendar_action}
    </section>'''

    communication_rows = ''
    for row in list(journey.get("communications") or []):
        retry_control = "—"
        if row.get("status") in {"failed", "not_configured"}:
            retry_control = f'''<form method="post" action="/admin/building/bookings/{_esc(reservation_id)}/communications/{_esc(row.get("milestone"))}/retry"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="return_to" value="{_esc(return_to)}#communications"><button class="lead-button" type="submit">Retry</button></form>'''
        communication_rows += f'<tr><td>{_esc(str(row.get("milestone") or "update").replace("_", " ").title())}<small>Template v{_esc(row.get("template_version"))}</small></td><td>{_status(str(row.get("status") or "queued"))}</td><td>{_esc(_when(row.get("delivered_at") or row.get("sent_at")))}</td><td>{_esc(row.get("provider_reference") or row.get("last_error") or "No provider evidence")}</td><td>{retry_control}</td></tr>'
    if not communication_rows:
        communication_rows = '<tr><td colspan="5">No later-stage customer messages yet.</td></tr>'
    communications_section = f'''<section class="lead-panel" id="communications"><div class="lead-panel__head"><div><h2>Customer communications</h2><p>Every milestone keeps its immutable template version and delivery evidence.</p></div></div><div class="lead-table"><table><thead><tr><th>Milestone</th><th>State</th><th>Sent or delivered</th><th>Evidence</th><th>Recovery</th></tr></thead><tbody>{communication_rows}</tbody></table></div></section>'''

    checklist_blocks = ''
    for checklist in list(journey.get("checklists") or []):
        item_rows = ''
        for item in list(checklist.get("items") or []):
            terminal = reservation.get("status") in {"cancelled", "expired", "completed"}
            control = 'Read only'
            if not terminal:
                item_status = str(item.get("status") or "pending")
                status_options = ''.join(
                    f'<option value="{value}"{" selected" if value == item_status else ""}>{label}</option>'
                    for value, label in (("pending", "Pending"), ("completed", "Completed"), ("waived", "Waived"))
                )
                control = f'''<details class="lead-inline-action"><summary>Update</summary><form method="post" action="/admin/building/checklists/items/{_esc(item.get("id"))}/status"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="return_to" value="{_esc(return_to)}#operations"><label>State<select name="status">{status_options}</select></label><label>Owner<input name="assigned_owner" value="{_esc(item.get("assigned_owner") or "")}"></label><label>Due<input type="datetime-local" name="due_at" value="{_esc(item.get("due_at").astimezone(MOUNTAIN).strftime("%Y-%m-%dT%H:%M") if item.get("due_at") else "")}"></label><label>Evidence notes<textarea name="reason">{_esc(item.get("completion_reason") or "")}</textarea></label><label>Evidence reference<input name="evidence_reference" value="{_esc(item.get("evidence_reference") or "")}"></label><button class="lead-button" type="submit">Save operational evidence</button></form></details>'''
            item_rows += f'<tr><td>{_esc(item.get("label"))}</td><td>{_status(str(item.get("status") or "pending"))}</td><td>{_esc(item.get("assigned_owner") or "Unassigned")}</td><td>{_esc(_when(item.get("due_at")))}</td><td>{_esc(item.get("evidence_reference") or "Not recorded")}</td><td>{control}</td></tr>'
        rendered_items = item_rows or '<tr><td colspan="6">No checklist items.</td></tr>'
        checklist_blocks += f'<div class="lead-journey-content"><h3>{_esc(checklist.get("title"))} · {_esc(str(checklist.get("status") or "open").replace("_", " ").title())}</h3><div class="lead-table"><table><thead><tr><th>Work</th><th>State</th><th>Owner</th><th>Due</th><th>Evidence</th><th>Action</th></tr></thead><tbody>{rendered_items}</tbody></table></div></div>'
    if not checklist_blocks:
        checklist_blocks = '<div class="lead-journey-empty"><strong>Operations begin after confirmation.</strong><span>The governed checklist is created by the booking transition.</span></div>'
    service_rows = ''.join(f'<li><strong>{_esc(row.get("title"))}</strong><span>{_esc(str(row.get("priority") or "normal").title())} · {_esc(str(row.get("status") or "new").replace("_", " ").title())} · {_esc(row.get("assigned_owner") or "Unassigned")}</span></li>' for row in list(journey.get("service_requests") or [])) or '<li>No event support requests.</li>'
    closeout = ''
    if reservation.get("status") == "pre_event" and list(journey.get("checklists") or []) and all(row.get("status") == "completed" for row in journey.get("checklists") or []):
        closeout = f'''<form class="lead-primary-form" method="post" action="/admin/building/reservations/{_esc(reservation_id)}/transition"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="return_to" value="{_esc(return_to)}#operations"><input type="hidden" name="target_status" value="completed"><label>Closeout outcome and deposit disposition<textarea name="reason" required></textarea></label><button class="lead-button lead-button--primary" type="submit">Complete event closeout</button></form>'''
    operations_section = f'''<section class="lead-panel" id="operations"><div class="lead-panel__head"><div><h2>Event operations and closeout</h2><p>Owners, deadlines, completion or waiver evidence, and support work remain with this customer.</p></div></div>{checklist_blocks}<div class="lead-journey-content"><h3>Event support</h3><ol class="lead-follow-up">{service_rows}</ol><details class="lead-inline-action"><summary>Add event support request</summary><form method="post" action="/admin/building/service-requests"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="return_to" value="{_esc(return_to)}#operations"><input type="hidden" name="category" value="event_support"><input type="hidden" name="priority" value="normal"><input type="hidden" name="space_id" value="{_esc(reservation.get("space_id"))}"><input type="hidden" name="contact_id" value="{_esc((data.get("contact") or {}).get("id"))}"><input type="hidden" name="reservation_id" value="{_esc(reservation_id)}"><input type="hidden" name="source" value="operator"><input type="hidden" name="source_reference" value="inquiry:{_esc(inquiry_id)}"><input type="hidden" name="assigned_owner" value="{_esc(reservation.get("assigned_owner") or data.get("assigned_owner"))}"><label>Request<input name="title" required></label><label>Details<textarea name="description"></textarea></label><button class="lead-button" type="submit">Add support request</button></form></details>{closeout}</div></section>'''

    return summary + agreement_section + billing_section + confirmation_section + communications_section + operations_section


def render_inquiry_workspace(
    *,
    navigation: str,
    data: dict[str, Any],
    csrf_token: str,
    notice: str = "",
    error: str = "",
) -> str:
    """Render one customer-named inquiry without unrelated lead data."""

    next_action = _next_action(data)
    lifecycle = dict(data.get("lifecycle") or {})
    stage = str(lifecycle.get("stage") or "new")
    details = dict(data.get("details") or {})
    submitted = [
        ("Preferred event date", data.get("preferred_date")),
        *[
            (DETAIL_LABELS.get(str(key), str(key).replace("_", " ").title()), value)
            for key, value in details.items()
            if key != "eventHandoff" and str(value or "").strip()
        ],
    ]
    submitted_rows = "".join(
        f"<dt>{_esc(label)}</dt><dd>{_esc(value)}</dd>"
        for label, value in submitted
        if str(value or "").strip()
    ) or "<p>No additional event details were submitted.</p>"
    notification = dict(data.get("lead_notification") or {})
    escalation = dict(data.get("lead_escalation") or {})
    receipt = dict(data.get("customer_receipt") or {})
    notification_retry = (
        f'<form method="post" action="/admin/building/inquiries/{_esc(data.get("id"))}/notify"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><button class="lead-button" type="submit">Retry staff alert</button></form>'
        if notification.get("status") != "delivered"
        else ""
    )
    receipt_retry = (
        f'<form method="post" action="/admin/building/inquiries/{_esc(data.get("id"))}/receipt"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><button class="lead-button" type="submit">Retry acknowledgement</button></form>'
        if data.get("kind") == "event"
        and receipt.get("status") not in {"sent", "delivered", "delivery_delayed"}
        else ""
    )
    follow_up = list(data.get("follow_up_sequence") or [])
    follow_up_rows = "".join(
        f"<li><strong>{_esc(step.get('label'))}</strong><span>{_esc(str(step.get('status') or 'queued').replace('_', ' ').title())} · {_esc(step.get('due_at_display') or 'No due time')}</span></li>"
        for step in follow_up
    ) or "<li>No automated staff follow-up steps were recorded.</li>"
    interview = dict(data.get("event_interview") or {})
    answered = sum(
        bool(str(interview.get(key) or "").strip()) for key, _ in INTERVIEW_FIELDS
    )
    def _fieldset(title: str, description: str, fields: tuple) -> str:
        if not fields:
            return ""
        done = sum(bool(str(interview.get(key) or "").strip()) for key, _ in fields)
        labels = "".join(
            f'<label class="{"is-answered" if str(interview.get(key) or "").strip() else "is-missing"}">{_esc(label)}<textarea name="{_esc(key)}">{_esc(interview.get(key) or "")}</textarea></label>'
            for key, label in sorted(
                fields, key=lambda field: bool(str(interview.get(field[0]) or "").strip())
            )
        )
        return (
            f'<fieldset class="lead-interview__section"><legend>{_esc(title)}</legend>'
            f"<p>{_esc(description)}</p><span>{done}/{len(fields)} answered</span>"
            f'<div class="lead-interview__grid">{labels}</div></fieldset>'
        )

    required_done = sum(
        bool(str(interview.get(key) or "").strip())
        for key, _ in REQUIRED_INTERVIEW_FIELDS
    )
    required_block = _fieldset(
        "Needed to qualify",
        "These six answers are the only ones that gate qualifying this event.",
        REQUIRED_INTERVIEW_FIELDS,
    )
    optional_blocks = "".join(
        _fieldset(title, description, fields)
        for title, description, fields in OPTIONAL_INTERVIEW_SECTIONS
    )
    optional_total = len(INTERVIEW_FIELDS) - len(REQUIRED_INTERVIEW_FIELDS)
    section_blocks = (
        required_block
        + f'<details class="lead-interview__more"{" open" if required_done == len(REQUIRED_INTERVIEW_FIELDS) else ""}>'
        + f"<summary>Everything else ({optional_total} questions) — optional to qualify</summary>"
        + optional_blocks
        + "</details>"
    )
    contact_options = list(data.get("contact_options") or [])
    if contact_options:
        options = "".join(
            f'<option value="{_esc(item.get("id"))}">{_esc(item.get("label"))}</option>'
            for item in contact_options
        )
        # A dedicated link action: it writes the relationship only, so an
        # existing customer's saved details are never overwritten.
        link_existing_block = (
            f'<form class="lead-interview" method="post" action="/admin/building/inquiries/{_esc(data.get("id"))}/link-contact">'
            f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
            '<div class="lead-interview__grid" style="padding:0 20px">'
            f'<label style="grid-column:1/-1">Existing customer<select name="contact_id" required>'
            '<option value="">Choose a saved customer…</option>'
            f'{options}</select></label></div>'
            '<div class="lead-interview__save" style="padding:0 20px 16px">'
            '<button class="lead-button lead-button--primary" type="submit">Link this customer</button>'
            "<span>Links only. Their saved details are not changed.</span>"
            "</div></form>"
            '<p style="padding:0 20px;color:var(--agent-ink-muted);font-size:13px">Or add a new customer:</p>'
        )
    else:
        link_existing_block = ""
    contact = dict(data.get("contact") or {})
    if contact:
        customer_section = (
            '<section class="lead-panel"><div class="lead-panel__head"><div>'
            "<h2>Customer record</h2><p>This lead is linked to a saved customer.</p>"
            '</div></div><dl class="lead-contact">'
            f"<dt>Name</dt><dd>{_esc(contact.get('full_name'))}</dd>"
            f"<dt>Email</dt><dd>{_esc(contact.get('email'))}</dd>"
            f"<dt>Phone</dt><dd>{_esc(contact.get('phone') or chr(8212))}</dd>"
            "</dl></section>"
        )
    else:
        # Creating the customer used to mean leaving this page for the Contacts
        # tab and expanding a collapsed panel. Prefilled from the lead, so this
        # is a confirmation rather than re-typing what the customer already sent.
        customer_section = (
            '<section class="lead-panel"><div class="lead-panel__head"><div>'
            "<h2>Customer record</h2><p>No saved customer yet. Link an existing "
            "customer, or add a new one.</p></div></div>"
            + link_existing_block +
            '<form class="lead-interview" method="post" action="/admin/building/contacts">'
            f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
            f'<input type="hidden" name="source_reference" value="inquiry:{_esc(data.get("id"))}">'
            '<input type="hidden" name="relationship_type" value="prospect">'
            '<div class="lead-interview__grid" style="padding:0 20px">'
            f'<label>Full name<input name="full_name" value="{_esc(data.get("name"))}"></label>'
            f'<label>Email<input name="email" type="email" required value="{_esc(data.get("email"))}"></label>'
            f'<label>Phone<input name="phone" value="{_esc(data.get("phone"))}"></label>'
            '<label>Company<input name="company_name" placeholder="Optional"></label>'
            "</div>"
            '<div class="lead-interview__save" style="padding:0 20px 20px">'
            '<button class="lead-button lead-button--primary" type="submit">Save customer</button>'
            "<span>Creates the customer and links it to this lead. No message is sent.</span>"
            "</div></form></section>"
        )

    pricing = dict(data.get("pricing") or {})
    totals = dict(data.get("pricing_totals") or {})
    plans = list(data.get("rate_plans") or [])

    def _money(cents: object) -> str:
        return f"{int(cents or 0) / 100:,.2f}"

    addons = list(pricing.get("addons") or [])
    addon_rows = ""
    for index in range(len(addons) + 2):          # existing rows plus two blanks
        item = addons[index] if index < len(addons) else {}
        addon_rows += (
            '<div class="lead-price__addon">'
            f'<input name="addon_name_{index}" placeholder="Add-on" '
            f'value="{_esc(item.get("name", ""))}">'
            f'<input name="addon_amount_{index}" inputmode="decimal" placeholder="0.00" '
            f'value="{_money(item.get("amount_cents")) if item else ""}">'
            "</div>"
        )

    seeded_from = str(pricing.get("rate_plan_name") or "the owner-approved baseline")
    pricing_section = (
        '<section class="lead-panel" id="lead-pricing"><div class="lead-panel__head"><div>'
        "<h2>Pricing for this event</h2>"
        f"<p>Started from {_esc(seeded_from)}. Change anything here for this customer; "
        "the standard rate and every other lead stay as they are.</p></div>"
        f'<span class="lead-price__total">{_esc(pricing.get("currency") or "USD")} '
        f'<span data-total="total">{_money(totals.get("total_cents"))}</span></span></div>'
        f'<form class="lead-price" data-price-form method="post" action="/admin/building/inquiries/{_esc(data.get("id"))}/pricing">'
        f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
        '<div class="lead-price__grid">'
        f'<label>Hourly rate<input name="hourly_rate" inputmode="decimal" '
        f'value="{_money(pricing.get("hourly_rate_cents"))}"></label>'
        f'<label>Hours<input name="hours" inputmode="numeric" '
        f'value="{_esc(pricing.get("hours") or 0)}"></label>'
        f'<label>Cleaning fee<input name="cleaning_fee" inputmode="decimal" '
        f'value="{_money(pricing.get("cleaning_fee_cents"))}"></label>'
        f'<label>Deposit %<input name="deposit_percent" inputmode="decimal" '
        f'value="{int(pricing.get("deposit_percent_bps") or 0) / 100:g}"></label>'
        f'<label>Security deposit<input name="security_deposit" inputmode="decimal" '
        f'value="{_money(pricing.get("security_deposit_cents"))}"></label>'
        "</div>"
        f'<div class="lead-price__addons"><span>Add-ons</span>{addon_rows}</div>'
        '<div class="lead-price__grid">'
        f'<label>Discount<input name="discount" inputmode="decimal" '
        f'value="{_money(pricing.get("discount_cents"))}"></label>'
        f'<label>Reason for the discount<input name="discount_reason" '
        f'value="{_esc(pricing.get("discount_reason") or "")}" '
        'placeholder="Required if there is a discount"></label>'
        "</div>"
        '<table class="lead-pricing"><tbody>'
        f'<tr><td>Venue</td><td class="is-num" data-total="venue">{_money(totals.get("venue_cents"))}</td></tr>'
        f'<tr><td>Cleaning</td><td class="is-num" data-total="cleaning">{_money(totals.get("cleaning_cents"))}</td></tr>'
        f'<tr><td>Add-ons</td><td class="is-num" data-total="addons">{_money(totals.get("addons_cents"))}</td></tr>'
        f'<tr><td>Subtotal</td><td class="is-num" data-total="subtotal">{_money(totals.get("subtotal_cents"))}</td></tr>'
        f'<tr><td>Discount</td><td class="is-num">-<span data-total="discount">{_money(totals.get("discount_cents"))}</span></td></tr>'
        f'<tr><td>Taxable amount after discount</td><td class="is-num" data-total="taxable">{_money(totals.get("taxable_cents"))}</td></tr>'
        f'<tr><td>Sales tax ({int(pricing.get("tax_rate_bps") or 0) / 100:g}%)</td><td class="is-num" data-total="tax">{_money(totals.get("tax_cents"))}</td></tr>'
        f'<tr class="is-total"><td>Contract total</td><td class="is-num" data-total="total">{_money(totals.get("total_cents"))}</td></tr>'
        f'<tr><td>Booking deposit</td><td class="is-num" data-total="deposit">{_money(totals.get("deposit_cents"))}</td></tr>'
        f'<tr><td>Security deposit <span class="lead-price__note">refundable, not part of the total</span></td>'
        f'<td class="is-num" data-total="security">{_money(totals.get("security_deposit_cents"))}</td></tr>'
        f'<tr class="is-total"><td>Due to book</td><td class="is-num" data-total="due">{_money(totals.get("due_to_book_cents"))}</td></tr>'
        f'<tr><td>Balance before the event</td><td class="is-num" data-total="balance">{_money(totals.get("balance_cents"))}</td></tr>'
        "</tbody></table>"
        '<div class="lead-interview__save">'
        '<button class="lead-button lead-button--primary" type="submit" data-price-save>Save pricing</button>'
        '<span data-price-status>Applies to this lead only. Nothing is sent.</span>'
        "</div></form>"
        # The contract is an output of the lead, not a separate place to go.
        # Once it exists the lead links straight to it rather than offering to
        # make a second one.
        + _contract_link(data, csrf_token=csrf_token)
        + (
            '<p class="lead-price__standard">Standard rates: '
            + ", ".join(
                f'{_esc(plan.get("name"))} '
                f'{_esc(plan.get("public_price_display") or _money(plan.get("unit_amount_cents")))}'
                for plan in plans
            )
            + ' · <a href="/admin/building?view=settings#commercial-rate-plans">edit standards</a></p>'
            if plans else
            '<p class="lead-price__standard">No approved standard rate yet · '
            '<a href="/admin/building?view=settings#commercial-rate-plans">set one</a></p>'
        )
        + "</section>"
    )

    interview_section = (
        f'''<section class="lead-panel"><div class="lead-panel__head"><div><h2>Event interview</h2><p>{required_done} of {len(REQUIRED_INTERVIEW_FIELDS)} needed to qualify · {answered} of {len(INTERVIEW_FIELDS)} answered overall. Save partial progress at any time; this does not promise a date or price.</p></div></div>
            <details class="lead-interview" id="event-interview"{' open' if stage == 'responded' else ''}><summary>Review or update interview</summary><div class="lead-call-guide"><strong>Call guide</strong><span>Why this event? Which dates and full access window work? Who decides? What must the room support? What happens next, and when?</span></div><form data-interview-autosave method="post" action="/admin/building/inquiries/{_esc(data.get('id'))}/event-interview"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">{section_blocks}<div class="lead-interview__save"><button class="lead-button lead-button--primary" type="submit" name="save_mode" value="reviewed">Save and mark reviewed</button><span data-autosave-status role="status">Changes save automatically after you pause.</span></div></form></details>
          </section>'''
        if data.get("kind") == "event"
        else ""
    )
    # A hold needs the same interview answers the hold endpoint checks. Saying
    # so here beats letting an operator fill the form and be refused after.
    hold_blockers = (
        event_qualification_missing(interview, details)
        if data.get("kind") == "event"
        else []
    )
    if hold_blockers:
        hold_control = (
            '<div class="lead-interview__save">'
            '<button class="lead-button" type="submit" disabled>Hold this date</button>'
            '<span>Answer these in the interview above first: '
            + _esc(", ".join(hold_blockers))
            + '.</span></div>'
        )
    else:
        hold_control = (
            '<div class="lead-interview__save">'
            '<button class="lead-button lead-button--primary" type="submit">Hold this date</button>'
            '<span>Conflict-checked, held seven days, and freezes a quote. '
            'Nothing is sent.</span></div>'
        )
    availability_section = (
        _calendar_section(
            data,
            interview=interview,
            csrf_token=csrf_token,
            hold_control=hold_control,
            blockers=hold_blockers,
        )
        if data.get("kind") == "event" and not data.get("reservation_id")
        else ""
    )
    journey_sections = _journey_sections(data, csrf_token=csrf_token)
    def activity_line(item: dict[str, Any]) -> str:
        action = str(item.get("action") or "updated").replace("_", " ").title()
        return (
            f"<li><time>{_esc(_when(item.get('created_at')))}</time><div>"
            f"<strong>{_esc(action)}</strong>"
            f"<span>{_esc(item.get('actor') or 'System')}</span></div></li>"
        )

    entries = list(data.get("activity", []))
    recent = "".join(activity_line(item) for item in entries[:ACTIVITY_VISIBLE])
    activity_rows = recent or "<li><div><strong>No activity recorded.</strong></div></li>"
    older = entries[ACTIVITY_VISIBLE:]
    if older:
        # The rest stays on the page but folded away, so the record is complete
        # without the lead reading as a log file.
        activity_rows += (
            '<li class="lead-activity__more"><details><summary>'
            f"Show {len(older)} earlier {'entry' if len(older) == 1 else 'entries'}"
            '</summary><ul class="lead-activity__list">'
            + "".join(activity_line(item) for item in older)
            + "</ul></details></li>"
        )
    messages = ""
    if notice:
        messages += f'<div class="app-alert app-alert--notice"><p>{_esc(notice)}</p></div>'
    if error:
        messages += f'<div class="app-alert app-alert--error"><p>{_esc(error)}</p></div>'
    response_form = ""
    if next_action["kind"] == "respond":
        response_form = f"""
        <form class="lead-response" method="post" action="/admin/building/inquiries/{_esc(data.get('id'))}/lifecycle">
          <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
          <input type="hidden" name="target_stage" value="responded">
          <input type="hidden" name="assigned_owner" value="{_esc(data.get('assigned_owner'))}">
          <label>Response channel<select name="channel"><option value="email">Email</option><option value="phone">Phone</option><option value="text">Text</option><option value="in_person">In person</option><option value="other">Other</option></select></label>
          <label>What happened<textarea name="notes" required placeholder="Record the meaningful response and agreed next step."></textarea></label>
          <button class="lead-button lead-button--primary" type="submit">Record response</button>
        </form>"""
    elif next_action["kind"] == "qualify":
        response_form = f'''<form method="post" action="/admin/building/inquiries/{_esc(data.get('id'))}/lifecycle"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}"><input type="hidden" name="target_stage" value="qualified"><input type="hidden" name="assigned_owner" value="{_esc(data.get('assigned_owner'))}"><input type="hidden" name="channel" value="other"><input type="hidden" name="notes" value="Minimum event discovery completed; ready for date review."><button class="lead-button lead-button--primary" type="submit">Qualify for date review</button></form>'''

    # Everything after responding now happens on this page, so a panel sending
    # the operator to another screen is a leftover from when it did not. A
    # prompt that points further down this page still earns its place.
    on_page = next_action["kind"] in {"respond", "qualify"} or str(
        next_action.get("href") or ""
    ).startswith("#")
    next_section = ""
    if on_page:
        if next_action["kind"] not in {"respond", "qualify"}:
            response_form = (
                f'<a class="lead-button lead-button--primary" '
                f'href="{_esc(next_action["href"])}">{_esc(next_action["label"])} →</a>'
            )
        next_section = f"""
      <section class="lead-next" aria-labelledby="lead-next-title">
        <div><p class="app-eyebrow">Do this next</p><h2 id="lead-next-title">{_esc(next_action['title'])}</h2><p>{_esc(next_action['body'])}</p><div class="lead-next__contact"><a href="mailto:{_esc(data.get('email'))}">{_esc(data.get('email'))}</a><span>{_esc(data.get('phone') or 'No phone provided')}</span></div></div>
        {response_form}
      </section>"""

    body = f"""
      <header class="app-page-header lead-header">
        <div>
          <a class="lead-back" href="/admin/building/sales">← All inquiries</a>
          <p class="app-eyebrow">Building inquiry</p>
          <h1>{_esc(data.get('name') or data.get('email'))}</h1>
          <p>{_esc(data.get('kind', 'Lead').title())} request · received {_esc(_when(data.get('created_at')))}</p>
        </div>
        <div class="lead-header__states">{_status(stage)}{_status('test record') if data.get('is_test') else ''}</div>
      </header>
      {messages}{next_section}
      <section class="lead-layout">
        <div class="lead-stack">
          <section class="lead-panel"><div class="lead-panel__head"><div><h2>Original website submission</h2><p>The prospect's words, preserved under plain-language labels.</p></div></div><dl class="lead-details">{submitted_rows}</dl></section>
          {customer_section}
          {interview_section}
          {availability_section}
          {pricing_section}
          {journey_sections}
          <section class="lead-panel" id="activity"><div class="lead-panel__head"><div><h2>Activity</h2><p>Audited intake, delivery, and lifecycle evidence.</p></div></div><ol class="lead-activity">{activity_rows}</ol></section>
        </div>
        <aside class="lead-stack">
          <section class="lead-panel lead-contact"><div class="lead-panel__head"><div><h2>Contact</h2><p>Use the same customer identity through booking.</p></div></div><dl>
            <dt>Email</dt><dd><a href="mailto:{_esc(data.get('email'))}">{_esc(data.get('email'))}</a></dd>
            <dt>Phone</dt><dd>{_esc(data.get('phone') or 'Not provided')}</dd>
            <dt>Owner</dt><dd>{_esc(data.get('assigned_owner') or 'Unassigned')}</dd>
            <dt>Response due</dt><dd>{_esc(_when(data.get('response_due_at')))}</dd>
          </dl></section>
          <section class="lead-panel"><div class="lead-panel__head"><div><h2>Delivery evidence</h2><p>Accepted is distinct from delivered.</p></div></div><dl class="lead-details">
            <dt>Staff Slack alert</dt><dd>{_status(str(notification.get('status') or 'unknown'))}</dd>
            <dt>Overdue escalation</dt><dd>{_status(str(escalation.get('status') or 'not needed'))}</dd>
            <dt>Customer acknowledgement</dt><dd>{_status(str(receipt.get('status') or 'unknown'))}</dd>
          </dl><div class="lead-delivery-actions">{notification_retry}{receipt_retry}</div></section>
          <section class="lead-panel"><div class="lead-panel__head"><div><h2>Follow-up plan</h2><p>Internal tasks; these do not send customer messages.</p></div></div><ol class="lead-follow-up">{follow_up_rows}</ol></section>
          <section class="lead-panel"><div class="lead-panel__head"><div><h2>Source</h2></div></div><dl class="lead-details"><dt>Source</dt><dd>{_esc(data.get('source') or 'Unknown')}</dd><dt>Reference</dt><dd>{_esc(data.get('source_reference') or 'None')}</dd><dt>Campaign</dt><dd>{_esc((data.get('attribution') or {}).get('campaign') or 'No campaign')}</dd></dl></section>
        </aside>
      </section>
      <details class="lead-technical"><summary>Technical record details</summary><p>Inquiry reference: <code>{_esc(data.get('id'))}</code></p></details>
      <script>
      (() => {{
        const form = document.querySelector('[data-interview-autosave]');
        const status = document.querySelector('[data-autosave-status]');
        if (!form || !status || !window.fetch) return;
        let timer;
        const save = async () => {{
          status.textContent = 'Saving…';
          try {{
            const response = await fetch(form.action, {{method:'POST', body:new FormData(form), headers:{{'X-Requested-With':'building-interview-autosave'}}}});
            if (!response.ok) throw new Error('save failed');
            status.textContent = 'Saved.';
          }} catch (_error) {{ status.textContent = 'Could not autosave. Use “Save and mark reviewed.”'; }}
        }};
        form.addEventListener('input', () => {{ clearTimeout(timer); timer = setTimeout(save, 900); }});
        form.addEventListener('change', () => {{ clearTimeout(timer); timer = setTimeout(save, 250); }});
      }})();
      (() => {{
        // A live echo of compute_totals in building_lead_pricing.py. Every step
        // below mirrors that function, including how it rounds, because a
        // preview that disagrees with the saved number is worse than no preview.
        const form = document.querySelector('[data-price-form]');
        if (!form) return;
        const status = form.querySelector('[data-price-status]');
        const save = form.querySelector('[data-price-save]');
        const contract = document.querySelector('.lead-price__contract button');
        const settled = status ? status.textContent : '';
        const cell = key => document.querySelectorAll(`[data-total="${{key}}"]`);
        // Python rounds half to even; Math.round does not. On "1.005" that is a
        // one cent disagreement, which is exactly the kind of thing nobody
        // believes until it reaches an invoice.
        const halfEven = value => {{
          const low = Math.floor(value), rest = value - low;
          if (Math.abs(rest - 0.5) > 1e-9) return Math.round(value);
          return low % 2 === 0 ? low : low + 1;
        }};
        const cents = name => {{
          const raw = String((form.elements[name] || {{}}).value ?? '')
            .trim().replace(/,/g, '').replace(/\\$/g, '');
          if (!raw) return 0;
          const amount = Number(raw);
          if (!Number.isFinite(amount)) return null;
          return halfEven(amount * 100);
        }};
        const money = value => (value / 100).toLocaleString('en-US', {{
          minimumFractionDigits: 2, maximumFractionDigits: 2,
        }});
        const recompute = () => {{
          const rate = cents('hourly_rate');
          const cleaning = cents('cleaning_fee');
          const security = cents('security_deposit');
          const discountRaw = cents('discount');
          const hoursText = String((form.elements['hours'] || {{}}).value ?? '').trim();
          const hours = hoursText === '' ? 0 : Number(hoursText);
          const percentText = String((form.elements['deposit_percent'] || {{}}).value ?? '').trim();
          const percent = percentText === '' ? 0 : Number(percentText);
          let addons = 0, broken = false;
          for (const field of form.querySelectorAll('[name^="addon_amount_"]')) {{
            const value = cents(field.name);
            if (value === null) {{ broken = true; break; }}
            addons += Math.max(0, value);
          }}
          // Say which field is wrong. Blaming "the amounts" when the deposit
          // percent is out of range sends someone hunting the wrong box.
          let wrong = '';
          if (broken || rate === null || cleaning === null
              || security === null || discountRaw === null) {{
            wrong = 'Amounts need to be numbers.';
          }} else if (!Number.isInteger(hours) || hours < 0) {{
            wrong = 'Hours needs to be a whole number.';
          }} else if (!Number.isFinite(percent) || percent < 0 || percent > 100) {{
            wrong = 'Deposit % needs to be between 0 and 100.';
          }}
          if (wrong) {{
            // Leaving save enabled here would send the operator into a server
            // refusal, and the redirect takes every unsaved edit with it.
            if (save) save.disabled = true;
            if (contract) contract.disabled = true;
            if (status) status.textContent = wrong;
            return;
          }}
          const venue = Math.max(0, hours) * Math.max(0, rate);
          const subtotal = venue + Math.max(0, cleaning) + addons;
          const discount = Math.min(Math.max(0, discountRaw), subtotal);
          const taxable = subtotal - discount;
          const taxRate = {int(pricing.get("tax_rate_bps") or 0)};
          const tax = Math.floor((taxable * taxRate + 5000) / 10000);
          const total = taxable + tax;
          const bps = halfEven(percent * 100);
          const deposit = Math.min(Math.floor((total * Math.max(0, bps) + 5000) / 10000), total);
          const held = Math.max(0, security);
          const values = {{
            venue, cleaning: Math.max(0, cleaning), addons, subtotal, discount, taxable, tax,
            total, deposit, security: held,
            due: deposit + held, balance: total - deposit,
          }};
          for (const [key, value] of Object.entries(values)) {{
            for (const node of cell(key)) node.textContent = money(value);
          }}
          // Saving a discount with no reason is refused by the server, and the
          // redirect would take every unsaved edit on this form with it.
          const reason = String((form.elements['discount_reason'] || {{}}).value ?? '').trim();
          const blocked = discount > 0 && !reason;
          if (save) save.disabled = blocked;
          if (contract) contract.disabled = blocked;
          if (status) status.textContent = blocked
            ? 'Add a reason for the discount before saving.'
            : settled;
        }};
        form.addEventListener('input', recompute);
        recompute();
      }})();
      </script>
    """
    styles = """
    <style>
      .building-inquiry-workspace .app-page{display:grid;gap:20px;padding-block:28px 64px}.building-inquiry-workspace .app-eyebrow{margin:0;color:var(--agent-ink-muted);font:700 .75rem/1.3 "Montserrat",sans-serif;letter-spacing:.06em;text-transform:uppercase}
      .lead-header{align-items:flex-start;margin:0}.lead-back{display:inline-flex;margin-bottom:18px;color:var(--agent-blue-strong);font-weight:700}.lead-header__states{display:flex;flex-wrap:wrap;gap:8px}
      .lead-next{display:grid;grid-template-columns:minmax(0,1fr) minmax(280px,.7fr);gap:24px;align-items:start;padding:24px 26px;border:1px solid rgba(94,159,196,.35);border-radius:var(--agent-radius-panel);background:linear-gradient(135deg,#fff,#f1f8fb)}.lead-next h2{margin:3px 0 7px}.lead-next p{margin:0;color:var(--agent-ink-muted)}
      .lead-next__contact{display:flex;flex-wrap:wrap;gap:6px 14px;margin-top:14px;font-weight:700}.lead-next__contact a,.lead-next__contact span{overflow-wrap:anywhere}
      .lead-response{display:grid;gap:10px}.lead-response label{display:grid;gap:5px;font-weight:700}.lead-response textarea{min-height:86px}.lead-button{display:inline-flex;align-items:center;justify-content:center;min-height:42px;padding:9px 14px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);font:700 .8rem/1.2 "Montserrat",sans-serif;text-decoration:none;cursor:pointer}.lead-button--primary{border-color:var(--agent-blue-strong);background:var(--agent-blue-strong);color:#fff}.lead-button:disabled{opacity:.45;cursor:not-allowed}.lead-button--danger{border-color:#a3372f;background:#a3372f;color:#fff}
      .lead-layout{display:grid;grid-template-columns:minmax(0,1.4fr) minmax(300px,.7fr);gap:20px;align-items:start}.lead-stack{display:grid;gap:20px}.lead-panel{min-width:0;overflow:hidden;border:1px solid var(--agent-border);border-radius:var(--agent-radius-panel);background:var(--agent-surface)}.lead-panel__head{display:flex;justify-content:space-between;gap:16px;align-items:start;padding:18px 20px}.lead-panel__head h2{margin:0}.lead-panel__head p{margin:5px 0 0;color:var(--agent-ink-muted)}
      .lead-details,.lead-contact dl{display:grid;grid-template-columns:minmax(130px,.45fr) minmax(0,1fr);margin:0}.lead-details dt,.lead-details dd,.lead-contact dt,.lead-contact dd{margin:0;padding:11px 18px;border-top:1px solid var(--agent-border)}.lead-details dt,.lead-contact dt{color:var(--agent-ink-muted);font-size:12px;font-weight:800}.lead-details dd,.lead-contact dd{overflow-wrap:anywhere}
      .lead-follow-up,.lead-activity{margin:0;padding:0;list-style:none}.lead-follow-up li,.lead-activity li{display:grid;gap:3px;padding:13px 18px;border-top:1px solid var(--agent-border)}.lead-follow-up span,.lead-activity span,.lead-activity time{color:var(--agent-ink-muted);font-size:12px}.lead-activity li{grid-template-columns:150px 1fr}.lead-activity div{display:grid;gap:3px}.lead-technical{padding:14px 18px;border:1px dashed var(--agent-border);border-radius:var(--agent-radius-control)}
      .lead-delivery-actions{display:flex;flex-wrap:wrap;gap:8px;padding:14px 18px;border-top:1px solid var(--agent-border)}
      .lead-interview{border-top:1px solid var(--agent-border);scroll-margin-top:140px}.lead-interview>summary{padding:14px 20px;color:var(--agent-blue-strong);font-weight:800;cursor:pointer}.lead-interview form{display:grid;gap:16px;padding:0 20px 20px}.lead-interview__section{display:grid;gap:10px;margin:0;padding:16px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control)}.lead-interview__section legend{padding:0 6px;font-weight:800}.lead-price{display:grid;gap:14px;padding:0 20px 18px}.lead-price__grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}.lead-price label{display:grid;gap:5px;font-weight:700;font-size:13px}.lead-price input{min-height:38px;padding:7px 9px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);font:14px/1.4 Inter,sans-serif}.lead-price__addons{display:grid;gap:8px}.lead-price__addons>span{font-weight:700;font-size:13px}.lead-price__addon{display:grid;grid-template-columns:minmax(0,2fr) minmax(0,1fr);gap:8px}.lead-price__total{font:800 1.1rem/1 Montserrat,sans-serif}.lead-price__contract{display:flex;flex-wrap:wrap;align-items:center;gap:10px;padding:14px 0 0;border-top:1px solid var(--agent-border)}.lead-price__contract span{color:var(--agent-ink-muted);font-size:12px}.lead-price__note{display:block;font-weight:400;color:var(--agent-ink-muted);font-size:11px}.lead-price__standard{margin:0;padding:0 20px 18px;color:var(--agent-ink-muted);font-size:12px}.lead-pricing .is-num{text-align:right;font-variant-numeric:tabular-nums}.lead-pricing .is-total td{font-weight:800;border-top:2px solid var(--agent-border)}.lead-pricing{width:100%;border-collapse:collapse}.lead-pricing th,.lead-pricing td{padding:10px 12px;border-bottom:1px solid var(--agent-border);text-align:left}.lead-pricing th{color:var(--agent-ink-muted);font-size:12px;text-transform:uppercase;letter-spacing:.04em}.lead-pricing-wrap{padding:0 20px 18px}.lead-pricing-wrap p{margin:12px 0 0;color:var(--agent-ink-muted);font-size:13px}.lead-interview__more{margin:4px 0}.lead-interview__more>summary{padding:10px 4px;color:var(--agent-blue-strong);font-weight:800;cursor:pointer}.lead-interview__more>fieldset{margin-top:12px}.lead-interview__section>p{margin:0;color:var(--agent-ink-muted);font-size:13px}.lead-interview__section>span{color:var(--agent-blue-strong);font-size:12px;font-weight:800}.lead-interview__grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.lead-interview label{display:grid;gap:5px;font-weight:700}.lead-interview label.is-missing{order:-1}.lead-interview textarea{min-height:80px}.lead-call-guide{display:grid;gap:4px;margin:0 20px 16px;padding:14px;border-radius:var(--agent-radius-control);background:#f1f8fb}.lead-call-guide span{color:var(--agent-ink-muted);font-size:13px;line-height:1.5}.lead-interview__save{display:flex;flex-wrap:wrap;align-items:center;gap:10px}.lead-interview__save span{color:var(--agent-ink-muted);font-size:12px}
      .lead-availability--hold{border-top:1px solid var(--agent-border);padding-top:16px;margin-top:4px}.lead-availability--hold h3{margin:0;font:800 .95rem/1.2 Montserrat,sans-serif}.lead-availability{display:grid;gap:14px;padding:0 20px 18px}.lead-availability__dates,.lead-availability__times{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}.lead-availability__dates{grid-template-columns:repeat(3,minmax(0,1fr))}.lead-availability label{display:grid;gap:5px;font-weight:700}.lead-availability__results{padding:0 20px 20px}.lead-availability__results>p{color:var(--agent-ink-muted)}.lead-availability__results ul{display:grid;gap:8px;margin:0;padding:0;list-style:none}.lead-availability__results li{display:grid;grid-template-columns:130px auto minmax(0,1fr);gap:10px;align-items:center;padding:10px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control)}.lead-availability__results small{color:var(--agent-ink-muted)}
      .lead-cal{display:grid;gap:14px;padding:0 20px 4px}.lead-cal__bar{display:flex;align-items:center;justify-content:space-between;gap:12px}.lead-cal__bar strong{font:800 1rem/1 Montserrat,sans-serif}.lead-cal__nav{display:inline-flex;align-items:center;justify-content:center;min-width:38px;min-height:38px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);color:var(--agent-blue-strong);font-weight:800;text-decoration:none}.lead-cal__nav:hover{background:#f1f8fb}
      .lead-cal__grid{width:100%;border-collapse:collapse;table-layout:fixed}.lead-cal__grid th{padding:6px 0;color:var(--agent-ink-muted);font-size:11px;text-transform:uppercase;letter-spacing:.04em}.lead-cal__grid abbr{text-decoration:none;border:0}.lead-cal__day{padding:2px;vertical-align:top}.lead-cal__day>a,.lead-cal__day>span{display:grid;gap:2px;align-content:start;min-height:58px;padding:6px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);text-decoration:none;color:inherit}.lead-cal__day>a:hover{border-color:var(--agent-blue-strong);background:#f1f8fb}.lead-cal__num{font-weight:800;font-size:13px}.lead-cal__note{color:var(--agent-ink-muted);font-size:10px;line-height:1.25}
      .lead-cal__day--pending>a,.lead-cal__day--pending>span{background:#fff6e5;border-color:#e6c384}.lead-cal__day--booked>a,.lead-cal__day--booked>span{background:#eceff3;border-color:#c3cbd6;color:var(--agent-ink-muted)}.lead-cal__day--external>a,.lead-cal__day--external>span{background:#f4f1fa;border-color:#cec3e6;color:var(--agent-ink-muted)}.lead-cal__day--headsup>a,.lead-cal__day--headsup>span{background:#fffdf2;border-color:#ded7a8}.lead-cal__day.is-outside>a,.lead-cal__day.is-outside>span{opacity:.45}.lead-cal__day.is-past>a,.lead-cal__day.is-past>span{opacity:.35}.lead-cal__day.is-today .lead-cal__num{color:var(--agent-blue-strong)}.lead-cal__day.is-selected>a{border:2px solid var(--agent-blue-strong);background:#e8f4fa}
      .lead-cal__legend{display:flex;flex-wrap:wrap;gap:8px}.lead-cal__key{display:inline-flex;align-items:center;gap:6px;padding:3px 9px;border:1px solid var(--agent-border);border-radius:999px;font-size:11px;font-weight:700;color:var(--agent-ink-muted)}.lead-cal__key--pending{background:#fff6e5;border-color:#e6c384}.lead-cal__key--booked{background:#eceff3;border-color:#c3cbd6}.lead-cal__key--external{background:#f4f1fa;border-color:#cec3e6}.lead-cal__key--headsup{background:#fffdf2;border-color:#ded7a8}.lead-cal__key--requested{border-left:4px solid var(--agent-blue-strong)}.lead-cal__day.is-requested>a,.lead-cal__day.is-requested>span{border-left:4px solid var(--agent-blue-strong)}
      .lead-cal__warning{margin:0;padding:10px 12px;border:1px solid #e6c384;border-radius:var(--agent-radius-control);background:#fff6e5;font-size:12px}.lead-cal__prompt{margin:0;padding:0 20px 18px;color:var(--agent-ink-muted);font-size:13px}.lead-cal__clash{margin:0;padding:10px 12px;border:1px solid #d9a49e;border-radius:var(--agent-radius-control);background:#fdf1ef;font-size:13px}.lead-cal__confirm{display:grid;gap:12px;margin:0 20px 18px;padding:16px;border:1px solid var(--agent-blue-strong);border-radius:var(--agent-radius-control);background:#f1f8fb}.lead-cal__confirm h3{margin:0;font:800 .95rem/1.3 Montserrat,sans-serif}.lead-cal__confirm p{margin:0;font-size:13px;line-height:1.5}.lead-cal__summary{margin:0;color:var(--agent-ink-muted);font-size:13px}.lead-availability__times select{min-height:38px;padding:7px 9px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);font:14px/1.4 Inter,sans-serif}
      .lead-journey-content,.lead-journey-empty,.lead-primary-form{display:grid;gap:12px;padding:16px 20px;border-top:1px solid var(--agent-border)}.lead-journey-content h3,.lead-journey-empty strong{margin:0}.lead-journey-empty span,.lead-action-note,.lead-primary-form>span{color:var(--agent-ink-muted);font-size:12px}.lead-journey-ok{margin:0;color:#11665f;font-weight:700}.lead-journey-list{margin:0;padding-left:20px}.lead-table{overflow-x:auto;border-top:1px solid var(--agent-border)}.lead-table table{width:100%;border-collapse:collapse}.lead-table th,.lead-table td{padding:10px 12px;border-bottom:1px solid var(--agent-border);text-align:left;vertical-align:top}.lead-table th{color:var(--agent-ink-muted);font-size:11px;text-transform:uppercase;letter-spacing:.04em}.lead-table small{display:block;margin-top:3px;color:var(--agent-ink-muted)}.lead-table form{display:flex;flex-wrap:wrap;gap:8px}.lead-inline-action{padding:0}.lead-inline-action>summary{cursor:pointer;color:var(--agent-blue-strong);font-weight:800}.lead-inline-action form{display:grid;gap:10px;margin-top:10px;padding:14px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);background:var(--agent-surface-soft)}.lead-inline-action label,.lead-primary-form label{display:grid;gap:5px;font-weight:700}.lead-inline-action textarea,.lead-primary-form textarea{min-height:76px}
      .lead-activity__more{display:block}.lead-activity__more>summary{padding:8px 0;color:var(--agent-blue-strong);font-weight:800;cursor:pointer}.lead-activity__list{margin:0;padding:0;list-style:none}
      @media(max-width:900px){.lead-next,.lead-layout{grid-template-columns:1fr}.lead-next .lead-button{justify-self:start}}
      @media(max-width:600px){.lead-details,.lead-contact dl,.lead-interview__grid,.lead-availability__dates,.lead-availability__times,.lead-availability__results li{grid-template-columns:1fr}.lead-details dt,.lead-contact dt{padding-bottom:0}.lead-details dd,.lead-contact dd{padding-top:4px}.lead-activity li{grid-template-columns:1fr}.lead-panel__head{display:grid}.lead-next{padding:20px}.lead-response{width:100%}}
    </style>
    """
    return render_operator_document(
        title=f"{data.get('name') or 'Inquiry'} | Anata Agent",
        navigation=navigation,
        body=body,
        page_class="building-inquiry-workspace",
        extra_head=styles,
    )
