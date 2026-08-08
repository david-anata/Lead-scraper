"""Plain-language staff workspace for one Building inquiry."""

from __future__ import annotations

import html
from datetime import datetime
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from sales_support_agent.services.ui_shell import render_operator_document
from sales_support_agent.services.building_lead_intake import (
    event_qualification_missing,
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

    def select(name: str, chosen: str, label: str) -> str:
        items = "".join(
            '<option value="{value}"{sel}{dis}>{text}</option>'.format(
                value=_esc(option.get("value")),
                sel=" selected" if option.get("value") == chosen else "",
                dis=" disabled" if option.get("taken") else "",
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
    return f'''<form class="lead-availability lead-availability--hold" method="post" action="/admin/building/inquiries/{_esc(data.get('id'))}/hold-date">
            <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
            <input type="hidden" name="event_date" value="{_esc(selected)}">
            <h3>{_esc(data.get("selected_label") or selected)}</h3>
            <div class="lead-availability__times">
              {select("guest_start_time", guest_start, "Guests arrive")}
              {select("guest_end_time", guest_end, "Guests leave")}
              <label>Attendance<input type="number" name="attendance" min="1" value="{_esc(_attendance_guess(interview))}"></label>
            </div>{summary}
            {hold_control}
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
            "href": f"/admin/building/bookings?inquiry_id={quote(inquiry_id)}#review-event-date",
            "label": "Review date",
        }
    if data.get("reservation_id"):
        return {
            "title": "Continue the booking journey.",
            "body": "The customer request is linked to a booking. Continue from its single guided workspace.",
            "kind": "link",
            "href": f"/admin/building/bookings/{quote(str(data['reservation_id']))}",
            "label": "Open booking",
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
        f'{_money(totals.get("total_cents"))}</span></div>'
        f'<form class="lead-price" method="post" action="/admin/building/inquiries/{_esc(data.get("id"))}/pricing">'
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
        f'<tr><td>Venue</td><td class="is-num">{_money(totals.get("venue_cents"))}</td></tr>'
        f'<tr><td>Cleaning</td><td class="is-num">{_money(totals.get("cleaning_cents"))}</td></tr>'
        f'<tr><td>Add-ons</td><td class="is-num">{_money(totals.get("addons_cents"))}</td></tr>'
        f'<tr><td>Subtotal</td><td class="is-num">{_money(totals.get("subtotal_cents"))}</td></tr>'
        f'<tr><td>Discount</td><td class="is-num">-{_money(totals.get("discount_cents"))}</td></tr>'
        f'<tr class="is-total"><td>Contract total</td><td class="is-num">{_money(totals.get("total_cents"))}</td></tr>'
        f'<tr><td>Booking deposit</td><td class="is-num">{_money(totals.get("deposit_cents"))}</td></tr>'
        f'<tr><td>Security deposit <span class="lead-price__note">refundable, not part of the total</span></td>'
        f'<td class="is-num">{_money(totals.get("security_deposit_cents"))}</td></tr>'
        f'<tr class="is-total"><td>Due to book</td><td class="is-num">{_money(totals.get("due_to_book_cents"))}</td></tr>'
        f'<tr><td>Balance before the event</td><td class="is-num">{_money(totals.get("balance_cents"))}</td></tr>'
        "</tbody></table>"
        '<div class="lead-interview__save">'
        '<button class="lead-button lead-button--primary" type="submit">Save pricing</button>'
        "<span>Applies to this lead only. Nothing is sent.</span>"
        "</div></form>"
        # The contract is an output of the lead, not a separate place to go.
        f'<form class="lead-price__contract" method="post" '
        f'action="/admin/building/inquiries/{_esc(data.get("id"))}/contract">'
        f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
        '<button class="lead-button" type="submit">Create the contract</button>'
        "<span>Writes this pricing into the booking and prepares the contract. "
        "Nothing is sent.</span>"
        "</form>"
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
        )
        if data.get("kind") == "event" and not data.get("reservation_id")
        else ""
    )
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
          {pricing_section}
          {interview_section}
          {availability_section}
          <section class="lead-panel"><div class="lead-panel__head"><div><h2>Activity</h2><p>Audited intake, delivery, and lifecycle evidence.</p></div></div><ol class="lead-activity">{activity_rows}</ol></section>
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
      </script>
    """
    styles = """
    <style>
      .building-inquiry-workspace .app-page{display:grid;gap:20px;padding-block:28px 64px}.building-inquiry-workspace .app-eyebrow{margin:0;color:var(--agent-ink-muted);font:700 .75rem/1.3 "Montserrat",sans-serif;letter-spacing:.06em;text-transform:uppercase}
      .lead-header{align-items:flex-start;margin:0}.lead-back{display:inline-flex;margin-bottom:18px;color:var(--agent-blue-strong);font-weight:700}.lead-header__states{display:flex;flex-wrap:wrap;gap:8px}
      .lead-next{display:grid;grid-template-columns:minmax(0,1fr) minmax(280px,.7fr);gap:24px;align-items:start;padding:24px 26px;border:1px solid rgba(94,159,196,.35);border-radius:var(--agent-radius-panel);background:linear-gradient(135deg,#fff,#f1f8fb)}.lead-next h2{margin:3px 0 7px}.lead-next p{margin:0;color:var(--agent-ink-muted)}
      .lead-next__contact{display:flex;flex-wrap:wrap;gap:6px 14px;margin-top:14px;font-weight:700}.lead-next__contact a,.lead-next__contact span{overflow-wrap:anywhere}
      .lead-response{display:grid;gap:10px}.lead-response label{display:grid;gap:5px;font-weight:700}.lead-response textarea{min-height:86px}.lead-button{display:inline-flex;align-items:center;justify-content:center;min-height:42px;padding:9px 14px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);font:700 .8rem/1.2 "Montserrat",sans-serif;text-decoration:none;cursor:pointer}.lead-button--primary{border-color:var(--agent-blue-strong);background:var(--agent-blue-strong);color:#fff}
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
      .lead-cal__warning{margin:0;padding:10px 12px;border:1px solid #e6c384;border-radius:var(--agent-radius-control);background:#fff6e5;font-size:12px}.lead-cal__prompt{margin:0;padding:0 20px 18px;color:var(--agent-ink-muted);font-size:13px}.lead-cal__summary{margin:0;color:var(--agent-ink-muted);font-size:13px}.lead-availability__times select{min-height:38px;padding:7px 9px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);font:14px/1.4 Inter,sans-serif}
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
