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
    interview_section = (
        f'''<section class="lead-panel"><div class="lead-panel__head"><div><h2>Event interview</h2><p>{required_done} of {len(REQUIRED_INTERVIEW_FIELDS)} needed to qualify · {answered} of {len(INTERVIEW_FIELDS)} answered overall. Save partial progress at any time; this does not promise a date or price.</p></div></div>
            <details class="lead-interview" id="event-interview"{' open' if stage == 'responded' else ''}><summary>Review or update interview</summary><div class="lead-call-guide"><strong>Call guide</strong><span>Why this event? Which dates and full access window work? Who decides? What must the room support? What happens next, and when?</span></div><form data-interview-autosave method="post" action="/admin/building/inquiries/{_esc(data.get('id'))}/event-interview"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">{section_blocks}<div class="lead-interview__save"><button class="lead-button lead-button--primary" type="submit" name="save_mode" value="reviewed">Save and mark reviewed</button><span data-autosave-status role="status">Changes save automatically after you pause.</span></div></form></details>
          </section>'''
        if data.get("kind") == "event"
        else ""
    )
    candidate_values = list(dict.fromkeys(
        value for value in (
            data.get("preferred_date"),
            details.get("alternateDate") or details.get("alternate_date"),
            details.get("backupDate2") or details.get("backup_date_2"),
        ) if value
    ))[:3]
    candidate_inputs = "".join(
        f'<label>Choice {index}<input type="date" name="candidate_date" value="{_esc(value)}"></label>'
        for index, value in enumerate(candidate_values + [""] * (3 - len(candidate_values)), start=1)
    )
    availability_section = (
        f'''<section class="lead-panel" id="date-review"><div class="lead-panel__head"><div><h2>Date review</h2><p>Compare up to three dates against Agent holds and the Anata Events calendar. Unknown never means available.</p></div></div>
          <form class="lead-availability" data-availability-form data-endpoint="/admin/building/inquiries/{_esc(data.get('id'))}/availability">
            <div class="lead-availability__dates">{candidate_inputs}</div>
            <div class="lead-availability__times">
              <label>Setup begins<input type="time" name="setup_start_time" value="{_esc(details.get('accessStartTime') or '')}"></label>
              <label>Guests begin<input type="time" name="guest_start_time" value="{_esc(details.get('guestStartTime') or '')}"></label>
              <label>Guests end<input type="time" name="guest_end_time" value="{_esc(details.get('guestEndTime') or '')}"></label>
              <label>Teardown ends<input type="time" name="teardown_end_time" value="{_esc(details.get('accessEndTime') or '')}"></label>
            </div>
            <div class="lead-interview__save"><button class="lead-button lead-button--primary" type="submit">Check dates</button><span>Read-only check; this creates no hold.</span></div>
          </form><div class="lead-availability__results" data-availability-results aria-live="polite"></div>
        </section>'''
        if data.get("kind") == "event" and stage == "qualified" and not data.get("reservation_id")
        else ""
    )
    activity_rows = "".join(
        f"<li><time>{_esc(_when(item.get('created_at')))}</time><div><strong>{_esc(str(item.get('action') or 'updated').replace('_', ' ').title())}</strong><span>{_esc(item.get('actor') or 'System')}</span></div></li>"
        for item in data.get("activity", [])
    ) or "<li><div><strong>No activity recorded.</strong></div></li>"
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
    else:
        response_form = f'<a class="lead-button lead-button--primary" href="{_esc(next_action["href"])}">{_esc(next_action["label"])} →</a>'

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
      {messages}
      <section class="lead-next" aria-labelledby="lead-next-title">
        <div><p class="app-eyebrow">Do this next</p><h2 id="lead-next-title">{_esc(next_action['title'])}</h2><p>{_esc(next_action['body'])}</p><div class="lead-next__contact"><a href="mailto:{_esc(data.get('email'))}">{_esc(data.get('email'))}</a><span>{_esc(data.get('phone') or 'No phone provided')}</span></div></div>
        {response_form}
      </section>
      <section class="lead-layout">
        <div class="lead-stack">
          <section class="lead-panel"><div class="lead-panel__head"><div><h2>Original website submission</h2><p>The prospect's words, preserved under plain-language labels.</p></div></div><dl class="lead-details">{submitted_rows}</dl></section>
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
      (() => {{
        const form = document.querySelector('[data-availability-form]');
        const output = document.querySelector('[data-availability-results]');
        if (!form || !output || !window.fetch) return;
        const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[char]));
        form.addEventListener('submit', async event => {{
          event.preventDefault();
          const values = new FormData(form);
          const dates = values.getAll('candidate_date').map(value => String(value).trim()).filter(Boolean);
          if (!dates.length) {{ output.innerHTML = '<p class="app-alert app-alert--error">Add at least one candidate date.</p>'; return; }}
          const params = new URLSearchParams({{dates: dates.join(',')}});
          for (const key of ['setup_start_time','guest_start_time','guest_end_time','teardown_end_time']) params.set(key, String(values.get(key) || ''));
          output.innerHTML = '<p>Checking Agent and Anata Events…</p>';
          try {{
            const response = await fetch(`${{form.dataset.endpoint}}?${{params}}`, {{headers:{{'Accept':'application/json'}}}});
            const result = await response.json();
            if (!response.ok) throw new Error(result.detail || 'Date review failed.');
            const rows = result.dates.map(item => `<li><strong>${{esc(item.date)}}</strong><span class="app-status app-status--${{item.status === 'available' ? 'confirmed' : item.status === 'unknown' ? 'neutral' : 'blocked'}}">${{esc(item.status)}}</span><small>${{esc(item.message)}}</small></li>`).join('');
            const alternatives = result.nearby_alternatives.length ? `<p><strong>Nearby options:</strong> ${{result.nearby_alternatives.map(item => esc(item.date)).join(', ')}}</p>` : '';
            output.innerHTML = `<p>Checked ${{esc(result.checked_at)}} by ${{esc(result.checked_by)}}.</p><ul>${{rows}}</ul>${{alternatives}}<p><a class="lead-button lead-button--primary" href="/admin/building/bookings#review-event-date">Continue to governed hold</a></p>`;
          }} catch (error) {{ output.innerHTML = `<p class="app-alert app-alert--error">${{esc(error.message)}} No hold was created.</p>`; }}
        }});
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
      .lead-interview{border-top:1px solid var(--agent-border);scroll-margin-top:140px}.lead-interview>summary{padding:14px 20px;color:var(--agent-blue-strong);font-weight:800;cursor:pointer}.lead-interview form{display:grid;gap:16px;padding:0 20px 20px}.lead-interview__section{display:grid;gap:10px;margin:0;padding:16px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control)}.lead-interview__section legend{padding:0 6px;font-weight:800}.lead-interview__more{margin:4px 0}.lead-interview__more>summary{padding:10px 4px;color:var(--agent-blue-strong);font-weight:800;cursor:pointer}.lead-interview__more>fieldset{margin-top:12px}.lead-interview__section>p{margin:0;color:var(--agent-ink-muted);font-size:13px}.lead-interview__section>span{color:var(--agent-blue-strong);font-size:12px;font-weight:800}.lead-interview__grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.lead-interview label{display:grid;gap:5px;font-weight:700}.lead-interview label.is-missing{order:-1}.lead-interview textarea{min-height:80px}.lead-call-guide{display:grid;gap:4px;margin:0 20px 16px;padding:14px;border-radius:var(--agent-radius-control);background:#f1f8fb}.lead-call-guide span{color:var(--agent-ink-muted);font-size:13px;line-height:1.5}.lead-interview__save{display:flex;flex-wrap:wrap;align-items:center;gap:10px}.lead-interview__save span{color:var(--agent-ink-muted);font-size:12px}
      .lead-availability{display:grid;gap:14px;padding:0 20px 18px}.lead-availability__dates,.lead-availability__times{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}.lead-availability__dates{grid-template-columns:repeat(3,minmax(0,1fr))}.lead-availability label{display:grid;gap:5px;font-weight:700}.lead-availability__results{padding:0 20px 20px}.lead-availability__results>p{color:var(--agent-ink-muted)}.lead-availability__results ul{display:grid;gap:8px;margin:0;padding:0;list-style:none}.lead-availability__results li{display:grid;grid-template-columns:130px auto minmax(0,1fr);gap:10px;align-items:center;padding:10px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control)}.lead-availability__results small{color:var(--agent-ink-muted)}
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
