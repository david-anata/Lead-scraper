"""LLM-backed follow-up email draft for a HubSpot deal.

Same pattern as services/fulfillment_deck/llm.py: lazy anthropic import,
ANTHROPIC_API_KEY from env, JSON-only system prompt, deterministic fallback.
Override model via SALES_FOLLOWUP_MODEL env var.

Three Anata hooks referenced in drafts:
  deck       → Full Amazon Marketing Analysis
  rate_sheet → Fulfillment Rate Sheet
  ads_audit  → Amazon Advertising Audit

The draft leads with whichever hook has been generated for this deal; if none
have been sent, it pitches the most relevant one first.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.parse
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "claude-haiku-4-5-20251001"

HOOK_LABELS: dict[str, str] = {
    "deck": "Full Amazon Marketing Analysis",
    "rate_sheet": "Fulfillment Rate Sheet",
    "ads_audit": "Amazon Advertising Audit",
}
_HOOK_ORDER = ("deck", "rate_sheet", "ads_audit")

_SYSTEM = (
    "You are a concise, confident sales rep for Anata — a fulfillment and "
    "Amazon growth agency. Write a short follow-up email to a prospect. "
    "Return ONLY a JSON object with exactly two keys: "
    "\"subject\" (string) and \"body\" (string, plain text, no markdown, "
    "max 110 words). The email must: "
    "1) Be addressed to the prospect's company (or contact first name if provided). "
    "2) If hooks were already sent: follow up on them, offer to walk through on a call. "
    "3) If hooks were NOT yet sent: pitch the lead hook (first in the not-yet-sent list) "
    "   as a concrete deliverable ready for them. "
    "4) Close with a simple ask: 20-minute call this week. "
    "5) Sound like a human — no 'I hope this email finds you well'. "
    "6) Sign off with the rep's first name only. "
    "Never invent numbers. Never use markdown in the body. "
    "No JSON outside the response object."
)


@dataclass
class DraftEmail:
    subject: str
    body: str
    hooks_sent: list[str] = field(default_factory=list)
    hooks_pending: list[str] = field(default_factory=list)
    model: str = "template"
    contact_emails: list[str] = field(default_factory=list)


def _parse_json(text: str) -> Optional[dict]:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                return None
    return None


def _fallback_draft(
    *,
    company_name: str,
    contact_first_name: str,
    owner_first_name: str,
    hooks_sent: list[str],
    hooks_pending: list[str],
) -> str:
    """Template body — never blank, never raises."""
    greeting = f"Hi {contact_first_name}," if contact_first_name else "Hi there,"
    sign = owner_first_name or "The Anata Team"
    company = company_name or "your brand"

    if hooks_sent and not hooks_pending:
        sent_labels = " and ".join(HOOK_LABELS.get(h, h) for h in hooks_sent[:2])
        return (
            f"{greeting}\n\n"
            f"Wanted to follow up on the {sent_labels} we put together for {company}. "
            "Happy to walk through the key findings on a quick call — usually 20 minutes.\n\n"
            f"Would this week work?\n\n{sign}"
        )

    lead = hooks_pending[0] if hooks_pending else "deck"
    lead_label = HOOK_LABELS.get(lead, lead)
    return (
        f"{greeting}\n\n"
        f"We put together a {lead_label} for {company} and I wanted to share it with you. "
        "It's specific to your situation — a 20-minute call is all it takes to walk through what we found "
        "and where we can move the needle.\n\n"
        f"When's a good time this week?\n\n{sign}"
    )


def _fallback_subject(
    *,
    company_name: str,
    hooks_sent: list[str],
    hooks_pending: list[str],
) -> str:
    company = company_name or "your brand"
    if hooks_pending:
        return f"{HOOK_LABELS.get(hooks_pending[0], 'Analysis')} for {company}"
    if hooks_sent:
        return f"Following up — {company}"
    return f"Quick follow-up — {company}"


def build_followup_draft(
    *,
    company_name: str,
    contact_first_name: str,
    owner_email: str,
    deal_name: str,
    deal_amount_cents: int,
    hooks_sent: list[str],
    hooks_pending: list[str],
    recent_subject: str = "",
    contact_emails: list[str] | None = None,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
) -> DraftEmail:
    """Generate a follow-up draft. Falls back to a template if no API key."""
    owner_first = (owner_email or "").split("@")[0].split(".")[0].capitalize()
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    resolved_model = model or os.environ.get("SALES_FOLLOWUP_MODEL") or _DEFAULT_MODEL

    subject = _fallback_subject(
        company_name=company_name, hooks_sent=hooks_sent, hooks_pending=hooks_pending
    )
    body = _fallback_draft(
        company_name=company_name,
        contact_first_name=contact_first_name,
        owner_first_name=owner_first,
        hooks_sent=hooks_sent,
        hooks_pending=hooks_pending,
    )

    if key:
        ctx = {
            "company": company_name or deal_name,
            "contact_first_name": contact_first_name or None,
            "rep_first_name": owner_first,
            "deal_amount_usd": round(deal_amount_cents / 100, 2) if deal_amount_cents else None,
            "hooks_already_sent": [HOOK_LABELS.get(h, h) for h in hooks_sent],
            "hooks_not_yet_sent": [HOOK_LABELS.get(h, h) for h in hooks_pending],
            "last_email_subject": recent_subject or None,
        }
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=key)
            msg = client.messages.create(
                model=resolved_model,
                max_tokens=600,
                system=_SYSTEM,
                messages=[{"role": "user", "content": json.dumps(ctx)}],
            )
            text = (msg.content[0].text if msg.content else "").strip()
            data = _parse_json(text)
            if data and data.get("subject") and data.get("body"):
                return DraftEmail(
                    subject=str(data["subject"]),
                    body=str(data["body"]),
                    hooks_sent=hooks_sent,
                    hooks_pending=hooks_pending,
                    model=getattr(msg, "model", resolved_model),
                    contact_emails=list(contact_emails or []),
                )
            logger.warning("[sales_followup] LLM returned unparseable output; using template")
        except Exception:  # noqa: BLE001
            logger.warning("[sales_followup] LLM call failed; using template", exc_info=True)

    return DraftEmail(
        subject=subject,
        body=body,
        hooks_sent=hooks_sent,
        hooks_pending=hooks_pending,
        contact_emails=list(contact_emails or []),
    )


def render_draft_followup_page(
    draft: DraftEmail,
    *,
    deal_id: str,
    deal_name: str,
    user: dict | None = None,
) -> str:
    import html as _html

    def _esc(v: object) -> str:
        return _html.escape(str(v or ""))

    from sales_support_agent.services.admin_nav import (
        render_agent_favicon_links,
        render_agent_nav,
        render_agent_nav_styles,
    )

    nav_styles = render_agent_nav_styles()
    nav = render_agent_nav("sales", sales_section="sales_deals", user=user)

    to_emails = ", ".join(draft.contact_emails) if draft.contact_emails else ""
    mailto = (
        f"mailto:{_esc(to_emails)}"
        f"?subject={urllib.parse.quote(draft.subject, safe='')}"
        f"&body={urllib.parse.quote(draft.body, safe='')}"
    )

    hook_tags = ""
    for h in draft.hooks_pending:
        hook_tags += f'<span class="hook hook--pending">{_esc(HOOK_LABELS.get(h, h))}</span>'
    for h in draft.hooks_sent:
        hook_tags += f'<span class="hook hook--sent">{_esc(HOOK_LABELS.get(h, h))} ✓</span>'

    body_escaped = _esc(draft.body)
    subject_escaped = _esc(draft.subject)

    styles = f"""
  :root {{--dark-blue:#2B3644;--light-blue:#85BBDA;--light-brown:#F9F7F3;
    --white:#FFF;--border:rgba(43,54,68,0.12);--shadow:rgba(43,54,68,0.10);}}
  *{{box-sizing:border-box;}}
  body{{margin:0;background:var(--light-brown);color:var(--dark-blue);
    font-family:"Inter","Segoe UI",sans-serif;}}
  a{{color:var(--dark-blue);}}
  {nav_styles}
  .shell{{max-width:820px;margin:0 auto;padding:24px 18px 64px;}}
  .crumbs{{font-size:12.5px;margin:0 0 12px;}}
  .crumbs a{{color:rgba(43,54,68,0.6);text-decoration:none;}}
  .workspace{{background:var(--white);border:1px solid var(--border);
    border-radius:20px;box-shadow:0 18px 40px var(--shadow);padding:24px 26px 28px;margin-bottom:18px;}}
  h1{{font-family:"Montserrat",sans-serif;font-weight:800;font-size:22px;margin:0 0 4px;}}
  .eyebrow{{font-family:"Montserrat",sans-serif;font-weight:700;font-size:11px;
    letter-spacing:0.08em;text-transform:uppercase;color:rgba(43,54,68,0.55);margin:0 0 4px;}}
  h2{{font-family:"Montserrat",sans-serif;font-weight:800;font-size:14px;margin:0 0 10px;}}
  .field-label{{font-size:11px;text-transform:uppercase;letter-spacing:0.05em;
    color:rgba(43,54,68,0.55);margin:0 0 5px;font-family:"Montserrat",sans-serif;font-weight:700;}}
  .field-val{{background:var(--light-brown);border:1px solid var(--border);border-radius:10px;
    padding:10px 14px;font-size:14px;margin:0 0 16px;}}
  input.field-val{{width:100%;display:block;outline:none;font-family:inherit;color:inherit;}}
  textarea.draft{{width:100%;background:var(--light-brown);border:1px solid var(--border);
    border-radius:10px;padding:12px 14px;font-size:14px;font-family:inherit;
    line-height:1.6;resize:vertical;min-height:180px;}}
  .actions{{display:flex;gap:10px;flex-wrap:wrap;margin-top:16px;}}
  .btn{{font:inherit;font-weight:600;font-size:13.5px;border-radius:12px;
    padding:10px 18px;cursor:pointer;text-decoration:none;display:inline-block;border:none;}}
  .btn--primary{{background:var(--dark-blue);color:#fff;}}
  .btn--outline{{background:var(--white);border:1px solid var(--border);color:var(--dark-blue);}}
  .btn--outline:hover{{border-color:rgba(43,54,68,0.28);}}
  .hooks{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px;}}
  .hook{{font-size:11.5px;border-radius:8px;padding:3px 10px;font-weight:600;}}
  .hook--pending{{background:#fff4d9;border:1px solid #d2a94b;color:#7a5a12;}}
  .hook--sent{{background:rgba(47,143,91,0.12);border:1px solid #2f8f5b;color:#2f8f5b;}}
  .note{{font-size:12.5px;color:rgba(43,54,68,0.6);margin-top:12px;}}
"""

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>agent | Draft Follow-Up — {subject_escaped}</title>
    {render_agent_favicon_links()}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>{styles}</style>
  </head>
  <body>
    {nav}
    <main class="shell">
      <div class="crumbs">
        <a href="/admin/sales/deals">← Deal Board</a>
        &nbsp;/&nbsp;
        <a href="/admin/sales/deals/{_esc(deal_id)}">{_esc(deal_name)}</a>
      </div>

      <div class="workspace">
        <p class="eyebrow">Sales Priorities — Follow-up draft</p>
        <h1>Draft email for <span style="color:var(--light-blue)">{_esc(deal_name)}</span></h1>
        <p style="font-size:13.5px;color:rgba(43,54,68,0.7);margin:4px 0 16px">
          AI-generated from deal context. Edit before sending.
          {"Generated by " + _esc(draft.model) + "." if draft.model != "template" else "Built from template — add ANTHROPIC_API_KEY for personalized drafts."}
        </p>

        {"<div class='field-label'>Closing tools</div><div class='hooks'>" + hook_tags + "</div>" if hook_tags else ""}

        <div class="field-label">Subject</div>
        <input type="text" class="field-val" id="subj" value="{subject_escaped}">

        {"<div class='field-label'>To</div><div class='field-val'>" + _esc(to_emails) + "</div>" if to_emails else ""}

        <div class="field-label">Body</div>
        <textarea class="draft" id="body">{body_escaped}</textarea>

        <div class="actions">
          <button class="btn btn--primary" onclick="copyDraft()">Copy email</button>
          {"<a class='btn btn--outline' href='" + _esc(mailto) + "'>Open in email →</a>" if to_emails else ""}
          <a class="btn btn--outline" href="/admin/sales/deals/{_esc(deal_id)}/draft-followup">Regenerate →</a>
          <a class="btn btn--outline" href="/admin/sales/deals/{_esc(deal_id)}">← Back to deal</a>
        </div>
        {"" if to_emails else f"<p class='note'>No contacts on this deal — <a href='/admin/sales/deals/{_esc(deal_id)}'>go back to the deal</a> and add a contact in HubSpot to enable the To field and mail app link.</p>"}
        <p class="note">
          To log this email to HubSpot, send it then use "Log a call / email" in the HubSpot deal record.
          Full email send + auto-log is coming in a later phase.
        </p>
      </div>
    </main>

    <script>
    function copyDraft() {{
      const subj = document.getElementById('subj').value;
      const body = document.getElementById('body').value;
      const full = "Subject: " + subj + "\\n\\n" + body;
      navigator.clipboard.writeText(full).then(() => {{
        const btn = event.target;
        btn.textContent = 'Copied!';
        setTimeout(() => btn.textContent = 'Copy email', 2000);
      }}).catch(() => {{
        alert('Copy failed — please select the text manually.');
      }});
    }}
    </script>
  </body>
</html>"""
