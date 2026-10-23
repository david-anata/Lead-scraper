"""After the call: ClickUp Sales CRM, recap draft, day-10 trial review.

Driven by the Call Script page: when a live call ends, the page runs its
existing transcript review and posts the resulting analysis here. Each step
records its own result on the live session, so re-posting never duplicates a
ClickUp task or email, and a failed step is retried on the next post.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from sales_support_agent.models.entities import SalesCallLiveSession, SalesCallTracker

logger = logging.getLogger(__name__)

DEFAULT_LIST_ID = "900600702146"  # anata Sales space > Sales CRM
FIELD_COMPANY = "0bb668f4-a49a-4831-947f-b1f5d80a437c"
FIELD_EMAIL = "12e960ef-df48-4ce9-b0bd-c77f453590d4"
FIELD_NAME = "c566990e-bd2b-4039-8b17-f7093176aa12"
FIELD_VALUE = "8331d9d0-4f32-4d31-84dd-a901a3e3b8ec"
FIELD_SERVICES = "ba817770-5e13-4e8e-a6bf-dda6bb39d81f"
FIELD_MEETING_SET = "ed0d5bf2-3b9e-4ee0-a5db-c4e95a715ef4"

OFFER_VALUE = {"starter": 99, "growth": 199, "pro": 399, "full": 3000}
OFFER_LABEL = {
    "starter": "Anata Intelligence Starter $99/mo",
    "growth": "Anata Intelligence Growth $199/mo",
    "pro": "Anata Intelligence Pro $399/mo",
    "full": "Anata full service $3,000/mo + 5% over baseline",
}
TRIAL_LINE = "Start your 14-day trial: https://anatainc.com/platform/anata-intelligence"
_TASK_URL = re.compile(r"app\.clickup\.com/t/(?:\d+/)?([A-Za-z0-9_-]+)")
_DASHES = re.compile("[‒–—―]")


def list_id() -> str:
    return os.getenv("SALES_CALL_CLICKUP_LIST_ID", "").strip() or DEFAULT_LIST_ID


def recap_mode() -> str:
    mode = os.getenv("SALES_CALL_RECAP_MODE", "").strip().lower()
    return mode if mode in ("draft", "off") else "draft"


def agent_url() -> str:
    return (os.getenv("AGENT_PUBLIC_URL", "").strip() or "https://agent.anatainc.com").rstrip("/")


def status_for(outcome: str, pitched: bool) -> str:
    outcome = (outcome or "").lower()
    if outcome == "closed":
        return "won - onboarding"
    if outcome == "followup":
        return "follow up"
    if outcome == "lost":
        return "lost"
    return "working offered" if pitched else "working qualified"


def _txt(value: Any, limit: int = 4000) -> str:
    return _DASHES.sub("-", str(value or "")).strip()[:limit]


def _field_value(task: dict, field_id: str) -> Any:
    for f in task.get("custom_fields") or []:
        if f.get("id") == field_id:
            return f.get("value")
    return None


def task_id_from_link(link: str) -> str:
    m = _TASK_URL.search(link or "")
    return m.group(1) if m else ""


def find_task(client, tracker: SalesCallTracker, analysis: dict) -> Optional[dict]:
    tid = task_id_from_link(tracker.lead_link)
    if tid:
        try:
            return client.get_task(tid)
        except Exception as exc:  # stale link: fall back to search
            logger.info("lead link task %s not readable: %s", tid, exc)
    names = {
        n.strip().lower()
        for n in (tracker.company, tracker.prospect_name, analysis.get("prospect"))
        if n and str(n).strip()
    }
    if not names:
        return None
    for page in range(5):
        tasks = client.get_tasks(list_id(), include_closed=True, page=page)
        for t in tasks:
            company = str(_field_value(t, FIELD_COMPANY) or "").strip().lower()
            if str(t.get("name") or "").strip().lower() in names or (company and company in names):
                return t
        if len(tasks) < 100:
            break
    return None


def _comment(tracker: SalesCallTracker, analysis: dict, offer: str) -> str:
    notes = analysis.get("notes") if isinstance(analysis.get("notes"), dict) else {}
    beliefs = [b.get("b") for b in analysis.get("beliefs") or [] if isinstance(b, dict) and b.get("b")]
    objs = [o.get("k") for o in analysis.get("objs") or [] if isinstance(o, dict) and o.get("k")]
    steps = [
        f"- {s.get('step')} ({s.get('owner') or 'rep'}{', ' + s.get('when') if s.get('when') else ''})"
        for s in notes.get("nextSteps") or []
        if isinstance(s, dict) and s.get("step")
    ]
    lines = [
        "Live call summary (Anata Call Script)",
        f"Outcome: {analysis.get('outcome') or 'unknown'}. Offer: {OFFER_LABEL.get(offer, 'none quoted')}.",
        "",
        _txt(notes.get("summary"), 1500),
        "",
        "Beliefs said: " + (", ".join(beliefs) or "none"),
        "Pains: " + (", ".join(analysis.get("pains") or []) or "none"),
        "Objections: " + (", ".join(objs) or "none"),
    ]
    if steps:
        lines += ["", "Next steps:"] + steps
    lines += ["", f"Tracker: {agent_url()}/admin/sales/call/{tracker.id}"]
    return _txt("\n".join(lines), 9000)


def _clickup_step(client, tracker: SalesCallTracker, analysis: dict, offer: str) -> dict:
    task = find_task(client, tracker, analysis)
    pitched = str(analysis.get("pitch") or "none") != "none"
    status = status_for(str(analysis.get("outcome") or ""), pitched)
    created = False
    if task is None:
        name = tracker.company or tracker.prospect_name or str(analysis.get("prospect") or "New lead")
        task = client.create_task(list_id(), {"name": name, "status": status})
        created = True
    else:
        client.update_task(task["id"], {"status": status})
    task_id = task["id"]
    if tracker.company and not _field_value(task, FIELD_COMPANY):
        client.set_custom_field_value(task_id, FIELD_COMPANY, tracker.company)
    if tracker.prospect_name and not _field_value(task, FIELD_NAME):
        client.set_custom_field_value(task_id, FIELD_NAME, tracker.prospect_name)
    if offer in OFFER_VALUE:
        client.set_custom_field_value(task_id, FIELD_VALUE, OFFER_VALUE[offer])
        client.set_custom_field_value(task_id, FIELD_SERVICES, OFFER_LABEL[offer])
    client.set_custom_field_value(task_id, FIELD_MEETING_SET, True)
    client.create_task_comment(task_id, _comment(tracker, analysis, offer))
    return {
        "ok": True,
        "taskId": task_id,
        "created": created,
        "status": status,
        "url": task.get("url") or f"https://app.clickup.com/t/{task_id}",
        "email": str(_field_value(task, FIELD_EMAIL) or ""),
    }


def _day10_step(client, tracker: SalesCallTracker, analysis: dict, crm: dict, when: datetime) -> dict:
    if str(analysis.get("outcome") or "").lower() != "closed":
        return {"ok": True, "skipped": True, "reason": "No trial started on this call."}
    due = when + timedelta(days=10)
    name = tracker.prospect_name or tracker.company or str(analysis.get("prospect") or "prospect")
    desc = (
        "Day-10 review of the 14-day Anata Intelligence trial, before it ends.\n"
        f"CRM task: {crm.get('url', '')}\n"
        f"Tracker: {agent_url()}/admin/sales/call/{tracker.id}"
    )
    task = client.create_task(
        list_id(),
        {
            "name": f"Day-10 trial review: {name}",
            "description": desc,
            "due_date": int(due.timestamp() * 1000),
            "due_date_time": False,
        },
    )
    return {"ok": True, "taskId": task.get("id"), "due": due.date().isoformat()}


def _recap_step(gmail, analysis: dict, offer: str, to_email: str) -> dict:
    if recap_mode() == "off":
        return {"ok": True, "skipped": True, "reason": "Recap is turned off (SALES_CALL_RECAP_MODE=off)."}
    email = analysis.get("email") if isinstance(analysis.get("email"), dict) else {}
    subject = _txt(email.get("subject"), 200)
    body = _txt(email.get("body"), 6000)
    if not subject or not body:
        return {"ok": False, "error": "The review did not produce a recap email."}
    if offer in ("starter", "growth", "pro") and "anatainc.com/platform/anata-intelligence" not in body:
        body = body.rstrip() + "\n\n" + TRIAL_LINE
    if not to_email:
        return {"ok": True, "skipped": True, "reason": "No prospect email on the ClickUp task, so no draft was created."}
    result = gmail.create_draft(to=(to_email,), subject=subject, text=body)
    if isinstance(result, dict) and result.get("skipped"):
        return {"ok": True, "skipped": True, "reason": str(result.get("reason") or "skipped")}
    return {"ok": True, "mode": "draft", "draftId": (result or {}).get("id", ""), "to": to_email}


def run_after_call(
    session: Session,
    live: SalesCallLiveSession,
    tracker: SalesCallTracker,
    analysis: Any,
    offer: str,
    *,
    clickup_client,
    gmail_client,
    when: Optional[datetime] = None,
) -> dict:
    """Run every step that hasn't succeeded yet; return all step results."""
    if not isinstance(analysis, dict):
        raise ValueError("analysis must be an object.")
    offer = offer if offer in OFFER_VALUE else "none"
    when = when or datetime.now(timezone.utc)
    stored = dict(live.after_call_json or {})
    steps = dict(stored.get("steps") or {})

    def done(name: str) -> bool:
        return bool((steps.get(name) or {}).get("ok"))

    if not done("clickup"):
        try:
            steps["clickup"] = _clickup_step(clickup_client, tracker, analysis, offer)
        except Exception as exc:
            logger.warning("after-call clickup failed: %s", exc)
            steps["clickup"] = {"ok": False, "error": str(exc)[:300]}
    crm = steps.get("clickup") or {}

    if not done("day10"):
        if not crm.get("ok"):
            steps["day10"] = {"ok": False, "error": "Waiting on the ClickUp step."}
        else:
            try:
                steps["day10"] = _day10_step(clickup_client, tracker, analysis, crm, when)
            except Exception as exc:
                logger.warning("after-call day10 failed: %s", exc)
                steps["day10"] = {"ok": False, "error": str(exc)[:300]}

    if not done("recap"):
        try:
            steps["recap"] = _recap_step(gmail_client, analysis, offer, str(crm.get("email") or ""))
        except Exception as exc:
            logger.warning("after-call recap failed: %s", exc)
            steps["recap"] = {"ok": False, "error": str(exc)[:300]}

    stored.update({"steps": steps, "offer": offer, "outcome": str(analysis.get("outcome") or ""), "updatedAt": when.isoformat()})
    live.after_call_json = stored
    flag_modified(live, "after_call_json")
    session.flush()
    return steps
