"""Runs Abby every few minutes: find new replies in Instantly and act on them.

ABBY_MODE (env): "off" (default), "shadow" (decide and post drafts to Slack,
send nothing), "live" (send). ABBY_DAILY_CAP limits replies per day (60).
Only replies received after ABBY_START_AT (ISO time, env) are handled, so
threads from before Abby existed stay with Gabe.

Safety rails, all in code:
  * each received email is handled exactly once (abby_handled table);
  * a thread where a person already replied by hand is left alone;
  * opt-outs block the company domain (or just the address for gmail-type
    domains) and never get a reply;
  * hostile, legal, unsure or failed drafts are held and posted to Slack.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

import requests
from sqlalchemy import text

import abby

logger = logging.getLogger(__name__)

API = "https://api.instantly.ai/api/v2"
FREEMAIL = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com", "aol.com",
            "live.com", "msn.com", "me.com", "proton.me", "protonmail.com", "gmx.com"}
MAX_PER_RUN = 10

_DDL = (
    """CREATE TABLE IF NOT EXISTS abby_handled (
        email_id TEXT PRIMARY KEY, thread_id TEXT, lead TEXT, category TEXT,
        action TEXT, mode TEXT, reply_id TEXT, detail TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS abby_followups (
        id SERIAL PRIMARY KEY, lead TEXT, reply_to_id TEXT, eaccount TEXT, subject TEXT,
        first_name TEXT, due_at TIMESTAMP, status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
)


def mode() -> str:
    m = (os.getenv("ABBY_MODE") or "off").strip().lower()
    return m if m in {"off", "shadow", "live"} else "off"


def start_at() -> datetime:
    raw = (os.getenv("ABBY_START_AT") or "").strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc) - timedelta(hours=2)


def ensure_tables(engine) -> None:
    """Postgres tables are created by migration; Vercel's role cannot run DDL."""
    if os.getenv("VERCEL") and engine.dialect.name == "postgresql":
        return
    ddl = [d.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
           if engine.dialect.name == "sqlite" else d for d in _DDL]
    with engine.begin() as conn:
        for d in ddl:
            conn.execute(text(d))


class Instantly:
    """Thin client. Every call returns (status, json) and never raises."""

    def __init__(self, key: str, session: Any = None):
        self.key = key
        self.http = session or requests

    def _call(self, method: str, path: str, **kw) -> tuple[int, Any]:
        try:
            r = self.http.request(method, f"{API}/{path}", timeout=30,
                                  headers={"Authorization": f"Bearer {self.key}"}, **kw)
            return r.status_code, (r.json() if r.content else None)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[abby] instantly %s %s failed: %s", method, path,
                           str(exc).replace(self.key, "<key>"))
            return 0, None

    def received_since(self, since: datetime, campaign_id: str) -> list[dict]:
        st, data = self._call("GET", "emails", params={
            "email_type": "received", "campaign_id": campaign_id, "sort_order": "asc",
            "limit": 50, "min_timestamp_created": since.isoformat().replace("+00:00", "Z")})
        return (data or {}).get("items") or [] if st == 200 else []

    def thread(self, thread_id: str) -> list[dict]:
        st, data = self._call("GET", "emails", params={
            "search": f"thread:{thread_id}", "sort_order": "asc", "limit": 30})
        return (data or {}).get("items") or [] if st == 200 else []

    def reply(self, *, reply_to: str, eaccount: str, subject: str, body: str,
              cc: list[str], extra: list[str]) -> tuple[int, Any]:
        payload: dict[str, Any] = {"reply_to_uuid": reply_to, "eaccount": eaccount,
                                   "subject": subject,
                                   "body": {"text": body, "html": abby.to_html(body)}}
        if cc:
            payload["cc_address_email_list"] = ",".join(cc)
        if extra:
            payload["additional_recipients"] = extra
        return self._call("POST", "emails/reply", json=payload)

    def set_interest(self, lead: str, value: int, campaign_id: str) -> int:
        return self._call("POST", "leads/update-interest-status",
                          json={"lead_email": lead, "interest_value": value,
                                "campaign_id": campaign_id})[0]

    def block(self, value: str) -> int:
        return self._call("POST", "block-lists-entries", json={"bl_value": value})[0]


def _handled_ids(engine, ids: list[str]) -> set[str]:
    if not ids:
        return set()
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT email_id FROM abby_handled")).fetchall()
    have = {r[0] for r in rows}
    return {i for i in ids if i in have}


def _record(engine, *, email: dict, decision: abby.Decision, run_mode: str,
            reply_id: str = "", detail: str = "") -> None:
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO abby_handled (email_id, thread_id, lead, category, action, mode, reply_id, detail) "
            "VALUES (:i, :t, :l, :c, :a, :m, :r, :d) ON CONFLICT (email_id) DO NOTHING"),
            {"i": email.get("id"), "t": email.get("thread_id"), "l": email.get("lead"),
             "c": decision.category, "a": decision.action, "m": run_mode,
             "r": reply_id, "d": (detail or decision.reason)[:500]})


def _thread_categories(engine, thread_id: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT category FROM abby_handled WHERE thread_id = :t AND action = 'reply'"),
                            {"t": thread_id}).fetchall()
    return [r[0] for r in rows]


def _abby_reply_ids(engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT reply_id FROM abby_handled WHERE reply_id <> ''")).fetchall()
    return {r[0] for r in rows}


def _sent_today(engine, now: datetime) -> int:
    since = (now - timedelta(hours=24)).replace(tzinfo=None)
    with engine.connect() as conn:
        row = conn.execute(text("SELECT COUNT(*) FROM abby_handled WHERE action = 'reply' AND mode = 'live' "
                                "AND created_at >= :s"), {"s": since}).fetchone()
    return int(row[0] or 0)


def _human_already_replied(thread: list[dict], received: dict, abby_ids: set[str]) -> bool:
    """A manual send (ue_type 3) in this thread that Abby did not write."""
    return any(m.get("ue_type") == 3 and m.get("id") not in abby_ids for m in thread)


def _subject(s: str) -> str:
    s = (s or "").strip()
    return s if s.lower().startswith("re:") else f"Re: {s}"


def run(engine, *, client: Instantly, campaign_id: str, now: Optional[datetime] = None,
        think: Optional[Callable[..., abby.Decision]] = None,
        post_slack: Optional[Callable[[str], Any]] = None,
        lead_info: Optional[Callable[[str], dict]] = None) -> dict[str, Any]:
    """One pass. Returns a summary; never raises on a single bad email."""
    now = now or datetime.now(timezone.utc)
    run_mode = mode()
    out: dict[str, Any] = {"mode": run_mode, "seen": 0, "replied": 0, "held": 0, "blocked": 0,
                           "ignored": 0, "skipped": 0, "followups": 0}
    if run_mode == "off":
        out["reason"] = "ABBY_MODE is off"
        return out
    ensure_tables(engine)
    think = think or abby.think
    post_slack = post_slack or (lambda t: None)
    lead_info = lead_info or (lambda e: {})
    cap = int(os.getenv("ABBY_DAILY_CAP") or 60)

    since = max(start_at(), now - timedelta(days=2))
    received = [e for e in client.received_since(since, campaign_id) if e.get("id")]
    done = _handled_ids(engine, [e["id"] for e in received])
    todo = [e for e in received if e["id"] not in done][:MAX_PER_RUN]
    abby_ids = _abby_reply_ids(engine)
    out["seen"] = len(todo)

    for email in todo:
        lead = str(email.get("lead") or email.get("from_address_email") or "").lower()
        thread = client.thread(email.get("thread_id") or "") or [email]
        if _human_already_replied(thread, email, abby_ids):
            _record(engine, email=email, decision=abby.Decision("other", "skip", reason="person already replied in thread"),
                    run_mode=run_mode)
            out["skipped"] += 1
            continue
        info = lead_info(lead) or {}
        decision = think(thread, lead_email=lead, company=info.get("company", ""),
                         first_name=info.get("first_name", ""),
                         thread_categories=_thread_categories(engine, email.get("thread_id") or ""))

        if decision.action == "reply" and run_mode == "live" and _sent_today(engine, now) >= cap:
            decision = abby.Decision(decision.category, "hold", reason="daily cap reached",
                                     slack=f"Abby hit her daily cap; {lead} is waiting for a reply.")

        reply_id, detail = "", ""
        if decision.action == "reply":
            if run_mode == "live":
                st, data = client.reply(reply_to=email["id"], eaccount=email.get("eaccount") or "",
                                        subject=_subject(email.get("subject") or ""), body=decision.reply,
                                        cc=decision.cc, extra=decision.to_extra)
                if st != 200:
                    decision = abby.Decision(decision.category, "hold", reason=f"send failed ({st})",
                                             slack=f"Abby's reply to {lead} failed to send ({st}). Nothing went out.")
                else:
                    reply_id = str((data or {}).get("id") or "")
                    out["replied"] += 1
                    if decision.follow_up_days:
                        with engine.begin() as conn:
                            conn.execute(text(
                                "INSERT INTO abby_followups (lead, reply_to_id, eaccount, subject, first_name, due_at) "
                                "VALUES (:l, :r, :e, :s, :f, :d)"),
                                {"l": lead, "r": reply_id or email["id"], "e": email.get("eaccount") or "",
                                 "s": _subject(email.get("subject") or ""), "f": info.get("first_name", ""),
                                 "d": (now + timedelta(days=decision.follow_up_days)).replace(tzinfo=None)})
            else:
                detail = "shadow: " + decision.reply
                post_slack(f"[Abby draft, not sent] {lead} ({decision.category})\n\n{decision.reply}")
        if decision.action == "block":
            if run_mode == "live":
                dom = decision.block_domain
                client.block(lead if (not dom or dom in FREEMAIL) else dom)
            out["blocked"] += 1
        if decision.interest is not None and run_mode == "live" and decision.action in {"reply", "block", "ignore", "hold"}:
            client.set_interest(lead, decision.interest, campaign_id)
        if decision.action == "hold":
            out["held"] += 1
        if decision.action == "ignore":
            out["ignored"] += 1
        if decision.slack and (decision.action == "hold" or run_mode == "live"):
            post_slack(decision.slack)
        _record(engine, email=email, decision=decision, run_mode=run_mode, reply_id=reply_id, detail=detail)

    out["followups"] = _send_due_followups(engine, client, now, run_mode, post_slack)
    return out


def followup_text(first_name: str) -> str:
    hi = f"Hi {first_name}," if first_name else "Hi,"
    return (f"{hi}\n\nChecking back in as promised. Is Amazon growth on the table now? "
            f"Happy to set up the free 14-day trial, where we build your Amazon strategy and run the ad automation. "
            f"Grab a time with David here: {abby.BOOKING_LINK}\n\nAbby")


def _send_due_followups(engine, client: Instantly, now: datetime, run_mode: str,
                        post_slack: Callable[[str], Any]) -> int:
    if run_mode != "live":
        return 0
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, lead, reply_to_id, eaccount, subject, first_name FROM abby_followups "
            "WHERE status = 'pending' AND due_at <= :n ORDER BY due_at LIMIT 5"),
            {"n": now.replace(tzinfo=None)}).fetchall()
    sent = 0
    for fid, lead, reply_to, eaccount, subject, first in rows:
        st, _ = client.reply(reply_to=reply_to, eaccount=eaccount, subject=subject,
                             body=followup_text(first or ""), cc=[], extra=[])
        with engine.begin() as conn:
            conn.execute(text("UPDATE abby_followups SET status = :s WHERE id = :i"),
                         {"s": "sent" if st == 200 else f"failed {st}", "i": fid})
        if st == 200:
            sent += 1
        else:
            post_slack(f"Abby's 60-day check-in to {lead} failed to send ({st}).")
    return sent
