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
  * hostile or legal replies blocklist the lead, mark them not interested,
    and get no further sends. Unsure or failed drafts are held for Slack;
  * departed-employee notices and automated forwards are ignored: no reply,
    no Interested status, no David CC, no follow-up. A departed contact is
    recorded so the brand can be tried again later;
  * out-of-office replies are not answered now. A follow-up is scheduled
    for the day after the parsed return date, or 7 days when no date is found;
  * the same handle_email() path serves the 5-minute poll and the reply webhook.
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
MAX_PER_RUN = 8   # Instantly allows ~20 email reads a minute

_DDL = (
    """CREATE TABLE IF NOT EXISTS abby_handled (
        email_id TEXT PRIMARY KEY, thread_id TEXT, lead TEXT, category TEXT,
        action TEXT, mode TEXT, reply_id TEXT, detail TEXT, campaign_id TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS abby_followups (
        id SERIAL PRIMARY KEY, lead TEXT, reply_to_id TEXT, eaccount TEXT, subject TEXT,
        first_name TEXT, due_at TIMESTAMP, status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS abby_followups_scheduled (
        id SERIAL PRIMARY KEY, lead TEXT, reply_to_id TEXT, eaccount TEXT, subject TEXT,
        first_name TEXT, brand TEXT, campaign_id TEXT, kind TEXT, due_at TIMESTAMP,
        status TEXT DEFAULT 'pending', body TEXT, source_email_id TEXT, detail TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, sent_at TIMESTAMP,
        UNIQUE (source_email_id, kind))""",
)


# The trial campaign and the five signal campaigns. ABBY_CAMPAIGN_IDS
# overrides this list. AGENT SDR and Re-engage are always watched in
# addition to whatever the env var names.
TRIAL_CAMPAIGN_ID = "e5f5046b-07e4-41ad-a395-79cf8766ccd0"
SIGNAL_CAMPAIGN_IDS = (
    "a333e00e-bc3b-4abf-bcbf-4e0eb126090c",
    "45a521a0-68e2-439f-9622-1672f9ede214",
    "1937c21a-89c4-458f-a949-306f90ae6258",
    "d12b5512-d34c-4331-841f-8e59263f5b5e",
    "8ebf1923-7ba4-4a63-96a8-adb18a58ddf6",
)


def watched_campaign_ids(reengage_ids: Optional[list[str]] = None) -> list[str]:
    """Campaigns Abby reads. Trial and the signal campaigns are the default extras; SDR and Re-engage stay."""
    import outbound_instantly_push as ip

    raw = os.getenv("ABBY_CAMPAIGN_IDS")
    if raw is None or not raw.strip():
        extras = [TRIAL_CAMPAIGN_ID, *SIGNAL_CAMPAIGN_IDS]
    else:
        extras = [part.strip() for part in raw.split(",") if part.strip()]
    ordered = [ip.LIVE_CAMPAIGN_ID, *(reengage_ids or []), *extras]
    seen: list[str] = []
    for campaign_id in ordered:
        if campaign_id and campaign_id not in seen:
            seen.append(campaign_id)
    return seen


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


def takeover_at() -> datetime:
    """When the backlog sweep and nudges begin (previewed first)."""
    raw = (os.getenv("ABBY_TAKEOVER_AT") or "2026-10-26T23:30:00Z").strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime(2026, 10, 26, 23, 30, tzinfo=timezone.utc)


def ensure_tables(engine) -> None:
    """Postgres tables are created by migration; Vercel's role cannot run DDL."""
    if os.getenv("VERCEL") and engine.dialect.name == "postgresql":
        return
    ddl = [d.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
           if engine.dialect.name == "sqlite" else d for d in _DDL]
    with engine.begin() as conn:
        for d in ddl:
            conn.execute(text(d))
    if engine.dialect.name == "postgresql":
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE abby_handled ADD COLUMN IF NOT EXISTS campaign_id TEXT"))
        return
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE abby_handled ADD COLUMN campaign_id TEXT"))
    except Exception:  # noqa: BLE001 — column already present
        pass


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

    def received_since(self, since: datetime, campaign_id: str, pages: int = 3) -> list[dict]:
        items: list[dict] = []
        params: dict[str, Any] = {
            "email_type": "received", "campaign_id": campaign_id, "sort_order": "asc",
            "limit": 100, "min_timestamp_created": since.isoformat().replace("+00:00", "Z")}
        for _ in range(pages):
            st, data = self._call("GET", "emails", params=params)
            if st != 200:
                break
            items += (data or {}).get("items") or []
            nxt = (data or {}).get("next_starting_after")
            if not nxt:
                break
            params["starting_after"] = nxt
        return items

    def interested_latest(self, campaign_id: str) -> list[dict]:
        """Latest email of each thread for leads marked Interested (status 1)."""
        st, data = self._call("GET", "emails", params={
            "i_status": 1, "latest_of_thread": "true", "campaign_id": campaign_id, "limit": 100})
        return (data or {}).get("items") or [] if st == 200 else []

    def campaign_ids_named(self, name: str) -> list[str]:
        st, data = self._call("GET", "campaigns", params={"search": name[:20], "limit": 10})
        items = (data or {}).get("items") or [] if st == 200 else []
        return [c["id"] for c in items if c.get("name") == name and c.get("id")]

    def lead(self, email: str) -> dict:
        st, data = self._call("POST", "leads/list", json={"contacts": [email], "limit": 1})
        items = (data or {}).get("items") or [] if st == 200 else []
        return items[0] if items else {}

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

    def get_email(self, email_id: str) -> dict:
        """One email by id. Empty when Instantly does not return it."""

        if not email_id:
            return {}
        status, data = self._call("GET", f"emails/{email_id}")
        return data if status == 200 and isinstance(data, dict) else {}

    def list_webhooks(self) -> tuple[int, Any]:
        """Every workspace webhook, one page at a time. ``(status, body)``."""

        items: list[dict] = []
        params: dict[str, Any] = {"limit": 100}
        status = 200
        body: Any = {"items": []}
        for _ in range(10):
            status, body = self._call("GET", "webhooks", params=params)
            if status != 200:
                return status, body
            batch = body.get("items") if isinstance(body, dict) else body
            if isinstance(batch, list):
                items.extend(item for item in batch if isinstance(item, dict))
            nxt = body.get("next_starting_after") if isinstance(body, dict) else None
            if not nxt:
                break
            params["starting_after"] = nxt
        return status, {"items": items}

    def create_webhook(self, payload: dict) -> tuple[int, Any]:
        return self._call("POST", "webhooks", json=payload)


def _naive(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


def _handled_ids(engine, ids: list[str]) -> set[str]:
    """Email ids that are finished. A processing row older than 15 minutes is not finished."""

    if not ids:
        return set()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=15)).replace(tzinfo=None)
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT email_id, action, created_at FROM abby_handled")).fetchall()
    have: set[str] = set()
    for email_id, action, created in rows:
        if action == "processing":
            stamp = _naive(created)
            if stamp is None or stamp < cutoff:
                continue
        have.add(email_id)
    return {i for i in ids if i in have}


def email_campaign_id(email: dict, fallback: str = "") -> str:
    """Campaign on the Instantly email, or the campaign Abby was reading."""

    raw = email.get("campaign_id")
    if isinstance(raw, dict):
        raw = raw.get("id") or raw.get("campaign_id")
    found = str(raw or "").strip()
    return found or str(fallback or "").strip()


def _record(engine, *, email: dict, decision: abby.Decision, run_mode: str,
            reply_id: str = "", detail: str = "", campaign_id: str = "") -> None:
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO abby_handled (email_id, thread_id, lead, category, action, mode, reply_id, detail, campaign_id) "
            "VALUES (:i, :t, :l, :c, :a, :m, :r, :d, :camp) ON CONFLICT (email_id) DO NOTHING"),
            {"i": email.get("id"), "t": email.get("thread_id"), "l": email.get("lead"),
             "c": decision.category, "a": decision.action, "m": run_mode,
             "r": reply_id, "d": _detail(email, decision, detail),
             "camp": email_campaign_id(email, campaign_id)})


def _detail(email: dict, decision: abby.Decision, detail: str) -> str:
    """Reason, plus what the prospect said and what Abby sent, so the daily
    action list can draft a reply without opening Instantly."""
    them = str((email.get("body") or {}).get("text") or email.get("content_preview") or "")
    them = re.split(r"\n\s*On .{5,90} wrote:|\nFrom: ", them)[0]
    them = re.sub(r"\s+", " ", them).strip()[:400]
    parts = [detail or decision.reason]
    if decision.note:
        parts.append(decision.note)
    if them:
        parts.append(f"THEM: {them}")
    if decision.reply and decision.action == "reply":
        parts.append("ABBY: " + re.sub(r"\s+", " ", decision.reply)[:300])
    return " | ".join(p for p in parts if p)[:1000]


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
        row = conn.execute(text("SELECT COUNT(*) FROM abby_handled WHERE action IN ('reply', 'nudge', 'followup') AND mode = 'live' "
                                "AND created_at >= :s"), {"s": since}).fetchone()
    return int(row[0] or 0)


def _ts(m: dict) -> str:
    return str(m.get("timestamp_created") or m.get("timestamp_email") or "")


def _sender(m: dict) -> str:
    return str(m.get("from_address_email") or "").strip().lower()


def is_team(address: str) -> bool:
    """Anyone at Anata: David, the team, or one of our own sending inboxes.

    David often answers from Gmail with the Abby inbox on the thread, so his
    reply lands in Instantly as a *received* email. It must never be treated
    as the prospect talking (2026-10-27: Abby answered David as if he were Brad).
    """
    a = (address or "").strip().lower()
    if not a or "@" not in a:
        return False
    if a == abby.DAVID_EMAIL:
        return True
    domain = a.split("@")[-1]
    extra = {d.strip().lower() for d in (os.getenv("ABBY_TEAM_DOMAINS") or "").split(",") if d.strip()}
    return "anata" in domain or domain in extra


def team_in_thread(thread: list[dict]) -> bool:
    """A person from Anata wrote in this thread (received from a team address).
    From then on the thread is theirs: Abby neither replies nor nudges."""
    return any(m.get("ue_type") == 2 and is_team(_sender(m)) for m in thread)


def _human_already_replied(thread: list[dict], received: dict, abby_ids: set[str]) -> bool:
    """Kept for the preview: a manual send (ue_type 3) Abby did not write."""
    return any(m.get("ue_type") == 3 and m.get("id") not in abby_ids for m in thread)


def already_answered(thread: list[dict], received: dict) -> bool:
    """Someone (Gabe, Abby, or a campaign step) sent something after this reply,
    or the prospect wrote again later (the later message is the one to answer)."""
    t = _ts(received)
    for m in thread:
        if m.get("id") == received.get("id") or not t:
            continue
        if _ts(m) > t and m.get("ue_type") in (1, 2, 3):
            return True
    return False


def _subject(s: str) -> str:
    s = (s or "").strip()
    return s if s.lower().startswith("re:") else f"Re: {s}"


STALE_CLAIM = timedelta(minutes=15)


def _claim(engine, email: dict, run_mode: str, campaign_id: str = "") -> bool:
    """True when this worker owns the email. A settled row is left alone.

    A processing row older than 15 minutes is cleared so a killed webhook
    attempt can be retried. ``already_answered`` still stops a second send.
    """

    email_id = email.get("id")
    if not email_id:
        return False
    params = {
        "i": email_id,
        "t": email.get("thread_id"),
        "l": email.get("lead"),
        "m": run_mode,
        "camp": email_campaign_id(email, campaign_id),
        "cutoff": (datetime.now(timezone.utc) - STALE_CLAIM).replace(tzinfo=None),
    }
    insert = (
        "INSERT INTO abby_handled (email_id, thread_id, lead, category, action, mode, reply_id, detail, campaign_id) "
        "VALUES (:i, :t, :l, '', 'processing', :m, '', '', :camp) "
        "ON CONFLICT (email_id) DO NOTHING RETURNING email_id"
    )
    with engine.begin() as conn:
        if conn.execute(text(insert), params).fetchone():
            return True
        cleared = conn.execute(text(
            "DELETE FROM abby_handled WHERE email_id = :i AND action = 'processing' "
            "AND created_at < :cutoff RETURNING email_id"
        ), params).fetchone()
        if not cleared:
            return False
        return conn.execute(text(insert), params).fetchone() is not None


def _finish(engine, *, email: dict, decision: abby.Decision, run_mode: str,
            reply_id: str = "", detail: str = "", campaign_id: str = "") -> None:
    camp = email_campaign_id(email, campaign_id)
    params = {
        "i": email.get("id"), "t": email.get("thread_id"), "l": email.get("lead"),
        "c": decision.category, "a": decision.action, "m": run_mode,
        "r": reply_id, "d": _detail(email, decision, detail),
        "camp": camp,
    }
    with engine.begin() as conn:
        updated = conn.execute(text(
            "UPDATE abby_handled SET thread_id = :t, lead = :l, category = :c, action = :a, "
            "mode = :m, reply_id = :r, detail = :d, campaign_id = :camp WHERE email_id = :i"
        ), params)
        wrote = updated.rowcount != 0
    if not wrote:
        _record(engine, email=email, decision=decision, run_mode=run_mode, reply_id=reply_id,
                detail=detail, campaign_id=camp)


def _message_text(email: dict) -> str:
    return str((email.get("body") or {}).get("text") or email.get("content_preview") or "")


def _is_suppressed(engine, lead: str) -> bool:
    try:
        import outbound_suppression as suppression

        emails, domains = suppression.lists(engine)
        return bool(suppression.matched_value(lead, "", emails, domains))
    except Exception:  # noqa: BLE001
        logger.exception("[abby] suppression lookup failed")
        return False


def _cancel_scheduled(engine, lead: str) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE abby_followups_scheduled SET status = 'cancelled' "
                "WHERE lead = :l AND status = 'pending'"
            ), {"l": lead})
    except Exception:  # noqa: BLE001
        logger.exception("[abby] could not cancel scheduled follow-ups")


def _schedule_rows(engine, *, email: dict, decision: abby.Decision, info: dict, lead: str,
                   reply_to: str, now: datetime, run_mode: str, campaign_id: str) -> int:
    """Insert playbook follow-ups. Live rows are pending; shadow rows are not sent."""

    if not decision.schedule_kind:
        return 0
    if decision.schedule_kind in {"not_now", "has_solution"} and decision.action != "reply":
        return 0
    if decision.schedule_kind == "out_of_office" and decision.action != "ignore":
        return 0
    import sales_support_agent.abby_playbook as playbook

    first = str(info.get("first_name") or "")
    brand = str(info.get("company") or "")
    reply_text = _message_text(email)
    status = "pending" if run_mode == "live" else "shadow"
    planned: list[tuple[str, datetime, str]] = []
    if decision.schedule_kind == "out_of_office":
        due = playbook.ooo_due(reply_text, now)
        planned.append(("out_of_office", due, playbook.render_ooo_followup(
            first_name=first, brand=brand, reply_text=reply_text)))
    elif decision.schedule_kind == "not_now":
        for day in playbook.NOT_NOW_DAYS:
            planned.append((f"not_now_{day}", now + timedelta(days=day), playbook.render_not_now(day, first)))
    elif decision.schedule_kind == "has_solution":
        due = now + timedelta(days=playbook.HAS_SOLUTION_SECOND_DAYS)
        planned.append(("has_solution_second", due, playbook.render_has_solution_second(first)))
    saved = 0
    for kind, due, body in planned:
        try:
            with engine.begin() as conn:
                conn.execute(text(
                    "INSERT INTO abby_followups_scheduled "
                    "(lead, reply_to_id, eaccount, subject, first_name, brand, campaign_id, kind, "
                    "due_at, status, body, source_email_id, detail) "
                    "VALUES (:l, :r, :e, :s, :f, :b, :c, :k, :d, :st, :body, :src, :detail) "
                    "ON CONFLICT (source_email_id, kind) DO NOTHING"
                ), {
                    "l": lead, "r": reply_to, "e": email.get("eaccount") or "",
                    "s": _subject(email.get("subject") or ""), "f": first, "b": brand,
                    "c": campaign_id, "k": kind, "d": due.replace(tzinfo=None),
                    "st": status, "body": body, "src": email.get("id") or "",
                    "detail": decision.schedule_kind,
                })
            saved += 1
        except Exception:  # noqa: BLE001
            logger.exception("[abby] could not schedule %s for %s", kind, lead)
    return saved


def handle_email(engine, email: dict, *, client: Instantly, campaign_id: str,
                 now: Optional[datetime] = None,
                 think: Optional[Callable[..., abby.Decision]] = None,
                 post_slack: Optional[Callable[[str], Any]] = None,
                 lead_info: Optional[Callable[[str], dict]] = None,
                 run_mode: Optional[str] = None, cap: Optional[int] = None) -> dict[str, int]:
    """Apply every guardrail to one received email and record the result.

    The poll and the reply webhook both call this. A second call for the same
    email id does nothing. Sends happen only in live mode, under the daily cap.
    """

    now = now or datetime.now(timezone.utc)
    run_mode = run_mode or mode()
    counts = {"replied": 0, "held": 0, "blocked": 0, "ignored": 0, "skipped": 0, "scheduled": 0}
    if run_mode == "off":
        return counts
    ensure_tables(engine)
    if not email.get("id"):
        counts["skipped"] += 1
        return counts
    resolved_campaign = email_campaign_id(email, campaign_id)
    email["campaign_id"] = resolved_campaign
    if not _claim(engine, email, run_mode, resolved_campaign):
        counts["skipped"] += 1
        return counts
    think = think or abby.think
    post_slack = post_slack or (lambda _t: None)
    lead_info = lead_info or (lambda _e: {})
    if cap is None:
        cap = int(os.getenv("ABBY_DAILY_CAP") or 60)
    active_campaign = resolved_campaign
    try:
        return _handle_claimed(
            engine, email, client=client, campaign_id=active_campaign, now=now,
            think=think, post_slack=post_slack, lead_info=lead_info,
            run_mode=run_mode, cap=cap, counts=counts,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[abby] failed while handling %s", email.get("id"))
        _finish(engine, email=email, decision=abby.Decision("other", "hold", reason="handler error"),
                run_mode=run_mode)
        counts["held"] += 1
        return counts


def _handle_claimed(engine, email: dict, *, client: Instantly, campaign_id: str, now: datetime,
                    think: Callable[..., abby.Decision], post_slack: Callable[[str], Any],
                    lead_info: Callable[[str], dict], run_mode: str, cap: int,
                    counts: dict[str, int]) -> dict[str, int]:
    lead = str(email.get("lead") or email.get("from_address_email") or "").lower()
    if is_team(_sender(email)) or is_team(lead):
        _finish(engine, email=email, decision=abby.Decision("other", "skip", reason="written by our team"),
                run_mode=run_mode)
        counts["skipped"] += 1
        return counts
    if lead and _is_suppressed(engine, lead):
        _finish(engine, email=email, decision=abby.Decision("other", "skip", reason="suppressed: no further sends"),
                run_mode=run_mode)
        counts["skipped"] += 1
        return counts
    thread = client.thread(email.get("thread_id") or "") or [email]
    if team_in_thread(thread):
        _finish(engine, email=email, decision=abby.Decision("other", "skip", reason="David has this thread"),
                run_mode=run_mode)
        counts["skipped"] += 1
        return counts
    if already_answered(thread, email):
        _finish(engine, email=email, decision=abby.Decision("other", "skip", reason="already answered"),
                run_mode=run_mode)
        counts["skipped"] += 1
        return counts
    info = lead_info(lead) or _instantly_info(client, lead)
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
                counts["replied"] += 1
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
        dom = decision.block_domain
        blocked_value = lead if (not dom or dom in FREEMAIL) else dom
        if run_mode == "live":
            client.block(blocked_value)
        reason = "hostile" if decision.category == "hostile" else "opt_out"
        _remember_opt_out(engine, blocked_value, post_slack, reason=reason)
        _cancel_scheduled(engine, lead)
        counts["blocked"] += 1
    elif decision.action == "reply" and decision.category in {"interested", "price", "question"}:
        _cancel_scheduled(engine, lead)
    if decision.interest is not None and run_mode == "live" and decision.action in {"reply", "block", "ignore", "hold"}:
        client.set_interest(lead, decision.interest, campaign_id)
    if decision.action == "hold":
        counts["held"] += 1
    if decision.action == "ignore":
        counts["ignored"] += 1
    if decision.slack and (decision.action in {"hold", "block"} or run_mode == "live"):
        post_slack(decision.slack)
    counts["scheduled"] += _schedule_rows(
        engine, email=email, decision=decision, info=info, lead=lead,
        reply_to=reply_id or str(email.get("id") or ""), now=now, run_mode=run_mode,
        campaign_id=campaign_id,
    )
    _finish(engine, email=email, decision=decision, run_mode=run_mode, reply_id=reply_id, detail=detail)
    return counts


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

    # David 2026-10-26: Abby takes over follow-ups, including the backlog of
    # unanswered replies from the last 30 days (threads Gabe started included).
    since = now - timedelta(days=int(os.getenv("ABBY_BACKLOG_DAYS") or 30))
    if now < takeover_at():
        since = max(start_at(), now - timedelta(days=2))   # before takeover: new replies only
    received = [e for e in client.received_since(since, campaign_id) if e.get("id")]
    done = _handled_ids(engine, [e["id"] for e in received])
    todo = [e for e in received if e["id"] not in done][:MAX_PER_RUN]
    out["seen"] = len(todo)

    for email in todo:
        part = handle_email(
            engine, email, client=client, campaign_id=campaign_id, now=now,
            think=think, post_slack=post_slack, lead_info=lead_info,
            run_mode=run_mode, cap=cap,
        )
        for key in ("replied", "held", "blocked", "ignored", "skipped", "scheduled"):
            out[key] = out.get(key, 0) + part.get(key, 0)

    out["scheduled_sent"] = send_due_scheduled(engine, client, now, run_mode, post_slack, cap)
    out["followups"] = _send_due_followups(engine, client, now, run_mode, post_slack)
    out["nudges"] = _send_nudges(engine, client, campaign_id, now, run_mode, post_slack, lead_info, cap)
    return out


def _remember_opt_out(engine, value: str, post_slack: Optional[Callable[[str], Any]] = None,
                      reason: str = "opt_out") -> None:
    """Keep Abby's opt-out or hostile block on our list as well as Instantly's blocklist.

    Shadow mode records the same row. Only live mode calls Instantly's block API.
    """
    try:
        import outbound_suppression as suppression
        kind = "email" if "@" in (value or "") else "domain"
        suppression.add(engine, kind=kind, value=value, reason=reason or "opt_out", source="abby")
    except Exception:  # noqa: BLE001
        logger.exception("[abby] could not record the opt-out")
        if post_slack is None:
            return
        try:
            post_slack(f"[abby] opt-out not saved: {value}")
        except Exception:  # noqa: BLE001
            logger.exception("[abby] could not post the opt-out failure")


def followup_text(first_name: str) -> str:
    hi = f"Hi {first_name}," if first_name else "Hi,"
    return (f"{hi}\n\nChecking back in as promised. Is Amazon growth on the table now? "
            f"Happy to set up the free 14-day trial, where we build your Amazon strategy and run the ad automation. "
            f"Grab a time with David here: {abby.BOOKING_LINK}\n\n{abby.SIGNATURE}")


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


def send_due_scheduled(engine, client: Instantly, now: datetime, run_mode: str,
                       post_slack: Callable[[str], Any], cap: int) -> int:
    """Send due rows from abby_followups_scheduled. Live mode only, under the daily cap.

    A row is claimed (pending -> sending) before the Instantly call so the poll
    and the follow-up cron cannot send it twice.
    """

    if run_mode != "live":
        return 0
    ensure_tables(engine)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, lead, reply_to_id, eaccount, subject, body FROM abby_followups_scheduled "
                "WHERE status = 'pending' AND due_at <= :n ORDER BY due_at LIMIT 8"
            ), {"n": now.replace(tzinfo=None)}).fetchall()
    except Exception:  # noqa: BLE001
        logger.exception("[abby] could not read scheduled follow-ups")
        return 0
    sent = 0
    capped = False
    for fid, lead, reply_to, eaccount, subject, body in rows:
        if _sent_today(engine, now) >= cap:
            capped = True
            break
        if lead and _is_suppressed(engine, str(lead)):
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE abby_followups_scheduled SET status = 'cancelled', detail = 'suppressed' "
                    "WHERE id = :i AND status = 'pending'"
                ), {"i": fid})
            continue
        with engine.begin() as conn:
            claimed = conn.execute(text(
                "UPDATE abby_followups_scheduled SET status = 'sending' "
                "WHERE id = :i AND status = 'pending' RETURNING id"
            ), {"i": fid}).fetchone()
            if not claimed:
                continue
        status, data = client.reply(
            reply_to=reply_to or "", eaccount=eaccount or "", subject=subject or "",
            body=body or "", cc=[], extra=[],
        )
        if status == 200:
            _record(
                engine,
                email={"id": f"followup:{fid}", "thread_id": "", "lead": lead, "body": {"text": ""}},
                decision=abby.Decision("followup", "followup", reason=f"scheduled follow-up {fid}"),
                run_mode="live",
                reply_id=str((data or {}).get("id") or ""),
            )
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE abby_followups_scheduled SET status = 'sent', sent_at = :n WHERE id = :i"
                ), {"n": now.replace(tzinfo=None), "i": fid})
            sent += 1
        else:
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE abby_followups_scheduled SET status = :s WHERE id = :i"
                ), {"s": f"failed {status}", "i": fid})
            post_slack(f"Abby's scheduled follow-up to {lead} failed to send ({status}).")
    if capped:
        post_slack("Abby hit her daily cap. Scheduled follow-ups are waiting.")
    return sent


def _instantly_info(client: Instantly, lead: str) -> dict:
    try:
        rec = client.lead(lead) or {}
    except Exception:  # noqa: BLE001
        return {}
    return {"first_name": str(rec.get("first_name") or ""), "company": str(rec.get("company_name") or "")}


# Nudges for Interested leads that went quiet: days since OUR last message
# before nudge 1, 2, 3 (3, then 4 more = day 7, then 7 more = day 14).
NUDGE_GAPS_DAYS = (3, 4, 7)
MAX_NUDGES_PER_RUN = 4


def nudge_text(n: int, first_name: str, company: str) -> tuple[str, list[str]]:
    hi = f"Hi {first_name}," if first_name else "Hi there,"
    link = abby.BOOKING_LINK
    sign = abby.SIGNATURE
    if n == 1:
        body = (f"{hi}\n\nQuick nudge in case this got buried. Happy to set up the free 14-day trial, "
                f"where we build your Amazon strategy and run the ad automation. Grab a time with David here: {link}\n\n{sign}")
        return body, []
    if n == 2:
        who = company or "your brand"
        body = (f"{hi}\n\nStill keen to show you what the free 14-day trial would look like for {who}. "
                f"Here is David's calendar: {link}\n\n{sign}")
        return body, []
    body = (f"{hi}\n\nLast note from me. If the timing is off, no worries at all. If you want the free 14-day trial, "
            f"David's calendar is here: {link}\n\n{sign}")
    return body, []


def _nudges_sent(engine, thread_id: str) -> int:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT COUNT(*) FROM abby_handled WHERE thread_id = :t AND action = 'nudge'"),
                           {"t": thread_id}).fetchone()
    return int(row[0] or 0)


def _nudges_done(engine, thread_id: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT 1 FROM abby_handled WHERE email_id = :i"),
                           {"i": f"nudge:{thread_id}:done"}).fetchone()
    return row is not None


def _parse_ts(s: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _send_nudges(engine, client: Instantly, campaign_id: str, now: datetime, run_mode: str,
                 post_slack: Callable[[str], Any], lead_info: Callable[[str], dict], cap: int) -> int:
    if run_mode == "off" or now < takeover_at():
        return 0
    sent = 0
    for latest in client.interested_latest(campaign_id):
        if sent >= MAX_NUDGES_PER_RUN:
            break
        if latest.get("ue_type") not in (1, 3):
            continue                      # prospect wrote last: the reply flow answers it
        thread_id = str(latest.get("thread_id") or "")
        n = _nudges_sent(engine, thread_id) + 1
        if not thread_id or n > len(NUDGE_GAPS_DAYS):
            continue
        last = _parse_ts(_ts(latest))
        if last is None or now - last < timedelta(days=NUDGE_GAPS_DAYS[n - 1]):
            continue
        if _nudges_done(engine, thread_id):
            continue
        thread = client.thread(thread_id)
        if team_in_thread(thread):
            _record(engine, email={"id": f"nudge:{thread_id}:done", "thread_id": thread_id,
                                   "lead": str(latest.get("lead") or ""),
                                   "campaign_id": latest.get("campaign_id") or campaign_id},
                    decision=abby.Decision("nudge", "skip", reason="David has this thread"),
                    run_mode=run_mode, campaign_id=campaign_id)
            continue
        received = [m for m in thread if m.get("ue_type") == 2 and not is_team(_sender(m))]
        if not received:
            continue                      # never replied: nothing to reply to safely
        anchor = received[-1]
        # Follow-ups a person already sent after the prospect's last message count
        # toward the three, so nobody gets six chasers.
        abby_ids = _abby_reply_ids(engine)
        manual = sum(1 for m in thread if m.get("ue_type") == 3 and _ts(m) > _ts(anchor)
                     and m.get("id") not in abby_ids)
        n = _nudges_sent(engine, thread_id) + manual + 1
        if n > len(NUDGE_GAPS_DAYS):
            _record(engine, email={"id": f"nudge:{thread_id}:done", "thread_id": thread_id,
                                   "lead": str(anchor.get("lead") or ""),
                                   "campaign_id": anchor.get("campaign_id") or campaign_id},
                    decision=abby.Decision("nudge", "skip", reason="already followed up 3 times"),
                    run_mode=run_mode, campaign_id=campaign_id)
            continue
        lead = str(anchor.get("lead") or latest.get("lead") or "").lower()
        info = lead_info(lead) or _instantly_info(client, lead)
        body, cc = nudge_text(n, info.get("first_name", ""), info.get("company", ""))
        if run_mode == "live" and _sent_today(engine, now) >= cap:
            break
        key = {"id": f"nudge:{thread_id}:{n}", "thread_id": thread_id, "lead": lead,
               "campaign_id": anchor.get("campaign_id") or latest.get("campaign_id") or campaign_id}
        if run_mode == "live":
            st, data = client.reply(reply_to=anchor["id"], eaccount=anchor.get("eaccount") or latest.get("eaccount") or "",
                                    subject=_subject(anchor.get("subject") or ""), body=body, cc=cc, extra=[])
            if st != 200:
                post_slack(f"Abby's nudge {n} to {lead} failed to send ({st}).")
                continue
            _record(engine, email=key, decision=abby.Decision("nudge", "nudge", reason=f"nudge {n}"),
                    run_mode=run_mode, reply_id=str((data or {}).get("id") or ""),
                    campaign_id=campaign_id)
            if n == len(NUDGE_GAPS_DAYS):
                post_slack(f"Abby sent her last nudge to {lead}. No booking yet.")
        else:
            post_slack(f"[Abby nudge {n}, not sent] {lead}\n\n{body}")
            _record(engine, email=key, decision=abby.Decision("nudge", "nudge", reason=f"shadow nudge {n}"),
                    run_mode=run_mode, campaign_id=campaign_id)
        sent += 1
    return sent


def preview_takeover(engine, client: Instantly, campaign_id: str, now: datetime,
                     think: Callable[..., abby.Decision], lead_info: Callable[[str], dict],
                     limit: int = 8) -> dict[str, Any]:
    """What the backlog sweep and nudges WOULD do. Sends and records nothing."""
    nudges = []
    for latest in client.interested_latest(campaign_id):
        if latest.get("ue_type") not in (1, 3):
            continue
        tid = str(latest.get("thread_id") or "")
        n = _nudges_sent(engine, tid) + 1
        last = _parse_ts(_ts(latest))
        if not tid or n > len(NUDGE_GAPS_DAYS) or last is None:
            continue
        due = (now - last) >= timedelta(days=NUDGE_GAPS_DAYS[n - 1])
        nudges.append({"lead": str(latest.get("lead") or ""), "nudge": n,
                       "days_since_last": (now - last).days, "due_now": due})
    since = now - timedelta(days=int(os.getenv("ABBY_BACKLOG_DAYS") or 30))
    received = [e for e in client.received_since(since, campaign_id) if e.get("id")]
    done = _handled_ids(engine, [e["id"] for e in received])
    backlog = []
    for e in received:
        if e["id"] in done or len(backlog) >= limit or is_team(_sender(e)):
            continue
        thread = client.thread(e.get("thread_id") or "") or [e]
        if team_in_thread(thread) or already_answered(thread, e):
            continue
        lead = str(e.get("lead") or "").lower()
        info = lead_info(lead) or _instantly_info(client, lead)
        d = think(thread, lead_email=lead, company=info.get("company", ""),
                  first_name=info.get("first_name", ""), thread_categories=[])
        backlog.append({"lead": lead, "received": _ts(e)[:10],
                        "their_message": str((e.get("body") or {}).get("text") or "")[:240],
                        "category": d.category, "action": d.action, "cc": d.cc, "reply": d.reply,
                        "reason": d.reason})
    return {"backlog": backlog, "nudges": nudges, "takeover_at": takeover_at().isoformat()}
