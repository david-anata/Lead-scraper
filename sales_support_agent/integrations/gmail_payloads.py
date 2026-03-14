"""Helpers for normalizing Gmail API payloads."""

from __future__ import annotations

import base64
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parseaddr
from typing import Any

from sales_support_agent.services.reply_templates import build_event_action_summary, build_event_reply_draft


EMAIL_REGEX = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
MEETING_KEYWORDS = ("meeting", "calendar", "schedule", "reschedule", "call", "zoom")
PRICING_KEYWORDS = ("pricing", "price", "quote", "proposal", "offer")


@dataclass(frozen=True)
class NormalizedGmailMessage:
    external_message_id: str
    external_thread_id: str
    external_event_key: str
    sender_name: str
    sender_email: str
    sender_domain: str
    subject: str
    snippet: str
    body_text: str
    candidate_emails: tuple[str, ...]
    classification: str
    urgency: str
    action_summary: str
    recommended_next_action: str
    suggested_reply_draft: str
    occurred_at: datetime
    raw_payload: dict[str, Any]


def normalize_gmail_message(payload: dict[str, Any], *, configured_source_domains: tuple[str, ...], matched_task: bool) -> NormalizedGmailMessage:
    headers = list(((payload.get("payload") or {}).get("headers")) or [])
    sender_name, sender_email, sender_domain = parse_sender(extract_header(headers, "From"))
    subject = extract_header(headers, "Subject")
    snippet = str(payload.get("snippet") or "").strip()
    body_text = extract_body_text(payload.get("payload") or {})
    occurred_at = parse_occurred_at(payload)
    classification = classify_gmail_message(
        sender_domain=sender_domain,
        subject=subject,
        body_text=body_text or snippet,
        configured_source_domains=configured_source_domains,
        matched_task=matched_task,
    )
    recommended_next_action = build_recommended_next_action(classification)
    action_summary = build_event_action_summary(
        classification=classification,
        occurred_at=occurred_at,
        recommended_next_action=recommended_next_action,
    )
    suggested_reply_draft = build_event_reply_draft(
        classification=classification,
        task_name=subject or sender_name or sender_email,
        sender_name=sender_name,
        sender_email=sender_email,
        occurred_at=occurred_at,
    )
    return NormalizedGmailMessage(
        external_message_id=str(payload.get("id") or ""),
        external_thread_id=str(payload.get("threadId") or ""),
        external_event_key=build_external_event_key(payload),
        sender_name=sender_name,
        sender_email=sender_email,
        sender_domain=sender_domain,
        subject=subject,
        snippet=snippet,
        body_text=body_text,
        candidate_emails=extract_candidate_emails(sender_email, subject, body_text),
        classification=classification,
        urgency=classification_to_urgency(classification),
        action_summary=action_summary,
        recommended_next_action=recommended_next_action,
        suggested_reply_draft=suggested_reply_draft,
        occurred_at=occurred_at,
        raw_payload=payload,
    )


def build_external_event_key(payload: dict[str, Any]) -> str:
    raw_key = "|".join([str(payload.get("id") or ""), str(payload.get("threadId") or "")])
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def extract_header(headers: list[dict[str, Any]], name: str) -> str:
    target = (name or "").strip().lower()
    for header in headers:
        if str(header.get("name") or "").strip().lower() == target:
            return str(header.get("value") or "").strip()
    return ""


def parse_sender(raw_header: str) -> tuple[str, str, str]:
    name, email = parseaddr(raw_header or "")
    normalized_email = (email or "").strip().lower()
    domain = normalized_email.split("@", 1)[1] if "@" in normalized_email else ""
    return (name or "").strip(), normalized_email, domain


def extract_body_text(payload: dict[str, Any]) -> str:
    body = _extract_plain_text_part(payload)
    if body:
        return " ".join(body.split())
    return ""


def _extract_plain_text_part(payload: dict[str, Any]) -> str:
    mime_type = str(payload.get("mimeType") or "")
    body_data = str(((payload.get("body") or {}).get("data")) or "")
    if mime_type == "text/plain" and body_data:
        return _decode_base64_url(body_data)

    parts = list(payload.get("parts") or [])
    for part in parts:
        text = _extract_plain_text_part(part)
        if text:
            return text

    if body_data:
        return _decode_base64_url(body_data)
    return ""


def _decode_base64_url(raw: str) -> str:
    padded = raw + "=" * (-len(raw) % 4)
    try:
        return base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8", errors="ignore")
    except Exception:
        return ""


def parse_occurred_at(payload: dict[str, Any]) -> datetime:
    raw = str(payload.get("internalDate") or "").strip()
    if raw.isdigit():
        return datetime.fromtimestamp(int(raw) / 1000, tz=timezone.utc)
    return datetime.now(timezone.utc)


def extract_candidate_emails(sender_email: str, subject: str, body_text: str) -> tuple[str, ...]:
    candidates: list[str] = []
    if sender_email:
        candidates.append(sender_email.strip().lower())
    for match in EMAIL_REGEX.findall(" ".join([subject or "", body_text or ""])):
        normalized = match.strip().lower()
        if normalized not in candidates:
            candidates.append(normalized)
    return tuple(candidates)


def classify_gmail_message(
    *,
    sender_domain: str,
    subject: str,
    body_text: str,
    configured_source_domains: tuple[str, ...],
    matched_task: bool,
) -> str:
    haystack = " ".join([subject or "", body_text or ""]).lower()
    if matched_task:
        if any(token in haystack for token in PRICING_KEYWORDS):
            return "pricing_or_offer_request"
        if any(token in haystack for token in MEETING_KEYWORDS):
            return "meeting_action_needed"
        return "reply_received"
    if sender_domain and sender_domain in {domain.lower() for domain in configured_source_domains}:
        return "lead_source_email"
    if any(token in haystack for token in MEETING_KEYWORDS):
        return "meeting_action_needed"
    return "triage_unmatched"


def classification_to_urgency(classification: str) -> str:
    if classification in {"reply_received", "pricing_or_offer_request", "meeting_action_needed"}:
        return "needs_immediate_review"
    return "follow_up_due"


def build_recommended_next_action(classification: str) -> str:
    actions = {
        "reply_received": "Reply today, confirm the next step, and update ClickUp.",
        "pricing_or_offer_request": "Send pricing or offer language today and confirm timeline.",
        "meeting_action_needed": "Respond with proposed meeting times or confirm the meeting change today.",
        "lead_source_email": "Review the source email, assign ownership, and confirm next outreach.",
        "triage_unmatched": "Review the email, identify the right owner, and decide whether it belongs in ClickUp.",
    }
    return actions.get(classification, "Review the email and decide the next action.")
