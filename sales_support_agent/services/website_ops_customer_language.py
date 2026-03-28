"""Customer language extraction for Website Ops MVP."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sales_support_agent.integrations.gmail import GmailClient, GmailIntegrationError
from sales_support_agent.integrations.gmail_payloads import normalize_gmail_message


QUESTION_PATTERN = re.compile(r"\?|(?:^|\s)(how|what|when|why|where|who|can|does|do|is|are)\b", re.IGNORECASE)
SERVICE_HINTS = {
    "fulfillment": ("fulfillment", "3pl", "warehouse", "inventory", "pick pack"),
    "shipping": ("shipping", "parcel", "delivery", "carrier", "rate shopping"),
    "ai": ("ai", "automation", "workflow", "intelligence"),
    "advertising": ("advertising", "ppc", "media buying", "paid search"),
    "web design": ("web design", "website", "landing page", "site redesign"),
}


@dataclass(frozen=True)
class CustomerQuestion:
    question_id: str
    question: str
    intent: str
    frequency: int
    source: str
    related_service: str
    redacted_examples: list[str]
    last_seen_at: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_text(value: str) -> str:
    return " ".join(str(value or "").split()).strip()


def _question_id(question: str, service: str) -> str:
    seed = f"{service}|{question.lower()}".encode("utf-8")
    return f"cq_{abs(hash(seed)) & 0xFFFFFFFF:08x}"


def _customer_language_dir(settings: Any) -> Path:
    path = Path(settings.website_ops_root) / "customer_language"
    path.mkdir(parents=True, exist_ok=True)
    return path


def fetch_gmail_threads(settings: Any, *, max_messages: int = 30) -> list[dict[str, Any]]:
    threads: dict[str, list[dict[str, Any]]] = defaultdict(list)
    clients = [GmailClient(settings, mailbox_account=account) for account in getattr(settings, "gmail_mailbox_accounts", ()) or ()]
    if not clients:
        clients = [GmailClient(settings)]
    for client in clients:
        if not client.is_configured():
            continue
        try:
            refs = client.list_messages(query=getattr(client, "poll_query", "newer_than:14d"), max_results=max_messages)
        except GmailIntegrationError:
            continue
        for ref in refs:
            message_id = str(ref.get("id") or "").strip()
            if not message_id:
                continue
            try:
                payload = client.get_message(message_id)
            except GmailIntegrationError:
                continue
            normalized = normalize_gmail_message(payload, configured_source_domains=client.source_domains, matched_task=False)
            threads[normalized.external_thread_id].append(
                {
                    "subject": normalized.subject,
                    "snippet": normalized.snippet,
                    "body_text": normalized.body_text,
                    "sender_email": normalized.sender_email,
                    "sender_domain": normalized.sender_domain,
                    "occurred_at": normalized.occurred_at.isoformat(),
                    "thread_id": normalized.external_thread_id,
                }
            )
    return [{"thread_id": thread_id, "messages": messages} for thread_id, messages in threads.items()]


def filter_relevant_threads(threads: list[dict[str, Any]], *, internal_domains: tuple[str, ...] = ("anatainc.com",)) -> list[dict[str, Any]]:
    results = []
    internal = {domain.lower() for domain in internal_domains if domain}
    for thread in threads:
        messages = list(thread.get("messages") or [])
        if len(messages) < 2:
            continue
        if all(str(message.get("sender_domain", "")).lower() in internal for message in messages):
            continue
        haystack = " ".join(_normalize_text(str(message.get("body_text", "") or message.get("snippet", ""))) for message in messages)
        if not QUESTION_PATTERN.search(haystack):
            continue
        results.append(thread)
    return results


def _detect_service(text: str) -> str:
    haystack = text.lower()
    for service, terms in SERVICE_HINTS.items():
        if any(term in haystack for term in terms):
            return service
    return ""


def extract_questions(threads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    raw_questions: list[dict[str, Any]] = []
    for thread in threads:
        examples: list[str] = []
        for message in thread.get("messages", []):
            body = _normalize_text(str(message.get("body_text", "") or message.get("snippet", "")))
            if not body:
                continue
            service = _detect_service(" ".join([str(message.get("subject", "")), body]))
            for sentence in re.split(r"(?<=[\?\.\!])\s+", body):
                normalized = _normalize_text(sentence)
                if not normalized:
                    continue
                if not QUESTION_PATTERN.search(normalized):
                    continue
                question = normalized if normalized.endswith("?") else normalized.split(".")[0].rstrip("?") + "?"
                examples.append(question)
                raw_questions.append(
                    {
                        "question": question,
                        "intent": "transactional" if service else "informational",
                        "related_service": service,
                        "source": "gmail",
                    }
                )
    return raw_questions


def normalize_questions(questions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    counts: Counter[tuple[str, str]] = Counter()
    examples: dict[tuple[str, str], list[str]] = defaultdict(list)
    for item in questions:
        question = _normalize_text(str(item.get("question", ""))).lower().rstrip("?")
        if not question:
            continue
        service = str(item.get("related_service", "")).strip()
        key = (question, service)
        counts[key] += 1
        examples[key].append(str(item.get("question", "")).strip())
        grouped[key] = {
            "question": question + "?",
            "intent": str(item.get("intent", "informational")),
            "source": str(item.get("source", "gmail")),
            "related_service": service,
        }
    normalized_items: list[dict[str, Any]] = []
    for key, base in grouped.items():
        question, service = key
        normalized_items.append(
            asdict(
                CustomerQuestion(
                    question_id=_question_id(question, service),
                    question=base["question"],
                    intent=base["intent"],
                    frequency=counts[key],
                    source=base["source"],
                    related_service=service,
                    redacted_examples=list(dict.fromkeys(examples[key]))[:3],
                    last_seen_at=_now_iso(),
                )
            )
        )
    return sorted(normalized_items, key=lambda item: (-int(item["frequency"]), item["question"]))


def collect_customer_questions(settings: Any, *, max_messages: int = 30) -> list[dict[str, Any]]:
    threads = fetch_gmail_threads(settings, max_messages=max_messages)
    filtered = filter_relevant_threads(threads)
    questions = normalize_questions(extract_questions(filtered))
    output = {
        "generated_at": _now_iso(),
        "questions": questions,
        "thread_count": len(filtered),
    }
    path = _customer_language_dir(settings) / f"customer_questions_{datetime.now(timezone.utc).date().isoformat()}.json"
    path.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    latest_path = _customer_language_dir(settings) / "latest.json"
    latest_path.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    return questions
