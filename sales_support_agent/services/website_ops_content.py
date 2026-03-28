"""Content payload generation for Website Ops MVP."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


COMPETITOR_BRAND_BLOCKLIST = (
    "search atlas",
    "linkgraph",
    "perplexity",
    "gemini",
    "chatgpt",
)


def clean_generated_content(text: str) -> str:
    cleaned = str(text or "")
    for brand in COMPETITOR_BRAND_BLOCKLIST:
        cleaned = re.sub(re.escape(brand), "competitor", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    sentences = re.split(r"(?<=[.!?])\s+", cleaned)
    short_sentences = []
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        words = sentence.split()
        if len(words) > 24:
            sentence = " ".join(words[:24]).rstrip(",;:") + "."
        short_sentences.append(sentence)
    cleaned = " ".join(short_sentences).strip()
    if cleaned and not re.match(r"^(what|how|when|why|anata|this|these)\b", cleaned, flags=re.IGNORECASE):
        cleaned = "Anata helps with " + cleaned[0].lower() + cleaned[1:]
    return cleaned


def _service_label(page: Mapping[str, Any], blueprint: Mapping[str, Any]) -> str:
    query = str(blueprint.get("query", "")).strip()
    if query:
        return query.title()
    title = str(page.get("title", "") or "").strip()
    return title or "Service"


def _question_lookup(questions: list[Mapping[str, Any]], related_service: str) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in questions
        if not related_service or str(item.get("related_service", "")).strip() == related_service
    ][:4]


def build_faq_payload(
    *,
    page: Mapping[str, Any],
    blueprint: Mapping[str, Any],
    customer_questions: list[Mapping[str, Any]],
) -> dict[str, Any]:
    related_service = str(page.get("related_service", "")).strip()
    service_label = _service_label(page, blueprint)
    candidates = _question_lookup(customer_questions, related_service) or _question_lookup(customer_questions, "")
    faq_questions = []
    for item in candidates[:3]:
        question = str(item.get("question", "")).strip()
        answer = clean_generated_content(
            f"{service_label} should answer {question.rstrip('?').lower()} directly with operational clarity, implementation steps, and fit criteria."
        )
        if question and answer:
            faq_questions.append({"question": question, "answer": answer})
    if not faq_questions:
        for pattern in list(blueprint.get("faq_patterns") or [])[:3]:
            question = str(pattern.get("question", "") if isinstance(pattern, dict) else pattern).strip()
            if not question:
                continue
            faq_questions.append(
                {
                    "question": question,
                    "answer": clean_generated_content(
                        f"{service_label} should explain this directly, define the scope, and show when a brand should act."
                    ),
                }
            )
    if not faq_questions:
        fallback_questions = [
            f"What does {service_label} include?",
            f"How does {service_label} onboarding work?",
            f"When should a brand choose {service_label}?",
        ]
        for question in fallback_questions:
            faq_questions.append(
                {
                    "question": question,
                    "answer": clean_generated_content(
                        f"{service_label} should answer this directly with scope, onboarding steps, timing, and fit guidance."
                    ),
                }
            )
    return {
        "heading": f"{service_label} FAQ",
        "questions": faq_questions,
        "definitions": [
            clean_generated_content(f"{service_label} should be explained in direct commercial language."),
            clean_generated_content(f"{service_label} must show what is included, who it fits, and why it matters."),
        ],
    }


def build_section_expansion_payload(
    *,
    page: Mapping[str, Any],
    blueprint: Mapping[str, Any],
    customer_questions: list[Mapping[str, Any]],
) -> dict[str, Any]:
    service_label = _service_label(page, blueprint)
    section_title = "How onboarding works"
    gaps = list(blueprint.get("content_gaps") or [])
    if gaps:
        first_gap = str(gaps[0]).strip().rstrip(".")
        if first_gap:
            section_title = first_gap[0].upper() + first_gap[1:]
    question_hint = ""
    if customer_questions:
        question_hint = str(customer_questions[0].get("question", "")).strip()
    paragraphs = [
        clean_generated_content(
            f"{service_label} should include a section that answers {question_hint or 'how implementation works'} in direct, step-by-step language."
        ),
        clean_generated_content(
            f"This section should explain timing, responsibilities, and what a brand should expect after kickoff."
        ),
    ]
    return {
        "heading": section_title,
        "body_html": "".join(f"<p>{paragraph}</p>" for paragraph in paragraphs if paragraph),
    }


def save_content_tasks(settings: Any, tasks: list[Mapping[str, Any]]) -> None:
    root = Path(settings.website_ops_root) / "content_tasks"
    root.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tasks": list(tasks),
    }
    daily_path = root / f"content_tasks_{datetime.now(timezone.utc).date().isoformat()}.json"
    latest_path = root / "latest.json"
    daily_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    latest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
