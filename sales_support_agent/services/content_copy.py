"""Source-grounded native copy generation for Content Operations."""

from __future__ import annotations

import json
import os
import re
from typing import Any


CHANNELS = (
    "linkedin_personal",
    "linkedin_company",
    "youtube",
    "instagram",
    "x",
)
CTA_BY_CHANNEL = {
    "linkedin_personal": "What would you change first in your operation?",
    "linkedin_company": "Save this framework for your next operating review.",
    "youtube": "Subscribe for more practical ecommerce operating systems.",
    "instagram": "Save this and share it with the operator who owns the next step.",
    "x": "What are you seeing in your operation?",
}


def _clean(value: str, *, limit: int = 20_000) -> str:
    return " ".join(str(value or "").replace("\N{EM DASH}", "-").split())[:limit]


def _source_points(transcript: str) -> list[str]:
    cleaned = _clean(transcript)
    sentences = [
        item.strip(" \t\r\n.-")
        for item in re.split(r"(?<=[.!?])\s+", cleaned)
        if len(item.strip()) >= 24
    ]
    return sentences[:4] or ([cleaned[:500]] if cleaned else [])


def deterministic_native_bundle(*, title: str, transcript: str) -> dict[str, str]:
    """Create conservative publishable copy using only transcript language."""

    topic = _clean(title, limit=180) or "An operator lesson"
    points = _source_points(transcript)
    if not points:
        raise ValueError("A transcript is required to generate native content.")
    lead = points[0]
    detail = points[1] if len(points) > 1 else points[0]
    proof = points[2] if len(points) > 2 else detail
    return {
        "linkedin_personal": (
            f"{lead}\n\n"
            f"I keep coming back to this operating lesson: {detail}\n\n"
            f"The practical move is to make the decision visible, assign an owner, "
            f"and verify the result. {proof}\n\n"
            f"{CTA_BY_CHANNEL['linkedin_personal']}"
        ),
        "linkedin_company": (
            f"{topic}\n\n"
            f"{lead}\n\n"
            f"Anata's operating takeaway:\n"
            f"1. Make the problem measurable.\n"
            f"2. Give the next action one owner.\n"
            f"3. Verify what changed.\n\n"
            f"{detail}\n\n{CTA_BY_CHANNEL['linkedin_company']}"
        ),
        "youtube": (
            f"{topic}\n\n"
            f"In this episode, David and the Anata team break down {topic.lower()}.\n\n"
            f"What you will learn:\n"
            f"- {lead}\n"
            f"- {detail}\n"
            f"- {proof}\n\n"
            f"{CTA_BY_CHANNEL['youtube']}"
        ),
        "instagram": (
            f"{lead}\n\n"
            f"The operator takeaway: {detail}\n\n"
            f"Make it visible. Assign the owner. Verify the result.\n\n"
            f"{CTA_BY_CHANNEL['instagram']}\n\n"
            "#ecommerce #operations #leadership"
        ),
        "x": (
            f"{lead}\n\n{detail}\n\n{CTA_BY_CHANNEL['x']}"
        ),
    }


def _ai_native_bundle(*, title: str, transcript: str) -> dict[str, str] | None:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key or os.getenv(
        "CONTENT_COPY_AI_ENABLED", "true"
    ).strip().lower() not in {"1", "true", "yes", "on"}:
        return None
    try:
        import anthropic

        message = anthropic.Anthropic(api_key=api_key).messages.create(
            model=os.getenv(
                "CONTENT_COPY_MODEL", "claude-haiku-4-5-20251001"
            ).strip(),
            max_tokens=3000,
            temperature=0.2,
            system=(
                "You create source-grounded Anata channel-native copy. Return only "
                "valid JSON with exactly these keys: linkedin_personal, "
                "linkedin_company, youtube, instagram, x. Never invent a fact, "
                "quote, result, client, or story. Use no em dash. Apply the Six C's: "
                "native Channel behavior, earned Credibility, clear ecommerce "
                "operations Category, useful Content, measurable Calibration intent, "
                "and one appropriate Collection CTA. X is staging-only. Each channel "
                "must be structurally distinct and ready to publish, not a writing "
                "instruction."
            ),
            messages=[
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "title": _clean(title, limit=300),
                            "transcript": _clean(transcript, limit=18_000),
                            "required_ctas": CTA_BY_CHANNEL,
                        }
                    ),
                }
            ],
        )
        text = "".join(
            str(getattr(block, "text", "") or "")
            for block in list(message.content or [])
        ).strip()
        decoded = json.loads(text)
        result = {key: _clean(decoded.get(key, "")) for key in CHANNELS}
        if all(len(result[key]) >= 80 for key in CHANNELS):
            return result
    except Exception:
        return None
    return None


def generate_native_bundle(*, title: str, transcript: str) -> dict[str, str]:
    """Generate all channel treatments with a source-only deterministic fallback."""

    return _ai_native_bundle(
        title=title,
        transcript=transcript,
    ) or deterministic_native_bundle(title=title, transcript=transcript)


def native_copy_quality(
    *,
    channel: str,
    body: str,
    title: str,
    has_transcript: bool,
) -> dict[str, Any]:
    """Apply testable Six C requirements to generated copy."""

    normalized = _clean(body)
    cta = CTA_BY_CHANNEL[channel]
    six_cs = {
        "channel": channel in CHANNELS and len(normalized) >= 80,
        "credibility": has_transcript,
        "category": bool(_clean(title)),
        "content": len(normalized.split()) >= 12,
        "calibration": True,
        "collection": cta in body,
    }
    checks = {
        "no_em_dash": "\N{EM DASH}" not in body,
        "not_a_writing_instruction": not normalized.lower().startswith(
            ("build ", "turn ", "write ", "create ")
        ),
        "has_required_cta": cta in body,
        "has_transcript": has_transcript,
        "has_category": bool(_clean(title)),
    }
    return {
        "passed": all(six_cs.values()) and all(checks.values()),
        "six_cs": six_cs,
        "checks": checks,
        "required": list(checks),
    }
