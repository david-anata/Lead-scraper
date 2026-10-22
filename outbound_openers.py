"""One personalised opening line per contact (replaces Clay's AI column).

The line fills ``{{personalization}}`` in the Instantly sequence. It asks, it
does not assert, and it only uses facts we were given. If the model is missing,
errors, or writes something that breaks a rule, a plain deterministic line is
used instead, so a lead is never blocked on the AI.

Hard rules, enforced in code not just in the prompt:
  * No em or en dashes (David's house style): they become hyphens.
  * No digits, so a made-up number can never reach a prospect.
  * No links, no email addresses, no cliches.
  * One or two short sentences.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
MAX_WORDS = 34

BANNED_PHRASES = (
    "hope this finds you", "hope you're well", "hope you are well", "game-changing",
    "game changing", "revolutionary", "cutting-edge", "cutting edge", "synergy",
    "i came across your", "touch base", "circle back", "reaching out",
    # Template tics the model fell into on the first live run.
    "i noticed", "we noticed", "building something", "something special",
    "i'm curious", "we're curious", "curious how", "ideal customer profile", "icp",
)

_SYSTEM = (
    "You write the first line of a cold email from Abby at Anata to a founder or "
    "marketing lead at a consumer brand. Anata Intelligence builds a brand's Amazon "
    "strategy and runs the ad actions automatically. Write ONE or TWO short sentences "
    "(under 30 words total) that reference something concrete about this brand from the "
    "facts given (what they sell, their category, or the signal), then ask one light, "
    "specific question about Amazon. Vary the structure: do not start with 'I noticed', "
    "'We noticed' or the brand name followed by 'is building'. Never use the words "
    "'curious', 'special', 'building something', or 'ideal customer profile'. Use only "
    "the facts provided; if a fact sounds like an internal note about why we chose them, "
    "do not repeat it. Never state a number, statistic, or result. No links. No em "
    "dashes. No greeting, no sign-off. Plain, warm, founder-to-founder."
)


def clean(text: str) -> str:
    """Normalise punctuation and whitespace the way our emails require."""
    t = (text or "").strip().strip('"').strip()
    t = t.replace("—", " - ").replace("–", " - ").replace("--", " - ")
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s+-\s+", " - ", t)
    return t


def validate(text: str) -> tuple[bool, str]:
    t = clean(text)
    if not t:
        return False, "empty"
    if len(t.split()) > MAX_WORDS:
        return False, "too long"
    if re.search(r"\d", t):
        return False, "contains a number"
    if re.search(r"https?://|www\.|@", t):
        return False, "contains a link or address"
    low = t.lower()
    for phrase in BANNED_PHRASES:
        if phrase in low:
            return False, f"cliche: {phrase}"
    return True, ""


def fallback_line(brand: str, niche: str = "", reason: str = "") -> str:
    """Deterministic opener. Honest, specific only to what we know. ``reason`` is
    an internal note and is deliberately never shown to a prospect."""
    name = (brand or "your brand").strip()
    if niche:
        return clean(f"{name} stood out while I was looking at {niche} brands. Is Amazon a real growth channel for you yet?")
    return clean(f"{name} stood out to me. Is Amazon a real growth channel for you yet?")


def _build_prompt(brand: str, niche: str, reason: str, categories: str, title: str) -> str:
    facts = [f"Brand: {brand}"]
    if niche:
        facts.append(f"Niche: {niche}")
    if categories:
        facts.append(f"Categories: {categories}")
    if reason:
        facts.append(f"Why they are on our list: {reason}")
    if title:
        facts.append(f"Recipient role: {title}")
    return "\n".join(facts) + "\n\nWrite the opening line now."


def _call_anthropic(prompt: str, api_key: str) -> str:
    import anthropic  # imported lazily so tests and fallbacks never need it

    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model=MODEL, max_tokens=120, system=_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )
    return "".join(getattr(b, "text", "") for b in msg.content)


def write_opener(
    lead: dict[str, Any],
    contact: Optional[dict[str, Any]] = None,
    *,
    llm: Optional[Callable[[str], str]] = None,
    api_key: Optional[str] = None,
) -> tuple[str, str]:
    """Return (line, source) where source is "ai" or "fallback"."""
    brand = str(lead.get("brand") or lead.get("domain") or "").strip()
    niche = str(lead.get("niche") or "").strip()
    reason = str(lead.get("reason") or "").strip()
    cats = lead.get("categories") or ""
    if isinstance(cats, (list, tuple)):
        cats = ", ".join(str(c) for c in cats[:3])
    title = str((contact or {}).get("title") or "").strip()

    key = api_key if api_key is not None else os.getenv("ANTHROPIC_API_KEY", "")
    if llm is None and key:
        llm = lambda p: _call_anthropic(p, key)  # noqa: E731
    if llm is not None:
        prompt = _build_prompt(brand, niche, reason, str(cats), title)
        try:
            for _attempt in range(2):
                raw = llm(prompt)
                ok, why = validate(raw)
                if ok:
                    return clean(raw), "ai"
                logger.info("[outbound-openers] rejected AI line (%s)", why)
                prompt = (prompt + f"\n\nYour last line was rejected ({why}). "
                          "Write a different one that follows every rule.")
        except Exception:  # noqa: BLE001 - never block a lead on the AI
            logger.exception("[outbound-openers] AI call failed, using fallback")
    return fallback_line(brand, niche, reason), "fallback"
