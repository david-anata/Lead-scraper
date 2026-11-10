"""Map the dialed Twilio number to a greeting context.

`LINE_CONTEXTS_JSON` is a JSON object. Keys are E.164 numbers plus a
`default` entry. Example (555 numbers are placeholders):

    {"default": {"id": "anata", "label": "Anata"},
     "+15550001111": {"id": "front_desk", "label": "Anata Building front desk"}}
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LineContext:
    id: str
    label: str
    greeting: str = ""
    source: str = ""

    def spoken_greeting(self) -> str:
        if self.greeting.strip():
            return self.greeting.strip()
        if self.label.strip().casefold() == "anata":
            return "Thanks for calling Anata. How can I help you today?"
        return f"Thanks for calling the {self.label.strip()}. How can I help you today?"


DEFAULT_LINE = LineContext(id="anata", label="Anata")


def normalize_phone(value: str) -> str:
    """Return E.164 for US-length numbers, or '' when the value is not a phone."""

    raw = (value or "").strip()
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if raw.startswith("+") and 8 <= len(digits) <= 15:
        return "+" + digits
    return ""


def parse_known_numbers(raw: str) -> frozenset[str]:
    found: set[str] = set()
    for part in (raw or "").split(","):
        number = normalize_phone(part)
        if number:
            found.add(number)
    return frozenset(found)


def parse_lines(raw: str) -> dict[str, LineContext]:
    """Parse the line map. Invalid JSON falls back to the default line."""

    lines = {"default": DEFAULT_LINE}
    text = (raw or "").strip()
    if not text:
        return lines
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        logger.warning("LINE_CONTEXTS_JSON is not valid JSON; using the default line.")
        return lines
    if not isinstance(payload, dict):
        logger.warning("LINE_CONTEXTS_JSON must be an object; using the default line.")
        return lines
    for key, value in payload.items():
        if not isinstance(value, dict):
            continue
        line = LineContext(
            id=str(value.get("id") or "anata").strip() or "anata",
            label=str(value.get("label") or "Anata").strip() or "Anata",
            greeting=str(value.get("greeting") or "").strip(),
            source=str(value.get("source") or "").strip(),
        )
        token = str(key).strip()
        if token.casefold() == "default":
            lines["default"] = line
            continue
        number = normalize_phone(token)
        if number:
            lines[number] = line
    return lines


def resolve_line(raw_json: str, dialed_number: str) -> LineContext:
    lines = parse_lines(raw_json)
    number = normalize_phone(dialed_number)
    if number and number in lines:
        return lines[number]
    return lines.get("default", DEFAULT_LINE)
