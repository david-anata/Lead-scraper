"""Five-step cold copy for the four live signal campaigns.

These bodies are the source Instantly is patched from. A manual edit in
Instantly is overwritten the next time ``yes-cta`` applies, so the words
live here.

Proof lines use only figures already published in
``sales_support_agent/services/deck/data/case_studies.json`` or on the
Anata case studies page as of 2026-11-19. No new numbers.

Step 1 delay is the wait before the next email (Instantly's meaning).
Delays 3, 4, 4, 5 send on days 0, 3, 7, 11, and 16.
"""

from __future__ import annotations

import re
from html import unescape
from typing import Any, Optional

from outbound_cold_sequence import OPT_OUT_LINE, POSTAL_ADDRESS

# campaign key, Instantly campaign id. Names in Instantly still say "v2".
CAMPAIGNS: tuple[tuple[str, str], ...] = (
    ("amz_on", "8e0df427-1f18-43ac-b0c6-e8ea787123d7"),
    ("amz_off", "138a162d-6654-4916-9959-20c11e31214b"),
    ("plan_upgrade", "4dc10f79-f7c9-43c5-9c3e-5e28e9cf4c72"),
    ("replatform", "d1c197e3-bf1f-4f41-bcb9-e30e73975a2c"),
)

# Wait before the next email, in days. The last step has no next email.
STEP_DELAYS: tuple[int, ...] = (3, 4, 4, 5, 0)
SEND_DAYS: tuple[int, ...] = (0, 3, 7, 11, 16)

ASK = "Can I send it to you?"
STEP1_SUBJECTS = (
    "quick question",
    "idea for {{companyName | your brand}}",
)
SIGNATURE = "Abby\nAccount Specialist, Anata"

# Worst-case opener the word budget must still clear. The report also
# quotes the midpoint of the 12 to 24 word opener range.
OPENER_MAX_WORDS = 24
OPENER_AVG_WORDS = 18
STEP1_WORD_LIMIT = 80
FOLLOWUP_MIN_WORDS = 25
FOLLOWUP_MAX_WORDS = 60

_TAG_RE = re.compile(r"<[^>]+>")
_LIQUID_RE = re.compile(r"\{%\s*.*?%\}", re.S)
_VAR_RE = re.compile(r"\{\{\s*([A-Za-z0-9_]+)(?:\s*\|\s*([^}]+))?\s*\}\}")
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
_DASH_RE = re.compile(r"\u2014|\u2013| - ")
_BANNED_RE = re.compile(
    r"reply\s+with|book(?:ing)?\s+(?:a\s+)?(?:call|time)|grab a time|"
    r"\bcalendar\b|worth a (?:quick )?intro|\bhop on\b|quick call",
    re.I,
)

# Pitch only. The greeting, opener, ask, and signature are wrapped on.
_PITCHES: dict[str, tuple[str, str]] = {
    "amz_on": (
        "{{companyName | your brand}} already sells on Amazon. I drafted a short plan for the product searches and the listings.",
        "{{companyName | your brand}} is already selling on Amazon. I drafted a short plan for the ads and the listings around the current products.",
    ),
    "amz_off": (
        "{{companyName | your brand}} is not on Amazon yet. I drafted a short plan for a first launch, using the products already on the site.",
        "{{companyName | your brand}} sells on its own site, and not on Amazon yet. I drafted what the first listings and ads would take.",
    ),
    "plan_upgrade": (
        "{{companyName | your brand}} recently upgraded its Shopify plan. I drafted a short Amazon plan that sits next to that change.",
        "{{companyName | your brand}} moved up a Shopify plan. I drafted a short Amazon plan from the products already on the store.",
    ),
    "replatform": (
        "{{companyName | your brand}} recently switched store platforms. I drafted a short Amazon plan to line up with the new catalog.",
        "{{companyName | your brand}} just moved to a new store platform. I drafted a short Amazon plan to set up alongside that move.",
    ),
}

# Follow-ups, two variants each: proof, Amazon opportunity, less effort, breakup.
_FOLLOWUPS: dict[str, tuple[tuple[str, str], ...]] = {
    "amz_on": (
        (
            "Zantrex grew revenue 40% within 30 days on our platform. That was one account. I can put a short plan together for {{companyName | your brand}}.",
            "Shadyvue lowered Amazon ad cost from 49.0% of sales to 40.4%, with 61% more search terms converting. I can put a short plan together for {{companyName | your brand}}.",
        ),
        (
            "I can list the Amazon searches that name the products and set them next to {{companyName | your brand}}'s listings.",
            "For a brand already on Amazon, the useful next page is the search that names the product. I can map those for {{companyName | your brand}}.",
        ),
        (
            "You do not have to rebuild the account for this. We draft the plan, and you review it before anything changes.",
            "We write the Amazon plan. {{companyName | your brand}} reads it. Nothing is changed on the account until you want that.",
        ),
        (
            "Last note from me. If Amazon work is not a priority for {{companyName | your brand}} right now, I will not write again.",
            "I will close this out here. I can still send the short plan for {{companyName | your brand}} if it is useful, and I will not follow up after this.",
        ),
    ),
    "amz_off": (
        (
            "Ocean Rx went from a fresh start on Amazon to the top 5 in its category. I can put a short plan together for {{companyName | your brand}}.",
            "Ocean Rx went from 27 ad clicks in May to 300 in October, starting from its first recorded advertising campaign. I can put a short plan together for {{companyName | your brand}}.",
        ),
        (
            "The first Amazon listings can use the products {{companyName | your brand}} already sells on its site. I can map that before any launch spend.",
            "A first launch only needs the product names, the offer, and a starting ad plan. I can draft those for {{companyName | your brand}}.",
        ),
        (
            "You do not have to hire someone to launch this. We draft the listings and the first ads, and you review the plan.",
            "The launch draft is on us. {{companyName | your brand}} reads the plan and decides whether to take the next step.",
        ),
        (
            "Last note from me. If a launch on Amazon is not a priority for {{companyName | your brand}}, I will not write again.",
            "I will leave it here. I can still send the launch plan for {{companyName | your brand}} if you want it, and I will not follow up after this.",
        ),
    ),
    "plan_upgrade": (
        (
            "SeroVital's Amazon sales moved from $12K a day to $33K a day, with margins preserved. I can put a short plan together for {{companyName | your brand}}.",
            "Sonrei, a skincare brand, grew 137% year over year, with total ad cost at 14% of sales. I can put a short plan together for {{companyName | your brand}}.",
        ),
        (
            "The plan upgrade is a natural moment to look at Amazon. I can show how {{companyName | your brand}}'s current products would read as listings.",
            "I can take the products on the upgraded Shopify store and draft Amazon titles for {{companyName | your brand}}.",
        ),
        (
            "No new hire for this. We draft the Amazon plan from the store you already run, and you review it.",
            "Your team keeps running Shopify. We write the Amazon plan so that work does not land on them.",
        ),
        (
            "Last note from me. If Amazon is not part of this plan upgrade for {{companyName | your brand}}, I will not write again.",
            "I will leave it here. I can still send the Amazon plan for {{companyName | your brand}} if the upgrade should include it, and I will not follow up after this.",
        ),
    ),
    "replatform": (
        (
            "Harvest and Home grew Amazon revenue 104% year over year while it added a new product line. I can put a short plan together for {{companyName | your brand}}.",
            "Harvest and Home grew the units ordered on Amazon 157% year over year as well. I can put a short plan together for {{companyName | your brand}}.",
        ),
        (
            "The new catalog is a clean list to match with Amazon. I can line {{companyName | your brand}}'s product names up with those searches.",
            "I can draft Amazon listings from the new store catalog, using the product names already on {{companyName | your brand}}.",
        ),
        (
            "{{companyName | your brand}} just changed platforms. We draft the Amazon plan so the team is not writing that as well.",
            "You review one plan. We draft the listings and the first ad setup after you say the plan is worth it.",
        ),
        (
            "Last note from me. If Amazon is not part of the platform move for {{companyName | your brand}}, I will not write again.",
            "I will leave it here. I can still send the Amazon plan for {{companyName | your brand}} if the new store should include it, and I will not follow up after this.",
        ),
    ),
}


def send_days(delays: tuple[int, ...] = STEP_DELAYS) -> tuple[int, ...]:
    """Calendar day of each step when ``delay`` waits before the next email."""

    day = 0
    days = [0]
    for delay in delays[:-1]:
        day += int(delay)
        days.append(day)
    return tuple(days)


def _wrap(middle: str, *, opener: bool) -> str:
    parts = ["Hi {{firstName | there}},", ""]
    if opener:
        parts.extend(["{{personalization}}", ""])
    parts.extend([middle.strip(), "", ASK, "", SIGNATURE, "", OPT_OUT_LINE, POSTAL_ADDRESS])
    return "\n".join(parts)


def plain_emails(campaign: str) -> tuple[tuple[str, str], ...]:
    """Five steps, two plain-text variants each. Step 1 includes the opener line."""

    pitches = _PITCHES[campaign]
    followups = _FOLLOWUPS[campaign]
    steps = [(_wrap(pitches[0], opener=True), _wrap(pitches[1], opener=True))]
    for pair in followups:
        steps.append((_wrap(pair[0], opener=False), _wrap(pair[1], opener=False)))
    return tuple(steps)


def _line_html(line: str) -> str:
    if line.strip():
        return f"<div>{line}</div>"
    return "<div><br /></div>"


def to_html(text: str, *, opener: bool) -> str:
    """Instantly HTML. The personalization block drops out without breaking tags."""

    lines = text.split("\n")
    if not opener:
        return "".join(_line_html(line) for line in lines)
    parts: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.strip() == "{{personalization}}":
            parts.append("<div>{% if personalization %}{{personalization}}</div>")
            if index + 1 < len(lines) and not lines[index + 1].strip():
                parts.append("<div><br /></div>")
                index += 1
            index += 1
            nxt = lines[index] if index < len(lines) else ""
            parts.append(f"<div>{{% endif %}}{nxt}</div>" if nxt.strip() else "<div>{% endif %}<br /></div>")
            index += 1
            continue
        parts.append(_line_html(line))
        index += 1
    return "".join(parts)


def steps_for(campaign: str) -> list[dict[str, Any]]:
    """Instantly sequence steps for one signal campaign."""

    emails = plain_emails(campaign)
    built: list[dict[str, Any]] = []
    for index, variants in enumerate(emails):
        subjects = STEP1_SUBJECTS if index == 0 else ("", "")
        built.append({
            "type": "email",
            "delay": STEP_DELAYS[index],
            "delay_unit": "days",
            "variants": [
                {"subject": subjects[variant_index], "body": to_html(body, opener=index == 0)}
                for variant_index, body in enumerate(variants)
            ],
        })
    return built


def steps_for_campaign_id(campaign_id: str) -> Optional[list[dict[str, Any]]]:
    """Steps for a live signal campaign id, or None when this id is not one."""

    for key, live_id in CAMPAIGNS:
        if live_id == campaign_id:
            return steps_for(key)
    return None


def _fill(text: str, *, opener_words: int) -> str:
    """Render fallbacks, and stand in for the opener with ``opener_words`` tokens."""

    without_tags = _LIQUID_RE.sub("", text or "")
    opener = " ".join(["opener"] * opener_words)
    filled = without_tags.replace("{{personalization}}", opener)

    def replace(match: re.Match[str]) -> str:
        fallback = (match.group(2) or "").strip()
        return fallback or match.group(1)

    return unescape(_VAR_RE.sub(replace, filled))


def visible_text(value: str) -> str:
    """Tags off, liquid off, fallbacks in. Used by the word and CTA checks."""

    return _fill(unescape(_TAG_RE.sub(" ", value or "")), opener_words=0)


def word_count(text: str, *, opener_words: int, include_footer: bool = False) -> int:
    """Words after fallbacks render. The CAN-SPAM footer is not part of the limit."""

    rendered = _fill(unescape(_TAG_RE.sub(" ", text or "")), opener_words=opener_words)
    if not include_footer:
        rendered = rendered.replace(OPT_OUT_LINE, " ").replace(POSTAL_ADDRESS, " ")
    return len(rendered.split())


def _bodies(step: dict[str, Any]) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    for variant in step.get("variants") or []:
        if isinstance(variant, dict):
            found.append((str(variant.get("subject") or ""), str(variant.get("body") or "")))
    return found


def validate_steps(steps: list[dict[str, Any]], *, campaign: str = "") -> list[str]:
    """Every rule a signal step must pass. Empty means the sequence is sendable."""

    label = campaign or "sequence"
    problems: list[str] = []
    if len(steps) != 5:
        problems.append(f"{label}: expected 5 steps, found {len(steps)}")
    if send_days(tuple(int(step.get("delay") or 0) for step in steps)) != SEND_DAYS:
        problems.append(f"{label}: delays must send on days {SEND_DAYS}")
    for index, step in enumerate(steps):
        bodies = _bodies(step)
        if len(bodies) != 2:
            problems.append(f"{label} step {index + 1}: expected 2 variants")
            continue
        for variant_index, (subject, body) in enumerate(bodies):
            where = f"{label} step {index + 1} variant {variant_index + 1}"
            raw = f"{subject}\n{body}"
            if _URL_RE.search(raw):
                problems.append(f"{where}: contains a URL")
            if _DASH_RE.search(raw):
                problems.append(f"{where}: contains a dash")
            if _BANNED_RE.search(visible_text(raw)):
                problems.append(f"{where}: contains a disallowed ask")
            shown = visible_text(body)
            if OPT_OUT_LINE not in body or POSTAL_ADDRESS not in body:
                problems.append(f"{where}: missing the CAN-SPAM footer")
            message = shown.replace(OPT_OUT_LINE, " ")
            if message.count("?") != 1 or ASK not in message:
                problems.append(f"{where}: the only ask must be '{ASK}'")
            if index == 0:
                if not subject.strip():
                    problems.append(f"{where}: step 1 needs a subject")
                if "{{personalization}}" not in body or "{{firstName | there}}" not in body:
                    problems.append(f"{where}: step 1 is missing a safe first name or opener")
                if "{{companyName | your brand}}" not in body:
                    problems.append(f"{where}: step 1 is missing a company fallback")
                count = word_count(body, opener_words=OPENER_MAX_WORDS)
                if count > STEP1_WORD_LIMIT:
                    problems.append(f"{where}: {count} words with a {OPENER_MAX_WORDS}-word opener")
            else:
                if subject.strip():
                    problems.append(f"{where}: follow-up subject must be empty so it stays in the thread")
                if "{{personalization}}" in body:
                    problems.append(f"{where}: follow-up repeats the opener")
                count = word_count(body, opener_words=0)
                if count < FOLLOWUP_MIN_WORDS or count > FOLLOWUP_MAX_WORDS:
                    problems.append(f"{where}: {count} words, limit is {FOLLOWUP_MIN_WORDS} to {FOLLOWUP_MAX_WORDS}")
    return problems


def require_valid(steps: list[dict[str, Any]], *, campaign: str = "") -> None:
    """Refuse to build a signal sequence that breaks a copy rule."""

    problems = validate_steps(steps, campaign=campaign)
    if problems:
        raise ValueError(problems[0])


def audit_rows() -> list[dict[str, Any]]:
    """Word counts for the operator report. Step 1 includes an average opener."""

    rows: list[dict[str, Any]] = []
    for key, _campaign_id in CAMPAIGNS:
        for index, variants in enumerate(plain_emails(key)):
            for variant_index, body in enumerate(variants):
                if index == 0:
                    words = word_count(body, opener_words=OPENER_AVG_WORDS)
                    ceiling = word_count(body, opener_words=OPENER_MAX_WORDS)
                else:
                    words = word_count(body, opener_words=0)
                    ceiling = words
                rows.append({
                    "campaign": key,
                    "step": index + 1,
                    "variant": variant_index + 1,
                    "words": words,
                    "words_at_max_opener": ceiling,
                    "text": body,
                })
    return rows


def _check_module_copy() -> None:
    if send_days() != SEND_DAYS:
        raise ValueError("Signal delays do not land on days 0, 3, 7, 11, 16.")
    for key, _campaign_id in CAMPAIGNS:
        require_valid(steps_for(key), campaign=key)


_check_module_copy()
