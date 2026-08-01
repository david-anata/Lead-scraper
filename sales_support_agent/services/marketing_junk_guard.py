"""Reject automated form spam before it reaches the lead list.

Public marketing forms attract scripted submissions. On 2026-07-31 David
received an internal lead alert for a submission whose name was 24 random
letters (`jRYUmRZKmbAAdcTVxYKORSZX`), whose company was `Pmynutqga LLC`, whose
phone was `6490216433` (an exchange code starting with `0`, which the North
American numbering plan does not issue), and whose address was
`oroqe.n.u.z.94.5@gmail.com` — one Gmail mailbox wearing five dots so the
per-email gate reads it as a stranger every time.

Nothing here blocks a submission on its own. A real lead trips zero signals; a
scripted one trips several. Suppression requires at least
`JUNK_SIGNAL_THRESHOLD` independent signals so an unusual-but-real person is
never silently dropped.
"""

from __future__ import annotations

import re

# Two independent signals. One alone is never enough: a real person can have a
# name our pronounceability test dislikes, or a phone typo, but not both.
JUNK_SIGNAL_THRESHOLD = 2

_VOWELS = set("aeiouy")
_GMAIL_DOMAINS = {"gmail.com", "googlemail.com"}

# Legal suffixes carry no signal about whether the company name is real.
_LEGAL_SUFFIX_RE = re.compile(
    r"\b(?:llc|l\.l\.c\.?|inc|inc\.|incorporated|corp|corp\.|corporation|ltd|ltd\.|limited|co|co\.|gmbh|plc|llp|lp)\b",
    re.IGNORECASE,
)
_NON_LETTER_RE = re.compile(r"[^a-z]+")


def normalize_email_identity(email: str) -> str:
    """Collapse an address to the mailbox that actually receives it.

    Gmail ignores dots in the local part and everything after a `+`, so
    `oroqe.n.u.z.94.5@gmail.com` and `oroqenuz945@gmail.com` are one inbox.
    Treating them as one identity is what makes a per-email limit mean
    anything. Other providers are left alone: dots are significant there.
    """
    cleaned = (email or "").strip().lower()
    if "@" not in cleaned:
        return cleaned
    local, _, domain = cleaned.rpartition("@")
    if domain in _GMAIL_DOMAINS:
        local = local.split("+", 1)[0].replace(".", "")
        domain = "gmail.com"
    else:
        local = local.split("+", 1)[0]
    return f"{local}@{domain}" if local else cleaned


def _looks_unpronounceable(raw: str) -> bool:
    """True when a word reads like keyboard output rather than language.

    Three independent tells, any one of which is enough for this single field:
    a consonant run no language sustains, a vowel famine, or case that flips
    mid-word the way a random generator produces and a keyboard does not.
    """
    for token in re.split(r"\s+", (raw or "").strip()):
        letters = _NON_LETTER_RE.sub("", token.lower())
        if len(letters) < 8:
            continue

        consonant_run = 0
        longest_run = 0
        vowels = 0
        for char in letters:
            if char in _VOWELS:
                vowels += 1
                consonant_run = 0
            else:
                consonant_run += 1
                longest_run = max(longest_run, consonant_run)
        # Six, not five: ordinary English compounds reach five ("Northshore",
        # "strengths") and must not be treated as noise.
        if longest_run >= 6:
            return True
        # Only for long words. Short English words genuinely starve of vowels
        # ("strengths" is nine letters and one vowel); a twelve-letter word
        # that does is not a word.
        if len(letters) >= 12 and vowels / len(letters) < 0.22:
            return True

        # Case transitions inside the word. "McDonald" and "O'Brien" have one;
        # "jRYUmRZKmbAAdcTVxYKORSZX" has many.
        alpha = [c for c in token if c.isalpha()]
        transitions = sum(
            1
            for prev, curr in zip(alpha, alpha[1:])
            if prev.isupper() != curr.isupper()
        )
        if transitions >= 4:
            return True
    return False


def _phone_is_implausible(raw: str) -> bool:
    """True when the digits cannot be a real North American number.

    Only applied to 10- and 11-digit input so international numbers, which we
    cannot validate, never count as a signal.
    """
    digits = re.sub(r"\D", "", raw or "")
    if digits.startswith("1") and len(digits) == 11:
        digits = digits[1:]
    if len(digits) != 10:
        return False
    area, exchange = digits[:3], digits[3:6]
    # NANP issues neither area nor central-office codes beginning 0 or 1.
    if area[0] in "01" or exchange[0] in "01":
        return True
    # A single repeated digit is a placeholder, not a number.
    return len(set(digits)) == 1


def _email_is_aliased(email: str) -> bool:
    """True when a Gmail local part is padded with dots to look like many people."""
    cleaned = (email or "").strip().lower()
    local, _, domain = cleaned.rpartition("@")
    if domain not in _GMAIL_DOMAINS:
        return False
    return local.split("+", 1)[0].count(".") >= 3


def junk_signals(*, email: str = "", qualification: dict | None = None) -> list[str]:
    """Name every automated-submission tell present. Empty means it reads real."""
    fields = qualification or {}
    signals: list[str] = []

    if _looks_unpronounceable(str(fields.get("name", "") or "")):
        signals.append("unpronounceable_name")

    company = _LEGAL_SUFFIX_RE.sub("", str(fields.get("company", "") or "")).strip()
    if _looks_unpronounceable(company):
        signals.append("unpronounceable_company")

    if _phone_is_implausible(str(fields.get("phone", "") or "")):
        signals.append("implausible_phone")

    if _email_is_aliased(email):
        signals.append("aliased_email")

    return signals


def is_automated_submission(*, email: str = "", qualification: dict | None = None) -> bool:
    """Whether to keep this submission out of the lead list and the alert inbox."""
    return len(junk_signals(email=email, qualification=qualification)) >= JUNK_SIGNAL_THRESHOLD
