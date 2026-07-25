"""Push sourced brands into Clay (docs/outbound/14).

Replaces the manual CSV download and import. One brand per request, because
Clay's docs describe posting JSON and a row appearing but do NOT document
whether the body may be an array; guessing wrong there silently mangles rows.
Our caps (25 to 40 a pull) make the extra calls trivial. `supports_batch` exists
so we can switch after a live test proves batching works, not before.

Two rules this module exists to enforce:
  * The webhook address is a secret. Anyone holding it can write rows into the
    table and burn the 50,000 submission budget, so it is never logged, never
    echoed in an error, and never returned to the page.
  * A brand Clay did not accept is NOT counted as contacted, so it comes back
    on the next pull instead of being silently lost.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import requests

logger = logging.getLogger(__name__)

# Clay caps a webhook source at 50,000 submissions for the life of the source,
# and deleting rows does not give the budget back.
CLAY_SUBMISSION_CAP = 50_000
DEFAULT_RESERVE = 500          # refuse to push into the last slice of budget

# Fields that ride through to Clay. The recipe, reason and settings version are
# what let the scoreboard later answer "which pull earns its place".
CLAY_FIELDS = (
    "domain", "brand", "niche", "country", "tier", "score",
    "reason", "recipe", "estimated_sales_yearly_cents", "categories",
)


def _redact(text: str, secret: str) -> str:
    """Never let the webhook address or token reach a log or a page."""
    out = str(text)
    for value in (secret or "",):
        if value and len(value) > 8:
            out = out.replace(value, "<clay-webhook>")
    return out


def to_clay_row(lead: dict[str, Any], *, config_version: int = 0) -> dict[str, Any]:
    """Flatten one sourced brand into the row Clay's table receives."""
    row: dict[str, Any] = {}
    for key in CLAY_FIELDS:
        value = lead.get(key)
        if isinstance(value, (list, tuple)):
            value = ", ".join(str(v) for v in value)
        row[key] = value
    row["settings_version"] = config_version
    return row


@dataclass
class PushResult:
    attempted: int = 0
    accepted: int = 0
    rejected: int = 0
    accepted_domains: list[str] = field(default_factory=list)
    rejected_domains: list[str] = field(default_factory=list)
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.rejected == 0 and self.attempted > 0

    @property
    def summary(self) -> str:
        if not self.attempted:
            return self.reason or "Nothing to send."
        if self.rejected == 0:
            return f"Clay accepted all {self.accepted}."
        if self.accepted == 0:
            return (f"Clay rejected all {self.rejected}. They are still marked as "
                    "not contacted and will come back on the next pull.")
        return (f"Clay accepted {self.accepted}. {self.rejected} were rejected and "
                "will come back on the next pull.")


def post_one(
    webhook_url: str,
    row: dict[str, Any],
    *,
    token: str = "",
    timeout: int = 20,
    max_retries: int = 2,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[bool, str]:
    """POST a single row. Returns (accepted, reason). Never raises, never leaks
    the address into the reason string."""
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    last = "no attempt made"
    for attempt in range(max_retries + 1):
        try:
            resp = requests.post(webhook_url, json=row, headers=headers, timeout=timeout)
        except Exception as exc:  # noqa: BLE001 — a network blip is a rejection, not a crash
            last = _redact(f"could not reach Clay: {exc}", webhook_url)
            if attempt < max_retries:
                sleep(min(2 ** attempt, 8))
                continue
            return False, last
        if 200 <= resp.status_code < 300:
            return True, ""
        # 429 and 5xx are worth retrying; a 4xx means the row or address is wrong.
        retryable = resp.status_code == 429 or resp.status_code >= 500
        last = f"Clay returned {resp.status_code}"
        if retryable and attempt < max_retries:
            sleep(min(2 ** attempt, 8))
            continue
        return False, last
    return False, last


def push_leads(
    webhook_url: str,
    leads: list[dict[str, Any]],
    *,
    token: str = "",
    config_version: int = 0,
    used_submissions: int = 0,
    reserve: int = DEFAULT_RESERVE,
    pace_seconds: float = 0.3,
    post: Optional[Callable[..., tuple[bool, str]]] = None,
    sleep: Callable[[float], None] = time.sleep,
) -> PushResult:
    """Send each brand to Clay, one request per brand.

    Stops before the submission budget is exhausted rather than silently burning
    the last of it. Whatever is not accepted is reported so the caller can leave
    those brands un-contacted.
    """
    post = post or post_one
    result = PushResult()

    if not webhook_url:
        result.reason = "Clay is not connected yet."
        return result
    if not leads:
        result.reason = "Nothing to send."
        return result

    remaining = CLAY_SUBMISSION_CAP - int(used_submissions or 0) - int(reserve or 0)
    if remaining <= 0:
        result.reason = (
            "Clay's submission budget for this webhook is nearly used up, so nothing "
            "was sent. Create a new webhook source in Clay to keep going.")
        return result

    sendable = leads[:remaining]
    if len(sendable) < len(leads):
        result.reason = (f"Only {len(sendable)} of {len(leads)} were sent to stay inside "
                         "Clay's submission budget.")

    for i, lead in enumerate(sendable):
        if i and pace_seconds:
            sleep(pace_seconds)
        domain = str(lead.get("domain") or "").strip().lower()
        accepted, why = post(webhook_url, to_clay_row(lead, config_version=config_version),
                             token=token)
        result.attempted += 1
        if accepted:
            result.accepted += 1
            result.accepted_domains.append(domain)
        else:
            result.rejected += 1
            result.rejected_domains.append(domain)
            if why and not result.reason:
                result.reason = why

    if result.rejected:
        logger.warning("[outbound-clay] %s of %s rejected by Clay",
                       result.rejected, result.attempted)
    return result


def load_clay_config() -> tuple[str, str]:
    """The two values David sets on Render. Never hardcoded, never rendered back."""
    url = (os.getenv("CLAY_WEBHOOK_URL") or "").strip()
    token = (os.getenv("CLAY_WEBHOOK_TOKEN") or "").strip()
    return url, token


def budget_note(used: int) -> str:
    """Plain-English state of the 50,000 submission budget."""
    used = max(0, int(used or 0))
    left = max(0, CLAY_SUBMISSION_CAP - used)
    if left <= DEFAULT_RESERVE:
        return f"{used:,} of {CLAY_SUBMISSION_CAP:,} used. Budget effectively spent."
    if left < 5_000:
        return f"{used:,} of {CLAY_SUBMISSION_CAP:,} used. Getting low."
    return f"{used:,} of {CLAY_SUBMISSION_CAP:,} submissions used."
