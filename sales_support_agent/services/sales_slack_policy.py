"""Which #sales posts are a status report, and which failure may post today.

Routine counts stay in the log. A failure kind posts at most once per
America/Denver day. Held replies, hot leads, and call prep are not failure
kinds, so they still post every time.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_DENVER = ZoneInfo("America/Denver")

# First match wins. More specific send failures come before the shared words.
_FAILURE_KINDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("scheduled_followup_failed", ("scheduled follow-up", "failed to send")),
    ("checkin_failed", ("60-day check-in", "failed to send")),
    ("nudge_failed", ("nudge", "failed to send")),
    ("reply_failed", ("reply to", "failed to send")),
    ("interest_unconfirmed", ("could not confirm interest",)),
    ("opt_out_not_saved", ("opt-out not saved",)),
    ("abby_daily_cap", ("daily cap",)),
)


def failure_kind(text: str) -> str | None:
    """The daily bucket for a failure alert, or None when the post is not one."""

    lowered = (text or "").lower()
    for kind, parts in _FAILURE_KINDS:
        if all(part in lowered for part in parts):
            return kind
    return None


def post_failure_once(
    engine: Any,
    post: Callable[[str], Any],
    *,
    kind: str,
    text: str,
    now: datetime | None = None,
) -> bool:
    """Post ``text`` once for this failure kind today. Later copies are logged."""

    from sales_support_agent.services.job_lease import claim_scheduled_job, finish_scheduled_job

    local = now.astimezone(_DENVER) if now and now.tzinfo else (now or datetime.now(_DENVER))
    if local.tzinfo is None:
        local = local.replace(tzinfo=_DENVER)
    else:
        local = local.astimezone(_DENVER)
    run_key = f"{local.date().isoformat()}:{kind}"[:128]
    try:
        lease = claim_scheduled_job(
            engine,
            job_key="sales_slack_failure",
            run_key=run_key,
            lease_minutes=36 * 60,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[sales-slack] could not claim %s", run_key)
        logger.error("[sales-slack] unposted failure %s: %s", kind, text)
        return False
    if lease is None:
        logger.info("[sales-slack] suppressed repeat %s: %s", kind, text)
        return False
    try:
        post(text)
    except Exception:  # noqa: BLE001
        logger.exception("[sales-slack] post failed for %s", kind)
        try:
            finish_scheduled_job(engine, lease, status="failed", details={"text": text[:500]})
        except Exception:  # noqa: BLE001
            logger.exception("[sales-slack] could not record failed post %s", kind)
        return False
    try:
        finish_scheduled_job(engine, lease, status="succeeded", details={"text": text[:500]})
    except Exception:  # noqa: BLE001
        logger.exception("[sales-slack] could not record posted %s", kind)
    return True
