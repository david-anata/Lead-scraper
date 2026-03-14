"""Daily email digest assembly."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, time, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import MailboxSignal
from sales_support_agent.services.notification_policy import STALE_URGENCY_LABELS, STALE_URGENCY_ORDER
from sales_support_agent.services.reminders import StaleDigestItem
from sales_support_agent.services.reply_templates import format_date_label, trim_for_slack


def digest_window(as_of_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(as_of_date, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def fetch_mailbox_signals(session: Session, *, as_of_date: date, max_items: int) -> list[MailboxSignal]:
    start, end = digest_window(as_of_date)
    query = (
        select(MailboxSignal)
        .where(MailboxSignal.received_at >= start, MailboxSignal.received_at < end)
        .order_by(MailboxSignal.received_at.desc())
        .limit(max_items)
    )
    return list(session.execute(query).scalars())


def build_daily_digest_subject(*, prefix: str, as_of_date: date) -> str:
    return f"{prefix} Daily digest for {as_of_date.isoformat()}"


def build_daily_digest_text(
    *,
    as_of_date: date,
    stale_items: list[StaleDigestItem],
    mailbox_signals: list[MailboxSignal],
    max_items: int,
) -> str:
    stale_visible = stale_items[:max_items]
    mailbox_visible = mailbox_signals[:max_items]
    stale_counts = Counter(item.urgency for item in stale_items)
    mailbox_counts = Counter(signal.urgency for signal in mailbox_signals)
    owner_counts = Counter(item.owner_label for item in stale_items)
    owner_counts.update(signal.owner_name or "Triage" for signal in mailbox_signals)

    lines = [
        f"SDR Support Daily Digest — {as_of_date.isoformat()}",
        "",
        "Summary",
        f"- Stale leads: {len(stale_items)}",
        f"- Mailbox findings: {len(mailbox_signals)}",
    ]
    for urgency in STALE_URGENCY_ORDER:
        total = stale_counts.get(urgency, 0) + mailbox_counts.get(urgency, 0)
        if total:
            lines.append(f"- {STALE_URGENCY_LABELS[urgency]}: {total}")
    if owner_counts:
        ranked_owners = sorted(owner_counts.items(), key=lambda item: (-item[1], item[0].lower()))
        lines.append("- By owner: " + ", ".join(f"{owner}: {count}" for owner, count in ranked_owners[:8]))

    if stale_items:
        lines.extend(["", "Stale lead follow-up"])
        for urgency in STALE_URGENCY_ORDER:
            urgency_items = [item for item in stale_visible if item.urgency == urgency]
            if not urgency_items:
                continue
            lines.append(f"{STALE_URGENCY_LABELS[urgency]}")
            for item in urgency_items:
                lines.extend(
                    [
                        f"- {item.owner_label} | {item.evaluation.lead.task_name} | {item.evaluation.lead.status} | {format_date_label(item.evaluation.assessment.anchor_date)}",
                        f"  Action: {item.action_summary}",
                        f"  Draft: {trim_for_slack(item.suggested_reply_draft, limit=280)}",
                        f"  Link: {item.evaluation.lead.task_url}",
                    ]
                )
        if len(stale_items) > len(stale_visible):
            lines.append(f"... {len(stale_items) - len(stale_visible)} more stale leads omitted")

    if mailbox_signals:
        lines.extend(["", "Mailbox findings"])
        grouped = {
            urgency: [signal for signal in mailbox_visible if signal.urgency == urgency]
            for urgency in STALE_URGENCY_ORDER
        }
        for urgency in STALE_URGENCY_ORDER:
            if not grouped[urgency]:
                continue
            lines.append(f"{STALE_URGENCY_LABELS[urgency]}")
            for signal in grouped[urgency]:
                owner = signal.owner_name or "Triage"
                lines.extend(
                    [
                        f"- {owner} | {signal.sender_email or signal.sender_domain} | {signal.subject or '(no subject)'} | {format_date_label(signal.received_at)}",
                        f"  Action: {signal.action_summary}",
                        f"  Draft: {trim_for_slack(signal.suggested_reply_draft, limit=280)}",
                        f"  Task: {signal.task_name or 'unmatched'} {signal.task_url or ''}".rstrip(),
                    ]
                )
        if len(mailbox_signals) > len(mailbox_visible):
            lines.append(f"... {len(mailbox_signals) - len(mailbox_visible)} more mailbox findings omitted")

    if not stale_items and not mailbox_signals:
        lines.extend(["", "No actionable SDR items were found today."])

    return "\n".join(lines).strip() + "\n"
