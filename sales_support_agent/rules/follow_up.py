"""Status-driven follow-up evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from sales_support_agent.config import StatusPolicy
from sales_support_agent.rules.business_days import add_business_days


@dataclass(frozen=True)
class FollowUpAssessment:
    state: str
    reason: str
    anchor_date: date
    recommended_next_action: str
    due_date: date | None = None
    overdue_date: date | None = None


def assess_status_follow_up(
    *,
    status: str,
    policy: StatusPolicy,
    created_at: datetime,
    as_of_date: date,
    meaningful_touch_at: datetime | None = None,
    next_follow_up_date: date | None = None,
    has_work_signal: bool = False,
) -> FollowUpAssessment | None:
    if policy.use_follow_up_date:
        if next_follow_up_date is None:
            return FollowUpAssessment(
                state="missing_next_step",
                reason="FOLLOW UP without a next follow-up date.",
                anchor_date=as_of_date,
                recommended_next_action="Set the next follow-up date and log the next step in ClickUp.",
            )

        overdue_date = add_business_days(next_follow_up_date, policy.overdue_days or 1)
        if as_of_date >= overdue_date:
            return FollowUpAssessment(
                state="overdue",
                reason=f"{status} is past its next follow-up date.",
                anchor_date=next_follow_up_date,
                recommended_next_action="Reach out now and update the ClickUp task with the result.",
                due_date=next_follow_up_date,
                overdue_date=overdue_date,
            )
        if as_of_date >= next_follow_up_date:
            return FollowUpAssessment(
                state="follow_up_due",
                reason=f"{status} is due for follow-up.",
                anchor_date=next_follow_up_date,
                recommended_next_action="Follow up today and log the result in ClickUp.",
                due_date=next_follow_up_date,
                overdue_date=overdue_date,
            )
        return None

    reference_date = meaningful_touch_at.date() if meaningful_touch_at else created_at.date()

    if not has_work_signal and policy.first_action_days is not None:
        first_action_date = add_business_days(reference_date, policy.first_action_days)
        if as_of_date >= first_action_date:
            state = "new_and_untouched"
            reason = f"{status} has no meaningful touch after creation."
            if policy.due_days is not None:
                due_date = add_business_days(reference_date, policy.due_days)
                if as_of_date >= due_date:
                    state = "follow_up_due"
                    reason = f"{status} has not been worked within the expected first-touch window."
                    if policy.overdue_days is not None:
                        overdue_date = add_business_days(reference_date, policy.overdue_days)
                        if as_of_date >= overdue_date:
                            return FollowUpAssessment(
                                state="overdue",
                                reason=f"{status} is overdue with no meaningful touch logged.",
                                anchor_date=reference_date,
                                recommended_next_action="Reach out immediately and record the first real touch in ClickUp.",
                                due_date=due_date,
                                overdue_date=overdue_date,
                            )
                    return FollowUpAssessment(
                        state=state,
                        reason=reason,
                        anchor_date=reference_date,
                        recommended_next_action="Make the first meaningful touch and log it in ClickUp.",
                        due_date=due_date,
                    )
            return FollowUpAssessment(
                state=state,
                reason=reason,
                anchor_date=reference_date,
                recommended_next_action="Work the lead and log the activity in ClickUp.",
                due_date=first_action_date,
            )

    if policy.due_days is None:
        return None

    due_date = add_business_days(reference_date, policy.due_days)
    if as_of_date >= due_date:
        overdue_date = add_business_days(reference_date, policy.overdue_days or policy.due_days)
        if policy.overdue_days is not None and as_of_date >= overdue_date:
            return FollowUpAssessment(
                state="overdue",
                reason=f"{status} is overdue based on the last meaningful touch.",
                anchor_date=reference_date,
                recommended_next_action="Follow up now and update the next action in ClickUp.",
                due_date=due_date,
                overdue_date=overdue_date,
            )
        return FollowUpAssessment(
            state="follow_up_due",
            reason=f"{status} is due for follow-up based on the last meaningful touch.",
            anchor_date=reference_date,
            recommended_next_action="Follow up today and log the outcome in ClickUp.",
            due_date=due_date,
            overdue_date=overdue_date,
        )
    return None

