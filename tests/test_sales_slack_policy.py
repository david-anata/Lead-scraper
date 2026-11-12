"""#sales keeps actionable posts and posts each failure kind once a day."""

from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine

from sales_support_agent.services.job_lease import ensure_job_lease_schema
from sales_support_agent.services.sales_slack_policy import failure_kind, post_failure_once

NOW = datetime(2026, 11, 12, 15, 0, tzinfo=ZoneInfo("UTC"))


def test_failure_kinds_leave_hot_leads_and_holds_alone():
    assert failure_kind(
        "Abby's scheduled follow-up to tina@silhouetteoutdoor.net failed to send (0)."
    ) == "scheduled_followup_failed"
    assert failure_kind("Abby's nudge 2 to ann@x.com failed to send (0).") == "nudge_failed"
    assert failure_kind("Abby's reply to ann@x.com failed to send (0). Nothing went out.") == "reply_failed"
    assert failure_kind("Abby hit her daily cap. Scheduled follow-ups are waiting.") == "abby_daily_cap"
    assert failure_kind("Hostile or legal reply from ann@x.com. The lead is blocked. Nothing sent.") is None
    assert failure_kind("Positive reply from ann@x.com (interested). Abby replied with the booking link.") is None
    assert failure_kind("Abby is not sure how to answer ann@x.com (question, confidence 0.40). Nothing sent.") is None


def test_the_same_failure_kind_posts_once_per_denver_day():
    engine = create_engine("sqlite://")
    ensure_job_lease_schema(engine)
    posted = []
    assert post_failure_once(
        engine, posted.append, kind="scheduled_followup_failed",
        text="Abby's scheduled follow-up to tina@x.net failed to send (0).", now=NOW,
    )
    assert not post_failure_once(
        engine, posted.append, kind="scheduled_followup_failed",
        text="Abby's scheduled follow-up to other@x.net failed to send (0).", now=NOW,
    )
    later = datetime(2026, 11, 13, 15, 0, tzinfo=ZoneInfo("UTC"))
    assert post_failure_once(
        engine, posted.append, kind="scheduled_followup_failed",
        text="Abby's scheduled follow-up to next@x.net failed to send (0).", now=later,
    )
    assert [item.split(" to ")[1].split(" ")[0] for item in posted] == ["tina@x.net", "next@x.net"]
