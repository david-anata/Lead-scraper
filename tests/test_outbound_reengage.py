import outbound_reengage as re_
import abby


LIVE = {"campaign_schedule": {"schedules": [{"name": "US", "timing": {"from": "07:00", "to": "17:00"},
                                             "days": {"1": True}, "timezone": "America/Boise"}]},
        "email_list": [f"a{i}@x.com" for i in range(18)], "email_gap": 5, "random_wait_max": 2}


def test_campaign_copies_live_inboxes_and_schedule():
    b = re_.campaign_body(LIVE)
    assert len(b["email_list"]) == 18 and b["daily_limit"] == 300
    assert b["campaign_schedule"]["schedules"][0]["timezone"] == "America/Boise"
    assert b["stop_on_reply"] and not b["link_tracking"] and not b["open_tracking"]


def test_three_steps_threaded_with_7_day_span():
    steps = re_.campaign_body(LIVE)["sequences"][0]["steps"]
    assert len(steps) == 3 and [s["delay"] for s in steps[:2]] == [3, 4]
    assert steps[1]["variants"][0]["subject"] == "" and steps[2]["variants"][0]["subject"] == ""


def test_copy_never_moves_and_only_no_reply():
    c = re_.copy_body("src", "dst")
    assert c["copy_leads"] is True and c["filter"] == "FILTER_VAL_COMPLETED_NO_REPLY"
    assert c["check_duplicates"] and c["skip_invalid_emails"]


def test_topup_keeps_only_completed_no_reply_and_does_not_send(monkeypatch):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    leads = [
        {"email": "done@brand.com", "status": 3, "campaign": "old", "first_name": "Done"},
        {"email": "done@brand.com", "status": 3, "campaign": "old"},
        {"email": "reply@brand.com", "status": 3, "email_reply_count": 1, "campaign": "old"},
        {"email": "nope@brand.com", "status": 3, "lt_interest_status": -1, "campaign": "old"},
        {"email": "gone@brand.com", "status": -2, "campaign": "old"},
        {"email": "live@brand.com", "status": 1, "campaign": "old"},
        {"email": "held@brand.com", "status": 3, "campaign": "old"},
        {"email": "again@brand.com", "status": 3, "campaign": re_.TARGET_CAMPAIGN_ID},
        {"email": "trial@brand.com", "status": 3, "campaign": "e5f5046b-07e4-41ad-a395-79cf8766ccd0"},
    ]
    chosen = re_.select_topup(
        leads,
        suppressed={"held@brand.com"},
        in_reengage=set(),
        active_elsewhere={"busy@brand.com"},
    )
    assert [row["email"] for row in chosen["eligible"]] == ["done@brand.com"]
    assert chosen["rejected"]["repeat"] == 1
    assert chosen["rejected"]["active thread"] == 1
    assert "negative or unsubscribed" in chosen["rejected"]
    monday = datetime(2026, 11, 2, 7, tzinfo=ZoneInfo("America/Denver"))
    monkeypatch.delenv("OUTBOUND_REENGAGE_TOPUP_ENABLED", raising=False)
    called = []
    dry = re_.run_weekly_topup(post=lambda *a, **k: called.append(1), now=monday)
    assert dry["dry_run"] is True and dry["added"] == 0 and called == []
    assert "not true" in dry["reason"]


def test_copy_has_no_em_dashes_and_offer_matches_abby():
    for t in (re_.EMAIL_1, re_.EMAIL_2, re_.EMAIL_3):
        assert "—" not in t and "–" not in t
    assert "$99" in re_.EMAIL_1 and "14 days" in re_.EMAIL_1 and "$99" in abby.FACTS
