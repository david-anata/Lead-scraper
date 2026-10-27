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


def test_copy_has_no_em_dashes_and_offer_matches_abby():
    for t in (re_.EMAIL_1, re_.EMAIL_2, re_.EMAIL_3):
        assert "—" not in t and "–" not in t
    assert "$99" in re_.EMAIL_1 and "14 days" in re_.EMAIL_1 and "$99" in abby.FACTS
