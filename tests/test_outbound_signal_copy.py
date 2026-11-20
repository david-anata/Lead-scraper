"""Signal-campaign copy rules, and the one-time Instantly apply."""

import re

import pytest

import abby
import outbound_reengage as reengage
import outbound_signal_copy as signal
import outbound_yes_cta as yes
from outbound_cold_sequence import cold_urls


def test_each_signal_campaign_has_five_steps_two_variants_and_passes_the_rules():
    assert signal.send_days() == (0, 3, 7, 11, 16)
    assert yes.APPLY_KEY == "yes-cta-v5"
    names = {name for name, _campaign_id in yes.LIVE_CAMPAIGNS}
    assert {"Trial Offer", "abby_reengage", "amz_on", "amz_off v2", "plan_upgrade", "replatform v2"} <= names
    for key, campaign_id in signal.CAMPAIGNS:
        steps = signal.steps_for(key)
        assert signal.validate_steps(steps, campaign=key) == []
        assert signal.steps_for_campaign_id(campaign_id) == steps
        assert [step["delay"] for step in steps] == [3, 4, 4, 5, 0]
        assert all(step["delay_unit"] == "days" and step["type"] == "email" for step in steps)
        for index, step in enumerate(steps):
            assert len(step["variants"]) == 2
            for variant in step["variants"]:
                raw = f"{variant['subject']}\n{variant['body']}"
                assert cold_urls(raw) == []
                assert "\u2014" not in raw and "\u2013" not in raw and " - " not in raw
                assert "http" not in raw.lower() and "www." not in raw.lower()
                visible = signal.visible_text(variant["body"])
                assert visible.replace("Not a fit? Just let me know and I won't follow up.", " ").count("?") == 1
                assert signal.ASK in visible
                assert "reply with" not in visible.lower()
                assert "book" not in visible.lower() and "calendar" not in visible.lower()
            if index == 0:
                assert step["variants"][0]["subject"] == "quick question"
                assert "{{companyName | your brand}}" in step["variants"][1]["subject"]
            else:
                assert [variant["subject"] for variant in step["variants"]] == ["", ""]


def test_step_one_stays_under_80_with_a_full_opener_and_followups_stay_in_range():
    for row in signal.audit_rows():
        assert "Not a fit? Just let me know and I won't follow up." in row["text"]
        assert "anata Inc, 1657 N. State St., Lehi, UT 84043" in row["text"]
        assert "http" not in row["text"].lower() and " - " not in row["text"]
        if row["step"] == 1:
            assert row["words_at_max_opener"] < 80
            assert row["words"] == signal.word_count(row["text"], opener_words=18)
            assert signal.word_count(row["text"], opener_words=24, include_footer=True) > row["words_at_max_opener"]
            assert "{{personalization}}" in row["text"]
            assert "{{firstName | there}}" in row["text"]
        else:
            assert 25 <= row["words"] <= 60
            assert "{{personalization}}" not in row["text"]


def test_a_missing_opener_drops_out_without_breaking_the_html():
    html = signal.steps_for("plan_upgrade")[0]["variants"][0]["body"]
    hidden = re.sub(r"\{%\s*if personalization\s*%\}.*?\{%\s*endif\s*%\}", "", html, flags=re.S)
    assert hidden.count("<div>") == hidden.count("</div>")
    assert "{{personalization}}" not in hidden
    assert "recently upgraded its Shopify plan" in hidden
    assert signal.ASK in hidden
    shown = html.replace("{% if personalization %}", "").replace("{% endif %}", "")
    assert shown.count("<div>") == shown.count("</div>")
    assert "{{personalization}}" in shown


def test_validators_reject_a_url_a_dash_a_long_step_and_a_second_ask():
    steps = signal.steps_for("amz_on")
    dirty = [dict(step) for step in steps]
    dirty[0] = dict(dirty[0])
    dirty[0]["variants"] = [dict(item) for item in dirty[0]["variants"]]
    dirty[0]["variants"][0]["body"] = dirty[0]["variants"][0]["body"] + "<div>See https://example.com</div>"
    assert any("URL" in item for item in signal.validate_steps(dirty, campaign="amz_on"))

    dashed = signal.steps_for("amz_off")
    dashed[1] = dict(dashed[1])
    dashed[1]["variants"] = [dict(item) for item in dashed[1]["variants"]]
    dashed[1]["variants"][0]["body"] = dashed[1]["variants"][0]["body"].replace("Ocean Rx", "Ocean Rx - the brand")
    assert any("dash" in item for item in signal.validate_steps(dashed, campaign="amz_off"))

    em = signal.steps_for("replatform")
    em[2] = dict(em[2])
    em[2]["variants"] = [dict(item) for item in em[2]["variants"]]
    em[2]["variants"][1]["subject"] = "a follow up"
    problems = signal.validate_steps(em, campaign="replatform")
    assert any("subject" in item for item in problems)

    long = signal.steps_for("plan_upgrade")
    long[3] = dict(long[3])
    long[3]["variants"] = [dict(item) for item in long[3]["variants"]]
    long[3]["variants"][0]["body"] = "<div>" + ("word " * 80) + signal.ASK + "</div>"
    assert any("words" in item for item in signal.validate_steps(long, campaign="plan_upgrade"))

    booked = signal.steps_for("amz_on")
    booked[4] = dict(booked[4])
    booked[4]["variants"] = [dict(item) for item in booked[4]["variants"]]
    booked[4]["variants"][1]["body"] = "<div>Can you reply with yes, or book a call?</div>"
    refused = signal.validate_steps(booked, campaign="amz_on")
    assert any("disallowed" in item or "only ask" in item for item in refused)
    with pytest.raises(ValueError):
        signal.require_valid(booked, campaign="amz_on")


def test_trial_offer_is_left_alone_and_reengage_keeps_its_offer_plus_the_footer():
    assert signal.steps_for_campaign_id("e5f5046b-07e4-41ad-a395-79cf8766ccd0") is None
    assert signal.steps_for_campaign_id(reengage.TARGET_CAMPAIGN_ID) is None
    assert "14 days" in reengage.EMAIL_1
    assert "Can I send it to you?" in reengage.EMAIL_1
    assert "e5f5046b-07e4-41ad-a395-79cf8766ccd0" in yes.SKIP_COPY_CAMPAIGN_IDS
    assert reengage.TARGET_CAMPAIGN_ID not in yes.SKIP_COPY_CAMPAIGN_IDS
    for text in (reengage.EMAIL_1, reengage.EMAIL_2, reengage.EMAIL_3):
        assert "Not a fit? Just let me know and I won't follow up." in text
        assert "anata Inc, 1657 N. State St., Lehi, UT 84043" in text
        assert "http" not in text.lower() and " - " not in text


def test_yes_still_sends_the_deck_and_the_new_ask_is_the_one_abby_recognizes():
    assert abby.explicit_yes("Yes")
    assert abby.explicit_yes("Sure, send it")
    decision = abby.deck_decision("positive", "ann@huppy.com", "Ann", "Huppy")
    assert decision.send_deck and decision.interest is None
    assert "book" not in decision.reply.lower()
    for row in signal.audit_rows():
        assert "can i send it to you?" in row["text"].lower()


class _Instantly:
    def __init__(self, steps):
        self.default_steps = steps
        self.by_id = {}
        self.reads = []
        self.patched = []
        self.paused = []

    def get_campaign(self, campaign_id):
        self.reads.append(campaign_id)
        steps = self.by_id.get(campaign_id, self.default_steps)
        return 200, {
            "id": campaign_id,
            "daily_limit": 40,
            "sequences": [{"steps": steps}],
        }

    def patch_campaign(self, campaign_id, body):
        self.patched.append((campaign_id, body))
        self.by_id[campaign_id] = body["sequences"][0]["steps"]
        return 200, {}

    def get_subsequence(self, subsequence_id):
        return 200, {"id": subsequence_id, "status": 1}

    def pause_subsequence(self, subsequence_id):
        self.paused.append(subsequence_id)
        return 200, {}


def test_apply_replaces_signal_steps_adds_the_reengage_footer_and_skips_trial():
    amz_on = "8e0df427-1f18-43ac-b0c6-e8ea787123d7"
    trial = "e5f5046b-07e4-41ad-a395-79cf8766ccd0"
    client = _Instantly([
        {"type": "email", "delay": 4, "variants": [
            {"subject": "old", "body": "<div>Open to a quick intro with David?</div>"},
            {"subject": "old b", "body": "<div>Open to a quick intro with David?</div>"},
        ]},
    ])
    report = yes.apply_yes_cta(client, campaigns=(
        ("amz_on", amz_on),
        ("Trial Offer", trial),
        ("abby_reengage", reengage.TARGET_CAMPAIGN_ID),
    ))
    assert trial not in client.reads
    assert [item["name"] for item in report["campaigns"] if item.get("skipped")] == ["Trial Offer"]
    assert len(client.patched) == 2
    patched_id, body = client.patched[0]
    assert patched_id == amz_on
    assert set(body) == {"sequences"}
    assert "daily_limit" not in body
    steps = body["sequences"][0]["steps"]
    assert len(steps) == 5
    assert [step["delay"] for step in steps] == [3, 4, 4, 5, 0]
    assert steps[1]["variants"][0]["subject"] == ""
    assert signal.ASK in steps[0]["variants"][0]["body"]
    assert "anata Inc, 1657 N. State St., Lehi, UT 84043" in steps[0]["variants"][0]["body"]
    assert "https://" not in steps[0]["variants"][0]["body"]
    assert report["campaigns"][0]["read_back"] == steps[0]["variants"][0]["body"]
    reengage_body = client.patched[1][1]
    assert set(reengage_body) == {"sequences"}
    kept = reengage_body["sequences"][0]["steps"][0]
    assert kept["delay"] == 4
    assert kept["variants"][0]["subject"] == "old"
    assert "Open to a quick intro with David?" in kept["variants"][0]["body"]
    assert "Not a fit? Just let me know and I won't follow up." in kept["variants"][0]["body"]
    assert client.paused == [yes.BOOKING_SUBSEQUENCE_ID]
    assert report["campaigns"][0]["patched"] is True

    again = _Instantly(steps)
    second = yes.apply_yes_cta(again, campaigns=(("amz_on", amz_on),))
    assert again.patched == []
    assert second["campaigns"][0]["patched"] is False
    assert second["campaigns"][0]["changed"] == 0
