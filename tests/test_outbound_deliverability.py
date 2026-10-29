"""Bounce events, suppression sync, and the webhook hook."""

from datetime import datetime, timezone

from sqlalchemy import create_engine, text

import outbound_bounces as bounces
import outbound_suppression as suppression
from sales_support_agent.services.instantly_webhooks import InstantlyWebhookService


def engine():
    return create_engine("sqlite://")


class _R:
    def __init__(self, code, data):
        self.status_code, self._d = code, data

    def json(self):
        return self._d


def test_blocklist_sync_stores_email_and_domain_and_resumes(monkeypatch):
    monkeypatch.setenv("INSTANTLY_API_KEY", "k" * 20)
    eng = engine()
    pages = [
        _R(200, {"items": [
            {"bl_value": "OptOut.com", "is_domain": True},
            {"bl_value": "Person@Brand.com", "is_domain": False},
        ], "next_starting_after": "cursor-1"}),
        _R(200, {"items": [{"bl_value": "shop.russh.com", "is_domain": True}], "next_starting_after": ""}),
    ]

    def get(url, **kw):
        return pages.pop(0)

    first = suppression.sync_blocklist(eng, get=get, max_pages=1)
    assert first["pages"] == 1 and first["synced"] == 2
    emails, domains = suppression.lists(eng)
    assert "optout.com" in domains and "person@brand.com" in emails
    assert suppression.get_cursor(eng, "instantly_blocklist") == "cursor-1"
    suppression.sync_blocklist(eng, get=get, max_pages=1)
    _, domains = suppression.lists(eng)
    assert "russh.com" in domains
    # The last page has no next cursor. Keep cursor-1. Do not save "".
    assert suppression.get_cursor(eng, "instantly_blocklist") == "cursor-1"


def test_push_match_covers_a_store_host():
    emails, domains = {"gone@brand.com"}, {"russh.com"}
    assert suppression.matched_value("gone@brand.com", "brand.com", emails, domains) == "gone@brand.com"
    assert suppression.matched_value("a@shop.russh.com", "shop.russh.com", set(), domains) == "russh.com"
    assert suppression.matched_value("a@mybasin.com", "mybasin.com", set(), {"basin.com"}) == ""


def test_webhook_bounce_is_one_row_and_suppresses_the_email():
    eng = engine()
    payload = {
        "event_type": "email_bounced", "lead_email": "A@Brand.com", "campaign_id": "camp",
        "timestamp": "2026-10-28T18:00:00Z", "email_id": "m1",
    }
    assert bounces.record_webhook(eng, payload)["event_type"] == "hard_bounce"
    bounces.record_webhook(eng, payload)
    report = bounces.bounce_report(eng, days=3)
    assert report["by_campaign"][0]["bounces"] == 1
    assert report["by_campaign"][0]["bounce_rate"] is None
    emails, _ = suppression.lists(eng)
    assert "a@brand.com" in emails


def test_send_and_bounce_rate_is_per_day_and_campaign():
    eng = engine()
    when = datetime(2026, 10, 28, 18, 0, tzinfo=timezone.utc)
    for n in range(4):
        bounces.record_event(eng, email=f"a{n}@b.com", campaign_id="camp", event_type="sent",
                             occurred_at=when, source="instantly_webhook", detail="",
                             external_key=f"sent-{n}")
    bounces.record_event(eng, email="a0@b.com", campaign_id="camp", event_type="hard_bounce",
                         occurred_at=when, source="instantly_webhook", detail="",
                         external_key="hard_bounce|camp|a0@b.com")
    bounces.record_event(eng, email="a1@b.com", campaign_id="other", event_type="sent",
                         occurred_at=when, source="instantly_webhook", detail="",
                         external_key="sent-other")
    report = bounces.bounce_report(eng, days=3)
    by_campaign = {row["campaign_id"]: row for row in report["by_campaign"]}
    assert by_campaign["camp"]["sent"] == 4
    assert by_campaign["camp"]["bounces"] == 1
    assert by_campaign["camp"]["bounce_rate"] == 25.0
    assert report["by_day"][0]["sent"] == 5
    assert report["by_day"][0]["bounces"] == 1


def test_soft_bounce_is_kept_and_not_suppressed():
    eng = engine()
    out = bounces.record_webhook(eng, {
        "event_type": "email_bounced", "lead_email": "a@b.com", "campaign_id": "camp",
        "timestamp": "2026-10-28T18:00:00Z", "email_id": "m2", "bounce_type": "soft",
    })
    assert out["event_type"] == "soft_bounce"
    assert suppression.lists(eng) == (set(), set())


def test_poll_records_a_bounced_lead_once():
    eng = engine()
    payload = {"items": [{
        "id": "lead-1", "email": "a@b.com", "status": -1, "campaign": "camp",
        "timestamp_updated": "2026-10-28T18:30:00Z",
    }], "next_starting_after": ""}

    def post(url, **kw):
        assert kw["json"]["filter"] in {"FILTER_VAL_BOUNCED", "FILTER_VAL_UNSUBSCRIBED"}
        if kw["json"]["filter"] == "FILTER_VAL_BOUNCED":
            return _R(200, payload)
        return _R(200, {"items": [], "next_starting_after": ""})

    out = bounces.poll_lead_outcomes(eng, key="k" * 20, post=post, max_pages=1)
    assert out["bounces"] == 1
    again = bounces.poll_lead_outcomes(eng, key="k" * 20, post=post, max_pages=1)
    assert again["bounces"] == 1
    with eng.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM outbound_delivery_events")).scalar()
    assert count == 1


def test_webhook_replaces_a_poll_timestamp():
    eng = engine()
    bounces.record_event(eng, email="a@b.com", campaign_id="camp", event_type="hard_bounce",
                         occurred_at="2026-10-01 00:00:00", source="instantly_poll", detail="",
                         external_key="hard_bounce|camp|a@b.com")
    bounces.record_webhook(eng, {
        "event_type": "email_bounced", "lead_email": "a@b.com", "campaign_id": "camp",
        "timestamp": "2026-10-28T18:00:00Z", "email_id": "m1",
    })
    with eng.connect() as conn:
        row = conn.execute(text(
            "SELECT occurred_at, source FROM outbound_delivery_events")).fetchone()
    assert row[1] == "instantly_webhook" and row[0].startswith("2026-10-28")


def test_bounce_webhook_records_without_a_clickup_lead(monkeypatch):
    eng = engine()
    monkeypatch.setattr("sales_support_agent.models.database.get_engine", lambda: eng)

    class Settings:
        instantly_webhook_allowed_event_types = ()

    result = InstantlyWebhookService(Settings(), None, None, None).process_webhook({
        "event_type": "email_bounced", "lead_email": "a@brand.com", "campaign_id": "camp",
        "timestamp": "2026-10-28T18:00:00Z", "email_id": "m1",
    })
    assert result["status"] == "ignored"
    assert result["reason"] == "event_type_not_enabled"
    emails, _ = suppression.lists(eng)
    assert "a@brand.com" in emails
    assert bounces.bounce_report(eng)["by_campaign"][0]["bounces"] == 1
