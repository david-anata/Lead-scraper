"""The #sales cleanup deletes only this bot's outbound status posts, and only when asked."""

from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient
from types import SimpleNamespace

from sales_support_agent.api import outbound_enrich_job as job
from sales_support_agent.services.sales_slack_cleanup import (
    BOT_APP,
    BOT_USER,
    STATUS_HEADER,
    cleanup_outbound_status_posts,
    is_outbound_status_post,
)

NOW = datetime(2026, 11, 12, 16, 0, tzinfo=timezone.utc)


def _message(text: str, *, user: str = BOT_USER, bot_id: str = BOT_APP, ts: str = "100.1",
             thread_ts: str = "") -> dict:
    payload = {"type": "message", "user": user, "bot_id": bot_id, "text": text, "ts": ts}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    return payload


def test_only_the_exact_status_header_from_this_bot_matches():
    body = f"{STATUS_HEADER}\nCompanies checked: 30 | with a decision-maker: 0 | contacts: 0"
    assert is_outbound_status_post(_message(body))
    assert not is_outbound_status_post(_message("Daily Leads for 2026-11-12\nUnique companies: 150"))
    assert not is_outbound_status_post(_message(
        "Abby's scheduled follow-up to tina@silhouetteoutdoor.net failed to send (0)."
    ))
    assert not is_outbound_status_post(_message("Abby's nudge 2 to ann@x.com failed to send (0)."))
    assert not is_outbound_status_post(_message(
        "Hostile or legal reply from ann@x.com. The lead is blocked. Nothing sent."
    ))
    assert not is_outbound_status_post(_message(
        "Abby is not sure how to answer ann@x.com (question, confidence 0.40). Nothing sent."
    ))
    assert not is_outbound_status_post(_message(body, user="U016FLC82CQ"))
    assert not is_outbound_status_post(_message(body, bot_id="B0BKE7VN1MX"))
    assert not is_outbound_status_post(_message(body, user="", bot_id=""))
    assert not is_outbound_status_post(_message(f"See this:\n{STATUS_HEADER}"))
    reply = _message(body, ts="100.2", thread_ts="100.1")
    assert not is_outbound_status_post(reply)


def test_dry_run_lists_matches_and_does_not_delete():
    pages = [{
        "ok": True,
        "messages": [
            _message(f"{STATUS_HEADER}\nCompanies checked: 30", ts="200.1"),
            _message("Daily Leads for 2026-11-12\nUnique companies: 150", ts="200.2"),
            _message("Abby's scheduled follow-up to tina@x.net failed to send (0).", ts="200.3"),
            _message(f"{STATUS_HEADER}\nBounces today: 2", ts="200.4"),
        ],
    }]
    deleted = []

    def history(_token, _channel, _oldest, _cursor, _sleep):
        return pages.pop(0)

    result = cleanup_outbound_status_posts(
        token="xoxb-test", now=NOW, apply=False,
        history=history, delete=lambda *a, **k: deleted.append(a), sleep=lambda _s: None,
    )
    assert result["dry_run"] is True
    assert result["matched"] == 2
    assert result["deleted"] == 0
    assert deleted == []
    assert [item["ts"] for item in result["posts"]] == ["200.1", "200.4"]
    assert {item["text"] for item in result["posts"]} == {STATUS_HEADER}


def test_apply_deletes_only_the_matched_timestamps_and_paces():
    waits = []
    deleted = []

    def history(_token, _channel, _oldest, _cursor, _sleep):
        return {"ok": True, "messages": [
            _message(f"{STATUS_HEADER}\nCompanies checked: 30", ts="300.1"),
            _message("Daily Leads for 2026-11-10", ts="300.2"),
        ]}

    def delete(_token, channel, ts, _sleep):
        deleted.append((channel, ts))
        return {"ok": True}

    result = cleanup_outbound_status_posts(
        token="xoxb-test", now=NOW, apply=True,
        history=history, delete=delete, sleep=waits.append,
    )
    assert result["dry_run"] is False
    assert result["deleted"] == 1
    assert result["failed"] == 0
    assert deleted == [("CQRMXS339", "300.1")]
    assert waits == [1.1]


def test_route_requires_auth_and_post_without_apply_stays_a_dry_run(monkeypatch):
    monkeypatch.setenv("CRON_SECRET", "s" * 16)
    seen = {}

    def fake_cleanup(*, apply: bool):
        seen["apply"] = apply
        return {"dry_run": not apply, "matched": 0, "deleted": 0}

    monkeypatch.setattr(job, "run_clay_status_cleanup", fake_cleanup)
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="")
    app.include_router(job.router)
    client = TestClient(app)
    assert client.get("/api/jobs/outbound-enrich/clay-status-posts").status_code == 401
    denied = client.post("/api/jobs/outbound-enrich/clay-status-posts?apply=true")
    assert denied.status_code == 401
    headers = {"Authorization": "Bearer " + ("s" * 16)}
    listed = client.get("/api/jobs/outbound-enrich/clay-status-posts", headers=headers)
    assert listed.status_code == 200
    assert listed.json()["dry_run"] is True
    assert seen["apply"] is False
    posted = client.post("/api/jobs/outbound-enrich/clay-status-posts", headers=headers)
    assert posted.status_code == 200
    assert posted.json()["dry_run"] is True
    assert seen["apply"] is False
    applied = client.post(
        "/api/jobs/outbound-enrich/clay-status-posts?apply=true", headers=headers,
    )
    assert applied.status_code == 200
    assert applied.json()["dry_run"] is False
    assert seen["apply"] is True
