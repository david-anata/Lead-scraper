from __future__ import annotations

from sqlalchemy import create_engine
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sales_support_agent.services import outbound_batches as batches
from sales_support_agent.services import outbound_memory
from sales_support_agent.services.outbound_delivery import deliver_daily_batch


def engine():
    value = create_engine("sqlite://", future=True)
    batches.ensure_tables(value)
    return value


def test_seeded_recipes_materialize_for_a_weekday():
    value = engine()
    assert len(batches.load_recipe_definitions(value)) == 6
    assert [recipe.key for recipe in batches.materialize_recipes(value, 0, {})] == ["icp_baseline"]


def test_custom_recipe_is_safe_and_versioned():
    value = engine()
    payload = {"key": "test_signal", "label": "Test signal", "template_key": "icp_baseline",
               "reason": "A specific timely signal", "tier": "B", "priority": "5",
               "weekdays": "0,2", "cap": "999", "active": "1", "include_in_daily": "1"}
    assert batches.save_recipe(value, payload, actor="qa@example.com")["ok"]
    saved = next(item for item in batches.load_recipe_definitions(value) if item["key"] == "test_signal")
    assert saved["cap"] == 150 and saved["version"] == 1
    assert batches.save_recipe(value, {**payload, "label": "Changed"}, actor="qa@example.com")["ok"]
    saved = next(item for item in batches.load_recipe_definitions(value) if item["key"] == "test_signal")
    assert saved["version"] == 2


def test_daily_batch_is_idempotent_and_freezes_deduped_csv():
    value = engine()
    batch_id, created = batches.create_batch(value, business_date="2026-08-21", trigger="scheduled", recipe_count=2, correlation_id="abc")
    same_id, created_again = batches.create_batch(value, business_date="2026-08-21", trigger="scheduled", recipe_count=2, correlation_id="def")
    assert created and not created_again and batch_id == same_id
    batch = batches.finalize_batch(value, batch_id=batch_id,
        recipe_runs=[{"recipe_key": "one", "recipe_label": "One", "status": "complete", "fresh": 1}, {"recipe_key": "two", "recipe_label": "Two", "status": "complete", "fresh": 1}],
        leads=[{"domain": "same.com", "brand": "Same", "recipe": "one"}, {"domain": "SAME.com", "brand": "Same", "recipe": "two"}],
        filename="anata-daily-leads-2026-08-21.csv")
    assert batch.unique_companies == 1 and batch.duplicates_removed == 1
    filename, csv = batches.batch_artifact(value, batch_id)
    assert filename == "anata-daily-leads-2026-08-21.csv" and csv.count("same.com") == 1
    assert batches.load_batch_leads(value, [batch_id])[0]["matched_recipes"] == ["one", "two"]


def test_batch_history_is_paginated():
    value = engine()
    for day in range(1, 13):
        batches.create_batch(value, business_date=f"2026-08-{day:02d}", trigger="scheduled", recipe_count=1, correlation_id=str(day))
    page, total = batches.load_batches(value, page=2, per_page=10)
    assert total == 12 and len(page) == 2


def test_daily_delivery_uses_one_exact_csv_for_email_and_slack():
    value = engine()
    outbound_memory.save_delivery_settings(value, {
        "enabled": "1", "email_enabled": "1", "slack_enabled": "1", "frequency": "daily",
        "email_recipients": "qa@example.com", "slack_channel": "CQA", "content_mode": "link",
    })
    batch_id, _ = batches.create_batch(value, business_date="2026-08-21", trigger="scheduled", recipe_count=1, correlation_id="x")
    batch = batches.finalize_batch(value, batch_id=batch_id,
        recipe_runs=[{"recipe_key": "one", "recipe_label": "One", "status": "complete", "fresh": 1}],
        leads=[{"domain": "exact.com", "brand": "Exact", "recipe": "one"}], filename="anata-daily-leads-2026-08-21.csv")
    send, slack = MagicMock(return_value=True), MagicMock()
    slack.upload_file.return_value = {"ok": True}
    with (patch("sales_support_agent.config.load_settings", return_value=SimpleNamespace(slack_bot_token="x", slack_channel_id="default")),
          patch("sales_support_agent.services.access.notify._send", send),
          patch("sales_support_agent.integrations.slack.SlackClient", return_value=slack)):
        result = deliver_daily_batch(value, batch, batches.load_batch_leads(value, [batch_id]))
    assert result == {"email": "delivered", "slack": "delivered"}
    assert b"exact.com" in send.call_args.kwargs["attachments"][0]["content"]
    assert slack.upload_file.call_args.kwargs["channel"] == "CQA"
    assert b"exact.com" in slack.upload_file.call_args.kwargs["content"]
