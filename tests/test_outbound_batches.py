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
    import outbound_recipes as recipes
    saved = batches.load_recipe_definitions(value)
    assert len(saved) == 6
    by_key = {item["key"]: item["weekdays"] for item in saved}
    assert by_key == {recipe.key: ",".join(str(day) for day in recipe.weekdays) for recipe in recipes.RECIPES}
    assert [recipe.key for recipe in batches.materialize_recipes(value, 0, {})] == ["icp_baseline"]


def test_recipe_migration_matches_the_code_defaults():
    from pathlib import Path
    import outbound_recipes as recipes
    sql = Path("supabase/migrations/20261028000000_outbound_recipe_tables.sql").read_text()
    assert "CREATE TABLE IF NOT EXISTS outbound_recipe_definitions" in sql
    assert "CREATE TABLE IF NOT EXISTS outbound_daily_batches" in sql
    assert "CREATE TABLE IF NOT EXISTS outbound_daily_batch_runs" in sql
    assert "CREATE TABLE IF NOT EXISTS outbound_daily_batch_companies" in sql
    assert "ON CONFLICT (recipe_key) DO NOTHING" in sql
    for recipe in recipes.RECIPES:
        assert f"'{recipe.key}'" in sql
        weekdays = ",".join(str(day) for day in recipe.weekdays)
        assert f"'{weekdays}'" in sql
        assert recipe.label in sql
        assert str(recipe.max_per_run) in sql


def test_a_missing_recipe_table_still_uses_the_code_defaults():
    value = create_engine("sqlite://", future=True)
    import outbound_recipes as recipes
    saved = batches.load_recipe_definitions(value)
    assert [item["key"] for item in saved] == [recipe.key for recipe in recipes.RECIPES]


def test_a_recipe_read_error_does_not_revive_paused_defaults():
    value = engine()
    assert batches.save_recipe(value, {
        "key": "icp_baseline", "label": "Baseline", "template_key": "icp_baseline",
        "reason": "Paused for the test", "tier": "C", "priority": "1",
        "weekdays": "0", "cap": "25", "active": "0", "include_in_daily": "0",
    }, actor="qa@example.com")["ok"]
    assert batches.materialize_recipes(value, 0, {}) == []

    class Boom:
        def __enter__(self):
            raise RuntimeError("connection reset")

        def __exit__(self, *args):
            return False

    value.connect = lambda: Boom()
    try:
        batches.load_recipe_definitions(value)
    except batches.RecipeReadError:
        pass
    else:
        raise AssertionError("a failed recipe read returned definitions")
    try:
        batches.materialize_recipes(value, 0, {})
    except batches.RecipeReadError:
        pass
    else:
        raise AssertionError("a failed recipe read revived the morning schedule")


def test_an_empty_recipe_table_falls_back_to_the_code_defaults():
    value = engine()
    from sqlalchemy import text
    with value.begin() as conn:
        conn.execute(text("DELETE FROM outbound_recipe_definitions"))
    import outbound_recipes as recipes
    saved = batches.load_recipe_definitions(value)
    assert [item["key"] for item in saved] == [recipe.key for recipe in recipes.RECIPES]
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


def _november_rows():
    """The rows Postgres rejected for batch 12 on 2026-11-11."""
    return [
        {"recipe_key": "amz_on", "recipe_label": "amz_on", "status": "complete",
         "fresh": 42, "scanned": 80, "matched": 42, "skipped_seen": 3},
        {"recipe_key": "amz_off", "recipe_label": "amz_off", "status": "complete",
         "fresh": 0, "scanned": 20, "matched": 0},
        {"recipe_key": "plan_upgrade", "recipe_label": "plan_upgrade", "status": "complete",
         "fresh": 0, "scanned": 11, "matched": 0},
        {"recipe_key": "replatform", "recipe_label": "replatform", "status": "complete",
         "fresh": 0, "scanned": 8, "matched": 0},
        {"recipe_key": "plan_upgrade", "recipe_label": "Upgraded to Shopify Plus",
         "status": "skipped", "fresh": 5, "scanned": 99,
         "error": "active campaign filter already sources this recipe"},
        {"recipe_key": "replatformed", "recipe_label": "Recently replatformed",
         "status": "skipped", "fresh": 0,
         "error": "active campaign filter already sources this recipe"},
    ]


def test_duplicate_recipe_keys_keep_the_complete_row():
    merged = batches.merge_recipe_runs(list(reversed(_november_rows())))
    by_key = {row["recipe_key"]: row for row in merged}
    assert list(by_key) == ["replatformed", "plan_upgrade", "replatform", "amz_off", "amz_on"]
    assert by_key["plan_upgrade"]["status"] == "complete"
    assert by_key["plan_upgrade"]["fresh"] == 0
    assert by_key["plan_upgrade"]["scanned"] == 11
    assert "already sources" not in str(by_key["plan_upgrade"].get("error") or "")
    assert by_key["replatformed"]["status"] == "skipped"
    value = engine()
    batch_id, _created = batches.create_batch(
        value, business_date="2026-11-11", trigger="scheduled",
        recipe_count=6, correlation_id="collision",
    )
    batch = batches.finalize_batch(
        value, batch_id=batch_id, recipe_runs=_november_rows(), leads=[],
        filename="anata-daily-leads-2026-11-11.csv",
    )
    stored = batches.batch_runs(value, batch_id)
    assert [row["recipe_key"] for row in stored].count("plan_upgrade") == 1
    plan = next(row for row in stored if row["recipe_key"] == "plan_upgrade")
    assert plan["status"] == "complete" and plan["fresh"] == 0
    assert batch.scanned == 80 + 20 + 11 + 8
    assert batch.status == "ready"
    assert batch.recipe_count == 5


def test_same_outcome_rows_add_counts_and_a_skip_does_not():
    merged = batches.merge_recipe_runs([
        {"recipe_key": "plan_upgrade", "status": "complete", "fresh": 42, "scanned": 80},
        {"recipe_key": "plan_upgrade", "status": "complete", "fresh": 3, "scanned": 10},
        {"recipe_key": "amz_on", "status": "skipped", "fresh": 9, "scanned": 50,
         "error": "held"},
        {"recipe_key": "amz_on", "status": "complete", "fresh": 1, "scanned": 4},
        {"recipe_key": "", "status": "failed", "fresh": 4, "error": "second"},
        {"recipe_key": "", "status": "complete", "fresh": 1},
    ])
    by_key = {row["recipe_key"]: row for row in merged}
    assert by_key["plan_upgrade"]["fresh"] == 45
    assert by_key["plan_upgrade"]["scanned"] == 90
    assert by_key["amz_on"]["status"] == "complete"
    assert by_key["amz_on"]["fresh"] == 1
    assert by_key["amz_on"]["scanned"] == 4
    assert by_key[""]["status"] == "complete"
    assert by_key[""]["fresh"] == 1


def test_a_failed_finalize_marks_the_batch_failed_and_a_finished_batch_stays_finished(monkeypatch):
    value = engine()
    batch_id, created = batches.create_batch(
        value, business_date="2026-11-11", trigger="scheduled",
        recipe_count=1, correlation_id="fail",
    )
    same_id, created_again = batches.create_batch(
        value, business_date="2026-11-11", trigger="scheduled",
        recipe_count=1, correlation_id="fail-2",
    )
    assert created and not created_again and same_id == batch_id
    reopened_id, reopened = batches.create_batch(
        value, business_date="2026-11-11", trigger="scheduled",
        recipe_count=1, correlation_id="reopen", resume_incomplete=True,
    )
    assert reopened_id == batch_id and reopened

    def boom(_leads):
        raise RuntimeError(
            'duplicate key value violates unique constraint "outbound_daily_batch_runs_pkey"'
        )

    monkeypatch.setattr("outbound_pipeline.leads_to_csv", boom)
    try:
        batches.finalize_batch(
            value, batch_id=batch_id, recipe_runs=[
                {"recipe_key": "amz_on", "status": "complete", "fresh": 1},
            ],
            leads=[], filename="anata-daily-leads-2026-11-11.csv",
        )
    except RuntimeError as exc:
        assert "outbound_daily_batch_runs_pkey" in str(exc)
    else:
        raise AssertionError("finalize swallowed the write failure")
    failed = batches.get_batch(value, batch_id)
    assert failed.status == "failed"
    assert "outbound_daily_batch_runs_pkey" in failed.error
    monkeypatch.undo()
    again_id, again = batches.create_batch(
        value, business_date="2026-11-11", trigger="scheduled",
        recipe_count=1, correlation_id="after-fail", resume_incomplete=True,
    )
    assert again_id == batch_id and again
    assert batches.get_batch(value, batch_id).status == "running"
    ready = batches.finalize_batch(
        value, batch_id=batch_id, recipe_runs=[
            {"recipe_key": "amz_on", "status": "complete", "fresh": 1},
        ],
        leads=[{"domain": "kept.example", "brand": "Kept", "recipe": "amz_on"}],
        filename="anata-daily-leads-2026-11-11.csv",
    )
    assert ready.status == "ready" and ready.unique_companies == 1
    finished_id, finished = batches.create_batch(
        value, business_date="2026-11-11", trigger="scheduled",
        recipe_count=1, correlation_id="finished", resume_incomplete=True,
    )
    assert finished_id == batch_id and not finished
    assert batches.get_batch(value, batch_id).status == "ready"
    batches.mark_batch_failed(value, batch_id, RuntimeError("must not downgrade a ready batch"))
    assert batches.get_batch(value, batch_id).status == "ready"


def test_existing_pull_tables_are_a_complete_no_ddl_fallback():
    value = create_engine("sqlite://", future=True)
    outbound_memory.ensure_runs_table(value)
    from sales_support_agent.services import outbound_settings
    outbound_settings.ensure_tables(value)
    with patch("sales_support_agent.services.outbound_batches._tables_available", return_value=False):
        assert batches.save_recipe(value, {
            "key": "fallback_signal", "label": "Fallback", "template_key": "icp_baseline",
            "reason": "Stored in the existing settings table", "tier": "B", "priority": "3",
            "weekdays": "0,1", "cap": "20", "active": "1", "include_in_daily": "1",
        }, actor="qa@example.com")["ok"]
        assert any(item["key"] == "fallback_signal" for item in batches.load_recipe_definitions(value))
        batch_id, created = batches.create_batch(value, business_date="2026-08-21", trigger="scheduled",
                                                 recipe_count=1, correlation_id="fallback")
        assert created
        batch = batches.finalize_batch(value, batch_id=batch_id,
            recipe_runs=[{"recipe_key": "fallback_signal", "recipe_label": "Fallback", "status": "complete", "fresh": 1}],
            leads=[{"domain": "fallback.com", "brand": "Fallback", "recipe": "fallback_signal"}],
            filename="anata-daily-leads-2026-08-21.csv")
        assert batch.unique_companies == 1
        assert "fallback.com" in batches.batch_artifact(value, batch_id)[1]
        history, total = batches.load_batches(value)
        assert total == 1 and history[0].id == batch_id
