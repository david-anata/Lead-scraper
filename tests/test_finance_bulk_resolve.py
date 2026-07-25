from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.bulk_resolve import (
    apply_bulk_action,
    latest_batch,
    list_review_items,
    preview_bulk_action,
    undo_batch,
)


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _obligation(engine, *, cid, name, amount, due, commitment_type="general",
                workflow_status="needs_review", status="planned"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="clickup", source_id=cid,
            record_kind="obligation", event_type="outflow", category="other",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=due, status=status, confidence="estimated",
            created_at=now, updated_at=now,
        )
        connection.execute(text(
            "UPDATE cash_events SET commitment_type=:ct, workflow_status=:ws WHERE id=:id"
        ), {"ct": commitment_type, "ws": workflow_status, "id": cid})


def _row(engine, cid):
    with engine.connect() as connection:
        return dict(connection.execute(text(
            "SELECT workflow_status, archived_at FROM cash_events WHERE id=:id"
        ), {"id": cid}).fetchone()._mapping)


def test_preview_reports_totals_and_writes_nothing():
    engine = _setup()
    _obligation(engine, cid="a", name="Adobe", amount=52_99, due=date(2026, 6, 1))
    _obligation(engine, cid="b", name="Rent", amount=12000_00, due=date(2026, 6, 1))

    preview = preview_bulk_action(["a", "b"], "write_off")
    assert preview["eligible_count"] == 2
    assert preview["amount_cents"] == 52_99 + 12000_00
    assert preview["skipped_count"] == 0
    # Nothing changed.
    assert _row(engine, "a")["archived_at"] is None
    assert _row(engine, "b")["archived_at"] is None


def test_protected_items_are_skipped_by_bulk_actions():
    engine = _setup()
    _obligation(engine, cid="pay", name="Gusto Payroll", amount=22400_00,
                due=date(2026, 6, 1), commitment_type="payroll")
    _obligation(engine, cid="tax", name="IRS", amount=5000_00,
                due=date(2026, 6, 1), commitment_type="tax")
    _obligation(engine, cid="ok", name="Adobe", amount=52_99, due=date(2026, 6, 1))

    preview = preview_bulk_action(["pay", "tax", "ok"], "write_off")
    assert preview["eligible_count"] == 1
    assert preview["skipped_count"] == 2

    result = apply_bulk_action(["pay", "tax", "ok"], "write_off",
                              reason="uncollectible", actor="qa@example.com")
    assert result["applied"] == 1
    assert result["skipped"] == 2
    assert _row(engine, "pay")["archived_at"] is None      # payroll untouched
    assert _row(engine, "tax")["archived_at"] is None      # tax untouched
    assert _row(engine, "ok")["workflow_status"] == "written_off"


def test_write_off_archives_without_deleting_and_undo_restores_exact_state():
    engine = _setup()
    _obligation(engine, cid="a", name="Adobe", amount=52_99, due=date(2026, 6, 1),
                workflow_status="scheduled")

    apply_bulk_action(["a"], "write_off", reason="vendor closed", actor="qa@example.com")
    after = _row(engine, "a")
    assert after["workflow_status"] == "written_off"
    assert after["archived_at"] is not None
    # The record still exists; nothing was deleted.
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM cash_events WHERE id='a'")).scalar() == 1

    batch = latest_batch()
    assert batch is not None and batch["item_count"] == 1
    undo_batch(str(batch["id"]), actor="qa@example.com")

    restored = _row(engine, "a")
    assert restored["workflow_status"] == "scheduled"   # exact prior state
    assert restored["archived_at"] is None
    assert latest_batch() is None


def test_reason_is_required():
    engine = _setup()
    _obligation(engine, cid="a", name="Adobe", amount=52_99, due=date(2026, 6, 1))
    with pytest.raises(ValueError):
        apply_bulk_action(["a"], "write_off", reason="   ", actor="qa")


def test_unknown_action_is_rejected():
    _setup()
    with pytest.raises(ValueError):
        preview_bulk_action(["a"], "delete_forever")
    with pytest.raises(ValueError):
        apply_bulk_action(["a"], "delete_forever", reason="x")


def test_bulk_action_writes_an_audit_entry():
    engine = _setup()
    _obligation(engine, cid="a", name="Adobe", amount=52_99, due=date(2026, 6, 1))
    result = apply_bulk_action(["a"], "write_off", reason="uncollectible", actor="qa@example.com")
    with engine.connect() as connection:
        audit = connection.execute(text(
            "SELECT action_type, actor FROM finance_action_audit WHERE entity_id=:id"
        ), {"id": result["batch_id"]}).fetchone()
    assert audit._mapping["action_type"] == "bulk_write_off"
    assert audit._mapping["actor"] == "qa@example.com"


def test_already_resolved_items_are_skipped():
    engine = _setup()
    _obligation(engine, cid="a", name="Adobe", amount=52_99, due=date(2026, 6, 1))
    apply_bulk_action(["a"], "write_off", reason="first pass", actor="qa")
    preview = preview_bulk_action(["a"], "write_off")
    assert preview["eligible_count"] == 0
    assert preview["skipped_count"] == 1


def test_review_list_groups_by_reason():
    engine = _setup()
    _obligation(engine, cid="a", name="No amount", amount=0, due=date(2026, 6, 1))
    data = list_review_items(as_of=date(2026, 7, 24))
    assert data["total"] >= 1
    labels = {group["label"] for group in data["groups"]}
    assert any("amount" in label.lower() for label in labels)
