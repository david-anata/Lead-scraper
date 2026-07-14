"""Finance V2 schema, open-balance, allocation, and installment tests."""

from __future__ import annotations

import unittest
from datetime import date, datetime, timezone

from sqlalchemy import create_engine, inspect, text

from sales_support_agent.models.database import (
    _backfill_legacy_settlements,
    create_session_factory,
    init_database,
    upsert_cash_event,
)
from sales_support_agent.services.cashflow.settlements import (
    allocate_matched_transaction,
    create_payment_installment,
    create_settlement_allocation,
    get_open_balance_cents,
    get_scheduled_amount_cents,
    get_settled_amount_cents,
    reverse_settlement_allocation,
)


class SettlementTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.session_factory = create_session_factory("sqlite:///:memory:")
        init_database(self.session_factory)
        self.engine = self.session_factory.kw["bind"]

    def add_event(
        self,
        event_id: str,
        amount_cents: int,
        *,
        record_kind: str = "obligation",
        event_type: str = "outflow",
        source: str = "manual",
        status: str = "planned",
        due_date: date = date(2027, 1, 15),
    ) -> None:
        with self.engine.begin() as connection:
            upsert_cash_event(
                connection,
                {
                    "id": event_id,
                    "source": source,
                    "source_id": event_id,
                    "record_kind": record_kind,
                    "event_type": event_type,
                    "amount_cents": amount_cents,
                    "due_date": due_date,
                    "status": status,
                },
            )

    def event_status(self, event_id: str) -> str:
        with self.engine.connect() as connection:
            return str(
                connection.execute(
                    text("SELECT status FROM cash_events WHERE id = :id"),
                    {"id": event_id},
                ).scalar_one()
            )


class TestFinanceSettlementSchema(SettlementTestCase):
    def test_additive_tables_and_cash_event_fields_exist(self) -> None:
        db_inspector = inspect(self.engine)
        self.assertTrue(
            {
                "payment_installments",
                "settlement_allocations",
                "finance_source_records",
                "finance_import_batches",
                "finance_import_rows",
            }.issubset(set(db_inspector.get_table_names()))
        )
        columns = {column["name"] for column in db_inspector.get_columns("cash_events")}
        self.assertTrue(
            {"record_kind", "pay_priority", "minimum_payment_cents", "flexibility"}.issubset(columns)
        )

    def test_legacy_bank_insert_is_classified_as_transaction(self) -> None:
        self.add_event("bank-1", 25_000, source="csv", status="posted")
        with self.engine.connect() as connection:
            kind = connection.execute(
                text("SELECT record_kind FROM cash_events WHERE id = 'bank-1'")
            ).scalar_one()
        self.assertEqual(kind, "transaction")


class TestOpenBalances(SettlementTestCase):
    def test_live_near_full_match_leaves_remainder_open_and_is_idempotent(self) -> None:
        self.add_event("bill", 100_000)
        self.add_event(
            "bank-payment", 90_000,
            record_kind="transaction", source="csv", status="posted",
        )

        with self.engine.begin() as connection:
            first = allocate_matched_transaction(
                connection,
                obligation_event_id="bill",
                transaction_event_id="bank-payment",
                idempotency_key="live-near-full",
            )
        with self.engine.begin() as connection:
            retry = allocate_matched_transaction(
                connection,
                obligation_event_id="bill",
                transaction_event_id="bank-payment",
                idempotency_key="live-near-full",
            )

        self.assertEqual(first["id"], retry["id"])
        self.assertEqual(get_settled_amount_cents("bill"), 90_000)
        self.assertEqual(get_open_balance_cents("bill"), 10_000)
        self.assertEqual(self.event_status("bill"), "planned")

    def test_partial_allocation_does_not_close_obligation(self) -> None:
        self.add_event("bill", 100_000)

        create_settlement_allocation(
            obligation_event_id="bill",
            amount_cents=30_000,
            idempotency_key="partial-bill",
        )

        self.assertEqual(get_settled_amount_cents("bill"), 30_000)
        self.assertEqual(get_open_balance_cents("bill"), 70_000)
        self.assertEqual(self.event_status("bill"), "planned")

    def test_full_allocation_closes_and_retry_is_idempotent(self) -> None:
        self.add_event("bill", 100_000)
        first = create_settlement_allocation(
            obligation_event_id="bill",
            amount_cents=100_000,
            idempotency_key="full-bill",
        )
        retry = create_settlement_allocation(
            obligation_event_id="bill",
            amount_cents=100_000,
            idempotency_key="full-bill",
        )

        self.assertEqual(first["id"], retry["id"])
        self.assertEqual(get_open_balance_cents("bill"), 0)
        self.assertEqual(self.event_status("bill"), "paid")
        with self.engine.connect() as connection:
            count = connection.execute(text("SELECT COUNT(*) FROM settlement_allocations")).scalar_one()
        self.assertEqual(count, 1)

    def test_future_dated_allocation_is_rejected(self) -> None:
        self.add_event("bill", 100_000)
        with self.assertRaisesRegex(ValueError, "future-dated"):
            create_settlement_allocation(
                obligation_event_id="bill",
                amount_cents=10_000,
                allocation_date=date(2099, 1, 1),
                idempotency_key="future-payment",
            )

    def test_delete_with_settlement_evidence_soft_cancels(self) -> None:
        from sales_support_agent.services.cashflow.obligations import delete_obligation

        self.add_event("bill", 100_000)
        create_settlement_allocation(
            obligation_event_id="bill",
            amount_cents=10_000,
            idempotency_key="protected-evidence",
        )

        self.assertTrue(delete_obligation("bill"))
        self.assertEqual(self.event_status("bill"), "cancelled")

    def test_legacy_near_full_match_backfills_only_transaction_amount(self) -> None:
        self.add_event("bill", 100_000, status="matched")
        self.add_event(
            "bank-payment", 90_000,
            record_kind="transaction", source="csv", status="matched",
        )
        with self.engine.begin() as connection:
            connection.execute(
                text("UPDATE cash_events SET matched_to_id = 'bill' WHERE id = 'bank-payment'")
            )

        _backfill_legacy_settlements(self.engine)

        self.assertEqual(get_settled_amount_cents("bill"), 90_000)
        self.assertEqual(get_open_balance_cents("bill"), 10_000)
        self.assertEqual(self.event_status("bill"), "planned")

    def test_legacy_backfill_does_not_duplicate_existing_allocation(self) -> None:
        self.add_event("bill", 100_000, status="matched")
        self.add_event(
            "bank-payment", 90_000,
            record_kind="transaction", source="csv", status="matched",
        )
        create_settlement_allocation(
            obligation_event_id="bill",
            transaction_event_id="bank-payment",
            amount_cents=60_000,
            idempotency_key="existing-match",
        )
        with self.engine.begin() as connection:
            connection.execute(
                text("UPDATE cash_events SET status = 'matched', matched_to_id = 'bill' WHERE id = 'bank-payment'")
            )

        _backfill_legacy_settlements(self.engine)

        self.assertEqual(get_settled_amount_cents("bill"), 90_000)
        self.assertEqual(get_open_balance_cents("bill"), 10_000)

    def test_over_allocation_is_rejected(self) -> None:
        self.add_event("bill", 10_000)
        with self.assertRaisesRegex(ValueError, "open balance"):
            create_settlement_allocation(
                obligation_event_id="bill",
                amount_cents=10_001,
                idempotency_key="too-large",
            )

    def test_reversal_is_append_only_and_reopens_balance(self) -> None:
        self.add_event("bill", 50_000, due_date=date(2020, 1, 1))
        allocation = create_settlement_allocation(
            obligation_event_id="bill",
            amount_cents=50_000,
            idempotency_key="pay-bill",
        )
        reversal = reverse_settlement_allocation(
            allocation["id"],
            idempotency_key="reverse-bill",
            notes="Wrong bank transaction",
        )
        retry = reverse_settlement_allocation(
            allocation["id"],
            idempotency_key="reverse-bill",
        )

        self.assertEqual(reversal["id"], retry["id"])
        self.assertEqual(get_settled_amount_cents("bill"), 0)
        self.assertEqual(get_open_balance_cents("bill"), 50_000)
        self.assertEqual(self.event_status("bill"), "overdue")
        with self.engine.connect() as connection:
            rows = connection.execute(text("SELECT COUNT(*) FROM settlement_allocations")).scalar_one()
        self.assertEqual(rows, 2)


class TestTransactionAllocationSafety(SettlementTestCase):
    def test_transaction_can_split_but_not_exceed_its_amount(self) -> None:
        self.add_event("bill-a", 70_000)
        self.add_event("bill-b", 50_000)
        self.add_event("bank", 100_000, record_kind="transaction", source="csv", status="posted")

        create_settlement_allocation(
            obligation_event_id="bill-a",
            transaction_event_id="bank",
            amount_cents=70_000,
            idempotency_key="bank-to-a",
        )
        with self.assertRaisesRegex(ValueError, "transaction unallocated balance"):
            create_settlement_allocation(
                obligation_event_id="bill-b",
                transaction_event_id="bank",
                amount_cents=40_000,
                idempotency_key="bank-to-b-too-large",
            )

    def test_transaction_direction_must_match_obligation(self) -> None:
        self.add_event("receivable", 50_000, event_type="inflow")
        self.add_event("debit", 50_000, record_kind="transaction", source="csv", status="posted")
        with self.assertRaisesRegex(ValueError, "directions"):
            create_settlement_allocation(
                obligation_event_id="receivable",
                transaction_event_id="debit",
                amount_cents=50_000,
                idempotency_key="wrong-direction",
            )


class TestPaymentInstallments(SettlementTestCase):
    def test_create_retry_schedule_and_settle_installment(self) -> None:
        self.add_event("rent", 100_000)
        first = create_payment_installment(
            obligation_event_id="rent",
            amount_cents=40_000,
            due_date=date(2027, 1, 10),
            idempotency_key="rent-first",
        )
        retry = create_payment_installment(
            obligation_event_id="rent",
            amount_cents=40_000,
            due_date=date(2027, 1, 10),
            idempotency_key="rent-first",
        )

        self.assertEqual(first["id"], retry["id"])
        self.assertEqual(
            get_scheduled_amount_cents(
                "rent",
                from_date=date(2027, 1, 1),
                to_date=date(2027, 1, 31),
            ),
            40_000,
        )

        create_settlement_allocation(
            obligation_event_id="rent",
            installment_id=first["id"],
            amount_cents=40_000,
            idempotency_key="settle-rent-first",
        )
        with self.engine.connect() as connection:
            status = connection.execute(
                text("SELECT status FROM payment_installments WHERE id = :id"),
                {"id": first["id"]},
            ).scalar_one()
        self.assertEqual(status, "paid")
        self.assertEqual(get_scheduled_amount_cents("rent"), 0)
        self.assertEqual(get_open_balance_cents("rent"), 60_000)

    def test_planned_installments_cannot_exceed_open_balance(self) -> None:
        self.add_event("rent", 100_000)
        create_payment_installment(
            obligation_event_id="rent",
            amount_cents=60_000,
            due_date=date(2027, 1, 10),
            idempotency_key="rent-one",
        )
        with self.assertRaisesRegex(ValueError, "cannot exceed"):
            create_payment_installment(
                obligation_event_id="rent",
                amount_cents=50_000,
                due_date=date(2027, 1, 20),
                idempotency_key="rent-two",
            )


class TestLegacyMigration(unittest.TestCase):
    def test_existing_sqlite_rows_are_backfilled_without_deletion(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as connection:
            connection.execute(text("""
                CREATE TABLE cash_events (
                    id TEXT PRIMARY KEY,
                    source TEXT NOT NULL DEFAULT 'manual',
                    source_id TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL DEFAULT 'outflow',
                    category TEXT NOT NULL DEFAULT 'uncategorized',
                    name TEXT NOT NULL DEFAULT '',
                    vendor_or_customer TEXT NOT NULL DEFAULT '',
                    amount_cents INTEGER NOT NULL DEFAULT 0,
                    due_date TEXT,
                    status TEXT NOT NULL DEFAULT 'planned',
                    confidence TEXT NOT NULL DEFAULT 'estimated',
                    created_at TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT ''
                )
            """))
            connection.execute(text("""
                INSERT INTO cash_events (id, source, source_id, event_type, status)
                VALUES ('legacy-bank', 'csv', 'tx-1', 'outflow', 'posted')
            """))

        from sqlalchemy.orm import sessionmaker

        factory = sessionmaker(bind=engine, future=True)
        init_database(factory)
        with engine.connect() as connection:
            row = connection.execute(
                text("SELECT id, record_kind FROM cash_events WHERE id = 'legacy-bank'")
            ).one()
        self.assertEqual(tuple(row), ("legacy-bank", "transaction"))


if __name__ == "__main__":
    unittest.main()
