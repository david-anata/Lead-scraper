from datetime import date, datetime, timezone

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.bill_audit import (
    dismiss_finding,
    run_bill_audit,
)

AS_OF = date(2026, 7, 24)


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _add(engine, *, cid, name, amount, day, category="software"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="plaid", source_id=cid,
            record_kind="transaction", event_type="outflow", category=category,
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=day, status="posted", confidence="confirmed",
            created_at=now, updated_at=now,
        )


def _kinds(findings):
    return sorted(f["kind"] for f in findings)


def test_detects_duplicate_charge():
    engine = _setup()
    _add(engine, cid="d1", name="Adobe", amount=52_99, day=date(2026, 7, 3))
    _add(engine, cid="d2", name="Adobe", amount=52_99, day=date(2026, 7, 3))
    findings = run_bill_audit(as_of=AS_OF)
    assert "duplicate" in _kinds(findings)
    dup = next(f for f in findings if f["kind"] == "duplicate")
    assert dup["severity"] == "high"


def test_detects_price_creep():
    engine = _setup()
    _add(engine, cid="c1", name="Comcast", amount=110_00, day=date(2026, 3, 1))
    _add(engine, cid="c2", name="Comcast", amount=120_00, day=date(2026, 5, 1))
    _add(engine, cid="c3", name="Comcast", amount=139_00, day=date(2026, 7, 1))
    findings = run_bill_audit(as_of=AS_OF)
    assert "price_creep" in _kinds(findings)


def test_does_not_flag_stable_recurring_charge():
    engine = _setup()
    for i, day in enumerate([date(2026, 3, 1), date(2026, 5, 1), date(2026, 7, 1)]):
        _add(engine, cid=f"s{i}", name="Figma", amount=45_00, day=day)
    findings = run_bill_audit(as_of=AS_OF)
    assert "price_creep" not in _kinds(findings)


def test_detects_category_spike():
    engine = _setup()
    # Baseline (prior 90 days): $360 total -> $120 per 30-day window.
    _add(engine, cid="b1", name="VendorA", amount=120_00, day=date(2026, 6, 14), category="ads")
    _add(engine, cid="b2", name="VendorB", amount=120_00, day=date(2026, 5, 15), category="ads")
    _add(engine, cid="b3", name="VendorC", amount=120_00, day=date(2026, 4, 15), category="ads")
    # Recent 30 days: $200 -> well above 1.3x the $120 baseline.
    _add(engine, cid="r1", name="VendorD", amount=200_00, day=date(2026, 7, 14), category="ads")
    findings = run_bill_audit(as_of=AS_OF)
    assert "category_spike" in _kinds(findings)


def test_dismissed_finding_stays_hidden_on_rerun():
    engine = _setup()
    _add(engine, cid="d1", name="Adobe", amount=52_99, day=date(2026, 7, 3))
    _add(engine, cid="d2", name="Adobe", amount=52_99, day=date(2026, 7, 3))
    findings = run_bill_audit(as_of=AS_OF)
    dup = next(f for f in findings if f["kind"] == "duplicate")

    dismiss_finding(dup["fingerprint"])
    again = run_bill_audit(as_of=AS_OF)
    assert dup["fingerprint"] not in {f["fingerprint"] for f in again}


def test_clean_books_produce_no_findings():
    engine = _setup()
    _add(engine, cid="one", name="Rent", amount=1200_00, day=date(2026, 7, 1), category="rent")
    assert run_bill_audit(as_of=AS_OF) == []
