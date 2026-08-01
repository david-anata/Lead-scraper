"""Walk one event from public enquiry to confirmed booking and invoice.

Runs the real endpoints against a throwaway database. Nothing touches
production: no email is sent, no calendar is written, no card is charged.
"""

from __future__ import annotations

import dataclasses
import os
import tempfile
from datetime import date, datetime, timedelta, timezone

DB = os.path.join(tempfile.gettempdir(), "arena_flow_walkthrough.db")
if os.path.exists(DB):
    os.remove(DB)
os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + DB)

from fastapi.testclient import TestClient  # noqa: E402

from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import (  # noqa: E402
    create_session_factory,
    init_database,
)
from sales_support_agent.models.entities import (  # noqa: E402
    BuildingOffering,
    BuildingRatePlan,
    BuildingSpace,
)

factory = create_session_factory("sqlite:///" + DB)
init_database(factory)
app.state.session_factory = factory
app.state.settings = dataclasses.replace(
    app.state.settings, internal_api_key="walkthrough-key"
)
client = TestClient(app)
H = {"X-Internal-Api-Key": "walkthrough-key"}

BOOK = "/api/internal/building/bookings"
BILL = "/api/internal/building/billing"

step_no = 0
failures: list[str] = []


def step(title: str, response, *, expect: int) -> dict:
    global step_no
    step_no += 1
    ok = response.status_code == expect
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {step_no:>2}. {title}")
    if not ok:
        failures.append(f"{step_no}. {title}: expected {expect}, got {response.status_code}")
        print(f"          -> {response.status_code} {response.text[:300]}")
        return {}
    try:
        return response.json()
    except Exception:
        return {}


def money(cents: int) -> str:
    return f"${cents / 100:,.2f}"


# ---------------------------------------------------------------- the building
starts = datetime.now(timezone.utc) + timedelta(days=45)
with factory() as session:
    session.add(
        BuildingSpace(
            id="arena",
            slug="arena",
            name="The Arena",
            space_type="event",
            capacity=200,
            status="available",
        )
    )
    session.add(
        BuildingOffering(
            id="arena-events",
            slug="arena-events",
            space_id="arena",
            name="Arena events",
            offering_type="event",
        )
    )
    session.add(
        BuildingRatePlan(
            id="arena-2026",
            offering_id="arena-events",
            name="Arena 2026",
            status="approved",
            booking_unit="hour",
            unit_amount_cents=17500,
            minimum_units=6,
            currency="USD",
            tax_status="review_required",
            effective_from=date.today(),
        )
    )
    session.commit()

client.put(
    f"{BILL}/accounts/ferro",
    headers=H,
    json={
        "id": "ferro",
        "account_name": "Ferro Events",
        "billing_email": "billing@ferro.example",
        "actor": "operator@anatainc.com",
    },
)

print("\n=== The Arena, one event, enquiry to invoice ===\n")

# ------------------------------------------------------------ 1. the enquiry
created = step(
    "Customer enquiry becomes a reservation",
    client.post(
        BOOK,
        headers=H,
        json={
            "id": "res-walk",
            "kind": "event",
            "space_id": "arena",
            "offering_id": "arena-events",
            "starts_at": starts.isoformat(),
            "ends_at": (starts + timedelta(hours=6)).isoformat(),
            "attendance": 120,
            "assigned_owner": "operator@anatainc.com",
            "source": "anatabuilding.com",
            "actor": "operator@anatainc.com",
        },
    ),
    expect=201,
)
if created:
    print(f"          status: {created['reservation']['status']}")

# ------------------------------------------------- 2. capacity is really checked
step(
    "A 400 person request is refused (Arena holds 200)",
    client.post(
        BOOK,
        headers=H,
        json={
            "id": "res-toobig",
            "kind": "event",
            "space_id": "arena",
            "starts_at": starts.isoformat(),
            "ends_at": (starts + timedelta(hours=6)).isoformat(),
            "attendance": 400,
            "actor": "operator@anatainc.com",
        },
    ),
    expect=422,
)

# ------------------------------------------------------- 3. the state machine
for target, extra in (
    ("requirements_review", {}),
    ("soft_hold", {"hold_expires_at": (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()}),
):
    step(
        f"Move the booking to {target.replace('_', ' ')}",
        client.post(
            f"{BOOK}/res-walk/transition",
            headers=H,
            json={"target_status": target, "actor": "operator@anatainc.com", **extra},
        ),
        expect=200,
    )

step(
    "Skipping straight to confirmed is refused",
    client.post(
        f"{BOOK}/res-walk/transition",
        headers=H,
        json={"target_status": "confirmed", "actor": "operator@anatainc.com"},
    ),
    expect=409,
)

# ------------------------------------------------------------- 4. the discount
step(
    "TAX UNDECIDED: a discounted quote is refused outright",
    client.post(
        f"{BOOK}/res-walk/proposals",
        headers=H,
        json={
            "id": "quote-walk",
            "status": "draft",
            "proposal_type": "quote",
            "rate_plan_id": "arena-2026",
            "pricing_subtotal_cents": 620000,
            "discount_cents": 70000,
            "discount_reason": "Repeat customer, third booking this year",
            "actor": "operator@anatainc.com",
        },
    ),
    expect=409,
)

client.post(
    BOOK,
    headers=H,
    json={
        "id": "res-flat",
        "kind": "event",
        "space_id": "arena",
        "offering_id": "arena-events",
        "starts_at": (starts + timedelta(days=2)).isoformat(),
        "ends_at": (starts + timedelta(days=2, hours=6)).isoformat(),
        "attendance": 80,
        "actor": "operator@anatainc.com",
    },
)
step(
    "TAX UNDECIDED: a flat list-price quote is still allowed",
    client.post(
        f"{BOOK}/res-flat/proposals",
        headers=H,
        json={
            "id": "quote-flat",
            "status": "draft",
            "proposal_type": "quote",
            "amount_cents": 620000,
            "line_items": [{"type": "package", "description": "Six hour block", "amount_cents": 620000}],
            "actor": "operator@anatainc.com",
        },
    ),
    expect=201,
)

# The owner records the tax decision. Everything below needs it.
with factory() as session:
    plan = session.get(BuildingRatePlan, "arena-2026")
    plan.tax_status = "non_taxable"
    session.commit()
print("\n          --- owner records the tax decision on the rate plan ---\n")

step(
    "A discount with no business reason is refused",
    client.post(
        f"{BOOK}/res-walk/proposals",
        headers=H,
        json={
            "id": "quote-walk",
            "status": "draft",
            "proposal_type": "quote",
            "rate_plan_id": "arena-2026",
            "pricing_subtotal_cents": 620000,
            "discount_cents": 70000,
            "discount_reason": "",
            "actor": "operator@anatainc.com",
        },
    ),
    expect=422,
)

step(
    "A discount larger than the event is refused",
    client.post(
        f"{BOOK}/res-walk/proposals",
        headers=H,
        json={
            "id": "quote-walk",
            "status": "draft",
            "proposal_type": "quote",
            "rate_plan_id": "arena-2026",
            "pricing_subtotal_cents": 620000,
            "discount_cents": 900000,
            "discount_reason": "Overly generous",
            "actor": "operator@anatainc.com",
        },
    ),
    expect=422,
)

quote = step(
    "Quote of $6,200 with a $700 repeat-customer discount",
    client.post(
        f"{BOOK}/res-walk/proposals",
        headers=H,
        json={
            "id": "quote-walk",
            "status": "draft",
            "proposal_type": "quote",
            "rate_plan_id": "arena-2026",
            "pricing_subtotal_cents": 620000,
            "discount_cents": 70000,
            "discount_reason": "Repeat customer, third booking this year",
            "terms_summary": "The Arena, six hour block, 120 guests",
            "actor": "operator@anatainc.com",
        },
    ),
    expect=201,
)
if quote:
    from sales_support_agent.models.entities import BuildingProposal as _BP

    with factory() as session:
        row = session.get(_BP, "quote-walk")
        print(f"          quote total: {money(row.amount_cents)}")
        for item in row.line_items_json:
            print(f"            - {item['description']}: {money(item['amount_cents'])}")

for status, extra in (
    ("approved", {}),
    ("sent", {"document_url": "https://example.com/quote-walk.pdf"}),
):
    step(
        f"Quote {status}",
        client.post(
            f"{BOOK}/res-walk/proposals",
            headers=H,
            json={
                "id": "quote-walk",
                "status": status,
                "proposal_type": "quote",
                "rate_plan_id": "arena-2026",
                "pricing_subtotal_cents": 620000,
                "discount_cents": 70000,
                "discount_reason": "Repeat customer, third booking this year",
                "approved_by": "david@anatainc.com",
                "actor": "operator@anatainc.com",
                **extra,
            },
        ),
        expect=201,
    )

step(
    "Booking moves to quote sent (needs a real approved, sent quote)",
    client.post(
        f"{BOOK}/res-walk/transition",
        headers=H,
        json={"target_status": "quote_sent", "actor": "operator@anatainc.com"},
    ),
    expect=200,
)

# A sent quote is immutable, so acceptance must echo the stored record exactly
# rather than recalculating it.
from sales_support_agent.models.entities import BuildingProposal as _BP2  # noqa: E402

with factory() as session:
    stored = session.get(_BP2, "quote-walk")
    echo = {
        "id": stored.id,
        "version": stored.version,
        "status": "accepted",
        "proposal_type": "quote",
        "currency": stored.currency,
        "amount_cents": stored.amount_cents,
        "line_items": list(stored.line_items_json or []),
        "rate_plan_id": stored.rate_plan_id or None,
        "terms_summary": stored.terms_summary,
        "valid_until": stored.valid_until.isoformat() if stored.valid_until else None,
        "document_url": stored.document_url,
        "approved_by": stored.approved_by,
        "actor": "operator@anatainc.com",
    }

step(
    "Customer accepts the quote",
    client.post(f"{BOOK}/res-walk/proposals", headers=H, json=echo),
    expect=201,
)

# ------------------------------------------------- 5. the quote reaches billing
sched = step(
    "Bill is drafted from the accepted quote",
    client.post(
        f"{BILL}/schedules/from-proposal",
        headers=H,
        json={
            "id": "sched-walk",
            "proposal_id": "quote-walk",
            "billing_account_id": "ferro",
            "starts_on": date.today().isoformat(),
            "actor": "operator@anatainc.com",
        },
    ),
    expect=201,
)
if sched:
    print(f"          billed amount: {money(sched['amount_cents'])}  (discount carried, nobody retyped it)")

step(
    "Bill approved",
    client.post(
        f"{BILL}/schedules/sched-walk/approve",
        headers=H,
        json={"actor": "david@anatainc.com"},
    ),
    expect=200,
)

preview = step(
    "Invoice preview",
    client.post(
        f"{BILL}/invoices",
        headers=H,
        json={
            "schedule_id": "sched-walk",
            "idempotency_key": "walkthrough-invoice-1",
            "execute": False,
            "actor": "operator@anatainc.com",
        },
    ),
    expect=200,
)
if preview:
    print(f"          invoice would be: {money(preview['proposal']['amount_cents'])}")

# -------------------------------------------------- 6. the renegotiation guard
with factory() as session:
    from sales_support_agent.models.entities import BuildingProposal

    row = session.get(BuildingProposal, "quote-walk")
    row.version = 2
    row.amount_cents = 500000
    session.commit()

step(
    "Customer renegotiates: the stale bill refuses to invoice",
    client.post(
        f"{BILL}/invoices",
        headers=H,
        json={
            "schedule_id": "sched-walk",
            "idempotency_key": "walkthrough-invoice-2",
            "execute": False,
            "actor": "operator@anatainc.com",
        },
    ),
    expect=409,
)

# --------------------------------------------------------- 7. through to booked
for target in ("contract_pending", "deposit_due"):
    step(
        f"Move the booking to {target.replace('_', ' ')}",
        client.post(
            f"{BOOK}/res-walk/transition",
            headers=H,
            json={"target_status": target, "actor": "operator@anatainc.com"},
        ),
        expect=200,
    )

step(
    "Confirming with no signed agreement is refused",
    client.post(
        f"{BOOK}/res-walk/transition",
        headers=H,
        json={"target_status": "confirmed", "actor": "operator@anatainc.com"},
    ),
    expect=409,
)

step(
    "Recording a signed agreement with no proof is refused",
    client.post(
        f"{BOOK}/res-walk/agreements",
        headers=H,
        json={
            "id": "agr-walk",
            "status": "signed",
            "provider": "dropbox_sign",
            "provider_reference": "",
            "actor": "operator@anatainc.com",
        },
    ),
    expect=422,
)

step(
    "Signed agreement recorded with provider proof",
    client.post(
        f"{BOOK}/res-walk/agreements",
        headers=H,
        json={
            "id": "agr-walk",
            "status": "signed",
            "provider": "dropbox_sign",
            "provider_reference": "walkthrough-signature-001",
            "template_name": "Arena event agreement v1",
            "document_url": "https://example.com/agr-walk.pdf",
            "actor": "operator@anatainc.com",
        },
    ),
    expect=201,
)

step(
    "Marking a deposit paid with no payment proof is refused",
    client.post(
        f"{BOOK}/res-walk/deposit-evidence",
        headers=H,
        json={
            "id": "dep-bad",
            "status": "paid",
            "amount_cents": 275000,
            "provider": "stripe",
            "provider_reference": "",
            "actor": "operator@anatainc.com",
        },
    ),
    expect=422,
)

step(
    "50 percent deposit recorded as paid, with payment proof",
    client.post(
        f"{BOOK}/res-walk/deposit-evidence",
        headers=H,
        json={
            "id": "dep-walk",
            "status": "paid",
            "amount_cents": 275000,
            "provider": "stripe",
            "provider_reference": "pi_walkthrough_001",
            "actor": "operator@anatainc.com",
        },
    ),
    expect=201,
)

# Confirmation also requires a named person who is responsible for the event.
from sales_support_agent.models.entities import (  # noqa: E402
    BuildingContact as _BC,
    BuildingReservation as _BR,
)

with factory() as session:
    session.add(
        _BC(
            id="contact-walk",
            email="rosalind@ferro.example",
            full_name="Rosalind Ferro",
            phone="801-555-0142",
            company_name="Ferro Events",
        )
    )
    session.get(_BR, "res-walk").contact_id = "contact-walk"
    session.commit()

step(
    "Booking confirmed",
    client.post(
        f"{BOOK}/res-walk/transition",
        headers=H,
        json={"target_status": "confirmed", "actor": "operator@anatainc.com"},
    ),
    expect=200,
)

with factory() as session:
    booked = session.get(_BR, "res-walk")
    print(f"\n          final booking status: {booked.status}")
    print(f"          deposit status:       {booked.deposit_status}")

print(f"\n=== {step_no - len(failures)} of {step_no} steps passed ===")
for line in failures:
    print("  FAILED " + line)
