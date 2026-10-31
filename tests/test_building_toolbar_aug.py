"""Operator notes from August 2026 on Building pages.

Sunday calendars, readable submission fields, open leads on Today, the
customer's name on a booking, and a staff lead that can be saved from a phone
number.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sales_support_agent.services.building_inquiry_workspace import (
    detail_label,
    detail_value,
    render_inquiry_workspace,
    render_readonly_arena_calendar,
)
from sales_support_agent.services.building_page import render_building_page


def test_submission_keys_and_timestamps_read_as_words():
    assert detail_label("firstLandingPage") == "First landing page"
    assert detail_label("firstCapturedAt") == "First captured at"
    assert "Firstlandingpage" not in detail_label("firstLandingPage")
    rendered_time = detail_value("2026-08-08T20:42:07.215Z")
    assert "2026-08-08T20:42:07.215Z" not in rendered_time
    assert "Aug 08, 2026" in rendered_time
    assert "MT" in rendered_time

    page = render_inquiry_workspace(
        navigation="",
        csrf_token="token",
        data={
            "id": "lead-1",
            "name": "Mike Fleming",
            "email": "mike@example.com",
            "kind": "event",
            "created_at": datetime(2026, 8, 8, tzinfo=timezone.utc),
            "lifecycle": {"stage": "new"},
            "details": {
                "firstLandingPage": "/arena",
                "firstCapturedAt": "2026-08-08T20:42:07.215Z",
            },
            "calendar": {
                "label": "August 2026",
                "month": "2026-08-01",
                "previous": "2026-07-01",
                "next": "2026-09-01",
                "calendar_status": "connected",
                "cells": [],
            },
        },
    )
    assert "First landing page" in page
    assert "First captured at" in page
    assert "2026-08-08T20:42:07.215Z" not in page
    assert 'title="Sunday"' in page
    assert page.index('title="Sunday"') < page.index('title="Monday"')


def test_today_keeps_open_sales_leads_and_bookings_name_the_customer():
    calendar = render_readonly_arena_calendar(
        {
            "label": "August 2026",
            "previous": "2026-07",
            "next": "2026-09",
            "month": "2026-08",
            "calendar_status": "connected",
            "cells": [
                {
                    "iso": "2026-08-02",
                    "day": 2,
                    "state": "open",
                    "in_month": True,
                    "is_today": False,
                    "is_past": False,
                    "note": "",
                }
            ],
        },
        page_path="/admin/building/bookings",
        anchor="arena-month-bookings",
    )
    assert 'title="Sunday"' in calendar
    assert "cal_month=2026-07" in calendar

    page = render_building_page(
        user={"is_superadmin": True, "permissions": set(), "email": "admin@example.com"},
        view="today",
        spaces=[],
        offerings=[],
        contacts=[],
        segments=[],
        campaigns=[],
        inquiries=[
            {
                "id": "new-1",
                "name": "New Lead",
                "email": "new@example.com",
                "kind": "event",
                "source": "website",
                "lifecycle": {"stage": "new"},
            },
            {
                "id": "responded-1",
                "name": "Responded Lead",
                "email": "responded@example.com",
                "kind": "event",
                "source": "staff",
                "lifecycle": {"stage": "responded"},
            },
            {
                "id": "won-1",
                "name": "Won Lead",
                "email": "won@example.com",
                "kind": "event",
                "source": "staff",
                "lifecycle": {"stage": "closed_won"},
            },
        ],
        reservations=[
            {
                "id": "hold-1",
                "space_name": "Arena",
                "customer_name": "Elisa Edwards",
                "kind": "event",
                "starts_at": "Aug 28, 2026",
                "status": "soft_hold",
                "agreement_status": "not_started",
                "deposit_status": "not_started",
                "allowed_next": [],
            }
        ],
        invoices=[],
        adjustments=[],
        billing_accounts=[],
        billing_schedules=[],
        calendar_projections=[],
        checklists=[],
        service_requests=[],
        arena_calendar_today=calendar,
        arena_calendar_bookings=calendar,
    )
    assert "Responded Lead" in page
    assert "New Lead" in page
    assert "Won Lead" not in page.split('id="operator-queue"')[1].split("</section>")[0]
    assert "Elisa Edwards" in page
    assert ">Customer</th>" in page
    assert 'id="arena-calendar-today"' in page
    assert 'id="review-event-date"' in page
    assert "Fields marked" in page
    assert 'id="lead-email"' in page
    assert 'id="lead-email" name="email" type="email" required' not in page
