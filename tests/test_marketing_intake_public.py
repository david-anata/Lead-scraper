"""Two-step marketing-site intake: create → needs → email unlock.

Proves: the shared-secret header gates every route, the identity lookup is
patched out (no network), needs are stored and filtered to known chips, and
the unlock enforces the shared one-per-email-per-UTC-day gate (second unlock
with the same email 429s), with the background delivery patched out.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock
from sqlalchemy import event

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/mkt_intake_test.db")
os.environ.setdefault("MARKETING_SITE_INTAKE_KEY", "test-intake-key")

try:
    from fastapi.testclient import TestClient
    from sales_support_agent.api import marketing_router as M
    from sales_support_agent.main import app
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False

HEADERS = {"X-Internal-Api-Key": "test-intake-key"}


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class MarketingShelfPayloadTests(unittest.TestCase):
    @staticmethod
    def _product(
        asin: str,
        *,
        revenue: float | None,
        units: float | None,
        floor: bool = False,
        price: float = 20.0,
        brand: str = "TestBrand",
    ):
        from sales_support_agent.services.helium10 import XrayProduct

        suffix = "+" if floor else ""
        return XrayProduct(
            display_order=1,
            title=f"Product {asin}",
            asin=asin,
            url=f"https://www.amazon.com/dp/{asin}",
            image_url=f"https://images.example/{asin}.jpg",
            brand=brand,
            price=price,
            price_label=f"${price:.2f}",
            revenue=revenue,
            revenue_label=f"${revenue:,.0f}{suffix}" if revenue is not None else "N/A",
            units_sold=units,
            units_label=f"{int(units):,}{suffix}" if units is not None else "N/A",
            bsr=5000.0,
            bsr_label="5,000",
            rating=4.5,
            rating_label="4.5",
            review_count=120,
            category="Health",
            seller_country="",
            size_tier="",
            fulfillment="FBA",
            dimensions="10 x 6 x 3 in",
            weight="1.2 lb",
        )

    def test_product_payload_distinguishes_recent_sales_floor(self) -> None:
        payload = M._shelf_product_payload(
            self._product("B09ABCDEF1", revenue=10_000, units=500, floor=True)
        )
        self.assertEqual(payload["units_source"], "recent_sales")
        self.assertEqual(payload["revenue_source"], "recent_sales")
        self.assertEqual(payload["recent_sales"], 500)
        self.assertEqual(payload["estimated_revenue"], 10_000.0)

    def test_assembled_payload_uses_visible_five_product_set(self) -> None:
        products = [
            self._product(
                f"B09ABCDEF{i}",
                revenue=float(value),
                units=float(value / 20),
                brand=f"Brand {i}",
            )
            for i, value in enumerate((1000, 2000, 3000, 4000, 5000, 9000))
        ]
        payload = M._assemble_shelf_payload(
            self._product("B09TARGET01", revenue=2500, units=125, floor=True),
            products,
            ["Mixed evidence."],
        )
        self.assertEqual(payload["comparison_count"], 5)
        self.assertEqual(payload["revenue_product_count"], 5)
        self.assertEqual(payload["visible_revenue"], 15_000.0)
        self.assertEqual(payload["median_revenue"], 3_000.0)
        self.assertEqual(payload["target"]["units_source"], "recent_sales")
        self.assertEqual(payload["revenue_warning"], "Mixed evidence.")
        self.assertTrue(payload["captured_at"])
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["required_comparison_count"], 5)

    def test_assembled_payload_rejects_thin_or_same_brand_comparisons(self) -> None:
        target = self._product("B09TARGET01", revenue=2500, units=125, floor=True, brand="Example Brand")
        same_brand = self._product("B09SAME001", revenue=3000, units=150, brand="Example Brand")
        outside = self._product("B09OTHER01", revenue=4000, units=200, brand="Outside Brand")

        payload = M._assemble_shelf_payload(target, [same_brand, outside], [])

        self.assertEqual(payload["status"], "incomplete")
        self.assertEqual(payload["comparison_count"], 1)
        self.assertEqual(payload["competitors"][0]["brand"], "Outside Brand")


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class MarketingIntakeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from sales_support_agent.models.database import create_session_factory, init_database
        import dataclasses

        db = os.path.join(tempfile.gettempdir(), "mkt_intake_isolated.db")
        if os.path.exists(db):
            os.remove(db)
        factory = create_session_factory("sqlite:///" + db)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings, marketing_site_intake_key="test-intake-key"
        )
        cls.client = TestClient(app)

    @staticmethod
    def _rate_limit_request(client_key: str):
        from starlette.requests import Request

        return Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/",
                "headers": [
                    (
                        b"x-marketing-client-key",
                        client_key.encode("ascii"),
                    )
                ],
                "client": ("127.0.0.1", 1234),
                "server": ("testserver", 80),
                "scheme": "http",
                "app": app,
            }
        )

    def test_rate_limit_is_durable_and_isolated_by_client_digest(self) -> None:
        first = self._rate_limit_request("a" * 64)
        second = self._rate_limit_request("b" * 64)
        scope = f"test:{self.id()}"

        self.assertFalse(
            M.durable_rate_limited(first, scope=scope, limit=2)
        )
        self.assertFalse(
            M.durable_rate_limited(first, scope=scope, limit=2)
        )
        self.assertTrue(
            M.durable_rate_limited(first, scope=scope, limit=2)
        )
        self.assertFalse(
            M.durable_rate_limited(second, scope=scope, limit=2)
        )

        with M.session_scope(app.state.session_factory) as session:
            rows = session.execute(
                M.select(M.AutomationRun).where(
                    M.AutomationRun.run_type.like(
                        f"{M.RATE_LIMIT_RUN_TYPE_PREFIX}%"
                    )
                )
            ).scalars().all()
        matching = [
            row
            for row in rows
            if (row.metadata_json or {}).get("scope") == scope
        ]
        self.assertEqual(len(matching), 2)
        self.assertNotIn("a" * 64, str([row.metadata_json for row in matching]))

    def test_rate_limit_reuses_one_row_across_windows(self) -> None:
        request = self._rate_limit_request("c" * 64)
        scope = f"test:{self.id()}"
        self.assertFalse(M.durable_rate_limited(request, scope=scope, limit=2))

        with M.session_scope(app.state.session_factory) as session:
            row = session.execute(
                M.select(M.AutomationRun).where(
                    M.AutomationRun.run_type.like(
                        f"{M.RATE_LIMIT_RUN_TYPE_PREFIX}%"
                    )
                )
            ).scalars().all()
            matching = [
                item
                for item in row
                if (item.metadata_json or {}).get("scope") == scope
            ]
            self.assertEqual(len(matching), 1)
            metadata = dict(matching[0].metadata_json or {})
            matching[0].metadata_json = {
                **metadata,
                "bucket": int(metadata["bucket"]) - 1,
                "count": 999,
            }
            session.add(matching[0])

        self.assertFalse(M.durable_rate_limited(request, scope=scope, limit=2))
        with M.session_scope(app.state.session_factory) as session:
            matching = [
                item
                for item in session.execute(
                    M.select(M.AutomationRun).where(
                        M.AutomationRun.run_type.like(
                            f"{M.RATE_LIMIT_RUN_TYPE_PREFIX}%"
                        )
                    )
                ).scalars().all()
                if (item.metadata_json or {}).get("scope") == scope
            ]
            self.assertEqual(len(matching), 1)
            self.assertEqual((matching[0].metadata_json or {}).get("count"), 1)

    def test_analysis_status_lookup_has_fixed_query_count(self) -> None:
        email = "indexed-status@example.com"
        asin = "B0INDEXED01"
        with M.session_scope(app.state.session_factory) as session:
            run = M.AuditService(session).start_run(
                M.INTAKE_RUN_TYPE,
                trigger="test",
                metadata={"email": email, "asin": asin},
            )
            M._bind_analysis_lookup(
                session,
                run_id=int(run.id),
                email=email,
                asin=asin,
            )
            for index in range(100):
                session.add(
                    M.AutomationAction(
                        run_id=int(run.id),
                        clickup_task_id="",
                        system="marketing",
                        action_type=M.ANALYSIS_LOOKUP_ACTION,
                        dedupe_key=f"marketing-analysis:noise-{index:03d}",
                        success=True,
                        error_message="",
                        before_json={},
                        after_json={},
                    )
                )
            run_id = int(run.id)

        engine = app.state.session_factory.kw["bind"]
        statements: list[str] = []

        def count_query(_conn, _cursor, statement, _parameters, _context, _many) -> None:
            statements.append(statement)

        event.listen(engine, "before_cursor_execute", count_query)
        try:
            with M.session_scope(app.state.session_factory) as session:
                resolved = M._latest_intake(session, email=email, asin=asin)
        finally:
            event.remove(engine, "before_cursor_execute", count_query)

        self.assertIsNotNone(resolved)
        self.assertEqual(int(resolved.id), run_id)
        self.assertLessEqual(len(statements), 1, statements)

    def _create(self, kind: str = "asin", identifier: str = "B0TESTASIN1") -> dict:
        with mock.patch.object(
            M,
            "_asin_identity",
            return_value={
                "asin": "B0TESTASIN",
                "brand_name": "TestBrand",
                "product_title": "Test Product",
                "product_image": "https://img.example/x.jpg",
            },
        ), mock.patch.object(
            M,
            "_store_identity",
            return_value={
                "domain": "testbrand.com",
                "brand_name": "TestBrand",
                "product_title": "TestBrand Store",
                "product_image": "",
            },
        ):
            resp = self.client.post(
                "/api/public/marketing/intake",
                json={"identifier": identifier, "kind": kind, "source": "hero"},
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 201, resp.text)
        return resp.json()

    def test_auth_required(self) -> None:
        resp = self.client.post(
            "/api/public/marketing/intake",
            json={"identifier": "B0TESTASIN1", "kind": "asin"},
        )
        self.assertEqual(resp.status_code, 401)

    def test_oversized_body_is_rejected_before_identity_work(self) -> None:
        with mock.patch.object(M, "_asin_identity") as identity:
            resp = self.client.post(
                "/api/public/marketing/intake",
                content=b'{"identifier":"' + (b"A" * 16_384) + b'","kind":"asin"}',
                headers={**HEADERS, "Content-Type": "application/json"},
            )
        self.assertEqual(resp.status_code, 413, resp.text)
        self.assertEqual(resp.json()["detail"], "Request body is too large.")
        identity.assert_not_called()

    def test_create_returns_identity_and_token(self) -> None:
        data = self._create()
        self.assertIn("intake_id", data)
        self.assertTrue(data["token"])
        self.assertEqual(data["kind"], "asin")
        self.assertEqual(data["brand_name"], "TestBrand")
        self.assertEqual(data["product_title"], "Test Product")
        self.assertNotIn("dtc_domain", data)

    def test_status_exposes_a_public_draft_delivery_state(self) -> None:
        data = self._create()
        response = self.client.get(
            f"/api/public/marketing/intake/{data['intake_id']}",
            params={"token": data["token"]},
            headers=HEADERS,
        )
        self.assertEqual(response.status_code, 200, response.text)
        delivery = response.json()["delivery_status"]
        self.assertTrue(delivery["correlation_id"].startswith("mkt_"))
        self.assertEqual(delivery["request"]["status"], "draft")
        self.assertEqual(delivery["report"]["status"], "not_required")
        self.assertEqual(delivery["final_email"]["status"], "not_required")
        self.assertNotIn("email", delivery)

    def test_store_create_returns_dtc_domain(self) -> None:
        data = self._create(kind="store", identifier="testbrand.com")
        self.assertEqual(data["kind"], "store")
        self.assertEqual(data["dtc_domain"], "testbrand.com")
        status = self.client.get(
            f"/api/public/marketing/intake/{data['intake_id']}",
            params={"token": data["token"]},
            headers=HEADERS,
        ).json()
        self.assertEqual(status["kind"], "store")
        self.assertEqual(status["dtc_domain"], "testbrand.com")

    def test_needs_stored_and_filtered(self) -> None:
        data = self._create()
        with mock.patch.object(M, "_build_shelf"):
            resp = self.client.post(
            f"/api/public/marketing/intake/{data['intake_id']}/needs",
                json={"token": data["token"], "needs": ["analytics", "advertising", "bogus"]},
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        status = self.client.get(
            f"/api/public/marketing/intake/{data['intake_id']}",
            params={"token": data["token"]},
            headers=HEADERS,
        ).json()
        self.assertEqual(status["needs"], ["analytics", "advertising"])

    def test_needs_on_asin_intake_sets_shelf_pending(self) -> None:
        data = self._create()
        with mock.patch.object(M, "_build_shelf") as build:
            resp = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/needs",
                json={"token": data["token"], "needs": ["analytics"]},
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        build.assert_called_once()
        status = self.client.get(
            f"/api/public/marketing/intake/{data['intake_id']}",
            params={"token": data["token"]},
            headers=HEADERS,
        ).json()
        self.assertEqual(status["shelf"], {"status": "pending"})

    def test_needs_on_store_intake_has_no_shelf(self) -> None:
        data = self._create(kind="store", identifier="testbrand.com")
        with mock.patch.object(M, "_build_shelf") as build:
            resp = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/needs",
                json={"token": data["token"], "needs": ["analytics"]},
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        build.assert_not_called()
        status = self.client.get(
            f"/api/public/marketing/intake/{data['intake_id']}",
            params={"token": data["token"]},
            headers=HEADERS,
        ).json()
        self.assertIsNone(status["shelf"])

    def test_needs_wrong_token_403(self) -> None:
        data = self._create()
        resp = self.client.post(
            f"/api/public/marketing/intake/{data['intake_id']}/needs",
            json={"token": "wrong", "needs": ["analytics"]},
            headers=HEADERS,
        )
        self.assertEqual(resp.status_code, 403)

    def test_unlock_daily_gate_and_closers(self) -> None:
        import os as _os

        _os.environ["MARKETING_DAILY_GATE"] = "1"
        self.addCleanup(lambda: _os.environ.pop("MARKETING_DAILY_GATE", None))
        data = self._create()
        self.client.post(
            f"/api/public/marketing/intake/{data['intake_id']}/needs",
            json={"token": data["token"], "needs": ["analytics", "advertising"]},
            headers=HEADERS,
        )
        with mock.patch.object(
            M, "_send_internal_lead_email", return_value=True
        ), mock.patch.object(M, "_run_analysis_and_deliver") as deliver:
            resp = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={"token": data["token"], "email": "gate@example.com"},
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 202, resp.text)
        body = resp.json()
        self.assertEqual(body["status"], "building")
        self.assertTrue(body["closers"]["software"])
        self.assertTrue(body["closers"]["services"])
        self.assertTrue(body["delivery_status"]["correlation_id"].startswith("mkt_"))
        self.assertEqual(
            body["delivery_status"]["internal_notification"]["status"],
            "complete",
        )
        self.assertEqual(body["delivery_status"]["report"]["status"], "pending")
        deliver.assert_called_once()

        # Same email, same UTC day, a fresh intake → 429 daily_limit.
        second = self._create()
        with mock.patch.object(
            M, "_send_internal_lead_email", return_value=True
        ), mock.patch.object(M, "_run_analysis_and_deliver"):
            resp2 = self.client.post(
                f"/api/public/marketing/intake/{second['intake_id']}/unlock",
                json={"token": second["token"], "email": "gate@example.com"},
                headers=HEADERS,
            )
        self.assertEqual(resp2.status_code, 429)
        self.assertEqual(resp2.json()["reason"], "daily_limit")

    def test_store_unlock_uses_store_delivery(self) -> None:
        data = self._create(kind="store", identifier="testbrand.com")
        with mock.patch.object(
            M, "_send_internal_lead_email", return_value=True
        ), mock.patch.object(M, "_deliver_store_unlock") as deliver:
            resp = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={"token": data["token"], "email": "store@example.com"},
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 202, resp.text)
        body = resp.json()
        self.assertFalse(body["closers"]["software"])
        self.assertFalse(body["closers"]["services"])
        deliver.assert_called_once()

    def test_unlock_sanitizes_and_forwards_qualification(self) -> None:
        data = self._create()
        self.client.post(
            f"/api/public/marketing/intake/{data['intake_id']}/needs",
            json={"token": data["token"], "needs": ["advertising"]},
            headers=HEADERS,
        )
        qualification = {
            "name": "  David Narayan  ",
            "company": " Anata ",
            "phone": " 385-204-4649 ",
            "storefront": " https://example.com ",
            "revenue_range": "$250K–$1M",
            "challenge": " Improve advertising efficiency ",
            "next_step": "Book my review",
            "ignored": "must not persist",
        }
        with mock.patch.object(
            M, "_send_internal_lead_email", return_value=True
        ), mock.patch.object(M, "_run_analysis_and_deliver") as deliver:
            response = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={"token": data["token"], "email": "qualified@example.com", "qualification": qualification},
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 202, response.text)
        forwarded = deliver.call_args.kwargs["qualification"]
        self.assertEqual(forwarded["name"], "David Narayan")
        self.assertEqual(forwarded["phone"], "385-204-4649")
        self.assertNotIn("ignored", forwarded)
        with app.state.session_factory() as session:
            run = session.get(M.AutomationRun, int(data["intake_id"]))
            self.assertEqual(run.metadata_json["qualification"]["company"], "Anata")
            self.assertEqual(run.summary_json["internal_lead_email"], "delivered")

    def test_unlock_suppresses_the_observed_bot_submission(self) -> None:
        """The 2026-07-31 scripted submission must reach nothing downstream.

        It answers 202 like any other unlock so the script learns nothing, but
        sends no alert, writes no HubSpot record, and starts no paid analysis.
        """
        data = self._create()
        with mock.patch.object(
            M, "_send_internal_lead_email", return_value=True
        ) as lead_email, mock.patch.object(
            M, "_send_unlock_ack_email", return_value=True
        ) as ack_email, mock.patch.object(
            M, "_record_hubspot_lead", return_value=True
        ) as hubspot, mock.patch.object(M, "_run_analysis_and_deliver") as deliver:
            response = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={
                    "token": data["token"],
                    "email": "oroqe.n.u.z.94.5@gmail.com",
                    "qualification": {
                        "name": "jRYUmRZKmbAAdcTVxYKORSZX",
                        "company": "Pmynutqga LLC",
                        "phone": "6490216433",
                    },
                },
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 202, response.text)
        lead_email.assert_not_called()
        ack_email.assert_not_called()
        hubspot.assert_not_called()
        deliver.assert_not_called()

    def test_unlock_still_accepts_a_real_lead(self) -> None:
        """The guard must not cost a genuine submission."""
        data = self._create()
        with mock.patch.object(
            M, "_send_internal_lead_email", return_value=True
        ) as lead_email, mock.patch.object(M, "_run_analysis_and_deliver"):
            response = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={
                    "token": data["token"],
                    "email": "sarah.chen@brightleaf.com",
                    "qualification": {
                        "name": "Sarah Chen",
                        "company": "Brightleaf Supplements",
                        "phone": "(801) 555-0142",
                    },
                },
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 202, response.text)
        lead_email.assert_called_once()

    def test_unlock_does_not_claim_success_when_every_handoff_fails(self) -> None:
        data = self._create()
        with mock.patch.object(
            M, "_send_unlock_ack_email", return_value=False
        ), mock.patch.object(
            M, "_send_internal_lead_email", return_value=False
        ), mock.patch.object(
            M, "_record_hubspot_lead", return_value=False
        ), mock.patch.object(M, "_run_analysis_and_deliver"):
            response = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={"token": data["token"], "email": "lost@example.com"},
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(response.json()["status"], "delivery_unavailable")
        delivery = response.json()["delivery_status"]
        self.assertEqual(delivery["request"]["status"], "failed")
        self.assertTrue(delivery["request"]["retryable"])

    def test_status_exposes_ready_report_and_final_email_outcomes(self) -> None:
        data = self._create()
        with mock.patch.object(
            M, "_send_unlock_ack_email", return_value=True
        ), mock.patch.object(
            M, "_send_internal_lead_email", return_value=True
        ), mock.patch.object(
            M, "_record_hubspot_lead", return_value=True
        ), mock.patch.object(M, "_run_analysis_and_deliver"):
            unlocked = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={"token": data["token"], "email": "ready@example.com"},
                headers=HEADERS,
            )
        self.assertEqual(unlocked.status_code, 202, unlocked.text)
        with app.state.session_factory() as session:
            run = session.get(M.AutomationRun, int(data["intake_id"]))
            run.status = "success"
            run.summary_json = {
                **dict(run.summary_json or {}),
                "view_url": "https://agent.example/decks/tokenized",
                "email_delivery": "delivered",
            }
            session.add(run)
            session.commit()

        status = self.client.get(
            f"/api/public/marketing/intake/{data['intake_id']}",
            params={"token": data["token"]},
            headers=HEADERS,
        )
        self.assertEqual(status.status_code, 200, status.text)
        delivery = status.json()["delivery_status"]
        self.assertEqual(delivery["request"]["status"], "completed")
        self.assertEqual(delivery["report"], {
            "status": "complete",
            "result_url": "https://agent.example/decks/tokenized",
        })
        self.assertEqual(delivery["final_email"]["status"], "complete")

    def test_status_distinguishes_report_failure_from_lead_capture(self) -> None:
        data = self._create()
        with mock.patch.object(
            M, "_send_unlock_ack_email", return_value=True
        ), mock.patch.object(
            M, "_send_internal_lead_email", return_value=True
        ), mock.patch.object(
            M, "_record_hubspot_lead", return_value=True
        ), mock.patch.object(M, "_run_analysis_and_deliver"):
            unlocked = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={"token": data["token"], "email": "partial@example.com"},
                headers=HEADERS,
            )
        self.assertEqual(unlocked.status_code, 202, unlocked.text)
        with app.state.session_factory() as session:
            run = session.get(M.AutomationRun, int(data["intake_id"]))
            run.status = "failed"
            run.summary_json = {
                **dict(run.summary_json or {}),
                "email_delivery": "failed",
            }
            session.add(run)
            session.commit()

        status = self.client.get(
            f"/api/public/marketing/intake/{data['intake_id']}",
            params={"token": data["token"]},
            headers=HEADERS,
        )
        self.assertEqual(status.status_code, 200, status.text)
        delivery = status.json()["delivery_status"]
        self.assertEqual(delivery["request"]["status"], "partial_failure")
        self.assertEqual(delivery["report"]["status"], "failed")
        self.assertEqual(delivery["final_email"]["status"], "failed")

    def test_booking_confirmation_updates_hubspot_once(self) -> None:
        data = self._create()
        with mock.patch.object(
            M, "_send_internal_lead_email", return_value=True
        ), mock.patch.object(M, "_run_analysis_and_deliver"):
            unlocked = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={
                    "token": data["token"],
                    "email": "booked@example.com",
                    "qualification": {"company": "TestBrand"},
                },
                headers=HEADERS,
            )
        self.assertEqual(unlocked.status_code, 202, unlocked.text)
        with mock.patch.object(
            M,
            "_record_hubspot_booking",
            return_value=(True, "deal-123"),
        ) as record:
            first = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/booked",
                json={
                    "token": data["token"],
                    "source": "diagnostic-report-unlocked",
                },
                headers=HEADERS,
            )
            second = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/booked",
                json={
                    "token": data["token"],
                    "source": "diagnostic-report-unlocked",
                },
                headers=HEADERS,
            )
        self.assertEqual(first.status_code, 200, first.text)
        self.assertFalse(first.json()["duplicate"])
        self.assertTrue(second.json()["duplicate"])
        record.assert_called_once()

    def test_direct_booking_records_deal_alert_and_deduplicates(self) -> None:
        payload = {
            "email": "direct-booking@example.com",
            "tool": "strategy",
            "source": "header-book-a-call",
            "booking_reference": "meeting-abc-123",
            "qualification": {
                "name": "Direct Booker",
                "company": "Direct Brand",
                "phone": "385-555-0101",
            },
        }
        with mock.patch.object(
            M,
            "_record_hubspot_booking",
            return_value=(True, "deal-direct-123"),
        ) as record, mock.patch.object(
            M,
            "_send_internal_booking_email",
            return_value=True,
        ) as notify:
            first = self.client.post(
                "/api/public/marketing/booking",
                json=payload,
                headers=HEADERS,
            )
            second = self.client.post(
                "/api/public/marketing/booking",
                json=payload,
                headers=HEADERS,
            )
        self.assertEqual(first.status_code, 200, first.text)
        self.assertFalse(first.json()["duplicate"])
        self.assertEqual(first.json()["notification"], "delivered")
        self.assertEqual(second.status_code, 200, second.text)
        self.assertTrue(second.json()["duplicate"])
        record.assert_called_once()
        notify.assert_called_once()

    def test_direct_booking_rejects_missing_hubspot_contact(self) -> None:
        with mock.patch.object(
            M,
            "_record_hubspot_booking",
            return_value=(False, ""),
        ):
            response = self.client.post(
                "/api/public/marketing/booking",
                json={
                    "email": "unknown@example.com",
                    "tool": "strategy",
                    "source": "book",
                    "booking_reference": "missing-contact",
                },
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 502, response.text)

    def test_direct_booking_sanitizes_source_and_audit_reference(self) -> None:
        with mock.patch.object(
            M,
            "_record_hubspot_booking",
            return_value=(True, "deal-source-safe"),
        ) as record, mock.patch.object(
            M,
            "_send_internal_booking_email",
            return_value=True,
        ):
            response = self.client.post(
                "/api/public/marketing/booking",
                json={
                    "email": "source-safe@example.com",
                    "tool": "ads",
                    "source": "person@example.com?campaign=visitor text",
                    "booking_reference": "meeting-source-safe",
                    "qualification": {
                        "company": "Safe Source Brand",
                        "audit_run_id": "12345",
                    },
                },
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(record.call_args.kwargs["source"], "booking-page")
        self.assertEqual(record.call_args.kwargs["qualification"]["audit_run_id"], "12345")

        sanitized = M._sanitize_qualification({"audit_run_id": "123@example.com"})
        self.assertNotIn("audit_run_id", sanitized)

    def test_booking_updates_only_matching_strategy_deal(self) -> None:
        client = mock.Mock()
        client.is_configured = True
        client.find_contact_by_email.return_value = {
            "id": "contact-1",
            "properties": {"company": "Ocean Rx"},
        }
        client.list_associations.return_value = ["unrelated", "strategy"]
        client.batch_read.return_value = [
            {"id": "unrelated", "properties": {"dealname": "Ocean Rx - Fulfillment"}},
            {"id": "strategy", "properties": {"dealname": "Ocean Rx - Strategy Audit"}},
        ]
        with mock.patch.object(M, "HubSpotClient", return_value=client):
            recorded, deal_id = M._record_hubspot_booking(
                app.state.settings,
                email="ocean@example.com",
                brand_name="Ocean Rx",
                source="diagnostic-report-unlocked",
            )
        self.assertTrue(recorded)
        self.assertEqual(deal_id, "strategy")
        client.update_deal.assert_called_once_with(
            "strategy",
            {"dealstage": mock.ANY},
        )
        client.create_deal.assert_not_called()

    def test_qualified_contact_fields_sync_to_hubspot(self) -> None:
        client = mock.Mock()
        client.is_configured = True
        client.create_contact.return_value = {"id": "123"}
        with mock.patch.object(M, "HubSpotClient", return_value=client):
            M._record_hubspot_lead(
                app.state.settings,
                email="qualified@example.com",
                asin="B0TESTASIN",
                view_url="https://agent.example/deck",
                source="strategy-audit",
                needs=["advertising"],
                qualification={
                    "name": "David Narayan",
                    "company": "Anata",
                    "phone": "385-204-4649",
                    "storefront": "https://example.com",
                },
            )
        client.create_contact.assert_called_once_with({
            "email": "qualified@example.com",
            "firstname": "David Narayan",
            "company": "Anata",
            "phone": "385-204-4649",
            "website": "https://example.com",
        })
        client.update_contact.assert_called_once_with("123", {
            "firstname": "David Narayan",
            "company": "Anata",
            "phone": "385-204-4649",
            "website": "https://example.com",
        })

    def test_advertising_audit_rejects_store_url(self) -> None:
        response = self.client.post(
            "/api/public/marketing/advertising-audit",
            json={
                "product": "https://oceanrx.us",
                "email": "ads@example.com",
                "company": "Ocean Rx",
            },
            headers=HEADERS,
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["reason"], "invalid_product")

    def test_advertising_audit_accepts_amazon_url_and_returns_status_handle(self) -> None:
        with mock.patch.object(M, "_run_analysis_and_deliver") as deliver:
            response = self.client.post(
                "/api/public/marketing/advertising-audit",
                json={
                    "product": "https://www.amazon.com/example/dp/B09239YTZQ",
                    "email": "ads-accepted@example.com",
                    "company": "Ocean Rx",
                    "source": "anatainc.com/tools/advertising-audit",
                },
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 202, response.text)
        body = response.json()
        self.assertEqual(body["asin"], "B09239YTZQ")
        self.assertEqual(body["status"], "accepted")
        self.assertTrue(body["token"])
        deliver.assert_called_once()
        forwarded = deliver.call_args.kwargs
        self.assertEqual(forwarded["needs"], ["advertising"])
        self.assertEqual(forwarded["qualification"]["company"], "Ocean Rx")

        status = self.client.get(
            f"/api/public/marketing/advertising-audit/{body['run_id']}",
            params={"token": body["token"]},
            headers=HEADERS,
        )
        self.assertEqual(status.status_code, 200, status.text)
        self.assertEqual(status.json(), {
            "status": "building",
            "strategy_audit": "building",
            "advertising_audit": "reports_required",
            "email_delivery": "pending",
        })

    def test_advertising_audit_status_requires_run_token(self) -> None:
        with mock.patch.object(M, "_run_analysis_and_deliver"):
            body = self.client.post(
                "/api/public/marketing/advertising-audit",
                json={
                    "product": "B09239YTZQ",
                    "email": "ads-token@example.com",
                    "company": "Ocean Rx",
                },
                headers=HEADERS,
            ).json()
        response = self.client.get(
            f"/api/public/marketing/advertising-audit/{body['run_id']}",
            params={"token": "wrong"},
            headers=HEADERS,
        )
        self.assertEqual(response.status_code, 403)

    def test_advertising_audit_status_rejects_corrupt_lifecycle(self) -> None:
        with mock.patch.object(M, "_run_analysis_and_deliver"):
            body = self.client.post(
                "/api/public/marketing/advertising-audit",
                json={
                    "product": "B09239YTZQ",
                    "email": "ads-corrupt@example.com",
                    "company": "Ocean Rx",
                },
                headers=HEADERS,
            ).json()
        with M.session_scope(app.state.session_factory) as session:
            run = session.get(M.AutomationRun, int(body["run_id"]))
            run.summary_json = {**(run.summary_json or {}), "email_delivery": "mystery"}
            session.add(run)
        response = self.client.get(
            f"/api/public/marketing/advertising-audit/{body['run_id']}",
            params={"token": body["token"]},
            headers=HEADERS,
        )
        self.assertEqual(response.status_code, 500, response.text)
        self.assertEqual(response.json()["detail"], "Invalid advertising audit lifecycle.")

    def test_advertising_hubspot_note_names_tool_and_next_step(self) -> None:
        client = mock.Mock()
        client.is_configured = True
        client.create_contact.return_value = {"id": "321"}
        with mock.patch.object(M, "HubSpotClient", return_value=client):
            recorded = M._record_hubspot_lead(
                app.state.settings,
                email="ads-note@example.com",
                asin="B09239YTZQ",
                view_url="https://agent.example/deck",
                source="anatainc.com/tools/advertising-audit",
                needs=["advertising"],
                qualification={
                    "company": "Ocean Rx",
                    "next_step": "Call prospect and confirm the four-report handoff.",
                },
            )
        self.assertTrue(recorded)
        note = client.create_contact_note.call_args.kwargs["body"]
        self.assertIn("Advertising Audit requested", note)
        self.assertIn("Ocean Rx", note)
        self.assertIn("Call prospect and confirm the four-report handoff.", note)


if __name__ == "__main__":
    unittest.main()
