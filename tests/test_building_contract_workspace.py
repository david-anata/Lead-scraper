from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest import mock

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_contract_workspace_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-contract-workspace-session-secret",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAgreement,
        BuildingAgreementTemplate,
        BuildingAuditEvent,
        BuildingAvailabilityBlock,
        BuildingContact,
        BuildingPaymentRequestReadiness,
        BuildingProposal,
        BuildingReservation,
        BuildingSpace,
    )
    from sales_support_agent.services.admin_auth import create_user_session_token

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False

CONTRACTS = "/admin/building/contracts"


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingContractWorkspaceTests(unittest.TestCase):
    """The contract workspace must make every contract findable and honest.

    Preparation and approval still delegate to the guarded internal API, so these
    tests also assert that no provider object is created anywhere in the flow.
    """

    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"building_contract_workspace_{uuid.uuid4().hex}.db"
        )
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="contract-workspace-internal-key",
            building_campaign_token_secret="contract-workspace-signing-secret",
        )
        settings = app.state.agent_settings
        token = create_user_session_token(
            settings, email="david@anatainc.com", name="David", role="admin"
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.client.cookies.set(settings.admin_cookie_name, token)
        cls.browser_headers = {
            "Origin": "http://testserver",
            "Sec-Fetch-Mode": "navigate",
        }
        now = datetime.now(timezone.utc)
        cls.start = now + timedelta(days=45)
        with factory() as session:
            session.add_all([
                BuildingSpace(
                    id="contract-arena",
                    slug="contract-arena",
                    name="Contract Arena",
                    space_type="event",
                    capacity=250,
                    status="available",
                ),
                BuildingContact(
                    id="contract-host",
                    email="contract-host@example.com",
                    full_name="Cordelia Vance",
                    status="active",
                ),
            ])
            reservation = BuildingReservation(
                id="contract-event",
                kind="event",
                status="soft_hold",
                contact_id="contract-host",
                space_id="contract-arena",
                starts_at=cls.start,
                guest_starts_at=cls.start + timedelta(hours=2),
                guest_ends_at=cls.start + timedelta(hours=6),
                ends_at=cls.start + timedelta(hours=8),
                hold_expires_at=now + timedelta(days=5),
                attendance=140,
                deposit_required=True,
                assigned_owner="events@example.com",
                created_by="operator@example.com",
            )
            session.add(reservation)
            session.add(BuildingAvailabilityBlock(
                id="contract-block",
                space_id="contract-arena",
                state="soft_hold",
                starts_at=reservation.starts_at,
                ends_at=reservation.ends_at,
                expires_at=reservation.hold_expires_at,
                source="agent",
                source_reference="reservation:contract-event",
            ))
            session.add(BuildingProposal(
                id="contract-quote",
                reservation_id="contract-event",
                version=1,
                proposal_type="quote",
                status="draft",
                currency="USD",
                amount_cents=450000,
                line_items_json=[{
                    "type": "base",
                    "name": "Reviewed event package",
                    "quantity": 1,
                    "amount_cents": 450000,
                }],
                rate_plan_id="contract-rate-v1",
                rate_plan_snapshot_json={
                    "id": "contract-rate-v1",
                    "version": 1,
                    "deposit_type": "percent",
                    "deposit_percent_bps": 3000,
                    "cancellation_policy": "Non-refundable inside 30 days.",
                    "tax_status": "non_taxable",
                    "tax_rate_bps": 0,
                    "tax_note": "Reviewed as non-taxable.",
                    "included": ["Venue access", "Tables and chairs"],
                    "addons": [],
                },
                terms_summary="Frozen reviewed quote terms.",
                created_by="operator@example.com",
            ))
            # Legacy free-text evidence: no package, no checksum.
            session.add(BuildingAgreement(
                id="legacy-agreement",
                reservation_id="contract-event",
                version=9,
                status="signed",
                provider="manual",
                provider_reference="paper-file-2025",
                template_name="Vivint 2025 (customer specific)",
                created_by="operator@example.com",
                updated_at=now - timedelta(days=1),
            ))
            session.commit()

    def _csrf(self) -> str:
        page = self.client.get(CONTRACTS)
        self.assertEqual(page.status_code, 200, page.text)
        match = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(match, "contract page must expose a form token")
        return match.group(1)

    def _post(self, url: str, data: dict) -> object:
        return self.client.post(
            url,
            headers=self.browser_headers,
            follow_redirects=False,
            data={"_csrf_token": self._csrf(), **data},
        )

    def _approve_template(self) -> None:
        # Approved templates are immutable, so a second call is a no-op rather
        # than a failure. Tests should not have to know who ran before them.
        with self.factory() as session:
            existing = session.get(BuildingAgreementTemplate, "contract-template-v1")
            if existing is not None and existing.status == "approved":
                return
        headers = {"X-Internal-Api-Key": "contract-workspace-internal-key"}
        created = self.client.put(
            "/api/internal/building/agreement-readiness/templates/contract-template-v1",
            headers=headers,
            json={
                "id": "contract-template-v1",
                "template_key": "event-agreement",
                "version": 1,
                "name": "Event agreement",
                "template_reference": "approved-repository:event-agreement-v1",
                "merge_fields": [
                    "customer_name",
                    "customer_email",
                    "event_space",
                    "setup_starts_at",
                    "guest_starts_at",
                    "guest_ends_at",
                    "teardown_ends_at",
                    "attendance",
                    "quote_total",
                    "currency",
                    "deposit_amount",
                    "deposit_type",
                    "cancellation_policy",
                    "tax_terms",
                    "included",
                ],
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(created.status_code, 200, created.text)
        for target, confirmation, evidence in (
            ("in_review", "IN_REVIEW TEMPLATE contract-template-v1", ""),
            ("approved", "APPROVED TEMPLATE contract-template-v1", "legal-ticket-9"),
        ):
            moved = self.client.post(
                "/api/internal/building/agreement-readiness/templates/contract-template-v1/transition",
                headers=headers,
                json={
                    "target_status": target,
                    "confirmation": confirmation,
                    "evidence": evidence,
                    "actor": "reviewer@example.com",
                },
            )
            self.assertEqual(moved.status_code, 200, moved.text)

    def test_01_index_is_honest_while_no_template_is_approved(self) -> None:
        page = self.client.get(CONTRACTS)
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("No approved agreement template exists", page.text)
        self.assertIn("Blocked on an approved template", page.text)
        # The legacy free-text row is visible and labeled, never silently hidden.
        self.assertIn("Cordelia Vance", page.text)
        self.assertIn("Unverified", page.text)
        self.assertIn("Showing 1 of 1 contracts", page.text)

    def test_02_legacy_record_offers_no_governed_action(self) -> None:
        detail = self.client.get(f"{CONTRACTS}/legacy-agreement")
        self.assertEqual(detail.status_code, 200, detail.text)
        self.assertIn("Unverified legacy record", detail.text)
        self.assertIn("No frozen terms", detail.text)
        self.assertNotIn("Change agreement readiness", detail.text)

    def test_03_missing_contract_returns_not_found(self) -> None:
        missing = self.client.get(f"{CONTRACTS}/does-not-exist")
        self.assertEqual(missing.status_code, 404, missing.text)
        self.assertIn("Contract not found", missing.text)

    def test_04_operator_prepares_reviews_and_approves_a_contract(self) -> None:
        self._approve_template()
        page = self.client.get(CONTRACTS)
        self.assertNotIn("No approved agreement template exists", page.text)
        self.assertIn("Prepare immutable package", page.text)
        # Pickers offer real records, not typed identifiers.
        self.assertIn('value="contract-event"', page.text)
        self.assertIn('value="contract-quote"', page.text)
        self.assertIn('value="contract-template-v1"', page.text)
        key_match = re.search(r'name="idempotency_key" value="([^"]+)"', page.text)
        self.assertIsNotNone(key_match)

        prepared = self._post(f"{CONTRACTS}/packages", {
            "reservation_id": "contract-event",
            "quote_id": "contract-quote",
            "template_id": "contract-template-v1",
            "idempotency_key": key_match.group(1),
            "agreement_version": "1",
            "payment_version": "1",
        })
        self.assertEqual(prepared.status_code, 303, prepared.text)
        location = prepared.headers["location"]
        self.assertTrue(location.startswith(f"{CONTRACTS}/"), location)
        self.assertIn("notice=", location)
        agreement_id = location.split("?")[0].rsplit("/", 1)[-1]

        detail = self.client.get(f"{CONTRACTS}/{agreement_id}")
        self.assertEqual(detail.status_code, 200, detail.text)
        self.assertIn("Cordelia Vance", detail.text)
        self.assertIn("Contract Arena", detail.text)
        self.assertIn("USD 4,500.00", detail.text)
        self.assertIn("USD 1,350.00", detail.text)
        self.assertIn("Non-refundable inside 30 days.", detail.text)
        self.assertIn("agreement package prepared", detail.text.lower())

        for target, verb in (("in_review", "REVIEW"), ("approved", "APPROVE")):
            moved = self._post(f"{CONTRACTS}/{agreement_id}/transition", {
                "target_status": target,
                "confirmation": f"{verb} AGREEMENT {agreement_id}",
            })
            self.assertEqual(moved.status_code, 303, moved.text)
            self.assertIn("notice=", moved.headers["location"])
        for target, verb in (("in_review", "REVIEW"), ("approved", "APPROVE")):
            moved = self._post(f"{CONTRACTS}/{agreement_id}/payments/transition", {
                "target_status": target,
                "confirmation": f"{verb} PAYMENT {self._payment_id(agreement_id)}",
            })
            self.assertEqual(moved.status_code, 303, moved.text)
            self.assertIn("notice=", moved.headers["location"])

        with self.factory() as session:
            agreement = session.get(BuildingAgreement, agreement_id)
            payment = session.execute(
                __import__("sqlalchemy").select(BuildingPaymentRequestReadiness).where(
                    BuildingPaymentRequestReadiness.agreement_id == agreement_id
                )
            ).scalars().one()
            self.assertEqual(agreement.preparation_status, "approved")
            self.assertEqual(agreement.status, "draft")
            self.assertEqual(agreement.provider, "")
            self.assertEqual(payment.status, "approved")
            self.assertFalse(payment.metadata_json["provider_object_created"])
            self.assertFalse(payment.metadata_json["invoice_created"])
            self.assertFalse(payment.metadata_json["payment_received"])

        approved_page = self.client.get(f"{CONTRACTS}/{agreement_id}")
        self.assertIn("Ready", approved_page.text)

    def _payment_id(self, agreement_id: str) -> str:
        import sqlalchemy

        with self.factory() as session:
            return session.execute(
                sqlalchemy.select(BuildingPaymentRequestReadiness.id).where(
                    BuildingPaymentRequestReadiness.agreement_id == agreement_id
                )
            ).scalars().one()

    def test_05_typed_confirmation_still_fails_closed(self) -> None:
        with self.factory() as session:
            import sqlalchemy

            agreement_id = session.execute(
                sqlalchemy.select(BuildingAgreement.id).where(
                    BuildingAgreement.package_checksum != ""
                )
            ).scalars().first()
        self.assertIsNotNone(agreement_id)
        rejected = self._post(f"{CONTRACTS}/{agreement_id}/transition", {
            "target_status": "approved",
            "confirmation": "approve it",
        })
        self.assertEqual(rejected.status_code, 303, rejected.text)
        self.assertIn("error=", rejected.headers["location"])

    def test_06_search_and_filters_scope_the_workspace(self) -> None:
        everything = self.client.get(CONTRACTS)
        self.assertIn("Showing 2 of 2 contracts", everything.text)

        found = self.client.get(CONTRACTS, params={"q": "cordelia"})
        self.assertEqual(found.status_code, 200)
        self.assertIn("Showing 2 of 2 contracts", found.text)

        by_reservation = self.client.get(CONTRACTS, params={"q": "contract-event"})
        self.assertIn("Showing 2 of 2 contracts", by_reservation.text)

        missing = self.client.get(CONTRACTS, params={"q": "no-such-customer"})
        self.assertIn("Showing 0 of 2 contracts", missing.text)
        self.assertIn("No contracts match this scope", missing.text)

        unverified = self.client.get(CONTRACTS, params={"state": "Unverified"})
        self.assertIn("Showing 1 of 2 contracts", unverified.text)

        workspace_only = self.client.get(CONTRACTS, params={"type": "workspace"})
        self.assertIn("Showing 0 of 2 contracts", workspace_only.text)

    def test_07_legacy_readiness_url_redirects_permanently(self) -> None:
        moved = self.client.get(
            "/admin/building/agreement-readiness", follow_redirects=False
        )
        self.assertEqual(moved.status_code, 308, moved.text)
        self.assertEqual(moved.headers["location"], CONTRACTS)

    def test_08_control_room_routes_agreement_evidence_to_contracts(self) -> None:
        control = self.client.get("/admin/building")
        self.assertEqual(control.status_code, 200, control.text)
        self.assertIn("/admin/building/contracts", control.text)
        self.assertNotIn("Record agreement</button>", control.text)

    def test_08a_one_action_approves_the_whole_ladder(self) -> None:
        """Seven confirmations by the same person say nothing seven times. One
        action does the lot, and every step is still recorded on its own."""
        self._approve_template()
        key_match = re.search(
            r'name="idempotency_key" value="([^"]+)"',
            self.client.get(CONTRACTS).text,
        )
        import sqlalchemy
        with self.factory() as session:
            used = session.execute(
                sqlalchemy.select(BuildingAgreement.version).where(
                    BuildingAgreement.reservation_id == "contract-event"
                )
            ).scalars().all()
        version = str((max(used) + 1) if used else 1)
        prepared = self._post(f"{CONTRACTS}/packages", {
            "reservation_id": "contract-event",
            "quote_id": "contract-quote",
            "template_id": "contract-template-v1",
            "idempotency_key": key_match.group(1),
            "agreement_version": version,
            "payment_version": version,
        })
        self.assertEqual(prepared.status_code, 303, prepared.text)
        self.assertIn("notice=", prepared.headers["location"], prepared.headers["location"])
        agreement_id = prepared.headers["location"].split("?")[0].rsplit("/", 1)[-1]

        page = self.client.get(f"{CONTRACTS}/{agreement_id}")
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("Approve and create the signing copy", page.text)

        moved = self._post(f"{CONTRACTS}/{agreement_id}/ready-to-send", {})
        self.assertEqual(moved.status_code, 303, moved.text)

        import sqlalchemy
        with self.factory() as session:
            agreement = session.get(BuildingAgreement, agreement_id)
            self.assertEqual(agreement.preparation_status, "approved")
            payment_status = session.execute(
                sqlalchemy.select(BuildingPaymentRequestReadiness.status).where(
                    BuildingPaymentRequestReadiness.agreement_id == agreement_id
                )
            ).scalars().one()
            self.assertEqual(payment_status, "approved")
            # Collapsing the clicking must not collapse the record: each move is
            # still its own audit entry.
            actions = session.execute(
                sqlalchemy.select(BuildingAuditEvent.action).where(
                    BuildingAuditEvent.entity_id == agreement_id
                )
            ).scalars().all()
        self.assertGreaterEqual(
            len([item for item in actions if "readiness" in item or "review" in item
                 or "approv" in item]),
            2,
            f"each transition needs its own audit record, saw {actions}",
        )

    def test_08b_one_action_still_needs_approval_rights(self) -> None:
        """Collapsing the steps must not collapse who is allowed to take them."""
        limited_user = {
            "email": "limited@example.com",
            "permissions": {"building.agreements.prepare"},
            "is_superadmin": False,
            "session_issued_at": "",
        }
        index = self.client.get(CONTRACTS)
        ids = re.findall(rf'href="{CONTRACTS}/([a-z0-9-]+)"', index.text)
        self.assertTrue(ids)
        with mock.patch(
            "sales_support_agent.services.auth_deps.get_current_user",
            return_value=limited_user,
        ):
            forbidden = self.client.post(
                f"{CONTRACTS}/{ids[0]}/ready-to-send",
                headers=self.browser_headers,
                follow_redirects=False,
                data={"_csrf_token": "irrelevant"},
            )
        self.assertEqual(forbidden.status_code, 403, forbidden.text)

    def test_09_permission_and_csrf_fail_closed(self) -> None:
        limited_user = {
            "email": "limited@example.com",
            "permissions": {"building.manage"},
            "is_superadmin": False,
            "session_issued_at": "",
        }
        with mock.patch(
            "sales_support_agent.services.auth_deps.get_current_user",
            return_value=limited_user,
        ):
            forbidden = self.client.post(
                f"{CONTRACTS}/packages",
                headers=self.browser_headers,
                follow_redirects=False,
                data={
                    "_csrf_token": "irrelevant",
                    "reservation_id": "contract-event",
                    "quote_id": "contract-quote",
                    "template_id": "contract-template-v1",
                    "idempotency_key": "limited-user-key",
                },
            )
        self.assertEqual(forbidden.status_code, 403, forbidden.text)

        invalid_csrf = self.client.post(
            f"{CONTRACTS}/packages",
            headers=self.browser_headers,
            follow_redirects=False,
            data={
                "_csrf_token": "invalid",
                "reservation_id": "contract-event",
                "quote_id": "contract-quote",
                "template_id": "contract-template-v1",
                "idempotency_key": "invalid-csrf-key",
            },
        )
        self.assertEqual(invalid_csrf.status_code, 403, invalid_csrf.text)

    def test_10_no_send_sign_invoice_or_charge_control_exists(self) -> None:
        index = self.client.get(CONTRACTS)
        detail_ids = re.findall(rf'href="{CONTRACTS}/([a-z0-9-]+)"', index.text)
        self.assertTrue(detail_ids)
        for banned in ("Send contract", "Request signature", "Create invoice", "Charge"):
            self.assertNotIn(banned, index.text)
        for agreement_id in set(detail_ids):
            page = self.client.get(f"{CONTRACTS}/{agreement_id}")
            for banned in ("Send contract", "Request signature", "Create invoice"):
                self.assertNotIn(banned, page.text)


if __name__ == "__main__":
    unittest.main()
