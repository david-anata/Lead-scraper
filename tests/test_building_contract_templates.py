from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_contract_templates_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-contract-templates-session-secret",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAgreement,
        BuildingAgreementTemplate,
        BuildingAvailabilityBlock,
        BuildingContact,
        BuildingProposal,
        BuildingReservation,
        BuildingSpace,
    )
    from sales_support_agent.services.admin_auth import create_user_session_token
    from sales_support_agent.services.building_contract_templates import (
        TemplateValidationError,
        document_checksum,
        render_document_text,
        validate_template_content,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False

CONTRACTS = "/admin/building/contracts"
TEMPLATES = "/admin/building/contracts/templates"

BODY = """This agreement is between Anata Building and {{customer_name}}
({{customer_email}}) for use of {{event_space}}.

Guests arrive {{guest_starts_at}} and depart {{guest_ends_at}}. Expected
attendance is {{attendance}}.

The total is {{currency}} {{quote_total}} with {{deposit_amount}} required to
reserve the date.
"""


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class ContractTemplateRenderingTests(unittest.TestCase):
    """Rendering must be deterministic and must never invent a missing value."""

    def test_unknown_token_is_named_and_refused(self) -> None:
        with self.assertRaises(TemplateValidationError) as caught:
            validate_template_content(
                contract_type="event",
                body_markdown="Hello {{customer_name}} and {{secret_margin}}.",
                clauses=[],
            )
        self.assertIn("secret_margin", str(caught.exception))
        self.assertIn("customer_name", str(caught.exception))

    def test_merge_fields_are_derived_from_the_authored_body(self) -> None:
        derived = validate_template_content(
            contract_type="event",
            body_markdown="{{customer_name}} at {{event_space}}.",
            clauses=[{"title": "Cancellation", "body": "{{cancellation_policy}}"}],
        )
        self.assertEqual(
            derived, ["customer_name", "event_space", "cancellation_policy"]
        )

    def test_template_without_any_merge_field_is_refused(self) -> None:
        with self.assertRaises(TemplateValidationError):
            validate_template_content(
                contract_type="event",
                body_markdown="A fixed letter with no merge fields.",
                clauses=[],
            )

    def test_membership_fields_are_rejected_on_an_event_template(self) -> None:
        with self.assertRaises(TemplateValidationError) as caught:
            validate_template_content(
                contract_type="event",
                body_markdown="{{monthly_rate}} for {{customer_name}}.",
                clauses=[],
            )
        self.assertIn("monthly_rate", str(caught.exception))

    def test_rendering_is_deterministic_and_flags_missing_values(self) -> None:
        values = {"customer_name": "Cordelia Vance", "quote_total": 450000}
        first = render_document_text(
            name="Event agreement",
            body_markdown="{{customer_name}} owes {{quote_total}} for {{event_space}}.",
            clauses=[],
            merge_values=values,
        )
        second = render_document_text(
            name="Event agreement",
            body_markdown="{{customer_name}} owes {{quote_total}} for {{event_space}}.",
            clauses=[],
            merge_values=values,
        )
        self.assertEqual(first, second)
        self.assertEqual(document_checksum(first), document_checksum(second))
        self.assertIn("Cordelia Vance", first)
        self.assertIn("4,500.00", first)
        self.assertIn("[not provided]", first)


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class ContractTemplateEditorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"building_contract_templates_{uuid.uuid4().hex}.db"
        )
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="template-editor-internal-key",
            building_campaign_token_secret="template-editor-signing-secret",
        )
        settings = app.state.agent_settings
        token = create_user_session_token(
            settings, email="david@anatainc.com", name="David", role="admin"
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.client.cookies.set(settings.admin_cookie_name, token)
        cls.headers = {"Origin": "http://testserver", "Sec-Fetch-Mode": "navigate"}
        now = datetime.now(timezone.utc)
        start = now + timedelta(days=60)
        with factory() as session:
            session.add_all([
                BuildingSpace(
                    id="template-arena",
                    slug="template-arena",
                    name="Template Arena",
                    space_type="event",
                    capacity=180,
                    status="available",
                ),
                BuildingContact(
                    id="template-host",
                    email="rosalind@example.com",
                    full_name="Rosalind Ferro",
                    status="active",
                ),
            ])
            reservation = BuildingReservation(
                id="template-event",
                kind="event",
                status="soft_hold",
                contact_id="template-host",
                space_id="template-arena",
                starts_at=start,
                guest_starts_at=start + timedelta(hours=2),
                guest_ends_at=start + timedelta(hours=7),
                ends_at=start + timedelta(hours=9),
                hold_expires_at=now + timedelta(days=6),
                attendance=120,
                deposit_required=True,
                assigned_owner="events@example.com",
                created_by="operator@example.com",
            )
            session.add(reservation)
            session.add(BuildingAvailabilityBlock(
                id="template-block",
                space_id="template-arena",
                state="soft_hold",
                starts_at=reservation.starts_at,
                ends_at=reservation.ends_at,
                expires_at=reservation.hold_expires_at,
                source="agent",
                source_reference="reservation:template-event",
            ))
            session.add(BuildingProposal(
                id="template-quote",
                reservation_id="template-event",
                version=1,
                proposal_type="quote",
                status="draft",
                currency="USD",
                amount_cents=620000,
                line_items_json=[{
                    "type": "base",
                    "name": "Reviewed event package",
                    "quantity": 1,
                    "amount_cents": 620000,
                }],
                rate_plan_id="template-rate-v1",
                rate_plan_snapshot_json={
                    "id": "template-rate-v1",
                    "version": 1,
                    "deposit_type": "percent",
                    "deposit_percent_bps": 2500,
                    "cancellation_policy": "Non-refundable inside 21 days.",
                    "tax_status": "non_taxable",
                    "tax_rate_bps": 0,
                    "tax_note": "Reviewed as non-taxable.",
                    "included": ["Venue access"],
                    "addons": [],
                },
                terms_summary="Frozen reviewed quote terms.",
                created_by="operator@example.com",
            ))
            session.commit()

    def _csrf(self) -> str:
        page = self.client.get(TEMPLATES)
        self.assertEqual(page.status_code, 200, page.text)
        match = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(match)
        return match.group(1)

    def _post(self, url: str, data: dict):
        return self.client.post(
            url,
            headers=self.headers,
            follow_redirects=False,
            data={"_csrf_token": self._csrf(), **data},
        )

    def test_01_empty_registry_explains_the_block(self) -> None:
        page = self.client.get(TEMPLATES)
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("No templates yet", page.text)
        self.assertIn("Start a template version", page.text)

    def test_02_author_save_preview_and_approve_a_template(self) -> None:
        created = self._post(TEMPLATES, {
            "template_key": "event-agreement",
            "name": "Event agreement",
            "contract_type": "event",
        })
        self.assertEqual(created.status_code, 303, created.text)
        location = created.headers["location"]
        template_id = location.split("?")[0].rsplit("/", 1)[-1]
        self.assertEqual(template_id, "event-agreement-v1")

        rejected = self._post(f"{TEMPLATES}/{template_id}", {
            "name": "Event agreement",
            "contract_type": "event",
            "body_markdown": "Hello {{customer_name}} and {{profit_margin}}.",
            "clause_title_0": "",
            "clause_body_0": "",
        })
        self.assertEqual(rejected.status_code, 303, rejected.text)
        self.assertIn("error=", rejected.headers["location"])
        editor = self.client.get(rejected.headers["location"])
        self.assertIn("profit_margin", editor.text)
        with self.factory() as session:
            unsaved = session.get(BuildingAgreementTemplate, template_id)
            self.assertEqual(unsaved.body_markdown, "")

        saved = self._post(f"{TEMPLATES}/{template_id}", {
            "name": "Event agreement",
            "contract_type": "event",
            "body_markdown": BODY,
            "clause_title_0": "Cancellation",
            "clause_body_0": "{{cancellation_policy}}",
            "clause_title_1": "",
            "clause_body_1": "",
        })
        self.assertEqual(saved.status_code, 303, saved.text)
        self.assertIn("notice=", saved.headers["location"])

        preview = self.client.get(
            f"{TEMPLATES}/{template_id}", params={"preview": "template-event"}
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertIn("Rosalind Ferro", preview.text)
        self.assertIn("Template Arena", preview.text)
        self.assertIn("6,200.00", preview.text)
        self.assertIn("Non-refundable inside 21 days.", preview.text)
        self.assertNotIn("[not provided]", preview.text)

        for target, verb, evidence in (
            ("in_review", "IN_REVIEW", ""),
            ("approved", "APPROVED", "legal-review-ticket-42"),
        ):
            moved = self._post(f"{TEMPLATES}/{template_id}/transition", {
                "target_status": target,
                "confirmation": f"{verb} TEMPLATE {template_id}",
                "evidence": evidence,
            })
            self.assertEqual(moved.status_code, 303, moved.text)
            self.assertIn("notice=", moved.headers["location"])

    def test_03_approved_template_is_immutable_and_offers_a_next_version(self) -> None:
        template_id = "event-agreement-v1"
        page = self.client.get(f"{TEMPLATES}/{template_id}")
        self.assertIn("Approved and retired versions are immutable", page.text)
        self.assertIn("Start version 2", page.text)

        blocked = self._post(f"{TEMPLATES}/{template_id}", {
            "name": "Rewritten after approval",
            "contract_type": "event",
            "body_markdown": "{{customer_name}} rewritten.",
        })
        self.assertEqual(blocked.status_code, 303, blocked.text)
        self.assertIn("error=", blocked.headers["location"])
        with self.factory() as session:
            row = session.get(BuildingAgreementTemplate, template_id)
            self.assertEqual(row.name, "Event agreement")
            self.assertEqual(row.status, "approved")

        started = self._post(f"{TEMPLATES}/{template_id}/new-version", {})
        self.assertEqual(started.status_code, 303, started.text)
        next_id = started.headers["location"].split("?")[0].rsplit("/", 1)[-1]
        self.assertEqual(next_id, "event-agreement-v2")
        with self.factory() as session:
            copy = session.get(BuildingAgreementTemplate, next_id)
            self.assertEqual(copy.status, "draft")
            self.assertIn("{{customer_name}}", copy.body_markdown)
            self.assertEqual(len(copy.clauses_json), 1)

    def test_04_prepared_package_freezes_the_rendered_document(self) -> None:
        index = self.client.get(CONTRACTS)
        key = re.search(r'name="idempotency_key" value="([^"]+)"', index.text)
        self.assertIsNotNone(key, index.text)
        prepared = self.client.post(
            f"{CONTRACTS}/packages",
            headers=self.headers,
            follow_redirects=False,
            data={
                "_csrf_token": self._csrf(),
                "reservation_id": "template-event",
                "quote_id": "template-quote",
                "template_id": "event-agreement-v1",
                "idempotency_key": key.group(1),
                "agreement_version": "1",
                "payment_version": "1",
            },
        )
        self.assertEqual(prepared.status_code, 303, prepared.text)
        agreement_id = prepared.headers["location"].split("?")[0].rsplit("/", 1)[-1]

        with self.factory() as session:
            agreement = session.get(BuildingAgreement, agreement_id)
            document = agreement.package_snapshot_json["document"]
            self.assertEqual(document["format"], "markdown")
            self.assertIn("Rosalind Ferro", document["text"])
            self.assertNotIn("[not provided]", document["text"])
            self.assertEqual(
                document["checksum"], document_checksum(document["text"])
            )

        detail = self.client.get(f"{CONTRACTS}/{agreement_id}")
        self.assertIn("Frozen document", detail.text)
        # The document stays unavailable until the package is approved.
        self.assertNotIn(f'{CONTRACTS}/{agreement_id}/document"', detail.text)
        early = self.client.get(f"{CONTRACTS}/{agreement_id}/document")
        self.assertEqual(early.status_code, 409, early.text)

        for target, verb in (("in_review", "REVIEW"), ("approved", "APPROVE")):
            moved = self.client.post(
                f"{CONTRACTS}/{agreement_id}/transition",
                headers=self.headers,
                follow_redirects=False,
                data={
                    "_csrf_token": self._csrf(),
                    "target_status": target,
                    "confirmation": f"{verb} AGREEMENT {agreement_id}",
                },
            )
            self.assertEqual(moved.status_code, 303, moved.text)

        document = self.client.get(f"{CONTRACTS}/{agreement_id}/document")
        self.assertEqual(document.status_code, 200, document.text)
        self.assertIn("Rosalind Ferro", document.text)
        self.assertIn("Template Arena", document.text)
        self.assertIn("Print or save as PDF", document.text)
        self.assertIn("has not sent this contract", document.text)
        for banned in ("Send contract", "Request signature", "Create invoice"):
            self.assertNotIn(banned, document.text)

        approved_detail = self.client.get(f"{CONTRACTS}/{agreement_id}")
        self.assertIn(f'{CONTRACTS}/{agreement_id}/document"', approved_detail.text)

    def test_05_external_reference_templates_still_work(self) -> None:
        created = self._post(TEMPLATES, {
            "template_key": "external-agreement",
            "name": "External agreement",
            "contract_type": "event",
        })
        template_id = created.headers["location"].split("?")[0].rsplit("/", 1)[-1]
        saved = self._post(f"{TEMPLATES}/{template_id}", {
            "name": "External agreement",
            "contract_type": "event",
            "template_reference": "approved-repository:external-v1",
            "body_markdown": "",
        })
        self.assertEqual(saved.status_code, 303, saved.text)
        # No body and no merge fields: the draft is refused rather than saved
        # as an empty contract.
        self.assertIn("error=", saved.headers["location"])


if __name__ == "__main__":
    unittest.main()
