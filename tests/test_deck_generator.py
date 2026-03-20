from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

try:
    from sales_support_agent.models.database import create_session_factory, init_database, session_scope
    from sales_support_agent.models.entities import CanvaConnection
    from sales_support_agent.services.deck_generator import DeckGenerationService
    from sales_support_agent.services.token_seal import seal_token

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


class _FakeGoogleSheetsClient:
    def get_values(self) -> dict[str, object]:
        return {
            "range": "Sales!A1:B3",
            "values": [
                ["Metric", "Value"],
                ["Sales Total", "$12000"],
                ["Win Rate", "25%"],
            ],
        }


class _FakeCanvaClient:
    def __init__(self, *, include_image_field: bool = False):
        self.include_image_field = include_image_field
        self.created_payload: dict[str, object] | None = None

    def get_user_capabilities(self, access_token: str) -> dict[str, object]:
        return {"capabilities": {"autofill": True, "brand_template": True}}

    def refresh_access_token(self, refresh_token: str) -> dict[str, object]:
        return {"access_token": "fresh-access", "refresh_token": refresh_token, "expires_in": 3600}

    def get_brand_template_dataset(self, brand_template_id: str, access_token: str) -> dict[str, object]:
        dataset = {
            "sales_sales_total": {"type": "text"},
            "report_generated_date": {"type": "text"},
            "competitor_table": {"type": "chart"},
        }
        if self.include_image_field:
            dataset["competitor_logo"] = {"type": "image"}
        return {"dataset": dataset}

    def create_autofill_job(
        self,
        *,
        access_token: str,
        brand_template_id: str,
        title: str,
        data: dict[str, object],
    ) -> dict[str, object]:
        self.created_payload = data
        return {"job": {"id": "job-123"}}

    def get_autofill_job(self, job_id: str, access_token: str) -> dict[str, object]:
        return {
            "job": {
                "id": job_id,
                "status": "success",
                "result": {
                    "design": {
                        "id": "design-123",
                        "title": "Sales Deck | 2026-03-16",
                        "urls": {
                            "edit_url": "https://www.canva.com/design/edit-123",
                            "view_url": "https://www.canva.com/design/view-123",
                        },
                    }
                },
            }
        }


def _build_settings(**overrides: object) -> SimpleNamespace:
    values = {
        "google_sheets_spreadsheet_id": "spreadsheet-123",
        "google_sheets_sales_range": "Sales!A1:B3",
        "google_service_account_json": '{"type":"service_account"}',
        "canva_client_id": "client-id",
        "canva_client_secret": "client-secret",
        "canva_redirect_uri": "https://example.com/admin/api/canva/callback",
        "canva_brand_template_id": "brand-template-123",
        "canva_token_secret": "token-secret",
        "deck_canva_poll_interval_seconds": 1,
        "deck_canva_poll_attempts": 1,
        "deck_competitor_required_columns": (),
        "deck_competitor_allowed_columns": (),
        "deck_required_template_fields": (),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for deck generator tests")
class DeckGeneratorTests(unittest.TestCase):
    def test_generate_deck_merges_sources_and_returns_design_links(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        settings = _build_settings()
        canva_client = _FakeCanvaClient()

        with session_scope(session_factory) as session:
            session.add(
                CanvaConnection(
                    display_name="Deck Ops",
                    access_token_encrypted=seal_token(settings.canva_token_secret, "access-token"),
                    refresh_token_encrypted=seal_token(settings.canva_token_secret, "refresh-token"),
                    expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
                    capabilities_json={"autofill": True, "brand_template": True},
                    updated_at=datetime.now(timezone.utc),
                )
            )

        with session_scope(session_factory) as session:
            result = DeckGenerationService(
                settings,
                session,
                google_client=_FakeGoogleSheetsClient(),
                canva_client=canva_client,
            ).generate_deck(
                competitor_csv_bytes=b"Competitor,Score\nAcme,82\nGlobex,77\n",
                competitor_filename="competitors.csv",
                report_date=date(2026, 3, 16),
            )

        self.assertEqual(result.design_id, "design-123")
        self.assertEqual(result.sales_row_count, 2)
        self.assertEqual(result.competitor_row_count, 2)
        self.assertEqual(canva_client.created_payload["sales_sales_total"]["text"], "$12000")
        self.assertEqual(canva_client.created_payload["competitor_table"]["type"], "chart")

    def test_generate_deck_rejects_unsupported_image_fields(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        settings = _build_settings()

        with session_scope(session_factory) as session:
            session.add(
                CanvaConnection(
                    display_name="Deck Ops",
                    access_token_encrypted=seal_token(settings.canva_token_secret, "access-token"),
                    refresh_token_encrypted=seal_token(settings.canva_token_secret, "refresh-token"),
                    expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
                    capabilities_json={"autofill": True, "brand_template": True},
                    updated_at=datetime.now(timezone.utc),
                )
            )

        with session_scope(session_factory) as session:
            service = DeckGenerationService(
                settings,
                session,
                google_client=_FakeGoogleSheetsClient(),
                canva_client=_FakeCanvaClient(include_image_field=True),
            )
            with self.assertRaises(RuntimeError):
                service.generate_deck(
                    competitor_csv_bytes=b"Competitor,Score\nAcme,82\n",
                    competitor_filename="competitors.csv",
                )


if __name__ == "__main__":
    unittest.main()
