from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

try:
    from sales_support_agent.models.database import create_session_factory, init_database, session_scope
    from sales_support_agent.models.entities import CanvaConnection
    from sales_support_agent.integrations.amazon_sp_api import AmazonCatalogSnapshot
    from sales_support_agent.integrations.shopify import ShopifyProductSnapshot
    from sales_support_agent.services.deck_generator import DeckGenerationService
    from sales_support_agent.services.token_seal import seal_token

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


class _FakeGoogleSheetsClient:
    def __init__(self, values: list[list[str]] | None = None) -> None:
        self.values = values or [
            ["Metric", "Value"],
            ["Sales Total", "$12000"],
            ["Win Rate", "25%"],
        ]

    def get_values(self) -> dict[str, object]:
        return {
            "range": "Sales!A1:B3",
            "values": self.values,
        }


class _FakeCanvaClient:
    def __init__(
        self,
        *,
        include_image_field: bool = False,
        include_top_products_chart: bool = False,
        dataset_override: dict[str, dict[str, object]] | None = None,
    ):
        self.include_image_field = include_image_field
        self.include_top_products_chart = include_top_products_chart
        self.dataset_override = dataset_override
        self.created_payload: dict[str, object] | None = None

    def get_user_capabilities(self, access_token: str) -> dict[str, object]:
        return {"capabilities": {"autofill": True, "brand_template": True}}

    def refresh_access_token(self, refresh_token: str) -> dict[str, object]:
        return {"access_token": "fresh-access", "refresh_token": refresh_token, "expires_in": 3600}

    def get_brand_template_dataset(self, brand_template_id: str, access_token: str) -> dict[str, object]:
        if self.dataset_override is not None:
            return {"dataset": self.dataset_override}
        dataset = {
            "sales_sales_total": {"type": "text"},
            "report_generated_date": {"type": "text"},
            "competitor_table": {"type": "chart"},
        }
        if self.include_top_products_chart:
            dataset["top_products_by_bsr"] = {"type": "chart"}
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


class _FakeShopifyClient:
    def fetch_product(self, product_url: str) -> ShopifyProductSnapshot:
        return ShopifyProductSnapshot(
            source_url=product_url,
            domain="bonpatch.com",
            handle="clarity-patch",
            brand_name="BonPatch",
            title="Clarity Patch",
            description="A short hero description.",
            price="$34.00",
            currency="USD",
            image_url="https://cdn.example.com/clarity-patch.jpg",
            product_type="Patch",
            tags=("focus", "energy"),
            vendor="BonPatch",
        )


class _FakeAmazonClient:
    def __init__(self, *, configured: bool = True) -> None:
        self.configured = configured

    def is_configured(self) -> bool:
        return self.configured

    def get_catalog_item(self, asin: str, *, source_url: str = "") -> AmazonCatalogSnapshot:
        return AmazonCatalogSnapshot(
            asin=asin,
            title=f"Catalog {asin}",
            brand="Rival Brand",
            category="Patches",
            bsr="1250",
            dimensions="4 in x 3 in x 1 in",
            package_dimensions="5 in x 4 in x 2 in",
            marketplace_id="ATVPDKIKX0DER",
            source_url=source_url or f"https://www.amazon.com/dp/{asin}",
            raw_payload={},
        )


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

    def test_generate_deck_builds_top_products_by_bsr_chart(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        settings = _build_settings()
        canva_client = _FakeCanvaClient(include_top_products_chart=True)
        google_client = _FakeGoogleSheetsClient(
            values=[
                ["Product name", "BSR", "Sales", "Units", "Change from previous period"],
                ["Alpha Serum", "1450", "$12,000", "220", "+8%"],
                ["Beta Cream", "320", "$9,500", "180", "+4%"],
                ["Gamma Wash", "980", "$8,100", "160", "-2%"],
            ]
        )

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
            DeckGenerationService(
                settings,
                session,
                google_client=google_client,
                canva_client=canva_client,
            ).generate_deck(
                competitor_csv_bytes=b"Competitor,Score\nAcme,82\n",
                competitor_filename="competitors.csv",
                report_date=date(2026, 3, 16),
            )

        chart_rows = canva_client.created_payload["top_products_by_bsr"]["chart_data"]["rows"]
        self.assertEqual(chart_rows[0]["cells"][0]["value"], "Product name")
        self.assertEqual(chart_rows[1]["cells"][0]["value"], "Beta Cream")
        self.assertEqual(chart_rows[1]["cells"][1]["value"], 320.0)
        self.assertEqual(chart_rows[2]["cells"][0]["value"], "Gamma Wash")
        self.assertEqual(chart_rows[3]["cells"][0]["value"], "Alpha Serum")

    def test_generate_deck_supports_automation_first_template_inputs(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        settings = _build_settings(
            google_sheets_spreadsheet_id="",
            google_sheets_sales_range="",
            google_service_account_json="",
        )
        canva_client = _FakeCanvaClient(
            dataset_override={
                "brand_name": {"type": "text"},
                "hero_product_name": {"type": "text"},
                "market_summary": {"type": "text"},
                "competitor_1_name": {"type": "text"},
                "competitor_table": {"type": "chart"},
                "top_products_by_bsr": {"type": "chart"},
            }
        )

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
                shopify_client=_FakeShopifyClient(),
                amazon_client=_FakeAmazonClient(),
            ).generate_deck(
                shopify_product_url="https://bonpatch.com/products/clarity-patch",
                competitor_inputs=[
                    "https://www.amazon.com/dp/B000000001",
                    "B000000002",
                ],
                run_label="Automation Deck",
                report_date=date(2026, 3, 21),
            )

        self.assertEqual(result.competitor_row_count, 2)
        self.assertEqual(canva_client.created_payload["brand_name"]["text"], "BonPatch")
        self.assertEqual(canva_client.created_payload["hero_product_name"]["text"], "Clarity Patch")
        self.assertEqual(canva_client.created_payload["competitor_1_name"]["text"], "Catalog B000000001")
        self.assertEqual(canva_client.created_payload["market_summary"]["text"].startswith("We are benchmarking BonPatch"), True)
        chart_rows = canva_client.created_payload["competitor_table"]["chart_data"]["rows"]
        self.assertEqual(chart_rows[0]["cells"][0]["value"], "competitor")
        self.assertEqual(chart_rows[1]["cells"][0]["value"], "Catalog B000000001")
        self.assertEqual(chart_rows[1]["cells"][1]["value"], 1250.0)

    def test_generate_deck_supports_amazon_target_product_input(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        settings = _build_settings(
            google_sheets_spreadsheet_id="",
            google_sheets_sales_range="",
            google_service_account_json="",
        )
        canva_client = _FakeCanvaClient(
            dataset_override={
                "brand_name": {"type": "text"},
                "hero_product_name": {"type": "text"},
                "hero_product_input_type": {"type": "text"},
                "hero_product_dimensions": {"type": "text"},
                "competitor_table": {"type": "chart"},
            }
        )

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
            DeckGenerationService(
                settings,
                session,
                google_client=_FakeGoogleSheetsClient(),
                canva_client=canva_client,
                shopify_client=_FakeShopifyClient(),
                amazon_client=_FakeAmazonClient(),
            ).generate_deck(
                target_product_input="B08DK5RDJV",
                competitor_inputs=["B08YRDBFFX"],
                run_label="Amazon Target Deck",
                report_date=date(2026, 3, 22),
            )

        self.assertEqual(canva_client.created_payload["hero_product_input_type"]["text"], "amazon")
        self.assertEqual(canva_client.created_payload["hero_product_name"]["text"], "Catalog B08DK5RDJV")
        self.assertEqual(canva_client.created_payload["hero_product_dimensions"]["text"], "4 in x 3 in x 1 in")


if __name__ == "__main__":
    unittest.main()
