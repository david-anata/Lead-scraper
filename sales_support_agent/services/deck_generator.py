"""Deck generation service for the Amazon-first HTML sales deck output."""

from __future__ import annotations

import html
import io
import json
import re
import secrets
import time
import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.integrations.amazon_sp_api import AmazonSpApiClient
from sales_support_agent.integrations.canva import CanvaClient
from sales_support_agent.integrations.google_sheets import GoogleSheetsClient
from sales_support_agent.integrations.shopify import ShopifyStorefrontClient
from sales_support_agent.models.entities import AutomationRun, CanvaConnection
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.helium10 import (
    DistributionSlice,
    Helium10KeywordReport,
    Helium10XrayReport,
    KeywordInsight,
    XrayProduct,
    parse_keyword_csv,
    parse_xray_csv,
)
from sales_support_agent.services.product_research import ProductResearchService
from sales_support_agent.services.token_seal import seal_token, unseal_token


@dataclass(frozen=True)
class DeckDataset:
    text_fields: dict[str, str]
    chart_fields: dict[str, dict[str, Any]]
    warnings: list[str]
    sales_row_count: int
    competitor_row_count: int
    deck_payload: dict[str, Any]


@dataclass(frozen=True)
class DeckGenerationResult:
    run_id: int
    status: str
    message: str
    output_type: str
    design_id: str
    design_title: str
    edit_url: str
    view_url: str
    warnings: list[str]
    sales_row_count: int
    competitor_row_count: int
    template_fields: int


class DeckGenerationService:
    def __init__(
        self,
        settings: Settings,
        session: Session,
        *,
        google_client: GoogleSheetsClient | None = None,
        canva_client: CanvaClient | None = None,
        shopify_client: ShopifyStorefrontClient | None = None,
        amazon_client: AmazonSpApiClient | None = None,
    ):
        self.settings = settings
        self.session = session
        self.google_client = google_client or GoogleSheetsClient(settings)
        self.canva_client = canva_client or CanvaClient(settings)
        self.shopify_client = shopify_client or ShopifyStorefrontClient(settings)
        self.amazon_client = amazon_client or AmazonSpApiClient(settings)
        self.audit = AuditService(session)
        self.product_research = ProductResearchService(
            shopify_client=self.shopify_client,
            amazon_client=self.amazon_client,
        )

    def connect_canva(self, *, code: str, code_verifier: str) -> CanvaConnection:
        payload = self.canva_client.exchange_code(code=code, code_verifier=code_verifier)
        access_token = str(payload.get("access_token") or "").strip()
        refresh_token = str(payload.get("refresh_token") or "").strip()
        if not access_token or not refresh_token:
            raise RuntimeError("Canva OAuth did not return both access and refresh tokens.")

        capabilities_payload = self.canva_client.get_user_capabilities(access_token)
        connection = self._latest_canva_connection() or CanvaConnection()
        connection.canva_user_id = str(payload.get("user_id") or connection.canva_user_id or "").strip()
        connection.display_name = str(payload.get("username") or connection.display_name or "Connected Canva user").strip()
        connection.scope = self._scope_string(payload.get("scope"))
        connection.access_token_encrypted = seal_token(self.settings.canva_token_secret, access_token)
        connection.refresh_token_encrypted = seal_token(self.settings.canva_token_secret, refresh_token)
        connection.token_type = str(payload.get("token_type") or "Bearer").strip() or "Bearer"
        connection.expires_at = _expires_at_from_payload(payload)
        connection.capabilities_json = _normalize_capabilities(capabilities_payload)
        connection.last_validated_at = datetime.now(timezone.utc)
        connection.updated_at = datetime.now(timezone.utc)
        self.session.add(connection)
        self.session.flush()
        return connection

    def list_recent_runs(self, *, limit: int = 5) -> list[dict[str, Any]]:
        runs = list(
            self.session.execute(
                select(AutomationRun)
                .where(AutomationRun.run_type == "deck_generation")
                .order_by(AutomationRun.started_at.desc())
                .limit(limit)
            ).scalars()
        )
        return [self._run_summary(run) for run in runs]

    def get_connection_summary(self) -> dict[str, Any]:
        connection = self._latest_canva_connection()
        capabilities = dict(connection.capabilities_json) if connection else {}
        return {
            "connected": connection is not None,
            "display_name": connection.display_name if connection else "",
            "capabilities": {
                "autofill": bool(capabilities.get("autofill")),
                "brand_template": bool(capabilities.get("brand_template")),
            },
            "last_validated_at": connection.last_validated_at.isoformat() if connection and connection.last_validated_at else "",
        }

    def generate_deck(
        self,
        *,
        competitor_xray_csv_bytes: bytes | None = None,
        competitor_xray_filename: str = "",
        keyword_xray_csv_bytes: bytes | None = None,
        keyword_xray_filename: str = "",
        target_product_input: str = "",
        channels: list[str] | None = None,
        trigger: str = "admin_dashboard",
    ) -> DeckGenerationResult:
        effective_target_input = target_product_input.strip()
        enabled_channels = _normalize_channels(channels or [])
        run = self.audit.start_run(
            "deck_generation",
            trigger=trigger,
            metadata={
                "generation_mode": "amazon_first_html",
                "target_product_input": effective_target_input,
                "competitor_xray_filename": competitor_xray_filename,
                "keyword_xray_filename": keyword_xray_filename,
                "channels": enabled_channels,
            },
        )
        try:
            dataset = self._build_amazon_first_dataset(
                target_product_input=effective_target_input,
                competitor_xray_csv_bytes=competitor_xray_csv_bytes,
                keyword_xray_csv_bytes=keyword_xray_csv_bytes,
                channels=enabled_channels,
            )
            title = str(dataset.deck_payload.get("deck_title") or self._build_design_title(title_hint=effective_target_input)).strip()
            result = self._generate_html_deck(
                run=run,
                title=title,
                dataset=dataset,
                warnings=list(dataset.warnings),
            )
            self.audit.finish_run(
                run,
                status="success",
                summary={
                    "status": "success",
                    "message": result.message,
                    "output_type": result.output_type,
                    "design_id": result.design_id,
                    "design_title": result.design_title,
                    "edit_url": result.edit_url,
                    "view_url": result.view_url,
                    "warnings": result.warnings,
                    "sales_row_count": result.sales_row_count,
                    "competitor_row_count": result.competitor_row_count,
                    "template_fields": result.template_fields,
                    "export_token": run.summary_json.get("export_token") if isinstance(run.summary_json, dict) else "",
                    "deck_html": run.summary_json.get("deck_html") if isinstance(run.summary_json, dict) else "",
                    "deck_slug": run.summary_json.get("deck_slug") if isinstance(run.summary_json, dict) else "",
                    "target_product_identifier": run.summary_json.get("target_product_identifier") if isinstance(run.summary_json, dict) else "",
                    "channels": run.summary_json.get("channels") if isinstance(run.summary_json, dict) else [],
                    "view_count": run.summary_json.get("view_count") if isinstance(run.summary_json, dict) else 0,
                    "first_viewed_at": run.summary_json.get("first_viewed_at") if isinstance(run.summary_json, dict) else "",
                    "last_viewed_at": run.summary_json.get("last_viewed_at") if isinstance(run.summary_json, dict) else "",
                },
            )
            return result
        except Exception as exc:
            self.audit.finish_run(
                run,
                status="failed",
                summary={
                    "status": "failed",
                    "message": str(exc),
                },
            )
            raise

    def _build_design_title(self, *, title_hint: str) -> str:
        cleaned = re.sub(r"\s+", " ", title_hint or "").strip()
        if cleaned:
            return f"Anata Sales Deck | {cleaned}"[:255]
        return f"Anata Sales Deck | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"[:255]

    def _generate_deliverable(
        self,
        *,
        run: AutomationRun,
        title: str,
        dataset: DeckDataset,
        warnings: list[str],
    ) -> DeckGenerationResult:
        return self._generate_html_deck(run=run, title=title, dataset=dataset, warnings=warnings)

    def _generate_canva_deck(
        self,
        *,
        run: AutomationRun,
        title: str,
        dataset: DeckDataset,
        warnings: list[str],
    ) -> DeckGenerationResult:
        access_token = self._ensure_canva_access_token()
        template_payload = self.canva_client.get_brand_template_dataset(
            self.settings.canva_brand_template_id,
            access_token,
        )
        template_dataset = dict(template_payload.get("dataset", {}) or {})
        canva_data, canva_warnings = self._prepare_canva_data(template_dataset, dataset)
        warnings = [*warnings, *canva_warnings]

        job_payload = self.canva_client.create_autofill_job(
            access_token=access_token,
            brand_template_id=self.settings.canva_brand_template_id,
            title=title,
            data=canva_data,
        )
        job_id = str(dict(job_payload.get("job", {})).get("id") or job_payload.get("id") or "").strip()
        if not job_id:
            raise RuntimeError(f"Canva autofill did not return a job id: {job_payload}")

        final_payload = self._wait_for_autofill(job_id=job_id, access_token=access_token)
        job_details = dict(final_payload.get("job", {}) or {})
        if job_details.get("status") != "success":
            error_details = dict(job_details.get("error", {}) or {})
            raise RuntimeError(error_details.get("message") or f"Canva autofill failed: {job_details}")

        design = dict(dict(job_details.get("result", {}) or {}).get("design", {}) or {})
        urls = dict(design.get("urls", {}) or {})
        return DeckGenerationResult(
            run_id=run.id,
            status="success",
            message="Deck generated successfully.",
            output_type="canva",
            design_id=str(design.get("id") or "").strip(),
            design_title=str(design.get("title") or title).strip(),
            edit_url=str(urls.get("edit_url") or design.get("url") or "").strip(),
            view_url=str(urls.get("view_url") or design.get("url") or "").strip(),
            warnings=warnings,
            sales_row_count=dataset.sales_row_count,
            competitor_row_count=dataset.competitor_row_count,
            template_fields=len(template_dataset),
        )

    def _generate_html_deck(
        self,
        *,
        run: AutomationRun,
        title: str,
        dataset: DeckDataset,
        warnings: list[str],
    ) -> DeckGenerationResult:
        export_token = secrets.token_urlsafe(18)
        html_content = self._render_html_deck(title=title, dataset=dataset, warnings=warnings)
        deck_slug = _slugify(title) or f"deck-{run.id}"
        view_url = self._build_export_url(run_id=run.id, deck_slug=deck_slug, token=export_token)
        target_identifier = str(
            dataset.deck_payload.get("target", {}).get("asin")
            or dataset.deck_payload.get("target", {}).get("source_url")
            or ""
        ).strip()
        run.summary_json = {
            "status": "success",
            "message": "Deck generated successfully as an HTML report.",
            "output_type": "html",
            "design_id": f"html-deck-{run.id}",
            "design_title": title,
            "edit_url": view_url,
            "view_url": view_url,
            "warnings": warnings,
            "sales_row_count": dataset.sales_row_count,
            "competitor_row_count": dataset.competitor_row_count,
            "template_fields": 0,
            "export_token": export_token,
            "deck_html": html_content,
            "deck_slug": deck_slug,
            "target_product_identifier": target_identifier,
            "channels": list(dataset.deck_payload.get("channels", [])),
            "view_count": int(dict(run.summary_json or {}).get("view_count", 0) or 0),
            "first_viewed_at": str(dict(run.summary_json or {}).get("first_viewed_at", "") or ""),
            "last_viewed_at": str(dict(run.summary_json or {}).get("last_viewed_at", "") or ""),
        }
        self.session.add(run)
        self.session.flush()
        return DeckGenerationResult(
            run_id=run.id,
            status="success",
            message="Deck generated successfully as an HTML report.",
            output_type="html",
            design_id=f"html-deck-{run.id}",
            design_title=title,
            edit_url=view_url,
            view_url=view_url,
            warnings=warnings,
            sales_row_count=dataset.sales_row_count,
            competitor_row_count=dataset.competitor_row_count,
            template_fields=0,
        )

    def _build_dataset(
        self,
        *,
        sales_payload: dict[str, Any],
        competitor_csv_bytes: bytes,
        report_date: date | None,
        reporting_period: str,
    ) -> DeckDataset:
        text_fields: dict[str, str] = {}
        chart_fields: dict[str, dict[str, Any]] = {}
        warnings: list[str] = []

        values = [[str(cell) for cell in row] for row in sales_payload.get("values", [])]
        if not values:
            raise RuntimeError("Google Sheets returned no sales data for the configured range.")
        sales_rows, sales_scalar_fields = _normalize_sales_rows(values)
        text_fields.update(sales_scalar_fields)
        chart_fields["sales_table"] = _build_chart_data(sales_rows)
        top_products_rows = _build_top_products_by_bsr_rows(sales_rows)
        if top_products_rows is not None:
            chart_fields["top_products_by_bsr"] = _build_chart_data(top_products_rows)
            text_fields["top_products_by_bsr_row_count"] = str(max(len(top_products_rows) - 1, 0))
        text_fields["sales_source_range"] = str(sales_payload.get("range") or self.settings.google_sheets_sales_range)
        text_fields["sales_row_count"] = str(max(len(sales_rows) - 1, 0))

        competitor_rows, competitor_scalar_fields, competitor_warnings = self._parse_competitor_csv(competitor_csv_bytes)
        warnings.extend(competitor_warnings)
        text_fields.update(competitor_scalar_fields)
        chart_fields["competitor_table"] = _build_chart_data(competitor_rows)
        text_fields["competitor_row_count"] = str(max(len(competitor_rows) - 1, 0))

        effective_date = report_date or date.today()
        text_fields["report_generated_date"] = effective_date.isoformat()
        if reporting_period.strip():
            text_fields["reporting_period"] = reporting_period.strip()

        return DeckDataset(
            text_fields=text_fields,
            chart_fields=chart_fields,
            warnings=warnings,
            sales_row_count=max(len(sales_rows) - 1, 0),
            competitor_row_count=max(len(competitor_rows) - 1, 0),
            deck_payload={},
        )

    def _parse_competitor_csv(self, content: bytes) -> tuple[list[list[str]], dict[str, str], list[str]]:
        decoded = content.decode("utf-8-sig").strip()
        if not decoded:
            raise RuntimeError("Competitor CSV upload is empty.")
        reader = csv.DictReader(io.StringIO(decoded))
        raw_headers = [str(header or "").strip() for header in (reader.fieldnames or [])]
        if not raw_headers or any(not header for header in raw_headers):
            raise RuntimeError("Competitor CSV must contain a single header row with non-empty column names.")

        normalized_headers = [_normalize_key(header) for header in raw_headers]
        if len(set(normalized_headers)) != len(normalized_headers):
            raise RuntimeError("Competitor CSV contains duplicate headers after normalization.")

        required_headers = {_normalize_key(value) for value in self.settings.deck_competitor_required_columns}
        allowed_headers = {_normalize_key(value) for value in self.settings.deck_competitor_allowed_columns}
        header_set = set(normalized_headers)
        missing_required = sorted(required_headers - header_set)
        if missing_required:
            raise RuntimeError(f"Competitor CSV is missing required columns: {', '.join(missing_required)}")
        if allowed_headers:
            unexpected = sorted(header_set - allowed_headers)
            if unexpected:
                raise RuntimeError(f"Competitor CSV contains unsupported columns: {', '.join(unexpected)}")

        rows = []
        text_fields: dict[str, str] = {}
        data_rows = list(reader)
        if not data_rows:
            raise RuntimeError("Competitor CSV must include at least one data row.")
        rows.append(raw_headers)
        for row_index, row in enumerate(data_rows, start=1):
            ordered_values = [str(row.get(header, "") or "").strip() for header in raw_headers]
            rows.append(ordered_values)
            for header, value in zip(normalized_headers, ordered_values):
                text_fields[f"competitor_row_{row_index}_{header}"] = value
            if row_index == 1:
                for header, value in zip(normalized_headers, ordered_values):
                    text_fields[f"competitor_{header}"] = value

        warnings: list[str] = []
        if len(data_rows) > 99:
            warnings.append("Canva chart fields support up to 100 rows including headers; only the first 99 data rows were sent.")
            rows = rows[:100]
        if len(raw_headers) > 20:
            warnings.append("Canva chart fields support up to 20 columns; only the first 20 columns were sent.")
            rows = [[cell for cell in row[:20]] for row in rows]
        return rows, text_fields, warnings

    def _build_amazon_first_dataset(
        self,
        *,
        target_product_input: str,
        competitor_xray_csv_bytes: bytes | None,
        keyword_xray_csv_bytes: bytes | None,
        channels: list[str],
    ) -> DeckDataset:
        parsed_target = _parse_target_product_input(target_product_input)
        if parsed_target["source_type"] != "amazon" or not parsed_target["asin"]:
            raise RuntimeError("Target product must be an Amazon ASIN or Amazon product URL.")
        if competitor_xray_csv_bytes is None:
            raise RuntimeError("Competitor Xray CSV is required.")

        xray_report = parse_xray_csv(competitor_xray_csv_bytes)
        keyword_report = parse_keyword_csv(keyword_xray_csv_bytes)
        hero_product = self.product_research.enrich_target_product(parsed_target)
        warnings: list[str] = [*xray_report.warnings, *hero_product.warnings]
        if keyword_report:
            warnings.extend(keyword_report.warnings)

        target_row = xray_report.find_by_asin(parsed_target["asin"])
        primary_competitors = [product for product in xray_report.products if product.asin.upper() != parsed_target["asin"].upper()][:5]
        market_cards = _build_market_metric_cards(xray_report, keyword_report)
        keyword_cards = _build_keyword_metric_cards(keyword_report)
        seo_recommendations = _build_seo_recommendations(keyword_report, xray_report)
        cro_recommendations = _build_cro_recommendations(target_row, primary_competitors)
        creative_recommendations = _build_creative_recommendations(target_row, primary_competitors)
        channel_sections = _build_channel_sections(channels)
        competitor_rows = [["Product", "ASIN", "Brand", "Price", "Revenue", "Reviews", "BSR", "Fulfillment"]]
        for product in xray_report.products[:10]:
            competitor_rows.append(
                [
                    product.title,
                    product.asin,
                    product.brand,
                    product.price_label,
                    product.revenue_label,
                    str(product.review_count or ""),
                    product.bsr_label,
                    product.fulfillment,
                ]
            )

        keyword_rows = [["Keyword", "Search Volume", "Keyword Sales", "Competing Products", "Title Density"]]
        if keyword_report:
            for keyword in keyword_report.keywords[:10]:
                keyword_rows.append(
                    [
                        keyword.phrase,
                        keyword.search_volume_label,
                        keyword.keyword_sales_label,
                        str(keyword.competing_products or ""),
                        str(keyword.title_density or ""),
                    ]
                )

        target_title = (hero_product.title or parsed_target["product_name"] or parsed_target["asin"]).strip()
        target_brand = (
            hero_product.brand_name
            or (target_row.brand if target_row else "")
            or parsed_target.get("brand_name", "")
            or "Amazon Brand"
        ).strip()
        target_image_url = (hero_product.image_url or (target_row.image_url if target_row else "")).strip()
        target_price_label = (
            hero_product.price
            or (target_row.price_label if target_row else "")
            or "Unavailable"
        ).strip()
        target_bsr_label = (target_row.bsr_label if target_row else "").strip()
        target_review_count = target_row.review_count if target_row else None
        target_rating_label = (target_row.rating_label if target_row else "").strip()
        target_revenue_label = (target_row.revenue_label if target_row else "").strip()
        target_dimensions = (hero_product.dimensions or (target_row.dimensions if target_row else "")).strip()

        text_fields: dict[str, str] = {
            "deck_mode": "amazon_first_html",
            "brand_name": target_brand,
            "hero_product_name": target_title,
            "hero_product_source_url": hero_product.source_url or parsed_target["source_url"],
            "hero_product_input_type": parsed_target["source_type"],
            "hero_product_price": target_price_label,
            "hero_product_bsr": target_bsr_label,
            "hero_product_dimensions": target_dimensions,
            "hero_product_description": hero_product.description,
            "hero_product_type": hero_product.product_type or (target_row.category if target_row else ""),
            "hero_product_tags": ", ".join(hero_product.tags),
            "hero_product_image_url": target_image_url,
            "hero_product_snapshot": _build_target_snapshot_text(target_title, target_brand, target_row),
            "report_generated_date": datetime.now(timezone.utc).date().isoformat(),
            "reporting_period": datetime.now(timezone.utc).strftime("%B %d, %Y"),
            "market_summary": _build_market_summary(target_brand, xray_report, keyword_report),
            "executive_summary": _build_executive_summary(target_title, target_brand, xray_report, keyword_report),
            "cro_summary": " ".join(cro_recommendations[:2]),
            "seo_summary": " ".join(seo_recommendations[:2]),
            "creative_summary": " ".join(creative_recommendations[:2]),
            "advertising_summary": _build_advertising_summary(xray_report, keyword_report),
            "recommended_plan_summary": _build_plan_summary(channels),
            "expected_impact_summary": _build_expected_impact_summary(xray_report),
            "why_anata_summary": _build_why_anata_summary(channels),
            "cta_summary": f"Use this deck to align on the first growth sprint for {target_brand} and track whether the deck was viewed.",
            "deck_title": f"{target_brand} x anata - {target_title}",
            "target_asin": parsed_target["asin"],
            "target_rating": target_rating_label,
            "target_review_count": str(target_review_count or ""),
            "target_revenue": target_revenue_label,
        }
        for slot, product in enumerate(primary_competitors, start=1):
            text_fields[f"competitor_{slot}_name"] = product.title
            text_fields[f"competitor_{slot}_asin"] = product.asin
            text_fields[f"competitor_{slot}_brand"] = product.brand
            text_fields[f"competitor_{slot}_source_url"] = product.url
            text_fields[f"competitor_{slot}_image_url"] = product.image_url
            text_fields[f"competitor_{slot}_category"] = product.category
            text_fields[f"competitor_{slot}_bsr"] = product.bsr_label
            text_fields[f"competitor_{slot}_estimated_sales"] = product.revenue_label
            text_fields[f"competitor_{slot}_units"] = product.units_label
            text_fields[f"competitor_{slot}_review_count"] = str(product.review_count or "")
            text_fields[f"competitor_{slot}_strength"] = _build_competitor_strength(product)
            text_fields[f"competitor_{slot}_gap"] = _build_competitor_gap(product, target_row)

        text_fields["competitor_row_count"] = str(max(len(competitor_rows) - 1, 0))
        text_fields["sales_row_count"] = str(len(xray_report.products))
        return DeckDataset(
            text_fields=text_fields,
            chart_fields={
                "competitor_table": _build_chart_data(competitor_rows),
                "keyword_table": _build_chart_data(keyword_rows),
            },
            warnings=warnings,
            sales_row_count=len(xray_report.products),
            competitor_row_count=max(len(competitor_rows) - 1, 0),
            deck_payload={
                "deck_title": text_fields["deck_title"],
                "target": {
                    "asin": parsed_target["asin"],
                    "source_url": text_fields["hero_product_source_url"],
                    "title": target_title,
                    "brand_name": target_brand,
                    "image_url": target_image_url,
                    "price": target_price_label,
                    "bsr": target_bsr_label,
                    "rating": target_rating_label,
                    "review_count": target_review_count or 0,
                    "revenue": target_revenue_label,
                    "dimensions": target_dimensions,
                    "description": hero_product.description,
                    "type": text_fields["hero_product_type"],
                },
                "market_cards": market_cards,
                "keyword_cards": keyword_cards,
                "xray_report": xray_report,
                "keyword_report": keyword_report,
                "primary_competitors": primary_competitors,
                "seo_recommendations": seo_recommendations,
                "cro_recommendations": cro_recommendations,
                "creative_recommendations": creative_recommendations,
                "channel_sections": channel_sections,
                "channels": channels,
                "niche_keyword": keyword_report.keywords[0].phrase if keyword_report and keyword_report.keywords else parsed_target["asin"],
            },
        )

    def _prepare_canva_data(
        self,
        template_dataset: dict[str, dict[str, Any]],
        dataset: DeckDataset,
    ) -> tuple[dict[str, Any], list[str]]:
        if not template_dataset:
            raise RuntimeError("The configured Canva brand template does not expose any autofill fields yet.")

        canva_data: dict[str, Any] = {}
        warnings: list[str] = []
        missing_fields: list[str] = []
        unsupported_fields: list[str] = []

        required_fields = {_normalize_key(value) for value in self.settings.deck_required_template_fields}
        for field_name, definition in template_dataset.items():
            field_type = str(dict(definition).get("type") or "").strip().lower()
            normalized_name = _normalize_key(field_name)
            is_required = not required_fields or normalized_name in required_fields
            if field_type == "text":
                value = dataset.text_fields.get(normalized_name)
                if value is None:
                    if is_required:
                        missing_fields.append(normalized_name)
                    continue
                canva_data[field_name] = {"type": "text", "text": value}
            elif field_type == "chart":
                chart_data = dataset.chart_fields.get(normalized_name)
                if chart_data is None:
                    if is_required:
                        missing_fields.append(normalized_name)
                    continue
                canva_data[field_name] = {"type": "chart", "chart_data": chart_data}
            elif field_type == "image":
                unsupported_fields.append(normalized_name)
            else:
                warnings.append(f"Skipped unsupported Canva field type '{field_type}' for '{normalized_name}'.")

        if missing_fields:
            raise RuntimeError(
                "Template fields are missing matching backend data keys: "
                + ", ".join(sorted(missing_fields))
            )
        if unsupported_fields:
            raise RuntimeError(
                "Image autofill fields are not wired in this v1 implementation: "
                + ", ".join(sorted(unsupported_fields))
            )
        if not canva_data:
            raise RuntimeError("No overlapping template fields were found between the Canva template and the generated dataset.")
        return canva_data, warnings

    def _ensure_canva_access_token(self) -> str:
        connection = self._latest_canva_connection()
        if connection is None:
            raise RuntimeError("Canva is not connected yet. Connect Canva from the admin dashboard first.")

        now = datetime.now(timezone.utc)
        expires_at = connection.expires_at
        if expires_at is not None and expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at is None or expires_at <= now + timedelta(minutes=2):
            refresh_token = unseal_token(self.settings.canva_token_secret, connection.refresh_token_encrypted)
            payload = self.canva_client.refresh_access_token(refresh_token)
            refreshed_access_token = str(payload.get("access_token") or "").strip()
            if not refreshed_access_token:
                raise RuntimeError(f"Canva token refresh failed: {payload}")
            refreshed_refresh_token = str(payload.get("refresh_token") or refresh_token).strip()
            connection.access_token_encrypted = seal_token(self.settings.canva_token_secret, refreshed_access_token)
            connection.refresh_token_encrypted = seal_token(self.settings.canva_token_secret, refreshed_refresh_token)
            connection.scope = self._scope_string(payload.get("scope")) or connection.scope
            connection.expires_at = _expires_at_from_payload(payload)
            access_token = refreshed_access_token
        else:
            access_token = unseal_token(self.settings.canva_token_secret, connection.access_token_encrypted)

        capabilities_payload = self.canva_client.get_user_capabilities(access_token)
        capabilities = _normalize_capabilities(capabilities_payload)
        connection.capabilities_json = capabilities
        connection.last_validated_at = now
        connection.updated_at = now
        self.session.add(connection)
        self.session.flush()

        if not capabilities.get("autofill"):
            raise RuntimeError("The connected Canva user does not have the autofill capability enabled.")
        if not capabilities.get("brand_template"):
            raise RuntimeError("The connected Canva user does not have the brand_template capability enabled.")
        return access_token

    def _wait_for_autofill(self, *, job_id: str, access_token: str) -> dict[str, Any]:
        last_payload: dict[str, Any] = {}
        for attempt in range(self.settings.deck_canva_poll_attempts):
            payload = self.canva_client.get_autofill_job(job_id, access_token)
            last_payload = payload
            status = str(dict(payload.get("job", {})).get("status") or "").strip().lower()
            if status in {"success", "failed"}:
                return payload
            if attempt < self.settings.deck_canva_poll_attempts - 1:
                time.sleep(max(self.settings.deck_canva_poll_interval_seconds, 1))
        raise RuntimeError(f"Canva autofill job did not complete in time: {last_payload}")

    def _latest_canva_connection(self) -> CanvaConnection | None:
        return self.session.execute(
            select(CanvaConnection).order_by(CanvaConnection.updated_at.desc(), CanvaConnection.id.desc()).limit(1)
        ).scalar_one_or_none()

    def _required_data_settings(self, *, include_google_sheets: bool) -> list[str]:
        missing: list[str] = []
        if include_google_sheets:
            if not self.settings.google_sheets_spreadsheet_id:
                missing.append("GOOGLE_SHEETS_SPREADSHEET_ID")
            if not self.settings.google_sheets_sales_range:
                missing.append("GOOGLE_SHEETS_SALES_RANGE")
            if not self.settings.google_service_account_json:
                missing.append("GOOGLE_SERVICE_ACCOUNT_JSON")
        return missing

    def _canva_delivery_ready(self) -> bool:
        return True

    def _build_export_url(self, *, run_id: int, deck_slug: str, token: str) -> str:
        public_base_url = str(getattr(self.settings, "deck_public_base_url", "") or "").strip()
        relative_path = f"/decks/{deck_slug}/{run_id}/{token}"
        if public_base_url:
            parsed = urlparse(public_base_url)
            if parsed.scheme and parsed.netloc:
                return urljoin(f"{parsed.scheme}://{parsed.netloc}", relative_path)
        redirect_uri = str(getattr(self.settings, "canva_redirect_uri", "") or "").strip()
        if redirect_uri:
            parsed = urlparse(redirect_uri)
            if parsed.scheme and parsed.netloc:
                return urljoin(f"{parsed.scheme}://{parsed.netloc}", relative_path)
        return relative_path

    def _render_html_deck(self, *, title: str, dataset: DeckDataset, warnings: list[str]) -> str:
        payload = dataset.deck_payload
        target = dict(payload.get("target", {}))
        xray_report = payload.get("xray_report")
        keyword_report = payload.get("keyword_report")
        if not isinstance(xray_report, Helium10XrayReport):
            raise RuntimeError("Deck payload is missing the Xray report.")
        if keyword_report is not None and not isinstance(keyword_report, Helium10KeywordReport):
            keyword_report = None
        market_cards = list(payload.get("market_cards", []))
        keyword_cards = list(payload.get("keyword_cards", []))
        primary_competitors = list(payload.get("primary_competitors", []))
        seo_recommendations = list(payload.get("seo_recommendations", []))
        cro_recommendations = list(payload.get("cro_recommendations", []))
        creative_recommendations = list(payload.get("creative_recommendations", []))
        channel_sections = list(payload.get("channel_sections", []))
        brand_wordmark = self._load_brand_asset("assets/wordmark.svg")
        monogram = self._load_brand_asset("assets/monogram.svg")
        stylesheet = self._load_brand_stylesheet()
        warning_items = "".join(f"<li>{html.escape(item)}</li>" for item in warnings if item)
        competitor_cards = "".join(_render_competitor_card(product) for product in primary_competitors)
        keyword_rows = "".join(_render_keyword_row(keyword) for keyword in (keyword_report.keywords[:10] if keyword_report else []))
        revenue_bars = "".join(_render_revenue_bar(product, xray_report.total_revenue) for product in xray_report.products[:6])
        channel_html = "".join(_render_channel_section(section) for section in channel_sections)
        gallery_items = [target] + [_product_to_gallery_item(product) for product in primary_competitors[:4]]
        gallery_html = "".join(_render_gallery_card(item) for item in gallery_items if item)
        market_summary_html = "".join(_render_metric_card(card) for card in market_cards)
        keyword_summary_html = "".join(_render_metric_card(card) for card in keyword_cards)
        country_donut = _render_distribution_card("Seller country of origin", xray_report.seller_country_distribution)
        size_donut = _render_distribution_card("Size tier", xray_report.size_tier_distribution)
        fulfillment_donut = _render_distribution_card("Fulfillment", xray_report.fulfillment_distribution)
        niche_keyword = str(payload.get("niche_keyword") or target.get("asin") or "the niche").strip()
        niche_table_rows = "".join(
            _render_niche_summary_row(product, xray_report.total_revenue)
            for product in xray_report.products[:10]
        )
        target_summary_meter = _render_meter_group(
            [
                ("Review base", _bounded_ratio(target.get("review_count", 0), 300), f"{target.get('review_count', 0)} reviews"),
                ("Rating signal", _bounded_ratio(_coerce_number(str(target.get("rating", ""))) or 0, 5), target.get("rating") or "n/a"),
                ("Market presence", _inverse_bounded_ratio(_coerce_number(str(target.get("bsr", ""))) or 0, 150000), target.get("bsr") or "n/a"),
            ]
        )
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>{stylesheet}</style>
</head>
<body>
  <main class="deck">
    <section class="deck-toolbar">
      <div class="brand-toolbar">
        <div class="brand-monogram">{monogram}</div>
        <div class="brand-wordmark">{brand_wordmark}</div>
      </div>
      <button class="print-button" onclick="window.print()">Print / Save PDF</button>
    </section>

    <section class="slide slide-cover">
      <div class="cover-grid">
        <div>
          <p class="eyebrow">Amazon-first strategy deck</p>
          <h1>{html.escape(title)}</h1>
          <p class="lead">{html.escape(dataset.text_fields.get("executive_summary") or "")}</p>
          <div class="pill-row">
            <span class="pill">ASIN {html.escape(str(target.get("asin", "")))}</span>
            <span class="pill">{html.escape(dataset.text_fields.get("report_generated_date") or "")}</span>
            <span class="pill">{html.escape(", ".join(payload.get("channels", [])) or "amazon")}</span>
          </div>
        </div>
        <div class="cover-card">
          {_render_hero_media(target, monogram)}
        </div>
      </div>
    </section>

    <section class="slide">
      <div class="slide-head">
        <div>
          <p class="eyebrow">Keyword niche summary</p>
          <h2>Page 1 summary for "{html.escape(niche_keyword)}"</h2>
        </div>
        <p class="muted">{html.escape(dataset.text_fields.get("market_summary") or "")}</p>
      </div>
      <div class="metric-grid">{market_summary_html}</div>
      <div class="dashboard-grid">
        <div class="dashboard-card niche-table-card">
          <div class="card-head">
            <h3>Organic page 1 revenue breakdown</h3>
            <span class="muted">Top listings from the uploaded Helium 10 Xray export</span>
          </div>
          <div class="table-wrap niche-table-wrap">
            <table class="niche-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Product</th>
                  <th>Price</th>
                  <th>Revenue</th>
                  <th>Share</th>
                </tr>
              </thead>
              <tbody>
                {niche_table_rows}
              </tbody>
            </table>
          </div>
          <div class="revenue-bars">{revenue_bars}</div>
        </div>
        <div class="donut-grid">
          {country_donut}
          {size_donut}
          {fulfillment_donut}
        </div>
      </div>
    </section>

    <section class="slide">
      <div class="slide-head">
        <div>
          <p class="eyebrow">Target listing</p>
          <h2>{html.escape(str(target.get("title", "Target listing")))}</h2>
        </div>
        <p class="muted">{html.escape(dataset.text_fields.get("hero_product_snapshot") or "")}</p>
      </div>
      <div class="target-grid">
        <div class="target-panel">
          {_render_hero_media(target, monogram)}
        </div>
        <div class="target-panel">
          <div class="target-meta">
            <div><span>Brand</span><strong>{html.escape(str(target.get("brand_name", "")))}</strong></div>
            <div><span>Price</span><strong>{html.escape(str(target.get("price", "")))}</strong></div>
            <div><span>BSR</span><strong>{html.escape(str(target.get("bsr", "")))}</strong></div>
            <div><span>Revenue</span><strong>{html.escape(str(target.get("revenue", "")))}</strong></div>
            <div><span>Rating</span><strong>{html.escape(str(target.get("rating", "")) or "n/a")}</strong></div>
            <div><span>Reviews</span><strong>{html.escape(str(target.get("review_count", "")) or "n/a")}</strong></div>
          </div>
          <p>{html.escape(str(target.get("description", "")) or "No listing description was captured from the product page.")}</p>
          <p class="muted">{html.escape(str(target.get("dimensions", "")) or "Dimensions unavailable.")}</p>
          <div class="meter-group">{target_summary_meter}</div>
        </div>
      </div>
    </section>

    <section class="slide">
      <div class="slide-head">
        <div>
          <p class="eyebrow">Competitor landscape</p>
          <h2>Who owns the page one real estate</h2>
        </div>
        <p class="muted">This table is sourced from the Helium 10 Xray export, with the first five non-target listings featured as benchmark cards.</p>
      </div>
      <div class="table-wrap">{self._render_table_html(self._table_rows(dataset.chart_fields.get("competitor_table")))}</div>
      <div class="competitor-grid">{competitor_cards}</div>
    </section>

    <section class="slide">
      <div class="slide-head">
        <div>
          <p class="eyebrow">Search behavior</p>
          <h2>Keyword and customer search objective</h2>
        </div>
        <p class="muted">{html.escape(dataset.text_fields.get("seo_summary") or "")}</p>
      </div>
      <div class="metric-grid">{keyword_summary_html}</div>
      <div class="two-col split-top">
        <div class="dashboard-card">
          <div class="card-head">
            <h3>Top keyword opportunities</h3>
            <span class="muted">Highest search volume from the keyword export</span>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr><th>Keyword</th><th>Search volume</th><th>Sales</th><th>Competing products</th><th>Title density</th></tr>
              </thead>
              <tbody>
                {keyword_rows or "<tr><td colspan='5' class='muted'>No keyword CSV uploaded.</td></tr>"}
              </tbody>
            </table>
          </div>
        </div>
        <div class="recommendation-card">
          <h3>SEO actions</h3>
          <ul>{''.join(f"<li>{html.escape(item)}</li>" for item in seo_recommendations)}</ul>
        </div>
      </div>
    </section>

    <section class="slide">
      <div class="slide-head">
        <div>
          <p class="eyebrow">Conversion and PDP</p>
          <h2>Where the listing needs to improve</h2>
        </div>
        <p class="muted">{html.escape(dataset.text_fields.get("cro_summary") or "")}</p>
      </div>
      <div class="two-col split-top">
        <div class="recommendation-card">
          <h3>CRO recommendations</h3>
          <ul>{''.join(f"<li>{html.escape(item)}</li>" for item in cro_recommendations)}</ul>
        </div>
        <div class="recommendation-card">
          <h3>Creative recommendations</h3>
          <ul>{''.join(f"<li>{html.escape(item)}</li>" for item in creative_recommendations)}</ul>
        </div>
      </div>
      <div class="gallery-grid">{gallery_html}</div>
    </section>

    {channel_html}

    <section class="slide">
      <div class="slide-head">
        <div>
          <p class="eyebrow">Recommended plan</p>
          <h2>What happens next</h2>
        </div>
      </div>
      <div class="plan-grid">
        <div class="plan-card">
          <h3>Recommended plan</h3>
          <p>{html.escape(dataset.text_fields.get("recommended_plan_summary") or "")}</p>
        </div>
        <div class="plan-card">
          <h3>Expected impact</h3>
          <p>{html.escape(dataset.text_fields.get("expected_impact_summary") or "")}</p>
        </div>
        <div class="plan-card">
          <h3>Why anata</h3>
          <p>{html.escape(dataset.text_fields.get("why_anata_summary") or "")}</p>
        </div>
      </div>
      <div class="closing-card">
        <p>{html.escape(dataset.text_fields.get("cta_summary") or "")}</p>
      </div>
      {"<div class='warning'><strong>Generation notes</strong><ul>" + warning_items + "</ul></div>" if warning_items else ""}
    </section>
  </main>
</body>
</html>"""

    def _table_rows(self, chart_payload: dict[str, Any] | None) -> list[list[str]]:
        rows: list[list[str]] = []
        for row in list(dict(chart_payload or {}).get("rows", []) or []):
            cells = []
            for cell in list(dict(row).get("cells", []) or []):
                value = dict(cell).get("value")
                cells.append("" if value is None else str(value))
            if cells:
                rows.append(cells)
        return rows

    def _render_table_html(self, rows: list[list[str]]) -> str:
        if not rows:
            return "<p class='muted' style='padding:16px;'>No data available.</p>"
        header = rows[0]
        body = rows[1:]
        thead = "".join(f"<th>{html.escape(cell)}</th>" for cell in header)
        tbody = "".join(
            "<tr>" + "".join(f"<td>{html.escape(cell)}</td>" for cell in row) + "</tr>"
            for row in body
        )
        return f"<table><thead><tr>{thead}</tr></thead><tbody>{tbody}</tbody></table>"

    def _run_summary(self, run: AutomationRun) -> dict[str, Any]:
        summary = dict(run.summary_json or {})
        return {
            "id": run.id,
            "status": summary.get("status") or run.status,
            "message": summary.get("message", ""),
            "design_id": summary.get("design_id", ""),
            "design_title": summary.get("design_title", ""),
            "edit_url": summary.get("edit_url", ""),
            "view_url": summary.get("view_url", ""),
            "warnings": list(summary.get("warnings", []) or []),
            "output_type": summary.get("output_type", ""),
            "deck_slug": summary.get("deck_slug", ""),
            "view_count": int(summary.get("view_count", 0) or 0),
            "first_viewed_at": summary.get("first_viewed_at", ""),
            "last_viewed_at": summary.get("last_viewed_at", ""),
            "channels": list(summary.get("channels", []) or []),
            "started_at": run.started_at.isoformat() if run.started_at else "",
            "completed_at": run.completed_at.isoformat() if run.completed_at else "",
        }

    def _scope_string(self, value: Any) -> str:
        if isinstance(value, (list, tuple)):
            return " ".join(str(item).strip() for item in value if str(item).strip())
        return str(value or "").strip()

    def _load_brand_stylesheet(self) -> str:
        for path in _candidate_brand_paths(self.settings, "style.css"):
            if path.exists():
                return path.read_text(encoding="utf-8")
        return "body{font-family:Arial,sans-serif;background:#fff;color:#172033;}"

    def _load_brand_asset(self, relative_path: str) -> str:
        for path in _candidate_brand_paths(self.settings, relative_path):
            if path.exists():
                return path.read_text(encoding="utf-8")
        return ""


def _candidate_brand_paths(settings: Settings, relative_path: str) -> list[Path]:
    configured_root = Path(str(getattr(settings, "shared_brand_package_path", "") or "")).expanduser()
    repo_root = Path(__file__).resolve().parents[2]
    candidates: list[Path] = []
    if str(configured_root):
        candidates.append(configured_root / relative_path)
    candidates.append(repo_root / "shared" / "anata_brand" / relative_path)
    return candidates


def _normalize_channels(channels: list[str]) -> list[str]:
    allowed = {
        "amazon": "amazon",
        "shopify": "shopify",
        "tiktok_shop": "tiktok_shop",
        "tiktok": "tiktok_shop",
    }
    normalized: list[str] = []
    seen: set[str] = set()
    for value in channels:
        key = _normalize_key(str(value or ""))
        mapped = allowed.get(key)
        if not mapped or mapped in seen:
            continue
        seen.add(mapped)
        normalized.append(mapped)
    if "amazon" not in seen:
        normalized.insert(0, "amazon")
    return normalized


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")


def _build_target_snapshot_text(target_title: str, brand_name: str, target_row: XrayProduct | None) -> str:
    if target_row and target_row.revenue:
        return (
            f"{target_title} is the hero listing for {brand_name}. "
            f"In the current niche export it shows {target_row.revenue_label} in revenue and {target_row.bsr_label} BSR."
        )
    return (
        f"{target_title} is the hero listing for {brand_name}. "
        "Use this deck to compare the PDP against the niche leaders and tighten the initial go-to-market offer."
    )


def _build_market_summary(
    brand_name: str,
    xray_report: Helium10XrayReport,
    keyword_report: Helium10KeywordReport | None,
) -> str:
    lead_keyword = keyword_report.keywords[0].phrase if keyword_report and keyword_report.keywords else "the niche"
    search_volume = keyword_report.top_search_volume if keyword_report and keyword_report.top_search_volume else None
    search_text = f" The top keyword in the upload is {lead_keyword} with {search_volume:,} monthly searches." if search_volume else ""
    return (
        f"The current {lead_keyword} market shows {xray_report.search_results_count} comparable listings and "
        f"{_label_money_value(xray_report.total_revenue)} in 30-day competitor revenue across the uploaded Xray set."
        f"{search_text}"
    )


def _build_executive_summary(
    target_title: str,
    brand_name: str,
    xray_report: Helium10XrayReport,
    keyword_report: Helium10KeywordReport | None,
) -> str:
    keyword_text = ""
    if keyword_report and keyword_report.keywords:
        keyword_text = f" The keyword export highlights {keyword_report.keywords[0].phrase} as the leading search objective."
    return (
        f"This deck benchmarks {target_title} against the live Amazon market captured in the Xray export and translates the data into an offer, PDP, SEO, and service plan for {brand_name}."
        f"{keyword_text}"
    )


def _build_advertising_summary(xray_report: Helium10XrayReport, keyword_report: Helium10KeywordReport | None) -> str:
    top_keyword = keyword_report.keywords[0].phrase if keyword_report and keyword_report.keywords else "the primary search terms"
    return (
        f"Advertising should follow listing cleanup. Once the PDP is aligned, lean into {top_keyword} and the adjacent high-volume terms while exploiting low-review competitors in the category."
    )


def _build_plan_summary(channels: list[str]) -> str:
    services = []
    if "amazon" in channels:
        services.append("fix the Amazon PDP, imagery, and search coverage")
    if "shopify" in channels:
        services.append("align the Shopify storefront and conversion flow")
    if "tiktok_shop" in channels:
        services.append("package the TikTok Shop offer and creative")
    return "Phase 1: " + "; ".join(services[:3]) + ". Phase 2: launch the first measurement sprint and tune against live market response."


def _build_expected_impact_summary(xray_report: Helium10XrayReport) -> str:
    return (
        f"The niche is large enough to justify a tighter positioning and conversion sprint. {xray_report.under_75_reviews_count} listings are still competing with under 75 reviews, which leaves room for differentiated creative and offer design."
    )


def _build_why_anata_summary(channels: list[str]) -> str:
    scope = []
    if "amazon" in channels:
        scope.append("Amazon growth execution")
    if "shopify" in channels:
        scope.append("Shopify conversion systems")
    if "tiktok_shop" in channels:
        scope.append("TikTok Shop go-to-market support")
    return "Anata can own " + ", ".join(scope) + " without splitting CRO, creative, and acquisition across separate vendors."


def _build_market_metric_cards(
    xray_report: Helium10XrayReport,
    keyword_report: Helium10KeywordReport | None,
) -> list[dict[str, str]]:
    return [
        {"label": "30-day revenue", "value": _label_money_value(xray_report.total_revenue), "meta": f"Across {xray_report.search_results_count} products"},
        {"label": "30-day units sold", "value": _label_integer(xray_report.total_units_sold), "meta": "Summed from the Xray export"},
        {"label": "Average BSR", "value": _label_float(xray_report.average_bsr, 0), "meta": "Lower is stronger"},
        {"label": "Average price", "value": _label_money_value(xray_report.average_price or 0.0), "meta": "From the uploaded competitor set"},
        {"label": "Average rating", "value": _label_float(xray_report.average_rating, 1), "meta": "Competitive review signal"},
        {
            "label": "Open opportunity",
            "value": f"{xray_report.under_75_reviews_count}/{xray_report.search_results_count}",
            "meta": f"{xray_report.revenue_over_5000_count} listings clear $5k revenue while {xray_report.under_75_reviews_count} stay under 75 reviews.",
        },
    ]


def _build_keyword_metric_cards(keyword_report: Helium10KeywordReport | None) -> list[dict[str, str]]:
    if keyword_report is None:
        return [
            {"label": "Keyword coverage", "value": "Missing", "meta": "Upload the Xray keyword CSV to populate this slide"},
        ]
    return [
        {"label": "Keywords parsed", "value": str(len(keyword_report.keywords)), "meta": "Rows loaded from the keyword export"},
        {"label": "Total search volume", "value": _label_integer(keyword_report.total_search_volume), "meta": "Summed across the uploaded keywords"},
        {"label": "Average competing products", "value": _label_float(keyword_report.average_competing_products, 0), "meta": "Competitive density"},
        {"label": "Average title density", "value": _label_float(keyword_report.average_title_density, 0), "meta": "How crowded the SERP language is"},
    ]


def _build_seo_recommendations(
    keyword_report: Helium10KeywordReport | None,
    xray_report: Helium10XrayReport,
) -> list[str]:
    recommendations = [
        "Rewrite the listing title and first bullets around the highest-intent keyword cluster rather than generic supplement language.",
        "Use the lowest-title-density terms as the first indexing gap to attack before scaling paid traffic.",
    ]
    if keyword_report and keyword_report.keywords:
        recommendations.insert(
            0,
            f"Lead the SEO rewrite with {keyword_report.keywords[0].phrase} and the adjacent long-tail terms with meaningful search volume.",
        )
    if xray_report.under_75_reviews_count:
        recommendations.append("Push harder into keyword relevance while review barriers are still low across several competitors.")
    return recommendations[:4]


def _build_cro_recommendations(target_row: XrayProduct | None, competitors: list[XrayProduct]) -> list[str]:
    recommendations = [
        "Simplify the above-the-fold value proposition so the primary outcome is readable in under five seconds.",
        "Tighten the PDP information hierarchy: hero image, proof, ingredient context, usage, and trust markers should read in that order.",
    ]
    if target_row and (target_row.review_count or 0) < 75:
        recommendations.append("Compensate for the lighter review base with stronger proof blocks, FAQ coverage, and comparison framing.")
    if competitors:
        recommendations.append(f"Benchmark the first image stack against {competitors[0].title} and the other top revenue leaders, then close the gap on clarity and proof.")
    return recommendations[:4]


def _build_creative_recommendations(target_row: XrayProduct | None, competitors: list[XrayProduct]) -> list[str]:
    recommendations = [
        "Rebuild the hero image sequence so each panel communicates one claim, one proof point, or one use case.",
        "Add visual comparison and product-context frames instead of relying only on clinical or generic packaging shots.",
    ]
    if competitors:
        recommendations.append("Use the top competitor image stacks as a reference set for claim sequencing and CTA placement.")
    if target_row and not target_row.image_url:
        recommendations.append("Capture a clean primary listing image before the creative refresh so the deck has a stable hero asset.")
    return recommendations[:4]


def _build_competitor_strength(product: XrayProduct) -> str:
    return f"{product.title} converts enough demand to produce {product.revenue_label} in 30-day revenue with {product.bsr_label} BSR."


def _build_competitor_gap(product: XrayProduct, target_row: XrayProduct | None) -> str:
    if (product.review_count or 0) < 75:
        return "This listing is still winning with a relatively thin review moat, which makes it a useful target for differentiation."
    if target_row and product.price and target_row.price and product.price > target_row.price:
        return "The price anchor is higher than the target listing, which creates room for a sharper value story."
    return "Use this listing as a benchmark for claim clarity, review depth, and imagery sequence."


def _build_channel_sections(channels: list[str]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    if "amazon" in channels:
        sections.append(
            {
                "eyebrow": "Amazon offering",
                "title": "What we would execute on Amazon",
                "summary": "Amazon is the fully wired channel in v1, so this slide pairs the market data with the actual delivery scope.",
                "items": [
                    "PDP conversion optimization and image sequencing",
                    "Search coverage, title rewrite, and indexing priorities",
                    "Offer positioning, competitive pricing, and launch messaging",
                    "Advertising handoff once the PDP is conversion-ready",
                ],
            }
        )
    if "shopify" in channels:
        sections.append(
            {
                "eyebrow": "Shopify offering",
                "title": "How Shopify support would layer on",
                "summary": "Shopify is outlined as a service path in v1. It does not yet pull storefront data into the deck.",
                "items": [
                    "Landing-page and product-page CRO",
                    "Offer design and merchandising structure",
                    "Lifecycle capture and retention flow",
                    "Storefront positioning to match the Amazon narrative",
                ],
            }
        )
    if "tiktok_shop" in channels:
        sections.append(
            {
                "eyebrow": "TikTok Shop offering",
                "title": "How TikTok Shop support would layer on",
                "summary": "TikTok Shop is outlined as a service path in v1. It does not yet pull TikTok operational data into the deck.",
                "items": [
                    "Creator-first offer and hook strategy",
                    "Short-form creative packaging for conversion",
                    "Shop merchandising and SKU presentation",
                    "Paid / organic testing loop for scaling winners",
                ],
            }
        )
    return sections


def _render_metric_card(card: dict[str, str]) -> str:
    return (
        "<article class='metric-card'>"
        f"<span>{html.escape(card.get('label', ''))}</span>"
        f"<strong>{html.escape(card.get('value', ''))}</strong>"
        f"<small>{html.escape(card.get('meta', ''))}</small>"
        "</article>"
    )


def _render_competitor_card(product: XrayProduct) -> str:
    image = f"<img src='{html.escape(product.image_url)}' alt='{html.escape(product.title)}' />" if product.image_url else "<div class='image-fallback'>No image</div>"
    return (
        "<article class='competitor-card'>"
        f"<div class='competitor-media'>{image}</div>"
        "<div class='competitor-body'>"
        f"<h3>{html.escape(product.title)}</h3>"
        f"<p class='muted'>{html.escape(product.brand)} · {html.escape(product.asin)}</p>"
        f"<p><strong>{html.escape(product.revenue_label)}</strong> revenue · <strong>{html.escape(product.bsr_label)}</strong> BSR</p>"
        f"<p>{html.escape(_build_competitor_gap(product, None))}</p>"
        "</div>"
        "</article>"
    )


def _render_keyword_row(keyword: KeywordInsight) -> str:
    return (
        "<tr>"
        f"<td>{html.escape(keyword.phrase)}</td>"
        f"<td>{html.escape(keyword.search_volume_label)}</td>"
        f"<td>{html.escape(keyword.keyword_sales_label)}</td>"
        f"<td>{html.escape(str(keyword.competing_products or ''))}</td>"
        f"<td>{html.escape(str(keyword.title_density or ''))}</td>"
        "</tr>"
    )


def _render_revenue_bar(product: XrayProduct, total_revenue: float) -> str:
    share = 0.0 if total_revenue <= 0 else ((product.revenue or 0.0) / total_revenue)
    width = max(6, min(int(round(share * 100)), 100))
    return (
        "<article class='revenue-row'>"
        "<div class='revenue-labels'>"
        f"<strong>{html.escape(product.title)}</strong>"
        f"<span>{html.escape(product.revenue_label)}</span>"
        "</div>"
        f"<div class='revenue-track'><div class='revenue-fill' style='width:{width}%'></div></div>"
        "</article>"
    )


def _render_niche_summary_row(product: XrayProduct, total_revenue: float) -> str:
    share = 0.0 if total_revenue <= 0 else ((product.revenue or 0.0) / total_revenue) * 100
    image_html = (
        f"<img src='{html.escape(product.image_url)}' alt='{html.escape(product.title)}' />"
        if product.image_url
        else "<div class='image-fallback compact'>No image</div>"
    )
    return (
        "<tr>"
        f"<td>{html.escape(str(product.display_order))}</td>"
        "<td>"
        "<div class='niche-product-cell'>"
        f"<div class='niche-product-thumb'>{image_html}</div>"
        "<div>"
        f"<strong>{html.escape(product.title)}</strong>"
        f"<div class='muted'>{html.escape(product.asin)} · {html.escape(product.brand)}</div>"
        "</div>"
        "</div>"
        "</td>"
        f"<td>{html.escape(product.price_label)}</td>"
        f"<td>{html.escape(product.revenue_label)}</td>"
        f"<td>{share:.1f}%</td>"
        "</tr>"
    )


def _render_distribution_card(title: str, slices: list[DistributionSlice]) -> str:
    donut = _render_donut(slices)
    items = "".join(
        f"<li><span>{html.escape(item.label)}</span><strong>{item.count}</strong></li>"
        for item in slices[:6]
    )
    return (
        "<article class='distribution-card'>"
        f"<h3>{html.escape(title)}</h3>"
        f"{donut}"
        f"<ul>{items}</ul>"
        "</article>"
    )


def _render_donut(slices: list[DistributionSlice]) -> str:
    palette = ["#244d87", "#4f84c4", "#85bbda", "#bfa889", "#9e6d66", "#d9e8f4"]
    stops: list[str] = []
    start = 0.0
    for index, item in enumerate(slices[:6]):
        end = start + (item.share * 100)
        stops.append(f"{palette[index % len(palette)]} {start:.2f}% {end:.2f}%")
        start = end
    if start < 100:
        stops.append(f"#edf2f7 {start:.2f}% 100%")
    style = f"background: conic-gradient({', '.join(stops)});"
    return f"<div class='donut-chart'><div class='donut-visual' style=\"{style}\"></div></div>"


def _render_channel_section(section: dict[str, Any]) -> str:
    items = "".join(f"<li>{html.escape(str(item))}</li>" for item in section.get("items", []))
    return (
        "<section class='slide'>"
        f"<div class='slide-head'><div><p class='eyebrow'>{html.escape(str(section.get('eyebrow', '')))}</p><h2>{html.escape(str(section.get('title', '')))}</h2></div>"
        f"<p class='muted'>{html.escape(str(section.get('summary', '')))}</p></div>"
        f"<div class='recommendation-card'><ul>{items}</ul></div>"
        "</section>"
    )


def _render_hero_media(target: dict[str, Any], monogram: str) -> str:
    image_url = str(target.get("image_url", "") or "").strip()
    if image_url:
        return (
            "<div class='hero-media'>"
            f"<img src='{html.escape(image_url)}' alt='{html.escape(str(target.get('title', 'Target product')))}' />"
            "</div>"
        )
    return f"<div class='hero-media fallback'>{monogram}<span>No product image available</span></div>"


def _product_to_gallery_item(product: XrayProduct) -> dict[str, str]:
    return {
        "title": product.title,
        "subtitle": product.brand,
        "image_url": product.image_url,
        "meta": f"{product.revenue_label} revenue · {product.bsr_label} BSR",
    }


def _render_gallery_card(item: dict[str, Any]) -> str:
    image_url = str(item.get("image_url", "") or "").strip()
    media = f"<img src='{html.escape(image_url)}' alt='{html.escape(str(item.get('title', 'Listing image')))}' />" if image_url else "<div class='image-fallback'>Image unavailable</div>"
    return (
        "<article class='gallery-card'>"
        f"<div class='gallery-media'>{media}</div>"
        f"<strong>{html.escape(str(item.get('title', '')))}</strong>"
        f"<p>{html.escape(str(item.get('subtitle', '')))}</p>"
        f"<small>{html.escape(str(item.get('meta', '')))}</small>"
        "</article>"
    )


def _render_meter_group(items: list[tuple[str, float, str]]) -> str:
    return "".join(
        "<div class='meter-item'>"
        f"<span>{html.escape(label)}</span>"
        f"<div class='meter-track'><div class='meter-fill' style='width:{max(0, min(int(round(value * 100)), 100))}%'></div></div>"
        f"<small>{html.escape(meta)}</small>"
        "</div>"
        for label, value, meta in items
    )


def _bounded_ratio(value: float, ceiling: float) -> float:
    if ceiling <= 0:
        return 0.0
    return max(0.0, min(value / ceiling, 1.0))


def _inverse_bounded_ratio(value: float, ceiling: float) -> float:
    if ceiling <= 0:
        return 0.0
    return max(0.0, min(1.0 - (min(value, ceiling) / ceiling), 1.0))


def _label_integer(value: float | int | None) -> str:
    if value is None:
        return "n/a"
    return f"{int(round(value)):,}"


def _label_float(value: float | None, digits: int) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.{digits}f}"


def _label_money_value(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.2f}"


def _normalize_key(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")
    return normalized


def _normalize_competitor_inputs(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        for fragment in re.split(r"[\n,]+", str(raw_value or "")):
            cleaned = fragment.strip()
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            normalized.append(cleaned)
    return normalized


def _parse_target_product_input(value: str) -> dict[str, str]:
    cleaned = str(value or "").strip()
    if not cleaned:
        return {
            "source_type": "",
            "source_url": "",
            "domain": "",
            "brand_name": "",
            "product_handle": "",
            "product_name": "",
            "asin": "",
        }
    shopify_candidate = _parse_shopify_product_url(cleaned)
    if shopify_candidate["source_url"] and shopify_candidate["product_handle"]:
        return {
            "source_type": "shopify",
            "source_url": shopify_candidate["source_url"],
            "domain": shopify_candidate["domain"],
            "brand_name": shopify_candidate["brand_name"],
            "product_handle": shopify_candidate["product_handle"],
            "product_name": shopify_candidate["product_name"],
            "asin": "",
        }
    amazon_candidate = _parse_competitor_reference(cleaned)
    if amazon_candidate["asin"]:
        return {
            "source_type": "amazon",
            "source_url": amazon_candidate["source_url"],
            "domain": "amazon.com",
            "brand_name": "",
            "product_handle": amazon_candidate["asin"],
            "product_name": amazon_candidate["name"],
            "asin": amazon_candidate["asin"],
        }
    return {
        "source_type": "",
        "source_url": "",
        "domain": "",
        "brand_name": "",
        "product_handle": "",
        "product_name": "",
        "asin": "",
    }


def _parse_shopify_product_url(value: str) -> dict[str, str]:
    cleaned = str(value or "").strip()
    if not cleaned:
        return {
            "source_url": "",
            "domain": "",
            "brand_name": "",
            "product_handle": "",
            "product_name": "",
        }
    parsed = urlparse(cleaned if "://" in cleaned else f"https://{cleaned}")
    domain = (parsed.netloc or parsed.path).strip().lower().split("/")[0]
    path = parsed.path or ""
    handle = ""
    match = re.search(r"/products/([^/?#]+)", path, flags=re.IGNORECASE)
    if match:
        handle = match.group(1).strip()
    brand_source = domain.split(".")
    brand_token = brand_source[-2] if len(brand_source) >= 2 else domain
    brand_name = _titleize_slug(brand_token) or "Brand"
    product_name = _titleize_slug(handle) or f"{brand_name} Hero Product"
    canonical_url = cleaned if "://" in cleaned else f"https://{cleaned}"
    return {
        "source_url": canonical_url,
        "domain": domain,
        "brand_name": brand_name,
        "product_handle": handle,
        "product_name": product_name,
    }


def _parse_competitor_reference(value: str) -> dict[str, str]:
    cleaned = str(value or "").strip()
    asin_match = re.search(r"\b([A-Z0-9]{10})\b", cleaned.upper())
    parsed = urlparse(cleaned if "://" in cleaned else "")
    path = parsed.path if parsed.scheme else ""
    url_candidate = cleaned if parsed.scheme else ""
    name = ""
    if path:
        for pattern in (r"/dp/([A-Z0-9]{10})", r"/gp/product/([A-Z0-9]{10})", r"/([^/?#]+)/dp/[A-Z0-9]{10}"):
            path_match = re.search(pattern, path, flags=re.IGNORECASE)
            if path_match and pattern.startswith("/("):
                name = _titleize_slug(path_match.group(1))
                break
    asin = asin_match.group(1) if asin_match else ""
    identifier = asin or cleaned
    if not name:
        if asin:
            name = f"ASIN {asin}"
        else:
            name = _titleize_slug(cleaned.rsplit("/", 1)[-1]) or cleaned
    source_url = url_candidate or (f"https://www.amazon.com/dp/{asin}" if asin else cleaned)
    return {
        "name": name[:120],
        "identifier": identifier[:160],
        "source_url": source_url[:255],
        "asin": asin,
    }


def _titleize_slug(value: str) -> str:
    cleaned = re.sub(r"[_\-]+", " ", str(value or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""
    return " ".join(token.capitalize() for token in cleaned.split(" "))


def _normalize_capabilities(payload: dict[str, Any]) -> dict[str, bool]:
    capabilities = payload.get("capabilities")
    if not capabilities and isinstance(payload.get("user"), dict):
        capabilities = dict(payload.get("user", {})).get("capabilities")
    if isinstance(capabilities, dict):
        return {str(key): bool(value) for key, value in capabilities.items()}
    if isinstance(capabilities, list):
        return {str(item): True for item in capabilities}
    return {
        "autofill": False,
        "brand_template": False,
    }


def _expires_at_from_payload(payload: dict[str, Any]) -> datetime | None:
    expires_in = payload.get("expires_in")
    if expires_in is None:
        return None
    try:
        return datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))
    except Exception:
        return None


def _normalize_sales_rows(values: list[list[str]]) -> tuple[list[list[str]], dict[str, str]]:
    scalar_fields: dict[str, str] = {}
    first_row = values[0]
    if len(first_row) == 2 and all(len(row) >= 2 for row in values):
        normalized_rows = [["Metric", "Value"]]
        for source_index, row in enumerate(values, start=1):
            label = str(row[0] or "").strip()
            value = str(row[1] or "").strip()
            if source_index == 1 and _normalize_key(label) in {"metric", "label", "name"} and _normalize_key(value) in {"value", "amount"}:
                continue
            normalized_rows.append([label, value])
            if label:
                scalar_fields[f"sales_{_normalize_key(label)}"] = value
        return normalized_rows, scalar_fields

    headers = [_normalize_key(cell) or f"column_{index + 1}" for index, cell in enumerate(first_row)]
    normalized_rows = [[str(cell or "").strip() for cell in first_row]]
    for row_index, row in enumerate(values[1:], start=1):
        padded = [str(row[column_index] or "").strip() if column_index < len(row) else "" for column_index in range(len(headers))]
        normalized_rows.append(padded)
        for header, value in zip(headers, padded):
            scalar_fields[f"sales_row_{row_index}_{header}"] = value
        if row_index == 1:
            for header, value in zip(headers, padded):
                scalar_fields[f"sales_{header}"] = value
    return normalized_rows, scalar_fields


def _build_chart_data(rows: list[list[str]]) -> dict[str, Any]:
    clipped_rows = rows[:100]
    if clipped_rows:
        clipped_rows = [row[:20] for row in clipped_rows]
    return {
        "rows": [
            {
                "cells": [_build_chart_cell(cell, is_header=row_index == 0) for cell in row]
            }
            for row_index, row in enumerate(clipped_rows)
        ]
    }


def _build_top_products_by_bsr_rows(rows: list[list[str]]) -> list[list[str]] | None:
    if len(rows) < 2:
        return None

    header_row = rows[0]
    normalized_headers = [_normalize_key(cell) for cell in header_row]

    def _find_index(*candidates: str) -> int | None:
        for candidate in candidates:
            if candidate in normalized_headers:
                return normalized_headers.index(candidate)
        return None

    product_idx = _find_index("product_name", "product", "title", "item_name", "name")
    bsr_idx = _find_index("bsr", "best_seller_rank", "bestseller_rank", "sales_rank")
    sales_idx = _find_index("sales", "revenue", "sales_total", "sales_amount")
    units_idx = _find_index("units", "unit_sales", "ordered_units", "qty", "quantity")
    change_idx = _find_index(
        "change_from_previous_period",
        "change_vs_previous_period",
        "previous_period_change",
        "period_change",
        "sales_change",
        "mom_change",
        "change",
    )

    required_indexes = (product_idx, bsr_idx, sales_idx, units_idx, change_idx)
    if any(index is None for index in required_indexes):
        return None

    ranked_rows: list[tuple[float, list[str]]] = []
    for row in rows[1:]:
        padded = [str(cell or "").strip() for cell in row]
        max_index = max(index for index in required_indexes if index is not None)
        if len(padded) <= max_index:
            continue
        bsr_value = _coerce_number(padded[bsr_idx]) if bsr_idx is not None else None
        if bsr_value is None:
            continue
        ranked_rows.append(
            (
                bsr_value,
                [
                    padded[product_idx] if product_idx is not None else "",
                    padded[bsr_idx] if bsr_idx is not None else "",
                    padded[sales_idx] if sales_idx is not None else "",
                    padded[units_idx] if units_idx is not None else "",
                    padded[change_idx] if change_idx is not None else "",
                ],
            )
        )

    if not ranked_rows:
        return None

    ranked_rows.sort(key=lambda item: (item[0], item[1][0].lower()))
    top_rows = [["Product name", "BSR", "Sales", "Units", "Change from previous period"]]
    top_rows.extend(values for _, values in ranked_rows[:10])
    return top_rows


def _build_chart_cell(value: str, *, is_header: bool) -> dict[str, Any]:
    cleaned = str(value or "").strip()
    if is_header:
        return {"type": "string", "value": cleaned}
    lowered = cleaned.lower()
    if lowered in {"true", "false", "yes", "no"}:
        return {"type": "boolean", "value": lowered in {"true", "yes"}}
    number_value = _coerce_number(cleaned)
    if number_value is not None:
        return {"type": "number", "value": number_value}
    timestamp_value = _coerce_date_timestamp(cleaned)
    if timestamp_value is not None:
        return {"type": "date", "value": timestamp_value}
    return {"type": "string", "value": cleaned}


def _coerce_number(value: str) -> float | None:
    cleaned = value.replace(",", "").replace("$", "").replace("%", "").strip()
    if not cleaned:
        return None
    multiplier = 1.0
    if cleaned.lower().endswith("k"):
        multiplier = 1000.0
        cleaned = cleaned[:-1]
    elif cleaned.lower().endswith("m"):
        multiplier = 1_000_000.0
        cleaned = cleaned[:-1]
    if not re.fullmatch(r"-?\d+(\.\d+)?", cleaned):
        return None
    return float(cleaned) * multiplier


def _coerce_date_timestamp(value: str) -> int | None:
    if not value:
        return None
    for pattern in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return int(datetime.strptime(value, pattern).replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            continue
    return None
