"""Deck generation service for Google Sheets + Canva brand templates."""

from __future__ import annotations

import csv
import io
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings, get_missing_deck_generator_settings
from sales_support_agent.integrations.amazon_sp_api import AmazonSpApiClient
from sales_support_agent.integrations.canva import CanvaClient
from sales_support_agent.integrations.google_sheets import GoogleSheetsClient
from sales_support_agent.integrations.shopify import ShopifyStorefrontClient
from sales_support_agent.models.entities import AutomationRun, CanvaConnection
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.product_research import ProductResearchService
from sales_support_agent.services.token_seal import seal_token, unseal_token


@dataclass(frozen=True)
class DeckDataset:
    text_fields: dict[str, str]
    chart_fields: dict[str, dict[str, Any]]
    warnings: list[str]
    sales_row_count: int
    competitor_row_count: int


@dataclass(frozen=True)
class DeckGenerationResult:
    run_id: int
    status: str
    message: str
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
        competitor_csv_bytes: bytes | None = None,
        competitor_filename: str = "",
        target_product_input: str = "",
        shopify_product_url: str = "",
        competitor_inputs: list[str] | None = None,
        run_label: str = "",
        report_date: date | None = None,
        reporting_period: str = "",
        trigger: str = "admin_dashboard",
    ) -> DeckGenerationResult:
        effective_target_input = target_product_input.strip() or shopify_product_url.strip()
        normalized_competitors = _normalize_competitor_inputs(competitor_inputs or [])
        automation_mode = bool(effective_target_input or normalized_competitors)
        missing = get_missing_deck_generator_settings(
            self.settings,
            include_google_sheets=not automation_mode,
        )
        if missing:
            raise RuntimeError(f"Deck generator is missing environment variables: {', '.join(missing)}")

        run = self.audit.start_run(
            "deck_generation",
            trigger=trigger,
            metadata={
                "generation_mode": "automation_first" if automation_mode else "csv_upload",
                "competitor_filename": competitor_filename,
                "target_product_input": effective_target_input,
                "shopify_product_url": shopify_product_url,
                "competitor_inputs": normalized_competitors,
                "report_date": report_date.isoformat() if report_date else "",
                "reporting_period": reporting_period,
                "template_id": self.settings.canva_brand_template_id,
                "sheet_range": self.settings.google_sheets_sales_range,
            },
        )
        try:
            if automation_mode:
                dataset = self._build_automation_first_dataset(
                    target_product_input=effective_target_input,
                    competitor_inputs=normalized_competitors,
                    report_date=report_date,
                    reporting_period=reporting_period,
                )
            else:
                if competitor_csv_bytes is None:
                    raise RuntimeError("Competitor CSV upload is required when Shopify/Amazon inputs are not provided.")
                sales_payload = self.google_client.get_values()
                dataset = self._build_dataset(
                    sales_payload=sales_payload,
                    competitor_csv_bytes=competitor_csv_bytes,
                    report_date=report_date,
                    reporting_period=reporting_period,
                )

            access_token = self._ensure_canva_access_token()
            template_payload = self.canva_client.get_brand_template_dataset(
                self.settings.canva_brand_template_id,
                access_token,
            )
            template_dataset = dict(template_payload.get("dataset", {}) or {})
            canva_data, warnings = self._prepare_canva_data(template_dataset, dataset)
            warnings = [*dataset.warnings, *warnings]

            title = self._build_design_title(run_label=run_label, report_date=report_date, reporting_period=reporting_period)
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
            result = DeckGenerationResult(
                run_id=run.id,
                status="success",
                message="Deck generated successfully.",
                design_id=str(design.get("id") or "").strip(),
                design_title=str(design.get("title") or title).strip(),
                edit_url=str(urls.get("edit_url") or design.get("url") or "").strip(),
                view_url=str(urls.get("view_url") or design.get("url") or "").strip(),
                warnings=warnings,
                sales_row_count=dataset.sales_row_count,
                competitor_row_count=dataset.competitor_row_count,
                template_fields=len(template_dataset),
            )
            self.audit.finish_run(
                run,
                status="success",
                summary={
                    "status": "success",
                    "message": result.message,
                    "design_id": result.design_id,
                    "design_title": result.design_title,
                    "edit_url": result.edit_url,
                    "view_url": result.view_url,
                    "warnings": result.warnings,
                    "sales_row_count": result.sales_row_count,
                    "competitor_row_count": result.competitor_row_count,
                    "template_fields": result.template_fields,
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

    def _build_automation_first_dataset(
        self,
        *,
        target_product_input: str,
        competitor_inputs: list[str],
        report_date: date | None,
        reporting_period: str,
    ) -> DeckDataset:
        parsed_target = _parse_target_product_input(target_product_input)
        if not parsed_target["source_url"]:
            raise RuntimeError("Target product input is required for the automation-first deck flow.")
        if not competitor_inputs:
            raise RuntimeError("Provide at least one competitor Amazon URL or ASIN for the automation-first deck flow.")

        normalized_competitors = [_parse_competitor_reference(value) for value in competitor_inputs[:5]]
        warnings: list[str] = []
        if len(competitor_inputs) > 5:
            warnings.append("Only the first 5 competitor inputs were used in this v1 deck flow.")
        if len(normalized_competitors) < 5:
            warnings.append("Fewer than 5 competitor inputs were provided, so some comparison slots will stay blank.")

        hero_product = self.product_research.enrich_target_product(parsed_target)
        report_label = reporting_period.strip() or f"As of {(report_date or date.today()).isoformat()}"
        text_fields: dict[str, str] = {
            "deck_mode": "automation_first",
            "brand_name": hero_product.brand_name or parsed_target["brand_name"],
            "brand_domain": parsed_target["domain"],
            "brand_shopify_url": hero_product.source_url if parsed_target["source_type"] == "shopify" else "",
            "hero_product_name": hero_product.title or parsed_target["product_name"],
            "hero_product_handle": parsed_target["product_handle"],
            "hero_product_source_url": hero_product.source_url or parsed_target["source_url"],
            "hero_product_input_type": parsed_target["source_type"],
            "hero_product_price": hero_product.price or "Pending Shopify enrichment",
            "hero_product_bsr": "Pending SP-API enrichment",
            "hero_product_dimensions": hero_product.dimensions or "Pending SP-API enrichment",
            "hero_product_description": hero_product.description,
            "hero_product_type": hero_product.product_type,
            "hero_product_tags": ", ".join(hero_product.tags),
            "hero_product_image_url": hero_product.image_url,
            "hero_product_snapshot": (
                f"{hero_product.title or parsed_target['product_name']} anchors this deck as the hero product for {hero_product.brand_name or parsed_target['brand_name']}."
            ),
            "report_generated_date": (report_date or date.today()).isoformat(),
            "reporting_period": report_label,
            "market_summary": (
                f"We are benchmarking {hero_product.brand_name or parsed_target['brand_name']} against up to five live competitor listings. "
                "Use the comparison slides to validate BSR, category pressure, and listing depth before final recommendations."
            ),
            "executive_summary": (
                f"This automation-first deck uses the target product plus competitor Amazon identifiers to frame the market. "
                "Catalog, BSR, and listing-quality enrichment can be layered in without changing the Canva template."
            ),
            "cro_summary": (
                f"Start with the hero listing for {hero_product.title or parsed_target['product_name']}. "
                "Refine title hierarchy, bullet clarity, and conversion proof before scaling traffic."
            ),
            "seo_summary": (
                "Map the hero listing against competitor naming patterns, category language, and search-intent coverage. "
                "The template should leave room for indexing gaps and keyword priorities."
            ),
            "creative_summary": (
                "Use the competitor set to compare image sequencing, claim clarity, and visual proof. "
                "The deck should call out where design changes will increase click-through and conversion."
            ),
            "advertising_summary": (
                "Advertising recommendations should follow catalog and listing cleanup. "
                "Once the hero offer is clear, direct spend toward the gaps where competitor pressure is highest."
            ),
            "recommended_plan_summary": (
                "Phase 1: enrich the hero listing and lock the positioning. "
                "Phase 2: rebuild content and creative. Phase 3: scale acquisition with disciplined advertising."
            ),
            "expected_impact_summary": (
                "The goal of this deck is to move from identifier-based benchmarking to a production-ready growth plan "
                "without redesigning the Canva workflow."
            ),
            "why_anata_summary": (
                "Anata can turn the opportunity findings into execution across CRO, creative, SEO, and advertising "
                "without fragmenting ownership between multiple vendors."
            ),
            "cta_summary": (
                f"Use this deck to align on the hero SKU, the five main competitors, and the first implementation sprint for {hero_product.brand_name or parsed_target['brand_name']}."
            ),
        }
        warnings.extend(hero_product.warnings)

        competitor_rows = [["competitor", "bsr", "estimated_sales", "estimated_units", "price", "review_count"]]
        top_bsr_rows = [["product_name", "bsr", "sales", "units", "change_from_previous_period"]]
        for slot in range(1, 6):
            competitor = normalized_competitors[slot - 1] if slot - 1 < len(normalized_competitors) else None
            if competitor is None:
                text_fields[f"competitor_{slot}_name"] = ""
                text_fields[f"competitor_{slot}_identifier"] = ""
                text_fields[f"competitor_{slot}_source_url"] = ""
                text_fields[f"competitor_{slot}_bsr"] = ""
                text_fields[f"competitor_{slot}_estimated_sales"] = ""
                text_fields[f"competitor_{slot}_units"] = ""
                text_fields[f"competitor_{slot}_strength"] = ""
                text_fields[f"competitor_{slot}_gap"] = ""
                continue

            enriched_competitor = self.product_research.enrich_competitor_product(competitor)
            warnings.extend(enriched_competitor.warnings)
            text_fields[f"competitor_{slot}_name"] = enriched_competitor.name
            text_fields[f"competitor_{slot}_identifier"] = enriched_competitor.identifier
            text_fields[f"competitor_{slot}_source_url"] = enriched_competitor.source_url
            text_fields[f"competitor_{slot}_asin"] = enriched_competitor.asin
            text_fields[f"competitor_{slot}_brand"] = enriched_competitor.brand
            text_fields[f"competitor_{slot}_category"] = enriched_competitor.category
            text_fields[f"competitor_{slot}_dimensions"] = enriched_competitor.dimensions
            text_fields[f"competitor_{slot}_package_dimensions"] = enriched_competitor.package_dimensions
            text_fields[f"competitor_{slot}_bsr"] = enriched_competitor.bsr
            text_fields[f"competitor_{slot}_estimated_sales"] = enriched_competitor.estimated_sales
            text_fields[f"competitor_{slot}_units"] = enriched_competitor.estimated_units
            text_fields[f"competitor_{slot}_strength"] = enriched_competitor.strength
            text_fields[f"competitor_{slot}_gap"] = enriched_competitor.gap

            competitor_rows.append([enriched_competitor.name, enriched_competitor.bsr, enriched_competitor.estimated_sales, enriched_competitor.estimated_units, "", ""])
            top_bsr_rows.append([enriched_competitor.name, enriched_competitor.bsr, enriched_competitor.estimated_sales, enriched_competitor.estimated_units, ""])

        text_fields["competitor_row_count"] = str(len(competitor_rows) - 1)
        text_fields["top_products_by_bsr_row_count"] = str(len(top_bsr_rows) - 1)
        text_fields["sales_row_count"] = "0"

        return DeckDataset(
            text_fields=text_fields,
            chart_fields={
                "competitor_table": _build_chart_data(competitor_rows),
                "top_products_by_bsr": _build_chart_data(top_bsr_rows),
            },
            warnings=warnings,
            sales_row_count=0,
            competitor_row_count=len(competitor_rows) - 1,
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
        if connection.expires_at is None or connection.expires_at <= now + timedelta(minutes=2):
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

    def _build_design_title(self, *, run_label: str, report_date: date | None, reporting_period: str) -> str:
        if run_label.strip():
            return run_label.strip()[:255]
        if reporting_period.strip():
            return f"Sales Deck | {reporting_period.strip()}"[:255]
        if report_date:
            return f"Sales Deck | {report_date.isoformat()}"[:255]
        return f"Sales Deck | {date.today().isoformat()}"[:255]

    def _latest_canva_connection(self) -> CanvaConnection | None:
        return self.session.execute(
            select(CanvaConnection).order_by(CanvaConnection.updated_at.desc(), CanvaConnection.id.desc()).limit(1)
        ).scalar_one_or_none()

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
            "started_at": run.started_at.isoformat() if run.started_at else "",
            "completed_at": run.completed_at.isoformat() if run.completed_at else "",
        }

    def _scope_string(self, value: Any) -> str:
        if isinstance(value, (list, tuple)):
            return " ".join(str(item).strip() for item in value if str(item).strip())
        return str(value or "").strip()


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
