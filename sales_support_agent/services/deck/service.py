"""DeckGenerationService — orchestration class."""

from __future__ import annotations

import base64
import csv
import html
import io
import json
import mimetypes
import re
import secrets
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse
from typing import Any

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.integrations.amazon_sp_api import AmazonSpApiClient
from sales_support_agent.integrations.shopify import ShopifyStorefrontClient
from sales_support_agent.models.entities import AutomationRun
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.helium10 import (
    CerebroKeywordInsight,
    DistributionSlice,
    Helium10CerebroReport,
    Helium10KeywordReport,
    Helium10XrayReport,
    KeywordInsight,
    WordFrequencyReport,
    XrayProduct,
    parse_cerebro_csv,
    parse_keyword_csv,
    parse_keyword_csvs,
    parse_word_frequency_csv,
    parse_xray_csv,
    parse_xray_csvs,
)
from sales_support_agent.services.product_research import EnrichedHeroProduct, ProductResearchService

from sales_support_agent.services.deck.brand_assets import (
    load_brand_asset,
    load_brand_stylesheet,
)
from sales_support_agent.services.deck.dataset import (  # noqa: F401
    DeckDataset,
    DeckGenerationResult,
    _build_advertising_summary,
    _build_cerebro_metric_cards,
    _build_channel_sections,
    _build_creative_recommendations,
    _build_cro_recommendations,
    _build_executive_summary,
    _build_expected_impact_summary,
    _build_keyword_metric_cards,
    _build_keyword_rows,
    _build_market_metric_cards,
    _build_market_summary,
    _build_plan_summary,
    _build_search_insights,
    _build_seo_recommendations,
    _build_target_opportunities,
    _build_target_snapshot_text,
    _build_why_anata_summary,
    _find_target_row,
    _resolve_target_state,
)
from sales_support_agent.services.deck.formatting import (  # noqa: F401
    DEFAULT_CASE_STUDY_URL,
    DEFAULT_SERVICE_TABS,
    _brand_product_reference,
    _build_chart_data,
    _build_competitor_gap,
    _build_competitor_strength,
    _clean_listing_title,
    _format_channel_label,
    _format_display_date,
    _infer_brand_from_title,
    _is_generic_brand_name,
    _label_float,
    _label_share,
    _looks_like_raw_asin_label,
    _normalize_custom_offer_cards,
    _normalize_offers,
    _preferred_brand_name,
    _preferred_target_title,
    _slugify,
    _target_reference_label,
    _title_token_set,
    _titleize_slug,
    _trim_text,
)
from sales_support_agent.services.deck.parsing import (  # noqa: F401
    _parse_target_product_input,
)
from sales_support_agent.services.deck.rendering import (  # noqa: F401
    _product_to_gallery_item,
    _render_action_item,
    _render_cerebro_rank_summary,
    _render_competitor_landscape_table,
    _render_distribution_card,
    _render_embedded_resource_tabs,
    _render_emphasis_list_item,
    _render_gallery_card,
    _render_help_badge,
    _render_hero_media,
    _render_keyword_table,
    _render_metric_card,
    _render_niche_summary_row,
    _render_offer_card,
    _render_offering_tabs,
    _render_recommendation_item,
    _render_revenue_bar,
    _render_signal_list,
    _render_target_comparison_table,
    _render_word_frequency_bubbles,
)


# ---------------------------------------------------------------------------
# Sanity check: detect when the uploaded CSVs reference a different niche
# than the target product. Surfaces as a warning on the deck so a sales
# audit catches the mismatch before sending to a prospect.
# (Audit follow-up — direct ask after reviewing the Waiu Bottles deck.)
# ---------------------------------------------------------------------------

_NICHE_OVERLAP_THRESHOLD = 0.30  # need 30%+ token overlap to consider niches aligned


def _detect_niche_mismatch(
    *,
    target_title: str,
    keyword_report: Any,
    cerebro_report: Any,
    xray_report: Any,
) -> str:
    """Return a warning string if the keyword/cerebro CSVs reference a different
    niche than the target product. Empty string when datasets look aligned."""
    target_tokens = _title_token_set(target_title)
    if not target_tokens:
        return ""

    keyword_tokens: set[str] = set()
    if keyword_report and getattr(keyword_report, "keywords", None):
        for kw in keyword_report.keywords[:10]:
            keyword_tokens |= _title_token_set(getattr(kw, "phrase", ""))
    if cerebro_report and getattr(cerebro_report, "keywords", None):
        for kw in cerebro_report.keywords[:10]:
            keyword_tokens |= _title_token_set(getattr(kw, "phrase", ""))

    xray_tokens: set[str] = set()
    if xray_report and getattr(xray_report, "products", None):
        for product in xray_report.products[:10]:
            xray_tokens |= _title_token_set(getattr(product, "title", ""))

    issues: list[str] = []
    if keyword_tokens:
        overlap = len(target_tokens & keyword_tokens) / max(len(target_tokens), 1)
        if overlap < _NICHE_OVERLAP_THRESHOLD:
            issues.append("keyword CSV")
    if xray_tokens:
        overlap = len(target_tokens & xray_tokens) / max(len(target_tokens), 1)
        if overlap < _NICHE_OVERLAP_THRESHOLD:
            issues.append("Xray CSV")

    if not issues:
        return ""
    return (
        "Heads up — the target product and the "
        + " + ".join(issues)
        + " appear to reference different niches (token overlap below 30%). "
        + "Verify you uploaded the right datasets before sharing this deck."
    )


class DeckGenerationService:
    def __init__(
        self,
        settings: Settings,
        session: Session,
        *,
        shopify_client: ShopifyStorefrontClient | None = None,
        amazon_client: AmazonSpApiClient | None = None,
    ):
        self.settings = settings
        self.session = session
        self.shopify_client = shopify_client or ShopifyStorefrontClient(settings)
        self.amazon_client = amazon_client or AmazonSpApiClient(settings)
        self.audit = AuditService(session)
        self.product_research = ProductResearchService(
            shopify_client=self.shopify_client,
            amazon_client=self.amazon_client,
        )

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

    def generate_deck(
        self,
        *,
        competitor_xray_csv_bytes: bytes | None = None,
        competitor_xray_filename: str = "",
        competitor_xray_csv_payloads: list[tuple[str, bytes]] | None = None,
        keyword_xray_csv_bytes: bytes | None = None,
        keyword_xray_filename: str = "",
        keyword_xray_csv_payloads: list[tuple[str, bytes]] | None = None,
        cerebro_csv_bytes: bytes | None = None,
        cerebro_filename: str = "",
        word_frequency_csv_bytes: bytes | None = None,
        word_frequency_filename: str = "",
        target_product_input: str = "",
        channels: list[str] | None = None,
        creative_mockup_url: str = "",
        case_study_url: str = "",
        offers: list[str] | None = None,
        offer_payload_json: str = "",
        include_recommended_plan: bool = True,
        growth_plan_inputs: dict[str, Any] | None = None,
        trigger: str = "admin_dashboard",
    ) -> DeckGenerationResult:
        effective_target_input = target_product_input.strip()
        competitor_payloads = [
            (filename, content)
            for filename, content in (competitor_xray_csv_payloads or [])
            if content
        ]
        if not competitor_payloads and competitor_xray_csv_bytes is not None:
            competitor_payloads = [(competitor_xray_filename or "competitors.csv", competitor_xray_csv_bytes)]
        keyword_payloads = [
            (filename, content)
            for filename, content in (keyword_xray_csv_payloads or [])
            if content
        ]
        if not keyword_payloads and keyword_xray_csv_bytes is not None:
            keyword_payloads = [(keyword_xray_filename or "keywords.csv", keyword_xray_csv_bytes)]
        enabled_channels = list(DEFAULT_SERVICE_TABS)
        enabled_offers = _normalize_offers(offers or [])
        offer_cards = _normalize_custom_offer_cards(offer_payload_json=offer_payload_json, offers=enabled_offers)
        run = self.audit.start_run(
            "deck_generation",
            trigger=trigger,
            metadata={
                "generation_mode": "amazon_first_html",
                "target_product_input": effective_target_input,
                "competitor_xray_filename": ", ".join(filename for filename, _ in competitor_payloads),
                "keyword_xray_filename": ", ".join(filename for filename, _ in keyword_payloads),
                "cerebro_filename": cerebro_filename.strip(),
                "word_frequency_filename": word_frequency_filename.strip(),
                "channels": enabled_channels,
                "creative_mockup_url": creative_mockup_url.strip(),
                "case_study_url": case_study_url.strip(),
                "offers": [str(card.get("title", "")).strip() for card in offer_cards],
                "include_recommended_plan": bool(include_recommended_plan),
            },
        )
        try:
            dataset = self._build_amazon_first_dataset(
                target_product_input=effective_target_input,
                competitor_xray_csv_payloads=competitor_payloads,
                keyword_xray_csv_payloads=keyword_payloads,
                cerebro_csv_bytes=cerebro_csv_bytes,
                word_frequency_csv_bytes=word_frequency_csv_bytes,
                channels=enabled_channels,
                creative_mockup_url=creative_mockup_url.strip(),
                case_study_url=case_study_url.strip(),
                offers=enabled_offers,
                offer_cards=offer_cards,
                include_recommended_plan=bool(include_recommended_plan),
                growth_plan_inputs=growth_plan_inputs,
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
                    **{
                        # Forward the persisted snapshot fields from
                        # _generate_html_deck so the audit summary mirrors
                        # what's on the AutomationRun row.
                        key: (run.summary_json or {}).get(key, default)
                        for key, default in (
                            ("export_token", ""),
                            ("deck_html", ""),
                            ("deck_slug", ""),
                            ("target_product_identifier", ""),
                            ("channels", []),
                            ("view_count", 0),
                            ("first_viewed_at", ""),
                            ("last_viewed_at", ""),
                        )
                    },
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
        target = dataset.deck_payload.get("target", {})
        target_identifier = str(target.get("asin") or target.get("source_url") or "").strip()

        # If this run already has view-tracking state (e.g. an admin-triggered
        # re-render of an existing run), preserve it. On first generation the
        # snapshot is empty, so the values default to zero / empty string.
        prior = dict(run.summary_json or {})
        prior_view_count = int(prior.get("view_count", 0) or 0)
        prior_first_viewed_at = str(prior.get("first_viewed_at", "") or "")
        prior_last_viewed_at = str(prior.get("last_viewed_at", "") or "")

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
            "view_count": prior_view_count,
            "first_viewed_at": prior_first_viewed_at,
            "last_viewed_at": prior_last_viewed_at,
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

    def _build_amazon_first_dataset(
        self,
        *,
        target_product_input: str,
        competitor_xray_csv_payloads: list[tuple[str, bytes]],
        keyword_xray_csv_payloads: list[tuple[str, bytes]],
        cerebro_csv_bytes: bytes | None,
        word_frequency_csv_bytes: bytes | None,
        channels: list[str],
        creative_mockup_url: str,
        case_study_url: str,
        offers: list[str],
        offer_cards: list[dict[str, str]],
        include_recommended_plan: bool,
        growth_plan_inputs: dict[str, Any] | None = None,
    ) -> DeckDataset:
        parsed_target = _parse_target_product_input(target_product_input)
        if parsed_target["source_type"] not in {"amazon", "shopify", "website"}:
            raise RuntimeError("Target product must be a product URL or an Amazon ASIN.")
        if not competitor_xray_csv_payloads:
            raise RuntimeError("Competitor Xray CSV is required.")

        xray_report = parse_xray_csvs([content for _, content in competitor_xray_csv_payloads])
        keyword_report = parse_keyword_csvs([content for _, content in keyword_xray_csv_payloads])
        try:
            cerebro_report = parse_cerebro_csv(cerebro_csv_bytes)
        except RuntimeError as exc:
            warnings = [str(exc)]
            cerebro_report = None
        else:
            warnings = []
        try:
            word_frequency_report = parse_word_frequency_csv(word_frequency_csv_bytes)
        except RuntimeError as exc:
            warnings.append(str(exc))
            word_frequency_report = None
        hero_product = self.product_research.enrich_target_product(parsed_target)
        warnings = [*warnings, *xray_report.warnings, *hero_product.warnings]
        if keyword_report:
            warnings.extend(keyword_report.warnings)
        if cerebro_report:
            warnings.extend(cerebro_report.warnings)
        if word_frequency_report:
            warnings.extend(word_frequency_report.warnings)

        # Audit follow-up: CSV-niche-mismatch sanity check.
        # If the target product title and the keyword/cerebro CSVs reference
        # totally different niches (low token overlap), surface a warning so
        # the deck doesn't silently glue mismatched datasets together (as
        # happened with the Waiu Bottles deck — breast milk cooler target
        # paired with hair-care keywords and a dog-food category).
        mismatch_warning = _detect_niche_mismatch(
            target_title=hero_product.title or parsed_target.get("product_name", ""),
            keyword_report=keyword_report,
            cerebro_report=cerebro_report,
            xray_report=xray_report,
        )
        if mismatch_warning:
            warnings.append(mismatch_warning)

        verified_target_asin = (parsed_target["asin"] or hero_product.asin).strip()
        target_match = _find_target_row(
            xray_report,
            asin=verified_target_asin,
            candidate_asin=hero_product.candidate_asin,
            source_url=hero_product.source_url or parsed_target["source_url"],
            title=hero_product.title or parsed_target["product_name"],
            brand_name=hero_product.brand_name or parsed_target.get("brand_name", ""),
        )
        target_row = target_match.product
        resolved_target_asin = (
            verified_target_asin
            or (target_row.asin if target_row else "")
        ).strip()
        primary_competitors = [
            product
            for product in xray_report.products
            if (not resolved_target_asin or product.asin.upper() != resolved_target_asin.upper())
            and (not target_row or product.asin.upper() != target_row.asin.upper())
        ][:10]
        market_cards = _build_market_metric_cards(xray_report, keyword_report)
        keyword_cards = _build_keyword_metric_cards(keyword_report)
        keyword_cards.extend(_build_cerebro_metric_cards(cerebro_report, word_frequency_report))
        search_insights = _build_search_insights(
            title=hero_product.title or parsed_target["product_name"],
            description=hero_product.description,
            keyword_report=keyword_report,
            cerebro_report=cerebro_report,
            word_frequency_report=word_frequency_report,
        )
        target_title = _preferred_target_title(
            hero_product.title,
            target_row.title if target_row else "",
            parsed_target["product_name"],
            parsed_target["asin"],
        )
        target_brand = _preferred_brand_name(
            hero_product.brand_name,
            target_row.brand if target_row else "",
            parsed_target.get("brand_name", ""),
        )
        display_title = _clean_listing_title(target_title)
        if _looks_like_raw_asin_label(display_title):
            fallback_phrase = keyword_report.keywords[0].phrase if keyword_report and keyword_report.keywords else ""
            if fallback_phrase:
                display_title = f"{_titleize_slug(fallback_phrase)} product"
            elif not _is_generic_brand_name(target_brand):
                display_title = f"{target_brand} product"
            else:
                display_title = "Target product"
        if _is_generic_brand_name(target_brand):
            inferred_brand = _infer_brand_from_title(display_title)
            if inferred_brand:
                target_brand = inferred_brand
        seo_recommendations = _build_seo_recommendations(
            keyword_report,
            cerebro_report,
            xray_report,
            search_insights,
            brand_name=target_brand,
            target_title=display_title,
        )
        cro_recommendations = _build_cro_recommendations(target_row, primary_competitors)
        creative_recommendations = _build_creative_recommendations(target_row, primary_competitors)
        channel_sections = _build_channel_sections(channels)
        competitor_rows = [["Product", "ASIN", "Brand", "Price", "Revenue", "Market share", "Reviews", "BSR", "Fulfillment"]]
        for product in xray_report.products[:10]:
            competitor_rows.append(
                [
                    product.title,
                    product.asin,
                    product.brand,
                    product.price_label,
                    product.revenue_label,
                    _label_share(product.revenue, xray_report.total_revenue),
                    str(product.review_count or ""),
                    product.bsr_label,
                    product.fulfillment,
                ]
            )

        keyword_rows = _build_keyword_rows(keyword_report, cerebro_report)

        target_image_url = (hero_product.image_url or (target_row.image_url if target_row else "")).strip()
        target_price_label = (
            hero_product.price
            or (target_row.price_label if target_row else "")
            or "Unavailable"
        ).strip()
        target_bsr_label = (
            (target_row.bsr_label if target_row and target_row.bsr_label != "n/a" else "")
            or _label_float(hero_product.bsr, 0)
        ).strip()
        target_review_count = target_row.review_count if target_row and target_row.review_count is not None else hero_product.review_count
        target_rating_label = (
            (target_row.rating_label if target_row and target_row.rating_label != "n/a" else "")
            or _label_float(hero_product.rating, 1)
        ).strip()
        target_revenue_label = (
            (target_row.revenue_label if target_row and target_row.revenue_label != "n/a" else "")
        ).strip()
        target_dimensions = (hero_product.dimensions or (target_row.dimensions if target_row else "")).strip()
        target_state = _resolve_target_state(parsed_target["source_type"], target_row=target_row, hero_product=hero_product)
        target_strengths, target_gaps = _build_target_opportunities(
            comparison_mode=target_state,
            brand_name=target_brand,
            target_row=target_row,
            hero_product=hero_product,
            search_insights=search_insights,
            market_average_price=xray_report.average_price or 0.0,
            best_seller=xray_report.products[0] if xray_report.products else None,
        )
        effective_date = datetime.now(timezone.utc).date()

        text_fields: dict[str, str] = {
            "deck_mode": "amazon_first_html",
            "brand_name": target_brand,
            "hero_product_name": _brand_product_reference(target_brand),
            "hero_product_source_url": hero_product.source_url or parsed_target["source_url"],
            "hero_product_input_type": parsed_target["source_type"],
            "hero_product_price": target_price_label,
            "hero_product_bsr": target_bsr_label,
            "hero_product_dimensions": target_dimensions,
            "hero_product_description": hero_product.description,
            "hero_product_type": hero_product.product_type or (target_row.category if target_row else ""),
            "hero_product_tags": ", ".join(hero_product.tags),
            "hero_product_image_url": target_image_url,
            "hero_product_snapshot": _build_target_snapshot_text(
                display_title,
                target_brand,
                target_row,
                comparison_mode=target_state,
                hero_product=hero_product,
            ),
            "report_generated_date": _format_display_date(effective_date),
            "reporting_period": _format_display_date(effective_date),
            "market_summary": _build_market_summary(target_brand, xray_report, keyword_report),
            "executive_summary": _build_executive_summary(display_title, target_brand, xray_report, keyword_report),
            "cro_summary": " ".join(cro_recommendations[:2]),
            "seo_summary": " ".join(seo_recommendations[:2]),
            "creative_summary": " ".join(creative_recommendations[:2]),
            "advertising_summary": _build_advertising_summary(xray_report, keyword_report),
            "recommended_plan_summary": _build_plan_summary(offer_cards, channels),
            "expected_impact_summary": _build_expected_impact_summary(xray_report),
            "why_anata_summary": _build_why_anata_summary(channels),
            "deck_title": f"{target_brand} x anata strategy deck".strip(" -"),
            "target_asin": resolved_target_asin,
            "target_rating": target_rating_label,
            "target_review_count": str(target_review_count or ""),
            "target_revenue": target_revenue_label,
            "target_comparison_mode": target_state,
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

        # Build the optional Growth Plan Synopsis section.
        growth_plan = None
        if growth_plan_inputs is not None:
            from sales_support_agent.services.deck.growth_plan import (
                build_growth_plan,
                parse_growth_plan_inputs,
            )
            inputs_obj = parse_growth_plan_inputs(growth_plan_inputs)
            target_units_int = int(round(target_row.units_sold or 0)) if target_row else 0
            top3_avg_sessions = None
            if xray_report.products:
                top3 = xray_report.products[:3]
                cvr_decimal = max(inputs_obj.conversion_rate_pct / 100.0, 0.001)
                top3_avg_units = sum(p.units_sold or 0.0 for p in top3) / len(top3)
                top3_avg_sessions = int(round(top3_avg_units / cvr_decimal))
            if inputs_obj.average_order_value is None:
                # Fall back to target's price as AOV proxy.
                price_str = str(target_price_label or "").lstrip("$").replace(",", "")
                try:
                    aov_fallback = float(price_str) if price_str else None
                except ValueError:
                    aov_fallback = None
                if aov_fallback is not None:
                    inputs_obj = type(inputs_obj)(
                        **{**inputs_obj.__dict__, "average_order_value": aov_fallback}
                    )
            growth_plan = build_growth_plan(
                inputs=inputs_obj,
                target_units=target_units_int,
                top3_competitor_avg_sessions=top3_avg_sessions,
            )

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
                    "asin": resolved_target_asin,
                    "source_url": text_fields["hero_product_source_url"],
                    "title": display_title,
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
                    "comparison_mode": target_state,
                    "match_provenance": target_match.provenance,
                    "match_confidence": target_match.confidence,
                    "market_metrics_source": hero_product.market_metrics_source or ("xray" if target_row else ""),
                },
                "market_cards": market_cards,
                "keyword_cards": keyword_cards,
                "xray_report": xray_report,
                "keyword_report": keyword_report,
                "cerebro_report": cerebro_report,
                "word_frequency_report": word_frequency_report,
                "keyword_table_rows": keyword_rows,
                "primary_competitors": primary_competitors,
                "seo_recommendations": seo_recommendations,
                "cro_recommendations": cro_recommendations,
                "creative_recommendations": creative_recommendations,
                "offering_sections": channel_sections,
                "channels": channels,
                "niche_keyword": (
                    cerebro_report.keywords[0].phrase
                    if cerebro_report and cerebro_report.keywords
                    else (
                        keyword_report.keywords[0].phrase
                        if keyword_report and keyword_report.keywords
                        else (parsed_target["product_name"] or display_title)
                    )
                ),
                "search_insights": search_insights,
                "target_strengths": target_strengths,
                "target_gaps": target_gaps,
                "creative_mockup_url": creative_mockup_url,
                "case_study_url": case_study_url or DEFAULT_CASE_STUDY_URL,
                "offer_cards": offer_cards,
                "include_recommended_plan": include_recommended_plan,
                "growth_plan": growth_plan,
            },
        )

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
        cerebro_report = payload.get("cerebro_report")
        if cerebro_report is not None and not isinstance(cerebro_report, Helium10CerebroReport):
            cerebro_report = None
        market_cards = list(payload.get("market_cards", []))
        keyword_cards = list(payload.get("keyword_cards", []))
        primary_competitors = list(payload.get("primary_competitors", []))
        seo_recommendations = list(payload.get("seo_recommendations", []))
        cro_recommendations = list(payload.get("cro_recommendations", []))
        creative_recommendations = list(payload.get("creative_recommendations", []))
        offering_sections = list(payload.get("offering_sections", []))
        search_insights = dict(payload.get("search_insights", {}))
        target_strengths = list(payload.get("target_strengths", []))
        target_gaps = list(payload.get("target_gaps", []))
        creative_mockup_url = str(payload.get("creative_mockup_url", "") or "").strip()
        case_study_url = str(payload.get("case_study_url", "") or "").strip()
        offer_cards = list(payload.get("offer_cards", []))
        include_recommended_plan = bool(payload.get("include_recommended_plan", True))
        growth_plan_obj = payload.get("growth_plan")
        if growth_plan_obj is not None:
            from sales_support_agent.services.deck.growth_plan import (
                GrowthPlan,
                render_growth_plan_section,
            )
            if isinstance(growth_plan_obj, GrowthPlan):
                growth_plan_html = render_growth_plan_section(
                    growth_plan_obj,
                    target_brand=str(payload.get("target", {}).get("brand_name") or "the prospect"),
                )
            else:
                growth_plan_html = ""
        else:
            growth_plan_html = ""
        monogram = self._load_brand_asset("assets/monogram.png")
        no_product_image = self._load_brand_asset("assets/no-product-image-available.png")
        stylesheet = self._load_brand_stylesheet()
        keyword_table_rows = payload.get("keyword_table_rows") or []
        if not isinstance(keyword_table_rows, list):
            keyword_table_rows = []
        revenue_bars = "".join(_render_revenue_bar(product, xray_report.total_revenue) for product in xray_report.products[:8])
        offering_html = _render_offering_tabs(offering_sections)
        gallery_items = [target] + [_product_to_gallery_item(product) for product in primary_competitors[:4]]
        gallery_html = "".join(_render_gallery_card(item) for item in gallery_items if item)
        market_summary_html = "".join(_render_metric_card(card) for card in market_cards)
        keyword_summary_html = "".join(_render_metric_card(card) for card in keyword_cards)
        country_donut = _render_distribution_card("Seller country of origin", xray_report.seller_country_distribution)
        size_donut = _render_distribution_card("Size tier", xray_report.size_tier_distribution)
        fulfillment_donut = _render_distribution_card("Fulfillment", xray_report.fulfillment_distribution)
        niche_keyword = str(payload.get("niche_keyword") or target.get("asin") or "the niche").strip()
        keyword_table_html = _render_keyword_table(keyword_table_rows)
        keyword_table_caption = "Top keyword opportunities from the Cerebro rank set" if cerebro_report else "Highest search volume from the current keyword dataset"
        keyword_rank_summary_html = _render_cerebro_rank_summary(cerebro_report)
        keyword_bubble_html = _render_word_frequency_bubbles(payload.get("word_frequency_report"))
        niche_table_rows = "".join(
            _render_niche_summary_row(product, xray_report.total_revenue)
            for product in xray_report.products[:10]
        )
        # Audit item 3: brand-aggregated view of the same data, toggled by a
        # button group above the table.
        from sales_support_agent.services.deck.rendering import (
            _aggregate_brands,
            _render_niche_summary_brand_row,
        )
        brand_buckets = _aggregate_brands(xray_report.products)[:10]
        niche_table_brand_rows = "".join(
            _render_niche_summary_brand_row(b, xray_report.total_revenue, i + 1)
            for i, b in enumerate(brand_buckets)
        )
        search_title_html = _render_signal_list("Title coverage", search_insights.get("title_hits", []), search_insights.get("title_misses", []), "Missing title targets")
        search_copy_html = _render_signal_list("Bullet / copy coverage", search_insights.get("copy_hits", []), search_insights.get("copy_misses", []), "Missing copy targets")
        target_strength_html = "".join(_render_action_item(item) for item in target_strengths)
        target_gap_html = "".join(_render_action_item(item) for item in target_gaps)
        best_seller = xray_report.products[0] if xray_report.products else None
        comparison_mode = str(target.get("comparison_mode", "") or "")
        launch_mode = comparison_mode == "concept_only"
        competitor_landscape_table = _render_competitor_landscape_table(xray_report.products[:10], xray_report.total_revenue)
        comparison_table_html = _render_target_comparison_table(target, best_seller, no_product_image)
        target_identifier = str(target.get("asin") or "").strip()
        resolved_target_title = _clean_listing_title(str(target.get("title", "") or ""))
        if resolved_target_title and not re.fullmatch(r"ASIN\s+[A-Z0-9]{10}", resolved_target_title, flags=re.IGNORECASE):
            target_reference_label = _trim_text(resolved_target_title, 40)
        elif target_identifier:
            target_reference_label = f"ASIN {target_identifier}"
        else:
            target_reference_label = _target_reference_label(target)
        recommended_plan_html = ""
        if include_recommended_plan:
            offer_html = "".join(_render_offer_card(card) for card in offer_cards)
            recommended_plan_html = f"""
    <section class="slide">
      <div class="slide-head">
        <div>
          <p class="eyebrow">Proposed offers</p>
          <h2>Proposed offers and next step</h2>
        </div>
        <p class="muted">Choose the operating model, then move directly into the first growth sprint with clear ownership and the next action already mapped.</p>
      </div>
      {f"<div class='offer-grid'>{offer_html}</div>" if offer_html else ""}
      <div class="plan-grid">
        <div class="plan-card plan-card-cta">
          <p class="eyebrow-subtle">Next action</p>
          <h3>Schedule a meeting</h3>
          <p>Review the engagement options, align on the first sprint, and map the next execution window.</p>
          <a class="plan-link" href="https://anatainc.com/contact" target="_blank" rel="noreferrer">Schedule a meeting</a>
        </div>
        <div class="plan-card">
          <p class="eyebrow-subtle">Why now</p>
          <h3>Expected impact</h3>
          <p>{html.escape(dataset.text_fields.get("expected_impact_summary") or "")}</p>
        </div>
        <div class="plan-card">
          <p class="eyebrow-subtle">What happens next</p>
          <h3>Recommended next step</h3>
          <p>{html.escape(dataset.text_fields.get("why_anata_summary") or "")}</p>
        </div>
      </div>
    </section>"""
        target_brand_display = str(target.get("brand_name") or target.get("brand") or "Prospect brand").strip()
        cover_title = _trim_text(_clean_listing_title(str(target.get("title", "") or title)), 40)
        resource_embed_html = _render_embedded_resource_tabs(
            case_study_url=case_study_url,
            creative_mockup_url=creative_mockup_url,
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
      </div>
      <button class="print-button" onclick="window.print()">Print / Save PDF</button>
    </section>

    <section class="slide slide-cover">
      <div class="cover-grid">
        <div>
          <p class="eyebrow">Amazon-first strategy deck</p>
          <h1>{html.escape(cover_title)}</h1>
          <p class="lead">{html.escape(dataset.text_fields.get("executive_summary") or "")}</p>
          <div class="pill-row">
            <span class="pill">{html.escape(target_reference_label)}</span>
            <span class="pill">{html.escape(dataset.text_fields.get("report_generated_date") or "")}</span>
            <span class="pill">{html.escape(" • ".join(_format_channel_label(value) for value in (payload.get("channels", []) or [])) or "Amazon")}</span>
          </div>
        </div>
        <div class="cover-card">
          {_render_hero_media(target, no_product_image)}
        </div>
      </div>
    </section>

    <section class="slide">
      <div class="slide-head">
        <div>
          <p class="eyebrow">Market summary</p>
          <h2>Summary of "{html.escape(niche_keyword)}"</h2>
        </div>
        <p class="muted">{html.escape(dataset.text_fields.get("market_summary") or "")}</p>
      </div>
      <div class="metric-grid">{market_summary_html}</div>
      <div class="dashboard-grid market-summary-grid">
        <div class="dashboard-card niche-table-card">
          <div class="card-head">
            <h3>Competitor revenue breakdown</h3>
            <div class="niche-toggle" role="tablist" aria-label="Competitor breakdown view">
              <button type="button" class="niche-toggle-btn is-active" data-niche-view="asin" aria-pressed="true">By ASIN</button>
              <button type="button" class="niche-toggle-btn" data-niche-view="brand" aria-pressed="false">By Brand</button>
            </div>
          </div>
          <div class="table-wrap niche-table-wrap">
            <table class="niche-table" data-niche-view-target>
              <thead>
                <tr>
                  <th>#</th>
                  <th data-asin-only>Product</th>
                  <th data-brand-only hidden>Brand</th>
                  <th>Price</th>
                  <th>Revenue</th>
                  <th>Share</th>
                </tr>
              </thead>
              <tbody data-view="asin">
                {niche_table_rows}
              </tbody>
              <tbody data-view="brand" hidden>
                {niche_table_brand_rows}
              </tbody>
            </table>
          </div>
          <div class="revenue-bars">{revenue_bars}</div>
        </div>
        <div class="donut-grid compact-donut-grid">
          {country_donut}
          {size_donut}
          {fulfillment_donut}
        </div>
      </div>
    </section>

    <section class="slide">
      <div class="slide-head">
        <div>
          <p class="eyebrow">{'Market entry benchmark' if launch_mode else 'Target listing'}</p>
          <h2>{'Launch benchmark opportunities' if launch_mode else 'Target listing opportunities'}</h2>
        </div>
        <p class="muted">{html.escape(dataset.text_fields.get("hero_product_snapshot") or "")}</p>
      </div>
      <div class="target-panel target-panel-full">
        {comparison_table_html}
      </div>
      <div class="two-col split-top">
        <div class="recommendation-card">
          <h3>{'What the product already signals well' if launch_mode else 'What the listing is already doing well'}</h3>
          <ul>{target_strength_html}</ul>
        </div>
        <div class="recommendation-card">
          <h3>{'What needs to be built before launch' if launch_mode else 'What is missing right now'}</h3>
          <ul>{target_gap_html}</ul>
        </div>
      </div>
    </section>

    <section class="slide">
      <div class="slide-head">
        <div>
          <p class="eyebrow">Competitor landscape</p>
          <h2>Who owns the page one real estate</h2>
        </div>
        <p class="muted">This view compares the top page-one listings across price, revenue, reviews, and share of the visible market.</p>
      </div>
      <div class="table-wrap">{competitor_landscape_table}</div>
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
        <div class="recommendation-card">
          {search_title_html}
        </div>
        <div class="recommendation-card">
          {search_copy_html}
        </div>
      </div>
      <div class="two-col split-top">
        <div class="dashboard-card">
          <div class="card-head">
            <h3>Top keyword opportunities</h3>
            <span class="muted">{html.escape(keyword_table_caption)}</span>
          </div>
          <div class="table-wrap">
            <table>
              {keyword_table_html}
            </table>
          </div>
        </div>
        <div class="recommendation-card">
          <h3>SEO actions {_render_help_badge("These are directional keyword and copy suggestions based on the current category set. Final indexing and conversion results will vary.")}</h3>
          <ul class="emphasis-list">{''.join(_render_emphasis_list_item(item) for item in seo_recommendations)}</ul>
        </div>
      </div>
      {(
        "<div class='two-col split-top'>"
        f"{keyword_rank_summary_html}"
        f"{keyword_bubble_html}"
        "</div>"
      ) if (keyword_rank_summary_html or keyword_bubble_html) else ""}
    </section>

    {growth_plan_html}

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
          <h3>CRO recommendations {_render_help_badge("CRO recommendations focus on PDP clarity, proof, and the path to purchase.")}</h3>
          <ul>{''.join(_render_recommendation_item(item) for item in cro_recommendations)}</ul>
        </div>
        <div class="recommendation-card">
          <h3>Creative recommendations {_render_help_badge("Creative recommendations focus on imagery, comparison frames, and visual proof.")}</h3>
          <ul>{''.join(_render_recommendation_item(item) for item in creative_recommendations)}</ul>
        </div>
      </div>
      <div class="gallery-grid">{gallery_html}</div>
      {resource_embed_html}
    </section>

    {offering_html}
      {recommended_plan_html}
  </main>
  <script>
    document.querySelectorAll(".offering-tabs").forEach((tabsRoot) => {{
      tabsRoot.querySelectorAll(".offering-tab").forEach((button) => {{
        button.addEventListener("click", () => {{
          const target = button.dataset.tab;
          const section = tabsRoot.closest(".slide");
          section?.querySelectorAll(".offering-tab").forEach((node) => node.classList.toggle("is-active", node === button));
          section?.querySelectorAll(".offering-panel").forEach((panel) => {{
            const isActive = panel.dataset.panel === target;
            panel.classList.toggle("is-active", isActive);
            panel.hidden = !isActive;
          }});
        }});
      }});
    }});
    document.querySelectorAll(".embedded-tabs").forEach((tabsRoot) => {{
      tabsRoot.querySelectorAll(".embedded-tab").forEach((button) => {{
        button.addEventListener("click", () => {{
          const target = button.dataset.tab;
          const section = tabsRoot.closest(".embedded-resource-section");
          section?.querySelectorAll(".embedded-tab").forEach((node) => node.classList.toggle("is-active", node === button));
          section?.querySelectorAll(".embedded-panel").forEach((panel) => {{
            const isActive = panel.dataset.panel === target;
            panel.classList.toggle("is-active", isActive);
            panel.hidden = !isActive;
          }});
        }});
      }});
    }});
    // Audit item 3: ASIN ↔ Brand toggle on competitor revenue breakdown table.
    document.querySelectorAll(".niche-toggle").forEach((toggleRoot) => {{
      const card = toggleRoot.closest(".niche-table-card") || toggleRoot.closest(".dashboard-card");
      toggleRoot.querySelectorAll(".niche-toggle-btn").forEach((button) => {{
        button.addEventListener("click", () => {{
          const view = button.dataset.nicheView;
          toggleRoot.querySelectorAll(".niche-toggle-btn").forEach((node) => {{
            const isActive = node === button;
            node.classList.toggle("is-active", isActive);
            node.setAttribute("aria-pressed", isActive ? "true" : "false");
          }});
          card?.querySelectorAll("tbody[data-view]").forEach((tbody) => {{
            tbody.hidden = tbody.dataset.view !== view;
          }});
          card?.querySelectorAll("[data-asin-only]").forEach((node) => {{
            node.hidden = view !== "asin";
          }});
          card?.querySelectorAll("[data-brand-only]").forEach((node) => {{
            node.hidden = view !== "brand";
          }});
        }});
      }});
    }});
  </script>
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
            "view_analytics": dict(summary.get("view_analytics", {}) or {}),
            "started_at": run.started_at.isoformat() if run.started_at else "",
            "completed_at": run.completed_at.isoformat() if run.completed_at else "",
        }

    def _load_brand_stylesheet(self) -> str:
        return load_brand_stylesheet(self.settings)

    def _load_brand_asset(self, relative_path: str) -> str:
        return load_brand_asset(self.settings, relative_path)
