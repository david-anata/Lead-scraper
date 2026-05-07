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
    load_brand_favicon_link,
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
    render_visual_proof_panel,
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
        amazon_client: AmazonSpApiClient | None = None,
    ):
        self.settings = settings
        self.session = session
        self.amazon_client = amazon_client or AmazonSpApiClient(settings)
        self.audit = AuditService(session)
        self.product_research = ProductResearchService(
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
        target_xray_csv_bytes: bytes | None = None,
        target_xray_filename: str = "",
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
                target_xray_csv_bytes=target_xray_csv_bytes,
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
                            ("story_markdown", ""),
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

        # Build the Story markdown companion (text-based deck for sales calls).
        # Persisted alongside the HTML so the Story routes can serve it without
        # re-running the dataset pipeline.
        try:
            from sales_support_agent.services.deck.growth_plan import GrowthPlan
            from sales_support_agent.services.deck.story import build_story_markdown

            _growth_plan_obj = dataset.deck_payload.get("growth_plan")
            _plan_for_story = (
                _growth_plan_obj if isinstance(_growth_plan_obj, GrowthPlan) else None
            )
            _target_aov_for_story = float(getattr(self, "_growth_plan_aov", 0.0) or 0.0)
            if _target_aov_for_story <= 0:
                _price_str = str(target.get("price") or "")
                try:
                    _target_aov_for_story = float(re.sub(r"[^0-9.]", "", _price_str) or 0)
                except ValueError:
                    _target_aov_for_story = 0.0
            story_markdown = build_story_markdown(
                payload=dataset.deck_payload,
                plan=_plan_for_story,
                target_brand=str(target.get("brand_name") or "the prospect"),
                target_aov=_target_aov_for_story,
            )
        except Exception as _story_exc:  # pragma: no cover - defensive only
            # Story is a nice-to-have; never block deck creation.
            import logging
            logging.getLogger(__name__).warning(
                "Story markdown generation failed: %s", _story_exc, exc_info=True
            )
            story_markdown = ""

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
            "story_markdown": story_markdown,
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
        target_xray_csv_bytes: bytes | None,
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
        if parsed_target["source_type"] not in {"amazon", "website"}:
            raise RuntimeError("Target product must be an Amazon ASIN/URL or a product website URL.")
        if not competitor_xray_csv_payloads:
            raise RuntimeError("Competitor Xray CSV is required.")

        xray_report = parse_xray_csvs([content for _, content in competitor_xray_csv_payloads])

        # When the user uploads a separate Target Xray CSV (single-row export
        # of just the prospect listing), parse it and use that row as the
        # canonical target. Solves the failure mode where the prospect's
        # ASIN isn't in the page-one competitor set, SP-API enrichment
        # isn't configured, and every target metric ends up "Unavailable".
        target_xray_row = None
        if target_xray_csv_bytes:
            try:
                target_xray_report = parse_xray_csvs([target_xray_csv_bytes])
                if target_xray_report.products:
                    target_xray_row = target_xray_report.products[0]
            except RuntimeError as exc:
                # Surface as a warning so the deck still renders.
                pass  # Will be added to warnings list below

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
        # If the user uploaded a separate Target Xray CSV, that row wins —
        # it's the most authoritative single-row export of the target's H10
        # data and supersedes whatever fuzzy match we got out of the
        # competitor set.
        if target_xray_row is not None:
            target_row = target_xray_row
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
            # Stash the resolved AOV (user override OR target-price fallback)
            # so the funnel renderer can use it for projected-revenue math.
            self._growth_plan_aov = float(inputs_obj.average_order_value or 0.0)

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
                # Use the AOV that was resolved at dataset-build time (user
                # override OR target-price fallback). Stashed on self by
                # _build_amazon_first_dataset.
                _target_aov = float(getattr(self, "_growth_plan_aov", 0.0) or 0.0)
                if _target_aov <= 0:
                    # Defensive fallback if the dataset didn't stash it
                    _target_price_str = str(payload.get("target", {}).get("price") or "")
                    try:
                        _target_aov = float(re.sub(r"[^0-9.]", "", _target_price_str) or 0)
                    except ValueError:
                        _target_aov = 0.0
                growth_plan_html = render_growth_plan_section(
                    growth_plan_obj,
                    target_brand=str(payload.get("target", {}).get("brand_name") or "the prospect"),
                    target_aov=_target_aov,
                )
            else:
                growth_plan_html = ""
        else:
            growth_plan_html = ""
        monogram = self._load_brand_asset("assets/monogram.png")
        no_product_image = self._load_brand_asset("assets/no-product-image-available.png")
        stylesheet = self._load_brand_stylesheet()
        favicon_link = load_brand_favicon_link(self.settings)
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
            # PR35: feature the FIRST offer card. Design has a "Recommended"
            # pill on the lead card + sky-deep filled CTA; subsequent cards
            # render plain with a navy filled CTA.
            offer_html = "".join(
                _render_offer_card(card, featured=(idx == 0))
                for idx, card in enumerate(offer_cards)
            )
            # PR35: Replace the legacy `.plan-grid` (3 stacked cards with
            # eyebrow-subtle / h3 / p) with the design's `.next-steps` 3-tile
            # horizontal grid (Step 1 = sky-deep CTA tile + 2 plain tiles).
            _impact = html.escape(dataset.text_fields.get("expected_impact_summary") or "")
            _next_step = html.escape(dataset.text_fields.get("why_anata_summary") or "")
            recommended_plan_html = f"""
    <section class="slide slide-offers" data-screen-label="08 Proposed offers">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Proposed offers and next step</p>
          <h2 class="slide-title">Choose your engagement</h2>
        </div>
        <p class="caption">Choose the operating model, then move directly into the first growth sprint with clear ownership and the next action already mapped.</p>
      </header>
      {f'<div class="offer-grid">{offer_html}</div>' if offer_html else ""}
      <div class="next-steps">
        <div class="next-step cta">
          <span class="num">Step 1</span>
          <h4>Pick your engagement</h4>
          <p>Confirm which offer fits your stage. We can also tailor scope if neither matches exactly.</p>
          <a class="link" href="https://anatainc.com/contact" target="_blank" rel="noreferrer">Schedule kickoff →</a>
        </div>
        <div class="next-step">
          <span class="num">Step 2</span>
          <h4>Why now</h4>
          <p>{_impact or "The category window is open — review density is within reach of the leaders, and the conversion mechanics are the unlock."}</p>
        </div>
        <div class="next-step">
          <span class="num">Step 3</span>
          <h4>What happens next</h4>
          <p>{_next_step or "Within 5 business days: kickoff call, audit access, first listing rewrite drafted. First paid campaigns live in week 2."}</p>
        </div>
      </div>
    </section>"""
        target_brand_display = str(target.get("brand_name") or target.get("brand") or "Prospect brand").strip()
        cover_title = _trim_text(_clean_listing_title(str(target.get("title", "") or title)), 40)
        resource_embed_html = _render_embedded_resource_tabs(
            case_study_url=case_study_url,
            creative_mockup_url=creative_mockup_url,
        )

        # ============================================================
        # PR32: Redesigned deck shell — left rail nav + exec summary +
        # findings strip + section dividers between slides.
        # ============================================================

        def _money_short(value: float) -> str:
            try:
                v = float(value)
            except (TypeError, ValueError):
                return "—"
            if v >= 1_000_000:
                return f"${v / 1_000_000:.1f}M"
            if v >= 1_000:
                return f"${v / 1_000:.0f}k"
            return f"${v:,.0f}"

        def _count_short(value: float | int) -> str:
            try:
                v = float(value)
            except (TypeError, ValueError):
                return "—"
            if v >= 1_000_000:
                return f"{v / 1_000_000:.1f}M"
            if v >= 1_000:
                return f"{v / 1_000:.0f}k"
            return f"{int(v):,}"

        # Exec summary headline values
        _niche_revenue = float(getattr(xray_report, "total_revenue", 0) or 0)
        _niche_units = int(getattr(xray_report, "total_units", 0) or 0)
        _current_sessions = (
            int(growth_plan_obj.current_sessions)
            if growth_plan_obj is not None and getattr(growth_plan_obj, "current_sessions", None) is not None
            else 0
        )
        _goal_sessions = (
            int(growth_plan_obj.goal_sessions)
            if growth_plan_obj is not None and getattr(growth_plan_obj, "goal_sessions", None) is not None
            else 0
        )
        _has_growth = growth_plan_obj is not None and _goal_sessions > 0

        _niche_label = niche_keyword or "this category"
        _exec_headline = (
            f"A {_money_short(_niche_revenue)} monthly opportunity"
            + (f" in the &ldquo;{html.escape(_niche_label)}&rdquo; category." if _niche_label else ".")
        )

        _exec_sub_text = (
            dataset.text_fields.get("executive_summary")
            or dataset.text_fields.get("market_summary")
            or ""
        )

        # Exec tiles — last tile becomes "Sessions · 4mo" if growth plan present,
        # otherwise "Avg price" as a soft fallback.
        _avg_price = (
            float(getattr(xray_report, "average_price", 0) or 0)
            if hasattr(xray_report, "average_price")
            else 0
        )
        _exec_tiles_html = (
            f'<div class="exec-tile is-primary">'
            f'<p class="lab">Category revenue</p>'
            f'<p class="val">{_money_short(_niche_revenue)}</p>'
            f'<p class="delta">monthly · top {len(getattr(xray_report, "products", []) or [])} brands</p>'
            f'</div>'
            f'<div class="exec-tile">'
            f'<p class="lab">Units sold</p>'
            f'<p class="val">{_count_short(_niche_units)}</p>'
            f'<p class="delta">monthly · category-wide</p>'
            f'</div>'
        )
        if _has_growth:
            _exec_tiles_html += (
                f'<div class="exec-tile">'
                f'<p class="lab">Sessions today</p>'
                f'<p class="val">{_count_short(_current_sessions)}</p>'
                f'<p class="delta">est. monthly · target</p>'
                f'</div>'
                f'<div class="exec-tile is-primary">'
                f'<p class="lab">Sessions · 4mo</p>'
                f'<p class="val">{_count_short(_goal_sessions)}</p>'
                f'<p class="delta">target · 5-channel ramp</p>'
                f'</div>'
            )
        else:
            _avg_listing_revenue = (
                float(getattr(xray_report, "average_revenue", 0) or 0)
                if hasattr(xray_report, "average_revenue")
                else (_niche_revenue / len(getattr(xray_report, "products", []) or [1]) if _niche_revenue else 0)
            )
            _exec_tiles_html += (
                f'<div class="exec-tile">'
                f'<p class="lab">Avg price</p>'
                f'<p class="val">{_money_short(_avg_price) if _avg_price else "—"}</p>'
                f'<p class="delta">category median</p>'
                f'</div>'
                f'<div class="exec-tile">'
                f'<p class="lab">Avg per listing</p>'
                f'<p class="val">{_money_short(_avg_listing_revenue)}</p>'
                f'<p class="delta">monthly · top 11</p>'
                f'</div>'
            )

        # PR34: exec product card uses the real Amazon product image when
        # available. Falls back to a navy gradient tile + brand-letter
        # initials only when neither a scraped image_url nor a brand
        # placeholder asset is present.
        _target_image_url = str(target.get("image_url", "") or "").strip()
        _target_thumb_label = (target_brand_display[:6].upper() or "TGT") if target_brand_display else "TGT"
        if _target_image_url:
            _target_pic_inner = (
                f'<img src="{html.escape(_target_image_url)}" '
                f'alt="{html.escape(_trim_text(_clean_listing_title(str(target.get("title", "") or "Target product")), 80))}" '
                f'loading="lazy" />'
            )
        else:
            _target_pic_inner = (
                f'<div class="placeholder" data-label="{html.escape(_target_thumb_label)}">'
                f'{html.escape(_target_thumb_label)}</div>'
            )
        _target_meta_bits = []
        if target.get("asin"):
            _target_meta_bits.append(html.escape(str(target.get("asin"))))
        if target.get("rating"):
            _target_meta_bits.append(f"{html.escape(str(target.get('rating')))} ★")
        if target.get("review_count_label"):
            _target_meta_bits.append(html.escape(str(target.get("review_count_label"))))
        elif target.get("reviews"):
            _target_meta_bits.append(html.escape(str(target.get("reviews"))))
        _exec_product_html = (
            f'<div class="exec-product">'
            f'<div class="pic">'
            f'<span class="label-tag">Target</span>'
            f'{_target_pic_inner}'
            f'</div>'
            f'<div>'
            f'<div class="name">{html.escape(_trim_text(_clean_listing_title(str(target.get("title", "") or "the prospect listing")), 60))}</div>'
            f'<div class="meta">{" · ".join(_target_meta_bits) or "&nbsp;"}</div>'
            f'</div>'
            f'</div>'
        )

        # Pills below the exec headline — derived from data with safe fallbacks.
        _pills: list[str] = []
        try:
            top_products = list(getattr(xray_report, "products", []) or [])[:3]
            target_reviews = int(target.get("review_count") or 0)
            top3_avg_reviews = (
                sum(int(getattr(p, "review_count", 0) or 0) for p in top_products) / max(len(top_products), 1)
                if top_products else 0
            )
            if target_reviews and top3_avg_reviews and target_reviews >= top3_avg_reviews * 0.5:
                _pills.append("Reviews within striking distance of the top 3")
        except Exception:
            pass
        if _avg_price > 0:
            _pills.append("Mid-tier price band, no entrenched leader")
        if keyword_report and getattr(keyword_report, "total_search_volume", None):
            _kw_total = int(getattr(keyword_report, "total_search_volume", 0) or 0)
            if _kw_total:
                _pills.append(f"Search demand strong · {_count_short(_kw_total)} monthly searches")
        if _has_growth:
            _phase_count = 4
            _channel_count = len(getattr(growth_plan_obj, "channels", []) or [])
            _pills.append(f"{_channel_count} channels mapped · {_phase_count} phases")
        if not _pills:
            _pills = [
                f"{len(getattr(xray_report, 'products', []) or [])} listings tracked",
                "Mid-tier price band",
                "Page-one competitors mapped",
            ]
        _pills_html = "".join(
            f'<span class="exec-pill"><span class="dot"></span>{html.escape(p)}</span>'
            for p in _pills[:4]
        )

        # Findings strip (3 cards). Take 1 strength + 1 gap and pair with
        # either the growth opportunity or a third gap as fallback.
        def _finding_card(num: str, lab: str, head: str, body: str) -> str:
            return (
                f'<article class="finding">'
                f'<div class="ic">{num}</div>'
                f'<p class="lab">{html.escape(lab)}</p>'
                f'<h3 class="h">{html.escape(head)}</h3>'
                f'<p class="body">{html.escape(body)}</p>'
                f'</article>'
            )

        def _first_text(items: list, default: str) -> tuple[str, str]:
            """Extract (heading, body) from a strength/gap item which may be a
            dict, a dataclass, or a plain string."""
            if not items:
                return default, ""
            first = items[0]
            if isinstance(first, dict):
                return str(first.get("title") or first.get("label") or default), str(first.get("description") or first.get("body") or "")
            if hasattr(first, "title") and hasattr(first, "description"):
                return str(getattr(first, "title", "") or default), str(getattr(first, "description", "") or "")
            text = str(first)
            # Try to split "Title — body" or "Title: body"
            for sep in (" — ", " - ", ": "):
                if sep in text:
                    h, b = text.split(sep, 1)
                    return h.strip(), b.strip()
            return text[:64].strip(), text[64:].strip()

        _strength_h, _strength_b = _first_text(target_strengths, "Strong fundamentals")
        _gap_h, _gap_b = _first_text(target_gaps, "Coverage gaps")
        _findings_html_parts = [
            _finding_card("①", "What's working", _strength_h, _strength_b or "The product fundamentals are in place."),
            _finding_card("②", "What's missing", _gap_h, _gap_b or "There's coverage to add before scaling traffic."),
        ]
        if _has_growth:
            _delta = max(0, _goal_sessions - _current_sessions)
            _multiplier = (_goal_sessions / max(_current_sessions, 1)) if _current_sessions > 0 else 0
            _multi_text = f"{_multiplier:.0f}× sessions in 4 months" if _multiplier >= 2 else f"+{_count_short(_delta)} sessions in 4 months"
            _findings_html_parts.append(
                _finding_card(
                    "③",
                    "The opportunity",
                    _multi_text,
                    f"5-channel ramp closes the gap from {_count_short(_current_sessions)} → {_count_short(_goal_sessions)} monthly sessions, anchored on industry-published timelines.",
                )
            )
        else:
            _h2, _b2 = _first_text(target_gaps[1:] if len(target_gaps) > 1 else [], "Listing optimization is the unlock")
            _findings_html_parts.append(
                _finding_card("③", "The opportunity", _h2, _b2 or "Tighten the PDP and SEO foundations before scaling demand.")
            )
        _findings_html = "".join(_findings_html_parts)

        # Left rail — section list w/ optional dots for growth plan / offers.
        # PR33: optional sections that aren't included render as `.is-disabled`
        # rail items (30% opacity, click is a no-op) per the design handoff,
        # rather than disappearing entirely. Keeps the wayfinding stable so
        # AEs always see the same 9-item rail across decks.
        def _rail_item(
            href: str,
            num: str,
            label: str,
            *,
            optional: bool = False,
            disabled: bool = False,
            summary_class: str = "",
            active: bool = False,
        ) -> str:
            cls = "rail-item"
            if summary_class:
                cls += f" {summary_class}"
            if active:
                cls += " active"
            if disabled:
                cls += " is-disabled"
            href_attr = "#" if disabled else f"#{href}"
            click_attr = ' onclick="event.preventDefault();return false;"' if disabled else ""
            aria_attr = ' aria-disabled="true" tabindex="-1"' if disabled else ""
            return (
                f'<li><a class="{cls}" href="{href_attr}"{click_attr}{aria_attr}>'
                f'<span class="num">{num}</span>{html.escape(label)}'
                + ('<span class="opt-dot" title="optional"></span>' if optional else '')
                + '</a></li>'
            )

        _rail_items_html = (
            _rail_item("summary", "★", "Executive summary", summary_class="is-summary", active=True)
            + _rail_item("sec-01", "01", "Market")
            + _rail_item("sec-02", "02", "Target listing")
            + _rail_item("sec-03", "03", "Competitors")
            + _rail_item("sec-04", "04", "Search behavior")
            + _rail_item("sec-05", "05", "Growth plan", optional=True, disabled=not _has_growth)
            + _rail_item("sec-06", "06", "Conversion & PDP")
            + _rail_item("sec-07", "07", "Service offerings")
            + _rail_item("sec-08", "08", "Proposed offers", optional=True, disabled=not include_recommended_plan)
        )

        def _section_divider(anchor: str, num: str, eyebrow: str, head: str, thesis: str, bullets: list[str]) -> str:
            return (
                f'<div class="section-divider" id="{anchor}">'
                f'<div class="sd-num">{num}</div>'
                f'<div>'
                f'<p class="sd-eye">{html.escape(eyebrow)}</p>'
                f'<h2 class="sd-h">{html.escape(head)}</h2>'
                f'<p class="sd-thesis">{html.escape(thesis)}</p>'
                f'</div>'
                f'<ul class="sd-bullets">'
                + "".join(f"<li>{html.escape(b)}</li>" for b in bullets)
                + '</ul>'
                f'</div>'
            )
        # ---- Section dividers ----
        _div_market = _section_divider(
            "sec-01", "01", "Section · The market",
            f'"{niche_keyword}" — the category at a glance' if niche_keyword else "The category at a glance",
            "A snapshot of category economics: who's selling, what they sell for, and how concentrated the revenue actually is.",
            ["6 category metrics", f"Top {min(len(getattr(xray_report, 'products', []) or []), 11)} brands", "Distribution donuts"],
        )
        _div_target = _section_divider(
            "sec-02", "02", "Section · Target listing",
            f"Where {target_brand_display} stands today",
            "A side-by-side of your listing against the category benchmark, with what's working and what's missing in plain English.",
            ["Comparison panel", "What's working", "What's missing"],
        )
        _div_competitors = _section_divider(
            "sec-03", "03", "Section · Competitive landscape",
            "Who owns the page-one real estate",
            "The top brands competing for the same buyer — by revenue, share, BSR, and review density.",
            ["Top 10 ranked", "Share of category", "Where you fit"],
        )
        _div_search = _section_divider(
            "sec-04", "04", "Section · Search behavior",
            "How buyers find products in this category",
            "Keyword volume, ranking position, and the search-intent terms your title is missing.",
            ["Keyword opportunities", "Title & bullet coverage", "Support-term demand"],
        )
        _div_growth = _section_divider(
            "sec-05", "05", "Section · Growth plan · Optional",
            "Closing the gap — 4 phases, 5 channels",
            "How sessions ramp from today to goal, anchored on industry-published timelines, with each channel's role and cost.",
            ["4-phase ramp", "Funnel by phase", "5 channel cards"],
        ) if _has_growth else ""
        _div_conversion = _section_divider(
            "sec-06", "06", "Section · Conversion & PDP",
            "Where the listing needs to improve",
            "Visual proof of what the benchmark does that you don't, plus prioritized CRO + creative recommendations.",
            ["Visual benchmark", "CRO recs", "Creative recs"],
        )
        _div_offerings = _section_divider(
            "sec-07", "07", "Section · Service offerings",
            "Integrated support model — what we do",
            "Five channels, one contract. Each tab shows the operating commitments we make per channel.",
            ["Amazon", "TikTok · Shopify", "3PL · Shipping OS"],
        )
        _div_offers = _section_divider(
            "sec-08", "08", "Section · Proposed offers · Optional",
            "If you're ready, here's what's next",
            "Two engagement models. The recommended one aligns our incentives with your growth.",
            ["2 offer cards", "Why now", "What happens next"],
        ) if include_recommended_plan else ""

        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  {favicon_link}
  <style>{stylesheet}</style>
</head>
<body>

<div class="app">

  <!-- ============= LEFT RAIL ============= -->
  <aside class="rail" id="rail">
    <div class="rail-brand">
      <div class="rail-logo">a</div>
      <div>
        <div class="rail-brand-name">Anata</div>
        <div class="rail-brand-sub">Strategy deck</div>
      </div>
    </div>

    <div class="rail-eye">Contents</div>
    <ul class="rail-list">{_rail_items_html}</ul>

    <div class="rail-progress" aria-hidden="true"><span id="rail-progress-bar"></span></div>
    <div class="rail-progress-label"><span id="rail-progress-text">Section ★ of {7 + (1 if _has_growth else 0) + (1 if include_recommended_plan else 0)}</span><span id="rail-pct">12%</span></div>

    <div class="rail-foot">
      <a class="rail-util" id="rail-open-story" href="#" onclick="event.preventDefault();window.open(location.pathname.replace(/\\/$/, '') + '/story', '_blank');return false;">Open one-pager <span class="arrow">↗</span></a>
      <a class="rail-util" id="rail-print" href="#" onclick="event.preventDefault();window.print();return false;">Print PDF <span class="arrow">↗</span></a>
      <a class="rail-util primary" href="#sec-08">Get started <span class="arrow">→</span></a>
    </div>
  </aside>

  <!-- ============= CONTENT ============= -->
  <main class="content">

    <!-- ===== EXECUTIVE SUMMARY ===== -->
    <!-- PR34: target product card moved to the header row (top-right)
         next to the headline + sub-copy. Tiles now span the full width
         below so all 4 stats line up uniformly. -->
    <section class="exec" id="summary" data-screen-label="00 Executive summary">
      <div class="exec-header">
        <div class="exec-headline">
          <p class="exec-eyebrow">Strategy summary · {html.escape(target_brand_display)}</p>
          <h1 class="exec-title">{_exec_headline}</h1>
          <p class="exec-sub">{html.escape(_exec_sub_text)}</p>
        </div>
        {_exec_product_html}
      </div>
      <div class="exec-grid">
        <div class="exec-tiles">{_exec_tiles_html}</div>
      </div>
      <div class="exec-pills">{_pills_html}</div>
      <div class="exec-cta-row">
        <span class="next">Start here</span>
        <span class="target">Walk through the category →</span>
        <span class="spacer"></span>
        <a class="scroll-link" href="#sec-01">Section 01 · Market</a>
        {('<a class="scroll-link" href="#sec-05">Section 05 · Growth plan</a>' if _has_growth else "")}
        {('<a class="scroll-link" href="#sec-08">Section 08 · Offers</a>' if include_recommended_plan else "")}
      </div>
    </section>

    <div class="findings">{_findings_html}</div>

    <!-- ===== 01 MARKET ===== -->
    {_div_market}
    <section class="slide" data-screen-label="01 Market summary">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Market summary</p>
          <h2 class="slide-title">Summary of "{html.escape(niche_keyword)}"</h2>
        </div>
        <p class="caption">{html.escape(dataset.text_fields.get("market_summary") or "")}</p>
      </header>

      <div class="metric-grid cols-6" style="margin-bottom:22px">{market_summary_html}</div>

      <div class="two-col">
        <div class="takeaway">
          <p class="lab">What this means</p>
          <p class="h">Where the category sits today.</p>
          <p>{html.escape(dataset.text_fields.get("advertising_summary") or dataset.text_fields.get("market_summary") or "Pulled from Helium 10 Xray over the visible market set.")}</p>
        </div>
        <div>
          <div class="card-h">
            <h3>Competitor revenue breakdown</h3>
            <div class="seg niche-toggle" role="tablist" aria-label="Competitor breakdown view">
              <button type="button" class="niche-toggle-btn active" data-niche-view="asin" aria-pressed="true">By ASIN</button>
              <button type="button" class="niche-toggle-btn" data-niche-view="brand" aria-pressed="false">By Brand</button>
            </div>
          </div>
          <table class="tbl niche-table" data-niche-view-target>
            <thead>
              <tr>
                <th data-asin-only>Product</th>
                <th data-brand-only hidden>Brand</th>
                <th class="num-col">Price</th>
                <th class="num-col">Revenue</th>
                <th class="num-col">Share</th>
              </tr>
            </thead>
            <tbody data-view="asin">{niche_table_rows}</tbody>
            <tbody data-view="brand" hidden>{niche_table_brand_rows}</tbody>
          </table>
        </div>
      </div>

      <div class="three-col" style="margin-top:22px">
        {country_donut}
        {size_donut}
        {fulfillment_donut}
      </div>
    </section>

    <!-- ===== 02 TARGET LISTING ===== -->
    {_div_target}
    <section class="slide" data-screen-label="02 Target listing">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">{'Market entry benchmark' if launch_mode else 'Target listing opportunities'}</p>
          <h2 class="slide-title">{'Launch benchmark opportunities' if launch_mode else 'Your listing vs. the benchmark'}</h2>
        </div>
        <p class="caption">{html.escape(dataset.text_fields.get("hero_product_snapshot") or "")}</p>
      </header>

      <div class="target-panel target-panel-full">{comparison_table_html}</div>

      <div class="two-col even" style="margin-top:24px">
        <div>
          <h3 style="font-size:14px;font-weight:700;margin:0 0 10px;color:#4d7a5d;letter-spacing:-0.01em">{'What the product already signals well' if launch_mode else "What's working"}</h3>
          <ul class="cklist good">{target_strength_html}</ul>
        </div>
        <div>
          <h3 style="font-size:14px;font-weight:700;margin:0 0 10px;color:#a55c5c;letter-spacing:-0.01em">{'What needs to be built before launch' if launch_mode else "What's missing"}</h3>
          <ul class="cklist bad">{target_gap_html}</ul>
        </div>
      </div>
    </section>

    <!-- ===== 03 COMPETITORS ===== -->
    {_div_competitors}
    <section class="slide" data-screen-label="03 Competitors">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Competitor landscape</p>
          <h2 class="slide-title">Who owns the page-one real estate</h2>
        </div>
        <p class="caption">Top brands by 30-day revenue. The brands above you compete on review momentum and creative depth — not price.</p>
      </header>
      <div class="table-wrap">{competitor_landscape_table}</div>
    </section>

    <!-- ===== 04 SEARCH BEHAVIOR ===== -->
    {_div_search}
    <section class="slide" data-screen-label="04 Search behavior">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Keyword and customer search objective</p>
          <h2 class="slide-title">How buyers find products like yours</h2>
        </div>
        <p class="caption">{html.escape(dataset.text_fields.get("seo_summary") or "")}</p>
      </header>

      <div class="metric-grid cols-4" style="margin-bottom:22px">{keyword_summary_html}</div>

      <div class="search-grid">
        <div class="card">
          <div class="card-h">
            <h3>Top keyword opportunities</h3>
            <span class="meta">{html.escape(keyword_table_caption)}</span>
          </div>
          <div class="table-wrap"><table class="tbl">{keyword_table_html}</table></div>
        </div>
        <div style="display:flex;flex-direction:column;gap:14px">
          <div class="card">{search_title_html}</div>
          <div class="card">{search_copy_html}</div>
          {keyword_bubble_html if keyword_bubble_html else ""}
        </div>
      </div>

      <div class="takeaway" style="margin-top:20px">
        <p class="lab">What this means</p>
        <p class="h">Most of the gap is coverage, not search volume.</p>
        <p>{html.escape((seo_recommendations[0] if seo_recommendations else "Title rewrites and bullet additions on missing high-intent terms unlock the unranked search volume your listing isn't currently positioned for.")[:280] if isinstance(seo_recommendations[0] if seo_recommendations else "", str) else "Title rewrites and bullet additions on missing high-intent terms unlock the unranked search volume your listing isn't currently positioned for.")}</p>
      </div>

      {keyword_rank_summary_html if keyword_rank_summary_html else ""}
    </section>

    <!-- ===== 05 GROWTH PLAN (optional) ===== -->
    {_div_growth}
    {growth_plan_html}

    <!-- ===== 06 CONVERSION ===== -->
    {_div_conversion}
    <section class="slide slide-conversion" data-screen-label="06 Conversion and PDP">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Conversion &amp; PDP</p>
          <h2 class="slide-title">Where the listing needs to improve</h2>
        </div>
        <p class="caption">{html.escape(dataset.text_fields.get("cro_summary") or "")}</p>
      </header>

      <div class="two-col">
        <div class="takeaway">
          <p class="lab">What this means</p>
          <p class="h">PDP mechanics, not the product, are the unlock.</p>
          <p>{html.escape((dataset.text_fields.get("expected_impact_summary") or "Closing CRO + creative gaps lifts conversion without changing product, price, or supply chain. The benchmark sets the ceiling — copy what's working.")[:300])}</p>
        </div>
        <div>
          {render_visual_proof_panel(
              target=target,
              best_seller=best_seller,
              cro_recommendations=cro_recommendations,
              creative_recommendations=creative_recommendations,
              missing_image_asset=no_product_image,
          )}
        </div>
      </div>

      <div class="rec-grid" style="margin-top:24px">
        <div class="rec-card">
          <h3>CRO recommendations</h3>
          <ul>{''.join(_render_recommendation_item(item) for item in cro_recommendations)}</ul>
        </div>
        <div class="rec-card">
          <h3>Creative recommendations</h3>
          <ul>{''.join(_render_recommendation_item(item) for item in creative_recommendations)}</ul>
        </div>
      </div>

      {f'<div class="gallery">{gallery_html}</div>' if gallery_html else ''}
      {resource_embed_html}
    </section>

    <!-- ===== 07 SERVICE OFFERINGS ===== -->
    {_div_offerings}
    {offering_html}

    <!-- ===== 08 PROPOSED OFFERS (optional) ===== -->
    {_div_offers}
    {recommended_plan_html}

  </main>
</div>

  <script>
    // ---- Rail active-section + progress on scroll ----
    (function() {{
      const railItems = document.querySelectorAll('.rail-item');
      const progressBar = document.getElementById('rail-progress-bar');
      const progressText = document.getElementById('rail-progress-text');
      const progressPct = document.getElementById('rail-pct');

      // Build sections from rail items so we don't drift if growth/offers are absent.
      const sections = Array.from(railItems).map(it => {{
        const a = it.querySelector('a') || it;
        const href = a.getAttribute('href') || '';
        return {{
          id: href.replace('#', ''),
          name: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
        }};
      }});

      function setActive(idx) {{
        railItems.forEach((it, i) => {{
          const a = it.querySelector('a') || it;
          a.classList.toggle('active', i === idx);
        }});
        const pct = Math.round(((idx + 1) / sections.length) * 100);
        if (progressBar) progressBar.style.width = pct + '%';
        if (progressText && sections[idx]) progressText.textContent = sections[idx].name;
        if (progressPct) progressPct.textContent = pct + '%';
      }}

      const observer = new IntersectionObserver((entries) => {{
        let bestIdx = -1, bestY = Infinity;
        entries.forEach(e => {{
          if (e.isIntersecting) {{
            const idx = sections.findIndex(s => s.id === e.target.id);
            const y = e.boundingClientRect.top;
            if (idx >= 0 && y >= -200 && y < bestY) {{ bestY = y; bestIdx = idx; }}
          }}
        }});
        if (bestIdx >= 0) setActive(bestIdx);
      }}, {{ rootMargin: '-10% 0px -60% 0px', threshold: [0, 0.25, 0.5, 1] }});

      sections.forEach(s => {{
        const el = document.getElementById(s.id);
        if (el) observer.observe(el);
      }});

      // Smooth scroll on rail clicks
      document.querySelectorAll('a[href^="#"]').forEach(a => {{
        a.addEventListener('click', (e) => {{
          const id = a.getAttribute('href').slice(1);
          const t = document.getElementById(id);
          if (t) {{ e.preventDefault(); t.scrollIntoView({{ behavior: 'smooth', block: 'start' }}); }}
        }});
      }});
    }})();

    // ---- ASIN/Brand toggle on the niche table ----
    document.querySelectorAll('.niche-toggle').forEach((toggleRoot) => {{
      const slide = toggleRoot.closest('.slide');
      toggleRoot.querySelectorAll('button').forEach((button) => {{
        button.addEventListener('click', () => {{
          const view = button.dataset.nicheView;
          toggleRoot.querySelectorAll('button').forEach((node) => {{
            const isActive = node === button;
            node.classList.toggle('active', isActive);
            node.setAttribute('aria-pressed', isActive ? 'true' : 'false');
          }});
          slide?.querySelectorAll('tbody[data-view]').forEach((tbody) => {{
            tbody.hidden = tbody.dataset.view !== view;
          }});
          slide?.querySelectorAll('[data-asin-only]').forEach((node) => {{
            node.hidden = view !== 'asin';
          }});
          slide?.querySelectorAll('[data-brand-only]').forEach((node) => {{
            node.hidden = view !== 'brand';
          }});
        }});
      }});
    }});

    // ---- Service offerings tabs (PR33: matches deck.css `.off-tabs`/`.off-pane`) ----
    document.querySelectorAll('#off-tabs').forEach((tabsRoot) => {{
      tabsRoot.querySelectorAll('button').forEach((button) => {{
        button.addEventListener('click', () => {{
          const target = button.dataset.off;
          const section = tabsRoot.closest('.slide');
          tabsRoot.querySelectorAll('button').forEach((node) => node.classList.toggle('active', node === button));
          section?.querySelectorAll('.off-pane').forEach((pane) => {{
            pane.hidden = pane.dataset.pane !== target;
          }});
        }});
      }});
    }});

    // ---- Embedded resource (Canva / case study) tabs ----
    document.querySelectorAll('.embedded-tabs').forEach((tabsRoot) => {{
      tabsRoot.querySelectorAll('.embedded-tab').forEach((button) => {{
        button.addEventListener('click', () => {{
          const target = button.dataset.tab;
          const section = tabsRoot.closest('.embedded-resource-section');
          section?.querySelectorAll('.embedded-tab').forEach((node) => node.classList.toggle('is-active', node === button));
          section?.querySelectorAll('.embedded-panel').forEach((panel) => {{
            const isActive = panel.dataset.panel === target;
            panel.classList.toggle('is-active', isActive);
            panel.hidden = !isActive;
          }});
        }});
      }});
    }});

    // ---- Growth plan funnel tabs (PR34: matches .funnel-tabs / .funnel-tab) ----
    document.querySelectorAll('.growth-funnel-tabbed').forEach((funnelRoot) => {{
      funnelRoot.querySelectorAll('.funnel-tab').forEach((button) => {{
        button.addEventListener('click', () => {{
          const phase = button.dataset.phase;
          funnelRoot.querySelectorAll('.funnel-tab').forEach((node) => {{
            const isActive = node === button;
            node.classList.toggle('active', isActive);
            node.setAttribute('aria-pressed', isActive ? 'true' : 'false');
          }});
          funnelRoot.querySelectorAll('.growth-funnel-panel').forEach((panel) => {{
            panel.hidden = panel.dataset.phase !== phase;
          }});
        }});
      }});
    }});
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
