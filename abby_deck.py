"""Build the Amazon strategy deck Abby sends after a yes.

This calls the same ``DeckGenerationService`` the marketing site uses for a
prospect strategy deck. It does not send email and it does not log the send.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProspectDeck:
    """A built deck and whether its content is fit to send."""

    view_url: str
    content_gate_passed: bool = True
    content_gate_findings: tuple[str, ...] = ()
    market_mode: str = ""
    evidence_status: str = "ready"
    market_evidence_sufficient: bool = True
    market_evidence_reason: str = ""


def _ship_gate(domain: str, company: str) -> str:
    """``digital`` or ``no_shippable_goods`` before a deck is built.

    An unreadable catalog does not hold the deck by itself. The StoreLeads
    fallback still applies when the company record carries it.
    """

    host = (domain or "").strip()
    if not host or " " in host or "." not in host.split("://", 1)[-1]:
        return ""
    try:
        from outbound_pipeline import digital_product_reason, fetch_public_catalog

        catalog = fetch_public_catalog(host)
        return digital_product_reason(
            {"domain": host, "merchant_name": company, "name": company},
            catalog,
        )
    except Exception:  # noqa: BLE001 — a catalog miss must not look like a built deck
        logger.info("[abby] catalog unreadable for %s", host)
        return ""


def _target(domain: str, product: str, asin: str) -> str:
    token = (asin or "").strip()
    if token:
        return token
    host = (domain or "").strip()
    if host:
        if host.startswith("http://") or host.startswith("https://"):
            return host
        if " " not in host and "." in host:
            return f"https://{host}"
    return (product or "").strip()


def build_prospect_deck(*, lead: str = "", company: str = "", domain: str = "",
                        first_name: str = "", product: str = "", asin: str = "",
                        settings: Any = None, session_factory: Any = None) -> ProspectDeck | str:
    """Return the deck and its content-gate result, or '' when it cannot be built.

    ``lead`` and ``first_name`` are accepted so the runner can pass the same
    keyword arguments it uses for a test double. They are not sent anywhere.
    """

    del lead, first_name
    target = _target(domain, product, asin)
    if not target:
        return ""
    host = (domain or "").strip()
    if not host and target.startswith("http"):
        host = target
    ship = _ship_gate(host, company)
    if ship:
        logger.info("[abby] holding deck for %s: %s", host or company, ship)
        return ProspectDeck(
            view_url="",
            content_gate_passed=False,
            content_gate_findings=(ship,),
            evidence_status="held",
            market_evidence_sufficient=False,
            market_evidence_reason=ship,
        )
    try:
        if settings is None:
            from sales_support_agent.config import load_settings

            settings = load_settings()
        if session_factory is None:
            from sqlalchemy.orm import sessionmaker

            from sales_support_agent.models.database import get_engine

            session_factory = sessionmaker(
                bind=get_engine(), autoflush=False, autocommit=False,
                future=True, expire_on_commit=False,
            )
        from sales_support_agent.models.database import session_scope
        from sales_support_agent.services.deck.formatting import (
            DEFAULT_SERVICE_TABS,
            _normalize_offers,
        )
        from sales_support_agent.services.deck_generator import DeckGenerationService

        with session_scope(session_factory) as session:
            result = DeckGenerationService(settings, session).generate_deck(
                target_product_input=target,
                competitor_xray_csv_payloads=[],
                keyword_xray_csv_payloads=[],
                channels=list(DEFAULT_SERVICE_TABS),
                offers=_normalize_offers([]),
                include_recommended_plan=True,
                growth_plan_inputs={},
                trigger="abby_yes",
                brand_name=(company or "").strip(),
                brand_website=target if target.startswith("http") else "",
            )
        findings = tuple(getattr(result, "content_gate_findings", ()) or ())
        return ProspectDeck(
            view_url=str(getattr(result, "view_url", "") or "").strip(),
            content_gate_passed=bool(getattr(result, "content_gate_passed", True)),
            content_gate_findings=tuple(str(item) for item in findings),
            market_mode=str(getattr(result, "market_mode", "") or ""),
            evidence_status=str(getattr(result, "evidence_status", "ready") or "ready"),
            market_evidence_sufficient=bool(getattr(result, "market_evidence_sufficient", True)),
            market_evidence_reason=str(getattr(result, "market_evidence_reason", "") or ""),
        )
    except Exception:  # noqa: BLE001 — the reply path records the miss and retries
        logger.exception("[abby] prospect deck was not built")
        return ""
