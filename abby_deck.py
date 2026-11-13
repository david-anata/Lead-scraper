"""Build the Amazon strategy deck Abby sends after a yes.

This calls the same ``DeckGenerationService`` the marketing site uses for a
prospect strategy deck. It does not send email and it does not log the send.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


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
                        settings: Any = None, session_factory: Any = None) -> str:
    """Return the public deck URL, or '' when the plan cannot be built.

    ``lead`` and ``first_name`` are accepted so the runner can pass the same
    keyword arguments it uses for a test double. They are not sent anywhere.
    """

    del lead, first_name
    target = _target(domain, product, asin)
    if not target:
        return ""
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
        return str(getattr(result, "view_url", "") or "").strip()
    except Exception:  # noqa: BLE001 — the reply path records the miss and retries
        logger.exception("[abby] prospect deck was not built")
        return ""
