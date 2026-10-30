"""Signal, offer, and target campaigns, and the weekly score of each one.

``outbound_campaigns`` is the source ``push_pending`` reads. The hardcoded
``RECIPE_CAMPAIGN_MAP`` is only the fallback when this table is empty or
cannot be read. Scoring writes proposals. It does not activate a campaign,
change a cap, or create anything in Instantly. David approves those.

StoreLeads filters stored here are limited to parameters confirmed on
https://storeleads.app/api (List Domains) on 2026-10-30. A key that is not
in ``CONFIRMED_FILTERS`` is dropped before a request is built. Relative
windows live under ``_relative_days`` and are turned into JSON timestamps
at query time. Keys that start with ``_`` are never sent.

Confirmed parameters:

- ``f:an``, ``f:an:op`` — app installed (``platform.token``)
- ``f:app_installed_at``, ``f:app_installed_at:min``, ``f:app_installed_at:max``
- ``f:scs``, ``f:scs:op`` — sales channel; ``Amazon`` is a documented value
  and ``op=not`` excludes it
- ``f:cratmin``, ``f:cratmax`` — store created on or after / on or before
- ``f:plan``, ``f:plan:op`` — ``Shopify Plus`` is a documented example
- ``f:last_plan_change_at:min``, ``f:last_plan_change_at:max``
- ``f:last_platform``, ``f:last_platform:op`` — values include
  ``woocommerce``, ``magento``, and ``bigcommerce``; multiple values need
  ``op=or``
- ``f:last_platform_change_at:min``, ``f:last_platform_change_at:max``
- ``f:ermin``, ``f:ermax`` — estimated monthly sales, USD cents
- ``f:empcmin``, ``f:empcmax`` — employee count
- ``f:cat``, ``f:cat:op`` — category
- ``f:cc``, ``f:cc:op`` — country code
- ``f:it``, ``f:it:op`` — contact type, including ``email``
- ``f:ds``, ``f:ds:op`` — ``Active``, ``Inactive``, ``Password Protected``
- ``f:lids``, ``f:lids:op`` — list id
- ``f:p``, ``f:p:op`` — platform, including ``shopify``

``name`` is ``signal|offer|target``. The five seeded rows are the live
signal campaigns. ``replatformed`` is an alias of ``replatform``, not a
sixth campaign.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

TABLE = "outbound_campaigns"
SCORES = "outbound_campaign_scores"

STATUSES = ("active", "throttled", "cut", "proposed")
ROUTABLE_STATUSES = frozenset({"active", "throttled"})

# List Domains parameters confirmed at https://storeleads.app/api.
CONFIRMED_FILTERS = frozenset({
    "f:an", "f:an:op",
    "f:app_installed_at", "f:app_installed_at:min", "f:app_installed_at:max",
    "f:scs", "f:scs:op",
    "f:cratmin", "f:cratmax",
    "f:plan", "f:plan:op",
    "f:last_plan_change_at:min", "f:last_plan_change_at:max",
    "f:last_platform", "f:last_platform:op",
    "f:last_platform_change_at:min", "f:last_platform_change_at:max",
    "f:ermin", "f:ermax",
    "f:empcmin", "f:empcmax",
    "f:cat", "f:cat:op",
    "f:cc", "f:cc:op",
    "f:it", "f:it:op",
    "f:ds", "f:ds:op",
    "f:lids", "f:lids:op",
    "f:p", "f:p:op",
})

# $10,000 and $20,000 a month, in the cents f:ermin / f:ermax document.
MONTHLY_10K_CENTS = 1_000_000
MONTHLY_20K_CENTS = 2_000_000

_BASE_FILTERS: dict[str, Any] = {
    "f:p": "shopify",
    "f:ds": "Active",
    "f:cc": "US",
    "f:it": "email",
    "f:ermin": MONTHLY_10K_CENTS,
    "f:ermax": MONTHLY_20K_CENTS,
}

# campaign_id, name, recipe_key, aliases, filters, daily_cap, template_ref.
# Aliases is a comma-separated list. replatformed shares the replatform row.
SEED_ROWS: tuple[dict[str, Any], ...] = (
    {
        "campaign_id": "a333e00e-bc3b-4abf-bcbf-4e0eb126090c",
        "name": "amz_on|standard|us_amazon_10_20k",
        "recipe_key": "amz_on",
        "recipe_aliases": "",
        "storeleads_filters": {**_BASE_FILTERS, "f:scs": "Amazon"},
        "daily_cap": 40,
        "template_ref": "amz_on",
    },
    {
        "campaign_id": "45a521a0-68e2-439f-9622-1672f9ede214",
        "name": "amz_off|standard|us_not_amazon_10_20k",
        "recipe_key": "amz_off",
        "recipe_aliases": "",
        "storeleads_filters": {**_BASE_FILTERS, "f:scs": "Amazon", "f:scs:op": "not"},
        "daily_cap": 40,
        "template_ref": "amz_off",
    },
    {
        "campaign_id": "1937c21a-89c4-458f-a949-306f90ae6258",
        "name": "plan_upgrade|standard|us_plus_90d",
        "recipe_key": "plan_upgrade",
        "recipe_aliases": "",
        "storeleads_filters": {
            **_BASE_FILTERS,
            "f:plan": "Shopify Plus",
            "_relative_days": {"f:last_plan_change_at:min": 90},
        },
        "daily_cap": 30,
        "template_ref": "plan_upgrade",
    },
    {
        "campaign_id": "d12b5512-d34c-4331-841f-8e59263f5b5e",
        "name": "replatform|standard|us_woo_magento_bc_120d",
        "recipe_key": "replatform",
        "recipe_aliases": "replatformed",
        "storeleads_filters": {
            **_BASE_FILTERS,
            "f:last_platform": "woocommerce,magento,bigcommerce",
            "f:last_platform:op": "or",
            "_relative_days": {"f:last_platform_change_at:min": 120},
        },
        "daily_cap": 25,
        "template_ref": "replatformed",
    },
    {
        "campaign_id": "8ebf1923-7ba4-4a63-96a8-adb18a58ddf6",
        "name": "abby_reengage|standard|prior_thread",
        "recipe_key": "abby_reengage",
        "recipe_aliases": "",
        "storeleads_filters": None,
        "daily_cap": 0,
        "template_ref": "abby_reengage",
    },
)

MIN_CONTACTED_TO_JUDGE = 150
_PROPOSAL_NAMESPACE = uuid.UUID("6f0c1b2a-9a41-4c3e-8d7e-0a941c0a9410")


@dataclass(frozen=True)
class CampaignRouting:
    """How ``push_pending`` should resolve a recipe.

    ``available`` is false when the table is missing, empty, or unreadable.
    The caller then uses ``RECIPE_CAMPAIGN_MAP``. ``blocked`` recipes exist
    on the table as ``cut`` or ``proposed`` and must not fall through to the
    default campaign.
    """

    available: bool
    by_recipe: dict[str, str] = field(default_factory=dict)
    blocked: frozenset[str] = field(default_factory=frozenset)


def _is_postgres(engine: Any) -> bool:
    return str(getattr(getattr(engine, "dialect", None), "name", "") or "") == "postgresql"


def _skip_ddl(engine: Any) -> bool:
    """Vercel's runtime role cannot create tables. The migration does that."""

    return bool(os.getenv("VERCEL")) and _is_postgres(engine)


def _create_campaigns_sql(postgres: bool) -> str:
    filters = "JSONB" if postgres else "TEXT"
    stamp = "TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP" if postgres else "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    return f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            campaign_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            recipe_key TEXT NOT NULL,
            recipe_aliases TEXT NOT NULL DEFAULT '',
            storeleads_filters {filters},
            status TEXT NOT NULL CHECK (status IN ('active', 'throttled', 'cut', 'proposed')),
            daily_cap INTEGER NOT NULL DEFAULT 0,
            template_ref TEXT NOT NULL DEFAULT '',
            created_at {stamp}
        )
    """


def _create_scores_sql(postgres: bool) -> str:
    identity = "BIGSERIAL PRIMARY KEY" if postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"
    stamp = "TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP" if postgres else "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    return f"""
        CREATE TABLE IF NOT EXISTS {SCORES} (
            id {identity},
            campaign_id TEXT NOT NULL,
            week_start TEXT NOT NULL,
            contacted INTEGER NOT NULL DEFAULT 0,
            positive_per_100 REAL,
            meetings_per_100 REAL,
            bounce_pct REAL,
            opt_out_pct REAL,
            judged INTEGER NOT NULL DEFAULT 0,
            action TEXT NOT NULL DEFAULT '',
            detail TEXT NOT NULL DEFAULT '',
            created_at {stamp},
            UNIQUE (campaign_id, week_start)
        )
    """


def ensure_tables(engine: Any) -> None:
    """Create both tables when this process is allowed to, then seed.

    Existing rows are left alone. A second boot does not overwrite a cap
    or a status David has changed.
    """

    if engine is None or _skip_ddl(engine):
        return
    from sales_support_agent.services.outbound_ddl import create_table_if_absent

    postgres = _is_postgres(engine)
    create_table_if_absent(engine, _create_campaigns_sql(postgres), TABLE)
    create_table_if_absent(engine, _create_scores_sql(postgres), SCORES)
    seed_campaigns(engine)


def seed_campaigns(engine: Any) -> int:
    """Insert the five signal campaigns. Conflicts do nothing."""

    if engine is None or _skip_ddl(engine):
        return 0
    from sales_support_agent.services.outbound_ddl import relation_exists

    if not relation_exists(engine, TABLE):
        return 0
    postgres = _is_postgres(engine)
    filters_expr = "CAST(:storeleads_filters AS JSONB)" if postgres else ":storeleads_filters"
    sql = text(
        f"""
        INSERT INTO {TABLE} (
            campaign_id, name, recipe_key, recipe_aliases, storeleads_filters,
            status, daily_cap, template_ref
        ) VALUES (
            :campaign_id, :name, :recipe_key, :recipe_aliases, {filters_expr},
            'active', :daily_cap, :template_ref
        )
        ON CONFLICT (campaign_id) DO NOTHING
        """
    )
    rows = []
    for item in SEED_ROWS:
        filters = item["storeleads_filters"]
        rows.append({
            "campaign_id": item["campaign_id"],
            "name": item["name"],
            "recipe_key": item["recipe_key"],
            "recipe_aliases": item["recipe_aliases"],
            "storeleads_filters": None if filters is None else json.dumps(filters, sort_keys=True),
            "daily_cap": int(item["daily_cap"]),
            "template_ref": item["template_ref"],
        })
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def parse_filters(value: Any) -> Optional[dict[str, Any]]:
    """Return a filter object, or None when the column is empty."""

    if value is None:
        return None
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            loaded = json.loads(raw)
        except ValueError:
            logger.warning("[outbound-campaigns] storeleads_filters is not JSON")
            return None
        return dict(loaded) if isinstance(loaded, dict) else None
    return None


def _iso(moment: datetime) -> str:
    """JSON timestamp StoreLeads documents for date filters."""

    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def resolve_filters(value: Any, now: Optional[datetime] = None) -> dict[str, Any]:
    """Filters safe to send: confirmed keys only, relative days resolved.

    ``_relative_days`` maps a confirmed parameter to a whole number of days
    back from ``now``. Anything else under a ``_`` key is dropped.
    """

    raw = parse_filters(value) or {}
    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    relative = raw.get("_relative_days") or {}
    if not isinstance(relative, dict):
        relative = {}
    resolved: dict[str, Any] = {}
    for key, item in raw.items():
        if str(key).startswith("_"):
            continue
        if key in CONFIRMED_FILTERS and item is not None:
            resolved[key] = item
    for key, days in relative.items():
        if key not in CONFIRMED_FILTERS:
            continue
        try:
            count = int(days)
        except (TypeError, ValueError):
            continue
        resolved[key] = _iso(moment - timedelta(days=count))
    return resolved


def _aliases(value: Any) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def load_routing(engine: Any) -> CampaignRouting:
    """Read the campaign table. Empty or unreadable means use the code map."""

    if engine is None:
        return CampaignRouting(False)
    try:
        from sales_support_agent.services.outbound_ddl import relation_exists

        if not relation_exists(engine, TABLE):
            return CampaignRouting(False)
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT campaign_id, recipe_key, recipe_aliases, status FROM {TABLE}"
            )).fetchall()
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-campaigns] could not read %s", TABLE)
        return CampaignRouting(False)
    if not rows:
        return CampaignRouting(False)
    by_recipe: dict[str, str] = {}
    blocked: set[str] = set()
    for campaign_id, recipe_key, aliases, status in rows:
        names = [str(recipe_key or "").strip(), *_aliases(aliases)]
        names = [name for name in names if name]
        state = str(status or "").strip()
        if state in ROUTABLE_STATUSES:
            for name in names:
                by_recipe.setdefault(name, str(campaign_id))
        else:
            blocked.update(names)
    blocked.difference_update(by_recipe)
    return CampaignRouting(True, by_recipe, frozenset(blocked))


def list_campaigns(engine: Any) -> list[dict[str, Any]]:
    """Every campaign row. Empty when the table cannot be read."""

    if engine is None:
        return []
    try:
        from sales_support_agent.services.outbound_ddl import relation_exists

        if not relation_exists(engine, TABLE):
            return []
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"""
                SELECT campaign_id, name, recipe_key, recipe_aliases, storeleads_filters,
                       status, daily_cap, template_ref
                FROM {TABLE}
                ORDER BY created_at, recipe_key
                """
            )).fetchall()
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-campaigns] could not list campaigns")
        return []
    out = []
    for row in rows:
        out.append({
            "campaign_id": row[0],
            "name": row[1],
            "recipe_key": row[2],
            "recipe_aliases": row[3] or "",
            "storeleads_filters": parse_filters(row[4]),
            "status": row[5],
            "daily_cap": int(row[6] or 0),
            "template_ref": row[7] or "",
        })
    return out


def judge_metrics(
    contacted: int,
    positive_per_100: Optional[float],
    meetings_per_100: Optional[float],
    bounce_pct: Optional[float],
    opt_out_pct: Optional[float],
) -> tuple[bool, str]:
    """Return ``(judged, action)``.

    Fewer than 150 contacted, or a missing rate, is ``insufficient``. A
    judged action is a proposal. Nothing here changes a campaign row.
    """

    rates = (positive_per_100, meetings_per_100, bounce_pct, opt_out_pct)
    if contacted < MIN_CONTACTED_TO_JUDGE or any(rate is None for rate in rates):
        return False, "insufficient"
    assert positive_per_100 is not None and meetings_per_100 is not None
    assert bounce_pct is not None and opt_out_pct is not None
    if opt_out_pct >= 2 or bounce_pct >= 8 or positive_per_100 < 0.5:
        return True, "cut"
    if opt_out_pct >= 1 or bounce_pct >= 4 or positive_per_100 < 1.5:
        return True, "throttle"
    if (
        positive_per_100 >= 3
        and meetings_per_100 >= 0.4
        and bounce_pct < 3
        and opt_out_pct < 0.5
    ):
        return True, "boost"
    return True, "hold"


def proposed_campaign_id(week_start: str, recipe_key: str, trait: str) -> str:
    """Stable id for a proposal. This is not an Instantly campaign id."""

    return str(uuid.uuid5(_PROPOSAL_NAMESPACE, f"{week_start}|{recipe_key}|{trait}"))


def variant_proposal(
    campaign: dict[str, Any],
    action: str,
    week_start: str,
) -> Optional[dict[str, Any]]:
    """One trait combination to propose, or None.

    Boost shifts the monthly sales band up. Cut tightens it. Both keep the
    parent's other confirmed filters. The row is always ``proposed``.
    """

    if action not in {"boost", "cut"}:
        return None
    filters = parse_filters(campaign.get("storeleads_filters"))
    if not filters or "f:ermin" not in filters or "f:ermax" not in filters:
        return None
    signal, offer, _target = _split_name(str(campaign.get("name") or ""))
    if action == "boost":
        trait = "rev_20_40k"
        target = "us_20_40k"
        ermin, ermax = 2_000_000, 4_000_000
    else:
        trait = "rev_10_15k"
        target = "us_10_15k"
        ermin, ermax = MONTHLY_10K_CENTS, 1_500_000
    recipe_key = f"{campaign.get('recipe_key') or 'signal'}__{trait}"
    proposed = dict(filters)
    proposed["f:ermin"] = ermin
    proposed["f:ermax"] = ermax
    return {
        "campaign_id": proposed_campaign_id(week_start, recipe_key, trait),
        "name": f"{signal}|{offer}|{target}",
        "recipe_key": recipe_key,
        "recipe_aliases": "",
        "storeleads_filters": proposed,
        "status": "proposed",
        "daily_cap": 0,
        "template_ref": str(campaign.get("template_ref") or ""),
    }


def _split_name(name: str) -> tuple[str, str, str]:
    parts = [part.strip() for part in name.split("|")]
    while len(parts) < 3:
        parts.append("standard" if len(parts) == 1 else "unspecified")
    return parts[0] or "signal", parts[1] or "standard", parts[2] or "unspecified"


def insert_proposed(engine: Any, proposal: dict[str, Any]) -> bool:
    """Insert one ``proposed`` row. Existing rows are not updated."""

    if str(proposal.get("status") or "") != "proposed":
        raise ValueError("scoring may only insert status=proposed")
    if engine is None:
        return False
    postgres = _is_postgres(engine)
    filters_expr = "CAST(:storeleads_filters AS JSONB)" if postgres else ":storeleads_filters"
    sql = text(
        f"""
        INSERT INTO {TABLE} (
            campaign_id, name, recipe_key, recipe_aliases, storeleads_filters,
            status, daily_cap, template_ref
        ) VALUES (
            :campaign_id, :name, :recipe_key, :recipe_aliases, {filters_expr},
            'proposed', 0, :template_ref
        )
        ON CONFLICT (campaign_id) DO NOTHING
        """
    )
    payload = {
        "campaign_id": proposal["campaign_id"],
        "name": proposal["name"],
        "recipe_key": proposal["recipe_key"],
        "recipe_aliases": proposal.get("recipe_aliases") or "",
        "storeleads_filters": json.dumps(proposal.get("storeleads_filters") or {}, sort_keys=True),
        "template_ref": proposal.get("template_ref") or "",
    }
    with engine.begin() as conn:
        result = conn.execute(sql, payload)
    return bool(getattr(result, "rowcount", 0))
