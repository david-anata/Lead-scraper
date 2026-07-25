"""Named StoreLeads pull recipes: what we pull, and when.

Each recipe is a saved StoreLeads query plus the timing that makes it a genuine
"why now" trigger rather than a static list. StoreLeads refreshes its data weekly
(normally Monday), so trigger recipes are pulled early in the week when the
newly-changed rows appear; the baseline recipe runs daily to keep volume steady.

Every recipe carries the reason it fired. That reason is what makes the opening
line defensible and it is also what we measure: the scoreboard reports booked
calls by recipe, so recipes that do not earn their place get retired.

Pure functions of a clock, so the exact query for any date is unit-testable
without a live key.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

# ---- ICP floor, applied to EVERY recipe --------------------------------------
# StoreLeads' revenue filters are MONTHLY sales in cents; our band is yearly.
_YEARLY_MIN_CENTS = 1_000_000_00      # $1M/yr
_YEARLY_MAX_CENTS = 15_000_000_00     # $15M/yr
MONTHLY_MIN_CENTS = _YEARLY_MIN_CENTS // 12
MONTHLY_MAX_CENTS = _YEARLY_MAX_CENTS // 12

BASE_FILTERS: dict[str, Any] = {
    "f:p": "shopify",
    "f:cc": "US,GB,CA,AU",
    "f:cc:op": "or",
    "f:ds": "Active",                       # skip password-protected / inactive
    "f:it": "email",                        # must have a contact route
    "f:ermin": MONTHLY_MIN_CENTS,
    "f:ermax": MONTHLY_MAX_CENTS,
    "f:tags": "Dropshipper,Print on Demand",
    "f:tags:op": "not",                     # brief 2: these are never a fit
}

# High-signal apps. Installing one means budget just moved; uninstalling one
# means they had the problem, tried a tool, and still have the problem.
GROWTH_APP_TOKENS = (
    "shopify.triplewhale-1",                # attribution/analytics
    "shopify.intelligems",                  # A/B testing
    "shopify.polar-analytics",              # analytics
    "shopify.lucky-orange",                 # heatmaps / CRO
    "shopify.microsoft-clarity",            # session replay
    "shopify.knocommerce",                  # post-purchase surveys / attribution
    "shopify.octane-ai-quiz-personalization",
    "shopify.rebuy",                        # personalisation / AOV
    "shopify.product-ads-by-criteo",        # paid ads
    "shopify.impact-1",                     # affiliate / partnerships
)


def _iso(dt: datetime) -> str:
    """StoreLeads wants JSON-style timestamps, e.g. 2026-04-23T18:25:43.511Z."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


@dataclass(frozen=True)
class Recipe:
    key: str
    label: str
    reason: str                 # the human "why now", used for the opener
    tier: str                   # expected heat: A hottest
    cadence: str                # "weekly" (trigger) or "daily" (baseline)
    max_per_run: int
    build: Callable[[datetime], dict[str, Any]] = field(repr=False)

    def params(self, now: Optional[datetime] = None) -> dict[str, Any]:
        """Full StoreLeads query for this recipe at a point in time."""
        now = now or datetime.now(timezone.utc)
        return {**BASE_FILTERS, **self.build(now)}


def _new_growth_app(now: datetime) -> dict[str, Any]:
    return {
        "f:an": ",".join(GROWTH_APP_TOKENS),
        "f:an:op": "or",
        "f:app_installed_at": ",".join(GROWTH_APP_TOKENS),
        "f:app_installed_at:min": _iso(now - timedelta(days=14)),
        "sort": "-er,rank",
    }


def _churned_tool(now: datetime) -> dict[str, Any]:
    return {
        "f:app_uninstalled_at": ",".join(GROWTH_APP_TOKENS),
        "f:app_uninstalled_at:min": _iso(now - timedelta(days=30)),
        "sort": "-er,rank",
    }


def _plan_upgrade(now: datetime) -> dict[str, Any]:
    return {
        "f:plan": "Shopify Plus",
        "f:last_plan_change_at:min": _iso(now - timedelta(days=60)),
        "sort": "-lplancat,rank",
    }


def _replatformed(now: datetime) -> dict[str, Any]:
    return {
        "f:last_platform_change_at:min": _iso(now - timedelta(days=90)),
        "sort": "-er,rank",
    }


def _social_surge(now: datetime) -> dict[str, Any]:
    return {
        "f:tiktokfollowers30dpmin": 25,     # +25% followers in 30 days
        "sort": "-fc30dp_10,rank",
    }


def _icp_baseline(now: datetime) -> dict[str, Any]:
    return {"sort": "-er,rank"}


RECIPES: tuple[Recipe, ...] = (
    Recipe(
        key="new_growth_app",
        label="Just installed a growth or CRO app",
        reason="They added a growth or conversion tool in the last two weeks",
        tier="A", cadence="weekly", max_per_run=40, build=_new_growth_app,
    ),
    Recipe(
        key="churned_tool",
        label="Just dropped a growth tool",
        reason="They removed a growth tool recently, so the problem is still open",
        tier="A", cadence="weekly", max_per_run=30, build=_churned_tool,
    ),
    Recipe(
        key="plan_upgrade",
        label="Upgraded to Shopify Plus",
        reason="They upgraded their store plan recently, which usually means new budget",
        tier="B", cadence="weekly", max_per_run=30, build=_plan_upgrade,
    ),
    Recipe(
        key="replatformed",
        label="Recently replatformed",
        reason="They moved platforms recently, so vendors are being chosen right now",
        tier="B", cadence="weekly", max_per_run=25, build=_replatformed,
    ),
    Recipe(
        key="social_surge",
        label="Social following spiking",
        reason="Their social following jumped in the last month",
        tier="B", cadence="weekly", max_per_run=25, build=_social_surge,
    ),
    Recipe(
        key="icp_baseline",
        label="Core ICP (volume filler)",
        reason="They fit our ideal customer profile",
        tier="C", cadence="daily", max_per_run=25, build=_icp_baseline,
    ),
)

_BY_KEY = {r.key: r for r in RECIPES}


def recipe(key: str) -> Optional[Recipe]:
    return _BY_KEY.get(key)


def recipes_for_day(weekday: int) -> list[Recipe]:
    """Which recipes run on a given weekday (Mon=0). StoreLeads refreshes weekly
    on Monday, so triggers are pulled Tue/Wed once the new rows have landed;
    the baseline runs every weekday to keep volume steady."""
    if weekday >= 5:            # weekends: no sending, so no pulling
        return []
    out = [r for r in RECIPES if r.cadence == "daily"]
    if weekday in (1, 2):       # Tuesday, Wednesday
        out = [r for r in RECIPES if r.cadence == "weekly"] + out
    return out


def daily_plan(now: Optional[datetime] = None) -> dict[str, Any]:
    """What we intend to pull today, and why. Drives the app's Lead Ops page."""
    now = now or datetime.now(timezone.utc)
    todays = recipes_for_day(now.weekday())
    return {
        "date": now.date().isoformat(),
        "weekday": now.strftime("%A"),
        "recipes": [
            {"key": r.key, "label": r.label, "tier": r.tier,
             "reason": r.reason, "max_per_run": r.max_per_run}
            for r in todays
        ],
        "planned_total": sum(r.max_per_run for r in todays),
    }
