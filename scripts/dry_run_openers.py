"""Print 20 fresh openers from recent leads. Read only.

The default run reads recent contacts for Trial Offer and the four v2
segments (plan_upgrade, replatform, amz_on, amz_off), attaches a stored
StoreLeads profile when one exists, and fills a missing product or story
from the public homepage. It never writes to Instantly or the database.

    python scripts/dry_run_openers.py
    python scripts/dry_run_openers.py --no-fetch
    python scripts/dry_run_openers.py --leads-file sample.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import outbound_openers as openers
import outbound_personalization as personalization
from outbound_contacts import split_person_name
from outbound_pipeline import clean_brand_name, copy_country_reason
from outbound_product_facts import (
    USER_AGENT,
    apply_product_facts,
    collect_product_facts,
    domain_brand_mismatch,
)

TRIAL_OFFER = "e5f5046b-07e4-41ad-a395-79cf8766ccd0"
SEGMENTS = (
    ("trial_offer", TRIAL_OFFER),
    ("plan_upgrade", "4dc10f79-f7c9-43c5-9c3e-5e28e9cf4c72"),
    ("replatform", "d1c197e3-bf1f-4f41-bcb9-e30e73975a2c"),
    ("amz_on", "8e0df427-1f18-43ac-b0c6-e8ea787123d7"),
    ("amz_off", "138a162d-6654-4916-9959-20c11e31214b"),
)
_OG_TITLE = re.compile(
    r"""<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']"""
    r"""|<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:title["']""",
    re.I,
)
_META_DESC = re.compile(
    r"""<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']"""
    r"""|<meta[^>]+content=["']([^"']+)["'][^>]+name=["']description["']""",
    re.I,
)
_OG_SITE = re.compile(
    r"""<meta[^>]+property=["']og:site_name["'][^>]+content=["']([^"']+)["']"""
    r"""|<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:site_name["']""",
    re.I,
)


def _database_url() -> str:
    return (
        os.getenv("SALES_AGENT_DB_URL")
        or os.getenv("DATABASE_URL")
        or ""
    ).strip()


def _segment(row: dict[str, Any]) -> str:
    campaign = str(row.get("campaign_id") or "")
    recipe = str(row.get("recipe") or "").strip()
    if campaign == TRIAL_OFFER or recipe in {"trial", "trial_offer", "icp_baseline", "social_surge"}:
        if campaign == TRIAL_OFFER:
            return "trial_offer"
    for name, campaign_id in SEGMENTS:
        if recipe in {name, "replatformed"} and name == "replatform":
            return "replatform"
        if recipe == name or campaign == campaign_id:
            return name
    if recipe in {"replatform", "replatformed"}:
        return "replatform"
    return recipe or "unknown"


def load_recent(url: str, *, per_segment: int = 4) -> list[dict[str, Any]]:
    """Newest contacts in each segment. SELECT only."""

    from sqlalchemy import create_engine, text

    engine = create_engine(url)
    found: list[dict[str, Any]] = []
    sql = text(
        """
        SELECT c.first_name, c.last_name, c.domain, c.brand, c.category,
               c.recipe, c.campaign_id, c.created_at::text AS created_at,
               l.lead_json
        FROM outbound_enriched_contacts c
        LEFT JOIN LATERAL (
            SELECT lead_json
            FROM outbound_pull_run_leads p
            WHERE p.domain = c.domain
            ORDER BY p.captured_at DESC
            LIMIT 1
        ) l ON true
        WHERE c.campaign_id = :campaign OR c.recipe = :recipe
           OR (:recipe = 'replatform' AND c.recipe = 'replatformed')
        ORDER BY c.created_at DESC
        LIMIT 80
        """
    )
    with engine.connect() as conn:
        for name, campaign_id in SEGMENTS:
            recipe = "trial_offer" if name == "trial_offer" else name
            rows = conn.execute(sql, {"campaign": campaign_id, "recipe": recipe}).mappings()
            picked = 0
            seen: set[str] = set()
            for row in rows:
                domain = str(row["domain"] or "").strip().lower()
                if not domain or domain in seen:
                    continue
                seen.add(domain)
                item = dict(row)
                item["segment"] = name
                found.append(item)
                picked += 1
                if picked >= per_segment:
                    break
    return found


def _profile_from_json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        payload = raw
    else:
        try:
            payload = json.loads(raw or "{}")
        except (TypeError, ValueError):
            return {}
    if not isinstance(payload, dict):
        return {}
    profile = payload.get("store_profile")
    return profile if isinstance(profile, dict) else {}


def _useful(profile: dict[str, Any]) -> bool:
    product = profile.get("most_recently_published_product")
    title = ""
    if isinstance(product, dict):
        title = str(product.get("title") or "")
    return bool(
        str(profile.get("description") or "").strip()
        or title.strip()
        or str(profile.get("store_locator_page") or "").strip()
        or str(profile.get("retailer_page") or "").strip()
    )


def _meta(html: str, pattern: re.Pattern[str]) -> str:
    match = pattern.search(html or "")
    if not match:
        return ""
    return " ".join(next(group for group in match.groups() if group).split())


def fetch_homepage_html(domain: str, *, timeout: float = 6.0) -> str:
    """Public homepage HTML. Used only after products.json is not a catalog."""

    import requests

    host = str(domain or "").strip().lower()
    if not host or " " in host:
        return ""
    try:
        response = requests.get(
            f"https://{host}",
            timeout=timeout,
            headers={"User-Agent": USER_AGENT, "Accept": "text/html"},
        )
    except Exception:
        return ""
    if response.status_code >= 400:
        return ""
    return response.text or ""


def fetch_public_fact(domain: str, *, timeout: float = 8.0) -> dict[str, str]:
    """Site name from the public homepage. A title is not a product."""

    html = fetch_homepage_html(domain, timeout=timeout)
    site_name = _meta(html, _OG_SITE)
    if not site_name:
        return {}
    return {"site_name": site_name[:120]}


def lead_for(
    row: dict[str, Any],
    *,
    facts: Optional[dict[str, Any]] = None,
    display_brand: str = "",
) -> dict[str, Any]:
    profile = _profile_from_json(row.get("lead_json") or row.get("store_profile") or {})
    if isinstance(row.get("store_profile"), dict):
        profile = {**profile, **row["store_profile"]}
    if facts and facts.get("source"):
        profile = apply_product_facts(profile, facts)
    segment = row.get("segment") or _segment(row)
    recipe = "trial_offer" if segment == "trial_offer" else (
        "replatform" if segment == "replatform" else str(row.get("recipe") or segment)
    )
    return {
        "brand": row.get("brand") or "",
        "display_brand": display_brand or (facts or {}).get("vendor") or row.get("display_brand") or "",
        "domain": row.get("domain") or "",
        "recipe": recipe,
        "categories": row.get("category") or row.get("categories") or "",
        "campaign_id": TRIAL_OFFER if segment == "trial_offer" else row.get("campaign_id") or "",
        "store_profile": profile,
    }


def model_timeout_seconds() -> float:
    """HTTP timeout for one dry-run model call. Default 30s. Override with OUTBOUND_OPENER_TIMEOUT."""

    raw = (os.getenv("OUTBOUND_OPENER_TIMEOUT") or "30").strip()
    try:
        return max(1.0, float(raw))
    except ValueError:
        return 30.0


def lead_budget_seconds() -> float:
    """Wall clock for one lead, including catalog reads and the model retry."""

    raw = (os.getenv("OUTBOUND_OPENER_LEAD_BUDGET") or "35").strip()
    try:
        return max(5.0, float(raw))
    except ValueError:
        return 35.0


def first_call_seconds() -> float:
    """Cap for the first model call. Default 22s. Override with OUTBOUND_OPENER_FIRST_CALL."""

    raw = (os.getenv("OUTBOUND_OPENER_FIRST_CALL") or "22").strip()
    try:
        return max(1.0, float(raw))
    except ValueError:
        return 22.0


def compatible_llm():
    """An OpenAI-compatible chat client, or None when the env is unset.

    Production copy still uses the AI Gateway. This is only for a local dry run.
    ``OUTBOUND_OPENER_BASE_URL`` is the API root, for example ``https://api.x.ai/v1``.
    ``OUTBOUND_OPENER_MODEL`` is the model name. ``OUTBOUND_OPENER_API_KEY`` is
    sent as a bearer token and is never printed. ``OUTBOUND_OPENER_TIMEOUT`` is
    the per-call timeout in seconds (default 30).
    """

    base = (os.getenv("OUTBOUND_OPENER_BASE_URL") or "").strip().rstrip("/")
    model = (os.getenv("OUTBOUND_OPENER_MODEL") or "").strip()
    if not base or not model:
        return None
    key = (os.getenv("OUTBOUND_OPENER_API_KEY") or "").strip()

    def call(prompt: str, timeout_s: Optional[float] = None) -> str:
        import requests

        from outbound_personalization import ModelTimeout

        headers = {"Content-Type": "application/json"}
        if key:
            headers["Authorization"] = f"Bearer {key}"
        limit = model_timeout_seconds() if timeout_s is None else timeout_s
        try:
            response = requests.post(
                f"{base}/chat/completions",
                headers=headers,
                json={
                    "model": model,
                    "temperature": 0.7,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=max(0.5, limit),
            )
        except requests.Timeout as exc:
            raise ModelTimeout("model timeout") from exc
        response.raise_for_status()
        payload = response.json()
        return str(payload["choices"][0]["message"]["content"] or "")

    return call


def render_row(
    row: dict[str, Any],
    *,
    facts: Optional[dict[str, Any]] = None,
    display_brand: str = "",
    llm: Optional[Any] = None,
    batch: Optional[list[str]] = None,
    angles: Optional[list[str]] = None,
    deadline: Optional[float] = None,
) -> dict[str, str]:
    """One opener pair. Nothing is saved."""

    openers._OPENERS_BY_DOMAIN.clear()
    openers._ANGLES_BY_DOMAIN.clear()
    lead = lead_for(row, facts=facts, display_brand=display_brand)
    first, last = split_person_name(row.get("first_name") or "", row.get("last_name") or "")
    company = clean_brand_name(
        lead.get("brand") or "",
        display_name=lead.get("display_brand") or "",
    )
    facts = facts or {}
    domain = str(row.get("domain") or "")
    gate = {
        "name": domain,
        "merchant_name": company or row.get("brand") or "",
        "currency_code": facts.get("currency_code") or "",
        "language_code": facts.get("language_code") or "",
    }
    hold = ""
    opener = ""
    angle = ""
    if facts.get("blocked"):
        hold = "skipped:blocked_category"
    elif not company:
        hold = "skipped:no recognizable brand"
    elif copy_country_reason(gate):
        from outbound_pipeline import country_eligibility

        _allowed, detail = country_eligibility(gate)
        hold = "skipped:non_us" + (f" ({detail})" if detail else "")
    elif domain_brand_mismatch(domain, str(row.get("brand") or ""), str(facts.get("vendor") or "")):
        hold = "skipped:domain_brand_mismatch"
    elif not facts.get("products"):
        hold = "skipped:no_product"
    else:
        writer = llm
        calls = {"n": 0}

        def allow_retry() -> bool:
            if deadline is None:
                return True
            return (deadline - time.monotonic()) >= 12

        if llm is not None:
            def writer(prompt: str, _llm=llm) -> str:
                calls["n"] += 1
                remaining = model_timeout_seconds() if deadline is None else deadline - time.monotonic()
                if remaining < 0.5:
                    raise personalization.ModelTimeout("lead budget")
                cap = first_call_seconds() if calls["n"] == 1 else remaining
                return _llm(prompt, timeout_s=min(model_timeout_seconds(), cap, remaining))

        opener, angle, source = personalization.generate(
            lead, llm=writer, batch=batch, angles=angles, allow_retry=allow_retry,
        )
        if source.startswith("skipped"):
            hold = source
            opener, angle = "", ""
        elif not opener:
            from sales_support_agent.services.llm_client import llm_available

            hold = "skipped:no_model" if llm is None and not llm_available() else "skipped:no_product"
            opener, angle = "", ""
    return {
        "segment": str(row.get("segment") or _segment(row)),
        "company": company or "(no recognizable brand)",
        "first_name": first,
        "last_name": last,
        "domain": domain,
        "opener": opener,
        "angle": angle,
        "hold": hold,
        "source": str(facts.get("source") or lead.get("store_profile", {}).get("product_fact_source") or ""),
        "pick": str(facts.get("pick") or ""),
        "product": str(lead.get("product_phrase") or facts.get("phrase") or ""),
    }


def render_report(rows: list[dict[str, str]]) -> str:
    lines = [f"{len(rows)} openers. Nothing was written to Instantly or the database.", ""]
    for index, row in enumerate(rows, start=1):
        who = row["first_name"] or "there"
        lines.append(f"### {index}. {row['company']} ({who}), {row['segment']}")
        lines.append(f"Domain: {row['domain']}")
        source = row.get("source") or "none"
        pick = row.get("pick") or "none"
        product = row.get("product") or "(none)"
        lines.append(f"Source: {source}")
        lines.append(f"Pick: {pick}")
        lines.append(f"Product: {product}")
        if row["opener"]:
            lines.append(f"Opener: {row['opener']}")
            lines.append(f"Angle: {row['angle'] or '(none)'}")
        else:
            lines.append(f"Skipped: {row['hold']}")
        lines.append("")
    held = [row for row in rows if row.get("hold")]
    lines.append(f"Skipped {len(held)} of {len(rows)}.")
    counts: dict[str, int] = {}
    for row in held:
        counts[row["hold"]] = counts.get(row["hold"], 0) + 1
    for reason, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- {reason}: {count}")
    return "\n".join(lines).rstrip() + "\n"


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Dry-run 20 openers from recent leads")
    parser.add_argument("--leads-file", default="")
    parser.add_argument(
        "--per-segment", type=int, default=None,
        help="Cap each segment. A leads file is read in full unless this is set.",
    )
    parser.add_argument("--no-fetch", action="store_true")
    args = parser.parse_args(argv)
    if args.leads_file:
        payload = json.loads(Path(args.leads_file).read_text())
        rows = payload if isinstance(payload, list) else payload.get("leads") or []
    else:
        url = _database_url()
        if not url:
            print("SALES_AGENT_DB_URL is not set, and no --leads-file was given.", file=sys.stderr)
            return 1
        rows = load_recent(url, per_segment=args.per_segment or 4)
    rendered = []
    llm = compatible_llm()
    batch: list[str] = []
    angles: list[str] = []
    if args.leads_file:
        cap = None if args.per_segment is None else args.per_segment * len(SEGMENTS)
    else:
        cap = (args.per_segment if args.per_segment is not None else 4) * len(SEGMENTS)
    budget = lead_budget_seconds()
    for row in rows:
        if cap is not None and len(rendered) >= cap:
            break
        profile = _profile_from_json(row.get("lead_json") or row.get("store_profile") or {})
        if isinstance(row.get("store_profile"), dict):
            profile = {**profile, **row["store_profile"]}
        facts: dict[str, Any] = {}
        display = ""
        started = time.monotonic()
        already = profile.get("representative_products") or profile.get("product_phrase")
        if not args.no_fetch and not already:
            domain = str(row.get("domain") or "")
            # StoreLeads is skipped here. This environment has no API key, and
            # the dry run must not call that API.
            facts = collect_product_facts(
                domain,
                brand=str(row.get("brand") or ""),
                allow_storeleads=False,
            )
            display = str(facts.get("site_name") or facts.get("vendor") or "")
        rendered.append(render_row(
            row, facts=facts, display_brand=display, llm=llm, batch=batch, angles=angles,
            deadline=started + budget,
        ))
    sys.stdout.write(render_report(rendered))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
