"""Dry-run StoreLeads personalization for leads that only have a name and email.

Default mode writes nothing. Pass domains on stdin or with --domains-file
(one host per line). With STORELEADS_API_KEY set, the script calls Bulk
Retrieve Domains and prints the opener and brandAngle it would use.

--apply updates Instantly custom variables for leads already in a campaign.
It is off unless you pass that flag. This repo task does not run --apply.

--sample writes the review file from the Nov 9 audit leads. That path does
not call StoreLeads or Instantly.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import outbound_openers as openers

# People and facts named in the Nov 9, 2026 audit. Nothing here was invented
# beyond the recipe each campaign already filters on (Amazon on or off).
SAMPLE_LEADS: list[dict[str, Any]] = [
    {
        "campaign": "amz_on",
        "email": "indrek@richhaircare.com",
        "brand": "RICH Hair Care",
        "domain": "richhaircare.com",
        "recipe": "amz_on",
        "categories": "",
        "note": "StoreLeads bulk lookup in the Nov 9 audit. Category was not in that lookup, so the before line is empty.",
        "store_profile": {
            "country_code": "US",
            "sales_channels": ["Amazon"],
            "apps": ["Klaviyo"],
            "technologies": ["Facebook Pixel", "Google Ads"],
            "product_count": 14,
            "avg_price_usd": 1948,
            "city": "Philadelphia",
            "store_locator_page": "https://richhaircare.com/pages/locator",
            "last_platform": "woocommerce",
            "last_platform_change_at": "2026-09-06T00:00:00Z",
            "employee_count": 12,
        },
    },
    {
        "campaign": "amz_off",
        "email": "olle@northtrampoline.com",
        "brand": "North Trampoline",
        "domain": "northtrampoline.com",
        "recipe": "amz_off",
        "categories": "",
        "note": "Recipe filter only. The live line in the audit was a generic catalog claim, not a fact about this brand.",
        "store_profile": {"country_code": "US", "sales_channels": []},
    },
    {
        "campaign": "amz_off",
        "email": "dylan@r-pur.com",
        "brand": "R-Pur",
        "domain": "r-pur.com",
        "recipe": "amz_off",
        "categories": "",
        "note": "Recipe filter only.",
        "store_profile": {"country_code": "US", "sales_channels": []},
    },
    {
        "campaign": "amz_off",
        "email": "marla@bbqing.com",
        "brand": "BBQing",
        "domain": "bbqing.com",
        "recipe": "amz_off",
        "categories": "",
        "note": "Title E-commerce Merchandise Associate. The title gate drops this contact. Copy is shown for review only.",
        "store_profile": {"country_code": "US", "sales_channels": []},
    },
    {
        "campaign": "amz_off",
        "email": "sappho@helloryse.com",
        "brand": "Helloryse",
        "domain": "helloryse.com",
        "recipe": "amz_off",
        "categories": "",
        "note": "Title Marketing & Support Associate. The title gate drops this contact. Copy is shown for review only.",
        "store_profile": {"country_code": "US", "sales_channels": []},
    },
    {
        "campaign": "amz_off",
        "email": "dayana@moxielash.com",
        "brand": "MoxieLash",
        "domain": "moxielash.com",
        "recipe": "amz_off",
        "categories": "",
        "note": "Recipe filter only. Title was E-Commerce and Marketing Operations, which is not a buyer title.",
        "store_profile": {"country_code": "US", "sales_channels": []},
    },
    {
        "campaign": "plan_upgrade",
        "email": "jennifer@kitbetter.com",
        "brand": "Kitbetter",
        "domain": "kitbetter.com",
        "recipe": "plan_upgrade",
        "categories": "",
        "note": "Recipe signal only. The audit gave both Jennifer and Grace the same plan-upgrade line.",
        "store_profile": {"country_code": "US", "plan": "Shopify Plus", "sales_channels": []},
    },
    {
        "campaign": "plan_upgrade",
        "email": "grace@kitbetter.com",
        "brand": "Kitbetter",
        "domain": "kitbetter.com",
        "recipe": "plan_upgrade",
        "categories": "",
        "note": "Same brand as Jennifer. One opener per domain.",
        "store_profile": {"country_code": "US", "plan": "Shopify Plus", "sales_channels": []},
    },
    {
        "campaign": "plan_upgrade",
        "email": "cammie@rockflowerpaper.com",
        "brand": "Rockflowerpaper",
        "domain": "rockflowerpaper.com",
        "recipe": "plan_upgrade",
        "categories": "",
        "note": "Recipe signal only.",
        "store_profile": {"country_code": "US", "plan": "Shopify Plus", "sales_channels": []},
    },
    {
        "campaign": "replatform",
        "email": "jaine@aurahome.com.au",
        "brand": "Aura Home",
        "domain": "aurahome.com.au",
        "recipe": "replatformed",
        "categories": "",
        "note": "Would be dropped. The audit found this store outside the US. Previous platform was not in the audit, so the line uses the recipe fact only.",
        "store_profile": {"country_code": "AU", "sales_channels": []},
    },
    {
        "campaign": "replatform",
        "email": "lucie@thecheesesociety.co.uk",
        "brand": "The Cheese Society",
        "domain": "thecheesesociety.co.uk",
        "recipe": "replatformed",
        "categories": "",
        "note": "Would be dropped. UK store. Previous platform was not in the audit.",
        "store_profile": {"country_code": "GB", "sales_channels": []},
    },
    {
        "campaign": "replatform",
        "email": "sigourney@beauticate.com",
        "brand": "Beauticate",
        "domain": "beauticate.com",
        "recipe": "replatformed",
        "categories": "",
        "note": "Recipe signal only. Previous platform was not in the audit.",
        "store_profile": {"country_code": "US", "sales_channels": []},
    },
    {
        "campaign": "abby_reengage",
        "email": "sunny@sunnyskorean.com",
        "brand": "Sunny's Korean",
        "domain": "sunnyskorean.com",
        "recipe": "abby_reengage",
        "categories": "",
        "note": "No StoreLeads profile yet. The backfill dry-run fills this.",
        "store_profile": {},
    },
    {
        "campaign": "abby_reengage",
        "email": "fischer@xviveaudio.com",
        "brand": "Xvive",
        "domain": "xviveaudio.com",
        "recipe": "abby_reengage",
        "categories": "",
        "note": "No StoreLeads profile yet. Positive reply on Nov 5.",
        "store_profile": {},
    },
    {
        "campaign": "abby_reengage",
        "email": "ducmai@ichooseglobal.com",
        "brand": "iChoose Global",
        "domain": "ichooseglobal.com",
        "recipe": "abby_reengage",
        "categories": "",
        "note": "No StoreLeads profile yet. The duplicate lead is why positives landed on the old campaign.",
        "store_profile": {},
    },
    {
        "campaign": "amz_off",
        "email": "coziya@coziya-outdoor.com",
        "brand": "Coziya",
        "domain": "coziya-outdoor.com",
        "recipe": "amz_off",
        "categories": "",
        "note": "Would be dropped. StoreLeads country is EE. Facts are from the Nov 9 bulk lookup, so the line shows what the builder would have said.",
        "store_profile": {
            "country_code": "EE",
            "city": "Tallinn",
            "sales_channels": [],
            "apps": ["Klaviyo", "Intelligems", "GoAffPro"],
            "product_count": 12,
            "trustpilot": {"review_count": 854, "avg_rating": 4.1},
            "contact_info": [{"type": "tiktok", "followers": 83800, "followers_30d": 1200}],
        },
    },
]


def _lines(lead: dict[str, Any], *, rich: bool) -> tuple[str, str, list[str]]:
    stamped = {**lead, "rich_personalization": rich}
    openers._OPENERS_BY_DOMAIN.pop(openers._domain_key(stamped), None)
    openers._ANGLES_BY_DOMAIN.pop(openers._domain_key(stamped), None)
    opener, _source = openers.write_opener(stamped)
    angle, _angle_source = openers.write_brand_angle(stamped)
    phrases = list(openers.collect_facts(stamped).rich_phrases) if rich else []
    return opener, angle, phrases


def render_sample(leads: list[dict[str, Any]]) -> str:
    blocks = [
        "# Personalization sample",
        "",
        "Before and after for leads named in the Nov 9, 2026 audit.",
        "Before is what today's builder writes from the facts still stored on the lead.",
        "An empty before line means we did not have a category or recipe sentence, not that Instantly sent a blank.",
        "After is the deterministic line with OUTBOUND_RICH_PERSONALIZATION_ENABLED on.",
        "Counts are words. No line contains a digit or an em dash.",
        "Rows marked dropped would not be emailed. The line is here so the wording can be reviewed.",
        "abby_reengage rows stay empty until the dry-run backfill has a StoreLeads profile.",
        "This file did not call Instantly or change any lead.",
        "",
    ]
    for lead in leads:
        before, before_angle = _lines(lead, rich=False)[:2]
        after, after_angle, phrases = _lines(lead, rich=True)
        blocks.extend([
            f"## {lead['campaign']}: {lead['email']}",
            "",
            f"Brand: {lead['brand']}",
            "",
            f"Note: {lead['note']}",
            "",
            f"Before personalization: {before or '(empty)'}",
            "",
            f"Before brandAngle: {before_angle or '(empty)'}",
            "",
            f"Facts the new builder would use: {', '.join(phrases) if phrases else '(none on file)'}",
            "",
            f"After personalization: {after or '(empty)'}",
            "",
            f"After brandAngle: {after_angle or '(empty)'}",
            "",
        ])
    return "\n".join(blocks).rstrip() + "\n"


def _targets_from(lines: list[str]) -> list[dict[str, str]]:
    """One target per line. An email is kept. A bare domain is looked up later."""

    targets: list[dict[str, str]] = []
    for raw in lines:
        text = raw.strip().lower()
        if not text or text.startswith("#"):
            continue
        email = ""
        domain = text
        if "@" in text:
            email = text.split(",", 1)[0].strip()
            rest = text.split(",", 1)[1].strip() if "," in text else ""
            domain = rest or email.split("@", 1)[1]
        else:
            domain = text.split(",", 1)[0].strip()
        if domain:
            targets.append({"email": email, "domain": domain})
    return targets


def _domains_from(path: str) -> list[dict[str, str]]:
    with open(path, encoding="utf-8") as handle:
        return _targets_from(handle.readlines())


def attach_campaign_emails(rows: list[dict[str, Any]], leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fill a missing email from campaign leads on the same domain.

    A row that already has an email is unchanged. A domain with several
    leads becomes one row per lead. A domain with no lead is left out.
    """

    by_domain: dict[str, list[str]] = {}
    for lead in leads:
        email = str(lead.get("email") or "").strip().lower()
        if "@" not in email:
            continue
        domain = email.split("@", 1)[1]
        by_domain.setdefault(domain, [])
        if email not in by_domain[domain]:
            by_domain[domain].append(email)
    attached: list[dict[str, Any]] = []
    for row in rows:
        email = str(row.get("email") or "").strip().lower()
        if email:
            attached.append({**row, "email": email})
            continue
        domain = str(row.get("domain") or "").strip().lower()
        for match in by_domain.get(domain, []):
            attached.append({**row, "email": match})
    return attached


def _dry_run(targets: list[dict[str, str]], *, lookup: Any = None) -> dict[str, Any]:
    import outbound_pipeline as pipeline

    store_key = (os.getenv("STORELEADS_API_KEY") or "").strip()
    domains = [item["domain"] for item in targets if item.get("domain")]
    if lookup is None:
        if not store_key:
            return {"ok": False, "reason": "STORELEADS_API_KEY is not set", "rows": []}
        found = pipeline.lookup_domains(store_key, domains)
    else:
        found = lookup(domains) or {}
    rows = []
    for target in targets:
        domain = target["domain"]
        store = found.get(domain) or {}
        country = str(store.get("country_code") or "").strip().upper()
        lead = {
            "brand": str(store.get("merchant_name") or domain),
            "domain": domain,
            "recipe": "abby_reengage",
            "categories": store.get("categories") or "",
            "store_profile": pipeline.store_profile(store) if store else {},
            "rich_personalization": True,
        }
        opener, angle, phrases = _lines(lead, rich=True)
        rows.append({
            "email": target.get("email") or "",
            "domain": domain,
            "country": country,
            "us": country == "US",
            "opener": opener,
            "brand_angle": angle,
            "facts": phrases,
        })
    return {"ok": True, "reason": "dry run", "rows": rows}


def _campaign_leads(api_key: str, campaign_id: str) -> list[dict[str, Any]]:
    """Leads already in the re-engage campaign. Read only."""

    import requests

    found: list[dict[str, Any]] = []
    body: dict[str, Any] = {"campaign": campaign_id, "limit": 100}
    for _page in range(30):
        response = requests.post(
            "https://api.instantly.ai/api/v2/leads/list",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=body,
            timeout=30,
        )
        if response.status_code != 200:
            break
        payload = response.json() or {}
        found.extend(item for item in (payload.get("items") or []) if isinstance(item, dict))
        nxt = str(payload.get("next_starting_after") or "").strip()
        if not nxt:
            break
        body["starting_after"] = nxt
    return found


def _apply(
    rows: list[dict[str, Any]],
    campaign_id: str,
    *,
    campaign_leads: list[dict[str, Any]] | None = None,
    list_leads: Any = None,
    patch_lead: Any = None,
    find_lead: Any = None,
) -> dict[str, Any]:
    """Update personalization on existing Instantly leads. Not the default."""

    import requests

    api_key = (os.getenv("INSTANTLY_API_KEY") or os.getenv("INSTANTLY_AI") or "").strip()
    if not api_key and patch_lead is None:
        return {"ok": False, "reason": "Instantly API key is not set", "updated": 0}
    if any(not row.get("email") for row in rows):
        leads = campaign_leads
        if leads is None and list_leads is not None:
            leads = list_leads(campaign_id)
        if leads is None:
            leads = _campaign_leads(api_key, campaign_id)
        rows = attach_campaign_emails(rows, leads or [])
    rows = [row for row in rows if row.get("email")]
    if not rows:
        return {"ok": False, "reason": "No campaign lead matched these domains.", "updated": 0}
    updated = 0
    errors = []
    for row in rows:
        if not row.get("us") or not row.get("opener"):
            continue
        if find_lead is not None:
            items = list(find_lead(row["email"]) or [])
        else:
            listed = requests.post(
                "https://api.instantly.ai/api/v2/leads/list",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"campaign": campaign_id, "contacts": [row["email"]], "limit": 10},
                timeout=30,
            )
            if listed.status_code != 200:
                errors.append(f"list {listed.status_code}")
                continue
            items = list((listed.json() or {}).get("items") or [])
        for item in items:
            lead_id = str(item.get("id") or "")
            if not lead_id:
                continue
            if patch_lead is not None:
                status = int(patch_lead(lead_id, row) or 0)
                if status < 300:
                    updated += 1
                else:
                    errors.append(f"patch {status}")
                continue
            patched = requests.patch(
                f"https://api.instantly.ai/api/v2/leads/{lead_id}",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={
                    "personalization": row["opener"],
                    "custom_variables": {"brandAngle": row["brand_angle"]},
                },
                timeout=30,
            )
            if patched.status_code < 300:
                updated += 1
            else:
                errors.append(f"patch {patched.status_code}")
    return {"ok": not errors, "updated": updated, "errors": errors[:10]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Dry-run re-engage personalization backfill")
    parser.add_argument("--domains-file", default="")
    parser.add_argument("--sample", default="")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--campaign-id", default="907d32fd-d6db-4ce6-8edc-485b78cce120")
    args = parser.parse_args(argv)
    if args.sample:
        text = render_sample(SAMPLE_LEADS)
        with open(args.sample, "w", encoding="utf-8") as handle:
            handle.write(text)
        print(f"wrote {args.sample}")
        return 0
    if args.domains_file:
        targets = _domains_from(args.domains_file)
    else:
        targets = _targets_from(sys.stdin.readlines())
    result = _dry_run(targets)
    print(json.dumps({key: value for key, value in result.items() if key != "rows"}, indent=2))
    for row in result.get("rows") or []:
        print(json.dumps(row))
    if not result.get("ok"):
        return 1
    if args.apply:
        applied = _apply(result["rows"], args.campaign_id)
        print(json.dumps(applied))
        return 0 if applied.get("ok") else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
