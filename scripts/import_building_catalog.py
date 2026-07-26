"""Preview or import a review-gated Anata Building catalog through Agent's API."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from sales_support_agent.api.building_router import OfferingInput, SpaceInput


DEFAULT_CATALOG = (
    REPOSITORY_ROOT
    / "config"
    / "building_catalog_canva_draft.json"
)
CONFIRMATION = "IMPORT_UNPUBLISHED_CANVA_DRAFT"


def load_catalog(path: Path) -> dict[str, Any]:
    """Load and validate a catalog while enforcing its unpublished safety gate."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    safety = payload.get("safety") or {}
    if safety.get("publish_allowed") is not False:
        raise ValueError("Draft catalog must explicitly set publish_allowed to false.")

    spaces = [SpaceInput.model_validate(item) for item in payload.get("spaces", [])]
    offerings: list[OfferingInput] = []
    evidence: dict[str, dict[str, str]] = {}
    for raw in payload.get("offerings", []):
        evidence[str(raw.get("id") or "")] = {
            "evidence_price_reference": str(raw.get("evidence_price_reference") or ""),
            "review_note": str(raw.get("review_note") or ""),
        }
        accepted = {
            key: value
            for key, value in raw.items()
            if key in OfferingInput.model_fields
        }
        offerings.append(OfferingInput.model_validate(accepted))

    if not spaces or not offerings:
        raise ValueError("Catalog must contain at least one space and one offering.")
    if any(space.status != "unavailable" or space.is_public for space in spaces):
        raise ValueError("Every draft space must be unavailable and private.")
    if any(offering.is_published or offering.price_display for offering in offerings):
        raise ValueError("Every draft offering must be unpublished with no public price.")

    return {
        "metadata": {
            "catalog_id": payload.get("catalog_id"),
            "source": payload.get("source"),
            "safety": safety,
        },
        "spaces": spaces,
        "offerings": offerings,
        "evidence": evidence,
    }


def preview(catalog: dict[str, Any]) -> dict[str, Any]:
    """Return a human-readable import plan without making network requests."""
    return {
        **catalog["metadata"],
        "mode": "preview",
        "writes": {
            "spaces": len(catalog["spaces"]),
            "offerings": len(catalog["offerings"]),
        },
        "invariants": {
            "all_spaces_private": all(not item.is_public for item in catalog["spaces"]),
            "all_spaces_unavailable": all(
                item.status == "unavailable" for item in catalog["spaces"]
            ),
            "all_offerings_unpublished": all(
                not item.is_published for item in catalog["offerings"]
            ),
            "all_public_prices_blank": all(
                not item.price_display for item in catalog["offerings"]
            ),
        },
        "space_ids": [item.id for item in catalog["spaces"]],
        "offering_ids": [item.id for item in catalog["offerings"]],
        "evidence": catalog["evidence"],
    }


def api_put(base_url: str, path: str, payload: dict[str, Any], api_key: str) -> None:
    """Send one audited upsert to Agent's internal Building API."""
    request = Request(
        f"{base_url.rstrip('/')}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Internal-API-Key": api_key,
        },
        method="PUT",
    )
    try:
        with urlopen(request, timeout=30) as response:
            if response.status >= 300:
                raise RuntimeError(f"Unexpected HTTP status {response.status}.")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Agent rejected {path}: HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"Could not reach Agent for {path}: {exc.reason}") from exc


def apply_catalog(catalog: dict[str, Any], base_url: str, api_key: str) -> dict[str, Any]:
    """Import spaces first, then linked offerings, through audited API routes."""
    for space in catalog["spaces"]:
        api_put(
            base_url,
            f"/api/internal/building/spaces/{space.id}",
            space.model_dump(mode="json"),
            api_key,
        )
    for offering in catalog["offerings"]:
        api_put(
            base_url,
            f"/api/internal/building/offerings/{offering.id}",
            offering.model_dump(mode="json"),
            api_key,
        )
    return {
        **preview(catalog),
        "mode": "applied",
        "base_url": base_url,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Preview a Canva-derived Building catalog. Applying requires an "
            "explicit confirmation phrase and imports private, unpublished records only."
        )
    )
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument(
        "--base-url",
        default=os.getenv("AGENT_BASE_URL", "https://agent.anatainc.com"),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        catalog = load_catalog(args.catalog.resolve())
        if not args.apply:
            result = preview(catalog)
        else:
            if args.confirm != CONFIRMATION:
                raise ValueError(
                    f"--apply requires --confirm {CONFIRMATION}."
                )
            api_key = os.getenv("SALES_AGENT_INTERNAL_API_KEY", "").strip()
            if not api_key:
                raise ValueError(
                    "SALES_AGENT_INTERNAL_API_KEY is required for --apply."
                )
            result = apply_catalog(catalog, args.base_url, api_key)
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2), file=sys.stderr)
        return 1
    print(json.dumps({"ok": True, **result}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
