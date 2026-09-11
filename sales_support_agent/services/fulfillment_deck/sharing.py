"""Customer-safe link metadata and deterministic branded preview cards."""
from __future__ import annotations

from functools import lru_cache
from html import escape
from io import BytesIO
import json
import re

from PIL import Image, ImageDraw, ImageFont

from sales_support_agent.config import load_settings
from sales_support_agent.services.deck.brand_assets import _candidate_brand_paths
from .workflow import document_kind


def _asset(relative: str):
    """Use the shared brand package first, then the bundled fallback."""
    return next(path for path in _candidate_brand_paths(load_settings(), relative) if path.is_file())


def share_identity(summary: dict) -> tuple[str, str]:
    """Use only the published recipient name and document type."""
    profile = summary.get("prospect_profile") or {}
    brand = str(profile.get("brand") or profile.get("company") or summary.get("prospect") or "Your brand")
    brand = " ".join(brand.split())[:96]
    kind = "Fulfillment proposal" if document_kind(summary) == "fulfillment_proposal" else "Shipping rate sheet"
    return brand, kind


def with_share_metadata(deck_html: str, summary: dict, url: str) -> str:
    """Refresh metadata for existing published HTML without mutating its content."""
    brand, kind = share_identity(summary)
    title = f"{brand} × Anata | {kind}"
    description = ("Your fulfillment plan, customer rates and estimated monthly invoice. Prepared by Anata."
                   if kind == "Fulfillment proposal" else "Explore your shipping rates by carrier and destination. Prepared by Anata.")
    image_url = url + "/share.png?v=1"
    # Old snapshots already have OG text tags; replace them instead of duplicating.
    deck_html = re.sub(r'<meta\b(?=[^>]*(?:property|name)=[\'"](?:og:|twitter:))[^>]*>', '', deck_html, flags=re.I)
    tags = {"og:title": title, "og:description": description, "og:type": "website",
            "og:site_name": "Anata", "og:url": url, "og:image": image_url,
            "og:image:secure_url": image_url, "og:image:type": "image/png",
            "og:image:width": "1200", "og:image:height": "630",
            "og:image:alt": f"{brand} × Anata — {kind}",
            "twitter:card": "summary_large_image", "twitter:title": title,
            "twitter:description": description, "twitter:image": image_url,
            "twitter:image:alt": f"{brand} × Anata — {kind}"}
    metadata = "\n".join(f'<meta {"name" if key.startswith("twitter:") else "property"}="{key}" content="{escape(value, quote=True)}">' for key, value in tags.items())
    return deck_html.replace("</head>", metadata + "\n</head>", 1)


@lru_cache(maxsize=128)
def render_share_image(brand: str, kind: str) -> bytes:
    """Render a 1200×630 PNG locally, without CRM, AI, network or pricing data."""
    colors = json.loads(_asset("tokens.json").read_text(encoding="utf-8"))["color"]
    canvas = Image.new("RGB", (1200, 630), colors["background"])
    draw = ImageDraw.Draw(canvas)
    def font(size: int, weight: int = 600):
        face = ImageFont.truetype(str(_asset("fonts/Montserrat.ttf")), size)
        face.set_variation_by_axes([weight])
        return face
    draw.rectangle((0, 0, 1200, 10), fill=colors["accent"])
    draw.rounded_rectangle((810, 40, 1160, 590), radius=24, fill=colors["ink"])
    logo = Image.open(_asset("assets/wordmark.png")).convert("RGBA")
    logo.thumbnail((178, 68), Image.Resampling.LANCZOS)
    canvas.paste(logo, (64, 58), logo)
    draw.text((64, 176), "PREPARED FOR", font=font(18), fill=colors["inkMuted"])
    # Fit up to two lines, including long/unbroken brand names.
    face = font(54, 750)
    lines, line = [], ""
    for char in brand:
        if draw.textlength(line + char, font=face) > 682:
            lines.append(line.rstrip()); line = ""
        line += char
    if line: lines.append(line.rstrip())
    lines = lines[:2]
    if sum(len(line) for line in lines) < len(brand.replace(" ", "")):
        lines[-1] = lines[-1][:-2] + "…"
    for i, line in enumerate(lines):
        draw.text((60, 218 + i * 66), line, font=face, fill=colors["ink"])
    draw.text((64, 385), kind, font=font(31, 650), fill=colors["ink"])
    subtitle = "Rates. Service. A clear path forward."
    draw.text((64, 440), subtitle, font=font(22, 450), fill=colors["inkMuted"])
    draw.line((64, 516, 745, 516), fill=colors["border"], width=2)
    draw.text((64, 544), "Explore your proposal  →" if kind == "Fulfillment proposal" else "Explore your rates  →", font=font(22, 650), fill=colors["ink"])
    draw.text((850, 88), "BUILT AROUND", font=font(17), fill=colors["accent"])
    draw.text((850, 119), "YOUR BUSINESS", font=font(17), fill=colors["accent"])
    labels = [("01", "Your rates"), ("02", "Your fulfillment"), ("03", "Your next step")]
    if kind != "Fulfillment proposal":
        labels[1] = ("02", "Your destinations")
    for i, (number, label) in enumerate(labels):
        y = 220 + i * 105
        draw.text((850, y), number, font=font(18), fill=colors["accent"])
        draw.text((850, y + 31), label, font=font(23, 650), fill=colors["surface"])
    output = BytesIO(); canvas.save(output, format="PNG", optimize=True)
    return output.getvalue()
