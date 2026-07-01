"""Branded social/SMS preview image generation for public sales decks."""

from __future__ import annotations

import io
from typing import Any, Mapping

import requests
from PIL import Image, ImageDraw, ImageFont


WIDTH = 1200
HEIGHT = 630


def render_sales_deck_preview_png(metadata: Mapping[str, Any]) -> bytes:
    brand = _clean(metadata.get("brand") or metadata.get("company_name") or "Anata prospect")
    title = _clean(metadata.get("title") or "Anata Strategy Deck")
    category = _clean(metadata.get("category") or "")
    description = _clean(metadata.get("description") or "")
    image_url = _clean(metadata.get("target_image_url") or "")

    img = Image.new("RGB", (WIDTH, HEIGHT), "#F9F7F3")
    draw = ImageDraw.Draw(img)
    _draw_background(draw)

    left = 72
    top = 72
    draw.rounded_rectangle((left, top, WIDTH - 72, HEIGHT - 72), radius=34, fill="#FFFFFF", outline="#DFD7C8", width=2)
    draw.rectangle((left, top, left + 12, HEIGHT - 72), fill="#2B3644")

    logo_font = _font(54, bold=True)
    label_font = _font(26, bold=True)
    title_font = _font(58, bold=True)
    body_font = _font(28)
    small_font = _font(24)

    draw.text((110, 104), "anata", font=logo_font, fill="#2B3644")
    draw.text((112, 178), "Strategy deck", font=label_font, fill="#6C7480")

    media_box = (780, 128, 1058, 406)
    product = _fetch_product_image(image_url)
    if product:
        _paste_contained(img, product, media_box)
    else:
        draw.rounded_rectangle(media_box, radius=30, fill="#2B3644")
        initials = _initials(brand)
        tw = draw.textlength(initials, font=title_font)
        draw.text(((media_box[0] + media_box[2] - tw) / 2, 232), initials, font=title_font, fill="#F9F7F3")

    draw.text((112, 236), brand[:42], font=label_font, fill="#85BBDA")
    y = 276
    for line in _wrap(title, title_font, 610, max_lines=3):
        draw.text((112, y), line, font=title_font, fill="#17222F")
        y += 66

    if category:
        draw.rounded_rectangle((112, 478, 112 + min(560, 28 + int(draw.textlength(category, font=small_font))), 520), radius=18, fill="#EEF5F8")
        draw.text((128, 486), category[:52], font=small_font, fill="#2B3644")
    elif description:
        for line in _wrap(description, body_font, 620, max_lines=2):
            draw.text((112, y + 10), line, font=body_font, fill="#59636F")
            y += 36

    draw.text((820, 472), "agent.anatainc.com", font=small_font, fill="#59636F")

    out = io.BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()


def _draw_background(draw: ImageDraw.ImageDraw) -> None:
    draw.rectangle((0, 0, WIDTH, HEIGHT), fill="#F9F7F3")
    draw.ellipse((900, -180, 1320, 250), fill="#E8F2F6")
    draw.ellipse((-140, 420, 320, 820), fill="#EFE6D8")


def _font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial Bold.ttf" if bold else "/Library/Fonts/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size=size)
        except Exception:
            continue
    return ImageFont.load_default()


def _fetch_product_image(url: str) -> Image.Image | None:
    if not url.startswith(("http://", "https://")):
        return None
    try:
        response = requests.get(url, timeout=4)
        response.raise_for_status()
        return Image.open(io.BytesIO(response.content)).convert("RGBA")
    except Exception:
        return None


def _paste_contained(base: Image.Image, product: Image.Image, box: tuple[int, int, int, int]) -> None:
    x1, y1, x2, y2 = box
    canvas = Image.new("RGBA", (x2 - x1, y2 - y1), "#FFFFFF")
    product.thumbnail((x2 - x1 - 32, y2 - y1 - 32), Image.Resampling.LANCZOS)
    px = (canvas.width - product.width) // 2
    py = (canvas.height - product.height) // 2
    canvas.alpha_composite(product, (px, py))
    mask = Image.new("L", canvas.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, canvas.width, canvas.height), radius=30, fill=255)
    base.paste(canvas.convert("RGB"), (x1, y1), mask)


def _wrap(text: str, font: ImageFont.ImageFont, width: int, *, max_lines: int) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current = ""
    measure = ImageDraw.Draw(Image.new("RGB", (1, 1))).textlength
    for word in words:
        trial = f"{current} {word}".strip()
        if measure(trial, font=font) <= width:
            current = trial
            continue
        if current:
            lines.append(current)
        current = word
        if len(lines) >= max_lines:
            break
    if current and len(lines) < max_lines:
        lines.append(current)
    if len(lines) == max_lines and len(" ".join(words)) > len(" ".join(lines)):
        lines[-1] = lines[-1].rstrip("., ") + "..."
    return lines or [text[:40]]


def _initials(value: str) -> str:
    pieces = [p for p in value.replace("&", " ").split() if p]
    return "".join(p[0].upper() for p in pieces[:2]) or "A"


def _clean(value: Any) -> str:
    return " ".join(str(value or "").strip().split())
