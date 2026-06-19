"""Free-form prospect intake for the Fulfillment Rate Sheet generator.

Sales reps paste notes, drop spreadsheets/CSVs/PDF brand decks/product
images, and point at a prospect's website; this layer flattens the text-like
inputs into one bounded text context for the LLM extraction step (llm.py)
and packages PDFs/images as base64 attachment dicts ready to become
Claude-native content blocks (document/image). Attachment budget: at most 4
PDF/image attachments and 18MB of raw bytes total; a single PDF over 8MB is
skipped outright. Mirrors brand_analysis/intake.py conventions:
intentionally forgiving — every file is wrapped in try/except, anything
unreadable degrades to a warning, and nothing here ever raises.
"""

from __future__ import annotations

import base64
import csv
import io
import logging
import re
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

# Size caps (characters ~= bytes for our purposes).
_NOTES_CAP = 20_000
_TEXT_FILE_CAP = 20_000
_WEBSITE_CAP = 15_000
_TOTAL_CAP = 60_000
_MAX_ROWS = 200

# Attachment budget for Claude-native PDF/image content blocks.
_MAX_ATTACHMENTS = 4
_MAX_ATTACHMENT_TOTAL_BYTES = 18 * 1024 * 1024  # 18MB raw across all attachments
_MAX_SINGLE_PDF_BYTES = 8 * 1024 * 1024         # a lone PDF over 8MB is skipped

_IMAGE_MEDIA_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
}

_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1\s*>", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _section(header: str, body: str) -> str:
    return f"=== {header} ===\n{body.strip()}\n"


def _read_csv(data: bytes) -> str:
    text = data.decode("utf-8", errors="replace")
    rows = []
    for i, row in enumerate(csv.reader(io.StringIO(text))):
        if i >= _MAX_ROWS:
            break
        rows.append(",".join("" if c is None else str(c) for c in row))
    return "\n".join(rows)


def _read_xlsx(data: bytes) -> str:
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    try:
        ws = wb.worksheets[0]
        try:
            ws.reset_dimensions()  # some exports zero the dimension
        except Exception:  # noqa: BLE001
            pass
        lines = []
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i >= _MAX_ROWS:
                break
            lines.append("\t".join("" if c is None else str(c) for c in row))
        return "\n".join(lines)
    finally:
        try:
            wb.close()
        except Exception:  # noqa: BLE001
            pass


def _attachment_kind(filename: str) -> tuple:
    """(kind, media_type) for attachable files, or (None, None)."""
    name = (filename or "").lower()
    if name.endswith(".pdf"):
        return "pdf", "application/pdf"
    for ext, media_type in _IMAGE_MEDIA_TYPES.items():
        if name.endswith(ext):
            return "image", media_type
    return None, None


def _read_file(filename: str, data: bytes, warnings: list) -> str:
    """One file -> text body, or "" with a warning appended."""
    name = (filename or "").lower()
    try:
        if name.endswith(".csv"):
            return _read_csv(data)
        if name.endswith((".xlsx", ".xlsm")):
            return _read_xlsx(data)
        if name.endswith((".txt", ".md")):
            return data.decode("utf-8", errors="replace")[:_TEXT_FILE_CAP]
        warnings.append(f"Unsupported file type: {filename} — skipped")
        return ""
    except Exception:  # noqa: BLE001 — intake never raises
        logger.warning("[fulfillment_deck] failed to read file %s", filename, exc_info=True)
        warnings.append(f"Could not read file: {filename} — skipped")
        return ""


def _fetch_website(url: str, warnings: list) -> str:
    """Fetch + crudely de-tag a prospect website. Failure -> warning, never raises."""
    try:
        import requests

        resp = requests.get(
            url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; AnataRateSheet/1.0)"},
        )
        html = resp.text or ""
        html = _SCRIPT_STYLE_RE.sub(" ", html)
        text = _TAG_RE.sub(" ", html)
        text = _WS_RE.sub(" ", text).strip()
        return text[:_WEBSITE_CAP]
    except Exception as exc:  # noqa: BLE001 — never raise on a flaky prospect site
        logger.warning("[fulfillment_deck] website fetch failed for %s", url, exc_info=True)
        warnings.append(f"Could not fetch website {url}: {exc.__class__.__name__}")
        return ""


# --- Brand identity scrape (logo + site name + tagline), SSRF-safe ---------

_MAX_LOGO_BYTES = 512 * 1024            # cap the inlined logo download at 512KB
_MAX_LOGO_DATA_URI = 700 * 1024         # guard the stored data-URI length
_BRAND_FETCH_TIMEOUT = 8                 # total seconds across head + image
_TAGLINE_CAP = 200

_OG_IMAGE_RE = re.compile(
    r'<meta[^>]+(?:property|name)\s*=\s*["\']og:image["\'][^>]*>', re.IGNORECASE
)
_OG_SITE_NAME_RE = re.compile(
    r'<meta[^>]+(?:property|name)\s*=\s*["\']og:site_name["\'][^>]*>', re.IGNORECASE
)
_OG_DESC_RE = re.compile(
    r'<meta[^>]+(?:property|name)\s*=\s*["\'](?:og:description|description)["\'][^>]*>',
    re.IGNORECASE,
)
_APPLE_ICON_RE = re.compile(
    r'<link[^>]+rel\s*=\s*["\'][^"\']*apple-touch-icon[^"\']*["\'][^>]*>', re.IGNORECASE
)
_ICON_RE = re.compile(
    r'<link[^>]+rel\s*=\s*["\'][^"\']*\bicon\b[^"\']*["\'][^>]*>', re.IGNORECASE
)
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_CONTENT_ATTR_RE = re.compile(r'content\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)
_HREF_ATTR_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)


def _meta_content(html: str, tag_re: re.Pattern) -> str:
    match = tag_re.search(html or "")
    if not match:
        return ""
    content = _CONTENT_ATTR_RE.search(match.group(0))
    return (content.group(1).strip() if content else "")


def _link_href(html: str, tag_re: re.Pattern) -> str:
    match = tag_re.search(html or "")
    if not match:
        return ""
    href = _HREF_ATTR_RE.search(match.group(0))
    return (href.group(1).strip() if href else "")


def fetch_brand_assets(website_url: str) -> dict:
    """Scrape a prospect's logo + identity from their site, SSRF-safely.

    Returns ``{logo_data_uri, site_name, tagline}`` — or ``{}`` on ANY failure
    (never raises, never blocks generation). The image fetch is restricted to
    the SAME host the admin supplied (http upgraded to https), capped at
    ``_MAX_LOGO_BYTES`` with an ``image/*`` content-type, and inlined as a
    size-bounded data-URI. Logo preference: og:image -> apple-touch-icon ->
    favicon. The tagline comes from og:site_name's description / meta
    description / <title>, clamped to 200 chars.
    """
    url = (website_url or "").strip()
    if not url:
        return {}
    try:
        import requests

        parsed = urlparse(url if "://" in url else "https://" + url)
        if parsed.scheme == "http":
            parsed = parsed._replace(scheme="https")
        if parsed.scheme != "https" or not parsed.hostname:
            return {}
        base = f"{parsed.scheme}://{parsed.netloc}"
        page_url = parsed.geturl()
        same_host = parsed.hostname.lower()

        resp = requests.get(
            page_url,
            timeout=_BRAND_FETCH_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0 (compatible; AnataRateSheet/1.0)"},
        )
        html = resp.text or ""

        site_name = _meta_content(html, _OG_SITE_NAME_RE)
        if not site_name:
            title_match = _TITLE_RE.search(html)
            if title_match:
                site_name = _WS_RE.sub(" ", title_match.group(1)).strip()
        tagline = _meta_content(html, _OG_DESC_RE)[:_TAGLINE_CAP]

        # Logo candidates, best first.
        candidates = [
            _meta_content(html, _OG_IMAGE_RE),
            _link_href(html, _APPLE_ICON_RE),
            _link_href(html, _ICON_RE),
            "/favicon.ico",
        ]
        logo_data_uri = ""
        for raw in candidates:
            if not raw:
                continue
            img_url = urljoin(base + "/", raw)
            img_parsed = urlparse(img_url)
            # SSRF guard: only fetch images from the SAME host (or a subdomain
            # of it), over https. No redirects to arbitrary hosts.
            if img_parsed.scheme != "https":
                continue
            host = (img_parsed.hostname or "").lower()
            if host != same_host and not host.endswith("." + same_host):
                continue
            data_uri = _fetch_logo_data_uri(img_url)
            if data_uri:
                logo_data_uri = data_uri
                break

        result = {}
        if logo_data_uri:
            result["logo_data_uri"] = logo_data_uri
        if site_name:
            result["site_name"] = site_name
        if tagline:
            result["tagline"] = tagline
        return result
    except Exception:  # noqa: BLE001 — brand scrape never blocks generation
        logger.warning("[fulfillment_deck] brand asset fetch failed for %s", url, exc_info=True)
        return {}


def _fetch_logo_data_uri(img_url: str) -> str:
    """Fetch one image, validate content-type + size, return a data-URI or ""."""
    try:
        import requests

        resp = requests.get(
            img_url,
            timeout=_BRAND_FETCH_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0 (compatible; AnataRateSheet/1.0)"},
            stream=True,
        )
        if resp.status_code != 200:
            return ""
        content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if not content_type.startswith("image/"):
            return ""
        data = resp.content or b""
        if not data or len(data) > _MAX_LOGO_BYTES:
            return ""
        data_uri = f"data:{content_type};base64,{base64.b64encode(data).decode('ascii')}"
        if len(data_uri) > _MAX_LOGO_DATA_URI:
            return ""
        return data_uri
    except Exception:  # noqa: BLE001
        return ""


def build_extraction_context(
    notes: str,
    files: list[tuple[str, bytes]],
    website_url: str,
) -> tuple[str, list[dict], list[str]]:
    """Flatten notes + uploaded files + website into one bounded context string
    plus a list of Claude-ready PDF/image attachments.

    Returns (context_text, attachments, warnings). Each attachment is
    ``{"name": str, "kind": "pdf"|"image", "media_type": str, "data_b64": str}``.
    Never raises.
    """
    warnings: list[str] = []
    sections: list[str] = []
    attachments: list[dict] = []
    attachment_bytes = 0

    notes = (notes or "").strip()
    if notes:
        sections.append(_section("SALES NOTES", notes[:_NOTES_CAP]))

    for filename, data in files or []:
        data = data or b""
        kind, media_type = _attachment_kind(filename)
        if kind is not None:
            if kind == "pdf" and len(data) > _MAX_SINGLE_PDF_BYTES:
                warnings.append(f"{filename} skipped (PDF over 8MB)")
                continue
            if (
                len(attachments) >= _MAX_ATTACHMENTS
                or attachment_bytes + len(data) > _MAX_ATTACHMENT_TOTAL_BYTES
            ):
                warnings.append(f"{filename} skipped (attachment budget)")
                continue
            attachments.append({
                "name": filename,
                "kind": kind,
                "media_type": media_type,
                "data_b64": base64.b64encode(data).decode("ascii"),
            })
            attachment_bytes += len(data)
            continue
        body = _read_file(filename, data, warnings)
        if body.strip():
            sections.append(_section(f"FILE: {filename}", body))

    url = (website_url or "").strip()
    if url:
        if "://" not in url:
            url = "https://" + url
        body = _fetch_website(url, warnings)
        if body:
            sections.append(_section(f"WEBSITE: {url}", body))

    context = "\n".join(sections)
    if len(context) > _TOTAL_CAP:
        context = context[:_TOTAL_CAP]
        warnings.append("Source material truncated to ~60KB for extraction.")
    return context, attachments, warnings
