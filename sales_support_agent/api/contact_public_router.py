"""Public contact-form intake for anatainc.com.

POST /api/public/contact
    The marketing site's server route forwards a validated contact note here with
    the shared secret header (``X-Internal-Api-Key`` == ``MARKETING_SITE_INTAKE_KEY``,
    the site's ``AGENT_API_KEY``). No CORS: browsers never call this directly.

    Body: {name, email, company?, phone?, service?, note, files?: [{name, type, data}]}
    ``data`` is base64. Up to 4 files, images (JPEG/PNG/WebP/GIF) or PDF, 3 MB total,
    checked by magic bytes. The note and files are posted to #sales in Slack.
"""

from __future__ import annotations

import base64
import binascii
import logging
import re
from typing import Any, Optional

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from sales_support_agent.api.fulfillment_public_router import _enforce_intake_key
from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.services.public_request_guard import (
    durable_rate_limit_response,
    read_public_json_object,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/public", tags=["contact-public"])

SALES_SLACK_CHANNEL = "CQRMXS339"  # #sales
MAX_FILES = 4
MAX_TOTAL_BYTES = 3_000_000
MAX_NOTE_CHARS = 3_500
# 3 MB of files as base64 plus the note.
MAX_BODY_BYTES = 4_400_000

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_EXTENSIONS = {
    "image/jpeg": (".jpg", ".jpeg"),
    "image/png": (".png",),
    "image/webp": (".webp",),
    "image/gif": (".gif",),
    "application/pdf": (".pdf",),
}


def sniff_type(data: bytes) -> Optional[str]:
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if data[:5] == b"%PDF-":
        return "application/pdf"
    return None


def _safe_name(raw: Any) -> str:
    base = re.split(r"[/\\]", str(raw or "file"))[-1]
    cleaned = re.sub(r"[^\w.\- ()]+", "", base).strip()[:120]
    return cleaned or "file"


def decode_files(raw: Any) -> tuple[list[tuple[str, bytes, str]], Optional[str]]:
    if raw in (None, ""):
        return [], None
    if not isinstance(raw, list):
        return [], "files must be a list."
    if len(raw) > MAX_FILES:
        return [], f"Attach up to {MAX_FILES} files."
    out: list[tuple[str, bytes, str]] = []
    total = 0
    for item in raw:
        if not isinstance(item, dict):
            return [], "Each file needs a name and data."
        try:
            data = base64.b64decode(str(item.get("data") or ""), validate=True)
        except (binascii.Error, ValueError):
            return [], "A file could not be read."
        if not data:
            return [], "A file is empty."
        total += len(data)
        if total > MAX_TOTAL_BYTES:
            return [], "Files can total 3 MB."
        mime = sniff_type(data)
        if mime is None:
            return [], "Only images (JPEG, PNG, WebP, GIF) and PDFs can be attached."
        name = _safe_name(item.get("name"))
        if not name.lower().endswith(_EXTENSIONS[mime]):
            name = f"{name.rsplit('.', 1)[0] if '.' in name else name}{_EXTENSIONS[mime][0]}"
        out.append((name, data, mime))
    return out, None


def _is_bot(body: dict) -> bool:
    return any(str(body.get(f) or "").strip() for f in ("hp", "website_hp", "company_website", "website"))


@router.post("/contact")
async def contact_submit(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    denied = _enforce_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    limited = durable_rate_limit_response(request, scope="contact:submit", limit=30)
    if limited is not None:
        return limited
    body, bad = await read_public_json_object(request, max_bytes=MAX_BODY_BYTES)
    if bad is not None:
        return bad
    if _is_bot(body):
        return JSONResponse(status_code=202, content={"ok": True, "delivery": "slack"})

    email = str(body.get("email") or "").strip()
    name = str(body.get("name") or "").strip()[:200]
    note = str(body.get("note") or "").strip()
    if not name or not _EMAIL_RE.match(email) or len(email) > 254:
        return JSONResponse(status_code=400, content={"detail": "A name and a valid email are required."})
    if not note:
        return JSONResponse(status_code=400, content={"detail": "The note is empty."})
    files, err = decode_files(body.get("files"))
    if err:
        return JSONResponse(status_code=400, content={"detail": err})

    if len(note) > MAX_NOTE_CHARS:
        note = note[:MAX_NOTE_CHARS] + "\n…(truncated)"
    text = f":envelope_with_arrow: *New anatainc.com contact form* from {name} <{email}>\n```\n{note}\n```"

    slack = SlackClient(request.app.state.settings)
    if not slack.is_configured(SALES_SLACK_CHANNEL):
        return JSONResponse(status_code=503, content={"detail": "Slack is not configured."})
    try:
        if files:
            slack.upload_files(files=files, initial_comment=text, channel=SALES_SLACK_CHANNEL)
        else:
            slack.post_message(text=text, channel=SALES_SLACK_CHANNEL)
    except Exception:  # noqa: BLE001
        logger.exception("[contact_public] Slack delivery failed")
        return JSONResponse(status_code=502, content={"detail": "Delivery failed."})
    return JSONResponse(status_code=200, content={"ok": True, "delivery": "slack", "file_count": len(files)})
