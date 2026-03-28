#!/usr/bin/env python3
"""Safe execution helpers for approved website-ops actions."""

from __future__ import annotations

import base64
import hashlib
import html
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .core import WebsiteOpsConfig, collect_page_observation, load_config


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_BACKUPS_ROOT = ROOT_DIR / "website-ops" / "backups"
SUPPORTED_ACTION_TYPES = {
    "replace_primary_heading",
    "rewrite_title_and_intro",
    "strengthen_primary_cta",
    "add_internal_links",
    "update_faq_ai_extraction",
    "inject_faq_block",
    "expand_service_page_section",
}
TEXT_WIDGET_TYPES = {"text-editor", "html"}
PROOF_WIDGET_TYPES = {"text-editor", "html", "icon-list"}
FAQ_WIDGET_TYPES = {"accordion", "toggle", "text-editor", "html"}


class ExecutionError(RuntimeError):
    """Raised when an approved action cannot be safely executed."""


def wp_site_url() -> str:
    return os.getenv("WP_SITE_URL", "").strip().rstrip("/")


def wp_username() -> str:
    return os.getenv("WP_USERNAME", "").strip()


def wp_application_password() -> str:
    return os.getenv("WP_APPLICATION_PASSWORD", "").strip()


def execution_enabled() -> bool:
    return os.getenv("WEBSITE_OPS_EXECUTE_APPROVED", "").strip().lower() in {"1", "true", "yes", "y"}


def backup_root() -> Path:
    configured = os.getenv("WEBSITE_OPS_BACKUPS_DIR", "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_BACKUPS_ROOT


def wp_headers() -> Dict[str, str]:
    username = wp_username()
    password = wp_application_password()
    if not wp_site_url() or not username or not password:
        raise ExecutionError("Missing WP_SITE_URL, WP_USERNAME, or WP_APPLICATION_PASSWORD.")
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def wp_request(path: str, *, method: str = "GET", payload: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    url = f"{wp_site_url()}{path}"
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=wp_headers(), method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8", errors="replace")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise ExecutionError(f"WordPress API error {exc.code} for {path}: {body}") from exc
    except urllib.error.URLError as exc:
        raise ExecutionError(f"WordPress API request failed for {path}: {exc.reason}") from exc


def infer_slug_from_url(page_url: str) -> str:
    path = urllib.parse.urlparse(str(page_url)).path.strip("/")
    if not path:
        return "home"
    return path.split("/")[-1]


def resolve_page_record(feedback: Mapping[str, Any]) -> Dict[str, Any]:
    target_post_id = str(feedback.get("target_post_id", "")).strip()
    if target_post_id:
        record = wp_request(f"/wp-json/wp/v2/pages/{urllib.parse.quote(target_post_id)}?context=edit")
        if record:
            return record

    page_url = str(feedback.get("page_url", "")).strip()
    if not page_url:
        raise ExecutionError("Approved action is missing page_url or target_post_id.")
    slug = infer_slug_from_url(page_url)
    candidates = wp_request(f"/wp-json/wp/v2/pages?slug={urllib.parse.quote(slug)}&context=edit&per_page=50")
    if not isinstance(candidates, list) or not candidates:
        raise ExecutionError(f"No WordPress page found for {page_url}.")
    for candidate in candidates:
        if str(candidate.get("link", "")).rstrip("/") == page_url.rstrip("/"):
            return candidate
    return candidates[0]


def parse_elementor_data(record: Mapping[str, Any]) -> List[Dict[str, Any]]:
    meta = record.get("meta") or {}
    raw = meta.get("_elementor_data")
    if not raw:
        raise ExecutionError("Page does not expose Elementor data in REST meta.")
    if isinstance(raw, list):
        return raw
    return json.loads(str(raw))


def walk_elements(elements: Sequence[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for element in elements:
        yield element
        for child in walk_elements(element.get("elements") or []):
            yield child


def walk_widget_refs(
    elements: List[Dict[str, Any]],
    *,
    parent: Optional[List[Dict[str, Any]]] = None,
) -> Iterable[Tuple[Dict[str, Any], List[Dict[str, Any]], int]]:
    current_parent = parent or elements
    for index, element in enumerate(elements):
        yield element, current_parent, index
        children = element.get("elements") or []
        if isinstance(children, list) and children:
            for child in walk_widget_refs(children, parent=children):
                yield child


def _new_widget_id(prefix: str) -> str:
    return hashlib.sha1(f"{prefix}-{datetime.now(timezone.utc).isoformat()}".encode("utf-8")).hexdigest()[:7]


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _strip_html(value: Any) -> str:
    return _normalize_text(re.sub(r"<[^>]+>", " ", str(value or "")))


def _find_widget_refs(elements: List[Dict[str, Any]], widget_types: set[str]) -> List[Tuple[Dict[str, Any], List[Dict[str, Any]], int]]:
    refs: List[Tuple[Dict[str, Any], List[Dict[str, Any]], int]] = []
    for element, parent, index in walk_widget_refs(elements):
        if str(element.get("widgetType", "")).strip() in widget_types:
            refs.append((element, parent, index))
    return refs


def _flatten_widget_refs(elements: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], List[Dict[str, Any]], int]]:
    return [ref for ref in walk_widget_refs(elements) if str(ref[0].get("widgetType", "")).strip()]


def _widget_text(element: Mapping[str, Any]) -> str:
    widget_type = str(element.get("widgetType", "")).strip()
    settings = element.get("settings") or {}
    if widget_type == "heading":
        return str(settings.get("title", ""))
    if widget_type == "button":
        return str(settings.get("text", ""))
    if widget_type == "icon-list":
        items = settings.get("icon_list") or []
        if isinstance(items, list):
            return " ".join(str(item.get("text", "")) for item in items if isinstance(item, dict))
        return ""
    if widget_type in TEXT_WIDGET_TYPES:
        return str(settings.get("editor", "") or settings.get("html", ""))
    return ""


def _set_widget_text(element: Dict[str, Any], value: str) -> None:
    widget_type = str(element.get("widgetType", "")).strip()
    settings = dict(element.get("settings") or {})
    if widget_type == "heading":
        settings["title"] = value
    elif widget_type == "button":
        settings["text"] = value
    elif widget_type == "icon-list":
        settings["icon_list"] = [{"text": line.strip()} for line in str(value).splitlines() if line.strip()]
    elif widget_type == "html":
        settings["html"] = value
    else:
        settings["editor"] = value
    element["settings"] = settings


def _make_text_widget(html_value: str) -> Dict[str, Any]:
    return {
        "id": _new_widget_id("text"),
        "elType": "widget",
        "widgetType": "text-editor",
        "settings": {"editor": html_value},
        "elements": [],
    }


def _make_heading_widget(text: str, *, level: str = "h2") -> Dict[str, Any]:
    return {
        "id": _new_widget_id("heading"),
        "elType": "widget",
        "widgetType": "heading",
        "settings": {"title": text, "header_size": level},
        "elements": [],
    }


def clean_generated_content(text: str) -> str:
    cleaned = str(text or "")
    for brand in ("search atlas", "linkgraph", "amazon", "shipbob", "red stag", "quiet platforms"):
        cleaned = re.sub(re.escape(brand), "competitor", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    sentences = [item.strip() for item in re.split(r"(?<=[.!?])\s+", cleaned) if item.strip()]
    normalized: list[str] = []
    for sentence in sentences:
        words = sentence.split()
        if len(words) > 24:
            sentence = " ".join(words[:24]).rstrip(",;:") + "."
        normalized.append(sentence)
    cleaned = " ".join(normalized).strip()
    if cleaned and not re.match(r"^(what|how|when|why|anata|this|these)\b", cleaned, flags=re.IGNORECASE):
        cleaned = "Anata answers directly: " + cleaned[0].lower() + cleaned[1:]
    return cleaned


def _elementor_html_snapshot(elements: Sequence[Dict[str, Any]]) -> str:
    chunks: list[str] = []
    for element, _, _ in _flatten_widget_refs(list(elements)):
        widget_type = str(element.get("widgetType", "")).strip()
        text = _widget_text(element)
        if not text:
            continue
        rendered = str(text)
        if widget_type == "heading":
            level = str((element.get("settings") or {}).get("header_size", "h2")).strip().lower() or "h2"
            chunks.append(f"<{level}>{rendered}</{level}>")
        elif widget_type == "button":
            chunks.append(f"<div class='cta-section'><button>{html.escape(_strip_html(rendered))}</button></div>")
        else:
            chunks.append(rendered)
    return "\n".join(chunks)


def faq_exists(page_html: str) -> bool:
    haystack = str(page_html or "")
    normalized = haystack.lower()
    if '<section class="anata-faq"' in normalized or "<section class='anata-faq'" in normalized:
        return True
    if "frequently asked" in normalized or re.search(r"\bfaq\b", normalized):
        return True
    return '"@type":"faqpage"' in normalized.replace(" ", "") or '"@type": "FAQPage"' in haystack


def _faq_marker_count(page_html: str) -> int:
    normalized = str(page_html or "").lower()
    count = 0
    count += normalized.count('class="anata-faq"') + normalized.count("class='anata-faq'")
    count += len(re.findall(r"\bfaq\b", normalized))
    count += normalized.count("frequently asked")
    count += normalized.replace(" ", "").count('"@type":"faqpage"')
    return count


def resolve_insertion_point(page_html: str) -> Dict[str, Any]:
    html_text = str(page_html or "")
    if not html_text.strip():
        return {"strategy": "end_of_content", "index": 0}
    first_major = re.search(r"</(?:p|section|div|h2)>", html_text, flags=re.IGNORECASE)
    cta = re.search(r"(book|contact|schedule|analysis|call|get started)", html_text, flags=re.IGNORECASE)
    if first_major and (cta is None or first_major.end() <= cta.start()):
        return {"strategy": "after_first_major_section", "index": first_major.end()}
    if cta:
        return {"strategy": "before_cta", "index": cta.start()}
    return {"strategy": "end_of_content", "index": len(html_text)}


def _resolve_widget_insertion_index(elements: List[Dict[str, Any]], insertion_point: Mapping[str, Any]) -> int:
    refs = _flatten_widget_refs(elements)
    strategy = str(insertion_point.get("strategy", "")).strip()
    if strategy == "after_first_major_section":
        text_or_heading_refs = [
            ref for ref in refs if str(ref[0].get("widgetType", "")).strip() in (TEXT_WIDGET_TYPES | {"heading"})
        ]
        if text_or_heading_refs:
            _, parent, index = text_or_heading_refs[min(1, len(text_or_heading_refs) - 1)]
            return index + 1 if parent is elements else len(elements)
    if strategy == "before_cta":
        button_ref = next((ref for ref in refs if str(ref[0].get("widgetType", "")) == "button"), None)
        if button_ref:
            _, parent, index = button_ref
            return index if parent is elements else len(elements)
    return len(elements)


def _sanitize_html_fragment(value: str) -> str:
    text = clean_generated_content(_strip_html(value))
    return html.escape(text)


def _parse_action_payload(feedback: Mapping[str, Any]) -> Dict[str, Any]:
    raw = str(feedback.get("action_value", "") or feedback.get("suggested_action_value", "") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {"text": raw}
    return parsed if isinstance(parsed, dict) else {"value": parsed}


def _fetch_live_html(url: str, *, config: WebsiteOpsConfig) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": config.user_agent})
    try:
        with urllib.request.urlopen(request, timeout=config.timeout_seconds) as response:
            return response.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as exc:
        raise ExecutionError(f"Failed to fetch live page for verification: {exc.reason}") from exc


def _verify_text_present(html_text: str, expected_text: str) -> bool:
    return _normalize_text(expected_text).lower() in _strip_html(html_text).lower()


def _infer_region_label(action_type: str) -> str:
    labels = {
        "replace_primary_heading": "Primary heading",
        "rewrite_title_and_intro": "Hero title and intro",
        "strengthen_primary_cta": "Primary CTA and proof block",
        "add_internal_links": "Intro/body copy insertion zone",
        "update_faq_ai_extraction": "FAQ / AI extraction section",
        "inject_faq_block": "FAQ insertion zone",
        "expand_service_page_section": "After first major section",
    }
    return labels.get(action_type, "Page region")


def execution_target_details(feedback: Mapping[str, Any]) -> Dict[str, Any]:
    action_type = str(feedback.get("action_type") or feedback.get("suggested_action_type") or "").strip()
    if action_type not in SUPPORTED_ACTION_TYPES:
        return {
            "eligible": False,
            "execution_eligibility": "manual_only",
            "target_region": _infer_region_label(action_type),
            "reason": "Unsupported action type.",
            "verification_requirements": [],
        }
    if not wp_site_url() or not wp_username() or not wp_application_password():
        return {
            "eligible": False,
            "execution_eligibility": "approval_required",
            "target_region": _infer_region_label(action_type),
            "reason": "WordPress execution credentials are not configured.",
            "verification_requirements": [],
        }
    try:
        record = resolve_page_record(feedback)
        elements = parse_elementor_data(record)
        flat = _flatten_widget_refs(elements)
    except ExecutionError as exc:
        return {
            "eligible": False,
            "execution_eligibility": "approval_required",
            "target_region": _infer_region_label(action_type),
            "reason": str(exc),
            "verification_requirements": [],
        }

    eligible = False
    reason = ""
    verification: list[str] = []
    if action_type == "replace_primary_heading":
        eligible = any(str(element.get("widgetType", "")) == "heading" for element, _, _ in flat)
        reason = "Primary heading widget located." if eligible else "No heading widget found."
        verification = ["Exactly one live H1", "Updated H1 text is visible"]
    elif action_type == "rewrite_title_and_intro":
        has_heading = any(str(element.get("widgetType", "")) == "heading" for element, _, _ in flat)
        has_text = any(str(element.get("widgetType", "")) in TEXT_WIDGET_TYPES for element, _, _ in flat)
        eligible = has_heading and has_text
        reason = "Hero heading and intro text widgets located." if eligible else "Required heading/text widgets were not found."
        verification = ["Live title contains new framing", "Intro text is visible", "Single H1 remains"]
    elif action_type == "strengthen_primary_cta":
        has_button = any(str(element.get("widgetType", "")) == "button" for element, _, _ in flat)
        has_support = any(str(element.get("widgetType", "")) in PROOF_WIDGET_TYPES for element, _, _ in flat)
        eligible = has_button and has_support
        reason = "CTA and nearby support widgets located." if eligible else "Required CTA/proof widgets were not found."
        verification = ["Updated CTA text is visible", "Proof/support copy is visible"]
    elif action_type == "add_internal_links":
        has_insertion = any(str(element.get("widgetType", "")) in TEXT_WIDGET_TYPES for element, _, _ in flat)
        eligible = has_insertion
        reason = "Approved insertion zone found." if eligible else "No text/html insertion zone found."
        verification = ["All inserted links resolve internally", "No duplicate anchors in the updated zone"]
    elif action_type == "update_faq_ai_extraction":
        eligible = True
        reason = "FAQ section can be replaced or appended deterministically."
        verification = ["FAQ heading is visible", "At least one generated question is visible", "No duplicate FAQ block"]
    elif action_type == "inject_faq_block":
        page_html = _elementor_html_snapshot(elements)
        has_duplicate = faq_exists(page_html)
        insertion_point = resolve_insertion_point(page_html)
        eligible = not has_duplicate and insertion_point.get("strategy") in {"after_first_major_section", "before_cta", "end_of_content"}
        reason = "Deterministic FAQ insertion point located." if eligible else "Existing FAQ found or no stable insertion point was resolved."
        verification = ["FAQ section exists after insert", "No duplicate FAQ block was created", "Generated FAQ question is visible"]
    elif action_type == "expand_service_page_section":
        insertion_point = resolve_insertion_point(_elementor_html_snapshot(elements))
        eligible = insertion_point.get("strategy") in {"after_first_major_section", "before_cta", "end_of_content"}
        reason = "Structured section insertion point located." if eligible else "No stable section insertion point was resolved."
        verification = ["New section heading is visible", "Section body renders under the inserted heading"]

    return {
        "eligible": eligible,
        "execution_eligibility": (
            "auto_execute"
            if eligible and action_type == "inject_faq_block"
            else "approval_required"
        ),
        "target_region": _infer_region_label(action_type),
        "reason": reason,
        "verification_requirements": verification,
    }


def update_primary_heading(elements: List[Dict[str, Any]], new_text: str = "") -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    first_heading: Optional[Dict[str, Any]] = None
    target_heading: Optional[Dict[str, Any]] = None
    demoted_widget_ids: List[str] = []
    for element in walk_elements(elements):
        if element.get("widgetType") != "heading":
            continue
        if first_heading is None:
            first_heading = element
        settings = element.get("settings") or {}
        if str(settings.get("header_size", "")).strip().lower() == "h1":
            if target_heading is None:
                target_heading = element
                continue
            demoted_settings = dict(settings)
            demoted_settings["header_size"] = "h2"
            element["settings"] = demoted_settings
            demoted_widget_ids.append(str(element.get("id", "")))
    target_heading = target_heading or first_heading
    if target_heading is None:
        raise ExecutionError("No Elementor heading widget found to update.")
    settings = dict(target_heading.get("settings") or {})
    before_text = str(settings.get("title", ""))
    before_size = str(settings.get("header_size", ""))
    after_text = new_text.strip() or before_text
    if not after_text:
        raise ExecutionError("No heading text found to promote as the primary H1.")
    settings["title"] = after_text
    settings["header_size"] = "h1"
    target_heading["settings"] = settings
    return elements, {
        "before_text": before_text,
        "after_text": after_text,
        "before_header_size": before_size,
        "after_header_size": "h1",
        "widget_id": str(target_heading.get("id", "")),
        "demoted_widget_ids": demoted_widget_ids,
    }


def update_title_and_intro(elements: List[Dict[str, Any]], payload: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    refs = _flatten_widget_refs(elements)
    heading_ref = next((ref for ref in refs if str(ref[0].get("widgetType", "")) == "heading"), None)
    text_ref = None
    if heading_ref:
        heading_index = refs.index(heading_ref)
        for ref in refs[heading_index + 1 :]:
            if str(ref[0].get("widgetType", "")) in TEXT_WIDGET_TYPES:
                text_ref = ref
                break
    if heading_ref is None or text_ref is None:
        raise ExecutionError("Could not locate a deterministic hero title and intro region.")
    heading, _, _ = heading_ref
    intro_widget, _, _ = text_ref
    heading_text = str(payload.get("heading", "") or payload.get("page_title", "") or "").strip()
    intro_html = str(payload.get("intro_html", "") or payload.get("intro", "") or "").strip()
    if not heading_text or not intro_html:
        raise ExecutionError("Title/intro payload is missing heading or intro content.")
    before_heading = _widget_text(heading)
    before_intro = _widget_text(intro_widget)
    _set_widget_text(heading, heading_text)
    _set_widget_text(intro_widget, intro_html)
    return elements, {
        "before_heading": before_heading,
        "after_heading": heading_text,
        "before_intro": before_intro,
        "after_intro": intro_html,
    }


def update_primary_cta(elements: List[Dict[str, Any]], payload: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    refs = _flatten_widget_refs(elements)
    button_ref = next((ref for ref in refs if str(ref[0].get("widgetType", "")) == "button"), None)
    if button_ref is None:
        raise ExecutionError("Could not locate a deterministic primary CTA button.")
    cta_widget, parent_list, button_index = button_ref
    proof_ref = None
    button_flat_index = refs.index(button_ref)
    for ref in refs[button_flat_index + 1 :]:
        if str(ref[0].get("widgetType", "")) in PROOF_WIDGET_TYPES:
            proof_ref = ref
            break
    cta_text = str(payload.get("cta_text", "") or "").strip()
    proof_html = str(payload.get("proof_html", "") or payload.get("proof_text", "") or "").strip()
    if not cta_text or not proof_html:
        raise ExecutionError("CTA payload is missing CTA or proof content.")
    before_cta = _widget_text(cta_widget)
    _set_widget_text(cta_widget, cta_text)
    created_proof = False
    if proof_ref is None:
        proof_widget = _make_text_widget(proof_html)
        parent_list.insert(button_index + 1, proof_widget)
        created_proof = True
        before_proof = ""
    else:
        proof_widget, _, _ = proof_ref
        before_proof = _widget_text(proof_widget)
        _set_widget_text(proof_widget, proof_html)
    return elements, {
        "before_cta": before_cta,
        "after_cta": cta_text,
        "before_proof": before_proof,
        "after_proof": proof_html,
        "created_proof_widget": created_proof,
    }


def update_internal_links(elements: List[Dict[str, Any]], payload: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    refs = _flatten_widget_refs(elements)
    insertion_ref = next((ref for ref in refs if str(ref[0].get("widgetType", "")) in TEXT_WIDGET_TYPES), None)
    if insertion_ref is None:
        raise ExecutionError("Could not locate a deterministic text block for internal link insertion.")
    links = payload.get("links") or []
    if not isinstance(links, list) or not links:
        raise ExecutionError("Internal link payload is missing approved links.")
    widget, _, _ = insertion_ref
    before_html = _widget_text(widget)
    existing_html = str(before_html or "")
    new_links = []
    for item in links[:3]:
        if not isinstance(item, dict):
            continue
        href = str(item.get("url", "")).strip()
        anchor = str(item.get("anchor", "")).strip()
        if not href or not anchor:
            continue
        if href in existing_html:
            continue
        new_links.append(f'<a href="{href}">{anchor}</a>')
    if not new_links:
        raise ExecutionError("No new internal links were eligible for insertion.")
    section_label = str(payload.get("section_label", "") or "Related services").strip()
    addition = f"<p><strong>{section_label}:</strong> " + ", ".join(new_links) + ".</p>"
    updated_html = (existing_html + "\n" + addition).strip() if existing_html else addition
    _set_widget_text(widget, updated_html)
    return elements, {
        "before_html": before_html,
        "after_html": updated_html,
        "inserted_links": [re.sub(r"<[^>]+>", "", item) for item in new_links],
    }


def update_faq_ai_block(elements: List[Dict[str, Any]], payload: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    refs = _flatten_widget_refs(elements)
    target_ref = None
    for ref in refs:
        widget_type = str(ref[0].get("widgetType", "")).strip()
        if widget_type not in FAQ_WIDGET_TYPES:
            continue
        if "faq" in _strip_html(_widget_text(ref[0])).lower() or "question" in _strip_html(_widget_text(ref[0])).lower():
            target_ref = ref
            break
    heading = str(payload.get("heading", "") or "Service FAQ").strip()
    definitions = [str(item).strip() for item in (payload.get("definitions") or []) if str(item).strip()]
    questions = payload.get("questions") or []
    citable = [str(item).strip() for item in (payload.get("citable_sentences") or []) if str(item).strip()]
    if not heading or not questions:
        raise ExecutionError("FAQ payload is missing required heading or questions.")
    blocks = [f"<h2>{heading}</h2>"]
    if definitions:
        blocks.append("<ul>" + "".join(f"<li>{item}</li>" for item in definitions[:3]) + "</ul>")
    for item in questions[:5]:
        if not isinstance(item, dict):
            continue
        question = str(item.get("question", "")).strip()
        answer = str(item.get("answer", "")).strip()
        if question and answer:
            blocks.append(f"<h3>{question}</h3><p>{answer}</p>")
    if citable:
        blocks.append("<p>" + " ".join(citable[:3]) + "</p>")
    faq_html = "".join(blocks)
    if target_ref is None:
        elements.append(_make_heading_widget(heading, level="h2"))
        elements.append(_make_text_widget(faq_html))
        return elements, {
            "before_html": "",
            "after_html": faq_html,
            "created_section": True,
        }
    widget, _, _ = target_ref
    before_html = _widget_text(widget)
    _set_widget_text(widget, faq_html)
    return elements, {
        "before_html": before_html,
        "after_html": faq_html,
        "created_section": False,
    }


def inject_faq_block(elements: List[Dict[str, Any]], payload: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    page_html = _elementor_html_snapshot(elements)
    before_count = _faq_marker_count(page_html)
    if faq_exists(page_html):
        raise ExecutionError("FAQ block already exists on the page.")
    insertion_point = resolve_insertion_point(page_html)
    insertion_index = _resolve_widget_insertion_index(elements, insertion_point)
    heading = _normalize_text(str(payload.get("heading", "") or "Service FAQ").strip())
    questions = payload.get("questions") or []
    if not heading or not isinstance(questions, list) or not questions:
        raise ExecutionError("FAQ payload is missing a heading or questions.")

    items: list[str] = []
    for item in questions[:5]:
        if not isinstance(item, dict):
            continue
        question = _sanitize_html_fragment(str(item.get("question", "")).strip())
        answer = _sanitize_html_fragment(str(item.get("answer", "")).strip())
        if question and answer:
            items.append(f'<div class="faq-item"><h3>{question}</h3><p>{answer}</p></div>')
    if not items:
        raise ExecutionError("FAQ payload did not contain any valid question-answer items.")
    faq_html = f'<section class="anata-faq"><h2>{html.escape(heading)}</h2>{"".join(items)}</section>'
    elements.insert(insertion_index, _make_text_widget(faq_html))
    return elements, {
        "before_faq_count": before_count,
        "after_faq_count": before_count + 1,
        "insertion_strategy": str(insertion_point.get("strategy", "end_of_content")),
        "faq_html": faq_html,
    }


def expand_service_page_section(elements: List[Dict[str, Any]], payload: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    heading = clean_generated_content(str(payload.get("heading", "")).strip())
    body_html = str(payload.get("body_html", "")).strip()
    if not heading or not body_html:
        raise ExecutionError("Section expansion payload is missing heading or body_html.")
    insertion_point = resolve_insertion_point(_elementor_html_snapshot(elements))
    insertion_index = _resolve_widget_insertion_index(elements, insertion_point)
    safe_paragraphs: list[str] = []
    for fragment in re.findall(r"<p>(.*?)</p>", body_html, flags=re.IGNORECASE | re.DOTALL):
        cleaned = clean_generated_content(fragment)
        if cleaned:
            safe_paragraphs.append(f"<p>{html.escape(cleaned)}</p>")
    if not safe_paragraphs:
        safe_paragraphs.append(f"<p>{html.escape(clean_generated_content(_strip_html(body_html)))}</p>")
    elements.insert(insertion_index, _make_heading_widget(heading, level="h2"))
    elements.insert(insertion_index + 1, _make_text_widget("".join(safe_paragraphs)))
    return elements, {
        "heading": heading,
        "body_html": "".join(safe_paragraphs),
        "insertion_strategy": str(insertion_point.get("strategy", "end_of_content")),
    }


def backup_page_record(record: Mapping[str, Any], *, timestamp: datetime) -> Path:
    run_dir = backup_root() / f"{timestamp.date().isoformat()}-approved-actions"
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / f"page-{record.get('id')}.json"
    path.write_text(json.dumps(record, indent=2, sort_keys=True))
    return path


def execute_feedback_action(
    feedback: Mapping[str, Any],
    *,
    config: Optional[WebsiteOpsConfig] = None,
    timestamp: Optional[datetime] = None,
) -> Dict[str, Any]:
    config = config or load_config()
    timestamp = timestamp or datetime.now(timezone.utc)
    action_type = str(feedback.get("action_type", "")).strip()
    if action_type not in SUPPORTED_ACTION_TYPES:
        raise ExecutionError(f"Unsupported action_type: {action_type or 'missing'}")
    action_payload = _parse_action_payload(feedback)
    action_value = str(feedback.get("action_value", "")).strip()

    record = resolve_page_record(feedback)
    backup_path = backup_page_record(record, timestamp=timestamp)
    elementor_data = parse_elementor_data(record)
    payload: Dict[str, Any] = {"meta": {"_elementor_data": json.dumps(elementor_data)}}

    if action_type == "replace_primary_heading":
        updated_data, change_summary = update_primary_heading(elementor_data, action_value)
        payload["meta"]["_elementor_data"] = json.dumps(updated_data)
    elif action_type == "rewrite_title_and_intro":
        updated_data, change_summary = update_title_and_intro(elementor_data, action_payload)
        payload["title"] = action_payload.get("page_title") or action_payload.get("heading") or record.get("title", {}).get("raw", "")
        payload["meta"]["_elementor_data"] = json.dumps(updated_data)
    elif action_type == "strengthen_primary_cta":
        updated_data, change_summary = update_primary_cta(elementor_data, action_payload)
        payload["meta"]["_elementor_data"] = json.dumps(updated_data)
    elif action_type == "add_internal_links":
        updated_data, change_summary = update_internal_links(elementor_data, action_payload)
        payload["meta"]["_elementor_data"] = json.dumps(updated_data)
    elif action_type == "inject_faq_block":
        updated_data, change_summary = inject_faq_block(elementor_data, action_payload)
        payload["meta"]["_elementor_data"] = json.dumps(updated_data)
    elif action_type == "expand_service_page_section":
        updated_data, change_summary = expand_service_page_section(elementor_data, action_payload)
        payload["meta"]["_elementor_data"] = json.dumps(updated_data)
    else:
        updated_data, change_summary = update_faq_ai_block(elementor_data, action_payload)
        payload["meta"]["_elementor_data"] = json.dumps(updated_data)

    updated_record = wp_request(f"/wp-json/wp/v2/pages/{record['id']}", method="POST", payload=payload)
    verification = collect_page_observation(str(feedback.get("page_url") or updated_record.get("link")), config=config)
    live_h1s = [str(value).strip() for value in (verification.get("h1") or []) if str(value).strip()]
    page_url = str(feedback.get("page_url") or updated_record.get("link") or "")
    live_html = _fetch_live_html(page_url, config=config)
    if action_type == "replace_primary_heading":
        if not live_h1s:
            raise ExecutionError("Verification failed. No live H1 found after execution.")
        if len(live_h1s) != 1:
            raise ExecutionError(f"Verification failed. Expected exactly one H1 but found {len(live_h1s)}.")
        if action_value and live_h1s[0] != action_value.strip():
            raise ExecutionError(f"Verification failed. Expected H1 '{action_value}' but found '{live_h1s[0]}'.")
    elif action_type == "rewrite_title_and_intro":
        expected_heading = str(action_payload.get("heading", "") or "").strip()
        expected_intro = str(action_payload.get("intro", "") or action_payload.get("intro_html", "") or "").strip()
        expected_title = str(action_payload.get("page_title", "") or expected_heading).strip()
        if len(live_h1s) != 1:
            raise ExecutionError("Verification failed. Expected exactly one live H1 after title/intro update.")
        if expected_heading and live_h1s and live_h1s[0] != expected_heading:
            raise ExecutionError("Verification failed. Live H1 did not match the rewritten title.")
        if expected_title and _normalize_text(expected_title).lower() not in _normalize_text(verification.get("title", "")).lower():
            raise ExecutionError("Verification failed. Live page title did not include the rewritten SERP title.")
        if expected_intro and not _verify_text_present(live_html, expected_intro):
            raise ExecutionError("Verification failed. Rewritten intro text is not visible on the live page.")
    elif action_type == "strengthen_primary_cta":
        if not _verify_text_present(live_html, str(action_payload.get("cta_text", "") or "")):
            raise ExecutionError("Verification failed. Updated CTA text is not visible on the live page.")
        if not _verify_text_present(live_html, str(action_payload.get("proof_text", "") or action_payload.get("proof_html", "") or "")):
            raise ExecutionError("Verification failed. Updated proof block is not visible on the live page.")
    elif action_type == "add_internal_links":
        for link in action_payload.get("links") or []:
            href = str(link.get("url", "")).strip() if isinstance(link, dict) else ""
            if href and href not in live_html:
                raise ExecutionError(f"Verification failed. Expected internal link '{href}' was not found.")
    elif action_type == "update_faq_ai_extraction":
        first_question = ""
        questions = action_payload.get("questions") or []
        if questions and isinstance(questions[0], dict):
            first_question = str(questions[0].get("question", "")).strip()
        if not _verify_text_present(live_html, str(action_payload.get("heading", "") or "Service FAQ")):
            raise ExecutionError("Verification failed. FAQ heading is not visible on the live page.")
        if first_question and not _verify_text_present(live_html, first_question):
            raise ExecutionError("Verification failed. Generated FAQ question is not visible on the live page.")
    elif action_type == "inject_faq_block":
        first_question = ""
        questions = action_payload.get("questions") or []
        if questions and isinstance(questions[0], dict):
            first_question = str(questions[0].get("question", "")).strip()
        if not faq_exists(live_html):
            raise ExecutionError("Verification failed. FAQ section was not detected on the live page.")
        before_count = int(change_summary.get("before_faq_count", 0) or 0)
        if _faq_marker_count(live_html) > before_count + 1:
            raise ExecutionError("Verification failed. Duplicate FAQ markers were created.")
        if not _verify_text_present(live_html, str(action_payload.get("heading", "") or "Service FAQ")):
            raise ExecutionError("Verification failed. Inserted FAQ heading is not visible on the live page.")
        if first_question and not _verify_text_present(live_html, first_question):
            raise ExecutionError("Verification failed. Inserted FAQ question is not visible on the live page.")
    elif action_type == "expand_service_page_section":
        expected_heading = str(action_payload.get("heading", "")).strip()
        expected_body = _strip_html(str(action_payload.get("body_html", "")).strip())
        if expected_heading and not _verify_text_present(live_html, expected_heading):
            raise ExecutionError("Verification failed. Inserted section heading is not visible on the live page.")
        if expected_body and not _verify_text_present(live_html, expected_body.split(".")[0]):
            raise ExecutionError("Verification failed. Inserted section body is not visible on the live page.")
    return {
        "feedback_id": feedback.get("feedback_id"),
        "action_type": action_type,
        "page_url": page_url,
        "target_post_id": updated_record.get("id"),
        "backup_path": str(backup_path),
        "executed_at": timestamp.isoformat(),
        "verification_status": "verified",
        "summary": change_summary,
    }


def execute_approved_feedback(
    feedback_entries: Sequence[Mapping[str, Any]],
    *,
    config: Optional[WebsiteOpsConfig] = None,
) -> List[Dict[str, Any]]:
    config = config or load_config()
    results: List[Dict[str, Any]] = []
    for feedback in feedback_entries:
        status = str(feedback.get("status", "")).strip().lower()
        if status != "approved":
            continue
        if not str(feedback.get("action_type", "")).strip():
            continue
        results.append(execute_feedback_action(feedback, config=config))
    return results
