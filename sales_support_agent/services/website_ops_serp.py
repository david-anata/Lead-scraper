"""SERP intelligence helpers for Website Ops MVP."""

from __future__ import annotations

import html
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import requests


SEARCH_ENDPOINT = "https://html.duckduckgo.com/html/"
QUESTION_STARTERS = ("what", "how", "when", "why", "where", "who", "can", "does", "do", "is", "are")
STOP_WORDS = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "from",
    "your",
    "into",
    "about",
    "have",
    "will",
    "more",
    "than",
    "when",
    "what",
    "which",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_text(value: str) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_question(value: str) -> str:
    text = _normalize_text(value).rstrip("?.!").lower()
    return text


def _extract_result_url(raw_href: str) -> str:
    parsed = urlparse(raw_href)
    if parsed.netloc and parsed.scheme in {"http", "https"}:
        return raw_href
    query = parse_qs(parsed.query)
    for key in ("uddg", "u"):
        if key in query and query[key]:
            return unquote(query[key][0])
    return ""


class _DuckDuckGoResultParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.urls: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        data = dict(attrs)
        class_name = str(data.get("class") or "")
        if "result__a" not in class_name:
            return
        href = _extract_result_url(str(data.get("href") or ""))
        if href and href not in self.urls:
            self.urls.append(href)


class _PageStructureParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._current_tag = ""
        self._buffer: list[str] = []
        self.headings: list[dict[str, str]] = []
        self.questions: list[str] = []
        self.paragraphs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"h1", "h2", "h3", "p", "summary"}:
            self._current_tag = tag
            self._buffer = []

    def handle_endtag(self, tag: str) -> None:
        if tag != self._current_tag:
            return
        text = _normalize_text("".join(self._buffer))
        self._current_tag = ""
        self._buffer = []
        if not text:
            return
        if tag in {"h1", "h2", "h3"}:
            self.headings.append({"level": tag, "text": text})
        elif tag == "p":
            self.paragraphs.append(text)
        elif tag == "summary":
            self.questions.append(text)
        if text.endswith("?") or text.lower().startswith(QUESTION_STARTERS):
            self.questions.append(text.rstrip("?") + "?")

    def handle_data(self, data: str) -> None:
        if self._current_tag:
            self._buffer.append(data)


def _fetch_html(url: str) -> str:
    response = requests.get(url, headers={"User-Agent": "Anata Website Ops SERP Bot/1.0"}, timeout=20)
    response.raise_for_status()
    return response.text


def get_top_serp_urls(query: str, *, max_results: int = 5) -> list[str]:
    normalized_query = _normalize_text(query)
    if not normalized_query:
        return []
    response = requests.post(
        SEARCH_ENDPOINT,
        data={"q": normalized_query},
        headers={"User-Agent": "Anata Website Ops SERP Bot/1.0"},
        timeout=20,
    )
    response.raise_for_status()
    parser = _DuckDuckGoResultParser()
    parser.feed(response.text)
    results: list[str] = []
    for url in parser.urls:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        if url not in results:
            results.append(url)
        if len(results) >= max_results:
            break
    return results


def extract_structured_headings(url: str) -> dict[str, Any]:
    html_text = _fetch_html(url)
    parser = _PageStructureParser()
    parser.feed(html_text)
    return {
        "url": url,
        "headings": parser.headings,
        "questions": list(dict.fromkeys(parser.questions)),
        "paragraphs": parser.paragraphs[:12],
        "content_length": len(_normalize_text(re.sub(r"<[^>]+>", " ", html_text))),
    }


def extract_repeated_faqs(urls: list[str]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for url in urls:
        try:
            page = extract_structured_headings(url)
        except Exception:
            continue
        seen_on_page = {_normalize_question(question) for question in page.get("questions", []) if question}
        for question in seen_on_page:
            if question:
                counts[question] += 1
    results = []
    for question, count in counts.items():
        if count >= 2:
            results.append({"question": question.rstrip("?").capitalize() + "?", "support_count": count})
    return sorted(results, key=lambda item: (-int(item["support_count"]), item["question"]))


def _repeated_headings(pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str]] = Counter()
    for page in pages:
        seen = {(item["level"], _normalize_text(item["text"]).lower()) for item in page.get("headings", []) if item.get("text")}
        for key in seen:
            counts[key] += 1
    results = []
    for (level, text), count in counts.items():
        if count >= 2:
            results.append({"heading": text.title(), "level": level, "support_count": count})
    return sorted(results, key=lambda item: (-int(item["support_count"]), item["heading"]))


def _topical_entities(pages: list[dict[str, Any]]) -> list[str]:
    token_counts: Counter[str] = Counter()
    for page in pages:
        words = re.findall(r"[A-Za-z][A-Za-z0-9-]{2,}", " ".join(item.get("text", "") for item in page.get("headings", [])))
        for word in words:
            normalized = word.lower()
            if normalized in STOP_WORDS:
                continue
            token_counts[normalized] += 1
    return [word.replace("-", " ") for word, count in token_counts.most_common(8) if count >= 2]


def build_blueprint(query: str) -> dict[str, Any]:
    urls = get_top_serp_urls(query)
    pages: list[dict[str, Any]] = []
    for url in urls:
        try:
            pages.append(extract_structured_headings(url))
        except Exception:
            continue
    faq_patterns = extract_repeated_faqs(urls)
    heading_structure = _repeated_headings(pages)
    gaps: list[str] = []
    if faq_patterns:
        gaps.append("Repeated FAQ patterns suggest a dedicated FAQ block is required.")
    if not any(item.get("heading", "").lower().startswith("what ") for item in heading_structure):
        gaps.append("SERP leaders frequently open with a direct definition block.")
    return {
        "blueprint_id": f"bp_{re.sub(r'[^a-z0-9]+', '-', query.lower()).strip('-')}_{datetime.now(timezone.utc).date().isoformat()}",
        "query": query,
        "created_at": _now_iso(),
        "source_urls": urls,
        "topical_entities": _topical_entities(pages),
        "heading_structure": heading_structure,
        "faq_patterns": faq_patterns,
        "content_gaps": gaps,
    }
