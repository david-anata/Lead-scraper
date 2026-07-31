"""Deterministic anti-slop and contextual evidence gates for generated articles."""

from __future__ import annotations

from typing import Any, Mapping, Sequence
from urllib.parse import urlparse


def contextual_evidence_errors(
    *,
    sections: Sequence[Mapping[str, Any]],
    sources: Sequence[Mapping[str, Any]],
) -> list[str]:
    source_urls = {
        str(item.get("url", "")).strip()
        for item in sources
        if isinstance(item, Mapping)
        and urlparse(str(item.get("url", "")).strip()).scheme == "https"
    }
    cited_urls: set[str] = set()
    internal_hrefs: set[str] = set()
    sections_with_citations = 0
    sections_with_internal_links = 0
    errors: list[str] = []

    for section in sections:
        citations = section.get("citations")
        if citations:
            if not isinstance(citations, list):
                errors.append("Section citations must be a list.")
            else:
                sections_with_citations += 1
                for citation in citations:
                    if not isinstance(citation, Mapping):
                        errors.append("A section citation is invalid.")
                        continue
                    href = str(citation.get("href", "")).strip()
                    title = str(citation.get("title", "")).strip()
                    if href not in source_urls or not title:
                        errors.append(
                            "Every section citation must match a titled top-level source."
                        )
                    else:
                        cited_urls.add(href)

        internal_links = section.get("internalLinks")
        if internal_links:
            if not isinstance(internal_links, list):
                errors.append("Contextual internal links must be a list.")
            else:
                sections_with_internal_links += 1
                for link in internal_links:
                    if not isinstance(link, Mapping):
                        errors.append("A contextual internal link is invalid.")
                        continue
                    href = str(link.get("href", "")).strip()
                    title = str(link.get("title", "")).strip()
                    if not title or not href.startswith("/") or href.startswith("//"):
                        errors.append(
                            "Every contextual internal link needs a title and scoped route."
                        )
                    else:
                        internal_hrefs.add(href)

    if sections_with_citations < 2 or len(cited_urls) < 2:
        errors.append(
            "Contextual citations must appear in at least two sections and reference two sources."
        )
    if sections_with_internal_links < 2 or len(internal_hrefs) < 2:
        errors.append(
            "Contextual internal links must appear in at least two sections and use two routes."
        )
    return list(dict.fromkeys(errors))
