"""Safe Screaming Frog ZIP ingestion for the Website Ops evidence inventory."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse


MAX_UPLOAD_BYTES = 10 * 1024 * 1024
MAX_ARCHIVE_BYTES = 50 * 1024 * 1024
MAX_ARCHIVE_FILES = 1_000
PRODUCTION_HOSTS = {"anatainc.com", "www.anatainc.com"}

PRIMARY_REPORTS = {
    "internal_all.csv",
    "response_codes_all.csv",
    "canonicals_all.csv",
    "directives_all.csv",
    "sitemaps_all.csv",
    "page_titles_all.csv",
    "meta_description_all.csv",
    "h1_all.csv",
    "h2_all.csv",
    "content_all.csv",
    "images_all.csv",
    "links_all.csv",
    "pagination_all.csv",
    "structured_data_all.csv",
    "pagespeed_all.csv",
    "search_console_all.csv",
}

ISSUE_REPORT_HINTS = (
    "error",
    "redirect_chain",
    "redirect_loop",
    "redirects_to_error",
    "duplicate",
    "orphan",
    "missing_alt",
    "missing_size",
    "blocked",
    "nonindexable",
    "noindex",
    "soft_404",
    "javascript_issues",
    "layout_shift",
    "lcp_",
    "content_low",
    "pages_without_internal",
    "client_error",
    "server_error",
    "no_response",
    "uncrawlable",
)


class ScreamingFrogImportError(ValueError):
    """Raised when an upload cannot be safely treated as crawl evidence."""


def _column(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _clean_url(value: Any) -> str:
    raw = str(value or "").strip()
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return ""
    path = parsed.path or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}{query}"


def _environment(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    if host in PRODUCTION_HOSTS:
        return "production"
    if host.endswith(".vercel.app"):
        return "sandbox"
    return "external"


def _first(row: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = row.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _integer(value: Any) -> int | None:
    raw = str(value or "").strip().replace(",", "")
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _read_csv(content: bytes) -> list[dict[str, str]]:
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return [
        {_column(key): str(value or "").strip() for key, value in row.items()}
        for row in reader
    ]


def _row_urls(row: Mapping[str, Any]) -> list[str]:
    preferred = (
        "address",
        "url",
        "page",
        "source",
        "source_url",
        "destination",
        "destination_url",
        "canonical_link_element_1",
    )
    urls: list[str] = []
    for key in preferred:
        url = _clean_url(row.get(key))
        if url and url not in urls:
            urls.append(url)
    return urls


def _base_record(url: str) -> dict[str, Any]:
    return {
        "url": url,
        "environment": _environment(url),
        "status_code": None,
        "indexability": "",
        "indexability_status": "",
        "canonical": "",
        "title": "",
        "meta_description": "",
        "h1": "",
        "h2": "",
        "word_count": None,
        "crawl_depth": None,
        "inlinks": None,
        "in_sitemap": "",
        "robots": "",
        "warnings": [],
        "source_reports": [],
    }


def _is_issue_report(name: str) -> bool:
    lowered = name.lower()
    if Path(lowered).name in PRIMARY_REPORTS:
        return False
    return (
        "/issues_reports/" in lowered
        or any(hint in Path(lowered).name for hint in ISSUE_REPORT_HINTS)
    )


def _merge_row(record: dict[str, Any], row: Mapping[str, Any], report: str) -> None:
    basename = Path(report).name.lower()
    record["source_reports"] = sorted(set([*record["source_reports"], basename]))
    scalar_fields = {
        "status_code": _integer(_first(row, "status_code", "status")),
        "indexability": _first(row, "indexability"),
        "indexability_status": _first(row, "indexability_status"),
        "canonical": _clean_url(
            _first(
                row,
                "canonical_link_element_1",
                "canonical",
                "canonical_url",
                "canonical_url_1",
            )
        ),
        "title": _first(row, "title_1", "title"),
        "meta_description": _first(
            row, "meta_description_1", "meta_description", "description"
        ),
        "h1": _first(row, "h1_1", "h1"),
        "h2": _first(row, "h2_1", "h2"),
        "word_count": _integer(_first(row, "word_count")),
        "crawl_depth": _integer(_first(row, "crawl_depth")),
        "inlinks": _integer(_first(row, "inlinks", "unique_inlinks")),
        "in_sitemap": _first(row, "in_sitemap"),
        "robots": _first(row, "meta_robots_1", "x_robots_tag_1", "robots"),
    }
    for key, value in scalar_fields.items():
        if value not in (None, ""):
            record[key] = value
    if not _is_issue_report(report):
        return
    evidence = (
        _first(row, "issue", "issue_name", "status", "status_code", "indexability_status")
        or "URL appears in this Screaming Frog issue export."
    )
    warning = {"report": basename, "evidence": evidence}
    if warning not in record["warnings"]:
        record["warnings"].append(warning)


def _load_existing(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _archive_entries(payload: bytes) -> Iterable[tuple[str, bytes]]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile as exc:
        raise ScreamingFrogImportError("The uploaded file is not a valid ZIP archive.") from exc
    with archive:
        entries = [entry for entry in archive.infolist() if not entry.is_dir()]
        if len(entries) > MAX_ARCHIVE_FILES:
            raise ScreamingFrogImportError("The archive contains too many files.")
        if sum(entry.file_size for entry in entries) > MAX_ARCHIVE_BYTES:
            raise ScreamingFrogImportError("The expanded archive is too large.")
        for entry in entries:
            parts = Path(entry.filename).parts
            if Path(entry.filename).is_absolute() or ".." in parts:
                raise ScreamingFrogImportError("The archive contains an unsafe path.")
            if not entry.filename.lower().endswith(".csv"):
                continue
            yield entry.filename.replace("\\", "/"), archive.read(entry)


def import_screaming_frog_zip(
    *,
    filename: str,
    payload: bytes,
    root: Path,
) -> dict[str, Any]:
    """Merge one ZIP into the durable crawl inventory without trusting warnings."""

    if not filename.lower().endswith(".zip"):
        raise ScreamingFrogImportError("Upload a Screaming Frog ZIP export.")
    if not payload:
        raise ScreamingFrogImportError("The uploaded archive is empty.")
    if len(payload) > MAX_UPLOAD_BYTES:
        raise ScreamingFrogImportError("The uploaded archive exceeds the 10 MB limit.")

    inventory_dir = root / "indexing"
    inventory_dir.mkdir(parents=True, exist_ok=True)
    inventory_path = inventory_dir / "crawl_inventory.json"
    existing = _load_existing(inventory_path)
    records_by_url = {
        str(item.get("url")): dict(item)
        for item in list(existing.get("records") or [])
        if isinstance(item, Mapping) and item.get("url")
    }

    report_names: list[str] = []
    row_count = 0
    recognized_count = 0
    for report, content in _archive_entries(payload):
        basename = Path(report).name.lower()
        recognized = (
            basename.endswith("_all.csv")
            or basename in PRIMARY_REPORTS
            or _is_issue_report(report)
        )
        if not recognized:
            continue
        recognized_count += 1
        report_names.append(report)
        for row in _read_csv(content):
            urls = _row_urls(row)
            if not urls:
                continue
            row_count += 1
            for url in urls:
                record = records_by_url.setdefault(url, _base_record(url))
                _merge_row(record, row, report)
    if not recognized_count:
        raise ScreamingFrogImportError(
            "No recognized Screaming Frog CSV reports were found in the archive."
        )

    digest = hashlib.sha256(payload).hexdigest()
    imported_at = datetime.now(timezone.utc).isoformat()
    imports = [
        dict(item)
        for item in list(existing.get("imports") or [])
        if isinstance(item, Mapping) and item.get("sha256") != digest
    ]
    imports.append(
        {
            "filename": Path(filename).name,
            "sha256": digest,
            "imported_at": imported_at,
            "recognized_reports": recognized_count,
            "evidence_rows": row_count,
        }
    )
    records = sorted(
        records_by_url.values(),
        key=lambda item: (
            {"production": 0, "sandbox": 1, "external": 2}.get(
                str(item.get("environment")), 3
            ),
            str(item.get("url")),
        ),
    )
    warning_counts: dict[str, int] = {}
    for record in records:
        record["warnings"] = sorted(
            list(record.get("warnings") or []),
            key=lambda item: (str(item.get("report")), str(item.get("evidence"))),
        )
        for warning in record["warnings"]:
            report = str(warning.get("report") or "unspecified")
            warning_counts[report] = warning_counts.get(report, 0) + 1
    summary = {
        "known_urls": len(records),
        "production_urls": sum(
            1 for item in records if item.get("environment") == "production"
        ),
        "sandbox_urls": sum(
            1 for item in records if item.get("environment") == "sandbox"
        ),
        "external_urls": sum(
            1 for item in records if item.get("environment") == "external"
        ),
        "urls_with_warnings": sum(1 for item in records if item.get("warnings")),
        "warning_counts": dict(
            sorted(warning_counts.items(), key=lambda item: (-item[1], item[0]))
        ),
    }
    inventory = {
        "generated_at": imported_at,
        "imports": imports,
        "records": records,
        "summary": summary,
        "policy": (
            "Crawler warnings are unverified evidence. Production changes require "
            "rendered-page, repository, intent, and outcome validation."
        ),
    }
    inventory_path.write_text(
        json.dumps(inventory, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    imports_dir = inventory_dir / "imports"
    imports_dir.mkdir(parents=True, exist_ok=True)
    (imports_dir / f"{digest}.zip").write_bytes(payload)
    return inventory


def load_crawl_inventory(root: Path) -> dict[str, Any]:
    path = root / "indexing" / "crawl_inventory.json"
    payload = _load_existing(path)
    return payload or {
        "generated_at": "",
        "imports": [],
        "records": [],
        "summary": {
            "known_urls": 0,
            "production_urls": 0,
            "sandbox_urls": 0,
            "external_urls": 0,
            "urls_with_warnings": 0,
            "warning_counts": {},
        },
    }
