"""Brand asset and stylesheet path resolution + cached loading.

Stylesheet and PNG assets are cached at module level (`functools.lru_cache`)
so a deck-generation call doesn't re-read the same file from disk every time
`_render_html_deck` runs.
"""

from __future__ import annotations

import base64
import mimetypes
from functools import lru_cache
from pathlib import Path

from sales_support_agent.config import Settings


_DEFAULT_STYLESHEET_FALLBACK = (
    "body{font-family:Arial,sans-serif;background:#fff;color:#172033;}"
)


def _candidate_brand_paths(settings: Settings, relative_path: str) -> list[Path]:
    """Return the ordered list of paths to check for a relative brand-package file."""
    configured_root = Path(str(getattr(settings, "shared_brand_package_path", "") or "")).expanduser()
    repo_root = Path(__file__).resolve().parents[3]
    candidates: list[Path] = []
    if str(configured_root):
        candidates.append(configured_root / relative_path)
    candidates.append(repo_root / "shared" / "anata_brand" / relative_path)
    return candidates


def _candidate_brand_asset_paths(settings: Settings, relative_path: str) -> list[Path]:
    """Asset variants — same logic but returns sibling extensions and a couple of legacy filenames."""
    candidates: list[Path] = []
    seen: set[str] = set()
    for base_path in _candidate_brand_paths(settings, relative_path):
        stem_path = base_path.with_suffix("")
        prioritized: list[Path] = []
        normalized = str(relative_path).replace("\\", "/")
        if normalized.endswith("assets/monogram.png"):
            prioritized.append(base_path.with_name("1.png"))
        if normalized.endswith("assets/wordmark.png"):
            prioritized.append(base_path.with_name("anata wordmark logo - black.png"))
        for candidate in prioritized:
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate)
        for suffix in (".png", ".svg", ".webp", ".jpg", ".jpeg"):
            candidate = stem_path.with_suffix(suffix)
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate)
    return candidates


@lru_cache(maxsize=8)
def _read_text_cached(path_str: str) -> str:
    """Read a text file once, cache by absolute path string."""
    return Path(path_str).read_text(encoding="utf-8")


@lru_cache(maxsize=32)
def _encode_asset_cached(path_str: str) -> str:
    """Encode a binary asset (PNG/JPG/SVG/WEBP) into the inline `<img>` form once."""
    path = Path(path_str)
    if path.suffix.lower() == ".svg":
        return path.read_text(encoding="utf-8")
    mime_type = mimetypes.guess_type(path_str)[0] or "application/octet-stream"
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"<img src='data:{mime_type};base64,{encoded}' alt='Anata brand asset' />"


def load_brand_stylesheet(settings: Settings) -> str:
    """Read `style.css` from the configured brand package, falling back to the
    repo copy and finally to a tiny inline stylesheet. Result is cached by
    resolved path."""
    for path in _candidate_brand_paths(settings, "style.css"):
        if path.exists():
            return _read_text_cached(str(path))
    return _DEFAULT_STYLESHEET_FALLBACK


def load_brand_asset(settings: Settings, relative_path: str) -> str:
    """Return the inline-`<img>` (or raw SVG) for a relative brand asset path.
    Returns an empty string if no candidate exists. Cached by resolved path."""
    for path in _candidate_brand_asset_paths(settings, relative_path):
        if path.exists():
            return _encode_asset_cached(str(path))
    return ""


def clear_caches() -> None:
    """Test/dev helper: drop the path-keyed caches. Production never calls this."""
    _read_text_cached.cache_clear()
    _encode_asset_cached.cache_clear()
