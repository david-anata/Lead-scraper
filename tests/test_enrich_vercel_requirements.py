"""Enrich's imports must be present in the dependency set Vercel installs.

Vercel builds this service from pyproject.toml locked by uv.lock. That is the
packaging added for deterministic Vercel installs. requirements.txt is what
Render installs, and it already listed tldextract, which is why only Vercel
crashed with ModuleNotFoundError.
"""

from __future__ import annotations

import ast
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
STDLIB = set(sys.stdlib_module_names)
LOCAL_TOP = {path.stem for path in ROOT.glob("*.py")}
LOCAL_TOP.add("sales_support_agent")

# Import name -> distribution name when they differ.
IMPORT_DIST = {
    "pil": "pillow",
    "yaml": "pyyaml",
    "dotenv": "python-dotenv",
    "jwt": "pyjwt",
    "bs4": "beautifulsoup4",
    "cv2": "opencv-python",
    "dateutil": "python-dateutil",
}


def _pyproject_dists() -> set[str]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    names = set()
    for raw in data["project"]["dependencies"]:
        name = raw.split(">=")[0].split("==")[0].split("<")[0].split("[")[0].strip()
        names.add(name.lower().replace("_", "-"))
    return names


def _lock_packages() -> set[str]:
    names = set()
    for line in (ROOT / "uv.lock").read_text(encoding="utf-8").splitlines():
        if line.startswith("name = "):
            names.add(line.split("=", 1)[1].strip().strip('"').lower())
    return names


def _module_path(module: str) -> Path | None:
    parts = module.split(".")
    if parts[0] == "sales_support_agent":
        base = ROOT.joinpath(*parts)
    elif parts[0] in LOCAL_TOP and len(parts) == 1:
        candidate = ROOT / f"{parts[0]}.py"
        return candidate if candidate.exists() else None
    else:
        return None
    if base.with_suffix(".py").exists():
        return base.with_suffix(".py")
    init = base / "__init__.py"
    return init if init.exists() else None


def _enrich_third_party_imports() -> set[str]:
    """Top-level third-party modules reachable from the enrich job, including lazy imports."""

    queue = [ROOT / "sales_support_agent/api/outbound_enrich_job.py"]
    seen: set[Path] = set()
    third: set[str] = set()

    def consider(name: str, level: int, current: Path) -> None:
        if level:
            relative = current.relative_to(ROOT)
            package = list(relative.parts[:-1])
            if level > 1:
                package = package[: 1 - level]
            full = ".".join([*package, name] if name else package)
            path = _module_path(full)
            if path:
                queue.append(path)
            return
        top = name.split(".", 1)[0]
        if top in STDLIB:
            return
        if top in LOCAL_TOP or top == "sales_support_agent":
            path = _module_path(name if top == "sales_support_agent" else top)
            if path:
                queue.append(path)
            return
        third.add(top)

    while queue:
        path = queue.pop().resolve()
        if path in seen or not path.exists():
            continue
        seen.add(path)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    consider(alias.name, 0, path)
            elif isinstance(node, ast.ImportFrom) and (node.module or node.level):
                consider(node.module or "", node.level or 0, path)
    return third


def test_vercel_lock_covers_every_enrich_third_party_import():
    imported = _enrich_third_party_imports()
    direct = _pyproject_dists()
    locked = _lock_packages()
    assert "tldextract" in imported
    assert "tldextract" in direct
    assert "tldextract" in locked
    # Render installs requirements.txt. Keep the same pin there.
    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8").lower()
    assert "tldextract" in requirements
    missing = []
    for module in sorted(imported):
        dist = IMPORT_DIST.get(module, module.replace("_", "-"))
        if dist not in locked and dist not in direct:
            missing.append(module)
    assert missing == []


def test_enrich_modules_import_and_suffix_extract_stays_offline(monkeypatch):
    def refuse_network(*_args, **_kwargs):
        raise AssertionError("tldextract tried to use the network")

    monkeypatch.setattr("requests.sessions.Session.request", refuse_network)
    monkeypatch.setattr("requests.request", refuse_network)

    import outbound_contacts
    import outbound_enrich
    from sales_support_agent.api import outbound_enrich_job

    assert outbound_enrich.run_enrichment
    assert outbound_enrich_job.run_enrich_job
    extractor = outbound_contacts._EXTRACT
    assert extractor.suffix_list_urls == ()
    assert extractor.fallback_to_snapshot is True
    assert extractor._cache.enabled is False

    parsed = extractor("https://ca.petlibro.com/shop")
    assert parsed.top_domain_under_public_suffix == "petlibro.com"


@pytest.mark.parametrize("module_name", ["outbound_contacts", "outbound_enrich"])
def test_named_enrich_modules_import(module_name: str):
    __import__(module_name)
