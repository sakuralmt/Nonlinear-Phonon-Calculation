from __future__ import annotations

from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parent
SOURCE_BUNDLE_ROOT = PACKAGE_ROOT.parent
RESOURCE_ROOT = PACKAGE_ROOT / "resources"


def bundle_path(relative_path: str) -> Path:
    relative = Path(relative_path)
    source_candidate = SOURCE_BUNDLE_ROOT / relative
    if source_candidate.exists():
        return source_candidate
    packaged_candidate = RESOURCE_ROOT / relative
    if packaged_candidate.exists():
        return packaged_candidate
    raise FileNotFoundError(f"Could not resolve bundled resource: {relative_path}")
