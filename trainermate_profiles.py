import os
from pathlib import Path

import trainermate_identity as identity_helpers


PROFILED_FILENAMES = {
    "app_state": "app_state.json",
    "providers": "providers.json",
    "courses_db": "courses.db",
    "zoom_accounts": "zoom_accounts.json",
    "alert_ack": "dashboard_alerts_ack.json",
    "course_removal_confirm": "course_removal_confirmed.json",
    "provider_certificate_manifest": "provider_certificate_cache_manifest.json",
    "automation_settings": "automation_settings.json",
    "access_cache": "access_cache.json",
    "bot_log": "bot_debug.log",
}


def profile_slug_for_ndors(ndors: str | None) -> str:
    return identity_helpers.profile_slug_for_ndors(ndors)


def profile_root(base_dir: Path) -> Path:
    return Path(base_dir) / "trainer_profiles"


def profile_dir(base_dir: Path, *, ndors: str | None = None, slug: str | None = None) -> Path:
    profile_slug = (slug or profile_slug_for_ndors(ndors)).strip() or "signed-out"
    root = profile_root(Path(base_dir)) / profile_slug
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return root


def profile_paths(base_dir: Path, *, ndors: str | None = None, slug: str | None = None) -> dict:
    root = profile_dir(base_dir, ndors=ndors, slug=slug)
    return {
        "root": root,
        "app_state": root / PROFILED_FILENAMES["app_state"],
        "providers": root / PROFILED_FILENAMES["providers"],
        "courses_db": root / PROFILED_FILENAMES["courses_db"],
        "zoom_accounts": root / PROFILED_FILENAMES["zoom_accounts"],
        "alert_ack": root / PROFILED_FILENAMES["alert_ack"],
        "course_removal_confirm": root / PROFILED_FILENAMES["course_removal_confirm"],
        "provider_certificate_manifest": root / PROFILED_FILENAMES["provider_certificate_manifest"],
        "automation_settings": root / PROFILED_FILENAMES["automation_settings"],
        "access_cache": root / PROFILED_FILENAMES["access_cache"],
        "bot_log": root / PROFILED_FILENAMES["bot_log"],
        "documents_dir": root / "trainer_documents",
    }


def data_dir_from_env(base_dir: Path) -> Path:
    raw = (os.getenv("TRAINERMATE_DATA_DIR") or "").strip()
    root = Path(raw) if raw else Path(base_dir)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return root


def profile_slug_from_env(default: str = "signed-out") -> str:
    return (os.getenv("TRAINERMATE_PROFILE_SLUG") or default).strip() or default
