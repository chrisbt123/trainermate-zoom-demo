import hashlib
import platform
import uuid
import requests
import keyring
from zoneinfo import ZoneInfo
import os
import re
import sqlite3
import json
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from dateutil.relativedelta import relativedelta
from playwright.sync_api import sync_playwright, TimeoutError, Error as PlaywrightError

APP_NAME = "TrainerMate"
APP_VERSION = "1.0.0"
BUILD_CHANNEL = "Production"
BUILD_NAME = "dashboard_app + bot_app"
BUILD_LABEL = f"{APP_NAME} v{APP_VERSION} {BUILD_CHANNEL}"
DASHBOARD_CANONICAL_URL = "http://127.0.0.1:5000"

LICENSING_API_BASE = os.getenv("TRAINERMATE_API_URL", "http://127.0.0.1:8000")

def get_device_id() -> str:
    raw = f"{platform.node()}-{uuid.getnode()}-{platform.platform()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def check_access(ndors_trainer_id: str, email: str | None):
    payload = {
        "ndors_trainer_id": ndors_trainer_id.strip(),
        "email": email.strip() if email else None,
        "device_id": get_device_id(),
        "device_name": platform.node(),
        "app_version": APP_VERSION,
    }

    response = requests.post(
        f"{LICENSING_API_BASE}/check-access",
        json=payload,
        timeout=20
    )
    response.raise_for_status()
    return response.json()


def record_sync(ndors_trainer_id: str, email: str | None):
    payload = {
        "ndors_trainer_id": ndors_trainer_id.strip(),
        "email": email.strip() if email else None,
        "device_id": get_device_id(),
        "device_name": platform.node(),
        "app_version": APP_VERSION,
    }

    response = requests.post(
        f"{LICENSING_API_BASE}/record-sync",
        json=payload,
        timeout=20
    )
    response.raise_for_status()
    return response.json()

try:
    from course_state import ensure_course, mark_error, mark_skipped, mark_success, update_root
except Exception:
    def ensure_course(course_key):
        return None
    def mark_error(course_key, step, msg):
        return None
    def mark_skipped(course_key, msg):
        return None
    def mark_success(course_key, msg="Done"):
        return None
    def update_root(**kwargs):
        return None

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

PROVIDER_NAME = "Essex"
LOCAL_TIMEZONE = ZoneInfo("Europe/London")

BOT_MODE = (os.getenv("BOT_MODE") or "normal").strip().lower()
ALLOW_ZOOM_REPLACE_ON_MISMATCH = (os.getenv("TRAINERMATE_ALLOW_ZOOM_REPLACE_ON_MISMATCH") or "").strip().lower() in {"1", "true", "yes", "y"}

ALLOW_ZOOM_CREATION = False
ALLOW_AUTOMATION = False
ALLOW_CALENDAR = False
FREE_SYNC_WINDOW_DAYS = 21
PAID_SYNC_WINDOW_DAYS = 84
LICENSED_SYNC_WINDOW_DAYS = FREE_SYNC_WINDOW_DAYS


class ZoomAuthRequired(RuntimeError):
    """Raised when the linked Zoom account token is expired or unauthorized."""
    pass


def zoom_auth_error_message(account_id=None):
    suffix = f" ({account_id})" if account_id else ""
    return f"Zoom account needs reconnecting{suffix}. Reconnect Zoom in TrainerMate, then sync this course again."



def get_zoom_credentials():
    try:
        return {
            "account_id": keyring.get_password("zoom_s2s", "account_id") or "",
            "client_id": keyring.get_password("zoom_s2s", "client_id") or "",
            "client_secret": keyring.get_password("zoom_s2s", "client_secret") or "",
        }
    except Exception as exc:
        print(f"[ZOOM] Failed to read Zoom credentials from keyring: {exc}")
        return {"account_id": "", "client_id": "", "client_secret": ""}

ZOOM_USER_ID = "me"

BASE_DIR = Path(__file__).resolve().parent

try:
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR / ".env")
except Exception:
    pass

APP_STATE_PATH = str(BASE_DIR / "app_state.json")
ZOOM_SETTINGS_PATH = str(BASE_DIR / "zoom_settings.json")
ZOOM_TEMPLATE_PATH = str(BASE_DIR / "zoom_template.json")
PROVIDERS_PATH = str(BASE_DIR / "providers.json")
ZOOM_ACCOUNTS_PATH = str(BASE_DIR / "zoom_accounts.json")
ZOOM_OAUTH_KEYRING_SERVICE = "trainermate_zoom_oauth"
# Zoom OAuth credentials must come from environment variables, a local .env file,
# or TrainerMate's local advanced setup file. Do not hardcode production secrets.
# Do not hardcode production secrets in distributable builds.
ZOOM_OAUTH_CONFIG_PATH = BASE_DIR / "zoom_oauth_config.json"


def _load_zoom_oauth_config_file():
    try:
        if ZOOM_OAUTH_CONFIG_PATH.exists():
            with ZOOM_OAUTH_CONFIG_PATH.open("r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception as exc:
        print(f"[ZOOM] Could not read local OAuth config: {exc}")
    return {}


_zoom_oauth_config = _load_zoom_oauth_config_file()
ZOOM_CLIENT_ID = (os.getenv("ZOOM_CLIENT_ID") or _zoom_oauth_config.get("client_id") or "").strip()
ZOOM_CLIENT_SECRET = (os.getenv("ZOOM_CLIENT_SECRET") or _zoom_oauth_config.get("client_secret") or "").strip()


def _first_non_empty(*values):
    for value in values:
        if value:
            return value
    return ""


def _get_keyring_password(service, account):
    try:
        return keyring.get_password(service, account) or ""
    except Exception:
        return ""


def get_portal_username():
    return _first_non_empty(
        os.getenv("ESSEX_PORTAL_USERNAME"),
        _get_keyring_password("essex_portal", "username"),
        _get_keyring_password("road_safety_portal", "username"),
        _get_keyring_password("road_safety_portal", "essex_username"),
    )


def get_portal_password():
    return _first_non_empty(
        os.getenv("ESSEX_PORTAL_PASSWORD"),
        _get_keyring_password("essex_portal", "password"),
        _get_keyring_password("road_safety_portal", "password"),
        _get_keyring_password("road_safety_portal", "essex_password"),
    )


DEFAULT_PROVIDERS = {"providers": []}


def provider_slug(value):
    text = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")
    return text or "provider"

BOT_PROVIDER_FILTER = provider_slug((os.getenv("BOT_PROVIDER_FILTER") or "").strip()) if (os.getenv("BOT_PROVIDER_FILTER") or "").strip() else ""

TRAINERMATE_SCAN_PROVIDER = provider_slug((os.getenv("TRAINERMATE_SCAN_PROVIDER") or "").strip()) if (os.getenv("TRAINERMATE_SCAN_PROVIDER") or "").strip() else ""
try:
    TRAINERMATE_SCAN_DAYS = int((os.getenv("TRAINERMATE_SCAN_DAYS") or "0").strip() or 0)
except Exception:
    TRAINERMATE_SCAN_DAYS = 0
if TRAINERMATE_SCAN_DAYS <= 0:
    TRAINERMATE_SCAN_DAYS = PAID_SYNC_WINDOW_DAYS
TRAINERMATE_SCAN_DAYS = max(1, min(TRAINERMATE_SCAN_DAYS, PAID_SYNC_WINDOW_DAYS))
TRAINERMATE_SCAN_SCOPE = (os.getenv("TRAINERMATE_SCAN_SCOPE") or "short").strip().lower()

# Optional single-course confirmation check. Dashboard sets these when the user
# asks to verify one suspicious row (for example a stale "Not checked" row).
TRAINERMATE_TARGET_COURSE_ID = (os.getenv("TRAINERMATE_TARGET_COURSE_ID") or "").strip()
TRAINERMATE_TARGET_COURSE_PROVIDER = (os.getenv("TRAINERMATE_TARGET_COURSE_PROVIDER") or "").strip()
TRAINERMATE_TARGET_COURSE_DATE_TIME = (os.getenv("TRAINERMATE_TARGET_COURSE_DATE_TIME") or "").strip()
TRAINERMATE_TARGET_COURSE_TITLE = (os.getenv("TRAINERMATE_TARGET_COURSE_TITLE") or "").strip()

def target_course_check_enabled():
    return bool(TRAINERMATE_TARGET_COURSE_ID or (TRAINERMATE_TARGET_COURSE_PROVIDER and TRAINERMATE_TARGET_COURSE_DATE_TIME and TRAINERMATE_TARGET_COURSE_TITLE))

def course_matches_target(course):
    if not target_course_check_enabled() or not isinstance(course, dict):
        return True

    # Exact visible alert values must win over course_id. Older/stale alerts can
    # carry the wrong course id when two same-provider courses exist on the same
    # day. If provider + date_time + title were supplied by the dashboard, never
    # allow an id-only match to broaden an 08:15 action into a 12:00 course.
    exact_target_supplied = bool(
        TRAINERMATE_TARGET_COURSE_PROVIDER
        and TRAINERMATE_TARGET_COURSE_DATE_TIME
        and TRAINERMATE_TARGET_COURSE_TITLE
    )
    provider_ok = not TRAINERMATE_TARGET_COURSE_PROVIDER or provider_slug(course.get("provider") or "") == provider_slug(TRAINERMATE_TARGET_COURSE_PROVIDER)
    dt_ok = not TRAINERMATE_TARGET_COURSE_DATE_TIME or (course.get("date_time") or "").strip() == TRAINERMATE_TARGET_COURSE_DATE_TIME
    title_ok = not TRAINERMATE_TARGET_COURSE_TITLE or provider_slug(course.get("title") or "") == provider_slug(TRAINERMATE_TARGET_COURSE_TITLE)

    if exact_target_supplied:
        return provider_ok and dt_ok and title_ok

    cid = (course.get("id") or "").strip()
    if TRAINERMATE_TARGET_COURSE_ID and cid == TRAINERMATE_TARGET_COURSE_ID:
        return True
    return provider_ok and dt_ok and title_ok

def target_course_payload():
    if not target_course_check_enabled():
        return None
    return {
        "id": TRAINERMATE_TARGET_COURSE_ID,
        "provider": TRAINERMATE_TARGET_COURSE_PROVIDER,
        "date_time": TRAINERMATE_TARGET_COURSE_DATE_TIME,
        "title": TRAINERMATE_TARGET_COURSE_TITLE,
    }


def target_course_datetime():
    """Return the stored target course datetime for single-course checks.

    Single-course confirmation must be anchored to the course's own date/time.
    It must never fall back to today or broaden into a rolling scan from today.
    """
    if not target_course_check_enabled() or not TRAINERMATE_TARGET_COURSE_DATE_TIME:
        return None
    try:
        return datetime.strptime(TRAINERMATE_TARGET_COURSE_DATE_TIME.strip(), "%Y-%m-%d %H:%M")
    except Exception:
        return None


def target_course_fobs_date():
    dt = target_course_datetime()
    return dt.strftime("%d/%m/%Y") if dt else ""

# Visibility/import range and sync range are intentionally different.
# Import broadly so every provider shows all known assigned future courses.
# Sync/update only inside the user's active account window.
try:
    PROVIDER_IMPORT_LOOKAHEAD_DAYS = int((os.getenv("TRAINERMATE_IMPORT_LOOKAHEAD_DAYS") or "548").strip() or 548)
except Exception:
    PROVIDER_IMPORT_LOOKAHEAD_DAYS = 548
PROVIDER_IMPORT_LOOKAHEAD_DAYS = max(84, min(PROVIDER_IMPORT_LOOKAHEAD_DAYS, 1095))


def provider_allowed_for_scan(provider_name):
    target = TRAINERMATE_SCAN_PROVIDER or BOT_PROVIDER_FILTER
    if not target or target == "all":
        return True
    return provider_slug(provider_name) == provider_slug(target)


def course_allowed_for_scan(course_date_time):
    if target_course_check_enabled():
        # Single-course checks are date anchored. Only the exact stored course
        # date/time is relevant; do not scan from today up to that date.
        target_dt = (TRAINERMATE_TARGET_COURSE_DATE_TIME or "").strip()
        return bool(target_dt and (course_date_time or "").strip() == target_dt)
    if not TRAINERMATE_SCAN_DAYS:
        return True
    try:
        course_dt = datetime.strptime((course_date_time or "").strip(), "%Y-%m-%d %H:%M")
        limit_dt = datetime.now() + timedelta(days=TRAINERMATE_SCAN_DAYS)
        return course_dt <= limit_dt
    except Exception:
        return True

def build_course_id(provider_name, date_time, start_text, title):
    provider_part = provider_slug(provider_name or PROVIDER_NAME)
    date_part = (date_time or "")[:10]
    time_part = (start_text or "").replace(":", "")
    title_part = provider_slug(title or "")
    return f"{provider_part}-{date_part}-{time_part}-{title_part}"


def normalize_provider_context(provider_name, detected_provider_name=""):
    configured = (provider_name or PROVIDER_NAME).strip() or PROVIDER_NAME
    detected = (detected_provider_name or "").strip()
    configured_slug = provider_slug(configured)
    detected_slug = provider_slug(detected)
    if detected and detected_slug and detected_slug != configured_slug:
        print(
            f"[PROVIDER] Warning: provider inferred from page/url ('{detected}') "
            f"does not match configured provider ('{configured}'). "
            f"Using configured provider as the source of truth."
        )
    return configured



def filter_courses_for_requested_scan(courses):
    filtered = []
    for course in courses or []:
        provider_name = (course.get("provider") or "").strip()
        course_dt = (course.get("date_time") or "").strip()

        if provider_name and not provider_allowed_for_scan(provider_name):
            print(f"[SCAN] Ignoring course outside provider scope: {provider_name} | {course.get('title','')}")
            continue

        if course_dt and not course_allowed_for_scan(course_dt):
            print(f"[SCAN] Ignoring course outside date window: {provider_name} | {course.get('title','')} | {course_dt}")
            continue

        if target_course_check_enabled() and not course_matches_target(course):
            print(f"[TARGET] Ignoring non-target course during single-course check: {provider_name} | {course.get('title','')} | {course_dt}")
            continue

        filtered.append(course)
    if target_course_check_enabled():
        print(f"[TARGET] Single-course confirmation check active. Matched {len(filtered)} course(s).")
    return filtered

def enforce_course_provider_context(course, provider_name):
    authoritative_provider = normalize_provider_context(provider_name)
    if not isinstance(course, dict):
        return course
    original_provider = (course.get("provider") or "").strip()
    if original_provider and provider_slug(original_provider) != provider_slug(authoritative_provider):
        print(
            f"[PROVIDER] Overriding course provider '{original_provider}' -> "
            f"'{authoritative_provider}' for course '{course.get('title', '')}' "
            f"at {course.get('date_time', '')}."
        )
    course["provider"] = authoritative_provider

    date_time = (course.get("date_time") or "").strip()
    title = (course.get("title") or "").strip()
    start_text = (course.get("time") or "").strip()
    if not start_text and date_time:
        try:
            start_text = datetime.strptime(date_time, "%Y-%m-%d %H:%M").strftime("%H:%M")
        except Exception:
            start_text = ""
    if date_time and start_text and title:
        expected_id = build_course_id(authoritative_provider, date_time, start_text, title)
        current_id = (course.get("id") or "").strip()
        if current_id != expected_id:
            if current_id:
                print(
                    f"[PROVIDER] Rebuilding course id for '{title}' "
                    f"from '{current_id}' to '{expected_id}'."
                )
            course["id"] = expected_id
    return course




def derive_courses_url(login_url):
    parsed = urlparse((login_url or "").strip())
    if parsed.scheme != "https" or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}/Trainer/CoursesBookedOn"


def get_provider_keyring_service(provider_id):
    provider_id = provider_slug(provider_id)
    if provider_id == "essex":
        return "essex_portal"
    return f"road_safety_provider_{provider_id}"


def get_provider_keyring_aliases(provider_id):
    provider_id = provider_slug(provider_id)
    aliases = []

    # Prefer the dashboard's provider-specific service first, then the older
    # bot aliases. Dashboard now also writes to all aliases, but this protects
    # installs that still have only the dashboard value saved.
    aliases.append(f"trainermate_provider_{provider_id}")

    if provider_id == "essex":
        aliases.extend(["essex_portal", "road_safety_portal"])
    else:
        aliases.extend([f"road_safety_provider_{provider_id}", "road_safety_portal"])
    seen = set()
    ordered = []
    for alias in aliases:
        if alias and alias not in seen:
            ordered.append(alias)
            seen.add(alias)
    return ordered


def get_provider_keyring_accounts(provider_id, field):
    provider_id = provider_slug(provider_id)
    normalized = provider_id.replace("-", "_")
    field = (field or "").strip().lower()
    accounts = [field]
    if provider_id == "essex":
        accounts.extend([f"essex_{field}", field])
    else:
        accounts.extend([f"{provider_id}_{field}", f"{normalized}_{field}"])
    seen = set()
    ordered = []
    for account in accounts:
        if account and account not in seen:
            ordered.append(account)
            seen.add(account)
    return ordered


def load_zoom_accounts_file():
    if not os.path.exists(ZOOM_ACCOUNTS_PATH):
        return []
    try:
        with open(ZOOM_ACCOUNTS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []

    accounts = data.get("accounts", []) if isinstance(data, dict) else []
    cleaned = []
    seen = set()
    for account in accounts:
        if not isinstance(account, dict):
            continue
        account_id = (account.get("id") or "").strip()
        if not account_id or account_id in seen:
            continue
        cleaned.append({
            "id": account_id,
            "email": (account.get("email") or "").strip(),
            "nickname": (account.get("nickname") or account.get("email") or "Zoom account").strip(),
            "is_default": bool(account.get("is_default", False)),
            "connected_at": (account.get("connected_at") or "").strip(),
            "last_verified_at": (account.get("last_verified_at") or "").strip(),
            "status": (account.get("status") or "connected").strip(),
        })
        seen.add(account_id)
    if cleaned and not any(a.get("is_default") for a in cleaned):
        cleaned[0]["is_default"] = True
    return cleaned


def get_default_zoom_account_id():
    for account in load_zoom_accounts_file():
        if account.get("is_default"):
            return (account.get("id") or "").strip()
    return ""


def normalize_provider_record(provider):
    if not isinstance(provider, dict):
        return None

    name = (provider.get("name") or "").strip()
    login_url = (provider.get("login_url") or "").strip()
    courses_url = (provider.get("courses_url") or derive_courses_url(login_url)).strip()
    pid = provider_slug(provider.get("id") or name)

    if not pid:
        return None

    if not name:
        name = pid.replace('-', ' ').title()

    color = (provider.get("color") or "").strip()
    if not (len(color) == 7 and color.startswith("#") and all(ch in "0123456789abcdefABCDEF" for ch in color[1:])):
        color = ""

    normalized_provider = {
        "id": pid,
        "name": name,
        "login_url": login_url,
        "courses_url": courses_url,
        "color": color,
        "active": bool(provider.get("active", True)),
        "supports_custom_time": bool(provider.get("supports_custom_time", True)),
        "supports_time_push": bool(provider.get("supports_time_push", False)),
        "zoom_mode": (provider.get("zoom_mode") or "trainer_default").strip() or "trainer_default",
        "zoom_account_id": (provider.get("zoom_account_id") or "").strip(),
        "provider_manages_zoom": bool(provider.get("provider_manages_zoom", False)),
        "never_overwrite_existing_zoom": bool(provider.get("provider_manages_zoom", False)),
    }

    if normalized_provider["provider_manages_zoom"]:
        normalized_provider["zoom_mode"] = "provider_managed"
        normalized_provider["never_overwrite_existing_zoom"] = True
    else:
        normalized_provider["never_overwrite_existing_zoom"] = False
        if normalized_provider["zoom_mode"] in {"trainer_specific", "linked_account"} and not normalized_provider["zoom_account_id"]:
            normalized_provider["zoom_mode"] = "trainer_default"

    return normalized_provider


def ensure_providers_file():
    default_payload = json.loads(json.dumps(DEFAULT_PROVIDERS))
    if not os.path.exists(PROVIDERS_PATH):
        save_json_file(PROVIDERS_PATH, default_payload)
        return default_payload["providers"]

    try:
        with open(PROVIDERS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return default_payload["providers"]

    providers = data.get("providers", []) if isinstance(data, dict) else []
    if not isinstance(providers, list):
        providers = []

    normalized = []
    seen = set()
    needs_write = False

    for provider in providers:
        normalized_provider = normalize_provider_record(provider)
        if not normalized_provider:
            needs_write = True
            continue
        if normalized_provider["id"] in seen:
            needs_write = True
            continue
        if provider != normalized_provider:
            needs_write = True
        normalized.append(normalized_provider)
        seen.add(normalized_provider["id"])

    if needs_write:
        save_json_file(PROVIDERS_PATH, {"providers": normalized})

    return normalized


def load_providers():
    providers = ensure_providers_file()
    filtered = []
    for provider in providers:
        if not isinstance(provider, dict):
            continue
        if not provider.get("active", True):
            continue
        provider_name = (provider.get("name") or "").strip()
        if not provider_allowed_for_scan(provider_name):
            print(f"[SCAN] Skipping provider outside requested scope: {provider_name}")
            continue
        filtered.append(provider)
    if TRAINERMATE_SCAN_PROVIDER and TRAINERMATE_SCAN_PROVIDER != "all":
        print(f"[SCAN] Provider filter active: {TRAINERMATE_SCAN_PROVIDER}")
    if TRAINERMATE_SCAN_DAYS:
        print(f"[SCAN] Date window active: next {TRAINERMATE_SCAN_DAYS} days")
    return filtered


def save_providers(providers):
    normalized = []
    seen = set()
    for provider in providers or []:
        normalized_provider = normalize_provider_record(provider)
        if not normalized_provider or normalized_provider["id"] in seen:
            continue
        normalized.append(normalized_provider)
        seen.add(normalized_provider["id"])

    save_json_file(PROVIDERS_PATH, {"providers": normalized})
    return normalized


def get_provider_config(provider_name):
    target_slug = provider_slug(provider_name)
    for provider in load_providers():
        if provider_slug(provider.get("id") or provider.get("name") or "") == target_slug:
            return provider
    return normalize_provider_record({"name": provider_name}) or {
        "id": target_slug,
        "name": provider_name,
        "login_url": "",
        "courses_url": "",
        "active": True,
        "supports_custom_time": True,
        "supports_time_push": False,
        "zoom_mode": "trainer_default",
        "zoom_account_id": "",
        "never_overwrite_existing_zoom": False,
        "provider_manages_zoom": False,
    }


def get_provider_zoom_account_id(provider_name):
    provider_config = get_provider_config(provider_name)
    if provider_config.get("provider_manages_zoom"):
        return ""

    explicit_account_id = (provider_config.get("zoom_account_id") or "").strip()
    if explicit_account_id:
        return explicit_account_id

    return get_default_zoom_account_id()


def get_provider_username(provider_id):
    values = []
    for service in get_provider_keyring_aliases(provider_id):
        for account in get_provider_keyring_accounts(provider_id, "username"):
            values.append(_get_keyring_password(service, account))
    return _first_non_empty(*values)


def get_provider_password(provider_id):
    values = []
    for service in get_provider_keyring_aliases(provider_id):
        for account in get_provider_keyring_accounts(provider_id, "password"):
            values.append(_get_keyring_password(service, account))
    return _first_non_empty(*values)


def detect_provider_from_url(url):
    parsed = urlparse(url or "")
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()

    for provider in load_providers():
        for field in ("courses_url", "login_url"):
            candidate = (provider.get(field) or "").strip()
            if not candidate:
                continue
            c = urlparse(candidate)
            c_host = (c.netloc or "").lower()
            c_path = (c.path or "").lower()
            if c_path and path == c_path:
                return provider.get("name") or PROVIDER_NAME
            if c_host and host == c_host:
                return provider.get("name") or PROVIDER_NAME

    return PROVIDER_NAME


def open_page_with_visible_url(page, target_url, label="portal", timeout=30000):
    """
    Navigate a Playwright page while avoiding a silent about:blank failure.
    The page is first given a tiny visible loading document, then goto is tried
    with domcontentloaded. If normal waiting times out but the browser did move
    away from about:blank, the run can continue instead of looking stuck.
    """
    target_url = (target_url or "").strip()
    if not target_url:
        raise ValueError(f"No {label} URL configured.")

    print(f"[PORTAL] Navigating to {label}: {target_url}")
    try:
        page.set_content(
            f"<html><body style='font-family:sans-serif;padding:20px'>"
            f"<h3>TrainerMate is opening {label}...</h3>"
            f"<p>{target_url}</p>"
            f"</body></html>"
        )
    except Exception:
        pass

    try:
        page.goto(target_url, timeout=timeout, wait_until="domcontentloaded")
        return True
    except TimeoutError:
        current_url = (getattr(page, "url", "") or "").strip()
        print(f"[PORTAL] Navigation timeout for {label}. Current URL: {current_url or 'unknown'}")
        if current_url and current_url != "about:blank":
            print("[PORTAL] Page moved away from about:blank, continuing with loaded content.")
            return True
        raise


DEFAULT_APP_STATE = {
    "sync_running": False,
    "stop_requested": False,
    "pid": None,
    "last_started_at": "",
    "last_stopped_at": "",
    "last_status": "Idle",
    "last_run_started_at": "",
    "last_run_finished_at": "",
    "last_run_status": "",
    "last_message": "",
    "last_pid": None,
    "current_provider": "",
    "run_summary": {},
    "health_issues": [],
    "last_success_at": "",
    "last_check_at": "",
}

DEFAULT_ZOOM_SETTINGS = {
    "host_video": True,
    "participant_video": False,
    "join_before_host": False,
    "waiting_room": True,
    "mute_upon_entry": True,
    "approval_type": 2,
    "audio": "both",
    "auto_recording": "none",
    "use_pmi": False,
    "meeting_authentication": False,
}


# ============================================================
# SETTINGS
# ============================================================

PORTAL_URL = "https://www.essexfobs.co.uk/Account/Login"
COURSES_URL = "https://www.essexfobs.co.uk/Trainer/CoursesBookedOn"

USERNAME = get_portal_username()
PASSWORD = get_portal_password()

# Keep this True if you want fallback test data when portal is unavailable
USE_SIMULATED_DATA_IF_PORTAL_DOWN = False

# How closely a Zoom meeting start time must match a course start time
ZOOM_MATCH_WINDOW_MINUTES = 5

# Essex-specific topic abbreviations.
# Add more mappings here as you confirm them.
ESSEX_TOPIC_MAP = {
    "speed awareness": "NSAC",
    "motorway awareness": "NMAC",
}

ESSEX_ACRONYM_STOPWORDS = {
    "a", "an", "the", "and", "of", "for", "to", "in", "on", "at", "by",
    "with", "from", "or", "into", "upon", "over", "under", "after", "before",
    "s",  # handles words like What's -> ["What", "s"]
}



def load_json_file(path, default_value):
    if not os.path.exists(path):
        return default_value.copy() if isinstance(default_value, dict) else default_value

    try:
        with open(path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        if isinstance(default_value, dict):
            merged = default_value.copy()
            if isinstance(loaded, dict):
                merged.update(loaded)
            return merged
        return loaded
    except Exception:
        return default_value.copy() if isinstance(default_value, dict) else default_value


def save_json_file(path, data):
    directory = os.path.dirname(path) or "."
    fd, tmp_name = tempfile.mkstemp(prefix=os.path.basename(path) + ".", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except Exception:
            pass


def mark_zoom_account_needs_reconnect(account_id, message=''):
    account_id = (account_id or '').strip()
    if not account_id:
        return
    try:
        data = load_json_file(ZOOM_ACCOUNTS_PATH, {'accounts': []})
        accounts = data.get('accounts', []) if isinstance(data, dict) else []
        now_text = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        changed = False
        for account in accounts:
            if not isinstance(account, dict):
                continue
            if (account.get('id') or '').strip() != account_id:
                continue
            account['status'] = 'needs_reconnect'
            account['last_error'] = (message or 'Zoom OAuth refresh failed.')[:180]
            account['last_verified_at'] = account.get('last_verified_at') or ''
            account['updated_at'] = now_text
            changed = True
            break
        if changed:
            save_json_file(ZOOM_ACCOUNTS_PATH, {'accounts': accounts})
    except Exception as exc:
        print(f"[ZOOM] Could not mark account as needing reconnect: {exc}")


def mark_zoom_account_connected(account_id):
    account_id = (account_id or '').strip()
    if not account_id:
        return
    try:
        data = load_json_file(ZOOM_ACCOUNTS_PATH, {'accounts': []})
        accounts = data.get('accounts', []) if isinstance(data, dict) else []
        now_text = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        changed = False
        for account in accounts:
            if not isinstance(account, dict):
                continue
            if (account.get('id') or '').strip() != account_id:
                continue
            account['status'] = 'connected'
            account['last_verified_at'] = now_text
            account.pop('last_error', None)
            changed = True
            break
        if changed:
            save_json_file(ZOOM_ACCOUNTS_PATH, {'accounts': accounts})
    except Exception as exc:
        print(f"[ZOOM] Could not update Zoom account health: {exc}")


def load_app_state():
    return load_json_file(APP_STATE_PATH, DEFAULT_APP_STATE)


def save_app_state(state):
    merged = DEFAULT_APP_STATE.copy()
    merged.update(state or {})
    save_json_file(APP_STATE_PATH, merged)


def update_app_state(**kwargs):
    state = load_app_state()
    state.update(kwargs)
    save_app_state(state)
    return state


def clear_stop_request():
    update_app_state(stop_requested=False)


def request_stop():
    update_app_state(stop_requested=True, last_message="Stop requested from dashboard.")


def stop_requested():
    return bool(load_app_state().get("stop_requested"))


def load_zoom_settings():
    return load_json_file(ZOOM_SETTINGS_PATH, DEFAULT_ZOOM_SETTINGS)


def load_zoom_template():
    return load_json_file(ZOOM_TEMPLATE_PATH, {})


def save_zoom_settings(settings):
    merged = DEFAULT_ZOOM_SETTINGS.copy()
    merged.update(settings or {})
    save_json_file(ZOOM_SETTINGS_PATH, merged)
    return merged


def get_effective_zoom_settings():
    template_settings = load_zoom_template()
    dashboard_overrides = load_zoom_settings()
    effective = {}
    if isinstance(template_settings, dict):
        effective.update(template_settings)
    if isinstance(dashboard_overrides, dict):
        effective.update(dashboard_overrides)

    print("[ZOOM] Effective meeting settings being used:")
    try:
        print(json.dumps(effective, indent=2, sort_keys=True))
    except Exception:
        print(effective)

    return effective


def should_stop_now():
    if stop_requested():
        print("[CONTROL] Stop requested. Exiting safely at next checkpoint.")
        return True
    return False


def maybe_stop():
    return should_stop_now()


def build_run_summary(sync_started_at):
    return {
        "started_at": sync_started_at,
        "mode": BOT_MODE or "normal",
        "provider_filter": BOT_PROVIDER_FILTER or "",
        "providers_requested": 0,
        "providers_attempted": 0,
        "providers_succeeded": 0,
        "providers_with_rows": 0,
        "providers_with_zero_rows": 0,
        "providers_failed": 0,
        "providers_missing_credentials": 0,
        "providers_unavailable": 0,
        "courses_found": 0,
        "courses_processed": 0,
        "db_created": 0,
        "db_updated": 0,
        "db_unchanged": 0,
        "db_reactivated": 0,
        "fobs_checked": 0,
        "fobs_updated": 0,
        "fobs_failed": 0,
        "inactive_marked": 0,
        "outcome": "running",
        "message": "Sync started.",
        "health_issues": [],
        "providers": [],
    }


def _safe_int(value):
    try:
        return int(value or 0)
    except Exception:
        return 0


def persist_run_summary(summary):
    safe_summary = dict(summary or {})
    update_app_state(run_summary=safe_summary, health_issues=safe_summary.get("health_issues", []), last_check_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        update_root(run_summary=safe_summary, health_issues=safe_summary.get("health_issues", []), last_check_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    except Exception:
        pass


# ============================================================
# DATABASE SETUP
# ============================================================

conn = sqlite3.connect(str(BASE_DIR / "courses.db"))
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS courses (
    id TEXT PRIMARY KEY,
    provider TEXT,
    title TEXT,
    date_time TEXT,
    meeting_id TEXT,
    meeting_link TEXT,
    meeting_password TEXT,
    status TEXT,
    active_in_portal INTEGER DEFAULT 1,
    last_seen_at TEXT
)
""")
conn.commit()

# Add new columns safely if the table already existed before
for alter_sql in [
    "ALTER TABLE courses ADD COLUMN provider TEXT",
    "ALTER TABLE courses ADD COLUMN last_seen_at TEXT",
    "ALTER TABLE courses ADD COLUMN meeting_password TEXT",
    "ALTER TABLE courses ADD COLUMN last_synced_at TEXT",
    "ALTER TABLE courses ADD COLUMN last_sync_status TEXT",
    "ALTER TABLE courses ADD COLUMN last_sync_action TEXT",
    "ALTER TABLE courses ADD COLUMN fobs_course_url TEXT",
]:
    try:
        cursor.execute(alter_sql)
        conn.commit()
    except sqlite3.OperationalError:
        pass

cursor.execute("DROP INDEX IF EXISTS idx_courses_title_datetime")
conn.commit()
cursor.execute("""
CREATE UNIQUE INDEX IF NOT EXISTS idx_courses_provider_title_datetime
ON courses (provider, title, date_time)
""")
conn.commit()

# ============================================================
# ZOOM / DATABASE HELPERS
# ============================================================

def _get_zoom_oauth_token(account_id, token_kind):
    try:
        return keyring.get_password(ZOOM_OAUTH_KEYRING_SERVICE, f"{token_kind}::{account_id}") or ""
    except Exception:
        return ""



def _set_zoom_oauth_tokens(account_id, access_token, refresh_token):
    try:
        if access_token:
            keyring.set_password(ZOOM_OAUTH_KEYRING_SERVICE, f"access::{account_id}", access_token)
        if refresh_token:
            keyring.set_password(ZOOM_OAUTH_KEYRING_SERVICE, f"refresh::{account_id}", refresh_token)
        mark_zoom_account_connected(account_id)
    except Exception as exc:
        print(f"[ZOOM] Failed to persist refreshed OAuth tokens for {account_id}: {exc}")


def get_zoom_s2s_access_token():
    """Compatibility fallback for older local installs using Zoom S2S credentials."""
    credentials = get_zoom_credentials()
    account_id = (credentials.get("account_id") or "").strip()
    client_id = (credentials.get("client_id") or "").strip()
    client_secret = (credentials.get("client_secret") or "").strip()
    if not (account_id and client_id and client_secret):
        return None
    try:
        response = requests.post(
            "https://zoom.us/oauth/token",
            params={
                "grant_type": "account_credentials",
                "account_id": account_id,
            },
            auth=(client_id, client_secret),
            timeout=30,
        )
        response.raise_for_status()
        token = (response.json().get("access_token") or "").strip()
        if token:
            print("[ZOOM] Using legacy Server-to-Server credentials for verification.")
            return token
    except requests.RequestException as exc:
        print(f"[ZOOM] Legacy Server-to-Server token request failed: {exc}")
    return None



def get_zoom_access_token(account_id=None):
    account_id = (account_id or "").strip()
    refresh_failed = False
    refresh_error = ""

    if account_id and ZOOM_CLIENT_ID and ZOOM_CLIENT_SECRET:
        refresh_token = _get_zoom_oauth_token(account_id, "refresh")
        if refresh_token:
            try:
                response = requests.post(
                    "https://zoom.us/oauth/token",
                    params={
                        "grant_type": "refresh_token",
                        "refresh_token": refresh_token,
                    },
                    auth=(ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET),
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()
                access_token = (data.get("access_token") or "").strip()
                refreshed_refresh_token = (data.get("refresh_token") or refresh_token).strip()
                if access_token:
                    _set_zoom_oauth_tokens(account_id, access_token, refreshed_refresh_token)
                    return access_token
                refresh_failed = True
                refresh_error = "Zoom returned no access token"
            except requests.RequestException as exc:
                refresh_failed = True
                refresh_error = str(exc)
                print(f"[ZOOM] OAuth refresh failed for {account_id}: {exc}")

    # If Zoom rejects a refresh token, do not fall through to a stale access
    # token. That just causes a later 401 during the real sync and makes the
    # course/FOBS flow look broken. Mark the account for reconnect only when
    # Zoom requires fresh user consent.
    if refresh_failed:
        mark_zoom_account_needs_reconnect(account_id, refresh_error)
        return None

    if account_id:
        access_token = _get_zoom_oauth_token(account_id, "access")
        if access_token:
            return access_token

    legacy_token = get_zoom_s2s_access_token()
    if legacy_token:
        return legacy_token

    if account_id:
        if not (ZOOM_CLIENT_ID and ZOOM_CLIENT_SECRET):
            print(
                f"[ZOOM] No usable Zoom token for linked account '{account_id}', "
                "and no OAuth app credentials are configured for token refresh."
            )
            mark_zoom_account_needs_reconnect(account_id, "Zoom OAuth app credentials are missing.")
        else:
            print(f"[ZOOM] No working OAuth token available for linked account '{account_id}'.")
            mark_zoom_account_needs_reconnect(account_id, "No working Zoom OAuth token is available.")
    else:
        print("[ZOOM] No linked Zoom account was supplied.")
    return None


def normalize_zoom_meeting_id(value):
    if value is None:
        return ""

    text = str(value).strip()
    digits_only = re.sub(r"\D", "", text)

    # Normal Zoom meeting IDs are numeric and are usually 9-11 digits.
    if 9 <= len(digits_only) <= 12:
        return digits_only

    return ""


def is_probably_zoom_join_link(value):
    text = (value or "").strip().lower()
    return "zoom.us/j/" in text or "zoom.us/w/" in text


def db_has_valid_zoom_details(zoom_data):
    if not zoom_data:
        return False

    normalized_meeting_id = normalize_zoom_meeting_id(zoom_data.get("meeting_id", ""))
    return bool(normalized_meeting_id)


def build_essex_course_code(title):
    title_clean = (title or "").strip()
    title_normalized = title_clean.lower()

    if title_normalized in ESSEX_TOPIC_MAP:
        return ESSEX_TOPIC_MAP[title_normalized]

    words = re.findall(r"[A-Za-z]+", title_clean)
    acronym_letters = [word[0].upper() for word in words if word.lower() not in ESSEX_ACRONYM_STOPWORDS]

    if acronym_letters:
        return "".join(acronym_letters)

    return title_clean.upper()


def format_zoom_topic(provider, title, date_time):
    provider_normalized = provider_slug(provider or "")
    dt = datetime.strptime(date_time, "%Y-%m-%d %H:%M")
    dt_text = dt.strftime('%d/%m/%Y %H:%M')
    clean_title = (title or "").strip()
    clean_provider = (provider or "").strip()

    if provider_normalized == "essex":
        short_title = build_essex_course_code(clean_title)
        return f"{short_title} {dt_text}"

    return f"{clean_provider} - {clean_title}, {dt_text}"


def get_zoom_details_for_course(conn, title, date_time, provider):
    local_cursor = conn.cursor()
    local_cursor.execute(
        """
        SELECT meeting_link, meeting_id, meeting_password
        FROM courses
        WHERE title = ?
          AND date_time = ?
          AND provider = ?
        LIMIT 1
        """,
        (title, date_time, provider),
    )
    row = local_cursor.fetchone()

    if not row:
        return None

    return {
        "meeting_link": row[0] or "",
        "meeting_id": row[1] or "",
        "meeting_password": row[2] or "",
    }


def save_zoom_details_to_course(conn, title, date_time, provider, zoom_data):
    normalized_meeting_id = normalize_zoom_meeting_id(zoom_data.get("meeting_id", ""))

    local_cursor = conn.cursor()
    local_cursor.execute(
        """
        UPDATE courses
        SET meeting_link = COALESCE(NULLIF(?, ''), meeting_link),
            meeting_id = ?,
            meeting_password = COALESCE(NULLIF(?, ''), meeting_password)
        WHERE title = ?
          AND date_time = ?
          AND provider = ?
        """,
        (
            (zoom_data.get("meeting_link") or "").strip(),
            normalized_meeting_id,
            (zoom_data.get("meeting_password") or "").strip(),
            title,
            date_time,
            provider,
        ),
    )
    conn.commit()


def update_meeting_password_in_db(conn, title, date_time, provider, meeting_password):
    local_cursor = conn.cursor()
    local_cursor.execute(
        """
        UPDATE courses
        SET meeting_password = ?
        WHERE title = ?
          AND date_time = ?
          AND provider = ?
        """,
        ((meeting_password or "").strip(), title, date_time, provider),
    )
    conn.commit()


def update_course_fobs_url(conn, title, date_time, provider, fobs_course_url):
    """Persist the exact FOBS course summary URL once the details page has been opened."""
    fobs_course_url = (fobs_course_url or "").strip()
    if not fobs_course_url.startswith("https://"):
        return
    local_cursor = conn.cursor()
    seen_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    local_cursor.execute(
        """
        UPDATE courses
        SET fobs_course_url = ?,
            last_seen_at = ?
        WHERE title = ?
          AND date_time = ?
          AND provider = ?
        """,
        (
            fobs_course_url,
            seen_at,
            title,
            date_time,
            provider,
        ),
    )
    if local_cursor.rowcount == 0:
        local_cursor.execute(
            """
            SELECT id
            FROM courses
            WHERE date_time = ?
              AND provider = ?
              AND COALESCE(active_in_portal, 1) = 1
            LIMIT 2
            """,
            (date_time, provider),
        )
        matches = local_cursor.fetchall()
        if len(matches) == 1:
            local_cursor.execute(
                """
                UPDATE courses
                SET fobs_course_url = ?,
                    last_seen_at = ?
                WHERE id = ?
                """,
                (fobs_course_url, seen_at, matches[0][0]),
            )
    conn.commit()


def update_course_sync_state(conn, title, date_time, provider, sync_status, sync_action):
    """Persist the actual per-course sync/check result for the dashboard."""
    local_cursor = conn.cursor()
    synced_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    local_cursor.execute(
        """
        UPDATE courses
        SET last_synced_at = ?,
            last_sync_status = ?,
            last_sync_action = ?,
            last_seen_at = ?
        WHERE title = ?
          AND date_time = ?
          AND provider = ?
        """,
        (
            synced_at,
            (sync_status or "").strip(),
            (sync_action or "").strip(),
            synced_at,
            title,
            date_time,
            provider,
        ),
    )
    if local_cursor.rowcount == 0:
        local_cursor.execute(
            """
            SELECT id
            FROM courses
            WHERE date_time = ?
              AND provider = ?
              AND COALESCE(active_in_portal, 1) = 1
            LIMIT 2
            """,
            (date_time, provider),
        )
        matches = local_cursor.fetchall()
        if len(matches) == 1:
            local_cursor.execute(
                """
                UPDATE courses
                SET last_synced_at = ?,
                    last_sync_status = ?,
                    last_sync_action = ?,
                    last_seen_at = ?
                WHERE id = ?
                """,
                (
                    synced_at,
                    (sync_status or "").strip(),
                    (sync_action or "").strip(),
                    synced_at,
                    matches[0][0],
                ),
            )
    conn.commit()


def get_zoom_meeting_details_by_id(meeting_id, account_id=None):
    normalized_meeting_id = normalize_zoom_meeting_id(meeting_id)
    if not normalized_meeting_id:
        return None

    access_token = get_zoom_access_token(account_id=account_id)
    if not access_token:
        return None
    response = requests.get(
        f"https://api.zoom.us/v2/meetings/{normalized_meeting_id}",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        timeout=30,
    )

    if response.status_code == 401:
        message = zoom_auth_error_message(account_id)
        print(f"[ZOOM] {message}")
        raise ZoomAuthRequired(message)

    if response.status_code == 404:
        return None

    response.raise_for_status()
    data = response.json()

    return {
        "meeting_link": data.get("join_url", "") or "",
        "meeting_id": normalize_zoom_meeting_id(data.get("id", "")),
        "meeting_password": data.get("password", "") or "",
        "topic": data.get("topic", "") or "",
        "start_time": data.get("start_time", "") or "",
    }


def fetch_zoom_password_by_meeting_id(meeting_id, account_id=None):
    meeting_details = get_zoom_meeting_details_by_id(meeting_id, account_id=account_id)
    if not meeting_details:
        print(f"No Zoom meeting details found for meeting_id={meeting_id}")
        return ""

    password = (meeting_details.get("meeting_password") or "").strip()

    if password:
        print(f"Fetched password from Zoom for meeting_id={meeting_id}")
    else:
        print(f"Zoom returned no password for meeting_id={meeting_id}")

    return password


def list_upcoming_zoom_meetings(account_id=None):
    access_token = get_zoom_access_token(account_id=account_id)
    if not access_token:
        return []

    meetings = []
    next_page_token = ""
    attempted_page_sizes = [100, 50, None]
    last_error = None

    for requested_page_size in attempted_page_sizes:
        meetings = []
        next_page_token = ""
        last_error = None

        while True:
            params = {
                "type": "scheduled",
            }
            if requested_page_size is not None:
                params["page_size"] = requested_page_size
            if next_page_token:
                params["next_page_token"] = next_page_token

            response = requests.get(
                f"https://api.zoom.us/v2/users/{ZOOM_USER_ID}/meetings",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                params=params,
                timeout=30,
            )

            if response.status_code == 401:
                message = zoom_auth_error_message(account_id)
                print(f"[ZOOM] {message}")
                raise ZoomAuthRequired(message)

            if response.status_code == 400:
                last_error = requests.HTTPError(
                    f"400 Client Error: Bad Request for url: {response.url}",
                    response=response,
                )
                print(
                    "[ZOOM] Meeting list request was rejected "
                    f"(type=scheduled, page_size={requested_page_size if requested_page_size is not None else 'default'})."
                )
                break

            response.raise_for_status()
            data = response.json()
            meetings.extend(data.get("meetings", []))

            next_page_token = data.get("next_page_token") or ""
            if not next_page_token:
                return meetings

        if last_error:
            continue

    if last_error:
        raise last_error

    return meetings


def parse_zoom_start_time(start_time_text):
    if not start_time_text:
        return None

    try:
        if start_time_text.endswith("Z"):
            dt = datetime.fromisoformat(start_time_text.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(start_time_text)
        return dt.astimezone(LOCAL_TIMEZONE)
    except ValueError:
        return None


def apply_zoom_settings_to_meeting(meeting_id, settings_patch, account_id=None):
    normalized_meeting_id = normalize_zoom_meeting_id(meeting_id)
    if not normalized_meeting_id:
        return False, "invalid meeting id"

    access_token = get_zoom_access_token(account_id=account_id)
    if not access_token:
        return False, "missing zoom access token"

    response = requests.patch(
        f"https://api.zoom.us/v2/meetings/{normalized_meeting_id}",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json={"settings": settings_patch},
        timeout=30,
    )

    if response.status_code == 404:
        return False, "meeting not found"

    try:
        response.raise_for_status()
    except Exception as exc:
        return False, str(exc)

    return True, "updated"



def update_zoom_meeting_topic(meeting_id, provider, title, date_time, account_id=None):
    """Rename a reused Zoom meeting when the provider changes the course title."""
    normalized_meeting_id = normalize_zoom_meeting_id(meeting_id)
    if not normalized_meeting_id:
        return False, "invalid meeting id"
    expected_topic = format_zoom_topic(provider, title, date_time)
    access_token = get_zoom_access_token(account_id=account_id)
    if not access_token:
        return False, "missing zoom access token"
    response = requests.patch(
        f"https://api.zoom.us/v2/meetings/{normalized_meeting_id}",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json={"topic": expected_topic},
        timeout=30,
    )
    if response.status_code == 404:
        return False, "meeting not found"
    try:
        response.raise_for_status()
    except Exception as exc:
        return False, str(exc)
    print(f"[ZOOM] Updated Zoom meeting topic to '{expected_topic}' for {provider} {date_time}.")
    return True, "updated"


def ensure_zoom_topic_matches_course(meeting_id, provider, title, date_time, account_id=None):
    """Best-effort topic tidy. Does not block the sync if Zoom write scope is missing."""
    normalized_meeting_id = normalize_zoom_meeting_id(meeting_id)
    if not normalized_meeting_id:
        return False, "invalid meeting id"
    expected_topic = format_zoom_topic(provider, title, date_time).strip()
    try:
        details = get_zoom_meeting_details_by_id(normalized_meeting_id, account_id=account_id)
    except Exception as exc:
        return False, f"could not read meeting: {exc}"
    if not details:
        return False, "meeting not found"
    current_topic = (details.get("topic") or "").strip()
    if current_topic == expected_topic:
        return True, "already correct"
    ok, message = update_zoom_meeting_topic(normalized_meeting_id, provider, title, date_time, account_id=account_id)
    if not ok:
        print(f"[ZOOM] Topic rename skipped for {provider} {date_time}: {message}")
    return ok, message


def bulk_update_existing_zoom_meetings(
    db_path=str(BASE_DIR / "courses.db"),
    settings_patch=None,
    provider=None,
    title=None,
    future_only=True,
    active_only=True,
    account_id=None,
):
    settings_patch = settings_patch or {}
    if not settings_patch:
        return {
            "selected": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "details": ["No settings selected."],
        }

    conn_local = sqlite3.connect(db_path)
    local_cursor = conn_local.cursor()

    query = """
        SELECT provider, title, date_time, meeting_id, meeting_link
        FROM courses
        WHERE 1=1
    """
    params = []

    if active_only:
        query += " AND active_in_portal = 1"

    if provider and provider != "All":
        query += " AND provider = ?"
        params.append(provider)

    if title and title != "All":
        query += " AND title = ?"
        params.append(title)

    query += " ORDER BY date_time ASC"

    rows = local_cursor.execute(query, params).fetchall()
    conn_local.close()

    now_local = datetime.now(LOCAL_TIMEZONE)
    details = []
    result = {"selected": 0, "updated": 0, "skipped": 0, "failed": 0, "details": details}

    for provider_name, title_text, date_time_text, meeting_id, meeting_link in rows:
        zoom_data = {
            "meeting_id": meeting_id or "",
            "meeting_link": meeting_link or "",
        }

        if not db_has_valid_zoom_details(zoom_data):
            result["skipped"] += 1
            details.append(f"Skipped invalid Zoom row: {provider_name} | {title_text} | {date_time_text}")
            continue

        result["selected"] += 1

        if future_only:
            try:
                course_dt = datetime.strptime(date_time_text, "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TIMEZONE)
                if course_dt < now_local:
                    result["skipped"] += 1
                    details.append(f"Skipped past meeting: {provider_name} | {title_text} | {date_time_text}")
                    continue
            except Exception:
                pass

        exists_live = get_zoom_meeting_details_by_id(meeting_id, account_id=account_id)
        if not exists_live:
            result["skipped"] += 1
            details.append(f"Skipped deleted Zoom meeting: {provider_name} | {title_text} | {date_time_text}")
            continue

        ok, message = apply_zoom_settings_to_meeting(meeting_id, settings_patch, account_id=account_id)
        if ok:
            result["updated"] += 1
            details.append(f"Updated: {provider_name} | {title_text} | {date_time_text}")
        else:
            result["failed"] += 1
            details.append(f"Failed: {provider_name} | {title_text} | {date_time_text} | {message}")

    return result



def find_matching_zoom_meeting(provider, title, date_time, account_id=None):
    expected_topic = format_zoom_topic(provider, title, date_time).strip().lower()
    expected_start = datetime.strptime(date_time, "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TIMEZONE)
    provider_text = (provider or "").strip().lower()
    title_text = (title or "").strip().lower()
    date_text = expected_start.strftime("%d/%m/%Y %H:%M").lower()

    print(f"[ZOOM] Looking for an existing meeting matching topic='{expected_topic}'")
    print(f"[ZOOM] Searching meetings using linked account_id='{account_id or 'NONE'}'")

    try:
        meetings = list_upcoming_zoom_meetings(account_id=account_id)
    except ZoomAuthRequired:
        raise
    except Exception as e:
        print(f"[ZOOM] Failed to list upcoming meetings: {e}")
        return None

    for meeting in meetings:
        topic = (meeting.get("topic") or "").strip().lower()
        zoom_start = parse_zoom_start_time(meeting.get("start_time", ""))

        if not topic or not zoom_start:
            continue

        minutes_apart = abs((zoom_start - expected_start).total_seconds()) / 60.0
        if minutes_apart > ZOOM_MATCH_WINDOW_MINUTES:
            continue

        if provider_slug(provider) == "essex":
            topic_matches = (topic == expected_topic)
        else:
            topic_matches = (
                topic == expected_topic
                or (provider_text in topic and title_text in topic and date_text in topic)
                or title_text in topic
            )

        if not topic_matches:
            continue

        full_details = get_zoom_meeting_details_by_id(meeting.get("id", ""), account_id=account_id)
        if not full_details:
            continue

        if not db_has_valid_zoom_details(full_details):
            continue

        print(
            f"[ZOOM] Found matching existing meeting: "
            f"{full_details['meeting_id']} | {full_details['topic']}"
        )
        return full_details

    print("[ZOOM] No matching existing meeting found.")
    return None


def create_zoom_meeting(course, account_id=None):
    if not ALLOW_ZOOM_CREATION:
        raise RuntimeError("Zoom creation is disabled for this plan.")

    print(f"[ZOOM] Creating meeting for provider='{course['provider']}' using linked account_id='{account_id or 'NONE'}'")
    access_token = get_zoom_access_token(account_id=account_id)
    if not access_token:
        raise RuntimeError(f"No Zoom OAuth token available for linked account '{account_id or 'NONE'}'")

    start_dt = datetime.strptime(course["date_time"], "%Y-%m-%d %H:%M").replace(
        tzinfo=LOCAL_TIMEZONE
    )

    topic = format_zoom_topic(course["provider"], course["title"], course["date_time"])

    payload = {
        "topic": topic,
        "type": 2,
        "start_time": start_dt.isoformat(),
        "duration": course.get("duration_minutes") or 180,
        "timezone": "Europe/London",
        "agenda": f"{course['title']} for {course['provider']}",
        "settings": get_effective_zoom_settings(),
    }

    response = requests.post(
        f"https://api.zoom.us/v2/users/{ZOOM_USER_ID}/meetings",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    if response.status_code == 401:
        message = zoom_auth_error_message(account_id)
        print(f"[ZOOM] {message}")
        raise ZoomAuthRequired(message)

    response.raise_for_status()
    data = response.json()

    print(
        f"[ZOOM] Created new meeting for: {course['title']} "
        f"({course['date_time']}) | topic='{topic}'"
    )

    return {
        "meeting_id": normalize_zoom_meeting_id(data.get("id", "")),
        "meeting_link": data.get("join_url", "") or "",
        "meeting_password": data.get("password", "") or "",
        "zoom_start_url": data.get("start_url", "") or "",
    }


def get_or_create_zoom_details_for_course(conn, provider, title, date_time, duration_minutes, account_id=None):
    existing_zoom_data = get_zoom_details_for_course(conn, title, date_time, provider)

    if db_has_valid_zoom_details(existing_zoom_data):
        normalized_db_meeting_id = normalize_zoom_meeting_id(existing_zoom_data.get("meeting_id", ""))
        print(f"[DB] Valid Zoom details found in SQLite. Checking Zoom for meeting_id={normalized_db_meeting_id} ...")

        live_zoom_data = get_zoom_meeting_details_by_id(normalized_db_meeting_id, account_id=account_id)
        if live_zoom_data and db_has_valid_zoom_details(live_zoom_data):
            print("[DB] Stored Zoom meeting still exists in Zoom. Reusing it.")

            meeting_password = (live_zoom_data.get("meeting_password") or "").strip()
            if not meeting_password:
                print("[DB] Live Zoom meeting has no password. Leaving blank.")
            else:
                update_meeting_password_in_db(
                    conn=conn,
                    title=title,
                    date_time=date_time,
                    provider=provider,
                    meeting_password=meeting_password,
                )

            zoom_data_to_save = {
                "meeting_link": (live_zoom_data.get("meeting_link") or "").strip(),
                "meeting_id": normalize_zoom_meeting_id(live_zoom_data.get("meeting_id", "")),
                "meeting_password": meeting_password,
            }
            save_zoom_details_to_course(
                conn=conn,
                title=title,
                date_time=date_time,
                provider=provider,
                zoom_data=zoom_data_to_save,
            )
            return "use_db", zoom_data_to_save

        print("[DB] Stored Zoom meeting no longer exists in Zoom or is no longer usable. Will find or create a replacement.")

    elif existing_zoom_data:
        print(f"[DB] Ignoring invalid or legacy Zoom data: {existing_zoom_data}")

    matching_zoom = find_matching_zoom_meeting(provider, title, date_time, account_id=account_id)
    if matching_zoom:
        zoom_data_to_save = {
            "meeting_link": matching_zoom.get("meeting_link", ""),
            "meeting_id": normalize_zoom_meeting_id(matching_zoom.get("meeting_id", "")),
            "meeting_password": matching_zoom.get("meeting_password", ""),
        }
        save_zoom_details_to_course(
            conn=conn,
            title=title,
            date_time=date_time,
            provider=provider,
            zoom_data=zoom_data_to_save,
        )
        print("[ZOOM] Stored matching existing Zoom meeting in SQLite.")
        return "use_existing_zoom", zoom_data_to_save

    print("[ZOOM] No usable DB or Zoom meeting found. Creating a new one now...")

    course_for_zoom = {
        "provider": provider,
        "title": title,
        "date_time": date_time,
        "duration_minutes": duration_minutes,
    }
    created_zoom_data = create_zoom_meeting(course_for_zoom, account_id=account_id)

    save_zoom_details_to_course(
        conn=conn,
        title=title,
        date_time=date_time,
        provider=provider,
        zoom_data=created_zoom_data,
    )
    print("[ZOOM] Stored newly created Zoom details in SQLite.")

    return "created_new", created_zoom_data


def backfill_missing_meeting_passwords():
    print("\nBackfilling missing meeting passwords...")

    cursor.execute("""
        SELECT id, title, date_time, provider, meeting_id
        FROM courses
        WHERE meeting_id IS NOT NULL
          AND meeting_id != ''
          AND (meeting_password IS NULL OR meeting_password = '')
    """)
    rows = cursor.fetchall()

    print(f"Rows needing password backfill: {len(rows)}")

    for course_id, title, date_time, provider, meeting_id in rows:
        try:
            normalized_meeting_id = normalize_zoom_meeting_id(meeting_id)
            if not normalized_meeting_id:
                print(f"Skipping invalid meeting_id for course {course_id}: {meeting_id}")
                continue

            provider_zoom_account_id = get_provider_zoom_account_id(provider)
            meeting_password = fetch_zoom_password_by_meeting_id(normalized_meeting_id, account_id=provider_zoom_account_id)
            if not meeting_password:
                continue

            update_meeting_password_in_db(
                conn=conn,
                title=title,
                date_time=date_time,
                provider=provider,
                meeting_password=meeting_password,
            )

            print(
                f"Backfilled password for {title} ({date_time}) "
                f"[course_id={course_id}]"
            )

        except Exception as e:
            print(
                f"Failed to backfill password for {title} ({date_time}) "
                f"[course_id={course_id}, meeting_id={meeting_id}]: {e}"
            )

# ============================================================
# GENERAL FLOW HELPERS
# ============================================================

def build_course_key(provider, title, date_time_text, fallback=None):
    provider_name = (provider or PROVIDER_NAME).strip() or PROVIDER_NAME
    title_part = (title or '').strip()
    dt_part = (date_time_text or '').strip()
    if title_part and dt_part:
        return f"{provider_name} | {title_part} | {dt_part}"
    if title_part:
        return f"{provider_name} | {title_part}"
    if fallback is not None:
        return f"{provider_name} | row_{fallback}"
    return provider_name


def parse_time_range(time_text):
    text = (time_text or '').strip()
    if 'to' not in text:
        return '', '', None
    start_text, end_text = [part.strip() for part in text.split('to', 1)]
    try:
        start_dt = datetime.strptime(start_text, '%H:%M')
        end_dt = datetime.strptime(end_text, '%H:%M')
        duration_minutes = int((end_dt - start_dt).total_seconds() // 60)
        if duration_minutes <= 0:
            duration_minutes += 24 * 60
    except Exception:
        duration_minutes = None
    return start_text, end_text, duration_minutes


def extract_courses_from_rows(rows, provider_name):
    courses = []
    provider_name = normalize_provider_context(provider_name)
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 7:
            continue
        date_text = (row[1] or '').strip()
        time_text = (row[2] or '').strip()
        title = (row[4] or '').strip()
        status = (row[6] or '').strip()
        if not date_text or not time_text or not title:
            continue
        try:
            db_date_time = convert_portal_date_time_to_db_format(date_text, time_text)
        except Exception:
            continue
        start_text, end_text, duration_minutes = parse_time_range(time_text)
        course_id = build_course_id(provider_name, db_date_time, start_text, title)
        courses.append({
            'id': course_id,
            'provider': provider_name,
            'title': title,
            'date': db_date_time[:10],
            'time': start_text,
            'end_time': end_text,
            'date_time': db_date_time,
            'duration_minutes': duration_minutes or 180,
            'status': status or '',
        })
    return filter_courses_for_requested_scan(courses)


def course_exists_by_id(course_id):
    local_cursor = conn.cursor()
    local_cursor.execute('SELECT 1 FROM courses WHERE id = ? LIMIT 1', (course_id,))
    return local_cursor.fetchone() is not None


def backfill_provider_for_existing_course(course):
    local_cursor = conn.cursor()
    local_cursor.execute(
        """
        UPDATE courses
        SET provider = ?,
            last_seen_at = ?
        WHERE id = ?
        """,
        (course.get('provider', PROVIDER_NAME), datetime.now().strftime('%Y-%m-%d %H:%M:%S'), course['id']),
    )
    conn.commit()

def find_active_same_provider_slot(course):
    """Find an active DB row for the same provider/date/time but a different course id/title."""
    local_cursor = conn.cursor()
    local_cursor.execute(
        """
        SELECT id, title, provider, date_time
        FROM courses
        WHERE provider = ?
          AND date_time = ?
          AND COALESCE(active_in_portal, 1) = 1
          AND id <> ?
        LIMIT 1
        """,
        (course.get('provider', PROVIDER_NAME), course.get('date_time', ''), course.get('id', '')),
    )
    return local_cursor.fetchone()


def retire_same_provider_replacement_if_needed(course):
    """If FOBS has replaced a same-provider course at the same slot, retire the stale row."""
    existing = find_active_same_provider_slot(course)
    if not existing:
        return False

    old_id, old_title, provider, date_time = existing
    if provider_slug(old_title or '') == provider_slug(course.get('title') or ''):
        return False

    now_text = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(
        f"[RECONCILE] Course replaced by provider: {provider} {date_time} "
        f"'{old_title}' -> '{course.get('title', '')}'."
    )
    cursor.execute(
        """
        UPDATE courses
        SET active_in_portal = 0,
            status = ?,
            last_seen_at = ?,
            last_synced_at = ?,
            last_sync_status = ?,
            last_sync_action = ?
        WHERE id = ?
        """,
        ("Replaced", now_text, now_text, "info", "Course replaced by provider", old_id),
    )
    conn.commit()
    return True


def flag_cross_provider_conflicts(course):
    """Flag same-time allocations across different providers for manual review."""
    local_cursor = conn.cursor()
    local_cursor.execute(
        """
        SELECT id, provider, title
        FROM courses
        WHERE date_time = ?
          AND COALESCE(active_in_portal, 1) = 1
          AND lower(COALESCE(provider, '')) <> lower(?)
        """,
        (course.get('date_time', ''), course.get('provider', PROVIDER_NAME)),
    )
    conflicts = local_cursor.fetchall()
    if not conflicts:
        return False

    now_text = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[RECONCILE] Cross-provider conflict detected for {course.get('date_time', '')}: {course.get('provider')} / {conflicts}")
    ids = [row[0] for row in conflicts] + [course.get('id')]
    for course_id in ids:
        cursor.execute(
            """
            UPDATE courses
            SET last_synced_at = ?,
                last_sync_status = ?,
                last_sync_action = ?
            WHERE id = ?
            """,
            (now_text, "needs_attention", "Conflict - check FOBS", course_id),
        )
    conn.commit()
    return True


def run_step(course_key, step, func, success_message='', retries=0):
    attempts = max(0, int(retries)) + 1
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            result = func()
            try:
                if success_message:
                    mark_success(course_key, success_message)
            except Exception:
                pass
            return result
        except Exception as exc:
            last_exc = exc
            try:
                mark_error(course_key, step, f'{type(exc).__name__}: {exc}')
            except Exception:
                pass
            if attempt >= attempts:
                raise
            time.sleep(0.5)
    if last_exc:
        raise last_exc


def safe_goto(page, url, wait_selector=None, timeout=15000):
    page.goto(url, timeout=timeout)
    if wait_selector:
        page.wait_for_selector(wait_selector, timeout=timeout)
    return True


def wait_for_fobs_course_list(page, timeout=15000):
    selectors = ['#endDate', 'input[name="searchCoruses"]', 'tr:has(td.ng-binding)']
    last_error = None
    for selector in selectors:
        try:
            page.wait_for_selector(selector, timeout=timeout)
            return True
        except Exception as exc:
            last_error = exc
    if last_error:
        raise last_error
    return False



def page_looks_like_login_screen(page):
    try:
        return (
            page.locator("#UserName").count() > 0
            and page.locator("#Password").count() > 0
        )
    except Exception:
        return False


def extract_portal_login_error_message(page):
    candidate_selectors = [
        ".validation-summary-errors",
        ".field-validation-error",
        ".text-danger",
        ".alert-danger",
        ".alert",
    ]
    for selector in candidate_selectors:
        try:
            locator = page.locator(selector)
            if locator.count() > 0:
                text = (locator.first.inner_text() or "").strip()
                if text:
                    return text
        except Exception:
            pass

    try:
        body_text = (page.locator("body").inner_text() or "").strip()
    except Exception:
        body_text = ""

    patterns = [
        r"invalid[^\n]*password",
        r"invalid[^\n]*login",
        r"incorrect[^\n]*password",
        r"incorrect[^\n]*username",
        r"login[^\n]*failed",
        r"unsuccessful[^\n]*login",
        r"account[^\n]*locked",
        r"too many[^\n]*attempts",
    ]
    for pattern in patterns:
        match = re.search(pattern, body_text, flags=re.IGNORECASE)
        if match:
            return match.group(0).strip()

    return ""

# ============================================================
# PORTAL / SCRAPING HELPERS
# ============================================================

def extract_summary_fields_from_body_text(body_text):
    title_match = re.search(r"Type\s+(.+)", body_text)
    date_match = re.search(r"Date\s+(.+)", body_text)
    time_match = re.search(r"Time\s+(.+)", body_text)

    title_text = title_match.group(1).strip() if title_match else ""
    date_text = date_match.group(1).strip() if date_match else ""
    time_text = time_match.group(1).strip() if time_match else ""

    return title_text, date_text, time_text


def convert_portal_date_time_to_db_format(portal_date_text, portal_time_text):
    start_time = portal_time_text.split("to")[0].strip()
    combined = f"{portal_date_text} {start_time}"

    dt = datetime.strptime(combined, "%A, %d %B %Y %H:%M")
    return dt.strftime("%Y-%m-%d %H:%M")


def course_starts_within_next_minutes(date_time_text, minutes=60):
    try:
        course_dt = datetime.strptime((date_time_text or '').strip(), "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TIMEZONE)
    except Exception:
        return False

    now_local = datetime.now(LOCAL_TIMEZONE)
    delta_seconds = (course_dt - now_local).total_seconds()
    return 0 <= delta_seconds < (minutes * 60)


def course_row_is_within_next_minutes(row_cells, minutes=60):
    if not isinstance(row_cells, (list, tuple)) or len(row_cells) < 3:
        return False, ""

    date_text = (row_cells[1] or '').strip()
    time_text = (row_cells[2] or '').strip()
    if not date_text or not time_text:
        return False, ""

    try:
        db_date_time = convert_portal_date_time_to_db_format(date_text, time_text)
    except Exception:
        return False, ""

    return course_starts_within_next_minutes(db_date_time, minutes=minutes), db_date_time


def course_status_blocks_sync(status_text):
    """Return True only for statuses that mean the course itself is gone.

    FOBS can show statuses such as Disabled to mean bookings are closed, not
    that trainer/Zoom assignment is blocked. Treat unknown provider statuses as
    actionable so future providers are not skipped just because they use
    different wording.
    """
    status = re.sub(r"[^a-z0-9]+", " ", (status_text or "").strip().lower()).strip()
    if not status:
        return False
    blocking_words = {
        "cancelled",
        "canceled",
        "deleted",
        "removed",
        "withdrawn",
    }
    return any(word in status.split() for word in blocking_words)




def sync_window_days():
    # Dashboard scan selections must never extend beyond the account licence window.
    # Free accounts: 21 days. Paid accounts: 84 days / 12 weeks.
    if TRAINERMATE_SCAN_DAYS:
        return min(TRAINERMATE_SCAN_DAYS, LICENSED_SYNC_WINDOW_DAYS)
    return LICENSED_SYNC_WINDOW_DAYS


def start_of_sync_window_date():
    if target_course_check_enabled():
        target_date = target_course_fobs_date()
        if target_date:
            return target_date
    return datetime.now(LOCAL_TIMEZONE).strftime("%d/%m/%Y")


def end_of_sync_window_date():
    days = sync_window_days()
    if days:
        return (datetime.now(LOCAL_TIMEZONE) + timedelta(days=days)).strftime("%d/%m/%Y")
    return (datetime.now(LOCAL_TIMEZONE) + relativedelta(months=6, day=31)).strftime("%d/%m/%Y")


def end_of_provider_import_date():
    """How far ahead we ask every provider portal for assigned courses.

    Single-course confirmation checks are anchored to the stored course date.
    If the course is not listed on that exact date, FOBS is treated as not
    having it live and the row is flagged for removal confirmation.
    """
    if target_course_check_enabled():
        target_date = target_course_fobs_date()
        if target_date:
            return target_date
    if TRAINERMATE_SCAN_DAYS:
        return end_of_sync_window_date()
    return (datetime.now(LOCAL_TIMEZONE) + timedelta(days=PROVIDER_IMPORT_LOOKAHEAD_DAYS)).strftime("%d/%m/%Y")


def sync_window_end_datetime():
    days = sync_window_days()
    if not days:
        return None
    return (datetime.now(LOCAL_TIMEZONE) + timedelta(days=days, hours=23, minutes=59, seconds=59))


def course_is_inside_sync_window(db_date_time: str) -> bool:
    end_dt = sync_window_end_datetime()
    if not end_dt:
        return True
    try:
        start_dt = datetime.now(LOCAL_TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
        course_dt = datetime.strptime(db_date_time, "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TIMEZONE)
        return start_dt <= course_dt <= end_dt
    except Exception:
        return True


def mask_meeting_id(value):
    digits = normalize_zoom_meeting_id(value)
    if not digits:
        return ""
    if len(digits) <= 4:
        return "*" * len(digits)
    return ("*" * (len(digits) - 4)) + digits[-4:]


def safe_zoom_log_data(zoom_data):
    zoom_data = zoom_data or {}
    return {
        "meeting_id": mask_meeting_id(zoom_data.get("meeting_id", "")),
        "meeting_link": "present" if zoom_data.get("meeting_link") else "missing",
        "meeting_password": "present" if zoom_data.get("meeting_password") else "missing",
    }


def extract_summary_fields_from_details_page(page):
    body_text = page.locator("body").inner_text()
    return extract_summary_fields_from_body_text(body_text)

def course_has_existing_zoom_details(page):
    try:
        portal_zoom = extract_portal_zoom_details(page)
        if normalize_zoom_meeting_id(portal_zoom.get("meeting_id", "")):
            print("[FOBS] Detected existing Meeting ID on details page.")
            return True
        if (portal_zoom.get("meeting_link") or "").strip():
            print("[FOBS] Detected existing Join Meeting link on details page.")
            return True
        if (portal_zoom.get("meeting_password") or "").strip():
            print("[FOBS] Detected existing Meeting Password on details page.")
            return True
    except Exception as exc:
        print(f"[FOBS] Structured Zoom detail detection failed: {exc}")

    try:
        body_text = page.locator("body").inner_text()
    except Exception:
        body_text = ""

    patterns = [
        r"Meeting ID\s*[:\-]?\s*\d",
        r"Meeting Password\s*[:\-]?\s*[A-Za-z0-9@._\-]+",
        r"Join Meeting",
        r"zoom\.us/j/",
        r"zoom\.us/w/",
    ]
    found = any(re.search(pattern, body_text, flags=re.IGNORECASE) for pattern in patterns)
    if found:
        print("[FOBS] Detected existing Zoom details from page text.")
    else:
        print("[FOBS] No existing Zoom details detected on details page.")
    return found


def format_meeting_id_for_display(meeting_id):
    digits = re.sub(r"\D", "", str(meeting_id or ""))

    if len(digits) == 11:
        return f"{digits[:3]} {digits[3:7]} {digits[7:]}"
    if len(digits) == 10:
        return f"{digits[:3]} {digits[3:6]} {digits[6:]}"
    if len(digits) == 9:
        return f"{digits[:3]} {digits[3:6]} {digits[6:]}"

    return digits


def click_save_button(page):
    try:
        print("[PORTAL] Attempting to click Save...")

        save_button = page.get_by_role("button", name="Save")
        if save_button.count() == 0:
            save_button = page.get_by_text("Save", exact=True)

        if save_button.count() == 0:
            print("[PORTAL] Save button not found.")
            return False

        save_button.first.click()
        print("[PORTAL] Save clicked.")
        return True
    except Exception as e:
        print(f"[PORTAL] Save click failed: {e}")
        return False


def confirm_save_success(page, expected_zoom_data=None):
    try:
        print("[PORTAL] Checking for save success...")
        page.wait_for_timeout(2500)

        current_url = page.url or ""
        if "CoursesBookedOn" in current_url:
            print("[PORTAL] Redirect detected after save.")
            return True

        body_text = page.locator("body").inner_text()
        lowered = body_text.lower()

        if "saved" in lowered or "success" in lowered or "updated" in lowered:
            print("[PORTAL] Success message detected.")
            return True

        if expected_zoom_data:
            meeting_id_digits = normalize_zoom_meeting_id(expected_zoom_data.get("meeting_id", ""))
            meeting_link = (expected_zoom_data.get("meeting_link") or "").strip()
            if meeting_id_digits and meeting_id_digits in re.sub(r"\D", "", body_text):
                print("[PORTAL] Meeting ID visible after save.")
                return True
            if meeting_link and meeting_link in body_text:
                print("[PORTAL] Meeting URL visible after save.")
                return True

        print("[PORTAL] Save success not confirmed.")
        return False
    except Exception as e:
        print(f"[PORTAL] Error checking save success: {e}")
        return False


def extract_portal_zoom_details(page):
    body_text = page.locator("body").inner_text()

    portal_meeting_id = ""
    portal_meeting_password = ""

    for label in ("Meeting ID", "Zoom Meeting ID"):
        try:
            field = page.get_by_label(label, exact=True)
            if field.count() > 0:
                portal_meeting_id = normalize_zoom_meeting_id(field.first.input_value(timeout=1000))
                if portal_meeting_id:
                    break
        except Exception:
            pass

    if not portal_meeting_id:
        meeting_id_match = re.search(
            r"(?:Zoom\s+)?Meeting\s*ID\s*[:\-]?\s*([\d\s]{9,24})",
            body_text,
            flags=re.IGNORECASE,
        )
        portal_meeting_id = normalize_zoom_meeting_id(meeting_id_match.group(1)) if meeting_id_match else ""

    for label in ("Meeting Password", "Meeting Passcode", "Passcode", "Password"):
        try:
            field = page.get_by_label(label, exact=True)
            if field.count() > 0:
                portal_meeting_password = (field.first.input_value(timeout=1000) or "").strip()
                if portal_meeting_password:
                    break
        except Exception:
            pass

    if not portal_meeting_password:
        password_match = re.search(
            r"(?:Meeting\s*)?(?:Password|Passcode)\s*(?:[:\-]|\s)\s*([A-Za-z0-9@._\-]{4,})",
            body_text,
            flags=re.IGNORECASE,
        )
        portal_meeting_password = (password_match.group(1).strip() if password_match else "")
        if portal_meeting_password.lower() in {"was", "were", "will", "with", "from", "this", "that"}:
            portal_meeting_password = ""

    join_link = ""
    try:
        join_link_locator = page.get_by_role("link", name="Join Meeting")
        if join_link_locator.count() > 0:
            join_link = (join_link_locator.first.get_attribute("href") or "").strip()
    except Exception:
        join_link = ""
    if not join_link:
        try:
            links = page.locator("a[href*='zoom.us/']")
            for index in range(min(links.count(), 20)):
                href = (links.nth(index).get_attribute("href") or "").strip()
                if is_probably_zoom_join_link(href):
                    join_link = href
                    break
        except Exception:
            join_link = ""

    return {
        "meeting_id": portal_meeting_id,
        "meeting_link": join_link,
        "meeting_password": portal_meeting_password,
    }


def _live_zoom_passcode(meeting):
    if not isinstance(meeting, dict):
        return ""
    settings = meeting.get("settings")
    settings_password = ""
    if isinstance(settings, dict):
        settings_password = (settings.get("password") or "").strip()
    return (meeting.get("password") or meeting.get("passcode") or settings_password or "").strip()


def get_portal_zoom_truth(page, account_id=None):
    """Compare FOBS Zoom details against live Zoom truth.

    Returns a dict with:
      state: match | mismatch | missing_fobs_zoom | unverifiable
      reason: short plain-English reason suitable for sync state text
      portal_zoom: extracted FOBS meeting details
      live_meeting: Zoom API meeting object (if available)
    """
    portal_zoom = extract_portal_zoom_details(page)
    meeting_id = normalize_zoom_meeting_id(portal_zoom.get("meeting_id", ""))

    if not meeting_id:
        return {
            "state": "missing_fobs_zoom",
            "reason": "FOBS has no Zoom details",
            "portal_zoom": portal_zoom,
            "live_meeting": None,
        }

    if not get_zoom_access_token(account_id=account_id):
        return {
            "state": "unverifiable",
            "reason": "Zoom verification unavailable",
            "portal_zoom": portal_zoom,
            "live_meeting": None,
        }

    try:
        live_meeting = get_zoom_meeting_details_by_id(meeting_id, account_id=account_id)
    except Exception as exc:
        print(f"[ZOOM] Could not verify portal meeting by exact Zoom Meeting ID: {exc}")
        if "No Zoom OAuth token available" in str(exc) or "No working OAuth token available" in str(exc):
            return {
                "state": "unverifiable",
                "reason": "Zoom verification unavailable",
                "portal_zoom": portal_zoom,
                "live_meeting": None,
            }
        live_meeting = None
        exact_lookup_failed = True
    else:
        exact_lookup_failed = False

    if live_meeting:
        portal_password = (portal_zoom.get("meeting_password") or "").strip()
        live_password = _live_zoom_passcode(live_meeting)
        if portal_password and live_password and portal_password != live_password:
            print(
                f"[ZOOM] Portal meeting {mask_meeting_id(meeting_id)} exists, but passcode differs between FOBS and Zoom."
            )
            return {
                "state": "mismatch",
                "reason": "passcode mismatch",
                "portal_zoom": portal_zoom,
                "live_meeting": live_meeting,
            }

        listed_topic = (live_meeting.get("topic") or "").strip()
        listed_start = (live_meeting.get("start_time") or "").strip()
        if listed_topic or listed_start:
            print(
                f"[ZOOM] Portal meeting {mask_meeting_id(meeting_id)} was verified directly in Zoom "
                f"(topic='{listed_topic}', start_time='{listed_start}')."
            )
        else:
            print(f"[ZOOM] Portal meeting {mask_meeting_id(meeting_id)} was verified directly in Zoom.")
        return {
            "state": "match",
            "reason": "meeting id and passcode verified",
            "portal_zoom": portal_zoom,
            "live_meeting": live_meeting,
        }

    if not exact_lookup_failed:
        print(f"[ZOOM] Portal meeting {mask_meeting_id(meeting_id)} does not exist in Zoom by exact Meeting ID.")
        return {
            "state": "mismatch",
            "reason": "meeting id mismatch (meeting missing/deleted in Zoom)",
            "portal_zoom": portal_zoom,
            "live_meeting": None,
        }

    try:
        visible_meetings = list_upcoming_zoom_meetings(account_id=account_id)
    except Exception as exc:
        print(f"[ZOOM] Could not list visible Zoom meetings for verification fallback: {exc}")
        if "No Zoom OAuth token available" in str(exc) or "No working OAuth token available" in str(exc):
            return {
                "state": "unverifiable",
                "reason": "Zoom verification unavailable",
                "portal_zoom": portal_zoom,
                "live_meeting": None,
            }
        return {
            "state": "unverifiable",
            "reason": "Zoom verification unavailable",
            "portal_zoom": portal_zoom,
            "live_meeting": None,
        }

    matched_visible_meeting = None
    for meeting in visible_meetings:
        listed_meeting_id = normalize_zoom_meeting_id(meeting.get("id", ""))
        if listed_meeting_id == meeting_id:
            matched_visible_meeting = meeting
            break

    if not matched_visible_meeting:
        print(
            f"[ZOOM] Portal meeting {mask_meeting_id(meeting_id)} could not be confirmed from the Zoom user's "
            "visible scheduled meetings list after exact lookup failed. Leaving FOBS unchanged."
        )
        return {
            "state": "mismatch",
            "reason": "meeting id mismatch (meeting missing/deleted in Zoom)",
            "portal_zoom": portal_zoom,
            "live_meeting": None,
        }

    listed_topic = (matched_visible_meeting.get("topic") or "").strip()
    listed_start = (matched_visible_meeting.get("start_time") or "").strip()

    if listed_topic or listed_start:
        print(
            f"[ZOOM] Portal meeting {mask_meeting_id(meeting_id)} is visible in Zoom "
            f"(topic='{listed_topic}', start_time='{listed_start}')."
        )
    else:
        print(f"[ZOOM] Portal meeting {mask_meeting_id(meeting_id)} is visible in Zoom.")

    return {
        "state": "match",
        "reason": "meeting id verified",
        "portal_zoom": portal_zoom,
        "live_meeting": matched_visible_meeting,
    }


def portal_zoom_details_are_live(page, account_id=None):
    verdict = get_portal_zoom_truth(page, account_id=account_id)
    state = (verdict.get("state") or "").strip().lower()
    if state == "match":
        return True
    if state == "unverifiable":
        return None
    return False

def fill_zoom_fields_if_empty(page, zoom_join_url, zoom_meeting_id, zoom_password=""):
    print("\nChecking whether Zoom fields are safe to write...")

    meeting_url = page.get_by_label("Meeting URL", exact=True)
    meeting_id = page.get_by_label("Meeting ID", exact=True)
    meeting_password = page.get_by_label("Meeting Password", exact=True)

    if meeting_url.count() == 0:
        print("Meeting URL field not found.")
        return False

    if meeting_id.count() == 0:
        print("Meeting ID field not found.")
        return False

    if meeting_password.count() == 0:
        print("Meeting Password field not found.")
        return False

    current_url = meeting_url.input_value().strip()
    current_id = meeting_id.input_value().strip()
    current_password = meeting_password.input_value().strip()

    print("Current Meeting URL present:", bool(current_url))
    print("Current Meeting ID present:", bool(current_id))
    print("Current Meeting Password present:", bool(current_password))

    if current_url or current_id or current_password:
        print("Existing Zoom details already present. Will NOT overwrite.")
        return False

    print("All Zoom fields are empty. Writing Zoom details now...")

    if zoom_join_url:
        meeting_url.fill(str(zoom_join_url))

    if zoom_meeting_id:
        formatted_id = format_meeting_id_for_display(zoom_meeting_id)
        print(f"[PORTAL] Writing formatted Meeting ID: {formatted_id}")
        meeting_id.fill(formatted_id)

    if zoom_password:
        meeting_password.fill(str(zoom_password))
    else:
        print("No meeting password stored, leaving password field blank.")

    print("Zoom details filled into portal form.")
    return True



def open_virtual_classroom_edit_screen(page):
    exact_labels = [
        "Edit Zoom Meeting Details",
        "Edit Virtual Classroom Details",
    ]

    for label in exact_labels:
        try:
            locator = page.get_by_text(label, exact=True)
            if locator.count() > 0:
                locator.first.click(timeout=5000)
                return label
        except Exception:
            pass

    fuzzy_patterns = [
        re.compile(r"edit\s+zoom\s+meeting\s+details", re.I),
        re.compile(r"edit\s+virtual\s+classroom\s+details", re.I),
        re.compile(r"edit.*virtual.*classroom", re.I),
        re.compile(r"edit.*zoom", re.I),
        re.compile(r"edit.*meeting.*details", re.I),
    ]

    selectors = ["a", "button", "[role='button']", "input[type='submit']", "input[type='button']"]
    seen = set()

    for selector in selectors:
        try:
            locators = page.locator(selector)
            count = min(locators.count(), 80)
        except Exception:
            continue

        for idx in range(count):
            try:
                candidate = locators.nth(idx)
                text_parts = [
                    (candidate.inner_text(timeout=1000) or "").strip(),
                    (candidate.get_attribute("title") or "").strip(),
                    (candidate.get_attribute("aria-label") or "").strip(),
                    (candidate.get_attribute("value") or "").strip(),
                ]
                candidate_text = " | ".join(part for part in text_parts if part)
                key = candidate_text.lower()
                if not key or key in seen:
                    continue
                seen.add(key)

                if any(pattern.search(candidate_text) for pattern in fuzzy_patterns):
                    candidate.click(timeout=5000)
                    return candidate_text
            except Exception:
                continue

    raise TimeoutError("Could not find a matching edit Zoom / virtual classroom details control on the page.")


def test_open_zoom_edit_screen(page, conn, provider, courses_url=None, max_rows=None):
    print("\nTesting Zoom edit screen...")

    stats = {
        "rows_seen": 0,
        "processed": 0,
        "updated": 0,
        "skipped": 0,
        "failed": 0,
    }
    processed_course_keys = set()

    def restore_filtered_course_list():
        target_courses_url = (courses_url or COURSES_URL).strip()
        target_end_date = end_of_provider_import_date()

        safe_goto(page, target_courses_url, wait_selector="#endDate", timeout=15000)

        # Single-course checks must stay anchored to the stored course date.
        # When returning from a detail page, FOBS can reset the list back to
        # today; without resetting startDate here the bot appears to restart
        # a broad scan.
        target_start_date = start_of_sync_window_date()
        if BOT_MODE == "urgent_14d" or target_course_check_enabled():
            page.evaluate(
                """([selector, value]) => {
                    const el = document.querySelector(selector);
                    if (!el) return;
                    el.removeAttribute('readonly');
                    el.value = value;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                ["#startDate", target_start_date],
            )

        page.evaluate(
            """([selector, value]) => {
                const el = document.querySelector(selector);
                if (!el) return;
                el.removeAttribute('readonly');
                el.value = value;
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            }""",
            ["#endDate", target_end_date],
        )
        page.click("input[name='searchCoruses']")
        wait_for_fobs_course_list(page, timeout=15000)
        page.wait_for_timeout(1200)
        return True

    def back_to_list_safely():
        try:
            if page.get_by_text("Back to List", exact=True).count() > 0:
                page.get_by_text("Back to List", exact=True).first.click()
                return restore_filtered_course_list()
        except Exception:
            pass

        try:
            if page.get_by_text("Back", exact=True).count() > 0:
                page.get_by_text("Back", exact=True).first.click()
                page.wait_for_timeout(800)
                if page.get_by_text("Back to List", exact=True).count() > 0:
                    page.get_by_text("Back to List", exact=True).first.click()
                    return restore_filtered_course_list()
        except Exception:
            pass

        try:
            return restore_filtered_course_list()
        except Exception:
            return False

    def snapshot_course_rows():
        wait_for_fobs_course_list(page, timeout=15000)
        rows = page.locator("tr:has(td.ng-binding)")
        detail_icons = page.locator("i.fa-solid.fa-clipboard-list[title='Course details']")
        row_count = min(rows.count(), detail_icons.count())
        targets = []

        for i in range(row_count):
            row_locator = rows.nth(i)
            row_cells_locator = row_locator.locator("td")
            row_cells = [row_cells_locator.nth(j).inner_text().strip() for j in range(row_cells_locator.count())]
            date_text = (row_cells[1] if len(row_cells) > 1 else "").strip()
            time_text = (row_cells[2] if len(row_cells) > 2 else "").strip()
            title_text = (row_cells[4] if len(row_cells) > 4 else "").strip()
            status_text = (row_cells[6] if len(row_cells) > 6 else "").strip()
            targets.append({
                "row_number": i,
                "row_cells": row_cells,
                "date_text": date_text,
                "time_text": time_text,
                "title_text": title_text,
                "status_text": status_text,
                "course_key": build_course_key(provider, title_text, f"{date_text} {time_text}".strip(), fallback=i),
            })

        stats["rows_seen"] = len(targets)
        return targets

    def find_matching_row_index(target):
        wait_for_fobs_course_list(page, timeout=15000)
        rows = page.locator("tr:has(td.ng-binding)")
        row_count = rows.count()

        for i in range(row_count):
            row_locator = rows.nth(i)
            row_cells_locator = row_locator.locator("td")
            row_cells = [row_cells_locator.nth(j).inner_text().strip() for j in range(row_cells_locator.count())]
            row_date = (row_cells[1] if len(row_cells) > 1 else "").strip()
            row_time = (row_cells[2] if len(row_cells) > 2 else "").strip()
            row_title = (row_cells[4] if len(row_cells) > 4 else "").strip()
            row_status = (row_cells[6] if len(row_cells) > 6 else "").strip()
            if row_date == target["date_text"] and row_time == target["time_text"] and row_title == target["title_text"] and row_status == target["status_text"]:
                return i, row_cells

        return None, []

    restore_filtered_course_list()
    row_targets = snapshot_course_rows()

    if target_course_check_enabled():
        target_dt = (TRAINERMATE_TARGET_COURSE_DATE_TIME or '').strip()
        target_title = provider_slug(TRAINERMATE_TARGET_COURSE_TITLE or '')
        exact_targets = []
        for target in row_targets:
            try:
                row_db_dt = convert_portal_date_time_to_db_format(target.get("date_text", ""), target.get("time_text", ""))
            except Exception:
                row_db_dt = ""
            row_title_slug = provider_slug(target.get("title_text") or "")
            if row_db_dt == target_dt and (not target_title or row_title_slug == target_title):
                exact_targets.append(target)
        print(f"[TARGET] Exact FOBS detail rows for target course: {len(exact_targets)} of {len(row_targets)} row(s) on the filtered date.")
        row_targets = exact_targets

    if max_rows is not None:
        row_targets = row_targets[:max_rows]

    if not row_targets:
        if target_course_check_enabled():
            print("[TARGET] Target course was not present on the exact FOBS date. No broad fallback scan will be run.")
        else:
            print("No course details icons found.")
        print("\nFOBS course processing summary:", stats)
        return stats

    for target in row_targets:
        if maybe_stop():
            return stats

        fallback_course_key = target["course_key"]
        ensure_course(fallback_course_key)
        print(f"\nChecking row {target['row_number']}...")

        try:
            imminent_from_row, imminent_db_date_time = course_row_is_within_next_minutes(target["row_cells"], minutes=60)
            if imminent_from_row:
                course_key = build_course_key(provider, target["title_text"], imminent_db_date_time, fallback=target["row_number"])
                ensure_course(course_key)
                print(f"[SAFETY] Skipping imminent course from list row without opening details: {course_key}")
                mark_skipped(course_key, "Skipped before opening details: course starts within next 60 minutes")
                stats["skipped"] += 1
                continue

            if not restore_filtered_course_list():
                raise Exception("Could not restore filtered FOBS course list before opening next course")

            matched_index, _ = find_matching_row_index(target)
            if matched_index is None:
                print(f"[FOBS] Could not re-find row after returning to list. Skipping target: {fallback_course_key}")
                mark_skipped(fallback_course_key, "Could not re-find row on refreshed filtered list")
                stats["skipped"] += 1
                continue

            detail_icons = page.locator("i.fa-solid.fa-clipboard-list[title='Course details']")
            if matched_index >= detail_icons.count():
                print(f"[FOBS] Matching row index exceeded live detail icon count. Skipping target: {fallback_course_key}")
                mark_skipped(fallback_course_key, "Matching row was found but detail icon was unavailable")
                stats["skipped"] += 1
                continue

            run_step(
                fallback_course_key,
                "open_course_details",
                lambda: detail_icons.nth(matched_index).click(timeout=10000),
                success_message="Opened course details",
                retries=1,
            )
            page.wait_for_timeout(3000)

            fobs_course_url = (page.url or "").strip()
            print("Opened course details page:", fobs_course_url)

            title_text, date_text, time_text = run_step(
                fallback_course_key,
                "read_course_summary",
                lambda: extract_summary_fields_from_details_page(page),
                success_message="Read course summary",
                retries=1,
            )
            course_key = build_course_key(provider, title_text, f"{date_text} {time_text}", fallback=target["row_number"])
            ensure_course(course_key)

            print("FOBS title:", title_text)
            print("FOBS date:", date_text)
            print("FOBS time:", time_text)

            if course_key in processed_course_keys:
                print(f"[FOBS] Course already handled in this run, skipping duplicate refresh hit: {course_key}")
                mark_skipped(course_key, "Already handled earlier in this run")
                stats["skipped"] += 1
                back_to_list_safely()
                continue

            if not title_text or not date_text or not time_text:
                mark_skipped(fallback_course_key, "Missing course summary fields in FOBS view")
                stats["skipped"] += 1
                print("Could not extract enough course details from page. Skipping row.")
                back_to_list_safely()
                continue

            stats["processed"] += 1

            db_date_time = run_step(
                course_key,
                "convert_fobs_datetime",
                lambda: convert_portal_date_time_to_db_format(date_text, time_text),
                success_message="Converted FOBS date/time",
                retries=0,
            )
            _, _, duration_minutes = run_step(
                course_key,
                "parse_duration",
                lambda: parse_time_range(time_text),
                success_message="Parsed course duration",
                retries=0,
            )
            if duration_minutes is None:
                duration_minutes = 180

            print("Converted DB date_time:", db_date_time)
            update_course_fobs_url(conn, title_text, db_date_time, provider, fobs_course_url)

            if not course_is_inside_sync_window(db_date_time):
                print(f"[WINDOW] Skipping course outside current sync window: {course_key}")
                mark_skipped(course_key, "Outside current sync window")
                stats["skipped"] += 1
                back_to_list_safely()
                continue

            if course_starts_within_next_minutes(db_date_time, minutes=60):
                print(f"[SAFETY] Skipping imminent course after opening details without editing: {course_key}")
                mark_skipped(course_key, "Skipped after verification: course starts within next 60 minutes")
                stats["skipped"] += 1
                back_to_list_safely()
                continue

            provider_config = get_provider_config(provider)
            provider_manages_zoom = bool(provider_config.get("provider_manages_zoom"))
            never_overwrite_existing_zoom = bool(provider_config.get("never_overwrite_existing_zoom", False))
            provider_zoom_account_id = get_provider_zoom_account_id(provider)

            has_zoom_details = run_step(
                course_key,
                "inspect_fobs_zoom",
                lambda: course_has_existing_zoom_details(page),
                success_message="Checked existing FOBS Zoom details",
                retries=1,
            )
            print("[FOBS] Zoom details already present:", has_zoom_details)

            if provider_manages_zoom:
                message = "Provider managed"
                if has_zoom_details:
                    message = "Provider managed: existing provider Zoom left unchanged"
                else:
                    message = "Provider managed: no Zoom changes made by TrainerMate"
                print(f"[FOBS] {message}")
                update_course_sync_state(conn, title_text, db_date_time, provider, "skipped", message)
                mark_skipped(course_key, message)
                processed_course_keys.add(course_key)
                stats["skipped"] += 1
                back_to_list_safely()
                continue

            if has_zoom_details:
                fobs_zoom_truth = run_step(
                    course_key,
                    "verify_fobs_zoom_live",
                    lambda: get_portal_zoom_truth(page, account_id=provider_zoom_account_id),
                    success_message="Verified FOBS Zoom against live Zoom",
                    retries=0,
                )
                truth_state = (fobs_zoom_truth or {}).get("state") if isinstance(fobs_zoom_truth, dict) else ""
                portal_zoom = (fobs_zoom_truth.get("portal_zoom") or {}) if isinstance(fobs_zoom_truth, dict) else {}
                portal_zoom_to_save = {
                    "meeting_link": (portal_zoom.get("meeting_link") or "").strip(),
                    "meeting_id": normalize_zoom_meeting_id(portal_zoom.get("meeting_id", "")),
                    "meeting_password": (portal_zoom.get("meeting_password") or "").strip(),
                }
                if db_has_valid_zoom_details(portal_zoom_to_save):
                    save_zoom_details_to_course(
                        conn=conn,
                        title=title_text,
                        date_time=db_date_time,
                        provider=provider,
                        zoom_data=portal_zoom_to_save,
                    )
                    print("[DB] Saved Zoom details currently shown in FOBS.")
                if truth_state == "match":
                    if db_has_valid_zoom_details(portal_zoom_to_save):
                        print("[DB] Synced SQLite Zoom details to the live FOBS meeting before skipping.")
                        topic_ok, topic_msg = ensure_zoom_topic_matches_course(
                            portal_zoom_to_save.get("meeting_id"), provider, title_text, db_date_time,
                            account_id=provider_zoom_account_id,
                        )
                        if topic_ok:
                            print("[ZOOM] Meeting topic checked/updated for current course title.")
                        else:
                            print(f"[ZOOM] Meeting topic was not updated: {topic_msg}")
                    print("[FOBS] Existing FOBS Zoom details are still live. Skipping without opening the edit screen.")
                    update_course_sync_state(conn, title_text, db_date_time, provider, "skipped", "FOBS + Zoom OK")
                    mark_skipped(course_key, "FOBS + Zoom OK")
                    processed_course_keys.add(course_key)
                    stats["skipped"] += 1
                    back_to_list_safely()
                    continue

                if truth_state == "unverifiable":
                    print("[FOBS] Existing FOBS Zoom details could not be verified because the linked Zoom token is unavailable. Leaving them unchanged.")
                    update_course_sync_state(conn, title_text, db_date_time, provider, "skipped", "Existing FOBS Zoom left unchanged because linked Zoom verification is unavailable")
                    mark_skipped(course_key, "Existing FOBS Zoom left unchanged because linked Zoom verification is unavailable")
                    processed_course_keys.add(course_key)
                    stats["skipped"] += 1
                    back_to_list_safely()
                    continue

                if not ALLOW_ZOOM_REPLACE_ON_MISMATCH:
                    mismatch_reason = ""
                    if isinstance(fobs_zoom_truth, dict):
                        mismatch_reason = (fobs_zoom_truth.get("reason") or "").strip()
                    message = "Zoom link mismatch confirmed on FOBS - manual decision needed"
                    if mismatch_reason:
                        message = f"Zoom link mismatch confirmed on FOBS ({mismatch_reason}) - manual decision needed"
                    print(f"[FOBS] {message}. Leaving FOBS unchanged.")
                    update_course_sync_state(conn, title_text, db_date_time, provider, "needs_attention", message)
                    mark_skipped(course_key, message)
                    processed_course_keys.add(course_key)
                    stats["skipped"] += 1
                    back_to_list_safely()
                    continue
                print("[FOBS] Existing FOBS Zoom details do not tally with live Zoom. Replacement explicitly confirmed by trainer.")
            else:
                print("[FOBS] No Zoom details found on the course details page. A meeting will be created and written.")

            try:
                path_used, zoom_data = run_step(
                    course_key,
                    "resolve_zoom",
                    lambda: get_or_create_zoom_details_for_course(
                        conn=conn,
                        provider=provider,
                        title=title_text,
                        date_time=db_date_time,
                        duration_minutes=duration_minutes,
                        account_id=provider_zoom_account_id,
                    ),
                    success_message="Resolved Zoom details",
                    retries=0,
                )
            except ZoomAuthRequired as exc:
                message = str(exc) or zoom_auth_error_message(provider_zoom_account_id)
                update_course_sync_state(conn, title_text, db_date_time, provider, "needs_attention", message)
                mark_skipped(course_key, message)
                processed_course_keys.add(course_key)
                stats["skipped"] += 1
                print(f"[ZOOM] {message}")
                back_to_list_safely()
                continue

            if not zoom_data:
                update_course_sync_state(conn, title_text, db_date_time, provider, "needs_attention", "No Zoom data available after DB and Zoom checks")
                mark_skipped(course_key, "No Zoom data available after DB and Zoom checks")
                processed_course_keys.add(course_key)
                stats["skipped"] += 1
                print("No Zoom data available after DB/Zoom checks. Skipping row.")
                back_to_list_safely()
                continue

            if not has_zoom_details:
                late_detected_zoom = run_step(
                    course_key,
                    "reinspect_fobs_zoom",
                    lambda: course_has_existing_zoom_details(page),
                    success_message="Rechecked existing FOBS Zoom details",
                    retries=0,
                )
                if late_detected_zoom:
                    print("[FOBS] Existing Zoom details were detected on recheck. Re-running live tally before opening edit screen.")
                    has_zoom_details = True
                    fobs_zoom_truth = run_step(
                        course_key,
                        "verify_fobs_zoom_live_recheck",
                        lambda: get_portal_zoom_truth(page, account_id=provider_zoom_account_id),
                        success_message="Verified FOBS Zoom against live Zoom after recheck",
                        retries=0,
                    )
                    truth_state = (fobs_zoom_truth or {}).get("state") if isinstance(fobs_zoom_truth, dict) else ""
                    portal_zoom = (fobs_zoom_truth.get("portal_zoom") or {}) if isinstance(fobs_zoom_truth, dict) else {}
                    portal_zoom_to_save = {
                        "meeting_link": (portal_zoom.get("meeting_link") or "").strip(),
                        "meeting_id": normalize_zoom_meeting_id(portal_zoom.get("meeting_id", "")),
                        "meeting_password": (portal_zoom.get("meeting_password") or "").strip(),
                    }
                    if db_has_valid_zoom_details(portal_zoom_to_save):
                        save_zoom_details_to_course(
                            conn=conn,
                            title=title_text,
                            date_time=db_date_time,
                            provider=provider,
                            zoom_data=portal_zoom_to_save,
                        )
                        print("[DB] Saved Zoom details currently shown in FOBS after recheck.")
                    if truth_state == "match":
                        if db_has_valid_zoom_details(portal_zoom_to_save):
                            print("[DB] Synced SQLite Zoom details to the live FOBS meeting after recheck.")
                            topic_ok, topic_msg = ensure_zoom_topic_matches_course(
                                portal_zoom_to_save.get("meeting_id"), provider, title_text, db_date_time,
                                account_id=provider_zoom_account_id,
                            )
                            if topic_ok:
                                print("[ZOOM] Meeting topic checked/updated for current course title.")
                            else:
                                print(f"[ZOOM] Meeting topic was not updated: {topic_msg}")
                        print("[FOBS] Existing FOBS Zoom details are still live after recheck. Skipping without opening the edit screen.")
                        update_course_sync_state(conn, title_text, db_date_time, provider, "skipped", "FOBS + Zoom OK")
                        mark_skipped(course_key, "FOBS + Zoom OK")
                        processed_course_keys.add(course_key)
                        stats["skipped"] += 1
                        back_to_list_safely()
                        continue
                    if truth_state == "unverifiable":
                        print("[FOBS] Existing FOBS Zoom details were detected on recheck but the linked Zoom token is unavailable. Leaving them unchanged.")
                        update_course_sync_state(conn, title_text, db_date_time, provider, "skipped", "Existing FOBS Zoom left unchanged because linked Zoom verification is unavailable")
                        mark_skipped(course_key, "Existing FOBS Zoom left unchanged because linked Zoom verification is unavailable")
                        processed_course_keys.add(course_key)
                        stats["skipped"] += 1
                        back_to_list_safely()
                        continue
                    if not ALLOW_ZOOM_REPLACE_ON_MISMATCH:
                        mismatch_reason = ""
                        if isinstance(fobs_zoom_truth, dict):
                            mismatch_reason = (fobs_zoom_truth.get("reason") or "").strip()
                        message = "Zoom link mismatch confirmed on FOBS - manual decision needed"
                        if mismatch_reason:
                            message = f"Zoom link mismatch confirmed on FOBS ({mismatch_reason}) - manual decision needed"
                        print(f"[FOBS] {message}. Leaving FOBS unchanged after recheck.")
                        update_course_sync_state(conn, title_text, db_date_time, provider, "needs_attention", message)
                        mark_skipped(course_key, message)
                        processed_course_keys.add(course_key)
                        stats["skipped"] += 1
                        back_to_list_safely()
                        continue
                    print("[FOBS] Existing FOBS Zoom details detected on recheck but do not tally with live Zoom. Replacement explicitly confirmed by trainer.")
            edit_label_used = run_step(
                course_key,
                "open_zoom_edit",
                lambda: open_virtual_classroom_edit_screen(page),
                success_message="Opened Zoom edit screen",
                retries=1,
            )
            print(f"[FOBS] Opened edit screen using: {edit_label_used}")
            page.wait_for_timeout(2000)

            print("Zoom data being sent to form:", safe_zoom_log_data(zoom_data))

            if has_zoom_details:
                def clear_stale_zoom_fields():
                    meeting_url = page.get_by_label("Meeting URL", exact=True)
                    meeting_id = page.get_by_label("Meeting ID", exact=True)
                    meeting_password = page.get_by_label("Meeting Password", exact=True)
                    meeting_url.fill("")
                    meeting_id.fill("")
                    meeting_password.fill("")
                    return True

                print("[FOBS] Replacing stale or missing FOBS Zoom details in the edit screen.")
                run_step(
                    course_key,
                    "clear_stale_fobs_zoom",
                    clear_stale_zoom_fields,
                    success_message="Cleared stale FOBS Zoom fields",
                    retries=1,
                )

            updated = run_step(
                course_key,
                "fill_zoom_fields",
                lambda: fill_zoom_fields_if_empty(
                    page,
                    zoom_join_url=zoom_data["meeting_link"],
                    zoom_meeting_id=zoom_data["meeting_id"],
                    zoom_password=zoom_data["meeting_password"],
                ),
                success_message="Filled Zoom fields in FOBS",
                retries=1,
            )
            print("Fields updated:", updated)

            if updated:
                saved = run_step(
                    course_key,
                    "click_save",
                    lambda: click_save_button(page),
                    success_message="Clicked save button",
                    retries=1,
                )
                if not saved:
                    mark_error(course_key, "click_save", "Save button was not clicked successfully")
                    stats["failed"] += 1
                    print("[FOBS] Save not clicked. Moving to next row.")
                    back_to_list_safely()
                    continue

                success = run_step(
                    course_key,
                    "confirm_save",
                    lambda: confirm_save_success(page, expected_zoom_data=zoom_data),
                    success_message="Checked save confirmation",
                    retries=2,
                )
                if success:
                    print("[FOBS] Save confirmed. Returning to filtered course list...")
                    update_course_sync_state(conn, title_text, db_date_time, provider, "success", "Zoom link updated")
                    mark_success(course_key, f"FOBS updated successfully via {path_used}")
                    processed_course_keys.add(course_key)
                    stats["updated"] += 1

                    if not back_to_list_safely():
                        raise Exception("Could not return to filtered FOBS list after save")

                    continue

                print("[FOBS] WARNING: Save may have failed.")
                mark_error(course_key, "confirm_save", "Save could not be confirmed after FOBS update")
                stats["failed"] += 1
                back_to_list_safely()
                continue

            update_course_sync_state(conn, title_text, db_date_time, provider, "skipped", "FOBS + Zoom OK")
            mark_skipped(course_key, "FOBS + Zoom OK")
            processed_course_keys.add(course_key)
            stats["skipped"] += 1
            back_to_list_safely()
            continue

        except Exception as exc:
            stats["failed"] += 1
            print(f"[FOBS] Row {target['row_number']} failed but processing will continue: {exc}")
            try:
                mark_error(fallback_course_key, "row_processing", f"{type(exc).__name__}: {exc}")
            except Exception:
                pass
            back_to_list_safely()
            continue

    print("\nFOBS course processing summary:", stats)
    return stats

def process_courses(courses, provider_name="Unknown"):
    provider_name = normalize_provider_context(provider_name)
    normalised_courses = []
    for course in courses or []:
        if isinstance(course, dict):
            normalised_courses.append(enforce_course_provider_context(dict(course), provider_name))
    courses = normalised_courses

    print(f"\nProcessing {len(courses)} course(s)...")
    stats = {
        "scraped": len(courses or []),
        "in_window": 0,
        "db_processed": 0,
        "skipped_outside_window": 0,
        "skipped_non_confirmed": 0,
        "skipped_imminent": 0,
        "db_new": 0,
        "db_updated": 0,
    }

    for course in courses:
        if maybe_stop():
            print("[CONTROL] Stopping before next course is processed.")
            break
        print(f"Checking course: {course['title']} ({course['date_time']}) [{course['status']}]")

        inside_sync_window = course_is_inside_sync_window(course.get("date_time", ""))
        if inside_sync_window:
            stats["in_window"] += 1
        else:
            print(f"[WINDOW] Saving future course for visibility only, outside active sync window: {course.get('title', '')} ({course.get('date_time', '')})")
            stats["skipped_outside_window"] += 1

        if course_starts_within_next_minutes(course.get("date_time", ""), minutes=60):
            print(f"Skipping imminent course within next 60 minutes: {course['title']} ({course['date_time']})")
            stats["skipped_imminent"] += 1
            continue

        if course_status_blocks_sync(course.get("status", "")):
            print(
                f"Skipping cancelled/removed course: "
                f"{course['title']} ({course['date_time']}) [{course.get('status', '')}]"
            )
            stats["skipped_non_confirmed"] += 1
            continue

        provider_replaced_course = retire_same_provider_replacement_if_needed(course)

        existing_by_id = course_exists_by_id(course["id"])
        if existing_by_id:
            print(f"Course already exists in DB: {course['title']} ({course['date_time']})")

            backfill_provider_for_existing_course(course)

            cursor.execute(
                """
                UPDATE courses
                SET provider = ?,
                    title = ?,
                    date_time = ?,
                    status = ?,
                    active_in_portal = 1,
                    last_seen_at = ?,
                    fobs_course_url = COALESCE(NULLIF(?, ''), fobs_course_url)
                WHERE id = ?
                """,
                (
                    course["provider"],
                    course["title"],
                    course["date_time"],
                    course["status"],
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    (course.get("fobs_course_url") or "").strip(),
                    course["id"],
                ),
            )
            conn.commit()
            if provider_replaced_course:
                update_course_sync_state(conn, course["title"], course["date_time"], course["provider"], "info", "Course replaced by provider")
            flag_cross_provider_conflicts(course)
            stats["db_processed"] += 1
            stats["db_updated"] += 1
            continue

        try:
            cursor.execute(
                """
                INSERT INTO courses (
                    id,
                    provider,
                    title,
                    date_time,
                    meeting_id,
                    meeting_link,
                    meeting_password,
                    status,
                    active_in_portal,
                    last_seen_at,
                    fobs_course_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    course["id"],
                    course.get("provider", PROVIDER_NAME),
                    course["title"],
                    course["date_time"],
                    None,
                    None,
                    None,
                    course["status"],
                    1,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    (course.get("fobs_course_url") or "").strip(),
                )
            )
            conn.commit()
            if provider_replaced_course:
                update_course_sync_state(conn, course["title"], course["date_time"], course["provider"], "info", "Course replaced by provider")
            flag_cross_provider_conflicts(course)
            print(f"Saved to DB: {course['title']} ({course['date_time']})")
            stats["db_processed"] += 1
            stats["db_new"] += 1
        except sqlite3.IntegrityError as exc:
            print(f"Already exists or conflicts in DB: {course['title']} ({course['date_time']}): {exc}")
            flag_cross_provider_conflicts(course)
            stats["db_processed"] += 1
            stats["db_updated"] += 1

    mode_label = "14-DAY" if BOT_MODE == "urgent_14d" else "FULL"
    print(f"[{provider_name}] Mode: {mode_label}")
    if BOT_MODE == "urgent_14d":
        print(f"[{provider_name}] Scraped: {stats['scraped']} | In Sync Window: {stats['in_window']} | DB Processed: {stats['db_processed']} | Future visibility only: {stats['skipped_outside_window']}")
    else:
        print(f"[{provider_name}] Scraped: {stats['scraped']} | In Sync Window: {stats['in_window']} | DB Processed: {stats['db_processed']} | Future visibility only: {stats['skipped_outside_window']}")
    print(f"[{provider_name}] DB New: {stats['db_new']} | DB Updated: {stats['db_updated']} | Skipped (status): {stats['skipped_non_confirmed']} | Skipped (imminent): {stats['skipped_imminent']}")
    return stats


def get_simulated_courses():
    return [
        {
            "id": "2026-04-21-1100-speed-awareness",
            "title": "Speed Awareness",
            "provider": "Essex",
            "date": "2026-04-21",
            "time": "11:00",
            "end_time": "14:00",
            "date_time": "2026-04-21 11:00",
            "duration_minutes": 180,
            "status": "Confirmed",
        },
        {
            "id": "2026-04-25-0945-speed-awareness",
            "title": "Speed Awareness",
            "provider": "Essex",
            "date": "2026-04-25",
            "time": "09:45",
            "end_time": "12:45",
            "date_time": "2026-04-25 09:45",
            "duration_minutes": 180,
            "status": "Confirmed",
        },
    ]

# ============================================================
# PORTAL LOGIN + COURSE PAGE
# ============================================================

def portal_check_and_login(provider_config=None):
    """
    Safe portal check for a single provider:
    - opens portal
    - checks if login page is reachable
    - attempts login ONCE only
    - if credentials are rejected, stops that provider immediately
    - if the browser/page is manually closed, stops only that provider
    - no repeated login attempts inside the run
    """

    provider_config = provider_config or {}
    provider_id = provider_slug(provider_config.get("id") or provider_config.get("name") or "essex")
    provider_name = (provider_config.get("name") or PROVIDER_NAME).strip() or PROVIDER_NAME
    login_url = (provider_config.get("login_url") or PORTAL_URL).strip() or PORTAL_URL
    courses_url = (provider_config.get("courses_url") or derive_courses_url(login_url) or COURSES_URL).strip()
    username = get_provider_username(provider_id)
    password = get_provider_password(provider_id)

    if not username or not password:
        print(f"Missing portal credentials for {provider_name}.")
        print("Please save them in the dashboard keyring-backed provider settings.")
        return None

    browser = None

    def safe_close_browser():
        nonlocal browser
        if browser is None:
            return
        try:
            if browser.is_connected():
                browser.close()
        except Exception:
            pass
        finally:
            browser = None

    with sync_playwright() as p:
        try:
            update_app_state(current_provider=provider_name, current_course="", last_message=f"Opening portal for {provider_name}...")
            print(f"Opening portal for {provider_name}...")
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            open_page_with_visible_url(page, login_url, label=f'{provider_name} login page', timeout=30000)
            page.wait_for_selector("#UserName", timeout=20000)
            page.wait_for_selector("#Password", timeout=20000)

            print("Portal is reachable.")

            page.fill("#UserName", username)
            page.fill("#Password", password)
            page.click("button[type='submit']")

            page.wait_for_timeout(5000)

            print("Login attempt completed.")
            print("Current URL after login:", page.url)

            if page_looks_like_login_screen(page):
                login_error = extract_portal_login_error_message(page) or "Login failed after a single attempt."
                print(f"[PORTAL] Login failed for {provider_name}: {login_error}")
                safe_close_browser()
                return {
                    "login_failed": True,
                    "provider": provider_name,
                    "message": login_error,
                }

            open_page_with_visible_url(page, courses_url, label=f'{provider_name} courses page', timeout=30000)
            page.wait_for_timeout(3000)

            print("Current URL on courses page:", page.url)

            detected_provider_name = detect_provider_from_url(page.url) or provider_name
            authoritative_provider_name = normalize_provider_context(provider_name, detected_provider_name)
            print("Detected provider:", detected_provider_name)
            print("Authoritative provider:", authoritative_provider_name)

            if BOT_MODE == "urgent_14d" or target_course_check_enabled():
                start_date = start_of_sync_window_date()
                print("Setting start date to:", start_date)
                page.evaluate(
                    """([selector, value]) => {
                        const el = document.querySelector(selector);
                        if (!el) return;
                        el.removeAttribute('readonly');
                        el.value = value;
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }""",
                    ["#startDate", start_date],
                )

            future_date = end_of_provider_import_date()
            print("Setting provider import end date to:", future_date)

            page.evaluate(
                """([selector, value]) => {
                    const el = document.querySelector(selector);
                    if (!el) return;
                    el.removeAttribute('readonly');
                    el.value = value;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                ["#endDate", future_date],
            )

            page.click("input[name='searchCoruses']")
            page.wait_for_timeout(3000)

            rows = page.locator("tr:has(td.ng-binding)")
            row_count = rows.count()
            update_app_state(current_provider=authoritative_provider_name, last_message=f"Found {row_count} course rows for {authoritative_provider_name}.")
            print("Number of rows found:", row_count)

            if maybe_stop():
                safe_close_browser()
                return []

            extracted_rows = []

            for i in range(row_count):
                row = rows.nth(i)
                cells = row.locator("td")
                cell_count = cells.count()

                clean_cells = []
                for j in range(cell_count):
                    cell_text = cells.nth(j).inner_text().strip()
                    clean_cells.append(cell_text)

                print(f"Row {i + 1}: {clean_cells}")
                extracted_rows.append(clean_cells)

            structured_courses = extract_courses_from_rows(extracted_rows, authoritative_provider_name)

            if maybe_stop():
                safe_close_browser()
                return structured_courses

            print("\nStructured courses:")
            for course in structured_courses:
                print(course)

            db_stats = process_courses(structured_courses, provider_name=authoritative_provider_name)
            fobs_stats = test_open_zoom_edit_screen(page, conn, authoritative_provider_name, courses_url=courses_url)
            print(f"[{authoritative_provider_name}] FOBS Rows: {fobs_stats.get('total_targets', 0)} | FOBS Checked: {fobs_stats.get('checked', 0)} | FOBS Updated: {fobs_stats.get('updated', 0)} | FOBS Skipped: {fobs_stats.get('skipped', 0)} | FOBS Failed: {fobs_stats.get('failed', 0)}")

            safe_close_browser()
            return {"courses": structured_courses, "db_stats": db_stats, "fobs_stats": fobs_stats}

        except TimeoutError:
            print(f"[PORTAL] Timeout while loading or interacting with the portal for {provider_name}.")
            safe_close_browser()
            return None

        except PlaywrightError as e:
            msg = str(e)
            if (
                "Target page, context or browser has been closed" in msg
                or "Browser has been closed" in msg
                or "TargetClosed" in msg
            ):
                print(f"[PORTAL] Browser/page was closed while processing {provider_name}. Marking this provider unavailable and continuing.")
                safe_close_browser()
                return None
            print(f"Unexpected Playwright portal error for {provider_name}: {e}")
            safe_close_browser()
            return None

        except Exception as e:
            print(f"Unexpected portal error for {provider_name}: {e}")
            safe_close_browser()
            return None

def mark_target_course_missing(sync_started_at, provider_name=None, scanned_courses=None):
    """For a single-course confirmation check, flag exactly that course if FOBS did not return it."""
    target = target_course_payload()
    if not target:
        return 0

    target_id = (target.get("id") or "").strip()
    target_provider = (target.get("provider") or provider_name or "").strip()
    target_dt = (target.get("date_time") or "").strip()
    target_title = (target.get("title") or "").strip()
    if not target_id and not (target_provider and target_dt and target_title):
        return 0

    for course in scanned_courses or []:
        if isinstance(course, dict) and course_matches_target(course):
            print("[TARGET] Target course still exists in FOBS; no missing flag required.")
            return 0

    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    params = [now_text, now_text]
    where = []
    if target_id:
        where.append("id = ?")
        params.append(target_id)
    else:
        where.extend(["provider = ?", "date_time = ?", "lower(title) = lower(?)"])
        params.extend([target_provider, target_dt, target_title])

    params.append(sync_started_at)
    cursor.execute(f"""
        UPDATE courses
        SET active_in_portal = 0,
            last_synced_at = ?,
            last_seen_at = COALESCE(last_seen_at, ?),
            last_sync_status = 'needs_confirmation',
            last_sync_action = 'Single-course check: provider did not return this course; confirm if cancelled/deleted'
        WHERE {' AND '.join(where)}
          AND COALESCE(status, '') <> 'Replaced'
          AND lower(COALESCE(last_sync_action, '')) NOT LIKE '%trainer confirmed removed%'
          AND (last_seen_at IS NULL OR last_seen_at < ?)
    """, params)
    affected = cursor.rowcount
    conn.commit()
    print(f"[TARGET] Flagged {affected} target course(s) as needing cancellation/deletion confirmation.")
    return affected


def mark_missing_courses_inactive(sync_started_at, provider_name=None, scanned_courses=None):
    """Flag provider courses missing from the latest scanned range for trainer confirmation.

    This is intentionally narrow: only the provider just scanned, only the date
    range actually returned by that scan, and never a broad cleanup.
    """
    provider_name = (provider_name or "").strip()
    scanned_courses = [c for c in (scanned_courses or []) if isinstance(c, dict)]
    if not provider_name or not scanned_courses:
        return 0

    scanned_ids = {(c.get("id") or "").strip() for c in scanned_courses if (c.get("id") or "").strip()}
    scanned_datetimes = sorted((c.get("date_time") or "").strip() for c in scanned_courses if (c.get("date_time") or "").strip())
    if not scanned_datetimes:
        return 0

    start_dt = scanned_datetimes[0]
    end_dt = scanned_datetimes[-1]
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\nChecking {provider_name} for courses missing from scanned range {start_dt} to {end_dt}...")

    params = [
        now_text,
        now_text,
        provider_name,
        start_dt,
        end_dt,
        sync_started_at,
    ]
    id_filter = ""
    if scanned_ids:
        placeholders = ",".join("?" for _ in scanned_ids)
        id_filter = f" AND id NOT IN ({placeholders})"
        params.extend(sorted(scanned_ids))

    cursor.execute(f"""
        UPDATE courses
        SET active_in_portal = 0,
            last_synced_at = ?,
            last_seen_at = COALESCE(last_seen_at, ?),
            last_sync_status = 'needs_confirmation',
            last_sync_action = 'Possibly removed/cancelled by provider - not found in latest provider scan'
        WHERE provider = ?
          AND date_time >= ?
          AND date_time <= ?
          AND COALESCE(status, '') <> 'Replaced'
          AND lower(COALESCE(last_sync_action, '')) NOT LIKE '%trainer confirmed removed%'
          AND (
              COALESCE(active_in_portal, 1) = 1
              OR COALESCE(last_sync_status, '') <> 'needs_confirmation'
          )
          AND (
              last_seen_at IS NULL
              OR last_seen_at < ?
          )
          {id_filter}
    """, params)

    affected = cursor.rowcount
    conn.commit()

    print(f"Flagged {affected} {provider_name} course(s) as possibly removed/cancelled; trainer confirmation required.")
    return affected

# ============================================================
# MAIN
# ============================================================

def save_trainer_identity(ndors_trainer_id: str, email: str = ""):
    ndors_value = (ndors_trainer_id or "").strip()
    email_value = (email or "").strip()

    if ndors_value:
        keyring.set_password("trainermate_account", "ndors_trainer_id", ndors_value)

    if email_value:
        keyring.set_password("trainermate_account", "trainer_email", email_value)


def get_licensing_identity():
    ndors_trainer_id = _first_non_empty(
        _get_keyring_password("trainermate_account", "ndors_trainer_id"),
        os.getenv("TRAINERMATE_NDORS_ID"),
        os.getenv("NDORS_TRAINER_ID"),
    ).strip()

    user_email = _first_non_empty(
        _get_keyring_password("trainermate_account", "trainer_email"),
        os.getenv("TRAINERMATE_EMAIL"),
        os.getenv("USER_EMAIL"),
    ).strip()

    return ndors_trainer_id, user_email


def _access_is_paid(access):
    access = access or {}
    features = access.get("features") if isinstance(access.get("features"), dict) else {}
    plan = str(access.get("plan") or access.get("tier") or "").strip().lower()
    if access.get("paid") is True or access.get("is_paid") is True:
        return True
    if plan and plan not in {"free", "trial", "starter"}:
        return True
    try:
        return int(features.get("sync_window_days") or 0) > FREE_SYNC_WINDOW_DAYS
    except Exception:
        return False


def apply_licensing_features(features, access=None):
    global ALLOW_ZOOM_CREATION, ALLOW_AUTOMATION, ALLOW_CALENDAR, LICENSED_SYNC_WINDOW_DAYS
    features = features or {}
    try:
        feature_days = int(features.get("sync_window_days") or 0)
    except Exception:
        feature_days = 0

    is_paid = _access_is_paid(access)
    if is_paid:
        # Paid accounts must cover the full 12-week operational window even if
        # an older API response still reports the legacy 21-day feature value.
        LICENSED_SYNC_WINDOW_DAYS = max(feature_days, PAID_SYNC_WINDOW_DAYS) if feature_days > 0 else PAID_SYNC_WINDOW_DAYS
    else:
        LICENSED_SYNC_WINDOW_DAYS = feature_days if feature_days > 0 else FREE_SYNC_WINDOW_DAYS
    ALLOW_ZOOM_CREATION = bool(features.get("zoom_creation", False))
    ALLOW_AUTOMATION = bool(features.get("automation", False))
    ALLOW_CALENDAR = bool(features.get("calendar", False))
    print(f"[LICENSING] Active sync window: {LICENSED_SYNC_WINDOW_DAYS} days")


def run_sync():
    user_ndors_trainer_id, user_email = get_licensing_identity()
    if not user_ndors_trainer_id:
        raise RuntimeError(
            "Missing NDORS trainer ID. Please save your account details in the dashboard before syncing."
        )

    access = check_access(user_ndors_trainer_id, user_email or None)
    if not access.get("allowed"):
        reason = (access.get("reason") or "access_denied").strip()
        if reason == "free_sync_limit_reached":
            raise RuntimeError("You’ve reached your free sync limit. Upgrade to continue.")
        if reason == "account_inactive":
            raise RuntimeError("Your account is not active. Please contact support.")
        raise RuntimeError("Sync is not allowed for this account right now.")

    features = access.get("features") or {}
    apply_licensing_features(features, access=access)

    print("[MAIN] run_sync started.")
    sync_started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary = build_run_summary(sync_started_at)
    persist_run_summary(summary)

    backfill_missing_meeting_passwords()
    if maybe_stop():
        summary["outcome"] = "stopped"
        summary["message"] = "Sync stopped by user before provider processing."
        persist_run_summary(summary)
        return summary

    active_providers = [p for p in load_providers() if isinstance(p, dict) and p.get("active", True)]
    summary["providers_requested"] = len(active_providers)

    if BOT_PROVIDER_FILTER:
        active_providers = [
            p for p in active_providers
            if provider_slug(p.get("id") or p.get("name") or "") == BOT_PROVIDER_FILTER
        ]
        print(f"[MAIN] Provider filter active: {BOT_PROVIDER_FILTER}. Matching providers: {[p.get('name') for p in active_providers]}")

    if not active_providers:
        if BOT_PROVIDER_FILTER:
            message = f"No active providers matched provider filter: {BOT_PROVIDER_FILTER}"
            print(f"\n{message}")
            summary["health_issues"].append(message)
        else:
            message = "No active providers configured."
            print(f"\n{message}")
            summary["health_issues"].append(message)
        summary["outcome"] = "completed_with_warnings"
        summary["message"] = message
        persist_run_summary(summary)
        return summary

    for provider in active_providers:
        if maybe_stop():
            summary["outcome"] = "stopped"
            summary["message"] = "Sync stopped by user."
            persist_run_summary(summary)
            return summary

        provider_name = (provider.get("name") or provider.get("id") or PROVIDER_NAME).strip() or PROVIDER_NAME
        provider_entry = {
            "id": provider_slug(provider.get("id") or provider_name),
            "name": provider_name,
            "status": "running",
            "message": "Starting provider sync.",
            "courses_found": 0,
            "courses_processed": 0,
            "db_created": 0,
            "db_updated": 0,
            "db_unchanged": 0,
            "db_reactivated": 0,
            "fobs_checked": 0,
            "fobs_updated": 0,
            "fobs_failed": 0,
        }
        summary["providers"].append(provider_entry)
        summary["providers_attempted"] += 1
        update_app_state(current_provider=provider_name, last_message=f"Checking {provider_name}...")
        persist_run_summary(summary)

        update_app_state(current_provider=provider_name, current_course="", last_message=f"Opening portal for {provider_name}...")
        print(f"\n[MAIN] Starting provider sync: {provider_name}")
        portal_result = portal_check_and_login(provider)

        if isinstance(portal_result, dict) and portal_result.get("login_failed"):
            provider_entry["status"] = "login_failed"
            provider_entry["message"] = (
                f"TrainerMate can no longer log in to {provider_name}. "
                "If the FOBS password has changed, go to Manage providers and reconfirm it."
            )
            provider_entry["detail"] = portal_result.get("message") or ""
            summary["providers_failed"] += 1
            summary["health_issues"].append(provider_entry["message"])
            persist_run_summary(summary)
            print(f"\nNo courses processed for {provider_name} because login failed after one attempt.")
            continue

        if portal_result is None:
            username = get_provider_username(provider_entry["id"])
            password = get_provider_password(provider_entry["id"])
            if not username or not password:
                provider_entry["status"] = "missing_credentials"
                provider_entry["message"] = f"Missing saved portal credentials for {provider_name}."
                summary["providers_missing_credentials"] += 1
            else:
                provider_entry["status"] = "unavailable"
                provider_entry["message"] = f"Portal unavailable or login did not complete for {provider_name}."
                summary["providers_unavailable"] += 1

            summary["providers_failed"] += 1
            summary["health_issues"].append(provider_entry["message"])
            persist_run_summary(summary)
            print(f"\nNo courses processed for {provider_name} because portal is unavailable or credentials are missing.")
            continue

        provider_courses = portal_result.get("courses") if isinstance(portal_result, dict) else []
        db_stats = portal_result.get("db_stats") if isinstance(portal_result, dict) else {}
        fobs_stats = portal_result.get("fobs_stats") if isinstance(portal_result, dict) else {}

        provider_entry["courses_found"] = _safe_int(len(provider_courses or []))
        provider_entry["courses_processed"] = _safe_int((db_stats or {}).get("db_processed"))
        provider_entry["db_created"] = _safe_int((db_stats or {}).get("db_new"))
        provider_entry["db_updated"] = _safe_int((db_stats or {}).get("db_updated"))
        provider_entry["db_unchanged"] = max(0, provider_entry["courses_found"] - provider_entry["courses_processed"])
        provider_entry["db_reactivated"] = _safe_int((db_stats or {}).get("db_reactivated"))
        provider_entry["fobs_checked"] = _safe_int((fobs_stats or {}).get("checked"))
        provider_entry["fobs_updated"] = _safe_int((fobs_stats or {}).get("updated"))
        provider_entry["fobs_failed"] = _safe_int((fobs_stats or {}).get("failed"))
        provider_entry["missing_flagged"] = 0

        if target_course_check_enabled() and provider_slug(provider_name) == provider_slug(TRAINERMATE_TARGET_COURSE_PROVIDER or provider_name):
            provider_entry["missing_flagged"] = mark_target_course_missing(
                sync_started_at,
                provider_name=provider_name,
                scanned_courses=provider_courses,
            )
        elif provider_entry["courses_found"] > 0 and provider_entry["courses_processed"] > 0:
            provider_entry["missing_flagged"] = mark_missing_courses_inactive(
                sync_started_at,
                provider_name=provider_name,
                scanned_courses=provider_courses,
            )

        summary["courses_found"] += provider_entry["courses_found"]
        summary["courses_processed"] += provider_entry["courses_processed"]
        summary["db_created"] += provider_entry["db_created"]
        summary["db_updated"] += provider_entry["db_updated"]
        summary["db_unchanged"] += provider_entry["db_unchanged"]
        summary["db_reactivated"] += provider_entry["db_reactivated"]
        summary["fobs_checked"] += provider_entry["fobs_checked"]
        summary["fobs_updated"] += provider_entry["fobs_updated"]
        summary["fobs_failed"] += provider_entry["fobs_failed"]
        summary["inactive_marked"] = _safe_int(summary.get("inactive_marked")) + provider_entry["missing_flagged"]

        useful_provider_work = (
            provider_entry["courses_found"] > 0
            or provider_entry["courses_processed"] > 0
            or provider_entry["fobs_checked"] > 0
            or provider_entry["fobs_updated"] > 0
        )
        if useful_provider_work:
            provider_entry["status"] = "success"
            provider_entry["message"] = (
                f"{provider_entry['courses_found']} found, "
                f"{provider_entry['courses_processed']} saved, "
                f"{provider_entry['fobs_updated']} Zoom link(s) updated for {provider_name}."
            )
            if provider_entry["missing_flagged"]:
                provider_entry["message"] += f" {provider_entry['missing_flagged']} missing course(s) need confirmation."
            summary["providers_succeeded"] += 1
            if provider_entry["courses_found"] > 0:
                summary["providers_with_rows"] += 1
        else:
            provider_entry["status"] = "zero_rows"
            provider_entry["message"] = f"Portal flow completed for {provider_name} but no rows were found."
            summary["providers_with_zero_rows"] += 1
            summary["health_issues"].append(provider_entry["message"])
            print(f"\n{provider_entry['message']}")

        persist_run_summary(summary)

    if summary["courses_processed"] > 0:
        try:
            record_sync(user_ndors_trainer_id, user_email or None)
        except Exception as exc:
            print(f"[LICENSING] Failed to record successful sync: {exc}")

        print("[MAIN] Broad inactive cleanup skipped. Missing courses were checked per provider scan.")

    if summary["providers_failed"] == summary["providers_attempted"] and summary["providers_attempted"] > 0:
        summary["outcome"] = "failed"
        summary["message"] = "No providers synced successfully. Review credentials or portal availability."
    elif summary["courses_processed"] == 0 and summary["fobs_checked"] == 0 and summary["fobs_updated"] == 0:
        summary["outcome"] = "completed_with_warnings"
        summary["message"] = "Sync finished, but no course rows were saved."
    elif summary["health_issues"]:
        summary["outcome"] = "completed_with_warnings"
        summary["message"] = (
            f"Sync completed with warnings. {summary['courses_processed']} saved, "
            f"{summary['fobs_updated']} Zoom link(s) updated."
        )
    else:
        summary["outcome"] = "completed"
        summary["message"] = (
            f"Sync completed successfully. {summary['courses_processed']} saved, "
            f"{summary['fobs_updated']} Zoom link(s) updated."
        )

    persist_run_summary(summary)
    return summary

def print_build_banner():
    print("=" * 60)
    print(BUILD_LABEL)
    print(BUILD_NAME)
    print(f"HTTP Dashboard: {DASHBOARD_CANONICAL_URL}")
    print("=" * 60)


def main():
    print_build_banner()
    print("[MAIN] Bot starting.")
    clear_stop_request()
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    update_app_state(
        sync_running=True,
        stop_requested=False,
        pid=os.getpid(),
        last_started_at=started_at,
        last_status="Running",
        last_run_started_at=started_at,
        last_run_finished_at="",
        last_run_status="running",
        last_message="Sync started.",
        last_pid=os.getpid(),
    )

    try:
        summary = run_sync() or {}
        final_status = "stopped" if stop_requested() else (summary.get("outcome") or "completed")
        final_message = "Sync stopped by user." if stop_requested() else (summary.get("message") or "Sync completed.")
        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status_label_map = {
            "stopped": "Stopped",
            "completed": "Completed",
            "completed_with_warnings": "Completed with warnings",
            "failed": "Failed",
            "running": "Running",
        }
        update_app_state(
            sync_running=False,
            pid=None,
            current_provider="",
            last_stopped_at=finished_at,
            last_status=status_label_map.get(final_status, "Completed"),
            last_run_finished_at=finished_at,
            last_run_status=final_status,
            last_message=final_message,
            pending_sync_request={},
            run_summary=summary,
            health_issues=(summary.get("health_issues") or []),
            last_check_at=finished_at,
            **({"last_success_at": finished_at} if final_status == "completed" else {}),
        )
    except Exception as exc:
        failed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_app_state(
            sync_running=False,
            pid=None,
            current_provider="",
            last_stopped_at=failed_at,
            last_status="Failed",
            last_run_finished_at=failed_at,
            last_run_status="failed",
            last_message=f"Sync failed: {exc}",
            pending_sync_request={},
            last_check_at=failed_at,
        )
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
