from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
import hashlib
import hmac
import json
import os
import secrets
import re
import smtplib
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import parse_qs

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from supabase import create_client, Client

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", encoding="utf-8-sig")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
FREE_SYNC_LIMIT = int(os.getenv("FREE_SYNC_LIMIT", "3"))

LICENSING_CACHE_PATH = BASE_DIR / "licensing_cache.json"
PENDING_SYNC_RECORDS_PATH = BASE_DIR / "pending_sync_records.json"
ADMIN_STATE_PATH = BASE_DIR / "admin_state.json"
ADMIN_COMMANDS_PATH = BASE_DIR / "admin_commands.json"
ADMIN_AUDIT_PATH = BASE_DIR / "admin_audit.json"
ADMIN_SETTINGS_PATH = BASE_DIR / "admin_settings.json"
SUPPORT_THREADS_PATH = BASE_DIR / "support_threads.json"
FAVICON_PATH = BASE_DIR / "static" / "favicon.ico"
SUPABASE_RETRIES = int(os.getenv("TRAINERMATE_SUPABASE_RETRIES", "2"))
SUPABASE_RETRY_DELAY_SECONDS = float(os.getenv("TRAINERMATE_SUPABASE_RETRY_DELAY_SECONDS", "0.4"))
ADMIN_TOKEN = os.getenv("TRAINERMATE_ADMIN_TOKEN", "")
ADMIN_COOKIE_SECURE = os.getenv("TRAINERMATE_ADMIN_COOKIE_SECURE", "0") == "1"
AUTH_RATE_LIMIT_LOCK = threading.Lock()
AUTH_RATE_LIMITS: dict[str, list[float]] = {}
AUTH_RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("TRAINERMATE_AUTH_RATE_LIMIT_WINDOW_SECONDS", "900"))
AUTH_RATE_LIMIT_MAX_ATTEMPTS = int(os.getenv("TRAINERMATE_AUTH_RATE_LIMIT_MAX_ATTEMPTS", "8"))
AUTH_RESET_RATE_LIMIT_MAX_ATTEMPTS = int(os.getenv("TRAINERMATE_AUTH_RESET_RATE_LIMIT_MAX_ATTEMPTS", "5"))
AUTH_ADMIN_RATE_LIMIT_MAX_ATTEMPTS = int(os.getenv("TRAINERMATE_AUTH_ADMIN_RATE_LIMIT_MAX_ATTEMPTS", "6"))
SMTP_HOST = os.getenv("TRAINERMATE_SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("TRAINERMATE_SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("TRAINERMATE_SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("TRAINERMATE_SMTP_PASSWORD", "")
SMTP_FROM_EMAIL = os.getenv("TRAINERMATE_SMTP_FROM_EMAIL", SMTP_USERNAME).strip()
SMTP_FROM_NAME = os.getenv("TRAINERMATE_SMTP_FROM_NAME", "TrainerMate Support").strip()
SMTP_USE_TLS = os.getenv("TRAINERMATE_SMTP_USE_TLS", "1") != "0"
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "").strip()
RESEND_FROM_EMAIL = os.getenv("RESEND_FROM_EMAIL", SMTP_FROM_EMAIL).strip()
RESEND_FROM_NAME = os.getenv("RESEND_FROM_NAME", SMTP_FROM_NAME).strip()

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing Supabase configuration")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

app = FastAPI(title="TrainerMate Licensing API")


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return FileResponse(FAVICON_PATH, media_type="image/x-icon")


def is_local_request(request: Request):
    client_host = ((request.client.host if request.client else "") or "").strip().lower()
    forwarded_for = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip().lower()
    host = ((request.url.hostname or "") if request.url else "").strip().lower()
    allowed = {"127.0.0.1", "::1", "localhost"}
    return client_host in allowed and (not forwarded_for or forwarded_for in allowed) and host in allowed


def request_ip(request: Request):
    forwarded_for = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    if forwarded_for:
        return forwarded_for.lower()
    return (((request.client.host if request.client else "") or "unknown").strip().lower() or "unknown")


def auth_rate_key(request: Request, scope: str, identity: str = ""):
    clean_identity = re.sub(r"[^a-z0-9_.@-]", "", (identity or "").strip().lower())[:80]
    return f"{scope}:{request_ip(request)}:{clean_identity}"


def check_auth_rate_limit(request: Request, scope: str, identity: str = "", max_attempts: int = AUTH_RATE_LIMIT_MAX_ATTEMPTS):
    key = auth_rate_key(request, scope, identity)
    now = time.time()
    with AUTH_RATE_LIMIT_LOCK:
        attempts = [item for item in AUTH_RATE_LIMITS.get(key, []) if now - item < AUTH_RATE_LIMIT_WINDOW_SECONDS]
        if len(attempts) >= max(1, int(max_attempts or 1)):
            retry_after = max(1, int(AUTH_RATE_LIMIT_WINDOW_SECONDS - (now - attempts[0])))
            raise HTTPException(
                status_code=429,
                detail=f"Too many attempts. Please wait {retry_after // 60 + 1} minute(s) before trying again.",
                headers={"Retry-After": str(retry_after)},
            )
        attempts.append(now)
        AUTH_RATE_LIMITS[key] = attempts


def clear_auth_rate_limit(request: Request, scope: str, identity: str = ""):
    with AUTH_RATE_LIMIT_LOCK:
        AUTH_RATE_LIMITS.pop(auth_rate_key(request, scope, identity), None)


@app.middleware("http")
async def admin_security_headers(request: Request, call_next):
    if not is_local_request(request):
        return JSONResponse({"detail": "Local access only"}, status_code=403)
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
    response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
    response.headers.setdefault("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self';")
    if request.url.path.startswith("/admin"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response



class AccessRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1)
    email: str | None = None
    device_id: str = Field(..., min_length=1)
    device_name: str | None = None
    app_version: str | None = None


class RedeemLicenceRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1)
    email: str | None = None
    licence_key: str = Field(..., min_length=1)


class AccountRegisterRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1)
    email: str = Field(..., min_length=3)
    password: str = Field(..., min_length=8)
    device_id: str = Field(..., min_length=1)
    device_name: str | None = None
    app_version: str | None = None


class AccountLoginRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)
    device_id: str = Field(..., min_length=1)
    device_name: str | None = None
    app_version: str | None = None


class AccountPasswordChangeRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1)
    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8)
    device_id: str = Field(..., min_length=1)
    device_name: str | None = None
    app_version: str | None = None


class PasswordResetRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1)
    email: str = Field(..., min_length=3)
    device_id: str = Field(..., min_length=1)
    device_name: str | None = None
    app_version: str | None = None


class PasswordResetConfirmRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1)
    reset_token: str = Field(..., min_length=8)
    password: str = Field(..., min_length=8)
    device_id: str = Field(..., min_length=1)
    device_name: str | None = None
    app_version: str | None = None


class AdminLoginRequest(BaseModel):
    token: str = Field(..., min_length=1)


class ClientHeartbeatRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1)
    email: str | None = None
    device_id: str = Field(..., min_length=1)
    device_name: str | None = None
    app_version: str | None = None
    build: str | None = None
    status: dict | None = None


class ClientCommandPollRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1)
    device_id: str = Field(..., min_length=1)


class ClientCommandResultRequest(BaseModel):
    command_id: str = Field(..., min_length=1)
    ndors_trainer_id: str = Field(..., min_length=1)
    device_id: str = Field(..., min_length=1)
    status: str = Field(..., min_length=1)
    message: str | None = None
    result: dict | None = None


class ClientSupportMessageRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1)
    email: str | None = None
    device_id: str | None = None
    device_name: str | None = None
    app_version: str | None = None
    build: str | None = None
    subject: str | None = None
    message: str = Field(..., min_length=1)
    summary: str | None = None
    status: dict | None = None
    thread_id: str | None = None


class AdminCommandCreateRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1)
    device_id: str | None = None
    command_type: str = Field(..., min_length=1)
    payload: dict | None = None


class AdminCommandCancelRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1)
    device_id: str | None = None
    command_type: str | None = None
    message: str | None = None


class AdminBroadcastMessageRequest(BaseModel):
    target: str = "selected"  # selected, all, active, paid, needs_attention
    ndors_trainer_id: str | None = None
    title: str | None = None
    message: str = Field(..., min_length=1)
    category: str | None = "info"


class AdminSupportReplyRequest(BaseModel):
    thread_id: str = Field(..., min_length=1)
    message: str = Field(..., min_length=1)
    title: str | None = None


class AdminSupportStatusRequest(BaseModel):
    thread_id: str = Field(..., min_length=1)
    status: str = "open"


class AdminSupportManageRequest(BaseModel):
    thread_id: str = Field(..., min_length=1)
    message_id: str | None = None
    note: str | None = None
    category: str | None = None
    priority: str | None = None


class AdminAccountUpdateRequest(BaseModel):
    plan: str | None = None
    status: str | None = None


class AdminAccountDeleteRequest(BaseModel):
    confirm_ndors_trainer_id: str = Field(..., min_length=1)
    confirm_delete: str = Field(..., min_length=1)


class AdminInvalidAccountDeleteRequest(BaseModel):
    confirm_account_identifier: str = Field(..., min_length=1)
    confirm_delete: str = Field(..., min_length=1)


class AdminPasswordResetRequest(BaseModel):
    confirm_ndors_trainer_id: str = Field(..., min_length=1)
    confirm_reset: str = Field(..., min_length=1)


class AdminLicenceCreateRequest(BaseModel):
    plan_type: str = "paid"
    issued_to_ndors_trainer_id: str | None = None
    expiry_date: str | None = None


class AdminSettingsUpdateRequest(BaseModel):
    latest_version: str | None = None
    minimum_version: str | None = None
    release_notes: str | None = None
    download_url: str | None = None
    installer_sha256: str | None = None
    mandatory_after: str | None = None


class AdminResetTrialRequest(BaseModel):
    free_syncs_used: int = 0


class AdminUpdatePromptRequest(BaseModel):
    message: str | None = None


class TemporaryLicensingBackendError(RuntimeError):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat()


AUTH_HASH_ITERATIONS = 260000


def password_hash(password: str, salt_hex: str | None = None):
    salt_hex = salt_hex or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        (password or "").encode("utf-8"),
        bytes.fromhex(salt_hex),
        AUTH_HASH_ITERATIONS,
    ).hex()
    return f"pbkdf2_sha256${AUTH_HASH_ITERATIONS}${salt_hex}${digest}"


def password_matches(password: str, stored_hash: str | None):
    try:
        method, iterations_text, salt_hex, expected = (stored_hash or "").split("$", 3)
        if method != "pbkdf2_sha256":
            return False
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            (password or "").encode("utf-8"),
            bytes.fromhex(salt_hex),
            int(iterations_text),
        ).hex()
        return hmac.compare_digest(digest, expected)
    except Exception:
        return False


def generate_temporary_password(length: int = 14):
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!?"
    while True:
        password = "".join(secrets.choice(alphabet) for _ in range(max(12, int(length or 14))))
        if (
            any(ch.isupper() for ch in password)
            and any(ch.islower() for ch in password)
            and any(ch.isdigit() for ch in password)
            and any(ch in "!?" for ch in password)
        ):
            return password


def generate_reset_token():
    raw = secrets.token_urlsafe(18)
    return raw.replace("-", "").replace("_", "")[:20]


def reset_token_hash(token: str):
    return hashlib.sha256((token or "").strip().encode("utf-8")).hexdigest()


def mask_email_address(email: str):
    email = (email or "").strip()
    if "@" not in email:
        return ""
    name, domain = email.split("@", 1)
    if len(name) <= 2:
        masked = name[:1] + "*" * max(1, len(name) - 1)
    else:
        masked = name[:2] + "*" * max(2, len(name) - 2)
    return f"{masked}@{domain}"


def primary_account_email(accounts):
    for row in accounts or []:
        email = (row.get("primary_email") or "").strip() if isinstance(row, dict) else ""
        if valid_email(email):
            return email
    account_ids = [row.get("id") for row in accounts or [] if isinstance(row, dict) and row.get("id")]
    if account_ids:
        try:
            result = execute_supabase(
                supabase.table("account_logins")
                .select("*")
                .in_("account_id", account_ids),
                "read account email for reset",
            )
            for row in result.data or []:
                email = (row.get("email") or "").strip()
                if valid_email(email):
                    return email
        except Exception:
            pass
    return ""


def configured_from_email():
    if RESEND_API_KEY and RESEND_FROM_EMAIL:
        return f"{RESEND_FROM_NAME} <{RESEND_FROM_EMAIL}>"
    if SMTP_HOST and SMTP_FROM_EMAIL:
        return f"{SMTP_FROM_NAME} <{SMTP_FROM_EMAIL}>"
    return ""


def send_email(to_email: str, subject: str, body: str):
    if not valid_email(to_email):
        raise HTTPException(status_code=400, detail="This account does not have a valid registered email address.")
    from_email = configured_from_email()
    if not from_email:
        raise HTTPException(status_code=503, detail="Password email is not configured on the API service handling this request. Set RESEND_API_KEY and RESEND_FROM_EMAIL there, then restart/redeploy it.")
    if RESEND_API_KEY and RESEND_FROM_EMAIL:
        payload = json.dumps({
            "from": from_email,
            "to": [to_email],
            "subject": subject,
            "text": body,
        }).encode("utf-8")
        request = urllib.request.Request(
            "https://api.resend.com/emails",
            data=payload,
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
                "User-Agent": "TrainerMate/1.0 password-reset",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                if response.status >= 300:
                    provider_detail = response.read().decode("utf-8", errors="replace")[:500]
                    raise HTTPException(status_code=503, detail=f"Password email provider rejected the message: {provider_detail or response.status}")
        except urllib.error.HTTPError as exc:
            provider_detail = ""
            try:
                provider_detail = exc.read().decode("utf-8", errors="replace")[:500]
            except Exception:
                provider_detail = str(exc)
            raise HTTPException(status_code=503, detail=f"Password email provider rejected the message: {provider_detail or exc}")
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Password email could not be sent: {exc}")
        return
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = from_email
    message["To"] = to_email
    message.set_content(body)
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
        if SMTP_USE_TLS:
            smtp.starttls()
        if SMTP_USERNAME or SMTP_PASSWORD:
            smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
        smtp.send_message(message)


def send_temporary_password_email(to_email: str, ndors: str, temporary_password: str):
    if not valid_email(to_email):
        raise HTTPException(status_code=400, detail="This account does not have a valid registered email address.")
    send_email(
        to_email,
        "Your TrainerMate temporary password",
        "TrainerMate support has reset your password.\n\n"
        f"NDORS trainer ID: {ndors}\n"
        f"Temporary password: {temporary_password}\n\n"
        "Open TrainerMate, log in with this temporary password, then choose a new private password when prompted.\n\n"
        "If you did not request this reset, contact TrainerMate support."
    )


def send_password_reset_token_email(to_email: str, ndors: str, reset_token: str):
    if not valid_email(to_email):
        raise HTTPException(status_code=400, detail="This account does not have a valid registered email address.")
    send_email(
        to_email,
        "Your TrainerMate password reset code",
        "A password reset was requested for your TrainerMate account.\n\n"
        f"NDORS trainer ID: {ndors}\n"
        f"Reset code: {reset_token}\n\n"
        "Enter this code in TrainerMate to choose a new password. It expires in 15 minutes and can only be used once.\n\n"
        "If you did not request this reset, ignore this email and contact TrainerMate support."
    )


def valid_email(value: str | None):
    text = (value or "").strip()
    return bool(text and "@" in text and "." in text.rsplit("@", 1)[-1] and " " not in text)


def valid_ndors_id(value: str | None):
    text = (value or "").strip()
    return bool(text and "@" not in text and " " not in text and len(text) <= 64 and re.match(r"^[A-Za-z0-9_-]+$", text))


def load_json_file(path: Path, default):
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, type(default)) else default
    except Exception:
        return default


def save_json_file(path: Path, data):
    """Write JSON safely, including on Windows where another process may briefly lock the target."""
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{int(time.time() * 1000)}.tmp")
    last_exc = None
    for attempt in range(8):
        try:
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, sort_keys=True)
            os.replace(tmp, path)
            return
        except PermissionError as exc:
            last_exc = exc
            time.sleep(0.08 * (attempt + 1))
        finally:
            try:
                if tmp.exists() and attempt == 7:
                    tmp.unlink()
            except Exception:
                pass
    if last_exc:
        raise last_exc


def admin_token_configured():
    return bool((ADMIN_TOKEN or "").strip())


def admin_cookie_value():
    if not admin_token_configured():
        return ""
    return hmac.new(ADMIN_TOKEN.encode("utf-8"), b"trainermate-admin", "sha256").hexdigest()


def admin_authorized(request: Request):
    if not admin_token_configured():
        return False
    supplied = request.headers.get("X-Admin-Token") or request.query_params.get("admin_token") or request.cookies.get("tm_admin")
    expected_cookie = admin_cookie_value()
    return bool(
        (supplied and hmac.compare_digest(str(supplied), ADMIN_TOKEN))
        or (supplied and expected_cookie and hmac.compare_digest(str(supplied), expected_cookie))
    )


def require_admin(request: Request):
    if not admin_authorized(request):
        raise HTTPException(status_code=401, detail="Admin login required")


def append_json_list(path: Path, item: dict, keep=500):
    items = load_json_file(path, [])
    if not isinstance(items, list):
        items = []
    items.append(item)
    save_json_file(path, items[-keep:])


def admin_audit(action: str, detail: dict | None = None):
    """Record admin audit events locally and, when available, in Supabase.

    The local JSON file remains as a safety net so the admin portal continues to
    work before the newer audit/database tables have been created.
    """
    item = {
        "at": utc_now(),
        "action": action,
        "detail": detail or {},
    }
    append_json_list(ADMIN_AUDIT_PATH, item)

    # Best-effort DB audit. Never let audit logging break licensing/support.
    try:
        execute_supabase(
            supabase.table("audit_log").insert({
                "actor": "admin",
                "action": action,
                "target_type": (detail or {}).get("target_type") or "admin",
                "target_id": (detail or {}).get("target_id"),
                "details": detail or {},
                "severity": (detail or {}).get("severity") or "info",
                "source": "admin_api",
            }),
            "write admin audit log",
        )
    except Exception:
        pass


def load_admin_state():
    data = load_json_file(ADMIN_STATE_PATH, {"devices": {}})
    return data if isinstance(data, dict) else {"devices": {}}


def save_admin_state(state):
    if not isinstance(state, dict):
        state = {"devices": {}}
    state.setdefault("devices", {})
    save_json_file(ADMIN_STATE_PATH, state)


def load_admin_commands():
    commands = load_json_file(ADMIN_COMMANDS_PATH, [])
    return commands if isinstance(commands, list) else []


def save_admin_commands(commands):
    save_json_file(ADMIN_COMMANDS_PATH, commands if isinstance(commands, list) else [])


def load_admin_settings():
    data = load_json_file(ADMIN_SETTINGS_PATH, {})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("latest_version", "1.0.0")
    data.setdefault("minimum_version", "1.0.0")
    data.setdefault("release_notes", "")
    data.setdefault("download_url", "")
    data.setdefault("installer_sha256", "")
    data.setdefault("mandatory_after", "")
    return data


def save_admin_settings(settings):
    current = load_admin_settings()
    for key in ("latest_version", "minimum_version", "release_notes", "download_url", "installer_sha256", "mandatory_after"):
        if key in settings and settings[key] is not None:
            current[key] = str(settings[key]).strip()
    save_json_file(ADMIN_SETTINGS_PATH, current)
    return current


def clean_support_text(value, limit=4000):
    text = str(value or "").replace("\x00", "").strip()
    return text[:limit]


def load_support_threads():
    data = load_json_file(SUPPORT_THREADS_PATH, [])
    return data if isinstance(data, list) else []


def save_support_threads(threads):
    if not isinstance(threads, list):
        threads = []
    save_json_file(SUPPORT_THREADS_PATH, threads[-500:])


def public_support_thread(thread):
    if not isinstance(thread, dict):
        return {}
    messages = thread.get("messages") if isinstance(thread.get("messages"), list) else []
    clean_messages = []
    for item in messages[-40:]:
        if not isinstance(item, dict) or item.get("deleted"):
            continue
        clean_messages.append({
            "id": item.get("id") or "",
            "at": item.get("at") or "",
            "from": item.get("from") or "",
            "message": clean_support_text(item.get("message"), 4000),
            "command_id": item.get("command_id") or "",
            "internal": bool(item.get("internal")),
        })
    notes = thread.get("notes") if isinstance(thread.get("notes"), list) else []
    clean_notes = []
    for note in notes[-20:]:
        if not isinstance(note, dict) or note.get("deleted"):
            continue
        clean_notes.append({
            "id": note.get("id") or "",
            "at": note.get("at") or "",
            "note": clean_support_text(note.get("note"), 2000),
        })
    return {
        "id": thread.get("id") or "",
        "status": thread.get("status") or "open",
        "archived": bool(thread.get("archived")),
        "deleted": bool(thread.get("deleted")),
        "category": clean_support_text(thread.get("category") or "General", 80),
        "priority": clean_support_text(thread.get("priority") or "normal", 40),
        "unread_admin_count": int(thread.get("unread_admin_count") or 0),
        "subject": clean_support_text(thread.get("subject"), 160),
        "ndors_trainer_id": thread.get("ndors_trainer_id") or "",
        "email": thread.get("email") or "",
        "device_id": thread.get("device_id") or "",
        "device_name": thread.get("device_name") or "",
        "app_version": thread.get("app_version") or "",
        "build": thread.get("build") or "",
        "created_at": thread.get("created_at") or "",
        "updated_at": thread.get("updated_at") or "",
        "last_admin_reply_at": thread.get("last_admin_reply_at") or "",
        "last_user_message_at": thread.get("last_user_message_at") or "",
        "summary": clean_support_text(thread.get("summary"), 2500),
        "status_payload": redact_sensitive(thread.get("status_payload") if isinstance(thread.get("status_payload"), dict) else {}),
        "messages": clean_messages,
        "notes": clean_notes,
    }


def find_support_thread(threads, ndors, subject, thread_id=""):
    norm_id = (thread_id or "").strip()
    if norm_id:
        for thread in threads:
            if isinstance(thread, dict) and thread.get("id") == norm_id:
                return thread
    norm_ndors = (ndors or "").strip().lower()
    norm_subject = (subject or "").strip().lower()
    for thread in reversed(threads):
        if not isinstance(thread, dict):
            continue
        if (thread.get("status") or "open") != "open":
            continue
        if (thread.get("ndors_trainer_id") or "").strip().lower() != norm_ndors:
            continue
        if (thread.get("subject") or "").strip().lower() == norm_subject:
            return thread
    return None


def queue_support_reply_command(ndors, title, message, thread_id):
    payload = {
        "title": clean_support_text(title or "TrainerMate support reply", 120),
        "message": clean_support_text(message, 900),
        "category": "info",
        "support_thread_id": thread_id,
    }
    try:
        return public_db_command(db_create_admin_command(ndors, "", "show_message", payload))
    except Exception:
        commands = load_admin_commands()
        command = {
            "id": secrets.token_urlsafe(18),
            "created_at": utc_now(),
            "updated_at": utc_now(),
            "ndors_trainer_id": ndors,
            "device_id": "",
            "command_type": "show_message",
            "payload": payload,
            "status": "queued",
            "message": "",
            "result": {},
        }
        commands.append(command)
        save_admin_commands(commands)
        return public_command(command)


def update_info_for_version(app_version: str | None):
    settings = load_admin_settings()
    current = (app_version or "").strip()
    latest = (settings.get("latest_version") or "").strip()
    minimum = (settings.get("minimum_version") or "").strip()
    update_available = bool(current and latest and version_tuple(current) < version_tuple(latest))
    update_required = bool(current and minimum and version_tuple(current) < version_tuple(minimum))
    return {
        "current_version": current,
        "latest_version": latest,
        "minimum_version": minimum,
        "update_available": update_available,
        "update_required": update_required,
        "download_url": settings.get("download_url") or "",
        "installer_sha256": settings.get("installer_sha256") or "",
        "mandatory_after": settings.get("mandatory_after") or "",
        "release_notes": settings.get("release_notes") or "",
    }


def version_tuple(value):
    parts = []
    for part in str(value or "").replace("-", ".").split("."):
        try:
            parts.append(int("".join(ch for ch in part if ch.isdigit()) or 0))
        except Exception:
            parts.append(0)
    return tuple((parts + [0, 0, 0])[:3])


def public_command(command):
    return {
        "id": command.get("id"),
        "created_at": command.get("created_at"),
        "ndors_trainer_id": command.get("ndors_trainer_id"),
        "device_id": command.get("device_id") or "",
        "command_type": command.get("command_type"),
        "payload": command.get("payload") or {},
        "status": command.get("status"),
        "message": command.get("message") or "",
        "updated_at": command.get("updated_at") or "",
        "result": command.get("result") or {},
    }


ADMIN_COMMAND_TYPES = {
    "health_check",
    "request_logs",
    "support_bundle",
    "refresh_certificates",
    "sync_today",
    "sync_all",
    "sync_courses",
    "repair_certificate_cache",
    "show_message",
    # New remote administration commands. These are consumed by the desktop app.
    "provider_add",
    "provider_update",
    "provider_remove",
    "provider_pause",
    "provider_resume",
    "provider_test_login",
    "courses_snapshot",
    "refresh_licence",
}


def redact_sensitive(value):
    """Return a copy of value with secrets masked before admin display/storage.

    Keep raw provider credentials only in the short-lived command payload that the
    desktop app polls. Command result handling clears that payload after use.
    """
    sensitive_keys = {
        "password",
        "provider_password",
        "client_secret",
        "access_token",
        "refresh_token",
        "token",
        "licence_key",
        "authorization",
        "cookie",
    }
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            key_text = str(key).lower()
            if key_text in sensitive_keys or any(marker in key_text for marker in ("password", "passcode", "secret", "token", "authorization", "cookie")):
                out[key] = "***"
            else:
                out[key] = redact_sensitive(item)
        return out
    if isinstance(value, list):
        return [redact_sensitive(item) for item in value]
    return value


def public_db_command(command):
    item = dict(command or {})
    if "redacted_payload" in item:
        item["payload"] = item.get("redacted_payload") or {}
    else:
        item["payload"] = redact_sensitive(item.get("payload") or {})
    item["result"] = redact_sensitive(item.get("result") or {})
    return public_command(item)


def get_account_by_ndors_optional(ndors_trainer_id: str):
    try:
        return get_account_by_ndors(ndors_trainer_id)
    except Exception:
        return None


def db_table_available(table_name: str) -> bool:
    try:
        execute_supabase(supabase.table(table_name).select("*").limit(1), f"check {table_name} table")
        return True
    except Exception:
        return False


def db_list_device_heartbeats(limit=1000):
    try:
        result = execute_supabase(
            supabase.table("device_heartbeats")
            .select("*")
            .order("last_seen_at", desc=True)
            .limit(limit),
            "read device heartbeats",
        )
        return result.data or []
    except Exception:
        return []


def db_list_recent_commands(limit=200):
    try:
        result = execute_supabase(
            supabase.table("admin_commands")
            .select("*")
            .order("created_at", desc=True)
            .limit(limit),
            "read admin commands",
        )
        return result.data or []
    except Exception:
        return []


def db_list_recent_courses(ndors_trainer_id: str, limit=80):
    try:
        result = execute_supabase(
            supabase.table("synced_courses")
            .select("*")
            .eq("ndors_trainer_id", ndors_trainer_id)
            .order("source_date_time_text")
            .limit(limit),
            "read synced courses",
        )
        return result.data or []
    except Exception:
        return []


def db_find_active_admin_command(ndors_trainer_id: str, device_id: str, command_type: str):
    """Return an existing active command so accidental double-clicks do not queue duplicates."""
    try:
        query = (
            supabase.table("admin_commands")
            .select("*")
            .eq("ndors_trainer_id", ndors_trainer_id)
            .eq("command_type", command_type)
            .in_("status", ["queued", "sent", "running"])
            .order("created_at", desc=True)
            .limit(10)
        )
        result = execute_supabase(query, "find active admin command")
        rows = result.data or []
        target_device = (device_id or "").strip()
        for row in rows:
            row_device = (row.get("device_id") or "").strip()
            # Empty device_id means any device. Treat it as the same command scope.
            if not target_device or not row_device or row_device == target_device:
                return row
    except Exception:
        return None
    return None


def json_find_active_admin_command(ndors_trainer_id: str, device_id: str, command_type: str):
    target_ndors = (ndors_trainer_id or "").strip().lower()
    target_device = (device_id or "").strip()
    for command in reversed(load_admin_commands()):
        if (command.get("ndors_trainer_id") or "").strip().lower() != target_ndors:
            continue
        if (command.get("command_type") or "") != command_type:
            continue
        if command.get("status") not in {"queued", "sent", "running"}:
            continue
        row_device = (command.get("device_id") or "").strip()
        if not target_device or not row_device or row_device == target_device:
            return command
    return None


def db_cancel_active_admin_commands(ndors_trainer_id: str, device_id: str = "", command_type: str | None = None, message: str = "Cancelled from admin."):
    query = (
        supabase.table("admin_commands")
        .select("*")
        .eq("ndors_trainer_id", ndors_trainer_id)
        .in_("status", ["queued", "sent", "running"])
        .order("created_at", desc=True)
        .limit(100)
    )
    if command_type:
        query = query.eq("command_type", command_type)
    result = execute_supabase(query, "find commands to cancel")
    rows = result.data or []
    target_device = (device_id or "").strip()
    ids = []
    for row in rows:
        row_device = (row.get("device_id") or "").strip()
        if target_device and row_device and row_device != target_device:
            continue
        ids.append(row.get("id"))
    ids = [item for item in ids if item]
    if not ids:
        return 0
    execute_supabase(
        supabase.table("admin_commands").update({
            "status": "cancelled",
            "message": message,
            "updated_at": utc_now(),
            "completed_at": utc_now(),
        }).in_("id", ids),
        "cancel active admin commands",
    )
    return len(ids)


def json_cancel_active_admin_commands(ndors_trainer_id: str, device_id: str = "", command_type: str | None = None, message: str = "Cancelled from admin."):
    commands = load_admin_commands()
    target_ndors = (ndors_trainer_id or "").strip().lower()
    target_device = (device_id or "").strip()
    changed = 0
    for command in commands:
        if (command.get("ndors_trainer_id") or "").strip().lower() != target_ndors:
            continue
        if command_type and (command.get("command_type") or "") != command_type:
            continue
        if command.get("status") not in {"queued", "sent", "running"}:
            continue
        row_device = (command.get("device_id") or "").strip()
        if target_device and row_device and row_device != target_device:
            continue
        command["status"] = "cancelled"
        command["message"] = message
        command["updated_at"] = utc_now()
        changed += 1
    if changed:
        save_admin_commands(commands)
    return changed


def db_cancel_admin_command_by_id(command_id: str, message: str = "Cancelled from admin."):
    command_id = (command_id or "").strip()
    if not command_id:
        return 0
    result = execute_supabase(
        supabase.table("admin_commands")
        .select("*")
        .eq("id", command_id)
        .limit(1),
        "find command to cancel by id",
    )
    rows = result.data or []
    if not rows:
        return 0
    row = rows[0]
    if row.get("status") not in {"queued", "sent", "running"}:
        return 0
    execute_supabase(
        supabase.table("admin_commands").update({
            "status": "cancelled",
            "message": message,
            "updated_at": utc_now(),
            "completed_at": utc_now(),
        }).eq("id", command_id),
        "cancel command by id",
    )
    return 1


def json_cancel_admin_command_by_id(command_id: str, message: str = "Cancelled from admin."):
    command_id = (command_id or "").strip()
    commands = load_admin_commands()
    changed = 0
    for command in commands:
        if (command.get("id") or "") != command_id:
            continue
        if command.get("status") not in {"queued", "sent", "running"}:
            return 0
        command["status"] = "cancelled"
        command["message"] = message
        command["updated_at"] = utc_now()
        changed = 1
        break
    if changed:
        save_admin_commands(commands)
    return changed


def db_create_admin_command(ndors_trainer_id: str, device_id: str, command_type: str, payload: dict | None):
    account = get_account_by_ndors_optional(ndors_trainer_id)
    raw_payload = payload or {}
    row = {
        "account_id": account.get("id") if account else None,
        "ndors_trainer_id": ndors_trainer_id,
        "device_id": device_id or None,
        "command_type": command_type,
        "payload": raw_payload,
        "redacted_payload": redact_sensitive(raw_payload),
        "status": "queued",
        "message": "",
        "result": {},
        "created_by": "admin",
    }
    result = execute_supabase(
        supabase.table("admin_commands").insert(row),
        "create admin command",
    )
    return (result.data or [row])[0]


def db_upsert_device_heartbeat(payload: ClientHeartbeatRequest, status: dict):
    account = get_account_by_ndors_optional(payload.ndors_trainer_id)
    providers = status.get("providers") if isinstance(status.get("providers"), list) else []
    zoom_accounts = status.get("zoom_accounts") if isinstance(status.get("zoom_accounts"), list) else []
    update_info = status.get("update") if isinstance(status.get("update"), dict) else update_info_for_version(payload.app_version)
    row = {
        "account_id": account.get("id") if account else None,
        "ndors_trainer_id": payload.ndors_trainer_id,
        "email": payload.email,
        "device_id": payload.device_id,
        "device_name": payload.device_name,
        "app_version": payload.app_version,
        "build": payload.build,
        "status": redact_sensitive(status),
        "providers": redact_sensitive(providers),
        "zoom_accounts": redact_sensitive(zoom_accounts),
        "update_info": update_info,
        "last_message": status.get("last_message") or status.get("message") or "",
        "last_status": status.get("last_status") or "",
        "sync_running": bool(status.get("sync_running")),
        "needs_attention": bool(status.get("needs_attention")),
        "last_seen_at": utc_now(),
    }
    result = execute_supabase(
        supabase.table("device_heartbeats")
        .upsert(row, on_conflict="ndors_trainer_id,device_id"),
        "upsert device heartbeat",
    )
    return (result.data or [row])[0]


def course_row_from_snapshot(account, ndors_trainer_id: str, device_id: str, course: dict):
    course_key = str(course.get("course_key") or course.get("id") or course.get("key") or "").strip()
    if not course_key:
        provider = str(course.get("provider") or course.get("provider_name") or "").strip()
        date_time = str(course.get("date_time") or course.get("source_date_time_text") or "").strip()
        title = str(course.get("title") or "").strip()
        course_key = f"{provider}::{date_time}::{title}".strip(":")
    if not course_key:
        return None
    return {
        "account_id": account.get("id") if account else None,
        "ndors_trainer_id": ndors_trainer_id,
        "device_id": device_id,
        "course_key": course_key,
        "provider_id": course.get("provider_id") or course.get("provider") or "",
        "provider_name": course.get("provider_name") or course.get("provider") or "",
        "title": course.get("title") or "",
        "source_date_time_text": course.get("date_time") or course.get("source_date_time_text") or "",
        "sync_status": course.get("sync_status") or course.get("status") or "",
        "sync_message": course.get("sync_message") or course.get("message") or course.get("last_message") or "",
        "last_synced_at": course.get("last_synced_at") or None,
        "active_in_portal": course.get("active_in_portal"),
        "has_zoom": course.get("has_zoom"),
        "zoom_status": course.get("zoom_status") or "",
        "raw_summary": redact_sensitive(course),
        "updated_at": utc_now(),
    }


def db_upsert_synced_courses(ndors_trainer_id: str, device_id: str, courses: list):
    if not isinstance(courses, list) or not courses:
        return 0
    account = get_account_by_ndors_optional(ndors_trainer_id)
    rows = []
    for course in courses:
        if not isinstance(course, dict):
            continue
        row = course_row_from_snapshot(account, ndors_trainer_id, device_id, course)
        if row:
            rows.append(row)
    if not rows:
        return 0
    execute_supabase(
        supabase.table("synced_courses")
        .upsert(rows, on_conflict="ndors_trainer_id,device_id,course_key"),
        "upsert synced courses",
    )
    return len(rows)


def db_save_support_bundle(payload: ClientCommandResultRequest):
    result_payload = payload.result or {}
    if not isinstance(result_payload, dict):
        return
    support_bundle = result_payload.get("support_bundle") or result_payload.get("bundle")
    if not support_bundle:
        return
    account = get_account_by_ndors_optional(payload.ndors_trainer_id)
    try:
        execute_supabase(
            supabase.table("support_bundles").insert({
                "account_id": account.get("id") if account else None,
                "ndors_trainer_id": payload.ndors_trainer_id,
                "device_id": payload.device_id,
                "command_id": payload.command_id,
                "bundle_type": "support_bundle",
                "summary": redact_sensitive(result_payload.get("summary") or {}),
                "payload": redact_sensitive(support_bundle),
            }),
            "save support bundle",
        )
    except Exception:
        pass


def admin_account_rows(limit=200):
    result = execute_supabase(
        supabase.table("accounts")
        .select("*")
        .order("created_at", desc=True)
        .limit(limit),
        "admin read accounts",
    )
    return result.data or []


def admin_usage_rows():
    result = execute_supabase(
        supabase.table("usage").select("*"),
        "admin read usage",
    )
    return result.data or []


def admin_device_rows():
    result = execute_supabase(
        supabase.table("devices").select("*"),
        "admin read devices",
    )
    return result.data or []


def admin_snapshot():
    accounts = admin_account_rows()
    settings = load_admin_settings()
    support_threads = sorted(
        [public_support_thread(thread) for thread in load_support_threads() if isinstance(thread, dict) and not thread.get("deleted")],
        key=lambda item: item.get("updated_at") or item.get("created_at") or "",
    )
    usage_by_account = {row.get("account_id"): row for row in admin_usage_rows() if isinstance(row, dict)}
    devices_by_account = {}
    for device in admin_device_rows():
        devices_by_account.setdefault(device.get("account_id"), []).append(device)

    # Prefer database-backed live admin state, but keep JSON fallback for older installs.
    db_heartbeats = db_list_device_heartbeats()
    state_devices = (load_admin_state().get("devices") or {})
    json_heartbeats = [value for value in state_devices.values() if isinstance(value, dict)]
    all_heartbeats = db_heartbeats or json_heartbeats

    db_commands = db_list_recent_commands()
    json_commands = load_admin_commands()
    commands = db_commands or json_commands

    users = []
    for account in accounts:
        account_id = account.get("id")
        ndors = (account.get("ndors_trainer_id") or "").strip()
        account_devices = devices_by_account.get(account_id, [])
        heartbeat_items = [
            value for value in all_heartbeats
            if isinstance(value, dict)
            and (value.get("ndors_trainer_id") or "").strip().lower() == ndors.lower()
        ]
        latest_heartbeat = sorted(heartbeat_items, key=lambda item: item.get("last_seen_at") or "")[-1] if heartbeat_items else {}
        status_payload = latest_heartbeat.get("status") if isinstance(latest_heartbeat.get("status"), dict) else {}
        providers = latest_heartbeat.get("providers") if isinstance(latest_heartbeat.get("providers"), list) else status_payload.get("providers")
        if not isinstance(providers, list):
            providers = []
        zoom_accounts = latest_heartbeat.get("zoom_accounts") if isinstance(latest_heartbeat.get("zoom_accounts"), list) else status_payload.get("zoom_accounts")
        if not isinstance(zoom_accounts, list):
            zoom_accounts = []
        app_version = latest_heartbeat.get("app_version") or ""
        update = latest_heartbeat.get("update_info") if isinstance(latest_heartbeat.get("update_info"), dict) else update_info_for_version(app_version)
        update_needed = bool(update.get("update_required"))
        usage = usage_by_account.get(account_id, {})
        pending_commands = [
            cmd for cmd in commands
            if (cmd.get("ndors_trainer_id") or "").strip().lower() == ndors.lower()
            and cmd.get("status") in {"queued", "sent", "running"}
        ]
        user_support_threads = [
            thread for thread in support_threads
            if (thread.get("ndors_trainer_id") or "").strip().lower() == ndors.lower()
        ]
        courses = db_list_recent_courses(ndors, limit=80)
        users.append({
            "id": account_id,
            "ndors_trainer_id": account.get("ndors_trainer_id"),
            "email": account.get("primary_email"),
            "plan": account.get("plan", "free"),
            "status": account.get("status", "active"),
            "created_at": account.get("created_at"),
            "last_sync_at": usage.get("last_sync_at"),
            "free_syncs_used": usage.get("free_syncs_used", 0),
            "device_count": len(account_devices),
            "devices": account_devices,
            "latest_device": latest_heartbeat,
            "providers": providers,
            "zoom_accounts": zoom_accounts,
            "courses": courses,
            "course_count": len(courses),
            "provider_count": len(providers),
            "active_provider_count": sum(1 for provider in providers if provider.get("active")),
            "zoom_connected": any((account.get("status") or "connected") == "connected" for account in zoom_accounts),
            "update_needed": update_needed,
            "update": update,
            "latest_version": settings.get("latest_version"),
            "minimum_version": settings.get("minimum_version"),
            "pending_commands": len(pending_commands),
            "support_threads": user_support_threads[:12],
            "open_support_threads": sum(1 for thread in user_support_threads if not thread.get("archived") and (thread.get("status") or "open") == "open"),
        })
    public_commands = [public_db_command(command) for command in commands[:80]] if db_commands else [public_command(command) for command in commands[-80:]][::-1]
    return {
        "users": users,
        "commands": public_commands,
        "support_threads": support_threads[::-1][:120],
        "audit": load_json_file(ADMIN_AUDIT_PATH, [])[-80:][::-1],
        "settings": settings,
        "stats": {
            "users": len(users),
            "active": sum(1 for user in users if user.get("status") == "active"),
            "paid": sum(1 for user in users if user.get("plan") in {"paid", "admin"}),
            "needs_attention": sum(1 for user in users if (user.get("latest_device") or {}).get("needs_attention") or (user.get("latest_device") or {}).get("status", {}).get("needs_attention")),
            "online_recent": sum(1 for user in users if (user.get("latest_device") or {}).get("last_seen_at")),
            "outdated": sum(1 for user in users if user.get("update_needed")),
            "open_support": sum(1 for thread in support_threads if not thread.get("archived") and (thread.get("status") or "open") == "open"),
        },
    }


def execute_supabase(query, operation="database operation"):
    """Execute a Supabase query with short retries.

    Supabase/PostgREST can occasionally disconnect. That should not make the
    local licensing API return a 500 to the dashboard.
    """
    last_exc = None
    for attempt in range(SUPABASE_RETRIES + 1):
        try:
            return query.execute()
        except Exception as exc:
            last_exc = exc
            print(f"[SUPABASE] {operation} failed on attempt {attempt + 1}: {exc}")
            if attempt < SUPABASE_RETRIES:
                time.sleep(SUPABASE_RETRY_DELAY_SECONDS)
    raise TemporaryLicensingBackendError(f"{operation} failed after retries: {last_exc}")


def account_features(plan: str):
    """Return explicit feature flags for the dashboard.

    Keep old keys (automation/calendar) for backwards compatibility, but expose
    clearer paid/free controls so the UI can gently gate paid features without
    hiding the rest of TrainerMate.
    """
    paid = plan in ("paid", "admin")
    sync_window_days = 84 if paid else 21
    return {
        # Backwards-compatible keys used by older dashboard/bot builds.
        "automation": paid,
        "calendar": paid,
        "zoom_creation": True,

        # Clearer feature flags for current dashboard builds.
        "manual_sync": True,
        "admin_triggered_sync": paid,
        "automatic_sync": paid,
        "scheduled_sync": paid,
        "calendar_sync": paid,
        "certificate_view": True,
        "certificate_manage": paid,
        "provider_setup": True,
        "zoom_connection": True,
        "sync_window_days": sync_window_days,
    }


def cache_key(ndors_trainer_id: str):
    return (ndors_trainer_id or "").strip().lower()


def load_access_cache():
    return load_json_file(LICENSING_CACHE_PATH, {})


def save_access_cache(cache):
    if isinstance(cache, dict):
        save_json_file(LICENSING_CACHE_PATH, cache)


def cache_access_response(ndors_trainer_id: str, response: dict):
    key = cache_key(ndors_trainer_id)
    if not key or not isinstance(response, dict):
        return
    cache = load_access_cache()
    cache[key] = {
        "cached_at": utc_now(),
        "response": response,
    }
    save_access_cache(cache)


def get_cached_access_response(ndors_trainer_id: str):
    key = cache_key(ndors_trainer_id)
    if not key:
        return None
    item = load_access_cache().get(key)
    if not isinstance(item, dict):
        return None
    response = item.get("response")
    if not isinstance(response, dict):
        return None

    out = dict(response)
    out["reason"] = "cached_access"
    out["licensing_cache_used"] = True
    out["licensing_cached_at"] = item.get("cached_at")
    return out


def temporary_access_response(ndors_trainer_id: str):
    cached = get_cached_access_response(ndors_trainer_id)
    if cached:
        return cached

    # First-run fallback: conservative but usable. Once Supabase responds again,
    # the proper account plan is cached and used.
    return {
        "allowed": True,
        "reason": "licensing_temporarily_unavailable",
        "plan": "free",
        "free_syncs_remaining": FREE_SYNC_LIMIT,
        "features": account_features("free"),
        "licensing_cache_used": False,
    }


def queue_pending_sync_record(payload: AccessRequest):
    pending = load_json_file(PENDING_SYNC_RECORDS_PATH, [])
    if not isinstance(pending, list):
        pending = []
    pending.append({
        "queued_at": utc_now(),
        "ndors_trainer_id": payload.ndors_trainer_id,
        "email": payload.email,
        "device_id": payload.device_id,
        "device_name": payload.device_name,
    })
    save_json_file(PENDING_SYNC_RECORDS_PATH, pending)


def best_account_row(rows):
    """Pick the account row the trainer/admin expects when duplicates exist.

    Older builds could create more than one row for the same NDORS trainer ID.
    If one duplicate is Paid and another is Free, PostgREST .limit(1) could
    return the Free row and make the desktop look downgraded. Prefer active
    Paid/Admin rows, then any Paid/Admin row, then active rows.
    """
    if not rows:
        return None

    def score(row):
        plan = str(row.get("plan") or "free").strip().lower()
        status = str(row.get("status") or "active").strip().lower()
        paid_score = 2 if plan == "admin" else 1 if plan == "paid" else 0
        active_score = 1 if status == "active" else 0
        created = str(row.get("updated_at") or row.get("created_at") or "")
        return (paid_score, active_score, created)

    return sorted([r for r in rows if isinstance(r, dict)], key=score)[-1]


def get_accounts_by_ndors(ndors_trainer_id: str):
    ndors = (ndors_trainer_id or "").strip()
    if not ndors:
        return []
    result = execute_supabase(
        supabase.table("accounts")
        .select("*")
        .eq("ndors_trainer_id", ndors),
        "read account"
    )
    return result.data or []


def update_accounts_by_ndors(ndors_trainer_id: str, updates: dict, operation: str):
    ndors = (ndors_trainer_id or "").strip()
    if not ndors:
        return []
    execute_supabase(
        supabase.table("accounts").update(updates).eq("ndors_trainer_id", ndors),
        operation,
    )
    return get_accounts_by_ndors(ndors)


def get_account_by_ndors(ndors_trainer_id: str):
    return best_account_row(get_accounts_by_ndors(ndors_trainer_id))


def account_email_matches(account_rows, email: str):
    supplied = (email or "").strip().lower()
    if not supplied:
        return False
    account_ids = []
    for row in account_rows or []:
        if not isinstance(row, dict):
            continue
        account_id = row.get("id")
        if account_id:
            account_ids.append(account_id)
        primary = (row.get("primary_email") or "").strip().lower()
        if primary and primary == supplied:
            return True
    if not account_ids:
        return False
    try:
        result = execute_supabase(
            supabase.table("account_logins")
            .select("*")
            .in_("account_id", account_ids),
            "read account login emails",
        )
        for row in result.data or []:
            if (row.get("email") or "").strip().lower() == supplied:
                return True
    except Exception:
        pass
    return False


def create_free_account(ndors_trainer_id: str, email: str | None, password_hash_value: str | None = None):
    row = {
        "ndors_trainer_id": ndors_trainer_id,
        "primary_email": email,
        "plan": "free",
        "status": "active",
    }
    if password_hash_value:
        row["password_hash"] = password_hash_value
        row["password_set_at"] = utc_now()
        row["password_must_change"] = False

    created = execute_supabase(
        supabase.table("accounts")
        .insert(row),
        "create free account"
    )

    if not created.data:
        raise HTTPException(status_code=500, detail="Failed to create account")

    account = created.data[0]

    execute_supabase(
        supabase.table("usage").insert({
            "account_id": account["id"],
            "free_syncs_used": 0
        }),
        "create usage row"
    )

    if email:
        execute_supabase(
            supabase.table("account_logins").insert({
                "account_id": account["id"],
                "email": email,
                "is_primary": True
            }),
            "create account login"
        )

    return account


def ensure_device(account_id: str, device_id: str, device_name: str | None):
    existing = execute_supabase(
        supabase.table("devices")
        .select("*")
        .eq("account_id", account_id)
        .eq("device_id", device_id)
        .limit(1),
        "read device"
    )

    if existing.data:
        execute_supabase(
            supabase.table("devices").update({
                "last_seen_at": utc_now(),
                "device_name": device_name
            }).eq("id", existing.data[0]["id"]),
            "update device"
        )
        return existing.data[0]

    created = execute_supabase(
        supabase.table("devices")
        .insert({
            "account_id": account_id,
            "device_id": device_id,
            "device_name": device_name,
            "status": "active"
        }),
        "create device"
    )
    return created.data[0] if created.data else None


def get_usage(account_id: str):
    result = execute_supabase(
        supabase.table("usage")
        .select("*")
        .eq("account_id", account_id)
        .limit(1),
        "read usage"
    )
    if result.data:
        return result.data[0]

    created = execute_supabase(
        supabase.table("usage")
        .insert({"account_id": account_id, "free_syncs_used": 0}),
        "create usage"
    )
    return created.data[0] if created.data else {"account_id": account_id, "free_syncs_used": 0}


def access_response_for_account(account: dict, app_version: str | None):
    plan = account.get("plan", "free")
    update = update_info_for_version(app_version)
    usage = get_usage(account["id"])
    free_used = int((usage or {}).get("free_syncs_used", 0) or 0)
    free_remaining = max(0, FREE_SYNC_LIMIT - free_used)
    allowed = bool(account.get("status") == "active" and not update.get("update_required") and not (plan == "free" and free_remaining <= 0))
    reason = "ok"
    if account.get("status") != "active":
        reason = "account_inactive"
    elif update.get("update_required"):
        reason = "update_required"
    elif plan == "free" and free_remaining <= 0:
        reason = "free_sync_limit_reached"
    return {
        "allowed": allowed,
        "reason": reason,
        "plan": plan,
        "password_must_change": bool(account.get("password_must_change")),
        "free_syncs_remaining": free_remaining if allowed else 0,
        "features": account_features(plan),
        "update": update,
    }


@app.get("/health")
def health():
    return {"ok": True}


def admin_portal_html():
    return r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TrainerMate Admin</title>
<style>
:root{--bg:#eef3f8;--panel:#fff;--ink:#0f172a;--muted:#64748b;--line:#d8e0ea;--blue:#2454d6;--blue2:#1d4ed8;--green:#047857;--red:#b91c1c;--amber:#b45309;--soft:#f8fafc}
*{box-sizing:border-box}
body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:var(--bg);color:var(--ink)}
.shell{display:grid;grid-template-columns:258px 1fr;min-height:100vh}
.side{background:#0f172a;color:#dbeafe;padding:18px;position:sticky;top:0;height:100vh;overflow:auto}
.brand{font-weight:950;font-size:21px;margin-bottom:6px}
.brand-sub{font-size:12px;color:#93c5fd;margin-bottom:20px}
.nav{display:grid;gap:6px}
.nav button{width:100%;text-align:left;border:0;background:transparent;color:#cbd5e1;padding:11px 12px;border-radius:10px;font-weight:800;cursor:pointer}
.nav button:hover{background:#172554;color:#fff}
.nav button.active{background:#2454d6;color:#fff}
.side-note{margin-top:18px;border-top:1px solid rgba(255,255,255,.12);padding-top:14px;font-size:12px;color:#94a3b8;line-height:1.4}
.main{padding:22px;display:grid;gap:16px;max-width:1720px}
.top{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}
h1{margin:0 0 4px;font-size:34px}
h2{margin:0;font-size:22px}
h3{margin:0;font-size:16px}
.badge{background:#dcfce7;color:#166534;padding:8px 12px;border-radius:999px;font-weight:900;font-size:12px}
.cards{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px}
.card,.panel{background:var(--panel);border:1px solid var(--line);border-radius:12px;box-shadow:0 1px 2px rgba(15,23,42,.04)}
.card{padding:16px}.card strong{display:block;font-size:30px;margin-top:2px}.card span{color:#526179}
.grid{display:grid;grid-template-columns:1.12fr .88fr;gap:16px}
.grid-2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}
.grid-3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
.panel{padding:16px}
.section-title{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:12px}
.stack{display:grid;gap:12px}
.table-wrap{overflow:auto;border-radius:10px}
.table{width:100%;border-collapse:collapse}
th,td{text-align:left;border-bottom:1px solid #e5eaf1;padding:10px;font-size:14px;vertical-align:top}
th{color:#526179;font-size:12px;text-transform:uppercase;letter-spacing:.03em}
.user-row{cursor:pointer}.user-row:hover{background:#f8fafc}.user-row.selected{background:#eff6ff}
.muted{color:var(--muted)}
.small{font-size:12px}
.actions{display:flex;flex-wrap:wrap;gap:8px}
button,select,input,textarea{border-radius:9px;border:1px solid #cbd5e1;padding:9px 10px;font:inherit}
button{background:var(--blue2);color:#fff;border:0;font-weight:900;cursor:pointer}
button:hover{filter:brightness(.96)}
button:disabled{opacity:.55;cursor:not-allowed}
.soft{background:#e2e8f0;color:#172033}
.danger{background:#b91c1c;color:#fff}
.success{background:#047857;color:#fff}
.ghost{background:#f8fafc;color:#172033;border:1px solid #d8e0ea}
.input-row{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.ok{color:#047857;font-weight:900}.bad{color:#b91c1c;font-weight:900}.warn{color:#b45309;font-weight:900}
.pill{display:inline-flex;align-items:center;gap:4px;border:1px solid #d8e0ea;border-radius:999px;padding:4px 9px;font-size:12px;margin:2px;background:#f8fafc;font-weight:800}
.pill.ok{border-color:#bbf7d0;background:#f0fdf4;color:#166534}.pill.bad{border-color:#fecaca;background:#fef2f2;color:#991b1b}.pill.warn{border-color:#fed7aa;background:#fff7ed;color:#9a3412}.pill.blue{border-color:#bfdbfe;background:#eff6ff;color:#1e40af}
.detail-box{border:1px solid #e5eaf1;background:#f8fafc;border-radius:10px;padding:12px}
.notice{border:1px solid #bfdbfe;background:#eff6ff;color:#1e3a8a;border-radius:10px;padding:10px;font-weight:800}
.notice.bad{border-color:#fecaca;background:#fef2f2;color:#991b1b}.notice.ok{border-color:#bbf7d0;background:#f0fdf4;color:#166534}.notice.warn{border-color:#fed7aa;background:#fff7ed;color:#9a3412}
.log{font-family:ui-monospace,Consolas,monospace;font-size:12px;background:#0b1220;color:#dbeafe;border-radius:10px;padding:12px;max-height:360px;overflow:auto;white-space:pre-wrap}
.view{display:none}.view.active{display:grid;gap:16px}
.cmd-card{border:1px solid #e5eaf1;border-radius:10px;padding:10px;background:#fff;display:grid;gap:4px}
.cmd-head{display:flex;justify-content:space-between;gap:8px;align-items:center}
.kv{display:grid;grid-template-columns:150px 1fr;gap:6px;font-size:14px}
.empty{padding:18px;border:1px dashed #cbd5e1;border-radius:10px;color:#64748b;background:#f8fafc}
.provider-card{border:1px solid #e5eaf1;border-radius:12px;padding:12px;background:#fff;display:grid;gap:8px}
.provider-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
.tabs{display:flex;gap:8px;flex-wrap:wrap}
.tabs button{background:#f8fafc;color:#172033;border:1px solid #d8e0ea}
.tabs button.active{background:#2454d6;color:#fff;border-color:#2454d6}
.help{background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;border-radius:10px;padding:10px;font-size:13px;line-height:1.45}
.tip{display:inline-flex;align-items:center;justify-content:center;width:19px;height:19px;border-radius:999px;background:#e0f2fe;color:#075985;font-size:12px;font-weight:950;cursor:help;margin-left:5px}.action-group{border:1px solid #e5eaf1;background:#f8fafc;border-radius:12px;padding:12px;display:grid;gap:9px}.action-group h3{display:flex;align-items:center;gap:5px}.preset-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.message-preview{border:1px solid #d8e0ea;border-radius:12px;background:#fff;padding:12px;min-height:80px}.toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.selected-ribbon{display:flex;justify-content:space-between;align-items:center;gap:14px;position:sticky;top:0;z-index:5;border-color:#bfdbfe;background:linear-gradient(180deg,#fff,#f8fbff)}
.selected-ribbon strong{font-size:18px;margin-right:8px}
.toast-area{position:fixed;right:18px;bottom:18px;z-index:30;display:grid;gap:10px;width:min(420px,calc(100vw - 36px))}.toast{border-radius:14px;padding:12px 14px;box-shadow:0 18px 45px rgba(15,23,42,.25);background:#0f172a;color:#fff;font-weight:850;border:1px solid rgba(255,255,255,.12);animation:toastIn .18s ease-out}.toast.ok{background:#047857}.toast.warn{background:#92400e}.toast.bad{background:#991b1b}@keyframes toastIn{from{transform:translateY(8px);opacity:0}to{transform:translateY(0);opacity:1}}.action-feed{border:1px solid #d8e0ea;background:#fff;border-radius:12px;padding:10px;display:grid;gap:8px}.action-item{border-left:4px solid #bfdbfe;background:#f8fafc;border-radius:9px;padding:8px 10px;font-size:13px}.action-item.ok{border-left-color:#22c55e}.action-item.warn{border-left-color:#f59e0b}.action-item.bad{border-left-color:#ef4444}.inline-help{font-size:13px;color:#475569;line-height:1.45}.btn-row{display:flex;gap:8px;flex-wrap:wrap}.button-note{display:block;font-size:11px;font-weight:700;opacity:.85;margin-top:2px}
.simple-card{border:1px solid #dbeafe;background:linear-gradient(180deg,#f8fbff,#fff);border-radius:14px;padding:14px;display:grid;gap:10px}.simple-card h3{font-size:15px}.advanced-panel{border:1px solid #e5eaf1;border-radius:12px;background:#fff}.advanced-panel summary{cursor:pointer;padding:12px;font-weight:950;color:#334155}.advanced-panel[open] summary{border-bottom:1px solid #e5eaf1}.advanced-panel .advanced-body{padding:12px;display:grid;gap:12px}.pending-panel{border:1px solid #fed7aa;background:#fff7ed;border-radius:14px;padding:12px;display:grid;gap:9px}.pending-panel.empty{border-color:#d8e0ea;background:#f8fafc}.pending-row{border:1px solid #e5eaf1;background:#fff;border-radius:11px;padding:10px;display:grid;grid-template-columns:1fr auto;gap:8px;align-items:center}.pending-row .meta{font-size:12px;color:#64748b;margin-top:2px}.help-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.help-tile{border:1px solid #e5eaf1;background:#fff;border-radius:12px;padding:11px;line-height:1.38}.help-tile strong{display:block;margin-bottom:3px}.quiet-label{font-size:12px;text-transform:uppercase;letter-spacing:.05em;color:#64748b;font-weight:950}.primary-action{font-size:15px;padding:11px 14px}.compact-actions{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.compact-actions button{width:100%}.status-explain{border-left:4px solid #bfdbfe;background:#f8fafc;border-radius:10px;padding:9px 11px;color:#475569;font-size:13px}.status-explain.warn{border-left-color:#f59e0b}.status-explain.bad{border-left-color:#ef4444}.status-explain.ok{border-left-color:#22c55e}.support-console{display:grid;grid-template-columns:360px 1fr;gap:14px}.support-toolbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center}.support-list{display:grid;gap:8px;max-height:680px;overflow:auto;padding-right:4px}.thread-card{border:1px solid #e5eaf1;background:#fff;border-radius:14px;padding:12px;display:grid;gap:6px;cursor:pointer}.thread-card:hover{background:#f8fafc}.thread-card.active{border-color:#2454d6;background:#eff6ff}.thread-card.archived{opacity:.72}.thread-subject{font-weight:950}.thread-preview{font-size:13px;color:#475569;line-height:1.35}.unread-dot{display:inline-flex;min-width:22px;height:22px;align-items:center;justify-content:center;border-radius:999px;background:#2454d6;color:#fff;font-size:12px;font-weight:950}.conversation{display:grid;gap:10px;max-height:520px;overflow:auto;padding:4px}.bubble{max-width:78%;border:1px solid #e5eaf1;border-radius:16px;padding:10px 12px;background:#fff}.bubble.admin{margin-left:auto;background:#eff6ff;border-color:#bfdbfe}.bubble.trainer{margin-right:auto;background:#f8fafc}.bubble .meta{font-size:11px;color:#64748b;margin-bottom:4px;font-weight:800}.thread-header{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;border-bottom:1px solid #e5eaf1;padding-bottom:10px}.note-card{border-left:4px solid #f59e0b;background:#fff7ed;border-radius:10px;padding:9px 11px;font-size:13px}.filter-chip{background:#f8fafc;color:#172033;border:1px solid #d8e0ea}.filter-chip.active{background:#2454d6;color:#fff;border-color:#2454d6}.bell{position:relative}.bell .count{position:absolute;right:-6px;top:-7px;background:#ef4444;color:white;border-radius:999px;font-size:11px;min-width:18px;height:18px;display:flex;align-items:center;justify-content:center;font-weight:950}.rec-action{border-left:4px solid #2454d6;background:#eff6ff;border-radius:10px;padding:9px 11px;font-size:13px;color:#1e3a8a}.activity-timeline{display:grid;gap:8px}.timeline-item{border-left:4px solid #bfdbfe;background:#f8fafc;border-radius:10px;padding:8px 10px;font-size:13px}.timeline-item.bad{border-left-color:#ef4444}.timeline-item.warn{border-left-color:#f59e0b}.timeline-item.ok{border-left-color:#22c55e}@media(max-width:1150px){.support-console{grid-template-columns:1fr}}
@media(max-width:900px){.help-grid,.compact-actions{grid-template-columns:1fr}}
@media(max-width:1150px){.shell{grid-template-columns:1fr}.side{position:static;height:auto}.cards,.grid,.grid-2,.grid-3{grid-template-columns:1fr}.input-row{grid-template-columns:1fr}}

/* ===== TrainerMate Simple Admin Mode override ===== */
body.simple-mode{background:#f4f7fb;color:#102033}
body.simple-mode .shell{grid-template-columns:220px 1fr}
body.simple-mode .side{background:#111827;padding:16px;height:100vh}
body.simple-mode .brand{font-size:20px}
body.simple-mode .brand-sub{font-size:13px;color:#bfdbfe}
body.simple-mode .side-note{display:none}
body.simple-mode .nav{gap:8px}
body.simple-mode .nav button{font-size:15px;padding:13px 12px;border-radius:14px}
body.simple-mode .nav button.simple-hidden{display:none!important}
body.simple-mode .main{max-width:1280px;margin:0 auto;padding:24px;gap:18px}
body.simple-mode h1{font-size:30px}
body.simple-mode h2{font-size:21px}
body.simple-mode .cards{grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}
body.simple-mode .card,body.simple-mode .panel{border-color:#e6edf5;border-radius:18px;box-shadow:0 8px 24px rgba(15,23,42,.045)}
body.simple-mode .card{padding:18px}.card strong{font-size:32px}
body.simple-mode .selected-ribbon{position:static;border:2px solid #bfdbfe;border-radius:20px;padding:16px;background:#fff}
body.simple-mode .selected-ribbon .actions button{padding:11px 13px}
body.simple-mode .grid{grid-template-columns:1fr;gap:18px}
body.simple-mode #view-overview>.grid-2{display:none}
body.simple-mode #view-users .grid{grid-template-columns:1fr}
body.simple-mode #view-users .table th:nth-child(3),body.simple-mode #view-users .table td:nth-child(3),body.simple-mode #view-users .table th:nth-child(4),body.simple-mode #view-users .table td:nth-child(4),body.simple-mode #view-users .table th:nth-child(5),body.simple-mode #view-users .table td:nth-child(5){display:none}
body.simple-mode #view-users .table th,body.simple-mode #view-users .table td{font-size:15px;padding:13px 10px}
body.simple-mode .simple-start{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}
body.simple-mode .simple-start .step{background:#fff;border:1px solid #e6edf5;border-radius:18px;padding:16px;box-shadow:0 8px 24px rgba(15,23,42,.045)}
body.simple-mode .simple-start .num{display:inline-grid;place-items:center;width:30px;height:30px;border-radius:999px;background:#2454d6;color:#fff;font-weight:950;margin-bottom:8px}
body.simple-mode .simple-start strong{display:block;font-size:16px;margin-bottom:5px}
body.simple-mode .simple-start span{color:#64748b;font-size:14px;line-height:1.4}
body.simple-mode .compact-actions{grid-template-columns:repeat(4,minmax(0,1fr))}
body.simple-mode .action-feed,body.simple-mode #commandHelpBox{display:none!important}
body.simple-mode details.advanced-panel{margin-top:4px;background:#f8fafc;border-style:dashed}
body.simple-mode details.advanced-panel summary{font-size:14px;color:#64748b}
body.simple-mode .support-console{grid-template-columns:330px 1fr;gap:18px}
body.simple-mode .thread-card{border-radius:16px;padding:14px}
body.simple-mode .thread-card .pill,body.simple-mode .thread-card .small{font-size:12px}
body.simple-mode .bubble{font-size:15px;line-height:1.45}
body.simple-mode #supportFilterAll,body.simple-mode #supportFilterArchived{display:none}
body.simple-mode .toolbar .ghost{display:none}
body.simple-mode .help,body.simple-mode .notice{font-weight:700;line-height:1.45}
body.simple-mode .support-selected-hint{background:#eff6ff;border:1px solid #bfdbfe;color:#1e3a8a;border-radius:16px;padding:14px;font-weight:800}
body.simple-mode .status-explain,body.simple-mode .rec-action{font-size:14px}
body.simple-mode .provider-card{border-radius:16px}
body.simple-mode .toast{border-radius:16px;font-size:14px}
@media(max-width:1000px){body.simple-mode .shell{grid-template-columns:1fr}body.simple-mode .side{height:auto;position:static}body.simple-mode .cards,body.simple-mode .simple-start,body.simple-mode .compact-actions,body.simple-mode .support-console{grid-template-columns:1fr}}

</style>
</head>
<body>
<div class="shell">
  <aside class="side">
    <div class="brand">TrainerMate Admin</div>
    <div class="brand-sub">Control centre</div>
    <div class="nav" id="nav">
      <button data-view="overview" class="active">Overview</button>
      <button data-view="users">Users</button>
      <button data-view="courses">Courses</button>
      <button data-view="providers">Providers / FOBS</button>
      <button data-view="messages">Messages</button>
      <button data-view="licences">Licences</button>
      <button data-view="devices">Devices</button>
      <button data-view="commands">Activity</button>
      <button data-view="support">Support</button>
      <button data-view="releases">Releases</button>
      <button data-view="audit">Audit</button>
    </div>
    <div class="side-note">
      Less clutter, safer defaults: common actions are up front, advanced tools are tucked away, pending commands can be cancelled, and messages are persistent.
    </div>
  </aside>

  <main class="main">
    <div class="top">
      <div>
        <h1>TrainerMate Admin</h1>
        <div class="muted">Remote support, licence control, FOBS/provider admin, synced courses, and app health</div>
      </div>
      <div class="actions"><button class="ghost bell" onclick="setView('support')" title="Open support inbox">Support <span id="supportBell" class="count" style="display:none">0</span></button><span class="badge">Production</span></div>
    </div>

    <section class="cards">
      <div class="card"><span>Users</span><strong id="statUsers">0</strong></div>
      <div class="card"><span>Paid</span><strong id="statPaid">0</strong></div>
      <div class="card"><span>Recent installs</span><strong id="statOnline">0</strong></div>
      <div class="card"><span>Needs attention</span><strong id="statAttention">0</strong></div>
      <div class="card"><span>Open support</span><strong id="statSupport">0</strong></div>
    </section>

    <section class="panel selected-ribbon" id="selectedRibbon">
      <div>
        <div class="muted small">Selected trainer</div>
        <strong id="ribbonTrainer">None selected</strong>
        <span id="ribbonMeta" class="muted"></span>
      </div>
      <div class="actions">
        <button class="soft" onclick="setView('users')">Choose user</button>
        <button onclick="sendCommand('health_check')">Health</button>
        <button onclick="sendCommand('courses_snapshot')">Courses</button>
        <button class="soft" onclick="openMessageComposer('selected')" title="Send a persistent support message to the selected trainer">Message</button>
      </div>
    </section>

    <section id="view-overview" class="view active">
      <div class="grid">
        <div class="panel">
          <div class="section-title"><h2>Needs attention</h2><button class="ghost" onclick="load()">Refresh</button></div>
          <div id="attentionList" class="stack"></div>
        </div>
        <div class="panel stack">
          <div class="section-title"><h2>Selected trainer</h2><span id="selectedBadge" class="pill">None</span></div>
          <div id="adminNotice" class="notice">Ready. Choose a trainer, then run one clear action at a time.</div>
          <div id="selectedSummary" class="empty">Choose a trainer from Users or Needs attention.</div>
          <div class="simple-card">
            <div><span class="quiet-label">Most useful actions</span><h3>Start here</h3></div>
            <div class="compact-actions">
              <button class="primary-action" onclick="sendCommand('health_check')" title="Safe diagnostic. Checks app, provider, Zoom and sync status. Does not change anything.">Check health<span class="button-note">safe diagnostic</span></button>
              <button class="primary-action" onclick="sendCommand('courses_snapshot')" title="Safe snapshot. Refreshes admin's view of courses. Does not update FOBS or Zoom.">Refresh courses<span class="button-note">safe snapshot</span></button>
              <button class="primary-action" onclick="sendSyncCourses('7days')" title="Runs normal TrainerMate sync for the next 7 days. May update FOBS/Zoom under normal rules.">Sync next 7 days<span class="button-note">normal sync</span></button>
              <button class="soft primary-action" onclick="openMessageComposer('selected')" title="Open the message centre composer for the selected trainer.">Message trainer<span class="button-note">persistent inbox</span></button>
            </div>
          </div>
          <div id="selectedCommandCards" class="pending-panel empty"></div>
          <details class="advanced-panel">
            <summary>Advanced support and sync tools</summary>
            <div class="advanced-body">
              <div class="help-grid">
                <div class="help-tile"><strong>Support bundle</strong>Collects a larger redacted diagnostic snapshot. Useful when something failed. Does not intentionally change courses.</div>
                <div class="help-tile"><strong>Request logs</strong>Requests recent app logs/debug messages. Useful for deeper troubleshooting.</div>
                <div class="help-tile"><strong>Refresh certificates</strong>Checks provider certificate/document status. May log into provider portals.</div>
                <div class="help-tile"><strong>Full sync</strong>Runs the trainer’s normal sync across their allowed licence window. May update FOBS/Zoom.</div>
              </div>
              <div class="actions">
                <button class="soft" onclick="sendCommand('support_bundle')" title="Collect a redacted support snapshot.">Support bundle</button>
                <button class="soft" onclick="sendCommand('request_logs')" title="Request recent app logs.">Request logs</button>
                <button class="soft" onclick="sendCommand('refresh_certificates')" title="Refresh provider certificate/document status.">Refresh certs</button>
                <button class="soft" onclick="sendSyncCourses('licence')" title="Sync the full licence window through the trainer app.">Sync full window</button>
                <button class="danger" onclick="cancelPendingCommands()" title="Cancel queued/sent/running commands for this trainer where possible.">Cancel pending</button>
              </div>
            </div>
          </details>
          <div class="status-explain" id="commandHelpBox"><strong>Admin command guide:</strong> most remote functions are queued. If the trainer is offline, the app will run them next time it opens.</div>
          <div class="action-feed"><div class="section-title"><strong>Admin request confirmations</strong><button class="ghost" onclick="clearActionLog()">Clear</button></div><div id="adminActionLog" class="stack"></div></div>
        </div>
      </div>
      <div class="grid-2">
        <div class="panel"><div class="section-title"><h2>Recent commands</h2><button class="ghost" onclick="setView('commands')">Open commands</button></div><div id="recentCommands" class="stack"></div></div>
        <div class="panel"><div class="section-title"><h2>Latest course activity</h2><button class="ghost" onclick="setView('courses')">Open courses</button></div><div id="latestCourses" class="stack"></div></div>
      </div>
    </section>

    <section id="view-users" class="view">
      <div class="grid">
        <div class="panel">
          <div class="section-title"><h2>Users</h2><input id="userSearch" placeholder="Search user, email, issue" oninput="renderAll()"></div>
          <div class="table-wrap"><table class="table"><thead><tr><th>User</th><th>Plan</th><th>App</th><th>Providers</th><th>Courses</th><th>Issue</th></tr></thead><tbody id="users"></tbody></table></div>
        </div>
        <div class="panel stack">
          <div class="section-title"><h2>Quick actions</h2><span id="quickUserBadge" class="pill">No user selected</span></div>
          <div id="selectedUserDetail" class="empty">Choose a user from the table.</div>
          <div class="simple-card">
            <div><span class="quiet-label">Everyday actions</span><h3>Simple trainer controls</h3></div>
            <div class="compact-actions">
              <button onclick="sendCommand('health_check')" title="Safe diagnostic. Checks app version, last sync, providers, Zoom and warnings. Does not change anything.">Health check<span class="button-note">safe diagnostic</span></button>
              <button onclick="sendCommand('courses_snapshot')" title="Safe snapshot. Asks the app to send admin its latest course list. Does not update FOBS or Zoom.">Refresh courses<span class="button-note">safe snapshot</span></button>
              <button onclick="sendSyncCourses('7days')" title="Normal TrainerMate sync for the next 7 days. May update Zoom/FOBS under normal rules.">Sync next 7 days<span class="button-note">recommended</span></button>
              <button class="soft" onclick="openMessageComposer('selected')" title="Send a persistent message to the trainer dashboard Message Centre.">Message trainer<span class="button-note">opens composer</span></button>
            </div>
          </div>
          <div id="userPendingActions" class="pending-panel empty"></div>
          <details class="advanced-panel">
            <summary>Advanced tools, account actions and full sync</summary>
            <div class="advanced-body">
              <div class="status-explain"><strong>Advanced tools:</strong> these are less common. Commands that need the trainer app will wait if the user is offline.</div>
              <div class="action-group"><h3>Diagnostics</h3><div class="actions">
                <button class="soft" onclick="sendCommand('support_bundle')" title="Collects a larger redacted diagnostic package for troubleshooting.">Support bundle</button>
                <button class="soft" onclick="sendCommand('request_logs')" title="Requests recent app logs. Use after a failed sync or error.">Request logs</button>
              </div></div>
              <div class="action-group"><h3>Sync options</h3><div class="actions">
                <button onclick="sendSyncCourses('today')" title="Runs normal sync for today only.">Sync today</button>
                <button class="soft" onclick="sendSyncCourses('licence')" title="Runs normal sync for the trainer’s full allowed licence window.">Sync full allowed window</button>
                <button class="soft" onclick="sendCommand('refresh_certificates')" title="Refresh provider certificate/document status.">Refresh certs</button>
              </div>
              <details class="advanced-sync"><summary>Custom sync options</summary>
                <div class="input-row"><select id="syncProviderSelect"><option value="all">All providers</option></select><select id="syncWindowSelect"><option value="7">Next 7 days</option><option value="14">Next 14 days</option><option value="21">Next 21 days</option><option value="0">Full allowed window</option></select></div>
                <div class="input-row"><select id="syncModeSelect"><option value="normal">Normal sync</option><option value="check_only">Check only / course snapshot</option><option value="repair">Repair Zoom mismatches if allowed</option><option value="certificates_only">Certificates only</option></select><button onclick="sendAdvancedSync()">Run custom sync</button></div>
                <div class="help">“Check only” is the safest custom mode. It refreshes visibility without updating FOBS or Zoom.</div>
              </details></div>
              <div class="action-group"><h3>Account and licence</h3><div class="actions">
                <button class="soft" onclick="setAccount('paid','active')">Set paid</button>
                <button class="soft" onclick="setAccount('free','active')">Set free</button>
                <button class="soft" onclick="resetTrial()">Reset trial</button>
                <button class="soft" onclick="forceResetPassword()">Force password reset</button>
                <button class="soft" onclick="sendUpdatePrompt()">Prompt update</button>
                <button class="soft" onclick="requestZoomReconnect()">Ask Zoom reconnect</button>
                <button class="danger" onclick="setAccount(null,'suspended')">Suspend</button>
                <button class="danger" onclick="deleteUser()">Delete user</button>
              </div></div>
              <button class="danger" onclick="cancelPendingCommands()">Cancel all pending actions for this trainer</button>
            </div>
          </details>
        </div>
      </div>
    </section>

    <section id="view-courses" class="view">
      <div class="panel">
        <div class="section-title">
          <div><h2>Courses</h2><div class="muted">Safe course summaries from the selected trainer's app.</div></div>
          <div class="actions"><select id="courseFilter" onchange="renderCourses()"><option value="all">All</option><option value="attention">Needs attention</option><option value="zoom">Missing / unknown Zoom</option><option value="next30">Next 30 days</option></select><button onclick="sendCommand('courses_snapshot')" title="Safe snapshot. Does not update FOBS or Zoom.">Refresh course list</button><button onclick="sendSyncCourses('7days')" title="Run normal sync for the next 7 days.">Sync next 7 days</button><button class="soft" onclick="sendSyncCourses('licence')" title="Run normal sync for the trainer’s allowed licence window.">Sync full window</button></div>
        </div>
        <div id="coursesTable"></div>
      </div>
    </section>

    <section id="view-providers" class="view">
      <div class="grid">
        <div class="panel">
          <div class="section-title"><div><h2>Configured providers</h2><div class="muted">Based on the latest trainer heartbeat.</div></div><button onclick="sendCommand('health_check')">Refresh status</button></div>
          <div id="providerCards" class="stack"></div>
        </div>
        <div class="panel stack">
          <div><h2>Add / update FOBS provider</h2><div class="muted">Credentials are sent once, saved on the trainer's computer, then cleared from command payloads.</div></div>
          <div class="input-row"><input id="providerId" placeholder="provider id, e.g. west-mids"><input id="providerName" placeholder="Provider name, e.g. West Mids"></div>
          <input id="providerLoginUrl" placeholder="FOBS login URL">
          <div class="input-row"><input id="providerUsername" placeholder="FOBS username"><input id="providerPassword" placeholder="FOBS password" type="password" autocomplete="new-password"></div>
          <label class="small"><input id="providerManagesZoom" type="checkbox"> Provider manages Zoom / TrainerMate should not overwrite provider Zoom links</label>
          <div class="actions">
            <button onclick="sendProviderCommand('provider_add')">Add provider</button>
            <button class="soft" onclick="sendProviderCommand('provider_update')">Update provider</button>
            <button class="soft" onclick="sendProviderCommand('provider_test_login')">Test login</button>
            <button class="danger" onclick="sendProviderCommand('provider_remove')">Remove provider</button>
          </div>
          <div class="help">Use provider IDs like <b>essex</b>, <b>west-mids</b>, or <b>lincolnshire</b>. Removing a provider stops future sync for that provider but does not delete historical courses.</div>
        </div>
      </div>
    </section>

    <section id="view-messages" class="view">
      <div class="grid">
        <div class="panel stack">
          <div>
            <h2>Message centre <span class="tip" title="Messages are delivered through the trainer app and saved locally in their TrainerMate message centre until read or dismissed.">?</span></h2>
            <div class="muted">Send a persistent support notice to one trainer or to a filtered group.</div>
          </div>
          <div class="input-row">
            <select id="messageTarget" onchange="updateMessageTargetHelp()">
              <option value="selected">Selected trainer only</option>
              <option value="all">All users</option>
              <option value="active">Active users</option>
              <option value="paid">Paid/admin users</option>
              <option value="needs_attention">Users needing attention</option>
            </select>
            <select id="messageCategory">
              <option value="info">Info</option>
              <option value="warning">Warning/action needed</option>
              <option value="success">Success/update</option>
            </select>
          </div>
          <input id="messageTitle" placeholder="Message title" value="TrainerMate support message">
          <textarea id="messageBody" rows="7" placeholder="Type the message the trainer should see in their Message Centre"></textarea>
          <div class="preset-grid">
            <button class="soft" onclick="useMessagePreset('zoom')" title="Preset: ask the trainer to reconnect Zoom">Zoom reconnect</button>
            <button class="soft" onclick="useMessagePreset('open')" title="Preset: ask the trainer to open TrainerMate so support commands can run">Open TrainerMate</button>
            <button class="soft" onclick="useMessagePreset('fobs')" title="Preset: ask trainer to update their FOBS login details">FOBS password</button>
            <button class="soft" onclick="useMessagePreset('update')" title="Preset: ask trainer to update TrainerMate">App update</button>
          </div>
          <div class="actions">
            <button onclick="sendMessageCentreMessage()" title="Queue this message through the admin command system. The user dashboard stores it in its Message Centre.">Send message</button>
            <button class="soft" onclick="clearMessageComposer()">Clear</button>
          </div>
          <div id="messageTargetHelp" class="help">Selected trainer only. Choose a trainer first if no trainer is selected.</div>
        </div>
        <div class="panel stack">
          <div><h2>Recent messages</h2><div class="muted">Latest message commands and delivery results.</div></div>
          <div id="messageHistory" class="stack"></div>
        </div>
      </div>
    </section>

    <section id="view-licences" class="view">
      <div class="grid-2">
        <div class="panel stack">
          <h2>Account control</h2>
          <div id="licenceSelected" class="empty">Select a trainer first.</div>
          <div class="actions">
            <button class="success" onclick="setAccount('paid','active')">Activate paid</button>
            <button class="soft" onclick="setAccount('free','active')">Set free</button>
            <button class="soft" onclick="resetTrial()">Reset free trial</button>
            <button class="danger" onclick="setAccount(null,'suspended')">Suspend account</button>
          </div>
        </div>
        <div class="panel stack">
          <h2>Create licence key</h2>
          <input id="licenceFor" placeholder="NDORS trainer ID, optional">
          <select id="licencePlan"><option value="paid">Paid</option><option value="admin">Admin</option></select>
          <input id="licenceExpiry" placeholder="Expiry date YYYY-MM-DD, optional">
          <button onclick="createLicence()">Create licence</button>
          <div id="licenceOutput" class="log">No licence created yet.</div>
        </div>
      </div>
    </section>

    <section id="view-devices" class="view">
      <div class="panel"><div class="section-title"><h2>Devices</h2><span class="muted">Latest device and heartbeat information</span></div><div id="devicesTable"></div></div>
    </section>

    <section id="view-commands" class="view">
      <div class="panel">
        <div class="section-title"><h2>Activity timeline</h2><div class="actions"><button class="soft" onclick="cancelPendingCommands()">Cancel selected user's pending</button><button class="ghost" onclick="load()">Refresh</button></div></div>
        <div id="commandsTable"></div>
      </div>
    </section>

    <section id="view-support" class="view">
      <div class="panel stack">
        <div class="section-title">
          <div>
            <h2>Support inbox</h2>
            <div class="muted">Threaded trainer messages, private notes, quick replies, diagnostics and gentle live alerts.</div>
          </div>
          <div class="actions"><button class="ghost" onclick="load()">Refresh</button><button onclick="openMessageComposer('selected')">New message</button></div>
        </div>
        <div class="support-toolbar">
          <input id="supportSearch" placeholder="Search threads, trainer, email, message" oninput="renderSupportThreads()">
          <button id="supportFilterOpen" class="filter-chip active" onclick="setSupportFilter('open')">Open</button>
          <button id="supportFilterWaiting" class="filter-chip" onclick="setSupportFilter('waiting')">Waiting</button>
          <button id="supportFilterResolved" class="filter-chip" onclick="setSupportFilter('resolved')">Resolved</button>
          <button id="supportFilterArchived" class="filter-chip" onclick="setSupportFilter('archived')">Archived</button>
          <button id="supportFilterAll" class="filter-chip" onclick="setSupportFilter('all')">All</button>
        </div>
        <div class="support-console">
          <div class="stack">
            <div class="notice">Live support checks are on. New trainer replies will pop up gently without using browser alert boxes.</div>
            <div id="supportThreads" class="support-list"></div>
          </div>
          <div class="stack">
            <div id="supportThreadDetail" class="empty">Choose a support thread to open the conversation.</div>
            <div class="grid-2">
              <div class="panel"><div class="section-title"><h2>Latest diagnostic result</h2></div><div id="supportOutput" class="log">No support result yet.</div></div>
              <div class="panel stack"><h2>Support shortcuts</h2><div class="compact-actions"><button onclick="sendCommand('health_check')">Health check<span class="button-note">safe</span></button><button onclick="sendCommand('courses_snapshot')">Refresh courses<span class="button-note">safe</span></button><button class="soft" onclick="sendCommand('support_bundle')">Support bundle<span class="button-note">diagnostic</span></button><button class="soft" onclick="requestZoomReconnect()">Ask Zoom reconnect<span class="button-note">message</span></button></div><div class="help">Use health check first. Escalate to support bundle or request logs only when you need deeper detail.</div></div>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section id="view-releases" class="view">
      <div class="panel stack">
        <h2>Release control</h2>
        <div class="grid-2">
          <input id="latestVersion" placeholder="Latest version">
          <input id="minimumVersion" placeholder="Minimum supported version">
          <input id="downloadUrl" placeholder="Installer download URL">
          <input id="installerSha256" placeholder="Installer SHA256">
          <input id="mandatoryAfter" placeholder="Mandatory after YYYY-MM-DD, optional">
        </div>
        <textarea id="releaseNotes" rows="5" placeholder="Release notes"></textarea>
        <div class="actions"><button onclick="saveSettings()">Save release settings</button><button class="soft" onclick="sendUpdatePrompt()">Prompt selected user</button></div>
        <div class="muted">Apps below the minimum version are blocked from syncing. Download URL and SHA256 are sent in update info.</div>
      </div>
    </section>

    <section id="view-audit" class="view">
      <div class="panel"><div class="section-title"><h2>Audit log</h2><button class="ghost" onclick="load()">Refresh</button></div><div class="log" id="audit"></div></div>
    </section>
  </main>
</div>
<div id="toastArea" class="toast-area"></div>

<script>
let selected=localStorage.getItem('tm_admin_selected')||null, snapshot=null, currentView=localStorage.getItem('tm_admin_view')||'overview', lastActionAt=0;
let supportFilter=localStorage.getItem('tm_support_filter')||'open', activeSupportThread=localStorage.getItem('tm_active_support_thread')||'', liveStarted=false, lastSnapshotSeen=null;
let actionLog=[]; try{actionLog=JSON.parse(localStorage.getItem('tm_admin_action_log')||'[]')||[]}catch(e){actionLog=[]}
const esc=(s)=>String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
const fmt=(s)=>s?String(s).replace('T',' ').replace(/\.\d+.*/,'').replace(/\+00:00/,' UTC'):'';
const lower=(s)=>String(s??'').toLowerCase();
function ago(s){
  if(!s) return 'Never';
  const t=Date.parse(String(s)); if(Number.isNaN(t)) return fmt(s);
  const sec=Math.max(0,Math.floor((Date.now()-t)/1000));
  if(sec<60) return sec+'s ago'; if(sec<3600) return Math.floor(sec/60)+'m ago'; if(sec<86400) return Math.floor(sec/3600)+'h ago';
  return Math.floor(sec/86400)+'d ago';
}
async function api(path, opts={}){
  const r=await fetch(path,{credentials:'same-origin',headers:{'Content-Type':'application/json',...(opts.headers||{})},...opts});
  if(!r.ok) throw new Error(await r.text());
  return r.json();
}
function setView(name){
  currentView=name||'overview';
  localStorage.setItem('tm_admin_view', currentView);
  document.querySelectorAll('.view').forEach(v=>v.classList.toggle('active',v.id==='view-'+currentView));
  document.querySelectorAll('#nav button').forEach(b=>b.classList.toggle('active',b.dataset.view===currentView));
  renderAll();
}
document.querySelectorAll('#nav button').forEach(b=>b.addEventListener('click',()=>setView(b.dataset.view)));
function looksLikeNdors(value){const text=String(value||'').trim(); return !!text && !text.includes('@') && !text.includes(' ') && /^[A-Za-z0-9_-]+$/.test(text) && text.length<=64;}
function userByNdors(ndors){return ((snapshot&&snapshot.users)||[]).find(x=>String(x.ndors_trainer_id)===String(ndors));}
function userBySelected(value){
  const text=String(value||'').trim();
  if(!text) return null;
  return ((snapshot&&snapshot.users)||[]).find(x=>String(x.ndors_trainer_id)===text)
    || ((snapshot&&snapshot.users)||[]).find(x=>String(x.email||'').toLowerCase()===text.toLowerCase())
    || null;
}
function selectedUser(){return selected?userBySelected(selected):null;}
function selectedNdors(){
  const u=selectedUser();
  const ndors=String((u&&u.ndors_trainer_id)||selected||'').trim();
  return looksLikeNdors(ndors)?ndors:'';
}
function selectedIdentifier(){
  const u=selectedUser();
  return String((u&&u.ndors_trainer_id)||selected||'').trim();
}
function commandsFor(ndors){return ((snapshot&&snapshot.commands)||[]).filter(c=>String(c.ndors_trainer_id)===String(ndors));}
function issueFor(user){
  const d=user.latest_device||{}, s=d.status||{};
  if(user.open_support_threads) return user.open_support_threads+' open support topic(s)';
  if(user.update_needed) return 'App update needed';
  if(d.needs_attention || s.needs_attention) return d.last_message || s.message || s.last_message || 'Needs attention';
  if(user.pending_commands) return user.pending_commands+' command(s) pending';
  if(!d.last_seen_at) return 'No recent app heartbeat';
  return 'OK';
}
function issueClass(user){const i=issueFor(user); return i==='OK'?'ok':(i.includes('pending')?'warn':'bad');}
function commandClass(status){status=lower(status); if(status==='completed') return 'ok'; if(['failed','expired'].includes(status)) return 'bad'; if(['queued','sent','running'].includes(status)) return 'warn'; return '';}
const COMMAND_INFO={
  health_check:{label:'Health check',safe:true,needsApp:true,desc:'Safe diagnostic. Reports whether the app is online plus version, sync, providers, Zoom and health warnings. It does not change anything.'},
  courses_snapshot:{label:'Refresh courses',safe:true,needsApp:true,desc:'Safe visibility tool. Sends admin a fresh safe list of courses from the trainer app. It does not change FOBS or Zoom.'},
  support_bundle:{label:'Support bundle',safe:true,needsApp:true,desc:'Collects a larger redacted diagnostic snapshot for troubleshooting.'},
  request_logs:{label:'Request logs',safe:true,needsApp:true,desc:'Requests recent logs/debug information from the trainer app.'},
  refresh_certificates:{label:'Refresh certs',safe:false,needsApp:true,desc:'Checks provider certificates/documents. May log into FOBS/provider portals.'},
  sync_today:{label:'Sync today',safe:false,needsApp:true,desc:'Runs today’s sync workflow. May update Zoom/FOBS under normal rules.'},
  sync_all:{label:'Sync all',safe:false,needsApp:true,desc:'Legacy wider sync command. Prefer Sync full allowed window.'},
  sync_courses:{label:'Sync courses',safe:false,needsApp:true,desc:'Runs a structured course sync through the trainer’s own app. May update Zoom/FOBS unless using Check only mode.'},
  show_message:{label:'Send message',safe:true,needsApp:true,desc:'Queues a persistent message in the trainer dashboard message centre.'},
  provider_add:{label:'Add provider',safe:false,needsApp:true,desc:'Adds a provider on the trainer app. Credentials are saved locally to that computer.'},
  provider_update:{label:'Update provider',safe:false,needsApp:true,desc:'Updates provider settings/login on the trainer app. Credentials are redacted after use.'},
  provider_remove:{label:'Remove provider',safe:false,needsApp:true,desc:'Removes a provider from the trainer app. Existing historical course records are not deleted.'},
  provider_pause:{label:'Pause provider',safe:false,needsApp:true,desc:'Pauses provider sync on the trainer app without deleting the provider.'},
  provider_resume:{label:'Resume provider',safe:false,needsApp:true,desc:'Resumes a paused provider on the trainer app.'},
  provider_test_login:{label:'Test FOBS login',safe:true,needsApp:true,desc:'Tests the saved provider username/password from the trainer computer.'}
};
function commandLabel(type){return (COMMAND_INFO[type]&&COMMAND_INFO[type].label)||type||'Command';}
function commandHelp(type){return (COMMAND_INFO[type]&&COMMAND_INFO[type].desc)||'Remote admin command. If the trainer app is offline, it will be queued until the app next checks in.';}
function isLikelyOffline(u){const d=(u&&u.latest_device)||{}; if(!d.last_seen_at) return true; const t=Date.parse(String(d.last_seen_at)); return Number.isNaN(t)||((Date.now()-t)>10*60*1000);}
function trainerOnlineText(u){if(!u) return 'No trainer selected.'; const d=u.latest_device||{}; if(!d.last_seen_at) return 'No recent app heartbeat. Commands will wait until the app opens.'; const offline=isLikelyOffline(u); return offline?`Last seen ${ago(d.last_seen_at)}. Commands will queue until the app opens.`:`Last seen ${ago(d.last_seen_at)}. Commands should run shortly.`;}
function renderActionLog(){const box=document.getElementById('adminActionLog'); if(!box) return; box.innerHTML=actionLog.slice(0,8).map(a=>`<div class="action-item ${esc(a.kind||'')}"><strong>${esc(a.title||'Admin request')}</strong><div>${esc(a.message||'')}</div><div class="muted small">${esc(a.when||'')}</div></div>`).join('')||'<div class="empty">No admin requests in this browser session yet.</div>'; }
function pushAction(title,message,kind=''){const item={title,message,kind,when:new Date().toLocaleTimeString()}; actionLog.unshift(item); actionLog=actionLog.slice(0,20); try{localStorage.setItem('tm_admin_action_log',JSON.stringify(actionLog));}catch(e){} renderActionLog();}
function clearActionLog(){actionLog=[]; try{localStorage.removeItem('tm_admin_action_log');}catch(e){} renderActionLog();}
function showToast(msg,kind=''){const area=document.getElementById('toastArea'); if(!area) return; const t=document.createElement('div'); t.className='toast '+kind; t.textContent=msg; area.prepend(t); setTimeout(()=>{t.style.opacity='0'; t.style.transform='translateY(8px)'; setTimeout(()=>t.remove(),250)},5200);}
function setNotice(msg,kind=''){const n=document.getElementById('adminNotice'); if(n){n.className='notice '+kind; n.textContent=msg;} showToast(msg,kind);}
function showActionToast(title,msg,kind='',actionText='',actionFn=null){const area=document.getElementById('toastArea'); if(!area) return; const t=document.createElement('div'); t.className='toast '+kind; t.innerHTML=`<strong>${esc(title)}</strong><div class="small" style="margin-top:4px">${esc(msg)}</div>${actionText?`<button class="ghost" style="margin-top:8px" type="button">${esc(actionText)}</button>`:''}`; if(actionText&&actionFn){t.querySelector('button').onclick=()=>{actionFn(); t.remove();};} area.prepend(t); setTimeout(()=>{t.style.opacity='0'; t.style.transform='translateY(8px)'; setTimeout(()=>t.remove(),250)},8000);}
function snapshotSignature(data){const threads=(data&&data.support_threads)||[], cmds=(data&&data.commands)||[]; return {threadCount:threads.length, unread:threads.reduce((a,t)=>a+(Number(t.unread_admin_count)||0),0), latestThread:(threads[0]&&((threads[0].id||'')+'|'+(threads[0].updated_at||'')+'|'+(threads[0].unread_admin_count||0)))||'', latestCommand:(cmds[0]&&((cmds[0].id||'')+'|'+(cmds[0].status||'')+'|'+(cmds[0].updated_at||cmds[0].created_at||'')))||''};}
function handleLiveChanges(oldSnap,newSnap){if(!oldSnap||!newSnap) return; const oldThreads=Object.fromEntries(((oldSnap.support_threads)||[]).map(t=>[t.id,t])); const newThreads=(newSnap.support_threads)||[]; const changed=newThreads.find(t=>(Number(t.unread_admin_count)||0)>0 && (!oldThreads[t.id] || (t.updated_at||'')>(oldThreads[t.id].updated_at||''))); if(changed){const last=latestThreadMessage(changed); showActionToast('New support message', `${changed.ndors_trainer_id||'Trainer'}: ${String(last.message||changed.subject||'New message').slice(0,110)}`, 'warn', 'Open thread', ()=>{activeSupportThread=changed.id; localStorage.setItem('tm_active_support_thread',activeSupportThread); if(changed.ndors_trainer_id) pick(changed.ndors_trainer_id); setView('support'); renderSupportThreads();});}
 const oldCmds=Object.fromEntries(((oldSnap.commands)||[]).map(c=>[c.id,c])); const done=((newSnap.commands)||[]).find(c=>oldCmds[c.id] && oldCmds[c.id].status!==c.status && ['completed','failed','cancelled'].includes(lower(c.status))); if(done) showActionToast(commandLabel(done.command_type), `${done.ndors_trainer_id||''} is now ${done.status}. ${done.message||''}`, commandClass(done.status));
}
function supportUnreadTotal(){return ((snapshot&&snapshot.support_threads)||[]).reduce((a,t)=>a+(Number(t.unread_admin_count)||0),0);}
function updateSupportBell(){const b=document.getElementById('supportBell'); if(!b) return; const n=supportUnreadTotal(); b.textContent=n; b.style.display=n?'flex':'none';}
function startLiveUpdates(){if(liveStarted) return; liveStarted=true; setInterval(()=>load({silent:true}),10000); setInterval(()=>{if(pendingCommandsForSelected().length) load({silent:true});},6000);}
function renderStats(){
  const st=(snapshot&&snapshot.stats)||{};
  statUsers.textContent=st.users||0; statPaid.textContent=st.paid||0; statOnline.textContent=st.online_recent||0; statAttention.textContent=st.needs_attention||0; statSupport.textContent=st.open_support||0;
}
function renderUsers(){
  const q=lower(document.getElementById('userSearch')?.value||'');
  const list=((snapshot&&snapshot.users)||[]).filter(u=>lower(JSON.stringify(u)).includes(q));
  const currentNdors=selectedNdors();
  const html=list.map(u=>`<tr class="user-row ${currentNdors===u.ndors_trainer_id?'selected':''}" onclick="pick('${esc(u.ndors_trainer_id||'')}')">
    <td><strong>${esc(u.ndors_trainer_id||'')}</strong><div class="muted">${esc(u.email||'')}</div></td>
    <td>${esc(u.plan||'')}<div class="muted">${esc(u.status||'')}</div></td>
    <td>${esc((u.latest_device&&u.latest_device.app_version)||'Unknown')}<div class="muted">${ago(u.latest_device&&u.latest_device.last_seen_at)}</div>${u.update_needed?'<div class="bad">Update needed</div>':''}</td>
    <td>${u.active_provider_count||0}/${u.provider_count||0}<div class="muted">Zoom ${u.zoom_connected?'OK':'unknown'}</div></td>
    <td>${u.course_count||0}</td>
    <td class="${issueClass(u)}">${esc(issueFor(u))}</td>
  </tr>`).join('');
  if(document.getElementById('users')) users.innerHTML=html||'<tr><td colspan="6" class="muted">No users found.</td></tr>';
}
function pick(ndors){
  const picked=userBySelected(ndors);
  selected=(picked&&picked.ndors_trainer_id)||ndors;
  if(selected) localStorage.setItem('tm_admin_selected', selected);
  const u=selectedUser();
  if(u && document.getElementById('licenceFor')){licenceFor.value=u.ndors_trainer_id||'';}
  renderAll();
  setNotice('Selected trainer '+(selectedNdors()||selected)+'.','ok');
}
function statusPills(u){
  const d=u.latest_device||{};
  return `<span class="pill ${u.status==='active'?'ok':'bad'}">${esc(u.status||'unknown')}</span>
          <span class="pill blue">${esc(u.plan||'free')}</span>
          <span class="pill ${d.last_seen_at?'ok':'bad'}">${d.last_seen_at?'Seen '+ago(d.last_seen_at):'No heartbeat'}</span>
          ${u.update_needed?'<span class="pill bad">Update required</span>':''}`;
}
function renderSelected(){
  const u=selectedUser();
  const helpBox=document.getElementById("commandHelpBox");
  const summary=document.getElementById('selectedSummary');
  const detail=document.getElementById('selectedUserDetail');
  const licence=document.getElementById('licenceSelected');
  const support=document.getElementById('supportSelected');
  const badge=document.getElementById('selectedBadge');
  const quick=document.getElementById('quickUserBadge');
  if(!u){
    [summary,detail,licence,support].forEach(el=>{if(el) el.className='empty', el.innerHTML='Choose a trainer first.'});
    if(badge) badge.textContent='None'; if(quick) quick.textContent='No user selected';
    if(helpBox) helpBox.innerHTML='<strong>Admin command guide:</strong> most remote functions are queued. If the trainer is offline, the app will run them next time it opens.';
    return;
  }
  if(badge) badge.textContent=(u.plan||'')+' / '+(u.status||'');
  if(quick) quick.textContent=u.ndors_trainer_id||'Selected';
  const d=u.latest_device||{}, s=d.status||{};
  if(helpBox) helpBox.innerHTML=`<strong>How admin requests work:</strong> ${esc(trainerOnlineText(u))}<br><span class="inline-help">Safe checks only collect information. Sync/provider actions may log into FOBS or update data according to normal TrainerMate rules. Hover the buttons for details.</span>`;
  const providers=(u.providers||[]).map(p=>`<span class="pill ${p.active?'ok':'warn'}">${esc(p.name||p.id)} ${p.active?'active':'paused'}</span>`).join('')||'<span class="muted">No provider heartbeat yet.</span>';
  const zoom=(u.zoom_accounts||[]).map(z=>`<span class="pill ${lower(z.status||'connected')==='connected'?'ok':'warn'}">${esc(z.nickname||z.email||z.id)} ${esc(z.status||'connected')}</span>`).join('')||'<span class="muted">No Zoom heartbeat yet.</span>';
  const html=`<div class="detail-box stack">
    <div>${statusPills(u)}</div>
    <div class="kv"><strong>Trainer</strong><div>${esc(u.ndors_trainer_id||'')}</div><strong>Email</strong><div>${esc(u.email||'')}</div><strong>Last seen</strong><div>${esc(ago(d.last_seen_at))}</div><strong>Device</strong><div>${esc(d.device_name||'Unknown')}</div><strong>App</strong><div>${esc(d.app_version||'Unknown')} ${esc(d.build||'')}</div><strong>Last sync</strong><div>${esc(u.last_sync_at||'Never')}</div><strong>Issue</strong><div class="${issueClass(u)}">${esc(issueFor(u))}</div></div>
    <div><strong>Providers</strong><br>${providers}</div>
    <div><strong>Zoom</strong><br>${zoom}</div>
    <div><strong>Latest app message</strong><div class="muted">${esc(d.last_message||s.last_message||s.message||'None reported')}</div></div>
  </div>`;
  [summary,detail,licence,support].forEach(el=>{if(el){el.className=''; el.innerHTML=html;}});
  renderSelectedCommands();
}
function commandStatusText(c){
  const st=lower(c&&c.status);
  if(st==='queued') return 'Waiting for the trainer app to come online and collect this.';
  if(st==='sent') return 'The trainer app has collected this. Waiting for completion.';
  if(st==='running') return 'The trainer app is working on this now.';
  if(st==='completed') return 'Completed successfully.';
  if(st==='failed') return 'The trainer app tried this and reported an error.';
  if(st==='cancelled') return 'Cancelled by admin.';
  if(st==='expired') return 'Expired before the trainer app completed it.';
  return c&&c.message?c.message:'No extra status yet.';
}
function pendingCommandsForSelected(){
  return selected?commandsFor(selected).filter(c=>['queued','sent','running'].includes(lower(c.status))):[];
}
function renderPendingActionsInto(id){
  const box=document.getElementById(id); if(!box) return;
  const pending=pendingCommandsForSelected();
  box.className='pending-panel '+(pending.length?'':'empty');
  if(!selected){box.innerHTML='<div class="empty">Choose a trainer to see pending actions.</div>';return;}
  if(!pending.length){box.innerHTML='<div class="cmd-head"><strong>No pending actions</strong><span class="pill ok">clear</span></div><div class="muted small">Nothing is queued or running for this trainer.</div>';return;}
  box.innerHTML=`<div class="cmd-head"><strong>Pending actions</strong><span class="pill warn">${pending.length} waiting/running</span></div>`+pending.map(c=>`<div class="pending-row"><div><strong>${esc(commandLabel(c.command_type))}</strong><div class="meta">${esc(c.status||'')} · ${esc(ago(c.created_at))} · ${esc(commandStatusText(c))}</div></div><button class="danger" onclick="cancelCommandById('${esc(c.id||'')}')" title="Cancel this command if it has not already been completed by the trainer app.">Cancel</button></div>`).join('')+`<button class="danger" onclick="cancelPendingCommands()">Cancel all pending for this trainer</button>`;
}
function renderSelectedCommands(){
  renderPendingActionsInto('selectedCommandCards');
  renderPendingActionsInto('userPendingActions');
}

function renderRibbon(){
  const u=selectedUser();
  const trainer=document.getElementById('ribbonTrainer');
  const meta=document.getElementById('ribbonMeta');
  if(!trainer||!meta) return;
  if(!u){trainer.textContent='None selected'; meta.textContent=''; return;}
  const d=u.latest_device||{};
  trainer.textContent=u.ndors_trainer_id||'Selected trainer';
  meta.textContent=` · ${u.email||'no email'} · ${u.plan||'free'} / ${u.status||'unknown'} · ${d.last_seen_at?'seen '+ago(d.last_seen_at):'no heartbeat'}`;
}
function recommendedActionForUser(u){const issue=lower(issueFor(u)); if(issue.includes('support')) return 'Open the support thread and reply or resolve it.'; if(issue.includes('update')) return 'Send the app update prompt.'; if(issue.includes('pending')) return 'Check Activity and cancel duplicates if needed.'; if(issue.includes('heartbeat')) return 'Ask trainer to open TrainerMate.'; return 'Run Health check first.';}
function renderAttention(){
  const list=((snapshot&&snapshot.users)||[]).filter(u=>issueFor(u)!=='OK').slice(0,12);
  attentionList.innerHTML=list.map(u=>`<div class="cmd-card" onclick="pick('${esc(u.ndors_trainer_id||'')}')" style="cursor:pointer"><div class="cmd-head"><strong>${esc(u.ndors_trainer_id||'')}</strong><span class="${issueClass(u)}">${esc(issueFor(u))}</span></div><div class="muted">${esc(u.email||'')} · ${esc((u.latest_device||{}).device_name||'No device')}</div><div class="rec-action"><strong>Suggested:</strong> ${esc(recommendedActionForUser(u))}</div></div>`).join('')||'<div class="empty">No users currently need attention.</div>';
}
function renderRecentCommands(){
  const box=document.getElementById('recentCommands'); if(!box) return;
  const cmds=((snapshot&&snapshot.commands)||[]).slice(0,8);
  box.innerHTML=cmds.map(c=>`<div class="cmd-card"><div class="cmd-head"><strong>${esc(commandLabel(c.command_type))}</strong><span class="pill ${commandClass(c.status)}">${esc(c.status||'')}</span></div><div class="muted">${esc(c.ndors_trainer_id||'')} · ${esc(ago(c.created_at))} · ${esc(c.message||'')}</div></div>`).join('')||'<div class="empty">No commands yet.</div>';
}
function allCourses(){
  const courses=[]; ((snapshot&&snapshot.users)||[]).forEach(u=>(u.courses||[]).forEach(c=>courses.push({...c,ndors_trainer_id:u.ndors_trainer_id,email:u.email})));
  return courses;
}
function renderLatestCourses(){
  const box=document.getElementById('latestCourses'); if(!box) return;
  const courses=allCourses().slice(0,8);
  box.innerHTML=courses.map(c=>`<div class="cmd-card"><div class="cmd-head"><strong>${esc(c.title||'Course')}</strong><span class="pill ${c.has_zoom?'ok':'warn'}">${c.has_zoom?'Zoom OK':'Zoom unknown'}</span></div><div class="muted">${esc(c.ndors_trainer_id||'')} · ${esc(c.provider_name||c.provider_id||'')} · ${esc(c.source_date_time_text||'')}</div></div>`).join('')||'<div class="empty">No course snapshots yet. Select a trainer and click Refresh courses.</div>';
}
function renderCourses(){
  const box=document.getElementById('coursesTable'); if(!box) return;
  const u=selectedUser(); if(!u){box.innerHTML='<div class="empty">Select a trainer first.</div>'; return;}
  let rows=u.courses||[];
  const f=document.getElementById('courseFilter')?.value||'all';
  if(f==='attention') rows=rows.filter(c=>lower(c.sync_status).includes('error')||lower(c.sync_status).includes('fail')||lower(c.sync_message).includes('error')||c.has_zoom===false);
  if(f==='zoom') rows=rows.filter(c=>!c.has_zoom);
  if(f==='next30'){const now=Date.now(), lim=now+30*864e5; rows=rows.filter(c=>{const t=Date.parse(c.source_date_time_text||c.course_start||'');return !Number.isNaN(t)&&t>=now&&t<=lim;});}
  box.innerHTML=`<div class="table-wrap"><table class="table"><thead><tr><th>Date/time</th><th>Provider</th><th>Course</th><th>Sync</th><th>Zoom</th><th>Message</th><th>Actions</th></tr></thead><tbody>${rows.map((c,i)=>`<tr><td>${esc(c.source_date_time_text||c.course_start||'')}</td><td>${esc(c.provider_name||c.provider_id||'')}</td><td><strong>${esc(c.title||'')}</strong></td><td>${esc(c.sync_status||'')}</td><td><span class="pill ${c.has_zoom?'ok':'warn'}">${c.has_zoom?'Yes':'Unknown'}</span></td><td class="muted">${esc(c.sync_message||'')}</td><td><button class="soft" onclick="syncSingleCourse(${i})" title="Queue normal sync for this one course. May update Zoom/FOBS for that course.">Sync this course</button></td></tr>`).join('')||'<tr><td colspan="7" class="muted">No courses stored yet.</td></tr>'}</tbody></table></div>`;
}
function renderProviders(){
  const box=document.getElementById('providerCards'); if(!box) return;
  const u=selectedUser(); if(!u){box.innerHTML='<div class="empty">Select a trainer first.</div>'; return;}
  const providers=u.providers||[];
  box.innerHTML=providers.map(p=>`<div class="provider-card"><div class="provider-head"><div><strong>${esc(p.name||p.id)}</strong><div class="muted small">${esc(p.id||'')}</div></div><span class="pill ${p.active?'ok':'warn'}">${p.active?'Active':'Paused'}</span></div><div class="actions"><button class="soft" onclick="fillProvider('${esc(p.id||'')}')">Use in form</button><button class="soft" onclick="sendProviderCommand('provider_test_login','${esc(p.id||'')}')">Test login</button><button class="soft" onclick="sendProviderCommand('${p.active?'provider_pause':'provider_resume'}','${esc(p.id||'')}')">${p.active?'Pause':'Resume'}</button><button class="danger" onclick="sendProviderCommand('provider_remove','${esc(p.id||'')}')">Remove</button></div><div class="muted small">Login saved: ${p.has_credentials?'yes':'unknown'} · Provider manages Zoom: ${p.provider_manages_zoom?'yes':'no'}</div></div>`).join('')||'<div class="empty">No providers reported yet. Run Health check or Refresh courses.</div>';
}
function fillProvider(id){
  const u=selectedUser(); if(!u) return;
  const p=(u.providers||[]).find(x=>String(x.id)===String(id)); if(!p) return;
  providerId.value=p.id||''; providerName.value=p.name||''; providerLoginUrl.value=p.login_url||''; providerManagesZoom.checked=!!p.provider_manages_zoom;
  setView('providers');
}
function renderDevices(){
  const box=document.getElementById('devicesTable'); if(!box) return;
  const rows=[]; ((snapshot&&snapshot.users)||[]).forEach(u=>(u.devices||[]).forEach(d=>rows.push({...d,ndors:u.ndors_trainer_id,email:u.email,latest:u.latest_device||{}})));
  box.innerHTML=`<div class="table-wrap"><table class="table"><thead><tr><th>Trainer</th><th>Device</th><th>Status</th><th>App</th><th>Last seen</th><th>Health</th></tr></thead><tbody>${rows.map(r=>`<tr><td><strong>${esc(r.ndors||'')}</strong><div class="muted">${esc(r.email||'')}</div></td><td>${esc(r.device_name||r.latest.device_name||'Unknown')}</td><td>${esc(r.status||'')}</td><td>${esc(r.app_version||r.latest.app_version||'')}</td><td>${esc(ago(r.last_seen_at||r.latest.last_seen_at))}</td><td>${r.needs_attention?'<span class="bad">Needs attention</span>':'<span class="ok">OK</span>'}</td></tr>`).join('')||'<tr><td colspan="6" class="muted">No devices found.</td></tr>'}</tbody></table></div>`;
}
function renderCommandsTable(){
  const box=document.getElementById('commandsTable'); if(!box) return;
  const rows=(snapshot&&snapshot.commands)||[];
  box.innerHTML=`<div class="status-explain"><strong>Status guide:</strong> queued = waiting for app, sent = app collected it, running = app is working, completed = finished, failed = needs attention. Pending rows can be cancelled if they have not already finished.</div><div class="table-wrap"><table class="table"><thead><tr><th>Created</th><th>Trainer</th><th>Command</th><th>Status</th><th>What it means</th><th>Message / result</th><th>Action</th></tr></thead><tbody>${rows.map(c=>`<tr><td>${esc(fmt(c.created_at))}</td><td>${esc(c.ndors_trainer_id||'')}</td><td><strong>${esc(commandLabel(c.command_type))}</strong><div class="muted small">${esc(commandHelp(c.command_type))}</div></td><td><span class="pill ${commandClass(c.status)}">${esc(c.status||'')}</span></td><td class="small">${esc(commandStatusText(c))}</td><td class="muted small">${esc(c.message||'')}${c.result&&Object.keys(c.result).length?' · '+esc(JSON.stringify(c.result).slice(0,220)):''}</td><td>${['queued','sent','running'].includes((c.status||'').toLowerCase())?`<button class="danger" onclick="cancelCommandById('${esc(c.id||'')}')" title="Cancel this command if it has not already been processed by the trainer app.">Cancel</button>`:''}</td></tr>`).join('')||'<tr><td colspan="7" class="muted">No commands yet.</td></tr>'}</tbody></table></div>`;
}
function renderSupport(){
  const box=document.getElementById('supportOutput'); if(!box) return;
  const cmds=selected?commandsFor(selected):((snapshot&&snapshot.commands)||[]);
  const latest=cmds.find(c=>c.command_type==='support_bundle'||c.command_type==='health_check'||c.command_type==='request_logs');
  box.textContent=latest?JSON.stringify(latest.result||{message:latest.message,status:latest.status},null,2):'No support result yet.';
  renderSupportThreads();
}
function supportThreadsForSelected(){
  const rows=(snapshot&&snapshot.support_threads)||[];
  const q=lower(document.getElementById('supportSearch')?.value||'');
  return rows.filter(t=>{
    if(selected && String(t.ndors_trainer_id)!==String(selected)) return false;
    if(t.deleted) return false;
    if(supportFilter==='open' && ((t.status||'open')==='resolved' || t.archived)) return false;
    if(supportFilter==='waiting' && !['waiting_for_admin','waiting_for_trainer'].includes(t.status||'')) return false;
    if(supportFilter==='resolved' && (t.status||'')!=='resolved') return false;
    if(supportFilter==='archived' && !t.archived) return false;
    if(q && !lower(JSON.stringify(t)).includes(q)) return false;
    return true;
  }).slice(0,80);
}
function setSupportFilter(name){supportFilter=name||'open'; localStorage.setItem('tm_support_filter',supportFilter); renderSupportThreads();}
function latestThreadMessage(t){const msgs=t.messages||[]; return msgs.length?msgs[msgs.length-1]:{};}
function threadStatusClass(t){if(t.priority==='urgent') return 'bad'; if(t.status==='resolved') return 'ok'; if(t.status==='waiting_for_trainer') return 'warn'; return 'blue';}
function recommendedActionForThread(t){
  const txt=lower((t.subject||'')+' '+(t.summary||'')+' '+JSON.stringify(t.status_payload||{})+' '+((latestThreadMessage(t)||{}).message||''));
  if(txt.includes('zoom')) return 'Recommended: send Zoom reconnect message, then run Health check when the trainer opens TrainerMate.';
  if(txt.includes('fobs')||txt.includes('password')||txt.includes('login')) return 'Recommended: ask trainer to check FOBS password, then use Test FOBS login.';
  if(txt.includes('sync')||txt.includes('course')) return 'Recommended: refresh course list first, then run a 7-day sync if the snapshot looks wrong.';
  if(txt.includes('update')||txt.includes('version')) return 'Recommended: send the app update prompt and check again after they reopen TrainerMate.';
  return 'Recommended: reply for more detail, then run Health check if you need app status.';
}
function renderSupportThreads(){
  const box=document.getElementById('supportThreads'); if(!box) return;
  ['Open','Waiting','Resolved','Archived','All'].forEach(n=>{const el=document.getElementById('supportFilter'+n); if(el) el.classList.toggle('active',supportFilter===n.toLowerCase());});
  const rows=supportThreadsForSelected();
  if(!rows.length){box.innerHTML=selected?'<div class="empty">No matching support conversations for this trainer.</div>':'<div class="empty">No matching support conversations yet.</div>'; renderSupportThreadDetail(null); return;}
  if(!rows.some(t=>t.id===activeSupportThread)) activeSupportThread=rows[0].id;
  box.innerHTML=rows.map(t=>{const last=latestThreadMessage(t); const unread=Number(t.unread_admin_count)||0; return `<div class="thread-card ${t.id===activeSupportThread?'active':''} ${t.archived?'archived':''}" onclick="selectSupportThread('${esc(t.id)}')">
    <div class="cmd-head"><span class="thread-subject">${esc(t.subject||'Support request')}</span>${unread?`<span class="unread-dot">${unread}</span>`:`<span class="pill ${threadStatusClass(t)}">${esc(t.status||'open')}</span>`}</div>
    <div class="muted small">${esc(t.ndors_trainer_id||'')} · ${esc(t.email||'')} · ${esc(ago(t.updated_at))}</div>
    <div>${t.category?`<span class="pill blue">${esc(t.category)}</span>`:''}<span class="pill ${t.priority==='urgent'?'bad':(t.priority==='low'?'ok':'')}">${esc(t.priority||'normal')}</span></div>
    <div class="thread-preview">${esc(last.message||t.summary||'No message preview')}</div>
  </div>`}).join('');
  renderSupportThreadDetail(rows.find(t=>t.id===activeSupportThread)||rows[0]);
}
function selectSupportThread(id){activeSupportThread=id; localStorage.setItem('tm_active_support_thread',id); renderSupportThreads(); markSupportRead(id,true);}
function renderSupportThreadDetail(t){
  const box=document.getElementById('supportThreadDetail'); if(!box) return;
  if(!t){box.className='empty'; box.innerHTML='Choose a support thread to open the conversation.'; return;}
  box.className='panel stack';
  const msgs=(t.messages||[]).map(m=>`<div class="bubble ${m.from==='admin'?'admin':'trainer'}"><div class="meta">${m.from==='admin'?'Support':'Trainer'} · ${esc(ago(m.at))}</div><div>${esc(m.message||'')}</div><div class="actions" style="margin-top:6px"><button class="ghost" onclick="copyText('${esc((m.message||'').replace(/`/g,''))}')">Copy</button><button class="ghost" onclick="deleteSupportMessage('${esc(t.id)}','${esc(m.id)}')">Delete</button></div></div>`).join('')||'<div class="empty">No messages yet.</div>';
  const notes=(t.notes||[]).map(n=>`<div class="note-card"><strong>Private note</strong><div>${esc(n.note||'')}</div><div class="muted small">${esc(ago(n.at))}</div></div>`).join('');
  box.innerHTML=`<div class="thread-header"><div><h2>${esc(t.subject||'Support request')}</h2><div class="muted">${esc(t.ndors_trainer_id||'')} · ${esc(t.email||'')} · ${esc(t.device_name||'')} · updated ${esc(ago(t.updated_at))}</div><div style="margin-top:6px"><span class="pill ${threadStatusClass(t)}">${esc(t.status||'open')}</span> ${t.unread_admin_count?`<span class="pill warn">${t.unread_admin_count} unread</span>`:''}</div></div><div class="actions"><button onclick="pick('${esc(t.ndors_trainer_id||'')}')">Select trainer</button><button class="soft" onclick="markSupportRead('${esc(t.id)}')">Mark read</button></div></div>
    <div class="rec-action"><strong>Suggested next step:</strong> ${esc(recommendedActionForThread(t))}</div>
    <div class="input-row"><select id="supportCategory-${esc(t.id)}"><option>General</option><option>Zoom</option><option>FOBS</option><option>Licence</option><option>Certificates</option><option>Sync</option><option>App update</option></select><select id="supportPriority-${esc(t.id)}"><option value="low">Low</option><option value="normal">Normal</option><option value="urgent">Urgent</option></select></div>
    <div class="actions"><button class="soft" onclick="saveSupportMeta('${esc(t.id)}')">Save category/priority</button><button class="soft" onclick="markSupportThread('${esc(t.id)}','open')">Open</button><button class="soft" onclick="markSupportThread('${esc(t.id)}','waiting_for_trainer')">Waiting for trainer</button><button class="success" onclick="markSupportThread('${esc(t.id)}','resolved')">Resolve</button><button class="soft" onclick="archiveSupportThread('${esc(t.id)}')">Archive</button><button class="danger" onclick="deleteSupportThread('${esc(t.id)}')">Delete</button></div>
    <div class="conversation">${msgs}</div>
    ${t.summary?`<div class="status-explain"><strong>Support summary</strong><br>${esc(t.summary)}</div>`:''}
    ${notes?`<div class="stack"><h3>Private admin notes</h3>${notes}</div>`:''}
    <textarea id="reply-${esc(t.id)}" rows="3" placeholder="Reply to trainer in this thread"></textarea>
    <div class="preset-grid"><button class="soft" onclick="quickReply('${esc(t.id)}','zoom')">Zoom reconnect reply</button><button class="soft" onclick="quickReply('${esc(t.id)}','fobs')">FOBS password reply</button><button class="soft" onclick="quickReply('${esc(t.id)}','open')">Ask to open app</button><button class="soft" onclick="quickReply('${esc(t.id)}','done')">Resolved reply</button></div>
    <div class="actions"><button onclick="replySupportThread('${esc(t.id)}')">Reply</button><button class="soft" onclick="addSupportNote('${esc(t.id)}')">Add private note</button><button class="soft" onclick="sendCommand('health_check')">Health check</button><button class="soft" onclick="sendCommand('request_logs')">Request logs</button></div>`;
  const cat=document.getElementById('supportCategory-'+t.id), pri=document.getElementById('supportPriority-'+t.id); if(cat) cat.value=t.category||'General'; if(pri) pri.value=t.priority||'normal';
}
function quickReply(id,kind){const el=document.getElementById('reply-'+id); if(!el) return; const r={zoom:'Please reconnect Zoom in TrainerMate, then leave the app open for a few minutes so I can check it has picked up correctly.',fobs:'Please check the FOBS username and password saved in TrainerMate. Once updated, leave the app open and I will run a login check.',open:'Please open TrainerMate and leave it running for a few minutes so the support checks can complete.',done:'Thanks, this should now be resolved. Please try again and message back here if it still is not right.'}; el.value=r[kind]||''; el.focus();}
function copyText(text){try{navigator.clipboard.writeText(text); showToast('Copied.','ok')}catch(e){}}
async function markSupportRead(id,silent=false){await api('/admin/api/support/read',{method:'POST',body:JSON.stringify({thread_id:id})}); if(!silent) setNotice('Thread marked read.','ok'); await load({silent:true});}
async function addSupportNote(id){const note=prompt('Private admin note - not shown to trainer:'); if(!note) return; await api('/admin/api/support/note',{method:'POST',body:JSON.stringify({thread_id:id,note})}); setNotice('Private note saved.','ok'); await load({silent:true});}
async function saveSupportMeta(id){const category=document.getElementById('supportCategory-'+id)?.value||'General'; const priority=document.getElementById('supportPriority-'+id)?.value||'normal'; await api('/admin/api/support/meta',{method:'POST',body:JSON.stringify({thread_id:id,category,priority})}); setNotice('Thread details updated.','ok'); await load({silent:true});}
async function archiveSupportThread(id){await api('/admin/api/support/archive',{method:'POST',body:JSON.stringify({thread_id:id})}); setNotice('Thread archived.','ok'); activeSupportThread=''; await load({silent:true});}
async function deleteSupportThread(id){if(!confirm('Delete this support thread from the admin inbox? It will be soft-deleted in the JSON file.')) return; await api('/admin/api/support/delete',{method:'POST',body:JSON.stringify({thread_id:id})}); setNotice('Thread deleted.','ok'); activeSupportThread=''; await load({silent:true});}
async function deleteSupportMessage(thread_id,message_id){if(!confirm('Delete this individual message from the visible thread?')) return; await api('/admin/api/support/message-delete',{method:'POST',body:JSON.stringify({thread_id,message_id})}); setNotice('Message deleted.','ok'); await load({silent:true});}
async function replySupportThread(id){
  const input=document.getElementById('reply-'+id);
  const message=(input&&input.value||'').trim();
  if(!message) return alert('Type a reply first.');
  const t=((snapshot&&snapshot.support_threads)||[]).find(x=>x.id===id)||{};
  const title='Support reply: '+(t.subject||t.ndors_trainer_id||'TrainerMate');
  const res=await api('/admin/api/support/reply',{method:'POST',body:JSON.stringify({thread_id:id,title,message})});
  if(input) input.value='';
  setNotice('Reply queued back to TrainerMate.','ok');
  pushAction('Support reply',`Reply queued for ${res.thread&&res.thread.ndors_trainer_id||''}.`,'ok');
  await load({silent:true});
}
async function markSupportThread(id,status){
  await api('/admin/api/support/status',{method:'POST',body:JSON.stringify({thread_id:id,status})});
  setNotice(status==='resolved'?'Support conversation resolved.':'Support status updated.','ok');
  await load({silent:true});
}
function renderAudit(){
  const items=(snapshot&&snapshot.audit)||[];
  audit.textContent=items.map(a=>`${a.at||a.created_at||''} ${a.action||''} ${JSON.stringify(a.detail||a.details||{})}`).join('\n')||'No audit yet.';
}

function renderMessages(){
  const box=document.getElementById('messageHistory'); if(!box) return;
  const rows=((snapshot&&snapshot.commands)||[]).filter(c=>c.command_type==='show_message');
  box.innerHTML=rows.slice(0,12).map(c=>`<div class="cmd-card"><div class="cmd-head"><strong>${esc(c.payload&&c.payload.title?c.payload.title:commandLabel(c.command_type))}</strong><span class="pill ${commandClass(c.status)}">${esc(c.status||'')}</span></div><div class="muted small">${esc(c.ndors_trainer_id||'')} · ${esc(ago(c.created_at))}</div><div class="small">${esc((c.payload&&c.payload.message)||c.message||'')}</div>${c.result&&Object.keys(c.result).length?`<div class="muted small">Result: ${esc(JSON.stringify(c.result).slice(0,220))}</div>`:''}</div>`).join('')||'<div class="empty">No messages sent yet.</div>';
}
function updateMessageTargetHelp(){
  const target=(document.getElementById('messageTarget')||{}).value||'selected';
  const help=document.getElementById('messageTargetHelp'); if(!help) return;
  const u=selectedUser();
  const text={selected:`Selected trainer only${u?': '+(u.ndors_trainer_id||''):' - choose a trainer first.'}`,all:'All users in the admin database. Use this carefully.',active:'Only accounts marked active.',paid:'Only paid/admin accounts.',needs_attention:'Only users currently flagged as needing attention or offline.'}[target]||'';
  help.textContent=text;
}
function useMessagePreset(kind){
  const presets={
    zoom:{title:'Reconnect Zoom',category:'warning',body:'Please reconnect your Zoom account in TrainerMate. Open TrainerMate, go to Zoom accounts, reconnect Zoom, then run sync again.'},
    open:{title:'Please open TrainerMate',category:'info',body:'Please open TrainerMate and leave it running for a few minutes so support can complete the requested checks.'},
    fobs:{title:'FOBS login needs checking',category:'warning',body:'TrainerMate may not be able to log into one of your FOBS providers. Please check your provider username/password in Manage providers.'},
    update:{title:'TrainerMate update available',category:'warning',body:'A TrainerMate update is available. Please install the latest version or contact support if you need help.'}
  };
  const p=presets[kind]||presets.open;
  messageTitle.value=p.title; messageBody.value=p.body; messageCategory.value=p.category;
}
function clearMessageComposer(){messageTitle.value='TrainerMate support message'; messageBody.value=''; messageCategory.value='info';}
function openMessageComposer(target='selected'){
  setView('messages');
  if(messageTarget){messageTarget.value=target||'selected'; updateMessageTargetHelp();}
  if(!messageBody.value.trim()) useMessagePreset(target==='selected'?'open':'update');
  setTimeout(()=>messageBody&&messageBody.focus(),50);
}
async function sendMessageCentreMessage(){
  const target=messageTarget.value||'selected';
  const body=(messageBody.value||'').trim();
  if(!body) return alert('Type a message first.');
  if(target==='selected' && !selected) return alert('Choose a trainer first, or change the target to a group.');
  const title=(messageTitle.value||'TrainerMate support message').trim();
  const category=messageCategory.value||'info';
  const confirmText=target==='selected'?'Send this message to the selected trainer?':`Send this message to ${target.replace('_',' ')} users?`;
  if(!confirm(confirmText)) return;
  const res=await api('/admin/api/messages/broadcast',{method:'POST',body:JSON.stringify({target,ndors_trainer_id:selected,title,message:body,category})});
  setNotice(`Message queued for ${res.queued||0} user(s). ${res.skipped||0} skipped.`, res.queued?'ok':'warn');
  pushAction('Message queued',`Target: ${target}. Queued: ${res.queued||0}. Skipped: ${res.skipped||0}.`,res.queued?'ok':'warn');
  await load();
}

function renderForms(){
  if(snapshot&&snapshot.settings){
    latestVersion.value=snapshot.settings.latest_version||''; minimumVersion.value=snapshot.settings.minimum_version||''; downloadUrl.value=snapshot.settings.download_url||''; installerSha256.value=snapshot.settings.installer_sha256||''; mandatoryAfter.value=snapshot.settings.mandatory_after||''; releaseNotes.value=snapshot.settings.release_notes||'';
  }
}
function updateSyncProviderOptions(){
  const sel=document.getElementById('syncProviderSelect'); if(!sel) return;
  const current=sel.value||'all';
  const providers=(selectedUser()&&selectedUser().providers)||[];
  sel.innerHTML='<option value="all">All providers</option>'+providers.map(p=>`<option value="${esc(p.id||p.name||'')}">${esc(p.name||p.id||'Provider')}</option>`).join('');
  if([...sel.options].some(o=>o.value===current)) sel.value=current;
}
function renderAll(){
  renderStats(); renderUsers(); renderSelected(); renderRibbon(); renderAttention(); renderRecentCommands(); renderLatestCourses(); renderCourses(); renderProviders(); renderMessages(); renderDevices(); renderCommandsTable(); renderSupport(); renderAudit(); renderForms(); updateMessageTargetHelp(); updateSyncProviderOptions(); renderActionLog();
}
async function load(opts={}){
  try{
    const previous=snapshot;
    const next=await api('/admin/api/snapshot');
    if(opts.silent) handleLiveChanges(previous,next);
    snapshot=next;
    renderAll();
    updateSupportBell();
  }
  catch(e){if(!opts.silent) setNotice('Could not load admin snapshot: '+e.message,'bad');}
}

function syncPayloadForPreset(preset){
  if(preset==='today') return {scope:'today',days:1,mode:'normal'};
  if(preset==='7days') return {scope:'days',days:7,mode:'normal'};
  if(preset==='licence') return {scope:'licence_window',mode:'normal'};
  return {scope:'days',days:7,mode:'normal'};
}
async function sendSyncCourses(preset){
  const payload=syncPayloadForPreset(preset);
  const labels={today:'Sync today', '7days':'Sync next 7 days', licence:'Sync full allowed window'};
  await sendCommand('sync_courses',{...payload,admin_label:labels[preset]||'Sync courses'});
}
async function sendAdvancedSync(){
  const provider=(document.getElementById('syncProviderSelect')||{}).value||'all';
  const daysRaw=(document.getElementById('syncWindowSelect')||{}).value||'7';
  const mode=(document.getElementById('syncModeSelect')||{}).value||'normal';
  const days=parseInt(daysRaw,10)||0;
  const payload={scope: days<=0?'licence_window':'days', days: days, provider_id:provider, mode:mode, dry_run:mode==='check_only', admin_label:'Advanced sync'};
  if(mode==='check_only'){
    const ok=confirm('Check only will refresh course visibility without updating FOBS or Zoom. Continue?');
    if(!ok) return;
  }
  await sendCommand('sync_courses',payload);
}
function coursePayload(course){
  return {scope:'course',mode:'normal',course_key:course.course_key||course.id||'',course_id:course.course_key||course.id||'',provider_id:course.provider_id||course.provider||course.provider_name||'',provider:course.provider_name||course.provider||'',date_time:course.source_date_time_text||course.date_time||course.course_start||'',title:course.title||'',admin_label:'Sync single course'};
}
async function syncSingleCourse(encodedIndex){
  if(!selected) return alert('Choose a trainer first.');
  const u=selectedUser();
  const courses=(u&&u.courses)||[];
  const course=courses[Number(encodedIndex)];
  if(!course) return alert('Course not found. Refresh the course list and try again.');
  if(!confirm('Queue sync for this single course? This may update Zoom/FOBS for that course.')) return;
  await sendCommand('sync_courses',coursePayload(course));
}
async function sendCommand(type,payload={}){
  if(!selected) return alert('Choose a trainer first.');
  const now=Date.now(); if(now-lastActionAt<700) return; lastActionAt=now;
  const u=selectedUser();
  const label=commandLabel(type);
  const info=commandHelp(type);
  const offline=isLikelyOffline(u);
  if(offline && (COMMAND_INFO[type]||{}).needsApp){
    const ok=confirm(`${label} will be queued, not run instantly.\n\n${trainerOnlineText(u)}\n\n${info}\n\nQueue anyway?`);
    if(!ok){pushAction(label,'Not queued - admin cancelled after offline warning.','warn'); return;}
  }
  try{
    const waiting=(COMMAND_INFO[type]||{}).needsApp ? trainerOnlineText(u) : 'This server-side change should apply immediately.';
    setNotice(`${label} requested. ${waiting}`,'warn');
    pushAction(label,`Requested for ${selected}. ${waiting}`,'warn');
    const res=await api('/admin/api/commands',{method:'POST',body:JSON.stringify({ndors_trainer_id:selected,command_type:type,payload})});
    if(res.deduplicated){
      setNotice(`${label} is already pending; no duplicate was created.`,'warn');
      pushAction(label,'Already pending - duplicate prevented.','warn');
    } else {
      setNotice(`${label} queued successfully. Watch Recent commands for queued → sent → completed.`,'ok');
      pushAction(label,'Queued successfully. Waiting for app pickup/completion.','ok');
    }
    if(type.startsWith('provider_') && typeof providerPassword!=='undefined'){providerPassword.value='';}
    await load();
  }catch(e){setNotice(`${label} failed to queue: ${e.message}`,'bad'); pushAction(label,'Failed to queue: '+e.message,'bad');}
}
function providerPayloadFromForm(forId=''){
  const pid=(forId||providerId.value||'').trim();
  return {provider_id:pid,provider_name:(providerName.value||pid).trim(),login_url:providerLoginUrl.value.trim(),credentials:{username:providerUsername.value.trim(),password:providerPassword.value},provider_manages_zoom:providerManagesZoom.checked,active:true};
}
async function sendProviderCommand(type, forId=''){
  if(!selected) return alert('Choose a trainer first.');
  const payload=providerPayloadFromForm(forId);
  if(!payload.provider_id) return alert('Enter or select a provider id first.');
  if(type==='provider_remove'&&!confirm('Remove this provider from the trainer app? Historical course records are kept.')) return;
  await sendCommand(type,payload);
}
async function cancelPendingCommands(){
  if(!selected) return alert('Choose a trainer first.');
  if(!confirm('Cancel all queued/sent/running commands for this trainer? Commands already actively executing may still finish.')) return;
  const res=await api('/admin/api/commands/cancel',{method:'POST',body:JSON.stringify({ndors_trainer_id:selected,message:'Cancelled from admin screen.'})});
  setNotice(`Cancelled ${res.cancelled||0} pending command(s).`,'ok'); pushAction('Cancel pending',`Cancelled ${res.cancelled||0} command(s) for ${selected}.`,'ok'); await load();
}
async function cancelCommandById(id){
  if(!id) return;
  if(!confirm('Cancel this command? If the trainer app has already started it, it may still finish.')) return;
  const res=await api('/admin/api/commands/'+encodeURIComponent(id)+'/cancel',{method:'POST',body:JSON.stringify({message:'Cancelled individually from admin screen.'})});
  setNotice(`Cancelled ${res.cancelled||0} command(s).`,'ok'); pushAction('Cancel command',`Cancelled command ${id}.`,'ok'); await load();
}
async function setAccount(plan,status){
  if(!selected) return alert('Choose a trainer first.');
  if(status==='suspended'&&!confirm('Suspend this trainer account? They will not be able to sync.')) return;
  await api('/admin/api/accounts/'+encodeURIComponent(selected),{method:'POST',body:JSON.stringify({plan,status})});
  setNotice('Account updated.','ok'); pushAction('Account updated',`Plan/status change saved for ${selected}.`,'ok'); await load();
}
async function resetTrial(){if(!selected) return alert('Choose a trainer first.'); await api('/admin/api/accounts/'+encodeURIComponent(selected)+'/reset-trial',{method:'POST',body:JSON.stringify({free_syncs_used:0})}); setNotice('Trial reset.','ok'); pushAction('Trial reset',`Free trial reset for ${selected}.`,'ok'); await load();}
async function forceResetPassword(){
  const ndors=selectedNdors();
  if(!ndors) return alert('Choose a trainer with a valid NDORS trainer ID first.');
  const u=selectedUser();
  const label=`${ndors}${u&&u.email?' / '+u.email:''}`;
  if(!confirm(`Force reset TrainerMate password for ${label}?\n\nOnly do this after verifying the trainer. The new password is not logged.`)) return;
  const typed=prompt(`Type the NDORS trainer ID exactly to confirm password reset:\n\n${ndors}`);
  if(typed!==ndors){setNotice('Password reset cancelled - NDORS ID did not match.','warn'); pushAction('Force password reset','Cancelled - confirmation did not match.','warn'); return;}
  const res=await api('/admin/api/accounts/'+encodeURIComponent(ndors)+'/force-password-reset',{method:'POST',body:JSON.stringify({confirm_ndors_trainer_id:typed,confirm_reset:'RESET PASSWORD'})});
  setNotice(`Temporary password emailed to ${res.delivered_to||'the registered user'}.`,'ok');
  pushAction('Force password reset',`Temporary password emailed for ${ndors}. User must change it on next login.`,'warn');
  alert(`Temporary password sent to ${res.delivered_to||'the registered user'}.\n\nThe password was not shown to admin and was not logged.`);
  await load();
}
async function deleteUser(){
  const ndors=selectedNdors();
  if(!ndors) return deleteInvalidUser();
  const u=selectedUser();
  const label=`${ndors}${u&&u.email?' / '+u.email:''}`;
  if(!confirm(`Delete trainer account ${label}?\n\nThis removes the admin/licensing account, login emails, devices, usage, queued commands, course snapshots and support bundles for this NDORS ID. Audit history is kept.`)) return;
  const typed=prompt(`Final confirmation: type the NDORS trainer ID exactly to delete this user:\n\n${ndors}`);
  if(typed!==ndors){setNotice('Delete cancelled - NDORS ID did not match.','warn'); pushAction('Delete user','Cancelled - confirmation did not match.','warn'); return;}
  await api('/admin/api/accounts/'+encodeURIComponent(ndors)+'/delete',{method:'POST',body:JSON.stringify({confirm_ndors_trainer_id:typed,confirm_delete:'DELETE USER'})});
  setNotice('User deleted.','ok'); pushAction('User deleted',`Deleted trainer account ${ndors}.`,'warn'); selected=''; localStorage.removeItem('tm_admin_selected'); await load();
}
async function deleteInvalidUser(){
  const identifier=selectedIdentifier();
  if(!identifier) return alert('Choose a trainer first.');
  if(looksLikeNdors(identifier)) return alert('Use the normal delete button for this NDORS trainer ID.');
  const u=selectedUser();
  const label=`${identifier}${u&&u.email&&u.email!==identifier?' / '+u.email:''}`;
  if(!confirm(`Delete invalid trainer account ${label}?\n\nThis is a cleanup tool for bad legacy/test rows where the NDORS trainer ID was saved as an email or another invalid value. Audit history is kept.`)) return;
  const typed=prompt(`Final confirmation: type this invalid account identifier exactly:\n\n${identifier}`);
  if(typed!==identifier){setNotice('Delete cancelled - confirmation did not match.','warn'); pushAction('Delete invalid user','Cancelled - confirmation did not match.','warn'); return;}
  await api('/admin/api/invalid-accounts/'+encodeURIComponent(identifier)+'/delete',{method:'POST',body:JSON.stringify({confirm_account_identifier:typed,confirm_delete:'DELETE INVALID USER'})});
  setNotice('Invalid account deleted.','ok'); pushAction('Invalid account deleted',`Deleted invalid account ${identifier}.`,'warn'); selected=''; localStorage.removeItem('tm_admin_selected'); await load();
}
async function sendTrainerMessage(){openMessageComposer('selected');}
async function sendUpdatePrompt(){
  if(!selected) return alert('Choose a trainer first.');
  await sendCommand('show_message',{title:'TrainerMate update available',message:'TrainerMate update is available. Please install the latest version or contact support.',category:'warning'});
}
async function requestZoomReconnect(){
  if(!selected) return alert('Choose a trainer first.');
  await sendCommand('show_message',{title:'Reconnect Zoom',message:'Your Zoom account needs reconnecting in TrainerMate. Open TrainerMate, go to Zoom accounts, reconnect Zoom, then try syncing again.',category:'warning'});
}
async function createLicence(){
  const res=await api('/admin/api/licences',{method:'POST',body:JSON.stringify({plan_type:licencePlan.value,issued_to_ndors_trainer_id:licenceFor.value||null,expiry_date:licenceExpiry.value||null})});
  licenceOutput.textContent=JSON.stringify(res.licence||res,null,2); setNotice('Licence created.','ok'); pushAction('Licence created','New licence key created.','ok'); await load();
}
async function saveSettings(){
  await api('/admin/api/settings',{method:'POST',body:JSON.stringify({latest_version:latestVersion.value,minimum_version:minimumVersion.value,download_url:downloadUrl.value,installer_sha256:installerSha256.value,mandatory_after:mandatoryAfter.value,release_notes:releaseNotes.value})});
  setNotice('Release settings saved.','ok'); pushAction('Release settings saved','Latest/minimum version settings were saved.','ok'); await load();
}


/* ===== Simple Admin Mode: reduce clutter and make the admin workflow obvious ===== */
function installSimpleAdminMode(){
  document.body.classList.add('simple-mode');
  const labels={overview:'Home',users:'Trainers',support:'Helpdesk',courses:'Courses',providers:'FOBS',releases:'Settings'};
  const hide=new Set(['messages','licences','devices','commands','audit']);
  document.querySelectorAll('#nav button').forEach(b=>{
    const v=b.dataset.view;
    if(labels[v]) b.textContent=labels[v];
    if(hide.has(v)) b.classList.add('simple-hidden');
  });
  if(hide.has(currentView)){currentView='overview'; localStorage.setItem('tm_admin_view','overview');}
  const top=document.querySelector('.top .muted');
  if(top) top.textContent='Pick a trainer, see what needs attention, then use one clear action at a time.';
  const h1=document.querySelector('.top h1'); if(h1) h1.textContent='TrainerMate Admin';
  const brandSub=document.querySelector('.brand-sub'); if(brandSub) brandSub.textContent='Simple support console';
  const side=document.querySelector('.side-note'); if(side) side.textContent='Simple mode: Home, Trainers, Helpdesk, Courses, FOBS and Settings.';
  const cards=document.querySelector('.cards');
  if(cards && !document.getElementById('simpleStart')){
    const help=document.createElement('section');
    help.id='simpleStart'; help.className='simple-start';
    help.innerHTML=`<div class="step"><div class="num">1</div><strong>Start at Home</strong><span>Only urgent items, open help requests and the selected trainer matter here.</span></div><div class="step"><div class="num">2</div><strong>Choose a trainer</strong><span>Use Trainers, then run Health, Refresh Courses, Sync, or Message.</span></div><div class="step"><div class="num">3</div><strong>Use Helpdesk</strong><span>Trainer messages are grouped into threads with unread badges and gentle live alerts.</span></div>`;
    cards.after(help);
  }
}
function applySimpleAdminCopy(){
  installSimpleAdminMode();
  const statLabels=[['statUsers','Trainers'],['statPaid','Paid'],['statOnline','Seen'],['statAttention','Needs help'],['statSupport','Open help']];
  statLabels.forEach(([id,label])=>{const el=document.getElementById(id); const card=el&&el.closest('.card'); const span=card&&card.querySelector('span'); if(span) span.textContent=label;});
  const rb=document.getElementById('ribbonTrainer'); if(rb && rb.textContent==='None selected') rb.textContent='No trainer selected';
  const buttons=document.querySelectorAll('#selectedRibbon button');
  if(buttons[0]) buttons[0].textContent='Pick trainer'; if(buttons[1]) buttons[1].textContent='Check'; if(buttons[2]) buttons[2].textContent='Courses'; if(buttons[3]) buttons[3].textContent='Message';
  const att=document.querySelector('#view-overview .section-title h2'); if(att) att.textContent='What needs attention?';
  const supportTitle=document.querySelector('#view-support h2'); if(supportTitle) supportTitle.textContent='Helpdesk';
  const supportSub=document.querySelector('#view-support .muted'); if(supportSub) supportSub.textContent='Threaded trainer messages. Keep it like a simple inbox.';
  const supportStack=document.querySelector('#view-support .support-console .stack');
  if(supportStack && !document.getElementById('supportSelectedHint')){
    const hint=document.createElement('div'); hint.id='supportSelectedHint'; hint.className='support-selected-hint'; hint.textContent='Tip: choose a trainer first if you only want to see their messages. Otherwise this shows all open helpdesk threads.'; supportStack.prepend(hint);
  }
  document.querySelectorAll('details.advanced-panel summary').forEach(s=>{ if(!s.textContent.includes('More tools')) s.textContent='More tools - only open this if the simple buttons did not solve it'; });
}
const tmOriginalRenderAll=renderAll;
renderAll=function(){tmOriginalRenderAll(); applySimpleAdminCopy();};
installSimpleAdminMode();

setView(currentView); load().then(()=>startLiveUpdates());
</script>
</body>
</html>
    """


@app.get("/admin", response_class=HTMLResponse)
def admin_home(request: Request):
    # Auto-login: if admin_token is in the URL, set the normal admin cookie and
    # redirect back to /admin so the page's API calls stay authorised.
    supplied_token = (request.query_params.get("admin_token") or "").strip()
    if supplied_token and admin_token_configured() and hmac.compare_digest(supplied_token, ADMIN_TOKEN):
        response = RedirectResponse("/admin", status_code=303)
        response.set_cookie("tm_admin", admin_cookie_value(), httponly=True, samesite="lax", secure=ADMIN_COOKIE_SECURE)
        admin_audit("admin_auto_login", {})
        return response

    if not admin_authorized(request):
        configured = admin_token_configured()
        return HTMLResponse(f"""
<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TrainerMate Admin</title>
<style>
body{{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:#0f172a;color:#e5e7eb;display:grid;place-items:center;min-height:100vh}}
.card{{width:min(440px,calc(100vw - 32px));background:#111827;border:1px solid #273449;border-radius:10px;padding:22px;box-shadow:0 20px 60px rgba(0,0,0,.32)}}
h1{{margin:0 0 8px;font-size:24px}}p{{color:#aeb9ca}}input,button{{width:100%;box-sizing:border-box;border-radius:8px;padding:12px;font:inherit}}input{{background:#0b1220;border:1px solid #334155;color:#fff}}button{{margin-top:12px;border:0;background:#2563eb;color:#fff;font-weight:800;cursor:pointer}}.warn{{color:#fbbf24}}
</style></head><body><form class="card" method="post" action="/admin/login">
<h1>TrainerMate Admin</h1><p>Remote support and licence control.</p>
{"<p class='warn'>Set TRAINERMATE_ADMIN_TOKEN before using the admin portal.</p>" if not configured else ""}
<input type="password" name="token" placeholder="Admin token" autofocus><button type="submit">Open admin</button>
</form></body></html>
        """)

    return HTMLResponse(admin_portal_html())


@app.post("/admin/login")
async def admin_login(request: Request):
    body = (await request.body()).decode("utf-8", errors="replace")
    token = (parse_qs(body).get("token") or [""])[0].strip()
    check_auth_rate_limit(request, "admin_login", "admin", AUTH_ADMIN_RATE_LIMIT_MAX_ATTEMPTS)
    if not admin_token_configured() or not hmac.compare_digest(token, ADMIN_TOKEN):
        return HTMLResponse("Admin login failed.", status_code=401)
    clear_auth_rate_limit(request, "admin_login", "admin")
    response = RedirectResponse("/admin", status_code=303)
    response.set_cookie("tm_admin", admin_cookie_value(), httponly=True, samesite="lax", secure=ADMIN_COOKIE_SECURE)
    admin_audit("admin_login", {})
    return response


@app.get("/admin/api/snapshot")
def admin_api_snapshot(request: Request):
    require_admin(request)
    return admin_snapshot()


@app.post("/admin/api/accounts/{ndors_trainer_id}")
def admin_update_account(ndors_trainer_id: str, payload: AdminAccountUpdateRequest, request: Request):
    require_admin(request)
    if not valid_ndors_id(ndors_trainer_id):
        raise HTTPException(status_code=400, detail="Admin account updates must use an NDORS trainer ID, not an email address")
    account = get_account_by_ndors(ndors_trainer_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    updates = {}
    if payload.plan in {"free", "paid", "admin"}:
        updates["plan"] = payload.plan
    if payload.status in {"active", "inactive", "suspended"}:
        updates["status"] = payload.status
    if not updates:
        raise HTTPException(status_code=400, detail="No valid account changes supplied")
    # Update every duplicate row for this NDORS ID so admin and dashboard cannot disagree.
    updated_rows = update_accounts_by_ndors(ndors_trainer_id, updates, "admin update account")
    updated_account = best_account_row(updated_rows) or dict(account, **updates)
    try:
        cache_access_response(ndors_trainer_id, access_response_for_account(updated_account, None))
    except Exception:
        pass
    admin_audit("account_update", {"ndors_trainer_id": ndors_trainer_id, **updates})

    # Ask the desktop app to refresh its cached licence/features. If the trainer
    # is offline this simply waits in the command queue until the app next opens.
    try:
        existing = db_find_active_admin_command(ndors_trainer_id, "", "refresh_licence")
        if not existing:
            db_create_admin_command(ndors_trainer_id, "", "refresh_licence", {"reason": "account_update", "updates": updates})
    except Exception:
        try:
            commands = load_admin_commands()
            if not json_find_active_admin_command(ndors_trainer_id, "", "refresh_licence"):
                commands.append({
                    "id": secrets.token_urlsafe(18),
                    "created_at": utc_now(),
                    "updated_at": utc_now(),
                    "ndors_trainer_id": ndors_trainer_id,
                    "device_id": "",
                    "command_type": "refresh_licence",
                    "payload": {"reason": "account_update", "updates": updates},
                    "status": "queued",
                    "message": "",
                    "result": {},
                })
                save_admin_commands(commands)
        except Exception:
            pass
    return {"ok": True}


@app.post("/admin/api/accounts/{ndors_trainer_id}/reset-trial")
def admin_reset_trial(ndors_trainer_id: str, payload: AdminResetTrialRequest, request: Request):
    require_admin(request)
    account = get_account_by_ndors(ndors_trainer_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    used = max(0, int(payload.free_syncs_used or 0))
    execute_supabase(
        supabase.table("usage").upsert({
            "account_id": account["id"],
            "free_syncs_used": used,
            "last_sync_at": None,
            "updated_at": utc_now(),
        }, on_conflict="account_id"),
        "admin reset trial",
    )
    execute_supabase(
        supabase.table("accounts").update({"plan": "free", "status": "active", "updated_at": utc_now()}).eq("ndors_trainer_id", ndors_trainer_id),
        "admin reactivate free account",
    )
    admin_audit("trial_reset", {"ndors_trainer_id": ndors_trainer_id, "free_syncs_used": used})
    try:
        if not db_find_active_admin_command(ndors_trainer_id, "", "refresh_licence"):
            db_create_admin_command(ndors_trainer_id, "", "refresh_licence", {"reason": "trial_reset", "free_syncs_used": used})
    except Exception:
        pass
    return {"ok": True}


@app.post("/admin/api/accounts/{ndors_trainer_id}/force-password-reset")
def admin_force_password_reset(ndors_trainer_id: str, payload: AdminPasswordResetRequest, request: Request):
    require_admin(request)
    ndors = (ndors_trainer_id or "").strip()
    if not valid_ndors_id(ndors):
        raise HTTPException(status_code=400, detail="Admin password resets must use an NDORS trainer ID")
    if (payload.confirm_ndors_trainer_id or "").strip() != ndors or (payload.confirm_reset or "").strip() != "RESET PASSWORD":
        raise HTTPException(status_code=400, detail="Password reset confirmation did not match")
    accounts = get_accounts_by_ndors(ndors)
    if not accounts:
        raise HTTPException(status_code=404, detail="Account not found")
    to_email = primary_account_email(accounts)
    if not to_email:
        raise HTTPException(status_code=400, detail="This account has no registered email address for password delivery.")
    previous_account = best_account_row(accounts) or {}
    temporary_password = generate_temporary_password()
    updates = {
        "password_hash": password_hash(temporary_password),
        "password_set_at": utc_now(),
        "password_must_change": True,
        "updated_at": utc_now(),
    }
    updated_rows = update_accounts_by_ndors(ndors, updates, "admin force password reset")
    account = best_account_row(updated_rows) or best_account_row(accounts)
    try:
        if account:
            cache_access_response(ndors, access_response_for_account(account, None))
    except Exception:
        pass
    try:
        send_temporary_password_email(to_email, ndors, temporary_password)
    except Exception as exc:
        rollback = {
            "password_hash": previous_account.get("password_hash"),
            "password_set_at": previous_account.get("password_set_at"),
            "password_must_change": bool(previous_account.get("password_must_change")),
            "updated_at": utc_now(),
        }
        try:
            execute_supabase(
                supabase.table("accounts").update(rollback).eq("ndors_trainer_id", ndors),
                "rollback failed admin password email",
            )
        except Exception:
            pass
        if isinstance(exc, HTTPException):
            raise exc
        raise HTTPException(status_code=503, detail="Temporary password email could not be sent. The password reset was not completed.")
    admin_audit("account_password_force_reset", {"ndors_trainer_id": ndors, "severity": "warning"})
    return {"ok": True, "delivered_to": mask_email_address(to_email), "must_change": True}


def delete_account_rows(identifier: str, *, valid_ndors: bool):
    account_ids = []
    deleted = {"accounts": 0}
    accounts = []
    if valid_ndors:
        accounts = get_accounts_by_ndors(identifier)
    else:
        result = execute_supabase(
            supabase.table("accounts").select("*").eq("ndors_trainer_id", identifier),
            "read invalid account for delete",
        )
        accounts = result.data or []
    if not accounts:
        raise HTTPException(status_code=404, detail="Account not found")
    account_ids = [row.get("id") for row in accounts if isinstance(row, dict) and row.get("id")]
    deleted["accounts"] = len(account_ids)

    def run_delete(table_name, query):
        try:
            result = execute_supabase(query, f"admin delete {table_name}")
            deleted[table_name] = len(result.data or []) if hasattr(result, "data") else 0
        except Exception as exc:
            deleted[table_name] = f"skipped: {exc}"

    if account_ids:
        run_delete("usage", supabase.table("usage").delete().in_("account_id", account_ids))
        run_delete("devices", supabase.table("devices").delete().in_("account_id", account_ids))
        run_delete("account_logins", supabase.table("account_logins").delete().in_("account_id", account_ids))
        try:
            execute_supabase(
                supabase.table("licences").update({
                    "account_id": None,
                    "issued_to_ndors_trainer_id": None,
                    "status": "unused",
                    "updated_at": utc_now(),
                }).in_("account_id", account_ids),
                "admin detach licences",
            )
            deleted["licences"] = "detached"
        except Exception as exc:
            deleted["licences"] = f"skipped: {exc}"
        try:
            execute_supabase(
                supabase.table("support_bundles").delete().in_("account_id", account_ids),
                "admin delete support bundles",
            )
            deleted["support_bundles"] = "deleted"
        except Exception as exc:
            deleted["support_bundles"] = f"skipped: {exc}"

    for table_name, column in (
        ("admin_commands", "ndors_trainer_id"),
        ("device_heartbeats", "ndors_trainer_id"),
        ("synced_courses", "ndors_trainer_id"),
    ):
        run_delete(table_name, supabase.table(table_name).delete().eq(column, identifier))

    run_delete("accounts", supabase.table("accounts").delete().eq("ndors_trainer_id", identifier))
    cache = load_access_cache()
    cache.pop(cache_key(identifier), None)
    save_access_cache(cache)
    return deleted


@app.post("/admin/api/accounts/{ndors_trainer_id}/delete")
def admin_delete_account(ndors_trainer_id: str, payload: AdminAccountDeleteRequest, request: Request):
    require_admin(request)
    ndors = (ndors_trainer_id or "").strip()
    if not valid_ndors_id(ndors):
        raise HTTPException(status_code=400, detail="Admin account deletes must use an NDORS trainer ID")
    if (payload.confirm_ndors_trainer_id or "").strip() != ndors or (payload.confirm_delete or "").strip() != "DELETE USER":
        raise HTTPException(status_code=400, detail="Delete confirmation did not match")
    deleted = delete_account_rows(ndors, valid_ndors=True)
    admin_audit("account_delete", {"ndors_trainer_id": ndors, "account_count": deleted.get("accounts"), "severity": "danger", "deleted": deleted})
    return {"ok": True, "deleted": deleted}


@app.post("/admin/api/invalid-accounts/{account_identifier}/delete")
def admin_delete_invalid_account(account_identifier: str, payload: AdminInvalidAccountDeleteRequest, request: Request):
    require_admin(request)
    identifier = (account_identifier or "").strip()
    if valid_ndors_id(identifier):
        raise HTTPException(status_code=400, detail="Use the normal delete button for valid NDORS trainer IDs")
    if (payload.confirm_account_identifier or "").strip() != identifier or (payload.confirm_delete or "").strip() != "DELETE INVALID USER":
        raise HTTPException(status_code=400, detail="Delete confirmation did not match")
    deleted = delete_account_rows(identifier, valid_ndors=False)
    admin_audit("invalid_account_delete", {"account_identifier": identifier, "account_count": deleted.get("accounts"), "severity": "danger", "deleted": deleted})
    return {"ok": True, "deleted": deleted}


@app.post("/admin/api/accounts/{ndors_trainer_id}/prompt-update")
def admin_prompt_update(ndors_trainer_id: str, payload: AdminUpdatePromptRequest, request: Request):
    require_admin(request)
    settings = load_admin_settings()
    command_payload = {
        "message": payload.message or f"TrainerMate {settings.get('latest_version', '')} is available. Please update when convenient.",
        "update": update_info_for_version(None),
    }
    try:
        command = db_create_admin_command(ndors_trainer_id, "", "show_message", command_payload)
        public = public_db_command(command)
    except Exception:
        commands = load_admin_commands()
        command = {
            "id": secrets.token_urlsafe(18),
            "created_at": utc_now(),
            "updated_at": utc_now(),
            "ndors_trainer_id": ndors_trainer_id,
            "device_id": "",
            "command_type": "show_message",
            "payload": command_payload,
            "status": "queued",
            "message": "",
        }
        commands.append(command)
        save_admin_commands(commands)
        public = public_command(command)
    admin_audit("update_prompt", public)
    return {"ok": True, "command": public}


@app.post("/admin/api/licences")
def admin_create_licence(payload: AdminLicenceCreateRequest, request: Request):
    require_admin(request)
    licence_key = "TM-" + secrets.token_urlsafe(18).replace("_", "").replace("-", "").upper()[:20]
    row = {
        "licence_key": licence_key,
        "plan_type": payload.plan_type if payload.plan_type in {"paid", "admin"} else "paid",
        "status": "unused",
        "issued_to_ndors_trainer_id": payload.issued_to_ndors_trainer_id,
        "expiry_date": payload.expiry_date,
    }
    result = execute_supabase(supabase.table("licences").insert(row), "admin create licence")
    admin_audit("licence_create", {"licence_key": licence_key, "issued_to": payload.issued_to_ndors_trainer_id})
    return {"ok": True, "licence": (result.data or [row])[0]}


@app.post("/admin/api/settings")
def admin_update_settings(payload: AdminSettingsUpdateRequest, request: Request):
    require_admin(request)
    settings = save_admin_settings({
        "latest_version": payload.latest_version,
        "minimum_version": payload.minimum_version,
        "release_notes": payload.release_notes,
        "download_url": payload.download_url,
        "installer_sha256": payload.installer_sha256,
        "mandatory_after": payload.mandatory_after,
    })
    admin_audit("settings_update", settings)
    return {"ok": True, "settings": settings}



@app.post("/admin/api/messages/broadcast")
def admin_broadcast_message(payload: AdminBroadcastMessageRequest, request: Request):
    require_admin(request)
    title = (payload.title or "TrainerMate support message").strip()[:140]
    message = (payload.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message is required")
    if len(message) > 900:
        message = message[:897].rstrip() + "..."
    category = (payload.category or "info").strip().lower()
    if category not in {"info", "warning", "success"}:
        category = "info"

    snapshot = admin_snapshot()
    users = snapshot.get("users") or []
    target = (payload.target or "selected").strip().lower()
    selected_ndors = (payload.ndors_trainer_id or "").strip()

    def user_matches(user):
        if target == "selected":
            return selected_ndors and (user.get("ndors_trainer_id") or "").strip().lower() == selected_ndors.lower()
        if target == "all":
            return True
        if target == "active":
            return user.get("status") == "active"
        if target == "paid":
            return user.get("plan") in {"paid", "admin"}
        if target == "needs_attention":
            latest = user.get("latest_device") or {}
            return bool(user.get("update_needed") or latest.get("needs_attention") or (latest.get("status") or {}).get("needs_attention") or not latest.get("last_seen_at"))
        return False

    targets = [u for u in users if user_matches(u)]
    queued = []
    skipped = 0
    command_payload = {"title": title, "message": message, "category": category, "source": "admin_message_centre"}
    for user in targets:
        ndors = (user.get("ndors_trainer_id") or "").strip()
        if not ndors:
            skipped += 1
            continue
        try:
            existing = db_find_active_admin_command(ndors, "", "show_message")
            if existing:
                skipped += 1
                continue
            command = db_create_admin_command(ndors, "", "show_message", command_payload)
            queued.append(public_db_command(command))
        except Exception:
            commands = load_admin_commands()
            existing = json_find_active_admin_command(ndors, "", "show_message")
            if existing:
                skipped += 1
                continue
            command = {
                "id": secrets.token_urlsafe(18),
                "created_at": utc_now(),
                "updated_at": utc_now(),
                "ndors_trainer_id": ndors,
                "device_id": "",
                "command_type": "show_message",
                "payload": command_payload,
                "status": "queued",
                "message": "",
            }
            commands.append(command)
            save_admin_commands(commands)
            queued.append(public_command(command))
    admin_audit("message_broadcast", {"target": target, "selected": selected_ndors, "queued": len(queued), "skipped": skipped, "title": title})
    return {"ok": True, "queued": len(queued), "skipped": skipped, "commands": queued[:20]}


@app.post("/admin/api/commands")
def admin_create_command(payload: AdminCommandCreateRequest, request: Request):
    require_admin(request)
    if payload.command_type not in ADMIN_COMMAND_TYPES:
        raise HTTPException(status_code=400, detail="Command is not allowed")

    deduped = False
    try:
        existing = db_find_active_admin_command(
            payload.ndors_trainer_id,
            payload.device_id or "",
            payload.command_type,
        )
        if existing:
            public = public_db_command(existing)
            deduped = True
        else:
            command = db_create_admin_command(
                payload.ndors_trainer_id,
                payload.device_id or "",
                payload.command_type,
                payload.payload or {},
            )
            public = public_db_command(command)
    except Exception:
        # JSON fallback for dev machines before the admin_commands table exists.
        existing = json_find_active_admin_command(
            payload.ndors_trainer_id,
            payload.device_id or "",
            payload.command_type,
        )
        if existing:
            public = public_command(existing)
            deduped = True
        else:
            commands = load_admin_commands()
            command = {
                "id": secrets.token_urlsafe(18),
                "created_at": utc_now(),
                "updated_at": utc_now(),
                "ndors_trainer_id": payload.ndors_trainer_id,
                "device_id": payload.device_id or "",
                "command_type": payload.command_type,
                "payload": payload.payload or {},
                "status": "queued",
                "message": "",
            }
            commands.append(command)
            save_admin_commands(commands)
            public = public_command(command)

    admin_audit("command_duplicate" if deduped else "command_create", public)
    return {"ok": True, "deduplicated": deduped, "command": public}


@app.post("/admin/api/commands/cancel")
def admin_cancel_commands(payload: AdminCommandCancelRequest, request: Request):
    require_admin(request)
    message = payload.message or "Cancelled from admin."
    try:
        count = db_cancel_active_admin_commands(
            payload.ndors_trainer_id,
            payload.device_id or "",
            payload.command_type or None,
            message,
        )
    except Exception:
        count = json_cancel_active_admin_commands(
            payload.ndors_trainer_id,
            payload.device_id or "",
            payload.command_type or None,
            message,
        )
    detail = {
        "ndors_trainer_id": payload.ndors_trainer_id,
        "device_id": payload.device_id or "",
        "command_type": payload.command_type or "",
        "cancelled": count,
    }
    admin_audit("command_cancel", detail)
    return {"ok": True, "cancelled": count}




@app.post("/admin/api/commands/{command_id}/cancel")
def admin_cancel_command_by_id(command_id: str, payload: dict, request: Request):
    require_admin(request)
    message = (payload or {}).get("message") or "Cancelled from admin."
    try:
        count = db_cancel_admin_command_by_id(command_id, message)
    except Exception:
        count = json_cancel_admin_command_by_id(command_id, message)
    admin_audit("command_cancel_one", {"command_id": command_id, "cancelled": count})
    return {"ok": True, "cancelled": count}


@app.post("/client/support-message")
def client_support_message(payload: ClientSupportMessageRequest):
    subject = clean_support_text(payload.subject or payload.ndors_trainer_id, 160) or payload.ndors_trainer_id
    message = clean_support_text(payload.message, 4000)
    threads = load_support_threads()
    thread = find_support_thread(threads, payload.ndors_trainer_id, subject, payload.thread_id or "")
    now = utc_now()
    created = False
    if not thread:
        thread = {
            "id": secrets.token_urlsafe(16),
            "created_at": now,
            "messages": [],
            "notes": [],
            "status": "open",
            "category": "General",
            "priority": "normal",
            "unread_admin_count": 0,
            "archived": False,
            "deleted": False,
        }
        threads.append(thread)
        created = True
    thread.update({
        "updated_at": now,
        "last_user_message_at": now,
        "status": "open",
        "archived": False,
        "deleted": False,
        "subject": subject,
        "ndors_trainer_id": payload.ndors_trainer_id,
        "email": payload.email or thread.get("email") or "",
        "device_id": payload.device_id or thread.get("device_id") or "",
        "device_name": payload.device_name or thread.get("device_name") or "",
        "app_version": payload.app_version or thread.get("app_version") or "",
        "build": payload.build or thread.get("build") or "",
        "summary": clean_support_text(payload.summary, 2500),
        "status_payload": redact_sensitive(payload.status or {}),
    })
    items = thread.setdefault("messages", [])
    if not isinstance(items, list):
        items = []
        thread["messages"] = items
    items.append({
        "id": secrets.token_urlsafe(10),
        "at": now,
        "from": "trainer",
        "message": message,
    })
    thread["unread_admin_count"] = int(thread.get("unread_admin_count") or 0) + 1
    save_support_threads(threads)
    admin_audit("support_thread_create" if created else "support_thread_update", {
        "target_type": "support_thread",
        "target_id": thread.get("id"),
        "ndors_trainer_id": payload.ndors_trainer_id,
        "subject": subject,
    })
    return {"ok": True, "thread": public_support_thread(thread)}


@app.post("/admin/api/support/reply")
def admin_support_reply(payload: AdminSupportReplyRequest, request: Request):
    require_admin(request)
    threads = load_support_threads()
    thread = next((item for item in threads if isinstance(item, dict) and item.get("id") == payload.thread_id), None)
    if not thread:
        raise HTTPException(status_code=404, detail="Support thread not found")
    ndors = (thread.get("ndors_trainer_id") or "").strip()
    if not ndors:
        raise HTTPException(status_code=400, detail="Support thread has no trainer ID")
    now = utc_now()
    title = clean_support_text(payload.title or f"Support reply: {thread.get('subject') or ndors}", 120)
    message = clean_support_text(payload.message, 4000)
    command = queue_support_reply_command(ndors, title, message, payload.thread_id)
    items = thread.setdefault("messages", [])
    if not isinstance(items, list):
        items = []
        thread["messages"] = items
    items.append({
        "id": secrets.token_urlsafe(10),
        "at": now,
        "from": "admin",
        "message": message,
        "command_id": command.get("id") or "",
    })
    thread["updated_at"] = now
    thread["last_admin_reply_at"] = now
    thread["status"] = "waiting_for_trainer"
    thread["archived"] = False
    thread["unread_admin_count"] = 0
    save_support_threads(threads)
    admin_audit("support_reply", {
        "target_type": "support_thread",
        "target_id": payload.thread_id,
        "ndors_trainer_id": ndors,
        "command_id": command.get("id") or "",
    })
    return {"ok": True, "thread": public_support_thread(thread), "command": command}


@app.post("/admin/api/support/status")
def admin_support_status(payload: AdminSupportStatusRequest, request: Request):
    require_admin(request)
    allowed = {"open", "waiting_for_trainer", "waiting_for_admin", "resolved"}
    status = payload.status if payload.status in allowed else "open"
    threads = load_support_threads()
    thread = next((item for item in threads if isinstance(item, dict) and item.get("id") == payload.thread_id), None)
    if not thread:
        raise HTTPException(status_code=404, detail="Support thread not found")
    thread["status"] = status
    thread["archived"] = False
    if status in {"resolved", "waiting_for_trainer"}:
        thread["unread_admin_count"] = 0
    thread["updated_at"] = utc_now()
    save_support_threads(threads)
    admin_audit("support_status", {"target_type": "support_thread", "target_id": payload.thread_id, "status": status})
    return {"ok": True, "thread": public_support_thread(thread)}


@app.post("/admin/api/support/archive")
def admin_support_archive(payload: AdminSupportManageRequest, request: Request):
    require_admin(request)
    threads = load_support_threads()
    thread = next((item for item in threads if isinstance(item, dict) and item.get("id") == payload.thread_id), None)
    if not thread:
        raise HTTPException(status_code=404, detail="Support thread not found")
    thread["archived"] = True
    thread["unread_admin_count"] = 0
    thread["updated_at"] = utc_now()
    save_support_threads(threads)
    admin_audit("support_archive", {"target_type": "support_thread", "target_id": payload.thread_id})
    return {"ok": True, "thread": public_support_thread(thread)}


@app.post("/admin/api/support/delete")
def admin_support_delete(payload: AdminSupportManageRequest, request: Request):
    require_admin(request)
    threads = load_support_threads()
    thread = next((item for item in threads if isinstance(item, dict) and item.get("id") == payload.thread_id), None)
    if not thread:
        raise HTTPException(status_code=404, detail="Support thread not found")
    thread["deleted"] = True
    thread["archived"] = True
    thread["unread_admin_count"] = 0
    thread["updated_at"] = utc_now()
    save_support_threads(threads)
    admin_audit("support_delete", {"target_type": "support_thread", "target_id": payload.thread_id, "severity": "warning"})
    return {"ok": True}


@app.post("/admin/api/support/read")
def admin_support_mark_read(payload: AdminSupportManageRequest, request: Request):
    require_admin(request)
    threads = load_support_threads()
    thread = next((item for item in threads if isinstance(item, dict) and item.get("id") == payload.thread_id), None)
    if not thread:
        raise HTTPException(status_code=404, detail="Support thread not found")
    thread["unread_admin_count"] = 0
    thread["updated_at"] = utc_now()
    save_support_threads(threads)
    admin_audit("support_mark_read", {"target_type": "support_thread", "target_id": payload.thread_id})
    return {"ok": True, "thread": public_support_thread(thread)}


@app.post("/admin/api/support/note")
def admin_support_note(payload: AdminSupportManageRequest, request: Request):
    require_admin(request)
    note = clean_support_text(payload.note, 2000)
    if not note:
        raise HTTPException(status_code=400, detail="Note is empty")
    threads = load_support_threads()
    thread = next((item for item in threads if isinstance(item, dict) and item.get("id") == payload.thread_id), None)
    if not thread:
        raise HTTPException(status_code=404, detail="Support thread not found")
    notes = thread.setdefault("notes", [])
    if not isinstance(notes, list):
        notes = []
        thread["notes"] = notes
    notes.append({"id": secrets.token_urlsafe(10), "at": utc_now(), "note": note})
    thread["updated_at"] = utc_now()
    save_support_threads(threads)
    admin_audit("support_note", {"target_type": "support_thread", "target_id": payload.thread_id})
    return {"ok": True, "thread": public_support_thread(thread)}


@app.post("/admin/api/support/meta")
def admin_support_meta(payload: AdminSupportManageRequest, request: Request):
    require_admin(request)
    threads = load_support_threads()
    thread = next((item for item in threads if isinstance(item, dict) and item.get("id") == payload.thread_id), None)
    if not thread:
        raise HTTPException(status_code=404, detail="Support thread not found")
    if payload.category is not None:
        thread["category"] = clean_support_text(payload.category or "General", 80) or "General"
    if payload.priority is not None:
        priority = clean_support_text(payload.priority or "normal", 40).lower()
        thread["priority"] = priority if priority in {"low", "normal", "urgent"} else "normal"
    thread["updated_at"] = utc_now()
    save_support_threads(threads)
    admin_audit("support_meta", {"target_type": "support_thread", "target_id": payload.thread_id, "category": thread.get("category"), "priority": thread.get("priority")})
    return {"ok": True, "thread": public_support_thread(thread)}


@app.post("/admin/api/support/message-delete")
def admin_support_message_delete(payload: AdminSupportManageRequest, request: Request):
    require_admin(request)
    threads = load_support_threads()
    thread = next((item for item in threads if isinstance(item, dict) and item.get("id") == payload.thread_id), None)
    if not thread:
        raise HTTPException(status_code=404, detail="Support thread not found")
    found = False
    for item in thread.get("messages") or []:
        if isinstance(item, dict) and item.get("id") == payload.message_id:
            item["deleted"] = True
            found = True
            break
    if not found:
        raise HTTPException(status_code=404, detail="Message not found")
    thread["updated_at"] = utc_now()
    save_support_threads(threads)
    admin_audit("support_message_delete", {"target_type": "support_thread", "target_id": payload.thread_id, "message_id": payload.message_id, "severity": "warning"})
    return {"ok": True, "thread": public_support_thread(thread)}


@app.post("/client/heartbeat")
def client_heartbeat(payload: ClientHeartbeatRequest):
    state = load_admin_state()
    key = f"{payload.ndors_trainer_id.strip().lower()}::{payload.device_id.strip()}"
    status = payload.status if isinstance(payload.status, dict) else {}
    update = update_info_for_version(payload.app_version)
    status.setdefault("update", update)

    # Keep remote support safe: the desktop app should never send passwords or tokens here.
    safe_status = redact_sensitive(status)
    state.setdefault("devices", {})[key] = {
        "last_seen_at": utc_now(),
        "ndors_trainer_id": payload.ndors_trainer_id,
        "email": payload.email,
        "device_id": payload.device_id,
        "device_name": payload.device_name,
        "app_version": payload.app_version,
        "build": payload.build,
        "status": safe_status,
    }
    save_admin_state(state)

    try:
        db_upsert_device_heartbeat(payload, safe_status)
        courses = status.get("courses")
        if isinstance(courses, list):
            db_upsert_synced_courses(payload.ndors_trainer_id, payload.device_id, courses)
    except Exception:
        pass

    return {"ok": True, "update": update}


@app.post("/client/commands")
def client_poll_commands(payload: ClientCommandPollRequest):
    out = []

    # Prefer Supabase-backed commands. Fall back to the original JSON queue if
    # the table has not been created yet.
    try:
        result = execute_supabase(
            supabase.table("admin_commands")
            .select("*")
            .eq("ndors_trainer_id", payload.ndors_trainer_id)
            .in_("status", ["queued", "sent"])
            .order("created_at")
            .limit(10),
            "poll admin commands",
        )
        rows = result.data or []
        selected = []
        for command in rows:
            target_device = command.get("device_id") or ""
            if target_device and target_device != payload.device_id:
                continue
            selected.append(command)
            if len(selected) >= 5:
                break

        for command in selected:
            execute_supabase(
                supabase.table("admin_commands").update({
                    "status": "sent",
                    "claimed_at": command.get("claimed_at") or utc_now(),
                    "updated_at": utc_now(),
                }).eq("id", command["id"]),
                "mark command sent",
            )
            out.append({
                "id": command.get("id"),
                "created_at": command.get("created_at"),
                "updated_at": utc_now(),
                "ndors_trainer_id": command.get("ndors_trainer_id"),
                "device_id": command.get("device_id") or "",
                "command_type": command.get("command_type"),
                # Clients need the raw payload so one-time provider credentials can be applied.
                # The result endpoint clears payload after completion.
                "payload": command.get("payload") or {},
                "status": "sent",
                "message": command.get("message") or "",
                "result": command.get("result") or {},
            })
        return {"ok": True, "commands": out}
    except Exception:
        pass

    commands = load_admin_commands()
    changed = False
    for command in commands:
        if command.get("status") not in {"queued", "sent"}:
            continue
        if (command.get("ndors_trainer_id") or "").strip().lower() != payload.ndors_trainer_id.strip().lower():
            continue
        if command.get("device_id") and command.get("device_id") != payload.device_id:
            continue
        command["status"] = "sent"
        command["updated_at"] = utc_now()
        out.append(public_command(command))
        changed = True
    if changed:
        save_admin_commands(commands)
    return {"ok": True, "commands": out[:5]}


@app.post("/client/commands/result")
def client_command_result(payload: ClientCommandResultRequest):
    clean_status = payload.status if payload.status in {"queued", "sent", "running", "completed", "failed", "cancelled", "expired"} else "completed"
    clean_result = redact_sensitive(payload.result or {})

    try:
        result = execute_supabase(
            supabase.table("admin_commands")
            .select("*")
            .eq("id", payload.command_id)
            .limit(1),
            "read command for result",
        )
        if result.data:
            updates = {
                "status": clean_status,
                "message": payload.message or "",
                "result": clean_result,
                "updated_at": utc_now(),
                # Clear raw command payload after use so credentials are not retained.
                "payload": {},
            }
            if clean_status in {"completed", "failed", "cancelled", "expired"}:
                updates["completed_at"] = utc_now()
            execute_supabase(
                supabase.table("admin_commands").update(updates).eq("id", payload.command_id),
                "update command result",
            )
            db_save_support_bundle(payload)

            courses = (payload.result or {}).get("courses") if isinstance(payload.result, dict) else None
            if isinstance(courses, list):
                try:
                    db_upsert_synced_courses(payload.ndors_trainer_id, payload.device_id, courses)
                except Exception:
                    pass

            admin_audit("command_result", {
                "command_id": payload.command_id,
                "status": clean_status,
                "ndors_trainer_id": payload.ndors_trainer_id,
            })
            return {"ok": True}
    except Exception:
        pass

    commands = load_admin_commands()
    found = False
    for command in commands:
        if command.get("id") != payload.command_id:
            continue
        found = True
        command["status"] = clean_status
        command["message"] = payload.message or ""
        command["result"] = clean_result
        command["payload"] = {}
        command["updated_at"] = utc_now()
        break
    if not found:
        raise HTTPException(status_code=404, detail="Command not found")
    save_admin_commands(commands)
    admin_audit("command_result", {"command_id": payload.command_id, "status": clean_status})
    return {"ok": True}


@app.post("/check-access")
def check_access(payload: AccessRequest):
    if not valid_ndors_id(payload.ndors_trainer_id):
        raise HTTPException(status_code=400, detail="Use your NDORS trainer ID, not your email address")
    try:
        account = get_account_by_ndors(payload.ndors_trainer_id)

        if not account:
            account = create_free_account(payload.ndors_trainer_id, payload.email)

        ensure_device(account["id"], payload.device_id, payload.device_name)
        response = access_response_for_account(account, payload.app_version)
        cache_access_response(payload.ndors_trainer_id, response)
        return response

    except TemporaryLicensingBackendError as exc:
        print(f"[LICENSING] Temporary backend issue. Using cached/fallback access: {exc}")
        return temporary_access_response(payload.ndors_trainer_id)


@app.post("/register-account")
def register_account(payload: AccountRegisterRequest, request: Request):
    check_auth_rate_limit(request, "register_account", payload.ndors_trainer_id, AUTH_RATE_LIMIT_MAX_ATTEMPTS)
    if not valid_ndors_id(payload.ndors_trainer_id):
        raise HTTPException(status_code=400, detail="Use your NDORS trainer ID, not your email address")
    if not valid_email(payload.email):
        raise HTTPException(status_code=400, detail="Enter a valid email address")
    if len(payload.password or "") < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    try:
        account = get_account_by_ndors(payload.ndors_trainer_id)
        new_hash = password_hash(payload.password)
        if not account:
            account = create_free_account(payload.ndors_trainer_id, payload.email, new_hash)
        else:
            existing_hash = account.get("password_hash") or ""
            primary_email = (account.get("primary_email") or "").strip().lower()
            supplied_email = (payload.email or "").strip().lower()
            if existing_hash and not password_matches(payload.password, existing_hash):
                raise HTTPException(status_code=409, detail="This NDORS ID is already registered. Please log in instead.")
            updates = {
                "primary_email": payload.email or account.get("primary_email"),
                "last_login_at": utc_now(),
            }
            if not existing_hash:
                updates["password_hash"] = new_hash
                updates["password_set_at"] = utc_now()
                updates["password_must_change"] = False
            if primary_email and supplied_email and primary_email != supplied_email and existing_hash:
                raise HTTPException(status_code=403, detail="That email does not match the registered account.")
            updated_rows = update_accounts_by_ndors(payload.ndors_trainer_id, updates, "register existing account")
            account = best_account_row(updated_rows) or dict(account, **updates)
        ensure_device(account["id"], payload.device_id, payload.device_name)
        response = access_response_for_account(account, payload.app_version)
        cache_access_response(payload.ndors_trainer_id, response)
        admin_audit("account_register", {"ndors_trainer_id": payload.ndors_trainer_id})
        clear_auth_rate_limit(request, "register_account", payload.ndors_trainer_id)
        return {"ok": True, "account": {"ndors_trainer_id": account.get("ndors_trainer_id"), "email": account.get("primary_email")}, "access": response}
    except TemporaryLicensingBackendError as exc:
        raise HTTPException(status_code=503, detail=f"Account service temporarily unavailable: {exc}")


@app.post("/reset-password")
def reset_password(payload: PasswordResetRequest, request: Request):
    check_auth_rate_limit(request, "reset_password", payload.ndors_trainer_id, AUTH_RESET_RATE_LIMIT_MAX_ATTEMPTS)
    if not valid_ndors_id(payload.ndors_trainer_id) or not valid_email(payload.email):
        raise HTTPException(status_code=400, detail="Enter your NDORS trainer ID and registered email address")
    try:
        accounts = get_accounts_by_ndors(payload.ndors_trainer_id)
        if accounts and account_email_matches(accounts, payload.email):
            account = best_account_row(accounts)
            reset_token = generate_reset_token()
            expires_at = (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat()
            execute_supabase(
                supabase.table("password_reset_tokens").insert({
                    "account_id": account.get("id"),
                    "ndors_trainer_id": payload.ndors_trainer_id,
                    "email": payload.email,
                    "token_hash": reset_token_hash(reset_token),
                    "expires_at": expires_at,
                    "created_at": utc_now(),
                }),
                "create password reset token",
            )
            send_password_reset_token_email(payload.email, payload.ndors_trainer_id, reset_token)
        admin_audit("account_password_reset_requested", {"ndors_trainer_id": payload.ndors_trainer_id})
        return {"ok": True, "message": "If those details match a TrainerMate account, a reset code has been emailed."}
    except HTTPException:
        raise
    except TemporaryLicensingBackendError as exc:
        raise HTTPException(status_code=503, detail=f"Account service temporarily unavailable: {exc}")


@app.post("/confirm-password-reset")
def confirm_password_reset(payload: PasswordResetConfirmRequest, request: Request):
    check_auth_rate_limit(request, "confirm_password_reset", payload.ndors_trainer_id, AUTH_RESET_RATE_LIMIT_MAX_ATTEMPTS)
    if not valid_ndors_id(payload.ndors_trainer_id):
        raise HTTPException(status_code=400, detail="Use your NDORS trainer ID, not your email address")
    if len(payload.password or "") < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    try:
        token_hash_value = reset_token_hash(payload.reset_token)
        result = execute_supabase(
            supabase.table("password_reset_tokens")
            .select("*")
            .eq("token_hash", token_hash_value)
            .eq("ndors_trainer_id", payload.ndors_trainer_id)
            .is_("used_at", "null")
            .limit(1),
            "read password reset token",
        )
        token_row = (result.data or [None])[0]
        if not token_row:
            raise HTTPException(status_code=403, detail="Reset code not recognised or already used.")
        expires_raw = token_row.get("expires_at") or ""
        try:
            expires_at = datetime.fromisoformat(str(expires_raw).replace("Z", "+00:00"))
        except Exception:
            expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        if expires_at < datetime.now(timezone.utc):
            raise HTTPException(status_code=403, detail="Reset code has expired. Request a new one.")
        account = get_account_by_ndors(payload.ndors_trainer_id)
        if not account or (token_row.get("account_id") and token_row.get("account_id") != account.get("id")):
            raise HTTPException(status_code=403, detail="Reset code could not be verified.")
        new_hash = password_hash(payload.password)
        updates = {
            "password_hash": new_hash,
            "password_set_at": utc_now(),
            "last_login_at": utc_now(),
            "password_must_change": False,
        }
        updated_rows = update_accounts_by_ndors(payload.ndors_trainer_id, updates, "confirm password reset")
        execute_supabase(
            supabase.table("password_reset_tokens").update({"used_at": utc_now()}).eq("id", token_row.get("id")),
            "mark password reset token used",
        )
        account = best_account_row(updated_rows) or account
        if account:
            ensure_device(account["id"], payload.device_id, payload.device_name)
            response = access_response_for_account(account, payload.app_version)
            cache_access_response(payload.ndors_trainer_id, response)
        else:
            response = {}
        admin_audit("account_password_reset", {"ndors_trainer_id": payload.ndors_trainer_id})
        clear_auth_rate_limit(request, "reset_password", payload.ndors_trainer_id)
        clear_auth_rate_limit(request, "confirm_password_reset", payload.ndors_trainer_id)
        clear_auth_rate_limit(request, "login_account", payload.ndors_trainer_id)
        return {"ok": True, "account": {"ndors_trainer_id": payload.ndors_trainer_id, "email": account.get("primary_email") if account else ""}, "access": response}
    except HTTPException:
        raise
    except TemporaryLicensingBackendError as exc:
        raise HTTPException(status_code=503, detail=f"Account service temporarily unavailable: {exc}")


@app.post("/login-account")
def login_account(payload: AccountLoginRequest, request: Request):
    check_auth_rate_limit(request, "login_account", payload.ndors_trainer_id, AUTH_RATE_LIMIT_MAX_ATTEMPTS)
    if not valid_ndors_id(payload.ndors_trainer_id):
        raise HTTPException(status_code=400, detail="Use your NDORS trainer ID, not your email address")
    try:
        account = get_account_by_ndors(payload.ndors_trainer_id)
        if not account or not account.get("password_hash"):
            raise HTTPException(status_code=404, detail="Account not registered yet on TrainerMate.")
        if not password_matches(payload.password, account.get("password_hash")):
            raise HTTPException(status_code=401, detail="NDORS ID or password not recognised.")
        execute_supabase(
            supabase.table("accounts").update({"last_login_at": utc_now()}).eq("ndors_trainer_id", payload.ndors_trainer_id),
            "record account login"
        )
        ensure_device(account["id"], payload.device_id, payload.device_name)
        response = access_response_for_account(account, payload.app_version)
        cache_access_response(payload.ndors_trainer_id, response)
        admin_audit("account_login", {"ndors_trainer_id": payload.ndors_trainer_id})
        clear_auth_rate_limit(request, "login_account", payload.ndors_trainer_id)
        return {"ok": True, "account": {"ndors_trainer_id": account.get("ndors_trainer_id"), "email": account.get("primary_email"), "password_must_change": bool(account.get("password_must_change"))}, "access": response}
    except TemporaryLicensingBackendError as exc:
        raise HTTPException(status_code=503, detail=f"Account service temporarily unavailable: {exc}")


@app.post("/change-password")
def change_password(payload: AccountPasswordChangeRequest, request: Request):
    check_auth_rate_limit(request, "change_password", payload.ndors_trainer_id, AUTH_RATE_LIMIT_MAX_ATTEMPTS)
    if not valid_ndors_id(payload.ndors_trainer_id):
        raise HTTPException(status_code=400, detail="Use your NDORS trainer ID, not your email address")
    if len(payload.new_password or "") < 8:
        raise HTTPException(status_code=400, detail="New password must be at least 8 characters")
    try:
        account = get_account_by_ndors(payload.ndors_trainer_id)
        if not account or not account.get("password_hash"):
            raise HTTPException(status_code=404, detail="Account not registered yet on TrainerMate.")
        if not password_matches(payload.current_password, account.get("password_hash")):
            raise HTTPException(status_code=401, detail="Current password not recognised.")
        updates = {
            "password_hash": password_hash(payload.new_password),
            "password_set_at": utc_now(),
            "password_must_change": False,
            "last_login_at": utc_now(),
        }
        updated_rows = update_accounts_by_ndors(payload.ndors_trainer_id, updates, "change account password")
        account = best_account_row(updated_rows) or dict(account, **updates)
        ensure_device(account["id"], payload.device_id, payload.device_name)
        response = access_response_for_account(account, payload.app_version)
        cache_access_response(payload.ndors_trainer_id, response)
        admin_audit("account_password_change", {"ndors_trainer_id": payload.ndors_trainer_id})
        clear_auth_rate_limit(request, "change_password", payload.ndors_trainer_id)
        clear_auth_rate_limit(request, "login_account", payload.ndors_trainer_id)
        return {"ok": True, "account": {"ndors_trainer_id": account.get("ndors_trainer_id"), "email": account.get("primary_email"), "password_must_change": False}, "access": response}
    except HTTPException:
        raise
    except TemporaryLicensingBackendError as exc:
        raise HTTPException(status_code=503, detail=f"Account service temporarily unavailable: {exc}")


@app.post("/record-sync")
def record_sync(payload: AccessRequest):
    try:
        account = get_account_by_ndors(payload.ndors_trainer_id)

        if not account:
            # Do not fail the desktop app if the account row cannot be read.
            queue_pending_sync_record(payload)
            return {
                "ok": True,
                "queued": True,
                "reason": "account_not_available_temporarily"
            }

        usage = get_usage(account["id"])

        if account.get("plan") == "free":
            new_used = int((usage or {}).get("free_syncs_used", 0) or 0) + 1
            execute_supabase(
                supabase.table("usage").update({
                    "free_syncs_used": new_used,
                    "last_sync_at": utc_now()
                }).eq("account_id", account["id"]),
                "record free sync"
            )

            return {
                "ok": True,
                "plan": "free",
                "free_syncs_used": new_used,
                "free_syncs_remaining": max(0, FREE_SYNC_LIMIT - new_used)
            }

        execute_supabase(
            supabase.table("usage").update({
                "last_sync_at": utc_now()
            }).eq("account_id", account["id"]),
            "record paid sync"
        )

        return {
            "ok": True,
            "plan": account.get("plan", "paid")
        }

    except TemporaryLicensingBackendError as exc:
        print(f"[LICENSING] Could not record sync now; queued locally: {exc}")
        queue_pending_sync_record(payload)
        return {
            "ok": True,
            "queued": True,
            "reason": "licensing_backend_temporarily_unavailable"
        }


@app.post("/redeem-licence")
def redeem_licence(payload: RedeemLicenceRequest):
    try:
        account = get_account_by_ndors(payload.ndors_trainer_id)

        if not account:
            account = create_free_account(payload.ndors_trainer_id, payload.email)

        licence_result = execute_supabase(
            supabase.table("licences")
            .select("*")
            .eq("licence_key", payload.licence_key)
            .limit(1),
            "read licence"
        )

        if not licence_result.data:
            raise HTTPException(status_code=404, detail="Licence not found")

        licence = licence_result.data[0]

        if licence["status"] not in ("active", "unused"):
            raise HTTPException(status_code=400, detail="Licence is not redeemable")

        if licence["issued_to_ndors_trainer_id"] and licence["issued_to_ndors_trainer_id"] != payload.ndors_trainer_id:
            raise HTTPException(status_code=403, detail="Licence belongs to a different trainer")

        expiry = licence.get("expiry_date")
        if expiry and expiry < utc_now():
            raise HTTPException(status_code=400, detail="Licence has expired")

        execute_supabase(
            supabase.table("licences").update({
                "account_id": account["id"],
                "status": "active",
                "redeemed_at": utc_now(),
                "issued_to_ndors_trainer_id": payload.ndors_trainer_id
            }).eq("id", licence["id"]),
            "redeem licence"
        )

        execute_supabase(
            supabase.table("accounts").update({
                "plan": licence["plan_type"],
                "primary_email": payload.email or account.get("primary_email")
            }).eq("id", account["id"]),
            "update account plan"
        )

        response = {
            "allowed": True,
            "reason": "ok",
            "plan": licence["plan_type"],
            "free_syncs_remaining": FREE_SYNC_LIMIT,
            "features": account_features(licence["plan_type"]),
        }
        cache_access_response(payload.ndors_trainer_id, response)

        return {
            "ok": True,
            "plan": licence["plan_type"]
        }

    except TemporaryLicensingBackendError:
        raise HTTPException(status_code=503, detail="Licence service temporarily unavailable. Please try again.")
