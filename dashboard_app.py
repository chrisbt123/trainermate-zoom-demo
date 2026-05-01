import shutil
import json, os, signal, sqlite3, subprocess, sys, uuid, threading, time, secrets, hmac, mimetypes, re, hashlib, ctypes
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlencode

import requests
from flask import Flask, Response, abort, jsonify, redirect, render_template_string, request, send_file, session, url_for
from werkzeug.exceptions import HTTPException, RequestEntityTooLarge

APP_NAME = "TrainerMate"
APP_VERSION = "1.0.0"
BUILD_CHANNEL = "Production"
BUILD_NAME = "dashboard_app + bot_app"
BUILD_LABEL = f"{APP_NAME} v{APP_VERSION} {BUILD_CHANNEL}"
DASHBOARD_CANONICAL_URL = "http://127.0.0.1:5000"

try:
    import keyring as _real_keyring
except Exception:
    _real_keyring = None

class _SafeKeyring:
    def get_password(self, service, username):
        if _real_keyring is None:
            return None
        try:
            return _real_keyring.get_password(service, username)
        except Exception:
            return None

    def set_password(self, service, username, password):
        if _real_keyring is None:
            return None
        try:
            return _real_keyring.set_password(service, username, password)
        except Exception:
            return None

    def delete_password(self, service, username):
        if _real_keyring is None:
            return None
        try:
            return _real_keyring.delete_password(service, username)
        except Exception:
            return None

keyring = _SafeKeyring()

try:
    from playwright.sync_api import sync_playwright
except Exception:
    sync_playwright = None

FOBS_BROWSER_SESSIONS = []
FOBS_LAUNCH_STATUS = {}
FOBS_LAUNCH_STATUS_LOCK = threading.Lock()
CERTIFICATE_SCAN_STATUS = {}
CERTIFICATE_SCAN_STATUS_LOCK = threading.Lock()
STARTUP_CERTIFICATE_SCAN_LOCK = threading.Lock()
STARTUP_CERTIFICATE_SCAN_STARTED = False
STARTUP_ZOOM_HEALTH_CHECK_LOCK = threading.Lock()
STARTUP_ZOOM_HEALTH_CHECK_STARTED = False
STARTUP_ZOOM_HEALTH_CHECK_STATUS = {
    'status': 'idle',
    'message': 'Zoom check has not run yet.',
    'detail': '',
    'updated_at': '',
}
PROVIDER_UPLOAD_QUEUE_LOCK = threading.Lock()
PROVIDER_UPLOAD_QUEUE_ACTIVE = False
PROVIDER_DELETE_CANCEL_LOCK = threading.Lock()
PROVIDER_DELETE_CANCEL_REQUESTS = set()
PROVIDER_CACHE_VERSION = 'v2_exact_download'

API_URL = os.getenv('TRAINERMATE_API_URL', 'http://127.0.0.1:8000')
BASE_DIR = Path(__file__).resolve().parent
APP_STATE_PATH = BASE_DIR / 'app_state.json'
PROVIDERS_PATH = BASE_DIR / 'providers.json'
PROVIDER_CATALOGUE_PATH = BASE_DIR / 'provider_catalogue.json'
COURSES_DB_PATH = BASE_DIR / 'courses.db'
BOT_APP_PATH = BASE_DIR / 'bot_app.py'
ZOOM_ACCOUNTS_PATH = BASE_DIR / 'zoom_accounts.json'
BOT_LOG_PATH = BASE_DIR / 'bot_debug.log'
ALERT_ACK_PATH = BASE_DIR / 'dashboard_alerts_ack.json'
COURSE_REMOVAL_CONFIRM_PATH = BASE_DIR / 'course_removal_confirmed.json'
DOCUMENTS_DIR = BASE_DIR / 'trainer_documents'
AUTOMATION_SETTINGS_PATH = BASE_DIR / 'automation_settings.json'
ACTIVITY_HISTORY_PATH = BASE_DIR / 'activity_history.json'

def env_int(name, default, minimum=None, maximum=None):
    try:
        value = int((os.getenv(name) or str(default)).strip() or default)
    except Exception:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def env_float(name, default, minimum=None, maximum=None):
    try:
        value = float((os.getenv(name) or str(default)).strip() or default)
    except Exception:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


FREE_SYNC_LIMIT = env_int('FREE_SYNC_LIMIT', 3, minimum=0)
FREE_SYNC_WINDOW_DAYS = 21
PAID_SYNC_WINDOW_DAYS = 84

# Zoom OAuth credentials can come from environment variables, a local .env file,
# or TrainerMate's local zoom_oauth_config.json file.
# This lets trainers connect another Zoom account without editing Python files.
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

ZOOM_OAUTH_CONFIG_PATH = BASE_DIR / 'zoom_oauth_config.json'

def _load_zoom_oauth_config_file():
    try:
        if ZOOM_OAUTH_CONFIG_PATH.exists():
            with ZOOM_OAUTH_CONFIG_PATH.open('r', encoding='utf-8') as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}

_zoom_oauth_config = _load_zoom_oauth_config_file()
ZOOM_CLIENT_ID = (os.getenv('ZOOM_CLIENT_ID') or _zoom_oauth_config.get('client_id') or '').strip()
ZOOM_CLIENT_SECRET = (os.getenv('ZOOM_CLIENT_SECRET') or _zoom_oauth_config.get('client_secret') or '').strip()
LOCAL_ZOOM_CALLBACK_URI = 'http://127.0.0.1:5000/zoom/callback'
ZOOM_APPROVED_RELAY_URI = 'https://demo.trainermate.xyz/zoom/callback'
# Keep the pending Zoom Marketplace redirect stable. Localhost is only used
# after the hosted callback relays the browser back to the desktop app.
ZOOM_REDIRECT_URI = ZOOM_APPROVED_RELAY_URI
ZOOM_RELAY_STATE_PREFIX = 'tmrelay:'
ACCESS_CACHE_PATH = BASE_DIR / 'access_cache.json'
# Keep licence cache deliberately short so admin plan changes show quickly.
ACCESS_CACHE_MAX_AGE_SECONDS = env_int('TRAINERMATE_ACCESS_CACHE_MAX_AGE_SECONDS', 20, minimum=0, maximum=300)
HOME_ACCESS_TIMEOUT_SECONDS = env_float('TRAINERMATE_HOME_ACCESS_TIMEOUT', 2.0, minimum=0.5, maximum=30.0)
ACTION_ACCESS_TIMEOUT_SECONDS = env_float('TRAINERMATE_ACTION_ACCESS_TIMEOUT', 15.0, minimum=1.0, maximum=60.0)
STARTUP_CERTIFICATE_SCAN_ENABLED = os.getenv('TRAINERMATE_STARTUP_CERTIFICATE_SCAN', '1') != '0'
STARTUP_CERTIFICATE_SCAN_DELAY_SECONDS = env_float('TRAINERMATE_STARTUP_CERTIFICATE_SCAN_DELAY', 2.0, minimum=0.0, maximum=60.0)
STARTUP_ZOOM_HEALTH_CHECK_ENABLED = os.getenv('TRAINERMATE_STARTUP_ZOOM_HEALTH_CHECK', '1') != '0'
STARTUP_ZOOM_HEALTH_CHECK_DELAY_SECONDS = env_float('TRAINERMATE_STARTUP_ZOOM_HEALTH_CHECK_DELAY', 1.0, minimum=0.0, maximum=60.0)

PROVIDER_PRESETS = {
    'essex': {'name': 'Essex', 'login_url': 'https://www.essexfobs.co.uk/Account/Login', 'color': '#2563eb', 'provider_manages_zoom': False, 'supports_custom_time': True, 'never_overwrite_existing_zoom': False},
    'west-mids': {'name': 'West Mids', 'login_url': 'https://www.westmidlandsfobs.org.uk/Account/Login', 'color': '#059669', 'provider_manages_zoom': False, 'supports_custom_time': True, 'never_overwrite_existing_zoom': False},
    'suffolk': {'name': 'Suffolk', 'login_url': '', 'color': '#d97706', 'provider_manages_zoom': False, 'supports_custom_time': True, 'never_overwrite_existing_zoom': False},
    'lincolnshire': {'name': 'Lincolnshire', 'login_url': 'https://www.lincsfobs.co.uk/Account/Login', 'color': '#7c3aed', 'provider_manages_zoom': True, 'supports_custom_time': True, 'never_overwrite_existing_zoom': True},
    'manual': {'name': '', 'login_url': '', 'color': '#0891b2', 'provider_manages_zoom': False, 'supports_custom_time': True, 'never_overwrite_existing_zoom': False},
}

app = Flask(__name__)
REVIEWER_DEMO_MODE = os.getenv('TRAINERMATE_REVIEWER_DEMO', '1').strip().lower() in {'1', 'true', 'yes', 'on'}
REVIEWER_PASSWORD = os.getenv('REVIEWER_PASSWORD', '').strip()
REVIEWER_TOKEN_STORE_PATH = BASE_DIR / 'reviewer_zoom_tokens.json'
REVIEWER_DEMO_SEEDED = False

app.secret_key = os.getenv('TRAINERMATE_DASHBOARD_SECRET') or os.getenv('FLASK_SECRET_KEY') or 'dev-secret'
if app.secret_key == 'dev-secret':
    _stored_dashboard_secret = keyring.get_password('trainermate', 'dashboard_secret') or ''
    if not _stored_dashboard_secret:
        _stored_dashboard_secret = secrets.token_urlsafe(48)
        keyring.set_password('trainermate', 'dashboard_secret', _stored_dashboard_secret)
    app.secret_key = _stored_dashboard_secret
app.config['MAX_CONTENT_LENGTH'] = env_int('TRAINERMATE_MAX_UPLOAD_MB', 20, minimum=1, maximum=100) * 1024 * 1024
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = (os.getenv('TRAINERMATE_COOKIE_SECURE') or os.getenv('SESSION_COOKIE_SECURE') or '0') == '1'

ALLOWED_DOCUMENT_EXTENSIONS = {'.pdf', '.jpg', '.jpeg', '.png', '.doc', '.docx', '.xls', '.xlsx', '.odt', '.tif', '.tiff'}
ALLOWED_DOCUMENT_MIME_TYPES = {
    'application/pdf',
    'image/jpeg',
    'image/png',
    'application/msword',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.oasis.opendocument.text',
    'image/tiff',
}

def is_local_request():
    remote = (request.remote_addr or '').strip()
    forwarded = (request.headers.get('X-Forwarded-For') or '').split(',')[0].strip()
    host = (request.host or '').split(':')[0].strip().lower()
    allowed = {'127.0.0.1', '::1', 'localhost'}
    return remote in allowed and (not forwarded or forwarded in allowed) and host in allowed

def csrf_token():
    token = session.get('_csrf_token')
    if not token:
        token = secrets.token_urlsafe(32)
        session['_csrf_token'] = token
    return token

def validate_csrf():
    expected = session.get('_csrf_token') or ''
    supplied = request.form.get('_csrf_token') or request.headers.get('X-CSRF-Token') or ''
    return bool(expected and supplied and hmac.compare_digest(str(expected), str(supplied)))

def csrf_hidden_field():
    return f"<input type='hidden' name='_csrf_token' value='{csrf_token()}'>"


def reviewer_demo_enabled():
    return REVIEWER_DEMO_MODE


def reviewer_demo_public_path():
    return request.path in {'/login', '/health', '/healthz'} or request.path.startswith('/static/')


def reviewer_demo_logged_in():
    return bool(session.get('reviewer_demo_ok'))


def ensure_reviewer_demo_seed():
    """Seed the hosted TrainerMate environment with TrainerMate course data.

    This intentionally does not use or require any real provider/FOBS credentials.
    The normal dashboard UI then displays these rows as if they had been imported
    from a provider system.
    """
    global REVIEWER_DEMO_SEEDED
    if REVIEWER_DEMO_SEEDED or not reviewer_demo_enabled():
        return
    REVIEWER_DEMO_SEEDED = True
    try:
        # Keep paid gates out of the reviewer's way so they can see the whole UI.
        save_json(ACCESS_CACHE_PATH, {
            'checked_at': utc_now_text(),
            'access': {
                'allowed': True,
                'paid': True,
                'plan': 'paid',
                'features': {
                    'sync_window_days': 84,
                    'automatic_sync': True,
                    'calendar_sync': True,
                    'certificate_manage': True,
                },
            },
        })
    except Exception:
        pass
    try:
        demo_provider = make_provider_defaults('Essex', 'https://essex.trainermate.local/Account/Login', True)
        demo_provider.update({
            'id': 'essex',
            'name': 'Essex',
            'login_url': 'https://essex.trainermate.local/Account/Login',
            'courses_url': 'https://essex.trainermate.local/Courses',
            'documents_url': 'https://essex.trainermate.local/Documents',
            'active': True,
            'color': '#2563eb',
            'zoom_account_id': get_default_zoom_account_id(),
            'provider_manages_zoom': False,
            'never_overwrite_existing_zoom': False,
            'read_only': False,
            'supports_custom_time': True,
        })
        save_providers([demo_provider])
        save_provider_credentials('essex', 'trainer@example.com', 'stored-secure-provider-password')
    except Exception:
        pass
    try:
        conn = sqlite3.connect(str(COURSES_DB_PATH))
        ensure_courses_sync_columns(conn)
        conn.execute("""
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
                last_seen_at TEXT,
                last_synced_at TEXT,
                last_sync_status TEXT,
                last_sync_action TEXT,
                fobs_course_url TEXT
            )
        """)
        now = datetime.now()
        demo_courses = [
            ('course-efaw-001', 'Emergency First Aid at Work', now + timedelta(days=12), '09:30'),
            ('course-manual-handling-001', 'Manual Handling Refresher', now + timedelta(days=14), '10:00'),
            ('course-safeguarding-001', 'Safeguarding Level 2', now + timedelta(days=19), '13:00'),
            ('course-medication-001', 'Medication Awareness', now + timedelta(days=23), '09:15'),
            ('course-fire-marshal-001', 'Fire Marshal Training', now + timedelta(days=26), '13:00'),
            ('course-food-safety-001', 'Food Safety Level 2', now + timedelta(days=31), '09:30'),
            ('course-moving-handling-001', 'Moving and Handling People', now + timedelta(days=36), '10:00'),
            ('course-mental-health-001', 'Mental Health Awareness', now + timedelta(days=42), '09:00'),
            ('course-infection-control-001', 'Infection Prevention and Control', now + timedelta(days=48), '14:00'),
            ('course-dementia-001', 'Dementia Awareness', now + timedelta(days=55), '09:30'),
            ('course-health-safety-001', 'Health and Safety in the Workplace', now + timedelta(days=63), '10:00'),
            ('course-lone-working-001', 'Lone Working and Personal Safety', now + timedelta(days=76), '11:00'),
        ]
        for course_id, title, date_base, time_text in demo_courses:
            hh, mm = [int(x) for x in time_text.split(':')]
            dt = date_base.replace(hour=hh, minute=mm, second=0, microsecond=0)
            conn.execute("""
                INSERT INTO courses (id, provider, title, date_time, meeting_id, meeting_link, meeting_password, status,
                                     active_in_portal, last_seen_at, last_synced_at, last_sync_status, last_sync_action, fobs_course_url)
                VALUES (?, 'Essex', ?, ?, '', '', '', 'Scheduled', 1, ?, '', '', 'Course imported from provider schedule.', ?)
                ON CONFLICT(id) DO UPDATE SET
                    provider=excluded.provider,
                    title=excluded.title,
                    date_time=excluded.date_time,
                    meeting_id=CASE WHEN COALESCE(?, '1') != '0' THEN '' ELSE courses.meeting_id END,
                    meeting_link=CASE WHEN COALESCE(?, '1') != '0' THEN '' ELSE courses.meeting_link END,
                    meeting_password=CASE WHEN COALESCE(?, '1') != '0' THEN '' ELSE courses.meeting_password END,
                    last_synced_at=CASE WHEN COALESCE(?, '1') != '0' THEN '' ELSE courses.last_synced_at END,
                    last_sync_status=CASE WHEN COALESCE(?, '1') != '0' THEN '' ELSE courses.last_sync_status END,
                    last_sync_action=CASE WHEN COALESCE(?, '1') != '0' THEN 'Course imported from provider schedule.' ELSE courses.last_sync_action END,
                    active_in_portal=1,
                    last_seen_at=excluded.last_seen_at,
                    fobs_course_url=excluded.fobs_course_url
            """, (course_id, title, dt.strftime('%Y-%m-%d %H:%M'), utc_now_text(), f'https://essex.trainermate.local/courses/{course_id}',
                  os.getenv('TRAINERMATE_RESET_SEEDED_COURSES', '0'), os.getenv('TRAINERMATE_RESET_SEEDED_COURSES', '0'),
                  os.getenv('TRAINERMATE_RESET_SEEDED_COURSES', '0'), os.getenv('TRAINERMATE_RESET_SEEDED_COURSES', '0'),
                  os.getenv('TRAINERMATE_RESET_SEEDED_COURSES', '0'), os.getenv('TRAINERMATE_RESET_SEEDED_COURSES', '0')))
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def reviewer_token_store_load():
    data = load_json(REVIEWER_TOKEN_STORE_PATH, {})
    return data if isinstance(data, dict) else {}


def reviewer_token_store_save(data):
    if isinstance(data, dict):
        save_json(REVIEWER_TOKEN_STORE_PATH, data)

def request_wants_json():
    accept = (request.headers.get('Accept') or '').lower()
    return (
        request.path.startswith('/api/')
        or request.path in {'/status', '/healthz'}
        or request.headers.get('X-Requested-With') == 'fetch'
        or ('application/json' in accept and 'text/html' not in accept)
    )

def friendly_error_response(title, message, status_code=400):
    if request_wants_json():
        return jsonify({'ok': False, 'error': message}), status_code
    return render_template_string("""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ title }}</title>
  <style>
    body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:#0b1220;color:#f8fafc;display:grid;place-items:center;min-height:100vh;padding:20px}
    .card{max-width:560px;border:1px solid rgba(125,211,252,.26);border-radius:18px;background:#111827;padding:22px;box-shadow:0 18px 45px rgba(0,0,0,.35)}
    h1{font-size:22px;margin:0 0 8px}p{color:#cbd5e1;line-height:1.45;margin:0 0 16px}.btn{display:inline-flex;text-decoration:none;color:#fff;background:#2563eb;border-radius:12px;padding:10px 14px;font-weight:800}
  
/* TrainerMate paid feature gate polish */
.tm-modal-backdrop[hidden]{display:none!important}
.tm-modal-backdrop{position:fixed;inset:0;background:rgba(2,6,23,.68);backdrop-filter:blur(5px);display:flex;align-items:center;justify-content:center;padding:22px;z-index:9999}
.tm-modal-card{width:min(560px,calc(100vw - 32px));background:linear-gradient(180deg,#f8fbff,#e9f2ff);color:#071225;border:1px solid rgba(96,165,250,.55);border-radius:26px;box-shadow:0 34px 90px rgba(0,0,0,.55);padding:26px}
.tm-modal-card h3{font-size:28px;line-height:1.15;margin:0 0 10px;color:#071225}.tm-modal-card p{margin:0 0 16px;color:#334155;line-height:1.55;font-size:16px}.tm-modal-sub{font-size:13px;color:#64748b;margin-bottom:18px}.tm-modal-actions{display:flex;justify-content:flex-end;gap:10px;flex-wrap:wrap}.tm-modal-actions .btn{min-width:120px}.tm-modal-close-x{float:right;border:0;background:#dbeafe;color:#0f172a;border-radius:999px;font-weight:900;width:34px;height:34px;cursor:pointer}
body.tm-modal-open{overflow:hidden}.tm-top-flash{display:none}
.tm-lock-wrap{position:relative}.tm-lock-wrap.is-locked > .tm-lock-content{opacity:.28;filter:grayscale(.2);pointer-events:none;user-select:none}.tm-lock-overlay{position:absolute;inset:0;z-index:6;display:flex;align-items:center;justify-content:center;padding:28px;background:rgba(2,6,23,.62);backdrop-filter:blur(3px);border-radius:24px}.tm-lock-card{width:min(520px,95%);background:linear-gradient(180deg,#f8fbff,#e9f2ff);color:#071225;border:1px solid rgba(96,165,250,.6);border-radius:24px;box-shadow:0 30px 80px rgba(0,0,0,.45);padding:24px;text-align:center}.tm-lock-card h3{margin:0 0 10px;font-size:26px}.tm-lock-card p{margin:0 0 16px;color:#334155;line-height:1.5}.tm-lock-card .btn{color:white}.tm-lock-card .helper-dark{font-size:13px;color:#64748b;margin-top:12px}.tm-paid-note{border:1px solid rgba(96,165,250,.28);background:rgba(37,99,235,.10);border-radius:16px;padding:14px;color:#cfe0ff;line-height:1.45}.tm-paid-note strong{color:white}.tm-disabled-select option:disabled{color:#94a3b8}.tm-account-note{border:1px solid rgba(96,165,250,.22);background:rgba(37,99,235,.08);border-radius:14px;padding:12px;color:#cbd5e1;line-height:1.45}.tm-account-note b{color:white}.tm-small-upgrade{font-size:12px;color:#93c5fd;margin-top:6px;line-height:1.4}.tm-cert-locked-actions{display:inline-flex;gap:8px;align-items:center;flex-wrap:wrap}

</style>
</head>
<body class='section-{{ current_section }}'>
  <div class="card">
    <h1>{{ title }}</h1>
    <p>{{ message }}</p>
    <a class="btn" href="{{ url_for('home') }}">Back to TrainerMate</a>
  </div>
</body>
</html>
    """, title=title, message=message), status_code

@app.before_request
def security_before_request():
    if reviewer_demo_enabled():
        ensure_reviewer_demo_seed()
        if not reviewer_demo_public_path() and not reviewer_demo_logged_in():
            if request_wants_json():
                return jsonify({'ok': False, 'error': 'Reviewer login required'}), 401
            return redirect(url_for('trainer_login', next=request.full_path if request.query_string else request.path))
    elif not is_local_request():
        abort(403)
    if request.method in {'POST', 'PUT', 'PATCH', 'DELETE'} and not validate_csrf():
        abort(400, description='Security check failed. Please refresh the dashboard and try again.')

@app.after_request
def security_after_request(response):
    response.headers.setdefault('X-Content-Type-Options', 'nosniff')
    response.headers.setdefault('X-Frame-Options', 'DENY')
    response.headers.setdefault('Referrer-Policy', 'same-origin')
    response.headers.setdefault('Permissions-Policy', 'camera=(), microphone=(), geolocation=()')
    response.headers.setdefault('Cross-Origin-Opener-Policy', 'same-origin')
    response.headers.setdefault('Cross-Origin-Resource-Policy', 'same-origin')
    response.headers.setdefault('Content-Security-Policy', "default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; img-src 'self' data:; font-src 'self' data: https://cdn.jsdelivr.net; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self' https://zoom.us;")
    if request.path.startswith('/documents') or request.endpoint == 'home':
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    ctype = response.headers.get('Content-Type', '')
    if request.method == 'GET' and response.status_code == 200 and 'text/html' in ctype:
        try:
            body = response.get_data(as_text=True)
            if '<form' in body:
                import re
                token_field = csrf_hidden_field()

                def add_csrf_to_post_form(match):
                    form_html = match.group(0)
                    attrs = form_html.lower()
                    if "name='_csrf_token'" in form_html or 'name="_csrf_token"' in form_html:
                        return form_html
                    if "method='post'" not in attrs and 'method="post"' not in attrs and 'method=post' not in attrs:
                        return form_html
                    return form_html + token_field

                body = re.sub(r'<form\b[^>]*>', add_csrf_to_post_form, body, flags=re.IGNORECASE)
                response.set_data(body)
                response.headers['Content-Length'] = str(len(response.get_data()))
        except Exception:
            pass

    if request.method == 'GET' and response.status_code == 200 and 'text/html' in response.headers.get('Content-Type', ''):
        try:
            body = response.get_data(as_text=True)
            if '</body>' in body and 'tmActivityPopupScript' not in body:
                body = body.replace('</body>', """
<script id="tmActivityPopupScript">
(function(){
  let seen = localStorage.getItem('tmActivitySeen') || '';
  function esc(v){return String(v||'').replace(/[&<>\"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c];});}
  function ensure(){
    let box=document.getElementById('tmActivityBubble');
    if(box) return box;
    const style=document.createElement('style');
    style.textContent='.tm-activity-bubble{position:fixed;right:22px;bottom:22px;z-index:9999;width:min(420px,calc(100vw - 32px));background:#0f172a;color:#fff;border:1px solid rgba(125,211,252,.38);border-radius:18px;box-shadow:0 18px 60px rgba(0,0,0,.42);padding:16px;display:none}.tm-activity-bubble.show{display:block}.tm-activity-bubble h3{margin:0 0 6px;font-size:17px}.tm-activity-bubble p{margin:0 0 12px;color:#dbeafe;line-height:1.4}.tm-activity-bubble .row{display:flex;gap:8px;justify-content:flex-end;flex-wrap:wrap}.tm-activity-bubble a,.tm-activity-bubble button{border:0;border-radius:11px;padding:9px 12px;font-weight:900;text-decoration:none;cursor:pointer}.tm-activity-bubble a{background:#2563eb;color:#fff}.tm-activity-bubble button{background:#1e293b;color:#dbeafe}';
    document.head.appendChild(style);
    box=document.createElement('div'); box.id='tmActivityBubble'; box.className='tm-activity-bubble';
    box.innerHTML='<h3></h3><p></p><div class="row"><button type="button">Close</button><a href="/activity">View details</a></div>';
    box.querySelector('button').onclick=function(){box.classList.remove('show');};
    document.body.appendChild(box); return box;
  }
  async function poll(){
    try{
      const res=await fetch('/api/activity?_='+Date.now(),{cache:'no-store'}); if(!res.ok) return;
      const data=await res.json();
      document.querySelectorAll('.message-count,.activity-count').forEach(function(b){ if(data.counts&&data.counts.unread){b.textContent=data.counts.unread;} });
      const item=data.popup; if(!item||!item.id||item.id===seen) return;
      seen=item.id; localStorage.setItem('tmActivitySeen', seen);
      const box=ensure(); box.querySelector('h3').innerHTML=esc(item.title||'TrainerMate update'); box.querySelector('p').innerHTML=esc(item.summary||item.message||''); box.classList.add('show');
      fetch('/activity/'+encodeURIComponent(item.id)+'/read',{method:'POST',headers:{'X-CSRF-Token':'""" + csrf_token() + """'}}).catch(function(){});
    }catch(e){}
  }
  window.addEventListener('load',function(){poll(); setInterval(poll,7000);});
})();
</script>
</body>""")
                response.set_data(body)
                response.headers['Content-Length'] = str(len(response.get_data()))
        except Exception:
            pass
    return response

@app.errorhandler(400)
def handle_bad_request(error):
    description = getattr(error, 'description', '') or 'TrainerMate could not use that request. Please refresh the dashboard and try again.'
    return friendly_error_response('Please refresh and try again', description, 400)

@app.errorhandler(403)
def handle_forbidden(error):
    return friendly_error_response('Open TrainerMate locally', 'For safety, TrainerMate only works from this computer at http://127.0.0.1:5000.', 403)

@app.errorhandler(404)
def handle_not_found(error):
    return friendly_error_response('That page is no longer available', 'The item may have moved, finished, or been refreshed. Go back to TrainerMate and choose it again.', 404)

@app.errorhandler(RequestEntityTooLarge)
@app.errorhandler(413)
def handle_file_too_large(error):
    return friendly_error_response('That file is too large', 'Choose a smaller file and try again. TrainerMate has not changed anything.', 413)

@app.errorhandler(Exception)
def handle_unexpected_error(error):
    if isinstance(error, HTTPException):
        return error
    return friendly_error_response('TrainerMate needs a quick refresh', 'Something unexpected happened, but TrainerMate has not deliberately deleted or overwritten anything. Refresh the dashboard and try again.', 500)

PROVIDER_COLOR_PALETTE = [
    '#2563eb', '#059669', '#d97706', '#dc2626', '#7c3aed',
    '#0891b2', '#be123c', '#4f46e5', '#0d9488', '#9333ea',
]

DOCUMENT_TYPES = [
    ('dbs', 'DBS certificate'),
    ('driving_licence', 'Driving licence'),
    ('adi_badge', 'ADI badge'),
    ('trainer_certificate', 'Trainer certificate'),
    ('insurance', 'Insurance'),
    ('safeguarding', 'Safeguarding'),
    ('first_aid', 'First aid'),
    ('right_to_work', 'Right to work'),
    ('other', 'Other'),
]

DOCUMENT_TYPE_LABELS = dict(DOCUMENT_TYPES)
DOCUMENT_WARNING_DAYS = 92

DEFAULT_PROVIDER_DOCUMENT_REQUIREMENTS = {
    'default': ['dbs', 'driving_licence', 'adi_badge', 'trainer_certificate', 'insurance'],
    'essex': ['dbs', 'driving_licence', 'adi_badge', 'trainer_certificate', 'insurance'],
    'west-mids': ['dbs', 'driving_licence', 'adi_badge', 'trainer_certificate', 'insurance'],
    'lincolnshire': ['dbs', 'driving_licence', 'adi_badge', 'trainer_certificate', 'insurance'],
    'suffolk': ['dbs', 'driving_licence', 'adi_badge', 'trainer_certificate', 'insurance'],
}


def normalize_hex_color(value):
    value = (value or '').strip()
    if len(value) == 7 and value.startswith('#'):
        digits = value[1:]
        if all(ch in '0123456789abcdefABCDEF' for ch in digits):
            return '#' + digits.lower()
    return ''


def default_provider_color(provider_id_or_name):
    slug = provider_slug(provider_id_or_name or 'provider')
    total = sum(ord(ch) for ch in slug)
    return PROVIDER_COLOR_PALETTE[total % len(PROVIDER_COLOR_PALETTE)]


def unique_provider_color(provider_id_or_name, used_colors):
    first = default_provider_color(provider_id_or_name)
    if first not in used_colors:
        return first
    for color in PROVIDER_COLOR_PALETTE:
        if color not in used_colors:
            return color
    return first


def readable_text_color(background):
    color = normalize_hex_color(background) or '#2563eb'
    r = int(color[1:3], 16)
    g = int(color[3:5], 16)
    b = int(color[5:7], 16)
    luminance = (0.299 * r + 0.587 * g + 0.114 * b)
    return '#0f172a' if luminance > 150 else '#ffffff'


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        with path.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path: Path, data):
    """Write JSON safely, including on Windows during bot/dashboard races."""
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    last_exc = None
    for attempt in range(8):
        try:
            with tmp.open('w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, sort_keys=True)
            os.replace(tmp, path)
            return
        except PermissionError as exc:
            last_exc = exc
            try:
                import time
                time.sleep(0.08 * (attempt + 1))
            except Exception:
                pass
        finally:
            try:
                if tmp.exists() and attempt == 7:
                    tmp.unlink()
            except Exception:
                pass
    raise last_exc


# ---------------------------------------------------------------------------
# Paid feature gates, Automatic Sync settings, and Messages & Activity
# ---------------------------------------------------------------------------

def access_features(access=None):
    access = access if isinstance(access, dict) else (check_access(prefer_cached=True) or {})
    features = access.get('features') if isinstance(access.get('features'), dict) else {}
    return features if isinstance(features, dict) else {}


def _flag_enabled(value):
    if isinstance(value, bool):
        return value
    text = str(value or '').strip().lower()
    if text in {'1', 'true', 'yes', 'y', 'on', 'enabled', 'paid'}:
        return True
    if text in {'0', 'false', 'no', 'n', 'off', 'disabled', 'free', ''}:
        return False
    return bool(value)


def feature_enabled(access, name, default=False):
    features = access_features(access)
    aliases = {
        'automatic_sync': ('automatic_sync', 'scheduled_sync', 'automation'),
        'scheduled_sync': ('scheduled_sync', 'automatic_sync', 'automation'),
        'certificate_manage': ('certificate_manage', 'certificate_management', 'certificates_manage'),
        'calendar_sync': ('calendar_sync', 'calendar'),
        'calendar': ('calendar_sync', 'calendar'),
    }
    for key in aliases.get(name, (name,)):
        if key in features:
            return _flag_enabled(features.get(key))
    if name in {'automatic_sync', 'scheduled_sync', 'admin_triggered_sync', 'certificate_manage', 'calendar_sync', 'calendar'}:
        return account_is_paid(access)
    return default


def paid_feature_message(feature_label):
    return (
        f'{feature_label} is included with TrainerMate Paid. '
        'Your providers, Zoom connection, settings and course history are still safe. '
        'Contact support or enter a licence key when you are ready to activate paid features.'
    )


def default_automation_settings():
    return {
        'enabled': False,
        'enable_when_paid': False,
        'daily_enabled': True,
        'daily_time': '06:00',
        'daily_days': 14,
        'weekly_enabled': True,
        'weekly_day': 'sunday',
        'weekly_time': '06:30',
        'weekly_days': PAID_SYNC_WINDOW_DAYS,
        'notifications_enabled': True,
        'notify_problems': True,
        'notify_course_changes': True,
        'notify_success_no_changes': False,
        'notify_support_messages': True,
        'popup_bubbles': True,
        'last_daily_run_at': '',
        'last_weekly_run_at': '',
        'last_activity_id': '',
    }


def load_automation_settings():
    data = load_json(AUTOMATION_SETTINGS_PATH, default_automation_settings())
    out = default_automation_settings()
    if isinstance(data, dict):
        out.update(data)
    try:
        out['daily_days'] = max(1, min(int(out.get('daily_days') or 14), PAID_SYNC_WINDOW_DAYS))
    except Exception:
        out['daily_days'] = 14
    try:
        out['weekly_days'] = max(1, min(int(out.get('weekly_days') or PAID_SYNC_WINDOW_DAYS), PAID_SYNC_WINDOW_DAYS))
    except Exception:
        out['weekly_days'] = PAID_SYNC_WINDOW_DAYS
    return out


def save_automation_settings(settings):
    current = load_automation_settings()
    if isinstance(settings, dict):
        current.update(settings)
    save_json(AUTOMATION_SETTINGS_PATH, current)
    return current


def load_activity_history():
    data = load_json(ACTIVITY_HISTORY_PATH, {'items': []})
    items = data.get('items', []) if isinstance(data, dict) else []
    return [item for item in items if isinstance(item, dict)]


def save_activity_history(items, keep=300):
    clean = [item for item in (items or []) if isinstance(item, dict)]
    save_json(ACTIVITY_HISTORY_PATH, {'items': clean[-keep:]})


def activity_counts(items=None):
    items = load_activity_history() if items is None else items
    active = [item for item in items if not item.get('dismissed_at')]
    return {
        'total': len(active),
        'unread': sum(1 for item in active if not item.get('read_at')),
        'problems': sum(1 for item in active if item.get('severity') in {'warning', 'error'}),
    }


def add_activity_item(kind, title, summary, severity='info', details=None, items=None, source='trainermate', notify=True):
    now = utc_now_text()
    item = {
        'id': f"{provider_slug(kind)}_{uuid.uuid4().hex[:12]}",
        'type': kind,
        'severity': severity,
        'title': title or 'TrainerMate update',
        'summary': summary or '',
        'message': summary or '',
        'created_at': now,
        'read_at': '',
        'dismissed_at': '',
        'source': source,
        'notify': bool(notify),
        'details': details or {},
        'items': items or [],
    }
    history = load_activity_history()
    history.append(item)
    save_activity_history(history)
    return item


def parse_activity_course_key(key, course_state=None):
    text = str(key or '')
    parts = [part.strip() for part in text.split(' | ')]
    provider = parts[0] if len(parts) > 0 else ''
    course_type = parts[1] if len(parts) > 1 else ''
    date_time = parts[2] if len(parts) > 2 else ''
    return {
        'provider': provider,
        'course_type': course_type,
        'date_time': date_time,
        'status': (course_state or {}).get('status') or '',
        'action': (course_state or {}).get('last_action') or '',
        'error': (course_state or {}).get('error') or '',
    }


def build_sync_activity_from_state(state, source='manual'):
    state = state if isinstance(state, dict) else load_app_state()
    summary = state.get('run_summary') if isinstance(state.get('run_summary'), dict) else {}
    courses_state = state.get('courses') if isinstance(state.get('courses'), dict) else {}
    created = int(summary.get('db_created') or 0)
    updated = int(summary.get('db_updated') or 0)
    fobs_updated = int(summary.get('fobs_updated') or 0)
    checked = int(summary.get('courses_processed') or 0) or len(courses_state)
    issues = summary.get('health_issues') if isinstance(summary.get('health_issues'), list) else []
    failed = int(summary.get('fobs_failed') or 0) + int(summary.get('providers_failed') or 0)
    severity = 'warning' if issues or failed else 'info'
    if created or updated or fobs_updated:
        title = 'TrainerMate found updates'
        bits = []
        if created:
            bits.append(f'{created} new course' + ('' if created == 1 else 's'))
        if updated:
            bits.append(f'{updated} course update' + ('' if updated == 1 else 's'))
        if fobs_updated:
            bits.append(f'{fobs_updated} Zoom/FOBS update' + ('' if fobs_updated == 1 else 's'))
        summary_text = '. '.join(bits) + '.'
    elif severity == 'warning':
        title = 'TrainerMate needs attention'
        summary_text = str((issues or ['A provider or Zoom item needs attention.'])[0])[:220]
    else:
        title = 'TrainerMate scan complete'
        summary_text = f'{checked} course' + ('' if checked == 1 else 's') + ' checked. No action needed.'

    detail_items = []
    for key, value in sorted(courses_state.items(), key=lambda item: (item[1] or {}).get('updated_at') or '', reverse=True)[:30]:
        detail_items.append(parse_activity_course_key(key, value if isinstance(value, dict) else {}))
    details = {
        'source': source,
        'courses_checked': checked,
        'new_courses': created,
        'course_updates': updated,
        'zoom_or_fobs_updates': fobs_updated,
        'needs_attention': len(issues) + failed,
        'message': summary.get('message') or '',
        'providers': summary.get('providers') or [],
    }
    return add_activity_item('automatic_sync' if source.startswith('auto') else 'sync', title, summary_text, severity, details=details, items=detail_items, source=source)


def notification_allowed_for_activity(item):
    settings = load_automation_settings()
    if not settings.get('notifications_enabled', True) or not settings.get('popup_bubbles', True):
        return False
    if item.get('type') == 'support_message':
        return bool(settings.get('notify_support_messages', True))
    severity = item.get('severity') or 'info'
    details = item.get('details') if isinstance(item.get('details'), dict) else {}
    if severity in {'warning', 'error'}:
        return bool(settings.get('notify_problems', True))
    changed = bool((details.get('new_courses') or 0) or (details.get('course_updates') or 0) or (details.get('zoom_or_fobs_updates') or 0))
    if changed:
        return bool(settings.get('notify_course_changes', True))
    return bool(settings.get('notify_success_no_changes', False))


def latest_popup_activity():
    for item in reversed(load_activity_history()):
        if item.get('dismissed_at') or item.get('read_at'):
            continue
        if item.get('notify') is False:
            continue
        if notification_allowed_for_activity(item):
            return item
    return None


def mark_activity_read(activity_id):
    items = load_activity_history()
    now = utc_now_text()
    changed = False
    for item in items:
        if item.get('id') == activity_id and not item.get('read_at'):
            item['read_at'] = now
            changed = True
    if changed:
        save_activity_history(items)
    return changed


def dismiss_activity(activity_id):
    items = load_activity_history()
    now = utc_now_text()
    changed = False
    for item in items:
        if item.get('id') == activity_id and not item.get('dismissed_at'):
            item['dismissed_at'] = now
            if not item.get('read_at'):
                item['read_at'] = now
            changed = True
    if changed:
        save_activity_history(items)
    return changed


def date_from_isoish(value):
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00')).date()
    except Exception:
        return None


def weekday_index(name):
    return {'monday':0,'tuesday':1,'wednesday':2,'thursday':3,'friday':4,'saturday':5,'sunday':6}.get(str(name or '').strip().lower(), 6)


def should_run_daily(settings, now=None):
    now = now or datetime.now()
    last = date_from_isoish(settings.get('last_daily_run_at'))
    if last == now.date():
        return False
    hh, mm = 6, 0
    try:
        hh, mm = [int(x) for x in str(settings.get('daily_time') or '06:00').split(':')[:2]]
    except Exception:
        pass
    return now.time() >= now.replace(hour=hh, minute=mm, second=0, microsecond=0).time()


def should_run_weekly(settings, now=None):
    now = now or datetime.now()
    if now.weekday() != weekday_index(settings.get('weekly_day') or 'sunday'):
        return False
    last = date_from_isoish(settings.get('last_weekly_run_at'))
    if last == now.date():
        return False
    hh, mm = 6, 30
    try:
        hh, mm = [int(x) for x in str(settings.get('weekly_time') or '06:30').split(':')[:2]]
    except Exception:
        pass
    return now.time() >= now.replace(hour=hh, minute=mm, second=0, microsecond=0).time()


def start_automation_scheduler():
    if os.getenv('TRAINERMATE_AUTOMATION_SCHEDULER', '1').strip().lower() in {'0','false','no'}:
        return
    def runner():
        time.sleep(env_float('TRAINERMATE_AUTOMATION_START_DELAY', 20.0, minimum=0, maximum=300))
        while True:
            try:
                settings = load_automation_settings()
                access = check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}
                if settings.get('enable_when_paid') and feature_enabled(access, 'automatic_sync') and not settings.get('enabled'):
                    settings = save_automation_settings({'enabled': True, 'enable_when_paid': False})
                    add_activity_item('licence', 'Automatic Sync is now available', 'Your paid plan is active. Automatic Sync has been enabled using your saved settings.', 'info', source='licence')
                if settings.get('enabled') and feature_enabled(access, 'automatic_sync'):
                    state = reconcile_running_state()
                    if not state.get('sync_running'):
                        if settings.get('weekly_enabled') and should_run_weekly(settings):
                            ok, msg = start_sync_process(scan_provider='all', scan_days=settings.get('weekly_days') or PAID_SYNC_WINDOW_DAYS, scan_scope='full_window', bot_mode='scheduled_weekly', source='auto_weekly')
                            if ok:
                                save_automation_settings({'last_weekly_run_at': utc_now_text()})
                        elif settings.get('daily_enabled') and should_run_daily(settings):
                            ok, msg = start_sync_process(scan_provider='all', scan_days=settings.get('daily_days') or 14, scan_scope='smart', bot_mode='urgent_14d', source='auto_daily')
                            if ok:
                                save_automation_settings({'last_daily_run_at': utc_now_text()})
            except Exception:
                pass
            time.sleep(env_float('TRAINERMATE_AUTOMATION_INTERVAL', 60.0, minimum=15, maximum=900))
    threading.Thread(target=runner, daemon=True).start()


def load_provider_catalogue():
    data = load_json(PROVIDER_CATALOGUE_PATH, {'providers': {}, 'excluded': {}})
    providers = data.get('providers', {}) if isinstance(data, dict) else {}
    if not isinstance(providers, dict):
        providers = {}
    cleaned = {}
    for key, entry in providers.items():
        if not isinstance(entry, dict):
            continue
        provider_id = provider_slug(key or entry.get('display_name') or '')
        if not provider_id:
            continue
        login_url = (entry.get('login_url') or '').strip()
        base_url = (entry.get('base_url') or '').strip()
        login_path = (entry.get('login_path') or '/Account/Login').strip() or '/Account/Login'
        if not login_url and base_url:
            login_url = urljoin(base_url.rstrip('/') + '/', login_path.lstrip('/'))
        cleaned[provider_id] = {
            'id': provider_id,
            'name': (entry.get('display_name') or entry.get('name') or provider_id.replace('-', ' ').title()).strip(),
            'login_url': login_url,
            'base_url': base_url,
            'support_status': (entry.get('support_status') or '').strip(),
            'supports_auto_sync': bool(entry.get('supports_auto_sync', bool(login_url))),
            'read_only': bool(entry.get('read_only', False)),
            'provider_manages_zoom': not bool(entry.get('zoom_writes_allowed', True)) or bool(entry.get('read_only', False)),
            'calendar_sync_allowed': bool(entry.get('calendar_sync_allowed', True)),
            'certificate_adapter': normalize_certificate_adapter(entry.get('certificate_adapter') or 'fobs_fastform'),
            'portal_type': normalize_certificate_adapter(entry.get('portal_type') or entry.get('certificate_adapter') or 'fobs_fastform'),
        }
    return cleaned


def provider_presets_for_ui():
    presets = {}
    for key, preset in PROVIDER_PRESETS.items():
        presets[key] = dict(preset)
    for key, entry in load_provider_catalogue().items():
        presets[key] = {
            'name': entry.get('name') or '',
            'login_url': entry.get('login_url') or '',
            'color': default_provider_color(key),
            'provider_manages_zoom': bool(entry.get('provider_manages_zoom')),
            'supports_custom_time': True,
            'never_overwrite_existing_zoom': bool(entry.get('provider_manages_zoom')),
            'certificate_adapter': entry.get('certificate_adapter') or 'fobs_fastform',
            'portal_type': entry.get('portal_type') or 'fobs_fastform',
            'support_status': entry.get('support_status') or '',
            'supports_auto_sync': bool(entry.get('supports_auto_sync')),
            'read_only': bool(entry.get('read_only')),
        }
    presets['manual'] = PROVIDER_PRESETS.get('manual', {'name': '', 'login_url': '', 'color': '#0891b2'})
    return presets


def provider_catalogue_options():
    catalogue = load_provider_catalogue()
    options = [
        {'id': 'manual', 'name': 'Other FOBS provider', 'ready': True, 'status': 'manual'},
    ]
    for key in sorted(catalogue, key=lambda item: catalogue[item].get('name') or item):
        entry = catalogue[key]
        options.append({
            'id': key,
            'name': entry.get('name') or key.replace('-', ' ').title(),
            'ready': bool(entry.get('login_url')),
            'login_url': entry.get('login_url') or '',
            'status': entry.get('support_status') or '',
        })
    return options


def save_zoom_oauth_config(client_id: str, client_secret: str, redirect_uri: str = ''):
    save_json(ZOOM_OAUTH_CONFIG_PATH, {
        'client_id': (client_id or '').strip(),
        'client_secret': (client_secret or '').strip(),
        'redirect_uri': ZOOM_APPROVED_RELAY_URI,
        'saved_at': utc_now_text() if 'utc_now_text' in globals() else ''
    })


def utc_now_text():
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')


def provider_slug(value: str) -> str:
    cleaned = ''.join(ch.lower() if ch.isalnum() else '-' for ch in (value or '').strip())
    while '--' in cleaned:
        cleaned = cleaned.replace('--', '-')
    return cleaned.strip('-') or 'provider'


def derive_courses_url(login_url: str) -> str:
    parsed = urlparse((login_url or '').strip())
    if parsed.scheme != 'https' or not parsed.netloc:
        return ''
    return f'{parsed.scheme}://{parsed.netloc}/Trainer/CoursesBookedOn'


def derive_documents_url(login_url: str) -> str:
    parsed = urlparse((login_url or '').strip())
    if parsed.scheme != 'https' or not parsed.netloc:
        return ''
    return f'{parsed.scheme}://{parsed.netloc}/Trainer/Documents'


def get_identity():
    # Hosted Zoom review environment: keep the trainer account setup complete
    # so reviewers can test the Zoom flow without local/keyring persistence.
    if reviewer_demo_enabled():
        return {
            'ndors': os.getenv('TRAINERMATE_REVIEW_NDORS_ID', '10294').strip() or '10294',
            'email': os.getenv('TRAINERMATE_REVIEW_EMAIL', 'reviewer.us').strip() or 'reviewer.us',
        }
    return {
        'ndors': keyring.get_password('trainermate', 'ndors_id') or '',
        'email': keyring.get_password('trainermate', 'email') or '',
    }


def mask_email(email: str) -> str:
    email = (email or '').strip()
    if '@' not in email:
        return email
    name, domain = email.split('@', 1)
    if len(name) <= 2:
        masked_name = name[:1] + '-' * max(0, len(name) - 1)
    else:
        masked_name = name[:2] + '-' * max(1, len(name) - 2)
    return f'{masked_name}@{domain}'


def get_device_id():
    device_id = keyring.get_password('trainermate', 'device_id')
    if not device_id:
        device_id = str(uuid.uuid4())
        keyring.set_password('trainermate', 'device_id', device_id)
    return device_id


def provider_keyring_service(provider_id: str) -> str:
    # Legacy dashboard keyring service. Kept for backwards compatibility.
    return f'trainermate_provider_{provider_slug(provider_id)}'


def provider_keyring_services(provider_id: str):
    """Return every keyring service that either dashboard or bot may use."""
    pid = provider_slug(provider_id)
    services = [provider_keyring_service(pid)]
    if pid == 'essex':
        services.extend(['essex_portal', 'road_safety_portal'])
    else:
        services.extend([f'road_safety_provider_{pid}', 'road_safety_portal'])

    seen = set()
    ordered = []
    for service in services:
        if service and service not in seen:
            ordered.append(service)
            seen.add(service)
    return ordered


def provider_keyring_accounts(provider_id: str, field: str):
    pid = provider_slug(provider_id)
    normalized = pid.replace('-', '_')
    field = (field or '').strip().lower()
    accounts = [field]
    if pid == 'essex':
        accounts.extend([f'essex_{field}', field])
    else:
        accounts.extend([f'{pid}_{field}', f'{normalized}_{field}', field])

    seen = set()
    ordered = []
    for account in accounts:
        if account and account not in seen:
            ordered.append(account)
            seen.add(account)
    return ordered


def _first_non_empty(*values):
    for value in values:
        if value:
            return value
    return ''


def get_provider_credentials(provider_id: str):
    values = {'username': [], 'password': []}
    for field in ('username', 'password'):
        for service in provider_keyring_services(provider_id):
            for account in provider_keyring_accounts(provider_id, field):
                values[field].append(keyring.get_password(service, account) or '')
    return {
        'username': _first_non_empty(*values['username']),
        'password': _first_non_empty(*values['password']),
    }


def save_provider_credentials(provider_id: str, username: str, password: str):
    clean_username = (username or '').strip()
    clean_password = (password or '').strip()

    # Write the same value to every alias the bot can read, plus the older
    # dashboard alias. This deliberately overwrites stale fallbacks.
    for service in provider_keyring_services(provider_id):
        for account in provider_keyring_accounts(provider_id, 'username'):
            keyring.set_password(service, account, clean_username)
        for account in provider_keyring_accounts(provider_id, 'password'):
            keyring.set_password(service, account, clean_password)


def update_provider_credentials_if_supplied(provider_id: str, username: str, password: str):
    clean_username = (username or '').strip()
    clean_password = (password or '').strip()
    if not clean_username and not clean_password:
        return
    existing = get_provider_credentials(provider_id)
    save_provider_credentials(
        provider_id,
        clean_username or (existing.get('username') or ''),
        clean_password or (existing.get('password') or ''),
    )


def load_app_state():
    default_state = {
        'sync_running': False,
        'stop_requested': False,
        'pid': None,
        'last_status': 'Idle',
        'last_message': '',
        'last_run_status': '',
        'run_summary': {},
        'health_issues': [],
        'courses': {},
        'current_course': '',
        'current_provider': '',
        'last_success_at': '',
        'last_run_finished_at': '',
        'last_run_started_at': '',
    }
    data = load_json(APP_STATE_PATH, default_state.copy())
    if not isinstance(data, dict):
        return default_state.copy()
    out = default_state.copy()
    out.update(data)
    return out


def update_app_state(**kwargs):
    state = load_app_state()
    state.update(kwargs)
    save_json(APP_STATE_PATH, state)
    return state


def load_zoom_accounts():
    data = load_json(ZOOM_ACCOUNTS_PATH, {'accounts': []})
    accounts = data.get('accounts', []) if isinstance(data, dict) else []
    out = []
    seen = set()
    for a in accounts:
        if not isinstance(a, dict):
            continue
        aid = (a.get('id') or '').strip()
        if not aid or aid in seen:
            continue
        out.append({
            'id': aid,
            'email': (a.get('email') or '').strip(),
            'nickname': (a.get('nickname') or '').strip() or (a.get('email') or 'Zoom account'),
            'is_default': bool(a.get('is_default', False)),
            'connected_at': (a.get('connected_at') or '').strip(),
            'last_verified_at': (a.get('last_verified_at') or '').strip(),
            'status': (a.get('status') or 'connected').strip(),
        })
        seen.add(aid)
    if out and not any(a['is_default'] for a in out):
        out[0]['is_default'] = True
    return out


def get_default_zoom_account_id():
    for account in load_zoom_accounts():
        if account.get('is_default'):
            return (account.get('id') or '').strip()
    accounts = load_zoom_accounts()
    return (accounts[0].get('id') or '').strip() if accounts else ''


def has_connected_zoom_account():
    """Return True only when a usable Zoom OAuth account exists."""
    for account in load_zoom_accounts():
        status = (account.get('status') or 'connected').strip().lower()
        if status in {'disconnected', 'needs_reconnect', 'needs reconnect', 'revoked', 'error'}:
            continue
        account_id = (account.get('id') or '').strip()
        if account_id and (get_zoom_oauth_token(account_id, 'access') or get_zoom_oauth_token(account_id, 'refresh')):
            return True
    return False


def zoom_required_sync_message():
    return 'Zoom is not connected yet. Open Zoom accounts and connect Zoom before syncing courses.'


def get_zoom_account_label(account_id=''):
    accounts = load_zoom_accounts()
    selected_id = (account_id or get_default_zoom_account_id() or '').strip()
    selected = next((a for a in accounts if (a.get('id') or '').strip() == selected_id), None)
    if not selected and accounts:
        selected = next((a for a in accounts if a.get('is_default')), None) or accounts[0]
    if not selected:
        return 'Linked Zoom account not selected'
    return (selected.get('nickname') or selected.get('email') or 'Zoom account').strip()

def save_zoom_accounts(accounts):
    cleaned = []
    seen = set()
    default_used = False
    for a in accounts:
        if not isinstance(a, dict):
            continue
        aid = (a.get('id') or '').strip()
        if not aid or aid in seen:
            continue
        is_default = bool(a.get('is_default', False)) and not default_used
        cleaned.append({
            'id': aid,
            'email': (a.get('email') or '').strip(),
            'nickname': (a.get('nickname') or '').strip() or (a.get('email') or 'Zoom account'),
            'is_default': is_default,
            'connected_at': (a.get('connected_at') or '').strip(),
            'last_verified_at': (a.get('last_verified_at') or '').strip(),
            'status': (a.get('status') or 'connected').strip(),
        })
        seen.add(aid)
        if is_default:
            default_used = True
    if cleaned and not default_used:
        cleaned[0]['is_default'] = True
    save_json(ZOOM_ACCOUNTS_PATH, {'accounts': cleaned})
    return cleaned


def set_zoom_tokens(account_id: str, access_token: str, refresh_token: str):
    keyring.set_password('trainermate_zoom_oauth', f'access::{account_id}', access_token)
    keyring.set_password('trainermate_zoom_oauth', f'refresh::{account_id}', refresh_token)
    if reviewer_demo_enabled():
        store = reviewer_token_store_load()
        store.setdefault(account_id, {})
        store[account_id]['access'] = access_token
        store[account_id]['refresh'] = refresh_token
        reviewer_token_store_save(store)


def get_zoom_oauth_token(account_id: str, token_kind: str):
    value = keyring.get_password('trainermate_zoom_oauth', f'{token_kind}::{account_id}') or ''
    if value:
        return value
    if reviewer_demo_enabled():
        return ((reviewer_token_store_load().get(account_id) or {}).get(token_kind) or '')
    return ''


def clear_zoom_tokens(account_id: str):
    for prefix in ('access', 'refresh'):
        try:
            keyring.delete_password('trainermate_zoom_oauth', f'{prefix}::{account_id}')
        except Exception:
            pass
    if reviewer_demo_enabled():
        store = reviewer_token_store_load()
        store.pop(account_id, None)
        reviewer_token_store_save(store)


def upsert_zoom_account(email: str, nickname: str, access_token: str, refresh_token: str):
    accounts = load_zoom_accounts()
    existing = next((a for a in accounts if a['email'].lower() == email.lower()), None)
    now = utc_now_text()
    if existing:
        existing['nickname'] = nickname or existing['nickname'] or email
        existing['last_verified_at'] = now
        existing['status'] = 'connected'
        account_id = existing['id']
    else:
        account_id = provider_slug(email or nickname or str(uuid.uuid4()))
        accounts.append({
            'id': account_id,
            'email': email,
            'nickname': nickname or email or 'Zoom account',
            'is_default': len(accounts) == 0,
            'connected_at': now,
            'last_verified_at': now,
            'status': 'connected',
        })
    save_zoom_accounts(accounts)
    set_zoom_tokens(account_id, access_token, refresh_token)
    return account_id


def make_provider_defaults(name='', login_url='', active=True):
    zoom_accounts = load_zoom_accounts()
    default_zoom = next((a['id'] for a in zoom_accounts if a.get('is_default')), '')
    return {
        'name': name,
        'login_url': login_url,
        'courses_url': derive_courses_url(login_url),
        'documents_url': derive_documents_url(login_url),
        'color': default_provider_color(name),
        'active': active,
        'supports_custom_time': True,
        'zoom_account_id': default_zoom,
        'never_overwrite_existing_zoom': False,
        'provider_manages_zoom': False,
        # Certificate adapter is provider-agnostic so future providers can be added
        # without rewriting the core certificate scan/upload workflow.
        # fobs_fastform handles the current FOBS portals that use DownloadDocument(id).
        'certificate_adapter': 'fobs_fastform',
        'portal_type': 'fobs_fastform',
        'read_only': False,
        'calendar_sync_allowed': True,
        'credentials': {'username': '', 'password': ''},
    }


def load_providers():
    data = load_json(PROVIDERS_PATH, {'providers': []})
    raw = data.get('providers', []) if isinstance(data, dict) else []
    defaults = make_provider_defaults()
    out = []
    seen = set()
    used_default_colors = set()
    catalogue = load_provider_catalogue()
    for p in raw:
        if not isinstance(p, dict):
            continue
        item = defaults.copy()
        item.update(p)
        item['id'] = provider_slug(item.get('id') or item.get('name') or 'provider')
        if item['id'] in seen:
            continue
        item['name'] = (item.get('name') or item['id']).strip()
        item['login_url'] = (item.get('login_url') or '').strip()
        item['courses_url'] = (item.get('courses_url') or derive_courses_url(item['login_url'])).strip()
        item['documents_url'] = (item.get('documents_url') or derive_documents_url(item['login_url'])).strip()
        item['certificate_adapter'] = normalize_certificate_adapter(item.get('certificate_adapter') or item.get('portal_type') or '')
        item['portal_type'] = item['certificate_adapter']
        explicit_color = normalize_hex_color(p.get('color'))
        item['color'] = explicit_color or unique_provider_color(item['id'], used_default_colors)
        used_default_colors.add(item['color'])
        item['credentials'] = get_provider_credentials(item['id'])
        catalogue_entry = catalogue.get(item['id']) or {}
        if 'read_only' not in p:
            item['read_only'] = bool(catalogue_entry.get('read_only', False))
        else:
            item['read_only'] = bool(item.get('read_only', False))
        if 'calendar_sync_allowed' not in p:
            item['calendar_sync_allowed'] = bool(catalogue_entry.get('calendar_sync_allowed', True))
        else:
            item['calendar_sync_allowed'] = bool(item.get('calendar_sync_allowed', True))
        item['provider_manages_zoom'] = bool(item.get('provider_manages_zoom', False))
        if item['read_only']:
            item['provider_manages_zoom'] = True
        item['never_overwrite_existing_zoom'] = item['provider_manages_zoom']
        out.append(item)
        seen.add(item['id'])
    return out


def save_providers(providers):
    cleaned = []
    seen = set()
    for p in providers:
        if not isinstance(p, dict):
            continue
        name = (p.get('name') or '').strip()
        if not name:
            continue
        pid = provider_slug(p.get('id') or name)
        if pid in seen:
            continue
        login_url = (p.get('login_url') or '').strip()
        read_only = bool(p.get('read_only', False))
        provider_manages_zoom = bool(p.get('provider_manages_zoom', False)) or read_only
        provider_color = normalize_hex_color(p.get('color')) or default_provider_color(pid)
        cleaned.append({
            'id': pid,
            'name': name,
            'login_url': login_url,
            'courses_url': (p.get('courses_url') or derive_courses_url(login_url)).strip(),
            'documents_url': (p.get('documents_url') or derive_documents_url(login_url)).strip(),
            'color': provider_color,
            'active': bool(p.get('active', True)),
            'supports_custom_time': bool(p.get('supports_custom_time', True)),
            'zoom_mode': 'provider_managed' if provider_manages_zoom else 'linked_account',
            'zoom_account_id': (p.get('zoom_account_id') or '').strip(),
            'never_overwrite_existing_zoom': provider_manages_zoom,
            'provider_manages_zoom': provider_manages_zoom,
            'certificate_adapter': normalize_certificate_adapter(p.get('certificate_adapter') or p.get('portal_type') or ''),
            'portal_type': normalize_certificate_adapter(p.get('portal_type') or p.get('certificate_adapter') or ''),
            'read_only': read_only,
            'calendar_sync_allowed': bool(p.get('calendar_sync_allowed', True)),
            'last_login_test_status': (p.get('last_login_test_status') or '').strip(),
            'last_login_test_message': (p.get('last_login_test_message') or '').strip(),
            'last_login_test_at': (p.get('last_login_test_at') or '').strip(),
            'paused_for_login': bool(p.get('paused_for_login', False)),
        })
        seen.add(pid)
    save_json(PROVIDERS_PATH, {'providers': cleaned})
    return cleaned


def add_provider_record(form):
    providers = load_providers()
    preset = (form.get('provider_preset') or '').strip()
    preset_data = provider_presets_for_ui().get(preset, {})
    name = (form.get('provider_name') or preset_data.get('name') or '').strip()
    login_url = (form.get('login_url') or preset_data.get('login_url') or '').strip()
    username = (form.get('provider_username') or '').strip()
    password = (form.get('provider_password') or '').strip()
    if not preset:
        return False, 'Choose a provider.', None
    if not name:
        return False, 'Enter a provider name.', None
    if not username or not password:
        return False, f'Enter the FOBS username and password for {name}.', None
    pid = provider_slug(name)
    if any(p['id'] == pid for p in providers):
        return False, 'A provider with that name already exists.', None
    if not derive_courses_url(login_url):
        return False, 'Enter a valid HTTPS FOBS login URL.', None
    provider = {
        'id': pid,
        'name': name,
        'login_url': login_url,
        'courses_url': derive_courses_url(login_url),
        'documents_url': derive_documents_url(login_url),
        'color': normalize_hex_color(form.get('provider_color')) or default_provider_color(pid),
        'active': form.get('active') == '1' if 'active' in form else True,
        'supports_custom_time': form.get('supports_custom_time') == '1' if 'supports_custom_time' in form else True,
        'zoom_account_id': (form.get('zoom_account_id') or '').strip(),
        'provider_manages_zoom': bool(form.get('provider_manages_zoom')),
        'never_overwrite_existing_zoom': bool(form.get('provider_manages_zoom')),
        'certificate_adapter': normalize_certificate_adapter(form.get('certificate_adapter') or preset_data.get('certificate_adapter') or ''),
        'portal_type': normalize_certificate_adapter(form.get('portal_type') or preset_data.get('portal_type') or ''),
        'read_only': bool(preset_data.get('read_only', False)),
        'calendar_sync_allowed': bool(preset_data.get('calendar_sync_allowed', True)),
    }
    if provider['read_only'] or provider['provider_manages_zoom']:
        provider['provider_manages_zoom'] = True
        provider['never_overwrite_existing_zoom'] = True
    return True, 'Provider added.', provider


def update_provider_record(provider_id, form):
    providers = load_providers()
    updated = False
    for provider in providers:
        if provider['id'] != provider_id:
            continue
        login_url = (form.get('login_url') or provider.get('login_url') or '').strip()
        if not derive_courses_url(login_url):
            return False, 'Enter a valid HTTPS FOBS login URL.'
        provider['login_url'] = login_url
        provider['courses_url'] = derive_courses_url(login_url)
        provider['documents_url'] = derive_documents_url(login_url)
        provider['active'] = bool(form.get('active'))
        provider['supports_custom_time'] = bool(form.get('supports_custom_time'))
        provider['provider_manages_zoom'] = bool(form.get('provider_manages_zoom'))
        if provider.get('read_only'):
            provider['provider_manages_zoom'] = True
        if provider.get('paused_for_login'):
            provider['active'] = False
        provider['zoom_account_id'] = (form.get('zoom_account_id') or '').strip()
        provider['color'] = normalize_hex_color(form.get('provider_color')) or provider.get('color') or default_provider_color(provider['id'])
        provider['never_overwrite_existing_zoom'] = provider['provider_manages_zoom']
        provider['certificate_adapter'] = normalize_certificate_adapter(form.get('certificate_adapter') or provider.get('certificate_adapter') or provider.get('portal_type') or '')
        provider['portal_type'] = provider['certificate_adapter']
        update_provider_credentials_if_supplied(provider_id, form.get('provider_username') or '', form.get('provider_password') or '')
        updated = True
        break
    if not updated:
        return False, 'Provider not found.'
    save_providers(providers)
    return True, 'Provider updated.'


def delete_provider_record(provider_id):
    providers = load_providers()
    kept = [p for p in providers if p['id'] != provider_id]
    if len(kept) == len(providers):
        return False, 'Provider not found.'
    save_providers(kept)
    return True, 'Provider deleted. No sync will take place for this provider until it is added again.'


def provider_login_problem_text(page):
    try:
        text = page.locator('body').inner_text(timeout=2500)
    except Exception:
        text = ''
    lower = (text or '').lower()
    friendly = [
        ('invalid', 'Login failed - please check the username and password.'),
        ('incorrect', 'Login failed - please check the username and password.'),
        ('failed', 'Login failed - please check the username and password.'),
        ('not recognised', 'Login failed - please check the username and password.'),
        ('locked', 'This provider account may be locked. Please check with the provider.'),
        ('disabled', 'This provider account may be disabled. Please check with the provider.'),
    ]
    for needle, message in friendly:
        if needle in lower:
            return message
    return 'Login failed - please check the username and password.'


def provider_login_screen_visible(page):
    try:
        return bool(page.locator('#UserName').count() and page.locator('#Password').count())
    except Exception:
        return False


def test_provider_login_once(provider, username='', password=''):
    provider_name = (provider.get('name') or 'provider').strip()
    login_url = (provider.get('login_url') or '').strip()
    username = (username or '').strip()
    password = (password or '').strip()
    if sync_playwright is None:
        return False, 'TrainerMate cannot test logins because the browser helper is not installed.'
    if not login_url:
        return False, f'{provider_name} needs a FOBS login address before TrainerMate can test it.'
    if not username or not password:
        return False, f'Enter the FOBS username and password for {provider_name}, then try again.'

    browser = None
    playwright = None
    try:
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(login_url, wait_until='domcontentloaded', timeout=25000)
        page.wait_for_selector('#UserName', timeout=15000)
        page.wait_for_selector('#Password', timeout=15000)
        page.fill('#UserName', username)
        page.fill('#Password', password)
        submit = page.locator("button[type='submit'], input[type='submit']").first
        submit.click(timeout=8000)
        try:
            page.wait_for_load_state('domcontentloaded', timeout=12000)
        except Exception:
            pass
        page.wait_for_timeout(1800)
        if provider_login_screen_visible(page):
            return False, provider_login_problem_text(page)
        current_url = (page.url or '').lower()
        if '/account/login' in current_url:
            return False, provider_login_problem_text(page)
        return True, f'Login successful for {provider_name}.'
    except Exception as exc:
        message = str(exc).lower()
        if 'timeout' in message:
            return False, f'{provider_name} did not respond in time. Please try again later.'
        return False, f'{provider_name} could not be reached. Please try again later.'
    finally:
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            if playwright:
                playwright.stop()
        except Exception:
            pass


def update_provider_login_test_status(provider_id, status, message):
    providers = load_providers()
    updated = False
    for provider in providers:
        if provider.get('id') != provider_id:
            continue
        provider['last_login_test_status'] = status
        provider['last_login_test_message'] = message
        provider['last_login_test_at'] = utc_now_text()
        if status == 'ok':
            provider['paused_for_login'] = False
            provider['active'] = True
        elif status == 'failed':
            provider['paused_for_login'] = True
            provider['active'] = False
        updated = True
        break
    if updated:
        save_providers(providers)


def sync_provider_login_failures_from_state():
    state = load_app_state()
    summary = state.get('run_summary') if isinstance(state.get('run_summary'), dict) else {}
    providers = load_providers()
    changed = False
    by_id = {provider.get('id'): provider for provider in providers}
    for item in summary.get('providers') or []:
        if not isinstance(item, dict):
            continue
        if (item.get('status') or '').strip() != 'login_failed':
            continue
        provider_id = provider_slug(item.get('id') or item.get('name') or '')
        provider = by_id.get(provider_id)
        if not provider:
            continue
        provider_name = provider.get('name') or item.get('name') or 'this provider'
        provider['last_login_test_status'] = 'failed'
        provider['last_login_test_message'] = (
            f'TrainerMate can no longer log in to {provider_name}. '
            'If the FOBS password has changed, reconfirm it here before syncing again.'
        )
        provider['last_login_test_at'] = utc_now_text()
        provider['paused_for_login'] = True
        provider['active'] = False
        changed = True
    if changed:
        save_providers(providers)


def setup_provider_rows(existing_providers=None):
    existing = {provider.get('id'): provider for provider in (existing_providers or load_providers())}
    rows = []
    for option in provider_catalogue_options():
        if option.get('id') == 'manual':
            continue
        preset = provider_presets_for_ui().get(option.get('id') or '', {})
        provider = existing.get(option.get('id'))
        ready = bool(preset.get('login_url'))
        rows.append({
            'id': option.get('id'),
            'name': option.get('name'),
            'ready': ready,
            'configured': bool(provider),
            'username': ((provider or {}).get('credentials') or {}).get('username') or '',
            'has_password': bool(((provider or {}).get('credentials') or {}).get('password')),
            'read_only': bool(preset.get('read_only') or (provider or {}).get('read_only')),
            'last_login_test_status': (provider or {}).get('last_login_test_status') or '',
            'last_login_test_message': (provider or {}).get('last_login_test_message') or '',
        })
    return rows


def save_setup_providers(form):
    selected = {provider_slug(value) for value in form.getlist('setup_provider') if provider_slug(value)}
    if not selected:
        return False, 'Choose at least one provider.'

    presets = provider_presets_for_ui()
    providers = load_providers()
    by_id = {provider.get('id'): provider for provider in providers}
    added = 0
    updated = 0
    skipped = []
    missing_login = []
    for provider_id in selected:
        preset = presets.get(provider_id, {})
        login_url = (preset.get('login_url') or '').strip()
        name = (preset.get('name') or provider_id.replace('-', ' ').title()).strip()
        if not login_url:
            skipped.append(name)
            continue
        username = form.get(f'username_{provider_id}') or ''
        password = form.get(f'password_{provider_id}') or ''
        existing_provider = by_id.get(provider_id) or {}
        existing_creds = existing_provider.get('credentials') or {}
        if not ((username or '').strip() or (existing_creds.get('username') or '').strip()) or not ((password or '').strip() or (existing_creds.get('password') or '').strip()):
            missing_login.append(name)
            continue
        if provider_id in by_id:
            provider = by_id[provider_id]
            provider['active'] = True
            provider['name'] = provider.get('name') or name
            provider['login_url'] = provider.get('login_url') or login_url
            provider['courses_url'] = provider.get('courses_url') or derive_courses_url(login_url)
            provider['documents_url'] = provider.get('documents_url') or derive_documents_url(login_url)
            provider['read_only'] = bool(provider.get('read_only') or preset.get('read_only', False))
            if provider['read_only']:
                provider['provider_manages_zoom'] = True
            update_provider_credentials_if_supplied(provider_id, username, password)
            updated += 1
            continue
        provider = make_provider_defaults(name, login_url, True)
        provider.update({
            'id': provider_id,
            'name': name,
            'login_url': login_url,
            'courses_url': derive_courses_url(login_url),
            'documents_url': derive_documents_url(login_url),
            'color': default_provider_color(provider_id),
            'active': True,
            'provider_manages_zoom': bool(preset.get('provider_manages_zoom', False) or preset.get('read_only', False)),
            'never_overwrite_existing_zoom': bool(preset.get('provider_manages_zoom', False) or preset.get('read_only', False)),
            'certificate_adapter': normalize_certificate_adapter(preset.get('certificate_adapter') or 'fobs_fastform'),
            'portal_type': normalize_certificate_adapter(preset.get('portal_type') or 'fobs_fastform'),
            'read_only': bool(preset.get('read_only', False)),
            'calendar_sync_allowed': bool(preset.get('calendar_sync_allowed', True)),
        })
        providers.append(provider)
        save_provider_credentials(provider_id, username, password)
        added += 1

    if added or updated:
        save_providers(providers)
    if missing_login and not (added or updated):
        return False, f'Enter the FOBS username and password for {", ".join(missing_login[:3])}.'
    if missing_login:
        message_extra = f' {len(missing_login)} provider(s) still need a username and password.'
    else:
        message_extra = ''
    if skipped and not (added or updated):
        return False, 'Those providers are not ready in TrainerMate yet. Choose a ready provider or use Manage providers.'
    message = f'Provider setup saved. {added} added, {updated} updated.' + message_extra
    if skipped:
        message += f' {len(skipped)} provider(s) still need setup details before they can be added.'
    return True, message


def load_cached_access():
    data = load_json(ACCESS_CACHE_PATH, {})
    return data if isinstance(data, dict) else {}


def save_cached_access(access):
    if isinstance(access, dict) and access:
        save_json(ACCESS_CACHE_PATH, {'checked_at': utc_now_text(), 'access': access})


def get_cached_access_if_fresh(max_age_seconds=ACCESS_CACHE_MAX_AGE_SECONDS):
    cached = load_cached_access()
    checked_at = (cached.get('checked_at') or '').strip()
    access = cached.get('access') if isinstance(cached.get('access'), dict) else None
    if not checked_at or not access:
        return None
    try:
        checked = datetime.strptime(checked_at, '%Y-%m-%dT%H:%M:%SZ')
        age = (datetime.utcnow() - checked).total_seconds()
        if age <= max_age_seconds:
            return access
    except Exception:
        return None
    return None


def review_paid_access():
    return {
        'allowed': True,
        'paid': True,
        'is_paid': True,
        'plan': 'paid',
        'tier': 'paid',
        'features': {
            'sync_window_days': PAID_SYNC_WINDOW_DAYS,
            'automatic_sync': True,
            'scheduled_sync': True,
            'calendar_sync': True,
            'calendar': True,
            'certificate_manage': True,
            'certificate_management': True,
        },
    }


def check_access(timeout_seconds=ACTION_ACCESS_TIMEOUT_SECONDS, prefer_cached=False):
    if reviewer_demo_enabled():
        access = review_paid_access()
        try:
            save_cached_access(access)
        except Exception:
            pass
        return access
    identity = get_identity()
    if not identity['ndors'].strip():
        return None
    if prefer_cached:
        fresh = get_cached_access_if_fresh()
        if fresh:
            return fresh
    payload = {'ndors_trainer_id': identity['ndors'].strip(), 'email': identity['email'].strip() or None, 'device_id': get_device_id(), 'device_name': 'desktop', 'app_version': APP_VERSION}
    try:
        r = requests.post(f'{API_URL}/check-access', json=payload, timeout=timeout_seconds)
        r.raise_for_status()
        data = r.json()
        save_cached_access(data)
        return data
    except Exception:
        cached = load_cached_access().get('access')
        return cached if isinstance(cached, dict) else None


def force_refresh_licence_from_admin(reason='admin_update'):
    """Bypass cached access after an admin licence change and record a gentle activity item."""
    cached = load_cached_access()
    before = cached.get('access') if isinstance(cached, dict) and isinstance(cached.get('access'), dict) else {}
    before_plan = (before.get('plan') if isinstance(before, dict) else '') or ''
    access = check_access(timeout_seconds=ACTION_ACCESS_TIMEOUT_SECONDS, prefer_cached=False) or {}
    save_cached_access(access)
    after_plan = (access.get('plan') or 'free') if isinstance(access, dict) else 'free'
    features = access.get('features') if isinstance(access, dict) and isinstance(access.get('features'), dict) else {}
    title = 'Your TrainerMate plan has been updated'
    if before_plan and before_plan != after_plan:
        msg = f'Your plan changed from {before_plan.title()} to {after_plan.title()}. TrainerMate has updated the features available on this computer.'
    else:
        msg = f'Your current plan is {after_plan.title()}. TrainerMate has refreshed the features available on this computer.'
    try:
        add_activity_item('licence', title, msg, 'info', details={'reason': reason, 'access': {'plan': after_plan, 'features': features}}, source='licence')
    except Exception:
        pass
    return access

def update_notice_from_access(access):
    update = access.get('update') if isinstance(access, dict) else {}
    if not isinstance(update, dict):
        return ''
    latest = update.get('latest_version') or ''
    if update.get('update_required'):
        return f"TrainerMate update required before syncing. Latest version: {latest or 'available'}."
    if update.get('update_available'):
        return f"TrainerMate update available: {latest or 'new version'}."
    return ''


def today_start_text():
    return datetime.now().strftime('%Y-%m-%d 00:00')


def ensure_courses_sync_columns(conn):
    """Keep dashboard compatible with older courses.db files."""
    for alter_sql in (
        'ALTER TABLE courses ADD COLUMN last_synced_at TEXT',
        'ALTER TABLE courses ADD COLUMN last_sync_status TEXT',
        'ALTER TABLE courses ADD COLUMN last_sync_action TEXT',
        'ALTER TABLE courses ADD COLUMN fobs_course_url TEXT',
    ):
        try:
            conn.execute(alter_sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass


def ensure_document_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trainer_documents (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            document_type TEXT NOT NULL,
            original_filename TEXT,
            stored_filename TEXT,
            file_path TEXT,
            issue_date TEXT,
            expiry_date TEXT,
            notes TEXT,
            status TEXT DEFAULT 'active',
            created_at TEXT,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS document_provider_links (
            id TEXT PRIMARY KEY,
            document_id TEXT NOT NULL,
            provider_id TEXT NOT NULL,
            provider_name TEXT NOT NULL,
            provider_status TEXT DEFAULT 'not_checked',
            provider_file_name TEXT,
            provider_checked_at TEXT,
            pending_action TEXT DEFAULT '',
            last_synced_at TEXT,
            notes TEXT,
            updated_at TEXT,
            UNIQUE(document_id, provider_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS provider_certificates (
            id TEXT PRIMARY KEY,
            provider_id TEXT NOT NULL,
            provider_name TEXT NOT NULL,
            certificate_name TEXT,
            detected_type TEXT,
            expiry_date TEXT,
            uploaded_at TEXT,
            source_url TEXT,
            provider_reference TEXT,
            raw_columns TEXT,
            download_url TEXT,
            cached_filename TEXT,
            file_hash TEXT,
            file_size INTEGER,
            content_type TEXT,
            downloaded_at TEXT,
            download_status TEXT,
            encryption TEXT,
            status TEXT DEFAULT 'seen',
            last_seen_at TEXT,
            updated_at TEXT
        )
    """)
    for alter_sql in (
        'ALTER TABLE provider_certificates ADD COLUMN source_url TEXT',
        'ALTER TABLE provider_certificates ADD COLUMN raw_columns TEXT',
        'ALTER TABLE provider_certificates ADD COLUMN download_url TEXT',
        'ALTER TABLE provider_certificates ADD COLUMN cached_filename TEXT',
        'ALTER TABLE provider_certificates ADD COLUMN file_hash TEXT',
        'ALTER TABLE provider_certificates ADD COLUMN file_size INTEGER',
        'ALTER TABLE provider_certificates ADD COLUMN content_type TEXT',
        'ALTER TABLE provider_certificates ADD COLUMN downloaded_at TEXT',
        'ALTER TABLE provider_certificates ADD COLUMN download_status TEXT',
        'ALTER TABLE provider_certificates ADD COLUMN encryption TEXT',
    ):
        try:
            conn.execute(alter_sql)
        except sqlite3.OperationalError:
            pass
    conn.commit()


def documents_conn():
    conn = sqlite3.connect(str(COURSES_DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_document_tables(conn)
    return conn


def set_certificate_scan_status(provider_id, status, message, detail=''):
    pid = provider_slug(provider_id or 'all')
    with CERTIFICATE_SCAN_STATUS_LOCK:
        CERTIFICATE_SCAN_STATUS[pid] = {
            'status': status,
            'message': message,
            'detail': detail,
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }


def get_certificate_scan_status(provider_id='all'):
    pid = provider_slug(provider_id or 'all')
    with CERTIFICATE_SCAN_STATUS_LOCK:
        return CERTIFICATE_SCAN_STATUS.get(pid, {
            'status': 'idle',
            'message': 'Certificate scan has not run yet.',
            'detail': '',
            'updated_at': '',
        })


def certificate_scan_snapshot():
    with CERTIFICATE_SCAN_STATUS_LOCK:
        items = {pid: dict(state) for pid, state in CERTIFICATE_SCAN_STATUS.items()}
    running = any((state.get('status') or '').lower() == 'running' for state in items.values())
    latest_all = items.get('all') if isinstance(items.get('all'), dict) else {}
    if (latest_all.get('status') or '').lower() in {'complete', 'idle', 'error'}:
        all_updated = latest_all.get('updated_at') or ''
        # Once the aggregate job has finished, ignore older per-provider
        # running states that were only progress messages from that job.
        running = any(
            (state.get('status') or '').lower() == 'running'
            and (state.get('updated_at') or '') > all_updated
            for pid, state in items.items()
            if pid != 'all'
        )
    latest = None
    for pid, state in items.items():
        candidate = dict(state)
        candidate['provider_id'] = pid
        if latest is None or (candidate.get('updated_at') or '') >= (latest.get('updated_at') or ''):
            latest = candidate
    latest = latest or {
        'provider_id': 'all',
        'status': 'idle',
        'message': 'No certificate refresh running.',
        'detail': 'Refresh FOBS certificates to check provider certificates.',
        'updated_at': '',
    }
    rows = []
    for pid, state in sorted(items.items(), key=lambda item: item[1].get('updated_at') or '', reverse=True)[:8]:
        rows.append({
            'provider_id': pid,
            'status': state.get('status') or 'idle',
            'message': state.get('message') or '',
            'detail': state.get('detail') or '',
            'updated_at': state.get('updated_at') or '',
        })
    return {'running': running, 'latest': latest, 'rows': rows}


def certificate_job_running():
    return bool(certificate_scan_snapshot().get('running'))


def provider_delete_cancel_key(certificate_id):
    return str(certificate_id or '').strip()


def request_provider_delete_cancel(certificate_id):
    key = provider_delete_cancel_key(certificate_id)
    if not key:
        return False
    with PROVIDER_DELETE_CANCEL_LOCK:
        PROVIDER_DELETE_CANCEL_REQUESTS.add(key)
    return True


def clear_provider_delete_cancel(certificate_id):
    key = provider_delete_cancel_key(certificate_id)
    if not key:
        return
    with PROVIDER_DELETE_CANCEL_LOCK:
        PROVIDER_DELETE_CANCEL_REQUESTS.discard(key)


def provider_delete_cancel_requested(certificate_id):
    key = provider_delete_cancel_key(certificate_id)
    if not key:
        return False
    with PROVIDER_DELETE_CANCEL_LOCK:
        return key in PROVIDER_DELETE_CANCEL_REQUESTS


def set_startup_zoom_health_status(status, message, detail=''):
    with STARTUP_ZOOM_HEALTH_CHECK_LOCK:
        STARTUP_ZOOM_HEALTH_CHECK_STATUS.update({
            'status': status,
            'message': message,
            'detail': detail,
            'updated_at': utc_now_text(),
        })


def get_startup_zoom_health_status():
    with STARTUP_ZOOM_HEALTH_CHECK_LOCK:
        return dict(STARTUP_ZOOM_HEALTH_CHECK_STATUS)


def parse_fobs_date(value):
    text = ' '.join((value or '').replace('\xa0', ' ').split()).strip()
    if not text:
        return ''
    for pattern in (r'\b\d{1,2}/\d{1,2}/\d{4}\b', r'\b\d{4}-\d{1,2}-\d{1,2}\b'):
        m = re.search(pattern, text)
        if not m:
            continue
        token = m.group(0)
        for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
            try:
                return datetime.strptime(token, fmt).strftime('%Y-%m-%d')
            except Exception:
                pass
    return ''


def detect_certificate_type(text):
    lower = (text or '').lower()
    checks = (
        ('dbs', 'DBS certificate'),
        ('licence', 'Driving licence'),
        ('license', 'Driving licence'),
        ('adi', 'ADI badge'),
        ('insurance', 'Insurance'),
        ('safeguard', 'Safeguarding'),
        ('first aid', 'First aid'),
        ('teaching', 'Teaching and learning certificate'),
        ('learning', 'Teaching and learning certificate'),
        ('right to work', 'Right to work'),
    )
    for needle, label in checks:
        if needle in lower:
            return label
    return ''


CERTIFICATE_ADAPTER_DEFAULT = 'fobs_fastform'
CERTIFICATE_ADAPTERS = {
    'fobs_fastform': {
        'label': 'FOBS / FastForm',
        'row_parser': 'fobs_certificates_section',
        'download_strategy': ('download_document_id', 'row_click', 'direct_url'),
        'supports_upload': True,
        'supports_delete': True,
    },
    'generic_html': {
        'label': 'Generic provider portal',
        'row_parser': 'fobs_certificates_section',
        'download_strategy': ('direct_url', 'row_click'),
        'supports_upload': False,
        'supports_delete': False,
    },
}


def normalize_certificate_adapter(value):
    adapter = provider_slug(value or CERTIFICATE_ADAPTER_DEFAULT).replace('-', '_')
    if adapter in {'fobs', 'fastform', 'fobs_fast_form'}:
        adapter = 'fobs_fastform'
    if adapter not in CERTIFICATE_ADAPTERS:
        adapter = CERTIFICATE_ADAPTER_DEFAULT
    return adapter


def provider_certificate_adapter(provider):
    """Return the certificate adapter config for this provider.

    New providers should set certificate_adapter/portal_type in providers.json.
    The core certificate workflow should call this instead of hardcoding Essex,
    West Mids, Lincolnshire, or any future provider name.
    """
    adapter_id = normalize_certificate_adapter(
        (provider or {}).get('certificate_adapter') or (provider or {}).get('portal_type') or ''
    )
    config = dict(CERTIFICATE_ADAPTERS.get(adapter_id) or CERTIFICATE_ADAPTERS[CERTIFICATE_ADAPTER_DEFAULT])
    config['id'] = adapter_id
    return config


def certificate_rows_from_provider_page(page, provider):
    adapter = provider_certificate_adapter(provider)
    # Current supported providers are FOBS/FastForm, but this wrapper is the
    # extension point for future portals with different certificate layouts.
    if adapter.get('row_parser') == 'fobs_certificates_section':
        return certificate_rows_from_fobs_page(page)
    return certificate_rows_from_fobs_page(page)


def provider_certificate_reference(provider, joined_text, download_document_id=''):
    provider_id = provider_slug((provider or {}).get('id') or (provider or {}).get('name') or 'provider')
    doc_id = re.sub(r'\D+', '', str(download_document_id or ''))
    if doc_id:
        return f'{provider_id}-document-{doc_id}'
    ref_seed = joined_text or str(uuid.uuid4())
    return hashlib.sha256(ref_seed.encode('utf-8', errors='ignore')).hexdigest()[:24]


def certificate_rows_from_fobs_page(page):
    """Return rows from FOBS sections explicitly headed Certificates only."""
    return page.evaluate("""
        () => {
            const clean = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim();
            const lower = (value) => clean(value).toLowerCase();
            const isVisible = (el) => {
                if (!el) return false;
                const style = window.getComputedStyle(el);
                const rect = el.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
            };
            const isCertificateHeading = (text) => {
                const value = lower(text);
                if (!value) return false;
                if (/(invoice|invoices|document|documents|allocation|allocations|message|messages)/.test(value)) return false;
                return /(^|\\b)certificates?(\\b|$)/.test(value);
            };
            const isExcludedSectionText = (text) => {
                const value = lower(text);
                return /(^|\\b)(invoice|invoices|document|documents|allocation|allocations|course presenter account|updated course details|course removal)(\\b|$)/.test(value)
                    && !/(^|\\b)certificates?(\\b|$)/.test(value);
            };
            const looksLikeCertificateRow = (values) => {
                const text = lower(values.join(' | '));
                if (!text || /(invoice|invoices|course allocation|allocation|message|messages|receipt|payment|course presenter account|updated course details|course removal)/.test(text)) return false;
                return /(dbs|disclosure|barring|licen[cs]e|driving|adi|insurance|safeguard|first aid|teaching|learning|right to work|certificate|certification|qualification|gdpr|data protection)/.test(text);
            };
            const sectionContainers = [
                'section', 'article', '.card', '.panel', '.box', '.tab-pane',
                '.accordion-item', '.collapse', '.content', '.form-group', 'fieldset',
                '.well', '.widget', '.portlet', '.container', '.row'
            ];
            const headingSelectors = 'h1,h2,h3,h4,h5,h6,legend,caption,.card-header,.panel-heading,.panel-title,.accordion-header,.tab-title,.nav-link,a,button';
            const containers = [];
            const seen = new Set();

            const addContainer = (container) => {
                if (!container || seen.has(container)) return;
                seen.add(container);
                containers.push(container);
            };
            const absoluteUrl = (href) => {
                try { return new URL(href, window.location.href).href; } catch (err) { return ''; }
            };
            const rowPayload = (el, values) => {
                const links = Array.from(el.querySelectorAll('a[href],[onclick]')).map((a) => {
                    const onclick = a.getAttribute('onclick') || '';
                    const downloadMatch = onclick.match(/DownloadDocument\\s*\\(\\s*['\"]?(\\d+)['\"]?\\s*\\)/i);
                    return {
                        text: clean(a.innerText || a.textContent || a.getAttribute('title') || ''),
                        href: absoluteUrl(a.getAttribute('href') || ''),
                        onclick,
                        download_document_id: downloadMatch ? downloadMatch[1] : ''
                    };
                }).filter((link) => (link.href && !/^javascript:/i.test(link.href)) || link.download_document_id);
                const rowOnclick = el.getAttribute && (el.getAttribute('onclick') || '');
                const rowDownloadMatch = rowOnclick.match(/DownloadDocument\\s*\\(\\s*['\"]?(\\d+)['\"]?\\s*\\)/i);
                if (rowDownloadMatch) {
                    links.unshift({
                        text: clean(el.innerText || el.textContent || ''),
                        href: '',
                        onclick: rowOnclick,
                        download_document_id: rowDownloadMatch[1]
                    });
                }
                return {values, links};
            };

            const addNearbyFastFormTable = (heading) => {
                let current = heading;
                for (let depth = 0; current && depth < 4; depth += 1, current = current.parentElement) {
                    let sibling = current.nextElementSibling;
                    for (let hop = 0; sibling && hop < 6; hop += 1, sibling = sibling.nextElementSibling) {
                        const siblingText = clean(sibling.innerText || sibling.textContent);
                        if (/^(documents?|invoices?|messages?|course allocations?)/i.test(siblingText)) return;
                        if (sibling.matches && sibling.matches('table')) addContainer(sibling);
                        if (sibling.querySelector && sibling.querySelector('table')) addContainer(sibling);
                        if (/\\bfile\\b/i.test(siblingText) && /\\buploaded\\b/i.test(siblingText)) addContainer(sibling);
                    }
                }
            };

            for (const heading of Array.from(document.querySelectorAll(headingSelectors))) {
                const headingText = clean(heading.innerText || heading.textContent);
                if (!isVisible(heading) || !isCertificateHeading(headingText)) continue;
                let container = null;
                if (heading.tagName && heading.tagName.toLowerCase() === 'caption') {
                    container = heading.closest('table');
                }
                if (!container) {
                    container = heading.closest(sectionContainers.join(','));
                }
                if (!container) {
                    const table = heading.parentElement && heading.parentElement.querySelector('table');
                    container = table || heading.parentElement;
                }
                addContainer(container);
                addNearbyFastFormTable(heading);

                // FastForm often renders a coloured section title and then the
                // table as the next block/sibling rather than inside a semantic
                // section. Walk nearby siblings but stop before another heading.
                let sibling = heading.nextElementSibling || (heading.parentElement && heading.parentElement.nextElementSibling);
                for (let hop = 0; sibling && hop < 6; hop += 1, sibling = sibling.nextElementSibling) {
                    const siblingText = clean(sibling.innerText || sibling.textContent);
                    if (siblingText && !isCertificateHeading(siblingText) && /^(documents?|invoices?|messages?|course allocations?|course presenter account|updated course details)/i.test(siblingText)) break;
                    if (sibling.querySelector && sibling.querySelector('table')) addContainer(sibling);
                    if (sibling.matches && sibling.matches('table')) addContainer(sibling);
                    if (/\\b(file|uploaded)\\b/i.test(siblingText) && sibling.querySelector && sibling.querySelector('tr')) addContainer(sibling);
                }
            }

            const rows = [];
            for (const container of containers) {
                const sectionText = clean(container.innerText || container.textContent);
                const tables = container.matches('table') ? [container] : Array.from(container.querySelectorAll('table'));
                if (!tables.length && !/(^|\\b)certificates?(\\b|$)/i.test(sectionText)) continue;
                for (const table of tables) {
                    const caption = clean((table.querySelector('caption') || {}).innerText);
                    if (caption && !isCertificateHeading(caption)) continue;
                    for (const tr of Array.from(table.querySelectorAll('tr'))) {
                        const cells = Array.from(tr.querySelectorAll('td'));
                        if (!cells.length) continue;
                        const values = cells.map((cell) => clean(cell.innerText || cell.textContent)).filter(Boolean);
                        if (values.length) rows.push(rowPayload(tr, values));
                    }
                }
            }

            if (rows.length) return rows;

            // Some FOBS pages render each certificate as a repeated block instead
            // of a table. Keep this scoped to explicit Certificates containers.
            for (const container of containers) {
                const candidates = Array.from(container.querySelectorAll('li,.row,.list-group-item,.certificate,.document-row'));
                for (const candidate of candidates) {
                    const text = clean(candidate.innerText || candidate.textContent);
                    if (!text || isExcludedSectionText(text)) continue;
                    if (!looksLikeCertificateRow([text])) continue;
                    rows.push(rowPayload(candidate, [text]));
                }
            }
            if (rows.length) return rows;

            return rows;
        }
    """) or []


def load_provider_certificates():
    conn = documents_conn()
    try:
        rows = [dict(r) for r in conn.execute("""
            SELECT * FROM provider_certificates
            WHERE COALESCE(status, 'seen') = 'seen'
            ORDER BY provider_name ASC, COALESCE(expiry_date, '9999-12-31') ASC, certificate_name ASC
        """).fetchall()]
    finally:
        conn.close()
    grouped = {}
    for row in rows:
        row['cached_file_available'] = provider_cached_file_available(row)
        grouped.setdefault(row.get('provider_id') or 'provider', []).append(row)
    return grouped


def provider_cached_file_available(cert):
    cached = ((cert or {}).get('cached_filename') or '').strip()
    if not cached:
        return False
    if ((cert or {}).get('download_status') or '') != PROVIDER_CACHE_VERSION:
        return False
    try:
        path = safe_provider_cache_path(cached)
    except Exception:
        return False
    try:
        return path.exists() and path.is_file()
    except OSError:
        return False


def existing_provider_certificate_cache(provider_id):
    conn = documents_conn()
    try:
        rows = [dict(r) for r in conn.execute("""
            SELECT provider_reference, download_url, cached_filename, file_hash, file_size,
                   content_type, downloaded_at, download_status, encryption
            FROM provider_certificates
            WHERE provider_id = ?
        """, (provider_id,)).fetchall()]
    finally:
        conn.close()
    cached = {}
    for row in rows:
        ref = (row.get('provider_reference') or '').strip()
        if not ref or not row.get('cached_filename'):
            continue
        if (row.get('download_status') or '') != PROVIDER_CACHE_VERSION:
            continue
        cached[ref] = {
            'download_url': row.get('download_url') or '',
            'cached_filename': row.get('cached_filename') or '',
            'file_hash': row.get('file_hash') or '',
            'file_size': row.get('file_size'),
            'content_type': row.get('content_type') or '',
            'downloaded_at': row.get('downloaded_at') or '',
            'download_status': row.get('download_status') or 'cached',
            'encryption': row.get('encryption') or '',
        }
    return cached


def cached_provider_file_for_ref(provider, provider_ref):
    provider_id = provider_slug((provider or {}).get('id') or (provider or {}).get('name') or 'provider')
    safe_ref = safe_document_filename(provider_ref or '')
    if not safe_ref:
        return {}
    conn = documents_conn()
    try:
        row = conn.execute("""
            SELECT cached_filename, file_hash, file_size, content_type, downloaded_at, download_status, encryption
            FROM provider_certificates
            WHERE provider_id = ? AND provider_reference = ? AND download_status = ?
            ORDER BY COALESCE(downloaded_at, '') DESC, COALESCE(updated_at, '') DESC
            LIMIT 1
        """, (provider_id, provider_ref, PROVIDER_CACHE_VERSION)).fetchone()
        row = dict(row) if row else None
    finally:
        conn.close()
    if row and row.get('cached_filename'):
        try:
            if safe_provider_cache_path(row.get('cached_filename')).exists():
                return {
                    'cached_filename': row.get('cached_filename') or '',
                    'file_size': row.get('file_size'),
                    'content_type': row.get('content_type') or '',
                    'downloaded_at': row.get('downloaded_at') or '',
                    'download_status': row.get('download_status') or PROVIDER_CACHE_VERSION,
                    'file_hash': row.get('file_hash') or '',
                    'encryption': row.get('encryption') or '',
                }
        except Exception:
            pass
    return {}


def normalize_certificate_match_text(value):
    text = re.sub(r'[^a-z0-9]+', ' ', (value or '').lower()).strip()
    replacements = {
        'licence': 'license',
        'driving license': 'driving',
        'driving licence': 'driving',
        'dbs certificate': 'dbs',
        'adi badge': 'adi',
        'trainer certificate': 'trainer',
        'first aid': 'firstaid',
        'right to work': 'rightwork',
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return re.sub(r'\s+', ' ', text).strip()


def certificate_match_tokens(doc):
    label = document_type_label(doc.get('document_type'))
    values = [
        doc.get('title') or '',
        label,
        doc.get('original_filename') or '',
        doc.get('notes') or '',
    ]
    tokens = {normalize_certificate_match_text(v) for v in values if v}
    doc_type = (doc.get('document_type') or '').strip()
    if doc_type == 'driving_licence':
        tokens.update({'driving', 'license'})
    elif doc_type == 'adi_badge':
        tokens.update({'adi'})
    elif doc_type == 'trainer_certificate':
        tokens.update({'trainer'})
    elif doc_type == 'right_to_work':
        tokens.update({'rightwork'})
    elif doc_type:
        tokens.add(normalize_certificate_match_text(doc_type))
    return {token for token in tokens if token}


def certificate_matches_local_doc(cert, doc):
    raw_columns = cert.get('raw_columns') or ''
    if isinstance(raw_columns, (list, tuple)):
        raw_columns = ' '.join(str(v or '') for v in raw_columns)
    cert_text = ' '.join([
        cert.get('certificate_name') or '',
        cert.get('detected_type') or '',
        cert.get('raw_text') or '',
        raw_columns,
    ])
    cert_norm = normalize_certificate_match_text(cert_text)
    if not cert_norm:
        return False
    cert_type = normalize_certificate_match_text(cert.get('detected_type') or '')
    for token in certificate_match_tokens(doc):
        if token and (token in cert_norm or (cert_type and token in cert_type)):
            return True
    return False


def build_certificate_match_overview(documents, providers, provider_certificates_by_provider):
    provider_lookup = {provider.get('id'): provider for provider in providers}
    rows = []
    matched_provider_refs = set()
    summary = {'in_fobs': 0, 'missing': 0, 'unassigned': 0, 'only_in_fobs': 0, 'warnings': 0}

    for doc in documents:
        links = doc.get('links') or []
        if not links:
            rows.append({
                'kind': 'local',
                'provider_id': '',
                'provider_name': 'Not assigned',
                'certificate_name': doc.get('title') or 'Certificate',
                'status': 'unassigned',
                'status_label': 'Not assigned',
                'status_class': 'neutral',
                'message': 'Choose which providers should use this certificate.',
                'document': doc,
                'provider_certificate': None,
            })
            summary['unassigned'] += 1
            continue

        for link in links:
            provider_id = link.get('provider_id') or ''
            provider = provider_lookup.get(provider_id) or {'name': link.get('provider_name') or provider_id}
            certs = provider_certificates_by_provider.get(provider_id, [])
            match = next((cert for cert in certs if certificate_matches_local_doc(cert, doc)), None)
            health_key = doc.get('health_key') or ''
            if match:
                matched_provider_refs.add(match.get('id') or match.get('provider_reference') or id(match))
                status = 'in_fobs'
                status_label = 'In FOBS'
                status_class = 'ok'
                message = 'Provider already has a matching certificate.'
                summary['in_fobs'] += 1
            else:
                status = 'missing'
                status_label = 'Needs attention'
                status_class = 'due'
                message = 'TrainerMate can send this to the provider.'
                summary['missing'] += 1
            if health_key in {'expired', 'expiring'}:
                status_class = 'due' if status_class == 'ok' else status_class
                message = doc.get('health_label') or message
                summary['warnings'] += 1
            rows.append({
                'kind': 'local',
                'provider_id': provider_id,
                'provider_name': provider.get('name') or provider_id,
                'certificate_name': doc.get('title') or 'Certificate',
                'status': status,
                'status_label': status_label,
                'status_class': status_class,
                'message': message,
                'document': doc,
                'provider_certificate': match,
            })

    for provider in providers:
        provider_id = provider.get('id') or ''
        for cert in provider_certificates_by_provider.get(provider_id, []):
            ref = cert.get('id') or cert.get('provider_reference') or id(cert)
            if ref in matched_provider_refs:
                continue
            rows.append({
                'kind': 'provider_only',
                'provider_id': provider_id,
                'provider_name': provider.get('name') or provider_id,
                'certificate_name': cert.get('certificate_name') or 'Certificate',
                'status': 'only_in_fobs',
                'status_label': 'Only in FOBS',
                'status_class': 'neutral',
                'message': 'Read-only provider certificate not yet matched to TrainerMate.',
                'document': None,
                'provider_certificate': cert,
            })
            summary['only_in_fobs'] += 1

    return {'rows': rows, 'summary': summary}


def choose_certificate_download_url(links):
    candidates = []
    safe_links = []
    for index, link in enumerate(links or []):
        href = (link.get('href') or '').strip()
        text = (link.get('text') or '').strip().lower()
        lower_href = href.lower()
        if not href or lower_href.startswith('javascript:'):
            continue
        if any(word in text for word in ('delete', 'remove', 'edit')) or any(word in lower_href for word in ('delete', 'remove')):
            continue
        safe_links.append(href)
        score = 0
        if any(word in text for word in ('download', 'view', 'open', 'file', 'certificate')):
            score += 3
        if any(word in lower_href for word in ('download', 'document', 'certificate', 'file', 'attachment')):
            score += 2
        if Path(urlparse(lower_href).path).suffix.lower() in ALLOWED_DOCUMENT_EXTENSIONS:
            score += 5
        # FOBS often makes the certificate title itself the download link,
        # so any safe link inside a certificate row is a valid fallback.
        candidates.append((score, -index, href))
    if candidates:
        return sorted(candidates, key=lambda item: (item[0], item[1]), reverse=True)[0][2]
    if safe_links:
        return safe_links[0]
    return ''


def extension_from_content_type(content_type, fallback_url=''):
    content_type = (content_type or '').split(';')[0].strip().lower()
    mapping = {
        'application/pdf': '.pdf',
        'image/jpeg': '.jpg',
        'image/png': '.png',
        'application/msword': '.doc',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
    }
    if content_type in mapping:
        return mapping[content_type]
    ext = Path(urlparse(fallback_url or '').path).suffix.lower()
    return ext if ext in ALLOWED_DOCUMENT_EXTENSIONS else '.pdf'


def content_type_is_cacheable_file(content_type, url=''):
    content_type = (content_type or '').split(';')[0].strip().lower()
    if content_type in ALLOWED_DOCUMENT_MIME_TYPES:
        return True
    return Path(urlparse(url or '').path).suffix.lower() in ALLOWED_DOCUMENT_EXTENSIONS


def resolve_certificate_file_url_from_html(page, landing_url):
    detail = page.context.new_page()
    try:
        detail.goto(landing_url, wait_until='domcontentloaded', timeout=30000)
        detail.wait_for_timeout(700)
        return detail.evaluate("""
            () => {
                const clean = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                const abs = (href) => { try { return new URL(href, window.location.href).href; } catch (err) { return ''; } };
                const bad = /(delete|remove|edit|logout|logoff|account\\/login)/i;
                const goodText = /(download|view|open|file|certificate|attachment|pdf|document)/i;
                const goodHref = /(download|certificate|document|attachment|file|pdf|trainer\\/documents)/i;
                const candidates = [];
                for (const a of Array.from(document.querySelectorAll('a[href]'))) {
                    const href = abs(a.getAttribute('href') || '');
                    const text = clean(a.innerText || a.textContent || a.getAttribute('title') || '');
                    if (!href || /^javascript:/i.test(href) || bad.test(href) || bad.test(text)) continue;
                    let score = 0;
                    if (goodText.test(text)) score += 3;
                    if (goodHref.test(href)) score += 2;
                    if (/\\.(pdf|png|jpe?g|docx?)($|[?#])/i.test(href)) score += 5;
                    if (score) candidates.push({href, score});
                }
                for (const frame of Array.from(document.querySelectorAll('iframe[src],embed[src],object[data]'))) {
                    const href = abs(frame.getAttribute('src') || frame.getAttribute('data') || '');
                    if (!href || bad.test(href)) continue;
                    let score = 4;
                    if (/\\.(pdf|png|jpe?g|docx?)($|[?#])/i.test(href)) score += 5;
                    candidates.push({href, score});
                }
                candidates.sort((a, b) => b.score - a.score);
                return candidates.length ? candidates[0].href : '';
            }
        """) or ''
    finally:
        try:
            detail.close()
        except Exception:
            pass


class _DataBlob(ctypes.Structure):
    _fields_ = [('cbData', ctypes.c_ulong), ('pbData', ctypes.POINTER(ctypes.c_ubyte))]


def protect_provider_cache_bytes(content):
    if os.name != 'nt':
        raise RuntimeError('Encrypted provider cache requires Windows DPAPI.')
    data = bytes(content or b'')
    in_buffer = ctypes.create_string_buffer(data)
    in_blob = _DataBlob(len(data), ctypes.cast(in_buffer, ctypes.POINTER(ctypes.c_ubyte)))
    out_blob = _DataBlob()
    description = 'TrainerMate provider certificate cache'
    if not ctypes.windll.crypt32.CryptProtectData(
        ctypes.byref(in_blob), description, None, None, None, 0, ctypes.byref(out_blob)
    ):
        raise ctypes.WinError()
    try:
        return ctypes.string_at(out_blob.pbData, out_blob.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(out_blob.pbData)


def unprotect_provider_cache_bytes(content):
    if os.name != 'nt':
        raise RuntimeError('Encrypted provider cache requires Windows DPAPI.')
    data = bytes(content or b'')
    in_buffer = ctypes.create_string_buffer(data)
    in_blob = _DataBlob(len(data), ctypes.cast(in_buffer, ctypes.POINTER(ctypes.c_ubyte)))
    out_blob = _DataBlob()
    if not ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(in_blob), None, None, None, None, 0, ctypes.byref(out_blob)
    ):
        raise ctypes.WinError()
    try:
        return ctypes.string_at(out_blob.pbData, out_blob.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(out_blob.pbData)


def cache_provider_certificate_bytes(provider, provider_ref, content, content_type='', download_url='', suggested_filename='', cache_status='cached'):
    content = bytes(content or b'')
    if not content:
        return {'download_url': download_url, 'download_status': 'empty'}
    file_hash = hashlib.sha256(content).hexdigest()
    fallback = suggested_filename or download_url
    guessed_type = (content_type or mimetypes.guess_type(fallback or '')[0] or '').strip()
    ext = extension_from_content_type(guessed_type, fallback)
    filename_base = f"{provider_slug(provider.get('id') or provider.get('name') or 'provider')}-{provider_ref}-{file_hash[:12]}{ext}"
    if os.name == 'nt':
        stored_filename = f"{filename_base}.dpapi"
        target = safe_provider_cache_path(stored_filename)
        target.write_bytes(protect_provider_cache_bytes(content))
        encryption = 'dpapi'
    else:
        # Development/non-Windows fallback. The app is localhost-only and the
        # provider cache directory is mode 0700; Windows production builds still
        # use DPAPI above.
        stored_filename = filename_base
        target = safe_provider_cache_path(stored_filename)
        target.write_bytes(content)
        encryption = ''
        if cache_status == 'cached':
            cache_status = 'cached_unencrypted_dev'
        elif cache_status == 'cached_via_page':
            cache_status = 'cached_unencrypted_dev_via_page'
        elif cache_status == 'cached_by_click':
            cache_status = 'cached_unencrypted_dev_by_click'
        elif cache_status == 'cached_by_row_click':
            cache_status = 'cached_unencrypted_dev_by_row_click'
        elif cache_status == 'cached_by_download_document_id':
            cache_status = 'cached_unencrypted_dev_by_download_document_id'
    if cache_status == 'cached_by_download_document_id':
        cache_status = PROVIDER_CACHE_VERSION
    return {
        'download_url': download_url,
        'cached_filename': stored_filename,
        'file_hash': file_hash,
        'file_size': len(content),
        'content_type': guessed_type,
        'downloaded_at': utc_now_text(),
        'download_status': cache_status,
        'encryption': encryption,
    }



def cache_provider_certificate_file_by_download_document_id(page, provider, provider_ref, cert):
    """FOBS FastForm rows use onclick=DownloadDocument(123). Capture that exact ID."""
    document_id = re.sub(r'\D+', '', str(cert.get('download_document_id') or ''))
    if not document_id:
        return {'download_status': 'no_link'}
    cert_name = (cert.get('certificate_name') or '').strip()
    try:
        handle = page.evaluate_handle("""
            ({documentId, certificateName}) => {
                const clean = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim();
                const visible = (el) => {
                    if (!el) return false;
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                };
                const wanted = clean(certificateName).toLowerCase();
                const links = Array.from(document.querySelectorAll('[onclick]')).filter(visible);
                const exactLinks = links.filter((el) => {
                    const onclick = el.getAttribute('onclick') || '';
                    const m = onclick.match(/DownloadDocument\\s*\\(\\s*['\"]?(\\d+)['\"]?\\s*\\)/i);
                    return m && m[1] === String(documentId);
                });
                if (wanted) {
                    const sameRow = exactLinks.find((el) => {
                        const row = el.closest('tr,li,.row,.list-group-item,.certificate,.document-row,.card,.panel') || el.parentElement;
                        return clean((row && (row.innerText || row.textContent)) || '').toLowerCase().includes(wanted);
                    });
                    if (sameRow) return sameRow;
                }
                return exactLinks[0] || null;
            }
        """, {'documentId': document_id, 'certificateName': cert_name})
        element = handle.as_element()
        if element:
            element.scroll_into_view_if_needed(timeout=3000)
            with page.expect_download(timeout=15000) as download_info:
                element.click(timeout=5000)
            download = download_info.value
        else:
            # Last resort: call the exact FastForm JS function if the element was not found.
            with page.expect_download(timeout=15000) as download_info:
                page.evaluate("""
                    (documentId) => {
                        if (typeof DownloadDocument !== 'function') {
                            throw new Error('DownloadDocument is not available on this page');
                        }
                        DownloadDocument(Number(documentId));
                    }
                """, document_id)
            download = download_info.value
        suggested = download.suggested_filename or safe_document_filename(cert_name or f'certificate-{document_id}.pdf')
        tmp_path = safe_provider_cache_path(f"tmp-{uuid.uuid4().hex}-{safe_document_filename(suggested)}")
        try:
            download.save_as(str(tmp_path))
            content = tmp_path.read_bytes()
        finally:
            try:
                tmp_path.unlink()
            except Exception:
                pass
        content_type = mimetypes.guess_type(suggested)[0] or ''
        return cache_provider_certificate_bytes(
            provider, provider_ref, content, content_type=content_type,
            download_url=f'DownloadDocument({document_id})', suggested_filename=suggested,
            cache_status='cached_by_download_document_id'
        )
    except Exception as exc:
        return {'download_url': f'DownloadDocument({document_id})', 'download_status': f'download_document_failed: {str(exc)[:140]}'}


def cache_provider_certificate_file_by_row_click(page, provider, provider_ref, cert):
    """Capture the download by clicking the exact certificate row/title itself."""
    cert_name = (cert.get('certificate_name') or '').strip()
    raw_columns = cert.get('raw_columns') or []
    text_candidates = []
    for value in [cert_name] + list(raw_columns):
        value = ' '.join(str(value or '').replace('\xa0', ' ').split()).strip()
        if value and value.lower() not in {'view', 'download', 'edit', 'delete'}:
            text_candidates.append(value)
    seen = set()
    text_candidates = [v for v in text_candidates if not (v.lower() in seen or seen.add(v.lower()))]
    if not text_candidates:
        return {'download_status': 'no_link'}
    try:
        handle = page.evaluate_handle("""
            (candidates) => {
                const clean = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim();
                const lower = (value) => clean(value).toLowerCase();
                const bad = /(delete|remove|edit|logout|logoff|account\\/login)/i;
                const candidatesLower = candidates.map((v) => lower(v)).filter(Boolean);
                const scoreEl = (el) => {
                    const text = clean(el.innerText || el.textContent || el.getAttribute('title') || '');
                    const ltext = lower(text);
                    if (!ltext || bad.test(ltext)) return 0;
                    let best = 0;
                    for (const c of candidatesLower) {
                        if (ltext === c) best = Math.max(best, 100 + c.length);
                        else if (ltext.includes(c)) best = Math.max(best, 70 + c.length);
                        else if (c.includes(ltext) && ltext.length >= 6) best = Math.max(best, 40 + ltext.length);
                    }
                    return best;
                };
                const clickable = Array.from(document.querySelectorAll('a[href],button,[onclick],td,span'));
                const ranked = clickable.map((el) => ({el, score: scoreEl(el)})).filter((x) => x.score > 0);
                ranked.sort((a, b) => b.score - a.score);
                if (!ranked.length) return null;
                const winner = ranked[0].el;
                return winner.closest('a[href],button,[onclick]') || winner;
            }
        """, text_candidates)
        element = handle.as_element()
        if not element:
            return {'download_status': 'click_row_not_found'}
        element.scroll_into_view_if_needed(timeout=3000)
        with page.expect_download(timeout=15000) as download_info:
            element.click(timeout=5000)
        download = download_info.value
        suggested = download.suggested_filename or safe_document_filename(cert_name or 'certificate.pdf')
        tmp_path = safe_provider_cache_path(f"tmp-{uuid.uuid4().hex}-{safe_document_filename(suggested)}")
        try:
            download.save_as(str(tmp_path))
            content = tmp_path.read_bytes()
        finally:
            try:
                tmp_path.unlink()
            except Exception:
                pass
        content_type = mimetypes.guess_type(suggested)[0] or ''
        return cache_provider_certificate_bytes(
            provider, provider_ref, content, content_type=content_type,
            download_url=(cert.get('download_url') or ''), suggested_filename=suggested,
            cache_status='cached_by_row_click'
        )
    except Exception as exc:
        return {'download_url': (cert.get('download_url') or ''), 'download_status': f'row_click_download_failed: {str(exc)[:140]}'}

def cache_provider_certificate_file_by_click(page, provider, provider_ref, download_url):
    """FOBS certificate links often trigger a browser download instead of exposing a PDF URL."""
    download_url = (download_url or '').strip()
    if not download_url:
        return {'download_status': 'no_link'}
    try:
        locator = page.locator('a[href]').filter(has_text='')
        handle = page.evaluate_handle("""
            (targetUrl) => {
                const abs = (href) => { try { return new URL(href, window.location.href).href; } catch (err) { return ''; } };
                const links = Array.from(document.querySelectorAll('a[href]'));
                return links.find((a) => abs(a.getAttribute('href') || '') === targetUrl) || null;
            }
        """, download_url)
        element = handle.as_element()
        if not element:
            return {'download_url': download_url, 'download_status': 'click_link_not_found'}
        element.scroll_into_view_if_needed(timeout=3000)
        with page.expect_download(timeout=12000) as download_info:
            element.click(timeout=5000)
        download = download_info.value
        suggested = download.suggested_filename or Path(urlparse(download_url).path).name or 'certificate.pdf'
        tmp_path = safe_provider_cache_path(f"tmp-{uuid.uuid4().hex}-{safe_document_filename(suggested)}")
        try:
            download.save_as(str(tmp_path))
            content = tmp_path.read_bytes()
        finally:
            try:
                tmp_path.unlink()
            except Exception:
                pass
        content_type = mimetypes.guess_type(suggested)[0] or ''
        return cache_provider_certificate_bytes(
            provider, provider_ref, content, content_type=content_type,
            download_url=download_url, suggested_filename=suggested, cache_status='cached_by_click'
        )
    except Exception as exc:
        return {'download_url': download_url, 'download_status': f'click_download_failed: {str(exc)[:140]}'}


def cache_provider_certificate_file(page, provider, provider_ref, cert):
    """Cache a provider certificate using the provider adapter's strategies.

    The existing FOBS portals use DownloadDocument(id). Future providers can use
    the same core flow with a different adapter/download strategy.
    """
    adapter = provider_certificate_adapter(provider)
    download_url = (cert.get('download_url') or '').strip()
    last_result = {'download_status': 'no_link'}

    for strategy in adapter.get('download_strategy') or ():
        if strategy == 'download_document_id':
            result = cache_provider_certificate_file_by_download_document_id(page, provider, provider_ref, cert)
        elif strategy == 'row_click':
            result = cache_provider_certificate_file_by_row_click(page, provider, provider_ref, cert)
        elif strategy == 'direct_url':
            if not download_url:
                result = {'download_status': 'no_link'}
            else:
                result = cache_provider_certificate_file_from_direct_url(page, provider, provider_ref, download_url)
        else:
            result = {'download_status': f'unknown_strategy_{strategy}'}

        if result.get('cached_filename'):
            result.setdefault('certificate_adapter', adapter.get('id'))
            return result
        if result.get('download_status') not in {'no_link', 'click_link_not_found'}:
            last_result = result

    return last_result


def cache_provider_certificate_file_from_direct_url(page, provider, provider_ref, download_url):
    try:
        response = page.context.request.get(download_url, timeout=30000)
        if not response.ok:
            click_result = cache_provider_certificate_file_by_click(page, provider, provider_ref, download_url)
            if click_result.get('cached_filename'):
                return click_result
            return {'download_url': download_url, 'download_status': f'failed_http_{response.status}'}
        content_type = (response.headers.get('content-type') or '').strip()
        content = response.body()
        if not content:
            click_result = cache_provider_certificate_file_by_click(page, provider, provider_ref, download_url)
            if click_result.get('cached_filename'):
                return click_result
            return {'download_url': download_url, 'download_status': 'empty'}
        if 'text/html' in content_type.lower() and not content_type_is_cacheable_file(content_type, download_url):
            resolved_url = resolve_certificate_file_url_from_html(page, download_url)
            if resolved_url and resolved_url != download_url:
                response = page.context.request.get(resolved_url, timeout=30000)
                if response.ok:
                    content_type = (response.headers.get('content-type') or '').strip()
                    content = response.body()
                    if content and ('text/html' not in content_type.lower() or content_type_is_cacheable_file(content_type, resolved_url)):
                        return cache_provider_certificate_bytes(
                            provider, provider_ref, content, content_type=content_type,
                            download_url=resolved_url, cache_status='cached_via_page'
                        )
            click_result = cache_provider_certificate_file_by_click(page, provider, provider_ref, download_url)
            if click_result.get('cached_filename'):
                return click_result
            return {'download_url': download_url, 'download_status': click_result.get('download_status') or 'html_no_file_link'}
        return cache_provider_certificate_bytes(
            provider, provider_ref, content, content_type=content_type,
            download_url=download_url, cache_status='cached'
        )
    except Exception as exc:
        return {'download_url': download_url, 'download_status': f'failed: {str(exc)[:160]}'}

def save_provider_certificate_scan(provider, certificates, source_url):
    now = utc_now_text()
    conn = documents_conn()
    try:
        provider_id = provider.get('id')
        if not certificates:
            existing = conn.execute(
                'SELECT COUNT(*) FROM provider_certificates WHERE provider_id = ?',
                (provider_id,),
            ).fetchone()[0]
            if existing:
                print(f"[CERTIFICATES] {provider.get('name') or provider_id}: zero rows found; keeping {existing} cached certificate row(s).")
            return False
        conn.execute("""
            UPDATE provider_certificates
            SET status = 'missing',
                updated_at = ?
            WHERE provider_id = ?
        """, (now, provider_id))
        seen_provider_refs = set()
        skipped_duplicates = 0
        for cert in certificates:
            raw_columns = cert.get('raw_columns') or []
            raw_text = ' | '.join(raw_columns)
            ref_seed = raw_text or cert.get('certificate_name') or str(uuid.uuid4())
            provider_ref = cert.get('provider_reference') or hashlib.sha256(ref_seed.encode('utf-8', errors='ignore')).hexdigest()[:24]
            if provider_ref in seen_provider_refs:
                skipped_duplicates += 1
                continue
            seen_provider_refs.add(provider_ref)
            existing = conn.execute("""
                SELECT id
                FROM provider_certificates
                WHERE provider_id = ? AND provider_reference = ?
                ORDER BY COALESCE(last_seen_at, '') DESC, COALESCE(updated_at, '') DESC
                LIMIT 1
            """, (provider_id, provider_ref)).fetchone()
            values = (
                provider.get('name'),
                cert.get('certificate_name') or 'Certificate',
                cert.get('detected_type') or '',
                cert.get('expiry_date') or '',
                cert.get('uploaded_at') or '',
                source_url or '',
                provider_ref,
                json.dumps(raw_columns, ensure_ascii=False),
                cert.get('download_url') or '',
                cert.get('cached_filename') or '',
                cert.get('file_hash') or '',
                int(cert.get('file_size') or 0),
                cert.get('content_type') or '',
                cert.get('downloaded_at') or '',
                cert.get('download_status') or '',
                cert.get('encryption') or '',
                now,
                now,
            )
            if existing:
                conn.execute("""
                    UPDATE provider_certificates
                    SET provider_name = ?,
                        certificate_name = ?,
                        detected_type = ?,
                        expiry_date = ?,
                        uploaded_at = ?,
                        source_url = ?,
                        provider_reference = ?,
                        raw_columns = ?,
                        download_url = ?,
                        cached_filename = ?,
                        file_hash = ?,
                        file_size = ?,
                        content_type = ?,
                        downloaded_at = ?,
                        download_status = ?,
                        encryption = ?,
                        status = 'seen',
                        last_seen_at = ?,
                        updated_at = ?
                    WHERE id = ?
                """, values + (existing['id'],))
            else:
                conn.execute("""
                    INSERT INTO provider_certificates (
                        id, provider_id, provider_name, certificate_name, detected_type,
                        expiry_date, uploaded_at, source_url, provider_reference,
                        raw_columns, download_url, cached_filename, file_hash, file_size,
                        content_type, downloaded_at, download_status, encryption, status, last_seen_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'seen', ?, ?)
                """, (
                    str(uuid.uuid4()), provider_id,
                    provider.get('name'),
                    cert.get('certificate_name') or 'Certificate',
                    cert.get('detected_type') or '',
                    cert.get('expiry_date') or '',
                    cert.get('uploaded_at') or '',
                    source_url or '', provider_ref,
                    json.dumps(raw_columns, ensure_ascii=False),
                    cert.get('download_url') or '',
                    cert.get('cached_filename') or '',
                    cert.get('file_hash') or '',
                    int(cert.get('file_size') or 0),
                    cert.get('content_type') or '',
                    cert.get('downloaded_at') or '',
                    cert.get('download_status') or '',
                    cert.get('encryption') or '',
                    now, now,
                ))
        if skipped_duplicates:
            print(f"[CERTIFICATES] {provider.get('name') or provider_id}: skipped {skipped_duplicates} duplicate certificate row(s).")
        conn.commit()
        return True
    finally:
        conn.close()


def update_document_provider_presence(provider, certificates):
    provider_id = provider_slug(provider.get('id') or provider.get('name') or '')
    provider_name = provider.get('name') or provider_id
    now = utc_now_text()
    conn = documents_conn()
    try:
        docs = [dict(row) for row in conn.execute("""
            SELECT d.*, l.id AS link_id
            FROM trainer_documents d
            JOIN document_provider_links l ON l.document_id = d.id
            WHERE d.status = 'active'
              AND l.provider_id = ?
        """, (provider_id,)).fetchall()]
        for doc in docs:
            match = next((cert for cert in certificates if certificate_matches_local_doc(cert, doc)), None)
            if match:
                conn.execute("""
                    UPDATE document_provider_links
                    SET provider_status = 'in_sync',
                        provider_file_name = ?,
                        provider_checked_at = ?,
                        pending_action = CASE
                            WHEN pending_action IN ('upload', 'dismissed_missing') THEN ''
                            ELSE pending_action
                        END,
                        notes = '',
                        updated_at = ?
                    WHERE id = ?
                """, (match.get('certificate_name') or '', now, now, doc['link_id']))
            else:
                pending_action = (doc.get('pending_action') or '').strip()
                if pending_action == 'upload':
                    conn.execute("""
                        UPDATE document_provider_links
                        SET provider_status = 'not_checked',
                            provider_checked_at = ?,
                            notes = ?,
                            updated_at = ?
                        WHERE id = ?
                    """, (now, f'TrainerMate has not sent this certificate to {provider_name} yet.', now, doc['link_id']))
                    continue
                conn.execute("""
                    UPDATE document_provider_links
                    SET provider_status = 'missing',
                        provider_checked_at = ?,
                        pending_action = CASE
                            WHEN pending_action = '' THEN 'review_missing'
                            ELSE pending_action
                        END,
                        notes = ?,
                        updated_at = ?
                    WHERE id = ?
                """, (now, f'This certificate no longer appears in {provider_name}.', now, doc['link_id']))
        conn.commit()
    finally:
        conn.close()


def provider_delete_pending_links():
    conn = documents_conn()
    try:
        rows = [dict(row) for row in conn.execute("""
            SELECT
                l.id AS link_id,
                l.provider_id,
                l.provider_name,
                l.provider_file_name,
                l.provider_checked_at,
                d.*
            FROM document_provider_links l
            JOIN trainer_documents d ON d.id = l.document_id
            WHERE d.status = 'active'
              AND l.pending_action = 'delete_provider_copy'
            ORDER BY l.provider_name ASC, d.title ASC
        """).fetchall()]
    finally:
        conn.close()
    return rows


def provider_upload_pending_links():
    conn = documents_conn()
    try:
        rows = [dict(row) for row in conn.execute("""
            SELECT
                l.id AS link_id,
                l.provider_id,
                l.provider_name,
                l.provider_file_name,
                l.provider_checked_at,
                d.*
            FROM document_provider_links l
            JOIN trainer_documents d ON d.id = l.document_id
            WHERE d.status = 'active'
              AND l.pending_action = 'upload'
            ORDER BY l.provider_name ASC, d.title ASC
        """).fetchall()]
    finally:
        conn.close()
    return rows


def mark_provider_delete_link(link_id, status, message=''):
    now = utc_now_text()
    if status == 'removed':
        provider_status = 'missing'
        pending_action = ''
    else:
        provider_status = 'needs_review'
        pending_action = 'delete_provider_copy'
    conn = documents_conn()
    try:
        conn.execute("""
            UPDATE document_provider_links
            SET provider_status = ?,
                provider_checked_at = ?,
                pending_action = ?,
                notes = ?,
                updated_at = ?
            WHERE id = ?
        """, (provider_status, now, pending_action, message or '', now, link_id))
        conn.commit()
    finally:
        conn.close()


def mark_provider_upload_link(link_id, status, message='', provider_file_name=''):
    now = utc_now_text()
    if status == 'uploaded':
        provider_status = 'in_sync'
        pending_action = ''
    else:
        provider_status = 'needs_review'
        pending_action = 'upload'
    conn = documents_conn()
    try:
        conn.execute("""
            UPDATE document_provider_links
            SET provider_status = ?,
                provider_checked_at = ?,
                provider_file_name = CASE
                    WHEN ? <> '' THEN ?
                    ELSE provider_file_name
                END,
                pending_action = ?,
                notes = ?,
                updated_at = ?
            WHERE id = ?
        """, (provider_status, now, provider_file_name or '', provider_file_name or '', pending_action, message or '', now, link_id))
        conn.commit()
    finally:
        conn.close()



def upsert_provider_certificate_from_upload(provider, link_doc, provider_file_name=''):
    """Reflect a successful submitted upload in the local FOBS certificate cache.

    FOBS can accept an upload but take a moment to show it in the certificate
    table. Updating the local provider cache here keeps the dashboard count and
    prompts in sync immediately, and the next provider refresh will replace this
    row with the live FOBS data if anything differs.
    """
    provider_id = provider_slug((provider or {}).get('id') or (provider or {}).get('name') or '')
    if not provider_id:
        return
    provider_name = (provider or {}).get('name') or provider_id
    doc_id = (link_doc or {}).get('id') or (link_doc or {}).get('document_id') or ''
    link_id = (link_doc or {}).get('link_id') or ''
    title = (link_doc or {}).get('title') or Path(provider_file_name or '').stem or 'Certificate'
    original = (link_doc or {}).get('original_filename') or provider_file_name or title
    provider_ref = f"{provider_id}-trainermate-upload-{doc_id or link_id or provider_slug(title)}"
    now = utc_now_text()
    raw_columns = [v for v in [title, original, (link_doc or {}).get('expiry_date') or '', 'Uploaded by TrainerMate'] if v]
    conn = documents_conn()
    try:
        conn.execute('DELETE FROM provider_certificates WHERE provider_id = ? AND provider_reference = ?', (provider_id, provider_ref))
        conn.execute("""
            INSERT INTO provider_certificates (
                id, provider_id, provider_name, certificate_name, detected_type,
                expiry_date, uploaded_at, source_url, provider_reference,
                raw_columns, download_url, cached_filename, file_hash, file_size,
                content_type, downloaded_at, download_status, encryption, status, last_seen_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'seen', ?, ?)
        """, (
            str(uuid.uuid4()), provider_id, provider_name, title,
            detect_certificate_type(' '.join(raw_columns)),
            (link_doc or {}).get('expiry_date') or '', now,
            (provider or {}).get('documents_url') or derive_documents_url((provider or {}).get('login_url') or ''),
            provider_ref, json.dumps(raw_columns, ensure_ascii=False),
            '', provider_file_name or original, '', 0, '', '', 'uploaded_by_trainermate', '', now, now,
        ))
        conn.commit()
    finally:
        conn.close()


def mark_provider_upload_failed(link_id, provider_id='', provider_name='', message='', provider_file_name=''):
    """Keep the upload actionable, but record the exact failure reason."""
    safe_message = (message or 'Upload failed.').strip()
    mark_provider_upload_link(link_id, 'review', safe_message, provider_file_name)
    if provider_id:
        set_certificate_scan_status(provider_id, 'error', f'{provider_name or provider_id} needs checking', safe_message)


def clear_unverified_provider_upload_cache(provider, link_doc):
    """Remove older optimistic rows created before FOBS actually confirmed the upload."""
    provider_id = provider_slug((provider or {}).get('id') or (provider or {}).get('name') or '')
    if not provider_id:
        return
    doc_id = (link_doc or {}).get('id') or (link_doc or {}).get('document_id') or ''
    link_id = (link_doc or {}).get('link_id') or ''
    title = (link_doc or {}).get('title') or 'Certificate'
    possible_refs = [
        f"{provider_id}-trainermate-upload-{doc_id or link_id or provider_slug(title)}",
    ]
    now = utc_now_text()
    conn = documents_conn()
    try:
        for ref in possible_refs:
            conn.execute("""
                DELETE FROM provider_certificates
                WHERE provider_id = ?
                  AND provider_reference = ?
                  AND COALESCE(download_status, '') = 'uploaded_by_trainermate'
            """, (provider_id, ref))
        # Also clear exact old optimistic duplicates for this provider/title.
        conn.execute("""
            DELETE FROM provider_certificates
            WHERE provider_id = ?
              AND lower(COALESCE(certificate_name, '')) = lower(?)
              AND COALESCE(download_status, '') = 'uploaded_by_trainermate'
              AND COALESCE(last_seen_at, '') <= ?
        """, (provider_id, title, now))
        conn.commit()
    finally:
        conn.close()


def provider_upload_validation_message(page):
    try:
        return page.evaluate("""
            () => {
                const clean = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim();
                const visible = (el) => {
                    if (!el) return false;
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                };
                const selectors = [
                    '.validation-summary-errors', '.field-validation-error', '.text-danger',
                    '.alert-danger', '.alert-error', '.error', '.help-block', '[role="alert"]'
                ];
                const messages = [];
                for (const selector of selectors) {
                    for (const el of Array.from(document.querySelectorAll(selector)).filter(visible)) {
                        const text = clean(el.innerText || el.textContent);
                        if (text && !messages.includes(text)) messages.push(text);
                    }
                }
                return messages.slice(0, 3).join(' ');
            }
        """) or ''
    except Exception:
        return ''


def fobs_upload_form_ready(page, form_handle, require_expiry=False):
    """Return (ok, reason) before TrainerMate clicks Upload."""
    try:
        state = form_handle.evaluate("""
            (form, requireExpiry) => {
                const clean = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim();
                const visible = (el) => {
                    if (!el) return false;
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width >= 0 && rect.height >= 0;
                };
                const root = form.closest('.ui-dialog,.modal,[role="dialog"]') || form;
                const file = root.querySelector('input[type="file"]') || form.querySelector('input[type="file"]');
                const fileCount = file && file.files ? file.files.length : 0;
                const expirySelectors = [
                    '#certDate', '#expiryDate', '#expirationDate', '#expiry_date', '#expiration_date',
                    '[name="certDate"]', '[name="expiryDate"]', '[name="expirationDate"]', '[name="expiry_date"]', '[name="expiration_date"]'
                ];
                let expiry = null;
                for (const selector of expirySelectors) {
                    expiry = root.querySelector(selector) || document.querySelector(selector);
                    if (expiry && visible(expiry)) break;
                    expiry = null;
                }
                if (!expiry) {
                    const fields = Array.from(root.querySelectorAll('input,textarea')).filter((el) => !el.disabled && el.type !== 'file' && el.type !== 'hidden');
                    expiry = fields.find((el) => /expir|expiry|valid.*to|cert.*date|certificate.*date|date/i.test(`${el.name || ''} ${el.id || ''} ${el.placeholder || ''} ${el.getAttribute('aria-label') || ''}`));
                }
                const selects = Array.from(root.querySelectorAll('select')).filter((el) => !el.disabled);
                const unselectedRequiredSelect = selects.find((el) => el.required && !el.value);
                return {
                    fileCount,
                    expiryPresent: !!expiry,
                    expiryValue: expiry ? clean(expiry.value || '') : '',
                    missingRequiredSelect: !!unselectedRequiredSelect
                };
            }
        """, bool(require_expiry)) or {}
        if int(state.get('fileCount') or 0) <= 0:
            return False, 'FOBS did not receive the selected certificate file.'
        if state.get('missingRequiredSelect'):
            return False, 'FOBS still has a required certificate type/dropdown empty.'
        if require_expiry and not state.get('expiryPresent'):
            return False, 'TrainerMate could not find the FOBS expiry date field, so it did not click Upload.'
        if require_expiry and not state.get('expiryValue'):
            return False, 'TrainerMate could not fill the FOBS expiry date field, so it did not click Upload.'
        validation = provider_upload_validation_message(page)
        if validation:
            return False, f'FOBS is showing a validation message: {validation[:180]}'
        return True, ''
    except Exception as exc:
        return False, f'TrainerMate could not check the upload form before submitting: {str(exc)[:140]}'


def live_certificate_matches_for_doc(section_rows, provider, doc):
    matches = []
    seen = set()
    for row in section_rows or []:
        values = row.get('values') or []
        links = row.get('links') or []
        values = [' '.join(str(v or '').replace('\xa0', ' ').split()).strip() for v in values]
        values = [v for v in values if v]
        if not values:
            continue
        joined = ' | '.join(values)
        download_document_id = ''
        for link in links:
            if isinstance(link, dict) and (link.get('download_document_id') or ''):
                download_document_id = str(link.get('download_document_id') or '').strip()
                break
        cert = {
            'certificate_name': next((v for v in values if not parse_fobs_date(v)), values[0]),
            'detected_type': detect_certificate_type(joined),
            'raw_columns': values,
            'provider_reference': provider_certificate_reference(provider, joined, download_document_id),
            'download_document_id': download_document_id,
            'download_url': f'DownloadDocument({download_document_id})' if download_document_id else choose_certificate_download_url(links),
        }
        if not certificate_matches_local_doc(cert, doc):
            continue
        key = cert.get('provider_reference') or normalize_certificate_match_text(joined)
        if key in seen:
            continue
        seen.add(key)
        cert['row_text'] = joined
        matches.append(cert)
    return matches


def download_document_id_from_cert(cert):
    text = ' '.join(str((cert or {}).get(key) or '') for key in ('download_document_id', 'download_url', 'provider_reference'))
    match = re.search(r'(?:DownloadDocument\s*\(\s*|document-)(\d+)', text, flags=re.IGNORECASE)
    return match.group(1) if match else ''


def provider_certificate_row_text(cert):
    raw_columns = []
    try:
        raw_value = (cert or {}).get('raw_columns') or ''
        parsed = json.loads(raw_value) if isinstance(raw_value, str) and raw_value else raw_value
        if isinstance(parsed, list):
            raw_columns = [str(value or '').strip() for value in parsed if str(value or '').strip()]
    except Exception:
        raw_columns = []
    if raw_columns:
        return ' | '.join(raw_columns)
    parts = [
        (cert or {}).get('certificate_name') or '',
        (cert or {}).get('expiry_date') or '',
        (cert or {}).get('uploaded_at') or '',
    ]
    return ' | '.join(part for part in parts if part)


def live_certificate_matches_provider_cert(section_rows, provider, provider_cert):
    expected_ref = (provider_cert or {}).get('provider_reference') or ''
    expected_download_id = download_document_id_from_cert(provider_cert)
    expected_name = normalize_certificate_match_text((provider_cert or {}).get('certificate_name') or '')
    expected_row_text = normalize_certificate_match_text(provider_certificate_row_text(provider_cert))
    matches = []
    seen = set()
    for row in section_rows or []:
        values = row.get('values') or []
        links = row.get('links') or []
        values = [' '.join(str(v or '').replace('\xa0', ' ').split()).strip() for v in values]
        values = [value for value in values if value]
        if not values:
            continue
        joined = ' | '.join(values)
        download_document_id = ''
        for link in links:
            if isinstance(link, dict) and (link.get('download_document_id') or ''):
                download_document_id = str(link.get('download_document_id') or '').strip()
                break
        provider_ref = provider_certificate_reference(provider, joined, download_document_id)
        row_name = normalize_certificate_match_text(next((value for value in values if not parse_fobs_date(value)), values[0]))
        row_text = normalize_certificate_match_text(joined)
        ref_ok = bool(expected_ref and provider_ref == expected_ref)
        download_ok = bool(expected_download_id and download_document_id == expected_download_id)
        name_ok = bool(expected_name and row_name == expected_name)
        row_text_ok = bool(expected_row_text and (row_text == expected_row_text or expected_row_text in row_text or row_text in expected_row_text))
        if expected_download_id:
            matched = download_ok
        elif expected_ref:
            matched = ref_ok
        else:
            matched = (name_ok and row_text_ok) or row_text_ok
        if not matched:
            continue
        key = provider_ref or row_text
        if key in seen:
            continue
        seen.add(key)
        matches.append({
            'certificate_name': next((value for value in values if not parse_fobs_date(value)), values[0]),
            'provider_reference': provider_ref,
            'download_document_id': download_document_id,
            'download_url': f'DownloadDocument({download_document_id})' if download_document_id else choose_certificate_download_url(links),
            'row_text': joined,
            'raw_columns': values,
        })
    return matches


def click_fobs_delete_for_certificate(page, cert):
    document_id = re.sub(r'\D+', '', str(cert.get('download_document_id') or cert.get('download_url') or ''))
    row_text = cert.get('row_text') or cert.get('certificate_name') or ''
    page.on('dialog', lambda dialog: dialog.accept())
    handle = page.evaluate_handle("""
        ({documentId, rowText}) => {
            const clean = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim();
            const includesRowText = (el) => {
                const wanted = clean(rowText).toLowerCase();
                if (!wanted) return false;
                const text = clean(el.innerText || el.textContent).toLowerCase();
                return text && (text === wanted || text.includes(wanted.slice(0, 80)));
            };
            const rowForDocument = () => {
                if (documentId) {
                    for (const el of Array.from(document.querySelectorAll('[onclick]'))) {
                        const onclick = el.getAttribute('onclick') || '';
                        const m = onclick.match(/DownloadDocument\\s*\\(\\s*['\"]?(\\d+)['\"]?\\s*\\)/i);
                        if (m && m[1] === String(documentId)) {
                            return el.closest('tr,li,.row,.list-group-item,.certificate,.document-row,.card,.panel') || el.parentElement;
                        }
                    }
                }
                for (const row of Array.from(document.querySelectorAll('tr,li,.row,.list-group-item,.certificate,.document-row'))) {
                    if (includesRowText(row)) return row;
                }
                return null;
            };
            const row = rowForDocument();
            if (!row) return null;
            const controls = Array.from(row.querySelectorAll('a,button,input[type="button"],input[type="submit"]'));
            return controls.find((el) => {
                const text = clean(el.innerText || el.textContent || el.value || el.getAttribute('title') || el.getAttribute('aria-label') || '');
                const href = el.getAttribute('href') || '';
                const onclick = el.getAttribute('onclick') || '';
                const combined = `${text} ${href} ${onclick}`.toLowerCase();
                if (/downloaddocument\\s*\\(/i.test(onclick)) return false;
                if (/deletedocument\\s*\\(/i.test(onclick)) return true;
                return /\\b(delete|remove|trash)\\b/.test(combined);
            }) || null;
        }
    """, {'documentId': document_id, 'rowText': row_text})
    element = handle.as_element()
    if not element:
        return False, 'Could not find a delete control for the exact certificate row.'
    element.scroll_into_view_if_needed(timeout=3000)
    try:
        element.click(timeout=5000)
        try:
            page.wait_for_load_state('domcontentloaded', timeout=7000)
        except Exception:
            pass
    except Exception as exc:
        return False, f'Could not click provider delete control: {str(exc)[:140]}'
    page.wait_for_timeout(1500)
    return True, 'Provider delete action clicked.'


def wait_for_provider_certificate_absent(page, provider, expected, matcher, documents_url, attempts=10, delay_ms=1800):
    last_matches = []
    for attempt in range(max(1, int(attempts or 1))):
        if attempt:
            page.wait_for_timeout(delay_ms)
        try:
            page.goto(documents_url, wait_until='domcontentloaded', timeout=30000)
            page.wait_for_timeout(900)
        except Exception:
            page.wait_for_timeout(delay_ms)
        rows = certificate_rows_from_provider_page(page, provider)
        last_matches = matcher(rows, provider, expected)
        if not last_matches:
            return True, []
    return False, last_matches


def find_fobs_certificate_upload_form(page):
    return page.evaluate_handle("""
        () => {
            const clean = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim();
            const lower = (value) => clean(value).toLowerCase();
            const visible = (el) => {
                if (!el) return false;
                const style = window.getComputedStyle(el);
                const rect = el.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && rect.width >= 0 && rect.height >= 0;
            };
            const certificateText = (el) => /\\b(certificates?|dbs|licen[cs]e|insurance|safeguard|first aid|qualification|document)\\b/i.test(clean(el.innerText || el.textContent));
            const fileInputs = Array.from(document.querySelectorAll('input[type="file"]'));
            const scored = fileInputs.map((input, index) => {
                const form = input.closest('form');
                const container = input.closest('form,.ui-dialog,.modal,[role="dialog"],.card,.panel,.box,.row,.well,section,fieldset') || input.parentElement;
                const text = clean((container && (container.innerText || container.textContent)) || '');
                let score = 0;
                if (certificateText(container || input)) score += 4;
                if (/\\b(upload|add|choose file|file)\\b/i.test(text)) score += 2;
                if (form) score += 1;
                if (visible(input)) score += 1;
                return {input, form, container, score, index};
            }).filter((item) => item.score > 0 || fileInputs.length === 1);
            scored.sort((a, b) => (b.score - a.score) || (a.index - b.index));
            if (!scored.length) return {found: false, ambiguous: false, count: 0};
            if (scored.length > 1 && scored[0].score === scored[1].score) return {found: false, ambiguous: true, count: scored.length};
            return {found: true, ambiguous: false, count: scored.length, input: scored[0].input, form: scored[0].form || scored[0].container};
        }
    """)


def reveal_fobs_certificate_upload_form(page):
    handle = page.evaluate_handle("""
        () => {
            const clean = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim();
            const visible = (el) => {
                if (!el) return false;
                const style = window.getComputedStyle(el);
                const rect = el.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
            };
            const controls = Array.from(document.querySelectorAll('a,button,input[type="button"],input[type="submit"]')).filter(visible);
            const candidates = controls.map((el, index) => {
                const text = clean(el.innerText || el.textContent || el.value || el.getAttribute('title') || el.getAttribute('aria-label') || '');
                const href = el.getAttribute('href') || '';
                const onclick = el.getAttribute('onclick') || '';
                const combined = `${text} ${href} ${onclick}`.toLowerCase();
                if (/downloaddocument\\s*\\(/i.test(onclick)) return null;
                if (/\\b(delete|remove|archive|cancel)\\b/.test(combined)) return null;
                let score = 0;
                if (/\\b(add|new|upload|choose|attach)\\b/.test(combined)) score += 3;
                if (/\\b(certificate|document|file|dbs|licen[cs]e|insurance)\\b/.test(combined)) score += 2;
                if (!score) return null;
                return {el, score, index};
            }).filter(Boolean);
            candidates.sort((a, b) => (b.score - a.score) || (a.index - b.index));
            if (!candidates.length) return null;
            if (candidates.length > 1 && candidates[0].score === candidates[1].score) return {ambiguous: true, count: candidates.length};
            return {control: candidates[0].el, ambiguous: false, count: candidates.length};
        }
    """)
    try:
        ambiguous = bool(handle.get_property('ambiguous').json_value())
        if ambiguous:
            return False, 'More than one possible upload button was found.'
    except Exception:
        pass
    control = handle.get_property('control').as_element()
    if not control:
        return False, ''
    try:
        control.scroll_into_view_if_needed(timeout=3000)
        control.click(timeout=5000)
        try:
            page.wait_for_load_state('domcontentloaded', timeout=7000)
        except Exception:
            pass
        page.wait_for_timeout(1200)
        return True, 'Upload area opened.'
    except Exception as exc:
        return False, f'Could not open the upload area: {str(exc)[:140]}'


def prepare_fobs_upload_modal(page, expiry_date=''):
    try:
        page.evaluate("""
            () => {
                const direct = Array.from(document.querySelectorAll('[onclick]')).find((el) => {
                    const onclick = el.getAttribute('onclick') || '';
                    return /UploadDocumentBooking\\s*\\(/i.test(onclick);
                });
                if (direct) {
                    direct.click();
                    return true;
                }
                if (typeof UploadDocumentBooking === 'function') {
                    UploadDocumentBooking();
                    return true;
                }
                return false;
            }
        """)
        page.wait_for_timeout(900)
    except Exception:
        pass
    try:
        page.locator('#docTypeUploadCert').wait_for(state='attached', timeout=4000)
        page.select_option('#docTypeUploadCert', 'cert')
        page.evaluate("""
            () => {
                const select = document.querySelector('#docTypeUploadCert');
                if (!select) return false;
                select.value = 'cert';
                select.dispatchEvent(new Event('input', {bubbles: true}));
                select.dispatchEvent(new Event('change', {bubbles: true}));
                if (typeof UploadDocumentType === 'function') {
                    UploadDocumentType();
                }
                return true;
            }
        """)
        page.wait_for_timeout(2200)
    except Exception:
        pass
    if expiry_date:
        fill_fobs_expiry_fields(page, expiry_date)


def fobs_display_date(value):
    text = (value or '').strip()
    if not text:
        return ''
    try:
        return datetime.strptime(text[:10], '%Y-%m-%d').strftime('%d/%m/%Y')
    except Exception:
        return text


def fill_fobs_expiry_fields(page, expiry_date):
    expiry_display = fobs_display_date(expiry_date)
    if not expiry_date:
        return 0
    try:
        return int(page.evaluate("""
            ({expiryIso, expiryDisplay}) => {
                const clean = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim().toLowerCase();
                const visible = (el) => {
                    if (!el) return false;
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width >= 0 && rect.height >= 0;
                };
                const dialog = Array.from(document.querySelectorAll('.ui-dialog,.modal,[role="dialog"]')).find(visible) || document;
                const fields = Array.from(dialog.querySelectorAll('input,textarea')).filter((el) => !el.disabled && el.type !== 'file' && el.type !== 'hidden' && el.type !== 'submit' && el.type !== 'button');
                const labelFor = (el) => {
                    const id = el.getAttribute('id') || '';
                    const direct = id ? document.querySelector(`label[for="${CSS.escape(id)}"]`) : null;
                    const nearby = el.closest('label,.form-group,.field,.row,div,p');
                    return clean(`${direct ? direct.innerText : ''} ${nearby ? nearby.innerText : ''} ${el.name || ''} ${el.id || ''} ${el.placeholder || ''} ${el.getAttribute('aria-label') || ''}`);
                };
                const fill = (el) => {
                    el.focus && el.focus();
                    el.value = el.type === 'date' ? expiryIso : (expiryDisplay || expiryIso);
                    el.dispatchEvent(new Event('input', {bubbles: true}));
                    el.dispatchEvent(new Event('change', {bubbles: true}));
                    el.blur && el.blur();
                };
                let filled = 0;
                const directExpirySelectors = [
                    '#certDate', '#expiryDate', '#expirationDate', '#expiry_date', '#expiration_date',
                    '[name="certDate"]', '[name="expiryDate"]', '[name="expirationDate"]', '[name="expiry_date"]', '[name="expiration_date"]'
                ];
                for (const selector of directExpirySelectors) {
                    const el = dialog.querySelector(selector) || document.querySelector(selector);
                    if (el && !el.disabled && el.type !== 'file' && el.type !== 'hidden' && visible(el)) {
                        fill(el);
                        filled += 1;
                        break;
                    }
                }
                for (const el of fields) {
                    if (/expir|expiry|valid.*to|end date|renewal|cert.*date|certificate.*date/.test(labelFor(el))) {
                        fill(el);
                        filled += 1;
                    }
                }
                if (!filled) {
                    const emptyDate = fields.find((el) => (el.type === 'date' || /date/i.test(`${el.name || ''} ${el.id || ''} ${el.placeholder || ''}`)) && !el.value);
                    if (emptyDate) {
                        fill(emptyDate);
                        filled += 1;
                    }
                }
                if (!filled) {
                    const emptyText = fields.find((el) => ['text', 'search', ''].includes((el.type || '').toLowerCase()) && !el.value);
                    if (emptyText) {
                        fill(emptyText);
                        filled += 1;
                    }
                }
                return filled;
            }
        """, {'expiryIso': expiry_date, 'expiryDisplay': expiry_display}) or 0)
    except Exception:
        return 0


def fill_fobs_upload_form(page, form_handle, doc, provider_file_name):
    form_handle.evaluate("""
        (form, {title, expiry, expiryDisplay, providerFileName}) => {
            const clean = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim();
            const fields = Array.from(form.querySelectorAll('input,textarea,select')).filter((el) => !el.disabled && el.type !== 'file' && el.type !== 'hidden' && el.type !== 'submit' && el.type !== 'button');
            const labelFor = (el) => {
                const id = el.getAttribute('id') || '';
                const direct = id ? form.querySelector(`label[for="${CSS.escape(id)}"]`) : null;
                const parent = el.closest('label');
                const nearby = el.closest('.form-group,.field,.row,div');
                return clean(`${direct ? direct.innerText : ''} ${parent ? parent.innerText : ''} ${nearby ? nearby.innerText : ''} ${el.name || ''} ${el.id || ''} ${el.placeholder || ''}`).toLowerCase();
            };
            for (const el of fields) {
                const label = labelFor(el);
                if (el.tagName.toLowerCase() === 'select') {
                    const options = Array.from(el.options || []);
                    const match = options.find((opt) => /certificate|dbs|licen[cs]e|insurance|document/i.test(opt.text || opt.value || ''));
                    if (match && !el.value) el.value = match.value;
                    el.dispatchEvent(new Event('change', {bubbles: true}));
                    continue;
                }
                if (el.type === 'date') {
                    if (/expir|valid.*to|end/.test(label) && expiry) el.value = expiry;
                    el.dispatchEvent(new Event('input', {bubbles: true}));
                    el.dispatchEvent(new Event('change', {bubbles: true}));
                    continue;
                }
                if (/expir|valid.*to/.test(label) && expiry) el.value = expiryDisplay || expiry;
                else if (/title|name|description|document|certificate|file/.test(label) && !el.value) el.value = providerFileName || title;
                el.dispatchEvent(new Event('input', {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
            }
        }
    """, {
        'title': doc.get('title') or 'Certificate',
        'expiry': doc.get('expiry_date') or '',
        'expiryDisplay': fobs_display_date(doc.get('expiry_date') or ''),
        'providerFileName': provider_file_name or doc.get('title') or 'Certificate',
    })


def submit_fobs_upload_form(page, form_handle):
    page.on('dialog', lambda dialog: dialog.accept())
    submit = form_handle.evaluate_handle("""
        (form) => {
            const clean = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim();
            const visible = (el) => {
                if (!el) return false;
                const style = window.getComputedStyle(el);
                const rect = el.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
            };
            const root = form.closest('.ui-dialog,.modal,[role="dialog"]') || form;
            const controls = Array.from(root.querySelectorAll('button,input[type="submit"],input[type="button"],a')).filter(visible);
            const scored = controls.map((el, index) => {
                const text = clean(el.innerText || el.textContent || el.value || el.getAttribute('title') || el.getAttribute('aria-label') || '').toLowerCase();
                const href = (el.getAttribute('href') || '').toLowerCase();
                const onclick = (el.getAttribute('onclick') || '').toLowerCase();
                const combined = `${text} ${href} ${onclick}`;
                if (/cancel|close|delete|remove|back|dismiss/.test(combined)) return {el, score: -100, index};
                let score = 0;
                if (/^upload$/.test(text)) score += 100;
                else if (/\bupload\b/.test(combined)) score += 80;
                if (/\bsave\b|\bsubmit\b|\badd\b/.test(combined)) score += 30;
                if ((el.tagName || '').toLowerCase() === 'button') score += 5;
                if ((el.getAttribute('type') || '').toLowerCase() === 'submit') score += 5;
                return {el, score, index};
            }).filter((item) => item.score > 0);
            scored.sort((a, b) => b.score - a.score || a.index - b.index);
            return scored.length ? scored[0].el : null;
        }
    """)
    element = submit.as_element()
    try:
        if element:
            element.scroll_into_view_if_needed(timeout=3000)
            element.click(timeout=5000)
        else:
            form_handle.evaluate("(form) => form.requestSubmit ? form.requestSubmit() : form.submit()")
        try:
            page.wait_for_load_state('domcontentloaded', timeout=10000)
        except Exception:
            pass
        page.wait_for_timeout(3500)
        validation = provider_upload_validation_message(page)
        if validation:
            return False, f'FOBS did not accept the upload: {validation[:180]}'
        return True, 'Provider upload submitted.'
    except Exception as exc:
        return False, f'Could not submit provider upload: {str(exc)[:140]}'

def wait_for_uploaded_certificate(page, provider, link_doc, documents_url):
    last_error = ''
    for _ in range(10):
        try:
            page.goto(documents_url, wait_until='domcontentloaded', timeout=30000)
            page.wait_for_timeout(2200)
            rows = certificate_rows_from_provider_page(page, provider)
            matches = live_certificate_matches_for_doc(rows, provider, link_doc)
            if matches:
                return True
            validation = provider_upload_validation_message(page)
            if validation:
                last_error = validation[:180]
        except Exception as exc:
            last_error = str(exc)[:140]
        page.wait_for_timeout(1500)
    return False

def upload_certificate_to_provider(provider, link_doc):
    provider_id = provider_slug(provider.get('id') or provider.get('name') or '')
    provider_name = provider.get('name') or provider_id
    adapter = provider_certificate_adapter(provider)
    if not adapter.get('supports_upload'):
        raise RuntimeError(f'{adapter.get("label") or "This provider"} does not support certificate upload yet.')
    login_url = (provider.get('login_url') or '').strip()
    documents_url = (provider.get('documents_url') or derive_documents_url(login_url)).strip()
    if sync_playwright is None:
        raise RuntimeError('Playwright is not installed.')
    if not login_url or not documents_url:
        raise RuntimeError('No FOBS documents URL is configured for this provider.')
    stored = (link_doc.get('stored_filename') or '').strip()
    if not stored:
        raise RuntimeError('No local certificate file is stored in TrainerMate.')
    file_path = safe_document_path(stored)
    if not file_path.exists():
        raise RuntimeError('The local certificate file could not be found.')
    if file_path.stat().st_size <= 0:
        raise RuntimeError('The selected certificate file is empty. Please choose the saved certificate file and TrainerMate will send it again.')
    creds = get_provider_credentials(provider_id)
    username = (creds.get('username') or '').strip()
    password = (creds.get('password') or '').strip()
    if not username or not password:
        raise RuntimeError('No saved FOBS username/password for this provider.')

    provider_file_name = link_doc.get('provider_file_name') or safe_provider_document_filename(link_doc.get('title') or 'Certificate', link_doc.get('original_filename') or stored)
    set_certificate_scan_status(provider_id, 'running', f'Sending certificate to {provider_name}', 'TrainerMate is adding the certificate to FOBS.')
    p = sync_playwright().start()
    browser = None
    try:
        headless = os.getenv('TRAINERMATE_SHOW_BROWSER', '0') != '1'
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        page.goto(login_url, wait_until='domcontentloaded', timeout=30000)
        page.wait_for_timeout(800)
        try:
            if page.locator('#UserName').count() and page.locator('#Password').count():
                page.fill('#UserName', username)
                page.fill('#Password', password)
                page.click("button[type='submit'], input[type='submit']")
                page.wait_for_timeout(2000)
        except Exception:
            pass
        page.goto(documents_url, wait_until='domcontentloaded', timeout=30000)
        page.wait_for_timeout(1500)
        try:
            if page.locator('#UserName').count() and page.locator('#Password').count():
                page.fill('#UserName', username)
                page.fill('#Password', password)
                page.click("button[type='submit'], input[type='submit']")
                page.wait_for_timeout(2000)
                page.goto(documents_url, wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(1200)
        except Exception:
            pass

        if live_certificate_matches_for_doc(certificate_rows_from_provider_page(page, provider), provider, link_doc):
            mark_provider_upload_link(link_doc['link_id'], 'uploaded', f'Already present in {provider_name}.', provider_file_name)
            return True, f'Already present in {provider_name}.'

        set_certificate_scan_status(provider_id, 'running', f'Opening upload area for {provider_name}', 'Looking for the certificate upload area.')
        prepare_fobs_upload_modal(page, link_doc.get('expiry_date') or '')
        upload_ref = find_fobs_certificate_upload_form(page)
        found = bool(upload_ref.get_property('found').json_value())
        ambiguous = bool(upload_ref.get_property('ambiguous').json_value())
        if not found and not ambiguous:
            reveal_fobs_certificate_upload_form(page)
            prepare_fobs_upload_modal(page, link_doc.get('expiry_date') or '')
            upload_ref = find_fobs_certificate_upload_form(page)
            found = bool(upload_ref.get_property('found').json_value())
            ambiguous = bool(upload_ref.get_property('ambiguous').json_value())
        if not found and not ambiguous:
            raise RuntimeError(f'TrainerMate could not find the upload area in {provider_name}.')
        if ambiguous:
            raise RuntimeError(f'TrainerMate found more than one upload area in {provider_name}, so it paused and left the certificate unchanged.')
        input_el = upload_ref.get_property('input').as_element()
        form_el = upload_ref.get_property('form').as_element()
        if not input_el or not form_el:
            raise RuntimeError(f'TrainerMate could not safely use the upload form for {provider_name}.')
        temp_upload_path = None
        try:
            upload_mime = mimetypes.guess_type(provider_file_name)[0] or mimetypes.guess_type(str(file_path))[0] or 'application/octet-stream'
            input_el.set_input_files({
                'name': provider_file_name,
                'mimeType': upload_mime,
                'buffer': file_path.read_bytes(),
            })
            fill_fobs_upload_form(page, form_el, link_doc, provider_file_name)
            if link_doc.get('expiry_date'):
                # FOBS/FastForm often renders #certDate only after a certificate type is selected.
                # Wait and fill it before clicking Upload; never submit if it is missing.
                expiry_filled = 0
                for _ in range(6):
                    expiry_filled = fill_fobs_expiry_fields(page, link_doc.get('expiry_date') or '')
                    if expiry_filled:
                        break
                    page.wait_for_timeout(500)
                if not expiry_filled:
                    raise RuntimeError(f'TrainerMate could not find the FOBS expiry date field for {provider_name}, so it did not click Upload.')
            ready, ready_message = fobs_upload_form_ready(page, form_el, require_expiry=bool(link_doc.get('expiry_date')))
            if not ready:
                raise RuntimeError(ready_message)
            ok, message = submit_fobs_upload_form(page, form_el)
            if not ok:
                raise RuntimeError(message)
        finally:
            try:
                if temp_upload_path and temp_upload_path != file_path and temp_upload_path.exists():
                    temp_upload_path.unlink()
            except Exception:
                pass
        set_certificate_scan_status(provider_id, 'running', f'Checking upload to {provider_name}', 'Waiting for the certificate to appear in FOBS.')
        verified_live = wait_for_uploaded_certificate(page, provider, link_doc, documents_url)
        if not verified_live:
            clear_unverified_provider_upload_cache(provider, link_doc)
            raise RuntimeError(f'{provider_name} did not show the certificate after upload. TrainerMate has left this as not uploaded so you can try again.')
        upsert_provider_certificate_from_upload(provider, link_doc, provider_file_name)
        mark_provider_upload_link(link_doc['link_id'], 'uploaded', f'Sent to {provider_name}.', provider_file_name)
        set_certificate_scan_status(provider_id, 'complete', f'{provider_name} certificate sent', f'{provider_file_name} is now in FOBS.')
        return True, f'Sent to {provider_name}.'
    finally:
        try:
            if 'context' in locals() and context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            p.stop()
        except Exception:
            pass


def delete_certificate_from_provider(provider, link_doc):
    provider_id = provider_slug(provider.get('id') or provider.get('name') or '')
    provider_name = provider.get('name') or provider_id
    adapter = provider_certificate_adapter(provider)
    if not adapter.get('supports_delete'):
        raise RuntimeError(f'{adapter.get("label") or "This provider"} does not support certificate removal yet.')
    login_url = (provider.get('login_url') or '').strip()
    documents_url = (provider.get('documents_url') or derive_documents_url(login_url)).strip()
    if sync_playwright is None:
        raise RuntimeError('Playwright is not installed.')
    if not login_url or not documents_url:
        raise RuntimeError('No FOBS documents URL is configured for this provider.')
    creds = get_provider_credentials(provider_id)
    username = (creds.get('username') or '').strip()
    password = (creds.get('password') or '').strip()
    if not username or not password:
        raise RuntimeError('No saved FOBS username/password for this provider.')

    set_certificate_scan_status(provider_id, 'running', f'Removing from {provider_name}', 'TrainerMate is asking FOBS to remove the certificate.')
    p = sync_playwright().start()
    browser = None
    try:
        headless = os.getenv('TRAINERMATE_SHOW_BROWSER', '0') != '1'
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        page.goto(login_url, wait_until='domcontentloaded', timeout=30000)
        page.wait_for_timeout(800)
        try:
            if page.locator('#UserName').count() and page.locator('#Password').count():
                page.fill('#UserName', username)
                page.fill('#Password', password)
                page.click("button[type='submit'], input[type='submit']")
                page.wait_for_timeout(2000)
        except Exception:
            pass
        page.goto(documents_url, wait_until='domcontentloaded', timeout=30000)
        page.wait_for_timeout(1500)
        try:
            if page.locator('#UserName').count() and page.locator('#Password').count():
                page.fill('#UserName', username)
                page.fill('#Password', password)
                page.click("button[type='submit'], input[type='submit']")
                page.wait_for_timeout(2000)
                page.goto(documents_url, wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(1200)
        except Exception:
            pass

        rows = certificate_rows_from_provider_page(page, provider)
        matches = live_certificate_matches_for_doc(rows, provider, link_doc)
        if not matches:
            mark_provider_delete_link(link_doc['link_id'], 'removed', f'This certificate no longer appears in {provider_name}.')
            return True, f'{provider_name} no longer shows this certificate.'
        if len(matches) > 1:
            raise RuntimeError(f'TrainerMate found {len(matches)} likely matches in {provider_name}, so it paused and left them unchanged.')

        ok, message = click_fobs_delete_for_certificate(page, matches[0])
        if not ok:
            raise RuntimeError(message)

        set_certificate_scan_status(provider_id, 'running', f'Checking {provider_name}', 'Making sure the certificate has gone from FOBS.')
        removed, remaining = wait_for_provider_certificate_absent(
            page,
            provider,
            link_doc,
            live_certificate_matches_for_doc,
            documents_url,
        )
        if not removed:
            raise RuntimeError(f'TrainerMate could not confirm the certificate has gone from {provider_name}. Please refresh FOBS and try again.')
        mark_provider_delete_link(link_doc['link_id'], 'removed', f'Removed from {provider_name}.')
        return True, f'Removed from {provider_name}.'
    finally:
        try:
            if 'context' in locals() and context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            p.stop()
        except Exception:
            pass


def provider_certificate_row(certificate_id):
    conn = documents_conn()
    try:
        row = conn.execute(
            'SELECT * FROM provider_certificates WHERE id = ?',
            (certificate_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def remove_provider_certificate_cache_row(certificate_id):
    conn = documents_conn()
    try:
        conn.execute('DELETE FROM provider_certificates WHERE id = ?', (certificate_id,))
        conn.commit()
    finally:
        conn.close()


def mark_matching_local_links_missing_after_provider_delete(provider, provider_cert):
    provider_id = provider_slug((provider or {}).get('id') or (provider or {}).get('name') or '')
    provider_name = (provider or {}).get('name') or provider_id
    now = utc_now_text()
    conn = documents_conn()
    try:
        docs = [dict(row) for row in conn.execute("""
            SELECT d.*, l.id AS link_id
            FROM trainer_documents d
            JOIN document_provider_links l ON l.document_id = d.id
            WHERE d.status = 'active'
              AND l.provider_id = ?
        """, (provider_id,)).fetchall()]
        for doc in docs:
            if not certificate_matches_local_doc(provider_cert, doc):
                continue
            conn.execute("""
                UPDATE document_provider_links
                SET provider_status = 'missing',
                    provider_checked_at = ?,
                    pending_action = CASE
                        WHEN pending_action = '' THEN 'review_missing'
                        ELSE pending_action
                    END,
                    notes = ?,
                    updated_at = ?
                WHERE id = ?
            """, (now, f'This certificate was removed from {provider_name}.', now, doc['link_id']))
        conn.commit()
    finally:
        conn.close()


def delete_provider_certificate_from_provider(provider, provider_cert, cancel_key=''):
    provider_id = provider_slug(provider.get('id') or provider.get('name') or '')
    provider_name = provider.get('name') or provider_id
    adapter = provider_certificate_adapter(provider)
    if not adapter.get('supports_delete'):
        raise RuntimeError(f'{adapter.get("label") or "This provider"} does not support certificate removal yet.')
    login_url = (provider.get('login_url') or '').strip()
    documents_url = (provider.get('documents_url') or derive_documents_url(login_url)).strip()
    if sync_playwright is None:
        raise RuntimeError('Playwright is not installed.')
    if not login_url or not documents_url:
        raise RuntimeError('No FOBS documents URL is configured for this provider.')
    creds = get_provider_credentials(provider_id)
    username = (creds.get('username') or '').strip()
    password = (creds.get('password') or '').strip()
    if not username or not password:
        raise RuntimeError('No saved FOBS username/password for this provider.')

    set_certificate_scan_status(provider_id, 'running', f'Removing from {provider_name}', 'TrainerMate is asking FOBS to remove the certificate.')
    p = sync_playwright().start()
    browser = None
    try:
        headless = os.getenv('TRAINERMATE_SHOW_BROWSER', '0') != '1'
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        page.goto(login_url, wait_until='domcontentloaded', timeout=30000)
        page.wait_for_timeout(800)
        try:
            if page.locator('#UserName').count() and page.locator('#Password').count():
                page.fill('#UserName', username)
                page.fill('#Password', password)
                page.click("button[type='submit'], input[type='submit']")
                page.wait_for_timeout(2000)
        except Exception:
            pass
        page.goto(documents_url, wait_until='domcontentloaded', timeout=30000)
        page.wait_for_timeout(1500)
        try:
            if page.locator('#UserName').count() and page.locator('#Password').count():
                page.fill('#UserName', username)
                page.fill('#Password', password)
                page.click("button[type='submit'], input[type='submit']")
                page.wait_for_timeout(2000)
                page.goto(documents_url, wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(1200)
        except Exception:
            pass

        if provider_delete_cancel_requested(cancel_key):
            raise RuntimeError('Removal cancelled before FOBS was changed.')
        matches = live_certificate_matches_provider_cert(certificate_rows_from_provider_page(page, provider), provider, provider_cert)
        if not matches:
            remove_provider_certificate_cache_row(provider_cert.get('id'))
            mark_matching_local_links_missing_after_provider_delete(provider, provider_cert)
            set_certificate_scan_status(provider_id, 'complete', f'{provider_name} certificate removed', f'{provider_cert.get("certificate_name") or "Certificate"} is no longer in FOBS.')
            return True, f'{provider_name} no longer shows this certificate.'
        if len(matches) > 1:
            raise RuntimeError(f'TrainerMate found {len(matches)} likely matches in {provider_name}, so it paused and left them unchanged.')
        if provider_delete_cancel_requested(cancel_key):
            raise RuntimeError('Removal cancelled before FOBS was changed.')
        ok, message = click_fobs_delete_for_certificate(page, matches[0])
        if not ok:
            raise RuntimeError(message)
        set_certificate_scan_status(provider_id, 'running', f'Checking {provider_name}', 'Making sure the certificate has gone from FOBS.')
        removed, remaining = wait_for_provider_certificate_absent(
            page,
            provider,
            provider_cert,
            live_certificate_matches_provider_cert,
            documents_url,
        )
        if not removed:
            raise RuntimeError(f'TrainerMate could not confirm the certificate has gone from {provider_name}. Please refresh FOBS and try again.')
        remove_provider_certificate_cache_row(provider_cert.get('id'))
        mark_matching_local_links_missing_after_provider_delete(provider, provider_cert)
        set_certificate_scan_status(provider_id, 'complete', f'{provider_name} certificate removed', f'{provider_cert.get("certificate_name") or "Certificate"} removed from FOBS.')
        return True, f'Removed from {provider_name}.'
    finally:
        try:
            if 'context' in locals() and context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            p.stop()
        except Exception:
            pass


def start_provider_certificate_delete_async(certificate_id):
    if certificate_job_running():
        return False, 'TrainerMate is already checking certificates. This will only take a moment.'
    cert = provider_certificate_row(certificate_id)
    if not cert:
        return False, 'Certificate not found in the provider list.'
    providers = {p['id']: p for p in load_providers()}
    provider = providers.get(provider_slug(cert.get('provider_id') or ''))
    if not provider:
        return False, 'Provider is no longer configured.'

    def runner():
        provider_id = provider_slug(provider.get('id') or provider.get('name') or '')
        try:
            delete_provider_certificate_from_provider(provider, cert, certificate_id)
        except Exception as exc:
            message = str(exc)
            if message.startswith('Removal cancelled'):
                set_certificate_scan_status(provider_id, 'cancelled', 'Removal cancelled', 'FOBS was not changed if deletion had not already started.')
            else:
                set_certificate_scan_status(provider_id, 'error', f'Could not confirm removal from {provider.get("name") or provider_id}', message)
        finally:
            clear_provider_delete_cancel(certificate_id)

    clear_provider_delete_cancel(certificate_id)
    set_certificate_scan_status(cert.get('provider_id') or provider.get('id'), 'running', f'Removing from {cert.get("provider_name") or provider.get("name")}', 'TrainerMate is asking FOBS to remove the certificate.')
    threading.Thread(target=runner, daemon=True).start()
    return True, 'Removing certificate from provider.'


def start_provider_delete_async():
    if certificate_job_running():
        return False
    pending = provider_delete_pending_links()
    if not pending:
        set_certificate_scan_status('all', 'idle', 'No provider removals pending.', '')
        return False
    providers = {p['id']: p for p in load_providers()}

    def runner():
        total = 0
        errors = 0
        set_certificate_scan_status('all', 'running', 'Removing selected provider copies', 'TrainerMate is asking FOBS to remove the selected certificates.')
        for link_doc in pending:
            provider = providers.get(provider_slug(link_doc.get('provider_id') or ''))
            if not provider:
                errors += 1
                mark_provider_delete_link(link_doc.get('link_id'), 'review', 'Provider is no longer configured.')
                continue
            try:
                ok, _ = delete_certificate_from_provider(provider, link_doc)
                if ok:
                    total += 1
            except Exception as exc:
                errors += 1
                mark_provider_delete_link(link_doc.get('link_id'), 'review', str(exc))
                set_certificate_scan_status(provider.get('id') or provider.get('name'), 'error', f'Could not remove from {provider.get("name")}', str(exc))
        if errors:
            set_certificate_scan_status('all', 'error', 'Provider removal paused for checking', f'{total} removed. {errors} need a quick look.')
        else:
            set_certificate_scan_status('all', 'complete', 'Provider removals complete', f'{total} provider copy/copies removed.')

    set_certificate_scan_status('all', 'running', 'Starting provider removal', 'Please wait, certificate update in progress.')
    threading.Thread(target=runner, daemon=True).start()
    return True


def start_provider_upload_async():
    if certificate_job_running():
        return False
    pending = provider_upload_pending_links()
    if not pending:
        set_certificate_scan_status('all', 'idle', 'No provider uploads pending.', '')
        return False
    providers = {p['id']: p for p in load_providers()}

    def runner():
        global PROVIDER_UPLOAD_QUEUE_ACTIVE
        total = 0
        errors = 0
        try:
            set_certificate_scan_status('all', 'running', 'Sending certificates to providers', 'TrainerMate is adding the certificates to FOBS.')
            for link_doc in pending:
                provider = providers.get(provider_slug(link_doc.get('provider_id') or ''))
                if not provider:
                    errors += 1
                    mark_provider_upload_link(link_doc.get('link_id'), 'review', 'Provider is no longer configured.')
                    continue
                try:
                    ok, _ = upload_certificate_to_provider(provider, link_doc)
                    if ok:
                        total += 1
                except Exception as exc:
                    errors += 1
                    clear_unverified_provider_upload_cache(provider, link_doc)
                    mark_provider_upload_failed(
                        link_doc.get('link_id'),
                        provider.get('id') or provider.get('name'),
                        provider.get('name') or provider.get('id'),
                        str(exc),
                        link_doc.get('provider_file_name') or ''
                    )
            if errors:
                set_certificate_scan_status('all', 'error', 'Certificate send paused for checking', f'{total} sent. {errors} need a quick look.')
            else:
                set_certificate_scan_status('all', 'complete', 'Certificates sent', f'{total} provider copy/copies sent.')
        finally:
            with PROVIDER_UPLOAD_QUEUE_LOCK:
                PROVIDER_UPLOAD_QUEUE_ACTIVE = False

    set_certificate_scan_status('all', 'running', 'Starting certificate send', 'Please wait, certificate update in progress.')
    threading.Thread(target=runner, daemon=True).start()
    return True


def ensure_provider_upload_runs_soon():
    global PROVIDER_UPLOAD_QUEUE_ACTIVE
    if start_provider_upload_async():
        return 'started'
    if not provider_upload_pending_links():
        return 'none'
    with PROVIDER_UPLOAD_QUEUE_LOCK:
        if PROVIDER_UPLOAD_QUEUE_ACTIVE:
            return 'queued'
        PROVIDER_UPLOAD_QUEUE_ACTIVE = True

    def runner():
        global PROVIDER_UPLOAD_QUEUE_ACTIVE
        try:
            deadline = time.time() + 900
            while time.time() < deadline:
                if not certificate_job_running() and start_provider_upload_async():
                    return
                time.sleep(1.5)
        finally:
            with PROVIDER_UPLOAD_QUEUE_LOCK:
                PROVIDER_UPLOAD_QUEUE_ACTIVE = False

    threading.Thread(target=runner, daemon=True).start()
    return 'queued'


def scan_provider_certificates(provider):
    """Read-only scan of certificates currently shown in a provider FOBS portal."""
    provider_id = provider_slug(provider.get('id') or provider.get('name') or '')
    provider_name = provider.get('name') or provider_id
    login_url = (provider.get('login_url') or '').strip()
    documents_url = (provider.get('documents_url') or derive_documents_url(login_url)).strip()
    if sync_playwright is None:
        raise RuntimeError('Playwright is not installed.')
    if not login_url or not documents_url:
        raise RuntimeError('No FOBS documents URL is configured for this provider.')
    creds = get_provider_credentials(provider_id)
    username = (creds.get('username') or '').strip()
    password = (creds.get('password') or '').strip()
    if not username or not password:
        raise RuntimeError('No saved FOBS username/password for this provider.')

    set_certificate_scan_status(provider_id, 'running', f'Checking {provider_name}', 'Reading the FOBS certificate list.')
    p = sync_playwright().start()
    browser = None
    try:
        headless = os.getenv('TRAINERMATE_SHOW_BROWSER', '0') != '1'
        browser = p.chromium.launch(headless=headless)
        # Use an explicit browser context so Playwright's request API can
        # share the logged-in FOBS session when caching provider certificate files.
        # browser.new_page() creates an implicit context that can raise
        # "Please use browser.new_context()" when page.context.request is used.
        context = browser.new_context()
        page = context.new_page()
        page.goto(login_url, wait_until='domcontentloaded', timeout=30000)
        page.wait_for_timeout(800)
        try:
            if page.locator('#UserName').count() and page.locator('#Password').count():
                set_certificate_scan_status(provider_id, 'running', f'Signing into {provider_name}', 'Entering saved FOBS credentials.')
                page.fill('#UserName', username)
                page.fill('#Password', password)
                page.click("button[type='submit'], input[type='submit']")
                page.wait_for_timeout(2000)
        except Exception:
            pass

        set_certificate_scan_status(provider_id, 'running', f'Checking {provider_name} certificates', 'Reading certificates currently stored in FOBS.')
        page.goto(documents_url, wait_until='domcontentloaded', timeout=30000)
        page.wait_for_timeout(1800)
        try:
            if page.locator('#UserName').count() and page.locator('#Password').count():
                set_certificate_scan_status(provider_id, 'running', f'Signing into {provider_name}', 'FOBS asked for login again before showing certificates.')
                page.fill('#UserName', username)
                page.fill('#Password', password)
                page.click("button[type='submit'], input[type='submit']")
                page.wait_for_timeout(2000)
                set_certificate_scan_status(provider_id, 'running', f'Opening {provider_name} certificates', 'Loading the FOBS certificates page after login.')
                page.goto(documents_url, wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(1200)
        except Exception:
            pass

        certificates = []
        seen_certificate_rows = set()
        existing_cache = existing_provider_certificate_cache(provider_id)
        section_rows = certificate_rows_from_provider_page(page, provider)
        print(f"[CERTIFICATES] {provider_name}: extracted {len(section_rows)} candidate certificate row(s) from FOBS.")
        set_certificate_scan_status(provider_id, 'running', f'Reading {provider_name} certificates', f'{len(section_rows)} candidate certificate row(s) found. Checking files now.')
        for row in section_rows:
            if isinstance(row, dict):
                values = row.get('values') or []
                links = row.get('links') or []
            else:
                values = row
                links = []
            download_document_id = ''
            for link in links:
                if isinstance(link, dict) and (link.get('download_document_id') or ''):
                    download_document_id = str(link.get('download_document_id') or '').strip()
                    break
            values = [' '.join(str(v or '').replace('\xa0', ' ').split()).strip() for v in values]
            values = [v for v in values if v]
            if not values:
                continue
            joined = ' | '.join(values)
            if re.search(r'\b(invoice|invoices|course allocation|allocation|documents?)\b', joined, flags=re.IGNORECASE):
                continue
            row_key = normalize_certificate_match_text(joined)
            if row_key in seen_certificate_rows:
                continue
            seen_certificate_rows.add(row_key)
            # Prefer the first useful non-date cell as the display name.
            name = ''
            for cell in values:
                if parse_fobs_date(cell):
                    continue
                if len(cell) >= 2 and cell.lower() not in {'edit', 'delete', 'view', 'download'}:
                    name = cell
                    break
            if not name:
                name = values[0]
            dates = [parse_fobs_date(v) for v in values]
            dates = [d for d in dates if d]
            expiry = dates[-1] if dates else ''
            uploaded = dates[0] if len(dates) > 1 else ''
            provider_ref = provider_certificate_reference(provider, joined, download_document_id)
            cert = {
                'certificate_name': name[:240],
                'detected_type': detect_certificate_type(joined),
                'expiry_date': expiry,
                'uploaded_at': uploaded,
                'raw_columns': values,
                'provider_reference': provider_ref,
                'download_document_id': download_document_id,
                'certificate_adapter': provider_certificate_adapter(provider).get('id'),
                'download_url': f'DownloadDocument({download_document_id})' if download_document_id else choose_certificate_download_url(links),
            }
            cert_label = shorten_message(name, 72)
            cached_copy = existing_cache.get(provider_ref) or cached_provider_file_for_ref(provider, provider_ref) or {}
            cached_filename = cached_copy.get('cached_filename') or ''
            if cached_filename:
                try:
                    if safe_provider_cache_path(cached_filename).exists():
                        cert.update(cached_copy)
                        cert['download_status'] = cached_copy.get('download_status') or 'cached'
                        set_certificate_scan_status(provider_id, 'running', f'Checked {cert_label}', f'Already saved locally. No download needed for {provider_name}.')
                        certificates.append(cert)
                        continue
                except Exception:
                    pass
            if cert.get('download_url'):
                set_certificate_scan_status(provider_id, 'running', f'Downloading {cert_label}', f'Caching provider copy from {provider_name}.')
            else:
                set_certificate_scan_status(provider_id, 'running', f'Checking {cert_label}', f'No direct file link found yet for {provider_name}.')
            cert.update(cache_provider_certificate_file(page, provider, provider_ref, cert))
            status = cert.get('download_status') or 'checked'
            if status.startswith('cached'):
                set_certificate_scan_status(provider_id, 'running', f'Cached {cert_label}', 'Encrypted provider copy saved locally.')
            elif status not in {'no_link'}:
                set_certificate_scan_status(provider_id, 'running', f'Checked {cert_label}', f'Provider copy status: {status}.')
            certificates.append(cert)

        set_certificate_scan_status(provider_id, 'running', f'Saving {provider_name} certificates', f'{len(certificates)} certificate record(s) ready.')
        saved = save_provider_certificate_scan(provider, certificates, documents_url)
        if certificates and saved:
            update_document_provider_presence(provider, certificates)
            set_certificate_scan_status(provider_id, 'complete', f'{provider_name} certificates checked', f'{len(certificates)} certificate(s) found in FOBS.')
        else:
            raise RuntimeError('FOBS returned no certificate rows, so the existing cache was kept.')
        return len(certificates)
    finally:
        try:
            if 'context' in locals() and context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            p.stop()
        except Exception:
            pass



def mark_zoom_account_status(account_id, status='', last_verified_at='', note=''):
    account_id = (account_id or '').strip()
    if not account_id:
        return []
    accounts = load_zoom_accounts()
    changed = False
    for account in accounts:
        if (account.get('id') or '').strip() != account_id:
            continue
        if status:
            account['status'] = status
        if last_verified_at:
            account['last_verified_at'] = last_verified_at
        if note:
            account['last_error'] = note[:180]
        elif 'last_error' in account and status == 'connected':
            account.pop('last_error', None)
        changed = True
        break
    if changed:
        save_zoom_accounts(accounts)
    return accounts


def refresh_zoom_oauth_account(account, quiet=True):
    """Best-effort silent OAuth maintenance for one linked Zoom account.

    Zoom access tokens are short lived and refresh tokens can rotate. This
    proactively refreshes and stores the newest refresh token so normal syncs do
    not surprise the trainer with an avoidable reconnect. If Zoom has revoked the
    refresh token, user consent is still required; we only mark the account so
    the dashboard can explain it later.
    """
    if not isinstance(account, dict):
        return False, 'invalid account'
    account_id = (account.get('id') or '').strip()
    if not account_id:
        return False, 'missing account id'
    if not (ZOOM_CLIENT_ID and ZOOM_CLIENT_SECRET):
        mark_zoom_account_status(account_id, 'needs_reconnect', note='Zoom OAuth app credentials are missing.')
        return False, 'missing oauth config'
    refresh_token = get_zoom_oauth_token(account_id, 'refresh')
    if not refresh_token:
        mark_zoom_account_status(account_id, 'needs_reconnect', note='Zoom refresh token is missing.')
        return False, 'missing refresh token'
    try:
        response = requests.post(
            'https://zoom.us/oauth/token',
            params={'grant_type': 'refresh_token', 'refresh_token': refresh_token},
            auth=(ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET),
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        access_token = (data.get('access_token') or '').strip()
        refreshed_refresh_token = (data.get('refresh_token') or refresh_token).strip()
        if not access_token:
            raise RuntimeError('Zoom returned no access token')
        set_zoom_tokens(account_id, access_token, refreshed_refresh_token)
        mark_zoom_account_status(account_id, 'connected', last_verified_at=utc_now_text())
        if not quiet:
            print(f'[ZOOM] Refreshed linked Zoom account {account_id}.')
        return True, 'refreshed'
    except Exception as exc:
        message = str(exc)[:180]
        mark_zoom_account_status(account_id, 'needs_reconnect', note=message)
        if not quiet:
            print(f'[ZOOM] Linked Zoom account {account_id} needs reconnect: {message}')
        return False, message


def start_startup_zoom_health_check_once(accounts=None):
    global STARTUP_ZOOM_HEALTH_CHECK_STARTED
    if not STARTUP_ZOOM_HEALTH_CHECK_ENABLED:
        set_startup_zoom_health_status('skipped', 'Zoom check skipped', 'Startup Zoom health checks are turned off.')
        return False
    with STARTUP_ZOOM_HEALTH_CHECK_LOCK:
        if STARTUP_ZOOM_HEALTH_CHECK_STARTED:
            return False
        STARTUP_ZOOM_HEALTH_CHECK_STARTED = True

    linked_accounts = [a for a in list(accounts or load_zoom_accounts()) if isinstance(a, dict) and (a.get('id') or '').strip()]
    if not linked_accounts:
        set_startup_zoom_health_status('skipped', 'No Zoom accounts to check', 'TrainerMate will ask if a Zoom account is needed.')
        return False
    set_startup_zoom_health_status('waiting', 'Checking Zoom shortly', 'TrainerMate will quietly make sure connected Zoom accounts are still healthy.')

    def runner():
        errors = 0
        try:
            if STARTUP_ZOOM_HEALTH_CHECK_DELAY_SECONDS > 0:
                time.sleep(STARTUP_ZOOM_HEALTH_CHECK_DELAY_SECONDS)
            set_startup_zoom_health_status('running', 'Checking Zoom', 'Refreshing saved Zoom access quietly if needed.')
            for account in linked_accounts:
                ok, _ = refresh_zoom_oauth_account(account, quiet=True)
                if not ok:
                    errors += 1
            if errors:
                set_startup_zoom_health_status('warning', 'Zoom needs attention', f'{errors} Zoom account(s) may need reconnecting.')
            else:
                set_startup_zoom_health_status('complete', 'Zoom ready', f'{len(linked_accounts)} Zoom account(s) checked.')
        except Exception as exc:
            set_startup_zoom_health_status('warning', 'Zoom needs attention', str(exc)[:180])

    threading.Thread(target=runner, daemon=True).start()
    return True

def start_certificate_scan_async(provider_id='all'):
    if certificate_job_running():
        return False
    # Certificate refresh is read-only, so scan every configured provider.
    # Course/Zoom sync still uses provider_options() to stay limited to active providers.
    providers = load_providers()
    if provider_id and provider_id != 'all':
        wanted = provider_slug(provider_id)
        providers = [p for p in providers if provider_slug(p.get('id') or p.get('name')) == wanted]
    if not providers:
        set_certificate_scan_status(provider_id or 'all', 'idle', 'No providers to scan.', '')
        return False

    def runner():
        set_certificate_scan_status(provider_id or 'all', 'running', 'Checking provider certificates', 'Refreshing certificates shown in FOBS.')
        total = 0
        errors = 0
        for provider in providers:
            pid = provider_slug(provider.get('id') or provider.get('name') or '')
            try:
                total += scan_provider_certificates(provider)
            except Exception as exc:
                errors += 1
                set_certificate_scan_status(pid, 'error', f'Could not check {provider.get("name") or pid} certificates', str(exc))
        if errors:
            set_certificate_scan_status(provider_id or 'all', 'error', 'Certificate refresh finished with warnings', f'{total} certificate(s) found. {errors} provider(s) need checking.')
        else:
            set_certificate_scan_status(provider_id or 'all', 'complete', 'Provider certificates refreshed', f'{total} certificate(s) found across {len(providers)} provider(s).')

    set_certificate_scan_status(provider_id or 'all', 'running', 'Starting certificate check', 'Please wait, certificate check in progress.')
    threading.Thread(target=runner, daemon=True).start()
    return True


def start_startup_certificate_scan_once(providers=None):
    global STARTUP_CERTIFICATE_SCAN_STARTED
    if not STARTUP_CERTIFICATE_SCAN_ENABLED:
        return False
    with STARTUP_CERTIFICATE_SCAN_LOCK:
        if STARTUP_CERTIFICATE_SCAN_STARTED or certificate_scan_snapshot().get('running'):
            return False

    eligible = []
    for provider in list(providers or load_providers()):
        if not provider.get('active', True):
            continue
        provider_id = provider_slug(provider.get('id') or provider.get('name') or '')
        login_url = (provider.get('login_url') or '').strip()
        documents_url = (provider.get('documents_url') or derive_documents_url(login_url)).strip()
        creds = get_provider_credentials(provider_id)
        if login_url and documents_url and (creds.get('username') or '').strip() and (creds.get('password') or '').strip():
            eligible.append(provider)

    if not eligible:
        return False
    with STARTUP_CERTIFICATE_SCAN_LOCK:
        if STARTUP_CERTIFICATE_SCAN_STARTED or certificate_scan_snapshot().get('running'):
            return False
        STARTUP_CERTIFICATE_SCAN_STARTED = True

    def runner():
        if STARTUP_CERTIFICATE_SCAN_DELAY_SECONDS > 0:
            time.sleep(STARTUP_CERTIFICATE_SCAN_DELAY_SECONDS)
        set_certificate_scan_status('all', 'running', 'Checking certificates', 'TrainerMate is refreshing the active provider certificate lists.')
        total = 0
        errors = 0
        for provider in eligible:
            pid = provider_slug(provider.get('id') or provider.get('name') or '')
            try:
                total += scan_provider_certificates(provider)
            except Exception as exc:
                errors += 1
                set_certificate_scan_status(pid, 'error', f'Could not check {provider.get("name") or pid} certificates', str(exc))
        if errors:
            set_certificate_scan_status('all', 'error', 'Certificate check finished with warnings', f'{total} certificate(s) found. {errors} provider(s) need checking.')
        else:
            set_certificate_scan_status('all', 'complete', 'Certificates checked', f'{total} certificate(s) found across {len(eligible)} active provider(s).')

    threading.Thread(target=runner, daemon=True).start()
    return True


def safe_document_filename(filename):
    name = os.path.basename((filename or '').strip()).replace('\\', '_').replace('/', '_')
    cleaned = ''.join(ch if ch.isalnum() or ch in (' ', '.', '-', '_') else '_' for ch in name).strip()
    return cleaned or 'document'


def safe_provider_document_filename(title, original_filename='document.pdf'):
    base = safe_document_filename(title or 'document')
    ext = Path(safe_document_filename(original_filename or '')).suffix.lower()
    if ext not in ALLOWED_DOCUMENT_EXTENSIONS:
        ext = '.pdf'
    if not base.lower().endswith(ext):
        base = f'{base}{ext}'
    return base


def ensure_secure_documents_dir():
    DOCUMENTS_DIR.mkdir(mode=0o700, exist_ok=True)
    (DOCUMENTS_DIR / 'provider_cache').mkdir(mode=0o700, exist_ok=True)
    try:
        os.chmod(DOCUMENTS_DIR, 0o700)
        os.chmod(DOCUMENTS_DIR / 'provider_cache', 0o700)
    except Exception:
        pass


def safe_document_path(stored_filename):
    clean = safe_document_filename(stored_filename)
    root = DOCUMENTS_DIR.resolve()
    path = (root / clean).resolve()
    if root != path and root not in path.parents:
        raise ValueError('Unsafe document path.')
    return path


def safe_provider_cache_path(stored_filename):
    clean = safe_document_filename(stored_filename)
    root = (DOCUMENTS_DIR / 'provider_cache').resolve()
    root.mkdir(mode=0o700, exist_ok=True)
    path = (root / clean).resolve()
    if root != path and root not in path.parents:
        raise ValueError('Unsafe provider cache path.')
    return path


def allowed_document_upload(file_storage):
    filename = safe_document_filename(getattr(file_storage, 'filename', '') or '')
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_DOCUMENT_EXTENSIONS:
        return False, 'That file type cannot be added. Please choose a PDF, Word document, spreadsheet, image or OpenDocument certificate.'
    mimetype = (getattr(file_storage, 'mimetype', '') or mimetypes.guess_type(filename)[0] or '').lower()
    if mimetype and mimetype not in ALLOWED_DOCUMENT_MIME_TYPES and mimetype != 'application/octet-stream':
        return False, 'That file does not look like a certificate file TrainerMate can store. Please choose a PDF, Word document, spreadsheet, image or OpenDocument certificate.'
    return True, ''


def document_row(document_id):
    conn = documents_conn()
    try:
        row = conn.execute("""
            SELECT *
            FROM trainer_documents
            WHERE id = ? AND status <> 'deleted'
        """, (document_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def document_type_label(value):
    return DOCUMENT_TYPE_LABELS.get((value or '').strip(), 'Other')


def parse_date_value(value):
    value = (value or '').strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, '%Y-%m-%d')
    except Exception:
        return None


def document_health(doc):
    if (doc.get('status') or '') != 'active':
        return 'archived', 'Deleted'
    expiry = parse_date_value(doc.get('expiry_date'))
    if not expiry:
        return 'no_expiry', 'No expiry'
    days = (expiry.date() - datetime.now().date()).days
    if days < 0:
        return 'expired', f'Expired {abs(days)} day(s) ago'
    if days <= DOCUMENT_WARNING_DAYS:
        return 'expiring', f'Expires in {days} day(s)'
    return 'ok', f'Valid until {expiry.strftime("%d/%m/%Y")}'


def provider_requirement_key(document_type):
    return (document_type or 'other').strip() or 'other'


def load_documents():
    if not COURSES_DB_PATH.exists():
        return []
    conn = documents_conn()
    try:
        docs = [dict(row) for row in conn.execute("""
            SELECT *
            FROM trainer_documents
            WHERE status <> 'deleted'
            ORDER BY
                CASE status WHEN 'active' THEN 0 ELSE 1 END,
                COALESCE(expiry_date, '9999-12-31') ASC,
                title ASC
        """).fetchall()]
        for doc in docs:
            links = [dict(row) for row in conn.execute("""
                SELECT *
                FROM document_provider_links
                WHERE document_id = ?
                ORDER BY provider_name ASC
            """, (doc['id'],)).fetchall()]
            doc['links'] = links
            doc['type_label'] = document_type_label(doc.get('document_type'))
            doc['health_key'], doc['health_label'] = document_health(doc)
            doc['provider_names'] = ', '.join(link.get('provider_name') or '' for link in links) or 'Not assigned'
            doc['pending_count'] = sum(1 for link in links if (link.get('pending_action') or '').strip())
        return docs
    finally:
        conn.close()


def document_expiry_warning_key(doc):
    title = normalize_certificate_match_text(doc.get('title') or doc.get('original_filename') or '')
    original = normalize_certificate_match_text(doc.get('original_filename') or '')
    expiry = (doc.get('expiry_date') or '').strip()
    return (title or original or str(doc.get('id') or ''), expiry)


def document_link_provider_names(doc):
    names = []
    seen = set()
    for link in doc.get('links') or []:
        name = (link.get('provider_name') or link.get('provider_id') or '').strip()
        key = provider_slug(name)
        if name and key not in seen:
            names.append(name)
            seen.add(key)
    return names


def document_summary(documents):
    summary = {'total': 0, 'active': 0, 'expired': 0, 'expiring': 0, 'pending': 0, 'needs_attention': 0}
    warning_keys = set()
    for doc in documents:
        summary['total'] += 1
        if (doc.get('status') or '') == 'active':
            summary['active'] += 1
        warning_key = document_expiry_warning_key(doc)
        count_warning = doc.get('health_key') in {'expired', 'expiring'} and warning_key not in warning_keys
        if count_warning:
            warning_keys.add(warning_key)
            if doc.get('health_key') == 'expired':
                summary['expired'] += 1
            elif doc.get('health_key') == 'expiring':
                summary['expiring'] += 1
        if doc.get('pending_count'):
            summary['pending'] += int(doc.get('pending_count') or 0)
        if count_warning or doc.get('pending_count'):
            summary['needs_attention'] += 1
    return summary


def document_expiry_warnings(documents, limit=4):
    grouped = {}
    for doc in documents or []:
        if doc.get('health_key') not in {'expired', 'expiring'}:
            continue
        key = document_expiry_warning_key(doc)
        item = grouped.setdefault(key, {
            'title': doc.get('title') or doc.get('original_filename') or 'Certificate',
            'message': doc.get('health_label') or '',
            'expiry_date': doc.get('expiry_date') or '',
            'level': doc.get('health_key') or '',
            'providers': [],
            'provider_keys': set(),
        })
        if item.get('level') != 'expired' and doc.get('health_key') == 'expired':
            item['level'] = 'expired'
            item['message'] = doc.get('health_label') or item.get('message') or ''
        for provider_name in document_link_provider_names(doc):
            provider_key = provider_slug(provider_name)
            if provider_key and provider_key not in item['provider_keys']:
                item['providers'].append(provider_name)
                item['provider_keys'].add(provider_key)
    warnings = []
    for item in grouped.values():
        item.pop('provider_keys', None)
        item['provider_text'] = ', '.join(item.get('providers') or []) or 'No provider assigned'
        warnings.append(item)
    warnings.sort(key=lambda item: (0 if item.get('level') == 'expired' else 1, item.get('expiry_date') or '9999-12-31', item.get('title') or ''))
    return warnings[:max(0, int(limit or 0))]


def certificate_attention_items(documents):
    """Build notification-only certificate prompts for the dashboard.

    Certificate alerts are deliberately non-actioning: they can be dismissed, and
    if the user has seen them once they auto-hide when the page is left. Uploads
    should only be started by the initial Add/Upload certificate flow.

    Multiple provider notices for the same TrainerMate certificate are collapsed
    into one row so future providers do not create alert spam.
    """
    dismissed = load_alert_ack()
    by_document = {}

    def item_priority(item):
        return (item.get('checked_at') or '', item.get('updated_at') or '')

    for doc in documents:
        if (doc.get('status') or '') != 'active':
            continue
        links = doc.get('links') or []
        for link in links:
            pending_action = (link.get('pending_action') or '').strip()
            provider_status = (link.get('provider_status') or '').strip()
            if pending_action == 'dismissed_missing':
                continue
            if provider_status != 'missing' and pending_action != 'upload':
                continue

            alert_id = certificate_notice_alert_id(link, doc)
            if alert_id in dismissed:
                continue

            provider_id = (link.get('provider_id') or '').strip()
            provider_name = (link.get('provider_name') or provider_id or 'provider').strip()
            document_id = doc.get('id') or provider_slug(doc.get('title') or 'certificate')
            group = by_document.setdefault(document_id, {
                'document': doc,
                'link': link,
                'document_id': document_id,
                'link_id': link.get('id') or '',
                'alert_id': alert_id,
                'alert_ids': [],
                'certificate_name': doc.get('title') or 'Certificate',
                'provider_id': provider_id,
                'provider_name': provider_name,
                'provider_names': [],
                'provider_names_text': provider_name,
                'message': '',
                'checked_at': link.get('provider_checked_at') or '',
                'updated_at': link.get('updated_at') or doc.get('updated_at') or '',
                'matching_elsewhere': [],
                'pending_action': pending_action,
                'notice_only': True,
                'has_upload_notice': False,
                'has_missing_notice': False,
            })

            if alert_id not in group['alert_ids']:
                group['alert_ids'].append(alert_id)
            if provider_name and provider_name not in group['provider_names']:
                group['provider_names'].append(provider_name)
            if pending_action == 'upload':
                group['has_upload_notice'] = True
            if provider_status == 'missing':
                group['has_missing_notice'] = True

            candidate = {
                'checked_at': link.get('provider_checked_at') or '',
                'updated_at': link.get('updated_at') or doc.get('updated_at') or '',
                'link': link,
                'link_id': link.get('id') or '',
                'provider_id': provider_id,
                'provider_name': provider_name,
                'pending_action': pending_action,
            }
            if item_priority(candidate) > item_priority(group):
                group['link'] = link
                group['link_id'] = link.get('id') or group.get('link_id') or ''
                group['alert_id'] = alert_id
                group['provider_id'] = provider_id
                group['provider_name'] = provider_name
                group['checked_at'] = candidate['checked_at']
                group['updated_at'] = candidate['updated_at']
                group['pending_action'] = pending_action

            existing_match_ids = {m.get('link_id') for m in group.get('matching_elsewhere') or []}
            for other in links:
                if other.get('id') == link.get('id'):
                    continue
                if (other.get('provider_status') or '').strip() != 'in_sync':
                    continue
                match = {
                    'link_id': other.get('id') or '',
                    'provider_name': other.get('provider_name') or other.get('provider_id') or 'provider',
                    'provider_file_name': other.get('provider_file_name') or doc.get('title') or 'Certificate',
                    'checked_at': other.get('provider_checked_at') or '',
                }
                if match.get('link_id') not in existing_match_ids:
                    group['matching_elsewhere'].append(match)
                    existing_match_ids.add(match.get('link_id'))

    items = []
    for group in by_document.values():
        provider_names = sorted(group.get('provider_names') or [], key=lambda value: value.lower())
        provider_text = ', '.join(provider_names) if provider_names else (group.get('provider_name') or 'the selected providers')
        group['provider_names'] = provider_names
        group['provider_names_text'] = provider_text
        if group.get('has_missing_notice'):
            group['message'] = f"This certificate no longer appears in: {provider_text}."
        elif group.get('has_upload_notice'):
            group['message'] = f"This certificate is queued for: {provider_text}. Uploads only start from the main certificate upload flow."
        else:
            group['message'] = f"This certificate needs attention in: {provider_text}."
        items.append(group)

    return sorted(
        items,
        key=lambda item: (
            (item.get('certificate_name') or '').lower(),
            (item.get('provider_names_text') or '').lower(),
        )
    )

def provider_document_requirements(provider_id):
    return DEFAULT_PROVIDER_DOCUMENT_REQUIREMENTS.get(provider_id) or DEFAULT_PROVIDER_DOCUMENT_REQUIREMENTS['default']


def provider_document_health(documents, providers):
    active_docs = [doc for doc in documents if (doc.get('status') or '') == 'active']
    out = []
    for provider in providers:
        provider_id = provider.get('id')
        required = provider_document_requirements(provider_id)
        rows = []
        missing_count = 0
        warning_count = 0
        for doc_type in required:
            matching_docs = [
                doc for doc in active_docs
                if doc.get('document_type') == doc_type
                and any(link.get('provider_id') == provider_id for link in doc.get('links', []))
            ]
            if not matching_docs:
                rows.append({
                    'label': document_type_label(doc_type),
                    'status': 'missing',
                    'message': 'Missing from TrainerMate',
                })
                missing_count += 1
                continue
            best = matching_docs[0]
            key = best.get('health_key') or 'unknown'
            if key in {'expired', 'expiring'}:
                warning_count += 1
            rows.append({
                'label': document_type_label(doc_type),
                'status': key,
                'message': best.get('health_label') or '',
                'document': best,
            })
        out.append({
            'provider': provider,
            'rows': rows,
            'missing_count': missing_count,
            'warning_count': warning_count,
        })
    return out


def upsert_document_provider_links(conn, document_id, provider_ids, pending_action='upload', provider_file_name=''):
    providers = {p['id']: p for p in load_providers()}
    now = utc_now_text()
    seen = set()
    for provider_id in provider_ids:
        provider_id = provider_slug(provider_id)
        if not provider_id or provider_id in seen:
            continue
        seen.add(provider_id)
        provider = providers.get(provider_id)
        if not provider:
            continue
        conn.execute("""
            INSERT INTO document_provider_links (
                id, document_id, provider_id, provider_name,
                provider_status, provider_file_name, pending_action, updated_at
            )
            VALUES (?, ?, ?, ?, 'not_checked', ?, ?, ?)
            ON CONFLICT(document_id, provider_id)
            DO UPDATE SET
                provider_name = excluded.provider_name,
                provider_file_name = CASE
                    WHEN COALESCE(document_provider_links.provider_file_name, '') = '' THEN excluded.provider_file_name
                    ELSE document_provider_links.provider_file_name
                END,
                pending_action = CASE
                    WHEN document_provider_links.provider_status IN ('in_sync', 'uploaded') THEN document_provider_links.pending_action
                    ELSE excluded.pending_action
                END,
                updated_at = excluded.updated_at
        """, (str(uuid.uuid4()), document_id, provider_id, provider['name'], provider_file_name, pending_action, now))


def valid_document_provider_ids(provider_ids):
    providers = {p['id'] for p in load_providers()}
    valid = []
    seen = set()
    for raw_provider_id in provider_ids or []:
        provider_id = provider_slug(raw_provider_id)
        if not provider_id or provider_id in seen or provider_id not in providers:
            continue
        valid.append(provider_id)
        seen.add(provider_id)
    return valid


def add_document_from_form(form, file_storage):
    if not file_storage or not getattr(file_storage, 'filename', ''):
        return False, 'Choose a file to add.'
    provider_ids = valid_document_provider_ids(form.getlist('provider_ids'))
    if not provider_ids:
        return False, 'Choose at least one provider for this certificate.'
    allowed, reason = allowed_document_upload(file_storage)
    if not allowed:
        return False, reason

    ensure_secure_documents_dir()
    doc_id = str(uuid.uuid4())
    original = safe_document_filename(file_storage.filename)
    title = Path(original).stem.strip() or original or 'Certificate'
    document_type = 'other'
    ext = Path(original).suffix.lower()
    stored = f"{doc_id}{ext if ext in ALLOWED_DOCUMENT_EXTENSIONS else '.bin'}"
    target = safe_document_path(stored)
    file_storage.save(str(target))
    try:
        if target.stat().st_size <= 0:
            try:
                target.unlink()
            except Exception:
                pass
            return False, 'That file is empty. Please choose the saved certificate file and try again.'
    except Exception:
        return False, 'TrainerMate could not read that file. Please choose the saved certificate file and try again.'
    provider_file_name = safe_provider_document_filename(title, original)
    now = utc_now_text()

    conn = documents_conn()
    try:
        conn.execute("""
            INSERT INTO trainer_documents (
                id, title, document_type, original_filename, stored_filename,
                file_path, issue_date, expiry_date, notes, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
        """, (
            doc_id,
            title,
            document_type,
            original,
            stored,
            str(target),
            '',
            (form.get('expiry_date') or '').strip(),
            '',
            now,
            now,
        ))
        upsert_document_provider_links(conn, doc_id, provider_ids, pending_action='upload', provider_file_name=provider_file_name)
        conn.commit()
        return True, 'Certificate added to TrainerMate.'
    finally:
        conn.close()


def repair_pending_upload_presence():
    conn = documents_conn()
    try:
        now = utc_now_text()
        conn.execute("""
            UPDATE document_provider_links
            SET provider_status = 'not_checked',
                notes = CASE
                    WHEN notes LIKE 'This certificate no longer appears in %' THEN ''
                    ELSE notes
                END,
                updated_at = ?
            WHERE pending_action = 'upload'
              AND provider_status = 'missing'
        """, (now,))
        conn.commit()
    finally:
        conn.close()


def archive_document(document_id):
    conn = documents_conn()
    try:
        now = utc_now_text()
        cur = conn.execute("""
            UPDATE trainer_documents
            SET status = 'deleted',
                updated_at = ?
            WHERE id = ?
        """, (now, document_id))
        conn.execute("""
            UPDATE document_provider_links
            SET updated_at = ?
            WHERE document_id = ?
        """, (now, document_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_document_status(document_id, status):
    if status not in {'active', 'archived', 'deleted'}:
        return False
    conn = documents_conn()
    try:
        now = utc_now_text()
        cur = conn.execute("""
            UPDATE trainer_documents
            SET status = ?,
                updated_at = ?
            WHERE id = ?
        """, (status, now, document_id))
        conn.execute("""
            UPDATE document_provider_links
            SET updated_at = ?
            WHERE document_id = ?
        """, (now, document_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def stage_document_link_reupload(link_id):
    conn = documents_conn()
    try:
        now = utc_now_text()
        cur = conn.execute("""
            UPDATE document_provider_links
            SET pending_action = 'upload',
                updated_at = ?
            WHERE id = ?
        """, (now, link_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def dismiss_missing_certificate_link(link_id):
    conn = documents_conn()
    try:
        now = utc_now_text()
        cur = conn.execute("""
            UPDATE document_provider_links
            SET pending_action = 'dismissed_missing',
                updated_at = ?
            WHERE id = ?
        """, (now, link_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def stage_document_links_for_provider_delete(link_ids):
    clean_ids = [str(link_id or '').strip() for link_id in link_ids if str(link_id or '').strip()]
    if not clean_ids:
        return 0
    conn = documents_conn()
    try:
        now = utc_now_text()
        staged = 0
        for link_id in clean_ids:
            cur = conn.execute("""
                UPDATE document_provider_links
                SET pending_action = 'delete_provider_copy',
                    updated_at = ?
                WHERE id = ?
            """, (now, link_id))
            staged += cur.rowcount
        conn.commit()
        return staged
    finally:
        conn.close()


def run_document_healthcheck():
    docs = load_documents()
    providers = provider_options()
    conn = documents_conn()
    try:
        now = utc_now_text()
        active_by_type_and_provider = set()
        for doc in docs:
            if (doc.get('status') or '') != 'active':
                continue
            for link in doc.get('links', []):
                active_by_type_and_provider.add((doc.get('document_type'), link.get('provider_id')))

        for doc in docs:
            if (doc.get('status') or '') != 'active':
                continue
            link_provider_ids = {link.get('provider_id') for link in doc.get('links', [])}
            assigned_ids = link_provider_ids
            upsert_document_provider_links(conn, doc['id'], assigned_ids, pending_action='upload')
            health_key, _ = document_health(doc)
            if health_key in {'expired', 'expiring'}:
                conn.execute("""
                    UPDATE document_provider_links
                    SET pending_action = CASE
                            WHEN pending_action = '' THEN 'review_expiry'
                            ELSE pending_action
                        END,
                        updated_at = ?
                    WHERE document_id = ?
                """, (now, doc['id']))
        for provider in providers:
            for doc_type in provider_document_requirements(provider.get('id')):
                if (doc_type, provider.get('id')) not in active_by_type_and_provider:
                    # Missing requirements are reported in the provider checklist.
                    pass
        conn.commit()
        return True, 'Certificate check complete. TrainerMate will show anything that needs attention.'
    finally:
        conn.close()


def prepare_document_provider_updates():
    docs = load_documents()
    conn = documents_conn()
    try:
        now = utc_now_text()
        staged = 0
        for doc in docs:
            if (doc.get('status') or '') != 'active':
                continue
            for link in doc.get('links', []):
                if (link.get('pending_action') or '').strip():
                    continue
                if (link.get('provider_status') or 'not_checked') in {'not_checked', 'missing', ''}:
                    conn.execute("""
                        UPDATE document_provider_links
                        SET pending_action = 'upload',
                            updated_at = ?
                        WHERE id = ?
                    """, (now, link['id']))
                    staged += 1
        conn.commit()
        return True, f'{staged} provider update(s) ready to send.'
    finally:
        conn.close()


def load_courses(provider_filter='all'):
    """Load the full known future schedule for display.

    Sync scope controls what the bot checks; it must not control what the
    dashboard shows. Older builds could mark out-of-scope rows inactive after a
    short sync, so this intentionally keeps future rows visible unless they were
    explicitly retired as a provider replacement.
    """
    if not COURSES_DB_PATH.exists():
        return []
    conn = sqlite3.connect(str(COURSES_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        ensure_courses_sync_columns(conn)
        q = """SELECT id, provider, title, date_time, meeting_id, meeting_link,
                    meeting_password, status, active_in_portal, last_seen_at,
                    last_synced_at, last_sync_status, last_sync_action,
                    COALESCE(fobs_course_url, '') AS fobs_course_url
               FROM courses
              WHERE date_time >= ?
                AND COALESCE(status, '') <> 'Replaced'
                AND lower(COALESCE(last_sync_action, '')) NOT LIKE '%course replaced by provider%'
                AND lower(COALESCE(last_sync_action, '')) NOT LIKE '%trainer confirmed removed%'
                AND (
                    COALESCE(active_in_portal, 1) = 1
                    OR COALESCE(last_sync_status, '') = 'needs_confirmation'
                    OR lower(COALESCE(last_sync_action, '')) LIKE '%possibly removed%'
                    OR lower(COALESCE(last_sync_action, '')) LIKE '%possibly cancelled%'
                    OR lower(COALESCE(last_sync_action, '')) LIKE '%not found in latest provider scan%'
                    OR COALESCE(last_synced_at, '') <> ''
                    OR COALESCE(meeting_id, '') <> ''
                    OR COALESCE(meeting_link, '') <> ''
                )"""
        params = [today_start_text()]
        if provider_filter != 'all':
            q += " AND (lower(replace(provider, ' ', '-')) = ? OR lower(provider) = ?)"
            params.extend([provider_filter.lower(), provider_filter.replace('-', ' ').lower()])
        q += ' ORDER BY date_time ASC, title ASC'
        return [dict(r) for r in conn.execute(q, params).fetchall()]
    finally:
        conn.close()


def course_counts_by_provider():
    """Count the same future rows the dashboard will show."""
    counts = {}
    if not COURSES_DB_PATH.exists():
        return counts
    conn = sqlite3.connect(str(COURSES_DB_PATH))
    try:
        ensure_courses_sync_columns(conn)
        q = """SELECT provider, COUNT(*)
               FROM courses
              WHERE date_time >= ?
                AND COALESCE(status, '') <> 'Replaced'
                AND lower(COALESCE(last_sync_action, '')) NOT LIKE '%course replaced by provider%'
                AND lower(COALESCE(last_sync_action, '')) NOT LIKE '%trainer confirmed removed%'
                AND (
                    COALESCE(active_in_portal, 1) = 1
                    OR COALESCE(last_sync_status, '') = 'needs_confirmation'
                    OR lower(COALESCE(last_sync_action, '')) LIKE '%possibly removed%'
                    OR lower(COALESCE(last_sync_action, '')) LIKE '%possibly cancelled%'
                    OR lower(COALESCE(last_sync_action, '')) LIKE '%not found in latest provider scan%'
                    OR COALESCE(last_synced_at, '') <> ''
                    OR COALESCE(meeting_id, '') <> ''
                    OR COALESCE(meeting_link, '') <> ''
                )
              GROUP BY provider"""
        for provider_name, count in conn.execute(q, (today_start_text(),)).fetchall():
            counts[provider_slug(provider_name or 'provider')] = int(count or 0)
        return counts
    finally:
        conn.close()


def course_counts_from_rows(rows):
    counts = {}
    for row in rows or []:
        provider_id = provider_slug(row.get('provider_id') or row.get('provider') or 'provider')
        counts[provider_id] = counts.get(provider_id, 0) + 1
    return counts


def find_matching_course_state(course_state, provider_name, title, db_date_time):
    if not isinstance(course_state, dict):
        return None
    target_provider = provider_name.strip().lower()
    target_title = title.strip().lower()
    target_dt = (db_date_time or '').strip().lower()
    for key, value in course_state.items():
        if not isinstance(value, dict):
            continue
        key_text = str(key).lower()
        if target_provider in key_text and target_title in key_text and target_dt[:16] in key_text:
            return value
    return None


def format_date_parts(text):
    try:
        dt = datetime.strptime(text, '%Y-%m-%d %H:%M')
        return dt.strftime('%a %d %b %Y'), dt.strftime('%H:%M')
    except Exception:
        return text or '', ''


def shorten_message(message, limit=64):
    text = (message or '').strip()
    if not text:
        return 'Waiting'
    mapping = {
        'FOBS already has valid live Zoom': 'Already ready',
        'FOBS updated successfully': 'Updated successfully',
        'Read course summary': 'Checked',
        'Already handled earlier in this run': 'Already handled',
        'Sync started from dashboard.': 'Sync started',
        'Stop requested from dashboard.': 'Stopping',
    }
    for k, v in mapping.items():
        if text.startswith(k):
            return v
    first = text.splitlines()[0]
    return first if len(first) <= limit else first[:limit - 1] + '...'


def human_status(raw_status):
    return {'running': 'Syncing', 'success': 'Ready', 'skipped': 'Ready', 'error': 'Needs attention', 'idle': 'Waiting', 'stopped': 'Stopped'}.get((raw_status or '').strip().lower(), (raw_status or 'Waiting').title())



def parse_dashboard_datetime(value):
    """Best-effort parse for dashboard timestamps."""
    text = (value or '').strip()
    if not text:
        return None
    formats = (
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S',
    )
    for fmt in formats:
        try:
            return datetime.strptime(text[:len(datetime.now().strftime(fmt))] if '%f' not in fmt else text, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        return None



def account_is_paid(access):
    """Best-effort account plan detection from the licensing response."""
    access = access or {}
    features = access.get('features') if isinstance(access.get('features'), dict) else {}
    plan = str(access.get('plan') or access.get('tier') or '').strip().lower()
    if access.get('paid') is True or access.get('is_paid') is True:
        return True
    if plan and plan not in {'free', 'trial', 'starter'}:
        return True
    try:
        return int(features.get('sync_window_days') or 0) > FREE_SYNC_WINDOW_DAYS
    except Exception:
        return False


def effective_sync_window_days(access):
    """Free users sync 21 days ahead; paid users sync 12 weeks ahead."""
    access = access or {}
    features = access.get('features') if isinstance(access.get('features'), dict) else {}
    try:
        feature_days = int(features.get('sync_window_days') or 0)
        if feature_days > 0:
            return feature_days
    except Exception:
        pass
    return PAID_SYNC_WINDOW_DAYS if account_is_paid(access) else FREE_SYNC_WINDOW_DAYS


def sync_window_label(days):
    if days == FREE_SYNC_WINDOW_DAYS:
        return '3-week'
    if days == PAID_SYNC_WINDOW_DAYS:
        return '12-week'
    if days and days % 7 == 0:
        return f'{days // 7}-week'
    return f'{days}-day'


def course_days_from_now(date_time_text):
    try:
        dt = datetime.strptime((date_time_text or '').strip(), '%Y-%m-%d %H:%M')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        return (dt - today).days
    except Exception:
        return None


def format_checked_date(value):
    checked = parse_dashboard_datetime(value)
    if not checked:
        return ''
    return checked.strftime('%d %b %Y')


def action_is_recent(last_synced_at, minutes=15):
    checked = parse_dashboard_datetime(last_synced_at)
    if not checked:
        return False
    return 0 <= (datetime.now() - checked).total_seconds() <= minutes * 60


def sync_status_from_course(last_synced_at, last_sync_status='', last_sync_action='', legacy_checked_at='', has_zoom=False):
    """Simplified, per-course sync status for operators.

    Uses last_synced_at first. For older database rows created before the new
    per-course sync columns existed, falls back to last_seen_at only when Zoom
    details already exist. This avoids contradictory messages such as
    "Not synced" plus "FOBS + Zoom OK".
    """
    checked_source = (last_synced_at or '').strip()
    status = (last_sync_status or '').strip().lower()
    action = (last_sync_action or '').strip()

    if not checked_source and has_zoom and legacy_checked_at:
        checked_source = (legacy_checked_at or '').strip()

    checked = parse_dashboard_datetime(checked_source)

    if status in {'error', 'failed', 'needs_attention'}:
        return 'Needs attention', 'bad', action or 'Review required', checked_source

    if not checked:
        return 'Not checked', 'neutral', 'Not yet checked', ''

    checked_text = format_checked_date(checked_source)
    note = f'Last checked {checked_text}' if checked_text else 'Last checked recently'
    age_days = (datetime.now() - checked).days

    if age_days > 7:
        return 'Sync due', 'due', note, checked_source

    return 'Synced', 'ok', note, checked_source



def load_course_removal_confirmations():
    data = load_json(COURSE_REMOVAL_CONFIRM_PATH, {'confirmed': {}})
    if not isinstance(data, dict):
        return {}
    confirmed = data.get('confirmed', {})
    return confirmed if isinstance(confirmed, dict) else {}


def save_course_removal_confirmations(confirmed):
    save_json(COURSE_REMOVAL_CONFIRM_PATH, {'confirmed': confirmed or {}})


def course_removal_key(course):
    return '|'.join([
        provider_slug(course.get('provider') or ''),
        (course.get('date_time') or '').strip(),
        (course.get('title') or '').strip().lower(),
    ])


def mark_course_confirmed_removed(course_id):
    """Trainer-confirmed removal hides a stale/cancelled provider course."""
    if not COURSES_DB_PATH.exists():
        return False
    conn = sqlite3.connect(str(COURSES_DB_PATH))
    try:
        ensure_courses_sync_columns(conn)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cur = conn.execute(
            """
            UPDATE courses
               SET active_in_portal = 0,
                   last_synced_at = ?,
                   last_sync_status = 'removed_confirmed',
                   last_sync_action = 'Trainer confirmed removed/cancelled in provider portal'
             WHERE id = ?
            """,
            (now, course_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()



def is_safe_external_url(url):
    parsed = urlparse((url or '').strip())
    return parsed.scheme == 'https' and bool(parsed.netloc)


def zoom_web_meeting_url(meeting_id):
    clean = ''.join(ch for ch in (meeting_id or '') if ch.isdigit())
    if not clean:
        return ''
    return f'https://zoom.us/meeting/{clean}'


def zoom_web_account_url(account_id=''):
    # Zoom browser sessions are controlled by Zoom itself. TrainerMate can open
    # the correct meeting page, but if the browser is signed into the wrong
    # account Zoom will ask the trainer to switch/sign in.
    return 'https://zoom.us/profile'


def provider_tools_for_row(row):
    exact_fobs_url = (row.get('fobs_course_url') or '').strip()
    provider_fobs_url = (row.get('provider_courses_url') or row.get('provider_login_url') or '').strip()
    fobs_url = exact_fobs_url if is_safe_external_url(exact_fobs_url) else provider_fobs_url
    if not is_safe_external_url(fobs_url):
        fobs_url = ''
    zoom_url = zoom_web_meeting_url(row.get('meeting_id') or '')
    return {
        'fobsUrl': fobs_url,
        'fobsUrlIsExact': bool(exact_fobs_url and fobs_url == exact_fobs_url),
        'zoomUrl': zoom_url,
        'zoomAccountUrl': zoom_web_account_url(row.get('zoom_account_id') or ''),
        'meetingLink': (row.get('meeting_link') or '').strip() if is_safe_external_url(row.get('meeting_link') or '') else '',
    }


def course_calendar_severity(row):
    """Gentle visual severity for calendar events."""
    status = (row.get('status_label') or '').strip().lower()
    note = (row.get('short_message') or '').strip().lower()
    if status in {'needs attention', 'needs confirmation'} or 'conflict' in note or 'confirm' in note:
        return 'attention'
    if status == 'sync due' or 'sync due' in note:
        return 'due'
    if status == 'scheduled for later sync' or 'beyond 12 weeks' in note or 'outside' in note:
        return 'later'
    if status in {'synced', 'ready'} or 'fobs + zoom ok' in note:
        return 'ok'
    return 'neutral'


def course_calendar_advice(row):
    note = (row.get('short_message') or '').strip()
    severity = course_calendar_severity(row)

    if severity == 'attention':
        if 'deleted' in note.lower() or 'cancelled' in note.lower() or 'confirm' in note.lower():
            return 'Check this course in FOBS. Only confirm removed if it has genuinely been cancelled or deleted.'
        if 'conflict' in note.lower():
            return 'Check FOBS, then update Zoom only when you are sure which course is correct.'
        return 'Manual check needed before TrainerMate changes anything.'
    if severity == 'due':
        return 'Run sync to re-check FOBS and Zoom.'
    if severity == 'later':
        return 'Visible now. TrainerMate will update it when it enters your sync window.'
    if severity == 'ok':
        return 'No action needed.'
    return 'No action needed yet.'


def build_calendar_events(provider_filter='all'):
    access = check_access(prefer_cached=True) or {}
    active_days = effective_sync_window_days(access)
    is_free = not account_is_paid(access)
    providers = load_providers()
    providers_by_slug = {provider_slug(p.get('name') or p.get('id') or ''): p for p in providers}

    raw_courses = load_courses(provider_filter)
    state = load_app_state()
    rows = build_course_rows(raw_courses, state, providers_by_slug, active_days, is_free)

    events = []
    for row in rows:
        try:
            dt = datetime.strptime(f"{row.get('date_label')} {row.get('time_label')}", '%a %d %b %Y %H:%M')
            start_iso = dt.isoformat()
        except Exception:
            continue

        severity = course_calendar_severity(row)
        tools = provider_tools_for_row(row)
        provider_color = normalize_hex_color(row.get('provider_color')) or default_provider_color(row.get('provider_id') or row.get('provider'))
        events.append({
            'id': row.get('id') or '',
            'title': f"{row.get('provider', '')}: {row.get('title', 'Course')}",
            'start': start_iso,
            'allDay': False,
            'className': [f'tm-cal-{severity}', 'tm-cal-provider-event'],
            'backgroundColor': provider_color,
            'borderColor': provider_color,
            'textColor': readable_text_color(provider_color),
            'extendedProps': {
                'courseId': row.get('id') or '',
                'provider': row.get('provider') or '',
                'providerColor': provider_color,
                'courseTitle': row.get('title') or '',
                'date': row.get('date_label') or '',
                'time': row.get('time_label') or '',
                'status': row.get('status_label') or '',
                'note': row.get('short_message') or '',
                'severity': severity,
                'advice': course_calendar_advice(row),
                'canConfirmRemoved': bool(row.get('can_confirm_removed')),
                'fobsUrl': tools.get('fobsUrl') or '',
                'fobsLaunchUrl': ('/calendar/open-fobs-course/' + (row.get('id') or '')) if row.get('id') else '',
                'fobsUrlIsExact': bool(tools.get('fobsUrlIsExact')),
                'zoomUrl': tools.get('zoomUrl') or '',
                'zoomAccountUrl': tools.get('zoomAccountUrl') or '',
                'meetingLink': tools.get('meetingLink') or '',
                'zoomAccountLabel': row.get('zoom_account_label') or 'Linked Zoom account not selected',
                'meetingId': row.get('meeting_id') or '',
                'providerManagesZoom': bool(row.get('provider_manages_zoom')),
            }
        })
    return events



def save_fobs_course_url_for_action(course_id, fobs_course_url):
    fobs_course_url = (fobs_course_url or '').strip()
    if not course_id or not fobs_course_url.startswith('https://'):
        return False
    conn = sqlite3.connect(str(COURSES_DB_PATH))
    try:
        ensure_courses_sync_columns(conn)
        try:
            conn.execute('ALTER TABLE courses ADD COLUMN fobs_course_url TEXT')
            conn.commit()
        except sqlite3.OperationalError:
            pass
        cur = conn.execute(
            """
            UPDATE courses
               SET fobs_course_url = ?,
                   last_seen_at = ?
             WHERE id = ?
            """,
            (fobs_course_url, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), course_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_fobs_launch_status(course_id, status, message):
    if not course_id:
        return
    with FOBS_LAUNCH_STATUS_LOCK:
        FOBS_LAUNCH_STATUS[course_id] = {
            'status': status,
            'message': message,
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }


def get_fobs_launch_status(course_id):
    with FOBS_LAUNCH_STATUS_LOCK:
        return FOBS_LAUNCH_STATUS.get(course_id, {
            'status': 'unknown',
            'message': 'No launch status is available yet.',
            'updated_at': '',
        })


def load_course_for_action(course_id):
    if not COURSES_DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(COURSES_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        ensure_courses_sync_columns(conn)
        try:
            conn.execute('ALTER TABLE courses ADD COLUMN fobs_course_url TEXT')
            conn.commit()
        except sqlite3.OperationalError:
            pass
        row = conn.execute(
            """SELECT id, provider, title, date_time, meeting_id, meeting_link,
                      COALESCE(fobs_course_url, '') AS fobs_course_url
                 FROM courses
                WHERE id = ?""",
            (course_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def load_course_for_exact_action(provider='', title='', date_time=''):
    """Find the exact visible course row selected by an alert/button.

    Some provider/day alert actions can carry a stale or duplicated course id.
    The visible row values are the safest selector, especially when two courses
    share the same provider and date. Prefer provider + exact start time + title,
    then fall back to provider + exact start time only.
    """
    if not COURSES_DB_PATH.exists():
        return None
    clean_provider = (provider or '').strip()
    clean_title = (title or '').strip()
    clean_date_time = (date_time or '').strip()
    if not clean_provider or not clean_date_time:
        return None
    conn = sqlite3.connect(str(COURSES_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        ensure_courses_sync_columns(conn)
        try:
            conn.execute('ALTER TABLE courses ADD COLUMN fobs_course_url TEXT')
            conn.commit()
        except sqlite3.OperationalError:
            pass
        base_select = """SELECT id, provider, title, date_time, meeting_id, meeting_link,
                              COALESCE(fobs_course_url, '') AS fobs_course_url
                         FROM courses
                        WHERE provider = ? AND date_time = ?"""
        params = [clean_provider, clean_date_time]
        if clean_title:
            row = conn.execute(base_select + " AND title = ? ORDER BY id LIMIT 1", params + [clean_title]).fetchone()
            if row:
                return dict(row)
        row = conn.execute(base_select + " ORDER BY id LIMIT 1", params).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _launch_authenticated_fobs_course(course_id):
    """Open a FOBS course for manual use without showing login/search steps."""
    set_fobs_launch_status(course_id, 'starting', 'Preparing FOBS course summary. Please wait...')
    course = load_course_for_action(course_id)
    if not course:
        raise RuntimeError('Course not found.')
    provider_name = (course.get('provider') or '').strip()
    providers = {provider_slug(p.get('name') or p.get('id') or ''): p for p in load_providers()}
    provider = providers.get(provider_slug(provider_name)) or {}
    provider_id = provider_slug(provider.get('id') or provider.get('name') or provider_name)
    creds = get_provider_credentials(provider_id)
    username = (creds.get('username') or '').strip()
    password = (creds.get('password') or '').strip()
    login_url = (provider.get('login_url') or '').strip()
    courses_url = (provider.get('courses_url') or derive_courses_url(login_url)).strip()
    exact_url = (course.get('fobs_course_url') or '').strip()

    if sync_playwright is None:
        raise RuntimeError('Playwright is not installed in this environment.')
    if not login_url or not courses_url:
        raise RuntimeError('No FOBS login URL is configured for this provider.')
    if not username or not password:
        raise RuntimeError('No saved FOBS username/password for this provider.')

    p = sync_playwright().start()
    hidden_browser = None
    visible_browser = None
    try:
        set_fobs_launch_status(course_id, 'login', f'Preparing {provider_name} FOBS. Please wait...')
        hidden_browser = p.chromium.launch(headless=True)
        context = hidden_browser.new_context()
        page = context.new_page()

        def safe_goto(url, timeout=30000):
            page.goto(url, wait_until='domcontentloaded', timeout=timeout)
            page.wait_for_timeout(800)

        def login_if_needed():
            try:
                if page.locator('#UserName').count() and page.locator('#Password').count():
                    page.fill('#UserName', username)
                    page.fill('#Password', password)
                    page.click("button[type='submit'], input[type='submit']")
                    page.wait_for_timeout(2500)
            except Exception:
                pass

        def page_is_login():
            try:
                return bool(page.locator('#UserName').count() and page.locator('#Password').count())
            except Exception:
                return False

        def open_visible_final(final_url, message):
            nonlocal visible_browser
            final_url = (final_url or '').strip()
            if not final_url.startswith('https://'):
                raise RuntimeError('FOBS course summary URL could not be resolved.')
            set_fobs_launch_status(course_id, 'opening_visible', 'Opening the FOBS course summary now...')
            storage_state = context.storage_state()
            visible_browser = p.chromium.launch(headless=False)
            visible_context = visible_browser.new_context(storage_state=storage_state)
            visible_page = visible_context.new_page()
            visible_page.goto(final_url, wait_until='domcontentloaded', timeout=30000)
            visible_page.wait_for_timeout(800)
            try:
                if hidden_browser:
                    hidden_browser.close()
            except Exception:
                pass
            FOBS_BROWSER_SESSIONS.append((p, visible_browser))
            set_fobs_launch_status(course_id, 'opened', message)
            while visible_browser.is_connected():
                time.sleep(1)
            return True

        safe_goto(login_url)
        login_if_needed()

        if exact_url.startswith('https://'):
            set_fobs_launch_status(course_id, 'opening_course', 'Loading the saved FOBS course summary...')
            safe_goto(exact_url)
            if page_is_login():
                login_if_needed()
                safe_goto(exact_url)
            if not page_is_login():
                open_visible_final(page.url or exact_url, 'FOBS course summary opened.')
                return

        set_fobs_launch_status(course_id, 'searching', 'Finding the FOBS course summary. Please wait...')
        safe_goto(courses_url)
        login_if_needed()

        try:
            target_dt = datetime.strptime((course.get('date_time') or '').strip(), '%Y-%m-%d %H:%M')
            target_date = target_dt.strftime('%d/%m/%Y')
            target_date_long = target_dt.strftime('%A, %d %B %Y')
            target_time = target_dt.strftime('%H:%M')
            end_date = target_dt.strftime('%d/%m/%Y')
        except Exception:
            target_date = ''
            target_date_long = ''
            target_time = ''
            end_date = ''

        if end_date:
            try:
                page.wait_for_selector('#endDate', timeout=10000)
                page.evaluate("""([value]) => {
                    const el = document.querySelector('#endDate');
                    if (!el) return;
                    el.removeAttribute('readonly');
                    el.value = value;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""", [end_date])
                page.click("input[name='searchCoruses']")
                page.wait_for_timeout(1500)
                set_fobs_launch_status(course_id, 'matching', 'Course list found. Opening the matching course summary...')
            except Exception:
                pass

        title = (course.get('title') or '').strip().lower()
        def norm_cell(value):
            return ' '.join((value or '').replace('\\xa0', ' ').split()).strip()

        final_url = ''
        clicked = False
        try:
            rows = page.locator('tr:has(td)')
            count = rows.count()
            for i in range(count):
                row = rows.nth(i)
                cells = row.locator('td')
                values = [norm_cell(cells.nth(j).inner_text()) for j in range(cells.count())]
                if len(values) < 5:
                    continue
                row_date = (values[1] if len(values) > 1 else '').strip()
                row_time = (values[2] if len(values) > 2 else '').strip()
                row_title = (values[4] if len(values) > 4 else '').strip().lower()
                date_ok = (not target_date) or row_date in {target_date, target_date_long}
                time_ok = (not target_time) or target_time in row_time
                title_ok = (not title) or title == row_title or title in row_title or row_title in title
                if not (date_ok and time_ok and title_ok):
                    continue
                detail_clicked = False
                try:
                    detail_clicked = bool(row.evaluate("""(row) => {
                        const selectors = [
                            'td:last-child a', 'td:last-child button', 'td:last-child [ng-click]',
                            'td:last-child [onclick]', 'td:last-child i', 'td:last-child svg', 'td:last-child *'
                        ];
                        for (const selector of selectors) {
                            const el = row.querySelector(selector);
                            if (!el) continue;
                            const target = el.closest('a,button,[ng-click],[onclick]') || el;
                            target.click();
                            return true;
                        }
                        const cells = row.querySelectorAll('td');
                        if (cells.length) { cells[cells.length - 1].click(); return true; }
                        return false;
                    }"""))
                except Exception:
                    detail_clicked = False
                if not detail_clicked:
                    for selector in ["td:last-child a", "td:last-child button", "td:last-child [ng-click]", "td:last-child i", "i.fa-solid.fa-clipboard-list", "i.fa-clipboard-list", "[title='Course details']", "[title*='details' i]"]:
                        try:
                            target = row.locator(selector).first
                            if target.count():
                                target.click(timeout=5000)
                                detail_clicked = True
                                break
                        except Exception:
                            continue
                if not detail_clicked:
                    try:
                        cells.nth(cells.count() - 1).click(timeout=5000)
                        detail_clicked = True
                    except Exception:
                        pass
                if not detail_clicked:
                    continue
                try:
                    page.wait_for_url("**/Course/CourseDetails**", timeout=10000)
                except Exception:
                    try:
                        page.wait_for_load_state('domcontentloaded', timeout=5000)
                    except Exception:
                        pass
                clicked = True
                final_url = (page.url or '').strip()
                if '/Course/CourseDetails' in final_url:
                    save_fobs_course_url_for_action(course_id, final_url)
                    open_visible_final(final_url, 'FOBS course summary opened and saved for next time.')
                    return
                break
        except Exception:
            clicked = False

        if not clicked and exact_url.startswith('https://'):
            set_fobs_launch_status(course_id, 'fallback', 'Opening saved FOBS course summary...')
            safe_goto(exact_url)
            open_visible_final(page.url or exact_url, 'FOBS course summary opened from saved link.')
            return
        if clicked and final_url.startswith('https://'):
            open_visible_final(final_url, 'FOBS course summary opened.')
            return
        set_fobs_launch_status(course_id, 'error', 'FOBS opened the course list, but TrainerMate could not identify the matching course summary.')
    except Exception:
        try:
            if hidden_browser:
                hidden_browser.close()
        except Exception:
            pass
        try:
            if visible_browser:
                visible_browser.close()
        except Exception:
            pass
        try:
            p.stop()
        except Exception:
            pass
        raise

def start_authenticated_fobs_course_open(course_id):
    def runner():
        try:
            _launch_authenticated_fobs_course(course_id)
        except Exception as exc:
            set_fobs_launch_status(course_id, 'error', str(exc))
            print(f'[FOBS-LAUNCH] Could not open FOBS course {course_id}: {exc}')

    set_fobs_launch_status(course_id, 'queued', 'FOBS launch queued.')
    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    return True


@app.get('/calendar/open-fobs-course/<course_id>')
def open_fobs_course(course_id):
    access = check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}
    if not feature_enabled(access, 'calendar_sync'):
        return jsonify({'ok': False, 'error': 'Calendar tools are included with TrainerMate Paid.'}), 403
    try:
        if not load_course_for_action(course_id):
            return jsonify({'ok': False, 'error': 'Course not found.'}), 404
        start_authenticated_fobs_course_open(course_id)
        return jsonify({
            'ok': True,
            'launchId': course_id,
            'message': 'TrainerMate is logging into FOBS and opening the course summary.'
        })
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 500


@app.get('/calendar/fobs-launch-status/<course_id>')
def fobs_launch_status(course_id):
    return jsonify({
        'ok': True,
        **get_fobs_launch_status(course_id),
    })
@app.get('/calendar-events')
def calendar_events():
    access = check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}
    if not feature_enabled(access, 'calendar_sync'):
        return jsonify([])
    provider = request.args.get('provider') or 'all'
    return jsonify(build_calendar_events(provider))


@app.post('/course/<course_id>/confirm-removed')
def confirm_course_removed(course_id):
    ok = mark_course_confirmed_removed(course_id)
    if ok:
        state = load_app_state()
        state['last_message'] = 'Trainer confirmed a provider-removed/cancelled course.'
        save_json(APP_STATE_PATH, state)
    return redirect(request.referrer or url_for('home', section='dashboard'))


def normalize_course_action(action, status='', has_zoom=False):
    """Use consistent plain-English wording for already-present Zoom links."""
    text = (action or '').strip()
    lower = text.lower()
    status_lower = (status or '').strip().lower()

    if (
        'existing zoom meeting verified and updated' in lower
        or 'existing zoom meeting updated' in lower
        or 'existing zoom meeting verified' in lower
    ):
        return 'Zoom re-verified'

    if (
        'existing zoom meeting found and linked' in lower
        or 'existing matching zoom meeting found' in lower
    ):
        return 'Existing Zoom meeting linked'

    if (
        'already has valid live zoom' in lower
        or 'already present' in lower
        or 'zoom joining instructions already present' in lower
        or (has_zoom and lower in {'read course summary', 'checked'})
    ):
        return 'FOBS + Zoom OK'

    if 'updated successfully' in lower or 'fobs updated successfully' in lower or 'zoom link updated' in lower:
        return 'Zoom link updated'

    if 'trainer confirmed removed' in lower:
        return 'Trainer confirmed removed'

    if 'possibly removed' in lower or 'possibly cancelled' in lower or 'not found in provider portal' in lower:
        return 'Possibly removed/cancelled by provider'

    if 'course replaced by provider' in lower:
        return 'Course replaced by provider'

    if 'zoom link mismatch confirmed' in lower or 'fobs joining link may not match' in lower or ('zoom' in lower and 'mismatch' in lower):
        return 'Zoom link mismatch confirmed'

    if 'conflict' in lower:
        return 'Conflict - check FOBS'

    if status_lower == 'skipped':
        return 'FOBS + Zoom OK' if not text else text

    return text



def course_has_zoom_identity(course):
    """Return True when the local course row has a usable Zoom identity.

    Meeting ID is the primary truth signal.
    A stored Zoom URL can still help suppress old raw-link mismatch warnings
    because Zoom URLs may vary by host, redirect style, and query string while
    still pointing at the same meeting.
    """
    meeting_id = ''.join(ch for ch in (course.get('meeting_id') or '') if ch.isdigit())
    meeting_link = (course.get('meeting_link') or '').strip().lower()
    if meeting_id:
        return True
    if 'zoom.' in meeting_link and ('/j/' in meeting_link or '/w/' in meeting_link or 'meeting' in meeting_link):
        return True
    return False


def zoom_mismatch_is_explicit(action_text):
    """Only keep mismatch alerts when the sync recorded a real ID/passcode difference."""
    lower = (action_text or '').lower()
    explicit_tokens = (
        'meeting id differs',
        'meeting id mismatch',
        'different meeting id',
        'password differs',
        'password mismatch',
        'passcode differs',
        'passcode mismatch',
        'different password',
        'different passcode',
        'meeting missing/deleted',
        'fobs has no zoom details',
        'no zoom details',
    )
    return any(token in lower for token in explicit_tokens)


def clear_false_zoom_mismatch_flags():
    """Clear stale raw-URL mismatch warnings once a course has Zoom details.

    Older sync runs could store "Zoom link mismatch confirmed" when the raw
    Zoom URL text differed, even though the Meeting ID/passcode in FOBS were
    actually correct. The dashboard should not keep surfacing those stale
    warnings once it has a meeting identity saved locally.
    """
    if not COURSES_DB_PATH.exists():
        return 0
    conn = sqlite3.connect(str(COURSES_DB_PATH))
    try:
        ensure_courses_sync_columns(conn)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        rows = conn.execute("""
            SELECT id, meeting_id, meeting_link, meeting_password, last_sync_action, last_sync_status
            FROM courses
            WHERE lower(COALESCE(last_sync_action, '')) LIKE '%mismatch%'
               OR lower(COALESCE(last_sync_action, '')) LIKE '%fobs joining link may not match%'
        """).fetchall()
        cleared = 0
        for row in rows:
            course = {
                'meeting_id': row[1] or '',
                'meeting_link': row[2] or '',
                'meeting_password': row[3] or '',
            }
            action = row[4] or ''
            meeting_id_digits = ''.join(ch for ch in (course.get('meeting_id') or '') if ch.isdigit())
            if not meeting_id_digits:
                continue
            if zoom_mismatch_is_explicit(action):
                continue
            conn.execute("""
                UPDATE courses
                   SET last_sync_status = CASE
                           WHEN lower(COALESCE(last_sync_status, '')) IN ('error', 'failed', 'needs_attention') THEN 'success'
                           ELSE COALESCE(NULLIF(last_sync_status, ''), 'success')
                       END,
                       last_sync_action = 'FOBS + Zoom OK',
                       last_synced_at = COALESCE(NULLIF(last_synced_at, ''), ?)
                 WHERE id = ?
            """, (now, row[0]))
            cleared += 1
        conn.commit()
        return cleared
    finally:
        conn.close()

def course_is_inactive_in_portal(course):
    status = (course.get('last_sync_status') or '').strip().lower()
    action = (course.get('last_sync_action') or '').strip().lower()
    if status != 'needs_confirmation' and not any(text in action for text in ('possibly removed', 'possibly cancelled', 'not found in latest provider scan')):
        return False
    try:
        return int(course.get('active_in_portal')) == 0
    except Exception:
        return False


def build_course_rows(raw_courses, app_state, providers_by_slug, active_sync_window_days=FREE_SYNC_WINDOW_DAYS, is_free_account=True):
    course_state = app_state.get('courses', {}) if isinstance(app_state.get('courses'), dict) else {}
    zoom_accounts_by_id = {a.get('id'): a for a in load_zoom_accounts()}
    removal_confirmations = load_course_removal_confirmations()
    rows = []
    for c in raw_courses:
        provider_name = (c.get('provider') or 'Unknown').strip() or 'Unknown'
        provider_id = provider_slug(provider_name)
        provider_config = providers_by_slug.get(provider_id, {})
        state_match = find_matching_course_state(course_state, provider_name, c.get('title', ''), c.get('date_time', ''))
        state_status = (state_match.get('status') if state_match else 'idle') or 'idle'
        raw_message = (state_match.get('last_action') if state_match else 'No sync state yet') or 'No sync state yet'
        dlabel, tlabel = format_date_parts(c.get('date_time') or '')
        has_zoom = course_has_zoom_identity(c)
        meeting_id_digits = ''.join(ch for ch in (c.get('meeting_id') or '') if ch.isdigit())
        meeting_password = (c.get('meeting_password') or '').strip()
        days_ahead = course_days_from_now(c.get('date_time') or '')
        outside_sync_window = bool(days_ahead is not None and active_sync_window_days and days_ahead > active_sync_window_days)

        db_sync_action = (c.get('last_sync_action') or '').strip()
        db_sync_status = (c.get('last_sync_status') or '').strip()
        db_synced_at = (c.get('last_synced_at') or '').strip()
        status_label, status_class, sync_note, checked_source = sync_status_from_course(db_synced_at, db_sync_status, db_sync_action, c.get('last_seen_at') or '', has_zoom)
        normalized_message = normalize_course_action(db_sync_action or raw_message, db_sync_status or state_status, has_zoom)
        # Avoid false Zoom mismatch alerts caused by different URL formats.
        # Meeting ID/passcode are the useful truth; raw Zoom URLs can vary safely.
        mismatch_text = db_sync_action or raw_message or ''
        explicit_zoom_difference = zoom_mismatch_is_explicit(mismatch_text)
        strong_zoom_identity = bool(meeting_id_digits)
        if normalized_message == 'Zoom link mismatch confirmed' and (has_zoom or strong_zoom_identity) and not explicit_zoom_difference:
            normalized_message = 'FOBS + Zoom OK'
            if db_sync_status.lower() in {'error', 'failed', 'needs_attention', 'success', 'skipped'}:
                status_label = 'Synced'
                status_class = 'ok'
        show_upgrade = False

        if outside_sync_window:
            status_label = 'Scheduled for later sync'
            status_class = 'later'
            if is_free_account and days_ahead is not None and days_ahead <= PAID_SYNC_WINDOW_DAYS:
                short_message = 'Outside your 3-week free sync window - upgrade to sync up to 12 weeks ahead.'
                show_upgrade = True
            else:
                short_message = 'Course beyond 12 weeks - will update later' if active_sync_window_days >= 84 else f'Outside the {sync_window_label(active_sync_window_days)} sync window - will update later.'
        elif provider_config.get('provider_manages_zoom'):
            status_label = 'Synced' if db_synced_at else 'Not synced'
            status_class = 'ok' if db_synced_at else 'neutral'
            short_message = 'Provider managed: Zoom read-only in TrainerMate'
        else:
            if status_class == 'bad':
                short_message = normalized_message or sync_note
            elif normalized_message == 'Course replaced by provider':
                short_message = 'Course replaced by provider'
            elif normalized_message == 'Conflict - check FOBS':
                short_message = 'Conflict - check FOBS'
            elif normalized_message == 'Zoom re-verified':
                short_message = 'Zoom re-verified - FOBS + Zoom OK'
                status_label = 'Synced'
                status_class = 'ok'
            elif normalized_message == 'Existing Zoom meeting linked':
                short_message = 'Existing Zoom meeting linked - FOBS + Zoom OK'
                status_label = 'Synced'
                status_class = 'ok'
            elif normalized_message == 'Zoom link updated' and action_is_recent(db_synced_at):
                short_message = 'Zoom link updated'
            elif (normalized_message == 'FOBS + Zoom OK' or has_zoom) and checked_source:
                short_message = f'FOBS + Zoom OK - {sync_note}' if sync_note else 'FOBS + Zoom OK'
            else:
                short_message = sync_note

        removal_key = course_removal_key(c)
        possibly_removed = (
            normalized_message == 'Possibly removed/cancelled by provider'
            or (course_is_inactive_in_portal(c) and not removal_confirmations.get(removal_key))
        )

        zoom_mismatch_confirmed = (normalized_message == 'Zoom link mismatch confirmed')

        if zoom_mismatch_confirmed:
            status_label = 'Needs attention'
            status_class = 'bad'
            short_message = 'Zoom link mismatch confirmed on FOBS - choose whether to replace it, keep it, or open FOBS manually.'
        elif possibly_removed:
            status_label = 'Needs confirmation'
            status_class = 'bad'
            short_message = 'Course may have been deleted/cancelled by provider - confirm to remove from TrainerMate.'
        elif outside_sync_window:
            status_label = 'Scheduled for later sync'
            status_class = 'neutral'
            if active_sync_window_days >= 84:
                short_message = 'Course beyond 12 weeks - will update later'
            elif is_free_account and has_zoom:
                short_message = 'Outside your 3-week free sync window - upgrade to sync up to 12 weeks ahead.'
            else:
                short_message = f'Outside the {sync_window_label(active_sync_window_days)} sync window - will update later.'

        rows.append({
            'id': c.get('id') or '',
            'provider': provider_name,
            'provider_id': provider_id,
            'title': c.get('title') or 'Untitled course',
            'date_label': dlabel,
            'time_label': tlabel,
            'status_label': status_label,
            'status_class': status_class,
            'short_message': short_message,
            'checked_source': checked_source,
            'show_upgrade': show_upgrade,
            'is_action_needed': status_label in {'Not checked', 'Not synced', 'Sync due', 'Needs attention', 'Needs confirmation'},
            'is_outside_window': outside_sync_window,
            'date_time_raw': c.get('date_time') or '',
            'last_seen_at': c.get('last_seen_at') or '',
            'can_confirm_removed': possibly_removed,
            'zoom_mismatch_confirmed': zoom_mismatch_confirmed,
            'meeting_id': c.get('meeting_id') or '',
            'meeting_link': c.get('meeting_link') or '',
            'meeting_password': c.get('meeting_password') or '',
            'fobs_course_url': c.get('fobs_course_url') or '',
            'provider_login_url': provider_config.get('login_url') or '',
            'provider_courses_url': provider_config.get('courses_url') or derive_courses_url(provider_config.get('login_url') or ''),
            'provider_color': normalize_hex_color(provider_config.get('color')) or default_provider_color(provider_id),
            'zoom_account_id': provider_config.get('zoom_account_id') or get_default_zoom_account_id(),
            'zoom_account_label': get_zoom_account_label(provider_config.get('zoom_account_id') or ''),
            'provider_manages_zoom': bool(provider_config.get('provider_manages_zoom')),
        })
    return rows

def suppress_stale_same_provider_slot_duplicates(rows):
    """Hide stale same-provider duplicate rows where FOBS has replaced one course with another.

    If the same provider/date/time has multiple course titles, keep the newest
    checked/current row. This covers provider replacements such as Essex NMAC
    being replaced by NSAC at the same slot.
    """
    grouped = {}
    for row in rows:
        key = (row.get('provider_id'), row.get('date_label'), row.get('time_label'))
        grouped.setdefault(key, []).append(row)

    filtered = []
    for key, group in grouped.items():
        titles = {r.get('title') for r in group}
        if len(group) <= 1 or len(titles) <= 1:
            filtered.extend(group)
            continue

        def score(row):
            checked = parse_dashboard_datetime(row.get('checked_source') or row.get('last_seen_at') or '')
            checked_ts = checked.timestamp() if checked else 0
            status_bonus = 2 if row.get('status_label') == 'Synced' else 1 if row.get('status_label') == 'Sync due' else 0
            return (checked_ts, status_bonus)

        newest = max(group, key=score)
        filtered.append(newest)
    return filtered


def load_alert_ack():
    data = load_json(ALERT_ACK_PATH, {'dismissed': []})
    if not isinstance(data, dict):
        return set()
    return set(str(x) for x in data.get('dismissed', []) if x)


def save_alert_ack(dismissed):
    save_json(ALERT_ACK_PATH, {'dismissed': sorted(set(str(x) for x in dismissed if x))})


def certificate_notice_alert_id(link, doc=None):
    """Stable id for one visible certificate/provider notice event.

    Certificate dismissals must survive normal page reloads and future provider
    refreshes. Older builds included provider_checked_at/updated_at in this id,
    which made the same missing-certificate notice look new every time FOBS was
    checked. Keep the id tied to the certificate/provider/problem instead.
    """
    link = link or {}
    doc = doc or {}
    parts = [
        'certificate-notice',
        str(link.get('id') or ''),
        str(doc.get('id') or ''),
        str(link.get('provider_id') or ''),
        str(link.get('provider_status') or ''),
        str(link.get('pending_action') or ''),
    ]
    return ':'.join(parts)


def build_dashboard_alerts(raw_courses, dismissed=None):
    """Build quiet operator alerts for provider replacements and cross-provider conflicts."""
    dismissed = dismissed or set()
    alerts = []

    def add_alert(alert_id, title, message, level='warning'):
        if alert_id in dismissed:
            return
        alerts.append({
            'id': alert_id,
            'title': title,
            'message': message,
            'level': level,
        })

    # Same provider, same date/time, different titles = likely provider replacement.
    by_provider_slot = {}
    for c in raw_courses or []:
        key = ((c.get('provider') or '').strip().lower(), (c.get('date_time') or c.get('date_time_raw') or '').strip())
        if key[0] and key[1]:
            by_provider_slot.setdefault(key, []).append(c)

    for (provider_key, date_time), group in by_provider_slot.items():
        titles = sorted(set((c.get('title') or '').strip() for c in group if (c.get('title') or '').strip()))
        if len(titles) <= 1:
            continue
        provider_name = (group[0].get('provider') or provider_key.title()).strip()
        dlabel, tlabel = format_date_parts(date_time)
        alert_id = 'replacement:' + provider_slug(provider_name) + ':' + date_time.replace(' ', 'T') + ':' + provider_slug('|'.join(titles))
        add_alert(
            alert_id,
            'Provider changed a course',
            f'{provider_name} has more than one course title for {dlabel} {tlabel}. Check FOBS, then dismiss this alert once confirmed.',
            'warning',
        )

    # Same date/time across providers = possible diary conflict.
    by_slot = {}
    for c in raw_courses or []:
        dt = (c.get('date_time') or c.get('date_time_raw') or '').strip()
        provider = (c.get('provider') or '').strip()
        if dt and provider:
            by_slot.setdefault(dt, set()).add(provider)
    for date_time, providers in by_slot.items():
        if len(providers) <= 1:
            continue
        dlabel, tlabel = format_date_parts(date_time)
        provider_list = ', '.join(sorted(providers))
        alert_id = 'conflict:' + date_time.replace(' ', 'T') + ':' + provider_slug(provider_list)
        add_alert(
            alert_id,
            'Possible provider conflict',
            f'Multiple providers have courses at {dlabel} {tlabel}: {provider_list}. Check FOBS if this is unexpected.',
            'bad',
        )

    return alerts


def get_status_dot_class(state):
    if state.get('sync_running'):
        return 'running'
    status_text = ((state.get('last_status') or '') + ' ' + (state.get('last_run_status') or '')).lower()
    if 'error' in status_text or 'failed' in status_text:
        return 'error'
    if 'warning' in status_text or 'stopped' in status_text:
        return 'warning'
    if 'success' in status_text or 'complete' in status_text:
        return 'success'
    return 'warning' if state.get('health_issues') else 'success'


def build_friendly_status(state):
    if state.get('sync_running'):
        return 'Syncing'
    raw = (state.get('last_status') or state.get('last_run_status') or 'Idle').strip().lower()
    if raw in ('stopped', 'stop', 'stopping'):
        return 'Stopped'
    if 'error' in raw or 'failed' in raw:
        return 'Needs attention'
    if 'success' in raw or 'complete' in raw:
        return 'Ready'
    return 'Ready' if not state.get('health_issues') else 'Needs attention'


def format_last_sync(state):
    for key in ('last_success_at', 'last_run_finished_at', 'last_run_started_at'):
        value = (state.get(key) or '').strip()
        if value:
            return value
    return ''


def is_process_running(pid):
    if not pid:
        return False
    try:
        if os.name == 'nt':
            import ctypes
            handle = ctypes.windll.kernel32.OpenProcess(0x1000, 0, int(pid))
            if handle == 0:
                return False
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def reconcile_running_state():
    state = load_app_state()
    pid = state.get('pid')
    if state.get('sync_running') and not pid:
        update_app_state(sync_running=False, pid=None, last_status='Stopped', last_run_status=state.get('last_run_status') or 'stopped', pending_sync_request={})
        state = load_app_state()
        return state
    if state.get('sync_running') and pid and not is_process_running(pid):
        source = ((state.get('pending_sync_request') or {}).get('source') or '').strip()
        if source:
            try:
                activity = build_sync_activity_from_state(state, source=source)
                auto_state = state.get('auto_sync') if isinstance(state.get('auto_sync'), dict) else {}
                auto_state.update({'last_completed_at': utc_now_text(), 'last_activity_id': activity.get('id'), 'last_message': activity.get('summary')})
                state['auto_sync'] = auto_state
            except Exception:
                pass
        update_app_state(sync_running=False, pid=None, last_status='Stopped', last_run_status=state.get('last_run_status') or 'stopped', pending_sync_request={})
        state = load_app_state()
    return state


def start_sync_process(scan_provider='all', scan_days=7, target_course=None, allow_zoom_replace=False, scan_scope='short', bot_mode='', source='manual'):
    if not BOT_APP_PATH.exists():
        return False, 'bot_app.py not found.'
    state = reconcile_running_state()
    if state.get('sync_running') and state.get('pid') and is_process_running(state.get('pid')):
        return False, 'Sync is already running.'
    identity = get_identity()
    if not identity['ndors'].strip():
        return False, 'Save your NDORS trainer ID before starting sync.'
    access = check_access(timeout_seconds=ACTION_ACCESS_TIMEOUT_SECONDS, prefer_cached=False) or {}
    if not access.get('allowed'):
        if access.get('reason') == 'update_required':
            notice = update_notice_from_access(access) or 'TrainerMate must be updated before syncing.'
            return False, notice
        if access.get('reason') == 'free_sync_limit_reached':
            return False, 'Your free sync trial has finished. Activate a paid licence to continue syncing.'
        return False, 'Access is blocked. Check your licence/account status or try again.'
    if not has_connected_zoom_account():
        update_app_state(sync_running=False, stop_requested=False, pid=None, last_status='Needs attention', last_message=zoom_required_sync_message())
        return False, zoom_required_sync_message()
    scan_provider = provider_slug(scan_provider or 'all')
    if scan_provider == 'provider':
        scan_provider = 'all'
    if scan_provider != 'all':
        valid_provider_ids = {provider_slug(p.get('id') or p.get('name') or '') for p in load_providers()}
        if scan_provider not in valid_provider_ids:
            return False, 'That provider is no longer available. Refresh TrainerMate and choose the provider again.'
    try:
        try:
            BOT_LOG_PATH.write_text(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sync starting...\n", encoding='utf-8')
        except Exception:
            pass
        log_handle = open(BOT_LOG_PATH, 'a', encoding='utf-8', errors='replace')
        try:
            scan_days = int(scan_days or 7)
        except Exception:
            scan_days = 7
        allowed_days = max(1, effective_sync_window_days(access))
        scan_days = allowed_days if scan_days <= 0 else scan_days
        scan_days = max(1, min(scan_days, allowed_days))
        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'
        env['TRAINERMATE_API_URL'] = API_URL
        env['TRAINERMATE_SCAN_DAYS'] = str(scan_days)
        env['TRAINERMATE_SCAN_PROVIDER'] = scan_provider
        env['TRAINERMATE_SCAN_SCOPE'] = str(scan_scope or 'short')
        if bot_mode:
            env['BOT_MODE'] = str(bot_mode)
        env['TRAINERMATE_HEADLESS'] = '1'
        env['TRAINERMATE_BROWSER_HEADLESS'] = '1'
        env['TRAINERMATE_SHOW_BROWSER'] = '0'
        env['PLAYWRIGHT_HEADLESS'] = '1'
        if allow_zoom_replace:
            env['TRAINERMATE_ALLOW_ZOOM_REPLACE_ON_MISMATCH'] = '1'
        if isinstance(target_course, dict) and target_course.get('id'):
            target_exact_key = course_action_exact_key(target_course)
            env['TRAINERMATE_TARGET_COURSE_ID'] = str(target_course.get('id') or '')
            env['TRAINERMATE_TARGET_COURSE_PROVIDER'] = str(target_course.get('provider') or '')
            env['TRAINERMATE_TARGET_COURSE_DATE_TIME'] = str(target_course.get('date_time') or '')
            env['TRAINERMATE_TARGET_COURSE_TITLE'] = str(target_course.get('title') or '')
            env['TRAINERMATE_TARGET_COURSE_KEY'] = target_exact_key
            env['TRAINERMATE_TARGET_COURSE_TIME'] = str(target_course.get('date_time') or '')
            env['TRAINERMATE_SCAN_SCOPE'] = 'single_course'
        kwargs = {'cwd': str(BASE_DIR), 'env': env, 'stdin': subprocess.DEVNULL, 'stdout': log_handle, 'stderr': subprocess.STDOUT}
        if os.name == 'nt':
            kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW
        process = subprocess.Popen([sys.executable, str(BOT_APP_PATH)], **kwargs)
        try:
            log_handle.close()
        except Exception:
            pass
        now_text = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        scope_text = 'all providers' if scan_provider == 'all' else scan_provider
        days_text = f'next {scan_days} days'
        if isinstance(target_course, dict) and target_course.get('id'):
            scope_text = f"single course: {target_course.get('provider', scan_provider)} {target_course.get('date_time', '')} {target_course.get('title', '')}"
        update_app_state(
            sync_running=True,
            stop_requested=False,
            pid=process.pid,
            last_pid=process.pid,
            last_status='Running',
            last_message=f'Sync started from dashboard: {scope_text}, {days_text}.',
            last_started_at=now_text,
            last_run_started_at=now_text,
            scan_request={
                'provider': scan_provider,
                'days': scan_days,
                'started_at': now_text,
                'target_course_id': (target_course or {}).get('id') if isinstance(target_course, dict) else '',
                'target_course_key': course_action_exact_key(target_course) if isinstance(target_course, dict) else '',
                'target_course_provider': (target_course or {}).get('provider') if isinstance(target_course, dict) else '',
                'target_course_date_time': (target_course or {}).get('date_time') if isinstance(target_course, dict) else '',
                'target_course_title': (target_course or {}).get('title') if isinstance(target_course, dict) else '',
                'source': source or 'manual',
                'scan_scope': scan_scope or 'short',
                'bot_mode': bot_mode or '',
            },
        )
        try:
            start_certificate_scan_async(scan_provider)
        except Exception:
            pass
        return True, f'Sync started for {scope_text}, {days_text}.'
    except Exception as exc:
        return False, f'Could not start sync: {exc}'


def stop_sync_process():
    state = load_app_state()
    pid = state.get('pid') or state.get('last_pid')
    update_app_state(
        stop_requested=True,
        sync_running=False,
        pid=None,
        last_status='Stopped',
        last_run_status='stopped',
        last_message='Sync stopped.',
        pending_sync_request={},
        scan_request={},
    )
    if pid:
        try:
            pid_int = int(pid)
            if os.name == 'nt':
                subprocess.run(['taskkill', '/PID', str(pid_int), '/T', '/F'], check=False, capture_output=True, text=True)
            else:
                try:
                    os.kill(pid_int, signal.SIGTERM)
                    time.sleep(0.4)
                except Exception:
                    pass
                if is_process_running(pid_int):
                    try:
                        os.kill(pid_int, signal.SIGKILL)
                    except Exception:
                        pass
        except Exception:
            pass
    return True, 'Sync stopped.'


def zoom_redirect_uri():
    return ZOOM_APPROVED_RELAY_URI


def zoom_auth_url(state_token):
    return 'https://zoom.us/oauth/authorize?' + urlencode({
        'response_type': 'code',
        'client_id': ZOOM_CLIENT_ID,
        'redirect_uri': zoom_redirect_uri(),
        'state': state_token,
    })


TEMPLATE = """
<!doctype html>
<html>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>TrainerMate</title>
<style>
:root{color-scheme:dark;--bg:#081225;--bg2:#0c1830;--panel:rgba(17,24,39,.92);--panel2:rgba(11,18,32,.94);--text:#f3f4f6;--muted:#9ca3af;--line:rgba(148,163,184,.16);--accent:#2563eb;--accent2:#38bdf8;--danger:#ef4444;--success:#22c55e;--warn:#f59e0b}
*{box-sizing:border-box}body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:linear-gradient(180deg,var(--bg),var(--bg2));color:var(--text)}a{color:inherit;text-decoration:none}
.startup-screen{position:fixed;inset:0;z-index:10000;display:none;align-items:center;justify-content:center;padding:18px;background:rgba(8,18,37,.88);backdrop-filter:blur(10px)}
.startup-screen.show{display:flex}.startup-card{width:min(520px,100%);border:1px solid rgba(125,211,252,.22);border-radius:22px;background:linear-gradient(180deg,rgba(15,23,42,.98),rgba(10,16,29,.98));box-shadow:0 24px 70px rgba(0,0,0,.48);padding:22px;display:grid;gap:18px}.startup-head{display:flex;align-items:flex-start;justify-content:space-between;gap:14px}.startup-title{font-size:22px;font-weight:900;color:#fff}.startup-subtitle{margin-top:4px;color:#cbd5e1;font-size:14px;line-height:1.4}.startup-skip{appearance:none;border:1px solid rgba(148,163,184,.28);border-radius:999px;background:rgba(15,23,42,.72);color:#dbeafe;padding:9px 13px;font-weight:800;cursor:pointer;white-space:nowrap}.startup-skip:hover{border-color:rgba(125,211,252,.45)}
.startup-steps{display:grid;gap:10px}.startup-step{display:grid;grid-template-columns:28px minmax(0,1fr) auto;gap:10px;align-items:center;padding:11px 12px;border:1px solid rgba(148,163,184,.16);border-radius:15px;background:rgba(15,23,42,.72)}.startup-step-icon{width:28px;height:28px;border-radius:999px;display:grid;place-items:center;background:rgba(148,163,184,.16);color:#cbd5e1;font-weight:900}.startup-step-main{min-width:0}.startup-step-label{font-weight:850;color:#f8fafc;line-height:1.2}.startup-step-detail{margin-top:2px;color:#aeb9c8;font-size:12px;line-height:1.3;overflow-wrap:anywhere}.startup-step-state{font-size:11px;border-radius:999px;padding:5px 8px;border:1px solid rgba(148,163,184,.20);color:#cbd5e1;background:rgba(15,23,42,.86);text-transform:lowercase}
.startup-step.state-running .startup-step-icon,.startup-step.state-waiting .startup-step-icon{background:rgba(37,99,235,.25);color:#bfdbfe;animation:tmPulse 1.2s infinite}.startup-step.state-complete .startup-step-icon,.startup-step.state-skipped .startup-step-icon{background:rgba(34,197,94,.16);color:#86efac;animation:none}.startup-step.state-warning .startup-step-icon,.startup-step.state-error .startup-step-icon{background:rgba(245,158,11,.18);color:#fde68a;animation:none}.startup-foot{color:#9ca3af;font-size:12px;line-height:1.4}
.shell{min-height:100vh;display:grid;grid-template-rows:auto 1fr}.top{display:flex;justify-content:space-between;align-items:flex-start;gap:16px;padding:18px 22px;border-bottom:1px solid var(--line);background:rgba(8,18,37,.96)}.brand{display:flex;gap:14px;align-items:center}.mark{width:42px;height:42px;border-radius:14px;background:linear-gradient(135deg,var(--accent2),var(--accent));display:grid;place-items:center;font-weight:800}.top h1{margin:0;font-size:22px}.muted{color:var(--muted)}.top-right{display:flex;align-items:flex-start;gap:10px;flex-wrap:wrap;justify-content:flex-end}.alert-menu{position:relative}.alert-menu summary{cursor:pointer;list-style:none}.alert-button{position:relative;width:42px;height:42px;border-radius:999px;border:1px solid rgba(245,158,11,.35);background:rgba(245,158,11,.12);color:#fde68a;font-weight:900;display:grid;place-items:center}.alert-count{position:absolute;top:-6px;right:-6px;min-width:20px;height:20px;padding:0 5px;border-radius:999px;background:#ef4444;color:white;font-size:11px;display:grid;place-items:center}.alert-dropdown{position:absolute;right:0;top:50px;z-index:30;width:min(420px,90vw);padding:12px;border:1px solid var(--line);border-radius:16px;background:rgba(11,18,32,.98);box-shadow:0 18px 44px rgba(0,0,0,.35);display:grid;gap:10px}.alert-item{display:grid;gap:6px;padding:10px;border:1px solid rgba(245,158,11,.22);border-radius:12px;background:rgba(15,23,42,.8)}.alert-title{font-weight:800}.alert-message{font-size:12px;color:var(--muted);line-height:1.35}.pill{display:inline-flex;align-items:center;gap:8px;padding:10px 14px;border:1px solid var(--line);border-radius:999px;background:rgba(17,24,39,.92);font-size:13px}.dot{width:10px;height:10px;border-radius:999px;background:var(--muted)}.running{background:var(--accent2)}.success{background:var(--success)}.warning{background:var(--warn)}.error{background:var(--danger)}.btn{appearance:none;border:0;border-radius:12px;padding:11px 16px;font-weight:700;cursor:pointer;color:#fff;background:linear-gradient(135deg,#2563eb,#1d4ed8)}.btn.soft{background:rgba(30,41,59,.88);border:1px solid var(--line)}.btn.warn{background:linear-gradient(135deg,#ef4444,#dc2626)}.btn.small{padding:8px 12px;border-radius:10px;font-size:13px}.layout{display:grid;grid-template-columns:250px minmax(0,1fr);gap:18px;padding:18px}.panel{background:var(--panel);border:1px solid var(--line);border-radius:20px;overflow:hidden}.sidebar{padding:12px}.nav,.sub{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:12px;border-radius:14px;border:1px solid transparent;background:rgba(15,23,42,.46);color:var(--text);text-decoration:none;margin-bottom:8px}.nav:hover,.sub:hover{border-color:rgba(56,189,248,.25)}.active{background:rgba(37,99,235,.16);border-color:rgba(56,189,248,.35)}details.navgroup summary{list-style:none;cursor:pointer;padding:12px;border-radius:14px;background:rgba(15,23,42,.46);border:1px solid transparent;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center}.sublist{padding-left:10px}.badge{display:inline-flex;align-items:center;justify-content:center;min-width:24px;padding:4px 8px;border-radius:999px;background:rgba(15,23,42,.95);color:var(--muted);border:1px solid var(--line);font-size:12px}.main{display:grid;gap:18px;min-width:0}.flash{padding:12px 14px;border-radius:14px;border:1px solid var(--line);background:rgba(17,24,39,.92)}.hero{display:flex;justify-content:space-between;align-items:flex-start;gap:16px;padding:18px}.hero h2{margin:0 0 6px;font-size:22px}.hero-actions{display:flex;gap:10px;flex-wrap:wrap}.stats{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;padding:0 18px 18px}.stat{background:var(--panel2);border:1px solid var(--line);border-radius:18px;padding:16px}.stat h3{margin:0;font-size:28px}.stat p{margin:4px 0 0;color:var(--muted)}.head{padding:16px 18px 13px;border-bottom:1px solid var(--line)}.head h3{margin:0;font-size:16px}.head p{margin:4px 0 0;color:var(--muted);font-size:13px}.block{padding:18px}.tablewrap{overflow:auto}.course-table{width:100%;border-collapse:collapse}.course-table th,.course-table td{padding:14px 18px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}.course-table th{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.04em;white-space:nowrap}.course-table td.title-cell{min-width:260px}.status-tag{display:inline-flex;align-items:center;padding:6px 10px;border-radius:999px;border:1px solid var(--line);background:rgba(15,23,42,.7);font-size:12px;white-space:nowrap}.status-tag.ok{border-color:rgba(34,197,94,.35);background:rgba(34,197,94,.12)}.status-tag.due{border-color:rgba(245,158,11,.45);background:rgba(245,158,11,.14)}.status-tag.bad{border-color:rgba(239,68,68,.42);background:rgba(239,68,68,.14)}.status-tag.neutral{border-color:rgba(148,163,184,.2);background:rgba(148,163,184,.08)}.status-tag.later{border-color:rgba(56,189,248,.28);background:rgba(56,189,248,.1)}.recommend{display:flex;justify-content:space-between;align-items:center;gap:12px;margin:0 18px 18px;padding:14px;border:1px solid rgba(56,189,248,.22);border-radius:16px;background:rgba(14,25,45,.9)}.recommend strong{display:block}.compact-actions{display:flex;gap:8px;flex-wrap:wrap;align-items:center}.upgrade-link{display:inline-flex;margin-left:8px;padding:5px 9px;border-radius:999px;border:1px solid rgba(56,189,248,.32);background:rgba(37,99,235,.18);font-size:12px;font-weight:700}.empty{padding:28px 18px;color:var(--muted)}.grid-two{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.field{display:grid;gap:8px}.field label{font-size:13px;color:var(--muted)}.field input,.field select{width:100%;padding:12px 13px;border-radius:12px;border:1px solid var(--line);background:rgba(10,16,29,.98);color:var(--text)}.checkbox{display:flex;align-items:center;justify-content:flex-start;gap:10px;padding:12px;border-radius:14px;border:1px solid var(--line);background:rgba(10,16,29,.98);text-align:left}.checkbox input[type=checkbox]{width:auto;min-width:18px;height:18px;margin:0;flex:0 0 auto;accent-color:#2563eb}.checkbox span{display:inline-block;line-height:1.2}.checkbox.master{border-color:rgba(56,189,248,.35);background:rgba(37,99,235,.14);font-weight:800}.document-provider-tools{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:8px}.document-provider-grid .checkbox{min-height:48px}.helper{font-size:12px;color:var(--muted)}.stack{display:grid;gap:12px}.provider-card,.zoom-card{background:var(--panel2);border:1px solid var(--line);border-radius:18px;padding:16px;display:grid;gap:12px}.provider-header,.zoom-header{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}.inline-actions{display:flex;gap:8px;flex-wrap:wrap}.danger-text{color:#fecaca}.top-account details{min-width:260px}.top-account summary{cursor:pointer;list-style:none;padding:10px 14px;border:1px solid var(--line);border-radius:16px;background:rgba(17,24,39,.92)}.top-account .dropdown{margin-top:10px;padding:14px;border:1px solid var(--line);border-radius:16px;background:rgba(11,18,32,.98);display:grid;gap:12px;min-width:280px;max-width:340px}.kicker{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em} .footer-note{padding:0 18px 18px;color:var(--muted);font-size:12px}
@media (max-width:980px){.layout{grid-template-columns:1fr}.stats,.grid-two{grid-template-columns:1fr}.top{flex-direction:column}.top-right{justify-content:flex-start}.top-account details{width:100%}.top-account .dropdown{min-width:0;max-width:none}}
.field input[type="color"]{height:46px;padding:6px;cursor:pointer}
.checkbox.linked-setting{opacity:.82}
.advanced-setup{border:1px solid var(--line);border-radius:14px;padding:12px;background:rgba(10,16,29,.72)}
.advanced-setup summary{cursor:pointer;font-weight:800}
.advanced-setup[open] summary{margin-bottom:10px}
.document-strip{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;padding:0 18px 18px}
.document-strip div{border:1px solid var(--line);border-radius:14px;background:rgba(10,16,29,.62);padding:12px}
.document-strip strong{display:block;font-size:22px}
.document-strip span{color:var(--muted);font-size:12px}
.expiry-warning-list{margin:0 18px 18px;display:grid;gap:8px}
.expiry-warning-row{display:flex;justify-content:space-between;gap:12px;align-items:center;border:1px solid rgba(245,158,11,.28);border-radius:12px;background:rgba(113,63,18,.16);padding:10px 12px}
.expiry-warning-main{display:grid;gap:2px;min-width:0}.expiry-warning-main strong{color:#fff}.expiry-warning-main small{color:#cbd5e1}
.expiry-warning-row span{color:#fde68a;font-size:13px;white-space:nowrap}
.certificate-progress{margin:0 18px 18px;border:1px solid rgba(56,189,248,.25);border-radius:16px;background:rgba(14,25,45,.86);padding:14px;display:grid;gap:8px}
.certificate-progress-top{display:flex;justify-content:space-between;gap:12px;align-items:center}
.certificate-progress-title{font-weight:800}.certificate-progress-detail{color:var(--muted);font-size:13px}
.certificate-progress.running .status-tag{border-color:rgba(56,189,248,.38);background:rgba(56,189,248,.13);color:#bae6fd}
.certificate-progress.idle{opacity:.86}
.certificate-workspace{position:relative}
.certificate-workspace.certificate-busy .certificate-progress{display:none}
.certificate-workspace.certificate-has-attention:not(.certificate-busy) .certificate-progress{display:none}
.certificate-workspace.certificate-busy .certificate-job-control:disabled{opacity:.55;cursor:not-allowed}
.certificate-busy-note{display:none;margin:0 18px 18px;border:1px solid rgba(56,189,248,.42);border-radius:16px;background:linear-gradient(135deg,rgba(8,47,73,.92),rgba(15,23,42,.96));padding:16px;color:#dbeafe;box-shadow:0 14px 30px rgba(2,8,23,.22)}
.certificate-workspace.certificate-busy .certificate-busy-note{display:block}
.certificate-busy-note-top{display:flex;align-items:center;justify-content:space-between;gap:14px}
.certificate-busy-main{display:flex;align-items:center;gap:12px}
.certificate-spinner{width:28px;height:28px;border-radius:999px;border:3px solid rgba(147,197,253,.22);border-top-color:#60a5fa;animation:tmSpin .9s linear infinite;flex:0 0 auto}
.certificate-busy-title{font-weight:900;color:#fff;font-size:17px}
.certificate-busy-detail{margin-top:3px;color:#bfdbfe;font-size:13px}
.certificate-busy-sub{margin-top:12px;color:#d6e4f0;font-size:13px}
@keyframes tmSpin{to{transform:rotate(360deg)}}
.certificate-attention{margin:0 18px 18px;border:1px solid rgba(250,204,21,.26);border-radius:14px;background:rgba(113,63,18,.18);padding:14px;display:grid;gap:10px}
.certificate-attention-title{font-weight:800;color:#fef3c7}
.certificate-attention-row{display:flex;justify-content:space-between;gap:14px;align-items:center;border-top:1px solid rgba(250,204,21,.16);padding-top:10px}
.certificate-attention-row:first-of-type{border-top:0;padding-top:0}
.certificate-attention-row strong{color:#fff}.certificate-attention-row p{margin:3px 0 0;color:#d6e4f0;font-size:13px}
.certificate-attention-actions{display:flex;flex-wrap:wrap;gap:8px;justify-content:flex-end}
.certificate-cross-provider{margin-top:10px;border-top:1px solid rgba(250,204,21,.14);padding-top:10px;display:grid;gap:8px}
.certificate-cross-provider .helper{color:#fde68a}
.certificate-cross-provider label{display:flex;gap:8px;align-items:flex-start;color:#e2e8f0;font-size:13px;font-weight:700}
.certificate-cross-provider small{display:block;color:#b6c7d8;font-weight:500;margin-top:2px}
.provider-chip{display:inline-flex;align-items:center;width:max-content;border-radius:999px;padding:5px 9px;background:rgba(255,255,255,.10);border:1px solid rgba(255,255,255,.16);color:#e2e8f0;font-size:12px;font-weight:800}
.btn.ghost{background:transparent;border:1px solid rgba(148,163,184,.28);color:#cbd5e1}
.btn.remove-fobs{background:#fff7ed!important;border:1px solid #fdba74!important;color:#9a3412!important;box-shadow:0 1px 0 rgba(15,23,42,.04);white-space:nowrap}
.btn.remove-fobs:hover{background:#ffedd5!important;border-color:#fb923c!important;color:#7c2d12!important}
.btn.remove-fobs:disabled{background:#f1f5f9!important;border-color:#cbd5e1!important;color:#94a3b8!important;box-shadow:none}
.provider-delete-progress{display:none;margin-top:8px;width:180px;max-width:100%;height:7px;border-radius:999px;background:#e2e8f0;overflow:hidden;border:1px solid rgba(15,23,42,.08)}
.provider-delete-progress i{display:block;width:42%;height:100%;border-radius:999px;background:#38bdf8;animation:tmSlide 1.05s infinite ease-in-out}
.provider-certificate-delete-form.is-working .provider-delete-progress{display:block}
.provider-certificate-delete-form.is-working .btn.remove-fobs{background:#eff6ff!important;border-color:#93c5fd!important;color:#1e3a8a!important}
.provider-delete-cancel{display:none;margin-top:7px;background:#f8fafc!important;border:1px solid #cbd5e1!important;color:#334155!important}
.provider-certificate-delete-form.is-working .provider-delete-cancel{display:inline-flex}
@keyframes tmSlide{0%{transform:translateX(-120%)}50%{transform:translateX(90%)}100%{transform:translateX(250%)}}
.cert-more summary{list-style:none;cursor:pointer}.cert-more summary::-webkit-details-marker{display:none}.cert-more[open] summary{margin-bottom:8px}
.nav-alert{display:inline-flex;align-items:center;justify-content:center;min-width:18px;height:18px;margin-left:6px;border-radius:999px;background:rgba(250,204,21,.18);border:1px solid rgba(250,204,21,.42);color:#fde68a;font-size:12px;font-weight:900}
.add-document-drawer{margin:0 18px 18px;border:1px solid var(--line);border-radius:16px;background:rgba(10,16,29,.58)}
.add-document-drawer summary{cursor:pointer;list-style:none;padding:14px 16px;display:flex;justify-content:space-between;align-items:center;font-weight:800}
.add-document-drawer form{padding:0 16px 16px}
.document-panel .tablewrap{overflow:visible}
.document-panel .course-table,.document-panel .course-table tbody{display:block;width:100%}
.document-panel .course-table thead{display:none}
.document-panel .course-table tr{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;align-items:center;padding:14px 18px;border-bottom:1px solid var(--line)}
.document-panel .course-table td{display:block;border:0;padding:0}
.document-panel .course-table td:nth-child(3){grid-column:1;color:var(--muted);font-size:12px}
.document-panel .course-table td:nth-child(4){grid-column:1 / -1;display:none}
.document-panel .course-table td:nth-child(5){grid-column:2;grid-row:1 / span 2}
.compact-checklist{padding:0}
.compact-checklist summary{cursor:pointer;list-style:none}
.compact-checklist .provider-header{padding:14px 20px;align-items:center}
.compact-checklist .document-requirement-grid{padding-top:12px}
.cert-provider-card{position:relative}
.cert-provider-card[open]>.provider-refresh-form{position:absolute;top:9px;right:104px;margin:0;z-index:2}
.cert-provider-card[open]>.provider-refresh-form .btn{white-space:nowrap}
.document-provider-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
/* Certificates page readability polish: page-scoped colours only, no behaviour changes */
.certificate-panel{background:linear-gradient(180deg,rgba(13,27,52,.96),rgba(11,18,32,.94));border-color:rgba(125,211,252,.20);box-shadow:0 16px 36px rgba(0,0,0,.16)}
.certificate-panel .hero{background:linear-gradient(135deg,rgba(14,116,144,.16),rgba(37,99,235,.08));border-bottom:1px solid rgba(125,211,252,.16)}
.certificate-panel .hero h2,.certificate-panel .head h3{font-size:22px;color:#f8fafc;letter-spacing:.01em}
.certificate-panel .hero .muted,.certificate-panel .head p{color:#bcd7f5;font-size:15px;line-height:1.45}
.certificate-dashboard-panel .document-strip{padding-top:18px;border-top:1px solid rgba(125,211,252,.08)}
.certificate-dashboard-panel .document-strip div{background:linear-gradient(180deg,rgba(15,52,85,.58),rgba(10,16,29,.84));border-color:rgba(125,211,252,.18);box-shadow:inset 0 1px 0 rgba(255,255,255,.035)}
.certificate-dashboard-panel .document-strip strong{color:#ffffff;font-size:25px;line-height:1.05}
.certificate-dashboard-panel .document-strip span{color:#bfd3e8;font-size:13px}
.certificate-progress{background:linear-gradient(135deg,rgba(8,47,73,.78),rgba(15,23,42,.88));border-color:rgba(56,189,248,.36)}
.certificate-progress-title{color:#f8fafc;font-size:16px}.certificate-progress-detail{color:#c4d9ee}
.add-document-drawer{background:linear-gradient(180deg,rgba(15,23,42,.82),rgba(10,16,29,.88));border-color:rgba(148,163,184,.22)}
.add-document-drawer summary{font-size:17px;color:#f8fafc}.add-document-drawer[open] summary{border-bottom:1px solid rgba(148,163,184,.16);margin-bottom:14px}
.document-provider-tools{background:rgba(8,47,73,.26);border:1px solid rgba(125,211,252,.18);border-radius:14px;padding:10px 12px}
.checkbox{background:rgba(15,23,42,.86);border-color:rgba(148,163,184,.20)}.checkbox.master{background:rgba(14,116,144,.28);border-color:rgba(125,211,252,.36)}
.certificate-fobs-panel>.head{background:linear-gradient(135deg,rgba(14,116,144,.18),rgba(37,99,235,.06));border-bottom-color:rgba(125,211,252,.16)}
.certificate-fobs-panel .block{background:rgba(8,13,25,.18)}
.cert-provider-card{background:linear-gradient(180deg,rgba(15,23,42,.92),rgba(10,16,29,.92));border-color:rgba(125,211,252,.17)}
.cert-provider-card summary.provider-header{padding:4px 4px 2px}.cert-provider-card summary.provider-header strong{font-size:18px;color:#ffffff}.cert-provider-card[open]{border-color:rgba(56,189,248,.35);box-shadow:0 0 0 1px rgba(56,189,248,.06)}
.cert-provider-card .status-tag{background:rgba(30,64,106,.42);border-color:rgba(147,197,253,.28);color:#dbeafe}
.certificate-fobs-panel .course-table th{color:#bfdbfe;background:rgba(8,47,73,.24);border-bottom-color:rgba(125,211,252,.18)}
.certificate-fobs-panel .course-table td{border-bottom-color:rgba(148,163,184,.14)}
.certificate-fobs-panel .course-table tbody tr:hover{background:rgba(14,116,144,.10)}

/* Simple certificates page readability polish */
.certificate-dashboard-panel .document-strip{grid-template-columns:repeat(2,minmax(0,1fr));max-width:760px}
.certificate-dashboard-panel .document-strip div{background:rgba(15,23,42,.82);border-color:rgba(148,163,184,.20)}
.certificate-fobs-panel{background:#f3f7fb;color:#102033;border-color:rgba(15,23,42,.14);box-shadow:0 14px 38px rgba(0,0,0,.16)}
.certificate-fobs-panel>.fobs-head{background:linear-gradient(180deg,#ffffff,#eef6ff);border-bottom:1px solid rgba(15,23,42,.12)}
.certificate-fobs-panel>.fobs-head h3{color:#0f172a;font-size:24px}
.certificate-fobs-panel>.fobs-head p{color:#475569;font-size:15px}
.certificate-fobs-panel .block{background:#f3f7fb;padding:20px 22px}
.cert-provider-card{background:#ffffff!important;border:2px solid color-mix(in srgb, var(--provider-color) 38%, #dbeafe)!important;border-radius:18px;box-shadow:0 8px 20px rgba(15,23,42,.07);overflow:hidden}
.cert-provider-card+.cert-provider-card{margin-top:8px}
.cert-provider-card summary.provider-header{padding:18px 20px!important;align-items:center;cursor:pointer;list-style:none;background:linear-gradient(90deg,color-mix(in srgb, var(--provider-color) 13%, white),#ffffff)}
.cert-provider-card summary.provider-header::-webkit-details-marker{display:none}
.provider-summary-main{display:flex;align-items:center;gap:14px;min-width:0}
.provider-colour-dot{width:18px;height:18px;border-radius:999px;background:var(--provider-color);box-shadow:0 0 0 4px color-mix(in srgb, var(--provider-color) 16%, transparent)}
.cert-provider-card summary.provider-header strong{display:block;color:#0f172a!important;font-size:22px!important;line-height:1.15}
.provider-summary-helper{display:block;color:#64748b;font-size:13px;margin-top:3px}
.provider-summary-right{display:flex;align-items:center;gap:10px;flex-shrink:0}
.cert-provider-card .status-tag{background:#ffffff!important;border-color:rgba(15,23,42,.16)!important;color:#0f172a!important;font-weight:800;font-size:14px;padding:8px 12px}
.expand-word{display:inline-flex;align-items:center;gap:6px;border-radius:999px;padding:8px 12px;background:color-mix(in srgb, var(--provider-color) 14%, #ffffff);color:#0f172a;border:1px solid color-mix(in srgb, var(--provider-color) 30%, #cbd5e1);font-weight:800;font-size:13px}
.expand-word::after{content:'v';font-size:10px;color:var(--provider-color)}
.cert-provider-card[open] .expand-word{background:var(--provider-color);color:#fff;border-color:var(--provider-color)}
.cert-provider-card[open] .expand-word::after{content:'^';color:#fff}
.cert-provider-card[open]{box-shadow:0 12px 28px rgba(15,23,42,.12);border-color:var(--provider-color)!important}
.cert-provider-card[open]>.provider-refresh-form{position:static!important;margin:0 20px 16px!important;padding-top:0;z-index:auto}
.cert-provider-card[open]>.provider-refresh-form .btn{background:color-mix(in srgb, var(--provider-color) 16%, #ffffff);border:1px solid color-mix(in srgb, var(--provider-color) 38%, #cbd5e1);color:#0f172a}
.certificate-fobs-panel .tablewrap{margin:0 20px 20px;border:1px solid rgba(15,23,42,.12);border-radius:16px;background:#fff;box-shadow:inset 4px 0 0 var(--provider-color)}
.certificate-fobs-panel .course-table th{color:#475569!important;background:#f8fafc!important;border-bottom-color:rgba(15,23,42,.12)!important}
.certificate-fobs-panel .course-table td{color:#0f172a;border-bottom-color:rgba(15,23,42,.10)!important}
.certificate-fobs-panel .course-table td strong{color:#0f172a}
.certificate-fobs-panel .course-table tbody tr:hover{background:color-mix(in srgb, var(--provider-color) 7%, #ffffff)!important}
.certificate-fobs-panel .helper{color:#64748b}
.certificate-fobs-panel .empty{color:#64748b;background:#fff;margin:0 20px 20px;border-radius:14px;border:1px dashed rgba(15,23,42,.18)}
@media (max-width:720px){.certificate-dashboard-panel .document-strip{grid-template-columns:1fr}.provider-summary-right{align-items:flex-end;flex-direction:column}.cert-provider-card summary.provider-header strong{font-size:19px!important}}
@media (max-width:720px){.certificate-attention-row{align-items:flex-start;flex-direction:column}.certificate-attention-actions{justify-content:flex-start}}


/* Softer final certificates FOBS section: clear, but not bright */
.certificate-fobs-panel{background:#dbe7f2!important;color:#102033!important;border-color:rgba(96,165,250,.20)!important;box-shadow:0 12px 30px rgba(2,8,23,.20)!important}
.certificate-fobs-panel>.fobs-head{background:linear-gradient(180deg,#eaf2fb,#dbe7f2)!important;border-bottom:1px solid rgba(15,23,42,.14)!important}
.certificate-fobs-panel>.fobs-head h3{color:#0f172a!important}
.certificate-fobs-panel>.fobs-head p{color:#334155!important}
.certificate-fobs-panel .block{background:#dbe7f2!important;padding:18px 20px!important}
.cert-provider-card{background:#eef4fa!important;border:1px solid color-mix(in srgb, var(--provider-color) 30%, #b8c7d8)!important;border-radius:16px!important;box-shadow:0 5px 14px rgba(15,23,42,.09)!important}
.cert-provider-card+.cert-provider-card{margin-top:10px!important}
.cert-provider-card summary.provider-header{padding:16px 18px!important;background:linear-gradient(90deg,color-mix(in srgb, var(--provider-color) 9%, #eef4fa),#eef4fa)!important}
.provider-colour-dot{width:15px!important;height:15px!important;box-shadow:0 0 0 4px color-mix(in srgb, var(--provider-color) 11%, transparent)!important}
.cert-provider-card summary.provider-header strong{font-size:20px!important;color:#0f172a!important}
.provider-summary-helper{color:#475569!important;font-size:13px!important}
.cert-provider-card .status-tag{background:#f8fafc!important;border-color:rgba(15,23,42,.14)!important;color:#0f172a!important}
.expand-word{background:color-mix(in srgb, var(--provider-color) 10%, #f8fafc)!important;border-color:color-mix(in srgb, var(--provider-color) 25%, #cbd5e1)!important;color:#0f172a!important}
.cert-provider-card[open] .expand-word{background:color-mix(in srgb, var(--provider-color) 75%, #334155)!important;color:#fff!important;border-color:transparent!important}
.cert-provider-card[open]{border-color:color-mix(in srgb, var(--provider-color) 55%, #94a3b8)!important;box-shadow:0 8px 22px rgba(15,23,42,.13)!important}
.certificate-fobs-panel .tablewrap{background:#f8fafc!important;border-color:rgba(15,23,42,.13)!important;box-shadow:inset 4px 0 0 color-mix(in srgb, var(--provider-color) 65%, #94a3b8)!important}
.certificate-fobs-panel .course-table th{background:#eef2f7!important;color:#334155!important}
.certificate-fobs-panel .course-table td{background:#f8fafc!important;color:#0f172a!important}
.certificate-fobs-panel .course-table tbody tr:hover td{background:color-mix(in srgb, var(--provider-color) 6%, #f8fafc)!important}

.document-requirement-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.document-requirement{display:flex;justify-content:space-between;gap:10px;align-items:center;border:1px solid var(--line);border-radius:12px;padding:10px;background:rgba(10,16,29,.58)}
@media (max-width:720px){.document-provider-grid,.document-requirement-grid,.document-strip{grid-template-columns:1fr}.document-panel .course-table tr{grid-template-columns:1fr}.document-panel .course-table td:nth-child(5){grid-column:1;grid-row:auto}.cert-provider-card[open]>.provider-refresh-form{position:static;margin:0 20px 12px}.compact-checklist .provider-header{padding:14px 16px}}

.live-status-panel{margin-top:18px}
.live-status-panel details summary{list-style:none;cursor:pointer;padding:16px 18px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid var(--line)}
.live-status-panel details summary::-webkit-details-marker{display:none}
.live-status-body{padding:18px;display:grid;gap:14px}
.live-status-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
.live-status-card{background:var(--panel2);border:1px solid var(--line);border-radius:14px;padding:13px;min-height:78px}
.live-status-card strong{display:block;font-size:12px;color:var(--muted);margin-bottom:6px}
.live-status-card div{font-size:15px;line-height:1.35}
.live-status-list{display:grid;gap:8px}
.live-status-row{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;padding:10px 12px;border:1px solid var(--line);border-radius:12px;background:rgba(15,23,42,.55)}
.live-status-row span:first-child{color:#dbeafe}
.live-status-row span:last-child{color:var(--muted);text-align:right}
.live-status-note{font-size:12px;color:var(--muted)}
@media (max-width:980px){.live-status-grid{grid-template-columns:1fr}.live-status-row{display:grid}.live-status-row span:last-child{text-align:left}}


.scan-form{display:flex;gap:8px;flex-wrap:wrap;align-items:end;justify-content:flex-end}
.scan-field{display:grid;gap:5px}
.scan-field label{font-size:11px;color:var(--muted)}
.scan-field select{padding:9px 10px;border-radius:10px;border:1px solid var(--line);background:rgba(10,16,29,.98);color:var(--text);min-width:130px}
.status-tag.ok{border-color:rgba(34,197,94,.35);background:rgba(34,197,94,.12);color:#bbf7d0}
.status-tag.due{border-color:rgba(245,158,11,.35);background:rgba(245,158,11,.12);color:#fde68a}
.status-tag.neutral{border-color:rgba(148,163,184,.35);background:rgba(148,163,184,.12);color:#e5e7eb}
.status-tag.bad{border-color:rgba(239,68,68,.35);background:rgba(239,68,68,.12);color:#fecaca}


.calendar-toolbar{display:flex;gap:8px 14px;align-items:center;flex-wrap:wrap;margin:0 0 14px;color:#aab8cf;font-size:13px}
.cal-dot{width:10px;height:10px;border-radius:999px;display:inline-block}
.cal-dot.ok{background:#16a34a}.cal-dot.due{background:#f59e0b}.cal-dot.later{background:#64748b}.cal-dot.attention{background:#dc2626}
#tmCalendar{background:#0f172a;border:1px solid #25324a;border-radius:18px;padding:12px;min-height:680px}
.fc{color:#e5edf8}
.fc .fc-toolbar-title{font-size:22px;font-weight:800;color:#f8fafc}
.fc .fc-button{border-radius:999px!important;border:0!important;background:#2563eb!important}
.fc .fc-button-primary:not(:disabled).fc-button-active{background:#1e40af!important}
.fc .fc-daygrid-day-number{color:#dbeafe;text-decoration:none}
.fc .fc-col-header-cell{background:#eaf2ff!important}
.fc .fc-col-header-cell-cushion{color:#0f172a!important;text-decoration:none;font-weight:800}
.fc .fc-list-day-cushion{background:#eaf2ff!important}
.fc .fc-list-day-text,.fc .fc-list-day-side-text,.fc .fc-list-day-cushion a{color:#0f172a!important;text-decoration:none;font-weight:800}
.fc .fc-daygrid-day{background:#111827}
.fc .fc-day-today{background:#172554!important}
.fc-theme-standard td,.fc-theme-standard th,.fc-theme-standard .fc-scrollgrid{border-color:#25324a!important}
.fc-event{border:1px solid transparent!important;border-radius:10px!important;padding:2px 4px!important;cursor:pointer}
.tm-cal-provider-event{box-shadow:none}
.tm-cal-ok,.tm-cal-due,.tm-cal-later,.tm-cal-attention,.tm-cal-neutral{box-shadow:none}
.course-modal{position:fixed;inset:0;background:rgba(2,6,23,.62);display:none;align-items:center;justify-content:center;padding:20px;z-index:9999}
.course-modal.open{display:flex}
.course-modal-card{background:#0f172a;color:#f8fafc;border:1px solid #25324a;border-radius:24px;box-shadow:0 24px 80px rgba(0,0,0,.45);padding:24px;max-width:560px;width:100%;position:relative}
.modal-close{position:absolute;right:18px;top:14px;border:0;background:#1e293b;color:#f8fafc;border-radius:999px;width:32px;height:32px;font-size:22px;cursor:pointer}
.modal-meta{display:flex;gap:12px;color:#aab8cf;margin:8px 0 14px}
.advice-box{background:#111827;border:1px solid #25324a;border-radius:16px;padding:14px;margin-top:16px}
.advice-box p{margin:6px 0 0;color:#cbd5e1}

.fc-event{white-space:normal!important;overflow:visible!important}
.fc-daygrid-event{align-items:flex-start!important}
.tm-cal-event{display:grid;gap:1px;line-height:1.18;white-space:normal;overflow-wrap:anywhere;max-width:100%}
.tm-cal-time{font-size:11px;font-weight:800;opacity:.9}
.tm-cal-title{font-size:12px;font-weight:700}
.tm-cal-provider{font-size:10px;opacity:.82}
.modal-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:12px 0}
.modal-field{border:1px solid #25324a;border-radius:14px;background:#111827;padding:10px}
.modal-field span{display:block;color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px}
.modal-field strong{display:block;color:#f8fafc;font-size:14px;overflow-wrap:anywhere}
.modal-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:16px}
.modal-actions .btn.disabled{opacity:.45;pointer-events:none}
.launch-status{min-height:20px;margin:10px 0 0;color:#bfdbfe;font-size:13px;line-height:1.35}
.launch-status.warn{color:#fde68a}
.launch-status.error{color:#fecaca}

.tm-progress-bubble{position:fixed;right:16px;bottom:16px;width:min(380px,calc(100vw - 32px));z-index:9999;border:1px solid var(--line);border-radius:18px;background:#0f172a;box-shadow:0 18px 45px rgba(0,0,0,.5);overflow:hidden}
.tm-progress-bubble summary{list-style:none;cursor:pointer;display:grid;grid-template-columns:34px minmax(0,1fr) auto;gap:4px 12px;align-items:center;padding:12px 14px;min-width:0}
.tm-progress-bubble summary::-webkit-details-marker{display:none}
.tm-progress-icon{width:34px;height:34px;grid-row:1 / span 2;border-radius:999px;display:grid;place-items:center;background:#1d4ed8;color:#fff;font-weight:900}
.tm-progress-main{min-width:0;display:block}.tm-progress-title{display:block;font-size:15px;line-height:1.2;font-weight:800;color:#f8fafc;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.tm-progress-subtitle{display:block;grid-column:2 / -1;font-size:12px;line-height:1.25;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-top:1px}
.tm-progress-state{justify-self:end;align-self:center;font-size:11px;color:#bfdbfe;background:rgba(37,99,235,.18);border:1px solid rgba(96,165,250,.28);padding:4px 8px;border-radius:999px}
.tm-progress-body{border-top:1px solid var(--line);padding:12px 14px 14px;display:grid;gap:10px;max-height:320px;overflow:auto;background:#0b1220}
.tm-progress-step{display:grid;grid-template-columns:18px minmax(0,1fr);gap:8px;align-items:start;color:#dbeafe;font-size:13px;line-height:1.35}.tm-progress-step i{width:10px;height:10px;border-radius:999px;background:#60a5fa;margin-top:4px}.tm-progress-step span{min-width:0;overflow-wrap:anywhere}.tm-progress-step small{display:block;color:#9ca8bb;font-size:12px;line-height:1.35;margin-top:2px;overflow-wrap:anywhere}
.tm-progress-bubble.idle .tm-progress-icon{background:#475569}.tm-progress-bubble.running .tm-progress-icon{animation:tmPulse 1.2s infinite}.tm-progress-bubble.error .tm-progress-icon{background:#991b1b}
.setup-list{display:grid;gap:12px}
.setup-provider{border:1px solid var(--line);border-radius:14px;background:rgba(15,23,42,.56);padding:14px;display:grid;gap:12px}
.setup-provider-main{display:flex;align-items:center;justify-content:space-between;gap:12px}
.setup-provider-title{display:flex;align-items:center;gap:10px;font-weight:800;color:#f8fafc}
.setup-provider-title input{width:18px;height:18px}
.setup-provider-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
.setup-provider.disabled{opacity:.58}
.setup-checklist{display:grid;gap:9px;margin-top:14px}
.setup-checklist div{display:flex;align-items:center;gap:8px;color:#dbeafe}
.setup-checklist b{display:grid;place-items:center;width:22px;height:22px;border-radius:999px;background:rgba(37,99,235,.18);border:1px solid rgba(96,165,250,.28);color:#bfdbfe}
.manual-provider-field{display:none}
form:has(#provider_preset option[value="manual"]:checked) .manual-provider-field{display:grid!important}
.dashboard-home{display:grid;gap:18px}
.tm-hero-board{display:grid;grid-template-columns:minmax(0,1.45fr) minmax(300px,.8fr);gap:16px;align-items:stretch}
.tm-ready-panel{border:1px solid rgba(125,211,252,.18);border-radius:22px;background:linear-gradient(135deg,rgba(15,23,42,.94),rgba(12,28,48,.90));padding:22px;display:grid;gap:18px;min-width:0}
.tm-ready-panel.ready{border-color:rgba(34,197,94,.28);background:linear-gradient(135deg,rgba(12,35,35,.96),rgba(14,42,58,.88))}
.tm-ready-panel.attention{border-color:rgba(245,158,11,.32);background:linear-gradient(135deg,rgba(42,30,13,.96),rgba(28,35,52,.88))}
.tm-ready-panel.running{border-color:rgba(96,165,250,.34)}
.tm-ready-top{display:flex;justify-content:space-between;gap:14px;align-items:flex-start}
.tm-ready-label{display:inline-flex;align-items:center;gap:8px;width:max-content;border-radius:999px;padding:7px 11px;background:rgba(148,163,184,.12);border:1px solid rgba(148,163,184,.18);font-size:12px;font-weight:900;text-transform:uppercase;letter-spacing:.04em;color:#dbeafe}
.tm-ready-dot{width:10px;height:10px;border-radius:999px;background:#22c55e}.tm-ready-panel.attention .tm-ready-dot{background:#f59e0b}.tm-ready-panel.running .tm-ready-dot{background:#60a5fa;animation:tmPulse 1.2s infinite}
.tm-ready-panel h2{margin:0;color:#fff;font-size:34px;line-height:1.05;letter-spacing:0}.tm-ready-panel p{margin:0;color:#cbd5e1;line-height:1.45;max-width:720px}
.tm-quick-actions{display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end}.tm-quick-actions .scan-form{display:flex;flex-wrap:wrap;gap:10px;align-items:end}.tm-quick-actions .scan-field{min-width:155px}.tm-quick-actions label{font-size:12px;color:#a8c4e8;margin-bottom:5px}.tm-quick-actions select{min-height:41px}
.tm-metric-row{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}.tm-metric{border:1px solid rgba(148,163,184,.14);border-radius:16px;background:rgba(2,6,23,.22);padding:13px;min-width:0}.tm-metric strong{display:block;font-size:24px;color:#fff;line-height:1}.tm-metric span{display:block;margin-top:5px;color:#aeb9c8;font-size:12px}
.tm-next-panel{border:1px solid rgba(148,163,184,.16);border-radius:22px;background:rgba(15,23,42,.82);padding:18px;display:grid;gap:14px;min-width:0}
.tm-panel-title{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}.tm-panel-title h3{margin:0;color:#fff;font-size:18px}.tm-panel-title p{margin:4px 0 0;color:#9ca3af;font-size:13px}.tm-panel-title a{color:#bfdbfe;font-size:13px;font-weight:800}
.tm-timeline{display:grid;gap:10px}.tm-course-card{display:grid;grid-template-columns:52px minmax(0,1fr) auto;gap:12px;align-items:center;border:1px solid rgba(148,163,184,.12);border-radius:16px;background:rgba(8,13,25,.46);padding:12px;min-width:0}.tm-course-time{display:grid;place-items:center;min-height:50px;border-radius:14px;background:rgba(37,99,235,.16);border:1px solid rgba(96,165,250,.22);color:#dbeafe;font-weight:900}.tm-course-main{min-width:0}.tm-course-main strong{display:block;color:#fff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.tm-course-main span{display:block;color:#aeb9c8;font-size:13px;margin-top:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.tm-provider-dot{display:inline-block;width:9px;height:9px;border-radius:99px;margin-right:6px;vertical-align:middle}
.tm-dashboard-grid{display:grid;grid-template-columns:minmax(0,1.05fr) minmax(320px,.72fr);gap:18px}.tm-section{border:1px solid rgba(148,163,184,.16);border-radius:20px;background:rgba(15,23,42,.74);padding:18px;min-width:0}.tm-section + .tm-section{margin-top:18px}
.tm-recommend-card{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:16px;align-items:center;border:1px solid rgba(56,189,248,.24);border-radius:20px;background:linear-gradient(135deg,rgba(8,47,73,.34),rgba(15,23,42,.78));padding:18px}.tm-recommend-card strong{display:block;color:#fff;font-size:18px}.tm-recommend-card .helper{margin-top:6px}
.tm-provider-health{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px}.tm-provider-tile{border:1px solid rgba(148,163,184,.14);border-radius:16px;background:rgba(8,13,25,.38);padding:13px;display:grid;gap:8px;min-width:0}.tm-provider-name{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:10px;color:#fff;font-weight:850}.tm-provider-name span:first-child{min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.tm-chip{display:inline-flex;width:max-content;align-items:center;border-radius:999px;padding:5px 9px;font-size:12px;font-weight:850;border:1px solid rgba(148,163,184,.18);background:rgba(148,163,184,.10);color:#dbeafe;white-space:nowrap;flex-shrink:0}.tm-chip.ok{border-color:rgba(34,197,94,.24);background:rgba(34,197,94,.12);color:#bbf7d0}.tm-chip.warn{border-color:rgba(245,158,11,.28);background:rgba(245,158,11,.12);color:#fde68a}.tm-chip.neutral{color:#cbd5e1}
.tm-items-list{display:grid;gap:10px}.tm-check-item{border:1px solid rgba(245,158,11,.22);border-radius:16px;background:rgba(42,30,13,.18);padding:13px}.tm-check-item strong{display:block;color:#fff}.tm-check-item p{margin:5px 0 0;color:#cbd5e1;font-size:13px;line-height:1.35}
.tm-mini-calendar{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:6px}.tm-mini-day{min-height:64px;border:1px solid rgba(148,163,184,.12);border-radius:12px;background:rgba(8,13,25,.34);padding:7px;display:grid;align-content:start;gap:5px}.tm-mini-day b{font-size:12px;color:#cbd5e1}.tm-mini-event{height:7px;border-radius:999px;background:#2563eb}
.tm-activity-list{display:grid;gap:10px}.tm-activity-row{border-left:3px solid #38bdf8;padding:4px 0 4px 10px}.tm-activity-row strong{display:block;color:#f8fafc;font-size:14px}.tm-activity-row span{display:block;color:#9ca3af;font-size:12px;line-height:1.35;margin-top:2px}
.tm-table-disclosure summary{cursor:pointer;list-style:none;display:flex;justify-content:space-between;gap:12px;align-items:center}.tm-table-disclosure summary::-webkit-details-marker{display:none}
.dashboard-home{color:#172033}
.dashboard-home .helper,.dashboard-home .tm-panel-title p{color:#64748b}
.dashboard-home .empty{background:#eef4fb;color:#475569;border-color:rgba(15,23,42,.10)}
.tm-ready-panel{background:radial-gradient(circle at 16% 12%,rgba(224,242,254,.48),transparent 30%),radial-gradient(circle at 82% 18%,rgba(219,234,254,.34),transparent 32%),linear-gradient(135deg,#0891b2 0%,#0ea5e9 48%,#2563eb 100%);border:0;box-shadow:0 22px 55px rgba(14,165,233,.22)}
.tm-ready-panel.ready{background:radial-gradient(circle at 16% 12%,rgba(224,242,254,.56),transparent 30%),radial-gradient(circle at 82% 18%,rgba(219,234,254,.38),transparent 32%),linear-gradient(135deg,#0891b2 0%,#0ea5e9 48%,#2563eb 100%);border:0}
.tm-ready-panel.attention{background:radial-gradient(circle at 16% 12%,rgba(255,251,235,.40),transparent 30%),linear-gradient(135deg,#f59e0b 0%,#14b8a6 52%,#2563eb 100%);border:0}
.tm-ready-panel h2{font-size:38px}.tm-ready-panel p{color:#e0f2fe;font-size:16px}
.tm-ready-label{background:rgba(255,255,255,.18);border-color:rgba(255,255,255,.20);color:#fff}.tm-ready-panel .tm-chip{background:rgba(255,255,255,.16);border-color:rgba(255,255,255,.22);color:#fff}
.tm-metric{background:rgba(255,255,255,.14);border-color:rgba(255,255,255,.16)}.tm-metric span{color:#dbeafe}
.tm-next-panel,.tm-section,.tm-recommend-card{background:#f8fbff;border:1px solid rgba(15,23,42,.10);box-shadow:0 14px 35px rgba(15,23,42,.08);color:#172033}
.tm-panel-title h3,.tm-recommend-card strong,.tm-provider-name,.tm-course-main strong,.tm-activity-row strong{color:#0f172a}
.tm-panel-title a{color:#1d4ed8}
.tm-course-card{background:#ffffff;border-color:rgba(15,23,42,.08);box-shadow:0 6px 18px rgba(15,23,42,.05)}
.tm-course-main span{color:#475569}.tm-course-time{background:#e8f1ff;border-color:#bfdbfe;color:#1e3a8a}
.tm-recommend-card{background:linear-gradient(135deg,#eef8ff,#f8fbff);border-color:#bfdbfe}
.tm-provider-tile{background:#ffffff;border-color:rgba(15,23,42,.09);box-shadow:0 6px 18px rgba(15,23,42,.04)}
.tm-check-item{background:#fff8e8;border-color:#fde68a}.tm-check-item strong{color:#172033}.tm-check-item p{color:#475569}
.tm-mini-day{background:#ffffff;border-color:rgba(15,23,42,.09)}.tm-mini-day b{color:#334155}
.tm-activity-row strong{color:#172033}.tm-activity-row span{color:#64748b}
.tm-table-disclosure strong{color:#0f172a}

/* Concept 1 shared app polish */
body{background:linear-gradient(180deg,#091528 0,#0d1d35 106px,#eef5f9 106px,#eef5f9 100%);color:#203047}
.top{background:rgba(9,21,40,.96);border-bottom:1px solid rgba(125,211,252,.16);box-shadow:0 12px 32px rgba(2,6,23,.18)}
.top h1{color:#f8fafc}.top .muted{color:#bcd7f5}
.mark{background:linear-gradient(135deg,#14b8a6,#2563eb);box-shadow:0 10px 24px rgba(20,184,166,.22)}
.layout{align-items:start;padding:20px;gap:20px;min-height:calc(100vh - 107px);background:linear-gradient(180deg,#e8f2f8,#edf5f9)}
.sidebar{position:sticky;top:18px;background:rgba(11,24,44,.92);border-color:rgba(125,211,252,.16);box-shadow:0 18px 44px rgba(2,6,23,.18)}
.sidebar.panel{background:rgba(11,24,44,.92);border-color:rgba(125,211,252,.16)}
.sidebar .muted{color:#bcd7f5}
.nav,.sub,details.navgroup summary{border-radius:16px;background:transparent;border-color:transparent;color:#dbeafe}
.nav-left{display:flex;align-items:center;gap:11px;min-width:0}.nav-left span:last-child{min-width:0;overflow:hidden;text-overflow:ellipsis}
.nav-icon{width:22px;height:22px;border-radius:8px;display:inline-grid;place-items:center;flex:0 0 auto;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.12);position:relative}
.nav-icon:before,.nav-icon:after{content:"";position:absolute;box-sizing:border-box}
.nav-icon-dashboard:before{width:10px;height:10px;border-radius:3px;border:2px solid currentColor;opacity:.9}.nav-icon-dashboard:after{width:4px;height:4px;border-radius:2px;background:currentColor;right:4px;bottom:4px}
.nav-icon-providers:before{width:12px;height:8px;border-radius:999px 999px 4px 4px;border:2px solid currentColor;top:5px}.nav-icon-providers:after{width:16px;height:5px;border-radius:999px;border:2px solid currentColor;bottom:4px}
.nav-icon-manage:before{width:12px;height:12px;border-radius:4px;border:2px solid currentColor}.nav-icon-manage:after{width:7px;height:2px;background:currentColor;transform:rotate(45deg);right:3px;bottom:5px;border-radius:2px}
.nav-icon-zoom:before{width:13px;height:9px;border-radius:4px;border:2px solid currentColor;left:4px}.nav-icon-zoom:after{width:6px;height:7px;border:2px solid currentColor;border-left:0;right:2px;border-radius:0 3px 3px 0}
.nav-icon-sync:before{width:13px;height:13px;border-radius:999px;border:2px solid currentColor;border-right-color:transparent}.nav-icon-sync:after{width:5px;height:5px;border-top:2px solid currentColor;border-right:2px solid currentColor;right:4px;top:5px;transform:rotate(45deg)}
.nav-icon-activity:before{width:13px;height:11px;border-radius:4px;border:2px solid currentColor}.nav-icon-activity:after{width:7px;height:2px;background:currentColor;left:7px;top:10px;box-shadow:0 4px 0 currentColor}
.nav-icon-calendar:before{width:14px;height:13px;border-radius:4px;border:2px solid currentColor}.nav-icon-calendar:after{width:10px;height:2px;background:currentColor;top:7px}
.nav-icon-files:before{width:11px;height:14px;border-radius:3px;border:2px solid currentColor}.nav-icon-files:after{width:6px;height:2px;background:currentColor;left:8px;top:8px;box-shadow:0 4px 0 currentColor}
.nav.active .nav-icon,.sub.active .nav-icon{background:rgba(125,211,252,.16);border-color:rgba(125,211,252,.30);color:#e0f2fe}
.nav:hover,.sub:hover,details.navgroup summary:hover{background:rgba(255,255,255,.07);border-color:rgba(125,211,252,.20)}
.nav.active,.sub.active,.active{background:linear-gradient(135deg,rgba(20,184,166,.22),rgba(37,99,235,.26));border-color:rgba(125,211,252,.34);box-shadow:inset 0 1px 0 rgba(255,255,255,.06)}
.badge{background:rgba(255,255,255,.10);border-color:rgba(255,255,255,.14);color:#dbeafe}
.main{gap:20px;align-content:start;padding:0;border:0;background:transparent;box-shadow:none}
.live-status-panel,.footer-note{display:none!important}
.section-activity .main::after,.section-zoom_accounts .main::after{content:"";display:block;min-height:260px;border-radius:28px;border:1px solid rgba(51,65,85,.08);background:radial-gradient(circle at 14% 28%,rgba(20,184,166,.20),transparent 22%),radial-gradient(circle at 86% 24%,rgba(37,99,235,.16),transparent 26%),linear-gradient(135deg,rgba(255,255,255,.44),rgba(226,240,248,.86));box-shadow:inset 0 1px 0 rgba(255,255,255,.82),0 14px 34px rgba(15,35,58,.06)}
.section-activity .main::before,.section-zoom_accounts .main::before{content:"";display:none}
.panel,.card,.provider-card,.zoom-card{background:linear-gradient(180deg,#f6fafd,#edf5fa);border:1px solid rgba(51,65,85,.11);border-radius:22px;box-shadow:0 10px 28px rgba(15,35,58,.08);color:#203047}
.panel .hero,.card>.head,.panel>.head{background:linear-gradient(135deg,#e7f3fb,#f3f8fc);border-bottom:1px solid rgba(51,65,85,.09)}
.main>.panel>.head,.main>.card>.head{padding:30px 32px;min-height:132px;display:flex;flex-direction:column;justify-content:center;background:radial-gradient(circle at 12% 12%,rgba(125,211,252,.28),transparent 34%),linear-gradient(135deg,#0f766e,#155e75 54%,#2563eb);border-bottom:0;color:#f8fafc}
.main>.panel>.head h3,.main>.card>.head h3{font-size:28px;line-height:1.1;color:#fff;margin-bottom:8px}
.main>.panel>.head p,.main>.card>.head p{max-width:760px;color:#e0f2fe;font-size:15px;line-height:1.5}
.main>.panel>.block,.main>.card>.block{padding:22px}
.main>.panel>.empty{margin:22px}
.main>.panel>.head+.empty{margin:0;padding:22px;background:linear-gradient(180deg,#f6fafd,#edf5fa);border:0;border-radius:0}
.main>.panel>.head+.empty .calendar-toolbar{margin-bottom:14px}
.main>.panel>.head+.empty #tmCalendar{box-shadow:0 16px 36px rgba(15,35,58,.12)}
.main>.panel>.block.stack,.main>.card>.block.stack{background:linear-gradient(180deg,#f6fafd,#edf5fa)}
.main>.panel>.block .field:first-child input,.main>.panel>.block .field:first-child select{max-width:560px}
.hero h2,.head h3,.provider-card strong,.zoom-card strong,.card h3{color:#172033}
.muted,.helper,.head p,.stat p{color:#5f7188}
.stat,.setup-provider,.document-strip div,.modal-field,.live-status-card{background:linear-gradient(180deg,#fbfdff,#f3f7fb);border-color:rgba(51,65,85,.10);box-shadow:0 6px 18px rgba(15,35,58,.045)}
.field input,.field select,.field textarea,input,select,textarea{background:#fbfdff;color:#203047;border-color:rgba(51,65,85,.15)}
.field input:focus,.field select:focus,.field textarea:focus,input:focus,select:focus,textarea:focus{outline:2px solid rgba(37,99,235,.18);border-color:#60a5fa}
.checkbox,.advanced-setup,.add-document-drawer{background:linear-gradient(180deg,#fbfdff,#f5f9fc);border-color:rgba(51,65,85,.11);color:#203047}
.checkbox.master{background:#eef8ff;border-color:#bfdbfe}
.btn{background:linear-gradient(135deg,#0f766e,#2563eb);box-shadow:0 8px 18px rgba(37,99,235,.16)}
.btn.soft,.btn.ghost{background:linear-gradient(180deg,#fbfdff,#eef5fb);border:1px solid rgba(51,65,85,.12);color:#1d4ed8;box-shadow:0 6px 14px rgba(15,35,58,.055)}
.btn.warn{background:linear-gradient(135deg,#f97316,#dc2626);color:#fff}
.btn,.btn.small,.btn.soft,.btn.ghost,.btn.warn,button.btn,a.btn{font-weight:900;text-shadow:none;line-height:1.15}
.btn:not(.soft):not(.ghost){color:#ffffff}
.btn.soft,.btn.ghost{color:#1746b5!important;background:linear-gradient(180deg,#ffffff,#eef5fb)!important;border-color:rgba(37,99,235,.18)!important}
.btn.warn{color:#ffffff!important}
.btn:disabled{opacity:.62;cursor:not-allowed}
.flash{background:#ecfeff;border-color:#a5f3fc;color:#155e75;box-shadow:0 10px 24px rgba(15,23,42,.06)}
.course-table th{background:#e8f2fb;color:#53657a;border-color:rgba(51,65,85,.08)}
.course-table td{border-color:rgba(51,65,85,.08);color:#203047}
.status-tag{background:#eef4fb;border-color:rgba(15,23,42,.10);color:#334155}
.status-tag.ok{background:#bbf7d0;border-color:#22c55e;color:#14532d;font-weight:900;box-shadow:inset 0 1px 0 rgba(255,255,255,.55)}.status-tag.due{background:#fef3c7;border-color:#fde68a;color:#92400e;font-weight:900}.status-tag.bad{background:#fee2e2;border-color:#fecaca;color:#991b1b;font-weight:900}.status-tag.neutral,.status-tag.later{background:#e0f2fe;border-color:#bae6fd;color:#075985;font-weight:900}
.recommend{background:#f0f9ff;border-color:#bae6fd;color:#172033}
.empty{background:#eef4fb;border:1px solid rgba(15,23,42,.08);border-radius:18px;color:#64748b}
.top-account summary,.top-account .dropdown,.alert-dropdown,.alert-item{background:#fbfdff;color:#203047;border-color:rgba(51,65,85,.10);box-shadow:0 16px 36px rgba(15,35,58,.12)}
.alert-title{color:#0f172a}.alert-message{color:#64748b}
.pill{background:rgba(255,255,255,.10);border-color:rgba(255,255,255,.16);color:#e0f2fe}
.certificate-panel{background:linear-gradient(180deg,#f4f9fd,#eef5fa);border-color:rgba(51,65,85,.10)}
.certificate-panel .hero h2,.certificate-panel .head h3{color:#0f172a}.certificate-panel .hero .muted,.certificate-panel .head p{color:#64748b}
.certificate-dashboard-panel .document-strip div{background:#fbfdff;border-color:rgba(51,65,85,.09)}
.certificate-dashboard-panel .document-strip strong{color:#0f172a}.certificate-dashboard-panel .document-strip span{color:#64748b}
.expiry-warning-row{background:#fff7ed;border-color:#fdba74}.expiry-warning-main strong{color:#7c2d12}.expiry-warning-main small{color:#92400e}.expiry-warning-row span{color:#9a3412}
.add-document-drawer summary{color:#0f172a}
.tm-progress-bubble{background:#fbfdff;color:#203047;border-color:rgba(51,65,85,.10);box-shadow:0 18px 45px rgba(15,35,58,.18)}
.tm-progress-title{color:#0f172a}.tm-progress-subtitle{color:#64748b}.tm-progress-body{background:#f8fbff;border-color:rgba(15,23,42,.08)}.tm-progress-step{color:#334155}
.tm-progress-bubble.idle{display:none}
.dashboard-home .tm-chip.neutral{background:#eef4fb;border-color:rgba(15,23,42,.10);color:#475569}
.tm-ready-panel .tm-chip.neutral{background:rgba(255,255,255,.16);border-color:rgba(255,255,255,.22);color:#fff}
.tm-ready-panel .tm-quick-actions select{background:rgba(255,255,255,.94);border-color:rgba(255,255,255,.35);color:#0f172a}
.tm-ready-panel .tm-quick-actions label{color:#dff7ff}
.dashboard-home{color:#203047}
.tm-next-panel,.tm-section,.tm-recommend-card{background:linear-gradient(180deg,#f4f9fd,#eef5fa);border-color:rgba(51,65,85,.10);box-shadow:0 16px 38px rgba(15,35,58,.09);color:#203047}
.dashboard-home .tm-dashboard-grid{grid-template-columns:1fr}.dashboard-home .tm-dashboard-grid>aside{display:none}.dashboard-home .tm-dashboard-grid>div>.tm-section:first-child{display:none}
.tm-upcoming-list summary{padding-bottom:14px;border-bottom:1px solid rgba(51,65,85,.08)}.tm-upcoming-list .tablewrap{margin-top:0!important;border-radius:0 0 18px 18px}.tm-upcoming-list .course-table th{font-size:11px}.tm-upcoming-list .course-table td{background:rgba(255,255,255,.30)}
.tm-chip.ok{background:#bbf7d0!important;border-color:#22c55e!important;color:#14532d!important;font-weight:900}
.tm-panel-title h3,.tm-recommend-card strong,.tm-provider-name,.tm-course-main strong,.tm-activity-row strong{color:#172033}
.tm-course-card,.tm-provider-tile,.tm-mini-day{background:linear-gradient(180deg,#fbfdff,#f3f7fb);border-color:rgba(51,65,85,.09);box-shadow:0 6px 18px rgba(15,35,58,.045)}
.tm-course-main span,.tm-activity-row span,.dashboard-home .helper,.dashboard-home .tm-panel-title p{color:#5f7188}
.tm-check-item{background:#fff8ed;border-color:#f7d79c}.tm-check-item strong{color:#203047}.tm-check-item p{color:#5f7188}
.empty{background:#e8f0f7;border-color:rgba(51,65,85,.10);color:#5f7188}

/* Softer grey-blue app canvas */
body{background:linear-gradient(180deg,#0b1728 0,#102237 106px,#d8e2ea 106px,#d8e2ea 100%);color:#203047}
.layout{background:linear-gradient(180deg,#d7e2ea,#dce6ed 52%,#d8e2ea);box-shadow:inset 0 1px 0 rgba(255,255,255,.42)}
.panel,.card,.provider-card,.zoom-card{background:linear-gradient(180deg,#eef4f8,#e5edf3);border-color:rgba(42,55,72,.14);box-shadow:0 10px 26px rgba(24,38,55,.075)}
.panel .hero,.card>.head,.panel>.head{background:linear-gradient(135deg,#e1ebf2,#edf3f7);border-bottom-color:rgba(42,55,72,.10)}
.main>.panel>.head,.main>.card>.head{background:radial-gradient(circle at 14% 14%,rgba(186,202,216,.32),transparent 34%),linear-gradient(135deg,#40586f,#2e6f86 55%,#315f9d);box-shadow:inset 0 1px 0 rgba(255,255,255,.14)}
.main>.panel>.head+.empty,.main>.panel>.block.stack,.main>.card>.block.stack{background:linear-gradient(180deg,#eef4f8,#e5edf3)}
.main>.panel>.head+.empty #tmCalendar{box-shadow:0 14px 30px rgba(24,38,55,.10)}
.tm-next-panel,.tm-section,.tm-recommend-card,.certificate-panel{background:linear-gradient(180deg,#eaf1f6,#dfe9f0);border-color:rgba(42,55,72,.13);box-shadow:0 14px 32px rgba(24,38,55,.075)}
.stat,.setup-provider,.document-strip div,.modal-field,.live-status-card,.tm-course-card,.tm-provider-tile,.tm-mini-day{background:linear-gradient(180deg,#f2f6f9,#e8eff4);border-color:rgba(42,55,72,.11);box-shadow:0 6px 16px rgba(24,38,55,.04)}
.field input,.field select,.field textarea,input,select,textarea,.top-account summary,.top-account .dropdown,.alert-dropdown,.alert-item{background:#f2f6f9;border-color:rgba(42,55,72,.15);color:#203047}
.checkbox,.advanced-setup,.add-document-drawer{background:linear-gradient(180deg,#f1f5f8,#e7eef3);border-color:rgba(42,55,72,.13)}
.btn.soft,.btn.ghost{background:linear-gradient(180deg,#f4f7fa,#e5edf4)!important;border-color:rgba(49,95,157,.20)!important}
.course-table th{background:#dbe6ee;color:#4f6072}
.course-table td{background:rgba(238,244,248,.56);border-color:rgba(42,55,72,.09)}
.tm-upcoming-list .course-table td{background:rgba(238,244,248,.64)}
.empty{background:#dfe8ef;border-color:rgba(42,55,72,.12);color:#596c7e}
.section-activity .main::after,.section-zoom_accounts .main::after{background:linear-gradient(135deg,rgba(225,235,242,.70),rgba(212,225,234,.92));border-color:rgba(42,55,72,.09);box-shadow:inset 0 1px 0 rgba(255,255,255,.52),0 12px 28px rgba(24,38,55,.045)}
.tm-ready-panel{background:radial-gradient(circle at 16% 12%,rgba(226,232,240,.24),transparent 30%),linear-gradient(135deg,#465f76 0%,#2c7891 50%,#315f9d 100%);border:0}
.tm-ready-panel.ready{background:radial-gradient(circle at 16% 12%,rgba(226,232,240,.25),transparent 30%),linear-gradient(135deg,#48667b 0%,#287f94 48%,#315f9d 100%);border:0}
.tm-ready-panel.attention{background:radial-gradient(circle at 16% 12%,rgba(255,247,237,.28),transparent 30%),linear-gradient(135deg,#9a6b2f 0%,#3b7d8b 52%,#315f9d 100%);border:0}
@keyframes tmPulse{0%{box-shadow:0 0 0 0 rgba(96,165,250,.55)}70%{box-shadow:0 0 0 9px rgba(96,165,250,0)}100%{box-shadow:0 0 0 0 rgba(96,165,250,0)}}
@media(max-width:1100px){.tm-hero-board,.tm-dashboard-grid{grid-template-columns:1fr}.tm-metric-row{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:720px){.modal-grid,.setup-provider-fields{grid-template-columns:1fr}.fc .fc-toolbar{display:grid;gap:8px}.fc .fc-toolbar-chunk{display:flex;justify-content:center;flex-wrap:wrap}}

</style>
</head>
<body class='section-{{ current_section }}'>
{% if current_section == 'dashboard' %}
<div class="startup-screen" id="tmStartupScreen" aria-live="polite" aria-modal="true" role="dialog">
  <div class="startup-card">
    <div class="startup-head">
      <div>
        <div class="startup-title" id="tmStartupTitle">Getting TrainerMate ready</div>
        <div class="startup-subtitle" id="tmStartupSubtitle">TrainerMate is getting today ready.</div>
      </div>
      <button class="startup-skip" id="tmStartupSkip" type="button">Skip</button>
    </div>
    <div class="startup-steps" id="tmStartupSteps">
      <div class="startup-step state-waiting"><span class="startup-step-icon">...</span><span class="startup-step-main"><span class="startup-step-label">Checking Zoom</span><span class="startup-step-detail">Waiting to start.</span></span><span class="startup-step-state">waiting</span></div>
      <div class="startup-step state-waiting"><span class="startup-step-icon">...</span><span class="startup-step-main"><span class="startup-step-label">Checking providers</span><span class="startup-step-detail">Waiting to start.</span></span><span class="startup-step-state">waiting</span></div>
      <div class="startup-step state-complete"><span class="startup-step-icon">OK</span><span class="startup-step-main"><span class="startup-step-label">Loading today's courses</span><span class="startup-step-detail">Preparing dashboard.</span></span><span class="startup-step-state">ready</span></div>
    </div>
    <div class="startup-foot">You can skip this. TrainerMate will keep checking for you.</div>
  </div>
</div>
{% endif %}
<div class='shell'>
  <div class='top'>
    <div class='brand'>
      <div class='mark'>TM</div>
      <div>
        <h1>TrainerMate</h1>
        <div class='muted'>Simple trainer navigation</div>
        <div class='muted' style='font-size:12px;margin-top:3px'>{{ build_label }}</div>
      </div>
    </div>
    <div class='top-right'>
      {% if dashboard_alerts %}
        <details class='alert-menu'>
          <summary><span class='alert-button'>!<span class='alert-count'>{{ dashboard_alerts|length }}</span></span></summary>
          <div class='alert-dropdown'>
            <div class='kicker'>Items to check</div>
            {% for alert in dashboard_alerts %}
              <div class='alert-item'>
                <div class='alert-title'>{{ alert.title }}</div>
                <div class='alert-message'>{{ alert.message }}</div>
                <div class='compact-actions'>
                  {% if alert.action == 'target_course' and alert.course_id %}
                    <form method='post' action='{{ url_for("check_course_only", course_id=alert.course_id) }}' onsubmit='this.closest(".alert-item").style.display="none"; const badge=document.querySelector(".alert-count"); if(badge){ const n=Math.max(0,(parseInt(badge.textContent||"1",10)||1)-1); badge.textContent=n; if(!n) badge.style.display="none"; }'>
                      <input type='hidden' name='course_key' value='{{ alert.course_key or "" }}'>
                      <input type='hidden' name='provider' value='{{ alert.provider or "" }}'>
                      <input type='hidden' name='title' value='{{ alert.title_text or "" }}'>
                      <input type='hidden' name='date_time' value='{{ alert.date_time_raw or "" }}'>
                      <button class='btn small' type='submit'>Check this course only</button>
                    </form>
                    <form method='post' action='{{ url_for("confirm_course_removed", course_id=alert.course_id) }}' onsubmit='return confirm("Delete this course from TrainerMate? Only do this if FOBS no longer shows it live.")'>
                      <button class='btn warn small' type='submit'>Delete</button>
                    </form>
                  {% elif alert.action == 'zoom_resolution' and alert.course_id %}
                    <form method='post' action='{{ url_for("replace_course_zoom", course_id=alert.course_id) }}' onsubmit='return confirm("Replace the FOBS Zoom link with the correct TrainerMate Zoom link for this course?")'>
                      <button class='btn small' type='submit'>Replace with correct Zoom link</button>
                    </form>
                    <form method='post' action='{{ url_for("keep_fobs_zoom", course_id=alert.course_id) }}'>
                      <button class='btn soft small' type='submit'>Keep FOBS link</button>
                    </form>
                    <a class='btn soft small' href='{{ url_for("open_fobs_course", course_id=alert.course_id) }}' target='_blank'>Open FOBS manually</a>
                  {% elif alert.action == 'sync' %}
                    <form method='post' action='{{ url_for("start_sync") }}'>
                      <input type='hidden' name='scan_provider' value='all'>
                      <input type='hidden' name='scan_days' value='{{ active_sync_window_days }}'>
                      <button class='btn small' type='submit'>Start sync</button>
                    </form>
                  {% endif %}
                  {% if alert.id %}
                    <form method='post' action='{{ url_for("dismiss_alert") }}' class='tm-instant-dismiss-form'>
                      {{ csrf_hidden_field()|safe }}
                      <input type='hidden' name='alert_id' value='{{ alert.id }}'>
                      <button class='btn soft small' type='submit'>Dismiss</button>
                    </form>
                  {% else %}
                    <button class='btn soft small' type='button' onclick='const d=this.closest("details"); if(d){d.open=false;}'>Close</button>
                  {% endif %}
                </div>
              </div>
            {% endfor %}
          </div>
        </details>
      {% endif %}
      <div class='pill'><span class='dot {{ status_dot_class }}'></span>{{ friendly_status }}</div>
      {% if last_sync_text %}<div class='pill'>Last sync {{ last_sync_text }}</div>{% endif %}
      <div class='top-account'>
        <details>
          <summary>{% if identity.ndors or identity.email %}Account - {{ account_plan_label }}{% else %}Set up account{% endif %}</summary>
          <div class='dropdown'>
            {% if identity.ndors or identity.email %}<div class='helper'>NDORS {{ identity.ndors or '-' }}{% if identity.email %} - {{ masked_email }}{% endif %}</div>{% endif %}
            <form method='post' action='{{ url_for("save_account") }}' class='stack'>
              <div class='field'><label>NDORS trainer ID</label><input name='ndors' placeholder='NDORS trainer ID' value='{{ identity.ndors }}'></div>
              <div class='field'><label>Email</label><input name='email' placeholder='Email address' value='{{ identity.email }}'></div>
              <div class='inline-actions'>
                <button class='btn small' type='submit'>Save account</button>
                <a class='btn soft small' href='{{ url_for("test_access") }}'>Check access</a>
              </div>
            </form>
            {% if identity.ndors %}
              {% if account_plan_label == 'Free' %}
                <div class='tm-account-note'><b>Free account active</b><br>You can keep using TrainerMate for setup, providers, Zoom and course viewing. Paid unlocks the 12-week sync window, Automatic Sync, Calendar, and certificate management.</div>
                <form method='post' action='{{ url_for("redeem") }}' style='display:grid;gap:10px;margin-top:10px'><input name='key' placeholder='Enter licence key'><button class='btn small' type='submit'>Activate paid licence</button></form>
              {% elif access and access.allowed %}
                <div class='helper'>Licence active - {{ account_plan_label }} - {{ active_sync_window_label }} sync window</div>
              {% elif access and not access.allowed %}
                <div class='tm-account-note'><b>Account needs attention</b><br>TrainerMate could not confirm full access. Your saved providers, Zoom connection and settings are still safe.</div>
                <form method='post' action='{{ url_for("redeem") }}' style='display:grid;gap:10px;margin-top:10px'><input name='key' placeholder='Enter licence key'><button class='btn small' type='submit'>Activate licence</button></form>
              {% endif %}
            {% endif %}
          </div>
        </details>
      </div>
    </div>
  </div>
  <div class='layout'>
    <div class='panel sidebar'>
      <a class='nav {% if current_section == "dashboard" %}active{% endif %}' href='{{ url_for("home", section="dashboard", provider=selected_provider if selected_provider != "all" else None) }}'><span class='nav-left'><span class='nav-icon nav-icon-dashboard'></span><span>Dashboard</span></span></a>
      {% if not providers or current_section == "setup" %}<a class='nav {% if current_section == "setup" %}active{% endif %}' href='{{ url_for("home", section="setup") }}'><span class='nav-left'><span class='nav-icon nav-icon-manage'></span><span>Setup</span></span></a>{% endif %}
      <details class='navgroup' {% if selected_provider != "all" %}open{% endif %}>
        <summary><span class='nav-left'><span class='nav-icon nav-icon-providers'></span><span>Providers</span></span><span class='badge'>{{ providers|length }}</span></summary>
        <div class='sublist'>
          <a class='sub {% if selected_provider == "all" %}active{% endif %}' href='{{ url_for("home", section="dashboard") }}'><span class='nav-left'><span class='nav-icon nav-icon-providers'></span><span>All providers</span></span><span class='badge'>{{ total_courses }}</span></a>
          {% for provider in providers %}
            <a class='sub {% if selected_provider == provider.id %}active{% endif %}' href='{{ url_for("home", section="dashboard", provider=provider.id) }}'><span class='nav-left'><span class='nav-icon nav-icon-providers'></span><span>{{ provider.name }}</span></span><span class='badge'>{{ provider.course_count }}</span></a>
          {% endfor %}
        </div>
      </details>
      <a class='nav {% if current_section == "manage_providers" %}active{% endif %}' href='{{ url_for("home", section="manage_providers") }}'><span class='nav-left'><span class='nav-icon nav-icon-manage'></span><span>Manage providers</span></span></a>
      <a class='nav {% if current_section == "zoom_accounts" %}active{% endif %}' href='{{ url_for("home", section="zoom_accounts") }}'><span class='nav-left'><span class='nav-icon nav-icon-zoom'></span><span>Zoom accounts</span></span></a>
      <a class='nav {% if current_section == "automation" %}active{% endif %}' href='{{ url_for("home", section="automation") }}'><span class='nav-left'><span class='nav-icon nav-icon-sync'></span><span>Automatic Sync</span></span></a>
      <a class='nav {% if current_section == "activity" %}active{% endif %}' href='{{ url_for("home", section="activity") }}'><span class='nav-left'><span class='nav-icon nav-icon-activity'></span><span>Messages & Activity{% if activity_counts.unread %}<span class='nav-alert'>{{ activity_counts.unread }}</span>{% endif %}</span></span></a>
      <a class='nav {% if current_section == "calendar" %}active{% endif %}' href='{{ url_for("home", section="calendar") }}'><span class='nav-left'><span class='nav-icon nav-icon-calendar'></span><span>Calendar</span></span></a>
      <a class='nav {% if current_section == "files" %}active{% endif %}' href='{{ url_for("home", section="files") }}'><span class='nav-left'><span class='nav-icon nav-icon-files'></span><span>Certificates{% if certificate_alerts_visible %}<span class='nav-alert'>!</span>{% endif %}</span></span></a>
    </div>
    <div class='main'>
      {% if flash %}<div class='flash tm-top-flash'>{{ flash.text }}</div>{% endif %}
      <div class='tm-modal-backdrop' id='tmInfoModal' {% if not flash and not show_locked_section_modal %}hidden{% endif %}>
        <div class='tm-modal-card' role='dialog' aria-modal='true' aria-labelledby='tmInfoModalTitle'>
          <button class='tm-modal-close-x' type='button' id='tmInfoModalX' data-tm-modal-close aria-label='Dismiss'>x</button>
          <h3 id='tmInfoModalTitle'>{% if show_locked_section_modal %}{{ locked_section_title }}{% elif flash and flash.category == 'success' %}Done{% elif flash and flash.category == 'warning' %}Please note{% else %}TrainerMate{% endif %}</h3>
          <p id='tmInfoModalMessage'>{% if show_locked_section_modal %}{{ locked_section_message }}{% elif flash %}{{ flash.text }}{% endif %}</p>
          <div class='tm-modal-sub'>Your current settings, providers, Zoom connection and course history are safe.</div>
          <div class='tm-modal-actions'><button class='btn' type='button' id='tmInfoModalClose' data-tm-modal-close>Dismiss</button></div>
        </div>
      </div>

      {% if current_section == 'setup' %}
        <div class='panel'>
          <div class='hero'>
            <div>
              <div class='kicker'>First setup</div>
              <h2>Welcome to TrainerMate</h2>
              <div class='muted'>Choose the providers you work with and save the FOBS login details for each one.</div>
            </div>
            <div class='setup-checklist'>
              <div><b>{% if zoom_accounts %}OK{% else %}1{% endif %}</b> Zoom account{% if zoom_accounts %} connected{% else %} not connected yet{% endif %}</div>
              <div><b>{% if providers %}OK{% else %}2{% endif %}</b> Providers{% if providers %} selected{% else %} needed{% endif %}</div>
              <div><b>3</b> Test provider logins</div>
            </div>
          </div>
        </div>
        <div class='panel'>
          <div class='head'><h3>Which providers do you work with?</h3><p>TrainerMate will keep the technical FOBS addresses in the background where it can.</p></div>
          <div class='block'>
            <form method='post' action='{{ url_for("setup_providers") }}' class='stack'>
              <div class='setup-list'>
                {% for item in setup_provider_rows %}
                  <div class='setup-provider {% if not item.ready %}disabled{% endif %}'>
                    <div class='setup-provider-main'>
                      <label class='setup-provider-title'>
                        <input type='checkbox' name='setup_provider' value='{{ item.id }}' {% if item.configured %}checked{% endif %} {% if not item.ready %}disabled{% endif %}>
                        <span>{{ item.name }}</span>
                      </label>
                      <span class='status-tag {% if item.configured %}ok{% elif item.ready %}neutral{% else %}due{% endif %}'>{% if item.configured %}Selected{% elif item.ready %}Ready{% else %}Coming soon{% endif %}</span>
                    </div>
                    {% if item.ready %}
                      <div class='setup-provider-fields'>
                        <div class='field'><label>Username</label><input name='username_{{ item.id }}' value='{{ item.username }}' placeholder='FOBS username'></div>
                        <div class='field'><label>Password</label><input type='password' name='password_{{ item.id }}' placeholder='{% if item.has_password %}Saved - leave blank to keep{% else %}FOBS password{% endif %}'></div>
                      </div>
                      {% if item.read_only %}<div class='helper'>Read-only provider. TrainerMate can check courses but will not write Zoom details here.</div>{% endif %}
                      {% if item.last_login_test_message %}<div class='helper'>{{ item.last_login_test_message }}</div>{% endif %}
                    {% else %}
                      <div class='helper'>This provider is in the catalogue, but TrainerMate still needs the confirmed FOBS setup details before it can be selected here.</div>
                    {% endif %}
                  </div>
                {% endfor %}
              </div>
              <div class='inline-actions'>
                <button class='btn' type='submit'>Save providers</button>
                <a class='btn soft' href='{{ url_for("home", section="zoom_accounts") }}'>Connect Zoom</a>
                <a class='btn soft' href='{{ url_for("home", section="manage_providers") }}'>Advanced provider settings</a>
              </div>
            </form>
          </div>
        </div>
      {% elif current_section == 'dashboard' %}
        <div class='dashboard-home'>
          <div class='tm-hero-board'>
            <section class='tm-ready-panel {{ dashboard_ready.tone }}'>
              <div class='tm-ready-top'>
                <span class='tm-ready-label'><span class='tm-ready-dot'></span>{{ dashboard_ready.label }}</span>
                <span class='tm-chip neutral'>{{ selected_provider_name }}</span>
              </div>
              <div>
                <h2>{{ dashboard_ready.title }}</h2>
                <p>{{ dashboard_ready.message }}</p>
              </div>
              <div class='tm-quick-actions'>
                {% if state.sync_running %}
                  <form method='post' action='{{ url_for("stop_sync", provider=selected_provider if selected_provider != "all" else None) }}'><button class='btn warn' type='submit'>Stop sync</button></form>
                {% else %}
                  <form method='post' action='{{ url_for("start_sync", provider=selected_provider if selected_provider != "all" else None) }}' class='scan-form'>
                    <div class='scan-field'>
                      <label>Scan window</label>
                      <select name='scan_days' class='{% if account_plan_label == "Free" %}tm-disabled-select{% endif %}'>
                        {% if account_plan_label == 'Free' %}
                          <option value='{{ active_sync_window_days }}' selected>Free window - {{ active_sync_window_label }}</option>
                          <option disabled>Paid unlocks the 12-week window</option>
                        {% else %}
                          <option value='7' selected>Next 7 days</option>
                          <option value='14'>Next 14 days</option>
                          <option value='30'>Next 30 days</option>
                          <option value='60'>Next 60 days</option>
                          <option value='{{ active_sync_window_days }}'>Account window ({{ active_sync_window_label }})</option>
                        {% endif %}
                      </select>
                    </div>
                    <div class='scan-field'>
                      <label>Provider</label>
                      <select name='scan_provider'>
                        <option value='all' {% if selected_provider == "all" %}selected{% endif %}>All providers</option>
                        {% for provider in providers %}
                          <option value='{{ provider.id }}' {% if selected_provider == provider.id %}selected{% endif %}>{{ provider.name }}</option>
                        {% endfor %}
                      </select>
                    </div>
                    <button class='btn' type='submit'>Sync quietly</button>
                  </form>
                {% endif %}
                <a class='btn soft' href='{{ url_for("home", section="calendar") }}'>Calendar</a>
                <a class='btn soft' href='{{ url_for("home", section="manage_providers") }}'>Providers</a>
              </div>
              <div class='tm-metric-row'>
                <div class='tm-metric'><strong>{{ today_courses|length }}</strong><span>Today</span></div>
                <div class='tm-metric'><strong>{{ filtered_courses|length }}</strong><span>Next {{ active_sync_window_label }}</span></div>
                <div class='tm-metric'><strong>{{ synced_count }}</strong><span>Synced</span></div>
                <div class='tm-metric'><strong>{{ attention_count }}</strong><span>Needs attention</span></div>
              </div>
            </section>
            <aside class='tm-next-panel'>
              <div class='tm-panel-title'>
                <div><h3>Next courses</h3><p>The diary items TrainerMate knows about.</p></div>
                <a href='{{ url_for("home", section="calendar") }}'>Open calendar</a>
              </div>
              <div class='tm-timeline'>
                {% for row in next_courses %}
                  <div class='tm-course-card'>
                    <div class='tm-course-time'>{{ row.time_label }}</div>
                    <div class='tm-course-main'>
                      <strong>{{ row.title }}</strong>
                      <span><i class='tm-provider-dot' style='background:{{ row.provider_color }}'></i>{{ row.provider }} - {{ row.date_label }}</span>
                    </div>
                    <span class='status-tag {{ row.status_class }}'>{{ row.status_label }}</span>
                  </div>
                {% else %}
                  <div class='empty'>No upcoming courses to show yet.</div>
                {% endfor %}
              </div>
            </aside>
          </div>

          <section class='tm-recommend-card'>
            <div>
              <div class='kicker'>Recommended next action</div>
              <strong>{{ recommendation.title }}</strong>
              <div class='helper'>{{ recommendation.reason }}</div>
            </div>
            <div class='compact-actions'>
              {% if recommendation.action == 'target_course' and recommendation.course_id and not state.sync_running %}
                <form method='post' action='{{ url_for("check_course_only", course_id=recommendation.course_id) }}'>
                  <input type='hidden' name='course_key' value='{{ recommendation.course_key or "" }}'>
                  <input type='hidden' name='provider' value='{{ recommendation.provider_name or "" }}'>
                  <input type='hidden' name='title' value='{{ recommendation.title_text or "" }}'>
                  <input type='hidden' name='date_time' value='{{ recommendation.date_time_raw or "" }}'>
                  <button class='btn small' type='submit'>Check course only</button>
                </form>
              {% elif recommendation.action == 'zoom_resolution' and recommendation.course_id and not state.sync_running %}
                <form method='post' action='{{ url_for("replace_course_zoom", course_id=recommendation.course_id) }}'><button class='btn small' type='submit'>Replace Zoom link</button></form>
                <form method='post' action='{{ url_for("keep_fobs_zoom", course_id=recommendation.course_id) }}'><button class='btn soft small' type='submit'>Keep FOBS link</button></form>
              {% elif recommendation.action == 'sync' and not state.sync_running %}
                <form method='post' action='{{ url_for("start_sync", provider=recommendation.provider if recommendation.provider != "all" else None) }}'>
                  <input type='hidden' name='scan_provider' value='{{ recommendation.provider }}'>
                  <input type='hidden' name='scan_days' value='{{ recommendation.days }}'>
                  <button class='btn small' type='submit'>Sync quietly</button>
                </form>
              {% elif recommendation.action == 'providers' %}
                <a class='btn small' href='{{ url_for("home", section="manage_providers") }}'>Open providers</a>
              {% elif recommendation.action == 'zoom' %}
                <a class='btn small' href='{{ url_for("home", section="zoom_accounts") }}'>Open Zoom</a>
              {% endif %}
              <a class='btn soft small' href='{{ url_for("home", section="dashboard", provider=selected_provider if selected_provider != "all" else None) }}'>Refresh</a>
            </div>
          </section>

          <div class='tm-dashboard-grid'>
            <div>
              <section class='tm-section'>
                <div class='tm-panel-title'><div><h3>Provider health</h3><p>Clear status for each configured provider.</p></div><a href='{{ url_for("home", section="manage_providers") }}'>Manage</a></div>
                <div class='tm-provider-health'>
                  {% for provider in provider_health %}
                    <div class='tm-provider-tile'>
                      <div class='tm-provider-name'><span><i class='tm-provider-dot' style='background:{{ provider.color }}'></i>{{ provider.name }}</span><span class='tm-chip {{ provider.health_tone }}'>{{ provider.health_label }}</span></div>
                      <div class='helper'>{{ provider.course_count }} course(s) in view{% if provider.provider_manages_zoom %} - Zoom read-only{% endif %}</div>
                    </div>
                  {% else %}
                    <div class='empty'>Add your first provider to get started.</div>
                  {% endfor %}
                </div>
              </section>
              <section class='tm-section'>
                <details class='tm-table-disclosure tm-upcoming-list' open>
                  <summary><span><strong>Upcoming courses</strong><br><span class='helper'>The older practical course list, shown openly for quick checking.</span></span><span class='tm-chip neutral'>{{ filtered_courses|length }}</span></summary>
                  {% if filtered_courses %}
                    <div class='tablewrap' style='margin-top:14px'>
                      <table class='course-table'>
                        <thead><tr><th>Provider</th><th>Course</th><th>Date</th><th>Time</th><th>Sync status</th><th>Notes</th></tr></thead>
                        <tbody>
                          {% for row in filtered_courses %}
                            <tr>
                              <td>{{ row.provider }}</td>
                              <td class='title-cell'>{{ row.title }}</td>
                              <td>{{ row.date_label }}</td>
                              <td>{{ row.time_label }}</td>
                              <td><span class='status-tag {{ row.status_class }}'>{{ row.status_label }}</span></td>
                              <td>{{ row.short_message }}{% if row.show_upgrade %}<a class='upgrade-link' href='https://www.trainermate.xyz/upgrade' target='_blank' rel='noopener'>Upgrade</a>{% endif %}{% if reviewer_demo_mode %}<form method='post' action='{{ url_for("reviewer_create_or_verify_zoom", course_id=row.id) }}' style='display:inline;margin-left:10px'><button class='btn small' type='submit'>{% if row.meeting_id %}Verify Zoom meeting{% else %}Create Zoom meeting{% endif %}</button></form><form method='post' action='{{ url_for("replace_course_zoom", course_id=row.id) }}' style='display:inline;margin-left:8px'><button class='btn soft small' type='submit'>Replace/update Zoom</button></form>{% endif %}{% if row.is_action_needed and not state.sync_running and not reviewer_demo_mode %}<form method='post' action='{{ url_for("check_course_only", course_id=row.id) }}' style='display:inline;margin-left:10px'><input type='hidden' name='course_key' value='{{ course_action_exact_key(row) }}'><input type='hidden' name='provider' value='{{ row.provider or "" }}'><input type='hidden' name='title' value='{{ row.title or "" }}'><input type='hidden' name='date_time' value='{{ row.date_time_raw or "" }}'><button class='btn small' type='submit'>Check course only</button></form>{% endif %}{% if row.can_confirm_removed %}<form method='post' action='{{ url_for("confirm_course_removed", course_id=row.id) }}' style='display:inline;margin-left:10px' onsubmit='return confirm("Only confirm if this course has genuinely been deleted/cancelled in FOBS. Remove it from TrainerMate?")'><button class='btn warn small' type='submit'>Confirm removed</button></form>{% endif %}</td>
                            </tr>
                          {% endfor %}
                        </tbody>
                      </table>
                    </div>
                  {% else %}<div class='empty' style='margin-top:14px'>No courses to show for this view yet.</div>{% endif %}
                </details>
              </section>
            </div>
            <aside>
              <section class='tm-section'>
                <div class='tm-panel-title'><div><h3>Items to check</h3><p>Only things that may need a trainer decision.</p></div></div>
                <div class='tm-items-list'>
                  {% for alert in dashboard_alerts[:4] %}
                    <div class='tm-check-item'><strong>{{ alert.title }}</strong><p>{{ alert.message }}</p></div>
                  {% else %}
                    <div class='empty'>No trainer action needed right now.</div>
                  {% endfor %}
                </div>
              </section>
              <section class='tm-section'>
                <div class='tm-panel-title'><div><h3>Week glance</h3><p>A light view of the next few days.</p></div><a href='{{ url_for("home", section="calendar") }}'>Full calendar</a></div>
                <div class='tm-mini-calendar'>
                  {% for day in mini_calendar_days %}
                    <div class='tm-mini-day'>
                      <b>{{ day.label }}</b>
                      {% for event in day.events[:3] %}<span class='tm-mini-event' title='{{ event.title }}' style='background:{{ event.provider_color }}'></span>{% endfor %}
                    </div>
                  {% endfor %}
                </div>
              </section>
              <section class='tm-section'>
                <div class='tm-panel-title'><div><h3>Recent activity</h3><p>Quiet updates from TrainerMate.</p></div><a href='{{ url_for("home", section="activity") }}'>View all</a></div>
                <div class='tm-activity-list'>
                  {% for item in dashboard_activity_items %}
                    <div class='tm-activity-row'><strong>{{ item.title }}</strong><span>{{ item.summary or item.message }}</span></div>
                  {% else %}
                    <div class='empty'>Activity will appear here after syncs or support messages.</div>
                  {% endfor %}
                </div>
              </section>
            </aside>
          </div>
        </div>
        {% if false %}
        <div class='panel'>
          <div class='hero'>
            <div>
              <div class='kicker'>Overview</div>
              <h2>{{ selected_provider_name }}</h2>
              <div class='muted'>{{ status_message }}</div>
            </div>
            <div class='hero-actions'>
              {% if state.sync_running %}
                <form method='post' action='{{ url_for("stop_sync", provider=selected_provider if selected_provider != "all" else None) }}'><button class='btn warn' type='submit'>Stop sync</button></form>
              {% else %}
                <form method='post' action='{{ url_for("start_sync", provider=selected_provider if selected_provider != "all" else None) }}' class='scan-form'>
                  <div class='scan-field'>
                    <label>Scan</label>
                    <select name='scan_days' class='{% if account_plan_label == "Free" %}tm-disabled-select{% endif %}'>
                      {% if account_plan_label == 'Free' %}
                        <option value='{{ active_sync_window_days }}' selected>Free window - {{ active_sync_window_label }}</option>
                        <option disabled>Paid unlocks the 12-week window</option>
                      {% else %}
                        <option value='7' selected>Next 7 days</option>
                        <option value='14'>Next 14 days</option>
                        <option value='30'>Next 30 days</option>
                        <option value='60'>Next 60 days</option>
                        <option value='{{ active_sync_window_days }}'>Account window ({{ active_sync_window_label }})</option>
                      {% endif %}
                    </select>
                    {% if account_plan_label == 'Free' %}<div class='tm-small-upgrade'>Free syncs are limited to the 3-week window. Paid unlocks the 12-week window.</div>{% endif %}
                  </div>
                  <div class='scan-field'>
                    <label>Provider</label>
                    <select name='scan_provider'>
                      <option value='all' {% if selected_provider == "all" %}selected{% endif %}>All providers</option>
                      {% for provider in providers %}
                        <option value='{{ provider.id }}' {% if selected_provider == provider.id %}selected{% endif %}>{{ provider.name }}</option>
                      {% endfor %}
                    </select>
                  </div>
                  <button class='btn' type='submit'>Start sync</button>
                </form>
              {% endif %}
              <a class='btn soft' href='{{ url_for("home", section="manage_providers") }}'>Manage providers</a>
            </div>
          </div>
          <div class='stats'>
            <div class='stat'><h3>{{ filtered_courses|length }}</h3><p>Courses shown</p></div>
            <div class='stat'><h3>{{ synced_count }}</h3><p>Synced</p></div>
            <div class='stat'><h3>{{ attention_count }}</h3><p>Need action</p></div>
          </div>
          <div class='recommend'>
            <div>
              <div class='kicker'>Recommended next action</div>
              <strong>{{ recommendation.title }}</strong>
              <div class='helper'>{{ recommendation.reason }}</div>
            </div>
            <div class='compact-actions'>
              {% if recommendation.action == 'target_course' and recommendation.course_id and not state.sync_running %}
                <form method='post' action='{{ url_for("check_course_only", course_id=recommendation.course_id) }}'>
                  <input type='hidden' name='course_key' value='{{ recommendation.course_key or "" }}'>
                  <input type='hidden' name='provider' value='{{ recommendation.provider_name or "" }}'>
                  <input type='hidden' name='title' value='{{ recommendation.title_text or "" }}'>
                  <input type='hidden' name='date_time' value='{{ recommendation.date_time_raw or "" }}'>
                  <button class='btn small' type='submit'>Check course only</button>
                </form>
              {% elif recommendation.action == 'sync' and not state.sync_running %}
                <form method='post' action='{{ url_for("start_sync", provider=recommendation.provider if recommendation.provider != "all" else None) }}'>
                  <input type='hidden' name='scan_provider' value='{{ recommendation.provider }}'>
                  <input type='hidden' name='scan_days' value='{{ recommendation.days }}'>
                  <button class='btn small' type='submit'>Start</button>
                </form>
              {% elif recommendation.action == 'providers' %}
                <a class='btn small' href='{{ url_for("home", section="manage_providers") }}'>Open settings</a>
              {% elif recommendation.action == 'zoom' %}
                <a class='btn small' href='{{ url_for("home", section="zoom_accounts") }}'>Open Zoom</a>
              {% endif %}
              <a class='btn soft small' href='{{ url_for("home", section="dashboard", provider=selected_provider if selected_provider != "all" else None) }}'>Refresh</a>
            </div>
          </div>
        </div>
        <div class='panel'>
          <div class='head'><h3>Upcoming courses</h3><p>All known future allocations</p></div>
          {% if filtered_courses %}
            <div class='tablewrap'>
              <table class='course-table'>
                <thead><tr><th>Provider</th><th>Course</th><th>Date</th><th>Time</th><th>Sync status</th><th>Notes</th></tr></thead>
                <tbody>
                  {% for row in filtered_courses %}
                    <tr>
                      <td>{{ row.provider }}</td>
                      <td class='title-cell'>{{ row.title }}</td>
                      <td>{{ row.date_label }}</td>
                      <td>{{ row.time_label }}</td>
                      <td><span class='status-tag {{ row.status_class }}'>{{ row.status_label }}</span></td>
                      <td>{{ row.short_message }}{% if row.show_upgrade %}<a class='upgrade-link' href='https://www.trainermate.xyz/upgrade' target='_blank' rel='noopener'>Upgrade</a>{% endif %}{% if row.is_action_needed and not state.sync_running %}<form method='post' action='{{ url_for("check_course_only", course_id=row.id) }}' style='display:inline;margin-left:10px'><input type='hidden' name='course_key' value='{{ course_action_exact_key(row) }}'><input type='hidden' name='provider' value='{{ row.provider or "" }}'><input type='hidden' name='title' value='{{ row.title or "" }}'><input type='hidden' name='date_time' value='{{ row.date_time_raw or "" }}'><button class='btn small' type='submit'>Check course only</button></form>{% endif %}{% if row.can_confirm_removed %}<form method='post' action='{{ url_for("confirm_course_removed", course_id=row.id) }}' style='display:inline;margin-left:10px' onsubmit='return confirm("Only confirm if this course has genuinely been deleted/cancelled in FOBS. Remove it from TrainerMate?")'><button class='btn warn small' type='submit'>Confirm removed</button></form>{% endif %}</td>
                    </tr>
                  {% endfor %}
                </tbody>
              </table>
            </div>
          {% else %}<div class='empty'>No courses to show for this view yet.</div>{% endif %}
        </div>
        {% endif %}
      {% elif current_section == 'manage_providers' %}
        <div class='panel'>
          <div class='head'><h3>Manage providers</h3><p>Add providers, manage FOBS logins, choose linked Zoom accounts, and delete providers.</p></div>
          <div class='block'>
            <form method='post' action='{{ url_for("add_provider") }}' class='stack'>
              <div class='grid-two'>
                <div class='field'>
                  <label>Provider</label>
                  <select name='provider_preset' id='provider_preset' required onchange="(function(s){var o=s.options[s.selectedIndex];var manual=s.value==='manual';var n=document.getElementById('provider_name');var u=document.getElementById('login_url');document.querySelectorAll('.manual-provider-field').forEach(function(f){f.style.display=manual?'grid':'none';});if(n){n.value=manual?'':(o.getAttribute('data-provider-name')||'');n.readOnly=!manual;n.required=manual;}if(u){u.value=manual?'':(o.getAttribute('data-login-url')||'');u.readOnly=!manual&&!!u.value;u.required=manual;}})(this)">
                    <option value='' selected disabled>Click here to select provider</option>
                    {% for option in provider_catalogue_options %}
                      <option value='{{ option.id }}' data-provider-name='{{ option.name }}' data-login-url='{{ option.login_url }}' data-ready='{% if option.ready %}1{% else %}0{% endif %}'>{{ option.name }}{% if option.status == "needs_public_confirmation" %} - FOBS details needed{% endif %}</option>
                    {% endfor %}
                  </select>
                </div>
                <div class='field manual-provider-field'><label>Provider name</label><input name='provider_name' id='provider_name' value='{{ provider_form.name }}' placeholder='Provider name'></div>
              </div>
              <div class='grid-two'>
                <div class='field manual-provider-field'><label>FOBS login URL</label><input name='login_url' id='login_url' value='{{ provider_form.login_url }}' placeholder='https://.../Account/Login'></div>
                <div class='field'><label>Linked Zoom account</label><select name='zoom_account_id'>{% for account in zoom_accounts %}<option value='{{ account.id }}' {% if provider_form.zoom_account_id == account.id %}selected{% endif %}>{{ account.nickname }}{% if account.email %} - {{ account.email }}{% endif %}</option>{% endfor %}{% if not zoom_accounts %}<option value=''>No Zoom accounts connected yet</option>{% endif %}</select></div>
              </div>
              <div class='grid-two'>
                <div class='field'><label>Calendar colour</label><input type='color' name='provider_color' id='provider_color' value='{{ provider_form.color }}'></div>
              </div>
              <div class='grid-two'>
                <div class='field'><label>FOBS username</label><input name='provider_username' placeholder='FOBS username' required></div>
                <div class='field'><label>FOBS password</label><input type='password' name='provider_password' placeholder='FOBS password' required></div>
              </div>
              <div class='stack'>
                <label class='checkbox'><input type='checkbox' name='active' value='1' {% if provider_form.active %}checked{% endif %}> <span>Active for sync</span></label>
                <label class='checkbox'><input type='checkbox' name='supports_custom_time' value='1' {% if provider_form.supports_custom_time %}checked{% endif %}> <span>Allow custom times in TrainerMate</span></label>
                <label class='checkbox'><input type='checkbox' class='provider-managed-toggle' name='provider_manages_zoom' value='1' {% if provider_form.provider_manages_zoom %}checked{% endif %}> <span>Provider enters Zoom for me</span></label>
              </div>
              <div class='inline-actions'>
                <button class='btn' type='submit'>Add provider</button>
                <a class='btn soft' href='{{ url_for("home", section="zoom_accounts") }}'>Manage Zoom accounts</a>
              </div>
            </form>
          </div>
        </div>
        {% if providers %}
          {% for provider in providers %}
            <div class='provider-card'>
              <div class='provider-header'>
                <div>
                  <strong>{{ provider.name }}</strong>
                  <div class='helper'>
                    {% if provider.last_login_test_status == 'ok' %}
                      Login checked
                    {% elif provider.last_login_test_status == 'failed' %}
                      Login needs checking - sync paused
                    {% elif provider.credentials.username and provider.credentials.password %}
                      Login details saved
                    {% else %}
                      Add FOBS login details
                    {% endif %}
                    {% if provider.read_only %} - Read-only{% endif %}
                  </div>
                </div>
                <span class='status-tag {% if provider.last_login_test_status == "ok" %}ok{% elif provider.last_login_test_status == "failed" or provider.paused_for_login %}bad{% else %}neutral{% endif %}'>{% if provider.last_login_test_status == 'ok' %}Login OK{% elif provider.last_login_test_status == 'failed' or provider.paused_for_login %}Sync paused{% elif provider.active %}Active{% else %}Inactive{% endif %}</span>
              </div>
              <form method='post' action='{{ url_for("update_provider", provider_id=provider.id) }}' class='stack'>
                <div class='grid-two'>
                  <input type='hidden' name='login_url' value='{{ provider.login_url }}'>
                  <div class='field'><label>Linked Zoom account</label><select name='zoom_account_id'>{% for account in zoom_accounts %}<option value='{{ account.id }}' {% if provider.zoom_account_id == account.id %}selected{% endif %}>{{ account.nickname }}{% if account.email %} - {{ account.email }}{% endif %}</option>{% endfor %}{% if not zoom_accounts %}<option value=''>No Zoom accounts connected yet</option>{% endif %}</select></div>
                </div>
                <div class='grid-two'>
                  <div class='field'><label>Calendar colour</label><input type='color' name='provider_color' value='{{ provider.color }}'></div>
                </div>
                <div class='grid-two'>
                  <div class='field'><label>FOBS username</label><input name='provider_username' value='{{ provider.credentials.username }}'></div>
                  <div class='field'><label>FOBS password</label><input type='password' name='provider_password' placeholder='{% if provider.credentials.password %}Saved - leave blank to keep{% else %}FOBS password{% endif %}'></div>
                </div>
                <div class='stack'>
                  <label class='checkbox'><input type='checkbox' name='active' value='1' {% if provider.active %}checked{% endif %}> <span>Active for sync</span></label>
                  <label class='checkbox'><input type='checkbox' name='supports_custom_time' value='1' {% if provider.supports_custom_time %}checked{% endif %}> <span>Allow custom times in TrainerMate</span></label>
                  <label class='checkbox'><input type='checkbox' class='provider-managed-toggle' name='provider_manages_zoom' value='1' {% if provider.provider_manages_zoom %}checked{% endif %}> <span>Provider enters Zoom for me</span></label>
                </div>
                {% if provider.last_login_test_message %}
                  <div class='helper'>{{ provider.last_login_test_message }}</div>
                {% endif %}
                <div class='inline-actions'>
                  <button class='btn small' type='submit'>Save provider</button>
                  <button class='btn soft small' type='submit' formaction='{{ url_for("test_provider_login", provider_id=provider.id) }}' formmethod='post'>Test login</button>
                  <button class='btn warn small' type='submit' formaction='{{ url_for("delete_provider", provider_id=provider.id) }}' formmethod='post' onclick='return confirm("Delete {{ provider.name }}? No sync will take place for this provider until it is added again.")'>Delete provider</button>
                </div>
              </form>
            </div>
          {% endfor %}
        {% else %}<div class='panel'><div class='empty'>No providers added yet.</div></div>{% endif %}
      {% elif current_section == 'zoom_accounts' %}
        <div class='panel'>
          <div class='head'><h3>Zoom accounts</h3><p>Connect the Zoom account TrainerMate should use for course meetings. TrainerMate never asks for your Zoom password.</p></div>
          <div class='block stack'>
            {% if ZOOM_CLIENT_ID and ZOOM_CLIENT_SECRET %}
              <div class='stack' style='max-width:460px'>
                <div class='inline-actions'>
                  <a class='btn' href='{{ url_for("zoom_connect_start") }}?zoom_nickname=TrainerMate'>Connect Zoom account</a>
                </div>
                <div class='helper'>You will be sent to Zoom to approve TrainerMate. After approval you will return here.</div>
              </div>
            {% else %}
              <div class='provider-card'>
                <strong>Zoom connection is not ready yet</strong>
                <div class='helper'>TrainerMate's Zoom app is awaiting Zoom approval. Once approved, this page will show a simple Connect Zoom account button. For now, existing connected accounts can still be used.</div>
                <details class='advanced-setup'>
                  <summary>Advanced setup for testing</summary>
                  <div class='helper'>Only use this if you are setting up the TrainerMate Zoom app. Normal users should not need these details.</div>
                  <form method='post' action='{{ url_for("zoom_oauth_config_save") }}' style='display:grid;gap:12px;margin-top:12px;max-width:560px'>
                    {{ csrf_hidden_field()|safe }}
                    <div class='field'><label>Zoom Client ID</label><input name='client_id' autocomplete='off' required></div>
                    <div class='field'><label>Zoom Client Secret</label><input name='client_secret' autocomplete='off' required></div>
                    <div class='field'><label>Redirect URI</label><input name='redirect_uri' value='{{ ZOOM_REDIRECT_URI }}' autocomplete='off'></div>
                    <div class='helper'>This must match the redirect URI in the Zoom app configuration.</div>
                    <div class='inline-actions'><button class='btn soft' type='submit'>Save advanced Zoom setup</button></div>
                  </form>
                </details>
              </div>
            {% endif %}
          </div>
        </div>
        {% if zoom_accounts %}
          {% for account in zoom_accounts %}
            <div class='zoom-card'>
              <div class='zoom-header'>
                <div>
                  <strong>{{ account.nickname }}</strong>
                  <div class='helper'>{{ account.email or 'Connected account' }}</div>
                </div>
                <span class='status-tag'>{% if account.is_default %}Default{% else %}Connected{% endif %}</span>
              </div>
              <div class='inline-actions'>
                {% if not account.is_default %}<form method='post' action='{{ url_for("zoom_set_default", account_id=account.id) }}'>{{ csrf_hidden_field()|safe }}<button class='btn soft small' type='submit'>Make default</button></form>{% endif %}
                <form method='post' action='{{ url_for("zoom_disconnect", account_id=account.id) }}' onsubmit='return confirm("Disconnect this Zoom account?")'>{{ csrf_hidden_field()|safe }}<button class='btn warn small' type='submit'>Disconnect</button></form>
              </div>
            </div>
          {% endfor %}
        {% else %}<div class='panel'><div class='empty'>No Zoom accounts connected yet.</div></div>{% endif %}
      {% elif current_section == 'automation' %}
        <div class='tm-lock-wrap {% if not automatic_sync_allowed %}is-locked{% endif %}'>
          <div class='tm-lock-content'>
            <section class='card'><div class='head'><h3>Automatic Sync</h3><p>Let TrainerMate quietly check upcoming courses in the background. You can change or turn this off at any time.</p></div><div class='block stack'>
              {% if not automatic_sync_allowed %}<div class='tm-paid-note'><strong>Preview of a paid feature</strong><br>These settings show what Automatic Sync can do. Scheduled background syncing starts only when your paid plan is active.</div>{% endif %}
              <form method='post' action='{{ url_for("save_automation_route") }}' class='stack'>
            <label class='checkbox master'><input type='checkbox' name='enabled' value='1' {% if automation_settings.enabled and automatic_sync_allowed %}checked{% endif %}><span>Enable Automatic Sync</span></label>
            {% if not automatic_sync_allowed %}<label class='checkbox'><input type='checkbox' name='enable_when_paid' value='1' {% if automation_settings.enable_when_paid %}checked{% endif %}><span>Turn this on automatically when my paid plan is active</span></label>{% endif %}
            <div class='grid-two'><label class='checkbox'><input type='checkbox' name='daily_enabled' value='1' {% if automation_settings.daily_enabled %}checked{% endif %}><span>Daily light check</span></label><div class='field'><label>Daily time</label><input type='time' name='daily_time' value='{{ automation_settings.daily_time }}'></div></div>
            <div class='field'><label>Daily scope</label><select name='daily_days'><option value='7' {% if automation_settings.daily_days == 7 %}selected{% endif %}>Next 7 days</option><option value='14' {% if automation_settings.daily_days == 14 %}selected{% endif %}>Next 14 days plus items needing attention</option><option value='21' {% if automation_settings.daily_days == 21 %}selected{% endif %}>Next 21 days</option></select><div class='helper'>Recommended: next 14 days. A fuller weekly check covers the paid window.</div></div>
            <div class='grid-two'><label class='checkbox'><input type='checkbox' name='weekly_enabled' value='1' {% if automation_settings.weekly_enabled %}checked{% endif %}><span>Weekly full-window check</span></label><div class='field'><label>Weekly day</label><select name='weekly_day'><option value='sunday' {% if automation_settings.weekly_day == 'sunday' %}selected{% endif %}>Sunday</option><option value='monday' {% if automation_settings.weekly_day == 'monday' %}selected{% endif %}>Monday</option><option value='friday' {% if automation_settings.weekly_day == 'friday' %}selected{% endif %}>Friday</option></select></div></div>
            <div class='field'><label>Weekly time</label><input type='time' name='weekly_time' value='{{ automation_settings.weekly_time }}'></div>
            <h3>Gentle notifications</h3>
            <label class='checkbox'><input type='checkbox' name='notifications_enabled' value='1' {% if automation_settings.notifications_enabled %}checked{% endif %}><span>Use TrainerMate notifications</span></label>
            <label class='checkbox'><input type='checkbox' name='notify_course_changes' value='1' {% if automation_settings.notify_course_changes %}checked{% endif %}><span>Tell me when courses are found or updated</span></label>
            <label class='checkbox'><input type='checkbox' name='notify_problems' value='1' {% if automation_settings.notify_problems %}checked{% endif %}><span>Tell me when something needs attention</span></label>
            <label class='checkbox'><input type='checkbox' name='notify_success_no_changes' value='1' {% if automation_settings.notify_success_no_changes %}checked{% endif %}><span>Tell me when a scan completes with no changes</span></label>
            <label class='checkbox'><input type='checkbox' name='notify_support_messages' value='1' {% if automation_settings.notify_support_messages %}checked{% endif %}><span>Show support/admin message notifications</span></label>
            <label class='checkbox'><input type='checkbox' name='popup_bubbles' value='1' {% if automation_settings.popup_bubbles %}checked{% endif %}><span>Show small pop-up bubbles</span></label>
            <div class='inline-actions'><button class='btn' type='submit'>Save Automatic Sync settings</button></div>
          </form>
          <form method='post' action='{{ url_for("automation_run_now") }}'><button class='btn soft' type='submit'>Run quiet check now</button></form>
              <p class='helper'>Automatic Sync only runs while TrainerMate is open in Python mode. When packaged later, it can run from the desktop app/tray.</p>
            </div></section>
          </div>
          {% if not automatic_sync_allowed %}
            <div class='tm-lock-overlay'><div class='tm-lock-card'><h3>Automatic Sync is a paid feature</h3><p>Free users can run manual trial syncs. Paid users can let TrainerMate quietly check upcoming courses in the background and notify them only when something changes or needs attention.</p><button class='btn tm-paid-modal-trigger' type='button' data-title='Automatic Sync is included with TrainerMate Paid' data-message='Automatic Sync can run a daily light check and a weekly full-window check while TrainerMate is open. It will not start on Free.'>Got it</button><div class='helper-dark'>Nothing has been enabled on this account.</div></div></div>
          {% endif %}
        </div>
      {% elif current_section == 'activity' %}
        <section class='card'><div class='head'><h3>Messages & Activity</h3><p>Support messages, sync summaries, course updates and items that need attention.</p></div><div class='block stack'>
          {% if activity_items %}{% for item in activity_items[:12] %}<div class='provider-card'><div class='provider-header'><div><strong>{{ item.title }}</strong><div class='helper'>{{ item.created_at }} - {{ item.type|replace('_',' ') }}{% if not item.read_at %} - New{% endif %}</div></div><span class='status-tag {{ "bad" if item.severity in ["warning","error"] else "ok" }}'>{{ item.severity }}</span></div><p>{{ item.summary or item.message }}</p>{% if item.items %}<details><summary>View course detail</summary>{% for c in item.items[:10] %}<div class='helper' style='padding:8px 0;border-top:1px solid var(--line)'><strong>{{ c.provider }}</strong> - {{ c.date_time }}<br>{{ c.course_type }}<br>{{ c.action or c.status or c.error }}</div>{% endfor %}</details>{% endif %}</div>{% endfor %}<a class='btn soft' href='{{ url_for("activity_centre") }}'>Open full activity centre</a>{% else %}<div class='empty'>No messages or activity yet.</div>{% endif %}
        </div></section>
      {% elif current_section == 'calendar' %}
        {% if calendar_sync_allowed %}
          <div class='panel'><div class='head'><h3>Calendar</h3><p>Click any course to see details and open the right provider tools</p></div><div class='empty'><div class='calendar-toolbar'>{% for provider in providers %}<span><i class='cal-dot' style='background:{{ provider.color }}'></i> {{ provider.name }}</span>{% endfor %}<span>Status is shown on each course</span></div><div id='tmCalendar'></div></div></div>
        {% else %}
          <div class='tm-lock-wrap is-locked'><div class='tm-lock-content'><div class='panel'><div class='head'><h3>Calendar</h3><p>Calendar view and calendar sync are included with TrainerMate Paid.</p></div><div class='empty' style='min-height:360px'>Your course list remains available on the dashboard.</div></div></div><div class='tm-lock-overlay'><div class='tm-lock-card'><h3>Calendar is a paid feature</h3><p>Free users can still view courses, manage providers and connect Zoom. Paid unlocks calendar tools and automatic calendar updates.</p><button class='btn tm-paid-modal-trigger' type='button' data-title='Calendar is included with TrainerMate Paid' data-message='Calendar tools unlock when your paid plan is active. Your existing courses and settings have not been changed.'>Got it</button><div class='helper-dark'>Nothing has been changed.</div></div></div></div>
        {% endif %}
      {% elif current_section == 'files' %}
      <div class='certificate-workspace{% if certificate_alerts_visible %} certificate-has-attention{% endif %}' id='tmCertificateWorkspace'>
        <div class='panel certificate-panel certificate-dashboard-panel'>
          <div class='hero'>
            <div><h2>Certificates</h2><p class='muted'>Add your certificates, check expiry dates, and compare them with what FOBS currently holds.</p></div>
            <div class='hero-actions'>
              {% if certificate_manage_allowed %}<form method='post' action='{{ url_for("scan_all_certificates") }}' class='certificate-refresh-form'><button class='btn soft certificate-job-control' type='submit'>Refresh FOBS certificates</button></form>{% else %}<button class='btn soft tm-paid-modal-trigger' type='button' data-title='Certificates are included with TrainerMate Paid' data-message='You can still view certificate status on Free. Refreshing provider certificates, uploading files and deleting FOBS copies are paid features.'>Refresh FOBS certificates</button>{% endif %}
            </div>
          </div>
          <div class='document-strip'>
            <div><strong>{{ document_summary.expired + document_summary.expiring }}</strong><span>Expiry warnings</span></div>
          </div>
          {% if document_expiry_warnings %}
            <div class='expiry-warning-list'>
              {% for item in document_expiry_warnings %}
                <div class='expiry-warning-row'>
                  <div class='expiry-warning-main'><strong>{{ item.title }}</strong><small>{{ item.provider_text }}</small></div>
                  <span>{{ item.message }}</span>
                </div>
              {% endfor %}
            </div>
          {% endif %}
          <div class='certificate-progress idle' id='tmCertificateProgress'>
            <div class='certificate-progress-top'>
              <div>
                <div class='certificate-progress-title' id='tmCertificateProgressTitle'>Certificate refresh idle</div>
                <div class='certificate-progress-detail' id='tmCertificateProgressDetail'>Refresh FOBS certificates to check provider files.</div>
              </div>
              <span class='status-tag neutral' id='tmCertificateProgressState'>idle</span>
            </div>
          </div>
          <div class='certificate-busy-note' id='tmCertificateBusyNote'>
            <div class='certificate-busy-note-top'>
              <div class='certificate-busy-main'>
                <span class='certificate-spinner'></span>
                <div>
                  <div class='certificate-busy-title' id='tmCertificateBusyTitle'>TrainerMate is checking certificates</div>
                <div class='certificate-busy-detail' id='tmCertificateBusyDetail'>Checking the provider certificate lists.</div>
                </div>
              </div>
              <span class='status-tag due'>working</span>
            </div>
            <div class='certificate-busy-sub'>You can still view certificates and use this page. Only actions that start another FOBS job are paused.</div>
          </div>
          <div class='certificate-action-zone' id='tmCertificateActionZone'>
          {% if certificate_alerts_visible %}
            <div class='certificate-attention'>
              <div class='certificate-attention-title'>Certificates !</div>
              {% for item in certificate_alerts_visible %}
                <div class='certificate-attention-row' data-auto-dismiss-alert-id='{{ (item.alert_ids or [item.alert_id])|join(",") }}'>
                  <div>
                    <strong>{{ item.certificate_name }}</strong>
                    <p>{{ item.message }}</p>
                  </div>
                  <div class='certificate-attention-actions'>
                    <form method='post' action='{{ url_for("dismiss_certificate_prompt", link_id=item.link_id) }}'>{{ csrf_hidden_field()|safe }}{% for alert_id in item.alert_ids %}<input type='hidden' name='alert_id' value='{{ alert_id }}'>{% endfor %}<button class='btn ghost small' type='submit'>Dismiss</button></form>
                  </div>
                </div>
              {% endfor %}
            </div>
          {% endif %}
          <div class='tm-lock-wrap {% if not certificate_manage_allowed %}is-locked{% endif %}'>
            <div class='tm-lock-content'>
              <details class='add-document-drawer' {% if add_document_form %}open{% endif %}>
                <summary><span>Add certificate</span><span class='badge'>+</span></summary>
                <form method='post' action='{{ url_for("add_document") }}' enctype='multipart/form-data' class='stack'>
              <p class='helper'>TrainerMate will use the selected file name as the certificate name.</p>
              <div class='field'><label>Expiry date</label><input type='date' name='expiry_date' value='{{ add_document_form.expiry_date or "" }}'></div>
              <div class='field'><label>File</label><input type='file' name='document_file' accept='.docx,.pdf,.doc,.xls,.xlsx,.odt,.jpg,.jpeg,.png,.tif,.tiff' required>{% if add_document_form %}<span class='helper'>Please choose the certificate file again. Your details below have been kept.</span>{% endif %}</div>
              <div class='field document-provider-field'><label>Use for providers</label>
                {% if providers %}
                  <div class='document-provider-tools'>
                    <label class='checkbox master'><input type='checkbox' id='selectAllDocumentProviders' checked> <span>Use for all providers</span></label>
                    <span class='helper'>Untick any provider that should not use this certificate.</span>
                  </div>
                  <div class='document-provider-grid'>
                    {% for provider in providers %}
                      <label class='checkbox'><input type='checkbox' class='document-provider-checkbox' name='provider_ids' value='{{ provider.id }}' {% if not add_document_form or provider.id in add_document_form.provider_ids %}checked{% endif %}> <span>{{ provider.name }}</span></label>
                    {% endfor %}
                  </div>
                {% else %}
                  <div class='helper'>Add providers first, then assign documents to them.</div>
                {% endif %}
              </div>
              <div class='inline-actions'><button class='btn' type='submit'>Add to TrainerMate</button></div>
                </form>
              </details>
            </div>
            {% if not certificate_manage_allowed %}
              <div class='tm-lock-overlay'><div class='tm-lock-card'><h3>Certificate upload is a paid feature</h3><p>You can still view certificate status on Free. Uploading certificates and sending them to FOBS unlocks on TrainerMate Paid.</p><button class='btn tm-paid-modal-trigger' type='button' data-title='Certificate management is included with TrainerMate Paid' data-message='Certificate upload, replacement and FOBS certificate management are available once your paid plan is active.'>Got it</button></div></div>
            {% endif %}
          </div>
          </div>
        </div>

        <div class='panel certificate-panel certificate-fobs-panel certificate-action-zone'>
          <div class='head fobs-head'><h3>Certificates currently in FOBS</h3><p>Click a provider below to open its certificate list. The colours match your provider colours.</p></div>
          <div class='block stack'>
            {% for provider in providers %}
              {% set certs = provider_certificates_by_provider.get(provider.id, []) %}
              <details class='provider-card compact-checklist cert-provider-card' style='--provider-color: {{ provider.color }};'>
                <summary class='provider-header cert-provider-summary'>
                  <span class='provider-summary-main'>
                    <span class='provider-colour-dot'></span>
                    <span>
                      <strong>{{ provider.name }}</strong>
                      <span class='provider-summary-helper'>Click to show certificates</span>
                    </span>
                  </span>
                  <span class='provider-summary-right'>
                    <span class='status-tag neutral'>{{ certs|length }} found</span>
                    <span class='expand-word'>Open</span>
                  </span>
                </summary>
                {% if certificate_manage_allowed %}<form method='post' action='{{ url_for("scan_provider_certificates_route", provider_id=provider.id) }}' class='certificate-refresh-form provider-refresh-form'><button class='btn soft small certificate-job-control' type='submit'>Refresh {{ provider.name }}</button></form>{% else %}<button class='btn soft small tm-paid-modal-trigger' type='button' data-title='Certificates are included with TrainerMate Paid' data-message='Refreshing certificates from FOBS is available on the paid plan. You can still view the most recent certificate status on Free.'>Refresh {{ provider.name }}</button>{% endif %}
                {% if certs %}
                  <div class='tablewrap'>
                    <table class='course-table'>
                      <thead><tr><th>Certificate in FOBS</th><th>Expiry</th><th>File</th><th>Action</th></tr></thead>
                      <tbody>
                        {% for cert in certs %}
                          <tr>
                            <td><strong>{{ cert.certificate_name }}</strong></td>
                            <td>{% if cert.expiry_date %}{{ cert.expiry_date }}{% else %}-{% endif %}</td>
                            <td>
                              {% if cert.cached_file_available %}
                                <a class='btn soft small' href='{{ url_for("view_provider_certificate_file", certificate_id=cert.id) }}' target='_blank' rel='noopener'>View file</a>
                              {% else %}
                                <span class='helper'>Refresh to save file</span>
                              {% endif %}
                            </td>
                            <td>
                              <form method='post' action='{{ url_for("delete_provider_certificate_route", certificate_id=cert.id) }}' class='provider-certificate-delete-form' data-provider-name='{{ provider.name }}' data-cancel-action='{{ url_for("cancel_provider_certificate_delete_route", certificate_id=cert.id) }}' onsubmit='return confirm("Remove this certificate from {{ provider.name }} FOBS? TrainerMate will check FOBS afterwards before updating the list.");'>
                                {{ csrf_hidden_field()|safe }}
                                {% if certificate_manage_allowed %}<button class='btn remove-fobs small certificate-job-control' type='submit'>Remove from FOBS</button>{% else %}<button class='btn remove-fobs small tm-paid-modal-trigger' type='button' data-title='Certificate management is included with TrainerMate Paid' data-message='Removing certificates from FOBS is a paid feature. Nothing has been removed from FOBS.'>Remove from FOBS</button>{% endif %}
                                <button class='btn provider-delete-cancel small' type='button'>Cancel</button>
                                <div class='provider-delete-progress' aria-hidden='true'><i></i></div>
                              </form>
                            </td>
                          </tr>
                        {% endfor %}
                      </tbody>
                    </table>
                  </div>
                {% else %}
                  <div class='empty'>No FOBS certificates scanned yet for {{ provider.name }}.</div>
                {% endif %}
              </details>
            {% endfor %}
            {% if not providers %}<div class='empty'>Add providers first.</div>{% endif %}
          </div>
        </div>
      </div>
      {% endif %}


      <div class='panel live-status-panel'>
        <details>
          <summary><span>Live status</span><span class='badge' id='tmLiveBadge'>live</span></summary>
          <div class='live-status-body'>
            <div class='live-status-grid'>
              <div class='live-status-card'><strong>Sync state</strong><div id='tmLiveSyncState'>Loading...</div></div>
              <div class='live-status-card'><strong>Current provider</strong><div id='tmLiveProvider'>-</div></div>
              <div class='live-status-card'><strong>Current course</strong><div id='tmLiveCourse'>-</div></div>
              <div class='live-status-card'><strong>Zoom result</strong><div id='tmLiveZoom'>Waiting</div></div>
            </div>
            <div class='live-status-list' id='tmLiveList'>
              <div class='live-status-row'><span>Waiting for status</span><span>Live updates will appear here during sync.</span></div>
            </div>
            <div class='live-status-note'>Read-only live status from app_state.json. No debug log and no buttons.</div>
          </div>
        </details>
      </div>

      <div class='footer-note'>Technical details are intentionally kept out of the main workspace.<br>{{ build_label }} - {{ build_name }}</div>
    </div>
  </div>
</div>
<script>
function tmOpenModal(title, message){
  const modal = document.getElementById('tmInfoModal');
  if(!modal) return;
  const titleEl = document.getElementById('tmInfoModalTitle');
  const msgEl = document.getElementById('tmInfoModalMessage');
  if(titleEl) titleEl.textContent = title || 'TrainerMate';
  if(msgEl) msgEl.textContent = message || '';
  modal.hidden = false;
  document.body.classList.add('tm-modal-open');
}
function tmCloseModal(){
  const modal = document.getElementById('tmInfoModal');
  if(!modal) return;
  modal.hidden = true;
  document.body.classList.remove('tm-modal-open');
}
document.addEventListener('click', function(e){
  const trigger = e.target.closest('.tm-paid-modal-trigger');
  if(trigger){ e.preventDefault(); tmOpenModal(trigger.dataset.title, trigger.dataset.message); return; }
  const closeTrigger = e.target.closest && e.target.closest('[data-tm-modal-close],#tmInfoModalClose,#tmInfoModalX');
  if(closeTrigger || e.target.id === 'tmInfoModal'){
    e.preventDefault();
    tmCloseModal();
  }
});
document.addEventListener('keydown', function(e){ if(e.key === 'Escape') tmCloseModal(); });
window.addEventListener('load', function(){ const modal=document.getElementById('tmInfoModal'); if(modal && !modal.hidden) document.body.classList.add('tm-modal-open'); });
const presets = {{ provider_presets_json|safe }};
const presetSelect = document.getElementById('provider_preset');
const providerName = document.getElementById('provider_name');
const loginUrl = document.getElementById('login_url');
const providerColor = document.getElementById('provider_color');
window.tmProviderPresetChanged = function(selectEl) {
  const s = selectEl || document.getElementById('provider_preset');
  const n = document.getElementById('provider_name');
  const u = document.getElementById('login_url');
  const option = s && s.options ? s.options[s.selectedIndex] : null;
  const isManual = !!s && s.value === 'manual';
  const name = option ? (option.getAttribute('data-provider-name') || '') : '';
  const url = option ? (option.getAttribute('data-login-url') || '') : '';
  if (n) {
    n.value = isManual ? '' : name;
    n.readOnly = !isManual;
    n.required = isManual;
    n.placeholder = isManual ? 'Provider name' : name;
  }
  if (u) {
    u.value = isManual ? '' : url;
    u.readOnly = !isManual && !!url;
    u.required = isManual;
    u.placeholder = isManual ? 'https://.../Account/Login' : (url || 'FOBS details not confirmed yet');
  }
  document.querySelectorAll('.manual-provider-field').forEach(function(field){
    field.style.display = isManual ? 'grid' : 'none';
  });
};
(() => {
  const screen = document.getElementById('tmStartupScreen');
  if (!screen) return;
  const title = document.getElementById('tmStartupTitle');
  const subtitle = document.getElementById('tmStartupSubtitle');
  const stepsEl = document.getElementById('tmStartupSteps');
  const skip = document.getElementById('tmStartupSkip');
  const skippedKey = 'tmStartupScreenSkipped';
  let hidden = false;
  let startedAt = Date.now();
  let intervalId = null;

  function hide(){
    if (hidden) return;
    hidden = true;
    screen.classList.remove('show');
    if (intervalId) window.clearInterval(intervalId);
  }

  function iconFor(status){
    status = (status || '').toLowerCase();
    if (status === 'complete') return 'OK';
    if (status === 'skipped') return '-';
    if (status === 'warning' || status === 'error') return '!';
    return '...';
  }

  function render(data){
    if (!stepsEl) return;
    if (title) title.textContent = data.message || 'Getting TrainerMate ready';
    if (subtitle) subtitle.textContent = data.running ? 'TrainerMate is getting today ready.' : 'Ready when you are.';
    stepsEl.innerHTML = '';
    (data.steps || []).forEach(function(step){
      const status = (step.status || 'waiting').toLowerCase();
      const row = document.createElement('div');
      row.className = 'startup-step state-' + status;
      const icon = document.createElement('span');
      icon.className = 'startup-step-icon';
      icon.textContent = iconFor(status);
      const main = document.createElement('span');
      main.className = 'startup-step-main';
      const label = document.createElement('span');
      label.className = 'startup-step-label';
      label.textContent = step.label || '';
      const detail = document.createElement('span');
      detail.className = 'startup-step-detail';
      detail.textContent = step.detail || '';
      const state = document.createElement('span');
      state.className = 'startup-step-state';
      state.textContent = status === 'complete' ? 'done' : status;
      main.appendChild(label);
      main.appendChild(detail);
      row.appendChild(icon);
      row.appendChild(main);
      row.appendChild(state);
      stepsEl.appendChild(row);
    });
  }

  async function pollStartup(){
    if (hidden) return;
    try {
      const r = await fetch('/startup-status?_=' + Date.now(), {cache: 'no-store'});
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      render(data);
      if (data.done || (!data.running && Date.now() - startedAt > 1600)) {
        window.setTimeout(hide, data.needs_attention ? 1800 : 700);
      }
    } catch (_) {
      if (Date.now() - startedAt > 3500) hide();
    }
    if (Date.now() - startedAt > 14000) hide();
  }

  if (skip) {
    skip.addEventListener('click', function(){
      try { sessionStorage.setItem(skippedKey, '1'); } catch (_) {}
      hide();
    });
  }

  try {
    if (sessionStorage.getItem(skippedKey) === '1') return;
  } catch (_) {}
  window.setTimeout(function(){
    if (!hidden) screen.classList.add('show');
  }, 120);
  pollStartup();
  intervalId = window.setInterval(pollStartup, 850);
})();
if (presetSelect && providerName && loginUrl) {
  const applyProviderPreset = () => {
    const option = presetSelect.options[presetSelect.selectedIndex];
    const selected = presets[presetSelect.value] || {};
    const isManual = presetSelect.value === 'manual';
    const optionName = option && option.getAttribute('data-provider-name') ? option.getAttribute('data-provider-name') : '';
    const optionUrl = option && option.getAttribute('data-login-url') ? option.getAttribute('data-login-url') : '';
    const name = String(selected.name || optionName || '').trim();
    const url = String(selected.login_url || optionUrl || '').trim();
    if (!isManual) providerName.value = name;
    else if (!providerName.value) providerName.value = '';
    loginUrl.value = isManual ? loginUrl.value : url;
    providerName.readOnly = !isManual;
    loginUrl.readOnly = !isManual && !!url;
    providerName.placeholder = isManual ? 'Provider name' : name;
    loginUrl.placeholder = isManual ? 'https://.../Account/Login' : (url || 'FOBS details not confirmed yet');
    if (providerColor && typeof selected.color === 'string') providerColor.value = selected.color;
    const managed = document.querySelector('.provider-managed-toggle');
    if (managed && typeof selected.provider_manages_zoom !== 'undefined') managed.checked = !!selected.provider_manages_zoom;
    syncManagedZoom(document);
    window.tmProviderPresetChanged(presetSelect);
  };
  presetSelect.addEventListener('change', applyProviderPreset);
  presetSelect.addEventListener('change', () => window.tmProviderPresetChanged(presetSelect));
  presetSelect.addEventListener('click', () => window.tmProviderPresetChanged(presetSelect));
  applyProviderPreset();
  window.tmProviderPresetChanged(presetSelect);
}
function syncManagedZoom(root) {
  const forms = root.querySelectorAll ? root.querySelectorAll('form') : [];
  forms.forEach(form => {
    const managed = form.querySelector('.provider-managed-toggle');
    const overwrite = form.querySelector('.overwrite-toggle');
    if (!managed) return;
    const refresh = () => {
      if (overwrite && managed.checked) {
        overwrite.checked = true;
        overwrite.disabled = true;
        overwrite.closest('.checkbox')?.classList.add('linked-setting');
      } else if (overwrite) {
        overwrite.disabled = false;
        overwrite.closest('.checkbox')?.classList.remove('linked-setting');
      }
    };
    managed.addEventListener('change', refresh);
    refresh();
  });
}
syncManagedZoom(document);

(() => {
  const rows = Array.from(document.querySelectorAll('[data-auto-dismiss-alert-id]'));
  if (!rows.length) return;
  let sent = false;
  const dismissSeenCertificateNotices = () => {
    if (sent) return;
    sent = true;
    const ids = Array.from(new Set(rows.flatMap((row) => (row.getAttribute('data-auto-dismiss-alert-id') || '').split(',')).map((id) => id.trim()).filter(Boolean)));
    if (!ids.length) return;
    const form = new FormData();
    form.append('_csrf_token', '{{ csrf_token() }}');
    ids.forEach((id) => form.append('alert_ids', id));
    try {
      if (navigator.sendBeacon) {
        navigator.sendBeacon('{{ url_for("auto_dismiss_missing_certificate_prompts") }}', form);
        return;
      }
    } catch (_) {}
    try {
      fetch('{{ url_for("auto_dismiss_missing_certificate_prompts") }}', {method: 'POST', body: form, keepalive: true, credentials: 'same-origin'});
    } catch (_) {}
  };
  window.addEventListener('pagehide', dismissSeenCertificateNotices);
  document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'hidden') dismissSeenCertificateNotices(); });
})();

(() => {
  document.querySelectorAll('form').forEach(function(form){
    if (form.dataset.tmSubmitGuard === '1') return;
    form.dataset.tmSubmitGuard = '1';
    form.addEventListener('submit', function(event){
      if (event.defaultPrevented) return true;
      const method = (form.getAttribute('method') || 'get').toLowerCase();
      if (method === 'post' && !form.querySelector('input[name="_csrf_token"]')) {
        const token = document.createElement('input');
        token.type = 'hidden';
        token.name = '_csrf_token';
        token.value = '{{ csrf_token() }}';
        form.appendChild(token);
      }
      if (form.dataset.tmSubmitting === '1') {
        event.preventDefault();
        return false;
      }
      form.dataset.tmSubmitting = '1';
      window.setTimeout(function(){
        form.querySelectorAll('button[type="submit"],input[type="submit"]').forEach(function(button){
          button.disabled = true;
          if (button.tagName.toLowerCase() === 'button' && !button.dataset.tmOriginalText) {
            button.dataset.tmOriginalText = button.textContent || '';
            if ((button.textContent || '').trim()) button.textContent = 'Working...';
          }
        });
      }, 0);
      return true;
    });
  });
})();

(() => {
  const debugDetailsEl = document.getElementById('debugDetails');
  const debugLogEl = document.getElementById('debugLog');
  const clearDebugBtn = document.getElementById('clearDebugBtn');
  const copyDebugBtn = document.getElementById('copyDebugBtn');
  const providerEl = document.getElementById('debugProvider');
  const courseEl = document.getElementById('debugCourse');
  const messageEl = document.getElementById('debugMessage');

  if (!debugLogEl) return;

  let userPinnedToBottom = true;
  let lastText = debugLogEl.textContent || '';
  let clearHoldUntil = 0;

  function isNearBottom() {
    return (debugLogEl.scrollHeight - debugLogEl.scrollTop - debugLogEl.clientHeight) < 24;
  }

  function forceBottom() {
    debugLogEl.scrollTop = debugLogEl.scrollHeight;
  }

  function setTerminalText(text) {
    text = text || '';
    if (text === lastText) return;
    lastText = text;
    debugLogEl.textContent = text;
    if (userPinnedToBottom) {
      forceBottom();
      requestAnimationFrame(forceBottom);
    }
  }

  debugLogEl.addEventListener('scroll', () => {
    userPinnedToBottom = isNearBottom();
    debugLogEl.classList.toggle('user-scrolled', !userPinnedToBottom);
  }, {passive: true});

  if (debugDetailsEl) {
    debugDetailsEl.addEventListener('toggle', () => {
      if (debugDetailsEl.open && userPinnedToBottom) requestAnimationFrame(forceBottom);
    });
  }

  async function refreshDebugState() {
    try {
      const r = await fetch('/debug-state?_=' + Date.now(), {cache: 'no-store'});
      if (!r.ok) return;
      const s = await r.json();
      if (providerEl) providerEl.textContent = s.current_provider || '-';
      if (courseEl) courseEl.textContent = s.current_course || '-';
      if (messageEl) messageEl.textContent = s.last_message || s.last_status || s.last_run_status || 'Idle';
    } catch (_) {}
  }

  async function refreshDebugLog() {
    if (Date.now() < clearHoldUntil) return;
    try {
      const wasPinned = isNearBottom();
      if (wasPinned) userPinnedToBottom = true;
      const r = await fetch('/debug-log?lines=180&_=' + Date.now(), {cache: 'no-store'});
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      const text = typeof data.text === 'string' ? data.text : ((data.lines || []).join('\n'));
      setTerminalText(text);
    } catch (err) {
      setTerminalText('Live log unavailable: ' + err.message);
    }
  }

  async function clearDebugLog() {
    clearHoldUntil = Date.now() + 1200;
    userPinnedToBottom = true;
    setTerminalText('');
    forceBottom();
    try {
      const r = await fetch('/debug-log/clear?_=' + Date.now(), {
        method: 'POST',
        cache: 'no-store',
        headers: {'Cache-Control': 'no-cache'}
      });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      setTerminalText('');
      setTimeout(refreshDebugLog, 1300);
    } catch (err) {
      setTerminalText('Could not clear log: ' + err.message);
    }
  }

  function fallbackCopy(text) {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.setAttribute('readonly', '');
    ta.style.position = 'fixed';
    ta.style.left = '-9999px';
    ta.style.top = '0';
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    const ok = document.execCommand('copy');
    document.body.removeChild(ta);
    if (!ok) throw new Error('copy command failed');
  }

  async function copyLast30Lines() {
    const text = (debugLogEl.textContent || '').split('\n').slice(-30).join('\n');
    try {
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(text);
      } else {
        fallbackCopy(text);
      }
      copyDebugBtn.textContent = 'Copied';
    } catch (err) {
      try {
        fallbackCopy(text);
        copyDebugBtn.textContent = 'Copied';
      } catch (_) {
        copyDebugBtn.textContent = 'Copy failed';
      }
    }
    setTimeout(() => { copyDebugBtn.textContent = 'Copy last 30 lines'; }, 1200);
  }

  if (clearDebugBtn) clearDebugBtn.onclick = clearDebugLog;
  if (copyDebugBtn) copyDebugBtn.onclick = copyLast30Lines;

  userPinnedToBottom = true;
  forceBottom();
  refreshDebugState();
  refreshDebugLog();
  setInterval(refreshDebugState, 1000);
  setInterval(refreshDebugLog, 750);
  window.addEventListener('load', () => requestAnimationFrame(forceBottom));
})();
</script>

<script>
(function(){
  function setText(id, value){
    var el = document.getElementById(id);
    if(el) el.textContent = value || '-';
  }

  function makeRow(left, right){
    var div = document.createElement('div');
    div.className = 'live-status-row';
    var a = document.createElement('span');
    var b = document.createElement('span');
    a.textContent = left || '';
    b.textContent = right || '';
    div.appendChild(a);
    div.appendChild(b);
    return div;
  }

  var tmCertificateRefreshPending = sessionStorage.getItem('tmCertificateRefreshPending') === '1';
  var tmCertificateRefreshSawRunning = sessionStorage.getItem('tmCertificateRefreshSawRunning') === '1';
  var tmObservedCertificateRunning = sessionStorage.getItem('tmObservedCertificateRunning') === '1';
  var tmCertificateStayInPlace = sessionStorage.getItem('tmCertificateStayInPlace') === '1';
  var tmCertificateDeleteRow = null;
  function markCertificateRefreshStarted(){
    tmCertificateRefreshPending = true;
    tmCertificateRefreshSawRunning = false;
    tmCertificateStayInPlace = false;
    sessionStorage.setItem('tmCertificateRefreshPending', '1');
    sessionStorage.setItem('tmCertificateRefreshSawRunning', '0');
    sessionStorage.removeItem('tmCertificateStayInPlace');
  }
  function markCertificateInPlaceStarted(){
    tmCertificateRefreshPending = true;
    tmCertificateRefreshSawRunning = false;
    tmCertificateStayInPlace = true;
    sessionStorage.setItem('tmCertificateRefreshPending', '1');
    sessionStorage.setItem('tmCertificateRefreshSawRunning', '0');
    sessionStorage.setItem('tmCertificateStayInPlace', '1');
  }
  function markCertificateRefreshRunning(){
    tmCertificateRefreshSawRunning = true;
    tmObservedCertificateRunning = true;
    sessionStorage.setItem('tmCertificateRefreshSawRunning', '1');
    sessionStorage.setItem('tmObservedCertificateRunning', '1');
  }
  function clearCertificateRefreshMarker(){
    tmCertificateRefreshPending = false;
    tmCertificateRefreshSawRunning = false;
    tmObservedCertificateRunning = false;
    sessionStorage.removeItem('tmCertificateRefreshPending');
    sessionStorage.removeItem('tmCertificateRefreshSawRunning');
    sessionStorage.removeItem('tmObservedCertificateRunning');
    sessionStorage.removeItem('tmCertificateStayInPlace');
  }
  function reloadCertificatesView(){
    clearCertificateRefreshMarker();
    var target = new URL(window.location.href);
    target.searchParams.set('section', 'files');
    target.searchParams.set('certificates_refreshed', Date.now().toString());
    window.location.href = target.toString();
  }
  function finishCertificateInPlace(latestStatus){
    var row = tmCertificateDeleteRow;
    clearCertificateRefreshMarker();
    if(row && (latestStatus || '').toLowerCase() === 'complete'){
      row.style.transition = 'opacity .22s ease';
      row.style.opacity = '0';
      window.setTimeout(function(){ if(row.parentNode) row.parentNode.removeChild(row); }, 240);
    } else if(row) {
      row.querySelectorAll('button,input,select,textarea').forEach(function(control){ control.disabled = false; });
      row.querySelectorAll('.provider-certificate-delete-form').forEach(function(form){ form.classList.remove('is-working'); form.dataset.tmSubmitting = '0'; });
      row.querySelectorAll('.btn.remove-fobs').forEach(function(button){ button.textContent = button.dataset.tmOriginalText || 'Remove from FOBS'; });
      row.querySelectorAll('.provider-delete-cancel').forEach(function(button){ button.textContent = 'Cancel'; });
    }
    tmCertificateDeleteRow = null;
  }

  async function refreshTrainerMateLiveStatus(){
    try{
      var r = await fetch('/live-status?_=' + Date.now(), {cache:'no-store'});
      if(!r.ok) throw new Error('HTTP ' + r.status);
      var data = await r.json();

      setText('tmLiveBadge', data.running ? 'syncing' : 'idle');
      setText('tmLiveSyncState', data.sync_state || 'Idle');
      setText('tmLiveProvider', data.current_provider || '-');
      setText('tmLiveCourse', data.current_course || '-');
      setText('tmLiveZoom', data.zoom_result || 'Waiting');

      var list = document.getElementById('tmLiveList');
      if(list){
        list.innerHTML = '';
        (data.rows || []).forEach(function(item){
          list.appendChild(makeRow(item.left, item.right));
        });
      }
      var bubble = document.getElementById('tmProgressBubble');
      var title = document.getElementById('tmProgressTitle');
      var subtitle = document.getElementById('tmProgressSubtitle');
      var state = document.getElementById('tmProgressState');
      var icon = document.getElementById('tmProgressIcon');
      var body = document.getElementById('tmProgressBody');
      var certProgress = document.getElementById('tmCertificateProgress');
      var certTitle = document.getElementById('tmCertificateProgressTitle');
      var certDetail = document.getElementById('tmCertificateProgressDetail');
      var certState = document.getElementById('tmCertificateProgressState');
      var certWorkspace = document.getElementById('tmCertificateWorkspace');
      var certBusyNote = document.getElementById('tmCertificateBusyNote');
      var certBusyTitle = document.getElementById('tmCertificateBusyTitle');
      var certBusyDetail = document.getElementById('tmCertificateBusyDetail');
      if(certWorkspace) certWorkspace.classList.toggle('certificate-has-attention', !!document.querySelector('.certificate-attention'));
      if(certProgress){
        var cert = data.certificate_scan || {};
        var latest = cert.latest || {};
        var certRunning = !!data.certificate_running;
        certProgress.classList.remove('idle','running','error');
        certProgress.classList.add(certRunning ? 'running' : ((latest.status || '').toLowerCase() === 'error' ? 'error' : 'idle'));
        if(certTitle) certTitle.textContent = latest.message || (certRunning ? 'Refreshing FOBS certificates' : 'Certificate refresh idle');
        if(certDetail) certDetail.textContent = latest.detail || (certRunning ? 'Checking provider certificate lists.' : 'Refresh FOBS certificates to check provider files.');
        if(certState) {
          certState.textContent = certRunning ? 'working' : (latest.status || 'idle');
          certState.className = 'status-tag ' + (certRunning ? 'due' : ((latest.status || '').toLowerCase() === 'error' ? 'bad' : 'neutral'));
        }
        var detailText = (latest.detail || '').toLowerCase();
        var messageText = (latest.message || '').toLowerCase();
        var lightCertificateTask = certRunning && (detailText.indexOf('remove the certificate') >= 0 || detailText.indexOf('gone from fobs') >= 0 || messageText.indexOf('removing from') >= 0);
        if(certWorkspace) certWorkspace.classList.toggle('certificate-busy', certRunning && !lightCertificateTask);
        if(certBusyTitle) certBusyTitle.textContent = latest.message || 'TrainerMate is checking certificates';
        if(certBusyDetail) certBusyDetail.textContent = latest.detail || 'Checking the provider certificate lists.';
        document.querySelectorAll('.certificate-job-control').forEach(function(control){
          if(control.closest('.provider-certificate-delete-form.is-working')) return;
          control.disabled = certRunning && !lightCertificateTask;
        });
        if(certRunning){
          markCertificateRefreshRunning();
        } else if((tmCertificateRefreshPending || tmObservedCertificateRunning || tmCertificateRefreshSawRunning) && ['complete','error','cancelled'].indexOf((latest.status || '').toLowerCase()) >= 0){
          if(tmCertificateStayInPlace){
            window.setTimeout(function(){ finishCertificateInPlace(latest.status || ''); }, 350);
          } else {
            window.setTimeout(reloadCertificatesView, 350);
          }
        }
      }
      document.querySelectorAll('.provider-certificate-delete-form').forEach(function(form){
        if(form.dataset.tmBound === '1') return;
        form.dataset.tmBound = '1';
        form.addEventListener('submit', function(e){
          e.preventDefault();
          if(form.dataset.tmSubmitting === '1') return false;
          form.dataset.tmSubmitting = '1';
          markCertificateInPlaceStarted();
          tmCertificateDeleteRow = form.closest('tr');
          form.classList.add('is-working');
          var btn = form.querySelector('button');
          var cancelBtn = form.querySelector('.provider-delete-cancel');
          if(btn){
            btn.disabled = true;
            if(!btn.dataset.tmOriginalText) btn.dataset.tmOriginalText = btn.textContent || 'Remove from FOBS';
            btn.textContent = 'Removing...';
          }
          if(cancelBtn) cancelBtn.disabled = false;
          if(certProgress){
            certProgress.classList.remove('idle','error');
            certProgress.classList.add('running');
          }
          var providerName = form.dataset.providerName || 'provider';
          if(certTitle) certTitle.textContent = 'Removing from ' + providerName;
          if(certDetail) certDetail.textContent = 'TrainerMate is asking FOBS to remove the certificate.';
          if(certState) {
            certState.textContent = 'working';
            certState.className = 'status-tag due';
          }
          fetch(form.action, {
            method:'POST',
            body:new FormData(form),
            credentials:'same-origin',
            headers:{'Accept':'application/json'}
          })
            .then(function(r){ if(!r.ok) throw new Error('HTTP ' + r.status); return r.json().catch(function(){ return {}; }); })
            .then(function(payload){
              if(payload && payload.started === false) throw new Error(payload.message || 'Not started');
              refreshTrainerMateLiveStatus();
            })
            .catch(function(){
              form.classList.remove('is-working');
              form.dataset.tmSubmitting = '0';
              if(btn){
                btn.disabled = false;
                btn.textContent = btn.dataset.tmOriginalText || 'Remove from FOBS';
              }
              if(cancelBtn) cancelBtn.disabled = false;
              clearCertificateRefreshMarker();
            });
        }, true);
        var cancel = form.querySelector('.provider-delete-cancel');
        if(cancel && cancel.dataset.tmCancelBound !== '1'){
          cancel.dataset.tmCancelBound = '1';
          cancel.addEventListener('click', function(){
            if(form.dataset.tmSubmitting !== '1') return;
            cancel.disabled = true;
            cancel.textContent = 'Cancelling...';
            if(certDetail) certDetail.textContent = 'Cancelling if FOBS has not removed it yet.';
            fetch(form.dataset.cancelAction || '', {
              method:'POST',
              body:new FormData(form),
              credentials:'same-origin',
              headers:{'Accept':'application/json'}
            }).catch(function(){});
          });
        }
      });
      document.querySelectorAll('.certificate-refresh-form').forEach(function(form){
        if(form.dataset.tmBound === '1') return;
        form.dataset.tmBound = '1';
        form.addEventListener('submit', function(e){
          e.preventDefault();
          markCertificateRefreshStarted();
          var btn = form.querySelector('button');
          if(btn){
            btn.disabled = true;
            btn.textContent = 'Starting...';
          }
          if(certProgress){
            certProgress.classList.remove('idle','error');
            certProgress.classList.add('running');
          }
          if(certTitle) certTitle.textContent = 'Starting certificate refresh';
          if(certDetail) certDetail.textContent = 'TrainerMate will keep checking this for you.';
          if(certState) {
            certState.textContent = 'working';
            certState.className = 'status-tag due';
          }
          fetch(form.action, {method:'POST', body:new FormData(form), credentials:'same-origin'})
            .then(function(){ refreshTrainerMateLiveStatus(); })
            .catch(function(){ form.submit(); });
        });
      });
      if(bubble && body){
        bubble.classList.remove('idle','running','error');
        bubble.classList.add(data.running ? 'running' : ((data.sync_state || '').toLowerCase().indexOf('error') >= 0 ? 'error' : 'idle'));
        if(title) title.textContent = data.certificate_running ? 'Checking certificates' : (data.running ? 'Syncing TrainerMate' : 'TrainerMate ready');
        if(subtitle) subtitle.textContent = data.running ? (data.progress_summary || data.current_course || data.current_provider || 'Working on this now.') : 'No sync running.';
        if(state) state.textContent = data.running ? 'working' : 'idle';
        if(icon) icon.textContent = data.running ? '...' : 'OK';
        body.innerHTML = '';
        (data.rows || []).slice(0, 8).forEach(function(item){
          var step = document.createElement('div');
          step.className = 'tm-progress-step';
          var dot = document.createElement('i');
          var span = document.createElement('span');
          span.textContent = item.left || '';
          var small = document.createElement('small');
          small.textContent = item.right || '';
          span.appendChild(small);
          step.appendChild(dot);
          step.appendChild(span);
          body.appendChild(step);
        });
      }
    }catch(err){
      // Keep the page calm if the one-second status poll misses while the dashboard is starting/reloading.
      setText('tmLiveBadge', 'idle');
      setText('tmLiveSyncState', 'Waiting for dashboard status');
      var certProgress = document.getElementById('tmCertificateProgress');
      var certTitle = document.getElementById('tmCertificateProgressTitle');
      var certDetail = document.getElementById('tmCertificateProgressDetail');
      var certState = document.getElementById('tmCertificateProgressState');
      if(certProgress){
        certProgress.classList.remove('running','error');
        certProgress.classList.add('idle');
      }
      var certWorkspace = document.getElementById('tmCertificateWorkspace');
      if(certWorkspace) certWorkspace.classList.remove('certificate-busy');
      document.querySelectorAll('.certificate-job-control').forEach(function(control){
        control.disabled = false;
      });
      if(certTitle) certTitle.textContent = 'Dashboard status loading';
      if(certDetail) certDetail.textContent = 'This usually clears after a refresh or restart.';
      if(certState){
        certState.textContent = 'idle';
        certState.className = 'status-tag neutral';
      }
      var list = document.getElementById('tmLiveList');
      if(list){
        list.innerHTML = '';
        list.appendChild(makeRow('Waiting for status', 'Refresh the page if this stays here.'));
      }
    }
  }

  if(document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', refreshTrainerMateLiveStatus);
  } else {
    refreshTrainerMateLiveStatus();
  }
  setInterval(refreshTrainerMateLiveStatus, 1000);
})();
(function(){
  const master = document.getElementById('selectAllDocumentProviders');
  const boxes = Array.from(document.querySelectorAll('.document-provider-checkbox'));
  if (!master || !boxes.length) return;
  const syncMaster = () => { master.checked = boxes.every((box) => box.checked); };
  master.addEventListener('change', () => {
    boxes.forEach((box) => { box.checked = master.checked; });
  });
  boxes.forEach((box) => box.addEventListener('change', syncMaster));
  syncMaster();
})();

</script>




<details class="tm-progress-bubble idle" id="tmProgressBubble">
  <summary>
    <span class="tm-progress-icon" id="tmProgressIcon">OK</span>
    <span class="tm-progress-main"><span class="tm-progress-title" id="tmProgressTitle">TrainerMate ready</span><span class="tm-progress-subtitle" id="tmProgressSubtitle">Sync progress will appear here.</span></span>
    <span class="tm-progress-state" id="tmProgressState">idle</span>
  </summary>
  <div class="tm-progress-body" id="tmProgressBody">
    <div class="tm-progress-step"><i></i><span>Waiting<small>Start sync to see provider, course, Zoom and certificate progress.</small></span></div>
  </div>
</details>

<div class="course-modal" id="tmCourseModal" aria-hidden="true">
  <div class="course-modal-card" role="dialog" aria-modal="true" aria-labelledby="tmModalTitle">
    <button class="modal-close" id="tmModalClose" type="button" data-calendar-modal-close aria-label="Close">x</button>
    <div class="kicker" id="tmModalProvider">Provider</div>
    <h2 id="tmModalTitle" style="margin:4px 42px 8px 0;font-size:22px;line-height:1.2">Course</h2>
    <div class="modal-meta"><span id="tmModalDate">-</span><span id="tmModalTime">-</span><span class="status-tag neutral" id="tmModalStatus">Status</span></div>
    <div class="modal-grid">
      <div class="modal-field"><span>Provider</span><strong id="tmModalProviderName">-</strong></div>
      <div class="modal-field"><span>Zoom account</span><strong id="tmModalZoomAccount">-</strong></div>
      <div class="modal-field"><span>Meeting ID</span><strong id="tmModalMeetingId">-</strong></div>
      <div class="modal-field"><span>FOBS page</span><strong id="tmModalFobsSpecific">-</strong></div>
    </div>
    <div class="advice-box"><strong>Next step</strong><p id="tmModalAdvice">-</p></div>
    <div class="modal-actions">
      <button class="btn small" type="button" id="tmOpenBothBtn">Open Zoom meeting + FOBS</button>
      <a class="btn soft small" id="tmOpenZoomBtn" href="#" target="_blank" rel="noopener">Open Zoom meeting</a>
      <a class="btn soft small" id="tmOpenFobsBtn" href="#" target="_blank" rel="noopener">Open FOBS course</a>
      <a class="btn soft small" id="tmOpenJoinBtn" href="#" target="_blank" rel="noopener">Open join link</a>
      <form method="post" id="tmConfirmRemovedForm" style="display:none" onsubmit="return confirm('Only confirm removed if you have checked FOBS and the course has genuinely been cancelled or deleted. Continue?')">
        <button class="btn warn small" type="submit">Confirm removed</button>
      </form>
    </div>
    <p class="launch-status" id="tmLaunchStatus" role="status" aria-live="polite"></p>
    <p class="helper" id="tmModalHelper" style="margin-top:12px">Opens the Zoom meeting in your browser and starts the FOBS course summary in TrainerMate's authenticated browser.</p>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/fullcalendar@6.1.15/index.global.min.js"></script>
<script>
(function(){
  const calendarEl = document.getElementById('tmCalendar');
  if(!calendarEl) return;

  const modal = document.getElementById('tmCourseModal');
  const close = document.getElementById('tmModalClose');
  const confirmForm = document.getElementById('tmConfirmRemovedForm');
  const openBothBtn = document.getElementById('tmOpenBothBtn');
  const openZoomBtn = document.getElementById('tmOpenZoomBtn');
  const openFobsBtn = document.getElementById('tmOpenFobsBtn');
  const openJoinBtn = document.getElementById('tmOpenJoinBtn');
  const launchStatus = document.getElementById('tmLaunchStatus');
  let currentToolUrls = {};
  let fobsStatusTimer = null;
  function setText(id, value){ const el=document.getElementById(id); if(el) el.textContent=value || '-'; }
  function setLink(el, url){ if(!el) return; if(url){el.href=url;el.classList.remove('disabled');el.removeAttribute('aria-disabled');}else{el.href='#';el.classList.add('disabled');el.setAttribute('aria-disabled','true');} }
  function openUrl(url){ if(url){ window.open(url, '_blank', 'noopener'); } }
  function setLaunchStatus(message, level){
    if(!launchStatus) return;
    launchStatus.textContent = message || '';
    launchStatus.className = 'launch-status' + (level ? ' ' + level : '');
  }
  function stopFobsStatusPolling(){
    if(fobsStatusTimer){
      clearInterval(fobsStatusTimer);
      fobsStatusTimer = null;
    }
  }
  function pollFobsStatus(launchId){
    stopFobsStatusPolling();
    if(!launchId) return;
    const refresh = function(){
      fetch('/calendar/fobs-launch-status/' + encodeURIComponent(launchId), {cache: 'no-store'})
        .then(function(response){ if(!response.ok){ throw new Error('HTTP ' + response.status); } return response.json(); })
        .then(function(data){
          const status = data.status || '';
          if(data.message){ setLaunchStatus(data.message, status === 'error' ? 'error' : ''); }
          if(status === 'opened' || status === 'error'){
            stopFobsStatusPolling();
          }
        })
        .catch(function(err){
          setLaunchStatus('Could not read FOBS launch status: ' + err, 'warn');
          stopFobsStatusPolling();
        });
    };
    refresh();
    fobsStatusTimer = setInterval(refresh, 1000);
  }
  function launchFobsCourse(url){
    if(!url){ return false; }
    setLaunchStatus('Starting authenticated FOBS browser. This can take a few seconds...', '');
    fetch(url, {method: 'GET', credentials: 'same-origin'})
      .then(function(response){
        if(!response.ok){ throw new Error('HTTP ' + response.status); }
        return response.json().catch(function(){ return {}; });
      })
      .then(function(data){
        if(data && data.ok === false){ throw new Error(data.error || 'FOBS launch failed'); }
        setLaunchStatus('FOBS is opening in a separate TrainerMate browser window. It will log in and then open the course summary.', '');
        if(data && data.launchId){ pollFobsStatus(data.launchId); }
      })
      .catch(function(err){ setLaunchStatus('TrainerMate could not start FOBS automatically: ' + err, 'error'); });
    return true;
  }
  function openTwoTabs(firstUrl, secondUrl){
    // Avoid popup blockers by opening only the Zoom tab from the browser.
    // FOBS is opened by TrainerMate in an authenticated Playwright browser window.
    const zoomUrl = firstUrl || '';
    const fobsUrl = secondUrl || '';
    if(zoomUrl){
      const tab = window.open(zoomUrl, '_blank', 'noopener');
      if(!tab){
        setLaunchStatus('Zoom tab was blocked. Use the separate Open Zoom meeting button, or allow popups for TrainerMate.', 'warn');
      } else {
        setLaunchStatus('Zoom meeting opened. Starting FOBS next...', '');
      }
    }
    if(fobsUrl){
      launchFobsCourse(fobsUrl);
    }
    if(!zoomUrl && !fobsUrl){
      setLaunchStatus('No Zoom or FOBS link is available for this course yet. Run sync/import first.', 'warn');
    }
  }
  function setStatusClass(value){ const el=document.getElementById('tmModalStatus'); if(!el) return; el.className='status-tag neutral'; const v=(value||'').toLowerCase(); if(v.includes('sync due')) el.className='status-tag due'; else if(v.includes('need')) el.className='status-tag bad'; else if(v.includes('synced')||v.includes('ready')) el.className='status-tag ok'; else if(v.includes('later')||v.includes('scheduled')) el.className='status-tag later'; }
  function openModal(props){
    currentToolUrls = props || {};
    stopFobsStatusPolling();
    setLaunchStatus('', '');
    setText('tmModalProvider', props.provider || 'Course');
    setText('tmModalProviderName', props.provider || '-');
    setText('tmModalTitle', props.courseTitle || 'Course');
    setText('tmModalDate', props.date || '');
    setText('tmModalTime', props.time || '');
    setText('tmModalStatus', props.status || 'Status');
    setStatusClass(props.status || '');
    setText('tmModalAdvice', props.advice || props.note || '');
    const zoomLabel = props.providerManagesZoom ? 'Provider managed' : (props.zoomAccountLabel || 'Linked Zoom account not selected');
    setText('tmModalZoomAccount', zoomLabel);
    setText('tmModalMeetingId', props.meetingId || 'No Zoom meeting yet');
    setText('tmModalFobsSpecific', props.fobsUrl ? (props.fobsUrlIsExact ? 'Course summary' : 'Provider course list') : 'Not available yet');
    setLink(openZoomBtn, props.zoomUrl || props.zoomAccountUrl || '');
    if(openZoomBtn){ openZoomBtn.textContent = props.zoomUrl ? 'Open Zoom meeting' : 'Open Zoom account'; }
    setLink(openFobsBtn, props.fobsLaunchUrl || props.fobsUrl || '');
    if(openFobsBtn){ openFobsBtn.textContent = 'Open FOBS course'; }
    setLink(openJoinBtn, props.meetingLink || '');
    if(openBothBtn){
      openBothBtn.disabled = !(props.zoomUrl || props.fobsLaunchUrl || props.fobsUrl);
      openBothBtn.classList.toggle('disabled', openBothBtn.disabled);
      openBothBtn.textContent = 'Open Zoom meeting + FOBS';
    }
    const helper = document.getElementById('tmModalHelper');
    if(helper){
      helper.textContent = props.zoomUrl
        ? ('Opens the Zoom meeting summary. If Zoom asks you to sign in or switch account, use ' + zoomLabel + '. Then FOBS opens separately.')
        : ('No specific Zoom meeting is stored yet. Open ' + zoomLabel + ' and FOBS to check this course.');
    }
    if(confirmForm){
      if(props.canConfirmRemoved && props.courseId){
        confirmForm.style.display = 'block';
        confirmForm.action = '/course/' + encodeURIComponent(props.courseId) + '/confirm-removed';
      } else {
        confirmForm.style.display = 'none';
        confirmForm.action = '#';
      }
    }
    if(modal) modal.classList.add('open');
  }
  if(openBothBtn) openBothBtn.onclick = function(){
    openTwoTabs(currentToolUrls.zoomUrl || '', currentToolUrls.fobsLaunchUrl || currentToolUrls.fobsUrl || '');
  };
  if(openFobsBtn) openFobsBtn.onclick = function(e){
    const fobsUrl = currentToolUrls.fobsLaunchUrl || currentToolUrls.fobsUrl || '';
    if(fobsUrl && fobsUrl.indexOf('/calendar/open-fobs-course/') === 0){
      e.preventDefault();
      launchFobsCourse(fobsUrl);
    }
  };
  function closeCourseModal(e){
    if(e) e.preventDefault();
    stopFobsStatusPolling();
    if(modal) modal.classList.remove('open');
  }
  if(close) close.addEventListener('click', closeCourseModal);
  if(modal) modal.addEventListener('click', (e) => {
    const closeTrigger = e.target && e.target.closest && e.target.closest('[data-calendar-modal-close],#tmModalClose,.modal-close');
    if(e.target === modal || closeTrigger){
      closeCourseModal(e);
    }
  });

  const params = new URLSearchParams(window.location.search);
  const provider = params.get('provider') || 'all';

  const calendar = new FullCalendar.Calendar(calendarEl, {
    initialView: 'dayGridMonth',
    height: 'auto',
    firstDay: 1,
    nowIndicator: true,
    headerToolbar: {
      left: 'prev,next today',
      center: 'title',
      right: 'dayGridMonth,timeGridWeek,listMonth'
    },
    eventTimeFormat: {hour: '2-digit', minute: '2-digit', hour12: false},
    slotLabelFormat: {hour: '2-digit', minute: '2-digit', hour12: false},
    dayHeaderFormat: {weekday: 'short', day: 'numeric', month: 'numeric'},
    dayMaxEventRows: 4,
    views: { dayGridMonth: { dayMaxEventRows: 4 } },
    events: '/calendar-events?provider=' + encodeURIComponent(provider),
    eventContent: function(arg) {
      const esc = function(value){return String(value || '').replace(/[&<>]/g, function(c){return ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]);});};
      const p = arg.event.extendedProps || {};
      return {html: '<div class="tm-cal-event"><div class="tm-cal-time">' + esc(arg.timeText) + '</div><div class="tm-cal-title">' + esc(p.courseTitle || arg.event.title || 'Course') + '</div><div class="tm-cal-provider">' + esc(p.provider || '') + (p.status ? ' - ' + esc(p.status) : '') + '</div></div>'};
    },
    eventClick: function(info) {
      info.jsEvent.preventDefault();
      openModal(info.event.extendedProps || {});
    },
    // TRAINERMATE_PROTECTED: calendar-provider-colours
    // Do not remove this block. Month view must force provider colours onto the rendered event element.
    eventDidMount: function(info) {
      const p = info.event.extendedProps || {};
      const color = p.providerColor || info.event.backgroundColor || '#2563eb';
      const textColor = info.event.textColor || '#ffffff';
      info.el.style.backgroundColor = color;
      info.el.style.borderColor = color;
      info.el.style.color = textColor;
      info.el.title = (p.status || '') + (p.note ? ' - ' + p.note : '');
    }
  });
  calendar.render();
})();
</script>

</body>
</html>
"""


def set_flash(text, category='info'):
    text = str(text or '').strip()
    text = re.sub(r'(?i)(access[_ -]?token|refresh[_ -]?token|client[_ -]?secret|password)\s*[:=]\s*\S+', r'\1 hidden', text)
    if len(text) > 360:
        text = text[:357].rstrip() + '...'
    session['message'] = text
    session['message_category'] = category


def get_flash():
    text = session.pop('message', None)
    category = session.pop('message_category', 'info')
    return {'text': text, 'category': category} if text else None


def remember_add_document_form(form):
    session['add_document_form'] = {
        'expiry_date': (form.get('expiry_date') or '').strip(),
        'provider_ids': valid_document_provider_ids(form.getlist('provider_ids')),
    }


def _debug_state_lines():
    lines = []
    try:
        state = load_app_state()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        lines.append(f'[{now}] Dashboard live debug')
        lines.append(f"status={state.get('last_status') or 'Idle'} running={bool(state.get('sync_running'))} pid={state.get('pid') or ''}")
        lines.append(f"provider={state.get('current_provider') or '-'}")
        lines.append(f"course={state.get('current_course') or '-'}")
        lines.append(f"message={state.get('last_message') or state.get('last_status') or 'Idle'}")
        if state.get('last_run_status'):
            lines.append(f"last_run_status={state.get('last_run_status')}")

        summary = state.get('run_summary') if isinstance(state.get('run_summary'), dict) else {}
        if summary:
            useful = []
            for key in ('outcome', 'message', 'providers_attempted', 'providers_succeeded', 'providers_failed', 'courses_found', 'courses_processed', 'fobs_checked', 'fobs_updated', 'fobs_failed'):
                if key in summary:
                    useful.append(f'{key}={summary.get(key)}')
            if useful:
                lines.append('summary: ' + ' | '.join(useful))

        courses = state.get('courses') if isinstance(state.get('courses'), dict) else {}
        recent = []
        for key, value in courses.items():
            if isinstance(value, dict):
                recent.append((value.get('updated_at') or '', key, value))
        for _, key, value in sorted(recent)[-20:]:
            status = value.get('status') or ''
            step = value.get('step') or ''
            action = value.get('last_action') or value.get('error') or ''
            updated = value.get('updated_at') or ''
            lines.append(f'{updated} {status}/{step}: {key} - {action}')
    except Exception as exc:
        lines.append(f'Could not read dashboard state: {exc}')
    return lines

def tail_bot_log(max_lines=120):
    """Return only the latest physical bot_debug.log lines.

    Keep this deliberately boring: the terminal should display the file tail,
    not synthetic appended state lines. That makes Clear log genuinely empty.
    """
    try:
        max_lines = max(1, min(int(max_lines or 120), 500))
    except Exception:
        max_lines = 120

    if not BOT_LOG_PATH.exists():
        return []

    try:
        with BOT_LOG_PATH.open('r', encoding='utf-8', errors='replace') as f:
            lines = [line.rstrip('\n') for line in f.readlines()]
    except Exception as exc:
        return [f'Could not read debug log: {exc}']

    latest_start = None
    for idx, line in enumerate(lines):
        if 'Sync starting' in line or '[MAIN] Bot starting.' in line:
            latest_start = idx
    if latest_start is not None:
        lines = lines[latest_start:]

    return lines[-max_lines:]


@app.route('/debug-log')
def debug_log():
    try:
        max_lines = int(request.args.get('lines', '160') or 160)
    except Exception:
        max_lines = 160
    lines = tail_bot_log(max_lines)
    return jsonify({'ok': True, 'lines': lines, 'text': '\n'.join(lines)})


@app.route('/debug-log/clear', methods=['POST'])
def clear_debug_log():
    try:
        BOT_LOG_PATH.write_text('', encoding='utf-8')
        return jsonify({'ok': True, 'lines': [], 'text': '', 'message': 'Log cleared.'})
    except Exception as exc:
        return jsonify({'ok': False, 'lines': [f'Could not clear debug log: {exc}'], 'message': f'Could not clear debug log: {exc}'}), 500


@app.route('/debug-state')
def debug_state():
    state = reconcile_running_state()
    return jsonify({
        'sync_running': bool(state.get('sync_running')),
        'pid': state.get('pid'),
        'last_status': state.get('last_status') or '',
        'last_run_status': state.get('last_run_status') or '',
        'last_message': state.get('last_message') or '',
        'current_provider': state.get('current_provider') or '',
        'current_course': state.get('current_course') or '',
    })




def _parse_live_course_datetime(text):
    """Best-effort parser for course date strings inside app_state course keys."""
    import re
    from datetime import datetime

    raw = str(text or '')

    # Examples:
    # Essex | Speed Awareness | Tuesday, 13 October 2026 09:00 to 12:00
    # Saturday, 09 May 2026 13:45 to 16:45
    match = re.search(
        r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+'
        r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s+(\d{1,2}):(\d{2})',
        raw
    )
    if match:
        day, month_name, year, hour, minute = match.group(2), match.group(3), match.group(4), match.group(5), match.group(6)
        try:
            return datetime.strptime(f'{day} {month_name} {year} {hour}:{minute}', '%d %B %Y %H:%M')
        except Exception:
            pass

    # Database-style fallback: 2026-10-13 09:00
    match = re.search(r'(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})', raw)
    if match:
        try:
            return datetime.strptime(f'{match.group(1)} {match.group(2)}', '%Y-%m-%d %H:%M')
        except Exception:
            pass

    return None


def _live_course_sort_key(item):
    """Sort app_state course entries by real course date first, then update time."""
    updated_at, key, value = item
    course_dt = _parse_live_course_datetime(key)
    if course_dt:
        return (0, course_dt, str(key))
    return (1, str(updated_at or ''), str(key))

@app.route('/live-status')
def live_status_panel():
    state = reconcile_running_state()
    cert_snapshot = certificate_scan_snapshot()

    running = bool(state.get('sync_running') or state.get('running'))
    cert_running = bool(cert_snapshot.get('running'))
    current_provider = (state.get('current_provider') or '').strip()
    current_course = (state.get('current_course') or '').strip()
    last_status = (state.get('last_status') or state.get('status') or '').strip()
    last_message = (state.get('last_message') or state.get('message') or '').strip()
    last_run_status = (state.get('last_run_status') or '').strip()

    sync_state = 'Sync running' if running else ('Certificate refresh running' if cert_running else (last_status or last_run_status or 'Idle'))
    zoom_result = 'Waiting'
    rows = []

    if running and current_provider:
        rows.append({'left': f'Signing into {current_provider}', 'right': 'Checking courses and certificates.'})
    if running and current_course:
        rows.append({'left': 'Checking course', 'right': current_course})
    if running and last_message:
        rows.append({'left': 'Latest update', 'right': last_message})

    scan_request = state.get('scan_request') if isinstance(state.get('scan_request'), dict) else {}
    if running and scan_request:
        provider_text = scan_request.get('provider') or 'all'
        days = scan_request.get('days')
        days_text = f'next {days} days'
        rows.append({'left': 'Requested scan', 'right': f'{provider_text} - {days_text}'})

    summary = state.get('run_summary') if isinstance(state.get('run_summary'), dict) else {}
    if summary and running:
        msg = summary.get('message') or summary.get('outcome') or ''
        if msg:
            rows.append({'left': 'Current update', 'right': str(msg)})

        counters = []
        for label, key in (
            ('found', 'courses_found'),
            ('processed', 'courses_processed'),
            ('checked', 'fobs_checked'),
            ('updated', 'fobs_updated'),
            ('failed', 'fobs_failed'),
        ):
            if summary.get(key) is not None:
                counters.append(f"{summary.get(key)} {label}")
        if counters and running:
            rows.append({'left': 'Course totals', 'right': ' - '.join(counters)})

        if summary.get('fobs_updated') is not None:
            zoom_result = f"{summary.get('fobs_updated')} Zoom link(s) updated"
        elif summary.get('fobs_checked') is not None:
            zoom_result = f"{summary.get('fobs_checked')} Zoom link(s) checked"

    if running:
        courses = state.get('courses') if isinstance(state.get('courses'), dict) else {}
        recent = []
        for key, value in courses.items():
            if isinstance(value, dict):
                recent.append((value.get('updated_at') or '', str(key), value))

        for _, key, value in sorted(recent, key=_live_course_sort_key)[:8]:
            action = (value.get('last_action') or value.get('message') or value.get('error') or value.get('status') or '').strip()
            status = (value.get('status') or '').strip().lower()
            lower = action.lower()

            if 'already has valid live zoom' in lower or 'already present' in lower or 'zoom joining instructions already present' in lower:
                result = 'FOBS + Zoom OK'
                zoom_result = 'Existing Zoom instructions found'
            elif 'updated successfully' in lower or 'fobs updated successfully' in lower:
                result = 'Zoom link updated successfully'
                zoom_result = 'Zoom link updated'
            elif status == 'skipped' or lower == 'read course summary':
                result = 'FOBS + Zoom OK' if lower == 'read course summary' else (action or 'Skipped')
            elif status == 'success':
                result = action or 'Ready'
            elif status == 'error':
                result = 'Needs attention'
                if action:
                    result += f' - {action}'
            else:
                result = action or status.title() or 'Checked'

            rows.append({'left': key, 'right': result})

    for cert_state in cert_snapshot.get('rows', []):
        msg = cert_state.get('message') or ''
        detail = cert_state.get('detail') or ''
        if msg and (cert_running or not rows):
            rows.append({'left': msg, 'right': detail})

    if not rows:
        rows.append({'left': 'No sync running', 'right': 'Start sync or refresh certificates to see live progress here.'})

    if cert_running:
        latest_cert = cert_snapshot.get('latest') or {}
        progress_summary = latest_cert.get('detail') or latest_cert.get('message') or 'Refreshing FOBS certificates.'
    elif running:
        progress_summary = current_course or last_message or (rows[0].get('right') if rows else '')
    else:
        progress_summary = 'No sync running.'

    return jsonify({
        'running': running or cert_running,
        'sync_running': running,
        'certificate_running': cert_running,
        'sync_state': sync_state,
        'current_provider': current_provider or '-',
        'current_course': current_course or '-',
        'zoom_result': zoom_result,
        'progress_summary': progress_summary,
        'certificate_scan': cert_snapshot,
        'rows': rows,
    })


@app.route('/startup-status')
def startup_status_panel():
    state = load_app_state()
    cert_snapshot = certificate_scan_snapshot()
    zoom_status = get_startup_zoom_health_status()
    providers = load_providers()
    active_provider_count = sum(1 for provider in providers if provider.get('active', True))
    course_count = sum(course_counts_by_provider().values())

    cert_latest = cert_snapshot.get('latest') or {}
    cert_status = (cert_latest.get('status') or 'idle').lower()
    if not STARTUP_CERTIFICATE_SCAN_ENABLED:
        provider_step = {
            'key': 'providers',
            'label': 'Checking providers',
            'status': 'skipped',
            'detail': 'Provider startup checks are turned off.',
        }
    elif active_provider_count <= 0:
        provider_step = {
            'key': 'providers',
            'label': 'Checking providers',
            'status': 'skipped',
            'detail': 'No active providers are set for sync.',
        }
    elif cert_snapshot.get('running'):
        provider_step = {
            'key': 'providers',
            'label': cert_latest.get('message') or 'Checking providers',
            'status': 'running',
            'detail': cert_latest.get('detail') or 'Checking provider certificate lists.',
        }
    elif cert_status in {'complete', 'error'}:
        provider_step = {
            'key': 'providers',
            'label': cert_latest.get('message') or 'Checking providers',
            'status': cert_status,
            'detail': cert_latest.get('detail') or '',
        }
    elif STARTUP_CERTIFICATE_SCAN_STARTED:
        provider_step = {
            'key': 'providers',
            'label': 'Checking providers',
            'status': 'waiting',
            'detail': 'Provider certificate check is queued.',
        }
    else:
        provider_step = {
            'key': 'providers',
            'label': 'Checking providers',
            'status': 'waiting',
            'detail': 'Provider checks will start shortly.',
        }

    zoom_state = (zoom_status.get('status') or 'idle').lower()
    if zoom_state == 'idle' and not STARTUP_ZOOM_HEALTH_CHECK_STARTED:
        zoom_state = 'waiting'
    zoom_step = {
        'key': 'zoom',
        'label': zoom_status.get('message') or 'Checking Zoom',
        'status': zoom_state,
        'detail': zoom_status.get('detail') or 'Making sure Zoom is ready.',
    }

    course_step = {
        'key': 'courses',
        'label': "Loading today's courses",
        'status': 'complete',
        'detail': f'{course_count} future course(s) loaded.' if course_count else 'Course list is ready.',
    }
    dashboard_step = {
        'key': 'dashboard',
        'label': 'Opening dashboard',
        'status': 'complete',
        'detail': BUILD_LABEL,
    }
    steps = [zoom_step, provider_step, course_step, dashboard_step]
    active_states = {'waiting', 'running', 'idle'}
    terminal_states = {'complete', 'skipped', 'error', 'warning'}
    running = any((step.get('status') or '').lower() in active_states for step in steps)
    done = all((step.get('status') or '').lower() in terminal_states for step in steps)
    has_attention = any((step.get('status') or '').lower() in {'error', 'warning'} for step in steps)

    return jsonify({
        'running': running and not done,
        'done': done,
        'needs_attention': has_attention,
        'sync_running': bool(state.get('sync_running') or state.get('running')),
        'steps': steps,
        'message': 'Getting TrainerMate ready' if running and not done else ('TrainerMate is ready' if not has_attention else 'TrainerMate is ready, with something to check'),
    })




def days_until_course_row(row):
    """Best effort days-until calculation from rendered dashboard row labels."""
    try:
        date_label = (row.get('date_label') or '').strip()
        time_label = (row.get('time_label') or '00:00').strip() or '00:00'
        dt = datetime.strptime(f'{date_label} {time_label}', '%a %d %b %Y %H:%M')
        return max(0, (dt.date() - datetime.now().date()).days)
    except Exception:
        return None


def scan_days_for_course_datetime(date_time_text, active_sync_window_days):
    try:
        dt = datetime.strptime((date_time_text or '').strip()[:16], '%Y-%m-%d %H:%M')
        days = max(1, (dt.date() - datetime.now().date()).days + 1)
    except Exception:
        days = 14
    cap = active_sync_window_days or 84
    return max(1, min(days, cap))


def best_single_course_check_row(rows):
    actionable = []
    for row in rows or []:
        if row.get('is_outside_window'):
            continue
        label = (row.get('status_label') or '').lower()
        msg = (row.get('short_message') or '').lower()
        if label in {'not checked', 'needs confirmation', 'needs attention', 'sync due'} or 'cancelled' in msg or 'deleted' in msg or 'not yet checked' in msg:
            actionable.append(row)
    if not actionable:
        return None
    return sorted(actionable, key=lambda r: (days_until_course_row(r) if days_until_course_row(r) is not None else 9999, r.get('provider',''), r.get('title','')))[0]



def course_action_exact_key(course_or_row):
    """Stable exact selector for a course alert/action.

    Course IDs alone can be unsafe when a provider has multiple courses on the
    same day, so alert actions also carry provider, title, and exact start time.
    The bot gets the same exact key through the environment.
    """
    item = course_or_row or {}
    provider = provider_slug(item.get('provider') or item.get('provider_id') or '')
    title = provider_slug(item.get('title') or '')
    date_time = (item.get('date_time') or item.get('date_time_raw') or '').strip()
    compact_dt = re.sub(r'[^0-9A-Za-z]+', '', date_time)
    course_id = str(item.get('id') or '').strip()
    return ':'.join(part for part in (provider, compact_dt, title, course_id) if part)

def build_course_action_alerts(rows, dismissed=None, state=None):
    dismissed = dismissed or set()
    state = state if isinstance(state, dict) else {}
    sync_recommended = []
    active_target_course_id = ''
    active_target_course_key = ''
    active_target_provider = ''
    active_target_title = ''
    active_target_date_time = ''
    if state.get('sync_running'):
        scan_request = state.get('scan_request') if isinstance(state.get('scan_request'), dict) else {}
        active_target_course_id = str(scan_request.get('target_course_id') or '').strip()
        active_target_course_key = str(scan_request.get('target_course_key') or '').strip()
        active_target_provider = provider_slug(scan_request.get('target_course_provider') or scan_request.get('provider') or '')
        active_target_title = provider_slug(scan_request.get('target_course_title') or '')
        active_target_date_time = str(scan_request.get('target_course_date_time') or '').strip()

    alerts = []
    for row in rows or []:
        if not row.get('is_action_needed') or row.get('is_outside_window'):
            continue
        course_id = str(row.get('id') or '').strip()
        if not course_id:
            continue

        exact_key = course_action_exact_key(row)
        # If the user has already started a single-course check for this exact
        # course, remove only that alert while the check is running. Other
        # courses on the same provider/day must remain independently actionable.
        # Use the exact submitted provider/title/start time as a second guard so
        # a stale or reused course id cannot leave the actioned alert visible.
        active_exact_match = (
            active_target_provider
            and active_target_title
            and active_target_date_time
            and provider_slug(row.get('provider') or '') == active_target_provider
            and provider_slug(row.get('title') or '') == active_target_title
            and (row.get('date_time_raw') or '').strip() == active_target_date_time
        )
        if (active_target_course_key and exact_key == active_target_course_key) or active_exact_match or (not active_target_course_key and active_target_course_id and course_id == active_target_course_id):
            continue

        alert_id = 'course-action:' + exact_key
        if alert_id in dismissed:
            continue
        row_status = (row.get('status_label') or '').lower()
        row_message = (row.get('short_message') or '').lower()
        zoom_resolution_alert = bool(row.get('zoom_mismatch_confirmed')) or 'zoom link mismatch confirmed' in row_message
        critical_course_alert = (
            zoom_resolution_alert
            or row_status == 'needs confirmation'
            or bool(row.get('can_confirm_removed'))
            or 'deleted' in row_message
            or 'cancelled' in row_message
            or 'confirm' in row_message
        )
        if not critical_course_alert and row_status in {'not checked', 'not synced', 'sync due'}:
            sync_recommended.append(row)
            continue
        title = 'Course needs checking'
        action = 'target_course'
        if zoom_resolution_alert:
            title = 'Zoom link mismatch confirmed'
            action = 'zoom_resolution'
        elif row_status == 'needs confirmation' or row.get('can_confirm_removed'):
            title = 'Confirm provider cancellation/deletion'
        elif row_status in {'not checked', 'not synced', 'sync due'}:
            title = 'Courses within 12 weeks detected. Sync recommended.'
        message = f"{row.get('provider')} - {row.get('title')} - {row.get('date_label')} {row.get('time_label')}. {row.get('short_message')}"
        alerts.append({
            'id': alert_id,
            'title': title,
            'message': message,
            'level': 'warning',
            'course_id': course_id,
            'course_key': exact_key,
            'provider': row.get('provider') or '',
            'title_text': row.get('title') or '',
            'date_time_raw': row.get('date_time_raw') or '',
            'action': action,
        })
    if sync_recommended and 'course-sync-recommended' not in dismissed:
        providers = sorted({row.get('provider') or 'Provider' for row in sync_recommended})
        days = [days_until_course_row(row) for row in sync_recommended]
        days = [day for day in days if day is not None]
        window_text = '12 weeks' if not days or max(days) > 14 else '2 weeks'
        alerts.insert(0, {
            'id': 'course-sync-recommended',
            'title': f'Courses within {window_text} detected. Sync recommended.',
            'message': f'{len(sync_recommended)} course(s) across {", ".join(providers[:3])} are ready for a normal sync.',
            'level': 'info',
            'course_id': '',
            'course_key': '',
            'provider': 'all',
            'title_text': '',
            'date_time_raw': '',
            'action': 'sync',
        })
    return alerts


def recommended_scan_days_for_rows(rows, active_sync_window_days):
    """Smallest scan window needed for the recommended action."""
    max_needed = 7
    for row in rows or []:
        days = days_until_course_row(row)
        if days is not None:
            max_needed = max(max_needed, days)

    cap = active_sync_window_days or 84
    max_needed = min(max_needed, cap)

    if max_needed <= 7:
        return 7
    if max_needed <= 14:
        return 14
    if max_needed <= 30:
        return 30
    if max_needed <= 60:
        return 60
    return cap

def build_recommendation(courses, providers, state, active_sync_window_days):
    """Return one compact recommendation, capped to the issue date range."""
    health_text = ' '.join(str(x) for x in (state.get('health_issues') or [])) if isinstance(state, dict) else ''
    last_message = (state.get('last_message') or '') if isinstance(state, dict) else ''
    combined = f'{health_text} {last_message}'.lower()

    if 'login' in combined or 'credential' in combined:
        return {'action': 'providers', 'title': 'Provider login needs checking', 'reason': 'TrainerMate can no longer log in to one provider. If the password changed, open Manage providers and reconfirm it.', 'provider': 'all', 'days': min(active_sync_window_days or 84, 14)}
    if 'zoom' in combined and ('token' in combined or 'disconnect' in combined or 'oauth' in combined):
        return {'action': 'zoom', 'title': 'Check Zoom connection', 'reason': 'Zoom access needs attention before syncing will be reliable.', 'provider': 'all', 'days': min(active_sync_window_days or 84, 14)}

    action_rows = [
        c for c in courses
        if c.get('is_action_needed')
        and not c.get('is_outside_window')
        and c.get('status_label') != 'Scheduled for later sync'
    ]

    if not action_rows:
        return {'action': 'none', 'title': 'No action needed', 'reason': f'Courses outside the {sync_window_label(active_sync_window_days)} sync window are scheduled for later and are not alerts.', 'provider': 'all', 'days': min(active_sync_window_days or 84, 14)}

    zoom_resolution_row = next((r for r in action_rows if r.get('zoom_mismatch_confirmed')), None)
    if zoom_resolution_row:
        return {
            'action': 'zoom_resolution',
            'title': 'Resolve Zoom link mismatch',
            'reason': f"{zoom_resolution_row.get('provider')} - {zoom_resolution_row.get('title')} - {zoom_resolution_row.get('date_label')} - {zoom_resolution_row.get('time_label')} has already been checked and needs a decision.",
            'provider': zoom_resolution_row.get('provider_id') or 'all',
            'days': scan_days_for_course_datetime(zoom_resolution_row.get('date_time_raw') or '', active_sync_window_days),
            'course_id': zoom_resolution_row.get('id') or '',
            'course_key': course_action_exact_key(zoom_resolution_row),
            'title_text': zoom_resolution_row.get('title') or '',
            'date_time_raw': zoom_resolution_row.get('date_time_raw') or '',
            'provider_name': zoom_resolution_row.get('provider') or '',
        }

    target_row = best_single_course_check_row(action_rows)
    if target_row:
        return {
            'action': 'target_course',
            'title': f"Check {target_row.get('provider')} course only",
            'reason': f"{target_row.get('title')} - {target_row.get('date_label')} - {target_row.get('time_label')} needs a confirmation check.",
            'provider': target_row.get('provider_id') or 'all',
            'days': scan_days_for_course_datetime(target_row.get('date_time_raw') or '', active_sync_window_days),
            'course_id': target_row.get('id') or '',
            'course_key': course_action_exact_key(target_row),
            'title_text': target_row.get('title') or '',
            'date_time_raw': target_row.get('date_time_raw') or '',
            'provider_name': target_row.get('provider') or '',
        }

    rec_days = recommended_scan_days_for_rows(action_rows, active_sync_window_days)
    rec_label = f'next {rec_days} days' if rec_days else sync_window_label(active_sync_window_days)

    by_provider = {}
    for c in action_rows:
        by_provider.setdefault(c.get('provider_id') or 'all', []).append(c)

    if len(by_provider) == 1:
        provider_id, rows = next(iter(by_provider.items()))
        provider_name = rows[0].get('provider') or 'selected provider'
        return {'action': 'sync', 'title': f'Run {provider_name} sync', 'reason': f'{len(rows)} course(s) need checking within the {rec_label}.', 'provider': provider_id, 'days': rec_days}

    return {'action': 'sync', 'title': 'Run all providers sync', 'reason': f'{len(action_rows)} course(s) need checking across multiple providers within the {rec_label}.', 'provider': 'all', 'days': rec_days}


def dashboard_ready_model(state, courses, providers, zoom_accounts, dashboard_alerts):
    attention_count = sum(1 for row in courses or [] if row.get('is_action_needed'))
    provider_issues = [
        provider for provider in providers or []
        if not provider.get('active', True)
        or not ((provider.get('credentials') or {}).get('username') and (provider.get('credentials') or {}).get('password'))
    ]
    zoom_connected = any((account.get('status') or 'connected').lower() == 'connected' for account in zoom_accounts or [])
    if state.get('sync_running'):
        return {
            'tone': 'running',
            'label': 'Sync running',
            'title': 'TrainerMate is checking your courses',
            'message': state.get('last_message') or 'This can carry on quietly while you work.',
        }
    if dashboard_alerts or attention_count or state.get('health_issues') or provider_issues or not zoom_connected:
        return {
            'tone': 'attention',
            'label': 'Needs attention',
            'title': 'A few things need a look',
            'message': (state.get('health_issues') or [None])[0] or f'{attention_count + len(provider_issues)} item(s) may need a quick check.',
        }
    return {
        'tone': 'ready',
        'label': 'Ready',
        'title': 'Ready for today',
        'message': 'Courses, providers and Zoom look in order.',
    }


def dashboard_provider_health(providers, counts):
    out = []
    for provider in providers or []:
        creds = provider.get('credentials') or {}
        has_credentials = bool(creds.get('username') and creds.get('password'))
        active = bool(provider.get('active', True))
        if not active:
            label = 'Paused'
            tone = 'warn'
        elif provider.get('provider_manages_zoom'):
            label = 'Provider Zoom'
            tone = 'neutral'
        elif has_credentials:
            label = 'Ready'
            tone = 'ok'
        else:
            label = 'Login needed'
            tone = 'warn'
        item = dict(provider)
        item.update({
            'health_label': label,
            'health_tone': tone,
            'course_count': counts.get(provider.get('id'), 0),
        })
        out.append(item)
    return out


def course_rows_for_today(rows):
    today = []
    for row in rows or []:
        if course_days_from_now(row.get('date_time_raw') or '') == 0:
            today.append(row)
    return today


def compact_activity_items(limit=4):
    return [item for item in reversed(load_activity_history()) if not item.get('dismissed_at')][:limit]



@app.route('/login', methods=['GET', 'POST'])
def trainer_login():
    if not reviewer_demo_enabled():
        return redirect(url_for('home'))
    error = ''
    default_email = os.getenv('TRAINERMATE_LOGIN_EMAIL', 'reviewer@zoom.us').strip() or 'reviewer@zoom.us'
    entered_email = default_email
    if request.method == 'POST':
        entered_email = (request.form.get('email') or '').strip().lower()
        supplied = request.form.get('password') or ''
        expected = REVIEWER_PASSWORD or 'zoomreview'
        if entered_email == default_email.lower() and hmac.compare_digest(supplied, expected):
            session['reviewer_demo_ok'] = True
            return redirect(request.args.get('next') or url_for('home'))
        error = 'The email or password was not accepted.'
    return render_template_string("""
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>TrainerMate Login</title>
<style>body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:#0b1220;color:#e5eefb;min-height:100vh;display:grid;place-items:center;padding:24px}.card{width:min(520px,100%);background:#111827;border:1px solid rgba(96,165,250,.35);border-radius:24px;padding:28px;box-shadow:0 24px 80px rgba(0,0,0,.35)}h1{margin:0 0 8px;font-size:28px}.muted{color:#a9b7cc;line-height:1.5;margin:0 0 22px}.field{display:grid;gap:8px;margin-bottom:16px}label{font-weight:800}input{border:1px solid #334155;background:#0f172a;color:white;border-radius:12px;padding:12px;font-size:16px}.btn{width:100%;border:0;border-radius:12px;background:#2563eb;color:white;font-weight:900;padding:12px 16px;cursor:pointer;font-size:16px}.err{color:#fecaca;background:rgba(220,38,38,.18);border:1px solid rgba(248,113,113,.35);border-radius:12px;padding:10px;margin-bottom:14px}</style></head>
<body><main class="card"><h1>TrainerMate</h1><p class="muted">Sign in to open your TrainerMate dashboard.</p>{% if error %}<div class="err">{{ error }}</div>{% endif %}<form method="post">{{ csrf_hidden_field()|safe }}<div class="field"><label>Email</label><input type="email" name="email" value="{{ entered_email }}" autocomplete="username" autofocus required></div><div class="field"><label>Password</label><input type="password" name="password" autocomplete="current-password" required></div><button class="btn" type="submit">Sign in</button></form></main></body></html>
    """, error=error, entered_email=entered_email, csrf_hidden_field=csrf_hidden_field)
@app.route('/logout', methods=['POST'])
def trainer_logout():
    session.pop('reviewer_demo_ok', None)
    return redirect(url_for('trainer_login'))

@app.route('/health')
def health_check():
    return 'OK', 200

@app.route('/')
@app.route('/dashboard')
def home():
    state = reconcile_running_state()
    access = check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True)
    identity = get_identity()
    sync_provider_login_failures_from_state()
    providers = load_providers()
    zoom_accounts = load_zoom_accounts()
    start_startup_zoom_health_check_once(zoom_accounts)
    counts = {}
    selected_provider = provider_slug(request.args.get('provider', 'all'))
    if selected_provider == 'provider':
        selected_provider = 'all'
    current_section = (request.args.get('section') or 'dashboard').strip().lower()
    if current_section not in {'dashboard', 'setup', 'manage_providers', 'zoom_accounts', 'automation', 'activity', 'calendar', 'files'}:
        current_section = 'dashboard'
    if current_section == 'dashboard' and not providers:
        current_section = 'setup'
    if current_section == 'dashboard':
        start_startup_certificate_scan_once(providers)
    clear_false_zoom_mismatch_flags()
    raw_courses = load_courses(selected_provider)
    repair_pending_upload_presence()
    trainer_documents = load_documents()
    doc_summary = document_summary(trainer_documents)
    expiry_warnings = document_expiry_warnings(trainer_documents)
    document_provider_health = provider_document_health(trainer_documents, providers)
    provider_certificates_by_provider = load_provider_certificates()
    certificate_match_overview = build_certificate_match_overview(trainer_documents, providers, provider_certificates_by_provider)
    certificate_attention = certificate_attention_items(trainer_documents)
    certificate_alerts_visible = [] if certificate_job_running() else certificate_attention
    alert_dismissed = load_alert_ack()
    dashboard_alerts = []
    providers_by_slug = {p['id']: p for p in providers}
    is_paid_account = account_is_paid(access)
    calendar_sync_allowed = feature_enabled(access, 'calendar_sync')
    certificate_manage_allowed = feature_enabled(access, 'certificate_manage')
    automatic_sync_allowed_value = feature_enabled(access, 'automatic_sync')
    active_sync_window_days = effective_sync_window_days(access)
    filtered_courses = suppress_stale_same_provider_slot_duplicates(build_course_rows(raw_courses, state, providers_by_slug, active_sync_window_days=active_sync_window_days, is_free_account=not is_paid_account))
    counts = course_counts_from_rows(filtered_courses)
    for provider in providers:
        provider['course_count'] = counts.get(provider['id'], 0)
    dashboard_alerts = build_course_action_alerts(filtered_courses, alert_dismissed) + build_dashboard_alerts(filtered_courses, alert_dismissed)
    recommendation = build_recommendation(filtered_courses, providers, state, active_sync_window_days)
    total_courses = sum(counts.values())
    today_courses = course_rows_for_today(filtered_courses)
    next_courses = filtered_courses[:5]
    provider_health = dashboard_provider_health(providers, counts)
    dashboard_ready = dashboard_ready_model(state, filtered_courses, providers, zoom_accounts, dashboard_alerts)
    dashboard_activity_items = compact_activity_items(4)
    mini_calendar_days = []
    today_date = datetime.now().date()
    for offset in range(7):
        day_date = today_date + timedelta(days=offset)
        events = []
        for row in filtered_courses:
            parsed = parse_dashboard_datetime(row.get('date_time_raw') or '')
            if parsed and parsed.date() == day_date:
                events.append(row)
        mini_calendar_days.append({'label': day_date.strftime('%a %d'), 'events': events[:4]})
    selected_provider_name = next((p['name'] for p in providers if p['id'] == selected_provider), 'All providers') if selected_provider != 'all' else 'All providers'
    status_message = 'Everything looks in order.'
    if state.get('sync_running'):
        status_message = 'TrainerMate is checking courses and Zoom details.'
    elif state.get('health_issues'):
        status_message = shorten_message((state.get('health_issues') or [''])[0], 120)
    elif state.get('last_message'):
        status_message = shorten_message(state.get('last_message') or '', 120)

    debug_current_provider = state.get('current_provider') or selected_provider_name or '-'
    debug_current_course = state.get('current_course') or '-'
    debug_latest_message = state.get('last_message') or state.get('last_status') or status_message or 'Idle'
    initial_debug_log = '\n'.join(tail_bot_log(80)) or 'No debug output yet.'
    provider_form = make_provider_defaults(request.args.get('provider_name', ''), request.args.get('login_url', ''), True)
    if request.args.get('zoom_account_id') is not None:
        provider_form['zoom_account_id'] = request.args.get('zoom_account_id')
    if request.args.get('provider_color') is not None:
        provider_form['color'] = normalize_hex_color(request.args.get('provider_color')) or provider_form.get('color')
    for field in ('supports_custom_time', 'provider_manages_zoom', 'never_overwrite_existing_zoom'):
        if request.args.get(field) is not None:
            provider_form[field] = request.args.get(field) == '1'
    add_document_form = session.pop('add_document_form', {})
    return render_template_string(
        TEMPLATE,
        access=access,
        account_plan_label=('Paid' if is_paid_account else 'Free'),
        is_paid_account=is_paid_account,
        calendar_sync_allowed=calendar_sync_allowed,
        certificate_manage_allowed=certificate_manage_allowed,
        show_locked_section_modal=(current_section in {'calendar', 'automation'} and ((current_section == 'calendar' and not calendar_sync_allowed) or (current_section == 'automation' and not automatic_sync_allowed_value))),
        locked_section_title=('Calendar is included with TrainerMate Paid' if current_section == 'calendar' else 'Automatic Sync is included with TrainerMate Paid'),
        locked_section_message=('Calendar tools unlock when your paid plan is active.' if current_section == 'calendar' else 'Automatic Sync can run quiet checks in the background when your paid plan is active.'),
        active_sync_window_days=active_sync_window_days,
        active_sync_window_label=sync_window_label(active_sync_window_days),
        recommendation=recommendation,
        dashboard_alerts=dashboard_alerts,
        identity=identity,
        masked_email=mask_email(identity.get('email', '')),
        state=state,
        providers=providers,
        zoom_accounts=zoom_accounts,
        selected_provider=selected_provider,
        selected_provider_name=selected_provider_name,
        total_courses=total_courses,
        filtered_courses=filtered_courses,
        today_courses=today_courses,
        next_courses=next_courses,
        provider_health=provider_health,
        dashboard_ready=dashboard_ready,
        dashboard_activity_items=dashboard_activity_items,
        mini_calendar_days=mini_calendar_days,
        synced_count=sum(1 for c in filtered_courses if c['status_label'] == 'Synced'),
        attention_count=sum(1 for c in filtered_courses if c.get('is_action_needed')),
        status_dot_class=get_status_dot_class(state),
        friendly_status=build_friendly_status(state),
        status_message=status_message,
        debug_current_provider=debug_current_provider,
        debug_current_course=debug_current_course,
        debug_latest_message=debug_latest_message,
        initial_debug_log=initial_debug_log,
        last_sync_text=format_last_sync(state),
        flash=get_flash(),
        auto_refresh=False,
        current_section=current_section,
        provider_form=provider_form,
        add_document_form=add_document_form,
        provider_presets_json=json.dumps(provider_presets_for_ui()),
        provider_catalogue_options=provider_catalogue_options(),
        setup_provider_rows=setup_provider_rows(providers),
        trainer_documents=trainer_documents,
        document_summary=doc_summary,
        document_expiry_warnings=expiry_warnings,
        document_provider_health=document_provider_health,
        provider_certificates_by_provider=provider_certificates_by_provider,
        certificate_match_overview=certificate_match_overview,
        automation_settings=load_automation_settings(),
        automatic_sync_allowed=automatic_sync_allowed_value,
        activity_items=[item for item in reversed(load_activity_history()) if not item.get('dismissed_at')],
        activity_counts=activity_counts(),
        certificate_attention_items=certificate_attention,
        certificate_alerts_visible=certificate_alerts_visible,
        document_types=DOCUMENT_TYPES,
        ZOOM_CLIENT_ID=ZOOM_CLIENT_ID,
        ZOOM_CLIENT_SECRET=ZOOM_CLIENT_SECRET,
        ZOOM_REDIRECT_URI=ZOOM_REDIRECT_URI,
        csrf_hidden_field=csrf_hidden_field,
        csrf_token=csrf_token,
        course_action_exact_key=course_action_exact_key,
        build_label=BUILD_LABEL,
        build_name=BUILD_NAME,
        dashboard_canonical_url=DASHBOARD_CANONICAL_URL,
    )



@app.route('/activity')
def activity_centre():
    items = [item for item in reversed(load_activity_history()) if not item.get('dismissed_at')]
    counts = activity_counts(list(reversed(items)))
    return render_template_string("""
<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>TrainerMate Activity</title>
<style>:root{color-scheme:dark;--bg:#081225;--panel:#111827;--line:rgba(148,163,184,.18);--text:#f8fafc;--muted:#9ca3af;--blue:#2563eb;--warn:#f59e0b;--red:#ef4444}body{margin:0;background:linear-gradient(180deg,#081225,#0c1830);color:var(--text);font-family:Inter,Segoe UI,Arial,sans-serif}.wrap{max-width:980px;margin:0 auto;padding:24px}.top{display:flex;justify-content:space-between;gap:14px;align-items:flex-start;margin-bottom:18px}.btn{border:0;border-radius:12px;padding:10px 14px;background:var(--blue);color:#fff;font-weight:800;text-decoration:none;cursor:pointer}.btn.soft{background:#1f2937;border:1px solid var(--line)}.card{border:1px solid var(--line);border-radius:18px;background:rgba(17,24,39,.92);padding:16px;margin-bottom:12px}.card.unread{border-color:rgba(56,189,248,.42)}.meta{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.06em}.title{font-weight:900;font-size:18px;margin:5px 0}.summary{color:#dbeafe;line-height:1.45}.details{margin-top:12px;border-top:1px solid var(--line);padding-top:12px;color:#cbd5e1}.detail-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px}.tile{border:1px solid var(--line);border-radius:12px;padding:10px;background:#0f172a}.tile strong{display:block;font-size:20px}.course{border-top:1px solid var(--line);padding:10px 0}.course:first-child{border-top:0}.actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}.empty{color:var(--muted);border:1px solid var(--line);border-radius:18px;padding:24px;background:#111827}@media(max-width:720px){.detail-grid{grid-template-columns:1fr 1fr}.top{display:block}}</style></head><body><main class="wrap"><div class="top"><div><h1>Messages & Activity</h1><p style="color:var(--muted)">Support messages, automatic sync results, and things that need attention.</p><p><span>{{ counts.unread }} unread</span> - <span>{{ counts.total }} active</span></p></div><a class="btn soft" href="{{ url_for('home') }}">Back to dashboard</a></div>{% if items %}{% for item in items %}<article class="card {% if not item.read_at %}unread{% endif %}"><div class="meta">{{ item.created_at }} - {{ item.type|replace('_',' ') }}{% if not item.read_at %} - New{% endif %}</div><div class="title">{{ item.title }}</div><div class="summary">{{ item.summary or item.message }}</div>{% if item.details %}<div class="details"><div class="detail-grid"><div class="tile"><span>Checked</span><strong>{{ item.details.courses_checked or 0 }}</strong></div><div class="tile"><span>New</span><strong>{{ item.details.new_courses or 0 }}</strong></div><div class="tile"><span>Updated</span><strong>{{ item.details.course_updates or 0 }}</strong></div><div class="tile"><span>Zoom/FOBS</span><strong>{{ item.details.zoom_or_fobs_updates or 0 }}</strong></div></div></div>{% endif %}{% if item.items %}<div class="details"><strong>Course detail</strong>{% for c in item.items[:20] %}<div class="course"><strong>{{ c.provider or 'Provider' }}</strong> - {{ c.date_time or '' }}<br>{{ c.course_type or '' }}<br><span style="color:#9ca3af">{{ c.action or c.status or c.error }}</span></div>{% endfor %}</div>{% endif %}<div class="actions">{% if not item.read_at %}<form method="post" action="{{ url_for('read_activity_route', activity_id=item.id) }}"><button class="btn soft" type="submit">Mark read</button></form>{% endif %}<form method="post" action="{{ url_for('dismiss_activity_route', activity_id=item.id) }}"><button class="btn soft" type="submit">Dismiss</button></form></div></article>{% endfor %}{% else %}<div class="empty">No messages or activity yet.</div>{% endif %}</main></body></html>""", items=items, counts=counts, csrf_hidden_field=csrf_hidden_field)


@app.route('/messages')
def messages_alias():
    return redirect(url_for('activity_centre'))


@app.route('/api/activity')
def api_activity():
    items = [item for item in load_activity_history() if not item.get('dismissed_at')]
    popup = latest_popup_activity()
    return jsonify({'ok': True, 'counts': activity_counts(items), 'items': list(reversed(items[-50:])), 'popup': popup})


@app.route('/activity/<activity_id>/read', methods=['POST'])
def read_activity_route(activity_id):
    mark_activity_read(activity_id)
    return redirect(request.referrer or url_for('activity_centre'))


@app.route('/activity/<activity_id>/dismiss', methods=['POST'])
def dismiss_activity_route(activity_id):
    dismiss_activity(activity_id)
    return redirect(request.referrer or url_for('activity_centre'))


@app.route('/automation/save', methods=['POST'])
def save_automation_route():
    access = check_access(timeout_seconds=ACTION_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}
    paid_auto = feature_enabled(access, 'automatic_sync')
    requested_enabled = bool(request.form.get('enabled'))
    settings = {
        'daily_enabled': bool(request.form.get('daily_enabled')),
        'daily_time': (request.form.get('daily_time') or '06:00').strip() or '06:00',
        'daily_days': int(request.form.get('daily_days') or 14),
        'weekly_enabled': bool(request.form.get('weekly_enabled')),
        'weekly_day': (request.form.get('weekly_day') or 'sunday').strip().lower(),
        'weekly_time': (request.form.get('weekly_time') or '06:30').strip() or '06:30',
        'notifications_enabled': bool(request.form.get('notifications_enabled')),
        'notify_problems': bool(request.form.get('notify_problems')),
        'notify_course_changes': bool(request.form.get('notify_course_changes')),
        'notify_success_no_changes': bool(request.form.get('notify_success_no_changes')),
        'notify_support_messages': bool(request.form.get('notify_support_messages')),
        'popup_bubbles': bool(request.form.get('popup_bubbles')),
    }
    if requested_enabled and paid_auto:
        settings['enabled'] = True
        settings['enable_when_paid'] = False
        set_flash('Automatic Sync is enabled. TrainerMate will run quiet checks using these settings.', 'success')
    elif requested_enabled and not paid_auto:
        settings['enabled'] = False
        settings['enable_when_paid'] = bool(request.form.get('enable_when_paid'))
        set_flash('Automatic Sync is included with TrainerMate Paid. Nothing has been enabled on Free.', 'warning')
    else:
        settings['enabled'] = False
        settings['enable_when_paid'] = False
        set_flash('Automatic Sync settings saved.', 'success')
    save_automation_settings(settings)
    return redirect(url_for('home', section='automation'))


@app.route('/automation/run-now', methods=['POST'])
def automation_run_now():
    access = check_access(timeout_seconds=ACTION_ACCESS_TIMEOUT_SECONDS, prefer_cached=False) or {}
    if not feature_enabled(access, 'automatic_sync'):
        set_flash('Automatic Sync is included with TrainerMate Paid. Nothing has been enabled on Free.', 'warning')
        return redirect(url_for('home', section='automation'))
    settings = load_automation_settings()
    ok, message = start_sync_process(scan_provider='all', scan_days=settings.get('daily_days') or 14, scan_scope='smart', bot_mode='urgent_14d', source='auto_manual')
    set_flash(message, 'success' if ok else 'warning')
    return redirect(url_for('home', section='automation'))

@app.route('/alerts/dismiss', methods=['POST'])
def dismiss_alert():
    alert_id = (request.form.get('alert_id') or '').strip()
    dismissed = load_alert_ack()
    if alert_id:
        dismissed.add(alert_id)
        save_alert_ack(dismissed)
    target = request.referrer or url_for('home', section='dashboard')
    host_url = request.host_url or ''
    if host_url and target.startswith(host_url):
        parsed = urlparse(target)
        target = parsed.path + (('?' + parsed.query) if parsed.query else '')
    if not str(target).startswith('/'):
        target = url_for('home', section='dashboard')
    return redirect(target)


@app.route('/setup/providers', methods=['POST'])
def setup_providers():
    ok, message = save_setup_providers(request.form)
    set_flash(message, 'success' if ok else 'warning')
    return redirect(url_for('home', section='manage_providers' if ok else 'setup'))


@app.route('/providers/add', methods=['POST'])
def add_provider():
    ok, message, provider = add_provider_record(request.form)
    if ok and provider:
        test_ok, test_message = test_provider_login_once(
            provider,
            request.form.get('provider_username') or '',
            request.form.get('provider_password') or '',
        )
        if test_ok:
            providers = load_providers()
            providers.append(provider)
            save_providers(providers)
            save_provider_credentials(provider.get('id') or '', request.form.get('provider_username') or '', request.form.get('provider_password') or '')
            update_provider_login_test_status(provider.get('id') or '', 'ok', test_message)
            message = f'{provider.get("name") or "Provider"} added. Login checked successfully.'
        else:
            ok = False
            message = (
                f'TrainerMate could not log in to {provider.get("name") or "this provider"}. '
                'Please check the username and password. Repeated failed FOBS logins may lock the provider account.'
            )
    set_flash(message, 'success' if ok else 'warning')
    if ok:
        return redirect(url_for('home', section='dashboard', provider=provider_slug(request.form.get('provider_name') or '')))
    return redirect(url_for('home', section='manage_providers', provider_name=request.form.get('provider_name') or '', login_url=request.form.get('login_url') or '', provider_color=request.form.get('provider_color') or '', supports_custom_time='1' if request.form.get('supports_custom_time') else '', provider_manages_zoom='1' if request.form.get('provider_manages_zoom') else '', zoom_account_id=request.form.get('zoom_account_id') or ''))


@app.route('/providers/update/<provider_id>', methods=['POST'])
def update_provider(provider_id):
    ok, message = update_provider_record(provider_id, request.form)
    set_flash(message, 'success' if ok else 'error')
    return redirect(url_for('home', section='manage_providers'))


@app.route('/providers/test-login/<provider_id>', methods=['POST'])
def test_provider_login(provider_id):
    providers = load_providers()
    provider = next((p for p in providers if p.get('id') == provider_id), None)
    if not provider:
        set_flash('Provider not found.', 'warning')
        return redirect(url_for('home', section='manage_providers'))
    update_provider_credentials_if_supplied(provider_id, request.form.get('provider_username') or '', request.form.get('provider_password') or '')
    if request.form.get('login_url'):
        provider = dict(provider)
        provider['login_url'] = (request.form.get('login_url') or provider.get('login_url') or '').strip()
    creds = get_provider_credentials(provider_id)
    ok, message = test_provider_login_once(provider, creds.get('username') or '', creds.get('password') or '')
    update_provider_login_test_status(provider_id, 'ok' if ok else 'failed', message)
    set_flash(message, 'success' if ok else 'warning')
    return redirect(url_for('home', section='manage_providers'))


@app.route('/providers/delete/<provider_id>', methods=['POST'])
def delete_provider(provider_id):
    ok, message = delete_provider_record(provider_id)
    set_flash(message, 'success' if ok else 'error')
    return redirect(url_for('home', section='manage_providers'))


@app.route('/zoom/oauth-config', methods=['POST'])
def zoom_oauth_config_save():
    global ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET, ZOOM_REDIRECT_URI
    client_id = (request.form.get('client_id') or '').strip()
    client_secret = (request.form.get('client_secret') or '').strip()
    redirect_uri = ZOOM_APPROVED_RELAY_URI
    if not client_id or not client_secret:
        set_flash('Enter both Zoom Client ID and Zoom Client Secret.', 'warning')
        return redirect(url_for('home', section='zoom_accounts'))
    save_zoom_oauth_config(client_id, client_secret, redirect_uri)
    ZOOM_CLIENT_ID = client_id
    ZOOM_CLIENT_SECRET = client_secret
    ZOOM_REDIRECT_URI = redirect_uri
    set_flash('Zoom OAuth setup saved. You can now connect another Zoom account.', 'success')
    return redirect(url_for('home', section='zoom_accounts'))


@app.route('/zoom/connect/start', methods=['GET', 'POST'])
def zoom_connect_start():
    if not (ZOOM_CLIENT_ID and ZOOM_CLIENT_SECRET):
        set_flash('Add ZOOM_CLIENT_ID and ZOOM_CLIENT_SECRET before testing Zoom connect.', 'warning')
        return redirect(url_for('home', section='zoom_accounts'))
    session['zoom_oauth_state'] = ZOOM_RELAY_STATE_PREFIX + secrets.token_urlsafe(24)
    if request.method == 'POST':
        session['zoom_oauth_nickname'] = (request.form.get('zoom_nickname') or '').strip()
    else:
        session['zoom_oauth_nickname'] = (request.args.get('zoom_nickname') or '').strip()
    return redirect(zoom_auth_url(session['zoom_oauth_state']))


@app.route('/zoom/callback')
def zoom_callback():
    state_token = (request.args.get('state') or '').strip()
    expected_state = session.get('zoom_oauth_state') or ''
    if not state_token or state_token != expected_state or not state_token.startswith(ZOOM_RELAY_STATE_PREFIX):
        set_flash('Zoom connection could not be verified.', 'error')
        return redirect(url_for('home', section='zoom_accounts'))
    code = (request.args.get('code') or '').strip()
    if not code:
        set_flash('Zoom did not return an authorization code.', 'error')
        return redirect(url_for('home', section='zoom_accounts'))
    try:
        token = requests.post(
            'https://zoom.us/oauth/token',
            params={'grant_type': 'authorization_code', 'code': code, 'redirect_uri': zoom_redirect_uri()},
            auth=(ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET),
            timeout=20,
        )
        token.raise_for_status()
        token_data = token.json()
        access_token = (token_data.get('access_token') or '').strip()
        refresh_token = (token_data.get('refresh_token') or '').strip()
        me = requests.get(
            'https://api.zoom.us/v2/users/me',
            headers={'Authorization': f'Bearer {access_token}'},
            timeout=20,
        )
        me.raise_for_status()
        me_data = me.json()
        email = (me_data.get('email') or '').strip()
        nickname = session.get('zoom_oauth_nickname') or email or (me_data.get('display_name') or 'Zoom account')
        upsert_zoom_account(email, nickname, access_token, refresh_token)
        set_flash(f'Zoom connected for {email or nickname}.', 'success')
    except requests.HTTPError as exc:
        response_text = ''
        try:
            response_text = exc.response.text[:400]
        except Exception:
            response_text = ''
        detail = f'{exc}' + (f' | {response_text}' if response_text else '')
        set_flash(f'Zoom connection failed: {detail}', 'error')
    except Exception as exc:
        set_flash(f'Zoom connection failed: {exc}', 'error')
    session.pop('zoom_oauth_state', None)
    session.pop('zoom_oauth_nickname', None)
    return redirect(url_for('home', section='zoom_accounts'))


@app.route('/zoom/set-default/<account_id>', methods=['POST'])
def zoom_set_default(account_id):
    accounts = load_zoom_accounts()
    found = False
    for account in accounts:
        account['is_default'] = account['id'] == account_id
        if account['id'] == account_id:
            found = True
    if found:
        save_zoom_accounts(accounts)
        set_flash('Default Zoom account updated.', 'success')
    else:
        set_flash('Zoom account not found.', 'error')
    return redirect(url_for('home', section='zoom_accounts'))


@app.route('/zoom/disconnect/<account_id>', methods=['POST'])
def zoom_disconnect(account_id):
    accounts = [a for a in load_zoom_accounts() if a['id'] != account_id]
    save_zoom_accounts(accounts)
    clear_zoom_tokens(account_id)
    providers = load_providers()
    changed = False
    for provider in providers:
        if provider.get('zoom_account_id') == account_id:
            provider['zoom_account_id'] = ''
            changed = True
    if changed:
        save_providers(providers)
    set_flash('Zoom account disconnected.', 'success')
    return redirect(url_for('home', section='zoom_accounts'))


@app.route('/documents/<document_id>/view')
def view_document(document_id):
    def unavailable(message):
        return render_template_string("""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Certificate unavailable</title>
  <style>
    body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:#0b1220;color:#f8fafc;display:grid;place-items:center;min-height:100vh;padding:20px}
    .card{max-width:520px;border:1px solid rgba(125,211,252,.26);border-radius:18px;background:#111827;padding:22px;box-shadow:0 18px 45px rgba(0,0,0,.35)}
    h1{font-size:22px;margin:0 0 8px}p{color:#cbd5e1;line-height:1.45;margin:0 0 16px}.btn{display:inline-flex;text-decoration:none;color:#fff;background:#2563eb;border-radius:12px;padding:10px 14px;font-weight:800}
  </style>
</head>
<body>
  <div class="card">
    <h1>Certificate file unavailable</h1>
    <p>{{ message }}</p>
    <a class="btn" href="{{ url_for('home', section='files') }}">Back to certificates</a>
  </div>
</body>
</html>
        """, message=message), 200

    doc = document_row(document_id)
    if not doc:
        return unavailable('TrainerMate could not find this certificate record. It may have already been deleted.')
    stored = (doc.get('stored_filename') or '').strip()
    if not stored:
        return unavailable('TrainerMate has the certificate details, but no saved file is attached.')
    try:
        path = safe_document_path(stored)
    except Exception:
        return unavailable('TrainerMate could not safely open this saved file.')
    if not path or not path.exists() or not path.is_file():
        return unavailable('The saved certificate file is missing from this computer. Add the certificate again if you still need TrainerMate to send it to providers.')
    download_name = safe_provider_document_filename(
        doc.get('title') or doc.get('original_filename') or 'document',
        doc.get('original_filename') or stored,
    )
    response = send_file(str(path), as_attachment=False, download_name=download_name, conditional=False)
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route('/provider-certificates/<certificate_id>/view')
def view_provider_certificate_file(certificate_id):
    def unavailable(message):
        return render_template_string("""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Certificate unavailable</title>
  <style>
    body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:#0b1220;color:#f8fafc;display:grid;place-items:center;min-height:100vh;padding:20px}
    .card{max-width:520px;border:1px solid rgba(125,211,252,.26);border-radius:18px;background:#111827;padding:22px;box-shadow:0 18px 45px rgba(0,0,0,.35)}
    h1{font-size:22px;margin:0 0 8px}p{color:#cbd5e1;line-height:1.45;margin:0 0 16px}.btn{display:inline-flex;text-decoration:none;color:#fff;background:#2563eb;border-radius:12px;padding:10px 14px;font-weight:800}
  </style>
</head>
<body>
  <div class="card">
    <h1>Certificate file unavailable</h1>
    <p>{{ message }}</p>
    <a class="btn" href="{{ url_for('home', section='files') }}">Back to certificates</a>
  </div>
</body>
</html>
        """, message=message), 200

    conn = documents_conn()
    try:
        cert = conn.execute(
            'SELECT * FROM provider_certificates WHERE id = ?',
            (certificate_id,),
        ).fetchone()
        cert = dict(cert) if cert else None
    finally:
        conn.close()
    if not cert or not (cert.get('cached_filename') or '').strip():
        return unavailable("This provider certificate is no longer in TrainerMate's current FOBS list, or the local copy has not been saved yet.")
    if (cert.get('status') or 'seen') != 'seen':
        return unavailable("This certificate no longer appears in the current FOBS certificate list. Refresh the provider if you think it should still be there.")
    if (cert.get('download_status') or '') != PROVIDER_CACHE_VERSION:
        return unavailable("TrainerMate needs to refresh this provider file before opening it. Refresh this provider's FOBS certificates to save a fresh copy.")
    try:
        path = safe_provider_cache_path(cert.get('cached_filename'))
    except Exception:
        path = None
    if not path or not path.exists() or not path.is_file():
        return unavailable("TrainerMate can still see the certificate record, but the saved local file is missing. Refresh this provider's FOBS certificates to save a fresh copy.")
    download_name = safe_provider_document_filename(
        cert.get('certificate_name') or 'provider certificate',
        cert.get('cached_filename') or 'certificate.pdf',
    )
    if (cert.get('encryption') or '').strip().lower() == 'dpapi':
        try:
            content = unprotect_provider_cache_bytes(path.read_bytes())
        except Exception:
            return unavailable("TrainerMate could not open the saved local copy. Refresh this provider's FOBS certificates to save a fresh copy.")
        response = Response(content, mimetype=(cert.get('content_type') or 'application/octet-stream'))
        response.headers['Content-Disposition'] = f'inline; filename="{download_name}"'
    else:
        response = send_file(
            str(path),
            as_attachment=False,
            download_name=download_name,
            mimetype=(cert.get('content_type') or None),
            conditional=False,
        )
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.post('/provider-certificates/<certificate_id>/delete')
def delete_provider_certificate_route(certificate_id):
    if not feature_enabled(check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}, 'certificate_manage'):
        msg = paid_feature_message('Certificate management')
        if request.headers.get('Accept') == 'application/json' or request.headers.get('X-Requested-With') == 'fetch':
            return jsonify({'started': False, 'message': msg})
        set_flash(msg, 'warning')
        return redirect(url_for('home', section='files'))
    started, message = start_provider_certificate_delete_async(certificate_id)
    if request.headers.get('Accept') == 'application/json' or request.headers.get('X-Requested-With') == 'fetch':
        return jsonify({'started': started, 'message': message})
    set_flash(message, 'success' if started else 'warning')
    return redirect(url_for('home', section='files'))


@app.post('/provider-certificates/<certificate_id>/delete/cancel')
def cancel_provider_certificate_delete_route(certificate_id):
    if not feature_enabled(check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}, 'certificate_manage'):
        msg = paid_feature_message('Certificate management')
        if request.headers.get('Accept') == 'application/json' or request.headers.get('X-Requested-With') == 'fetch':
            return jsonify({'ok': False, 'message': msg})
        set_flash(msg, 'warning')
        return redirect(url_for('home', section='files'))
    ok = request_provider_delete_cancel(certificate_id)
    message = 'Cancel requested. If FOBS has not removed it yet, TrainerMate will stop.'
    if request.headers.get('Accept') == 'application/json' or request.headers.get('X-Requested-With') == 'fetch':
        return jsonify({'ok': ok, 'message': message})
    set_flash(message if ok else 'Could not cancel this removal.', 'success' if ok else 'warning')
    return redirect(url_for('home', section='files'))


@app.post('/certificates/scan/<provider_id>')
def scan_provider_certificates_route(provider_id):
    if not feature_enabled(check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}, 'certificate_manage'):
        set_flash(paid_feature_message('Certificate management'), 'warning')
        return redirect(url_for('home', section='files'))
    started = start_certificate_scan_async(provider_id)
    set_flash('Refreshing FOBS certificates.' if started else 'TrainerMate is already checking certificates. This will only take a moment.', 'success' if started else 'warning')
    return redirect(url_for('home', section='files'))


@app.post('/certificates/scan-all')
def scan_all_certificates():
    if not feature_enabled(check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}, 'certificate_manage'):
        set_flash(paid_feature_message('Certificate management'), 'warning')
        return redirect(url_for('home', section='files'))
    started = start_certificate_scan_async('all')
    set_flash('Refreshing FOBS certificates.' if started else 'TrainerMate is already checking certificates. This will only take a moment.', 'success' if started else 'warning')
    return redirect(url_for('home', section='files'))


@app.route('/documents/add', methods=['POST'])
def add_document():
    if not feature_enabled(check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}, 'certificate_manage'):
        set_flash(paid_feature_message('Certificate management'), 'warning')
        return redirect(url_for('home', section='files'))
    ok, message = add_document_from_form(request.form, request.files.get('document_file'))
    if not ok:
        remember_add_document_form(request.form)
        set_flash(message, 'warning')
        return redirect(url_for('home', section='files'))
    provider_ids = valid_document_provider_ids(request.form.getlist('provider_ids'))
    upload_state = ensure_provider_upload_runs_soon() if provider_ids else 'none'
    if provider_ids and upload_state == 'started':
        message = 'Certificate added. TrainerMate is sending it to the selected provider portals now.'
    elif provider_ids and upload_state == 'queued':
        message = 'Certificate added. TrainerMate will send it to the selected provider portals as soon as the current check finishes.'
    set_flash(message, 'success' if ok else 'warning')
    return redirect(url_for('home', section='files'))


@app.route('/documents/<document_id>/archive', methods=['POST'])
def archive_document_route(document_id):
    if not feature_enabled(check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}, 'certificate_manage'):
        set_flash(paid_feature_message('Certificate management'), 'warning')
        return redirect(url_for('home', section='files'))
    ok = archive_document(document_id)
    set_flash(
        'Certificate deleted from TrainerMate. FOBS copies are not deleted yet.' if ok else 'Certificate not found.',
        'success' if ok else 'warning',
    )
    return redirect(url_for('home', section='files'))


@app.post('/documents/link/<link_id>/reupload')
def reupload_certificate_link(link_id):
    if not feature_enabled(check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}, 'certificate_manage'):
        set_flash(paid_feature_message('Certificate management'), 'warning')
        return redirect(url_for('home', section='files'))
    ok = stage_document_link_reupload(link_id)
    if ok:
        upload_state = ensure_provider_upload_runs_soon()
        started = upload_state == 'started'
    else:
        started = False
        upload_state = 'none'
    set_flash('Sending certificate to provider.' if started else ('TrainerMate will send this as soon as the current check finishes.' if upload_state == 'queued' else ('TrainerMate is already checking certificates. This will only take a moment.' if ok else 'Certificate link not found.')), 'success' if ok else 'warning')
    return redirect(url_for('home', section='files'))


@app.post('/documents/link/<link_id>/dismiss-missing')
def dismiss_certificate_prompt(link_id):
    alert_ids = [str(value or '').strip() for value in request.form.getlist('alert_id')]
    alert_ids = [value for value in alert_ids if value]
    dismissed = load_alert_ack()
    if alert_ids:
        for alert_id in alert_ids:
            dismissed.add(alert_id)
        save_alert_ack(dismissed)
        ok = True
    else:
        # Backwards-compatible fallback for old pages/forms.
        ok = dismiss_missing_certificate_link(link_id)
    set_flash('Certificate reminder dismissed.' if ok else 'Certificate link not found.', 'success' if ok else 'warning')
    return redirect(url_for('home', section='files'))


@app.post('/documents/auto-dismiss-missing')
def auto_dismiss_missing_certificate_prompts():
    dismissed_count = 0
    dismissed = load_alert_ack()
    for alert_id in request.form.getlist('alert_ids'):
        alert_id = str(alert_id or '').strip()
        if alert_id and alert_id not in dismissed:
            dismissed.add(alert_id)
            dismissed_count += 1
    if dismissed_count:
        save_alert_ack(dismissed)
    if request.headers.get('Accept') == 'application/json' or request.headers.get('X-Requested-With') == 'fetch':
        return jsonify({'dismissed': dismissed_count})
    return ('', 204)


@app.post('/documents/provider-delete')
def stage_provider_certificate_delete():
    if not feature_enabled(check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}, 'certificate_manage'):
        set_flash(paid_feature_message('Certificate management'), 'warning')
        return redirect(url_for('home', section='files'))
    staged = stage_document_links_for_provider_delete(request.form.getlist('link_ids'))
    started = False
    if staged:
        dismiss_missing_certificate_link(request.form.get('source_link_id') or '')
        started = start_provider_delete_async()
    set_flash(
        f'{staged} provider removal request(s) started.' if started else ('TrainerMate is already checking certificates. This will only take a moment.' if staged else 'No matching provider copies were selected.'),
        'success' if started else 'warning',
    )
    return redirect(url_for('home', section='files'))


@app.post('/documents/<document_id>/archive-soft')
def archive_certificate_document(document_id):
    if not feature_enabled(check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}, 'certificate_manage'):
        set_flash(paid_feature_message('Certificate management'), 'warning')
        return redirect(url_for('home', section='files'))
    ok = set_document_status(document_id, 'archived')
    set_flash('Certificate archived in TrainerMate. FOBS copies are unchanged.' if ok else 'Certificate not found.', 'success' if ok else 'warning')
    return redirect(url_for('home', section='files'))


@app.post('/documents/<document_id>/delete')
def delete_certificate_document(document_id):
    if not feature_enabled(check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}, 'certificate_manage'):
        set_flash(paid_feature_message('Certificate management'), 'warning')
        return redirect(url_for('home', section='files'))
    ok = set_document_status(document_id, 'deleted')
    set_flash('Certificate deleted from TrainerMate. FOBS copies are unchanged.' if ok else 'Certificate not found.', 'success' if ok else 'warning')
    return redirect(url_for('home', section='files'))


@app.route('/documents/healthcheck', methods=['POST'])
def documents_healthcheck():
    ok, message = run_document_healthcheck()
    set_flash(message, 'success' if ok else 'warning')
    return redirect(url_for('home', section='files'))


@app.route('/documents/prepare-sync', methods=['POST'])
def documents_prepare_sync():
    if not feature_enabled(check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True) or {}, 'certificate_manage'):
        set_flash(paid_feature_message('Certificate management'), 'warning')
        return redirect(url_for('home', section='files'))
    ok, message = prepare_document_provider_updates()
    set_flash(message, 'success' if ok else 'warning')
    return redirect(url_for('home', section='files'))


@app.route('/save', methods=['POST'])
def save_account():
    ndors = (request.form.get('ndors') or '').strip()
    email = (request.form.get('email') or '').strip()
    if ndors and len(ndors) > 64:
        set_flash('That trainer ID looks too long. Please check it and try again.', 'warning')
        return redirect(url_for('home'))
    if email and not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
        set_flash('That email address does not look right. Please check it and try again.', 'warning')
        return redirect(url_for('home'))
    if not ndors and not email:
        set_flash('Enter your trainer ID or email before saving.', 'warning')
        return redirect(url_for('home'))
    if ndors:
        keyring.set_password('trainermate', 'ndors_id', ndors)
    if email:
        keyring.set_password('trainermate', 'email', email)
    set_flash('Account details saved.', 'success')
    return redirect(url_for('home'))


@app.route('/test')
def test_access():
    access = check_access(timeout_seconds=ACTION_ACCESS_TIMEOUT_SECONDS, prefer_cached=False)
    if access:
        set_flash(f"Access {'allowed' if access.get('allowed') else 'blocked'} - Plan: {access.get('plan', 'unknown')}", 'success' if access.get('allowed') else 'warning')
    else:
        set_flash('Could not reach licensing API or no account is saved.', 'error')
    return redirect(url_for('home'))



def reviewer_zoom_access_token_or_message(account_id=''):
    accounts = load_zoom_accounts()
    account = next((a for a in accounts if a.get('id') == (account_id or get_default_zoom_account_id())), None)
    if not account:
        return '', 'Connect a Zoom account first.'
    account_id = account.get('id') or ''
    access_token = get_zoom_oauth_token(account_id, 'access')
    if access_token:
        return access_token, ''
    ok, message = refresh_zoom_oauth_account(account, quiet=True)
    if ok:
        return get_zoom_oauth_token(account_id, 'access'), ''
    return '', 'Zoom needs reconnecting. Open Zoom accounts and connect Zoom again.'


def reviewer_course_by_id(course_id):
    conn = sqlite3.connect(str(COURSES_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        ensure_courses_sync_columns(conn)
        row = conn.execute('SELECT * FROM courses WHERE id = ?', (course_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def reset_reviewer_course_state(disconnect_zoom=False):
    """Reset seeded course rows to a clean trainer-ready state."""
    if not reviewer_demo_enabled():
        return
    conn = sqlite3.connect(str(COURSES_DB_PATH))
    try:
        ensure_courses_sync_columns(conn)
        conn.execute("""
            UPDATE courses
               SET meeting_id = '',
                   meeting_link = '',
                   meeting_password = '',
                   last_synced_at = '',
                   last_sync_status = '',
                   last_sync_action = 'Course imported from provider schedule.'
             WHERE provider = 'Essex'
        """)
        conn.commit()
    finally:
        conn.close()
    if disconnect_zoom:
        save_zoom_accounts([])


@app.route('/reset-dashboard-data', methods=['GET', 'POST'])
def reset_dashboard_data():
    if not reviewer_demo_enabled():
        return redirect(url_for('home'))
    if not reviewer_demo_logged_in():
        return redirect(url_for('trainer_login', next=request.path))
    reset_reviewer_course_state(disconnect_zoom=request.args.get('disconnect') == '1')
    set_flash('TrainerMate has refreshed the course list.', 'success')
    return redirect(url_for('home', section='dashboard', provider='essex'))


def reviewer_update_course_zoom(course_id, meeting_id, join_url, password='', action='Zoom meeting created for TrainerMate'):
    conn = sqlite3.connect(str(COURSES_DB_PATH))
    try:
        ensure_courses_sync_columns(conn)
        conn.execute("""
            UPDATE courses
               SET meeting_id = ?, meeting_link = ?, meeting_password = ?, last_synced_at = ?,
                   last_sync_status = 'success', last_sync_action = ?
             WHERE id = ?
        """, (str(meeting_id or ''), join_url or '', password or '', utc_now_text(), action, course_id))
        conn.commit()
    finally:
        conn.close()


def reviewer_zoom_payload_for_course(course, replace=False):
    start_dt = parse_dashboard_datetime(course.get('date_time') or '') or (datetime.now() + timedelta(days=7))
    topic = f"TrainerMate - {course.get('title') or 'Training course'}"
    return {
        'topic': topic,
        'type': 2,
        'start_time': start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'duration': 120,
        'timezone': 'Europe/London',
        'agenda': 'TrainerMate scheduled course.',
        'settings': {
            'join_before_host': False,
            'waiting_room': True,
            'approval_type': 2,
        },
    }


def reviewer_zoom_request(method, url, token, **kwargs):
    headers = kwargs.pop('headers', {}) or {}
    headers['Authorization'] = f'Bearer {token}'
    response = requests.request(method, url, headers=headers, timeout=kwargs.pop('timeout', 20), **kwargs)
    if response.status_code == 401:
        account = next((a for a in load_zoom_accounts() if a.get('id') == get_default_zoom_account_id()), None)
        if account and refresh_zoom_oauth_account(account, quiet=True)[0]:
            token = get_zoom_oauth_token(account.get('id') or '', 'access')
            headers['Authorization'] = f'Bearer {token}'
            response = requests.request(method, url, headers=headers, timeout=20, **kwargs)
    return response


def reviewer_find_existing_zoom_meeting(course, token):
    """Find an existing Zoom meeting for the seeded course before creating one."""
    expected_topic = f"TrainerMate - {course.get('title') or 'Training course'}".strip().lower()
    expected_start = parse_dashboard_datetime(course.get('date_time') or '')
    for meeting_type in ('upcoming', 'scheduled'):
        response = reviewer_zoom_request('GET', 'https://api.zoom.us/v2/users/me/meetings', token, params={'type': meeting_type, 'page_size': 100})
        if response.status_code >= 400:
            continue
        for meeting in (response.json().get('meetings') or []):
            topic = (meeting.get('topic') or '').strip().lower()
            if topic != expected_topic and not topic.startswith(expected_topic + ' -'):
                continue
            if expected_start:
                meeting_start_raw = meeting.get('start_time') or ''
                try:
                    meeting_start = datetime.fromisoformat(meeting_start_raw.replace('Z', '+00:00')).replace(tzinfo=None)
                    if abs((meeting_start - expected_start).total_seconds()) > 12 * 60 * 60:
                        continue
                except Exception:
                    pass
            return meeting
    return None


def reviewer_patch_zoom_meeting(course, meeting_id, token):
    payload = reviewer_zoom_payload_for_course(course, replace=True)
    response = reviewer_zoom_request('PATCH', f'https://api.zoom.us/v2/meetings/{meeting_id}', token, json=payload)
    if response.status_code not in (200, 204):
        return False, response
    get_response = reviewer_zoom_request('GET', f'https://api.zoom.us/v2/meetings/{meeting_id}', token)
    if get_response.status_code == 200:
        data = get_response.json()
        reviewer_update_course_zoom(course.get('id'), data.get('id') or meeting_id, data.get('join_url') or course.get('meeting_link'), data.get('password') or course.get('meeting_password') or '', 'Existing Zoom meeting verified and updated for TrainerMate')
    else:
        reviewer_update_course_zoom(course.get('id'), meeting_id, course.get('meeting_link'), course.get('meeting_password'), 'Existing Zoom meeting updated for TrainerMate')
    return True, response


def reviewer_create_zoom_meeting(course, replace=False):
    token, message = reviewer_zoom_access_token_or_message()
    if not token:
        return False, message

    saved_meeting_id = ''.join(ch for ch in str(course.get('meeting_id') or '') if ch.isdigit())

    # Saved meeting path: verify first, then update in place. This demonstrates
    # meeting:read plus meeting:update/write behaviour without creating duplicates.
    if saved_meeting_id:
        existing = reviewer_zoom_request('GET', f'https://api.zoom.us/v2/meetings/{saved_meeting_id}', token)
        if existing.status_code == 200:
            ok, patch_response = reviewer_patch_zoom_meeting(course, saved_meeting_id, token)
            if ok:
                return True, f"Existing Zoom meeting verified and updated for {course.get('title')}. Meeting ID: {saved_meeting_id}"
            return False, 'TrainerMate found the existing Zoom meeting, but Zoom would not update it. Please try reconnecting Zoom.'
        if existing.status_code not in (404, 410):
            return False, 'TrainerMate could not verify the existing Zoom meeting just now. Please try again in a moment.'

    # No valid saved ID or saved ID was deleted in Zoom: find a matching existing
    # meeting before creating anything, so reviewer testing does not create duplicates.
    matching = reviewer_find_existing_zoom_meeting(course, token)
    if matching and matching.get('id'):
        meeting_id = matching.get('id')
        ok, patch_response = reviewer_patch_zoom_meeting(course, meeting_id, token)
        if ok:
            return True, f"Existing Zoom meeting found, linked, and updated for {course.get('title')}. Meeting ID: {meeting_id}"
        reviewer_update_course_zoom(course.get('id'), meeting_id, matching.get('join_url') or '', matching.get('password') or '', 'Existing Zoom meeting found and linked for TrainerMate')
        return True, f"Existing Zoom meeting found and linked for {course.get('title')}. Meeting ID: {meeting_id}"

    # Only create when there is no saved, verified, or matching meeting.
    payload = reviewer_zoom_payload_for_course(course, replace=replace)
    response = reviewer_zoom_request('POST', 'https://api.zoom.us/v2/users/me/meetings', token, json=payload)
    response.raise_for_status()
    data = response.json()
    reviewer_update_course_zoom(course.get('id'), data.get('id'), data.get('join_url'), data.get('password') or '', 'Zoom meeting created for TrainerMate')
    return True, f"Zoom meeting created for {course.get('title')}. Meeting ID: {data.get('id')}"


def reviewer_demo_courses_for_sync(scan_provider='all', scan_days=7):
    """Return seeded reviewer courses that should be processed by the demo sync."""
    provider_filter = provider_slug(scan_provider or 'all')
    try:
        days = int(scan_days or 7)
    except Exception:
        days = 7
    days = max(1, min(days, PAID_SYNC_WINDOW_DAYS))
    start_dt = datetime.now() - timedelta(hours=1)
    end_dt = datetime.now() + timedelta(days=days)

    conn = sqlite3.connect(str(COURSES_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        ensure_courses_sync_columns(conn)
        rows = conn.execute("""
            SELECT *
              FROM courses
             WHERE COALESCE(active_in_portal, 1) = 1
             ORDER BY date_time ASC, title ASC
        """).fetchall()
    finally:
        conn.close()

    out = []
    for row in rows:
        course = dict(row)
        if provider_filter != 'all' and provider_slug(course.get('provider') or '') != provider_filter:
            continue
        course_dt = parse_dashboard_datetime(course.get('date_time') or '')
        if course_dt and (course_dt < start_dt or course_dt > end_dt):
            continue
        out.append(course)
    return out


def reviewer_sync_seeded_courses(scan_provider='all', scan_days=7):
    """Run the hosted reviewer demo sync against seeded courses, without FOBS scraping."""
    token, token_message = reviewer_zoom_access_token_or_message()
    if not token:
        update_app_state(sync_running=False, pid=None, last_status='Needs attention', last_run_status='blocked', last_message=token_message)
        return False, token_message

    courses = reviewer_demo_courses_for_sync(scan_provider=scan_provider, scan_days=scan_days)
    if not courses:
        message = 'No seeded courses matched that sync window.'
        update_app_state(sync_running=False, pid=None, last_status='Completed with warnings', last_run_status='completed_with_warnings', last_message=message)
        return False, message

    started_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    summary = {
        'outcome': 'running',
        'message': 'Reviewer demo sync is checking seeded courses.',
        'providers': sorted({c.get('provider') or 'Provider' for c in courses}),
        'courses_found': len(courses),
        'courses_processed': 0,
        'fobs_checked': 0,
        'fobs_updated': 0,
        'fobs_failed': 0,
        'health_issues': [],
    }
    update_app_state(
        sync_running=True,
        pid=os.getpid(),
        last_pid=os.getpid(),
        stop_requested=False,
        last_status='Running',
        last_run_status='running',
        last_started_at=started_at,
        last_run_started_at=started_at,
        last_run_finished_at='',
        last_message='Reviewer demo sync started for seeded courses.',
        current_provider='',
        current_course='',
        run_summary=summary,
        scan_request={'provider': provider_slug(scan_provider or 'all'), 'days': scan_days, 'started_at': started_at, 'source': 'reviewer_demo'},
    )

    course_states = {}
    failures = []
    for course in courses:
        if stop_requested():
            break
        provider = course.get('provider') or 'Provider'
        title = course.get('title') or 'Training course'
        date_time = course.get('date_time') or ''
        course_key = f'{provider} | {title} | {date_time}'.strip(' |')
        update_app_state(current_provider=provider, current_course=course_key, last_message=f'Checking Zoom meeting for {title}.')
        try:
            ok, message = reviewer_create_zoom_meeting(course, replace=False)
        except Exception as exc:
            ok, message = False, f'Zoom sync failed for {title}: {exc}'
        summary['courses_processed'] += 1
        summary['fobs_checked'] += 1
        if ok:
            summary['fobs_updated'] += 1
            status = 'success'
        else:
            summary['fobs_failed'] += 1
            failures.append(message)
            status = 'error'
        course_states[course_key] = {
            'status': status,
            'last_action': message,
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        update_app_state(courses=course_states, run_summary=summary, last_message=message)

    finished_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    stopped = stop_requested()
    if stopped:
        outcome = 'stopped'
        final_status = 'Stopped'
        final_message = 'Reviewer demo sync stopped by user.'
    elif failures:
        outcome = 'completed_with_warnings'
        final_status = 'Completed with warnings'
        final_message = f"Reviewer demo sync finished with {len(failures)} course issue(s)."
        summary['health_issues'] = failures[:5]
    else:
        outcome = 'completed'
        final_status = 'Completed'
        final_message = f"Reviewer demo sync completed. {summary['courses_processed']} seeded course(s) checked."

    summary['outcome'] = outcome
    summary['message'] = final_message
    update_app_state(
        sync_running=False,
        pid=None,
        current_provider='',
        current_course='',
        last_status=final_status,
        last_run_status=outcome,
        last_run_finished_at=finished_at,
        last_stopped_at=finished_at,
        last_message=final_message,
        run_summary=summary,
        health_issues=summary.get('health_issues') or [],
        pending_sync_request={},
        scan_request={},
        **({'last_success_at': finished_at} if outcome == 'completed' else {}),
    )
    return not failures and not stopped, final_message


@app.post('/reviewer/course/<course_id>/zoom')
def reviewer_create_or_verify_zoom(course_id):
    if not reviewer_demo_enabled():
        return redirect(url_for('home'))
    course = reviewer_course_by_id(course_id)
    if not course:
        set_flash('Course not found.', 'warning')
        return redirect(url_for('home', section='dashboard'))
    if course.get('meeting_id'):
        token, message = reviewer_zoom_access_token_or_message()
        if not token:
            set_flash(message, 'warning')
        else:
            r = requests.get(f"https://api.zoom.us/v2/meetings/{course.get('meeting_id')}", headers={'Authorization': f'Bearer {token}'}, timeout=20)
            if r.status_code == 200:
                reviewer_update_course_zoom(course_id, course.get('meeting_id'), course.get('meeting_link'), course.get('meeting_password'), 'Existing Zoom meeting verified for TrainerMate')
                set_flash('Existing Zoom meeting re-verified. The course note has been refreshed.', 'success')
            else:
                ok, msg = reviewer_create_zoom_meeting(course, replace=True)
                set_flash(msg, 'success' if ok else 'warning')
    else:
        ok, msg = reviewer_create_zoom_meeting(course, replace=False)
        set_flash(msg, 'success' if ok else 'warning')
    return redirect(url_for('home', section='dashboard', provider='essex'))

@app.route('/sync/course/<course_id>', methods=['POST'])
def check_course_only(course_id):
    if reviewer_demo_enabled():
        return reviewer_create_or_verify_zoom(course_id)
    submitted_key = (request.form.get('course_key') or '').strip()
    submitted_provider = (request.form.get('provider') or '').strip()
    submitted_title = (request.form.get('title') or '').strip()
    submitted_date_time = (request.form.get('date_time') or '').strip()

    # Prefer the exact visible row values submitted by the alert/button. The URL
    # course id is still accepted as a fallback, but must not be allowed to
    # redirect an 08:15 alert into a different same-day course such as 12:00.
    exact_course = load_course_for_exact_action(submitted_provider, submitted_title, submitted_date_time)
    course = exact_course or load_course_for_action(course_id)
    if not course:
        set_flash('Course not found for confirmation check.', 'warning')
        return redirect(request.referrer or url_for('home', section='dashboard'))

    exact_key = course_action_exact_key(course)
    if submitted_key and submitted_key != exact_key and not exact_course:
        set_flash('That course alert no longer matches the saved course row. Refreshing the dashboard so you can choose the exact course again.', 'warning')
        return redirect(url_for('home', section='dashboard', provider=provider_slug(submitted_provider or course.get('provider') or 'all')))

    # Carry the exact visible alert values through when present, so same-day
    # courses cannot be mixed up by provider/date-only matching.
    if submitted_provider:
        course['provider'] = submitted_provider
    if submitted_title:
        course['title'] = submitted_title
    if submitted_date_time:
        course['date_time'] = submitted_date_time
    provider_id = provider_slug(course.get('provider') or 'all')
    access = check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True)
    active_days = effective_sync_window_days(access)
    scan_days = scan_days_for_course_datetime(course.get('date_time') or '', active_days)
    ok, message = start_sync_process(scan_provider=provider_id, scan_days=scan_days, target_course=course)
    if ok:
        message = f"Single-course confirmation check started: {course.get('provider')} - {course.get('title')} - {course.get('date_time')}."
    set_flash(message, 'success' if ok else 'warning')
    return redirect(url_for('home', section='dashboard', provider=provider_id))


@app.post('/course/<course_id>/replace-zoom')
def replace_course_zoom(course_id):
    if reviewer_demo_enabled():
        course = reviewer_course_by_id(course_id)
        if not course:
            set_flash('Course not found for Zoom replacement.', 'warning')
            return redirect(url_for('home', section='dashboard'))
        ok, msg = reviewer_create_zoom_meeting(course, replace=True)
        set_flash(msg, 'success' if ok else 'warning')
        return redirect(url_for('home', section='dashboard', provider='essex'))
    course = load_course_for_action(course_id)
    if not course:
        set_flash('Course not found for Zoom replacement.', 'warning')
        return redirect(request.referrer or url_for('home', section='dashboard'))
    provider_id = provider_slug(course.get('provider') or 'all')
    access = check_access(timeout_seconds=HOME_ACCESS_TIMEOUT_SECONDS, prefer_cached=True)
    active_days = effective_sync_window_days(access)
    scan_days = scan_days_for_course_datetime(course.get('date_time') or '', active_days)
    ok, message = start_sync_process(scan_provider=provider_id, scan_days=scan_days, target_course=course, allow_zoom_replace=True)
    if ok:
        message = f"Zoom replacement started for: {course.get('provider')} - {course.get('title')} - {course.get('date_time')}."
    set_flash(message, 'success' if ok else 'warning')
    return redirect(url_for('home', section='dashboard', provider=provider_id))


@app.post('/course/<course_id>/keep-fobs-zoom')
def keep_fobs_zoom(course_id):
    course = load_course_for_action(course_id)
    if not course:
        set_flash('Course not found.', 'warning')
        return redirect(request.referrer or url_for('home', section='dashboard'))
    conn = sqlite3.connect(str(COURSES_DB_PATH))
    try:
        ensure_courses_sync_columns(conn)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute(
            """
            UPDATE courses
               SET last_synced_at = ?,
                   last_sync_status = 'skipped',
                   last_sync_action = 'Trainer chose to keep the existing FOBS Zoom link'
             WHERE id = ?
            """,
            (now, course_id),
        )
        conn.commit()
    finally:
        conn.close()
    set_flash('Kept the existing FOBS Zoom link for this course.', 'success')
    return redirect(url_for('home', section='dashboard', provider=provider_slug(course.get('provider') or 'all')))


@app.route('/sync/start', methods=['POST'])
def start_sync():
    scan_provider = request.form.get('scan_provider') or request.args.get('provider') or 'all'
    scan_days = request.form.get('scan_days') or 7
    if reviewer_demo_enabled():
        ok, message = reviewer_sync_seeded_courses(scan_provider=scan_provider, scan_days=scan_days)
    else:
        ok, message = start_sync_process(scan_provider=scan_provider, scan_days=scan_days)
    set_flash(message, 'success' if ok else 'warning')
    redirect_provider = scan_provider if scan_provider and scan_provider != 'all' else None
    return redirect(url_for('home', section='dashboard', provider=redirect_provider))


@app.route('/sync/stop', methods=['POST'])
def stop_sync():
    ok, message = stop_sync_process()
    set_flash(message, 'warning' if ok else 'error')
    return redirect(url_for('home', section='dashboard', provider=request.args.get('provider') or None))


@app.route('/redeem', methods=['POST'])
def redeem():
    identity = get_identity()
    ndors = identity['ndors'].strip()
    email = identity['email'].strip() or None
    key = (request.form.get('key') or '').strip()
    if not ndors:
        set_flash('Save your NDORS trainer ID before activating a licence.', 'warning')
        return redirect(url_for('home'))
    if not key:
        set_flash('Enter a licence key to activate.', 'warning')
        return redirect(url_for('home'))
    try:
        response = requests.post(f'{API_URL}/redeem-licence', json={'ndors_trainer_id': ndors, 'email': email, 'licence_key': key}, timeout=20)
        if response.status_code == 200:
            set_flash('Licence activated.', 'success')
        else:
            try:
                detail = response.json().get('detail') or response.text
            except Exception:
                detail = response.text or 'Licence activation failed.'
            set_flash(str(detail), 'error')
    except Exception as exc:
        set_flash(f'Could not reach licensing API: {exc}', 'error')
    return redirect(url_for('home'))


@app.route('/api/state')
def api_state():
    return jsonify(reconcile_running_state())


@app.route('/status')
def app_status():
    state = reconcile_running_state()
    return jsonify({
        'app': APP_NAME,
        'version': APP_VERSION,
        'channel': BUILD_CHANNEL,
        'build': BUILD_NAME,
        'label': BUILD_LABEL,
        'dashboard_url': DASHBOARD_CANONICAL_URL,
        'status': 'running',
        'pid': os.getpid(),
        'sync_running': bool(state.get('sync_running')),
        'update_notice': update_notice_from_access(check_access(prefer_cached=True) or {}),
    })


@app.route('/healthz')
def healthz():
    return jsonify({'ok': True, 'pid': os.getpid(), 'app': APP_NAME, 'version': APP_VERSION, 'channel': BUILD_CHANNEL, 'build': BUILD_NAME})


def sanitize_support_text(text):
    text = str(text or '')
    text = re.sub(r'(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+', r'\1hidden', text)
    text = re.sub(r'(?im)^(.*(?:authorization|cookie)\s*[:=]\s*).+$', r'\1hidden', text)
    text = re.sub(r'(?i)(access[_ -]?token|refresh[_ -]?token|client[_ -]?secret|password|passcode)\s*[:=]\s*\S+', r'\1 hidden', text)
    text = re.sub(r'(?i)(meeting password present:\s*)true', r'\1yes', text)
    return text[-12000:]


def remote_admin_status_payload():
    state = reconcile_running_state()
    providers = load_providers()
    zoom_accounts = load_zoom_accounts()
    cert_status = certificate_scan_snapshot()
    health_issues = state.get('health_issues') if isinstance(state.get('health_issues'), list) else []
    return {
        'sync_running': bool(state.get('sync_running')),
        'last_status': state.get('last_status') or '',
        'last_message': shorten_message(state.get('last_message') or state.get('last_run_status') or '', 240),
        'needs_attention': bool(health_issues or 'error' in (state.get('last_status') or '').lower()),
        'message': shorten_message((health_issues[0] if health_issues else state.get('last_message') or 'Ready'), 240),
        'providers': [
            {
                'id': provider.get('id'),
                'name': provider.get('name'),
                'active': bool(provider.get('active', True)),
                'has_login_url': bool(provider.get('login_url')),
                'has_credentials': bool((provider.get('credentials') or {}).get('username') and (provider.get('credentials') or {}).get('password')),
                'provider_manages_zoom': bool(provider.get('provider_manages_zoom')),
            }
            for provider in providers
        ],
        'zoom_accounts': [
            {
                'id': account.get('id'),
                'email': account.get('email'),
                'nickname': account.get('nickname'),
                'status': account.get('status') or 'connected',
                'is_default': bool(account.get('is_default')),
            }
            for account in zoom_accounts
        ],
        'certificate_job': {
            'running': bool(cert_status.get('running')),
            'latest': cert_status.get('latest') or {},
        },
        'automation': {
            **load_automation_settings(),
            'activity_counts': activity_counts(),
        },
    }


def remote_admin_support_bundle():
    status = remote_admin_status_payload()
    docs = load_documents()
    courses = []
    try:
        raw_courses = load_courses()
        courses = raw_courses[:50]
    except Exception:
        courses = []
    return {
        'collected_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'app': {
            'name': APP_NAME,
            'version': APP_VERSION,
            'build': BUILD_NAME,
            'dashboard_url': DASHBOARD_CANONICAL_URL,
        },
        'status': status,
        'documents': {
            'summary': document_summary(docs),
            'expiry_warnings': document_expiry_warnings(docs, limit=10),
            'total': len(docs),
        },
        'courses': {
            'sample_count': len(courses),
            'upcoming_sample': [
                {
                    'provider': course.get('provider'),
                    'title': course.get('title'),
                    'date_time': course.get('date_time'),
                    'status': course.get('status'),
                    'has_zoom': bool(course.get('meeting_link') or course.get('meeting_id')),
                }
                for course in courses[:20]
            ],
        },
        'logs': {
            'bot_tail': sanitize_support_text('\n'.join(tail_bot_log(160))),
        },
    }


def remote_admin_identity_payload():
    identity = get_identity()
    return {
        'ndors_trainer_id': identity.get('ndors') or '',
        'email': identity.get('email') or '',
        'device_id': get_device_id(),
        'device_name': os.environ.get('COMPUTERNAME') or os.environ.get('HOSTNAME') or '',
        'app_version': APP_VERSION,
        'build': BUILD_NAME,
    }


def post_remote_admin_result(command_id, status, message='', result=None):
    payload = remote_admin_identity_payload()
    payload.update({
        'command_id': command_id,
        'status': status,
        'message': shorten_message(message or '', 500),
        'result': result or {},
    })
    try:
        requests.post(f'{API_URL}/client/commands/result', json=payload, timeout=8)
    except Exception:
        pass


def execute_remote_admin_command(command):
    command_id = command.get('id') or ''
    command_type = command.get('command_type') or ''
    try:
        if command_type == 'health_check':
            return 'completed', 'Health check complete.', remote_admin_status_payload()
        if command_type == 'support_bundle':
            return 'completed', 'Support bundle collected.', remote_admin_support_bundle()
        if command_type == 'request_logs':
            return 'completed', 'Support log collected.', {'log_tail': sanitize_support_text('\n'.join(tail_bot_log(120)))}
        if command_type == 'refresh_certificates':
            started = start_certificate_scan_async('all')
            return ('running' if started else 'completed'), ('Certificate refresh started.' if started else 'Certificate refresh was already running or no providers were available.'), {}
        if command_type == 'sync_today':
            ok, message = start_sync_process(scan_provider='all', scan_days=1)
            return ('running' if ok else 'needs_attention'), message, {}
        if command_type == 'sync_all':
            ok, message = start_sync_process(scan_provider='all', scan_days=7)
            return ('running' if ok else 'needs_attention'), message, {}
        if command_type == 'repair_certificate_cache':
            ok, message = run_document_healthcheck()
            return ('completed' if ok else 'needs_attention'), message, {}
        if command_type == 'refresh_licence':
            access = force_refresh_licence_from_admin('admin_command')
            return 'completed', 'Licence refreshed.', {'plan': access.get('plan'), 'features': access.get('features') or {}}
        if command_type == 'show_message':
            payload = command.get('payload') if isinstance(command.get('payload'), dict) else {}
            message = shorten_message(payload.get('message') or 'TrainerMate support has sent you a message.', 500)
            title = shorten_message(payload.get('title') or 'TrainerMate support message', 120)
            update = payload.get('update') if isinstance(payload.get('update'), dict) else {}
            if update.get('download_url'):
                message = shorten_message(f"{message} Download: {update.get('download_url')}", 650)
            item = add_activity_item('support_message', title, message, 'warning', details={'payload': payload}, source='admin')
            return 'completed', 'Message added to TrainerMate Message Centre.', {'activity_id': item.get('id')}
        return 'refused', 'TrainerMate does not recognise this support command.', {}
    except Exception as exc:
        return 'needs_attention', str(exc), {}


def remote_admin_agent_once():
    identity = remote_admin_identity_payload()
    if not identity.get('ndors_trainer_id'):
        return
    heartbeat = dict(identity)
    heartbeat['status'] = remote_admin_status_payload()
    hb_response = requests.post(f'{API_URL}/client/heartbeat', json=heartbeat, timeout=8)
    try:
        hb_data = hb_response.json() if hb_response is not None else {}
        if isinstance(hb_data, dict) and isinstance(hb_data.get('update'), dict):
            cached = load_cached_access()
            cached_access = cached.get('access') if isinstance(cached, dict) else {}
            if isinstance(cached_access, dict):
                cached_access['update'] = hb_data.get('update')
                save_cached_access(cached_access)
    except Exception:
        pass
    response = requests.post(
        f'{API_URL}/client/commands',
        json={'ndors_trainer_id': identity['ndors_trainer_id'], 'device_id': identity['device_id']},
        timeout=8,
    )
    if response.status_code != 200:
        return
    for command in (response.json().get('commands') or [])[:5]:
        status, message, result = execute_remote_admin_command(command)
        post_remote_admin_result(command.get('id') or '', status, message, result)


def start_remote_admin_agent():
    if os.getenv('TRAINERMATE_REMOTE_ADMIN', '1').strip().lower() in {'0', 'false', 'no'}:
        return

    def runner():
        time.sleep(env_float('TRAINERMATE_REMOTE_ADMIN_DELAY', 8.0, minimum=0.0, maximum=120.0))
        while True:
            try:
                remote_admin_agent_once()
            except Exception:
                pass
            time.sleep(env_float('TRAINERMATE_REMOTE_ADMIN_INTERVAL', 60.0, minimum=15.0, maximum=900.0))

    threading.Thread(target=runner, daemon=True).start()


if __name__ == '__main__':
    print('=' * 60)
    print(BUILD_LABEL)
    print(BUILD_NAME)
    print(f'HTTP Dashboard: {DASHBOARD_CANONICAL_URL}')
    print('=' * 60)
    start_automation_scheduler()
    start_remote_admin_agent()
    app.run(
        host='127.0.0.1',
        port=5000,
        debug=False,
        use_reloader=False,
        threaded=True,
    )
