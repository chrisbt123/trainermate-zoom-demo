from pathlib import Path

root = Path(__file__).resolve().parent
dashboard = root / "dashboard_app.py"
text = dashboard.read_text(encoding="utf-8")
courses = (root / "trainermate_courses.py").read_text(encoding="utf-8")
diagnostics = (root / "trainermate_diagnostics.py").read_text(encoding="utf-8")
certificates = (root / "trainermate_certificates.py").read_text(encoding="utf-8")
utils = (root / "trainermate_utils.py").read_text(encoding="utf-8")
activity = (root / "trainermate_activity.py").read_text(encoding="utf-8")
api = (root / "main.py").read_text(encoding="utf-8")
provider_picker = (root / "static" / "document_provider_picker.js").read_text(encoding="utf-8")
support_js = (root / "static" / "support.js").read_text(encoding="utf-8")
calendar_js = (root / "static" / "calendar.js").read_text(encoding="utf-8")
certificate_status_js = (root / "static" / "certificate_status.js").read_text(encoding="utf-8")
app_ui_js = (root / "static" / "app_ui.js").read_text(encoding="utf-8")
live_status_js = (root / "static" / "live_status.js").read_text(encoding="utf-8")
activity_popup_js = (root / "static" / "activity_popup.js").read_text(encoding="utf-8")
dashboard_css = (root / "static" / "dashboard.css").read_text(encoding="utf-8")
activity_css = (root / "static" / "activity.css").read_text(encoding="utf-8")
zoom_review = (root / "ZOOM_MARKETPLACE_REVIEW.md").read_text(encoding="utf-8")
zoom_deauth = (root / "ZOOM_DEAUTHORIZATION.md").read_text(encoding="utf-8")

checks = {
    "calendar provider colours": [
        "TRAINERMATE_PROTECTED: calendar-provider-colours",
        "eventDidMount: function(info)",
        "providerColor",
        "info.el.style.backgroundColor",
        "info.el.style.borderColor",
        "info.el.style.color",
        "backgroundColor': provider_color",
        "borderColor': provider_color",
        "textColor': readable_text_color(provider_color)",
    ],
    "course visibility filter": [
        "from trainermate_courses import",
        "visible_course_where_clause",
        "def load_courses(provider_filter='all'):",
        "def course_counts_by_provider():",
    ],
    "course helper module": [
        "def visible_course_where_clause():",
        "duplicate_removed",
        "removed_confirmed",
        "duplicate resolved by trainer",
        "def suppress_stale_same_provider_slot_duplicates(rows):",
    ],
    "calendar/list parity": [
        "def build_calendar_events(provider_filter='all'):",
        "rows = suppress_stale_same_provider_slot_duplicates(",
        "filtered_courses = suppress_stale_same_provider_slot_duplicates(",
    ],
    "calendar static js": [
        "calendar.js",
        "data-events-url",
    ],
    "certificate all-provider picker": [
        "import trainermate_certificates as certificate_helpers",
        "name='use_all_providers' value='1'",
        "def selected_document_provider_ids(form):",
        "certificate_helpers.selected_document_provider_ids",
        "document_provider_picker.js",
        "all_document_providers_selected",
        "disabled aria-disabled='true'",
    ],
    "certificate helper module": [
        "def valid_document_provider_ids(provider_ids, providers):",
        "def all_document_provider_ids(providers):",
        "def selected_document_provider_ids(form, providers):",
        "use_all_providers",
    ],
    "certificate picker static js": [
        "function syncDocumentProviderPicker()",
        "setLocked(master.checked)",
        "box.disabled = locked",
    ],
    "certificate status static js": [
        "certificate_status.js",
        "applyCertificateStatus",
        "tmCertificateAutoReloadSawRunning",
    ],
    "app ui static js": [
        "app_ui.js",
        "TRAINERMATE_UI_CONFIG",
        "tmOpenModal",
        "tmProviderPresetChanged",
        "tmStartupScreen",
        "config.showStartupOverlay === true",
        "debugLog",
    ],
    "live status static js": [
        "live_status.js",
        "refreshTrainerMateLiveStatus",
        "tmProgressBubble",
        "provider-certificate-delete-form",
    ],
    "activity popup static js": [
        "activity_popup.js",
        "TRAINERMATE_ACTIVITY_CONFIG",
        "tmActivityBubble",
        "/api/activity",
    ],
    "dashboard static css": [
        "dashboard.css",
        "startup-screen",
        "document-provider-grid",
        "tm-progress-bubble",
        "course-modal",
    ],
    "activity static css": [
        "activity.css",
        "detail-grid",
        "card.unread",
        "Messages & Activity",
    ],
    "diagnostics module and page": [
        "from trainermate_diagnostics import",
        "current_section == \"diagnostics\"",
        "current_section == 'diagnostics'",
        "diagnostics_summary_lines",
        "diagnostics_log_text",
    ],
    "diagnostics helper module": [
        "def debug_tools_enabled():",
        "def tail_log(path, max_lines=120",
        "def support_summary_lines(",
        "def support_message_text(",
    ],
    "support static js": [
        "function buildSupportMessage()",
        "tmSupportForm",
        "tmSupportWhatsApp",
        "tmCopySupportSummary",
    ],
    "shared provider utils": [
        "from trainermate_utils import provider_slug",
    ],
    "provider util module": [
        "def provider_slug(value):",
    ],
    "activity helper module": [
        "from trainermate_activity import",
        "def load_activity_history():",
        "def add_activity_item(",
        "def build_sync_activity_from_state(",
        "def compact_activity_items(",
    ],
    "zoom marketplace review pack": [
        "meeting:write",
        "PATCH /v2/meetings/{meetingId}",
        "OAuth client secret is not stored in `zoom_oauth_config.json`",
        "Before Submission Checklist",
        "ZOOM_DEAUTHORIZATION.md",
    ],
    "zoom deauthorization docs": [
        "Disconnect in TrainerMate",
        "Remove in Zoom Marketplace",
        "deletes stored Zoom OAuth access/refresh tokens",
    ],
    "local registration and login": [
        "AUTH_KEYRING_SERVICE = 'trainermate_auth'",
        "def auth_welcome():",
        "def auth_register():",
        "def auth_login():",
        "def auth_change_password():",
        "CHANGE_PASSWORD_TEMPLATE",
        "require_password_change(ndors)",
        "requests.post(f'{API_URL}/change-password'",
        "/register-account",
        "/login-account",
        "Create free account",
        "Already registered?",
        "Remember me on this computer",
        "def update_remember_me():",
        "def local_remember_me_enabled():",
        "def set_local_remember_me(enabled):",
        "LOCAL_AUTH_RATE_LIMITS",
        "def check_local_auth_rate_limit(",
        "Too many attempts. Please wait",
        "Only set the local dashboard password after the account service accepts",
        "registration into a password bypass",
        "Reset password",
        "Email reset code",
        "RESET_CONFIRM_TEMPLATE",
        "requests.post(f'{API_URL}/confirm-password-reset'",
        "requests.post(f'{API_URL}/reset-password'",
        "value=\"\" autocomplete=\"email\" required",
        "plan_text in {'paid', 'pro', 'premium', 'admin', 'licenced', 'licensed'}",
        "status_text in {'paid', 'licenced', 'licensed'}",
        "reason in {'ok', 'account_inactive', 'free_sync_limit_reached', 'update_required'}",
        "old paid cache must not mask a deliberate downgrade",
        "Lock TrainerMate",
        "hashlib.pbkdf2_hmac",
        "requests.post(f'{API_URL}/register-account'",
    ],
    "server account password auth": [
        "class AccountRegisterRequest",
        "class AccountLoginRequest",
        "class AccountPasswordChangeRequest",
        "AUTH_RATE_LIMITS",
        "def check_auth_rate_limit(",
        "clear_auth_rate_limit(request, \"login_account\"",
        "def password_hash(",
        "def password_matches(",
        '@app.post("/register-account")',
        '@app.post("/login-account")',
        '@app.post("/reset-password")',
        '@app.post("/confirm-password-reset")',
        '@app.post("/change-password")',
        "create password reset token",
        "send_password_reset_token_email(",
        "RESEND_API_KEY",
        "https://api.resend.com/emails",
        "def send_email(",
        "Reset code not recognised or already used",
        "password_must_change",
        "def generate_temporary_password(",
        "def generate_reset_token(",
        "def account_email_matches(",
        "If those details match a TrainerMate account, a reset code has been emailed.",
        "password_hash",
        "last_login_at",
        "account_password_reset",
    ],
    "admin account delete": [
        "class AdminAccountDeleteRequest",
        "class AdminPasswordResetRequest",
        "Force password reset",
        "async function forceResetPassword()",
        "confirm_reset:'RESET PASSWORD'",
        "temporary_password",
        "SMTP_HOST",
        "RESEND_FROM_EMAIL",
        "send_temporary_password_email(",
        "Set RESEND_API_KEY and RESEND_FROM_EMAIL on Render",
        "delivered_to",
        "The password was not shown to admin",
        "User must change it on next login",
        '@app.post("/admin/api/accounts/{ndors_trainer_id}/force-password-reset")',
        "def admin_force_password_reset(",
        "Password reset confirmation did not match",
        "account_password_force_reset",
        "Delete user",
        "async function deleteUser()",
        "confirm_delete:'DELETE USER'",
        '@app.post("/admin/api/accounts/{ndors_trainer_id}/delete")',
        "def admin_delete_account(",
        "Delete confirmation did not match",
        "admin_audit(\"account_delete\"",
    ],
}

for name, required in checks.items():
    haystack = text
    if name == "course helper module":
        haystack = courses
    elif name == "diagnostics helper module":
        haystack = diagnostics
    elif name == "certificate helper module":
        haystack = certificates
    elif name == "provider util module":
        haystack = utils
    elif name == "activity helper module":
        haystack = activity + text
    elif name == "zoom marketplace review pack":
        haystack = zoom_review + text
    elif name == "zoom deauthorization docs":
        haystack = zoom_deauth
    elif name == "server account password auth":
        haystack = api + (root / "trainermate_supabase_upgrade.sql").read_text(encoding="utf-8")
    elif name == "admin account delete":
        haystack = api
    elif name == "certificate picker static js":
        haystack = provider_picker
    elif name == "certificate status static js":
        haystack = certificate_status_js + text
    elif name == "app ui static js":
        haystack = app_ui_js + text
    elif name == "live status static js":
        haystack = live_status_js + text
    elif name == "activity popup static js":
        haystack = activity_popup_js + text
    elif name == "dashboard static css":
        haystack = dashboard_css + text
    elif name == "activity static css":
        haystack = activity_css + text
    elif name == "support static js":
        haystack = support_js
    elif name == "calendar provider colours":
        haystack = calendar_js + text
    missing = [item for item in required if item not in haystack]
    if missing:
        raise SystemExit(f"{name} protection failed: missing " + ", ".join(missing))

for forbidden in [
    ".fc-event{border:0!important",
    "2026-05-23",
    "fallback_path",
    "_save_fallback",
    "reset-password', json={",
]:
    if forbidden in text:
        raise SystemExit(f"Regression protection failed: forbidden text found: {forbidden}")

print("OK: TrainerMate regression protections present")
