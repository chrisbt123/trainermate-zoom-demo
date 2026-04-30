import base64
import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import requests
from flask import Flask, redirect, render_template_string, request, session, url_for

APP_NAME = "TrainerMate Reviewer Demo"
ZOOM_AUTH_URL = "https://zoom.us/oauth/authorize"
ZOOM_TOKEN_URL = "https://zoom.us/oauth/token"
ZOOM_API_BASE = "https://api.zoom.us/v2"


def load_zoom_config():
    cfg = {}
    if os.path.exists("zoom_oauth_config.json"):
        try:
            with open("zoom_oauth_config.json", "r", encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception:
            pass
    return {
        "client_id": os.getenv("ZOOM_CLIENT_ID") or cfg.get("client_id", ""),
        "client_secret": os.getenv("ZOOM_CLIENT_SECRET") or cfg.get("client_secret", ""),
        "redirect_uri": os.getenv("ZOOM_REDIRECT_URI") or cfg.get("redirect_uri", "https://demo.trainermate.xyz/zoom/callback"),
    }


def create_app():
    app = Flask(__name__)
    app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(32))
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = os.getenv("SESSION_COOKIE_SECURE", "1") != "0"
    return app


app = create_app()

COURSES = [
    {
        "id": "EFAW-120526",
        "title": "Emergency First Aid at Work",
        "provider": "Demo Provider",
        "trainer": "Demo Trainer",
        "date": "2026-05-12",
        "start": "09:30",
        "end": "16:30",
        "learners": 12,
        "delivery": "Online",
        "notes": "Dummy course data replicating a real TrainerMate scheduled course.",
    },
    {
        "id": "MAN-140526",
        "title": "Manual Handling Refresher",
        "provider": "Demo Provider",
        "trainer": "Demo Trainer",
        "date": "2026-05-14",
        "start": "10:00",
        "end": "12:00",
        "learners": 8,
        "delivery": "Online",
        "notes": "Dummy course data used for Zoom meeting creation and verification.",
    },
    {
        "id": "SAFE-190526",
        "title": "Safeguarding Level 2",
        "provider": "Demo Provider",
        "trainer": "Demo Trainer",
        "date": "2026-05-19",
        "start": "13:00",
        "end": "16:00",
        "learners": 10,
        "delivery": "Online",
        "notes": "Dummy course data; no third-party provider credentials are used.",
    },
]


def now_utc():
    return datetime.now(timezone.utc)


def parse_dt(course, key):
    return datetime.fromisoformat(f"{course['date']}T{course[key]}:00").replace(tzinfo=timezone.utc)


def get_course(course_id):
    return next((c for c in COURSES if c["id"] == course_id), None)


def is_logged_in():
    return session.get("reviewer_logged_in") is True


def require_login():
    if not is_logged_in():
        return redirect(url_for("login", next=request.path))
    return None


def token_store():
    session.setdefault("zoom", {})
    return session["zoom"]


def zoom_connected():
    return bool(token_store().get("access_token"))


def basic_auth_header(client_id, client_secret):
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def save_token_payload(payload):
    z = token_store()
    expires_in = int(payload.get("expires_in", 3600))
    z["access_token"] = payload.get("access_token")
    if payload.get("refresh_token"):
        z["refresh_token"] = payload.get("refresh_token")
    z["token_type"] = payload.get("token_type", "bearer")
    z["expires_at"] = (now_utc() + timedelta(seconds=max(60, expires_in - 120))).isoformat()
    z["last_token_update"] = now_utc().isoformat()
    session.modified = True


def clear_zoom():
    session.pop("zoom", None)
    session.modified = True


def refresh_zoom_if_needed(force=False):
    z = token_store()
    if not z.get("refresh_token"):
        return False, "Zoom needs reconnecting. Please connect Zoom again."
    expires_at = z.get("expires_at")
    if not force and expires_at:
        try:
            if datetime.fromisoformat(expires_at) > now_utc() + timedelta(minutes=5):
                return True, "Zoom token is still valid."
        except Exception:
            pass
    cfg = load_zoom_config()
    if not cfg["client_id"] or not cfg["client_secret"]:
        return False, "Zoom app credentials are not configured on the server."
    resp = requests.post(
        ZOOM_TOKEN_URL,
        headers={"Authorization": basic_auth_header(cfg["client_id"], cfg["client_secret"]), "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "refresh_token", "refresh_token": z["refresh_token"]},
        timeout=20,
    )
    if not resp.ok:
        clear_zoom()
        return False, "Zoom needs reconnecting. TrainerMate could not refresh the Zoom connection automatically."
    save_token_payload(resp.json())
    return True, "Zoom connection refreshed."


def zoom_request(method, path, **kwargs):
    ok, msg = refresh_zoom_if_needed()
    if not ok:
        return None, msg
    z = token_store()
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {z['access_token']}"
    headers["Content-Type"] = "application/json"
    url = f"{ZOOM_API_BASE}{path}"
    resp = requests.request(method, url, headers=headers, timeout=25, **kwargs)
    if resp.status_code in (401, 403):
        ok, msg = refresh_zoom_if_needed(force=True)
        if not ok:
            return None, msg
        headers["Authorization"] = f"Bearer {token_store()['access_token']}"
        resp = requests.request(method, url, headers=headers, timeout=25, **kwargs)
    if not resp.ok:
        try:
            err = resp.json()
        except Exception:
            err = {"message": resp.text[:300]}
        return None, f"Zoom request failed: {err.get('message') or err}"
    if resp.status_code == 204 or not resp.text:
        return {}, None
    return resp.json(), None


BASE_TEMPLATE = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ title or 'TrainerMate Reviewer Demo' }}</title>
  <style>
    body{font-family:Arial, sans-serif;margin:0;background:#f6f7fb;color:#18202a}.wrap{max-width:1100px;margin:0 auto;padding:24px}.nav{background:#172033;color:white}.nav .wrap{display:flex;justify-content:space-between;align-items:center;padding-top:14px;padding-bottom:14px}.nav a{color:white;text-decoration:none;margin-left:16px}.card{background:white;border-radius:14px;padding:20px;margin:18px 0;box-shadow:0 4px 18px rgba(0,0,0,.06)}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px}.btn{display:inline-block;border:0;border-radius:10px;padding:10px 14px;margin:4px;background:#172033;color:white;text-decoration:none;cursor:pointer;font-size:14px}.btn.secondary{background:#eef1f5;color:#172033}.btn.danger{background:#a83232}.pill{display:inline-block;border-radius:999px;padding:6px 10px;font-size:13px;background:#eef1f5}.ok{background:#dcfce7;color:#14532d}.warn{background:#fef3c7;color:#92400e}.bad{background:#fee2e2;color:#991b1b}table{width:100%;border-collapse:collapse}td,th{padding:10px;border-bottom:1px solid #e8ecf2;text-align:left}.muted{color:#667085}.msg{border-radius:12px;padding:12px;background:#eef1f5;margin:12px 0}.success{background:#dcfce7}.error{background:#fee2e2}.small{font-size:13px}.input{padding:10px;border:1px solid #ccd3dd;border-radius:9px;min-width:260px}.check li{margin:8px 0}.code{font-family:monospace;background:#eef1f5;padding:2px 5px;border-radius:5px}</style>
</head>
<body>
  <div class="nav"><div class="wrap"><div><strong>TrainerMate</strong> Reviewer Demo</div><div>{% if logged_in %}<a href="{{ url_for('dashboard') }}">Dashboard</a><a href="{{ url_for('courses') }}">Courses</a><a href="{{ url_for('checklist') }}">Checklist</a><a href="{{ url_for('logout') }}">Log out</a>{% endif %}</div></div></div>
  <main class="wrap">
    {% if message %}<div class="msg {{ message_class or '' }}">{{ message }}</div>{% endif %}
    {{ body|safe }}
  </main>
</body>
</html>
"""


def page(body, title=None, message=None, message_class=None):
    return render_template_string(BASE_TEMPLATE, body=body, title=title, message=message, message_class=message_class, logged_in=is_logged_in())


def zoom_status_html():
    if zoom_connected():
        return '<span class="pill ok">Zoom connected</span>'
    return '<span class="pill warn">Zoom not connected</span>'


def course_meeting(course_id):
    return session.get("meetings", {}).get(course_id, {})


def save_course_meeting(course_id, data):
    session.setdefault("meetings", {})
    session["meetings"][course_id] = data
    session.modified = True


@app.route("/")
def root():
    if not is_logged_in():
        return redirect(url_for("login"))
    return redirect(url_for("dashboard"))


@app.route("/login", methods=["GET", "POST"])
def login():
    password = os.getenv("REVIEWER_PASSWORD", "TrainerMateReview2026!")
    if request.method == "POST":
        if request.form.get("password") == password:
            session["reviewer_logged_in"] = True
            session.setdefault("checklist", {})["login"] = True
            session.modified = True
            return redirect(request.args.get("next") or url_for("dashboard"))
        return page('<div class="card"><h1>Reviewer login</h1><p class="bad pill">Incorrect password</p><form method="post"><input class="input" type="password" name="password" placeholder="Reviewer password"><button class="btn">Log in</button></form></div>', "Login")
    return page('<div class="card"><h1>TrainerMate Reviewer Demo</h1><p>This sandbox uses dummy provider/course data and the real Zoom OAuth/API flow for Marketplace review.</p><form method="post"><input class="input" type="password" name="password" placeholder="Reviewer password"><button class="btn">Log in</button></form><p class="muted small">No private provider credentials are used in this demo.</p></div>', "Login")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard")
def dashboard():
    r = require_login()
    if r: return r
    body = f'''
    <div class="card"><h1>TrainerMate reviewer dashboard</h1><p class="muted">Logged in as Demo Trainer · Provider: Demo Provider · Environment: Hosted reviewer sandbox</p><p>{zoom_status_html()}</p><a class="btn" href="{url_for('zoom_connect')}">Connect / Refresh Zoom</a><a class="btn secondary" href="{url_for('zoom_account')}">Check Zoom account</a><a class="btn secondary" href="{url_for('zoom_read_test')}">Run meeting API read test</a><a class="btn danger" href="{url_for('zoom_disconnect')}">Disconnect Zoom</a></div>
    <div class="grid">
      <div class="card"><h2>Upcoming dummy courses</h2><p>Course data is preloaded to mirror a real TrainerMate workflow without private provider access.</p><a class="btn" href="{url_for('courses')}">View courses</a></div>
      <div class="card"><h2>Review purpose</h2><p>Reviewers can test OAuth, callback handling, token exchange, account verification, meeting read/create/update/delete behavior, and reconnect/deauthorization states.</p></div>
    </div>
    '''
    return page(body, "Dashboard")


@app.route("/courses")
def courses():
    r = require_login()
    if r: return r
    rows = []
    for c in COURSES:
        m = course_meeting(c["id"])
        status = "Meeting created" if m.get("id") else "No meeting yet"
        rows.append(f'<tr><td><strong>{c["title"]}</strong><br><span class="muted small">{c["id"]}</span></td><td>{c["date"]}</td><td>{c["start"]}–{c["end"]}</td><td>{c["provider"]}</td><td>{status}</td><td><a class="btn secondary" href="{url_for("course_detail", course_id=c["id"])}">View</a></td></tr>')
    body = '<div class="card"><h1>Dummy scheduled courses</h1><table><tr><th>Course</th><th>Date</th><th>Time</th><th>Provider</th><th>Zoom status</th><th></th></tr>' + ''.join(rows) + '</table></div>'
    return page(body, "Courses")


@app.route("/course/<course_id>")
def course_detail(course_id):
    r = require_login()
    if r: return r
    c = get_course(course_id)
    if not c:
        return page('<div class="card">Course not found.</div>', "Course")
    m = course_meeting(course_id)
    meeting_html = '<p><span class="pill warn">No Zoom meeting created yet</span></p>'
    if m.get("id"):
        meeting_html = f'<p><span class="pill ok">Zoom meeting created</span></p><p><strong>Meeting ID:</strong> {m.get("id")}</p><p><strong>Join URL:</strong> <a href="{m.get("join_url")}" target="_blank">{m.get("join_url")}</a></p><p class="muted small">Last action: {m.get("last_action", "created")}</p>'
    body = f'''
    <div class="card"><h1>{c["title"]}</h1><p class="muted">{c["provider"]} · {c["trainer"]} · {c["learners"]} learners · {c["delivery"]}</p><p><strong>Date/time:</strong> {c["date"]} {c["start"]}–{c["end"]}</p><p>{c["notes"]}</p></div>
    <div class="card"><h2>Zoom meeting workflow</h2>{meeting_html}<p>{zoom_status_html()}</p><a class="btn" href="{url_for('create_meeting', course_id=course_id)}">Create Zoom meeting</a><a class="btn secondary" href="{url_for('verify_meeting', course_id=course_id)}">Verify existing meeting</a><a class="btn secondary" href="{url_for('update_meeting', course_id=course_id)}">Replace / update meeting</a><a class="btn danger" href="{url_for('delete_meeting', course_id=course_id)}">Delete demo meeting</a></div>
    <div class="card"><h2>Provider workflow note</h2><p>In the live desktop workflow, provider course data is read locally by the authorised trainer. This review environment uses dummy provider/course records so Zoom reviewers can test the complete Zoom-facing flow without accessing protected third-party systems.</p></div>
    '''
    return page(body, c["title"])


@app.route("/zoom/connect")
def zoom_connect():
    r = require_login()
    if r: return r
    cfg = load_zoom_config()
    if not cfg["client_id"]:
        return page('<div class="card">Zoom Client ID is not configured.</div>', "Zoom", "Zoom credentials missing", "error")
    state = secrets.token_urlsafe(24)
    session["zoom_oauth_state"] = state
    params = {"response_type": "code", "client_id": cfg["client_id"], "redirect_uri": cfg["redirect_uri"], "state": state}
    return redirect(f"{ZOOM_AUTH_URL}?{urlencode(params)}")


@app.route("/zoom/callback")
def zoom_callback():
    if not is_logged_in():
        # Preserve callback state by setting a clear message, not exposing technical errors.
        return redirect(url_for("login", next=request.full_path))
    state = request.args.get("state")
    code = request.args.get("code")
    if not code or state != session.get("zoom_oauth_state"):
        return page('<div class="card"><h1>Zoom connection needs attention</h1><p>The Zoom sign-in could not be completed. Please return to the dashboard and click Connect Zoom again.</p><a class="btn" href="/dashboard">Back to dashboard</a></div>', "Zoom", "Zoom connection could not be completed.", "error")
    cfg = load_zoom_config()
    resp = requests.post(
        ZOOM_TOKEN_URL,
        headers={"Authorization": basic_auth_header(cfg["client_id"], cfg["client_secret"]), "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "authorization_code", "code": code, "redirect_uri": cfg["redirect_uri"]},
        timeout=25,
    )
    if not resp.ok:
        return page('<div class="card"><h1>Zoom connection needs attention</h1><p>TrainerMate could not finish connecting Zoom. Please try Connect Zoom again.</p><a class="btn" href="/dashboard">Back to dashboard</a></div>', "Zoom", "Zoom token exchange failed.", "error")
    save_token_payload(resp.json())
    session.setdefault("checklist", {})["connect"] = True
    session.modified = True
    return redirect(url_for("zoom_account"))


@app.route("/zoom/account")
def zoom_account():
    r = require_login()
    if r: return r
    data, err = zoom_request("GET", "/users/me")
    if err:
        return page('<div class="card"><h1>Zoom needs reconnecting</h1><p>TrainerMate could not verify the Zoom account automatically.</p><a class="btn" href="/zoom/connect">Reconnect Zoom</a></div>', "Zoom", err, "error")
    session.setdefault("checklist", {})["account"] = True
    session.modified = True
    body = f'<div class="card"><h1>Zoom account verified</h1><p><span class="pill ok">Zoom account verification completed successfully</span></p><p><strong>Zoom user:</strong> {data.get("email", "Verified account")}</p><a class="btn" href="/dashboard">Back to dashboard</a></div>'
    return page(body, "Zoom account")


@app.route("/zoom/read-test")
def zoom_read_test():
    r = require_login()
    if r: return r
    data, err = zoom_request("GET", "/users/me/meetings?page_size=10")
    if err:
        return page('<div class="card"><h1>Zoom needs reconnecting</h1><p>TrainerMate could not run the meeting read test.</p><a class="btn" href="/zoom/connect">Reconnect Zoom</a></div>', "Zoom", err, "error")
    session.setdefault("checklist", {})["read"] = True
    session.modified = True
    return page(f'<div class="card"><h1>Meeting API read test completed</h1><p><span class="pill ok">Meeting API read access verified</span></p><p>Meetings returned in test response: {len(data.get("meetings", []))}</p><a class="btn" href="/dashboard">Back to dashboard</a></div>', "Meeting read")


@app.route("/course/<course_id>/create")
def create_meeting(course_id):
    r = require_login()
    if r: return r
    c = get_course(course_id)
    if not c: return redirect(url_for("courses"))
    if not zoom_connected():
        return redirect(url_for("zoom_connect"))
    start_dt = parse_dt(c, "start").isoformat().replace("+00:00", "Z")
    duration = int((parse_dt(c, "end") - parse_dt(c, "start")).total_seconds() / 60)
    payload = {"topic": f"TrainerMate Demo - {c['title']}", "type": 2, "start_time": start_dt, "duration": duration, "timezone": "Europe/London", "agenda": f"Dummy TrainerMate course: {c['title']} ({c['id']})", "settings": {"join_before_host": False, "waiting_room": True}}
    data, err = zoom_request("POST", "/users/me/meetings", json=payload)
    if err:
        return page(f'<div class="card"><h1>Meeting could not be created</h1><p>{err}</p><a class="btn" href="{url_for("course_detail", course_id=course_id)}">Back to course</a></div>', "Create meeting", err, "error")
    save_course_meeting(course_id, {"id": data.get("id"), "join_url": data.get("join_url"), "uuid": data.get("uuid"), "last_action": "created", "updated_at": now_utc().isoformat()})
    session.setdefault("checklist", {})["create"] = True
    session.modified = True
    return redirect(url_for("course_detail", course_id=course_id))


@app.route("/course/<course_id>/verify")
def verify_meeting(course_id):
    r = require_login()
    if r: return r
    m = course_meeting(course_id)
    if not m.get("id"):
        return page(f'<div class="card"><h1>No meeting exists yet</h1><p>Create a Zoom meeting for this dummy course first, then verify it.</p><a class="btn" href="{url_for("course_detail", course_id=course_id)}">Back to course</a></div>', "Verify meeting")
    data, err = zoom_request("GET", f"/meetings/{m['id']}")
    if err:
        return page(f'<div class="card"><h1>Meeting verification needs attention</h1><p>{err}</p><a class="btn" href="{url_for("course_detail", course_id=course_id)}">Back to course</a></div>', "Verify meeting", err, "error")
    m["last_action"] = "verified"
    m["updated_at"] = now_utc().isoformat()
    save_course_meeting(course_id, m)
    session.setdefault("checklist", {})["verify"] = True
    session.modified = True
    return page(f'<div class="card"><h1>Existing Zoom meeting verified</h1><p><span class="pill ok">Meeting verified successfully</span></p><p><strong>Meeting ID:</strong> {data.get("id")}</p><p><strong>Topic:</strong> {data.get("topic")}</p><a class="btn" href="{url_for("course_detail", course_id=course_id)}">Back to course</a></div>', "Verify meeting")


@app.route("/course/<course_id>/update")
def update_meeting(course_id):
    r = require_login()
    if r: return r
    c = get_course(course_id)
    m = course_meeting(course_id)
    if not m.get("id"):
        return redirect(url_for("create_meeting", course_id=course_id))
    payload = {"topic": f"TrainerMate Demo - UPDATED - {c['title']}", "agenda": f"Updated by TrainerMate reviewer demo at {now_utc().isoformat()}"}
    _, err = zoom_request("PATCH", f"/meetings/{m['id']}", json=payload)
    if err:
        return page(f'<div class="card"><h1>Meeting update needs attention</h1><p>{err}</p><a class="btn" href="{url_for("course_detail", course_id=course_id)}">Back to course</a></div>', "Update meeting", err, "error")
    m["last_action"] = "updated/replaced"
    m["updated_at"] = now_utc().isoformat()
    save_course_meeting(course_id, m)
    session.setdefault("checklist", {})["update"] = True
    session.modified = True
    return redirect(url_for("course_detail", course_id=course_id))


@app.route("/course/<course_id>/delete")
def delete_meeting(course_id):
    r = require_login()
    if r: return r
    m = course_meeting(course_id)
    if m.get("id"):
        zoom_request("DELETE", f"/meetings/{m['id']}")
    session.setdefault("meetings", {}).pop(course_id, None)
    session.modified = True
    return redirect(url_for("course_detail", course_id=course_id))


@app.route("/zoom/disconnect")
def zoom_disconnect():
    r = require_login()
    if r: return r
    clear_zoom()
    session.setdefault("checklist", {})["disconnect"] = True
    session.modified = True
    return page('<div class="card"><h1>Zoom disconnected</h1><p><span class="pill warn">Zoom is not connected</span></p><p>Reviewers can now test the reconnect flow.</p><a class="btn" href="/zoom/connect">Reconnect Zoom</a><a class="btn secondary" href="/dashboard">Back to dashboard</a></div>', "Zoom disconnected")


@app.route("/checklist")
def checklist():
    r = require_login()
    if r: return r
    done = session.get("checklist", {})
    items = [("login", "Log in to TrainerMate reviewer dashboard"), ("connect", "Connect Zoom using Production Client ID"), ("account", "Confirm Zoom account verification"), ("read", "Run meeting API read test"), ("create", "Create Zoom meeting for a dummy course"), ("verify", "Verify existing Zoom meeting"), ("update", "Replace/update Zoom meeting"), ("disconnect", "Disconnect Zoom and test reconnect state")]
    lis = ''.join([f'<li>{"✅" if done.get(k) else "⬜"} {label}</li>' for k, label in items])
    body = f'<div class="card"><h1>Reviewer test checklist</h1><ul class="check">{lis}</ul><a class="btn" href="/courses">Continue testing</a></div>'
    return page(body, "Checklist")


@app.route("/healthz")
def healthz():
    return {"ok": True, "app": APP_NAME}


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
