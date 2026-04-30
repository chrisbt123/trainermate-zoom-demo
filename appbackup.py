import os
import secrets
from urllib.parse import urlencode

import requests
from flask import Flask, redirect, request, session, render_template_string

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_urlsafe(32))

ZOOM_CLIENT_ID = os.getenv("ZOOM_CLIENT_ID", "").strip()
ZOOM_CLIENT_SECRET = os.getenv("ZOOM_CLIENT_SECRET", "").strip()
ZOOM_REDIRECT_URI = os.getenv(
    "ZOOM_REDIRECT_URI",
    "https://demo.trainermate.xyz/zoom/callback"
).strip()


HOME_HTML = """
<!doctype html>
<html>
<head>
  <title>TrainerMate Zoom Demo</title>
  <style>
    body{font-family:Arial,sans-serif;max-width:900px;margin:40px auto;padding:0 20px;line-height:1.6}
    .card{border:1px solid #ddd;border-radius:14px;padding:22px;margin:20px 0;background:#fafafa}
    a.button,button{display:inline-block;background:#0b57d0;color:white;padding:12px 18px;border-radius:10px;text-decoration:none;border:0;font-weight:bold}
    code{background:#eee;padding:2px 5px;border-radius:4px}
  </style>
</head>
<body>
  <h1>TrainerMate Zoom Reviewer Demo</h1>

  <div class="card">
    <p>This hosted demo allows Zoom reviewers to test the TrainerMate Zoom OAuth flow using the Production Client ID.</p>
    <p>No FOBS/provider portal access is required for Zoom Marketplace review.</p>
  </div>

  <div class="card">
    <h2>Test Zoom OAuth</h2>
    <p>Click below to authorize TrainerMate with your Zoom account.</p>
    <a class="button" href="/connect-zoom">Connect Zoom Account</a>
  </div>

  <div class="card">
    <h2>Reviewer Notes</h2>
    <ul>
      <li>TrainerMate is normally a secure local desktop workflow.</li>
      <li>This hosted page is provided only so Zoom reviewers can test OAuth and API access.</li>
      <li>Provider portal credentials are private third-party credentials and are not supplied.</li>
    </ul>
  </div>
</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(HOME_HTML)


@app.route("/connect-zoom")
def connect_zoom():
    if not ZOOM_CLIENT_ID:
        return "ZOOM_CLIENT_ID is not configured.", 500

    state = secrets.token_urlsafe(24)
    session["oauth_state"] = state

    params = {
        "response_type": "code",
        "client_id": ZOOM_CLIENT_ID,
        "redirect_uri": ZOOM_REDIRECT_URI,
        "state": state,
    }

    return redirect("https://zoom.us/oauth/authorize?" + urlencode(params))


@app.route("/zoom/callback")
def zoom_callback():
    error = request.args.get("error")
    if error:
        return f"Zoom returned an error: {error}", 400

    code = request.args.get("code")
    state = request.args.get("state")

    if not code:
        return "Zoom did not return an authorization code.", 400

    if not state or state != session.get("oauth_state"):
        return "Invalid OAuth state. Please restart the Zoom connection flow.", 400

    if not ZOOM_CLIENT_ID or not ZOOM_CLIENT_SECRET:
        return "Zoom credentials are not configured.", 500

    token_response = requests.post(
        "https://zoom.us/oauth/token",
        params={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": ZOOM_REDIRECT_URI,
        },
        auth=(ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET),
        timeout=30,
    )

    if token_response.status_code >= 400:
        return f"Zoom token exchange failed: {token_response.text}", 400

    token_data = token_response.json()
    access_token = token_data.get("access_token")

    user_response = requests.get(
        "https://api.zoom.us/v2/users/me",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )

    if user_response.status_code >= 400:
        return f"Zoom user lookup failed: {user_response.text}", 400

    user_data = user_response.json()

    meetings_response = requests.get(
        "https://api.zoom.us/v2/users/me/meetings",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"type": "scheduled", "page_size": 30},
        timeout=30,
    )

    meeting_status = "Meeting read test completed successfully."
    if meetings_response.status_code >= 400:
        meeting_status = f"Meeting read test failed: {meetings_response.text}"

    return render_template_string("""
<!doctype html>
<html>
<head>
  <title>TrainerMate Zoom Connected</title>
  <style>
    body{font-family:Arial,sans-serif;max-width:900px;margin:40px auto;padding:0 20px;line-height:1.6}
    .card{border:1px solid #ddd;border-radius:14px;padding:22px;margin:20px 0;background:#fafafa}
    .ok{color:#0a7a28;font-weight:bold}
    code{background:#eee;padding:2px 5px;border-radius:4px}
  </style>
</head>
<body>
  <h1>Zoom Account Connected</h1>

  <div class="card">
    <p class="ok">OAuth authorization completed successfully.</p>
    <p><strong>Zoom account:</strong> {{ email }}</p>
    <p><strong>Zoom user ID:</strong> {{ user_id }}</p>
    <p><strong>Meeting API test:</strong> {{ meeting_status }}</p>
  </div>

  <div class="card">
    <h2>Reviewer Confirmation</h2>
    <p>This confirms that TrainerMate can:</p>
    <ul>
      <li>Redirect users to Zoom OAuth</li>
      <li>Receive the OAuth callback</li>
      <li>Exchange the authorization code for tokens</li>
      <li>Read the authorized Zoom user profile</li>
      <li>Call the Zoom meetings API using the authorized account</li>
    </ul>
  </div>

  <p><a href="/">Back to demo home</a></p>
</body>
</html>
    """, email=user_data.get("email", ""), user_id=user_data.get("id", ""), meeting_status=meeting_status)


@app.route("/health")
def health():
    return {"ok": True}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
