"""TrainerMate Zoom OAuth broker.

This is a deliberately small hosted service for the approved Zoom callback URL:
    https://demo.trainermate.xyz/zoom/callback

It keeps the Zoom Client Secret on the hosted backend, starts Zoom OAuth for the
local TrainerMate desktop app, exchanges Zoom authorization codes server-side,
and hands a short-lived one-time broker code back to the local desktop app.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from pydantic import BaseModel, Field

APP_NAME = "TrainerMate Zoom connection service"
BASE_DIR = Path(__file__).resolve().parent
PENDING_PATH = Path(os.getenv("TRAINERMATE_ZOOM_PENDING_PATH", "/tmp/trainermate_zoom_pending.json"))

ZOOM_CLIENT_ID = os.getenv("ZOOM_CLIENT_ID", "").strip()
ZOOM_CLIENT_SECRET = os.getenv("ZOOM_CLIENT_SECRET", "").strip()
ZOOM_REDIRECT_URI = os.getenv("TRAINERMATE_ZOOM_REDIRECT_URI", "https://demo.trainermate.xyz/zoom/callback").strip()
STATE_SECRET = os.getenv("TRAINERMATE_ZOOM_OAUTH_STATE_SECRET", "").strip()
STATE_TTL_SECONDS = int(os.getenv("TRAINERMATE_ZOOM_STATE_TTL_SECONDS", "600"))
BROKER_CODE_TTL_SECONDS = int(os.getenv("TRAINERMATE_ZOOM_BROKER_CODE_TTL_SECONDS", "300"))

ALLOWED_LOCAL_CALLBACKS = {
    "http://127.0.0.1:5000/zoom/callback",
    "http://localhost:5000/zoom/callback",
}

app = FastAPI(title=APP_NAME, docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    allow_credentials=False,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"],
)


class ZoomOAuthStartRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1, max_length=64)
    email: str | None = None
    device_id: str = Field(..., min_length=1, max_length=160)
    device_name: str | None = None
    app_version: str | None = None
    client_app: str | None = None
    state: str = Field(..., min_length=8)
    return_url: str = Field(..., min_length=1)
    nickname: str | None = None


class ZoomOAuthRedeemRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1, max_length=64)
    email: str | None = None
    device_id: str = Field(..., min_length=1, max_length=160)
    device_name: str | None = None
    app_version: str | None = None
    client_app: str | None = None
    state: str = Field(..., min_length=8)
    broker_code: str = Field(..., min_length=16)


class ZoomOAuthRefreshRequest(BaseModel):
    ndors_trainer_id: str = Field(..., min_length=1, max_length=64)
    email: str | None = None
    device_id: str = Field(..., min_length=1, max_length=160)
    device_name: str | None = None
    app_version: str | None = None
    client_app: str | None = None
    account_id: str | None = None
    refresh_token: str = Field(..., min_length=10)


def now_ts() -> int:
    return int(time.time())


def valid_ndors_id(value: str | None) -> bool:
    text = (value or "").strip()
    return bool(text and "@" not in text and " " not in text and len(text) <= 64 and re.match(r"^[A-Za-z0-9_-]+$", text))


def service_ready() -> bool:
    return bool(ZOOM_CLIENT_ID and ZOOM_CLIENT_SECRET and ZOOM_REDIRECT_URI and STATE_SECRET)


def safe_local_return_url(value: str) -> str:
    text = (value or "").strip()
    if text not in ALLOWED_LOCAL_CALLBACKS:
        raise HTTPException(status_code=400, detail="Invalid TrainerMate return URL")
    return text


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value or "") + padding)


def sign_state(payload: dict[str, Any]) -> str:
    if not STATE_SECRET:
        raise HTTPException(status_code=503, detail="Zoom OAuth state signing is not configured")
    body = b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    sig = hmac.new(STATE_SECRET.encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()
    return body + "." + b64url_encode(sig)


def verify_state(token: str) -> dict[str, Any]:
    try:
        body, sig = (token or "").split(".", 1)
        expected = b64url_encode(hmac.new(STATE_SECRET.encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest())
        if not hmac.compare_digest(sig, expected):
            raise ValueError("state signature mismatch")
        payload = json.loads(b64url_decode(body).decode("utf-8"))
        if int(payload.get("exp") or 0) < now_ts():
            raise ValueError("state expired")
        return payload if isinstance(payload, dict) else {}
    except Exception:
        raise HTTPException(status_code=400, detail="Zoom connection could not be verified")


def load_pending() -> dict[str, dict[str, Any]]:
    try:
        data = json.loads(PENDING_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    if not isinstance(data, dict):
        data = {}
    current = now_ts()
    cleaned = {k: v for k, v in data.items() if isinstance(v, dict) and int(v.get("exp") or 0) >= current}
    if cleaned != data:
        save_pending(cleaned)
    return cleaned


def save_pending(data: dict[str, dict[str, Any]]) -> None:
    PENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = PENDING_PATH.with_name(PENDING_PATH.name + f".{os.getpid()}.{secrets.token_hex(4)}.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, PENDING_PATH)
    try:
        os.chmod(PENDING_PATH, 0o600)
    except Exception:
        pass


def create_broker_code(entry: dict[str, Any]) -> str:
    data = load_pending()
    code = secrets.token_urlsafe(32)
    clean = dict(entry)
    clean["exp"] = now_ts() + max(60, BROKER_CODE_TTL_SECONDS)
    data[code] = clean
    save_pending(data)
    return code


def pop_broker_code(code: str) -> dict[str, Any]:
    data = load_pending()
    entry = data.pop((code or "").strip(), None)
    save_pending(data)
    if not isinstance(entry, dict):
        raise HTTPException(status_code=400, detail="Zoom connection code has expired. Please try Connect Zoom again.")
    return entry


def zoom_token_request(params: dict[str, str]) -> dict[str, Any]:
    if not service_ready():
        raise HTTPException(status_code=503, detail="Zoom OAuth is not configured")
    query = urllib.parse.urlencode(params)
    basic = base64.b64encode(f"{ZOOM_CLIENT_ID}:{ZOOM_CLIENT_SECRET}".encode("utf-8")).decode("ascii")
    request = urllib.request.Request(
        "https://zoom.us/oauth/token?" + query,
        data=b"",
        headers={
            "Authorization": f"Basic {basic}",
            "User-Agent": "TrainerMate/1.0 Zoom OAuth",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=25) as response:
            data = json.loads(response.read().decode("utf-8"))
            return data if isinstance(data, dict) else {}
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            detail = str(exc)
        raise HTTPException(status_code=502, detail=f"Zoom rejected the OAuth request: {detail or exc}")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Zoom OAuth request failed: {exc}")


def zoom_api_get_me(access_token: str) -> dict[str, Any]:
    if not access_token:
        return {}
    request = urllib.request.Request(
        "https://api.zoom.us/v2/users/me",
        headers={
            "Authorization": f"Bearer {access_token}",
            "User-Agent": "TrainerMate/1.0 Zoom OAuth",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=25) as response:
            data = json.loads(response.read().decode("utf-8"))
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
    response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
    response.headers.setdefault("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
    if str(request.url.scheme).lower() == "https":
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response


@app.get("/")
def home():
    return PlainTextResponse("TrainerMate Zoom connection service is running.")


@app.get("/health")
@app.get("/healthz")
def health():
    return JSONResponse({"ok": True, "service": "trainermate-zoom-broker", "zoom_configured": service_ready()})


@app.post("/zoom/oauth/start")
def zoom_oauth_start(payload: ZoomOAuthStartRequest):
    if not service_ready():
        raise HTTPException(status_code=503, detail="Zoom OAuth is not configured")
    ndors = (payload.ndors_trainer_id or "").strip()
    if not valid_ndors_id(ndors):
        raise HTTPException(status_code=400, detail="Invalid trainer account")
    local_state = (payload.state or "").strip()
    if not local_state.startswith("tmrelay:"):
        raise HTTPException(status_code=400, detail="Invalid Zoom state")
    return_url = safe_local_return_url(payload.return_url)
    broker_state = sign_state({
        "v": 1,
        "exp": now_ts() + max(60, STATE_TTL_SECONDS),
        "nonce": secrets.token_urlsafe(16),
        "ndors": ndors,
        "email": (payload.email or "").strip(),
        "device_id": (payload.device_id or "").strip(),
        "local_state": local_state,
        "return_url": return_url,
        "nickname": (payload.nickname or "").strip(),
    })
    authorize_url = "https://zoom.us/oauth/authorize?" + urllib.parse.urlencode({
        "response_type": "code",
        "client_id": ZOOM_CLIENT_ID,
        "redirect_uri": ZOOM_REDIRECT_URI,
        "state": broker_state,
    })
    return {"ok": True, "authorize_url": authorize_url}


@app.get("/zoom/callback")
def zoom_oauth_callback(request: Request):
    state_token = (request.query_params.get("state") or "").strip()
    try:
        state = verify_state(state_token)
        return_url = safe_local_return_url(state.get("return_url") or "")
        local_state = (state.get("local_state") or "").strip()
    except Exception:
        return HTMLResponse("Zoom connection could not be verified. Please close this tab and try again from TrainerMate.", status_code=400)

    zoom_error = (request.query_params.get("error") or "").strip()
    if zoom_error:
        return RedirectResponse(return_url + "?" + urllib.parse.urlencode({"state": local_state, "zoom_error": zoom_error}))

    code = (request.query_params.get("code") or "").strip()
    if not code:
        return RedirectResponse(return_url + "?" + urllib.parse.urlencode({"state": local_state, "zoom_error": "missing_code"}))

    try:
        token_data = zoom_token_request({
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": ZOOM_REDIRECT_URI,
        })
        access_token = (token_data.get("access_token") or "").strip()
        refresh_token = (token_data.get("refresh_token") or "").strip()
        if not access_token or not refresh_token:
            raise HTTPException(status_code=502, detail="Zoom returned incomplete OAuth tokens")
        broker_code = create_broker_code({
            "local_state": local_state,
            "ndors": state.get("ndors") or "",
            "email": state.get("email") or "",
            "device_id": state.get("device_id") or "",
            "token_data": token_data,
            "zoom_user": zoom_api_get_me(access_token),
        })
        return RedirectResponse(return_url + "?" + urllib.parse.urlencode({"state": local_state, "broker_code": broker_code}))
    except Exception:
        return RedirectResponse(return_url + "?" + urllib.parse.urlencode({"state": local_state, "zoom_error": "oauth_failed"}))


@app.post("/zoom/oauth/redeem")
def zoom_oauth_redeem(payload: ZoomOAuthRedeemRequest):
    ndors = (payload.ndors_trainer_id or "").strip()
    if not valid_ndors_id(ndors):
        raise HTTPException(status_code=400, detail="Invalid trainer account")
    entry = pop_broker_code(payload.broker_code)
    if (entry.get("local_state") or "") != (payload.state or ""):
        raise HTTPException(status_code=400, detail="Zoom connection code did not match this TrainerMate session")
    if (entry.get("ndors") or "").strip() != ndors:
        raise HTTPException(status_code=403, detail="Zoom connection code belongs to a different trainer account")
    expected_device = (entry.get("device_id") or "").strip()
    if expected_device and expected_device != (payload.device_id or "").strip():
        raise HTTPException(status_code=403, detail="Zoom connection code belongs to a different device")
    token_data = entry.get("token_data") if isinstance(entry.get("token_data"), dict) else {}
    return {
        "ok": True,
        "access_token": token_data.get("access_token") or "",
        "refresh_token": token_data.get("refresh_token") or "",
        "expires_in": token_data.get("expires_in"),
        "scope": token_data.get("scope") or "",
        "token_type": token_data.get("token_type") or "bearer",
        "zoom_user": entry.get("zoom_user") if isinstance(entry.get("zoom_user"), dict) else {},
    }


@app.post("/zoom/oauth/refresh")
def zoom_oauth_refresh(payload: ZoomOAuthRefreshRequest):
    if not valid_ndors_id(payload.ndors_trainer_id):
        raise HTTPException(status_code=400, detail="Invalid trainer account")
    data = zoom_token_request({
        "grant_type": "refresh_token",
        "refresh_token": payload.refresh_token,
    })
    return {"ok": True, **(data if isinstance(data, dict) else {})}
