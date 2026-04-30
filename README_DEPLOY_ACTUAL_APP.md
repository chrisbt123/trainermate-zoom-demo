# TrainerMate actual dashboard reviewer demo

This package is the real `dashboard_app.py` dashboard, patched for a hosted Zoom Marketplace reviewer mode.

It uses dummy provider/course data, but keeps the real TrainerMate dashboard UI and real Zoom OAuth/API actions.

## Render environment variables

Required:

- `TRAINERMATE_REVIEWER_DEMO=1`
- `ZOOM_CLIENT_ID=<Production Zoom Client ID>`
- `ZOOM_CLIENT_SECRET=<Production Zoom Client Secret>`
- `ZOOM_REDIRECT_URI=https://demo.trainermate.xyz/zoom/callback`
- `REVIEWER_PASSWORD=<temporary reviewer password>`
- `FLASK_SECRET_KEY=<long random secret>`
- `SESSION_COOKIE_SECURE=1`

Recommended:

- `TRAINERMATE_STARTUP_CERTIFICATE_SCAN=0`
- `TRAINERMATE_STARTUP_ZOOM_HEALTH_CHECK=0`
- `TRAINERMATE_AUTOMATION_SCHEDULER=0`
- `TRAINERMATE_REMOTE_ADMIN=0`

## Render commands

Build command:

```bash
pip install -r requirements.txt
```

Start command:

```bash
gunicorn app:app
```

## Reviewer flow

1. Open `https://demo.trainermate.xyz`.
2. Log in with the shared reviewer password.
3. Open Zoom accounts and connect Zoom.
4. Return to Dashboard.
5. Use dummy course rows to create/verify Zoom meetings.
6. Use Replace/update Zoom to demonstrate meeting replacement/update behaviour.
7. Disconnect Zoom from the Zoom accounts page.

No real provider credentials are included or required.
